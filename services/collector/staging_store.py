"""SQLite proof ledger only. No connection or write to an operating database."""
from __future__ import annotations
from datetime import datetime, timedelta, timezone
import hashlib
import json
import sqlite3
import uuid


def now():
    return datetime.now(timezone.utc).isoformat()


class StagingStore:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA busy_timeout=5000")
        self.db.executescript("""
          CREATE TABLE IF NOT EXISTS staging_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
          INSERT OR REPLACE INTO staging_meta VALUES('label','STAGING ONLY');
          INSERT OR REPLACE INTO staging_meta VALUES('operating_database_connected','false');
          CREATE TABLE IF NOT EXISTS backfill_jobs(
            job_key TEXT PRIMARY KEY,source_id TEXT NOT NULL,status TEXT NOT NULL,window_start TEXT NOT NULL,window_end TEXT NOT NULL,
            cursor TEXT NOT NULL DEFAULT '{}',metrics TEXT NOT NULL DEFAULT '{}',owner TEXT,lease_until TEXT,attempts INTEGER NOT NULL DEFAULT 0,
            available_at TEXT,error_code TEXT,finished_at TEXT);
          CREATE TABLE IF NOT EXISTS http_responses(
            id TEXT PRIMARY KEY,source_id TEXT NOT NULL,request_url TEXT NOT NULL,http_status INTEGER,headers_json TEXT NOT NULL,
            raw_body BLOB NOT NULL,raw_sha256 TEXT NOT NULL,observed_at TEXT NOT NULL,job_key TEXT NOT NULL);
          CREATE INDEX IF NOT EXISTS responses_source ON http_responses(source_id,observed_at);
          CREATE TABLE IF NOT EXISTS documents(
            source_id TEXT NOT NULL,external_key TEXT NOT NULL,raw_sha256 TEXT NOT NULL,document_json TEXT NOT NULL,
            published_at TEXT,source_updated_at TEXT,observed_at TEXT NOT NULL,PRIMARY KEY(source_id,external_key,raw_sha256));
          CREATE INDEX IF NOT EXISTS documents_publication ON documents(published_at);
        """)

    def claim(self, source, start, end, at=None):
        at = at or now()
        key = source["id"] + ":" + start + ":" + end
        owner = str(uuid.uuid4())
        expiry = (datetime.fromisoformat(at) + timedelta(minutes=5)).isoformat()
        with self.db:
            # A different window for the same source must not run in parallel.
            self.db.execute("UPDATE backfill_jobs SET status='pending',owner=NULL,lease_until=NULL WHERE source_id=? AND status='processing' AND lease_until<=?", (source["id"], at))
            self.db.execute("INSERT OR IGNORE INTO backfill_jobs(job_key,source_id,status,window_start,window_end) VALUES(?,?,'pending',?,?)", (key, source["id"], start, end))
            row = self.db.execute("SELECT * FROM backfill_jobs WHERE job_key=?", (key,)).fetchone()
            if row["status"] == "success":
                return key, None, json.loads(row["cursor"]), json.loads(row["metrics"])
            if row["lease_until"] and row["lease_until"] > at or row["available_at"] and row["available_at"] > at:
                return key, None, json.loads(row["cursor"]), {**json.loads(row["metrics"]), "claim_blocked": True}
            if self.db.execute("SELECT 1 FROM backfill_jobs WHERE source_id=? AND status='processing' AND lease_until>?", (source["id"], at)).fetchone():
                return key, None, {}, {"claim_blocked": True}
            updated = self.db.execute("UPDATE backfill_jobs SET status='processing',owner=?,lease_until=?,attempts=attempts+1 WHERE job_key=? AND (lease_until IS NULL OR lease_until<=?)", (owner, expiry, key, at))
            if updated.rowcount != 1:
                return key, None, {}, {"claim_blocked": True}
        return key, owner, json.loads(row["cursor"]), json.loads(row["metrics"])

    def save_page(self, key, owner, source, request_url, status, raw, headers, observed_at, documents, cursor, metrics):
        """Raw response, documents and checkpoint commit atomically before next fetch."""
        with self.db:
            row = self.db.execute("SELECT owner,lease_until FROM backfill_jobs WHERE job_key=?", (key,)).fetchone()
            if not row or row["owner"] != owner or row["lease_until"] <= now():
                raise RuntimeError("staging_job_lease_lost")
            self.db.execute("INSERT INTO http_responses VALUES(?,?,?,?,?,?,?,?,?)", (str(uuid.uuid4()), source["id"], request_url,
                status, json.dumps(headers), raw, hashlib.sha256(raw).hexdigest(), observed_at, key))
            for document in documents:
                self.db.execute("INSERT OR IGNORE INTO documents VALUES(?,?,?,?,?,?,?)", (source["id"], document["external_key"], document["raw_sha256"],
                    json.dumps(document, ensure_ascii=False), document["published_at"], document["source_updated_at"], observed_at))
            expiry = (datetime.fromisoformat(now()) + timedelta(minutes=5)).isoformat()
            self.db.execute("UPDATE backfill_jobs SET cursor=?,metrics=?,lease_until=? WHERE job_key=? AND owner=?", (json.dumps(cursor), json.dumps(metrics), expiry, key, owner))

    def finish(self, key, owner, success, metrics, error_code=None, retry_after=0):
        at = now()
        with self.db:
            row = self.db.execute("SELECT attempts,owner FROM backfill_jobs WHERE job_key=?", (key,)).fetchone()
            if not row or row["owner"] != owner:
                raise RuntimeError("staging_job_owner_mismatch")
            delay = max(retry_after, min(86400, 900 * (2 ** min(row["attempts"] - 1, 7))))
            due = None if success else (datetime.fromisoformat(at) + timedelta(seconds=delay)).isoformat()
            self.db.execute("UPDATE backfill_jobs SET status=?,metrics=?,owner=NULL,lease_until=NULL,available_at=?,error_code=?,finished_at=? WHERE job_key=? AND owner=?", ("success" if success else "failed", json.dumps(metrics), due, error_code, at, key, owner))

    def documents_for(self, source_id, start, end):
        # Keep only the latest observed revision per source+external identity.
        rows = self.db.execute("SELECT document_json FROM documents WHERE source_id=? AND published_at>=? AND published_at<? ORDER BY observed_at", (source_id, start, end)).fetchall()
        latest = {}
        for row in rows:
            document = json.loads(row[0])
            latest[document["external_key"]] = document
        return list(latest.values())

    def close(self):
        self.db.close()
