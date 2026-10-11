"""Bounded live public-source 72h backfill, SQLite evidence and public staging snapshot.

No Supabase credentials are read. This is not an operating collection deployment.
Run --live explicitly; tests inject fetch and sleep and never call external services.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import json
from pathlib import Path
import sqlite3
import time
from urllib.parse import urlsplit
from engine import fetch_source, ValidationError, normalize_url
from official_adapter import aware_timestamp, parse_steam_documents, steam_request
from staging_store import StagingStore, now

DEFAULT_END = "2026-10-10T08:40:00Z"


class LedgerReplay:
    """Read-only original HTTP evidence; never falls back to a network request."""
    def __init__(self, path):
        self.db = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)
        rows = self.db.execute("SELECT source_id,request_url,http_status,raw_body,headers_json,observed_at,raw_sha256 FROM http_responses ORDER BY observed_at").fetchall()
        import hashlib
        if any(hashlib.sha256(row[3]).hexdigest() != row[6] for row in rows):
            self.db.close()
            raise ValidationError("replay_raw_hash_mismatch")
        self.pages = {(row[0], normalize_url(row[1])): row for row in rows}
        self.original_observed_at = sorted(row[5] for row in rows)
        self.last_observed_at = None
        self.calls = 0

    def __call__(self, source):
        row = self.pages.get((source["id"], normalize_url(source["url"])))
        if not row:
            raise ValidationError("replay_page_not_recorded")
        import hashlib
        if hashlib.sha256(row[3]).hexdigest() != row[6]:
            raise ValidationError("replay_raw_hash_mismatch")
        self.last_observed_at = row[5]
        self.calls += 1
        return row[2], bytes(row[3]), json.loads(row[4])

    def close(self):
        self.db.close()


def retry_after_seconds(value, at=None):
    try:
        return max(0, min(604800, int(value)))
    except (ValueError, TypeError):
        try:
            parsed = parsedate_to_datetime(value)
            return max(0, min(604800, int((parsed - (at or datetime.now(timezone.utc))).total_seconds())))
        except (ValueError, TypeError, OverflowError):
            return 0


def collect_staging_source(store, source, start, end, fetch=fetch_source, sleep=time.sleep, max_pages=10, page_size=50, deadline=None):
    if not source.get("approved_for_staging") or source.get("access_status") != "approved":
        raise ValidationError("source_access_not_approved")
    key, owner, cursor, previous_metrics = store.claim(source, start.isoformat(), end.isoformat())
    metrics = {"pagesFetched": 0, "itemsSeen": 0, "documentsStored": 0, "itemsSkipped": 0, **previous_metrics}
    if owner is None:
        blocked = metrics.get("claim_blocked", False)
        return {"status": "FETCH_FAILED" if blocked else "success", "reasonCode": "JOB_LOCK_OR_BACKOFF" if blocked else "CACHED_COMPLETED_JOB",
                "metrics": metrics, "windowComplete": not blocked, "fetchAttemptsThisRun": 0, "notices": store.documents_for(source["id"], start.isoformat(), end.isoformat())}
    enddate = cursor.get("enddate", int(end.timestamp()) + 1)
    success, reason, status, last_http, retry_after, host_limited, attempts_this_run = False, None, "FETCH_FAILED", None, 0, False, 0
    try:
        if cursor.get("complete"):
            success, status = True, "success"  # recover a commit completed before worker termination
        for page in range(0 if success else max_pages):
            if deadline is not None and time.monotonic() >= deadline:
                raise ValidationError("run_time_limit")
            sleep(max(1, source.get("rate_policy", {}).get("minimum_request_spacing_seconds", 1)))
            request = steam_request({**source, "enabled": True}, enddate, page_size)
            attempts_this_run += 1
            last_http, raw, headers = fetch(request)
            recorded_observation = getattr(fetch, "last_observed_at", None)
            observed = recorded_observation if isinstance(recorded_observation, str) else now()
            metrics["pagesFetched"] += 1
            if last_http != 200:
                store.save_page(key, owner, source, request["url"], last_http, raw, headers, observed, [], cursor, metrics)
                retry_after = retry_after_seconds(headers.get("retry-after"))
                host_limited = last_http == 429
                status = "ACCESS_RESTRICTED" if last_http in (401, 403) else "FETCH_FAILED"
                raise ValidationError("source_http_" + str(last_http))
            try:
                documents, parsed_metrics = parse_steam_documents(raw, source, observed)
            except Exception:
                store.save_page(key, owner, source, request["url"], last_http, raw, headers, observed, [], cursor, metrics)
                raise
            metrics["itemsSeen"] += parsed_metrics["items_seen"]
            metrics["documentsStored"] += len(documents)
            metrics["itemsSkipped"] += parsed_metrics["items_skipped"]
            dates = [int(aware_timestamp(document["published_at"]).timestamp()) for document in documents if document["published_at"]]
            missing_dates = any(document["published_at"] is None for document in documents)
            oldest = min(dates) if dates else None
            # A full page sharing its oldest timestamp cannot prove there are no omitted
            # same-second posts: the documented cursor is strictly earlier than enddate.
            boundary_ambiguous = (parsed_metrics["items_seen"] >= page_size and oldest is not None
                                  and dates.count(oldest) > 1 and oldest >= int(start.timestamp()))
            complete = not missing_dates and not boundary_ambiguous and (parsed_metrics["items_seen"] < page_size or oldest is not None and oldest < int(start.timestamp()))
            next_cursor = {"enddate": oldest if oldest is not None else enddate, "complete": complete}
            store.save_page(key, owner, source, request["url"], last_http, raw, headers, observed, documents, next_cursor, metrics)
            cursor = next_cursor
            if missing_dates or boundary_ambiguous:
                status, reason = "DATE_UNCONFIRMED", "PUBLICATION_TIMESTAMP_MISSING" if missing_dates else "PAGINATION_TIMESTAMP_BOUNDARY_AMBIGUOUS"
                break
            if complete:
                success, status, reason = True, "success", "PUBLIC_FEED_WINDOW_FETCHED"
                break
            if not dates or oldest >= enddate:
                status, reason = "DATE_UNCONFIRMED", "PAGINATION_CURSOR_NOT_ADVANCING"
                break
            enddate = oldest
        else:
            if not success:
                reason = "PAGE_LIMIT_REACHED"
    except Exception as error:
        reason = str(error)[:160] if isinstance(error, ValidationError) else type(error).__name__
    store.finish(key, owner, success, metrics, reason, retry_after)
    return {"status": status, "reasonCode": reason, "metrics": metrics, "windowComplete": success,
            "fetchAttemptsThisRun": attempts_this_run,
            "httpStatus": last_http, "hostRateLimited": host_limited,
            "notices": store.documents_for(source["id"], start.isoformat(), end.isoformat())}


def public_notice(document, source, game_id):
    return {"id": source["id"] + ":" + document["external_key"], "gameId": game_id,
            "title": document["original_title"], "description": document["original_description"],
            "publishedAt": document["published_at"], "sourceUpdatedAt": document["source_updated_at"],
            "firstCheckedAt": document["observed_at"], "url": document["official_url"],
            "language": document["original_language"], "serverRegion": document["server_region"], "serverLabel": document["server_label"],
            "verificationStatus": "pending", "scheduleStatus": "DATE_UNCONFIRMED", "eventTypeCandidates": document["event_type_candidates"],
            "rawPublicationTimestamp": document["raw_publication_timestamp"], "rawSha256": document["raw_sha256"],
            "pickups": [{"serverRegion": pickup["server_region"], "serverLabel": pickup["server_label"], "pickupKind": None,
                         "characters": [], "weapons": [], "startsAt": None, "endsAt": None,
                         "officialUrl": pickup["official_url"], "verificationStatus": "pending"} for pickup in document["pickups"]]}


def run_backfill(registry, store, start, end, fetch=fetch_source, sleep=time.sleep, max_pages=10, timeout_seconds=900):
    if (end - start) > timedelta(hours=72) or end <= start:
        raise ValidationError("backfill_window_must_be_at_most_72_hours")
    deadline = time.monotonic() + timeout_seconds
    results, notices, limited_hosts = [], [], set()
    for game in registry["games"]:
        entry = {"gameId": game["gameId"], "status": game["baselineStatus"], "reasonCode": game["reasonCode"],
                 "sourceCount": len(game["sources"]), "windowComplete": False, "metrics": {"pagesFetched": 0, "itemsSeen": 0, "documentsStored": 0},
                 "officialWebsite": game.get("officialWebsite"), "serviceStatus": "UNKNOWN", "serverRegion": "UNSPECIFIED"}
        for source in game["sources"]:
            host = urlsplit(source["url"]).hostname
            if host in limited_hosts:
                entry.update(status="FETCH_FAILED", reasonCode="HOST_RATE_LIMIT_DEFERRED")
                continue
            if time.monotonic() >= deadline:
                entry.update(status="FETCH_FAILED", reasonCode="RUN_TIME_LIMIT_NOT_FETCHED")
                continue
            outcome = collect_staging_source(store, source, start, end, fetch, sleep, max_pages=max_pages, deadline=deadline)
            entry.update({key: value for key, value in outcome.items() if key not in ("notices", "hostRateLimited")})
            entry["sourceUrl"] = source["url"]
            entry["noticeCount"] = len(outcome["notices"])
            notices.extend(public_notice(document, source, game["gameId"]) for document in outcome["notices"])
            if outcome.get("hostRateLimited"):
                limited_hosts.add(host)
        results.append(entry)
    statuses = {status: sum(game["status"] == status for game in results) for status in ("success", "NO_SOURCE", "ACCESS_RESTRICTED", "DATE_UNCONFIRMED", "FETCH_FAILED")}
    return {"schemaVersion": 1, "recordedAt": now(), "window": {"start": start.isoformat(), "end": end.isoformat(), "hours": (end-start).total_seconds()/3600},
            "status": "STAGING_ONLY", "operatingDatabaseStatus": "DB_NOT_CONFIGURED", "operatingDatabaseWrites": 0,
            "automaticRemoteCollectionDeployed": False, "notificationEligible": False,
            "gameResults": results, "metrics": {"games": len(results), "statuses": statuses, "notices": len(notices), "events": 0,
                "pagesFetched": sum(game["metrics"].get("pagesFetched", 0) for game in results), "dateUnconfirmedNotices": len(notices)},
            "events": [], "notices": notices,
            "scopeNotes": ["Only reviewed public publisher announcement feeds were queried; no comprehensive all-channel coverage claim.",
                "Publication window is [start,end); ongoing events posted earlier are not established by this backfill.",
                "Steam date is publication time; the feed does not expose source modification time. Complete last-72h edit coverage is unverified.",
                "The public feed supplies publication dates, not verified schedule dates; no calendar event is fabricated.",
                "Server region, account attendance completion, characters/weapons and event boundaries remain unverified.",
                "Raw HTTP responses and document revisions are kept in a local SQLite staging proof ledger; no operating Supabase write occurred."]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", default="data/official-sources.json")
    parser.add_argument("--window-end", default=DEFAULT_END)
    parser.add_argument("--hours", type=int, default=72)
    parser.add_argument("--sqlite", default="work/official-backfill-staging.sqlite")
    parser.add_argument("--snapshot", default="data/official-event-snapshot.json")
    parser.add_argument("--report", default="reports/official-backfill-72h.json")
    parser.add_argument("--max-pages", type=int, default=10)
    parser.add_argument("--timeout-seconds", type=int, default=900)
    access = parser.add_mutually_exclusive_group(required=True)
    access.add_argument("--live", action="store_true", help="Explicitly permit reviewed public network fetches; zero paid resources")
    access.add_argument("--replay-from", help="Read-only replay of original SQLite HTTP ledger; performs zero network requests")
    args = parser.parse_args()
    if args.replay_from and Path(args.replay_from).resolve() == Path(args.sqlite).resolve():
        parser.error("Replay output SQLite must differ from preserved source evidence")
    end = aware_timestamp(args.window_end)
    start = end - timedelta(hours=args.hours)
    if not 1 <= args.hours <= 72 or not 1 <= args.max_pages <= 20 or not 1 <= args.timeout_seconds <= 1800:
        parser.error("bounded window/pages/timeout required")
    for path in (args.sqlite, args.snapshot, args.report):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    registry = json.loads(Path(args.registry).read_text(encoding="utf-8-sig"))
    store = StagingStore(args.sqlite)
    replay = LedgerReplay(args.replay_from) if args.replay_from else None
    try:
        snapshot = run_backfill(registry, store, start, end, fetch=replay or fetch_source,
                                sleep=(lambda _: None) if replay else time.sleep, max_pages=args.max_pages, timeout_seconds=args.timeout_seconds)
        snapshot["accessMode"] = "REAL_HTTP_EVIDENCE_REPLAY" if replay else "LIVE_PUBLIC_HTTP"
        snapshot["newNetworkRequests"] = 0 if replay else sum(game.get("fetchAttemptsThisRun", 0) for game in snapshot["gameResults"])
        if replay:
            snapshot["replayEvidence"] = {"sqlite": args.replay_from, "pagesReplayed": replay.calls,
                "originalFetchStartedAt": replay.original_observed_at[0] if replay.original_observed_at else None,
                "originalFetchFinishedAt": replay.original_observed_at[-1] if replay.original_observed_at else None,
                "rawHashesVerified": True, "mockData": False}
        text = json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n"
        Path(args.snapshot).write_text(text, encoding="utf-8")
        Path(args.report).write_text(text, encoding="utf-8")
        print(json.dumps({"status": snapshot["status"], "recordedAt": snapshot["recordedAt"], "metrics": snapshot["metrics"], "snapshot": args.snapshot, "sqlite": args.sqlite,
                          "operatingDatabaseWrites": 0}, ensure_ascii=False), flush=True)
    finally:
        store.close()
        if replay:
            replay.close()
