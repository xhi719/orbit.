"""Admin-only Firestore adapter; imports cloud dependencies only after explicit apply.

No paid service is created. A read-only billing check must prove no billing account
is attached before any Firestore/FCM operation. MemoryStore is test/local evidence.
"""
from __future__ import annotations
from copy import deepcopy
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
from urllib.request import Request, urlopen


class SafetyError(ValueError):
    pass


def iso(value):
    return value.isoformat() if isinstance(value, datetime) else value


def timestamp(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    if not isinstance(value, str):
        raise SafetyError("aware_timestamp_required")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise SafetyError("aware_timestamp_required")
    return result.astimezone(timezone.utc)


class MemoryStore:
    remote = False

    def __init__(self, records=None):
        self.records = deepcopy(records or {})
        self.writes = 0
        self.reads = 0

    def get(self, path):
        self.reads += 1
        return deepcopy(self.records.get(path))

    def put(self, path, value):
        self.records[path] = deepcopy(value)
        self.writes += 1

    def mutate(self, path, callback):
        value, result = callback(self.get(path))
        if value is not None:
            self.put(path, value)
        return result

    def list(self, collection, limit=200):
        prefix = collection + "/"
        rows = [(path[len(prefix):], deepcopy(value)) for path, value in self.records.items()
                if path.startswith(prefix) and "/" not in path[len(prefix):]][:limit]
        self.reads += max(1,len(rows))
        return rows

    def delete(self, path):
        self.records.pop(path, None)
        self.writes += 1

    def list_equal(self, collection, field, value, limit=50):
        if not 1<=limit<=50:raise SafetyError("bounded_source_query_limit_required")
        prefix=collection+"/"
        rows=[(path[len(prefix):],deepcopy(row)) for path,row in self.records.items()
              if path.startswith(prefix) and "/" not in path[len(prefix):] and row.get(field)==value][:limit]
        self.reads+=max(1,len(rows))
        return rows

    def list_cursor(self,collection,after_id=None,limit=25):
        if not 1<=limit<=100 or after_id is not None and (not isinstance(after_id,str) or '/' in after_id):raise SafetyError("bounded_document_cursor_required")
        prefix=collection+"/"
        rows=sorted((path[len(prefix):],deepcopy(value)) for path,value in self.records.items()
                    if path.startswith(prefix) and "/" not in path[len(prefix):] and (after_id is None or path[len(prefix):]>after_id))[:limit]
        self.reads+=max(1,len(rows));return rows

    def list_since(self,collection,field,since,limit=200):
        if collection!="gameEvents" or field!="announcementPublishedAt" or not 1<=limit<=200:raise SafetyError("bounded_announcement_query_required")
        cutoff=timestamp(since);prefix=collection+"/";rows=[]
        for path,value in self.records.items():
            if not path.startswith(prefix) or "/" in path[len(prefix):] or not value.get(field):continue
            try:clock=timestamp(value[field])
            except (ValueError,TypeError):continue
            if clock>=cutoff:rows.append((path[len(prefix):],deepcopy(value)))
        rows=sorted(rows,key=lambda item:(timestamp(item[1][field]),item[0]),reverse=True)[:limit]
        self.reads+=max(1,len(rows));return rows


def verify_no_billing(project_id, credentials, opener=urlopen):
    """Never attach/detach billing. Any unavailable/ambiguous response fails closed."""
    from google.auth.transport.requests import Request as AuthRequest
    credentials.refresh(AuthRequest())
    request = Request(f"https://cloudbilling.googleapis.com/v1/projects/{project_id}/billingInfo",
                      headers={"Authorization": "Bearer " + credentials.token})
    try:
        with opener(request, timeout=20) as response:
            payload = json.loads(response.read(65536))
    except Exception as error:
        raise SafetyError("billing_state_verification_unavailable") from error
    if payload.get("billingEnabled") is not False or payload.get("billingAccountName"):
        raise SafetyError("billing_account_attached_or_state_unknown")
    return True


class FirestoreStore:
    remote = True

    def __init__(self, env=None):
        env = env or os.environ
        if env.get("ORBIT_FIREBASE_APPLY") != "true" or env.get("ORBIT_FIREBASE_SPARK_CONFIRMED") != "true":
            raise SafetyError("explicit_spark_apply_not_authorized")
        self.project_id = env.get("FIREBASE_PROJECT_ID", "")
        if not re.fullmatch(r"[a-z][a-z0-9-]{4,61}[a-z0-9]", self.project_id):
            raise SafetyError("firebase_project_id_required")
        raw = env.get("FIREBASE_SERVICE_ACCOUNT_JSON")
        if not raw and env.get("GOOGLE_APPLICATION_CREDENTIALS"):
            raw = Path(env["GOOGLE_APPLICATION_CREDENTIALS"]).read_text(encoding="utf-8")
        if not raw:
            raise SafetyError("firebase_admin_credentials_not_configured")
        try:
            account = json.loads(raw)
            local_adc = account.get("type") == "authorized_user" and env.get("ORBIT_FIREBASE_LOCAL_ADC_ALLOWED") == "true" and bool(env.get("GOOGLE_APPLICATION_CREDENTIALS")) and not env.get("FIREBASE_SERVICE_ACCOUNT_JSON")
            if not local_adc and (account.get("project_id") != self.project_id or account.get("type") != "service_account"):
                raise SafetyError("credential_project_mismatch")
        except (ValueError, TypeError) as error:
            raise SafetyError("firebase_admin_credential_invalid") from error
        # Local emulator is NEVER silently treated as production or allowed to
        # bypass this production guard. Emulator rule tests use client SDK only.
        if env.get("FIRESTORE_EMULATOR_HOST"):
            raise SafetyError("production_adapter_rejects_emulator_environment")
        import google.auth
        billing_credentials, _ = google.auth.load_credentials_from_dict(account, scopes=["https://www.googleapis.com/auth/cloud-platform"])
        verify_no_billing(self.project_id, billing_credentials)
        import firebase_admin
        from firebase_admin import credentials, firestore
        admin_credential = credentials.ApplicationDefault() if local_adc else credentials.Certificate(account)
        self.app = firebase_admin.initialize_app(admin_credential, {"projectId": self.project_id}, name="orbit-spark-" + self.project_id)
        self.db = firestore.client(app=self.app)
        self.firestore = firestore
        self.writes = 0
        self.reads = 0
        self.max_reads = 2500
        self.max_writes = 500
        from zoneinfo import ZoneInfo
        self.quota_day = datetime.now(timezone.utc).astimezone(ZoneInfo("America/Los_Angeles")).date().isoformat()
        prior = self.db.document("quotaBudgets/" + self.quota_day).get(timeout=20).to_dict() or {}
        self.reads += 1
        if prior.get("writes",0) >= 14000 or prior.get("reads",0) >= 35000:
            raise SafetyError("collector_daily_soft_budget_exhausted")

    def guard(self, writing=False):
        if self.reads >= self.max_reads or writing and self.writes >= self.max_writes:
            raise SafetyError("worker_run_soft_budget_exhausted")

    def get(self, path):
        self.guard()
        self.reads += 1
        return self.db.document(path).get(timeout=20).to_dict()

    def put(self, path, value):
        self.guard(writing=True)
        self.db.document(path).set(value, timeout=20)
        self.writes += 1

    def mutate(self, path, callback):
        self.guard(writing=True)
        ref = self.db.document(path)
        @self.firestore.transactional
        def apply(transaction):
            self.reads += 1
            value, result = callback(ref.get(transaction=transaction).to_dict())
            if value is not None:
                transaction.set(ref, value)
            return value is not None, result
        changed, result = apply(self.db.transaction(max_attempts=3))
        self.writes += int(changed)
        return result

    def list(self, collection, limit=200):
        self.guard()
        rows = [(document.id, document.to_dict()) for document in self.db.collection(collection).limit(min(limit,self.max_reads-self.reads)).stream(timeout=20)]
        self.reads += max(1,len(rows))
        return rows

    def delete(self, path):
        self.guard(writing=True)
        self.db.document(path).delete(timeout=20)
        self.writes += 1

    def list_equal(self, collection, field, value, limit=50):
        self.guard()
        if not 1<=limit<=50:raise SafetyError("bounded_source_query_limit_required")
        from google.cloud.firestore_v1.base_query import FieldFilter
        remaining=min(limit,self.max_reads-self.reads)
        query=self.db.collection(collection).where(filter=FieldFilter(field,"==",value)).limit(remaining)
        rows=[(document.id,document.to_dict()) for document in query.stream(timeout=20)]
        self.reads+=max(1,len(rows))
        return rows

    def list_cursor(self,collection,after_id=None,limit=25):
        self.guard()
        if not 1<=limit<=100 or after_id is not None and (not isinstance(after_id,str) or '/' in after_id):raise SafetyError("bounded_document_cursor_required")
        remaining=min(limit,self.max_reads-self.reads)
        parent=self.db.collection(collection)
        query=parent.order_by("__name__")
        if after_id is not None:query=query.start_after([parent.document(after_id)])
        rows=[(document.id,document.to_dict()) for document in query.limit(remaining).stream(timeout=20)]
        self.reads+=max(1,len(rows));return rows

    def list_since(self,collection,field,since,limit=200):
        self.guard()
        if collection!="gameEvents" or field!="announcementPublishedAt" or not 1<=limit<=200:raise SafetyError("bounded_announcement_query_required")
        from google.cloud.firestore_v1.base_query import FieldFilter
        remaining=min(limit,self.max_reads-self.reads)
        query=self.db.collection(collection).where(filter=FieldFilter(field,">=",timestamp(since))).order_by(field,direction="DESCENDING").limit(remaining)
        rows=[(document.id,document.to_dict()) for document in query.stream(timeout=20)]
        self.reads+=max(1,len(rows));return rows

    def finish_usage(self):
        """Worker-only observations; Firebase client traffic is NOT measured here."""
        ref = self.db.document("quotaBudgets/" + self.quota_day)
        @self.firestore.transactional
        def finish(transaction):
            old = ref.get(transaction=transaction).to_dict() or {}
            values = {"reads":old.get("reads",0)+self.reads+1,"writes":old.get("writes",0)+self.writes+2,
                      "scope":"externalWorkersOnly","updatedAt":datetime.now(timezone.utc)}
            transaction.set(ref,values)
            return values
        values = finish(self.db.transaction(max_attempts=3))
        max_percent = max(values["reads"] / 50000,values["writes"] / 20000)*100
        level = "95" if max_percent>=95 else "85" if max_percent>=85 else "70" if max_percent>=70 else "normal"
        self.db.document("usageSnapshots/"+self.quota_day).set({**values,"readUsagePercent":round(values["reads"]/50000*100,2),
             "writeUsagePercent":round(values["writes"]/20000*100,2),"warningThreshold":level,
             "fullProjectUsageVerified":False,"clientUsageIncluded":False,"storageUsageBytes":None,"transferUsageBytes":None},timeout=20)
        return values

