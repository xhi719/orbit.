"""Admin-only Firestore adapter; imports cloud dependencies only after explicit apply.

No paid service is created. A read-only billing check must prove no billing account
is attached before any Firestore/FCM operation. MemoryStore is test/local evidence.
"""
from __future__ import annotations
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
from urllib.request import Request, urlopen


class SafetyError(ValueError):
    pass


class ProviderQuotaUnavailable(SafetyError):
    """The provider rejected work; worker counters cannot prove free headroom."""
    def __init__(self,operation="unknown"):
        self.operation=operation
        super().__init__("firestore_provider_quota_exhausted")


def is_provider_quota_error(error):
    """Recognize SDK quota errors without importing cloud dependencies or text."""
    seen=set()
    for _ in range(5):
        if error is None or id(error) in seen:return False
        seen.add(id(error))
        if any(base.__module__=="google.api_core.exceptions" and base.__name__ in ("ResourceExhausted","TooManyRequests") for base in type(error).__mro__):return True
        error=getattr(error,"cause",None) or getattr(error,"__cause__",None)
    return False


CONTENT_CLEANUP_COLLECTIONS = frozenset({"gameEvents", "pickupSchedules", "collectionDocuments"})


def document_fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, default=iso).encode()).hexdigest()


def cleanup_path(path):
    parts = path.split("/") if isinstance(path, str) else []
    if len(parts) != 2 or parts[0] not in CONTENT_CLEANUP_COLLECTIONS or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,200}", parts[1]):
        raise SafetyError("public_content_cleanup_path_required")
    return path


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


class BudgetReservation:
    @contextmanager
    def reserve_budget(self, *, reads=0, writes=0):
        """Keep room for lease release and audit records without raising limits."""
        previous_reads = getattr(self, "reserved_reads", 0)
        previous_writes = getattr(self, "reserved_writes", 0)
        self.reserved_reads = previous_reads + reads
        self.reserved_writes = previous_writes + writes
        try:
            yield
        finally:
            self.reserved_reads = previous_reads
            self.reserved_writes = previous_writes


class MemoryStore(BudgetReservation):
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

    def delete_content_if_unchanged(self, path, expected_fingerprint, guards=None, predicate=None):
        cleanup_path(path)
        guards = guards or {}
        for guard_path in guards: cleanup_path(guard_path)
        row = self.get(path)
        if row is None or document_fingerprint(row) != expected_fingerprint: return False
        guard_rows = {guard_path:self.get(guard_path) for guard_path in guards}
        if any(document_fingerprint(guard_rows[guard_path]) != expected for guard_path, expected in guards.items()): return False
        if predicate is not None and not predicate(row, guard_rows): return False
        self.delete(path)
        return True

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

    def list_stage_dates(self,date_keys,limit=50):
        if not isinstance(date_keys,list)or not 1<=len(date_keys)<=8 or any(not isinstance(day,str)or not re.fullmatch(r"20\d{2}-\d{2}-\d{2}",day)for day in date_keys)or not 1<=limit<=50:raise SafetyError("bounded_public_stage_date_query_required")
        rows=[(path.split("/",1)[1],deepcopy(row))for path,row in self.records.items()if path.startswith("gameEvents/")and "/"not in path.split("/",1)[1]and set(row.get("announcementStageDateKeys",[])).intersection(date_keys)][:limit]
        self.reads+=max(1,len(rows));return rows

    def list_expired_youtube(self,collection,before,limit=50):
        if collection not in ("gameEvents","collectionDocuments") or not 1<=limit<=50:raise SafetyError("bounded_public_youtube_expiry_query_required")
        cutoff=timestamp(before);rows=[];prefix=collection+"/"
        for path,row in self.records.items():
            if not path.startswith(prefix) or "/" in path[len(prefix):] or row.get("metadataExpiresAt") is None:continue
            try:expires=timestamp(row["metadataExpiresAt"])
            except (ValueError,TypeError):continue
            if expires<=cutoff:rows.append((path[len(prefix):],deepcopy(row)))
        rows.sort(key=lambda value:(timestamp(value[1]["metadataExpiresAt"]),value[0]))
        rows=rows[:limit];self.reads+=max(1,len(rows));return rows


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


class FirestoreStore(BudgetReservation):
    remote = True

    def __init__(self, env=None, *, initialize_quota=True):
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
        app_name="orbit-spark-"+self.project_id
        try:self.app=firebase_admin.get_app(app_name)
        except ValueError:self.app=firebase_admin.initialize_app(admin_credential,{"projectId":self.project_id},name=app_name)
        self.db = firestore.client(app=self.app)
        self.firestore = firestore
        self.writes = 0
        self.reads = 0
        self.public_news_changes={}
        self.max_reads = 2500
        self.max_writes = 500
        from zoneinfo import ZoneInfo
        self.quota_day = datetime.now(timezone.utc).astimezone(ZoneInfo("America/Los_Angeles")).date().isoformat()
        if not initialize_quota:return
        prior = self._provider_call(lambda:self.db.document("quotaBudgets/" + self.quota_day).get(timeout=20,retry=None).to_dict(),operation="read") or {}
        self.reads += 1
        self.daily_remaining_reads=max(0,35000-prior.get("reads",0))
        self.daily_remaining_writes=max(0,14000-prior.get("writes",0))
        if prior.get("writes",0) >= 14000 or prior.get("reads",0) >= 35000:
            raise SafetyError("collector_daily_soft_budget_exhausted")

    def read_headroom(self):
        return max(0, min(max(0,self.max_reads-1), max(0, getattr(self, "daily_remaining_reads", self.max_reads) - 1))
                   - self.reads - getattr(self, "reserved_reads", 0))

    def write_headroom(self):
        return max(0, min(max(0,self.max_writes-2), max(0, getattr(self, "daily_remaining_writes", self.max_writes) - 2))
                   - self.writes - getattr(self, "reserved_writes", 0))

    def guard(self, writing=False):
        if getattr(self,"provider_quota_exhausted",False):raise ProviderQuotaUnavailable(getattr(self,"provider_quota_operation","unknown"))
        if self.read_headroom() == 0 or writing and self.write_headroom() == 0:
            raise SafetyError("worker_run_soft_budget_exhausted")

    def _provider_call(self, callback, *, operation="unknown"):
        if getattr(self,"provider_quota_exhausted",False):raise ProviderQuotaUnavailable(getattr(self,"provider_quota_operation","unknown"))
        try:return callback()
        except Exception as error:
            if isinstance(error,ProviderQuotaUnavailable):raise
            if not is_provider_quota_error(error):raise
            self.provider_quota_exhausted=True;self.provider_quota_operation=operation
            raise ProviderQuotaUnavailable(operation) from None

    def _record_public_change(self,path,value):
        parts=path.split("/")
        if len(parts)==2 and parts[0]in("gameEvents","pickupSchedules")and re.fullmatch(r"[A-Za-z0-9_.:-]{1,200}",parts[1]):
            if not hasattr(self,"public_news_changes"):self.public_news_changes={}
            self.public_news_changes[path]=deepcopy(value)
            self.public_news_confirmed_at=datetime.now(timezone.utc).isoformat()

    def get(self, path):
        self.guard()
        self.reads += 1
        return self._provider_call(lambda:self.db.document(path).get(timeout=20,retry=None).to_dict(),operation="read")

    def put(self, path, value):
        self.guard(writing=True)
        self._provider_call(lambda:self.db.document(path).set(value, timeout=20,retry=None),operation="write")
        self.writes += 1
        self._record_public_change(path,value)

    def mutate(self, path, callback):
        self.guard(writing=True)
        ref = self.db.document(path)
        @self.firestore.transactional
        def apply(transaction):
            self.guard(writing=True)
            self.reads += 1
            old=self._provider_call(lambda:ref.get(transaction=transaction,timeout=20,retry=None).to_dict(),operation="read")
            value, result = callback(old)
            if value is not None:
                transaction.set(ref, value)
            return value, result
        value, result = self._provider_call(lambda:apply(self.db.transaction(max_attempts=3)),operation="write")
        self.writes += int(value is not None)
        if value is not None:self._record_public_change(path,value)
        return result

    def list(self, collection, limit=200):
        self.guard()
        rows = self._provider_call(lambda:[(document.id, document.to_dict()) for document in self.db.collection(collection).limit(min(limit,self.read_headroom())).stream(timeout=20,retry=None)],operation="read")
        self.reads += max(1,len(rows))
        return rows

    def delete(self, path):
        self.guard(writing=True)
        self._provider_call(lambda:self.db.document(path).delete(timeout=20,retry=None),operation="write")
        self.writes += 1
        self._record_public_change(path,None)

    def delete_content_if_unchanged(self, path, expected_fingerprint, guards=None, predicate=None):
        """A reviewed public cleanup cannot delete data changed since its dry run."""
        cleanup_path(path)
        guards = guards or {}
        for guard_path in guards: cleanup_path(guard_path)
        self.guard(writing=True)
        ref = self.db.document(path)
        @self.firestore.transactional
        def apply(transaction):
            self.guard(writing=True)
            self.reads += 1
            row = ref.get(transaction=transaction,timeout=20,retry=None).to_dict()
            if row is None or document_fingerprint(row) != expected_fingerprint: return False
            guard_rows = {}
            for guard_path, expected in guards.items():
                self.guard()
                self.reads += 1
                guard_row = self.db.document(guard_path).get(transaction=transaction,timeout=20,retry=None).to_dict()
                if document_fingerprint(guard_row) != expected: return False
                guard_rows[guard_path] = guard_row
            if predicate is not None and not predicate(row, guard_rows): return False
            transaction.delete(ref)
            return True
        changed = self._provider_call(lambda:apply(self.db.transaction(max_attempts=3)),operation="write")
        self.writes += int(changed)
        if changed:self._record_public_change(path,None)
        return changed

    def list_equal(self, collection, field, value, limit=50):
        self.guard()
        if not 1<=limit<=50:raise SafetyError("bounded_source_query_limit_required")
        from google.cloud.firestore_v1.base_query import FieldFilter
        remaining=min(limit,self.read_headroom())
        query=self.db.collection(collection).where(filter=FieldFilter(field,"==",value)).limit(remaining)
        rows=self._provider_call(lambda:[(document.id,document.to_dict()) for document in query.stream(timeout=20,retry=None)],operation="read")
        self.reads+=max(1,len(rows))
        return rows

    def list_cursor(self,collection,after_id=None,limit=25):
        self.guard()
        if not 1<=limit<=100 or after_id is not None and (not isinstance(after_id,str) or '/' in after_id):raise SafetyError("bounded_document_cursor_required")
        remaining=min(limit,self.read_headroom())
        parent=self.db.collection(collection)
        query=parent.order_by("__name__")
        if after_id is not None:query=query.start_after([parent.document(after_id)])
        rows=self._provider_call(lambda:[(document.id,document.to_dict()) for document in query.limit(remaining).stream(timeout=20,retry=None)],operation="read")
        self.reads+=max(1,len(rows));return rows

    def list_since(self,collection,field,since,limit=200):
        self.guard()
        if collection!="gameEvents" or field!="announcementPublishedAt" or not 1<=limit<=200:raise SafetyError("bounded_announcement_query_required")
        from google.cloud.firestore_v1.base_query import FieldFilter
        remaining=min(limit,self.read_headroom())
        query=self.db.collection(collection).where(filter=FieldFilter(field,">=",timestamp(since))).order_by(field,direction="DESCENDING").limit(remaining)
        rows=self._provider_call(lambda:[(document.id,document.to_dict()) for document in query.stream(timeout=20,retry=None)],operation="read")
        self.reads+=max(1,len(rows));return rows

    def list_stage_dates(self,date_keys,limit=50):
        self.guard()
        if not isinstance(date_keys,list)or not 1<=len(date_keys)<=8 or any(not isinstance(day,str)or not re.fullmatch(r"20\d{2}-\d{2}-\d{2}",day)for day in date_keys)or not 1<=limit<=50:raise SafetyError("bounded_public_stage_date_query_required")
        from google.cloud.firestore_v1.base_query import FieldFilter
        remaining=min(limit,self.read_headroom())
        query=self.db.collection("gameEvents").where(filter=FieldFilter("announcementStageDateKeys","array_contains_any",date_keys)).limit(remaining)
        rows=self._provider_call(lambda:[(document.id,document.to_dict())for document in query.stream(timeout=20,retry=None)],operation="read")
        self.reads+=max(1,len(rows));return rows

    def list_expired_youtube(self,collection,before,limit=50):
        self.guard()
        if collection not in ("gameEvents","collectionDocuments") or not 1<=limit<=50:raise SafetyError("bounded_public_youtube_expiry_query_required")
        from google.cloud.firestore_v1.base_query import FieldFilter
        remaining=min(limit,self.read_headroom())
        # The lower bound excludes null/non-timestamp legacy metadata from
        # monopolizing the bounded expiry queue. Both bounds use one field index.
        query=self.db.collection(collection).where(filter=FieldFilter("metadataExpiresAt",">=",datetime(1970,1,1,tzinfo=timezone.utc))).where(filter=FieldFilter("metadataExpiresAt","<=",timestamp(before))).order_by("metadataExpiresAt").limit(remaining)
        rows=self._provider_call(lambda:[(document.id,document.to_dict())for document in query.stream(timeout=20,retry=None)],operation="read")
        self.reads+=max(1,len(rows));return rows

    def finish_usage(self):
        """Worker-only observations; Firebase client traffic is NOT measured here."""
        if getattr(self,"provider_quota_exhausted",False):raise ProviderQuotaUnavailable(getattr(self,"provider_quota_operation","unknown"))
        ref = self.db.document("quotaBudgets/" + self.quota_day)
        @self.firestore.transactional
        def finish(transaction):
            old = self._provider_call(lambda:ref.get(transaction=transaction,timeout=20,retry=None).to_dict(),operation="read") or {}
            values = {"reads":old.get("reads",0)+self.reads+1,"writes":old.get("writes",0)+self.writes+2,
                      "scope":"externalWorkersOnly","updatedAt":datetime.now(timezone.utc)}
            transaction.set(ref,values)
            return values
        values = self._provider_call(lambda:finish(self.db.transaction(max_attempts=3)),operation="write")
        max_percent = max(values["reads"] / 50000,values["writes"] / 20000)*100
        level = "95" if max_percent>=95 else "85" if max_percent>=85 else "70" if max_percent>=70 else "normal"
        self._provider_call(lambda:self.db.document("usageSnapshots/"+self.quota_day).set({**values,"readUsagePercent":round(values["reads"]/50000*100,2),
             "writeUsagePercent":round(values["writes"]/20000*100,2),"warningThreshold":level,
             "fullProjectUsageVerified":False,"clientUsageIncluded":False,"storageUsageBytes":None,"transferUsageBytes":None},timeout=20,retry=None),operation="write")
        return values


class PublicNewsCreateOnlyStore(FirestoreStore):
    """Bounded recovery writer: no Firestore read or existing-document update."""
    recovery_only=True

    def __init__(self,env=None,*,allowed_source_ids):
        if not isinstance(allowed_source_ids,(list,set,tuple))or not allowed_source_ids:raise SafetyError("recovery_approved_sources_required")
        self.allowed_source_ids=frozenset(allowed_source_ids)
        super().__init__(env,initialize_quota=False)
        self.max_writes=25;self.max_reads=0;self.create_attempts=0

    def create_public_news(self,path,value):
        if getattr(self,"provider_quota_exhausted",False):raise ProviderQuotaUnavailable(getattr(self,"provider_quota_operation","write"))
        if self.create_attempts>=25 or self.writes>=25:raise SafetyError("recovery_write_budget_exhausted")
        if not isinstance(value,dict)or path!="gameEvents/"+str(value.get("id"))or not re.fullmatch(r"[a-f0-9]{64}",str(value.get("id",""))):raise SafetyError("recovery_parent_path_required")
        games=value.get("gameIds")
        if not isinstance(games,list)or len(games)!=1 or value.get("sourceId")not in self.allowed_source_ids:raise SafetyError("recovery_source_identity_required")
        identity={"game":games[0],"source":value["sourceId"],"region":value.get("serverRegion"),"server":value.get("serverLabel"),"externalKey":value.get("externalKey"),"pickupKind":None}
        expected=hashlib.sha256(json.dumps(identity,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
        if expected!=value["id"]:raise SafetyError("recovery_canonical_event_id_required")
        if (value.get("newsEligible")is not True or value.get("calendarEligible")is not False or value.get("sourceClockOnly")is not False
            or value.get("verificationStatus")!="pending"or value.get("scheduleStatus")!="DATE_UNCONFIRMED"or value.get("status")!="unconfirmed"
            or any(value.get(key)is not None for key in ("parentEventId","sourceSectionKey","startsAt","endsAt","reservationStartsAt","reservationEndsAt","ticketSalesAt","originalTimezone","candidateStartsAt","candidateEndsAt","candidateTimezone","calendarPeriod","calendarRole","calendarRoleEvidence","scheduleOrigin","calendarAudience","announcementKind","pairedTicketNoticeState","venue","dailyOpeningHours","regionBasis","gameServerRegion","scheduleKind"))
            or value.get("announcementStages",[])!=[] or value.get("announcementStageDateKeys",[])!=[]
            or value.get("derivedSectionCount",0)!=0 or value.get("derivedCharacterAnnouncements",0)!=0):raise SafetyError("recovery_pending_parent_news_only")
        if len(json.dumps(value,ensure_ascii=False,default=iso).encode())>900000:raise SafetyError("event_document_exceeds_safe_firestore_size")
        self.create_attempts+=1
        try:self._provider_call(lambda:self.db.document(path).create(value,timeout=20,retry=None),operation="write")
        except Exception as error:
            if any(base.__module__=="google.api_core.exceptions"and base.__name__ in("AlreadyExists","Conflict")for base in type(error).__mro__):return False
            raise
        self.writes+=1;self._record_public_change(path,value)
        return True

    def _no_recovery_read_or_update(self,*args,**kwargs):raise SafetyError("recovery_create_only_no_control_access")
    get=put=mutate=list=delete=list_equal=list_cursor=list_since=list_stage_dates=list_expired_youtube=delete_content_if_unchanged=finish_usage=_no_recovery_read_or_update

