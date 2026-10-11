"""Firestore Spark publisher feed worker; publication dates never become schedules.

Default command is a local plan with zero external requests and zero remote writes.
--live fetches reviewed official sources; --apply additionally requires Admin safety
checks and pre-enabled Firestore sources. Retained Supabase workers are inactive.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sys
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "collector"))
from engine import fetch_source, ValidationError
from official_adapter import steam_request, parse_steam_documents, aware_timestamp
from backfill import retry_after_seconds
from store import MemoryStore, FirestoreStore, SafetyError, timestamp

UTC = timezone.utc


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, default=lambda v:v.isoformat()).encode()).hexdigest()


def reviewed(source):
    return (source.get("approved_for_staging") is True and source.get("access_status") == "approved"
            and source.get("parser") == "steam_news" and source.get("terms_checked_at")
            and source.get("notice_hosts_verified_at") and source.get("terms_url", "").startswith("https://")
            and source.get("allowed_notice_hosts") and source.get("feed_allowlist") == ["steam_community_announcements"])


def source_row(source, game_id):
    """Only non-secret registered public provider metadata is exported."""
    return {"id":source["id"], "gameId":game_id, "url":source["url"], "parser":"steam_news",
            "steamAppid":source["steam_appid"], "enabled":False, "accessStatus":source["access_status"],
            "approvedForStaging":bool(source.get("approved_for_staging")), "termsUrl":source["terms_url"],
            "termsCheckedAt":timestamp(source["terms_checked_at"]),
            "noticeHostsVerifiedAt":timestamp(source["notice_hosts_verified_at"]),
            "allowedNoticeHosts":source["allowed_notice_hosts"], "feedAllowlist":source["feed_allowlist"],
            "intervalMinutes":max(15, source.get("collection_interval_minutes",15)),
            "serverRegion":source.get("server_region") or "UNSPECIFIED", "serverLabel":source.get("server_label"),
            "health":{"status":"DISABLED", "consecutiveFailures":0}}


def operating_source(row):
    """Admin-reviewed Firestore source configuration is authoritative in apply mode."""
    return {"id":row["id"],"game_id":row["gameId"],"url":row["url"],"parser":row["parser"],
            "steam_appid":row["steamAppid"],"enabled":row.get("enabled",False),
            "approved_for_staging":row.get("approvedForStaging",False),"access_status":row.get("accessStatus"),
            "terms_url":row.get("termsUrl"),"terms_checked_at":row.get("termsCheckedAt"),
            "notice_hosts_verified_at":row.get("noticeHostsVerifiedAt"),"allowed_notice_hosts":row.get("allowedNoticeHosts",[]),
            "feed_allowlist":row.get("feedAllowlist",[]),"collection_interval_minutes":max(15,row.get("intervalMinutes",15)),
            "server_region":row.get("serverRegion") or "UNSPECIFIED","server_label":row.get("serverLabel"),
            "method":"official_api","official_host":"api.steampowered.com","request_timeout_seconds":20,"max_response_bytes":2097152}


def seed_catalogue(store, games, registry):
    result={"games":0,"sources":0,"unchanged":0}
    for game in games:
        row={**game, "id":game["id"], "resetRules":[], "verifiedResetRuleIds":[]}
        # Candidate seed is not permission to overwrite independently verified reset rules.
        def upsert(old):
            if old:
                row.update({k:old[k] for k in ["resetRules","verifiedResetRuleIds"] if k in old})
            return (None,False) if old == row else (row,True)
        changed=store.mutate("games/"+game["id"],upsert)
        result["games" if changed else "unchanged"]+=1
    for game in registry["games"]:
        for source in game.get("sources",[]):
            if not reviewed(source):
                continue
            row=source_row(source,game["gameId"])
            def upsert_source(old):
                if old:
                    row.update({k:old[k] for k in ["enabled","health","lastCheckedAt","lastSuccessAt","lastErrorAt","lastReverifiedAt","nextRunAt"] if k in old})
                return (None,False) if old == row else (row,True)
            changed=store.mutate("gameSources/"+source["id"],upsert_source)
            result["sources" if changed else "unchanged"]+=1
    return result


def event_identity(document, source):
    return digest({"game":source["game_id"],"source":source["id"],"region":document["server_region"],
                   "server":document["server_label"],"externalKey":document["external_key"],"pickupKind":None})


def upsert_notice(store, source, document, observed):
    event_id=event_identity(document,source)
    proof_id=digest([source["id"],document["external_key"],document["raw_sha256"]])
    if len(json.dumps(document["raw_json"],ensure_ascii=False).encode())>700000:
        raise SafetyError("raw_document_exceeds_safe_firestore_size")
    store.mutate("collectionDocuments/"+proof_id,lambda old:(None,False) if old else
        ({"sourceId":source["id"],"externalKey":document["external_key"],"rawSha256":document["raw_sha256"],
          "rawJson":document["raw_json"],"publishedAt":timestamp(document["published_at"]) if document["published_at"] else None,
          "rawPublicationTimestamp":document["raw_publication_timestamp"],"observedAt":observed,
          "officialUrl":document["official_url"]},True))
    def update(old):
        if old and old.get("rawSha256") == document["raw_sha256"]:
            return None,"unchanged"
        row={**(old or {}),"id":event_id,"sourceId":source["id"],"externalKey":document["external_key"],
             "gameIds":[source["game_id"]],"serverRegion":document["server_region"],"serverLabel":document["server_label"],
             "title":document["original_title"],"description":document["original_description"],
             "originalTitle":document["original_title"],"originalDescription":document["original_description"],
             "originalLanguage":document["original_language"],"officialUrl":document["official_url"],
             "publishedAt":timestamp(document["published_at"]) if document["published_at"] else None,
             "sourceUpdatedAt":timestamp(document["source_updated_at"]) if document["source_updated_at"] else None,
             "rawSha256":document["raw_sha256"],"eventTypeCandidates":document["event_type_candidates"],
             "firstCheckedAt":old.get("firstCheckedAt",observed) if old else observed,
             "lastCheckedAt":observed,"lastChangedAt":observed,"verificationStatus":"pending",
             "revision":(old.get("revision",0) if old else 0)+1}
        # Raw notices do not erase manual dates/categories; changes revoke approval.
        for key,default in {"startsAt":None,"endsAt":None,"reservationStartsAt":None,"reservationEndsAt":None,
                            "ticketSalesAt":None,"originalTimezone":None,"category":"unclassified",
                            "scheduleStatus":"DATE_UNCONFIRMED","status":"unconfirmed"}.items():
            row.setdefault(key,default)
        return row,"updated" if old else "created"
    outcome=store.mutate("gameEvents/"+event_id,update)
    for pickup in document.get("pickups",[]):
        pickup_id=digest({"event":event_id,"game":source["game_id"],"region":pickup["server_region"],"server":pickup["server_label"],"kind":pickup["pickup_kind"]})
        def update_pickup(old):
            if outcome == "unchanged" and old:
                return None,False
            row={**(old or {}),"id":pickup_id,"eventId":event_id,"gameId":source["game_id"],
                 "serverRegion":pickup["server_region"],"serverLabel":pickup["server_label"],
                 "officialUrl":pickup["official_url"],"verificationStatus":"pending","lastCheckedAt":observed,
                 "firstCheckedAt":old.get("firstCheckedAt",observed) if old else observed}
            for key,default in {"pickupKind":None,"characters":[],"weapons":[],"startsAt":None,"endsAt":None}.items():
                row.setdefault(key,default)
            return row,True
        store.mutate("pickupSchedules/"+pickup_id,update_pickup)
    return outcome


def _claim_source_mode(store, source_id, now, mode, token, automatic_reverify=False):
    """Select a due source's effective mode inside its atomic lease transaction.

    A daily cron may be blocked by the preceding incremental source interval.
    The next already-due incremental request carries overdue revalidation forward
    without bypassing that interval, active lease, failure backoff or 24h gate.
    """
    def update(old):
        old=old or {}
        if old.get("leaseUntil") and timestamp(old["leaseUntil"])>now:
            return None,False
        if old.get("nextRunAt") and timestamp(old["nextRunAt"])>now:
            return None,False
        selected_mode=mode
        if automatic_reverify and mode=="incremental" and (not old.get("lastReverifiedAt") or timestamp(old["lastReverifiedAt"])<=now-timedelta(hours=24)):
            selected_mode="reverify"
        if selected_mode == "reverify" and old.get("lastReverifiedAt") and timestamp(old["lastReverifiedAt"])>now-timedelta(hours=24):
            return None,False
        return {**old,"token":token,"leaseUntil":now+timedelta(minutes=12),"claimedAt":now,"mode":selected_mode},selected_mode
    return store.mutate("collectorLocks/"+source_id,update)


def claim_source(store, source_id, now, mode, token):
    return bool(_claim_source_mode(store,source_id,now,mode,token))


def finish_source(store, source, token, now, success, error, http_status, mode, retry_after=0):
    lock_path="collectorLocks/"+source["id"]
    def update(old):
        if not old or old.get("token")!=token:
            return None,False
        count=0 if success else old.get("consecutiveFailures",0)+1
        delay=max(max(15,source.get("collection_interval_minutes",15))*60,
                  min(86400,900*2**min(count,6)) if not success else 0,retry_after)
        row={**old,"leaseUntil":None,"token":None,"consecutiveFailures":count,"nextRunAt":now+timedelta(seconds=delay)}
        if success and mode=="reverify":row["lastReverifiedAt"]=now
        return row,True
    if not store.mutate(lock_path,update):
        raise SafetyError("source_lease_lost")
    def health(old):
        if not old:return None,False
        row={**old,"lastCheckedAt":now,"health":{"status":"success" if success else "FETCH_FAILED",
             "httpStatus":http_status,"consecutiveFailures":store.get(lock_path)["consecutiveFailures"],"reasonCode":error}}
        if success:row["lastSuccessAt"]=now
        else:row["lastErrorAt"]=now
        if success and mode=="reverify":row["lastReverifiedAt"]=now
        return row,True
    store.mutate("gameSources/"+source["id"],health)


def collect_one(store, source, start, end, mode="backfill", fetch=fetch_source, sleep=time.sleep, now=None, deadline=None, max_pages=10):
    observed=now or datetime.now(UTC)
    if not reviewed(source):raise SafetyError("source_review_required")
    if end<=start or end-start>timedelta(hours=72):raise SafetyError("window_must_be_at_most_72_hours")
    if store.remote:
        config=store.get("gameSources/"+source["id"])
        if not config or config.get("enabled") is not True:
            return {"status":"DISABLED","sourceId":source["id"],"writes":0}
    token=uuid.uuid4().hex
    requested_mode=mode
    selected_mode=_claim_source_mode(store,source["id"],observed,mode,token,automatic_reverify=True)
    if not selected_mode:
        return {"status":"DEFERRED","sourceId":source["id"],"writes":0}
    mode=selected_mode
    run_id=digest([source["id"],token]);started_writes=store.writes
    metrics={"pagesFetched":0,"itemsSeen":0,"created":0,"updated":0,"unchanged":0,"verifiedSchedules":0,
             "knownEventsForReverify":0,"knownEventsQueryLimitReached":False,"olderUnseenSkipped":0}
    success,error,http_status,retry_after=False,None,None,0
    cursor=int(end.timestamp())+1
    try:
        known_event_ids=set()
        if mode=="reverify":
            known_event_ids={event_id for event_id,_ in store.list_equal("gameEvents","sourceId",source["id"],50)}
            metrics.update(knownEventsForReverify=len(known_event_ids),knownEventsQueryLimitReached=len(known_event_ids)==50)
        for _ in range(max_pages):
            if deadline is not None and time.monotonic()>=deadline:raise SafetyError("run_time_limit")
            sleep(1)
            request=steam_request({**source,"enabled":True},cursor,50)
            http_status,raw,headers=fetch(request);metrics["pagesFetched"]+=1
            if http_status!=200:
                retry_after=retry_after_seconds(headers.get("retry-after"))
                raise SafetyError("source_http_"+str(http_status))
            documents,parsed=parse_steam_documents(raw,source,observed.isoformat())
            metrics["itemsSeen"]+=parsed["items_seen"]
            dates=[]
            for document in documents:
                if not document["published_at"]:raise SafetyError("publication_timestamp_unconfirmed")
                date=aware_timestamp(document["published_at"]);dates.append(int(date.timestamp()))
                if start<=date<end or mode=="reverify" and event_identity(document,source) in known_event_ids:
                    metrics[upsert_notice(store,source,document,observed)]+=1
                elif mode=="reverify":
                    metrics["olderUnseenSkipped"]+=1
            oldest=min(dates) if dates else None
            if parsed["items_seen"]>=50 and oldest is not None and dates.count(oldest)>1 and oldest>=int(start.timestamp()):
                raise SafetyError("pagination_boundary_ambiguous")
            if mode=="reverify" or parsed["items_seen"]<50 or oldest is not None and oldest<int(start.timestamp()):
                success=True;break
            if oldest is None or oldest>=cursor:raise SafetyError("pagination_cursor_not_advancing")
            cursor=oldest
        if not success:raise SafetyError("page_limit_reached")
    except Exception as failure:
        error=str(failure)[:120] if isinstance(failure,(SafetyError,ValidationError)) else type(failure).__name__
        store.put("collectionErrors/"+run_id,{"sourceId":source["id"],"gameId":source["game_id"],"reasonCode":error,
                  "httpStatus":http_status,"createdAt":observed,"retryAfterSeconds":retry_after})
    finished=datetime.now(UTC)
    finish_source(store,source,token,finished,success,error,http_status,mode,retry_after)
    result={"sourceId":source["id"],"gameId":source["game_id"],"status":"success" if success else "FETCH_FAILED",
            "windowStart":start,"windowEnd":end,"mode":mode,"requestedMode":requested_mode,"metrics":metrics,"reasonCode":error,
            "httpStatus":http_status,"createdAt":observed,"finishedAt":finished,
            "lastCommittedCursor":cursor,"writes":store.writes-started_writes}
    store.put("collectionRuns/"+run_id,result)
    return result


def run(registry,store,start,end,mode="backfill",fetch=fetch_source,sleep=time.sleep,timeout_seconds=480,max_sources=50):
    deadline=time.monotonic()+timeout_seconds
    results=[];limited_hosts=set()
    from urllib.parse import urlsplit
    operating_registry=registry
    if store.remote:
        operating_registry={"games":[{"gameId":row["gameId"],"sources":[operating_source({**row,"id":source_id})]}
                           for source_id,row in store.list("gameSources",200) if row.get("enabled") is True]}
    for game in operating_registry["games"]:
        for raw_source in game.get("sources",[]):
            if len(results)>=max_sources or time.monotonic()>=deadline:break
            source={**raw_source,"game_id":game["gameId"]}
            host=urlsplit(source["url"]).hostname
            if host in limited_hosts:
                results.append({"sourceId":source["id"],"status":"DEFERRED","reasonCode":"HOST_RATE_LIMITED"});continue
            result=collect_one(store,source,start,end,mode,fetch,sleep,deadline=deadline)
            results.append(result)
            if result.get("httpStatus")==429:limited_hosts.add(host)
    return {"recordedAt":datetime.now(UTC),"backend":"FIREBASE" if store.remote else "LOCAL_STAGING",
            "remoteFirestoreWrites":store.writes if store.remote else 0,"fcmSends":0,"windowHours":(end-start).total_seconds()/3600,
            "results":results,"verifiedEventSchedules":0,"fullSourceCoverageVerified":False}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry",default="data/official-sources.json")
    parser.add_argument("--mode",choices=["backfill","incremental","reverify"],default="backfill")
    parser.add_argument("--hours",type=int,default=72)
    parser.add_argument("--live",action="store_true")
    parser.add_argument("--apply",action="store_true")
    parser.add_argument("--seed",action="store_true")
    parser.add_argument("--activate-reviewed-sources",action="store_true")
    parser.add_argument("--report",default="reports/firebase-collector.json")
    args=parser.parse_args()
    if not 1<=args.hours<=72:parser.error("hours must be 1..72")
    registry=json.loads(Path(args.registry).read_text(encoding="utf-8-sig"))
    store=FirestoreStore() if args.apply else MemoryStore()
    if args.activate_reviewed_sources:
        if not store.remote:parser.error("activation requires --apply and explicit Spark authorization")
        activated=0
        for game in registry["games"]:
            for source in game.get("sources",[]):
                if not reviewed(source):continue
                row=store.get("gameSources/"+source["id"])
                if not row:raise SafetyError("seed_reviewed_source_before_activation")
                # Reset rule/service region verification is independent from feed permission.
                expected=source_row(source,game["gameId"])
                if any(row.get(k)!=expected.get(k) for k in ["gameId","url","steamAppid","termsUrl","allowedNoticeHosts","feedAllowlist"]):raise SafetyError("source_configuration_changed_requires_manual_review")
                if row.get("enabled") is not True:
                    store.put("gameSources/"+source["id"],{**row,"enabled":True,"health":{"status":"READY","consecutiveFailures":0}});activated+=1
        result={"status":"EXPLICIT_REVIEWED_SOURCE_ACTIVATION","activatedSources":activated,"remoteFirestoreWrites":store.writes}
    elif args.seed:
        result=seed_catalogue(store,json.loads(Path("data/game-candidates.json").read_text(encoding="utf-8-sig")),registry)
        result={"status":"SEEDED" if store.remote else "DRY_RUN","remoteFirestoreWrites":store.writes if store.remote else 0,**result}
    elif not args.live:
        result={"status":"PLAN_ONLY","reviewedSources":sum(reviewed(s) for g in registry["games"] for s in g.get("sources",[])),
                "remoteFirestoreWrites":0,"httpRequests":0,"fcmSends":0,"firebaseConnected":False}
    else:
        end=datetime.now(UTC);start=end-timedelta(hours=args.hours)
        result=run(registry,store,start,end,args.mode)
    if store.remote:
        store.finish_usage()
        result.update(usageAccountingWrites=2,totalFirestoreWrites=store.writes+2)
    output=Path(args.report);output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(result,ensure_ascii=False,indent=2,default=lambda v:v.isoformat())+"\n",encoding="utf-8")
    if args.live and not store.remote:
        evidence=output.with_name(output.stem+"-local-evidence.json")
        evidence.write_text(json.dumps(store.records,ensure_ascii=False,indent=2,default=lambda v:v.isoformat())+"\n",encoding="utf-8")
    print(json.dumps({"status":result.get("status",result.get("backend")),"report":str(output),"remoteFirestoreWrites":result["remoteFirestoreWrites"]}))


if __name__=="__main__":
    try:main()
    except SafetyError as error:
        print(json.dumps({"status":"BLOCKED","reasonCode":str(error),"firebaseConnectionVerified":False}));sys.exit(1)
