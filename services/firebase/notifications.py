"""Bounded Spark FCM dispatcher. No task reminders and no invented schedules.

Provider acceptance is not device display/receipt. Ambiguous interrupted sends are
quarantined rather than retried automatically, avoiding known transport duplicates.
"""
from __future__ import annotations
from datetime import datetime, timedelta, timezone
import argparse
import json
import re
from pathlib import Path
import uuid
from zoneinfo import ZoneInfo
from collector import digest
from store import FirestoreStore, MemoryStore, SafetyError, timestamp

UTC=timezone.utc
OFFSETS={"7d":10080,"1d":1440,"1h":60,"start":0}
TEXT={"ko":("푸시 알림 테스트","이 알림이 보이면 앱에서 수신 확인을 눌러 주세요."),
      "en":("Push notification test","If this notification appears, confirm receipt in the app."),
      "ja":("プッシュ通知テスト","この通知が表示されたら、アプリで受信を確認してください。")}
ATTENDANCE_TEXT={"ko":("ORBIT 출석 확인","{game}: 이번 출석 기간의 게임 실행 기록이 아직 확인되지 않았습니다."),
                 "en":("ORBIT attendance reminder","{game}: a game launch has not been confirmed for this attendance period."),
                 "ja":("ORBIT 出席確認","{game}: 今回の出席期間のゲーム起動記録がまだ確認できていません。")}


def reset_cycle(rule,now):
    """Use only a reviewed server-specific reset. DST ambiguity fails closed."""
    if not isinstance(rule,dict) or rule.get("verified") is not True or not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,63}",rule.get("id","")):
        return None
    if not isinstance(rule.get("evidenceUrl"),str) or not rule["evidenceUrl"].startswith("https://"):
        return None
    if not re.fullmatch(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]",rule.get("dailyTime","")):return None
    try:
        zone=ZoneInfo(rule["timezone"]);local=now.astimezone(zone)
        hour,minute=map(int,rule["dailyTime"].split(":"))
        day=local.date()-timedelta(days=int((local.hour,local.minute)<(hour,minute)))
        starts=[]
        for date in [day,day+timedelta(days=1)]:
            wall=datetime(date.year,date.month,date.day,hour,minute)
            first=wall.replace(tzinfo=zone,fold=0);second=wall.replace(tzinfo=zone,fold=1)
            if first.utcoffset()!=second.utcoffset() or first.astimezone(UTC).astimezone(zone).replace(tzinfo=None)!=wall:return None
            starts.append(first.astimezone(UTC))
        return {"ruleId":rule["id"],"localDate":day.isoformat(),"timezone":rule["timezone"],
                "cycleKey":rule["id"]+":"+day.isoformat(),"cycleStart":starts[0],"cycleEnd":starts[1]}
    except (KeyError,TypeError,ValueError):return None


def completed_cycle(row,game_id,cycle):
    """Old or other-server summaries cannot suppress this server's reminders.

    Raw launch records may have expired after 30 days. The retained summary is
    not rewritten or reclassified as an official game-server login.
    """
    try:
        return bool(row and row.get("gameId")==game_id and row.get("completionStatus")=="completed"
            and row.get("source")=="device_observed" and isinstance(row.get("executionCount"),int) and row["executionCount"]>0
            and row.get("verifiedResetRuleId")==cycle["ruleId"] and row.get("cycleKey")==cycle["cycleKey"]
            and row.get("timezone")==cycle["timezone"] and timestamp(row.get("cycleStart"))==cycle["cycleStart"]
            and timestamp(row.get("cycleEnd"))==cycle["cycleEnd"]
            and cycle["cycleStart"]<=timestamp(row.get("firstExecutionAt"))<=timestamp(row.get("lastExecutionAt"))<cycle["cycleEnd"])
    except (ValueError,TypeError,KeyError):return False


def attendance_context(store,uid,game_id,now):
    profile=store.get("users/"+uid) or {}
    if not profile or profile.get("deletionRequestedAt") or profile.get("notificationsEnabled") is False:return None
    if quiet({"quietStart":profile.get("dndStart"),"quietEnd":profile.get("dndEnd"),"timezone":profile.get("timezone","UTC")},now):return None
    global_settings=store.get(f"users/{uid}/notificationSettings/global") or {}
    if global_settings.get("enabled") is False or global_settings.get("attendanceEnabled") is not True or quiet(global_settings,now):return None
    selected=store.get(f"users/{uid}/userGames/{game_id}") or {}
    if not selected.get("enabled") or not selected.get("resetRuleId"):return None
    game_settings=store.get(f"users/{uid}/notificationSettings/{game_id}")
    settings={**global_settings,**(game_settings or {})}
    if game_settings is not None and "attendanceDeliveryMode" not in game_settings:settings["attendanceDeliveryMode"]="local"
    # Explicit channel selection prevents native local and remote attendance
    # schedulers from producing two alerts. Existing preferences default local.
    if settings.get("enabled") is False or settings.get("attendanceEnabled") is not True or settings.get("attendanceDeliveryMode","local")!="remote" or quiet(settings,now):return None
    game=store.get("games/"+game_id) or {};rule_id=selected["resetRuleId"]
    if rule_id not in game.get("verifiedResetRuleIds",[]):return None
    rule=game.get("resetRuleById",{}).get(rule_id)
    if not rule or rule.get("id")!=rule_id:return None
    region=rule.get("region",rule.get("serverRegion"))
    if region and region!=selected.get("serverRegion"):return None
    cycle=reset_cycle(rule,now)
    if not cycle:return None
    row_id=game_id+"_"+rule_id+"_"+cycle["localDate"]
    aggregate=store.get(f"users/{uid}/attendanceDaily/{row_id}")
    cleared=False
    if profile.get("executionRecordsDeletedAt"):
        try:cleared=not aggregate or timestamp(aggregate.get("lastExecutionAt"))<=timestamp(profile["executionRecordsDeletedAt"])
        except (ValueError,TypeError):cleared=True
    if not cleared and completed_cycle(aggregate,game_id,cycle):return None
    return {**cycle,"gameId":game_id,"game":game,"settings":settings,"aggregateId":row_id}


def plan_attendance_user(store,uid,now):
    global_settings=store.get(f"users/{uid}/notificationSettings/global") or {}
    if global_settings.get("attendanceEnabled") is not True or global_settings.get("enabled") is False:return 0
    planned=0
    for game_id,selected in store.list(f"users/{uid}/userGames",20):
        if not selected.get("enabled"):continue
        context=attendance_context(store,uid,game_id,now)
        if not context:continue
        settings=context["settings"]
        try:zone=ZoneInfo(settings.get("timezone") or "UTC")
        except (ValueError,KeyError):continue
        local=now.astimezone(zone)
        slots=list(dict.fromkeys(settings.get("reminderTimes",["18:00","20:00","22:00"])))[:3]
        for slot in slots:
            if not isinstance(slot,str) or not re.fullmatch(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]",slot):continue
            hour,minute=map(int,slot.split(":"));wall=local.replace(hour=hour,minute=minute,second=0,microsecond=0)
            if wall.replace(fold=0).utcoffset()!=wall.replace(fold=1).utcoffset():continue
            due=wall.astimezone(UTC)
            if not now-timedelta(minutes=30)<=due<=now+timedelta(minutes=15) or not context["cycleStart"]<=due<context["cycleEnd"]:continue
            key=digest([uid,"attendance",game_id,context["cycleKey"],slot])
            if store.get(f"users/{uid}/notificationJobs/{key}"):continue
            def reserve(old):
                old=old or {"uid":uid,"gameId":game_id,"cycleKey":context["cycleKey"],"keys":[],"createdAt":now}
                keys=old.get("keys",[])
                if key in keys:return None,True
                if len(keys)>=3:return None,False
                return {**old,"keys":keys+[key],"updatedAt":now},True
            # Reserve before creating a job; an interrupted reservation consumes
            # a slot safely instead of permitting >3 remote reminders per cycle.
            if not store.mutate(f"users/{uid}/attendanceReminderLimits/"+digest([game_id,context["cycleKey"]]),reserve):continue
            row={"uid":uid,"kind":"attendance","gameId":game_id,"cycleKey":context["cycleKey"],
                 "resetRuleId":context["ruleId"],"reminderSlot":slot,"status":"requested","dueAt":due,
                 "createdAt":now,"updatedAt":now,"dedupeKey":key,"attempts":0}
            created=store.mutate(f"users/{uid}/notificationJobs/{key}",lambda old:(None,False) if old else (row,True))
            planned+=int(created)
    return planned


def quiet(settings,now):
    start,end=settings.get("quietStart"),settings.get("quietEnd")
    if not start or not end:return False
    try:local=now.astimezone(ZoneInfo(settings.get("timezone") or "UTC")).strftime("%H:%M")
    except (ValueError,KeyError):return True
    if start==end:return True
    return start<=local<end if start<end else local>=start or local<end


def eligible(store,uid,event,kind,now):
    profile=store.get("users/"+uid)
    if not profile or profile.get("deletionRequestedAt"):return False
    if kind in ["TASK_REMINDER","task_reminder","reminder"]:return False
    if kind=="push_test":return True  # explicit owner's self test, never arbitrary payload
    if kind=="attendance":
        if not event or len(event.get("gameIds",[]))!=1:return False
        context=attendance_context(store,uid,event["gameIds"][0],now)
        return bool(context and event.get("cycleKey")==context["cycleKey"])
    if profile.get("notificationsEnabled") is False:return False
    if quiet({"quietStart":profile.get("dndStart"),"quietEnd":profile.get("dndEnd"),"timezone":profile.get("timezone","UTC")},now):return False
    if kind not in {anchor+":"+stage for anchor in ["event","reservation"] for stage in OFFSETS}:return False
    if not event or event.get("verificationStatus")!="verified" or event.get("status") in ["cancelled","canceled","postponed","ended"]:return False
    region=event.get("serverRegion")
    if not region or region=="UNSPECIFIED":return False
    global_settings=store.get(f"users/{uid}/notificationSettings/global") or {"enabled":True,"regions":profile.get("regions",[]),"timezone":profile.get("timezone","UTC")}
    if global_settings.get("enabled") is False or quiet(global_settings,now):return False
    anchor_kind,stage=kind.split(":",1)
    if "types" in global_settings and anchor_kind not in global_settings["types"]:return False
    for game_id in event.get("gameIds",[]):
        selected=store.get(f"users/{uid}/userGames/{game_id}")
        if not selected or not selected.get("enabled",True):continue
        settings=store.get(f"users/{uid}/notificationSettings/{game_id}") or global_settings
        if not settings.get("enabled",True) or quiet(settings,now):continue
        if "types" in settings and anchor_kind not in settings["types"]:continue
        if stage not in settings.get("stages",list(OFFSETS)) or OFFSETS.get(stage) not in settings.get("offsets",list(OFFSETS.values())):continue
        categories=settings.get("eventCategories",["all"])
        if "all" not in categories and event.get("category") not in categories:continue
        regions=settings.get("regions",profile.get("regions",[]))
        if region not in regions:continue  # GLOBAL must be explicitly chosen; unknown is never GLOBAL
        if kind.startswith("reservation") and settings.get("reservationEnabled") is False:continue
        if kind.startswith("event") and settings.get("eventEnabled") is False:continue
        return True
    return False


def plan_user(store,uid,events,now):
    planned=0
    for event in events:
        for anchor_kind,field in [("event","startsAt"),("reservation","reservationStartsAt")]:
            if not event.get(field):continue
            anchor=timestamp(event[field])
            for stage,minutes in OFFSETS.items():
                due=anchor-timedelta(minutes=minutes)
                # Frequent cron with 30-minute catchup; outage older than this is explicit missed.
                if not now-timedelta(minutes=30)<=due<=now+timedelta(minutes=15):continue
                kind=anchor_kind+":"+stage
                if not eligible(store,uid,event,kind,now):continue
                selected_settings=[]
                for game_id in event.get("gameIds",[]):
                    if store.get(f"users/{uid}/userGames/{game_id}"):
                        settings=store.get(f"users/{uid}/notificationSettings/{game_id}") or store.get(f"users/{uid}/notificationSettings/global") or {}
                        if stage in settings.get("stages",list(OFFSETS)) and minutes in settings.get("offsets",list(OFFSETS.values())):
                            selected_settings.append(game_id)
                if not selected_settings:continue
                key=digest([uid,event["id"],event.get("revision",1),kind,anchor.isoformat()])
                row={"uid":uid,"kind":kind,"eventId":event["id"],"eventRevision":event.get("revision",1),
                     "status":"requested","dueAt":due,"createdAt":now,"updatedAt":now,"dedupeKey":key,"attempts":0}
                created=store.mutate(f"users/{uid}/notificationJobs/{key}",lambda old:(None,False) if old else (row,True))
                planned+=int(created)
    return planned


def fixed_payload(store,uid,job,event=None,now=None):
    profile=store.get("users/"+uid) or {}
    lang=job.get("language",profile.get("language","en"))
    if job["kind"]=="push_test":
        title,body=TEXT.get(lang,TEXT["en"])
        return {"title":title,"body":body,"url":"/settings","jobId":job["id"],"tag":"orbit-test-"+job["id"],
                "kind":"push_test","gameId":"","gameIds":[],"category":"","eventId":"","recipientUid":uid}
    now=now or datetime.now(UTC)
    if job["kind"]=="attendance":
        context=attendance_context(store,uid,job["gameId"],now)
        if not context or context["cycleKey"]!=job["cycleKey"]:raise SafetyError("attendance_became_ineligible")
        title,body=ATTENDANCE_TEXT.get(lang,ATTENDANCE_TEXT["en"])
        name=context["game"].get("names",{}).get(lang) or context["game"].get("names",{}).get("en") or job["gameId"]
        return {"title":title,"body":body.format(game=name)[:500],"url":"/dashboard","jobId":job["id"],
                "tag":"orbit-attendance-"+job["dedupeKey"],"kind":"attendance","gameId":job["gameId"],
                "gameIds":[job["gameId"]],"category":"attendance","eventId":"","recipientUid":uid,
                "cycleKey":context["cycleKey"],"resetRuleId":context["ruleId"]}
    eligible_games=[game_id for game_id in event.get("gameIds",[])
                    if eligible(store,uid,{**event,"gameIds":[game_id]},job["kind"],now)]
    return {"title":event.get("originalTitle",event.get("title","Official event"))[:200],
            "body":event.get("originalDescription",event.get("description",""))[:500],
            "url":"/events/"+event["id"],"jobId":job["id"],"tag":"orbit-event-"+job.get("dedupeKey",job["id"]),
            "kind":job["kind"],"gameId":eligible_games[0] if eligible_games else "",
            "gameIds":eligible_games,"category":event.get("category",""),"eventId":event["id"],"recipientUid":uid}


def dispatch(store,uid,job_id,sender,now=None):
    now=now or datetime.now(UTC);path=f"users/{uid}/notificationJobs/{job_id}"
    token=uuid.uuid4().hex
    def claim(old):
        if not old or old.get("status")!="requested" or old.get("dueAt") and timestamp(old["dueAt"])>now:return None,None
        if old.get("kind") in ["TASK_REMINDER","task_reminder","reminder"]:
            return {**old,"status":"cancelled","reasonCode":"TASK_REMINDERS_DISABLED","updatedAt":now},None
        return {**old,"id":job_id,"status":"sending","leaseToken":token,"attempts":old.get("attempts",0)+1,"updatedAt":now},old
    job=store.mutate(path,claim)
    if not job:return "skipped"
    job={**job,"id":job_id}
    event=store.get("gameEvents/"+job["eventId"]) if job.get("eventId") else None
    if job["kind"]=="attendance":event={"gameIds":[job.get("gameId")],"cycleKey":job.get("cycleKey")}
    if job.get("eventId") and event and job.get("eventRevision")!=event.get("revision",1) or not eligible(store,uid,event,job["kind"],now):
        store.put(path,{**store.get(path),"status":"cancelled","reasonCode":"INELIGIBLE_AT_SEND","updatedAt":now});return "cancelled"
    try:payload=fixed_payload(store,uid,job,event,now)
    except SafetyError:
        store.put(path,{**store.get(path),"status":"cancelled","reasonCode":"INELIGIBLE_BEFORE_PAYLOAD","updatedAt":now});return "cancelled"
    devices=[]
    for device_id,sub in store.list(f"users/{uid}/notificationSubscriptions",20):
        if sub.get("uid")!=uid or not sub.get("enabled") or sub.get("transport")!="fcm":continue
        if job.get("deviceId") and job["deviceId"]!=device_id:continue
        if sub.get("expiresAt") and timestamp(sub["expiresAt"])<=now:continue
        if sub.get("platform") not in ["android","web"]:continue  # Native Windows FCM SDK unsupported
        devices.append((device_id,sub))
    accepted,failed,ambiguous=0,0,0
    for device_id,sub in devices:
        try:
            provider_id=sender(sub["token"],payload)
            accepted+=1
            store.put("notificationLogs/"+digest([uid,job_id,device_id]),{"uid":uid,"jobId":job_id,"deviceId":device_id,
                      "status":"provider_accepted","providerMessageId":provider_id,"createdAt":now})
        except Exception as error:
            failed+=1;ambiguous+=1
            # Never log token or provider exception bodies, which may contain credentials.
            store.put("notificationLogs/"+digest([uid,job_id,device_id]),{"uid":uid,"jobId":job_id,"deviceId":device_id,
                      "status":"delivery_unconfirmed","reasonCode":type(error).__name__,"createdAt":now})
    status="partially_accepted" if accepted and failed else "provider_accepted" if accepted else "delivery_unconfirmed" if ambiguous else "in_app_only"
    final={**store.get(path),"status":status,"leaseToken":None,"acceptedDeviceCount":accepted,"failedDeviceCount":failed,"updatedAt":now,"humanConfirmedAt":None}
    store.put(path,final)
    store.put(f"users/{uid}/notifications/{job_id}",{"uid":uid,"title":payload["title"],"body":payload["body"],"url":payload["url"],"status":status,"readAt":None,"createdAt":now})
    return status


def run(store,sender,now=None,max_users=50):
    now=now or datetime.now(UTC);events=[value for _,value in store.list("gameEvents",200)]
    stats={"users":0,"planned":0,"attendancePlanned":0,"providerAcceptedJobs":0,"inAppOnly":0,"cancelled":0}
    for uid,profile in store.list("users",max_users):
        if profile.get("deletionRequestedAt"):continue
        stats["users"]+=1;stats["planned"]+=plan_user(store,uid,events,now)
        stats["attendancePlanned"]+=plan_attendance_user(store,uid,now)
        for job_id,job in store.list(f"users/{uid}/notificationJobs",100):
            if job.get("status")=="sending" and timestamp(job["updatedAt"])<now-timedelta(minutes=12):
                store.put(f"users/{uid}/notificationJobs/{job_id}",{**job,"status":"delivery_unconfirmed","reasonCode":"INTERRUPTED_SEND_NO_AUTORETRY","updatedAt":now});continue
            if job.get("status")!="requested":continue
            status=dispatch(store,uid,job_id,sender,now)
            if status in ["provider_accepted","partially_accepted"]:stats["providerAcceptedJobs"]+=1
            if status=="in_app_only":stats["inAppOnly"]+=1
            if status=="cancelled":stats["cancelled"]+=1
    return stats


def fcm_message(messaging,token,payload):
    # Data-only payload: the web worker and Android receiver display it themselves.
    # A notification payload would let the SDK bypass our same-origin click handling.
    recipient_uid=payload.get("recipientUid")
    if not isinstance(recipient_uid,str) or not 1<=len(recipient_uid)<=128:raise SafetyError("fcm_recipient_uid_required")
    data={"recipientUid":recipient_uid,"title":payload["title"],"body":payload["body"],"url":payload["url"],
          "jobId":payload["jobId"],"dedupeKey":payload["tag"],"kind":payload.get("kind",""),
          "gameId":payload.get("gameId",""),"gameIds":json.dumps(payload.get("gameIds",[]),ensure_ascii=False),
          "category":payload.get("category",""),"eventId":payload.get("eventId","")}
    if payload.get("kind")=="attendance":data.update({"cycleKey":payload["cycleKey"],"resetRuleId":payload["resetRuleId"]})
    # FCM data payloads have a 4096-byte ceiling. Preserve routing/filter fields,
    # then shorten only text; never reinterpret an omitted game as a global event.
    if len(json.dumps(data,ensure_ascii=False).encode())>3500:
        data["gameIds"]=json.dumps([data["gameId"]] if data["gameId"] else [])
        data["body"]=data["body"].encode()[:1000].decode("utf-8",errors="ignore")
        data["title"]=data["title"].encode()[:600].decode("utf-8",errors="ignore")
    if len(json.dumps(data,ensure_ascii=False).encode())>3500:raise SafetyError("fcm_payload_too_large")
    return messaging.Message(token=token,data=data,
        android=messaging.AndroidConfig(collapse_key=payload["tag"],priority="high"))


def main():
    import os
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("--apply",action="store_true");args=parser.parse_args()
    if not args.apply or os.environ.get("ORBIT_FIREBASE_NOTIFICATIONS_ENABLED")!="true":
        print(json.dumps({"status":"DISABLED","fcmSends":0,"remoteWrites":0}));return
    store=FirestoreStore()
    from firebase_admin import messaging
    def send(token,payload):
        return messaging.send(fcm_message(messaging,token,payload),app=store.app)
    result=run(store,send);store.finish_usage();print(json.dumps(result))

if __name__=="__main__":main()
