"""Bounded Spark FCM dispatcher. No task reminders and no invented schedules.

Provider acceptance is not device display/receipt. Ambiguous interrupted sends are
quarantined rather than retried automatically, avoiding known transport duplicates.
"""
from __future__ import annotations
from datetime import datetime, timedelta, timezone
import argparse
import html
import json
import re
from pathlib import Path
import uuid
import unicodedata
from zoneinfo import ZoneInfo
from collector import digest, operating_source, reviewed, reviewed_calendar_role, safe_public_url, content_publication_status, apply_notice_content_policy
from urllib.parse import urlsplit
from store import FirestoreStore, MemoryStore, SafetyError, timestamp

UTC=timezone.utc
OFFSETS={"7d":10080,"1d":1440,"1h":60,"start":0}
ANNOUNCEMENT_KIND="announcement:published"
ANNOUNCEMENT_LOOKBACK=timedelta(hours=72)


def content_policy_allows(event, now, *, calendar=False):
    """A classified or migrated public post cannot bypass its display policy.

    Legacy independently reviewed notifications retain their existing source
    checks. New metadata is recomputed rather than trusting a client-like flag.
    Attendance and explicit test notifications do not use this public policy.
    """
    if not event:return False
    if not any(key in event for key in ("contentKind","contentClassification","newsEligible","calendarEligible")):
        return True
    current=apply_notice_content_policy(event,now)
    if (event.get("newsEligible") is not True or current.get("newsEligible") is not True
        or event.get("contentKind")!=current.get("contentKind")
        or event.get("contentClassification")!=current.get("contentClassification")):
        return False
    return not calendar or event.get("calendarEligible") is True and current.get("calendarEligible") is True


def announcement_identity(store,event,now,source_cache=None):
    """Current official source and source-bound main role, never date approval."""
    if not event or not content_policy_allows(event,now) or content_publication_status(event.get("publishedAt"),now)!="INCLUDED" or event.get("calendarRole") not in ("main-character","main-event") or event.get("status") in ("cancelled","canceled","postponed","ended"):return None
    source_id=event.get("sourceId");cache=source_cache if source_cache is not None else {}
    if not isinstance(source_id,str) or not source_id or "/" in source_id:return None
    if source_id not in cache:cache[source_id]=store.get("gameSources/"+source_id)
    public=cache[source_id]
    if not public or public.get("id",source_id)!=source_id or public.get("enabled") is not True:return None
    source=operating_source({**public,"id":source_id})
    if not reviewed(source) or source.get("game_id") not in event.get("gameIds",[]):return None
    try:
        if not safe_public_url(source.get("terms_url")) or any(timestamp(source.get(field))>now for field in ("terms_checked_at","notice_hosts_verified_at")):return None
    except (ValueError,TypeError):return None
    url=safe_public_url(event.get("officialUrl"))
    if not url or not url.startswith("https://") or urlsplit(url).hostname not in source.get("allowed_notice_hosts",[]):return None
    sha=event.get("rawSha256");selected=event.get("displayTitle") or event.get("sourceSectionTitle") or event.get("originalTitle") or event.get("title")
    if not isinstance(sha,str) or len(sha)!=64 or any(char not in "0123456789abcdef" for char in sha) or not isinstance(selected,str) or not selected.strip():return None
    external=event.get("externalKey")
    if not isinstance(external,str) or not external:return None
    raw_key=("raw",digest([source_id,external,sha]))
    if raw_key not in cache:cache[raw_key]=store.get("collectionDocuments/"+raw_key[1])
    raw=cache[raw_key] or {}
    if any(raw.get(field)!=value for field,value in [("sourceId",source_id),("externalKey",external),("rawSha256",sha),("officialUrl",event.get("officialUrl"))]):return None
    proof=event.get("calendarRoleEvidence") or {};verified=reviewed_calendar_role(dict(event),source)
    if (proof.get("status")!="verified" or proof.get("sourceSha256")!=sha or proof.get("sourceTitle")!=selected
        or proof.get("role")!=event["calendarRole"] or verified.get("calendarRole")!=event["calendarRole"]
        or verified.get("calendarRoleEvidence")!=proof):return None
    signature=digest([sha,selected,event.get("characterNames",[])])
    revision=event.get("announcementRevision")
    if event.get("announcementContentSha256")!=signature or not isinstance(revision,int) or isinstance(revision,bool) or not 1<=revision<=1000000:return None
    try:
        published=timestamp(event.get("publishedAt"));updated=timestamp(event["sourceUpdatedAt"]) if event.get("sourceUpdatedAt") else None
        if timestamp(raw.get("publishedAt"))!=published or updated and timestamp(raw.get("sourceUpdatedAt"))!=updated:return None
        clock=updated if updated and updated>=published else published
        if timestamp(event.get("announcementPublishedAt"))!=clock or clock>now or clock<now-ANNOUNCEMENT_LOOKBACK:return None
    except (ValueError,TypeError):return None
    region=event.get("serverRegion")
    if region not in ("KR","JP","GLOBAL","UNSPECIFIED") or region!=source.get("server_region"):return None
    category=event.get("categoryCandidate") or event.get("category")
    if event["calendarRole"]=="main-character" and category!="recruitment":return None
    if event["calendarRole"]=="main-event" and category not in ("limited-event","login-reward"):return None
    return {"gameId":source["game_id"],"sourceId":source_id,"sourceSha256":sha,"sourceTitle":selected,"role":event["calendarRole"],
        "revision":revision,"publishedAt":clock,"category":category,"region":region,
        "scope":"unscoped_official_notice" if region=="UNSPECIFIED" else "regional_official_notice"}


def announcement_contexts(store,uid,now,game_ids=None,check_quiet=True):
    profile=store.get("users/"+uid)
    if not profile or profile.get("uid",uid)!=uid or profile.get("deletionRequestedAt") or profile.get("notificationsEnabled") is False:return {}
    global_settings=store.get(f"users/{uid}/notificationSettings/global") or {}
    def permitted(settings):
        return (settings.get("uid",uid)==uid and settings.get("enabled",True) is not False and settings.get("announcementEnabled",True) is not False
            and settings.get("eventEnabled",True) is not False and ("types" not in settings or "event" in settings["types"]))
    if not permitted(global_settings):return {}
    if check_quiet and (quiet({"quietStart":profile.get("dndStart"),"quietEnd":profile.get("dndEnd"),"timezone":profile.get("timezone","UTC")},now) or quiet(global_settings,now)):return {}
    selections=[(game_id,store.get(f"users/{uid}/userGames/{game_id}")) for game_id in game_ids] if game_ids is not None else store.list(f"users/{uid}/userGames",100)
    contexts={}
    for game_id,selected in selections:
        if not isinstance(game_id,str) or "/" in game_id or not selected or selected.get("uid")!=uid or selected.get("gameId",game_id)!=game_id or selected.get("enabled") is not True:continue
        settings=store.get(f"users/{uid}/notificationSettings/{game_id}") or global_settings
        if not permitted(settings) or check_quiet and quiet(settings,now):continue
        try:
            activation=[timestamp(value) for value in [global_settings.get("announcementSubscribedAt"),settings.get("announcementSubscribedAt"),selected.get("createdAt")] if value is not None]
            if any(value>now for value in activation):continue
        except (ValueError,TypeError):continue
        contexts[game_id]={"profile":profile,"global":global_settings,"settings":settings,"selected":selected,"activationAt":max(activation) if activation else None}
    return contexts


def announcement_region_allowed(context,proof):
    global_settings=context["global"];settings=context["settings"];profile=context["profile"]
    if proof["region"]=="UNSPECIFIED":
        if global_settings.get("globalAnnouncementsEnabled") is not True or settings.get("globalAnnouncementsEnabled",global_settings.get("globalAnnouncementsEnabled")) is not True:return False
    elif any(proof["region"] not in regions for regions in [profile.get("regions",[]),global_settings.get("regions",profile.get("regions",[])),settings.get("regions",profile.get("regions",[]))]):return False
    for preferences in (global_settings,settings):
        categories=preferences.get("eventCategories",["all"])
        if "all" not in categories and proof["category"] not in categories:return False
    return True


def announcement_state_eligible(store,uid,game_id,context,proof):
    state=store.get(f"users/{uid}/announcementNotificationState/{game_id}")
    if not state or state.get("uid")!=uid or state.get("gameId")!=game_id or state.get("active") is not True:return False
    try:
        baseline=timestamp(state["baselineAt"]);activation=context["activationAt"]
        return (not activation or activation<=baseline) and baseline<proof["publishedAt"]
    except (ValueError,TypeError,KeyError):return False


def eligible_announcement_games(store,uid,event,now,source_cache=None):
    proof=announcement_identity(store,event,now,source_cache)
    if not proof:return []
    contexts=announcement_contexts(store,uid,now,[proof["gameId"]])
    return [game_id for game_id,context in contexts.items() if announcement_region_allowed(context,proof) and announcement_state_eligible(store,uid,game_id,context,proof)]


def plan_announcement_user(store,uid,events,now,source_cache=None):
    """First connection/re-enabling arms a baseline and never replays backfill."""
    contexts=announcement_contexts(store,uid,now,check_quiet=False)
    prior=dict(store.list(f"users/{uid}/announcementNotificationState",100));fresh=set()
    for game_id,state in prior.items():
        if state.get("active") is True and game_id not in contexts:
            store.put(f"users/{uid}/announcementNotificationState/{game_id}",{**state,"active":False,"updatedAt":now})
    for game_id,context in contexts.items():
        state=prior.get(game_id);activation=context["activationAt"]
        try:armed=bool(state and state.get("uid")==uid and state.get("gameId")==game_id and state.get("active") is True and (not activation or activation<=timestamp(state["baselineAt"])))
        except (ValueError,TypeError,KeyError):armed=False
        if not armed:
            store.put(f"users/{uid}/announcementNotificationState/{game_id}",{"uid":uid,"gameId":game_id,"active":True,"baselineAt":now,
                "createdAt":(state or {}).get("createdAt",now),"updatedAt":now});fresh.add(game_id)
    planned=0
    for event in events:
        proof=announcement_identity(store,event,now,source_cache)
        if not proof or proof["gameId"] in fresh or not eligible_announcement_games(store,uid,event,now,source_cache):continue
        key=digest([uid,event["id"],proof["revision"],ANNOUNCEMENT_KIND])
        row={"uid":uid,"kind":ANNOUNCEMENT_KIND,"eventId":event["id"],"announcementRevision":proof["revision"],
            "sourceId":proof["sourceId"],"sourceSha256":proof["sourceSha256"],"sourceTitle":proof["sourceTitle"],"announcementRole":proof["role"],
            "scope":proof["scope"],"category":proof["category"],"status":"requested","dueAt":now,"createdAt":now,"updatedAt":now,"dedupeKey":key,"attempts":0}
        planned+=int(store.mutate(f"users/{uid}/notificationJobs/{key}",lambda old:(None,False) if old else (row,True)))
    return planned
TEXT={"ko":("푸시 알림 테스트","이 알림이 보이면 앱에서 수신 확인을 눌러 주세요."),
      "en":("Push notification test","If this notification appears, confirm receipt in the app."),
      "ja":("プッシュ通知テスト","この通知が表示されたら、アプリで受信を確認してください。")}
ATTENDANCE_TEXT={"ko":("ORBIT 출석 확인","{game}: 이번 출석 기간의 게임 실행 기록이 아직 확인되지 않았습니다."),
                 "en":("ORBIT attendance reminder","{game}: a game launch has not been confirmed for this attendance period."),
                 "ja":("ORBIT 出席確認","{game}: 今回の出席期間のゲーム起動記録がまだ確認できていません。")}
EVENT_NEWS={
 "ko":{"official":"게임 공식 소식","pickup":"캐릭터·무기 픽업 소식","new":"최고 등급 신규 캐릭터 픽업 소식","rerun":"최고 등급 캐릭터 복각 소식",
  "event":"게임 이벤트 소식","offline":"오프라인 행사 소식","goods":"굿즈 판매 소식","broadcast":"공식 방송 소식","update":"게임 업데이트 소식","maintenance":"점검 소식",
  "body":"자세한 내용은 원문 공식 공지에서 확인해 주세요."},
 "en":{"official":"Official game notice","pickup":"Character or weapon pickup notice","new":"New top-rarity character pickup notice","rerun":"Top-rarity character rerun notice",
  "event":"Game event notice","offline":"Offline event notice","goods":"Merchandise sales notice","broadcast":"Official broadcast notice","update":"Game update notice","maintenance":"Maintenance notice",
  "body":"Open the original official notice for details."},
 "ja":{"official":"ゲーム公式のお知らせ","pickup":"キャラクター・武器ピックアップのお知らせ","new":"最高レアリティ新キャラクターピックアップのお知らせ","rerun":"最高レアリティキャラクター復刻のお知らせ",
  "event":"ゲームイベントのお知らせ","offline":"オフラインイベントのお知らせ","goods":"グッズ販売のお知らせ","broadcast":"公式配信のお知らせ","update":"ゲームアップデートのお知らせ","maintenance":"メンテナンスのお知らせ",
  "body":"詳しくは原文の公式お知らせをご確認ください。"}}


def _content_language(value):
    return re.split(r"[-_]",value.strip().lower())[0] if isinstance(value,str) else ""


def _plain_event_text(value):
    if not isinstance(value,str):return ""
    text=re.sub(r"<(?:script|style)\b[^>]*>.*?</(?:script|style)>","",value,flags=re.I|re.S)
    text=re.sub(r"\[img(?:=[^\]]*)?\].*?\[/img\]","",text,flags=re.I|re.S)
    text=re.sub(r"</?(?:p|div|h[1-6]|li|br)\b[^>]*>","\n",text,flags=re.I)
    text=re.sub(r"<[^>]+>","",text)
    text=re.sub(r"\[/?(?:p|h[1-6]|list|\*)(?:=[^\]]*|\s[^\]]*)?\]","\n",text,flags=re.I)
    text=re.sub(r"\[/?(?:b|i|u|s|strike|url|quote|table|tr|td|th|previewyoutube|video)(?:=[^\]]*)?\]","",text,flags=re.I)
    text=html.unescape(text)
    return re.sub(r"\n{3,}","\n\n",re.sub(r"[ \t]+\n","\n",text)).strip()


def _text_matches_language(text,lang):
    letters=[char for char in text if char.isalpha()]
    if not letters:return False
    names=[unicodedata.name(char,"") for char in letters]
    hangul=sum(name.startswith("HANGUL") for name in names)
    kana=sum(name.startswith(("HIRAGANA","KATAKANA")) for name in names)
    han=sum(name.startswith(("CJK UNIFIED IDEOGRAPH","CJK COMPATIBILITY IDEOGRAPH")) for name in names)
    if lang=="ko":return hangul>=2 and hangul/len(letters)>=0.2
    if lang=="ja":return not hangul and (kana>0 or han>=2) and (kana+han)/len(letters)>=0.2
    return not hangul and not kana and not han and bool(re.search(r"[a-z]",text,re.I))


def _neutral_event_title(event,lang):
    category=str(event.get("category","")).lower().replace("_","-")
    kind="official"
    if category in ("pickup","recruitment","banner","gacha","character-pickup","weapon-pickup"):
        kind=event.get("announcementKind") if event.get("rarityLabel") and event.get("announcementKind") in ("new","rerun") else "pickup"
    elif category in ("limited-event","in-game","in-game-event","ingame-event","login-reward","login-event","attendance-event","collaboration"):kind="event"
    elif category in ("offline","offline-event","cafe","popup","pop-up","exhibition","fanmeeting","fan-meeting","concert","orchestra","festival","ticket","ticket-sale","ticket-sales"):kind="offline"
    elif category in ("merchandise","goods","goods-sale","goods-sales","goods-reservation","merchandise-reservation","preorder","pre-order","restock"):kind="goods"
    elif category in ("broadcast","livestream","live-stream"):kind="broadcast"
    elif category=="update":kind="update"
    elif category in ("maintenance","maintenance-extension"):kind="maintenance"
    return EVENT_NEWS[lang][kind]


def localized_event_payload(event,lang):
    """Use the same verified language/source guards as the web presentation.

    English source text is never a Korean/Japanese fallback. An extraction-only
    revision increment does not stale a translation with the exact same source
    hash and selected title; changed source bytes or section title always do.
    """
    lang=_content_language(lang)
    if lang not in EVENT_NEWS:lang="en"
    selected=event.get("displayTitle") or event.get("sourceSectionTitle") or event.get("originalTitle") or event.get("title") or ""
    title=_plain_event_text(selected)
    body=_plain_event_text(event.get("displayDescription") or event.get("originalDescription") or event.get("description"))
    source_language=_content_language(event.get("originalLanguage"))
    translations=event.get("translations") or {}
    candidate=translations.get(lang) if isinstance(translations,dict) else None
    usable=isinstance(candidate,dict) and candidate.get("status") in ("completed","verified")
    if usable:
        same_source=bool(candidate.get("sourceSha256") and candidate["sourceSha256"]==event.get("rawSha256")
            and candidate.get("sourceTitle") and candidate["sourceTitle"]==selected)
        if (candidate.get("verificationStatus") and candidate["verificationStatus"]!="verified"
            or candidate.get("targetLanguage") and _content_language(candidate["targetLanguage"])!=lang
            or candidate.get("sourceLanguage") and _content_language(candidate["sourceLanguage"])!=source_language
            or candidate.get("sourceTitle") and candidate["sourceTitle"]!=selected
            or candidate.get("sourceRevision") is not None and event.get("revision") is not None and candidate["sourceRevision"]!=event["revision"] and not same_source
            or candidate.get("sourceSha256") and event.get("rawSha256") and candidate["sourceSha256"]!=event["rawSha256"]):usable=False
    def local_field(key,original):
        translated=_plain_event_text(candidate.get(key)) if usable else ""
        if translated and _text_matches_language(translated,lang) and (source_language==lang or unicodedata.normalize("NFKC",translated)!=unicodedata.normalize("NFKC",original)):
            return translated
        if source_language==lang and _text_matches_language(original,lang):return original
        return ""
    return local_field("title",title) or _neutral_event_title(event,lang),local_field("description",body) or EVENT_NEWS[lang]["body"]


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
    if kind==ANNOUNCEMENT_KIND:return bool(eligible_announcement_games(store,uid,event,now))
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
    if not event or not content_policy_allows(event,now,calendar=True) or content_publication_status(event.get("publishedAt"),now)!="INCLUDED" or event.get("verificationStatus")!="verified" or event.get("status") in ["cancelled","canceled","postponed","ended"]:return False
    if event.get("sourceProvider")=="youtube" or event.get("sourceChannelId") or event.get("sourceVideoId"):
        try:
            checked,expires=timestamp(event.get("metadataCheckedAt")),timestamp(event.get("metadataExpiresAt"))
            if checked>now or not checked<expires<=checked+timedelta(days=30) or expires<=now:return False
        except (ValueError,TypeError):return False
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
    eligible_games=eligible_announcement_games(store,uid,event,now) if job["kind"]==ANNOUNCEMENT_KIND else [game_id for game_id in event.get("gameIds",[])
                    if eligible(store,uid,{**event,"gameIds":[game_id]},job["kind"],now)]
    if job["kind"]==ANNOUNCEMENT_KIND and not eligible_games:raise SafetyError("announcement_became_ineligible")
    presented={**event,"category":event.get("categoryCandidate") or event.get("category")} if job["kind"]==ANNOUNCEMENT_KIND else event
    title,body=localized_event_payload(presented,lang)
    if job["kind"]==ANNOUNCEMENT_KIND:
        lead={"ko":"공식에서 새 공지를 게시했습니다.","en":"A new official announcement was published.","ja":"公式から新しいお知らせが公開されました。"}.get(lang,"A new official announcement was published.")
        if event.get("serverRegion")=="UNSPECIFIED":lead+=" "+{"ko":"서버 지역이 확인되지 않은 소식입니다.","en":"The server region has not been confirmed.","ja":"サーバー地域は未確認です。"}.get(lang,"The server region has not been confirmed.")
        body=lead+" "+body
    payload={"title":title[:200],"body":body[:500],
            "url":"/events/"+event["id"],"jobId":job["id"],"tag":"orbit-event-"+job.get("dedupeKey",job["id"]),
            "kind":job["kind"],"gameId":eligible_games[0] if eligible_games else "",
            "gameIds":eligible_games,"category":presented.get("category",""),"eventId":event["id"],"recipientUid":uid}
    if job["kind"]==ANNOUNCEMENT_KIND:payload.update(scope=job["scope"],announcementRole=job["announcementRole"])
    return payload


def announcement_job_current(store,uid,job,event,now):
    proof=announcement_identity(store,event,now)
    return bool(proof and job.get("uid")==uid and job.get("announcementRevision")==proof["revision"]
        and all(job.get(field)==proof[key] for field,key in [("sourceId","sourceId"),("sourceSha256","sourceSha256"),("sourceTitle","sourceTitle"),("announcementRole","role"),("scope","scope"),("category","category")])
        and eligible_announcement_games(store,uid,event,now))


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
    valid_revision=announcement_job_current(store,uid,job,event,now) if job["kind"]==ANNOUNCEMENT_KIND else not (job.get("eventId") and event and job.get("eventRevision")!=event.get("revision",1))
    if not valid_revision or not eligible(store,uid,event,job["kind"],now):
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
    accepted,failed,ambiguous,withheld=0,0,0,0
    for device_id,sub in devices:
        if job["kind"]==ANNOUNCEMENT_KIND:
            latest_event=store.get("gameEvents/"+job["eventId"]);latest_sub=store.get(f"users/{uid}/notificationSubscriptions/{device_id}") or {}
            if (not announcement_job_current(store,uid,job,latest_event,now) or latest_sub.get("uid")!=uid
                or latest_sub.get("enabled") is not True or latest_sub.get("transport")!="fcm" or latest_sub.get("token")!=sub.get("token")):
                withheld+=1;continue
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
    status="partially_accepted" if accepted and (failed or withheld) else "provider_accepted" if accepted else "delivery_unconfirmed" if ambiguous else "cancelled" if withheld else "in_app_only"
    final={**store.get(path),"status":status,"leaseToken":None,"acceptedDeviceCount":accepted,"failedDeviceCount":failed,"updatedAt":now,"humanConfirmedAt":None}
    if withheld:final.update(withheldDeviceCount=withheld,reasonCode="INELIGIBLE_BEFORE_PROVIDER")
    store.put(path,final)
    if status=="cancelled":return status
    store.put(f"users/{uid}/notifications/{job_id}",{"uid":uid,"title":payload["title"],"body":payload["body"],"url":payload["url"],"status":status,"readAt":None,"createdAt":now})
    return status


def run(store,sender,now=None,max_users=50):
    now=now or datetime.now(UTC);events=[value for _,value in store.list("gameEvents",200)]
    announcement_rows=store.list_since("gameEvents","announcementPublishedAt",now-ANNOUNCEMENT_LOOKBACK,200)
    announcements=[value for _,value in announcement_rows];source_cache={}
    stats={"users":0,"planned":0,"announcementPlanned":0,"announcementScanned":len(announcements),"announcementScanLimitReached":len(announcement_rows)==200,"attendancePlanned":0,"providerAcceptedJobs":0,"inAppOnly":0,"cancelled":0}
    for uid,profile in store.list("users",max_users):
        if profile.get("deletionRequestedAt"):continue
        stats["users"]+=1;stats["planned"]+=plan_user(store,uid,events,now)
        stats["announcementPlanned"]+=plan_announcement_user(store,uid,announcements,now,source_cache)
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
    if payload.get("kind")==ANNOUNCEMENT_KIND:data.update({"scope":payload["scope"],"announcementRole":payload["announcementRole"]})
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
