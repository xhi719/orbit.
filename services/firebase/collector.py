"""Firestore Spark publisher worker with reviewed explicit schedule extraction.

Default command is a local plan with zero external requests and zero remote writes.
--live fetches reviewed official sources; --apply additionally requires Admin safety
checks and pre-enabled Firestore sources. Retained Supabase workers are inactive.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import html
import ipaddress
import json
from pathlib import Path
import re
import sys
import time
import uuid
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "collector"))
from engine import fetch_source, ValidationError, normalize_url
from official_adapter import steam_request, parse_steam_documents, aware_timestamp
from backfill import retry_after_seconds
from store import MemoryStore, FirestoreStore, SafetyError, timestamp

UTC = timezone.utc
EXTRACTION_VERSION = 3


def source_policy(source):
    """Source-specific opt-ins remain separate from permission to read a feed."""
    return {key: source.get(key) for key in (
        "schedule_extraction_approved", "schedule_extraction_reviewed_at", "schedule_extraction_evidence_url",
        "content_language", "server_region", "server_label", "highest_character_rarity", "image_reuse",
        "section_schedule_format", "paired_recruitment_period_approved")}


def plain_notice(value):
    # Preserve paragraph boundaries for semantic labels and local character roles.
    text = re.sub(r"\[url(?:=[^\]]+)?\]([^\[]*)\[/url\]", r"\1", value, flags=re.I)
    text = re.sub(r"<(?:br\s*/?|/p|/div|/h[1-6]|/li)>|\[/(?:p|h[1-6]|list|\*)\]", "\n", text, flags=re.I)
    return html.unescape(re.sub(r"<[^>]*>|\[/?(?:b|i|u|h[1-6]|p|list|\*)\]", "", text))


def safe_public_url(value):
    try:
        url=normalize_url(value)
        host=urlsplit(url).hostname
        if host=="localhost" or host.endswith((".localhost",".local")):
            return None
        try:
            if not ipaddress.ip_address(host).is_global:return None
        except ValueError:
            pass
        return url
    except (ValidationError, ValueError, TypeError):
        return None


DATE_TIME = re.compile(
    r"(?P<year>20\d{2})\s*[-/.年년]\s*(?P<month>\d{1,2})\s*[-/.月월]\s*(?P<day>\d{1,2})\s*[日일.]?"
    r"(?:[T\s]+)(?P<hour>\d{1,2})(?::|時|시)(?P<minute>\d{2})(?:分|분)?(?::(?P<second>\d{2}))?"
    r"(?P<zone>Z|[+-]\d{2}:\d{2})?", re.I)
PERIOD_LABEL = re.compile(r"event\s+(?:duration|period)|banner\s+(?:duration|period)|availability\s+period|"
    r"discount\s+duration|(?:이벤트|픽업|모집|판매|개최|진행)\s*기간|(?:開催|イベント|ピックアップ|募集)期間|^\s*[▸▶•]?\s*(?:[A-Za-z -]{1,40}\s+)?(?:duration|period)\s*[:：]", re.I)
START_LABEL = re.compile(r"^\s*(?:start(?:s|ing)?(?:\s+(?:date|time))?|시작(?:\s*(?:일시|시각))?|開始(?:日時|時刻)?)\s*[:：]", re.I)
END_LABEL = re.compile(r"^\s*(?:end(?:s|ing)?(?:\s+(?:date|time))?|종료(?:\s*(?:일시|시각))?|終了(?:日時|時刻)?)\s*[:：]", re.I)
ZONE_LABEL = re.compile(r"\b(?:UTC|GMT)\s*([+-])\s*(\d{1,2})(?::?(\d{2}))?\b|\b(KST|JST)\b|\b(UTC|GMT)\b", re.I)
MONTH_NAMES={name.lower():index for index,name in enumerate(("January","February","March","April","May","June","July","August","September","October","November","December"),1)}


def explicit_english_dates(line):
    months="|".join(MONTH_NAMES)
    def month_first(match):
        return f"{match.group(3)}-{MONTH_NAMES[match.group(1).lower()]:02d}-{int(match.group(2)):02d}"
    def day_first(match):
        return f"{match.group(3)}-{MONTH_NAMES[match.group(2).lower()]:02d}-{int(match.group(1)):02d}"
    line=re.sub(r"\b("+months+r")\s+(\d{1,2})(?:st|nd|rd|th)?\s*,?\s+(20\d{2})\b",month_first,line,flags=re.I)
    return re.sub(r"\b(\d{1,2})(?:st|nd|rd|th)?\s+("+months+r")\s*,?\s+(20\d{2})\b",day_first,line,flags=re.I)


def extract_schedule(text):
    """Only labelled complete timestamps become a candidate; no inferred year/time.

    Publisher post time never participates. Multiple interval candidates, missing
    offsets, contradictory offsets and impossible/descending ranges fail closed.
    """
    lines = text.splitlines()
    reasons = []
    zone_offsets = set()
    for zone in ZONE_LABEL.finditer(text):
        minutes = 0 if zone.group(5) else 540 if zone.group(4) else (int(zone.group(2)) * 60 + int(zone.group(3) or 0)) * (1 if zone.group(1) == "+" else -1)
        if abs(minutes) > 840 or zone.group(3) and int(zone.group(3))>59:
            reasons.append("TIMEZONE_INVALID")
        else:
            zone_offsets.add(minutes)
    intervals = []
    labelled_start, labelled_end = [], []
    for index,original_line in enumerate(lines):
        line=explicit_english_dates(original_line)
        matches = list(DATE_TIME.finditer(line))
        if PERIOD_LABEL.search(line) and not matches and index+1<len(lines):
            # Only the immediately following nonblank row can complete a label.
            candidate=next((row for row in lines[index+1:index+3] if row.strip()),"")
            if candidate and not re.match(r"^\s*(?:■|▸|\[h[1-6]\])",candidate,re.I):
                original_line=original_line+"\n"+candidate
                line=explicit_english_dates(original_line)
                matches=list(DATE_TIME.finditer(line))
        if (PERIOD_LABEL.search(line) or START_LABEL.search(line) or END_LABEL.search(line)) and re.search(r"\b(?:AM|PM)\b",line,re.I):
            reasons.append("TWELVE_HOUR_CLOCK_UNSUPPORTED")
        if PERIOD_LABEL.search(line) and len(matches)>2:
            reasons.append("SCHEDULE_BOUNDARIES_AMBIGUOUS")
        if PERIOD_LABEL.search(line) and len(matches) == 2:
            intervals.append((matches[0], matches[1], original_line[:1000]))
        elif START_LABEL.search(line) and len(matches) == 1:
            labelled_start.append((matches[0], original_line[:1000]))
        elif END_LABEL.search(line) and len(matches) == 1:
            labelled_end.append((matches[0], original_line[:1000]))
    if labelled_start or labelled_end:
        if len(labelled_start) == len(labelled_end) == 1:
            intervals.append((labelled_start[0][0], labelled_end[0][0], labelled_start[0][1] + "\n" + labelled_end[0][1]))
        else:
            reasons.append("SCHEDULE_BOUNDARIES_AMBIGUOUS")
    unique = {(a.group(0), b.group(0)): (a, b, proof) for a, b, proof in intervals}
    if len(unique) != 1:
        reasons.append("SCHEDULE_MISSING" if not unique else "SCHEDULE_CONFLICT")
        return {"startsAt": None, "endsAt": None, "originalTimezone": None, "evidenceText": None, "reasonCodes": list(dict.fromkeys(reasons))}
    first, last, proof = next(iter(unique.values()))
    offsets = []
    values = []
    for match in (first, last):
        marker = match.group("zone")
        if marker:
            offset = 0 if marker.upper() == "Z" else (int(marker[1:3]) * 60 + int(marker[-2:])) * (1 if marker[0] == "+" else -1)
            if abs(offset) > 840 or marker.upper() != "Z" and int(marker[-2:]) > 59:
                reasons.append("TIMEZONE_INVALID")
        elif len(zone_offsets) == 1:
            offset = next(iter(zone_offsets))
        else:
            reasons.append("TIMEZONE_MISSING" if not zone_offsets else "TIMEZONE_CONFLICT")
            continue
        offsets.append(offset)
        try:
            values.append(datetime(*(int(match.group(k)) for k in ("year", "month", "day", "hour", "minute")),int(match.group("second") or 0), tzinfo=timezone(timedelta(minutes=offset))).astimezone(UTC))
        except (ValueError, OverflowError):
            reasons.append("SCHEDULE_INVALID_DATE")
    if len(set(offsets)) > 1 or zone_offsets and set(offsets) != zone_offsets:
        reasons.append("TIMEZONE_CONFLICT")
    if len(values) == 2 and values[1] <= values[0]:
        reasons.append("SCHEDULE_INVALID_RANGE")
    if reasons or len(values) != 2:
        return {"startsAt": None, "endsAt": None, "originalTimezone": None, "evidenceText": proof, "reasonCodes": list(dict.fromkeys(reasons))}
    offset = offsets[0]
    zone = "UTC" if offset == 0 else "UTC" + ("+" if offset > 0 else "-") + f"{abs(offset)//60:02d}:{abs(offset)%60:02d}"
    return {"startsAt": values[0], "endsAt": values[1], "originalTimezone": zone, "evidenceText": proof, "reasonCodes": []}


SECTION_HEADING=re.compile(r"^\s*■\s*(.{1,300})\s*$|^\s*\[h[1-6]\](.{1,300}?)\[/h[1-6]\]\s*$|^\s*<h[1-6][^>]*>(.{1,300}?)</h[1-6]>\s*$",re.I)


def section_documents(document,source):
    """Reviewed heading blocks are independent schedules, never one merged range.

    GF2's adjacent character/weapon recruitment pair shares a following period
    only when that exact publisher format was separately reviewed. Otherwise the
    character heading without a period remains pending instead of borrowing it.
    """
    if document.get("_section_key") or source.get("section_schedule_format")!="heading_period_v1":return []
    lines=document["original_description"].splitlines()
    blocks=[]
    for line in lines:
        heading=SECTION_HEADING.match(line)
        if heading:
            title=html.unescape(re.sub(r"<[^>]*>","",next(value for value in heading.groups() if value))).strip().strip("[]")
            blocks.append({"title":title,"lines":[line]})
        elif blocks:
            blocks[-1]["lines"].append(line)
    if len(blocks)>40:raise SafetyError("section_limit_exceeded")
    result=[];seen={}
    for index,block in enumerate(blocks):
        title=block["title"];content="\n".join(block["lines"]).strip()
        character=bool(re.match(r"Targeted Procurement(?: Event)?\s*[|｜-]",title,re.I))
        weapon=bool(re.match(r"Military Upgrade\s*[|｜-]",title,re.I))
        if character and source.get("paired_recruitment_period_approved") is True and index+1<len(blocks):
            following=blocks[index+1]
            if re.match(r"Military Upgrade\s*[|｜-]",following["title"],re.I) and not extract_schedule(plain_notice(content))["startsAt"]:
                next_schedule=extract_schedule(plain_notice("\n".join(following["lines"])))
                if next_schedule["startsAt"]:
                    content+="\n"+next_schedule["evidenceText"]
        chars=re.findall(r"\b(?:Elite|Standard)\s+Doll\s*\[([^\]\n]{1,100})\]",content,re.I) if character else []
        weapons=re.findall(r"\b(?:Elite|Standard)\s+Weapon\s*\[([^\]\n]{1,100})\]",content,re.I) if weapon else []
        category="recruitment" if character or weapon else "login-reward" if re.search(r"\b(?:log\s*in|sign\s*in)\b|출석|ログイン",content,re.I) else "goods" if re.search(r"\b(?:outfit|skin|discount)\b|의상|衣装",title,re.I) else "limited-event"
        seen[title]=seen.get(title,0)+1
        key=digest({"title":title,"occurrence":seen[title]})
        pickups=[]
        if chars or weapons:
            pickups=[{"server_region":document["server_region"],"server_label":document["server_label"],
                "pickup_kind":"character" if chars else "weapon","characters":chars,"weapons":weapons,
                "official_url":document["official_url"]}]
        result.append({**document,"original_description":content,"_raw_original_description":document["original_description"],
            "_section_key":key,"_section_title":title,"_section_category":category,"_parent_event_id":event_identity(document,source),
            "event_type_candidates":["pickup"] if category=="recruitment" else [category],"pickups":pickups,
            "_raw_character_names":chars})
    return result


def image_candidates(document, source):
    """Keep original URLs as evidence; never download, proxy or approve by origin.

    Rendering approval requires a separately reviewed exact-host reuse policy.
    Community images can be supported through that same explicit policy, never
    through membership in a community or a search-engine result alone.
    """
    raw = document["original_description"]
    found = re.findall(r"\[img\](https://[^\[\s]+)\[/img\]|<img\b[^>]*\bsrc=[\"'](https://[^\"']+)[\"']", raw, re.I)
    policy = source.get("image_reuse") or {}
    approved = policy.get("rightsStatus") == "APPROVED" and safe_public_url(policy.get("evidenceUrl")) and policy.get("checkedAt")
    candidates = []
    for pair in found[:20]:
        url = safe_public_url(html.unescape(next(value for value in pair if value)))
        if not url or any(row["url"] == url for row in candidates):
            continue
        allowed = approved and urlsplit(url).hostname in policy.get("allowedHosts", [])
        candidates.append({"url": url, "sourceUrl": document["official_url"], "kind": "PICKUP_ART" if "pickup" in document.get("event_type_candidates", []) else "NOTICE_ART",
            "rightsStatus": "APPROVED" if allowed else "UNCONFIRMED", "rightsEvidenceUrl": policy.get("evidenceUrl") if allowed else None})
    return candidates


def highest_rarity_targets(text, source):
    """Never assume SSR/S/6-star means the highest tier across all games.

    The reviewed game/source rule supplies exact tier aliases. Both the rarity
    and new/rerun wording must be local to the same quoted-character line.
    Mixed-role clauses and unquoted or ambiguous targets stay as the source post.
    """
    rule = source.get("highest_character_rarity") or {}
    labels = rule.get("labels", [])
    if not rule.get("checkedAt") or not safe_public_url(rule.get("evidenceUrl")) or not isinstance(labels, list) or not labels or len(labels) > 10:
        return []
    if any(not isinstance(label, str) or not 1 <= len(label) <= 30 for label in labels):
        return []
    highest = re.compile(r"(?<![\w])(?:" + "|".join(re.escape(label) for label in labels) + r")(?![\w])", re.I)
    new_words = re.compile(r"\bnew\b|\bdebut\b|신규|새로운|新登場|新規", re.I)
    rerun_words = re.compile(r"\bre[- ]?run\b|\breturn(?:ing|s)?\b|복각|再登場|復刻", re.I)
    character_words = re.compile(r"\b(?:character|doll)s?\b|캐릭터|인형|キャラクター|人形", re.I)
    excluded = re.compile(r"\b(?:weapon|outfit|costume|skin)\b|무기|의상|스킨|武器|衣装", re.I)
    targets = []
    for line in text.splitlines():
        tiers = list(highest.finditer(line))
        if len(tiers) != 1 or not character_words.search(line) or excluded.search(line):
            continue
        role_text=re.sub(r'[“"「『\[]([^”"」』\]\n]{1,100})[”"」』\]]', "", line)
        is_new, is_rerun = bool(new_words.search(role_text)), bool(rerun_words.search(role_text))
        if is_new == is_rerun:
            continue
        # A secondary tier in the line could refer to a quoted featured 4-star.
        if re.search(r"(?<!\d)[1-6]\s*[- ]?star\b|[1-6]\s*성|★[1-6]|[1-6]★|\b(?:SSR|SR|SS|S|A|B)\b", highest.sub("", line), re.I):
            continue
        names = re.findall(r'[“"「『\[]([^”"」』\]\n]{1,100})[”"」』\]]', line)
        if len(names) != 1:
            continue
        row = {"name": names[0].strip(), "announcementKind": "new" if is_new else "rerun", "rarityLabel": tiers[0].group(0), "evidenceText": line[:1000]}
        if row["name"] and not any(old["name"] == row["name"] for old in targets):
            targets.append(row)
    return targets[:20]


def enrich_document(document, source):
    text = plain_notice(document["original_title"] + "\n" + document["original_description"])
    schedule = extract_schedule(text)
    reasons = list(schedule["reasonCodes"])
    authorized = reviewed(source) and source.get("schedule_extraction_approved") is True and source.get("schedule_extraction_reviewed_at") and safe_public_url(source.get("schedule_extraction_evidence_url"))
    if urlsplit(document.get("official_url","")).hostname not in source.get("allowed_notice_hosts",[]):
        authorized=False
        reasons.append("OFFICIAL_NOTICE_HOST_UNCONFIRMED")
    if not authorized:
        reasons.append("EXTRACTION_REVIEW_REQUIRED")
    region = document.get("server_region")
    if region not in ("KR", "JP", "GLOBAL"):
        reasons.append("SERVER_REGION_UNCONFIRMED")
    # Explicit competing regional labels require a regional adapter, not merging.
    region_names={"kr":"KR","한국":"KR","jp":"JP","일본":"JP","global":"GLOBAL","글로벌":"GLOBAL"}
    regions = {region_names[token.lower()] for token in re.findall(r"\b(KR|JP|GLOBAL|한국|일본|글로벌)\s*(?:server|서버)", text, re.I)}
    if len(regions) > 1 or regions and region not in regions:
        reasons.append("SERVER_REGION_CONFLICT")
    hints = document.get("event_type_candidates", [])
    category = document.get("_section_category") or next((kind for kind in ("pickup", "login_reward", "maintenance", "update", "official_broadcast") if kind in hints), "unclassified")
    category = "recruitment" if category == "pickup" else category
    verified = not reasons and category != "unclassified"
    images = image_candidates(document, source)
    targets = highest_rarity_targets(text, source) if "pickup" in hints and reviewed(source) else []
    cancelled = bool(re.search(r"\b(?:event|banner)\s+(?:is\s+|has\s+been\s+)?cancelled\b|(?:이벤트|픽업)\s*(?:가\s*)?취소|(?:イベント|ピックアップ).*中止", text, re.I))
    return {**document, "schedule": schedule, "schedule_verified": bool(verified), "category_candidate": category,
        "extraction_version": EXTRACTION_VERSION, "extraction_policy_sha256": digest(source_policy(source)),
        "schedule_reason_codes": list(dict.fromkeys(reasons)), "image_candidates": images,
        "approved_image": next((image for image in images if image["rightsStatus"] == "APPROVED"), None),
        "highest_rarity_targets": targets, "cancelled": cancelled,"section_documents":section_documents(document,source)}


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
            "health":{"status":"DISABLED", "consecutiveFailures":0},
            "scheduleExtractionApproved":source.get("schedule_extraction_approved",False),
            "scheduleExtractionReviewedAt":source.get("schedule_extraction_reviewed_at"),
            "scheduleExtractionEvidenceUrl":source.get("schedule_extraction_evidence_url"),
            "contentLanguage":source.get("content_language"),
            "highestCharacterRarity":source.get("highest_character_rarity"),"imageReuse":source.get("image_reuse"),
            "sectionScheduleFormat":source.get("section_schedule_format"),"pairedRecruitmentPeriodApproved":source.get("paired_recruitment_period_approved",False)}


def operating_source(row):
    """Admin-reviewed Firestore source configuration is authoritative in apply mode."""
    return {"id":row["id"],"game_id":row["gameId"],"url":row["url"],"parser":row["parser"],
            "steam_appid":row["steamAppid"],"enabled":row.get("enabled",False),
            "approved_for_staging":row.get("approvedForStaging",False),"access_status":row.get("accessStatus"),
            "terms_url":row.get("termsUrl"),"terms_checked_at":row.get("termsCheckedAt"),
            "notice_hosts_verified_at":row.get("noticeHostsVerifiedAt"),"allowed_notice_hosts":row.get("allowedNoticeHosts",[]),
            "feed_allowlist":row.get("feedAllowlist",[]),"collection_interval_minutes":max(15,row.get("intervalMinutes",15)),
            "server_region":row.get("serverRegion") or "UNSPECIFIED","server_label":row.get("serverLabel"),
            "method":"official_api","official_host":"api.steampowered.com","request_timeout_seconds":20,"max_response_bytes":2097152,
            "schedule_extraction_approved":row.get("scheduleExtractionApproved",False),
            "schedule_extraction_reviewed_at":row.get("scheduleExtractionReviewedAt"),
            "schedule_extraction_evidence_url":row.get("scheduleExtractionEvidenceUrl"),
            "content_language":row.get("contentLanguage"),"highest_character_rarity":row.get("highestCharacterRarity"),
            "image_reuse":row.get("imageReuse"),"section_schedule_format":row.get("sectionScheduleFormat"),
            "paired_recruitment_period_approved":row.get("pairedRecruitmentPeriodApproved",False)}


def seed_catalogue(store, games, registry):
    result={"games":0,"sources":0,"unchanged":0}
    for game in games:
        row={**game, "id":game["id"], "resetRules":[], "verifiedResetRuleIds":[]}
        # Candidate seed is not permission to overwrite independently verified reset rules.
        def upsert(old):
            if old:
                row.update({k:old[k] for k in ["resetRules","verifiedResetRuleIds","resetRuleById"] if k in old})
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
                    row.update({k:old[k] for k in ["enabled","health","lastCheckedAt","lastSuccessAt","lastErrorAt","lastReverifiedAt","nextRunAt",
                        "scheduleExtractionApproved","scheduleExtractionReviewedAt","scheduleExtractionEvidenceUrl","contentLanguage","highestCharacterRarity","imageReuse","sectionScheduleFormat","pairedRecruitmentPeriodApproved"] if k in old})
                return (None,False) if old == row else (row,True)
            changed=store.mutate("gameSources/"+source["id"],upsert_source)
            result["sources" if changed else "unchanged"]+=1
    return result


def event_identity(document, source):
    identity={"game":source["game_id"],"source":source["id"],"region":document["server_region"],
                   "server":document["server_label"],"externalKey":document["external_key"],"pickupKind":None}
    if document.get("_section_key"):identity["sectionKey"]=document["_section_key"]
    return digest(identity)


def confirmed_window_overlap(document,source,start,end):
    """An older publisher overview can supply currently active official periods.

    At most the already fetched bounded page is inspected. No guessed post dates,
    unreviewed schedules or arbitrary history expansion qualify for insertion.
    """
    enriched=enrich_document(document,source)
    scopes=[enriched]+[enrich_document(section,source) for section in enriched["section_documents"]]
    return any(row["schedule_verified"] and row["schedule"]["startsAt"]<end and row["schedule"]["endsAt"]>=start for row in scopes)


def retire_derived_event(store,event_id,row,observed,depth=0):
    if row.get("status")!="cancelled" or row.get("verificationStatus")!="pending":
        store.put("gameEvents/"+event_id,{**row,"verificationStatus":"pending","status":"cancelled","lastChangedAt":observed,"revision":row.get("revision",0)+1})
    for pickup_id,pickup in store.list_equal("pickupSchedules","eventId",event_id,50):
        if pickup.get("status")!="cancelled" or pickup.get("verificationStatus")!="pending":
            store.put("pickupSchedules/"+pickup_id,{**pickup,"verificationStatus":"pending","status":"cancelled","lastChangedAt":observed})
    if depth<1:
        for child_id,child in store.list_equal("gameEvents","parentEventId",event_id,50):
            retire_derived_event(store,child_id,child,observed,depth+1)


def upsert_notice(store, source, document, observed):
    document=enrich_document(document,source)
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
        if old and old.get("rawSha256") == document["raw_sha256"] and old.get("extractionVersion")==EXTRACTION_VERSION and old.get("extractionPolicySha256")==document["extraction_policy_sha256"]:
            if old.get("verificationStatus")=="verified" and old.get("status") not in ("cancelled","postponed") and old.get("startsAt") and old.get("endsAt"):
                state="ended" if timestamp(old["endsAt"])<=observed else "ongoing" if timestamp(old["startsAt"])<=observed else "upcoming"
                if state!=old.get("status"):
                    return {**old,"status":state,"lastCheckedAt":observed,"lastChangedAt":observed,"revision":old.get("revision",0)+1},"updated"
            return None,"unchanged"
        row={**(old or {}),"id":event_id,"sourceId":source["id"],"externalKey":document["external_key"],
             "gameIds":[source["game_id"]],"serverRegion":document["server_region"],"serverLabel":document["server_label"],
             "title":document.get("_section_title") or document["original_title"],"description":document["original_description"],
             "displayTitle":document.get("_section_title"),"sourceSectionTitle":document.get("_section_title"),
             "sourceSectionKey":document.get("_section_key"),"parentEventId":document.get("_parent_event_id"),
             "characterNames":document.get("_raw_character_names",[]),
             "originalTitle":document["original_title"],"originalDescription":document.get("_raw_original_description") or document["original_description"],
             "originalLanguage":source.get("content_language") or document["original_language"],"officialUrl":document["official_url"],
             "publishedAt":timestamp(document["published_at"]) if document["published_at"] else None,
             "sourceUpdatedAt":timestamp(document["source_updated_at"]) if document["source_updated_at"] else None,
             "rawSha256":document["raw_sha256"],"eventTypeCandidates":document["event_type_candidates"],
             "firstCheckedAt":old.get("firstCheckedAt",observed) if old else observed,
             "lastCheckedAt":observed,"lastChangedAt":observed,"verificationStatus":"pending",
             "extractionVersion":EXTRACTION_VERSION,"extractionPolicySha256":document["extraction_policy_sha256"],
             "scheduleEvidence":{"method":"explicit_labelled_timestamps","evidenceText":document["schedule"]["evidenceText"],
                 "reasonCodes":document["schedule_reason_codes"]},
             "imageCandidates":document["image_candidates"],"image":document["approved_image"],
             "announcementKind":"aggregate" if document["highest_rarity_targets"] else None,
             "derivedCharacterAnnouncements":len(document["highest_rarity_targets"]),
             "derivedSectionCount":len(document["section_documents"]),
             "revision":(old.get("revision",0) if old else 0)+1}
        # Raw notices do not erase manual dates/categories; changes revoke approval.
        for key,default in {"startsAt":None,"endsAt":None,"reservationStartsAt":None,"reservationEndsAt":None,
                            "ticketSalesAt":None,"originalTimezone":None,"category":"unclassified",
                            "scheduleStatus":"DATE_UNCONFIRMED","status":"unconfirmed"}.items():
            row.setdefault(key,default)
        if document["schedule_verified"]:
            # Manual reviewed boundaries survive unless the source independently
            # matches them. A conflict revokes approval rather than overwriting.
            schedule=document["schedule"]
            conflict=old and old.get("scheduleOrigin")!="automatic" and any(old.get(key) is not None and timestamp(old[key])!=schedule[key] for key in ("startsAt","endsAt"))
            if conflict:
                row["scheduleEvidence"]["reasonCodes"].append("MANUAL_SCHEDULE_CONFLICT")
            else:
                row.update({key:schedule[key] for key in ("startsAt","endsAt","originalTimezone")})
                row.update(scheduleOrigin="automatic",category=document["category_candidate"],verificationStatus="verified",lastVerifiedAt=observed,
                    scheduleStatus="DATE_VERIFIED",status="ended" if schedule["endsAt"]<=observed else "ongoing" if schedule["startsAt"]<=observed else "upcoming")
        elif old and old.get("scheduleOrigin")=="automatic":
            row.update(startsAt=None,endsAt=None,originalTimezone=None,scheduleStatus="DATE_UNCONFIRMED",status="unconfirmed")
        if document["cancelled"]:
            row.update(status="cancelled",scheduleStatus="CANCELLED")
        if len(json.dumps(row,ensure_ascii=False,default=lambda value:value.isoformat()).encode())>900000:
            raise SafetyError("event_document_exceeds_safe_firestore_size")
        return row,"updated" if old else "created"
    outcome=store.mutate("gameEvents/"+event_id,update)
    for pickup in document.get("pickups",[]):
        pickup_id=digest({"event":event_id,"game":source["game_id"],"region":pickup["server_region"],"server":pickup["server_label"],"kind":pickup["pickup_kind"]})
        def update_pickup(old):
            if outcome == "unchanged" and old:
                return None,False
            row={**(old or {}),"id":pickup_id,"eventId":event_id,"gameId":source["game_id"],"sourceId":source["id"],
                 "serverRegion":pickup["server_region"],"serverLabel":pickup["server_label"],
                 "officialUrl":pickup["official_url"],"verificationStatus":"pending","lastCheckedAt":observed,
                 "imageCandidates":document["image_candidates"],"image":document["approved_image"],
                 "status":"cancelled" if document["cancelled"] else "unconfirmed",
                 "firstCheckedAt":old.get("firstCheckedAt",observed) if old else observed}
            for key,default in {"pickupKind":None,"characters":[],"weapons":[],"startsAt":None,"endsAt":None}.items():
                row.setdefault(key,default)
            if document.get("_section_key") and pickup.get("pickup_kind") and (pickup.get("characters") or pickup.get("weapons")):
                parent=store.get("gameEvents/"+event_id)
                row.update(pickupKind=pickup["pickup_kind"],characters=pickup.get("characters",[]),weapons=pickup.get("weapons",[]),
                    startsAt=parent.get("startsAt"),endsAt=parent.get("endsAt"),verificationStatus=parent["verificationStatus"],
                    status=parent["status"],parentEventId=document["_parent_event_id"],originalTimezone=parent.get("originalTimezone"),
                    sourceSectionTitle=document["_section_title"],lastChangedAt=observed,lastVerifiedAt=parent.get("lastVerifiedAt"),scheduleOrigin="automatic")
            return row,True
        store.mutate("pickupSchedules/"+pickup_id,update_pickup)
    if outcome != "unchanged" or document["section_documents"]:
        # Each explicit highest-tier new/rerun character is independently addressable.
        # Unknown roles/tiers never become promotional child posts.
        parent=store.get("gameEvents/"+event_id)
        active_children={event_identity(section,source) for section in document["section_documents"]}
        for section in document["section_documents"]:
            upsert_notice(store,source,section,observed)
        for target in document["highest_rarity_targets"]:
            child_id=digest({"parent":event_id,"announcementKind":target["announcementKind"],"character":target["name"]})
            active_children.add(child_id)
            def child_update(old):
                title=target["name"]
                row={**parent,"id":child_id,"parentEventId":event_id,"announcementKind":target["announcementKind"],
                    "rarityLabel":target["rarityLabel"],"characters":[target["name"]],"characterEvidenceText":target["evidenceText"],
                    "title":title,"displayTitle":title,"characterNames":[target["name"]],"originalTitle":document["original_title"],"derivedCharacterAnnouncements":0,
                    "firstCheckedAt":old.get("firstCheckedAt",observed) if old else observed,
                    "revision":old.get("revision",0)+1 if old else 1}
                return row,True
            store.mutate("gameEvents/"+child_id,child_update)
            pickup_id=digest({"event":child_id,"kind":"character"})
            def child_pickup(old):
                row={"id":pickup_id,"eventId":child_id,"parentEventId":event_id,"gameId":source["game_id"],
                    "sourceId":source["id"],"serverRegion":parent["serverRegion"],"serverLabel":parent["serverLabel"],
                    "officialUrl":parent["officialUrl"],"pickupKind":"character","characters":[target["name"]],"weapons":[],
                    "announcementKind":target["announcementKind"],"rarityLabel":target["rarityLabel"],
                    "startsAt":parent.get("startsAt"),"endsAt":parent.get("endsAt"),"originalTimezone":parent.get("originalTimezone"),
                    "verificationStatus":parent["verificationStatus"],"status":parent["status"],"image":document["approved_image"],
                    "imageCandidates":document["image_candidates"],"lastCheckedAt":observed,"lastChangedAt":observed,
                    "firstCheckedAt":old.get("firstCheckedAt",observed) if old else observed,
                    "lastVerifiedAt":parent.get("lastVerifiedAt"),"scheduleOrigin":"automatic"}
                return row,True
            store.mutate("pickupSchedules/"+pickup_id,child_pickup)
        # Changed text removing a target must stop its old automatic calendar/push.
        for child_id,old in store.list_equal("gameEvents","parentEventId",event_id,50):
            if child_id not in active_children:
                retire_derived_event(store,child_id,old,observed)
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
             "knownEventsForReverify":0,"knownEventsQueryLimitReached":False,"olderUnseenSkipped":0,"ongoingOlderIncluded":0,
             "verifiedSectionSchedules":0,"derivedChildrenQueryLimitReached":False}
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
                ongoing_old=date<start and confirmed_window_overlap(document,source,start,end)
                if start<=date<end or ongoing_old or mode=="reverify" and event_identity(document,source) in known_event_ids:
                    if ongoing_old:metrics["ongoingOlderIncluded"]+=1
                    metrics[upsert_notice(store,source,document,observed)]+=1
                    stored=store.get("gameEvents/"+event_identity(document,source))
                    if stored and stored.get("verificationStatus")=="verified" and stored.get("startsAt") and stored.get("endsAt"):
                        metrics["verifiedSchedules"]+=1
                    if stored and stored.get("derivedSectionCount"):
                        children=store.list_equal("gameEvents","parentEventId",stored["id"],50)
                        metrics["derivedChildrenQueryLimitReached"]=metrics["derivedChildrenQueryLimitReached"] or len(children)==50
                        verified_children=[row for _,row in children if row.get("verificationStatus")=="verified" and row.get("startsAt") and row.get("endsAt") and row.get("status")!="cancelled"]
                        metrics["verifiedSchedules"]+=len(verified_children)
                        metrics["verifiedSectionSchedules"]+=sum(bool(row.get("sourceSectionKey")) for row in verified_children)
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
            "results":results,"verifiedEventSchedules":sum(row.get("metrics",{}).get("verifiedSchedules",0) for row in results),"fullSourceCoverageVerified":False,
            "extractionVersion":EXTRACTION_VERSION,"imageReuseRequiresSeparateReview":True}


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
