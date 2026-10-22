"""Firestore Spark publisher worker with reviewed explicit schedule extraction.

Default command is a local plan with zero external requests and zero remote writes.
--live fetches reviewed official sources; --apply additionally requires Admin safety
checks and pre-enabled Firestore sources. Retained Supabase workers are inactive.
"""
from __future__ import annotations
import argparse
from contextlib import nullcontext
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import html
from html.parser import HTMLParser
import ipaddress
import json
import os
from pathlib import Path
import re
import sys
import time
import uuid
import unicodedata
import xml.etree.ElementTree as ET
from zoneinfo import ZoneInfo
from urllib.parse import urlsplit, parse_qsl, urlencode, urljoin

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "collector"))
from engine import fetch_source, ValidationError, normalize_url, image_dimensions
from official_adapter import steam_request, parse_steam_documents, aware_timestamp, TYPES
from backfill import retry_after_seconds
from store import MemoryStore, FirestoreStore, PublicNewsCreateOnlyStore, SafetyError, ProviderQuotaUnavailable, is_provider_quota_error, timestamp, document_fingerprint, cleanup_path, CONTENT_CLEANUP_COLLECTIONS

UTC = timezone.utc
DATETIME_TYPE = datetime
EXTRACTION_VERSION = 12
CONTENT_PUBLISHED_NOT_BEFORE = "2026-10-01T00:00:00+09:00"
CONTENT_CUTOFF = timestamp(CONTENT_PUBLISHED_NOT_BEFORE)
CONTENT_PUBLISHED_BEFORE = None  # Continuous official news after the reviewed lower bound.
CONTENT_MONTH_END = timestamp("2026-11-01T00:00:00+09:00")  # Legacy October-only review scripts.
YOUTUBE_CHANNEL_ID = re.compile(r"UC[A-Za-z0-9_-]{22}")
YOUTUBE_VIDEO_ID = re.compile(r"[A-Za-z0-9_-]{11}")
YOUTUBE_FEED_PATHS = ("/feeds/videos.xml", "/xml/feeds/videos.xml")
YOUTUBE_FEED_DOCUMENTATION = "https://developers.google.com/youtube/v3/guides/push_notifications"
YOUTUBE_API_TERMS = "https://developers.google.com/youtube/terms/api-services-terms-of-service"
YOUTUBE_PARSERS = frozenset({"youtube_atom","youtube_data_api"})
OFFICIAL_CMS_PARSERS = frozenset({"kuro_cms", "genshin_cms"})
OFFICIAL_SITE_PARSERS = frozenset({"official_html","official_rss", *OFFICIAL_CMS_PARSERS})
YOUTUBE_NOTICE_IMAGE_POLICY_VERSION = 4
CONTENT_CLASSIFICATION_VERSION = 1
NEWS_ONLY_KINDS = frozenset({"character-pv", "event-pv"})
CALENDAR_PREVIEW_KINDS = frozenset({"broadcast-preview", "offline-preview", "pickup-preview", "event-preview"})
YOUTUBE_ADMITTED_NEWS_KINDS = NEWS_ONLY_KINDS | CALENDAR_PREVIEW_KINDS | frozenset({"goods-preview", "collaboration-preview", "game-notice"})

# This allow list describes a post, not everything mentioned in its boilerplate
# description. It is shared by new ingestion and the public-document migration.
CONTENT_EXCLUDED_TITLE = re.compile(r"\b(?:OST|soundtrack|music\s*video|song|cover|shorts|guide|tutorial|how\s*to|recap|highlights?)\b|#shorts|다시\s*보기|하이라이트|공략|가이드|쇼츠|음악(?!회)|커버|歌ってみた|攻略|解説|切り抜き", re.I)
CONTENT_CHARACTER_PV = re.compile(r"(?:캐릭터|인형|에이전트|ニケ|キャラクター|人形|エージェント|character|doll|agent)\s*(?:[^\n|｜:：]{0,60}?\s*)?(?:(?<![A-Za-z])PV(?![A-Za-z])|프로모션\s*비디오|promotion\s*video|demo|trailer|teaser)|\b(?:PV|trailer|teaser)\b\s*(?:[-|:：]\s*)?(?:캐릭터|인형|キャラクター|character|doll)", re.I)
CONTENT_EVENT_PV = re.compile(r"(?:이벤트|イベント|event)\s*(?:[^\n|｜:：]{0,60}?\s*)?(?:(?<![A-Za-z])PV(?![A-Za-z])|프로모션\s*비디오|promotion\s*video|trailer|teaser)|\b(?:PV|trailer|teaser)\b\s*(?:[-|:：]\s*)?(?:이벤트|イベント|event)", re.I)
CONTENT_PREVIEW = re.compile(r"예고|안내|공지|사전\s*안내|소식|공개|시작|개최|실시|신규|판매|예약|예매|오픈|\b(?:new|preview|announcement|notice|coming|announced|starting|available|opens?|begins?|launch|sale|release)\b|告知|予告|開催|開催決定|新規|新登場|販売|予約", re.I)
CONTENT_BROADCAST = re.compile(r"특별\s*방송|공식\s*(?:생)?방송|개발자\s*방송|생방송|\b(?:special\s*(?:program|broadcast)|live\s*stream|livestream|developer\s*(?:stream|broadcast))\b|公式\s*生放送|特別番組|生放送", re.I)
CONTENT_OFFLINE = re.compile(r"오프라인|콜라보\s*카페|팝업\s*스토어|전시회|콘서트|음악회|오케스트라|페스티벌|팬미팅|\b(?:offline|collab(?:oration)?\s*caf[eé]|pop[- ]?up|exhibition|concert|orchestra|festival|fan\s*meeting)\b|コラボカフェ|ポップアップ|展覧会|コンサート|オフライン|フェスティバル", re.I)
CONTENT_PICKUP = re.compile(r"픽업|모집|지정\s*발주|한정\s*뽑기|이벤트\s*기원|」\s*기원|캐릭터\s*이벤트\s*튜닝|\b(?:pick[- ]?up|banner|event\s*wishes|recruitment|targeted\s*procurement|military\s*upgrade)\b|ピックアップ|募集|召集|おいのり", re.I)
CONTENT_EVENT = re.compile(r"이벤트|\b(?:limited[- ]time\s*event|time[- ]limited\s*event|event)\b|イベント", re.I)
CONTENT_GOODS = re.compile(r"굿즈|상품\s*예약|예약\s*판매|재입고|\b(?:merchandise|merch|goods|pre[- ]?orders?|restock)\b|グッズ|予約販売|再入荷", re.I)
CONTENT_COLLABORATION = re.compile(r"콜라보|컬래버레이션|협업|\b(?:collab(?:oration)?|crossover)\b|コラボ", re.I)
CONTENT_GAME_NOTICE = re.compile(r"업데이트|점검|패치|사전\s*예약|서비스\s*종료|출시|접속\s*보상|출석|\b(?:update|maintenance|patch|pre[- ]?registration|service\s*(?:end|closure)|launch|login\s*(?:bonus|reward))\b|アップデート|メンテナンス|事前登録|サービス終了|ログインボーナス", re.I)


def classify_notice_content(row):
    """Classify only source-bound titles/reviewed roles, never body keywords.

    A character/event PV is news-only even if the description repeats a game's
    period. Music, guides, recaps and unrelated uploads are not game news here.
    A source-bound main-role review can classify a named scoped publisher block.
    """
    selected = row.get("displayTitle") or row.get("sourceSectionTitle") or row.get("originalTitle") or row.get("title") or ""
    title = plain_notice(selected) if isinstance(selected, str) else ""
    raw_hash = row.get("rawSha256")
    kind, status, method, evidence = "other", "unconfirmed", "explicit_notice_title_v1", title
    # An OST concert announcement is an offline event, not an OST upload.
    exclusion_title = re.sub(r"\b(?:OST|soundtrack)\b", "", title, flags=re.I) if CONTENT_OFFLINE.search(title) and CONTENT_PREVIEW.search(title) else title
    if CONTENT_EXCLUDED_TITLE.search(exclusion_title):
        status = "excluded"
    else:
        character, event = bool(CONTENT_CHARACTER_PV.search(title)), bool(CONTENT_EVENT_PV.search(title))
        if character != event:
            kind, status = ("character-pv" if character else "event-pv"), "verified"
        elif not character:
            if CONTENT_BROADCAST.search(title):
                kind, status = "broadcast-preview", "verified"
            elif CONTENT_PICKUP.search(title):
                # A named procurement/banner heading is itself a pickup notice;
                # a rerun does not need the word "new" to remain valid news.
                kind, status = "pickup-preview", "verified"
            elif CONTENT_PREVIEW.search(title) or CONTENT_GAME_NOTICE.search(title):
                for candidate, pattern in (("broadcast-preview", CONTENT_BROADCAST), ("offline-preview", CONTENT_OFFLINE), ("pickup-preview", CONTENT_PICKUP), ("goods-preview", CONTENT_GOODS), ("collaboration-preview", CONTENT_COLLABORATION), ("event-preview", CONTENT_EVENT), ("game-notice", CONTENT_GAME_NOTICE)):
                    if pattern.search(title):
                        kind, status = candidate, "verified"
                        break
            if kind == "other":
                proof = row.get("calendarRoleEvidence") or {}
                role = row.get("calendarRole")
                if (isinstance(proof, dict) and proof.get("status") == "verified" and proof.get("sourceSha256") == raw_hash
                    and proof.get("sourceTitle") == selected and proof.get("role") == role and role in ("main-character", "main-event")
                    and safe_public_url(proof.get("evidenceUrl"))):
                    kind, status, method = ("pickup-preview" if role == "main-character" else "event-preview"), "verified", "reviewed_main_role_v1"
                    evidence = proof.get("evidenceText") or title
    if not isinstance(raw_hash, str) or not re.fullmatch(r"[a-f0-9]{64}", raw_hash):
        kind, status = "other", "unconfirmed"
    return {"version": CONTENT_CLASSIFICATION_VERSION, "status": status, "method": method, "kind": kind,
        "sourceTitle": selected, "sourceSha256": raw_hash, "evidenceText": evidence[:1500]}


def notice_has_representative_image(row, observed=None):
    """The exact current source image is required; game covers never substitute."""
    for key in ("noticeImage", "image"):
        image = row.get(key)
        if not isinstance(image, dict) or image.get("subjectStatus") not in (None, "MATCHED") or not safe_public_url(image.get("url")) or GENERIC_GAME_IMAGE.search(image["url"]):
            continue
        proof, reference = image.get("subjectEvidence") or {}, image.get("sourceReference") or {}
        if (not isinstance(proof, dict) or proof.get("status") != "MATCHED" or proof.get("eventId") != row.get("id")
            or proof.get("officialUrl") != row.get("officialUrl") or image.get("sourceUrl") != row.get("officialUrl")):
            continue
        selected = row.get("displayTitle") or row.get("sourceSectionTitle") or row.get("originalTitle") or row.get("title")
        override = row.get("sourceImageOverride") or {}
        exact_subject = (proof.get("sourceSha256") == row.get("rawSha256") and proof.get("sourceTitle") == selected)
        exact_override = (isinstance(override, dict) and override.get("rawSha256") == row.get("rawSha256") and override.get("exactTitle") == selected
            and override.get("officialUrl") == row.get("officialUrl") and override.get("allowedImageUrl") == image.get("url"))
        if image.get("rightsStatus") == "APPROVED" and safe_public_url(image.get("rightsEvidenceUrl")) and exact_subject:
            return True
        if (not isinstance(reference, dict) or reference.get("mode") != "OFFICIAL_NOTICE_IMAGE"
            or reference.get("allowedImageUrl") != image.get("url") or not safe_public_url(reference.get("sourceUrl"))):
            continue
        if proof.get("method") == "source_post":
            verification = image.get("imageVerification") or {}
            if (row.get("sourceProvider") != "youtube" or proof.get("rawSha256") != row.get("rawSha256")
                or proof.get("sourceTitle") != row.get("originalTitle") or proof.get("videoId") != row.get("sourceVideoId")
                or proof.get("channelId") != row.get("sourceChannelId") or not isinstance(verification, dict)
                or verification.get("status") != "verified" or verification.get("method") != "SOURCE_DIMENSIONS_MATCH"):
                continue
            video,channel=row.get("sourceVideoId"),row.get("sourceChannelId")
            if (not isinstance(video,str)or not YOUTUBE_VIDEO_ID.fullmatch(video)or not isinstance(channel,str)or not YOUTUBE_CHANNEL_ID.fullmatch(channel)
                or row.get("officialUrl")!="https://www.youtube.com/watch?"+urlencode({"v":video})):continue
            if not youtube_notice_thumbnail_url(image.get("url"),video):continue
            if (type(verification.get("width")) is not int or type(verification.get("height")) is not int
                or verification["width"] < 320 or verification["height"] < 180
                or verification.get("width") != verification.get("expectedWidth") or verification.get("height") != verification.get("expectedHeight")
                or not re.fullmatch(r"[a-f0-9]{64}", str(verification.get("contentSha256", "")))):
                continue
            try:
                checked,expires=timestamp(row.get("metadataCheckedAt")),timestamp(row.get("metadataExpiresAt"))
                if expires!=checked+timedelta(days=30) or timestamp(reference.get("expiresAt"))!=expires or timestamp(reference.get("checkedAt"))!=checked:continue
                if observed is not None and (timestamp(reference.get("expiresAt")) <= timestamp(observed)
                    or timestamp(reference.get("checkedAt")) > timestamp(observed)):
                    continue
            except (ValueError, TypeError):
                continue
            return True
        if proof.get("method") in ("reviewed_image", "reviewed_notice_reference", "source_alt", "source_section"):
            if not (exact_subject or exact_override):continue
            if image.get("kind") not in ("PICKUP_ART", "EVENT_ART", "NOTICE_ART") or proof.get("kind") not in ("pickup", "event", "notice"):continue
            try:
                checked=timestamp(reference.get("checkedAt"))
                if observed is not None and checked>timestamp(observed):continue
                if reference.get("expiresAt")and observed is not None and timestamp(reference["expiresAt"])<=timestamp(observed):continue
            except (ValueError,TypeError):continue
            return True
    return False


def verified_calendar_day_period(row, observed=None):
    """An older publisher notice may establish an October calendar period.

    This does not admit the older article into news or invent game timestamps.
    Only a separately reviewed main role, exact source-bound period and image
    can survive the public-news cutoff. These rows are never clock-based push
    schedules merely because their calendar dates have been reviewed.
    """
    proof=row.get("calendarPeriod") or {}
    role=row.get("calendarRoleEvidence") or {}
    selected=row.get("displayTitle") or row.get("sourceSectionTitle") or row.get("originalTitle")
    if (not isinstance(proof,dict)or proof.get("version")!=1 or proof.get("mode")!="official_day_range_v1"
        or proof.get("status")!="verified" or proof.get("precision")!="day"
        or proof.get("sourceUrl")!=row.get("officialUrl") or not safe_public_url(proof.get("sourceUrl"))
        or not re.fullmatch(r"[a-f0-9]{64}",str(row.get("rawSha256","")))
        or proof.get("sourceSha256")!=row["rawSha256"] or proof.get("sourceTitle")!=selected
        or not isinstance(proof.get("evidenceText"),str)or not proof["evidenceText"].strip()
        or row.get("calendarRole")not in("main-character","main-event")or not isinstance(role,dict)
        or role.get("status")!="verified"or role.get("sourceSha256")!=row["rawSha256"]
        or role.get("sourceTitle")!=selected or role.get("evidenceUrl")!=row.get("officialUrl")
        or row.get("status")in("cancelled","postponed","rejected")):
        return False
    try:
        start,end=(datetime.strptime(proof[key],"%Y-%m-%d").date().isoformat()for key in("startsOn","endsOn"))
        if start!=proof["startsOn"]or end!=proof["endsOn"]or start>end or end<"2026-10-01"or start>="2026-11-01":return False
        clock=timestamp(observed or datetime.now(UTC))
        if timestamp(proof.get("checkedAt"))>clock or timestamp(role.get("checkedAt"))>clock:return False
    except(ValueError,TypeError,KeyError):return False
    auxiliary=[proof.get("auxiliarySourceUrl"),proof.get("auxiliarySourceSha256"),proof.get("auxiliaryEvidenceText")]
    if proof.get("startsOnBasis")not in(None,"version-update-day"):return False
    if proof.get("startsOnBasis")=="version-update-day"or any(value is not None for value in auxiliary):
        if not safe_public_url(auxiliary[0])or not re.fullmatch(r"[a-f0-9]{64}",str(auxiliary[1]))or not isinstance(auxiliary[2],str)or not auxiliary[2].strip():return False
    if row["calendarRole"]=="main-character" and (row.get("announcementKind")!="new"or not row.get("characterNames")or not row.get("rarityLabel")):return False
    return notice_has_representative_image(row,observed)


def apply_notice_content_policy(row, observed=None, *, source=None):
    """Idempotent public-only migration/ingestion policy; never changes dates."""
    if source is not None and reviewed(source):
        document = {"raw_sha256": row.get("rawSha256"), "original_title": row.get("originalTitle") or row.get("title"),
            "official_url": row.get("officialUrl"), "_section_title": row.get("displayTitle") or row.get("sourceSectionTitle"),
            "_raw_character_names": row.get("characterNames", []), "event_type_candidates": row.get("eventTypeCandidates", [])}
        regenerated = reviewed_notice_image(document, source, row.get("id"))
        if regenerated is not None:
            row = {**row, "image": regenerated}
    classification = classify_notice_content(row)
    image = notice_has_representative_image(row, observed)
    published = content_publication_status(row.get("publishedAt"), observed) == "INCLUDED"
    news = classification["status"] == "verified" and classification["kind"] != "other" and image and published
    calendar = False
    global_broadcast = verified_youtube_broadcast_point(row)
    if news and classification["kind"] in CALENDAR_PREVIEW_KINDS and row.get("verificationStatus") == "verified" and row.get("scheduleStatus") == "DATE_VERIFIED" and (row.get("serverRegion") in ("KR", "JP", "GLOBAL") or global_broadcast):
        try:
            starts, ends = timestamp(row.get("startsAt")), timestamp(row.get("endsAt"))
            calendar = ends > starts or ends == starts and row.get("scheduleKind") == "point" and classification["kind"] in ("broadcast-preview", "offline-preview")
        except (ValueError, TypeError):
            pass
    if classification["kind"]=="pickup-preview" and (row.get("announcementKind")in("rerun","aggregate")or row.get("pickupKind")=="weapon"
        or row.get("derivedCharacterAnnouncements",0)>0 or row.get("derivedSectionCount",0)>0):
        calendar=False
    source_clock = news and verified_pending_main_role_clock(row, classification)
    return {**row, "contentKind": classification["kind"], "contentClassification": classification, "newsEligible": news, "calendarEligible": bool(calendar or source_clock or verified_calendar_day_period(row,observed)), "sourceClockOnly": bool(source_clock)}


def verified_pending_main_role_clock(row, classification=None):
    classification = classification or classify_notice_content(row)
    evidence = row.get("scheduleEvidence") or {}
    role = row.get("calendarRoleEvidence") or {}
    selected = row.get("displayTitle") or row.get("sourceSectionTitle") or row.get("originalTitle") or row.get("title")
    if (classification["kind"] not in ("pickup-preview", "event-preview") or row.get("verificationStatus") != "pending"
        or row.get("scheduleStatus") not in ("DATE_UNCONFIRMED", "SERVER_REGION_UNCONFIRMED")
        or not isinstance(evidence, dict) or evidence.get("reasonCodes") != ["SERVER_REGION_UNCONFIRMED"]
        or evidence.get("method") != "explicit_labelled_timestamps" or not isinstance(evidence.get("evidenceText"), str)
        or not isinstance(role, dict) or role.get("status") != "verified" or role.get("sourceSha256") != row.get("rawSha256")
        or role.get("sourceTitle") != selected or role.get("role") != row.get("calendarRole")
        or row.get("calendarRole") not in ("main-character", "main-event") or not safe_public_url(role.get("evidenceUrl"))):
        return False
    parsed = extract_schedule(evidence["evidenceText"])
    if parsed["reasonCodes"]:return False
    try:
        return (timestamp(row.get("candidateStartsAt")) == parsed["startsAt"] and timestamp(row.get("candidateEndsAt")) == parsed["endsAt"]
            and row.get("candidateTimezone") == parsed["originalTimezone"])
    except (ValueError, TypeError):
        return False


def verified_youtube_broadcast_point(row):
    proof = row.get("scheduleEvidence") or {}
    selected = row.get("displayTitle") or row.get("sourceSectionTitle") or row.get("originalTitle") or row.get("title")
    video,channel=row.get("sourceVideoId"),row.get("sourceChannelId")
    if (row.get("sourceProvider") != "youtube" or not isinstance(video,str)or not YOUTUBE_VIDEO_ID.fullmatch(video)
        or not isinstance(channel,str)or not YOUTUBE_CHANNEL_ID.fullmatch(channel)
        or row.get("officialUrl")!="https://www.youtube.com/watch?"+urlencode({"v":video})
        or classify_notice_content(row)["kind"] != "broadcast-preview" or row.get("scheduleKind") != "point"
        or row.get("calendarAudience") != "GLOBAL_ANNOUNCEMENT" or not isinstance(proof, dict)
        or proof.get("method") != "youtube_live_details_v1" or proof.get("sourceSha256") != row.get("rawSha256")
        or proof.get("sourceTitle") != selected or proof.get("officialUrl") != row.get("officialUrl")
        or proof.get("videoId") != row.get("sourceVideoId") or proof.get("channelId") != row.get("sourceChannelId")
        or proof.get("providerField") not in ("liveStreamingDetails.actualStartTime", "liveStreamingDetails.scheduledStartTime")):
        return False
    try:
        return timestamp(proof.get("providerValue")) == timestamp(row.get("startsAt")) == timestamp(row.get("endsAt"))
    except (ValueError, TypeError):
        return False


def content_publication_status(value, observed=None):
    """The publication date, never a recent edit/schedule, defines retained news."""
    if isinstance(value, DATETIME_TYPE) and value.tzinfo is None: return "PUBLICATION_DATE_UNCONFIRMED"
    try: published = timestamp(value)
    except (ValueError, TypeError): return "PUBLICATION_DATE_UNCONFIRMED"
    if published < CONTENT_CUTOFF: return "PUBLISHED_BEFORE_CONTENT_CUTOFF"
    if observed is not None and published > timestamp(observed): return "PUBLICATION_DATE_IN_FUTURE"
    return "INCLUDED"


def youtube_notice_thumbnail_url(value, video):
    """Only an exact supplied video thumbnail and its public cache version."""
    url=safe_public_url(value)
    if not url or url!=value or not isinstance(video,str) or not YOUTUBE_VIDEO_ID.fullmatch(video):return None
    parsed=urlsplit(value)
    if (parsed.hostname not in ("i.ytimg.com","i1.ytimg.com","i2.ytimg.com","i3.ytimg.com","i4.ytimg.com")
        or parsed.fragment or parsed.query and not re.fullmatch(r"v=[A-Fa-f0-9]{8}",parsed.query)
        or not re.fullmatch(r"/vi(?:_webp)?/"+re.escape(video)+r"/(?:default|mqdefault|hqdefault|sddefault|maxresdefault|fhddefault|qhddefault|uhddefault)(?:_live)?\.(?:jpg|webp)",parsed.path)):return None
    return value


def automatic_youtube_notice_image(document, source, event_id, observed, *, image_fetch=None, deadline=None, probe_status=None):
    """Keep the exact official video's supplied thumbnail as notice evidence.

    A bounded in-memory image header/body check confirms supplied dimensions.
    No byte storage, reuse-rights grant or pickup inference.
    Derived characters/sections never inherit an article-level thumbnail.
    """
    if source.get("parser") not in YOUTUBE_PARSERS or not reviewed(source) or document.get("_section_key") or document.get("_parent_event_id"): return None
    raw = document.get("raw_json") or {}
    video = document.get("external_key")
    channel = source.get("youtube_channel_id")
    official = "https://www.youtube.com/watch?" + urlencode({"v": video}) if isinstance(video, str) else None
    if (not isinstance(raw, dict) or not isinstance(video, str) or not YOUTUBE_VIDEO_ID.fullmatch(video)
        or raw.get("videoId") != video or raw.get("channelId") != channel or raw.get("title") != document.get("original_title")
        or raw.get("officialUrl") != official or document.get("official_url") != official
        or digest(raw) != document.get("raw_sha256")): return None
    supplied = raw.get("thumbnailUrls")
    if not isinstance(supplied, list) or not 1 <= len(supplied) <= 8: return None
    probe_status=probe_status if probe_status is not None else {}
    probe_status.update(status="unconfirmed",requests=0,reasonCodes=[],policyVersion=YOUTUBE_NOTICE_IMAGE_POLICY_VERSION,checkedAt=timestamp(observed).isoformat())
    candidates = []
    quality = {"maxresdefault":0,"sddefault":1,"hqdefault":2,"mqdefault":3,"default":4,"fhddefault":5,"qhddefault":6,"uhddefault":7}
    metadata=document.get("source_image_metadata")
    if not isinstance(metadata,dict)or raw.get("thumbnailMetadata")!=metadata:
        probe_status["reasonCodes"].append("SOURCE_DIMENSIONS_UNCONFIRMED");return None
    for value in supplied:
        url = youtube_notice_thumbnail_url(value,video)
        if not url or url not in document.get("source_image_urls", []): continue
        parsed = urlsplit(url)
        match = re.fullmatch(r"/vi(?:_webp)?/" + re.escape(video) + r"/(default|mqdefault|hqdefault|sddefault|maxresdefault|fhddefault|qhddefault|uhddefault)(?:_live)?\.(?:jpg|webp)", parsed.path)
        if not match: continue
        expected=metadata.get(url)
        if (not isinstance(expected,dict)or not isinstance(expected.get("width"),int)or isinstance(expected["width"],bool)
            or not isinstance(expected.get("height"),int)or isinstance(expected["height"],bool)
            or not 320<=expected["width"]<=16384 or not 180<=expected["height"]<=16384):continue
        if any(candidate[1]==url for candidate in candidates):continue
        candidates.append((quality[match.group(1)], url,expected))
    if not candidates:
        probe_status["reasonCodes"].append("NO_ELIGIBLE_SOURCE_THUMBNAIL");return None
    if image_fetch is None:
        probe_status.update(status="deferred",reasonCodes=["IMAGE_PROBE_UNAVAILABLE"]);return None
    selected=None
    for _,url,expected in sorted(candidates)[:3]:
        if deadline is not None and time.monotonic()>=deadline:
            probe_status["reasonCodes"].append("IMAGE_PROBE_DEADLINE_EXHAUSTED");break
        probe_status["requests"]+=1
        request={"url":url,"official_host":urlsplit(url).hostname,"parser":"official_image_probe","enabled":True,"access_status":"approved",
            "terms_url":YOUTUBE_API_TERMS,"terms_checked_at":source["terms_checked_at"],"request_timeout_seconds":10,
            "max_response_bytes":2097152}
        if deadline is not None:request["request_timeout_seconds"]=max(1,min(10,int(deadline-time.monotonic())))
        try:http_status,pixels,headers=image_fetch(request)
        except Exception:
            probe_status["reasonCodes"].append("IMAGE_PROBE_FETCH_FAILED");continue
        content_type=str(headers.get("content-type","")).split(";",1)[0].strip().lower()
        if http_status!=200 or content_type not in ("image/jpeg","image/png")or not isinstance(pixels,bytes)or len(pixels)>2097152:
            probe_status["reasonCodes"].append("IMAGE_PROBE_HTTP_OR_CONTENT_TYPE_INVALID");continue
        actual=image_dimensions(pixels)
        if actual!=(expected["width"],expected["height"])or actual[0]<320 or actual[1]<180:
            probe_status["reasonCodes"].append("IMAGE_PROBE_DIMENSIONS_MISMATCH");continue
        verification={"status":"verified","method":"SOURCE_DIMENSIONS_MATCH","width":actual[0],"height":actual[1],
            "expectedWidth":expected["width"],"expectedHeight":expected["height"],"contentType":content_type,
            "contentSha256":hashlib.sha256(pixels).hexdigest(),"checkedAt":timestamp(observed).isoformat()}
        selected=(url,verification);break
    probe_status["reasonCodes"]=list(dict.fromkeys(probe_status["reasonCodes"]))
    if selected is None:
        if "IMAGE_PROBE_DEADLINE_EXHAUSTED"in probe_status["reasonCodes"]:probe_status["status"]="deferred"
        return None
    url,verification=selected;probe_status.update(status="verified",selectedUrl=url)
    return {"url": url, "sourceUrl": official, "kind": "NOTICE_ART", "rightsStatus": "UNCONFIRMED", "subjectStatus": "MATCHED",
        "imageVerification":verification,
        "sourceReference": {"mode": "OFFICIAL_NOTICE_IMAGE", "sourceUrl": official, "checkedAt": timestamp(observed).isoformat(), "allowedImageUrl": url,
            "expiresAt": (timestamp(observed)+timedelta(days=30)).isoformat()},
        "subjectEvidence": {"status": "MATCHED", "eventId": event_id, "officialUrl": official, "kind": "notice", "method": "source_post",
            "evidenceText": document["original_title"], "characterNames": [], "provider": "youtube", "channelId": channel,
            "videoId": video, "rawSha256": document["raw_sha256"], "sourceTitle": document["original_title"]}}


def youtube_document_metadata(document,source,observed):
    fields={"sourceProvider":None,"sourceChannelId":None,"sourceVideoId":None,"metadataCheckedAt":None,"metadataExpiresAt":None}
    if source.get("parser")in OFFICIAL_SITE_PARSERS|{"steam_news"} and reviewed(source):
        fields.update(sourceProvider="official-site",publicationPrecision=document.get("publication_precision")or"timestamp",publicationLocalDate=document.get("publication_local_date"))
        if document.get("announced_local_date"):fields["announcedLocalDate"]=document["announced_local_date"]
    if source.get("parser")not in YOUTUBE_PARSERS or not reviewed(source) or document.get("_section_key") or document.get("_parent_event_id"):return fields
    raw=document.get("raw_json") or {};video=document.get("external_key");channel=source.get("youtube_channel_id")
    if (not isinstance(raw,dict) or not isinstance(video,str) or not YOUTUBE_VIDEO_ID.fullmatch(video) or raw.get("videoId")!=video
        or raw.get("channelId")!=channel or raw.get("title")!=document.get("original_title") or digest(raw)!=document.get("raw_sha256")
        or raw.get("officialUrl")!=document.get("official_url") or document.get("official_url")!="https://www.youtube.com/watch?"+urlencode({"v":video})):return fields
    return {"sourceProvider":"youtube","sourceChannelId":channel,"sourceVideoId":video,"metadataCheckedAt":observed,"metadataExpiresAt":observed+timedelta(days=30)}


def automatic_official_notice_image(document,source,event_id,observed,image_fetch=None,deadline=None):
    """Exact article body art from a reviewed template/feed; no cover fallback."""
    if source.get("parser")not in OFFICIAL_SITE_PARSERS|{"steam_news"} or not reviewed(source)or image_fetch is None:return None
    raw=document.get("raw_json")or{}
    if source.get("parser")=="steam_news":
        if document.get("_section_key")or document.get("_parent_event_id"):return None
        from official_adapter import steam_notice_image_urls,STEAM_CLAN_IMAGE_HOST
        candidates=steam_notice_image_urls(document,source)
        allowed_hosts=[STEAM_CLAN_IMAGE_HOST]
    else:
        if digest(raw)!=document.get("raw_sha256")or raw.get("officialUrl")!=document.get("official_url")or raw.get("title")!=document.get("original_title"):return None
        candidates=[url for url in document.get("official_article_image_urls",[])if url in raw.get("imageUrls",[])]
        allowed_hosts=source["article_format"]["allowedImageHosts"]
    for candidate in candidates[:3]:
        url=safe_public_url(candidate)
        if not url or url!=candidate or urlsplit(url).hostname not in allowed_hosts or GENERIC_GAME_IMAGE.search(url):continue
        if deadline is not None and time.monotonic()>=deadline:return None
        try:
            status,pixels,headers=image_fetch({"url":url,"official_host":urlsplit(url).hostname,"parser":"official_image_probe","enabled":True,"access_status":"approved",
                "terms_url":source["terms_url"],"terms_checked_at":source["terms_checked_at"],"request_timeout_seconds":10,"max_response_bytes":2097152})
            content_type=str(headers.get("content-type","")).split(";",1)[0].strip().lower()
            dimensions=image_dimensions(pixels)if status==200 and isinstance(pixels,bytes)and len(pixels)<=2097152 and content_type in("image/jpeg","image/png","image/webp")else None
            if not dimensions or not 320<=dimensions[0]<=16384 or not 180<=dimensions[1]<=16384:continue
        except Exception:continue
        return {"url":url,"sourceUrl":document["official_url"],"kind":"NOTICE_ART","rightsStatus":"UNCONFIRMED","subjectStatus":"MATCHED",
            "imageVerification":{"status":"verified","method":"SOURCE_IMAGE_DIMENSIONS","width":dimensions[0],"height":dimensions[1],"contentType":content_type,"contentSha256":hashlib.sha256(pixels).hexdigest(),"checkedAt":timestamp(observed).isoformat()},
            "sourceReference":{"mode":"OFFICIAL_NOTICE_IMAGE","sourceUrl":document["official_url"],"checkedAt":timestamp(observed).isoformat(),"allowedImageUrl":url},
            "subjectEvidence":{"status":"MATCHED","eventId":event_id,"officialUrl":document["official_url"],"kind":"notice","method":"source_section",
                "sourceSha256":document["raw_sha256"],"sourceTitle":document["original_title"],"evidenceText":"Representative image inside the exact official article body","characterNames":[]}}
    return None


def youtube_request(source):
    """Only YouTube's channel feed is supported; no video-page scraper/API key."""
    channel = source.get("youtube_channel_id", "")
    if source.get("parser") != "youtube_atom" or not isinstance(channel, str) or not YOUTUBE_CHANNEL_ID.fullmatch(channel):
        raise ValidationError("youtube_channel_identity_required")
    url = safe_public_url(source.get("url"))
    if not url:
        raise ValidationError("youtube_public_feed_required")
    parsed = urlsplit(source["url"])
    if parsed.hostname != "www.youtube.com" or parsed.path not in YOUTUBE_FEED_PATHS or parsed.fragment or parse_qsl(parsed.query,keep_blank_values=True) != [("channel_id", channel)]:
        raise ValidationError("youtube_public_feed_required")
    return {**source, "enabled": True, "method": "official_feed", "official_host": "www.youtube.com"}


def parse_youtube_documents(raw, source, observed_at):
    """A publisher-verified channel must match every Atom entry.

    Publication/update times remain notice metadata. Recent feeds are bounded and
    not a historical/archive API, so no complete 72-hour coverage is asserted.
    Thumbnails are source evidence only until a separate reuse review approves them.
    """
    youtube_request(source)
    if len(raw) > 2097152 or b"\x00" in raw or re.search(br"<!\s*(?:DOCTYPE|ENTITY)\b", raw, re.I):
        raise ValidationError("youtube_xml_unsafe")
    try:
        feed = ET.fromstring(raw.decode("utf-8-sig"))
    except (ET.ParseError, UnicodeError) as error:
        raise ValidationError("youtube_xml_invalid") from error
    atom, yt, media = "{http://www.w3.org/2005/Atom}", "{http://www.youtube.com/xml/schemas/2015}", "{http://search.yahoo.com/mrss/}"
    channel = source["youtube_channel_id"]
    # Actual YouTube feeds omit the UC prefix in the root header, while entry
    # channelId stays the full stable ID. Accept only those exact two forms.
    header_ids = (channel, channel[2:])
    if feed.tag != atom+"feed" or feed.findtext(atom+"id") not in tuple("yt:channel:"+value for value in header_ids):
        raise ValidationError("youtube_response_channel_mismatch")
    if feed.findtext(yt+"channelId") not in (None, *header_ids):
        raise ValidationError("youtube_response_channel_mismatch")
    items = feed.findall(atom+"entry")
    if len(items) > 50:
        raise ValidationError("youtube_items_invalid")
    documents, seen = [], set()
    for item in items:
        video = item.findtext(yt+"videoId") or ""
        if item.findtext(yt+"channelId") != channel or not YOUTUBE_VIDEO_ID.fullmatch(video) or item.findtext(atom+"id") != "yt:video:"+video or video in seen:
            raise ValidationError("youtube_entry_identity_mismatch")
        seen.add(video)
        title, content = item.findtext(atom+"title"), item.findtext(media+"group/"+media+"description") or ""
        if not isinstance(title, str) or not 1 <= len(title.strip()) <= 500 or len(content) > 200000:
            raise ValidationError("youtube_document_fields_invalid")
        content = ""  # Do not persist/import a video's detailed description.
        published = item.findtext(atom+"published")
        updated = item.findtext(atom+"updated")
        published_time = aware_timestamp(published)
        if updated and aware_timestamp(updated) < published_time:
            raise ValidationError("youtube_update_before_publication")
        links = [node.get("href", "") for node in item.findall(atom+"link") if node.get("rel") == "alternate"]
        if len(links) != 1:
            raise ValidationError("youtube_video_url_required")
        original = urlsplit(links[0])
        watch = original.path == "/watch" and parse_qsl(original.query,keep_blank_values=True) == [("v", video)]
        shorts = original.scheme == "https" and original.path == "/shorts/"+video and not original.query and original.port in (None, 443)
        if original.scheme not in ("https", "http") or original.hostname not in ("www.youtube.com", "youtube.com") or original.username or original.password or original.port not in (None, 80, 443) or original.fragment or not (watch or shorts):
            raise ValidationError("youtube_video_url_mismatch")
        url = "https://www.youtube.com/watch?"+urlencode({"v": video})
        thumbnails = []
        thumbnail_metadata={}
        for node in item.findall(media+"group/"+media+"thumbnail")[:5]:
            image = youtube_notice_thumbnail_url(node.get("url"),video)
            if image and urlsplit(image).hostname in ("i.ytimg.com", "i1.ytimg.com", "i2.ytimg.com", "i3.ytimg.com", "i4.ytimg.com"):
                thumbnails.append(image)
                try:width=int(node.get("width",""));height=int(node.get("height",""))
                except (ValueError,TypeError):continue
                if 0<width<=16384 and 0<height<=16384:thumbnail_metadata[image]={"width":width,"height":height,"quality":"feed"}
        raw_item = {"channelId":channel, "videoId":video, "title":title, "description":content,
                    "published":published, "updated":updated, "officialUrl":url, "originalVideoUrl":links[0], "thumbnailUrls":thumbnails,"thumbnailMetadata":thumbnail_metadata}
        hints = [kind for kind, patterns in TYPES.items() if any(re.search(pattern, title+"\n"+content, re.I) for pattern in patterns)]
        documents.append({"external_key":video, "raw_sha256":digest(raw_item), "raw_json":raw_item,
            "raw_publication_timestamp":published, "published_at":published_time.isoformat(), "source_updated_at":updated,
            "observed_at":observed_at, "official_url":url, "original_title":title, "original_description":content,
            "original_language":source.get("content_language") or "unknown", "server_region":source.get("server_region") or "UNSPECIFIED",
            "server_label":source.get("server_label"), "event_type_candidates":hints, "pickups":[], "source_image_urls":thumbnails,"source_image_metadata":thumbnail_metadata})
    return documents, {"items_seen":len(items), "items_accepted":len(documents), "items_skipped":0,
        "skip_reasons":[], "window_coverage_verified":False, "coverage_reason":"RECENT_FEED_WITHOUT_HISTORY_PAGINATION"}


def source_policy(source):
    """Source-specific opt-ins remain separate from permission to read a feed."""
    policy = {key: source.get(key) for key in (
        "schedule_extraction_approved", "schedule_extraction_reviewed_at", "schedule_extraction_evidence_url",
        "content_language", "server_region", "server_label", "highest_character_rarity", "image_reuse",
        "section_schedule_format", "paired_recruitment_period_approved", "content_localization", "original_image_reference",
        "reviewed_notice_references", "calendar_role_policy", "localization_roster", "game_title_filter", "article_format", "paired_ticket_notice")}
    if source.get("news_scope") is not None:policy["news_scope"]=source["news_scope"]
    if source.get("parser") in YOUTUBE_PARSERS:
        policy["youtubeNoticeImagePolicyVersion"]=YOUTUBE_NOTICE_IMAGE_POLICY_VERSION
        policy.update({key:source.get(key) for key in ("youtube_channel_id", "publisher_identity_evidence_url", "publisher_identity_checked_at")})
        if source.get("parser")=="youtube_data_api":policy.update({key:source.get(key)for key in ("youtube_uploads_playlist_id","youtube_uploads_playlist_verified_at")})
    policy["contentPublishedNotBefore"] = CONTENT_PUBLISHED_NOT_BEFORE
    policy["contentPublishedBefore"] = CONTENT_PUBLISHED_BEFORE
    policy["contentClassificationVersion"] = CONTENT_CLASSIFICATION_VERSION
    return policy


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
    if document.get("_section_key"):return []
    paired=document.get("_paired_sections",[])
    if source.get("section_schedule_format")!="heading_period_v1":return paired
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
            "_raw_character_names":chars,"source_image_urls":[]})
    return [*result,*paired]


IMAGE_TAG = re.compile(r"\[img\](https://[^\[\s]+)\[/img\]|<img\b[^>]*>",re.I)
PICKUP_IMAGE_WORDS = re.compile(r"\b(?:pickup|banner|recruitment|procurement|gacha)\b|픽업|모집|ピックアップ|募集",re.I)
GENERIC_GAME_IMAGE = re.compile(r"(?:^|[ /_.-])(?:logo|icon|key[-_ ]?visual|app[-_ ]?icon|game[-_ ]?cover)(?:$|[ /_.-])",re.I)


def image_name(value):
    return re.sub(r"\s+"," ",plain_notice(value)).strip().casefold()


def image_mentions(text, name):
    needle=image_name(name)
    return bool(needle and re.search(r"(?<!\w)"+re.escape(needle)+r"(?!\w)",image_name(text)))


def image_subject_evidence(document, source, label, nearby, image_count):
    """Only local publisher labels establish what an image depicts.

    Feed thumbnails and article-level image ordering cannot establish a pickup
    banner. A reviewed isolated heading may supply its own single poster, but
    parent images are never inherited by an unrelated section/character.
    """
    if not reviewed(source):return None
    title=document.get("_section_title") or document["original_title"]
    names=document.get("_raw_character_names",[])
    hints=document.get("event_type_candidates",[])
    pickup="pickup" in hints or document.get("_section_category")=="recruitment"
    category=document.get("_section_category")
    kind="pickup" if pickup else "event" if category in ("limited-event","login-reward","goods","offline","merchandise") else "notice"
    # An explicit alt/title is stronger than proximity. A mismatched caption
    # cannot be overridden by a nearby paragraph naming another character.
    local=(label or nearby).strip()
    method=None;proof=None
    if label and image_mentions(label,title):
        method="source_alt";proof=label
    elif not label and document.get("_section_key") and source.get("section_schedule_format")=="heading_period_v1" and image_count==1:
        method="source_section";proof=title+"\n"+nearby
    elif names and all(image_mentions(local,name) for name in names) and PICKUP_IMAGE_WORDS.search(local):
        method="source_alt" if label else "source_section";proof=local
    elif pickup and PICKUP_IMAGE_WORDS.search(local):
        targets=highest_rarity_targets(plain_notice(document["original_description"]),source)
        labelled=[target for target in targets if image_mentions(local,target["name"])]
        if len(labelled)==1:
            names=[labelled[0]["name"]];method="source_alt" if label else "source_section";proof=local
    if not method:return None
    if names and not all(image_mentions(proof,name) for name in names):return None
    return {"status":"MATCHED","eventId":event_identity(document,source),"officialUrl":document["official_url"],
        "sourceSha256":document["raw_sha256"],"sourceTitle":title,"kind":kind,"method":method,"evidenceText":proof[:1000],"characterNames":names}


def image_candidates(document, source):
    """Retain rights and subject evidence independently; never pick the first art.

    Exact-host reuse review grants only permission. Alt text or an isolated
    reviewed publisher section must additionally match this event. Public image
    URLs without semantic evidence remain candidates, never rendering fallback.
    """
    raw = document["original_description"]
    found=[]
    tags=list(IMAGE_TAG.finditer(raw))[:20]
    for match in tags:
        tag=match.group(0)
        url=match.group(1)
        if not url:
            src=re.search(r"\bsrc\s*=\s*[\"'](https://[^\"']+)[\"']",tag,re.I)
            if not src:continue
            url=src.group(1)
        labels=re.findall(r"\b(?:alt|title)\s*=\s*[\"']([^\"']*)[\"']",tag,re.I)
        before=re.split(r"\n|\[/p\]|</p>|<br\s*/?>",raw[:match.start()],flags=re.I)
        after=re.split(r"\n|\[p(?:\s[^\]]*)?\]|<p(?:\s[^>]*)?>|<br\s*/?>",raw[match.end():],flags=re.I)
        # An adjacent different image terminates local context. Never borrow a
        # character name from an earlier image in the same multi-banner post.
        preceding=before[-1] if before else ""
        following=after[0] if after else ""
        nearby=plain_notice(preceding[-400:]+"\n"+following[:400])
        nearby=re.sub(r"\[img\].*?\[/img\]","",nearby,flags=re.I)
        found.append((url,html.unescape(" ".join(labels)),nearby))
    if not document.get("_section_key"):
        found += [(url, "", "") for url in document.get("source_image_urls", [])[:5] if isinstance(url, str)]
    policy = source.get("image_reuse") or {}
    approved = policy.get("rightsStatus") == "APPROVED" and safe_public_url(policy.get("evidenceUrl")) and policy.get("checkedAt")
    candidates = []
    for value,label,nearby in found[:20]:
        url = safe_public_url(html.unescape(value))
        if not url or any(row["url"] == url for row in candidates):
            continue
        allowed = approved and urlsplit(url).hostname in policy.get("allowedHosts", [])
        evidence=None if GENERIC_GAME_IMAGE.search(url+" "+label) else image_subject_evidence(document,source,label,nearby,len(tags))
        image_kind="PICKUP_ART" if "pickup" in document.get("event_type_candidates", []) else "EVENT_ART" if evidence and evidence["kind"]=="event" else "NOTICE_ART"
        candidates.append({"url": url, "sourceUrl": document["official_url"], "kind": image_kind,
            "rightsStatus": "APPROVED" if allowed else "UNCONFIRMED", "rightsEvidenceUrl": policy.get("evidenceUrl") if allowed else None,
            "subjectStatus":"MATCHED" if evidence else "UNCONFIRMED","subjectEvidence":evidence})
    return candidates


def approved_subject_image(images):
    matched=[image for image in images if image.get("rightsStatus")=="APPROVED" and image.get("subjectStatus")=="MATCHED"]
    # Different posters all matching a broad heading still require review.
    return matched[0] if len(matched)==1 else None


def character_image(images, target, child_id):
    """A split new/rerun character must have its own explicitly labelled banner."""
    matches=[]
    for image in images:
        evidence=image.get("subjectEvidence") or {}
        text=evidence.get("evidenceText","")
        if evidence.get("kind")!="pickup" or not image_mentions(text,target["name"]) or not PICKUP_IMAGE_WORDS.search(text):continue
        role_new=bool(re.search(r"\bnew\b|\bdebut\b|신규|新登場|新規",text,re.I))
        role_rerun=bool(re.search(r"\bre[- ]?run\b|\breturn(?:ing|s)?\b|복각|復刻|再登場",text,re.I))
        if role_new==role_rerun or ("new" if role_new else "rerun")!=target["announcementKind"]:continue
        # The source label must not explicitly feature a different quoted target.
        quoted=re.findall(r'[“"「『\[]([^”"」』\]\n]{1,100})[”"」』\]]',text)
        if quoted and any(image_name(name)!=image_name(target["name"]) for name in quoted):continue
        matches.append({**image,"subjectEvidence":{**evidence,"eventId":child_id,"sourceTitle":target["name"],"characterNames":[target["name"]],"announcementKind":target["announcementKind"]}})
    return matches,approved_subject_image(matches)


def retained_source_reference(old, document, event_id, *, title=None, characters=None, announcement_kind=None):
    """Keep a manually inspected exact publisher image until its notice changes.

    This retains an UNCONFIRMED original URL reference, never licensing/reuse
    approval. A source/policy reverify may not erase the inspected reference, but
    a raw content/section/character/role change invalidates it immediately.
    """
    if not old:return None
    image=old.get("image") or {}
    if not isinstance(image,dict):return None
    reference=image.get("sourceReference") or {}
    evidence=image.get("subjectEvidence") or {};override=old.get("sourceImageOverride") or {}
    if any(not isinstance(value,dict) for value in (image,reference,evidence,override)):return None
    exact_title=title if title is not None else document.get("_section_title") or document["original_title"]
    names=characters if characters is not None else document.get("_raw_character_names",[])
    official=document["official_url"]
    if (image.get("rightsStatus")!="UNCONFIRMED" or reference.get("mode")!="OFFICIAL_NOTICE_IMAGE"
        or old.get("rawSha256")!=document["raw_sha256"] or override.get("rawSha256")!=document["raw_sha256"]
        or override.get("exactTitle")!=exact_title or override.get("officialUrl")!=official
        or image.get("sourceUrl")!=official or evidence.get("officialUrl")!=official
        or evidence.get("status")!="MATCHED" or evidence.get("eventId")!=event_id
        or evidence.get("method")!="reviewed_image" or not evidence.get("evidenceText")
        or reference.get("allowedImageUrl")!=image.get("url") or override.get("allowedImageUrl")!=image.get("url")
        or reference.get("sourceUrl")!=override.get("sourceUrl") or not safe_public_url(reference.get("sourceUrl"))
        or not safe_public_url(image.get("url")) or not image["url"].startswith("https://")
        or not reference["sourceUrl"].startswith("https://")):
        return None
    try:
        if aware_timestamp(reference.get("checkedAt"))!=aware_timestamp(override.get("checkedAt")):return None
    except (ValidationError,TypeError):return None
    depicted=evidence.get("characterNames",[])
    if not isinstance(depicted,list) or any(not isinstance(name,str) for name in depicted):return None
    if {image_name(name) for name in names}!={image_name(name) for name in depicted}:return None
    if announcement_kind and evidence.get("announcementKind")!=announcement_kind:return None
    pickup="pickup" in document.get("event_type_candidates",[]) or bool(announcement_kind)
    if pickup and (image.get("kind")!="PICKUP_ART" or evidence.get("kind")!="pickup"):return None
    if not pickup and (image.get("kind") not in ("EVENT_ART","NOTICE_ART") or evidence.get("kind") not in ("event","notice")):return None
    return image


def notice_language(old, document, source):
    """An editorial language review is bound to this exact official raw notice.

    A feed-level language default is not a translation, and an arbitrary legacy
    originalLanguage value supplies no verification. Changed notice bytes revoke
    the per-article review rather than carrying it into a different source text.
    """
    fallback=source.get("content_language") or document.get("original_language") or "unknown"
    if fallback not in ("ko","en","ja"):fallback="unknown"
    if not old:return fallback,None
    proof=old.get("languageEvidence")
    if not isinstance(proof,dict):return fallback,None
    language=proof.get("language")
    if (proof.get("status")!="verified" or proof.get("method")!="editorial" or language not in ("ko","en","ja")
        or old.get("originalLanguage")!=language or old.get("rawSha256")!=document["raw_sha256"]
        or proof.get("sourceSha256")!=document["raw_sha256"] or proof.get("evidenceUrl")!=document["official_url"]
        or not safe_public_url(proof.get("evidenceUrl")) or not proof["evidenceUrl"].startswith("https://")):
        return fallback,None
    try:aware_timestamp(proof.get("checkedAt"))
    except (ValidationError,TypeError):return fallback,None
    return language,proof


def policy_timestamp(value):
    if isinstance(value,DATETIME_TYPE):value=value.isoformat()
    return aware_timestamp(value)


def reviewed_localization_policy(source):
    """A bounded source/game dictionary supplies names, never guessed translations."""
    policy=source.get("content_localization")
    if not reviewed(source) or not isinstance(policy,dict) or policy.get("mode")!="reviewed_templates_v1" or policy.get("gameId")!=source.get("game_id"):
        return None
    if type(policy.get("version")) is not int or not 1<=policy["version"]<=10000 or policy.get("sourceLanguage") not in ("en","ko","ja"):
        return None
    try:policy_timestamp(policy.get("reviewedAt"))
    except (ValidationError,TypeError):return None
    if not safe_public_url(policy.get("evidenceUrl")) or not policy["evidenceUrl"].startswith("https://"):
        return None
    terms=policy.get("terms")
    if not isinstance(terms,list) or not 1<=len(terms)<=200:return None
    index={}
    for term in terms:
        if not isinstance(term,dict) or term.get("kind") not in ("character","weapon","event") or not isinstance(term.get("sourceName"),str) or not 1<=len(term["sourceName"])<=150:
            return None
        aliases=term.get("aliases",[]);names=term.get("names")
        if not isinstance(aliases,list) or len(aliases)>10 or any(not isinstance(alias,str) or not 1<=len(alias)<=150 for alias in aliases) or not isinstance(names,dict) or not names:return None
        if not safe_public_url(term.get("evidenceUrl")) or not term["evidenceUrl"].startswith("https://"):return None
        try:policy_timestamp(term.get("checkedAt"))
        except (ValidationError,TypeError):return None
        for locale,name in names.items():
            if locale not in ("ko","ja") or not isinstance(name,str) or not 1<=len(name)<=150:return None
            model_name=term["kind"] in ("character","weapon") and name==term["sourceName"] and bool(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{1,30}',name)) and bool(re.search(r'\d',name))
            if locale=="ko" and not (re.search('[가-힣]',name) or model_name) or locale=="ja" and (re.search('[가-힣]',name) or not re.search('[ぁ-んァ-ヶ一-龯]',name)):return None
        for alias in [term["sourceName"],*aliases]:
            key=(term["kind"],image_name(alias))
            if key in index and index[key]!=term:return None
            index[key]=term
    formats=policy.get("formats",[])
    if not isinstance(formats,list) or any(value not in ("gf2_targeted_procurement_v1","gf2_military_upgrade_v1","highest_character_new_rerun_v1") for value in formats):return None
    return policy,index


def localized_template_title(row,source,locale,policy,index):
    selected=row.get("displayTitle") or row.get("sourceSectionTitle") or row.get("originalTitle") or row.get("title") or ""
    source_title=plain_notice(selected).strip(' ■[]')
    term=index.get(("event",image_name(source_title)))
    if term and row.get("categoryCandidate") not in ("recruitment","pickup"):
        return term["names"].get(locale)
    formats=policy.get("formats",[])
    body=plain_notice(row.get("description") or row.get("originalDescription") or "")
    patterns=(
        ("gf2_targeted_procurement_v1","character",r"Targeted Procurement(?: Event)?\s*[|:]\s*(.+)",r"drop rate for the Elite Doll\s*\[([^\]]+)\]\s+is increased",{"ko":"캐릭터 픽업","ja":"キャラクターピックアップ"}),
        ("gf2_military_upgrade_v1","weapon",r"Military Upgrade\s*[|:]\s*(.+)",r"drop rate for the Elite Weapon\s*\[([^\]]+)\]\s+is increased",{"ko":"무기 픽업","ja":"武器ピックアップ"}),
    )
    for rule,kind,pattern,proof,labels in patterns:
        match=re.fullmatch(pattern,source_title,re.I)
        if rule not in formats or not match:continue
        original=match.group(1).strip();term=index.get((kind,image_name(original)))
        local_names=[value for value in re.findall(proof,body,re.I) if image_name(value)==image_name(original)]
        if not term or not local_names or not term["names"].get(locale):return None
        return term["names"][locale]+" · "+labels[locale]
    if "highest_character_new_rerun_v1" in formats and row.get("announcementKind") in ("new","rerun") and row.get("rarityLabel"):
        term=index.get(("character",image_name(source_title)))
        if term and term["names"].get(locale) and row.get("characters")==[source_title] and row.get("characterEvidenceText"):
            labels={"ko":{"new":"신규 캐릭터 픽업","rerun":"복각 캐릭터 픽업"},"ja":{"new":"新キャラクターピックアップ","rerun":"復刻キャラクターピックアップ"}}
            return term["names"][locale]+" · "+labels[locale][row["announcementKind"]]
    return None


def localize_collected_row(row,source,observed):
    """Rebuild exact-source summaries on create/change or source-policy revision.

    Empty descriptions are intentional: no reward or uncertainty boilerplate is
    invented. Unknown names stay untranslated; their official raw text survives.
    """
    selected=row.get("displayTitle") or row.get("sourceSectionTitle") or row.get("originalTitle") or row.get("title") or ""
    existing=row.get("translations") or {};translations={}
    if isinstance(existing,dict):
        for locale,tr in existing.items():
            if (locale in ("ko","en","ja") and isinstance(tr,dict) and tr.get("sourceSha256")==row.get("rawSha256")
                and tr.get("sourceTitle")==selected and tr.get("sourceLanguage")==row.get("originalLanguage")):
                translations[locale]=tr
    row={**row,"translations":translations,"contentLocalization":None}
    validated=reviewed_localization_policy(source)
    if not validated or validated[0]["sourceLanguage"]!=row.get("originalLanguage") or urlsplit(row.get("officialUrl","")).hostname not in source.get("allowed_notice_hosts",[]):return row
    policy,index=validated;generated=[]
    for locale in ("ko","ja"):
        title=localized_template_title(row,source,locale,policy,index)
        if not title:continue
        translations[locale]={"title":title,"description":"","status":"completed","verificationStatus":"verified",
            "sourceLanguage":row["originalLanguage"],"targetLanguage":locale,"translatedAt":observed.isoformat(),"reviewedAt":policy["reviewedAt"],
            "sourceSha256":row["rawSha256"],"sourceTitle":selected,"sourceRevision":row["revision"],
            "method":"official_localized","contentType":"summary","evidenceUrl":row["officialUrl"]}
        generated.append(locale)
    if generated:row["contentLocalization"]={"method":"reviewed_templates_v1","policyVersion":policy["version"],"policySha256":digest(policy),"languages":generated}
    return row


def original_subject_image(images,source,observed):
    """Link only a unique exact labelled image present in this fetched notice.

    This opt-in never grants copying/reuse rights or guesses a role from order.
    Unlabelled homepage posters and multiple matches require separate review.
    """
    policy=source.get("original_image_reference")
    if not reviewed(source) or not isinstance(policy,dict) or policy.get("mode")!="matched_original_link_v1":return None
    if not safe_public_url(policy.get("evidenceUrl")) or not policy["evidenceUrl"].startswith("https://"):return None
    hosts=policy.get("allowedHosts")
    if not isinstance(hosts,list) or not 1<=len(hosts)<=20 or any(not isinstance(host,str) or not re.fullmatch(r'[A-Za-z0-9.-]+',host) for host in hosts):return None
    try:policy_timestamp(policy.get("reviewedAt"));checked=policy_timestamp(observed).isoformat()
    except (ValidationError,TypeError):return None
    matched=[]
    for image in images:
        evidence=image.get("subjectEvidence") or {}
        if (image.get("rightsStatus")=="UNCONFIRMED" and image.get("subjectStatus")=="MATCHED"
            and evidence.get("method") in ("source_alt","source_section") and evidence.get("status")=="MATCHED"
            and safe_public_url(image.get("url")) and image["url"].startswith("https://") and urlsplit(image["url"]).hostname in hosts
            and safe_public_url(image.get("sourceUrl")) and image["sourceUrl"].startswith("https://")
            and urlsplit(image["sourceUrl"]).hostname in source.get("allowed_notice_hosts",[]) and evidence.get("officialUrl")==image["sourceUrl"]):
            matched.append({**image,"sourceReference":{"mode":"OFFICIAL_NOTICE_IMAGE","sourceUrl":image["sourceUrl"],"checkedAt":checked,"allowedImageUrl":image["url"]}})
    return matched[0] if len(matched)==1 else None


def reviewed_notice_image(document,source,event_id):
    """A reviewed source policy can reapply an exact previously inspected poster.

    Different raw bytes/title/subject never reuse that old banner. No inference
    from character resemblance, filename order or a regional publisher logo.
    """
    policy=source.get("reviewed_notice_references")
    if not reviewed(source) or not isinstance(policy,dict) or policy.get("mode")!="exact_reviewed_notice_links_v1":return None
    references=policy.get("references",[])
    if not isinstance(references,list) or len(references)>50:return None
    selected=document.get("_section_title") or document["original_title"];pickup="pickup" in document.get("event_type_candidates",[])
    names=document.get("_raw_character_names",[]);matched=[]
    for ref in references:
        if not isinstance(ref,dict) or ref.get("sourceSha256")!=document["raw_sha256"] or ref.get("sourceTitle")!=selected or ref.get("officialUrl")!=document["official_url"]:continue
        if ref.get("kind")!=("PICKUP_ART" if pickup else "EVENT_ART") or ref.get("characterNames",[])!=names or not isinstance(ref.get("evidenceText"),str) or not ref["evidenceText"]:continue
        if any(not safe_public_url(ref.get(key)) or not ref[key].startswith("https://") for key in ("url","sourceUrl")):continue
        if GENERIC_GAME_IMAGE.search(ref["url"]):continue
        try:checked=policy_timestamp(ref.get("checkedAt")).isoformat()
        except (ValidationError,TypeError):continue
        matched.append({"url":ref["url"],"sourceUrl":document["official_url"],"kind":ref["kind"],"rightsStatus":"UNCONFIRMED",
            "sourceReference":{"mode":"OFFICIAL_NOTICE_IMAGE","sourceUrl":ref["sourceUrl"],"checkedAt":checked,"allowedImageUrl":ref["url"]},
            "subjectEvidence":{"status":"MATCHED","eventId":event_id,"officialUrl":document["official_url"],"kind":"pickup" if pickup else "event",
                "sourceSha256":document["raw_sha256"],"sourceTitle":selected,"method":"reviewed_image","evidenceText":ref["evidenceText"],"characterNames":names}})
    return matched[0] if len(matched)==1 else None


def automatic_main_role_policy(source):
    """A source format opt-in is separate from reading permission/date approval."""
    policy=(source.get("calendar_role_policy") or {}).get("automaticRules")
    if not reviewed(source) or not isinstance(policy,dict):return None
    fields={"mode","gameId","parser","newHighestCharacter","mainEventFormat","newCharacterFormat","evidenceUrl","checkedAt"}
    if (set(policy)-fields or policy.get("mode")!="explicit_main_role_extraction_v1"
        or policy.get("gameId")!=source.get("game_id") or policy.get("parser")!=source.get("parser")
        or not isinstance(policy.get("newHighestCharacter"),bool) or policy.get("mainEventFormat") not in (None,"gf2_story_event_v1")
        or policy.get("newCharacterFormat") not in (None,"gf2_new_doll_block_v1")
        or not safe_public_url(policy.get("evidenceUrl")) or not policy["evidenceUrl"].startswith("https://")):return None
    if policy.get("mainEventFormat") and (source.get("game_id")!="girls-frontline-2" or source.get("parser")!="steam_news" or source.get("section_schedule_format")!="heading_period_v1"):return None
    if policy.get("newCharacterFormat") and (source.get("game_id")!="girls-frontline-2" or source.get("parser")!="steam_news"):return None
    try:policy_timestamp(policy.get("checkedAt"))
    except (ValidationError,TypeError):return None
    return policy


def automatic_main_role(row,source):
    policy=automatic_main_role_policy(source)
    if not policy:return None
    selected=row.get("displayTitle") or row.get("sourceSectionTitle") or row.get("originalTitle") or row.get("title")
    text=plain_notice(row.get("originalDescription") or "")
    if policy["newHighestCharacter"] and row.get("announcementKind")=="new" and row.get("categoryCandidate")=="recruitment":
        highest=source.get("highest_character_rarity") or {}
        try:policy_timestamp(highest.get("checkedAt"))
        except (ValidationError,TypeError):return None
        if not safe_public_url(highest.get("evidenceUrl")) or not highest["evidenceUrl"].startswith("https://"):return None
        # Re-extract the actual original paragraph. Neither a roster position nor
        # a Targeted Procurement heading establishes a character's debut.
        extracted=highest_rarity_targets(text,source)+new_doll_block_targets(text,source)
        matched=[target for target in extracted if target["announcementKind"]=="new"
            and row.get("characterNames")==[target["name"]] and row.get("characters")==[target["name"]]]
        matched=[target for target in matched if target["evidenceText"]==row.get("characterEvidenceText")]
        if len(matched)==1 and selected==matched[0]["name"] and row.get("characterEvidenceText")==matched[0]["evidenceText"]:
            return "main-character",matched[0]["evidenceText"]
    if policy.get("mainEventFormat")=="gf2_story_event_v1" and row.get("sourceSectionKey") and row.get("categoryCandidate")=="limited-event" and not row.get("characterNames"):
        # The reviewed GF2 publisher grammar explicitly calls the same named
        # section a Time-Limited Event with its own Game Modes. Generic rewards,
        # login, shop, supply and other heading blocks never satisfy this rule.
        prefix="During the Time-Limited Event ["+str(selected)+"],"
        lines=[line.strip() for line in text.splitlines() if line.strip().startswith(prefix)
            and "by participating in event Game Modes." in line]
        if len(lines)==1 and len(selected or "")<=150:return "main-event",lines[0][:1500]
    return None


def reviewed_calendar_role(row,source):
    """Exact reviews or explicit separately reviewed publisher grammar only."""
    policy=source.get("calendar_role_policy")
    row={**row,"calendarRole":None,"calendarRoleEvidence":None}
    if not reviewed(source) or not isinstance(policy,dict) or policy.get("mode")!="reviewed_main_roles_v1":return row
    roles=policy.get("roles",[])
    if not isinstance(roles,list) or len(roles)>50:return row
    selected=row.get("displayTitle") or row.get("sourceSectionTitle") or row.get("originalTitle") or row.get("title")
    for role in roles:
        if not isinstance(role,dict) or role.get("sourceSha256")!=row.get("rawSha256") or role.get("sourceTitle")!=selected or role.get("role") not in ("main-character","main-event","other"):continue
        if role.get("characterNames",[])!=row.get("characterNames",[]) or not safe_public_url(role.get("evidenceUrl")) or not role["evidenceUrl"].startswith("https://"):continue
        if role["role"]=="main-character" and (row.get("categoryCandidate")!="recruitment" or not row.get("characterNames")):continue
        if role["role"]=="main-event" and row.get("categoryCandidate") not in ("limited-event","login-reward"):continue
        try:checked=policy_timestamp(role.get("checkedAt")).isoformat()
        except (ValidationError,TypeError):continue
        row.update(calendarRole=role["role"],calendarRoleEvidence={"status":"verified","sourceSha256":row["rawSha256"],"sourceTitle":selected,"role":role["role"],"evidenceUrl":role["evidenceUrl"],"checkedAt":checked})
        return row
    automatic=automatic_main_role(row,source)
    if automatic:
        role,proof=automatic;rules=automatic_main_role_policy(source)
        row.update(calendarRole=role,calendarRoleEvidence={"status":"verified","sourceSha256":row["rawSha256"],"sourceTitle":selected,
            "role":role,"evidenceUrl":rules["evidenceUrl"],"checkedAt":rules["checkedAt"],"method":"explicit_source_main_role_v1",
            "policySha256":digest(rules),"evidenceText":proof})
    return row


def announcement_metadata(row,old,observed):
    """Notice content revisions are independent of dates/extraction/translation."""
    title=row.get("displayTitle") or row.get("sourceSectionTitle") or row.get("originalTitle") or row.get("title")
    signature=digest([row.get("rawSha256"),title,row.get("characterNames",[])])
    previous=(old or {}).get("announcementContentSha256")
    changed=previous is not None and previous!=signature
    published=row.get("publishedAt");updated=row.get("sourceUpdatedAt")
    # Only the provider's explicit publication/update timestamp establishes when
    # an announcement became public. Observation time never invents that fact.
    try:
        clock=timestamp(updated) if updated and published and timestamp(updated)>=timestamp(published) else timestamp(published) if published else None
    except (ValueError,TypeError):clock=None
    return {**row,"announcementContentSha256":signature,"announcementRevision":(old or {}).get("announcementRevision",1)+int(changed),
        "announcementPublishedAt":clock,"announcementObservedAt":observed if changed or previous is None else old.get("announcementObservedAt",observed)}


GF2_ROSTER_URLS={"en":"https://gf2.haoplay.com/en/roleinfo/allroles.html","ko":"https://gf2.haoplay.com/kr/roleinfo/allroles.html"}


class OfficialPairedRoster(HTMLParser):
    """Extract names only from publisher-labelled links with stable role IDs.

    Portrait filenames have a different identifier; image URLs never identify
    the character and are neither parsed nor fetched by this adapter.
    """
    def __init__(self,url):
        super().__init__(convert_charrefs=True);self.url=url;self.depth=0;self.active=None;self.rows={}
    def handle_starttag(self,tag,attributes):
        if tag in ("img","br","hr","meta","link","input","source","area","base","embed","param","track","wbr"):return
        self.depth+=1;attrs=dict(attributes)
        if tag=="a" and "allroles-roles-main-div" in attrs.get("class","").split():
            target=urlsplit(urljoin(self.url,attrs.get("href","")));pairs=parse_qsl(target.query,keep_blank_values=True)
            if target.scheme!="https" or target.hostname!="gf2.haoplay.com" or target.path!=urlsplit(self.url).path.removesuffix("allroles.html") or target.username or target.fragment or len(pairs)!=1 or pairs[0][0]!="role" or not re.fullmatch(r'\d{1,5}',pairs[0][1]):
                raise ValidationError("roster_role_identity_invalid")
            self.active={"id":pairs[0][1],"depth":self.depth,"nameDepth":None,"text":[]}
        if self.active and tag=="div" and "allroles-roles-name-name-div" in attrs.get("class","").split():self.active["nameDepth"]=self.depth
    def handle_data(self,value):
        if self.active and self.active["nameDepth"]==self.depth:self.active["text"].append(value)
    def handle_endtag(self,tag):
        if self.active and tag=="a" and self.active["depth"]==self.depth:
            name=re.sub(r'\s+',' ',''.join(self.active["text"])).strip();identity=self.active["id"]
            if not 1<=len(name)<=150 or identity in self.rows and self.rows[identity]!=name:raise ValidationError("roster_name_invalid_or_conflicting")
            self.rows[identity]=name;self.active=None
        if self.active and self.active["nameDepth"]==self.depth:self.active["nameDepth"]=None
        self.depth=max(0,self.depth-1)
    def handle_startendtag(self,tag,attrs):
        pass


def parse_official_roster(raw,url):
    if url not in GF2_ROSTER_URLS.values() or len(raw)>1048576 or b'\x00' in raw:raise ValidationError("roster_response_invalid")
    parser=OfficialPairedRoster(url)
    try:parser.feed(raw.decode('utf-8-sig'));parser.close()
    except UnicodeError as error:raise ValidationError("roster_utf8_required") from error
    if not 1<=len(parser.rows)<=200:raise ValidationError("roster_names_missing_or_unbounded")
    return parser.rows


def refresh_localization_roster(store,source,observed,fetch,sleep,deadline=None):
    """Two reviewed public requests per due source; failure preserves old names."""
    policy=source.get("localization_roster");disabled={"status":"DISABLED","pagesFetched":0,"dictionaryWrites":0}
    if not isinstance(policy,dict) or policy.get("enabled") is not True:return source,disabled
    if (not reviewed(source) or source.get("game_id")!="girls-frontline-2" or policy.get("mode")!="gf2_paired_roster_v1"
        or policy.get("gameId")!=source["game_id"] or policy.get("accessStatus")!="approved" or policy.get("sourceLanguage")!="en"
        or policy.get("sourceUrl")!=GF2_ROSTER_URLS["en"] or policy.get("targetUrls")!={"ko":GF2_ROSTER_URLS["ko"]}
        or not safe_public_url(policy.get("termsUrl")) or not policy["termsUrl"].startswith("https://") or not reviewed_localization_policy(source)):
        return source,{**disabled,"status":"REVIEW_REQUIRED"}
    try:policy_timestamp(policy.get("termsCheckedAt"));policy_timestamp(policy.get("identityCheckedAt"))
    except (ValidationError,TypeError):return source,{**disabled,"status":"REVIEW_REQUIRED"}
    metrics={"status":"FETCH_FAILED","pagesFetched":0,"dictionaryWrites":0};raws={}
    try:
        rosters={}
        for locale,url in GF2_ROSTER_URLS.items():
            if deadline is not None and time.monotonic()>=deadline:raise SafetyError("run_time_limit")
            sleep(1)
            request={"enabled":True,"access_status":"approved","terms_url":policy["termsUrl"],"terms_checked_at":policy["termsCheckedAt"],
                "url":url,"official_host":"gf2.haoplay.com","method":"public_html","request_timeout_seconds":10,"max_response_bytes":1048576}
            status,raw,_=fetch(request);metrics["pagesFetched"]+=1
            if status!=200:raise SafetyError("roster_http_"+str(status))
            raws[locale]=raw;rosters[locale]=parse_official_roster(raw,url)
        existing=source["content_localization"];updated=deepcopy(existing);terms=updated["terms"];changed=False;pair_names={};paired=0
        for identity in sorted(set(rosters['en'])&set(rosters['ko']),key=int):
            name,localized=rosters['en'][identity],rosters['ko'][identity]
            model_name=localized==name and bool(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{1,30}',name)) and bool(re.search(r'\d',name))
            if not re.search('[가-힣]',localized) and not model_name:continue
            key=image_name(name)
            if key in pair_names and pair_names[key]!=localized:raise ValidationError("roster_ambiguous_paired_name")
            pair_names[key]=localized;paired+=1
            previous=next((term for term in terms if term.get('kind')=='character' and (term.get('publisherUnitId')==identity or key in [image_name(alias) for alias in [term['sourceName'],*term.get('aliases',[])]])),None)
            if previous and previous.get('publisherUnitId')==identity and previous['sourceName']==name and previous['names'].get('ko')==localized:continue
            aliases=list(previous.get('aliases',[])) if previous else []
            if previous and previous['sourceName']!=name and previous['sourceName'] not in aliases:aliases.append(previous['sourceName'])
            term={**(previous or {}),'kind':'character','sourceName':name,'aliases':aliases,'names':{**(previous or {}).get('names',{}),'ko':localized},
                'publisherUnitId':identity,'evidenceUrl':GF2_ROSTER_URLS['ko'],'checkedAt':observed.isoformat(),
                'pairedSourceSha256':hashlib.sha256(raws['en']).hexdigest(),'pairedTargetSha256':hashlib.sha256(raws['ko']).hexdigest()}
            if previous:terms[terms.index(previous)]=term
            else:terms.append(term)
            changed=True
        metrics.update(status="SUCCESS",pairedNames=paired,unmatchedSourceUnits=len(set(rosters['en'])-set(rosters['ko'])),unmatchedTargetUnits=len(set(rosters['ko'])-set(rosters['en'])))
        if not changed:return source,metrics
        updated['version']+=1;candidate={**source,'content_localization':updated}
        if not reviewed_localization_policy(candidate):raise ValidationError("roster_dictionary_invalid")
        def persist(old):
            if not old or old.get('contentLocalization')!=existing:return None,False
            return {**old,'contentLocalization':updated},True
        if not store.mutate('gameSources/'+source['id'],persist):
            metrics.update(status='SOURCE_POLICY_CHANGED');return source,metrics
        metrics['dictionaryWrites']=1;return candidate,metrics
    except (SafetyError,ValidationError,ValueError,TypeError,KeyError) as error:
        metrics['reasonCode']=str(error)[:120];return source,metrics
    except Exception as error:
        metrics['reasonCode']=type(error).__name__;return source,metrics


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


def new_doll_block_targets(text,source):
    """Only the separately approved GF2 New Doll heading grammar is supported.

    A following Elite Doll item proves debut; an exact named procurement-rate
    sentence separately proves pickup. The article's story-event dates do not
    establish this character's recruitment period.
    """
    policy=automatic_main_role_policy(source)
    highest=source.get("highest_character_rarity") or {}
    if not policy or policy.get("newCharacterFormat")!="gf2_new_doll_block_v1" or policy.get("newHighestCharacter") is not True or "Elite" not in highest.get("labels",[]):return []
    try:policy_timestamp(highest.get("checkedAt"))
    except (ValidationError,TypeError):return []
    if not safe_public_url(highest.get("evidenceUrl")) or not highest["evidenceUrl"].startswith("https://"):return []
    lines=[line.strip().replace('\\[','[').replace('\\]',']') for line in text.splitlines()]
    targets=[];heading=None
    for line in lines:
        if not line:continue
        if re.fullmatch(r'-\s*New Dolls?\s*-',line):heading=line;continue
        if heading is None:continue
        item=re.fullmatch(r'■\s*Elite Doll\s*\[([^\]\n]{1,100})\]',line)
        if not item:heading=None;continue
        name=item.group(1).strip()
        if not name or any(char in name for char in '["「『'):continue
        escaped=re.escape(name)
        rate=[value for value in lines if re.fullmatch(r'(?:・\s*)?During the event, the drop rate for Elite Doll \['+escaped+r'\] and Elite Weapon \[[^\]\n]{1,100}\] will be increased\.',value)
            or re.fullmatch(r'The drop rate for the Elite Doll \['+escaped+r'\] is increased in the Targeted Procurement!',value)]
        if len(rate)!=1 or not any('Targeted Procurement' in value for value in lines):continue
        if not any(row['name']==name for row in targets):targets.append({'name':name,'announcementKind':'new','rarityLabel':'Elite',
            'evidenceText':heading+'\n'+line+'\n'+rate[0],'evidenceMethod':'gf2_new_doll_block_v1'})
    return targets[:20]


def enrich_document(document, source):
    text = plain_notice(document["original_title"] + "\n" + document["original_description"])
    classification = classify_notice_content({"rawSha256": document["raw_sha256"], "originalTitle": document["original_title"], "displayTitle": document.get("_section_title")})
    schedule = extract_schedule(text)
    schedule_kind = "interval"
    paired=verified_paired_ticket_section(document,source)
    if paired:schedule={**paired["schedule"],"reasonCodes":[],"evidenceText":paired["evidence"]["auxiliaryEvidenceText"]}
    live_point = youtube_broadcast_document_point(document, source, classification)
    if live_point:
        schedule = {"startsAt": live_point["clock"], "endsAt": live_point["clock"], "originalTimezone": "UTC", "evidenceText": live_point["providerField"] + ": " + live_point["providerValue"], "reasonCodes": []}
        schedule_kind = "point"
    reasons = list(schedule["reasonCodes"])
    authorized = reviewed(source) and (bool(paired)or source.get("schedule_extraction_approved") is True and source.get("schedule_extraction_reviewed_at") and safe_public_url(source.get("schedule_extraction_evidence_url")))
    if urlsplit(document.get("official_url","")).hostname not in source.get("allowed_notice_hosts",[]):
        authorized=False
        reasons.append("OFFICIAL_NOTICE_HOST_UNCONFIRMED")
    if not authorized and not live_point:
        reasons.append("EXTRACTION_REVIEW_REQUIRED")
    region = document.get("server_region")
    if region not in ("KR", "JP", "GLOBAL") and not live_point:
        reasons.append("SERVER_REGION_UNCONFIRMED")
    # Explicit competing regional labels require a regional adapter, not merging.
    region_names={"kr":"KR","한국":"KR","jp":"JP","일본":"JP","global":"GLOBAL","글로벌":"GLOBAL"}
    regions = {region_names[token.lower()] for token in re.findall(r"\b(KR|JP|GLOBAL|한국|일본|글로벌)\s*(?:server|서버)", text, re.I)}
    if len(regions) > 1 or regions and region not in regions:
        reasons.append("SERVER_REGION_CONFLICT")
    hints = document.get("event_type_candidates", [])
    category = document.get("_section_category") or next((kind for kind in ("pickup", "login_reward", "maintenance", "update", "official_broadcast") if kind in hints), "unclassified")
    category = "recruitment" if category == "pickup" else category
    category = {"broadcast-preview": "official_broadcast", "offline-preview": "offline", "pickup-preview": "recruitment", "event-preview": "limited-event",
        "goods-preview":"goods-sale","collaboration-preview":"collaboration"}.get(classification["kind"], category)
    if live_point: category = "official_broadcast"
    verified = not reasons and category != "unclassified" and classification["kind"] not in NEWS_ONLY_KINDS
    images = image_candidates(document, source)
    targets = highest_rarity_targets(text, source) if "pickup" in hints and reviewed(source) else []
    for target in new_doll_block_targets(text,source):
        if not any(old['name']==target['name'] for old in targets):targets.append(target)
    cancelled = bool(re.search(r"\b(?:event|banner)\s+(?:is\s+|has\s+been\s+)?cancelled\b|(?:이벤트|픽업)\s*(?:가\s*)?취소|(?:イベント|ピックアップ).*中止", text, re.I))
    return {**document, "schedule": schedule, "schedule_verified": bool(verified), "category_candidate": category, "schedule_kind": schedule_kind, "source_live_point": live_point,
        "extraction_version": EXTRACTION_VERSION, "extraction_policy_sha256": digest(source_policy(source)),
        "schedule_reason_codes": list(dict.fromkeys(reasons)), "image_candidates": images,
        "approved_image": paired.get("image")if paired else approved_subject_image(images),"referenced_image":original_subject_image(images,source,document.get("observed_at")),
        "paired_ticket_section":paired,
        "highest_rarity_targets": targets, "cancelled": cancelled,"section_documents":section_documents(document,source)}


def youtube_broadcast_document_point(document, source, classification=None):
    classification = classification or classify_notice_content({"rawSha256": document.get("raw_sha256"), "originalTitle": document.get("original_title")})
    if classification["kind"] != "broadcast-preview" or source.get("parser") != "youtube_data_api" or not reviewed(source):
        return None
    raw = document.get("raw_json") or {}
    details = document.get("source_live_details")
    video = document.get("external_key")
    if (not isinstance(raw, dict) or not isinstance(details, dict) or raw.get("liveStreamingDetails") != details
        or raw.get("channelId") != source.get("youtube_channel_id") or raw.get("videoId") != video
        or raw.get("title") != document.get("original_title") or digest(raw) != document.get("raw_sha256")
        or raw.get("officialUrl") != document.get("official_url") or document.get("official_url") != "https://www.youtube.com/watch?" + urlencode({"v": video})):
        return None
    field = "actualStartTime" if details.get("actualStartTime") else "scheduledStartTime"
    try: clock = aware_timestamp(details.get(field))
    except (ValidationError, TypeError): return None
    return {"clock": clock, "providerField": "liveStreamingDetails." + field, "providerValue": details[field]}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, default=lambda v:v.isoformat()).encode()).hexdigest()


def youtube_metadata_expired(row, now):
    """Only positively identified, previously fetched exact YouTube metadata."""
    channel, video = row.get("sourceChannelId"), row.get("sourceVideoId")
    if row.get("sourceProvider")!="youtube" or not isinstance(channel,str) or not YOUTUBE_CHANNEL_ID.fullmatch(channel) or not isinstance(video,str) or not YOUTUBE_VIDEO_ID.fullmatch(video):return False
    if row.get("officialUrl")!="https://www.youtube.com/watch?"+urlencode({"v":video}):return False
    if row.get("externalKey")!=video or not isinstance(row.get("rawSha256"),str)or not re.fullmatch(r"[a-f0-9]{64}",row["rawSha256"]):return False
    raw=row.get("rawJson")
    if raw is not None and (not isinstance(raw,dict) or raw.get("channelId")!=channel or raw.get("videoId")!=video or raw.get("officialUrl")!=row["officialUrl"]):return False
    try:checked=policy_timestamp(row.get("metadataCheckedAt"));expires=policy_timestamp(row.get("metadataExpiresAt"))
    except (ValidationError,ValueError,TypeError):return False
    return checked+timedelta(days=30)==expires and expires<=timestamp(now)


def content_cleanup_reason(row, now):
    gate=content_publication_status(row.get("publishedAt"))
    if gate=="PUBLISHED_BEFORE_CONTENT_CUTOFF" and not verified_calendar_day_period(row,now):return gate
    if youtube_metadata_expired(row,now):return "YOUTUBE_METADATA_EXPIRED"
    return None


def content_cleanup_plan(store, *, max_scan=500, max_delete=400, cursors=None, now=None):
    """Review only old public news; shared sources and all user data are excluded.

    Unknown publication dates remain untouched. A pickup without its own posted
    date needs an exact old parent event, which is rechecked at deletion time.
    Scan/deletion limits are per run and do not claim an exhaustive cleanup.
    """
    if not 1 <= max_scan <= 500 or not 1 <= max_delete <= 400: raise SafetyError("bounded_content_cleanup_required")
    cursors = cursors or {}; now=now or datetime.now(UTC)
    if set(cursors) - CONTENT_CLEANUP_COLLECTIONS: raise SafetyError("public_content_cleanup_path_required")
    result = {"status": "DRY_RUN", "contentPublishedNotBefore": CONTENT_PUBLISHED_NOT_BEFORE,
        "recordedAt": now, "scope": "PUBLIC_POSTED_CONTENT_ONLY", "remoteFirestoreWrites": 0, "fcmSends": 0,
        "maxScanPerCollection": max_scan, "maxDeletesPerRun": max_delete, "scanned": {}, "preserved": {}, "nextCursors": {}, "candidates": []}
    events = {}; retained_raw = set(); complete = {}; eligible = []; pickup_links = {}; raw_links = {}; raw_keys = {}
    for collection in ("gameEvents", "pickupSchedules", "collectionDocuments"):
        after = cursors.get(collection); scanned = 0; ended = False
        reasons = {}; result["preserved"][collection] = reasons
        while scanned < max_scan:
            count = min(50, max_scan-scanned)
            rows = store.list_cursor(collection, after, count)
            for doc_id, row in rows:
                path = cleanup_path(collection + "/" + doc_id)
                guards = {}; basis = "publishedAt"; published = row.get("publishedAt"); reason=content_cleanup_reason(row,now)
                if collection=="collectionDocuments":raw_keys[doc_id]=(row.get("sourceId"),row.get("externalKey"),row.get("rawSha256"))
                if collection == "gameEvents":
                    events[doc_id] = row
                    raw_links.setdefault((row.get("sourceId"), row.get("externalKey"), row.get("rawSha256")),set()).add(doc_id)
                    if reason is None:
                        retained_raw.add((row.get("sourceId"), row.get("externalKey"), row.get("rawSha256")))
                if collection == "pickupSchedules":
                    parent_id = row.get("eventId")
                    if isinstance(parent_id,str):pickup_links.setdefault(parent_id,set()).add(doc_id)
                    if not isinstance(parent_id, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,200}", parent_id):
                        reason = "EXACT_PARENT_EVENT_REQUIRED"
                        reasons[reason] = reasons.get(reason, 0)+1; continue
                    parent = events.get(parent_id)
                    if parent is None: parent = store.get(cleanup_path("gameEvents/"+parent_id))
                    if not parent or row.get("officialUrl") != parent.get("officialUrl") or row.get("gameId") not in parent.get("gameIds", []):
                        reason = "EXACT_PARENT_EVENT_REQUIRED"
                        reasons[reason] = reasons.get(reason, 0)+1; continue
                    if published is None: published = parent.get("publishedAt"); basis = "linkedEvent.publishedAt"
                    if content_publication_status(parent.get("publishedAt")) != content_publication_status(published):
                        reason = "PUBLICATION_PARENT_CONFLICT"
                        reasons[reason] = reasons.get(reason, 0)+1; continue
                    guards["gameEvents/"+parent_id] = document_fingerprint(parent)
                    reason=content_cleanup_reason(parent,now) if basis=="linkedEvent.publishedAt" else reason
                if reason is None:
                    reason=content_publication_status(published)
                    reasons[reason] = reasons.get(reason, 0)+1; continue
                if collection == "collectionDocuments" and (not complete.get("gameEvents") or (row.get("sourceId"), row.get("externalKey"), row.get("rawSha256")) in retained_raw):
                    reason = "RETAINED_OR_UNSCANNED_EVENT_MAY_REFERENCE_RAW"
                    reasons[reason] = reasons.get(reason, 0)+1; continue
                eligible.append({"collection": collection, "id": doc_id, "publishedAt": timestamp(published) if content_publication_status(published)!="PUBLICATION_DATE_UNCONFIRMED" else None, "basis": basis,"reasonCode":reason,
                    "fingerprint": document_fingerprint(row), "guards": guards})
            scanned += len(rows)
            if rows: after = rows[-1][0]
            if len(rows) < count: ended = True; break
        complete[collection] = ended
        result["scanned"][collection] = scanned
        result["nextCursors"][collection] = None if ended else after
    selected_pickups=set();selected_events=set()
    # Pickups must be selected before deleting the parent supplying their posted
    # date. A partial scan/cap leaves the parent intact for the next cleanup.
    for candidate in sorted(eligible,key=lambda row:({"pickupSchedules":0,"gameEvents":1,"collectionDocuments":2}[row["collection"]],row["id"])):
        collection=candidate["collection"];reasons=result["preserved"][collection];reason=None
        if len(result["candidates"])>=max_delete:reason="DELETE_LIMIT_REACHED"
        elif collection=="gameEvents":
            children=pickup_links.get(candidate["id"],set())
            if not complete.get("pickupSchedules") or children-selected_pickups or len(children)>50:reason="DEPENDENT_PICKUPS_NOT_FULLY_SELECTED"
            else:candidate["guards"]={"pickupSchedules/"+pickup_id:document_fingerprint(None)for pickup_id in children}
        elif collection=="collectionDocuments":
            references=raw_links.get(raw_keys[candidate["id"]],set())
            if not complete.get("gameEvents") or references-selected_events:reason="RETAINED_OR_UNSCANNED_EVENT_MAY_REFERENCE_RAW"
            elif len(references)>50:reason="RAW_REFERENCE_GUARD_LIMIT_REACHED"
            else:candidate["guards"]={"gameEvents/"+event_id:document_fingerprint(None)for event_id in references}
        if reason:reasons[reason]=reasons.get(reason,0)+1;continue
        result["candidates"].append(candidate)
        if collection=="pickupSchedules":selected_pickups.add(candidate["id"])
        if collection=="gameEvents":selected_events.add(candidate["id"])
    result["scanComplete"] = all(complete.values())
    result["deleteLimitReached"] = any(value.get("DELETE_LIMIT_REACHED") for value in result["preserved"].values())
    result["fullCleanupVerified"] = result["scanComplete"] and not eligible and not any(value.get("RETAINED_OR_UNSCANNED_EVENT_MAY_REFERENCE_RAW")or value.get("DEPENDENT_PICKUPS_NOT_FULLY_SELECTED") for value in result["preserved"].values())
    result["candidateCount"] = len(result["candidates"])
    result["planSha256"] = document_fingerprint(result)
    return result


def apply_content_cleanup(store, review, *, now=None):
    """Apply a concrete dry-run plan with transactional exact-byte guards."""
    if (not isinstance(review, dict) or review.get("status") != "DRY_RUN" or review.get("contentPublishedNotBefore") != CONTENT_PUBLISHED_NOT_BEFORE
        or review.get("scope") != "PUBLIC_POSTED_CONTENT_ONLY" or review.get("planSha256") != document_fingerprint({key:value for key,value in review.items() if key != "planSha256"})
        or not isinstance(review.get("candidates"), list) or len(review["candidates"]) > 400): raise SafetyError("reviewed_content_cleanup_plan_required")
    selected = [];now=now or datetime.now(UTC)
    for row in review["candidates"]:
        if not isinstance(row, dict) or row.get("collection") not in CONTENT_CLEANUP_COLLECTIONS: raise SafetyError("public_content_cleanup_path_required")
        path = cleanup_path(row["collection"]+"/"+row.get("id", ""))
        if row.get("reasonCode") not in ("PUBLISHED_BEFORE_CONTENT_CUTOFF","YOUTUBE_METADATA_EXPIRED"): raise SafetyError("only_old_posted_content_may_be_deleted")
        if row["reasonCode"]=="PUBLISHED_BEFORE_CONTENT_CUTOFF" and content_publication_status(row.get("publishedAt"))!="PUBLISHED_BEFORE_CONTENT_CUTOFF":raise SafetyError("only_old_posted_content_may_be_deleted")
        if not re.fullmatch(r"[a-f0-9]{64}", row.get("fingerprint", "")) or not isinstance(row.get("guards"), dict) or len(row["guards"]) > (1 if row["collection"]=="pickupSchedules" else 50): raise SafetyError("reviewed_content_cleanup_plan_required")
        for guard_path, proof in row["guards"].items():
            cleanup_path(guard_path)
            expected_collection="pickupSchedules/" if row["collection"]=="gameEvents" else "gameEvents/"
            if not guard_path.startswith(expected_collection) or not re.fullmatch(r"[a-f0-9]{64}", proof): raise SafetyError("reviewed_content_cleanup_plan_required")
            if row["collection"]!="pickupSchedules" and proof!=document_fingerprint(None):raise SafetyError("content_dependency_absence_guard_required")
        if row["collection"] == "pickupSchedules" and len(row["guards"]) != 1: raise SafetyError("exact_parent_cleanup_guard_required")
        selected.append((path, row))
    # Parent events remain present until all linked pickup guards have run.
    selected.sort(key=lambda value: ({"pickupSchedules":0,"gameEvents":1,"collectionDocuments":2}[value[1]["collection"]],value[0]))
    result = {"status": "APPLIED", "contentPublishedNotBefore": CONTENT_PUBLISHED_NOT_BEFORE, "reviewPlanSha256": review["planSha256"],
        "recordedAt": now, "fcmSends": 0, "deleted": {}, "changedSinceReview": 0,
        "reviewedCandidates": len(selected), "privateRecordsRead": 0, "sharedSourcesChanged": 0}
    writes = store.writes
    for path, row in selected:
        def valid(current,guard_rows):
            subject=current
            if row["collection"]=="pickupSchedules":
                parent=guard_rows.get("gameEvents/"+str(current.get("eventId")))
                if not parent or current.get("officialUrl")!=parent.get("officialUrl") or current.get("gameId") not in parent.get("gameIds",[]):return False
                if current.get("publishedAt") is None:subject=parent
            return content_cleanup_reason(subject,now)==row["reasonCode"]
        if store.delete_content_if_unchanged(path, row["fingerprint"], row["guards"], valid):
            key = row["collection"]; result["deleted"][key] = result["deleted"].get(key, 0)+1
        else: result["changedSinceReview"] += 1
    result.update(remoteFirestoreWrites=store.writes-writes if store.remote else 0, documentsDeleted=sum(result["deleted"].values()),
        fullCleanupVerified=False, requiresPostApplyRescan=True)
    return result


def expire_youtube_metadata(store,now=None):
    """Once-daily, expiry-only deletion; never initial cutoff or user cleanup.

    At most 50 expired public events and 50 raw snapshots are selected. Every
    deletion rechecks its fingerprint and exact provider/30-day evidence. A
    dependency or soft-budget limit is recorded and deferred, never bypassed.
    """
    now=now or datetime.now(UTC);path="collectorLocks/youtube-metadata-retention";token=uuid.uuid4().hex
    def claim(old):
        if old and (old.get("nextRunAt")and timestamp(old["nextRunAt"])>now or old.get("leaseUntil")and timestamp(old["leaseUntil"])>now):return None,False
        return {**(old or {}),"scope":"publicYoutubeMetadataOnly","leaseUntil":now+timedelta(minutes=12),"token":token},True
    # Cheap read first avoids a no-op write transaction at every cron tick.
    prior=store.get(path)
    if prior and(prior.get("nextRunAt")and timestamp(prior["nextRunAt"])>now or prior.get("leaseUntil")and timestamp(prior["leaseUntil"])>now):return {"status":"DEFERRED","reasonCode":"DAILY_YOUTUBE_RETENTION_NOT_DUE","fcmSends":0}
    if not store.mutate(path,claim):return {"status":"DEFERRED","reasonCode":"DAILY_YOUTUBE_RETENTION_LEASE_HELD","fcmSends":0}
    result={"status":"COMPLETED","selected":{},"deleted":{},"ignoredWithoutProof":0,"dependenciesRetained":0,"changedSinceRead":0,
        "queryLimitReached":False,"fullRetentionVerified":False,"privateRecordsRead":0,"fcmSends":0,"scope":"EXPIRED_YOUTUBE_METADATA_ONLY"}
    writes=store.writes
    try:
        for collection in ("gameEvents","collectionDocuments"):
            rows=store.list_expired_youtube(collection,now,50);result["selected"][collection]=len(rows)
            if len(rows)==50:result["queryLimitReached"]=True
            for doc_id,row in rows:
                if not youtube_metadata_expired(row,now):result["ignoredWithoutProof"]+=1;continue
                guards={}
                if collection=="gameEvents":
                    # Derived schedule data is not silently orphaned by metadata
                    # deletion; such a source needs the reviewed cleanup path.
                    pickups=store.list_equal("pickupSchedules","eventId",doc_id,1)
                    children=store.list_equal("gameEvents","parentEventId",doc_id,1)
                    if pickups or children:result["dependenciesRetained"]+=1;continue
                else:
                    references=[(key,value)for key,value in store.list_equal("gameEvents","rawSha256",row["rawSha256"],50)
                        if value.get("sourceId")==row.get("sourceId")and value.get("externalKey")==row.get("externalKey")]
                    if references:result["dependenciesRetained"]+=1;continue
                if store.delete_content_if_unchanged(collection+"/"+doc_id,document_fingerprint(row),guards,lambda current,_:youtube_metadata_expired(current,now)):
                    result["deleted"][collection]=result["deleted"].get(collection,0)+1
                else:result["changedSinceRead"]+=1
    except Exception as error:
        result.update(status="PARTIAL",reasonCode=str(error)if isinstance(error,SafetyError)else type(error).__name__)
    if result["queryLimitReached"]or result["dependenciesRetained"]or result["changedSinceRead"]or result["ignoredWithoutProof"]:result["status"]="PARTIAL"
    def finish(old):
        if not old or old.get("token")!=token:return None,False
        return {**old,"token":None,"leaseUntil":None,"nextRunAt":now+timedelta(hours=24),"lastCheckedAt":now,"lastResult":result},True
    try:result["completionRecorded"]=bool(store.mutate(path,finish))
    except SafetyError:result.update(status="PARTIAL",completionRecorded=False,reasonCode="RETENTION_SOFT_BUDGET_EXHAUSTED")
    if not result["completionRecorded"]:result["status"]="PARTIAL"
    result["remoteFirestoreWrites"]=store.writes-writes+1 if store.remote else 0
    return result


def reviewed_title_filter(source):
    policy=source.get("game_title_filter")
    if policy is None:return True
    if (not isinstance(policy,dict) or set(policy)-{"version","aliases","excludedAliases","checkedAt","evidenceUrl","required"}
        or policy.get("version")!=1 or policy.get("required") is not True or not safe_public_url(policy.get("evidenceUrl"))):return False
    values=policy.get("aliases")
    if not isinstance(values,list) or not 1<=len(values)<=20 or any(not isinstance(value,str) or not 2<=len(value.strip())<=150 or re.search(r"[<>\r\n]",value) for value in values):return False
    excluded=policy.get("excludedAliases",[])
    if not isinstance(excluded,list) or len(excluded)>20 or any(not isinstance(value,str) or not 2<=len(value.strip())<=150 or re.search(r"[<>\r\n]",value) for value in excluded):return False
    try:policy_timestamp(policy.get("checkedAt"))
    except (ValueError,TypeError):return False
    return True


def reviewed_paired_ticket_policy(source):
    policy=source.get("paired_ticket_notice")
    if policy is None:return True
    try:
        from official_adapter import paired_ticket_notice_request
        synthetic={"external_key":"518","official_url":policy.get("parentArticleUrl"),"raw_sha256":policy.get("parentSourceSha256"),"original_title":policy.get("parentTitle"),
            "original_description":"블루 아카이브 5주년 페스티벌 10월13일 10월15일 10월14일오후8시 10월16일오후8시 11월7일 8일"}
        paired_ticket_notice_request(source,synthetic)
        return True
    except (ValidationError,ValueError,TypeError,AttributeError):return False


def source_document_allowed(source,title):
    """Literal game names restrict a shared publisher channel to its one game.

    No user registration, guessed franchise alias or body-only mention can
    route another game's PV into this source's public game collection.
    """
    if not reviewed_title_filter(source):return False
    policy=source.get("game_title_filter")
    if policy is None:return True
    def normalized(value):return re.sub(r"\s+"," ",unicodedata.normalize("NFKC",value).casefold()).strip()
    value=normalized(title) if isinstance(title,str) else ""
    def matches(alias):
        needle=normalized(alias)
        if re.fullmatch(r"[a-z0-9 ._:-]+",needle):
            return bool(re.search(r"(?<![a-z0-9])"+re.escape(needle)+r"(?![a-z0-9])",value))
        return needle in value
    if any(matches(alias) for alias in policy.get("excludedAliases",[])):return False
    return any(matches(alias) for alias in policy["aliases"])


def reviewed(source):
    if source.get("news_scope") not in (None,"relevant_game_news_v1"):return False
    common = (source.get("approved_for_staging") is True and source.get("access_status") == "approved"
            and source.get("terms_checked_at")
            and source.get("notice_hosts_verified_at") and source.get("terms_url", "").startswith("https://")
            and source.get("allowed_notice_hosts"))
    if not common or not reviewed_title_filter(source)or not reviewed_paired_ticket_policy(source):
        return False
    if source.get("parser") == "steam_news":
        return source.get("feed_allowlist") == ["steam_community_announcements"]
    if source.get("parser") == "youtube_atom":
        try:
            youtube_request(source)
        except (ValidationError, ValueError, TypeError):
            return False
        return bool(source.get("feed_allowlist") == ["youtube_channel_videos"] and source.get("allowed_notice_hosts") == ["www.youtube.com"]
            and safe_public_url(source.get("publisher_identity_evidence_url")) and source.get("publisher_identity_checked_at")
            and source.get("terms_url") == YOUTUBE_FEED_DOCUMENTATION)
    if source.get("parser")=="youtube_data_api":
        try:
            from official_adapter import youtube_api_request
            youtube_api_request(source)
            policy_timestamp(source.get("youtube_uploads_playlist_verified_at"))
            policy_timestamp(source.get("publisher_identity_checked_at"))
        except (ImportError,ValidationError,ValueError,TypeError):return False
        return bool(source.get("feed_allowlist")==["youtube_uploads_playlist"] and source.get("allowed_notice_hosts")==["www.youtube.com"]
            and safe_public_url(source.get("publisher_identity_evidence_url")) and source.get("terms_url")==YOUTUBE_API_TERMS)
    if source.get("parser")in OFFICIAL_SITE_PARSERS:
        try:
            from official_adapter import official_html_request,official_rss_request
            if source["parser"] == "kuro_cms":
                from official_adapter import kuro_cms_request
                kuro_cms_request(source)
            elif source["parser"] == "genshin_cms":
                from official_adapter import genshin_cms_request
                genshin_cms_request(source)
            else:
                (official_html_request if source["parser"]=="official_html"else official_rss_request)(source)
            policy_timestamp(source.get("publisher_identity_checked_at"))
        except (ImportError,ValidationError,ValueError,TypeError):return False
        return bool(safe_public_url(source.get("publisher_identity_evidence_url"))and source.get("feed_allowlist")==["official_publisher_articles"]
            and(source["parser"]!="official_html"or source.get("game_title_filter")is not None))
    return False  # X API/social HTML scraping are deliberately unsupported in the 0-won worker.


def source_row(source, game_id):
    """Only non-secret registered public provider metadata is exported."""
    return {"id":source["id"], "gameId":game_id, "url":source["url"], "parser":source["parser"],
            "steamAppid":source.get("steam_appid"), "youtubeChannelId":source.get("youtube_channel_id"),
            **({"youtubeUploadsPlaylistId":source.get("youtube_uploads_playlist_id"),"youtubeUploadsPlaylistVerifiedAt":source.get("youtube_uploads_playlist_verified_at")}if source.get("parser")=="youtube_data_api" else {}),
            "publisherIdentityEvidenceUrl":source.get("publisher_identity_evidence_url"), "publisherIdentityCheckedAt":source.get("publisher_identity_checked_at"),
            "enabled":False, "accessStatus":source["access_status"],
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
            "sectionScheduleFormat":source.get("section_schedule_format"),"pairedRecruitmentPeriodApproved":source.get("paired_recruitment_period_approved",False),
            **({"contentLocalization":source["content_localization"]} if source.get("content_localization") is not None else {}),
            **({"originalImageReference":source["original_image_reference"]} if source.get("original_image_reference") is not None else {}),
            **({"reviewedNoticeReferences":source["reviewed_notice_references"]} if source.get("reviewed_notice_references") is not None else {}),
            **({"calendarRolePolicy":source["calendar_role_policy"]} if source.get("calendar_role_policy") is not None else {}),
            **({"localizationRoster":source["localization_roster"]} if source.get("localization_roster") is not None else {}),
            **({"gameTitleFilter":source["game_title_filter"]} if source.get("game_title_filter") is not None else {}),
            **({"articleFormat":source["article_format"]} if source.get("article_format") is not None else {}),
            **({"newsScope":source["news_scope"]} if source.get("news_scope") is not None else {}),
            **({"pairedTicketNotice":source["paired_ticket_notice"]} if source.get("paired_ticket_notice") is not None else {})}


def operating_source(row):
    """Admin-reviewed Firestore source configuration is authoritative in apply mode."""
    return {"id":row["id"],"game_id":row["gameId"],"url":row["url"],"parser":row["parser"],
            "steam_appid":row.get("steamAppid"), "youtube_channel_id":row.get("youtubeChannelId"),
            "youtube_uploads_playlist_id":row.get("youtubeUploadsPlaylistId"),"youtube_uploads_playlist_verified_at":row.get("youtubeUploadsPlaylistVerifiedAt"),
            "publisher_identity_evidence_url":row.get("publisherIdentityEvidenceUrl"), "publisher_identity_checked_at":row.get("publisherIdentityCheckedAt"), "enabled":row.get("enabled",False),
            "approved_for_staging":row.get("approvedForStaging",False),"access_status":row.get("accessStatus"),
            "terms_url":row.get("termsUrl"),"terms_checked_at":row.get("termsCheckedAt"),
            "notice_hosts_verified_at":row.get("noticeHostsVerifiedAt"),"allowed_notice_hosts":row.get("allowedNoticeHosts",[]),
            "feed_allowlist":row.get("feedAllowlist",[]),"collection_interval_minutes":max(15,row.get("intervalMinutes",15)),
            "server_region":row.get("serverRegion") or "UNSPECIFIED","server_label":row.get("serverLabel"),
            "method":"official_feed" if row.get("parser")=="youtube_atom" else "official_api",
            "official_host":"www.youtube.com" if row.get("parser")=="youtube_atom" else "www.googleapis.com" if row.get("parser")=="youtube_data_api" else urlsplit(row["url"]).hostname if row.get("parser")in OFFICIAL_SITE_PARSERS else "api.steampowered.com","request_timeout_seconds":20,"max_response_bytes":2097152,
            "schedule_extraction_approved":row.get("scheduleExtractionApproved",False),
            "schedule_extraction_reviewed_at":row.get("scheduleExtractionReviewedAt"),
            "schedule_extraction_evidence_url":row.get("scheduleExtractionEvidenceUrl"),
            "content_language":row.get("contentLanguage"),"highest_character_rarity":row.get("highestCharacterRarity"),
            "image_reuse":row.get("imageReuse"),"section_schedule_format":row.get("sectionScheduleFormat"),
            "content_localization":row.get("contentLocalization"),"original_image_reference":row.get("originalImageReference"),
            "reviewed_notice_references":row.get("reviewedNoticeReferences"),"calendar_role_policy":row.get("calendarRolePolicy"),
            "localization_roster":row.get("localizationRoster"),
            "game_title_filter":row.get("gameTitleFilter"),"article_format":row.get("articleFormat"),
            "news_scope":row.get("newsScope"),
            "paired_ticket_notice":row.get("pairedTicketNotice"),
            "next_run_at":row.get("nextRunAt"),"last_checked_at":row.get("lastCheckedAt"),
            "paired_recruitment_period_approved":row.get("pairedRecruitmentPeriodApproved",False)}


def recovery_source_policy_hash(source,game_id=None):
    """Bind cached approval to every reviewed source field and extraction policy."""
    if not reviewed(source):raise SafetyError("recovery_source_review_required")
    game_id=game_id or source.get("game_id")
    if not isinstance(game_id,str) or not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,120}",game_id):raise SafetyError("recovery_game_identity_required")
    row=source_row(source,game_id)
    for field in ("enabled","health"):row.pop(field,None)
    def canonical(value):
        if isinstance(value,DATETIME_TYPE):return timestamp(value).isoformat()
        if isinstance(value,dict):return {key:canonical(item)for key,item in value.items()}
        if isinstance(value,list):return [canonical(item)for item in value]
        if isinstance(value,str)and re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})",value):return timestamp(value).isoformat()
        return value
    normalized=operating_source({**row,"enabled":False})
    return digest(canonical({"source":row,"extractionPolicy":source_policy(normalized)}))


RECOVERY_SCOPE="APPROVED_PUBLIC_PARENT_NEWS_CREATE_ONLY"


def recovery_registry_sources(registry):
    sources={}
    for game in registry.get("games",[]):
        for raw in game.get("sources",[]):
            source={**raw,"game_id":game["gameId"]}
            if source.get("id")in sources:raise SafetyError("recovery_duplicate_source_identity")
            if reviewed(source):sources[source["id"]]=source
    return sources


def recovery_known_event(row):
    if not isinstance(row,dict):return None
    event_id=row.get("id");games=row.get("gameIds");source=row.get("sourceId");external=row.get("externalKey");url=safe_public_url(row.get("officialUrl"))
    if (not isinstance(event_id,str)or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,200}",event_id)
        or not isinstance(games,list)or not 1<=len(games)<=10 or any(not isinstance(game,str)or not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,120}",game)for game in games)
        or source is not None and(not isinstance(source,str)or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,200}",source))
        or external is not None and(not isinstance(external,str)or not 1<=len(external)<=512)
        or not url):return None
    return {"id":event_id,"sourceId":source,"gameIds":sorted(set(games)),"externalKey":external,"officialUrl":url}


def remember_recovery_event(store,row):
    state=getattr(store,"public_recovery_state",None)
    known=recovery_known_event(row)
    if not isinstance(state,dict)or not known:return
    rows=state.setdefault("knownEvents",[])
    if not any(item.get("id")==known["id"]for item in rows):
        if len(rows)>=10000:raise SafetyError("recovery_known_event_limit")
        rows.append(known)


def validate_recovery_state(state,registry,*,project_id,now=None,check_expiry=True):
    now=now or datetime.now(UTC)
    allowed_keys={"version","projectId","scope","sourceApprovalObservedAt","sourceApprovalExpiresAt","sources","knownEvents","rotationAfterSourceId"}
    if (not isinstance(state,dict)or set(state)-allowed_keys or state.get("version")!=1 or state.get("scope")!=RECOVERY_SCOPE
        or not project_id or state.get("projectId")!=project_id):raise SafetyError("recovery_manifest_invalid")
    try:observed=timestamp(state.get("sourceApprovalObservedAt"));expires=timestamp(state.get("sourceApprovalExpiresAt"))
    except (ValueError,TypeError):raise SafetyError("recovery_manifest_timestamp_invalid")from None
    if expires!=observed+timedelta(hours=24)or observed>now:raise SafetyError("recovery_manifest_timestamp_invalid")
    if check_expiry and expires<=now:raise SafetyError("recovery_manifest_expired")
    entries=state.get("sources");known=state.get("knownEvents")
    if not isinstance(entries,list)or not 1<=len(entries)<=1000 or not isinstance(known,list)or len(known)>10000:raise SafetyError("recovery_manifest_bounds_invalid")
    if any(not isinstance(row,dict)or set(row)!={"id","sourceId","gameIds","externalKey","officialUrl"}or recovery_known_event(row)!=row for row in known):raise SafetyError("recovery_known_event_invalid")
    current=recovery_registry_sources(registry);approved=[];seen=set()
    for entry in entries:
        if (not isinstance(entry,dict)or set(entry)!={"sourceId","gameId","policySha256"}
            or not isinstance(entry.get("sourceId"),str)or entry["sourceId"]in seen
            or not re.fullmatch(r"[a-f0-9]{64}",str(entry.get("policySha256","")))):raise SafetyError("recovery_source_manifest_invalid")
        seen.add(entry["sourceId"]);source=current.get(entry["sourceId"])
        if source and source["game_id"]==entry["gameId"]and recovery_source_policy_hash(source)==entry["policySha256"]:approved.append(source)
    rotation=state.get("rotationAfterSourceId")
    if rotation is not None and(not isinstance(rotation,str)or rotation not in seen):raise SafetyError("recovery_rotation_invalid")
    if not approved:raise SafetyError("recovery_source_policy_changed")
    return approved


def refresh_recovery_approval(store,registry,public_sources,*,now=None):
    now=now or datetime.now(UTC);current=recovery_registry_sources(registry);approved=[]
    for source_id,row in public_sources:
        local=current.get(source_id)
        if not local or row.get("enabled")is not True:continue
        source=operating_source({**row,"id":source_id})
        if reviewed(source)and recovery_source_policy_hash(source)==recovery_source_policy_hash(local):
            approved.append({"sourceId":source_id,"gameId":local["game_id"],"policySha256":recovery_source_policy_hash(local)})
    old=getattr(store,"public_recovery_state",None)or{};known=[]
    if old.get("projectId")==store.project_id:
        known=[row for row in old.get("knownEvents",[])if recovery_known_event(row)==row][:10000]
    rotation=old.get("rotationAfterSourceId")
    store.public_recovery_state={"version":1,"projectId":store.project_id,"scope":RECOVERY_SCOPE,
        "sourceApprovalObservedAt":now.isoformat(),"sourceApprovalExpiresAt":(now+timedelta(hours=24)).isoformat(),
        "sources":sorted(approved,key=lambda row:row["sourceId"]),"knownEvents":known,
        "rotationAfterSourceId":rotation if any(row["sourceId"]==rotation for row in approved)else None}
    return bool(approved)


def load_public_recovery_state(path,registry,project_id,*,now=None,check_expiry=True):
    candidates=[]
    if path:
        file=Path(path)
        if file.exists()and file.stat().st_size<=8388608:
            try:candidates.append(json.loads(file.read_text(encoding="utf-8-sig")))
            except (ValueError,UnicodeError):pass
    if registry.get("quotaRecoveryBootstrap")is not None:candidates.append(deepcopy(registry["quotaRecoveryBootstrap"]))
    ordered=[];reason="recovery_manifest_missing"
    for state in candidates:
        if not isinstance(state,dict)or state.get("projectId")!=project_id or state.get("scope")!=RECOVERY_SCOPE or state.get("version")!=1:
            reason="recovery_manifest_invalid";continue
        try:ordered.append((timestamp(state.get("sourceApprovalObservedAt")),state))
        except (ValueError,TypeError):reason="recovery_manifest_timestamp_invalid"
    if not ordered:raise SafetyError(reason)
    selected=max(ordered,key=lambda item:item[0])[1]
    # A newer authoritative observation may revoke every approval. Never revive
    # an older bootstrap merely because the newest cache has no eligible source.
    validate_recovery_state(selected,registry,project_id=project_id,now=now,check_expiry=check_expiry)
    # Existing confirmed identities remain conservative exclusions when an older
    # valid bootstrap and cache both exist. This never extends source approval.
    holder=MemoryStore();holder.public_recovery_state=selected
    for _,state in ordered:
        try:validate_recovery_state(state,registry,project_id=project_id,now=now,check_expiry=check_expiry)
        except SafetyError:continue
        for row in state["knownEvents"]:remember_recovery_event(holder,row)
    return selected


def save_public_recovery_state(path,state):
    if not path or not isinstance(state,dict):return
    payload=json.dumps(state,ensure_ascii=False,sort_keys=True,indent=2,default=lambda value:value.isoformat())+"\n"
    if len(payload.encode())>8388608:raise SafetyError("recovery_manifest_size_limit")
    destination=Path(path);destination.parent.mkdir(parents=True,exist_ok=True)
    temporary=destination.with_name(destination.name+".tmp");temporary.write_text(payload,encoding="utf-8");temporary.replace(destination)


def recovery_duplicate(state,row):
    candidate=recovery_known_event(row)
    if not candidate:return True
    for known in state["knownEvents"]:
        if known["id"]==candidate["id"]:return True
        if set(known["gameIds"]).intersection(candidate["gameIds"])and(
            known["officialUrl"]==candidate["officialUrl"]or known["sourceId"]==candidate["sourceId"]and known["externalKey"]is not None and known["externalKey"]==candidate["externalKey"]):return True
    return False


def recovery_parent_news(row,observed):
    if not isinstance(row,dict)or row.get("parentEventId")or row.get("sourceSectionKey")or row.get("newsEligible")is not True:return None
    if content_publication_status(row.get("publishedAt"),observed)!="INCLUDED"or not notice_has_representative_image(row,observed):return None
    result=deepcopy(row)
    for field in ("startsAt","endsAt","reservationStartsAt","reservationEndsAt","ticketSalesAt","originalTimezone","candidateStartsAt","candidateEndsAt","candidateTimezone","calendarPeriod","calendarRole","calendarRoleEvidence","scheduleOrigin","calendarAudience","lastVerifiedAt","announcementKind","pairedTicketNoticeState","venue","dailyOpeningHours","regionBasis","gameServerRegion","scheduleKind"):
        result[field]=None
    result.update(announcementStages=[],announcementStageDateKeys=[])
    result.update(verificationStatus="pending",scheduleStatus="DATE_UNCONFIRMED",status="unconfirmed",category="unclassified",calendarEligible=False,
        sourceClockOnly=False,derivedSectionCount=0,derivedCharacterAnnouncements=0,
        scheduleEvidence={"method":"public_news_recovery_only","reasonCodes":["EXISTING_READS_DEFERRED"]})
    return result


def run_public_news_recovery(registry,state,writer,*,fetch=fetch_source,sleep=time.sleep,now=None,timeout_seconds=480,clock=None):
    wall_clock=clock or (lambda:datetime.now(UTC))
    observed=now or wall_clock()
    sources=sorted(validate_recovery_state(state,registry,project_id=writer.project_id,now=observed),key=lambda source:source["id"])
    expires=timestamp(state["sourceApprovalExpiresAt"])
    approval_remaining=(expires-timestamp(wall_clock())).total_seconds()
    if approval_remaining<=0:raise SafetyError("recovery_manifest_expired")
    deadline=time.monotonic()+min(480,max(1,timeout_seconds),approval_remaining)
    after=state.get("rotationAfterSourceId")
    if after:sources=[source for source in sources if source["id"]>after]+[source for source in sources if source["id"]<=after]
    writer.public_recovery_state=state;results=[];created=0;conflicts=0;duplicates=0;attempted=0;write_blocked=False;write_quota_blocked=False;approval_expired=False
    for source in sources[:20]:
        if time.monotonic()>=deadline or getattr(writer,"create_attempts",0)>=25:break
        attempted+=1;stage=MemoryStore()
        def bounded_fetch(request):
            remaining=deadline-time.monotonic()
            if remaining<=0:raise SafetyError("run_time_limit")
            return fetch({**request,"request_timeout_seconds":max(1,min(request.get("request_timeout_seconds",20),int(remaining)))})
        collected=collect_one(stage,source,max(CONTENT_CUTOFF,observed-timedelta(hours=72)),observed,mode="backfill",fetch=bounded_fetch,sleep=sleep,now=observed,deadline=deadline,max_pages=1)
        state["rotationAfterSourceId"]=source["id"]
        result={"sourceId":source["id"],"status":collected["status"],"metrics":collected.get("metrics",{}),"created":0}
        results.append(result)
        for path,row in list(stage.records.items()):
            if not path.startswith("gameEvents/")or time.monotonic()>=deadline or getattr(writer,"create_attempts",0)>=25:continue
            row=recovery_parent_news(row,observed)
            if row is None:continue
            if recovery_duplicate(state,row):duplicates+=1;continue
            # Recheck real UTC immediately before the conditional RPC as source
            # fetching/image verification may have consumed the approval TTL.
            if timestamp(wall_clock())>=expires:
                approval_expired=True;result["status"]="APPROVAL_EXPIRED";break
            try:changed=writer.create_public_news("gameEvents/"+row["id"],row)
            except ProviderQuotaUnavailable:
                write_blocked=True;write_quota_blocked=True;result["status"]="WRITE_QUOTA_DEFERRED";break
            except Exception as error:
                result.update(status="CREATE_UNCONFIRMED",reasonCode=type(error).__name__);write_blocked=True;break
            if changed:
                created+=1;result["created"]+=1;remember_recovery_event(writer,row)
            else:conflicts+=1
        if write_blocked or approval_expired:break
    return {"status":"PUBLIC_NEWS_RECOVERY_PARTIAL"if created else "PUBLIC_NEWS_RECOVERY_DEFERRED",
        "backend":"FIREBASE","providerReadQuotaBlocked":True,"providerQuotaBlocked":True,"recoveryWriteQuotaBlocked":write_quota_blocked,
        "recoveryWriteUnconfirmed":write_blocked and not write_quota_blocked,
        "recoveryApprovalExpired":approval_expired or timestamp(wall_clock())>=expires,
        "existingUpdatesDeferred":True,"fullSourceCoverageVerified":False,"collectionSucceeded":False,"recoveryCreated":created,
        "sourceAttempts":attempted,"maxRecoverySources":20,"pageBudgetPerSource":1,"maxRecoveryWrites":25,"firestoreReads":0,
        "remoteFirestoreWrites":writer.writes,"createConflicts":conflicts,"knownDuplicatesSkipped":duplicates,
        "sourceApprovalObservedAt":state["sourceApprovalObservedAt"],"sourceApprovalExpiresAt":state["sourceApprovalExpiresAt"],
        "usageAccountingConfirmed":False,"usageAccountingDeferred":True,"fcmSends":0,"results":results,
        "newsOutcome":{"created":created,"updated":0,"eligibleNews":created,"fullSourceCoverageVerified":False},
        "publicNewsChanges":deepcopy(getattr(writer,"public_news_changes",{})),
        **({"publicNewsConfirmedAt":writer.public_news_confirmed_at}if getattr(writer,"public_news_changes",{})else {}),
        "recordedAt":datetime.now(UTC).isoformat()}


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
                        "scheduleExtractionApproved","scheduleExtractionReviewedAt","scheduleExtractionEvidenceUrl","contentLanguage","highestCharacterRarity","imageReuse","sectionScheduleFormat","pairedRecruitmentPeriodApproved",
                        "youtubeChannelId","youtubeUploadsPlaylistId","youtubeUploadsPlaylistVerifiedAt","publisherIdentityEvidenceUrl","publisherIdentityCheckedAt","contentLocalization","originalImageReference","reviewedNoticeReferences","calendarRolePolicy","localizationRoster","gameTitleFilter","articleFormat","pairedTicketNotice","newsScope"] if k in old})
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
    return any((row["schedule_verified"] or region_pending_period(row)) and row["schedule"]["startsAt"]<end and row["schedule"]["endsAt"]>=start for row in scopes)


def region_pending_period(document):
    """A reviewed source can establish dates without establishing the server.

    These are candidate source facts, never confirmed calendar/notification times.
    No other missing proof/conflict reason may use this bounded ingestion path.
    """
    return bool(document["schedule"]["startsAt"] and document["schedule"]["endsAt"] and
        set(document["schedule_reason_codes"])=={"SERVER_REGION_UNCONFIRMED"})


def verified_paired_ticket_section(document,source):
    paired=document.get("_paired_ticket_section")
    if not isinstance(paired,dict)or not reviewed_paired_ticket_policy(source):return None
    evidence=paired.get("evidence")or{};policy=source.get("paired_ticket_notice")or{}
    selected=document.get("_section_title")
    if (evidence.get("method")!="exact_ticketlink_notice_v1"or evidence.get("status")!="verified"
        or evidence.get("sourceSha256")!=document.get("raw_sha256")or document.get("raw_sha256")!=policy.get("parentSourceSha256")
        or evidence.get("sourceTitle")!=selected or evidence.get("parentTitle")!=policy.get("parentTitle")
        or evidence.get("parentArticleUrl")!=document.get("official_url")or evidence.get("noticeUrl")!=policy.get("noticeUrl")
        or not re.fullmatch(r"[a-f0-9]{64}",str(evidence.get("auxiliarySha256","")))or document.get("server_region")!="KR"):
        return None
    try:
        from official_adapter import paired_ticket_notice_request
        parent={**document,"_section_key":None,"original_description":document.get("_raw_original_description","")}
        paired_ticket_notice_request(source,parent)
        if not timestamp(evidence["auxiliaryPublishedAt"])<CONTENT_CUTOFF<=timestamp(document["published_at"]):return None
        stages=paired["announcementStages"]
        if [(row["kind"],row["round"],timestamp(row["startsAt"]))for row in stages]!=[("membership",1,datetime(2026,10,13,3,tzinfo=UTC)),("ticket",1,datetime(2026,10,14,11,tzinfo=UTC)),("membership",2,datetime(2026,10,15,3,tzinfo=UTC)),("ticket",2,datetime(2026,10,16,11,tzinfo=UTC))]:return None
        schedule=paired["schedule"]
        if timestamp(schedule["startsAt"])!=datetime(2026,11,7,1,tzinfo=UTC)or timestamp(schedule["endsAt"])!=datetime(2026,11,8,9,tzinfo=UTC)or schedule.get("originalTimezone")!="Asia/Seoul":return None
    except (ValidationError,ValueError,TypeError,KeyError):return None
    return paired


def prepare_paired_ticket_sections(source,document,observed,fetch,deadline=None):
    """Attach one festival section to its October post; no auxiliary post write."""
    from official_adapter import paired_ticket_notice_request,parse_paired_ticket_notice
    request=paired_ticket_notice_request(source,document)
    if fetch is None or deadline is not None and time.monotonic()>=deadline:raise ValidationError("paired_ticket_probe_unavailable")
    status,raw,_=fetch(request)
    if status!=200:raise ValidationError("paired_ticket_http_"+str(status))
    notice=parse_paired_ticket_notice(raw,source,document)
    key=digest({"pairedTicketNotice":notice["noticeUrl"],"title":notice["title"]})
    section={**document,"original_description":notice["body"],"_raw_original_description":document["original_description"],"_section_key":key,"_section_title":notice["title"],
        "_section_category":"offline","_parent_event_id":event_identity(document,source),"_raw_character_names":[],"event_type_candidates":["offline"],"pickups":[],"source_image_urls":[],
        "server_region":"KR","region_basis":"venue","game_server_region":document["server_region"]}
    evidence={"method":"exact_ticketlink_notice_v1","status":"verified","sourceSha256":document["raw_sha256"],"sourceTitle":notice["title"],
        "parentArticleUrl":document["official_url"],"parentTitle":document["original_title"],"noticeUrl":notice["noticeUrl"],"auxiliarySha256":notice["snapshotSha256"],
        "auxiliaryPublishedAt":notice["publishedAt"].isoformat(),"parentEvidenceText":document["original_description"],"auxiliaryEvidenceText":notice["body"]}
    section["_paired_ticket_section"]={"schedule":{"startsAt":notice["startsAt"],"endsAt":notice["endsAt"],"originalTimezone":notice["timezone"]},
        "evidence":evidence,"announcementStages":notice["stages"],"reservationStartsAt":notice["stages"][2]["startsAt"],"ticketSalesAt":notice["stages"][3]["startsAt"],
        "venue":notice["venue"],"dailyOpeningHours":notice["dailyOpeningHours"],"image":None}
    for candidate in notice["imageUrls"][:1]:
        if deadline is not None and time.monotonic()>=deadline:break
        response,pixels,headers=fetch({"url":candidate,"official_host":"image.toast.com","parser":"official_image_probe","enabled":True,"access_status":"approved", "terms_url":source["terms_url"],"terms_checked_at":source["terms_checked_at"],"request_timeout_seconds":10,"max_response_bytes":2097152})
        dimensions=image_dimensions(pixels)if response==200 and len(pixels)<=2097152 and str(headers.get("content-type","")).split(";",1)[0]in("image/jpeg","image/png","image/webp")else None
        if not dimensions:continue
        pixel_sha=hashlib.sha256(pixels).hexdigest()
        # The exact official notice publishes this verified small poster.
        # This exception is scoped to its fully corroborated parent/4 stages,
        # URL, measured dimensions and checked pixel checksum; unrelated small
        # images and revised bytes still fail the ordinary resolution rule.
        small_exact_poster=(candidate=="https://image.toast.com/aaaaab/ticketlink/TKL_2/5th_open1012.png"and dimensions==(200,247)
            and pixel_sha=="089c80ce7714a9dd862ac9e5a98a1925fcd1cb6628ee311e62387f78a05c4c7d")
        if not small_exact_poster and (not 320<=dimensions[0]<=16384 or not 180<=dimensions[1]<=16384):continue
        section["_paired_ticket_section"]["image"]={"url":candidate,"sourceUrl":document["official_url"],"kind":"EVENT_ART","rightsStatus":"UNCONFIRMED","subjectStatus":"MATCHED",
            "imageVerification":{"status":"verified","method":"SOURCE_IMAGE_DIMENSIONS","width":dimensions[0],"height":dimensions[1],"contentSha256":pixel_sha},
            "sourceReference":{"mode":"OFFICIAL_NOTICE_IMAGE","sourceUrl":notice["noticeUrl"],"checkedAt":timestamp(observed).isoformat(),"allowedImageUrl":candidate},
            "subjectEvidence":{"status":"MATCHED","eventId":event_identity(section,source),"officialUrl":document["official_url"],"kind":"event","method":"source_section",
                "sourceSha256":document["raw_sha256"],"sourceTitle":notice["title"],"evidenceText":notice["title"],"characterNames":[]}}
    if section["_paired_ticket_section"]["image"]is None:raise ValidationError("paired_festival_image_unconfirmed")
    return [section]


def verified_paired_ticket_event(row,source):
    """Rebind the public venue/stages to reviewed source + original parent facts."""
    if (not row or row.get("sourceProvider")!="official-site"or row.get("regionBasis")!="venue"or row.get("serverRegion")!="KR"
        or row.get("gameServerRegion")!=source.get("server_region")or row.get("gameIds")!=["blue-archive"]):return None
    paired={"evidence":row.get("scheduleEvidence"),"announcementStages":row.get("announcementStages"),
        "schedule":{"startsAt":row.get("startsAt"),"endsAt":row.get("endsAt"),"originalTimezone":row.get("originalTimezone")}}
    document={"raw_sha256":row.get("rawSha256"),"external_key":row.get("externalKey"),"original_title":row.get("originalTitle"),"official_url":row.get("officialUrl"),
        "published_at":timestamp(row["publishedAt"]).isoformat()if row.get("publishedAt")else None,"_section_title":row.get("displayTitle")or row.get("sourceSectionTitle"),
        "_raw_original_description":row.get("originalDescription"),"server_region":"KR","_paired_ticket_section":paired}
    return verified_paired_ticket_section(document,source)


def retire_derived_event(store,event_id,row,observed,depth=0):
    if row.get("status")!="cancelled" or row.get("verificationStatus")!="pending":
        store.put("gameEvents/"+event_id,{**row,"verificationStatus":"pending","status":"cancelled","lastChangedAt":observed,"revision":row.get("revision",0)+1})
    for pickup_id,pickup in store.list_equal("pickupSchedules","eventId",event_id,50):
        if pickup.get("status")!="cancelled" or pickup.get("verificationStatus")!="pending":
            store.put("pickupSchedules/"+pickup_id,{**pickup,"verificationStatus":"pending","status":"cancelled","lastChangedAt":observed})
    if depth<1:
        for child_id,child in store.list_equal("gameEvents","parentEventId",event_id,50):
            retire_derived_event(store,child_id,child,observed,depth+1)


def upsert_notice(store, source, document, observed, *, image_fetch=None, deadline=None):
    gate = content_publication_status(document.get("published_at"), observed)
    if gate != "INCLUDED": return "beforeContentCutoffSkipped" if gate == "PUBLISHED_BEFORE_CONTENT_CUTOFF" else "publicationUnconfirmedSkipped"
    if not source_document_allowed(source,document.get("original_title")):return "contentExcluded"
    if source.get("news_scope")=="relevant_game_news_v1" and source.get("parser")in OFFICIAL_SITE_PARSERS and not document.get("_section_key"):
        scoped=classify_notice_content({"rawSha256":document.get("raw_sha256"),"originalTitle":document.get("original_title")})
        if scoped["status"]!="verified"or scoped["kind"]not in YOUTUBE_ADMITTED_NEWS_KINDS:return "contentExcluded"
    if source.get("parser")in YOUTUBE_PARSERS:
        admission=classify_notice_content({"rawSha256":document.get("raw_sha256"),"originalTitle":document.get("original_title")})
        if admission["status"]!="verified"or admission["kind"]not in YOUTUBE_ADMITTED_NEWS_KINDS:
            # The loop and direct reviewed import entry point have the same
            # admission rule. New unrelated uploads cause no snapshot/event
            # write or thumbnail request. Existing public audit rows retain
            # their original fields and receive an explicit stable exclusion.
            path="gameEvents/"+event_identity(document,source)
            existing=store.get(path)
            if not existing:return "contentExcluded"
            proof={"version":1,"method":"explicit_youtube_title_admission_v1","sourceId":source["id"],"videoId":document["external_key"],
                "sourceSha256":document.get("raw_sha256"),"sourceTitle":document.get("original_title"),"classification":admission}
            def exclude(old):
                if (not old or old.get("sourceId")!=source["id"]or old.get("externalKey")!=document["external_key"]
                    or old.get("sourceProvider")not in(None,"youtube")or old.get("officialUrl")!=document.get("official_url")):return None,False
                if old.get("newsEligible")is False and old.get("calendarEligible")is False and old.get("contentIngestionState")=="excluded"and old.get("contentExclusionEvidence")==proof:return None,False
                return {**old,"newsEligible":False,"calendarEligible":False,"contentIngestionState":"excluded", "contentExcludedAt":old.get("contentExcludedAt",observed),"contentExclusionEvidence":proof},True
            return "contentSuppressedUpdated"if store.mutate(path,exclude)else "contentExcluded"
    if store.remote and hasattr(store,"max_writes")and store.max_writes-store.writes<20:raise SafetyError("collector_free_quota_headroom")
    pair=source.get("paired_ticket_notice")or{}
    if pair and not document.get("_section_key")and document.get("official_url")==pair.get("parentArticleUrl"):
        try:
            sections=prepare_paired_ticket_sections(source,document,observed,image_fetch,deadline)
            paired_state=digest([[row["_paired_ticket_section"]["evidence"]["auxiliarySha256"],row["_paired_ticket_section"]["image"]["imageVerification"]["contentSha256"]]for row in sections])
            document={**document,"_paired_sections":sections,"_paired_ticket_state":paired_state}
            document["_paired_sections"]=[{**row,"_paired_ticket_state":paired_state}for row in sections]
        except (ValidationError,ValueError,TypeError)as failure:
            document={**document,"_paired_sections":[],"_paired_ticket_state":"unconfirmed"}
            error_key=digest([source["id"],document["raw_sha256"],"PAIRED_TICKET_NOTICE",str(failure)])
            store.mutate("collectionErrors/"+error_key,lambda old:(None,False)if old else({"sourceId":source["id"],"gameId":source["game_id"],"kind":"PAIRED_TICKET_NOTICE","reasonCode":str(failure)[:120],"createdAt":observed},True))
    document=enrich_document(document,source)
    event_id=event_identity(document,source)
    proof_id=digest([source["id"],document["external_key"],document["raw_sha256"]])
    notice_image=None;notice_image_verification=None;image_probe_due=False
    if source.get("parser")in YOUTUBE_PARSERS and not document.get("_section_key"):
        previous=store.get("gameEvents/"+event_id)
        same=bool(previous and previous.get("rawSha256")==document["raw_sha256"]and previous.get("extractionVersion")==EXTRACTION_VERSION
            and previous.get("extractionPolicySha256")==document["extraction_policy_sha256"]and previous.get("contentIngestionState")!="excluded")
        try:image_probe_due=not same or not previous.get("metadataCheckedAt")or timestamp(previous["metadataCheckedAt"])<=observed-timedelta(hours=24)
        except (ValueError,TypeError):image_probe_due=True
        if image_probe_due:
            notice_image_verification={}
            notice_image=automatic_youtube_notice_image(document,source,event_id,observed,image_fetch=image_fetch,deadline=deadline,probe_status=notice_image_verification)
            if notice_image_verification.get("status")!="verified":
                error_key=digest([source["id"],document["external_key"],document["raw_sha256"],YOUTUBE_NOTICE_IMAGE_POLICY_VERSION,observed.date().isoformat()])
                store.mutate("collectionErrors/"+error_key,lambda old:(None,False)if old else({"sourceId":source["id"],"gameId":source["game_id"],"kind":"OFFICIAL_NOTICE_IMAGE",
                    "reasonCode":"NOTICE_IMAGE_VERIFICATION_FAILED","details":notice_image_verification,"createdAt":observed},True))
        else:notice_image=previous.get("noticeImage");notice_image_verification=previous.get("noticeImageVerification")
    elif source.get("parser")in OFFICIAL_SITE_PARSERS|{"steam_news"}:
        previous=store.get("gameEvents/"+event_id)
        same=bool(previous and previous.get("rawSha256")==document["raw_sha256"] and previous.get("extractionVersion")==EXTRACTION_VERSION
            and previous.get("extractionPolicySha256")==document["extraction_policy_sha256"])
        try:fresh=same and timestamp((previous.get("noticeImage")or previous.get("image")or{}).get("sourceReference",{}).get("checkedAt"))>observed-timedelta(hours=24)
        except (ValueError,TypeError):fresh=False
        if fresh:
            notice_image=previous.get("noticeImage")or previous.get("image")
        else:notice_image=automatic_official_notice_image(document,source,event_id,observed,image_fetch,deadline)
        if document.get("paired_ticket_section"):
            notice_image=(previous.get("noticeImage")or previous.get("image"))if fresh and previous.get("pairedTicketNoticeState")==document.get("_paired_ticket_state")else document["paired_ticket_section"]["image"]
    video_metadata=youtube_document_metadata(document,source,observed)
    if len(json.dumps(document["raw_json"],ensure_ascii=False).encode())>700000:
        raise SafetyError("raw_document_exceeds_safe_firestore_size")
    def raw_update(old):
        if old:
            if video_metadata["sourceProvider"]=="youtube":
                try:refresh=not old.get("metadataCheckedAt") or timestamp(old["metadataCheckedAt"])<=observed-timedelta(hours=24)
                except (ValueError,TypeError):refresh=True
                if refresh:return {**old,**video_metadata},True
            return None,False
        return {"sourceId":source["id"],"externalKey":document["external_key"],"rawSha256":document["raw_sha256"],
          "rawJson":document["raw_json"],"publishedAt":timestamp(document["published_at"]) if document["published_at"] else None,
          "rawPublicationTimestamp":document["raw_publication_timestamp"],"sourceUpdatedAt":timestamp(document["source_updated_at"]) if document.get("source_updated_at") else None,"observedAt":observed,
          "officialUrl":document["official_url"],**video_metadata},True
    store.mutate("collectionDocuments/"+proof_id,raw_update)
    def update(old):
        if old and old.get("rawSha256") == document["raw_sha256"] and old.get("extractionVersion")==EXTRACTION_VERSION and old.get("extractionPolicySha256")==document["extraction_policy_sha256"] and old.get("categoryCandidate")==document["category_candidate"] and old.get("pairedTicketNoticeState")==document.get("_paired_ticket_state")and not(source.get("parser")in YOUTUBE_PARSERS and old.get("contentIngestionState")=="excluded"):
            if old.get("verificationStatus")=="verified" and old.get("status") not in ("cancelled","postponed") and old.get("startsAt") and old.get("endsAt"):
                state="ended" if timestamp(old["endsAt"])<=observed else "ongoing" if timestamp(old["startsAt"])<=observed else "upcoming"
                if state!=old.get("status"):
                    return apply_notice_content_policy({**old,"status":state,"lastCheckedAt":observed,"lastChangedAt":observed,"revision":old.get("revision",0)+1},observed),"updated"
            if source.get("parser")in YOUTUBE_PARSERS:
                checked=old.get("metadataCheckedAt")
                try: refresh=checked is None or timestamp(checked)<=observed-timedelta(hours=24)
                except (ValueError,TypeError): refresh=True
                if refresh:
                    return apply_notice_content_policy({**old,"noticeImage":notice_image,"image":notice_image if old.get("categoryCandidate")!="recruitment" and not old.get("derivedCharacterAnnouncements") else old.get("image"),
                        "noticeImageVerification":notice_image_verification,**video_metadata,"lastCheckedAt":observed},observed),"updated"
            elif source.get("parser")in OFFICIAL_SITE_PARSERS|{"steam_news"} and notice_image!=old.get("noticeImage"):
                return apply_notice_content_policy({**old,"noticeImage":notice_image,"image":notice_image if old.get("categoryCandidate")!="recruitment" and not old.get("derivedCharacterAnnouncements")else old.get("image"),
                    **video_metadata,"lastCheckedAt":observed},observed),"updated"
            return None,"unchanged"
        retained_image=retained_source_reference(old,document,event_id)
        original_language,language_evidence=notice_language(old,document,source)
        row={**(old or {}),"id":event_id,"sourceId":source["id"],"externalKey":document["external_key"],
             "gameIds":[source["game_id"]],"serverRegion":document["server_region"],"serverLabel":document["server_label"],
             "title":document.get("_section_title") or document["original_title"],"description":document["original_description"],
             "displayTitle":document.get("_section_title"),"sourceSectionTitle":document.get("_section_title"),
             "sourceSectionKey":document.get("_section_key"),"parentEventId":document.get("_parent_event_id"),
             "characterNames":document.get("_raw_character_names",[]),
             "originalTitle":document["original_title"],"originalDescription":document.get("_raw_original_description") or document["original_description"],
             "originalLanguage":original_language,"languageEvidence":language_evidence,"officialUrl":document["official_url"],
             "publishedAt":timestamp(document["published_at"]) if document["published_at"] else None,
             "sourceUpdatedAt":timestamp(document["source_updated_at"]) if document["source_updated_at"] else None,
             "rawSha256":document["raw_sha256"],"eventTypeCandidates":document["event_type_candidates"],
             "categoryCandidate":document["category_candidate"],
             "firstCheckedAt":old.get("firstCheckedAt",observed) if old else observed,
             "lastCheckedAt":observed,"lastChangedAt":observed,"verificationStatus":"pending",
             "extractionVersion":EXTRACTION_VERSION,"extractionPolicySha256":document["extraction_policy_sha256"],
             "scheduleEvidence":{"method":"explicit_labelled_timestamps","evidenceText":document["schedule"]["evidenceText"],
                 "reasonCodes":document["schedule_reason_codes"]},
             "candidateStartsAt":document["schedule"]["startsAt"] if region_pending_period(document) else None,
             "candidateEndsAt":document["schedule"]["endsAt"] if region_pending_period(document) else None,
             "candidateTimezone":document["schedule"]["originalTimezone"] if region_pending_period(document) else None,
             "noticeImage":notice_image,
             "noticeImageVerification":notice_image_verification,
             **video_metadata,
             "imageCandidates":document["image_candidates"],"image":document["approved_image"] or document["referenced_image"] or reviewed_notice_image(document,source,event_id) or retained_image or (notice_image if document["category_candidate"]!="recruitment" and not document["highest_rarity_targets"] else None),
             "sourceImageOverride":old.get("sourceImageOverride") if retained_image and not (document["approved_image"] or document["referenced_image"]) else None,
             "announcementKind":"aggregate" if document["highest_rarity_targets"] else None,
             "derivedCharacterAnnouncements":len(document["highest_rarity_targets"]),
             "derivedSectionCount":len(document["section_documents"]),
             "pairedTicketNoticeState":document.get("_paired_ticket_state"),
             "regionBasis":document.get("region_basis"),"gameServerRegion":document.get("game_server_region"),
             "revision":(old.get("revision",0) if old else 0)+1}
        if source.get("parser")in YOUTUBE_PARSERS:row["contentIngestionState"]="admitted"
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
        row["scheduleKind"] = document["schedule_kind"]
        if document.get("source_live_point") and row.get("verificationStatus") == "verified":
            point = document["source_live_point"]
            row.update(calendarAudience="GLOBAL_ANNOUNCEMENT",scheduleOrigin="automatic")
            row["scheduleEvidence"] = {"method": "youtube_live_details_v1", "sourceSha256": row["rawSha256"],
                "sourceTitle": row.get("displayTitle") or row.get("sourceSectionTitle") or row["originalTitle"],
                "officialUrl": row["officialUrl"], "videoId": row["sourceVideoId"], "channelId": row["sourceChannelId"],
                "providerField": point["providerField"], "providerValue": point["providerValue"], "reasonCodes": [],
                "evidenceText": document["schedule"]["evidenceText"]}
        else:
            row["calendarAudience"] = None
        if document.get("paired_ticket_section")and document["schedule_verified"]:
            paired=document["paired_ticket_section"]
            row.update(reservationStartsAt=paired["reservationStartsAt"],ticketSalesAt=paired["ticketSalesAt"],venue=paired["venue"],dailyOpeningHours=paired["dailyOpeningHours"],
                announcementStages=paired["announcementStages"],announcementStageDateKeys=sorted({timestamp(stage["startsAt"]).date().isoformat()for stage in paired["announcementStages"]}),scheduleEvidence=paired["evidence"])
        row=reviewed_calendar_role(localize_collected_row(row,source,observed),source)
        same_reviewed_period=bool(old and old.get("sourceId")==source["id"] and old.get("externalKey")==document["external_key"] and old.get("rawSha256")==document["raw_sha256"]
            and old.get("originalTitle")==document["original_title"] and old.get("officialUrl")==document["official_url"]
            and verified_calendar_day_period(old,observed))
        if same_reviewed_period:
            for key in ("calendarPeriod","calendarRole","calendarRoleEvidence","announcementKind","characterNames","rarityLabel","displayTitle","sourceSectionTitle","image","noticeImage","sourceImageOverride"):
                if key in old:row[key]=deepcopy(old[key])
        elif old and old.get("calendarPeriod"):
            row.update(calendarPeriod=None,calendarRole=None,calendarRoleEvidence=None)
        row=apply_notice_content_policy(announcement_metadata(row,old,observed),observed)
        if len(json.dumps(row,ensure_ascii=False,default=lambda value:value.isoformat()).encode())>900000:
            raise SafetyError("event_document_exceeds_safe_firestore_size")
        return row,"updated" if old else "created"
    confirmed_parent={}
    def tracked_update(old):
        if old:remember_recovery_event(store,old)  # This read completed inside the transaction.
        value,result=update(old)
        confirmed_parent.clear()
        if value is not None:confirmed_parent.update(value)
        return value,result
    outcome=store.mutate("gameEvents/"+event_id,tracked_update)
    if confirmed_parent:remember_recovery_event(store,confirmed_parent)  # Commit returned successfully.
    for pickup in document.get("pickups",[]):
        pickup_id=digest({"event":event_id,"game":source["game_id"],"region":pickup["server_region"],"server":pickup["server_label"],"kind":pickup["pickup_kind"]})
        def update_pickup(old):
            if outcome == "unchanged" and old:
                return None,False
            current_event=store.get("gameEvents/"+event_id)
            row={**(old or {}),"id":pickup_id,"eventId":event_id,"gameId":source["game_id"],"sourceId":source["id"],
                 "serverRegion":pickup["server_region"],"serverLabel":pickup["server_label"],
                 "officialUrl":pickup["official_url"],"verificationStatus":"pending","lastCheckedAt":observed,
                 "imageCandidates":document["image_candidates"],"image":current_event.get("image"),
                 "status":"cancelled" if document["cancelled"] else "unconfirmed",
                 "candidateStartsAt":document["schedule"]["startsAt"] if region_pending_period(document) else None,
                 "candidateEndsAt":document["schedule"]["endsAt"] if region_pending_period(document) else None,
                 "candidateTimezone":document["schedule"]["originalTimezone"] if region_pending_period(document) else None,
                 "firstCheckedAt":old.get("firstCheckedAt",observed) if old else observed}
            row.update({key:current_event.get(key) for key in ("originalTitle","originalDescription","originalLanguage","rawSha256","revision",
                "displayTitle","sourceSectionTitle","translations","contentLocalization","calendarRole","calendarRoleEvidence","publishedAt","sourceUpdatedAt","contentKind","contentClassification","newsEligible","calendarEligible")})
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
            upsert_notice(store,source,section,observed,image_fetch=image_fetch,deadline=deadline)
        for target in document["highest_rarity_targets"]:
            # A structured recruitment section will create its own independent
            # character child. Do not create a second equivalent child from the
            # same parent overview and send the same debut announcement twice.
            if target.get("evidenceMethod")!="gf2_new_doll_block_v1" and any(section.get("_section_category")=="recruitment" and target["name"] in section.get("_raw_character_names",[]) for section in document["section_documents"]):continue
            child_id=digest({"parent":event_id,"announcementKind":target["announcementKind"],"character":target["name"]})
            child_images,child_artwork=character_image(document["image_candidates"],target,child_id)
            active_children.add(child_id)
            def child_update(old):
                title=target["name"]
                retained_image=retained_source_reference(old,document,child_id,title=target["name"],characters=[target["name"]],announcement_kind=target["announcementKind"])
                row={**parent,"id":child_id,"parentEventId":event_id,"announcementKind":target["announcementKind"],
                    "noticeImage":None,
                    "noticeImageVerification":None,
                    "sourceProvider":None,"sourceVideoId":None,"sourceChannelId":None,"metadataCheckedAt":None,"metadataExpiresAt":None,
                    "rarityLabel":target["rarityLabel"],"characters":[target["name"]],"characterEvidenceText":target["evidenceText"],
                    "characterEvidenceMethod":target.get("evidenceMethod","literal_same_line_v1"),
                    "title":title,"displayTitle":title,"characterNames":[target["name"]],"originalTitle":document["original_title"],"derivedCharacterAnnouncements":0,
                    "imageCandidates":child_images,"image":child_artwork or original_subject_image(child_images,source,document.get("observed_at")) or retained_image,
                    "sourceImageOverride":old.get("sourceImageOverride") if retained_image and not original_subject_image(child_images,source,document.get("observed_at")) else None,
                    "firstCheckedAt":old.get("firstCheckedAt",observed) if old else observed,
                    "revision":old.get("revision",0)+1 if old else 1}
                own_title=parent.get("sourceSectionTitle") or parent.get("displayTitle") or parent.get("originalTitle") or ""
                shared_recruitment_period=(target.get("evidenceMethod")!="gf2_new_doll_block_v1" and parent.get("categoryCandidate")=="recruitment"
                    and not parent.get("derivedSectionCount") and bool(PICKUP_IMAGE_WORDS.search(own_title)))
                if not shared_recruitment_period:
                    inactive=parent.get("status") if parent.get("status") in ("cancelled","canceled","postponed") else "unconfirmed"
                    row.update(categoryCandidate="recruitment",category="recruitment",eventTypeCandidates=["pickup"],
                        startsAt=None,endsAt=None,reservationStartsAt=None,reservationEndsAt=None,ticketSalesAt=None,
                        originalTimezone=None,candidateStartsAt=None,candidateEndsAt=None,candidateTimezone=None,lastVerifiedAt=None,
                        verificationStatus="pending",scheduleStatus="CANCELLED" if inactive in ("cancelled","canceled") else "DATE_UNCONFIRMED",status=inactive,scheduleOrigin=None,
                        scheduleEvidence={"method":"source_new_doll_block" if target.get("evidenceMethod")=="gf2_new_doll_block_v1" else "source_character_without_scoped_period",
                            "evidenceText":target["evidenceText"],"reasonCodes":["CHARACTER_RECRUITMENT_PERIOD_UNCONFIRMED"]})
                return apply_notice_content_policy(announcement_metadata(reviewed_calendar_role(localize_collected_row(row,source,observed),source),old,observed),observed),True
            store.mutate("gameEvents/"+child_id,child_update)
            pickup_id=digest({"event":child_id,"kind":"character"})
            def child_pickup(old):
                current_child=store.get("gameEvents/"+child_id)
                row={"id":pickup_id,"eventId":child_id,"parentEventId":event_id,"gameId":source["game_id"],
                    "sourceId":source["id"],"serverRegion":parent["serverRegion"],"serverLabel":parent["serverLabel"],
                    "officialUrl":parent["officialUrl"],"pickupKind":"character","characters":[target["name"]],"weapons":[],
                    "announcementKind":target["announcementKind"],"rarityLabel":target["rarityLabel"],
                    "startsAt":current_child.get("startsAt"),"endsAt":current_child.get("endsAt"),"originalTimezone":current_child.get("originalTimezone"),
                    "candidateStartsAt":current_child.get("candidateStartsAt"),"candidateEndsAt":current_child.get("candidateEndsAt"),"candidateTimezone":current_child.get("candidateTimezone"),
                    "verificationStatus":current_child["verificationStatus"],"status":current_child["status"],"image":current_child.get("image"),
                    "imageCandidates":child_images,"lastCheckedAt":observed,"lastChangedAt":observed,
                    "firstCheckedAt":old.get("firstCheckedAt",observed) if old else observed,
                    "lastVerifiedAt":current_child.get("lastVerifiedAt"),"scheduleOrigin":current_child.get("scheduleOrigin")}
                row.update({key:current_child.get(key) for key in ("originalTitle","originalDescription","originalLanguage","rawSha256","revision",
                    "displayTitle","sourceSectionTitle","translations","contentLocalization","calendarRole","calendarRoleEvidence","publishedAt","sourceUpdatedAt","contentKind","contentClassification","newsEligible","calendarEligible")})
                return row,True
            store.mutate("pickupSchedules/"+pickup_id,child_pickup)
        # Changed text removing a target must stop its old automatic calendar/push.
        for child_id,old in store.list_equal("gameEvents","parentEventId",event_id,50):
            if child_id not in active_children:
                retire_derived_event(store,child_id,old,observed)
    return outcome


def _claim_source_mode(store, source_id, now, mode, token, automatic_reverify=False, automatic_month_backfill=False):
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
        month=old.get("contentBackfill") or {}
        if automatic_month_backfill and mode=="incremental" and month.get("complete") is not True:
            selected_mode="content-backfill"
        elif automatic_reverify and mode=="incremental" and (not old.get("lastReverifiedAt") or timestamp(old["lastReverifiedAt"])<=now-timedelta(hours=24)):
            selected_mode="reverify"
        if selected_mode == "reverify" and old.get("lastReverifiedAt") and timestamp(old["lastReverifiedAt"])>now-timedelta(hours=24):
            return None,False
        return {**old,"token":token,"leaseUntil":now+timedelta(minutes=12),"claimedAt":now,"mode":selected_mode},selected_mode
    return store.mutate("collectorLocks/"+source_id,update)


def claim_source(store, source_id, now, mode, token):
    return bool(_claim_source_mode(store,source_id,now,mode,token))


def checkpoint_month_backfill(store,source,token,start,end,*,page_token=None,cursor=None,complete=False,pages=0):
    """Commit only fully processed pages under the active source lease.

    Tokens are public provider cursors, not credentials. A stopped worker starts
    at its last committed page rather than refetching its first ten pages. Source
    policy changes invalidate the cursor; no user selection controls this state.
    """
    def update(old):
        if not old or old.get("token")!=token:raise SafetyError("source_lease_lost")
        prior=old.get("contentBackfill") or {}
        same=prior.get("policySha256")==digest(source_policy(source)) and prior.get("windowStart")==start and prior.get("windowEnd")==end
        state={"version":1,"policySha256":digest(source_policy(source)),"windowStart":start,"windowEnd":end,
            "pageToken":page_token,"providerCursor":cursor,"complete":bool(complete),"pagesCompleted":(prior.get("pagesCompleted",0) if same else 0)+pages}
        return {**old,"contentBackfill":state},True
    store.mutate("collectorLocks/"+source["id"],update)


QUOTA_DEFERRAL_REASONS = frozenset({"collector_free_quota_headroom", "worker_run_soft_budget_exhausted", "collector_daily_soft_budget_exhausted"})


def finish_source(store, source, token, now, success, error, http_status, mode, retry_after=0, metrics=None):
    lock_path="collectorLocks/"+source["id"]
    def update(old):
        if not old or old.get("token")!=token:
            return None,False
        count=old.get("consecutiveFailures",0)if error in QUOTA_DEFERRAL_REASONS else 0 if success else old.get("consecutiveFailures",0)+1
        delay=max(max(15,source.get("collection_interval_minutes",15))*60,
                  min(86400,900*2**min(count,6)) if not success else 0,retry_after)
        row={**old,"leaseUntil":None,"token":None,"consecutiveFailures":count,"nextRunAt":now+timedelta(seconds=delay)}
        if success and (mode!="content-backfill" or (metrics or {}).get("freshHeadFetched")):
            row["lastFreshCheckedAt"]=now
        if success and mode=="reverify":row["lastReverifiedAt"]=now
        return row,True
    if not store.mutate(lock_path,update):
        raise SafetyError("source_lease_lost")
    def health(old):
        if not old:return None,False
        finished_lock=store.get(lock_path)
        row={**old,"lastCheckedAt":now,"nextRunAt":finished_lock["nextRunAt"],"health":{"status":"DEFERRED_QUOTA_BUDGET"if error in QUOTA_DEFERRAL_REASONS else "success" if success else "FETCH_FAILED",
             "httpStatus":http_status,"consecutiveFailures":finished_lock["consecutiveFailures"],"reasonCode":error}}
        if finished_lock.get("lastFreshCheckedAt"):row["lastFreshCheckedAt"]=finished_lock["lastFreshCheckedAt"]
        if metrics is not None:
            row["lastCollectionOutcome"]={key:metrics.get(key) for key in ("created","updated","unchanged","newsEligibleDocuments","newsBlockedByImage","newsBlockedByClassification","latestPublishedAtSeen","latestRetainedPublishedAt")}
        if success:row["lastSuccessAt"]=now
        else:row["lastErrorAt"]=now
        if success and mode=="reverify":row["lastReverifiedAt"]=now
        return row,True
    store.mutate("gameSources/"+source["id"],health)


def collect_one(store, source, start, end, mode="backfill", fetch=fetch_source, sleep=time.sleep, now=None, deadline=None, max_pages=10):
    observed=now or datetime.now(UTC)
    if not reviewed(source):raise SafetyError("source_review_required")
    content_backfill=mode=="content-backfill" and start==CONTENT_CUTOFF and end<=observed
    if end<=start or end-start>timedelta(hours=72) and not content_backfill:raise SafetyError("window_must_be_at_most_72_hours")
    if type(max_pages) is not int or not 1<=max_pages<=200:raise SafetyError("bounded_page_budget_required")
    start=max(start,CONTENT_CUTOFF)
    if end<=start:raise SafetyError("content_window_before_publication_cutoff")
    if source.get("parser")=="youtube_data_api" and not os.environ.get("YOUTUBE_API_KEY"):
        return {"status":"DEFERRED","sourceId":source["id"],"reasonCode":"YOUTUBE_API_KEY_NOT_CONFIGURED","writes":0,"httpRequests":0}
    if store.remote:
        config=store.get("gameSources/"+source["id"])
        if not config or config.get("enabled") is not True:
            return {"status":"DISABLED","sourceId":source["id"],"writes":0}
    token=uuid.uuid4().hex
    requested_mode=mode
    selected_mode=_claim_source_mode(store,source["id"],observed,mode,token,automatic_reverify=True,automatic_month_backfill=store.remote)
    if not selected_mode:
        return {"status":"DEFERRED","sourceId":source["id"],"writes":0}
    mode=selected_mode
    fresh_end=end
    # Reserve the small control tail even when a content page exhausts its
    # budget. This never bypasses the store's run or daily soft limits.
    budget_store=store if hasattr(store,"reserve_budget") else getattr(store,"store",None)
    reservation=budget_store.reserve_budget(reads=12,writes=4)if hasattr(budget_store,"reserve_budget")else nullcontext()
    if mode=="content-backfill":start=CONTENT_CUTOFF
    previous={};matching=False
    run_id=digest([source["id"],token]);started_writes=store.writes
    metrics={"pagesFetched":0,"itemsSeen":0,"created":0,"updated":0,"unchanged":0,"verifiedSchedules":0,
             "knownEventsForReverify":0,"knownEventsQueryLimitReached":False,"olderUnseenSkipped":0,"ongoingOlderIncluded":0,
             "verifiedSectionSchedules":0,"derivedChildrenQueryLimitReached":False}
    metrics.update(candidateSchedules=0,candidateSectionSchedules=0,beforeContentCutoffSkipped=0,publicationUnconfirmedSkipped=0,contentExcluded=0,contentSuppressedUpdated=0,videoDetailsRequests=0,videoDetailsFailures=0)
    metrics.update(afterContentMonthSkipped=0,backfillResumeUsed=False,backfillCursorCommitted=False,sharedChannelOtherGameSkipped=0,
        freshHeadFetched=False,newsEligibleDocuments=0,newsBlockedByImage=0,newsBlockedByClassification=0,
        latestPublishedAtSeen=None,latestRetainedPublishedAt=None,newsMetricScope="UNIQUE_PARENT_EVENTS_PROCESSED_PER_SOURCE_RUN")
    success,error,http_status,retry_after=False,None,None,0
    reported_event_ids=set()
    cursor=1 if source["parser"]=="official_html" or source["parser"]in OFFICIAL_CMS_PARSERS else int(end.timestamp())+1;page_token=None;seen_page_tokens=set()
    try:
        with reservation:
            if mode=="content-backfill":
                previous=(store.get("collectorLocks/"+source["id"]) or {}).get("contentBackfill") or {}
                try:
                    matching=(previous.get("version")==1 and previous.get("policySha256")==digest(source_policy(source))
                        and timestamp(previous.get("windowStart"))==start and start<timestamp(previous.get("windowEnd"))<=end
                        and previous.get("complete") is not True)
                except (ValueError,TypeError):matching=False
                if matching:end=timestamp(previous["windowEnd"])
            metrics["backfillResumeUsed"]=bool(matching)
            if matching:
                if source["parser"]=="youtube_data_api" and previous.get("pageToken"):
                    page_token=previous["pageToken"]
                    if not isinstance(page_token,str) or not re.fullmatch(r"[A-Za-z0-9_+=./:-]{1,512}",page_token):raise SafetyError("backfill_cursor_invalid")
                    seen_page_tokens.add(page_token)
                elif type(previous.get("providerCursor")) is int:cursor=previous["providerCursor"]
            pending_resume=None
            if matching and requested_mode=="incremental" and (page_token is not None or cursor!=(1 if source["parser"] in OFFICIAL_SITE_PARSERS else int(end.timestamp())+1)):
                # A frozen archive cursor must not hide uploads published since the
                # previous attempt. The fresh head consumes the same page budget.
                pending_resume=(cursor,page_token,end)
                cursor=1 if source["parser"] in OFFICIAL_SITE_PARSERS else int(fresh_end.timestamp())+1
                page_token=None;end=fresh_end
            source,roster_metrics=refresh_localization_roster(store,source,observed,fetch,sleep,deadline)
            metrics['localizationRoster']=roster_metrics
            if roster_metrics['status'] in ('FETCH_FAILED','SOURCE_POLICY_CHANGED','REVIEW_REQUIRED'):
                store.put('collectionErrors/'+run_id+'-roster',{'sourceId':source['id'],'gameId':source['game_id'],'kind':'LOCALIZATION_ROSTER',
                    'reasonCode':roster_metrics.get('reasonCode')or roster_metrics['status'],'createdAt':observed})
            known_event_ids=set()
            if mode=="reverify":
                known_event_ids={event_id for event_id,_ in store.list_equal("gameEvents","sourceId",source["id"],50)}
                metrics.update(knownEventsForReverify=len(known_event_ids),knownEventsQueryLimitReached=len(known_event_ids)==50)
            for page_index in range(max_pages):
                if deadline is not None and time.monotonic()>=deadline:
                    if mode=="content-backfill" and metrics["pagesFetched"] and metrics["backfillCursorCommitted"]:
                        success=True;metrics.update(pageLimitReached=True,timeBudgetReached=True,providerPaginationComplete=False);break
                    raise SafetyError("run_time_limit")
                sleep(1)
                if source["parser"]=="youtube_data_api":
                    from official_adapter import youtube_api_request,parse_youtube_api_documents
                    request=youtube_api_request(source,page_token=page_token,count=50)
                elif source["parser"]=="official_html":
                    from official_adapter import official_html_request,parse_official_html_index,parse_official_html_article
                    request=official_html_request(source,page=cursor)
                elif source["parser"]=="official_rss":
                    from official_adapter import official_rss_request,parse_official_rss_documents
                    request=official_rss_request(source)
                elif source["parser"]in OFFICIAL_CMS_PARSERS:
                    from official_adapter import kuro_cms_request,parse_kuro_cms_index,parse_kuro_cms_article,genshin_cms_request,parse_genshin_cms_index,parse_genshin_cms_article
                    cms_request,cms_index,cms_article=(kuro_cms_request,parse_kuro_cms_index,parse_kuro_cms_article)if source["parser"]=="kuro_cms"else(genshin_cms_request,parse_genshin_cms_index,parse_genshin_cms_article)
                    request=cms_request(source,**({"page":cursor}if source["parser"]=="genshin_cms"else{}))
                else:request=youtube_request(source) if source["parser"]=="youtube_atom" else steam_request({**source,"enabled":True},cursor,50)
                http_status,raw,headers=fetch(request);metrics["pagesFetched"]+=1
                if http_status!=200:
                    retry_after=retry_after_seconds(headers.get("retry-after"))
                    raise SafetyError("source_http_"+str(http_status))
                if source["parser"]in OFFICIAL_CMS_PARSERS:
                    entries=cms_index(raw,source);documents=[]
                    dates_for_index=[entry["publishedDate"]for entry in entries]
                    for entry in entries:
                        date=datetime.strptime(entry["publishedDate"],"%Y.%m.%d").replace(tzinfo=UTC)
                        if date.date()<CONTENT_CUTOFF.astimezone(timezone(timedelta(hours=9))).date()or date.date()>observed.astimezone(ZoneInfo(source["article_format"]["timezone"])).date():continue
                        if not source_document_allowed(source,entry["title"]):metrics["sharedChannelOtherGameSkipped"]+=1;continue
                        if source.get("news_scope")=="relevant_game_news_v1" and classify_notice_content({"originalTitle":entry["title"],"rawSha256":"a"*64})["kind"]=="other":metrics["contentExcluded"]+=1;continue
                        if metrics.get("publisherArticleRequests",0)>=40:raise SafetyError("publisher_article_request_limit")
                        if deadline is not None and time.monotonic()>=deadline:raise SafetyError("run_time_limit")
                        sleep(1);article_status,article_raw,_=fetch(cms_request(source,article_id=entry["id"]))
                        metrics["publisherArticleRequests"]=metrics.get("publisherArticleRequests",0)+1
                        if article_status!=200:raise SafetyError("publisher_article_http_"+str(article_status))
                        article=cms_article(article_raw,source,entry["url"],observed.isoformat())
                        if article.get("publication_local_date")!=entry["publishedDate"].replace(".","-")or plain_notice(article["original_title"])!=plain_notice(entry["title"]):raise SafetyError("publisher_list_article_identity_changed")
                        documents.append(article)
                    parsed={"items_seen":len(entries),"items_accepted":len(documents),"items_skipped":len(entries)-len(documents),"skip_reasons":[],
                        "publication_cutoff_reached":bool(dates_for_index and min(dates_for_index)<CONTENT_CUTOFF.astimezone(timezone(timedelta(hours=9))).strftime("%Y.%m.%d")),"coverage_reason":"PUBLIC_PUBLISHER_CMS_INDEX"}
                elif source["parser"]=="official_html":
                    entries=parse_official_html_index(raw,source,cursor);documents=[];dates_for_index=[entry["publishedDate"]for entry in entries]
                    if dates_for_index!=sorted(dates_for_index,reverse=True):raise SafetyError("publisher_board_publication_order_unconfirmed")
                    for entry in entries:
                        if not source_document_allowed(source,entry["title"]):metrics["sharedChannelOtherGameSkipped"]+=1;continue
                        date=datetime.strptime(entry["publishedDate"],"%Y.%m.%d").replace(tzinfo=timezone(timedelta(hours=9))).astimezone(UTC)
                        if date<CONTENT_CUTOFF or date>=end:continue
                        if deadline is not None and time.monotonic()>=deadline:raise SafetyError("run_time_limit")
                        sleep(1);article_status,article_raw,_=fetch(official_html_request(source,article_url=entry["url"]))
                        metrics["publisherArticleRequests"]=metrics.get("publisherArticleRequests",0)+1
                        if article_status!=200:raise SafetyError("publisher_article_http_"+str(article_status))
                        article=parse_official_html_article(article_raw,source,entry["url"],observed.isoformat())
                        if article["raw_publication_timestamp"]!=entry["publishedDate"]or plain_notice(article["original_title"])!=plain_notice(entry["title"]):raise SafetyError("publisher_list_article_identity_changed")
                        documents.append(article)
                    parsed={"items_seen":len(entries),"items_accepted":len(documents),"items_skipped":len(entries)-len(documents),"skip_reasons":[],
                        "publication_cutoff_reached":bool(dates_for_index and dates_for_index[-1]<CONTENT_CUTOFF.astimezone(timezone(timedelta(hours=9))).strftime("%Y.%m.%d"))}
                else:
                    documents,parsed=(parse_youtube_api_documents(raw,source,observed.isoformat()) if source["parser"]=="youtube_data_api" else parse_youtube_documents(raw,source,observed.isoformat()) if source["parser"]=="youtube_atom" else parse_official_rss_documents(raw,source,observed.isoformat(),published_start=CONTENT_CUTOFF,published_end=None) if source["parser"]=="official_rss" else parse_steam_documents(raw,source,observed.isoformat()))
                if source["parser"]=="youtube_atom":
                    metrics.update(windowCoverageVerified=False,coverageReason=parsed["coverage_reason"])
                if source["parser"]=="official_rss":
                    metrics.update(windowCoverageVerified=False,coverageReason=parsed["coverage_reason"])
                    metrics["beforeContentCutoffSkipped"]+=parsed.get("before_window_skipped",0)
                    metrics["afterContentMonthSkipped"]+=parsed.get("after_window_skipped",0)
                metrics["itemsSeen"]+=parsed["items_seen"]
                if source["parser"]=="youtube_data_api":
                    metrics["apiUnitsEstimated"]=metrics.get("apiUnitsEstimated",0)+1  # playlistItems.list costs 1 API quota unit/page.
                    metrics["providerSkipped"]=metrics.get("providerSkipped",0)+parsed.get("items_skipped",0)
                    metrics["providerSkipReasons"]=list(dict.fromkeys(metrics.get("providerSkipReasons",[])+parsed.get("skip_reasons",[])))[:20]
                    metrics.update(windowCoverageVerified=False,coverageReason="PUBLIC_UPLOADS_PLAYLIST_BOUNDED_PAGES")
                    broadcast_documents=[document for document in documents if content_publication_status(document.get("published_at"),observed)=="INCLUDED"
                        and classify_notice_content({"rawSha256":document["raw_sha256"],"originalTitle":document["original_title"]})["kind"]=="broadcast-preview"
                        and (start<=aware_timestamp(document["published_at"])<end or mode=="reverify" and event_identity(document,source) in known_event_ids)]
                    if broadcast_documents and (deadline is None or time.monotonic()<deadline):
                        from official_adapter import youtube_video_details_request,attach_youtube_video_details
                        metrics["videoDetailsRequests"]+=1;metrics["apiUnitsEstimated"]+=1
                        try:
                            details_status,details_raw,_=fetch(youtube_video_details_request(source,[document["external_key"]for document in broadcast_documents]))
                            if details_status!=200:raise ValidationError("youtube_video_details_http_"+str(details_status))
                            attached=attach_youtube_video_details(details_raw,source,broadcast_documents)
                            replacements={document["external_key"]:document for document in attached}
                            documents=[replacements.get(document["external_key"],document)for document in documents]
                        except Exception:
                            metrics["videoDetailsFailures"]+=1
                            error_key=digest([source["id"],run_id,"YOUTUBE_LIVE_DETAILS"])
                            store.put("collectionErrors/"+error_key,{"sourceId":source["id"],"gameId":source["game_id"],"kind":"YOUTUBE_LIVE_DETAILS","reasonCode":"LIVE_DETAILS_UNCONFIRMED","createdAt":observed})
                dates=[]
                for document in documents:
                    gate=content_publication_status(document.get("published_at"),observed)
                    if gate!="INCLUDED":
                        if gate=="PUBLISHED_BEFORE_CONTENT_CUTOFF":
                            metrics["beforeContentCutoffSkipped"]+=1
                            dates.append(int(timestamp(document["published_at"]).timestamp()))
                        elif gate=="PUBLISHED_AFTER_CONTENT_MONTH":
                            metrics["afterContentMonthSkipped"]+=1
                        else:
                            metrics["publicationUnconfirmedSkipped"]+=1
                            key=digest([source["id"],document.get("external_key"),document.get("raw_sha256"),gate])
                            store.mutate("collectionErrors/"+key,lambda old:(None,False) if old else ({"sourceId":source["id"],"gameId":source["game_id"],"kind":"PUBLICATION_QUARANTINE","reasonCode":gate,"createdAt":observed},True))
                        continue
                    date=aware_timestamp(document["published_at"]);dates.append(int(date.timestamp()))
                    if metrics["latestPublishedAtSeen"] is None or date>metrics["latestPublishedAtSeen"]:metrics["latestPublishedAtSeen"]=date
                    ongoing_old=date<start and confirmed_window_overlap(document,source,start,end)
                    changed=aware_timestamp(document["source_updated_at"]) if document.get("source_updated_at") else None
                    if start<=date<end or changed and start<=changed<end or ongoing_old or mode=="reverify" and event_identity(document,source) in known_event_ids:
                        if not source_document_allowed(source,document.get("original_title")):
                            metrics["sharedChannelOtherGameSkipped"]+=1;continue
                        if document.get("linked_article_required"):
                            from official_adapter import official_rss_article_request,attach_official_rss_article
                            if deadline is not None and time.monotonic()>=deadline:raise SafetyError("run_time_limit")
                            sleep(1);article_status,article_raw,_=fetch(official_rss_article_request(source,document))
                            metrics["publisherArticleRequests"]=metrics.get("publisherArticleRequests",0)+1
                            if article_status!=200:raise SafetyError("publisher_article_http_"+str(article_status))
                            document=attach_official_rss_article(article_raw,source,document)
                        if ongoing_old:metrics["ongoingOlderIncluded"]+=1
                        classification=classify_notice_content({"rawSha256":document["raw_sha256"],"originalTitle":document["original_title"]})
                        if source["parser"]in YOUTUBE_PARSERS and (classification["status"]!="verified"or classification["kind"]not in YOUTUBE_ADMITTED_NEWS_KINDS):
                            metrics["contentExcluded"]+=1
                            metrics["contentSuppressedUpdated"]+=int(upsert_notice(store,source,document,observed)=="contentSuppressedUpdated")
                            continue
                        outcome=upsert_notice(store,source,document,observed,image_fetch=fetch,deadline=deadline)
                        metrics[outcome]+=1
                        stored=store.get("gameEvents/"+event_identity(document,source))
                        if stored and stored["id"] not in reported_event_ids:
                            reported_event_ids.add(stored["id"])
                            retained=timestamp(stored["publishedAt"])
                            if metrics["latestRetainedPublishedAt"] is None or retained>metrics["latestRetainedPublishedAt"]:metrics["latestRetainedPublishedAt"]=retained
                            content=classify_notice_content(stored)
                            classified=content["status"]=="verified" and content["kind"]!="other"
                            metrics["newsEligibleDocuments"]+=int(stored.get("newsEligible") is True)
                            metrics["newsBlockedByImage"]+=int(classified and not notice_has_representative_image(stored,observed))
                            metrics["newsBlockedByClassification"]+=int(not classified)
                        if stored and source["parser"]in YOUTUBE_PARSERS and outcome!="unchanged":
                            image_check=stored.get("noticeImageVerification")or{}
                            metrics["thumbnailProbeRequests"]=metrics.get("thumbnailProbeRequests",0)+image_check.get("requests",0)
                            metrics["verifiedNoticeImages"]=metrics.get("verifiedNoticeImages",0)+int(image_check.get("status")=="verified")
                            metrics["unverifiedNoticeImages"]=metrics.get("unverifiedNoticeImages",0)+int(image_check.get("status")!="verified")
                        if stored and stored.get("verificationStatus")=="verified" and stored.get("startsAt") and stored.get("endsAt"):
                            metrics["verifiedSchedules"]+=1
                        if stored and stored.get("candidateStartsAt") and stored.get("candidateEndsAt") and stored.get("status")!="cancelled":
                            metrics["candidateSchedules"]+=1
                        if stored and stored.get("derivedSectionCount"):
                            children=store.list_equal("gameEvents","parentEventId",stored["id"],50)
                            metrics["derivedChildrenQueryLimitReached"]=metrics["derivedChildrenQueryLimitReached"] or len(children)==50
                            verified_children=[row for _,row in children if row.get("verificationStatus")=="verified" and row.get("startsAt") and row.get("endsAt") and row.get("status")!="cancelled"]
                            metrics["verifiedSchedules"]+=len(verified_children)
                            metrics["verifiedSectionSchedules"]+=sum(bool(row.get("sourceSectionKey")) for row in verified_children)
                            candidate_children=[row for _,row in children if row.get("verificationStatus")=="pending" and row.get("candidateStartsAt") and row.get("candidateEndsAt") and row.get("status")!="cancelled"]
                            metrics["candidateSchedules"]+=len(candidate_children)
                            metrics["candidateSectionSchedules"]+=sum(bool(row.get("sourceSectionKey")) for row in candidate_children)
                    elif mode=="reverify":
                        metrics["olderUnseenSkipped"]+=1
                if requested_mode=="incremental" and (not matching or pending_resume is not None):metrics["freshHeadFetched"]=True
                if pending_resume is not None:
                    cursor,page_token,end=pending_resume;pending_resume=None
                    if page_index==max_pages-1:
                        success=True;metrics.update(pageLimitReached=True,providerPaginationComplete=False,windowCoverageVerified=False,
                            coverageReason="FRESH_HEAD_FETCHED_ARCHIVE_RESUME_PENDING");break
                    continue
                oldest=min(dates) if dates else None
                if source["parser"]in OFFICIAL_SITE_PARSERS:
                    if source["parser"]in("official_rss","kuro_cms") or mode=="reverify" or not parsed["items_seen"] or parsed.get("publication_cutoff_reached"):
                        complete=source["parser"]in("official_html","genshin_cms","kuro_cms") and (source["parser"]=="kuro_cms"or not parsed["items_seen"]or parsed.get("publication_cutoff_reached"))
                        metrics.update(providerPaginationComplete=bool(complete),publicationCutoffReached=bool(parsed.get("publication_cutoff_reached")),windowCoverageVerified=bool(complete),
                            coverageReason="PUBLISHER_BOARD_DATE_BOUNDARY_REACHED" if complete else parsed.get("coverage_reason","PUBLIC_ARTICLES_BOUNDED_REVERIFY"))
                        if mode=="content-backfill":checkpoint_month_backfill(store,source,token,start,end,cursor=cursor,complete=True,pages=1);metrics["backfillCursorCommitted"]=True
                        success=True;break
                    cursor+=1
                    if mode=="content-backfill":checkpoint_month_backfill(store,source,token,start,end,cursor=cursor,pages=1);metrics["backfillCursorCommitted"]=True
                    continue
                if source["parser"]=="youtube_data_api":
                    next_page=parsed.get("next_page_token")
                    # Playlist position/added time is not necessarily video publication
                    # order. An old video alone must not hide later October videos.
                    if not next_page or mode=="reverify":
                        metrics["providerPaginationComplete"]=not bool(next_page)
                        metrics["publicationCutoffReached"]=oldest is not None and oldest<int(CONTENT_CUTOFF.timestamp())
                        if mode=="content-backfill":
                            checkpoint_month_backfill(store,source,token,start,end,complete=not bool(next_page),pages=1)
                            metrics.update(backfillCursorCommitted=True,windowCoverageVerified=not bool(next_page) and not metrics.get("providerSkipped",0),
                                coverageReason="PUBLIC_UPLOADS_PLAYLIST_EXHAUSTED" if not next_page else "PUBLIC_UPLOADS_PLAYLIST_BOUNDED_PAGES")
                        success=True;break
                    if not isinstance(next_page,str) or not re.fullmatch(r"[A-Za-z0-9_+=./:-]{1,512}",next_page) or next_page in seen_page_tokens:raise SafetyError("youtube_pagination_token_not_advancing")
                    seen_page_tokens.add(next_page);page_token=next_page
                    if mode=="content-backfill":
                        checkpoint_month_backfill(store,source,token,start,end,page_token=page_token,pages=1);metrics["backfillCursorCommitted"]=True
                    continue
                if parsed["items_seen"]>=50 and oldest is not None and dates.count(oldest)>1 and oldest>=int(start.timestamp()):
                    raise SafetyError("pagination_boundary_ambiguous")
                if source["parser"]=="youtube_atom" or mode=="reverify" or parsed["items_seen"]<50 or oldest is not None and oldest<int(start.timestamp()):
                    if mode=="content-backfill":
                        complete=source["parser"]!="youtube_atom"
                        checkpoint_month_backfill(store,source,token,start,end,cursor=cursor,complete=complete,pages=1)
                        metrics.update(backfillCursorCommitted=True,providerPaginationComplete=complete,windowCoverageVerified=complete)
                    success=True;break
                if oldest is None or oldest>=cursor:raise SafetyError("pagination_cursor_not_advancing")
                cursor=oldest
                if mode=="content-backfill":
                    checkpoint_month_backfill(store,source,token,start,end,cursor=cursor,pages=1);metrics["backfillCursorCommitted"]=True
            if not success:
                if source["parser"]=="youtube_data_api" and page_token is not None:
                    success=True
                    metrics.update(pageLimitReached=True,providerPaginationComplete=False,windowCoverageVerified=False,
                        coverageReason="PAGE_LIMIT_REACHED_WITH_MORE_UPLOADS")
                elif mode=="content-backfill" and metrics["backfillCursorCommitted"]:
                    success=True;metrics.update(pageLimitReached=True,providerPaginationComplete=False,windowCoverageVerified=False,coverageReason="PAGE_LIMIT_REACHED_WITH_RESUME_CURSOR")
                else:raise SafetyError("page_limit_reached")
    except Exception as failure:
        error=str(failure)[:120] if isinstance(failure,(SafetyError,ValidationError)) else type(failure).__name__
        store.put("collectionErrors/"+run_id,{"sourceId":source["id"],"gameId":source["game_id"],"reasonCode":error,
                  "httpStatus":http_status,"createdAt":observed,"retryAfterSeconds":retry_after})
    finished=datetime.now(UTC)
    finish_source(store,source,token,finished,success,error,http_status,mode,retry_after,metrics)
    result={"sourceId":source["id"],"gameId":source["game_id"],"status":"DEFERRED_QUOTA_BUDGET"if error in QUOTA_DEFERRAL_REASONS else "SUCCESS_BOUNDED" if success and metrics.get("pageLimitReached")else "success" if success else "FETCH_FAILED",
            "contentPublishedNotBefore":CONTENT_PUBLISHED_NOT_BEFORE,
            "windowStart":start,"windowEnd":end,"mode":mode,"requestedMode":requested_mode,"metrics":metrics,"reasonCode":error,
            "httpStatus":http_status,"createdAt":observed,"finishedAt":finished,
            "lastCommittedCursor":cursor,"writes":store.writes-started_writes}
    store.put("collectionRuns/"+run_id,result)
    return result


def adaptive_source_budget(store,requested,source_count,now=None,overdue_sources=0):
    """Worker-accounted soft quota sharing; provider Spark limits remain final.

    Repeated 116-source/15-minute maintenance would require at least 44,544
    writes/day. Reserve room for content, logs and other worker steps, and rotate
    the oldest checked sources instead of claiming every source runs at 15 min.
    """
    base={"enabled":False,"requestedSourceAttempts":requested,"effectiveSourceAttempts":requested,"workerAccountedOnly":True,"clientUsageIncluded":False}
    if not store.remote or not hasattr(store,"daily_remaining_writes"):return base
    from zoneinfo import ZoneInfo
    local=(now or datetime.now(UTC)).astimezone(ZoneInfo("America/Los_Angeles"));midnight=datetime.combine(local.date()+timedelta(days=1),datetime.min.time(),tzinfo=local.tzinfo)
    slots=max(1,int((midnight-local).total_seconds()+899)//900)
    writes=max(0,store.daily_remaining_writes-store.writes);reads=max(0,store.daily_remaining_reads-store.reads)
    per_slot_writes=writes//slots;per_slot_reads=reads//slots
    effective=min(requested,25,max(0,(per_slot_writes-24)//5),max(0,(per_slot_reads-source_count-32)//8))
    catchup=min(requested,overdue_sources,max(0,(min(writes-2,getattr(store,"max_writes",500)-store.writes-2)-24)//5),
        max(0,(min(reads-1,getattr(store,"max_reads",2500)-store.reads-1)-32)//8))
    effective=max(effective,catchup)
    write_budget=min(getattr(store,"max_writes",500)-store.writes,writes,max(per_slot_writes,catchup*5+26 if catchup else 0))
    return {**base,"enabled":True,"effectiveSourceAttempts":effective,"remainingScheduleSlotsInQuotaDay":slots,"remainingWorkerWrites":writes,"remainingWorkerReads":reads,
        "slotWriteBudget":per_slot_writes,"slotReadBudget":per_slot_reads,"effectiveRunWriteBudget":write_budget,
        "overdueSourceCount":overdue_sources,"catchupEnabled":catchup>0,"catchupThresholdHours":3,
        "reservedWritesPerSlot":24,"estimatedMaintenanceWritesPerSource":5,
        "unadaptedMaintenanceWritesPerDay":source_count*96*4,"estimatedMinimumRotationMinutes":((source_count+effective-1)//effective)*15 if effective else None,
        "reasonCode":"WORKER_FREE_QUOTA_ADAPTIVE_CAP"if effective else "WORKER_FREE_QUOTA_HEADROOM_LOW","allSourcesEvery15MinutesVerified":False}


def run(registry,store,start,end,mode="backfill",fetch=fetch_source,sleep=time.sleep,timeout_seconds=480,max_sources=100,max_pages=10):
    deadline=time.monotonic()+timeout_seconds
    results=[];limited_hosts=set();attempts=0
    from urllib.parse import urlsplit
    operating_registry=registry
    retention=expire_youtube_metadata(store)if store.remote else {"status":"LOCAL_STAGING_NOT_AUTOMATIC","remoteFirestoreWrites":0,"fcmSends":0}
    registry_limited=False
    if store.remote:
        public_sources=[];after=None
        for _ in range(10):
            page=store.list_cursor("gameSources",after,100);public_sources.extend(page)
            if len(page)<100:break
            after=page[-1][0]
        else:registry_limited=True
        if not registry_limited and hasattr(store,"project_id"):
            refresh_recovery_approval(store,registry,public_sources)
        operating_registry={"games":[{"gameId":row["gameId"],"sources":[operating_source({**row,"id":source_id})]}
                           for source_id,row in public_sources if row.get("enabled") is True]}
    sources=[{**raw_source,"game_id":game["gameId"]} for game in operating_registry["games"] for raw_source in game.get("sources",[])]
    def last_checked(source):
        try:return timestamp(source.get("last_checked_at")) if source.get("last_checked_at") else datetime.min.replace(tzinfo=UTC)
        except (ValueError,TypeError):return datetime.min.replace(tzinfo=UTC)
    now=datetime.now(UTC)
    def is_due(source):
        try:return not source.get("next_run_at") or timestamp(source["next_run_at"])<=now
        except (ValueError,TypeError):return True
    overdue=sum(is_due(source) and last_checked(source)<=now-timedelta(hours=3) for source in sources)
    budget=adaptive_source_budget(store,max_sources,len(sources),now,overdue)
    max_sources=budget["effectiveSourceAttempts"]
    if budget["enabled"]:
        store.max_writes=min(store.max_writes,max(store.writes+24,store.writes+budget["effectiveRunWriteBudget"]))
    # Previously unattempted or longest-waiting sources come first; failed
    # sources use lastChecked, so they cannot monopolize every subsequent run.
    sources.sort(key=lambda source:(last_checked(source),source["id"]))
    for source in sources:
        if attempts>=max_sources or time.monotonic()>=deadline:break
        if store.remote and hasattr(store,"max_writes")and(store.max_writes-store.writes<24 or store.max_reads-store.reads<50):
            results.append({"sourceId":source["id"],"status":"DEFERRED_QUOTA_BUDGET","reasonCode":"WORKER_FREE_QUOTA_HEADROOM_LOW"});break
        host=urlsplit(source["url"]).hostname
        if host in limited_hosts:
            results.append({"sourceId":source["id"],"status":"DEFERRED","reasonCode":"HOST_RATE_LIMITED"});continue
        if source.get("next_run_at"):
            try:
                if timestamp(source["next_run_at"])>datetime.now(UTC):
                    results.append({"sourceId":source["id"],"status":"DEFERRED","reasonCode":"SOURCE_NOT_DUE"});continue
            except (ValueError,TypeError):pass  # Atomic lease remains authoritative.
        if not reviewed(source):
            results.append({"sourceId":source["id"],"gameId":source["game_id"],"status":"REVIEW_REQUIRED","reasonCode":"SOURCE_POLICY_UNVERIFIED"});continue
        result=collect_one(store,source,start,end,mode,fetch,sleep,deadline=deadline,max_pages=max_pages)
        results.append(result)
        if result.get("status") not in ("DEFERRED","DISABLED"):attempts+=1
        if result.get("httpStatus")==429:limited_hosts.add(host)
    news_outcome={"scope":"UNIQUE_PARENT_EVENTS_PROCESSED_PER_SOURCE_RUN","eligibleNews":sum(row.get("metrics",{}).get("newsEligibleDocuments",0)for row in results),
        "heldForImage":sum(row.get("metrics",{}).get("newsBlockedByImage",0)for row in results),
        "heldForClassification":sum(row.get("metrics",{}).get("newsBlockedByClassification",0)for row in results),
        "created":sum(row.get("metrics",{}).get("created",0)for row in results),"updated":sum(row.get("metrics",{}).get("updated",0)for row in results),
        "failedSources":sum(row.get("status")=="FETCH_FAILED"for row in results),"notDueSources":sum(row.get("reasonCode")=="SOURCE_NOT_DUE"for row in results),
        "quotaDeferredSources":sum(row.get("status")=="DEFERRED_QUOTA_BUDGET"for row in results),"fullSourceCoverageVerified":False}
    return {"recordedAt":datetime.now(UTC),"backend":"FIREBASE" if store.remote else "LOCAL_STAGING",
            "youtubeMetadataRetention":retention,
            "contentPublishedNotBefore":CONTENT_PUBLISHED_NOT_BEFORE,"contentPublishedBefore":CONTENT_PUBLISHED_BEFORE,
            "sourceAttempts":attempts,"registeredSourcesConsidered":len(sources),"sourceRegistryLimitReached":registry_limited,
            "collectionScope":"PUBLIC_CATALOGUE_NOT_USER_SUBSCRIPTIONS","pageBudgetPerSource":max_pages,
            "quotaPolicy":budget,"newsOutcome":news_outcome,"unattemptedSources":max(0,len(sources)-attempts),
            "publicNewsChanges":deepcopy(getattr(store,"public_news_changes",{})),
            **({"publicNewsConfirmedAt":store.public_news_confirmed_at}if getattr(store,"public_news_changes",{})else {}),
            "remoteFirestoreWrites":store.writes if store.remote else 0,"fcmSends":0,"windowHours":(end-start).total_seconds()/3600,
            "results":results,"verifiedEventSchedules":sum(row.get("metrics",{}).get("verifiedSchedules",0) for row in results),"fullSourceCoverageVerified":False,
            "candidateEventSchedules":sum(row.get("metrics",{}).get("candidateSchedules",0) for row in results),
            "extractionVersion":EXTRACTION_VERSION,"imageReuseRequiresSeparateReview":True}


def provider_quota_result(store=None, previous=None):
    """A provider denial is observable even when Firestore bookkeeping cannot run."""
    return {**(previous or {}),"status":"PROVIDER_QUOTA_DEFERRED","reasonCode":"firestore_provider_quota_exhausted",
        "collectionSucceeded":False,"fullSourceCoverageVerified":False,"usageAccountingConfirmed":False,
        "usageAccountingDeferred":True,"workerRetriesAfterProviderQuota":0,
        "storeInitialized":store is not None,"remoteFirestoreWrites":getattr(store,"writes",0),"fcmSends":0,
        "providerQuotaBlocked":True,"existingUpdatesDeferred":True,
        "publicNewsChanges":deepcopy(getattr(store,"public_news_changes",{})),
        **({"publicNewsConfirmedAt":store.public_news_confirmed_at}if getattr(store,"public_news_changes",{})else {}),
        "recordedAt":datetime.now(UTC).isoformat()}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry",default="data/official-sources.json")
    parser.add_argument("--mode",choices=["backfill","incremental","reverify","content-backfill"],default="backfill")
    parser.add_argument("--hours",type=int,default=72)
    parser.add_argument("--max-sources",type=int,default=100)
    parser.add_argument("--pages-per-source",type=int,default=10,help="1..200 pages; a completed page cursor survives later runs")
    parser.add_argument("--live",action="store_true")
    parser.add_argument("--apply",action="store_true")
    parser.add_argument("--seed",action="store_true")
    parser.add_argument("--activate-reviewed-sources",action="store_true")
    parser.add_argument("--content-cleanup",action="store_true",help="Review only pre-October notices and expired exact YouTube metadata; --live reads production, --apply requires --cleanup-review")
    parser.add_argument("--cleanup-review",help="An exact previously inspected dry-run JSON; only used with --content-cleanup --apply")
    parser.add_argument("--report",default="reports/firebase-collector.json")
    parser.add_argument("--public-recovery-state",help="Public approval/known-identity cache only; never credentials or private records")
    args=parser.parse_args()
    if not 1<=args.hours<=72:parser.error("hours must be 1..72")
    if not 1<=args.max_sources<=200 or not 1<=args.pages_per_source<=200:parser.error("bounded source/page budgets must be 1..200")
    registry=json.loads(Path(args.registry).read_text(encoding="utf-8-sig"))
    if args.content_cleanup and (args.seed or args.activate_reviewed_sources):parser.error("content cleanup cannot activate or seed sources")
    store=None;result=None;recovery_state=None;recovery_writer=None;main_started=time.monotonic()
    project_id=os.environ.get("FIREBASE_PROJECT_ID","")
    if args.public_recovery_state or registry.get("quotaRecoveryBootstrap")is not None:
        try:recovery_state=load_public_recovery_state(args.public_recovery_state,registry,project_id,check_expiry=False)
        except SafetyError:pass
    try:
        store=FirestoreStore() if args.apply or args.content_cleanup and args.live else MemoryStore()
        if recovery_state is not None:store.public_recovery_state=recovery_state
        if args.content_cleanup:
            if args.apply:
                if not args.cleanup_review:parser.error("--apply cleanup requires a reviewed dry-run JSON")
                result=apply_content_cleanup(store,json.loads(Path(args.cleanup_review).read_text(encoding="utf-8-sig")))
            else:result=content_cleanup_plan(store)
        elif args.activate_reviewed_sources:
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
            end=datetime.now(UTC);start=CONTENT_CUTOFF if args.mode=="content-backfill" else max(CONTENT_CUTOFF,end-timedelta(hours=args.hours))
            result=run(registry,store,start,end,args.mode,max_sources=args.max_sources,max_pages=args.pages_per_source)
    except Exception as error:
        if isinstance(error,ProviderQuotaUnavailable) or is_provider_quota_error(error):
            result=provider_quota_result(store)
            operation=getattr(error,"operation",getattr(store,"provider_quota_operation","unknown"))
            result["providerReadQuotaBlocked"]=operation=="read"
            if operation=="read"and args.apply and args.live and not(args.seed or args.activate_reviewed_sources or args.content_cleanup):
                try:
                    current=getattr(store,"public_recovery_state",None)
                    if current is not None:
                        approved=validate_recovery_state(current,registry,project_id=project_id)
                        recovery_state=current
                    else:
                        recovery_state=load_public_recovery_state(args.public_recovery_state,registry,project_id)
                        approved=validate_recovery_state(recovery_state,registry,project_id=project_id)
                    remaining=480-(time.monotonic()-main_started)
                    if remaining<=0:raise SafetyError("recovery_run_time_limit")
                    recovery_writer=PublicNewsCreateOnlyStore(allowed_source_ids=[source["id"]for source in approved])
                    recovery_writer.public_recovery_state=recovery_state
                    recovered=run_public_news_recovery(registry,recovery_state,recovery_writer,timeout_seconds=remaining)
                    recovered["publicNewsChanges"]={**getattr(store,"public_news_changes",{}),**recovered["publicNewsChanges"]}
                    recovered["remoteFirestoreWrites"]+=getattr(store,"writes",0)
                    result=recovered
                except Exception as recovery_error:
                    result["recoveryReasonCode"]=str(recovery_error)if isinstance(recovery_error,SafetyError)else type(recovery_error).__name__
        elif isinstance(error,SafetyError) and str(error) in QUOTA_DEFERRAL_REASONS:
            result={"status":"DEFERRED_QUOTA_BUDGET","reasonCode":str(error),"fullSourceCoverageVerified":False,
                "remoteFirestoreWrites":getattr(store,"writes",0)if getattr(store,"remote",False)else 0,"fcmSends":0}
        else:raise
    finally:
        # Partial operations still count toward the free worker allowance.
        # A soft-budget deferral must not look like a zero-use successful run.
        if store is not None and store.remote and not (args.content_cleanup and not args.apply):
            if (result or {}).get("providerQuotaBlocked"):
                pass  # The provider rejected control reads; recovery never retries them.
            else:
                try:
                    store.finish_usage()
                    if result is not None:result.update(usageAccountingWrites=2,totalFirestoreWrites=store.writes+2,usageAccountingConfirmed=True)
                except Exception as error:
                    if not isinstance(error,ProviderQuotaUnavailable) and not is_provider_quota_error(error):raise
                    result=provider_quota_result(store,result)
        if store is not None and getattr(store,"public_recovery_state",None)is not None:recovery_state=store.public_recovery_state
        if recovery_writer is not None and getattr(recovery_writer,"public_recovery_state",None)is not None:recovery_state=recovery_writer.public_recovery_state
        if args.public_recovery_state and recovery_state is not None:save_public_recovery_state(args.public_recovery_state,recovery_state)
    if store is not None and store.remote:
        result["publicNewsChanges"]={**getattr(store,"public_news_changes",{}),**result.get("publicNewsChanges",{})}
    if recovery_writer is not None:
        # Receipts survive a later local/reporting failure. A proposed callback,
        # conflict or ambiguous timeout never enters this committed-change map.
        result["publicNewsChanges"]={**result.get("publicNewsChanges",{}),**getattr(recovery_writer,"public_news_changes",{})}
        result["remoteFirestoreWrites"]=getattr(store,"writes",0)+recovery_writer.writes
    confirmations=[getattr(adapter,"public_news_confirmed_at",None)for adapter in (store,recovery_writer)]
    if result.get("publicNewsChanges")and any(confirmations):result["publicNewsConfirmedAt"]=max(value for value in confirmations if value)
    result["recordedAt"]=datetime.now(UTC).isoformat()
    output=Path(args.report);output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(result,ensure_ascii=False,indent=2,default=lambda v:v.isoformat())+"\n",encoding="utf-8")
    if args.live and store is not None and not store.remote:
        evidence=output.with_name(output.stem+"-local-evidence.json")
        evidence.write_text(json.dumps(store.records,ensure_ascii=False,indent=2,default=lambda v:v.isoformat())+"\n",encoding="utf-8")
    print(json.dumps({"status":result.get("status",result.get("backend")),"report":str(output),"remoteFirestoreWrites":result["remoteFirestoreWrites"]}))


if __name__=="__main__":
    try:main()
    except SafetyError as error:
        if str(error)in("collector_daily_soft_budget_exhausted","worker_run_soft_budget_exhausted","collector_free_quota_headroom"):
            print(json.dumps({"status":"DEFERRED_QUOTA_BUDGET","reasonCode":str(error),"fullSourceCoverageVerified":False}));sys.exit(0)
        print(json.dumps({"status":"BLOCKED","reasonCode":str(error),"firebaseConnectionVerified":False}));sys.exit(1)

