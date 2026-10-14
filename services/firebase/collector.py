"""Firestore Spark publisher worker with reviewed explicit schedule extraction.

Default command is a local plan with zero external requests and zero remote writes.
--live fetches reviewed official sources; --apply additionally requires Admin safety
checks and pre-enabled Firestore sources. Retained Supabase workers are inactive.
"""
from __future__ import annotations
import argparse
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import html
from html.parser import HTMLParser
import ipaddress
import json
from pathlib import Path
import re
import sys
import time
import uuid
import xml.etree.ElementTree as ET
from urllib.parse import urlsplit, parse_qsl, urlencode, urljoin

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "collector"))
from engine import fetch_source, ValidationError, normalize_url
from official_adapter import steam_request, parse_steam_documents, aware_timestamp, TYPES
from backfill import retry_after_seconds
from store import MemoryStore, FirestoreStore, SafetyError, timestamp

UTC = timezone.utc
EXTRACTION_VERSION = 7
YOUTUBE_CHANNEL_ID = re.compile(r"UC[A-Za-z0-9_-]{22}")
YOUTUBE_VIDEO_ID = re.compile(r"[A-Za-z0-9_-]{11}")
YOUTUBE_FEED_PATHS = ("/feeds/videos.xml", "/xml/feeds/videos.xml")
YOUTUBE_FEED_DOCUMENTATION = "https://developers.google.com/youtube/v3/guides/push_notifications"


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
        for node in item.findall(media+"group/"+media+"thumbnail")[:5]:
            image = safe_public_url(node.get("url"))
            if image and urlsplit(image).hostname in ("i.ytimg.com", "i1.ytimg.com", "i2.ytimg.com", "i3.ytimg.com", "i4.ytimg.com"):
                thumbnails.append(image)
        raw_item = {"channelId":channel, "videoId":video, "title":title, "description":content,
                    "published":published, "updated":updated, "officialUrl":url, "originalVideoUrl":links[0], "thumbnailUrls":thumbnails}
        hints = [kind for kind, patterns in TYPES.items() if any(re.search(pattern, title+"\n"+content, re.I) for pattern in patterns)]
        documents.append({"external_key":video, "raw_sha256":digest(raw_item), "raw_json":raw_item,
            "raw_publication_timestamp":published, "published_at":published_time.isoformat(), "source_updated_at":updated,
            "observed_at":observed_at, "official_url":url, "original_title":title, "original_description":content,
            "original_language":source.get("content_language") or "unknown", "server_region":source.get("server_region") or "UNSPECIFIED",
            "server_label":source.get("server_label"), "event_type_candidates":hints, "pickups":[], "source_image_urls":thumbnails})
    return documents, {"items_seen":len(items), "items_accepted":len(documents), "items_skipped":0,
        "skip_reasons":[], "window_coverage_verified":False, "coverage_reason":"RECENT_FEED_WITHOUT_HISTORY_PAGINATION"}


def source_policy(source):
    """Source-specific opt-ins remain separate from permission to read a feed."""
    policy = {key: source.get(key) for key in (
        "schedule_extraction_approved", "schedule_extraction_reviewed_at", "schedule_extraction_evidence_url",
        "content_language", "server_region", "server_label", "highest_character_rarity", "image_reuse",
        "section_schedule_format", "paired_recruitment_period_approved", "content_localization", "original_image_reference",
        "reviewed_notice_references", "calendar_role_policy", "localization_roster")}
    if source.get("parser") == "youtube_atom":
        policy.update({key:source.get(key) for key in ("youtube_channel_id", "publisher_identity_evidence_url", "publisher_identity_checked_at")})
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
            "_raw_character_names":chars,"source_image_urls":[]})
    return result


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
        "kind":kind,"method":method,"evidenceText":proof[:1000],"characterNames":names}


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
        matches.append({**image,"subjectEvidence":{**evidence,"eventId":child_id,"characterNames":[target["name"]],"announcementKind":target["announcementKind"]}})
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
    if isinstance(value,datetime):value=value.isoformat()
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
                "method":"reviewed_image","evidenceText":ref["evidenceText"],"characterNames":names}})
    return matched[0] if len(matched)==1 else None


def automatic_main_role_policy(source):
    """A source format opt-in is separate from reading permission/date approval."""
    policy=(source.get("calendar_role_policy") or {}).get("automaticRules")
    if not reviewed(source) or not isinstance(policy,dict):return None
    fields={"mode","gameId","parser","newHighestCharacter","mainEventFormat","evidenceUrl","checkedAt"}
    if (set(policy)-fields or policy.get("mode")!="explicit_main_role_extraction_v1"
        or policy.get("gameId")!=source.get("game_id") or policy.get("parser")!=source.get("parser")
        or not isinstance(policy.get("newHighestCharacter"),bool) or policy.get("mainEventFormat") not in (None,"gf2_story_event_v1")
        or not safe_public_url(policy.get("evidenceUrl")) or not policy["evidenceUrl"].startswith("https://")):return None
    if policy.get("mainEventFormat") and (source.get("game_id")!="girls-frontline-2" or source.get("parser")!="steam_news" or source.get("section_schedule_format")!="heading_period_v1"):return None
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
        matched=[target for target in highest_rarity_targets(text,source) if target["announcementKind"]=="new"
            and row.get("characterNames")==[target["name"]] and row.get("characters")==[target["name"]]]
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
        "approved_image": approved_subject_image(images),"referenced_image":original_subject_image(images,source,document.get("observed_at")),
        "highest_rarity_targets": targets, "cancelled": cancelled,"section_documents":section_documents(document,source)}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, default=lambda v:v.isoformat()).encode()).hexdigest()


def reviewed(source):
    common = (source.get("approved_for_staging") is True and source.get("access_status") == "approved"
            and source.get("terms_checked_at")
            and source.get("notice_hosts_verified_at") and source.get("terms_url", "").startswith("https://")
            and source.get("allowed_notice_hosts"))
    if not common:
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
    return False  # X API/social HTML scraping are deliberately unsupported in the 0-won worker.


def source_row(source, game_id):
    """Only non-secret registered public provider metadata is exported."""
    return {"id":source["id"], "gameId":game_id, "url":source["url"], "parser":source["parser"],
            "steamAppid":source.get("steam_appid"), "youtubeChannelId":source.get("youtube_channel_id"),
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
            **({"localizationRoster":source["localization_roster"]} if source.get("localization_roster") is not None else {})}


def operating_source(row):
    """Admin-reviewed Firestore source configuration is authoritative in apply mode."""
    return {"id":row["id"],"game_id":row["gameId"],"url":row["url"],"parser":row["parser"],
            "steam_appid":row.get("steamAppid"), "youtube_channel_id":row.get("youtubeChannelId"),
            "publisher_identity_evidence_url":row.get("publisherIdentityEvidenceUrl"), "publisher_identity_checked_at":row.get("publisherIdentityCheckedAt"), "enabled":row.get("enabled",False),
            "approved_for_staging":row.get("approvedForStaging",False),"access_status":row.get("accessStatus"),
            "terms_url":row.get("termsUrl"),"terms_checked_at":row.get("termsCheckedAt"),
            "notice_hosts_verified_at":row.get("noticeHostsVerifiedAt"),"allowed_notice_hosts":row.get("allowedNoticeHosts",[]),
            "feed_allowlist":row.get("feedAllowlist",[]),"collection_interval_minutes":max(15,row.get("intervalMinutes",15)),
            "server_region":row.get("serverRegion") or "UNSPECIFIED","server_label":row.get("serverLabel"),
            "method":"official_feed" if row.get("parser")=="youtube_atom" else "official_api",
            "official_host":"www.youtube.com" if row.get("parser")=="youtube_atom" else "api.steampowered.com","request_timeout_seconds":20,"max_response_bytes":2097152,
            "schedule_extraction_approved":row.get("scheduleExtractionApproved",False),
            "schedule_extraction_reviewed_at":row.get("scheduleExtractionReviewedAt"),
            "schedule_extraction_evidence_url":row.get("scheduleExtractionEvidenceUrl"),
            "content_language":row.get("contentLanguage"),"highest_character_rarity":row.get("highestCharacterRarity"),
            "image_reuse":row.get("imageReuse"),"section_schedule_format":row.get("sectionScheduleFormat"),
            "content_localization":row.get("contentLocalization"),"original_image_reference":row.get("originalImageReference"),
            "reviewed_notice_references":row.get("reviewedNoticeReferences"),"calendar_role_policy":row.get("calendarRolePolicy"),
            "localization_roster":row.get("localizationRoster"),
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
                        "scheduleExtractionApproved","scheduleExtractionReviewedAt","scheduleExtractionEvidenceUrl","contentLanguage","highestCharacterRarity","imageReuse","sectionScheduleFormat","pairedRecruitmentPeriodApproved",
                        "youtubeChannelId","publisherIdentityEvidenceUrl","publisherIdentityCheckedAt","contentLocalization","originalImageReference","reviewedNoticeReferences","calendarRolePolicy","localizationRoster"] if k in old})
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
          "rawPublicationTimestamp":document["raw_publication_timestamp"],"sourceUpdatedAt":timestamp(document["source_updated_at"]) if document.get("source_updated_at") else None,"observedAt":observed,
          "officialUrl":document["official_url"]},True))
    def update(old):
        if old and old.get("rawSha256") == document["raw_sha256"] and old.get("extractionVersion")==EXTRACTION_VERSION and old.get("extractionPolicySha256")==document["extraction_policy_sha256"] and old.get("categoryCandidate")==document["category_candidate"]:
            if old.get("verificationStatus")=="verified" and old.get("status") not in ("cancelled","postponed") and old.get("startsAt") and old.get("endsAt"):
                state="ended" if timestamp(old["endsAt"])<=observed else "ongoing" if timestamp(old["startsAt"])<=observed else "upcoming"
                if state!=old.get("status"):
                    return {**old,"status":state,"lastCheckedAt":observed,"lastChangedAt":observed,"revision":old.get("revision",0)+1},"updated"
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
             "imageCandidates":document["image_candidates"],"image":document["approved_image"] or document["referenced_image"] or reviewed_notice_image(document,source,event_id) or retained_image,
             "sourceImageOverride":old.get("sourceImageOverride") if retained_image and not (document["approved_image"] or document["referenced_image"]) else None,
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
        row=announcement_metadata(reviewed_calendar_role(localize_collected_row(row,source,observed),source),old,observed)
        if len(json.dumps(row,ensure_ascii=False,default=lambda value:value.isoformat()).encode())>900000:
            raise SafetyError("event_document_exceeds_safe_firestore_size")
        return row,"updated" if old else "created"
    outcome=store.mutate("gameEvents/"+event_id,update)
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
                "displayTitle","sourceSectionTitle","translations","contentLocalization","calendarRole","calendarRoleEvidence")})
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
            # A structured recruitment section will create its own independent
            # character child. Do not create a second equivalent child from the
            # same parent overview and send the same debut announcement twice.
            if any(section.get("_section_category")=="recruitment" and target["name"] in section.get("_raw_character_names",[]) for section in document["section_documents"]):continue
            child_id=digest({"parent":event_id,"announcementKind":target["announcementKind"],"character":target["name"]})
            child_images,child_artwork=character_image(document["image_candidates"],target,child_id)
            active_children.add(child_id)
            def child_update(old):
                title=target["name"]
                retained_image=retained_source_reference(old,document,child_id,title=target["name"],characters=[target["name"]],announcement_kind=target["announcementKind"])
                row={**parent,"id":child_id,"parentEventId":event_id,"announcementKind":target["announcementKind"],
                    "rarityLabel":target["rarityLabel"],"characters":[target["name"]],"characterEvidenceText":target["evidenceText"],
                    "title":title,"displayTitle":title,"characterNames":[target["name"]],"originalTitle":document["original_title"],"derivedCharacterAnnouncements":0,
                    "imageCandidates":child_images,"image":child_artwork or original_subject_image(child_images,source,document.get("observed_at")) or retained_image,
                    "sourceImageOverride":old.get("sourceImageOverride") if retained_image and not original_subject_image(child_images,source,document.get("observed_at")) else None,
                    "firstCheckedAt":old.get("firstCheckedAt",observed) if old else observed,
                    "revision":old.get("revision",0)+1 if old else 1}
                return announcement_metadata(reviewed_calendar_role(localize_collected_row(row,source,observed),source),old,observed),True
            store.mutate("gameEvents/"+child_id,child_update)
            pickup_id=digest({"event":child_id,"kind":"character"})
            def child_pickup(old):
                current_child=store.get("gameEvents/"+child_id)
                row={"id":pickup_id,"eventId":child_id,"parentEventId":event_id,"gameId":source["game_id"],
                    "sourceId":source["id"],"serverRegion":parent["serverRegion"],"serverLabel":parent["serverLabel"],
                    "officialUrl":parent["officialUrl"],"pickupKind":"character","characters":[target["name"]],"weapons":[],
                    "announcementKind":target["announcementKind"],"rarityLabel":target["rarityLabel"],
                    "startsAt":parent.get("startsAt"),"endsAt":parent.get("endsAt"),"originalTimezone":parent.get("originalTimezone"),
                    "candidateStartsAt":parent.get("candidateStartsAt"),"candidateEndsAt":parent.get("candidateEndsAt"),"candidateTimezone":parent.get("candidateTimezone"),
                    "verificationStatus":parent["verificationStatus"],"status":parent["status"],"image":current_child.get("image"),
                    "imageCandidates":child_images,"lastCheckedAt":observed,"lastChangedAt":observed,
                    "firstCheckedAt":old.get("firstCheckedAt",observed) if old else observed,
                    "lastVerifiedAt":parent.get("lastVerifiedAt"),"scheduleOrigin":"automatic"}
                row.update({key:current_child.get(key) for key in ("originalTitle","originalDescription","originalLanguage","rawSha256","revision",
                    "displayTitle","sourceSectionTitle","translations","contentLocalization","calendarRole","calendarRoleEvidence")})
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
    metrics.update(candidateSchedules=0,candidateSectionSchedules=0)
    success,error,http_status,retry_after=False,None,None,0
    cursor=int(end.timestamp())+1
    try:
        source,roster_metrics=refresh_localization_roster(store,source,observed,fetch,sleep,deadline)
        metrics['localizationRoster']=roster_metrics
        if roster_metrics['status'] in ('FETCH_FAILED','SOURCE_POLICY_CHANGED','REVIEW_REQUIRED'):
            store.put('collectionErrors/'+run_id+'-roster',{'sourceId':source['id'],'gameId':source['game_id'],'kind':'LOCALIZATION_ROSTER',
                'reasonCode':roster_metrics.get('reasonCode')or roster_metrics['status'],'createdAt':observed})
        known_event_ids=set()
        if mode=="reverify":
            known_event_ids={event_id for event_id,_ in store.list_equal("gameEvents","sourceId",source["id"],50)}
            metrics.update(knownEventsForReverify=len(known_event_ids),knownEventsQueryLimitReached=len(known_event_ids)==50)
        for _ in range(max_pages):
            if deadline is not None and time.monotonic()>=deadline:raise SafetyError("run_time_limit")
            sleep(1)
            request=youtube_request(source) if source["parser"]=="youtube_atom" else steam_request({**source,"enabled":True},cursor,50)
            http_status,raw,headers=fetch(request);metrics["pagesFetched"]+=1
            if http_status!=200:
                retry_after=retry_after_seconds(headers.get("retry-after"))
                raise SafetyError("source_http_"+str(http_status))
            documents,parsed=(parse_youtube_documents(raw,source,observed.isoformat()) if source["parser"]=="youtube_atom" else parse_steam_documents(raw,source,observed.isoformat()))
            if source["parser"]=="youtube_atom":
                metrics.update(windowCoverageVerified=False,coverageReason=parsed["coverage_reason"])
            metrics["itemsSeen"]+=parsed["items_seen"]
            dates=[]
            for document in documents:
                if not document["published_at"]:raise SafetyError("publication_timestamp_unconfirmed")
                date=aware_timestamp(document["published_at"]);dates.append(int(date.timestamp()))
                ongoing_old=date<start and confirmed_window_overlap(document,source,start,end)
                changed=aware_timestamp(document["source_updated_at"]) if document.get("source_updated_at") else None
                if start<=date<end or changed and start<=changed<end or ongoing_old or mode=="reverify" and event_identity(document,source) in known_event_ids:
                    if ongoing_old:metrics["ongoingOlderIncluded"]+=1
                    metrics[upsert_notice(store,source,document,observed)]+=1
                    stored=store.get("gameEvents/"+event_identity(document,source))
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
            oldest=min(dates) if dates else None
            if parsed["items_seen"]>=50 and oldest is not None and dates.count(oldest)>1 and oldest>=int(start.timestamp()):
                raise SafetyError("pagination_boundary_ambiguous")
            if source["parser"]=="youtube_atom" or mode=="reverify" or parsed["items_seen"]<50 or oldest is not None and oldest<int(start.timestamp()):
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
            "candidateEventSchedules":sum(row.get("metrics",{}).get("candidateSchedules",0) for row in results),
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
