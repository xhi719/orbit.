"""Strict public Steam publisher-news adapter. Never turns publication into schedule.

Labels extracted from keywords are review candidates, not verified classifications.
Steam's documented public feed does not specify server regions or event boundaries.
"""
from __future__ import annotations
from datetime import datetime, timezone
import hashlib
import json
import re
from urllib.parse import urlencode, urlsplit, parse_qsl
from engine import ValidationError, normalize_url

UTC = timezone.utc
TYPES = {
    "pickup": (r"\bpick[- ]?up\b", "픽업", "ピックアップ", "召集", "招募", "banner"),
    "login_reward": (r"log[- ]?in\s+(?:reward|bonus|event)", "출석", "로그인 보상", "ログインボーナス", "ログインイベント", "签到"),
    "maintenance": (r"\bmaintenance\b", "점검", "メンテナンス", "维护"),
    "update": (r"\bupdate\b", r"\bpatch\b", "업데이트", "アップデート", "更新"),
    "official_broadcast": (r"\blive\s?stream\b", "공식 방송", "生放送"),
}


def aware_timestamp(value):
    if not isinstance(value, str) or "T" not in value:
        raise ValidationError("explicit_timestamp_required")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError()
        return parsed.astimezone(UTC)
    except (ValueError, TypeError, OverflowError) as error:
        raise ValidationError("explicit_timestamp_required") from error


def steam_request(source, enddate, count=50):
    if source.get("parser") != "steam_news" or not isinstance(source.get("steam_appid"), int):
        raise ValidationError("steam_source_identity_required")
    endpoint = "https://api.steampowered.com/ISteamNews/GetNewsForApp/v2/"
    parsed = urlsplit(source["url"])
    if normalize_url(source["url"]).split("?", 1)[0] != endpoint or any(key != "appid" or value != str(source["steam_appid"]) for key, value in parse_qsl(parsed.query)):
        raise ValidationError("steam_public_endpoint_required")
    query = urlencode({"appid": source["steam_appid"], "count": min(100, max(1, count)), "maxlength": 0,
                       "feeds": "steam_community_announcements", "enddate": int(enddate)})
    return {**source, "url": endpoint + "?" + query}


def parse_steam_documents(raw, source, observed_at):
    try:
        payload = json.loads(raw)
    except (ValueError, UnicodeError) as error:
        raise ValidationError("steam_json_invalid") from error
    appnews = payload.get("appnews") if isinstance(payload, dict) else None
    if not isinstance(appnews, dict) or appnews.get("appid") != source.get("steam_appid"):
        raise ValidationError("steam_response_game_mismatch")
    items = appnews.get("newsitems")
    if not isinstance(items, list) or len(items) > 100:
        raise ValidationError("steam_items_invalid")
    accepted, skipped = [], []
    for item in items:
        if not isinstance(item, dict):
            raise ValidationError("steam_item_invalid")
        if item.get("feedname") not in source.get("feed_allowlist", ["steam_community_announcements"]):
            skipped.append("non_publisher_feed")
            continue
        url = normalize_url(item.get("url", ""))
        if urlsplit(url).hostname not in source.get("allowed_notice_hosts", []):
            skipped.append("non_official_article_host")
            continue
        if urlsplit(url).hostname == "steamstore-a.akamaihd.net":
            expected = "/news/externalpost/steam_community_announcements/" + str(item.get("gid", ""))
            if urlsplit(url).path != expected or not re.fullmatch(r"\d+", str(item.get("gid", ""))) or urlsplit(url).query:
                skipped.append("invalid_provider_announcement_route")
                continue
        if item.get("appid", source["steam_appid"]) != source["steam_appid"]:
            raise ValidationError("steam_item_game_mismatch")
        title, content, gid = item.get("title"), item.get("contents", ""), item.get("gid")
        if not isinstance(title, str) or not 1 <= len(title.strip()) <= 500 or not isinstance(content, str) or not isinstance(gid, str) or not gid:
            raise ValidationError("steam_document_fields_invalid")
        published, raw_date = None, item.get("date")
        if isinstance(raw_date, int) and not isinstance(raw_date, bool) and raw_date > 0:
            try:
                published = datetime.fromtimestamp(raw_date, UTC).isoformat()
            except (ValueError, OverflowError, OSError):
                pass
        original_json = json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        hints = [kind for kind, patterns in TYPES.items() if any(re.search(pattern, title + "\n" + content, re.I) for pattern in patterns)]
        accepted.append({"external_key": gid, "raw_sha256": hashlib.sha256(original_json.encode()).hexdigest(),
            "raw_json": item, "raw_publication_timestamp": raw_date, "published_at": published,
            "source_updated_at": None, "observed_at": observed_at, "official_url": url,
            "original_title": title, "original_description": content, "original_language": "unknown",
            "server_region": source.get("server_region") or "UNSPECIFIED", "server_label": source.get("server_label"),
            "event_type_candidates": hints, "classification_status": "pending", "schedule_status": "DATE_UNCONFIRMED",
            "starts_at": None, "ends_at": None, "reservation_starts_at": None, "reservation_ends_at": None,
            "ticket_sales_at": None, "original_timezone": None,
            "pickups": [{"game_id": source["game_id"], "server_region": source.get("server_region") or "UNSPECIFIED",
                         "server_label": source.get("server_label"), "pickup_kind": None, "characters": [], "weapons": [],
                         "starts_at": None, "ends_at": None, "official_url": url, "verification_status": "pending"}] if "pickup" in hints else []})
    return accepted, {"items_seen": len(items), "items_accepted": len(accepted), "items_skipped": len(skipped), "skip_reasons": skipped}


def notice_event(document, source, categories):
    """A pending source notice for operating DB; absent dates stay NULL.

    Canonical identity includes source/game/server context, so regional announcements
    with the same publisher identifier are never accidentally merged together.
    """
    identity = {"game": source["game_id"], "source": source["id"], "region": document["server_region"],
                "server": document["server_label"], "pickup_type": None, "external_key": document["external_key"]}
    return {"canonical_key": hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest(),
        "notice_only": True,
        "original_title": document["original_title"], "original_description": document["original_description"],
        "original_language": document["original_language"], "official_notice_url": document["official_url"],
        "game_ids": [source["game_id"]], "regions": [document["server_region"]],
        "category_id": categories["unclassified"], "source_published_at": document["published_at"],
        "source_updated_at": document["source_updated_at"], "pickups": document["pickups"]}
