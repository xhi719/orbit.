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


YOUTUBE_API_ENDPOINT = "https://www.googleapis.com/youtube/v3/playlistItems"
YOUTUBE_VIDEO_DETAILS_ENDPOINT = "https://www.googleapis.com/youtube/v3/videos"
YOUTUBE_API_DOCUMENTATION = "https://developers.google.com/youtube/v3/docs/playlistItems/list"
_YOUTUBE_CHANNEL_ID = re.compile(r"UC[A-Za-z0-9_-]{22}")
_YOUTUBE_UPLOADS_ID = re.compile(r"UU[A-Za-z0-9_-]{22}")
_YOUTUBE_VIDEO_ID = re.compile(r"[A-Za-z0-9_-]{11}")
_YOUTUBE_PAGE_TOKEN = re.compile(r"[A-Za-z0-9_+=./:-]{1,512}")


def youtube_api_request(source, page_token=None, count=50):
    """Public uploads metadata only. A separate transport supplies a restricted
    API key in its header; source URLs, dictionaries and reports contain no key.
    The uploads playlist must have been observed from the official channel API,
    rather than manufactured by replacing a channel-ID prefix.
    """
    channel, playlist = source.get("youtube_channel_id"), source.get("youtube_uploads_playlist_id")
    if (source.get("parser") != "youtube_data_api" or not isinstance(channel, str) or not _YOUTUBE_CHANNEL_ID.fullmatch(channel)
        or not isinstance(playlist, str) or not _YOUTUBE_UPLOADS_ID.fullmatch(playlist)):
        raise ValidationError("youtube_api_source_identity_required")
    if any(str(key).lower() in {"key", "api_key", "apikey", "headers", "authorization", "access_token", "accesstoken"} for key in source):
        raise ValidationError("youtube_api_credentials_forbidden")
    try:
        parsed = urlsplit(source.get("url", ""))
        query = parse_qsl(parsed.query, keep_blank_values=True)
        if (parsed.scheme != "https" or parsed.hostname != "www.googleapis.com" or parsed.port not in (None,443)
            or parsed.username or parsed.password or parsed.path != "/youtube/v3/playlistItems" or parsed.fragment):
            raise ValidationError("youtube_api_public_endpoint_required")
    except (ValueError, TypeError) as error:
        raise ValidationError("youtube_api_public_endpoint_required") from error
    if len({key for key, _ in query}) != len(query) or any(key not in {"part", "playlistId", "maxResults", "pageToken"} for key, _ in query):
        raise ValidationError("youtube_api_query_invalid")
    for key, value in query:
        if (key == "part" and value != "snippet,contentDetails" or key == "playlistId" and value != playlist
            or key == "maxResults" and not re.fullmatch(r"(?:[1-9]|[1-4][0-9]|50)", value)
            or key == "pageToken" and not _YOUTUBE_PAGE_TOKEN.fullmatch(value)):
            raise ValidationError("youtube_api_query_invalid")
    if not isinstance(count,int) or isinstance(count,bool) or not 1 <= count <= 50:
        raise ValidationError("youtube_api_count_invalid")
    if page_token is not None and (not isinstance(page_token,str) or not _YOUTUBE_PAGE_TOKEN.fullmatch(page_token)):
        raise ValidationError("youtube_api_page_token_invalid")
    parameters = {"part":"snippet,contentDetails", "playlistId":playlist, "maxResults":count}
    if page_token is not None: parameters["pageToken"] = page_token
    return {**source, "url":YOUTUBE_API_ENDPOINT+"?"+urlencode(parameters), "method":"official_api", "official_host":"www.googleapis.com", "enabled":True}


def youtube_video_details_request(source, video_ids):
    """One bounded official metadata request, never search/scrape inference."""
    youtube_api_request(source)
    if (not isinstance(video_ids, list) or not 1 <= len(video_ids) <= 50
        or any(not isinstance(video, str) or not _YOUTUBE_VIDEO_ID.fullmatch(video) for video in video_ids) or len(set(video_ids)) != len(video_ids)):
        raise ValidationError("youtube_video_details_ids_invalid")
    return {**source, "url": YOUTUBE_VIDEO_DETAILS_ENDPOINT + "?" + urlencode({"part": "snippet,liveStreamingDetails", "id": ",".join(video_ids)}),
        "method": "official_api", "official_host": "www.googleapis.com", "enabled": True}


def attach_youtube_video_details(raw, source, documents):
    """Bind exact live start facts to their same-channel upload's raw hash.

    An official live point has no invented duration/end. Non-live or disappeared
    videos stay without a point. Playlist publication never supplies this time.
    """
    video_ids = [document["external_key"] for document in documents]
    youtube_video_details_request(source, video_ids)
    if not isinstance(raw, (bytes, str)) or len(raw) > 2097152:
        raise ValidationError("youtube_video_details_response_too_large")
    try:
        payload = json.loads(raw)
    except (ValueError, UnicodeError) as error:
        raise ValidationError("youtube_video_details_json_invalid") from error
    if not isinstance(payload, dict) or "error" in payload or payload.get("kind") != "youtube#videoListResponse":
        raise ValidationError("youtube_video_details_response_invalid")
    items = payload.get("items")
    if not isinstance(items, list) or len(items) > len(video_ids):
        raise ValidationError("youtube_video_details_items_invalid")
    by_id = {}
    for item in items:
        if not isinstance(item, dict) or item.get("kind") != "youtube#video" or item.get("id") not in video_ids or item["id"] in by_id:
            raise ValidationError("youtube_video_details_identity_mismatch")
        snippet = item.get("snippet")
        if not isinstance(snippet, dict) or snippet.get("channelId") != source["youtube_channel_id"]:
            raise ValidationError("youtube_video_details_channel_mismatch")
        document = next(document for document in documents if document["external_key"] == item["id"])
        if snippet.get("title") != document["original_title"]:
            raise ValidationError("youtube_video_details_title_changed")
        details = item.get("liveStreamingDetails") or {}
        if not isinstance(details, dict):
            raise ValidationError("youtube_video_details_live_fields_invalid")
        kept = {}
        for field in ("scheduledStartTime", "actualStartTime", "actualEndTime"):
            if field in details:
                aware_timestamp(details[field])
                kept[field] = details[field]
        if "actualStartTime" in kept and "actualEndTime" in kept and aware_timestamp(kept["actualEndTime"]) < aware_timestamp(kept["actualStartTime"]):
            raise ValidationError("youtube_video_details_end_before_start")
        by_id[item["id"]] = kept
    result = []
    for document in documents:
        copy = {**document}
        if document["external_key"] in by_id:
            raw_item = {**document["raw_json"], "liveStreamingDetails": by_id[document["external_key"]]}
            copy.update(raw_json=raw_item, raw_sha256=hashlib.sha256(json.dumps(raw_item, sort_keys=True, ensure_ascii=False).encode()).hexdigest(),
                source_live_details=by_id[document["external_key"]])
        result.append(copy)
    return result


def parse_youtube_api_documents(raw, source, observed_at):
    """Normalize the official uploads playlist without treating its add date as
    the video's public release, or its thumbnail as a character pickup banner.
    Private/deleted placeholders without a public release date are skipped.
    Cross-channel, cross-playlist and cross-video identities fail closed.
    """
    youtube_api_request(source)
    if not isinstance(raw,(bytes,str)) or len(raw) > 2097152:
        raise ValidationError("youtube_api_response_too_large")
    try:
        payload = json.loads(raw)
    except (ValueError,UnicodeError) as error:
        raise ValidationError("youtube_api_json_invalid") from error
    if not isinstance(payload,dict) or "error" in payload:
        raise ValidationError("youtube_api_error_response")
    if payload.get("kind") != "youtube#playlistItemListResponse":
        raise ValidationError("youtube_api_response_kind_invalid")
    items, continuation = payload.get("items"), payload.get("nextPageToken")
    if not isinstance(items,list) or len(items) > 50:
        raise ValidationError("youtube_api_items_invalid")
    if continuation is not None and (not isinstance(continuation,str) or not _YOUTUBE_PAGE_TOKEN.fullmatch(continuation)):
        raise ValidationError("youtube_api_page_token_invalid")
    channel, playlist = source["youtube_channel_id"], source["youtube_uploads_playlist_id"]
    documents, seen, skipped = [], set(), []
    for item in items:
        if not isinstance(item,dict) or item.get("kind") != "youtube#playlistItem":
            raise ValidationError("youtube_api_item_invalid")
        snippet, details = item.get("snippet"), item.get("contentDetails")
        if not isinstance(snippet,dict) or not isinstance(details,dict):
            raise ValidationError("youtube_api_item_invalid")
        if snippet.get("playlistId") != playlist or snippet.get("channelId") != channel:
            raise ValidationError("youtube_api_response_channel_mismatch")
        published = details.get("videoPublishedAt")
        if published is None:
            skipped.append("video_publication_unconfirmed")
            continue
        if snippet.get("videoOwnerChannelId") != channel:
            raise ValidationError("youtube_api_video_owner_mismatch")
        identity = snippet.get("resourceId")
        video = details.get("videoId")
        if (not isinstance(identity,dict) or identity.get("kind") != "youtube#video" or not isinstance(video,str)
            or not _YOUTUBE_VIDEO_ID.fullmatch(video) or identity.get("videoId") != video or video in seen):
            raise ValidationError("youtube_api_video_identity_mismatch")
        seen.add(video)
        title, content, playlist_item = snippet.get("title"), snippet.get("description", ""), item.get("id")
        if (not isinstance(title,str) or not 1 <= len(title.strip()) <= 500 or not isinstance(content,str) or len(content)>200000
            or not isinstance(playlist_item,str) or not 1 <= len(playlist_item) <= 256 or any(ord(char)<32 for char in playlist_item)):
            raise ValidationError("youtube_api_document_fields_invalid")
        published_time = aware_timestamp(published)
        added = snippet.get("publishedAt")
        if added is not None: aware_timestamp(added)
        thumbnails = []
        thumbnail_metadata = {}
        image_map = snippet.get("thumbnails", {})
        if not isinstance(image_map,dict): raise ValidationError("youtube_api_thumbnails_invalid")
        for quality in ("maxres","standard","high","medium","default","fhd","qhd","uhd"):
            image = image_map.get(quality)
            if not isinstance(image,dict): continue
            try:
                url = normalize_url(image.get("url"))
                parsed = urlsplit(url)
                if (parsed.hostname not in ("i.ytimg.com","i1.ytimg.com","i2.ytimg.com","i3.ytimg.com","i4.ytimg.com")
                    or not re.fullmatch(r"/(?:vi|vi_webp)/"+re.escape(video)+r"/[A-Za-z0-9_.-]+",parsed.path)):
                    continue
            except (ValidationError,ValueError,TypeError): continue
            if url not in thumbnails: thumbnails.append(url)
            width,height=image.get("width"),image.get("height")
            if isinstance(width,int) and not isinstance(width,bool) and isinstance(height,int) and not isinstance(height,bool) and 0<width<=16384 and 0<height<=16384:
                thumbnail_metadata[url]={"width":width,"height":height,"quality":quality}
        url = "https://www.youtube.com/watch?"+urlencode({"v":video})
        raw_item = {"channelId":channel,"videoId":video,"title":title,"description":content,"published":published,"updated":None,
            "officialUrl":url,"originalVideoUrl":url,"thumbnailUrls":thumbnails,"thumbnailMetadata":thumbnail_metadata,"playlistItemId":playlist_item,
            "uploadsPlaylistId":playlist,"addedToPlaylistAt":added}
        original_json = json.dumps(raw_item,ensure_ascii=False,sort_keys=True)
        hints = [kind for kind,patterns in TYPES.items() if any(re.search(pattern,title+"\n"+content,re.I) for pattern in patterns)]
        documents.append({"external_key":video,"raw_sha256":hashlib.sha256(original_json.encode()).hexdigest(),"raw_json":raw_item,
            "raw_publication_timestamp":published,"published_at":published_time.isoformat(),"source_updated_at":None,
            "observed_at":observed_at,"official_url":url,"original_title":title,"original_description":content,
            "source_image_metadata":thumbnail_metadata,
            "original_language":source.get("content_language") or "unknown","server_region":source.get("server_region") or "UNSPECIFIED",
            "server_label":source.get("server_label"),"event_type_candidates":hints,"pickups":[],"source_image_urls":thumbnails,
            "source_provider":"youtube","source_channel_id":channel,"source_video_id":video})
    return documents, {"items_seen":len(items),"items_accepted":len(documents),"items_skipped":len(skipped),"skip_reasons":skipped,
        "next_page_token":continuation,"window_coverage_verified":False,"coverage_reason":"PAGINATED_UPLOADS_BOUNDARY_NOT_YET_VERIFIED"}


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
