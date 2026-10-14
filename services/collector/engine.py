"""Official-source extraction. Feed publish dates are never treated as event dates."""
from __future__ import annotations
import hashlib
import html
from html.parser import HTMLParser
import http.client
import ipaddress
import json
import os
import re
import socket
import ssl
from datetime import datetime
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit
import uuid
import xml.etree.ElementTree as ET
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

TIME_FIELDS = ("starts_at", "ends_at", "reservation_starts_at", "reservation_ends_at", "ticket_sales_at")
STATUS_VALUES = {"upcoming", "ongoing", "ended", "postponed", "cancelled"}
KNOWN_FIELDS = {"category_id", "category_slug", "canonical_key", "original_title", "original_description", "original_language", "regions", "venue",
                "original_timezone", "official_notice_url", "official_reservation_url", "official_sales_url", "status", "game_ids", "content_hash",
                "source_published_at", "source_updated_at", "pickups", "notice_only", *TIME_FIELDS}


class ValidationError(ValueError):
    pass


def image_dimensions(raw: bytes) -> tuple[int, int] | None:
    """Read bounded raster headers only; no decoder, generated URL or pixel storage.

    A successful image HTTP response may still be YouTube's tiny placeholder.
    The caller also compares these dimensions with that exact API image entry.
    """
    if not isinstance(raw, bytes) or len(raw) > 2097152:
        return None
    width = height = 0
    if raw.startswith(b'\x89PNG\r\n\x1a\n') and len(raw) >= 24 and raw[12:16] == b'IHDR':
        width, height = int.from_bytes(raw[16:20], 'big'), int.from_bytes(raw[20:24], 'big')
    elif raw.startswith(b'\xff\xd8'):
        cursor = 2
        for _ in range(4096):
            if cursor + 2 > len(raw) or raw[cursor] != 255:
                return None
            while cursor < len(raw) and raw[cursor] == 255:
                cursor += 1
            if cursor >= len(raw): return None
            marker = raw[cursor]; cursor += 1
            if marker in (0xD9, 0xDA): return None
            if marker in (0x01, *range(0xD0, 0xD9)): continue
            if cursor + 2 > len(raw): return None
            length = int.from_bytes(raw[cursor:cursor+2], 'big')
            if length < 2 or cursor + length > len(raw): return None
            if marker in (0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF):
                if length < 8: return None
                height, width = int.from_bytes(raw[cursor+3:cursor+5], 'big'), int.from_bytes(raw[cursor+5:cursor+7], 'big')
                break
            cursor += length
    elif raw.startswith(b'RIFF') and len(raw) >= 30 and raw[8:12] == b'WEBP':
        tag = raw[12:16]
        if tag == b'VP8X':
            width, height = 1 + int.from_bytes(raw[24:27], 'little'), 1 + int.from_bytes(raw[27:30], 'little')
        elif tag == b'VP8 ' and raw[23:26] == b'\x9d\x01\x2a':
            width, height = int.from_bytes(raw[26:28], 'little') & 16383, int.from_bytes(raw[28:30], 'little') & 16383
        elif tag == b'VP8L' and raw[20] == 0x2F:
            bits = int.from_bytes(raw[21:25], 'little')
            width, height = 1 + (bits & 16383), 1 + ((bits >> 14) & 16383)
    return (width, height) if 0 < width <= 16384 and 0 < height <= 16384 else None


def normalize_url(value: str) -> str:
    if not isinstance(value, str) or len(value) > 4096:
        raise ValidationError("invalid_url")
    parsed = urlsplit(value)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.port not in (None, 443):
        raise ValidationError("https_url_required")
    query = [(key, val) for key, val in parse_qsl(parsed.query, keep_blank_values=True)
             if not key.lower().startswith("utm_") and key.lower() not in {"fbclid", "gclid"}]
    return urlunsplit(("https", parsed.hostname.lower(), parsed.path or "/", urlencode(sorted(query)), ""))


def validate_event(item: dict, source: dict, categories: dict[str, str]) -> dict:
    if not isinstance(item, dict) or set(item) - KNOWN_FIELDS:
        raise ValidationError("event_schema_unknown_fields")
    title = item.get("original_title")
    if not isinstance(title, str) or not 1 <= len(title.strip()) <= 500:
        raise ValidationError("event_title_required")
    notice = normalize_url(item.get("official_notice_url", ""))
    approved_hosts = {source["official_host"].lower()}
    if source.get("notice_hosts_verified_at") and source.get("terms_checked_at") and source.get("access_status") == "approved":
        approved_hosts.update(host.lower() for host in source.get("allowed_notice_hosts", []) if isinstance(host, str))
    if urlsplit(notice).hostname not in approved_hosts:
        raise ValidationError("outside_approved_official_host")
    description = item.get("original_description", "")
    if not isinstance(description, str) or len(description) > 50000:
        raise ValidationError("invalid_event_description")
    event = {"original_title": title.strip(), "original_description": description, "official_notice_url": notice,
             "canonical_key": item.get("canonical_key") or hashlib.sha256(notice.encode()).hexdigest(),
             "original_language": item.get("original_language", "unknown"), "status": item.get("status", "upcoming"),
             "regions": item.get("regions", ["UNSPECIFIED"]), "venue": item.get("venue"),
             "original_timezone": item.get("original_timezone"),
             "game_ids": item.get("game_ids", [source["game_id"]] if source.get("game_id") else [])}
    if not isinstance(event["canonical_key"], str) or not 1 <= len(event["canonical_key"]) <= 500:
        raise ValidationError("invalid_canonical_key")
    if event["status"] not in STATUS_VALUES:
        raise ValidationError("invalid_event_status")
    if not isinstance(event["regions"], list) or not 1 <= len(event["regions"]) <= 30 or any(not isinstance(r, str) or len(r) > 20 for r in event["regions"]):
        raise ValidationError("invalid_regions")
    if not isinstance(event["original_language"], str) or len(event["original_language"]) > 20:
        raise ValidationError("invalid_language")
    if event["venue"] is not None and (not isinstance(event["venue"], str) or len(event["venue"]) > 2000):
        raise ValidationError("invalid_venue")
    if not isinstance(event["game_ids"], list) or not event["game_ids"] or len(event["game_ids"]) > 100:
        raise ValidationError("associated_game_required")
    try:
        for game_id in event["game_ids"]:
            uuid.UUID(game_id)
    except (ValueError, TypeError, AttributeError) as error:
        raise ValidationError("invalid_game_id") from error
    category = item.get("category_id") or categories.get(item.get("category_slug", "unclassified"))
    if category not in categories.values():
        raise ValidationError("unknown_event_category")
    event["category_id"] = category
    timezone = event["original_timezone"]
    if timezone:
        try:
            ZoneInfo(timezone)
        except (ZoneInfoNotFoundError, TypeError) as error:
            raise ValidationError("invalid_original_timezone") from error
    for field in TIME_FIELDS:
        value = item.get(field)
        event[field] = None
        if value is not None:
            try:
                if not isinstance(value, str) or "T" not in value:
                    raise ValueError()
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                if parsed.tzinfo is None or parsed.utcoffset() is None:
                    raise ValueError()
                if not timezone:
                    raise ValidationError("explicit_original_timezone_required")
                event[field] = parsed.isoformat()
            except (ValueError, TypeError) as error:
                raise ValidationError("explicit_timestamp_with_offset_required:" + field) from error
    for begin, end in (("starts_at", "ends_at"), ("reservation_starts_at", "reservation_ends_at")):
        if event[begin] and event[end] and datetime.fromisoformat(event[end]) < datetime.fromisoformat(event[begin]):
            raise ValidationError("invalid_date_range:" + begin)
    for field in ("official_reservation_url", "official_sales_url"):
        event[field] = normalize_url(item[field]) if item.get(field) else None
    if "notice_only" in item:
        if not isinstance(item["notice_only"], bool):
            raise ValidationError("invalid_notice_only_flag")
        event["notice_only"] = item["notice_only"]
    for field in ("source_published_at", "source_updated_at"):
        if field not in item:
            continue  # Preserve old canonical hashes when no metadata is supplied.
        value = item[field]
        if value is None:
            event[field] = None
            continue
        try:
            if not isinstance(value, str) or "T" not in value:
                raise ValueError()
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None or parsed.utcoffset() is None:
                raise ValueError()
            event[field] = parsed.isoformat()
        except (ValueError, TypeError) as error:
            raise ValidationError("explicit_source_timestamp_required:" + field) from error
    if "pickups" in item:
        pickups = item["pickups"]
        if not isinstance(pickups, list) or len(pickups) > 100:
            raise ValidationError("invalid_pickups")
        validated = []
        allowed_fields = {"game_id", "server_region", "server_label", "pickup_kind", "characters", "weapons", "starts_at", "ends_at", "official_url", "verification_status"}
        for pickup in pickups:
            if not isinstance(pickup, dict) or set(pickup) - allowed_fields or pickup.get("game_id") not in event["game_ids"]:
                raise ValidationError("invalid_pickup_game_or_fields")
            if pickup.get("verification_status", "pending") != "pending":
                raise ValidationError("automatic_pickup_verification_forbidden")
            record = {**pickup, "server_region": pickup.get("server_region") or "UNSPECIFIED", "verification_status": "pending"}
            if record["server_region"] not in event["regions"]:
                raise ValidationError("pickup_region_not_event_region")
            for field, maximum in (("server_label", 200), ("pickup_kind", 80)):
                value = record.get(field)
                if value is not None and (not isinstance(value, str) or not 1 <= len(value) <= maximum):
                    raise ValidationError("invalid_pickup_" + field)
            for field in ("characters", "weapons"):
                values = record.get(field, [])
                if not isinstance(values, list) or len(values) > 100 or any(not isinstance(value, str) or not 1 <= len(value) <= 200 for value in values):
                    raise ValidationError("invalid_pickup_targets")
                record[field] = values
            record["official_url"] = normalize_url(record.get("official_url", notice))
            if urlsplit(record["official_url"]).hostname not in approved_hosts:
                raise ValidationError("outside_approved_pickup_host")
            for field in ("starts_at", "ends_at"):
                value = record.get(field)
                record[field] = None
                if value is not None:
                    try:
                        if not isinstance(value, str) or "T" not in value or not timezone:
                            raise ValueError()
                        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                        if parsed.tzinfo is None or parsed.utcoffset() is None:
                            raise ValueError()
                        record[field] = parsed.isoformat()
                    except (ValueError, TypeError) as error:
                        raise ValidationError("explicit_pickup_timestamp_required") from error
            if record["starts_at"] and record["ends_at"] and datetime.fromisoformat(record["ends_at"]) < datetime.fromisoformat(record["starts_at"]):
                raise ValidationError("invalid_pickup_date_range")
            validated.append(record)
        event["pickups"] = validated
    event["content_hash"] = hashlib.sha256(json.dumps(event, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    return event


class _JsonLdParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reading, self.current, self.blocks = False, [], []

    def handle_starttag(self, tag, attrs):
        if tag == "script" and dict(attrs).get("type") == "application/ld+json":
            self.reading, self.current = True, []

    def handle_data(self, data):
        if self.reading:
            self.current.append(data)

    def handle_endtag(self, tag):
        if tag == "script" and self.reading:
            self.blocks.append(json.loads("".join(self.current)))
            self.reading = False


def parse_payload(raw: bytes, parser: str, source: dict) -> list[dict]:
    if parser == "structured_json":
        payload = json.loads(raw)
        items = payload.get("events", []) if isinstance(payload, dict) else payload
        if not isinstance(items, list) or len(items) > 2000:
            raise ValidationError("event_array_required")
        return items
    if parser == "rss":
        if b"<!DOCTYPE" in raw.upper() or b"<!ENTITY" in raw.upper():
            raise ValidationError("xml_entities_not_allowed")
        root = ET.fromstring(raw)
        items = []
        for node in root.findall(".//item"):
            items.append({"original_title": node.findtext("title", ""), "original_description": html.unescape(node.findtext("description", "")),
                          "official_notice_url": node.findtext("link", "")})
        atom = "{http://www.w3.org/2005/Atom}"
        for node in root.findall(atom + "entry"):
            links = [link for link in node.findall(atom + "link") if link.get("rel", "alternate") == "alternate"]
            items.append({"original_title": node.findtext(atom + "title", ""), "original_description": node.findtext(atom + "summary", ""),
                          "official_notice_url": links[0].get("href", "") if links else ""})
        # pubDate/updated are publication timestamps, so no starts_at is inferred.
        if len(items) > 2000:
            raise ValidationError("feed_item_limit")
        return items
    if parser == "html_jsonld":
        # Arbitrary text/LLM extraction is intentionally unavailable without an approved adapter.
        parser_obj = _JsonLdParser()
        parser_obj.feed(raw.decode("utf-8"))
        result = []
        for block in parser_obj.blocks:
            objects = block if isinstance(block, list) else block.get("@graph", [block])
            for obj in objects:
                if not isinstance(obj, dict) or obj.get("@type") != "Event":
                    continue
                status = {"https://schema.org/EventCancelled": "cancelled", "https://schema.org/EventPostponed": "postponed"}.get(obj.get("eventStatus"), "upcoming")
                result.append({"original_title": obj.get("name"), "original_description": obj.get("description", ""),
                               "official_notice_url": obj.get("url", source["url"]), "starts_at": obj.get("startDate"), "ends_at": obj.get("endDate"),
                               "original_timezone": obj.get("originalTimezone"), "status": status})
        # JSON-LD often omits IANA timezone. Those items fail validation and require manual entry.
        return result
    raise ValidationError("unsupported_source_parser")


def _safe_address(host: str) -> str:
    addresses = [entry[4][0] for entry in socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)]
    if not addresses or any(not ipaddress.ip_address(address).is_global for address in addresses):
        raise ValidationError("private_source_address_blocked")
    return addresses[0]


class _PinnedHTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, host, address, timeout):
        super().__init__(host, timeout=timeout, context=ssl.create_default_context())
        self.address = address

    def connect(self):
        self.sock = socket.create_connection((self.address, self.port), self.timeout)
        self.sock = self._context.wrap_socket(self.sock, server_hostname=self.host)


def fetch_source(source: dict) -> tuple[int, bytes, dict]:
    if not source.get("enabled") or source.get("access_status") != "approved" or not source.get("terms_checked_at") or not source.get("terms_url"):
        raise ValidationError("source_access_not_approved")
    url = source["url"]
    headers = {"User-Agent": "SubcultureHubCollector/1.0 (official-source; terms-reviewed)", "Accept": "application/json,application/rss+xml,application/atom+xml,text/html"}
    # This reviewed auxiliary route negotiates JSON when JSON appears first.
    # Its exact DOM adapter needs the publisher's public HTML representation;
    # retain the same identity, URL, access checks and redirect restrictions.
    if source.get("parser") == "official_auxiliary_notice":
        headers["Accept"] = "text/html"
    youtube_api = source.get("parser") == "youtube_data_api"
    if youtube_api:
        endpoint = urlsplit(normalize_url(url))
        pairs = parse_qsl(endpoint.query, keep_blank_values=True)
        parameters = dict(pairs)
        parts = parameters.get("part", "").split(",")
        playlist_request = (endpoint.path == "/youtube/v3/playlistItems"
            and not set(parameters) - {"part", "playlistId", "maxResults", "pageToken"}
            and len(parts) == 2 and set(parts) == {"snippet", "contentDetails"}
            and re.fullmatch(r"UU[A-Za-z0-9_-]{22}", parameters.get("playlistId", ""))
            and ("maxResults" not in parameters or re.fullmatch(r"(?:[1-9]|[1-4][0-9]|50)", parameters["maxResults"]))
            and ("pageToken" not in parameters or re.fullmatch(r"[A-Za-z0-9_+=./:-]{1,512}", parameters["pageToken"])))
        video_ids = parameters.get("id", "").split(",")
        # Only public metadata of explicitly identified videos is allowed. No
        # account operations, search, owner-only parts or credential URL query.
        video_request = (endpoint.path == "/youtube/v3/videos"
            and set(parameters) == {"part", "id"}
            and len(parts) == 2 and set(parts) == {"snippet", "liveStreamingDetails"}
            and 1 <= len(video_ids) <= 50 and len(set(video_ids)) == len(video_ids)
            and all(re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id) for video_id in video_ids))
        if endpoint.hostname != "www.googleapis.com" or source.get("official_host") != "www.googleapis.com" \
            or len(pairs) != len(parameters) or not (playlist_request or video_request):
            raise ValidationError("youtube_api_endpoint_not_approved")
        key = os.environ.get("YOUTUBE_API_KEY", "")
        if not re.fullmatch(r"AIza[A-Za-z0-9_-]{35}", key):
            raise ValidationError("youtube_api_key_not_configured")
        headers["X-Goog-Api-Key"] = key
    if source.get("etag"):
        headers["If-None-Match"] = source["etag"]
    if source.get("last_modified"):
        headers["If-Modified-Since"] = source["last_modified"]
    for _ in range(4):
        parsed = urlsplit(normalize_url(url))
        if parsed.hostname != source["official_host"].lower():
            raise ValidationError("redirect_outside_official_host")
        address = _safe_address(parsed.hostname)
        connection = _PinnedHTTPSConnection(parsed.hostname, address, source.get("request_timeout_seconds", 20))
        try:
            connection.request("GET", urlunsplit(("", "", parsed.path, parsed.query, "")), headers=headers)
            response = connection.getresponse()
            response_headers = {key.lower(): value for key, value in response.getheaders()}
            if response.status in (301, 302, 303, 307, 308):
                if youtube_api:
                    raise ValidationError("authenticated_source_redirect_blocked")
                url = urljoin(url, response_headers.get("location", ""))
                continue
            if response.status == 304:
                return 304, b"", response_headers
            if response.status != 200:
                return response.status, b"", response_headers
            maximum = source.get("max_response_bytes", 2097152)
            raw = response.read(maximum + 1)
            if len(raw) > maximum:
                raise ValidationError("source_response_too_large")
            return response.status, raw, response_headers
        finally:
            connection.close()
    raise ValidationError("redirect_limit")
