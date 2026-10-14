"""Strict public Steam publisher-news adapter. Never turns publication into schedule.

Labels extracted from keywords are review candidates, not verified classifications.
Steam's documented public feed does not specify server regions or event boundaries.
"""
from __future__ import annotations
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
from html.parser import HTMLParser
from urllib.parse import urlencode, urlsplit, parse_qsl, urljoin
from zoneinfo import ZoneInfo
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
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
        # ORBIT stores the source title, thumbnail and watch link, not video
        # descriptions. Discard before building the persisted snapshot/hash or
        # classification hints; descriptions never establish event dates.
        content = ""
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


def official_html_request(source,page=1,article_url=None):
    """Reviewed publisher board, not arbitrary selectors, login or scraping."""
    policy=source.get("article_format") or {}
    if (source.get("parser")!="official_html" or policy.get("mode")!="nexon_board_v1" or policy.get("version")!=1
        or set(policy)-{"mode","version","reviewedAt","evidenceUrl","timezone","allowedImageHosts"}
        or policy.get("timezone")!="Asia/Seoul" or not policy.get("reviewedAt") or not policy.get("evidenceUrl")):
        raise ValidationError("reviewed_official_article_format_required")
    aware_timestamp(policy["reviewedAt"])
    if not normalize_url(policy["evidenceUrl"]).startswith("https://"):raise ValidationError("article_format_evidence_required")
    base=urlsplit(normalize_url(source.get("url")))
    if base.scheme!="https" or base.hostname!="www.nexongames.co.kr" or base.path!="/bbs/board.php" or base.fragment:
        raise ValidationError("exact_publisher_board_required")
    pairs=parse_qsl(base.query,keep_blank_values=True)
    if len(pairs)!=1 or pairs!=[("bo_table","media_event")]:raise ValidationError("exact_publisher_board_required")
    hosts=policy.get("allowedImageHosts")
    if not isinstance(hosts,list) or hosts!=["www.nexongames.co.kr"]:raise ValidationError("exact_article_image_hosts_required")
    if article_url:
        article=urlsplit(normalize_url(article_url));params=parse_qsl(article.query,keep_blank_values=True);query=dict(params)
        if (article.scheme!="https" or article.hostname!=base.hostname or article.path!=base.path or article.fragment
            or len(params)!=len(query) or set(query)-{"bo_table","wr_id","page"} or query.get("bo_table")!="media_event"
            or not re.fullmatch(r"[1-9]\d{0,8}",query.get("wr_id","")) or "page"in query and not re.fullmatch(r"[1-9]\d{0,4}",query["page"])):
            raise ValidationError("exact_publisher_article_required")
        query={"bo_table":"media_event","wr_id":query["wr_id"]}
    else:
        if type(page)is not int or not 1<=page<=1000:raise ValidationError("bounded_publisher_page_required")
        query={"bo_table":"media_event","page":page}
    return {**source,"url":"https://www.nexongames.co.kr/bbs/board.php?"+urlencode(query),"official_host":base.hostname,"enabled":True,"method":"public_page"}


class _PublisherBoardHTML(HTMLParser):
    """Exact reviewed board DOM fields; menus, profiles and scripts are omitted."""
    def __init__(self,source,url):
        super().__init__(convert_charrefs=True);self.source=source;self.url=url;self.stack=[];self.values={"title":[],"date":[],"body":[],"listdate":[]};self.images=[];self.links=[];self.anchor=None
    def handle_starttag(self,tag,attributes):
        attrs=dict(attributes);classes=attrs.get("class","").split();active={}
        if "bo_v_tit"in classes:active["title"]=True
        if "header_date"in classes:active["date"]=True
        if "wr_date"in classes:active["listdate"]=True;self.values["listdate"]=[]
        if attrs.get("id")=="bo_v_con":active["body"]=True
        if self.source.get("article_format",{}).get("linkedArticleFormat")=="prtimes_release_v1":
            if attrs.get("id")=="press-release-title":active["title"]=True
            if attrs.get("id")=="press-release-body":active["body"]=True
        if self.source.get("article_format",{}).get("linkedArticleFormat")=="ticketlink_notice_v1":
            if attrs.get("id")=="noticeTitle":active["title"]=True
            if "list_cont"in classes:active["body"]=True
            if "thumb"in classes:active["thumbnail"]=True
        body=any(node[1].get("body") for node in self.stack)
        if tag=="img" and (body or any(node[1].get("thumbnail")for node in self.stack)):
            try:
                url=normalize_url(urljoin(self.url,attrs.get("src","")))
                if urlsplit(url).hostname in self.source["article_format"]["allowedImageHosts"] and url not in self.images:self.images.append(url)
            except (ValidationError,ValueError,TypeError):pass
        if tag=="a":self.anchor={"href":urljoin(self.url,attrs.get("href","")),"text":[],"listTitle":"bo_tit"in classes,"publishedDate":"".join(self.values["listdate"]).strip()}
        if body and tag in ("p","br","div","li"):self.values["body"].append("\n")
        if tag not in ("img","br","hr","meta","link","input","source","area","base","embed","param","wbr"):self.stack.append((tag,active))
    def handle_startendtag(self,tag,attributes):
        self.handle_starttag(tag,attributes)
        if tag not in ("img","br","hr","meta","link","input","source","area","base","embed","param","wbr"):self.handle_endtag(tag)
    def handle_data(self,value):
        if any(node[0] in ("script","style") for node in self.stack):return
        for key in self.values:
            if any(node[1].get(key) for node in self.stack):self.values[key].append(value)
        if self.anchor:self.anchor["text"].append(value)
    def handle_endtag(self,tag):
        if tag=="a" and self.anchor:
            if self.anchor["listTitle"]:self.links.append({"url":self.anchor["href"],"title":re.sub(r"\s+"," ","".join(self.anchor["text"])).strip(),"publishedDate":self.anchor["publishedDate"]})
            self.anchor=None
        positions=[index for index,node in enumerate(self.stack)if node[0]==tag]
        if positions:self.stack=self.stack[:positions[-1]]


def parse_official_html_index(raw,source,page=1):
    request=official_html_request(source,page)
    if not isinstance(raw,(bytes,str)) or len(raw)>2097152:raise ValidationError("publisher_index_body_limit")
    parser=_PublisherBoardHTML(source,request["url"])
    try:parser.feed(raw.decode("utf-8-sig") if isinstance(raw,bytes) else raw)
    except (UnicodeError,ValueError)as error:raise ValidationError("publisher_index_html_invalid")from error
    found=[];seen=set()
    for link in parser.links:
        try:article=official_html_request(source,article_url=link["url"])
        except (ValidationError,ValueError,TypeError):continue
        if article["url"]not in seen:
            if not re.fullmatch(r"20\d{2}\.\d{2}\.\d{2}",link["publishedDate"]):raise ValidationError("publisher_list_date_required")
            seen.add(article["url"]);found.append({"url":article["url"],"title":link["title"],"publishedDate":link["publishedDate"]})
    if len(found)>100:raise ValidationError("bounded_publisher_article_list_required")
    return found


def parse_official_html_article(raw,source,url,observed_at):
    request=official_html_request(source,article_url=url)
    if not isinstance(raw,(bytes,str)) or len(raw)>2097152:raise ValidationError("publisher_article_body_limit")
    parser=_PublisherBoardHTML(source,request["url"])
    try:parser.feed(raw.decode("utf-8-sig") if isinstance(raw,bytes) else raw)
    except (UnicodeError,ValueError)as error:raise ValidationError("publisher_article_html_invalid")from error
    title=re.sub(r"\s+"," ","".join(parser.values["title"])).strip();day="".join(parser.values["date"]).strip();body="".join(parser.values["body"]).strip()
    if not 1<=len(title)<=500 or not 1<=len(body)<=200000 or not re.fullmatch(r"20\d{2}\.\d{2}\.\d{2}",day):raise ValidationError("explicit_article_title_publication_body_required")
    # The publisher prints a date, not a posting time. Midnight represents the
    # beginning of that reviewed local date; publicationPrecision preserves it.
    published=datetime.strptime(day,"%Y.%m.%d").replace(tzinfo=ZoneInfo(source["article_format"]["timezone"])).astimezone(UTC)
    identity=dict(parse_qsl(urlsplit(request["url"]).query))["wr_id"]
    snapshot={"articleId":identity,"title":title,"body":body,"publishedDate":day,"publicationPrecision":"day","publicationTimezone":source["article_format"]["timezone"],"officialUrl":request["url"],"imageUrls":parser.images[:20]}
    hints=[kind for kind,patterns in TYPES.items()if any(re.search(pattern,title+"\n"+body,re.I)for pattern in patterns)]
    return {"external_key":identity,"raw_sha256":hashlib.sha256(json.dumps(snapshot,ensure_ascii=False,sort_keys=True).encode()).hexdigest(),"raw_json":snapshot,
        "raw_publication_timestamp":day,"publication_precision":"day","publication_local_date":day.replace(".","-"),"published_at":published.isoformat(),"source_updated_at":None,"observed_at":observed_at,
        "official_url":request["url"],"original_title":title,"original_description":body,"original_language":source.get("content_language")or"unknown",
        "server_region":source.get("server_region")or"UNSPECIFIED","server_label":source.get("server_label"),"event_type_candidates":hints,"pickups":[],
        "source_provider":"official-site","source_image_urls":parser.images[:20],"official_article_image_urls":parser.images[:20]}


def official_rss_request(source):
    policy=source.get("article_format")or{}
    if (source.get("parser")!="official_rss" or policy.get("mode")!="official_rss_v1" or policy.get("version")!=1
        or set(policy)-{"mode","version","reviewedAt","evidenceUrl","allowedImageHosts","linkedArticleFormat"}
        or not isinstance(policy.get("allowedImageHosts"),list)or not 1<=len(policy["allowedImageHosts"])<=20):raise ValidationError("reviewed_publisher_rss_required")
    aware_timestamp(policy.get("reviewedAt"));normalize_url(policy.get("evidenceUrl"))
    url=normalize_url(source.get("url"));parsed=urlsplit(url)
    if parsed.scheme!="https"or parsed.hostname not in source.get("allowed_notice_hosts",[])or parsed.fragment:raise ValidationError("official_rss_host_required")
    linked=policy.get("linkedArticleFormat")
    if linked not in(None,"prtimes_release_v1"):raise ValidationError("reviewed_linked_article_format_required")
    if linked:
        query=parse_qsl(parsed.query,keep_blank_values=True)
        if (parsed.hostname!="prtimes.jp"or parsed.path!="/companyrdf.php"or len(query)!=1 or query[0][0]!="company_id"
            or not re.fullmatch(r"[1-9]\d{0,7}",query[0][1])or policy["allowedImageHosts"]!=["prcdn.freetls.fastly.net"]):raise ValidationError("exact_publisher_prtimes_rss_required")
    for host in policy["allowedImageHosts"]:
        if not isinstance(host,str)or not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?",host):raise ValidationError("exact_article_image_hosts_required")
    return {**source,"url":url,"official_host":parsed.hostname,"enabled":True,"method":"official_feed"}


def _rss_publication_window_bound(value):
    if value is None:return None
    if isinstance(value,datetime):
        if value.tzinfo is None or value.utcoffset() is None:raise ValidationError("rss_publication_window_timezone_required")
        return value.astimezone(UTC)
    return aware_timestamp(value)


def _rss_publication_clock(value,rdf):
    try:clock=aware_timestamp(value)if rdf else parsedate_to_datetime(value)
    except (ValueError,TypeError,OverflowError)as error:raise ValidationError("official_rss_publication_required")from error
    if clock.tzinfo is None or clock.utcoffset() is None:raise ValidationError("official_rss_publication_timezone_required")
    return clock.astimezone(UTC)


def _rss_publication_window(value,rdf,start,end):
    # Some publisher RDF archives expose a date alone. Such a date never
    # establishes a posting time or server timezone. Only omit it when its
    # entire possible day, conservatively allowing every UTC offset <24h,
    # is outside the requested window. Overlapping/invalid dates still fail.
    if rdf and isinstance(value,str)and re.fullmatch(r"\d{4}-\d{2}-\d{2}",value):
        try:
            day=datetime.fromisoformat(value).replace(tzinfo=UTC)
            earliest=day-timedelta(days=1);latest=day+timedelta(days=2)
        except (ValueError,TypeError,OverflowError)as error:raise ValidationError("official_rss_publication_required")from error
        if start is not None and latest<=start:return "before",None
        if end is not None and earliest>=end:return "after",None
        raise ValidationError("official_rss_publication_timezone_required")
    clock=_rss_publication_clock(value,rdf)
    if start is not None and clock<start:return "before",clock
    if end is not None and clock>=end:return "after",clock
    return None,clock


def parse_official_rss_documents(raw,source,observed_at,*,published_start=None,published_end=None):
    official_rss_request(source)
    start=_rss_publication_window_bound(published_start);end=_rss_publication_window_bound(published_end)
    if start is not None and end is not None and start>=end:raise ValidationError("ordered_rss_publication_window_required")
    has_window=start is not None or end is not None
    if not isinstance(raw,(bytes,str))or len(raw)>2097152:raise ValidationError("official_rss_body_limit")
    encoded=raw.encode()if isinstance(raw,str)else raw
    if b"\x00"in encoded or re.search(br"<!\s*(?:DOCTYPE|ENTITY)\b",encoded,re.I):raise ValidationError("official_rss_xml_unsafe")
    try:root=ET.fromstring(encoded.decode("utf-8-sig"))
    except (ET.ParseError,UnicodeError)as error:raise ValidationError("official_rss_xml_invalid")from error
    rdf_ns="{http://www.w3.org/1999/02/22-rdf-syntax-ns#}";rss_ns="{http://purl.org/rss/1.0/}";dc_ns="{http://purl.org/dc/elements/1.1/}"
    rdf=root.tag==rdf_ns+"RDF"
    if root.tag!="rss"and not rdf:raise ValidationError("official_rss_format_required")
    items=root.findall(rss_ns+"item")if rdf else root.findall("channel/item")
    if len(items)>(250 if rdf else 100):raise ValidationError("bounded_official_rss_items_required")
    content_ns="{http://purl.org/rss/1.0/modules/content/}";documents=[];seen=set();before=after=0
    for item in items:
        prefix=rss_ns if rdf else ""
        title=item.findtext(prefix+"title")or"";raw_url=item.findtext(prefix+"link")or"";date=item.findtext(dc_ns+"date")if rdf else item.findtext("pubDate");content=item.findtext(content_ns+"encoded")or item.findtext(prefix+"description")or"";clock=None
        if has_window:
            omitted,clock=_rss_publication_window(date,rdf,start,end)
            if omitted=="before":before+=1;continue
            if omitted=="after":after+=1;continue
        # Do not upgrade legacy HTTP articles. A proven out-of-window article
        # is omitted; every accepted article still needs the reviewed HTTPS
        # origin, timestamp and original RDF identity checks below.
        url=normalize_url(raw_url)
        if not 1<=len(title)<=500 or len(content)>200000 or urlsplit(url).hostname not in source.get("allowed_notice_hosts",[]):raise ValidationError("official_rss_article_identity_required")
        if clock is None:clock=_rss_publication_clock(date,rdf)
        if url in seen:raise ValidationError("official_rss_duplicate_article")
        seen.add(url);images=[]
        for value in re.findall(r"<img\b[^>]*\bsrc\s*=\s*['\"]([^'\"]+)['\"]",content,re.I):
            try:
                image=normalize_url(urljoin(url,value))
                if urlsplit(image).hostname in source["article_format"]["allowedImageHosts"]and image not in images:images.append(image)
            except (ValidationError,ValueError,TypeError):pass
        snapshot={"title":title,"body":content,"published":date,"officialUrl":url,"imageUrls":images[:20]}
        hints=[kind for kind,patterns in TYPES.items()if any(re.search(pattern,title+"\n"+content,re.I)for pattern in patterns)]
        if rdf and item.get(rdf_ns+"about")!=url:raise ValidationError("official_rdf_article_identity_mismatch")
        documents.append({"external_key":item.findtext("guid")or url,"raw_sha256":hashlib.sha256(json.dumps(snapshot,ensure_ascii=False,sort_keys=True).encode()).hexdigest(),"raw_json":snapshot,
            "raw_publication_timestamp":date,"published_at":clock.astimezone(UTC).isoformat(),"source_updated_at":None,"observed_at":observed_at,
            "official_url":url,"original_title":title,"original_description":content,"original_language":source.get("content_language")or"unknown",
            "server_region":source.get("server_region")or"UNSPECIFIED","server_label":source.get("server_label"),"event_type_candidates":hints,"pickups":[],
            "source_provider":"official-site","source_image_urls":images[:20],"official_article_image_urls":images[:20],
            "linked_article_required":source["article_format"].get("linkedArticleFormat")=="prtimes_release_v1"})
    return documents,{"items_seen":len(items),"items_accepted":len(documents),"items_skipped":before+after,
        "skip_reasons":(["before_publication_window"]if before else[])+(["after_publication_window"]if after else[]),
        "publication_window_skipped":before+after,"before_window_skipped":before,"after_window_skipped":after,
        "window_coverage_verified":False,"coverage_reason":"RECENT_PUBLISHER_RSS_WITHOUT_HISTORY_PAGINATION"}


def official_rss_article_request(source,document):
    official_rss_request(source)
    if source["article_format"].get("linkedArticleFormat")!="prtimes_release_v1":raise ValidationError("reviewed_linked_article_format_required")
    company=dict(parse_qsl(urlsplit(source["url"]).query))["company_id"]
    url=normalize_url(document.get("official_url"));parsed=urlsplit(url)
    if parsed.scheme!="https"or parsed.hostname!="prtimes.jp"or parsed.query or not re.fullmatch(r"/main/html/rd/p/\d{9}\."+re.escape(company.zfill(9))+r"\.html",parsed.path):raise ValidationError("same_company_prtimes_article_required")
    return {**source,"url":url,"official_host":"prtimes.jp","enabled":True,"method":"public_page"}


def attach_official_rss_article(raw,source,document):
    request=official_rss_article_request(source,document)
    prior=document.get("raw_json")or{}
    if (prior.get("title")!=document.get("original_title")or prior.get("officialUrl")!=document.get("official_url")
        or hashlib.sha256(json.dumps(prior,ensure_ascii=False,sort_keys=True).encode()).hexdigest()!=document.get("raw_sha256")):
        raise ValidationError("publisher_rss_document_hash_mismatch")
    if not isinstance(raw,(bytes,str))or len(raw)>2097152:raise ValidationError("publisher_article_body_limit")
    parser=_PublisherBoardHTML(source,request["url"])
    # HTML parsing ignores NULL in publisher text; remove it before comparing
    # the DOM title to the separately authenticated/date-bearing RSS title.
    try:parser.feed((raw.decode("utf-8-sig")if isinstance(raw,bytes)else raw).replace("\x00",""))
    except (UnicodeError,ValueError)as error:raise ValidationError("publisher_article_html_invalid")from error
    title=re.sub(r"\s+"," ","".join(parser.values["title"])).strip();body="".join(parser.values["body"]).strip()
    if title!=document["original_title"]or not 1<=len(body)<=200000:raise ValidationError("publisher_rss_article_title_body_mismatch")
    # The signed/date-bearing publisher feed establishes the posting timestamp;
    # a timezone-free HTML/JSON-LD timestamp never replaces that explicit fact.
    snapshot={"title":title,"body":body,"published":document["raw_publication_timestamp"],"officialUrl":request["url"],"imageUrls":parser.images[:20]}
    result={**document,"original_description":body,"raw_json":snapshot,"raw_sha256":hashlib.sha256(json.dumps(snapshot,ensure_ascii=False,sort_keys=True).encode()).hexdigest(),
        "source_image_urls":parser.images[:20],"official_article_image_urls":parser.images[:20]}
    dates=re.findall(r"開催日[：:]\s*(20\d{2})年(\d{1,2})月(\d{1,2})日",body)
    if len(set(dates))==1:
        year,month,day=map(int,dates[0]);local=datetime(year,month,day).date().isoformat()
        result["announced_local_date"]={"localDate":local,"precision":"day","evidenceText":"開催日："+"年".join([str(year),str(month)])+"月"+str(day)+"日","sourceUrl":request["url"]}
    return result


def paired_ticket_notice_request(source,document):
    """Exactly reviewed October parent + official ticket auxiliary notice.

    The auxiliary notice may be older: it never becomes a new October post or
    supplies the parent publication date. Changed parent facts fail closed.
    """
    policy=source.get("paired_ticket_notice")or{}
    fields={"mode","version","reviewedAt","evidenceUrl","parentArticleUrl","parentSourceSha256","parentTitle","noticeUrl","allowedImageHosts"}
    if (not isinstance(policy,dict)or set(policy)-fields or policy.get("mode")!="exact_ticketlink_notice_v1"or policy.get("version")!=1
        or source.get("game_id")!="blue-archive"or source.get("parser")!="official_html"
        or policy.get("allowedImageHosts")!=["image.toast.com"]or not re.fullmatch(r"[a-f0-9]{64}",str(policy.get("parentSourceSha256","")))):
        raise ValidationError("exact_paired_ticket_policy_required")
    aware_timestamp(policy.get("reviewedAt"));normalize_url(policy.get("evidenceUrl"))
    parent="https://www.nexongames.co.kr/bbs/board.php?bo_table=media_event&wr_id=518";notice="https://www.ticketlink.co.kr/help/notice/66055"
    if normalize_url(policy.get("parentArticleUrl"))!=normalize_url(parent)or policy.get("noticeUrl")!=notice:raise ValidationError("exact_paired_ticket_routes_required")
    if not isinstance(policy.get("parentTitle"),str)or not 1<=len(policy["parentTitle"])<=500:raise ValidationError("exact_paired_parent_title_required")
    if (document.get("official_url")!=normalize_url(parent)or document.get("raw_sha256")!=policy["parentSourceSha256"]or document.get("original_title")!=policy["parentTitle"]
        or document.get("external_key")!="518"or document.get("_section_key")):
        raise ValidationError("paired_ticket_parent_changed")
    compact=re.sub(r"\s+","",document.get("original_description",""))
    required=["블루아카이브","5주년페스티벌","10월13일","10월15일","10월14일오후8시","10월16일오후8시","11월7일","8일"]
    if any(value not in compact for value in required):raise ValidationError("paired_ticket_parent_dates_not_corroborated")
    return {**source,"url":notice,"official_host":"www.ticketlink.co.kr","parser":"official_auxiliary_notice","enabled":True,"method":"public_page"}


def parse_paired_ticket_notice(raw,source,document):
    request=paired_ticket_notice_request(source,document)
    if not isinstance(raw,(bytes,str))or len(raw)>2097152:raise ValidationError("publisher_article_body_limit")
    text=raw.decode("utf-8-sig")if isinstance(raw,bytes)else raw
    adapter={**source,"article_format":{"linkedArticleFormat":"ticketlink_notice_v1","allowedImageHosts":["image.toast.com"]}}
    parser=_PublisherBoardHTML(adapter,request["url"]);parser.feed(text)
    title=re.sub(r"\s+"," ","".join(parser.values["title"])).strip();body="".join(parser.values["body"]).strip()
    if ("블루 아카이브 5주년 페스티벌"not in title or "티켓오픈 안내"not in title or len(body)>200000
        or parser.images!=["https://image.toast.com/aaaaab/ticketlink/TKL_2/5th_open1012.png"]):raise ValidationError("exact_paired_festival_title_image_required")
    stages=[]
    for round_,kind,day,hour,label in [(1,"membership",13,12,"멤버십 인증"),(1,"ticket",14,20,"티켓 오픈"),(2,"membership",15,12,"멤버십 인증"),(2,"ticket",16,20,"티켓 오픈")]:
        pattern=str(round_)+r"차\s*"+label+r"[^\n:：]*[:：]\s*2026년\s*10월\s*"+str(day)+r"일[^\n\d]*"+str(hour)+r"시"
        matches=re.findall(pattern,body)
        if len(matches)!=1:raise ValidationError("exact_paired_ticket_stage_unconfirmed")
        stages.append({"round":round_,"kind":kind,"startsAt":datetime(2026,10,day,hour,tzinfo=ZoneInfo("Asia/Seoul")).astimezone(UTC),"evidenceText":matches[0]})
    compact=re.sub(r"\s+","",body)
    if any(value not in compact for value in ["공연기간:2026년11월7일(토)~11월8일(일)","공연시간:10시~18시","공연장소:킨텍스제2전시장9홀"]):raise ValidationError("exact_paired_festival_period_venue_required")
    registered=re.search(r"function\s+setNoticeRegisterDate\s*\([^)]*\)\s*\{\s*var\s+miliSeconds\s*=\s*(\d{13})",text)
    if not registered:raise ValidationError("ticket_auxiliary_publication_required")
    auxiliary_published=datetime.fromtimestamp(int(registered[1])/1000,UTC)
    if auxiliary_published>=aware_timestamp(document["published_at"]):raise ValidationError("expected_older_auxiliary_notice_required")
    snapshot={"title":title,"body":body,"imageUrls":parser.images,"publishedAt":auxiliary_published.isoformat(),"officialUrl":request["url"]}
    return {"title":title,"body":body,"imageUrls":parser.images,"publishedAt":auxiliary_published,
        "snapshotSha256":hashlib.sha256(json.dumps(snapshot,ensure_ascii=False,sort_keys=True).encode()).hexdigest(),"stages":stages,
        "startsAt":datetime(2026,11,7,10,tzinfo=ZoneInfo("Asia/Seoul")).astimezone(UTC),"endsAt":datetime(2026,11,8,18,tzinfo=ZoneInfo("Asia/Seoul")).astimezone(UTC),
        "timezone":"Asia/Seoul","dailyOpeningHours":{"start":"10:00","end":"18:00"},"venue":"킨텍스 제2전시장 9홀","noticeUrl":request["url"]}


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
