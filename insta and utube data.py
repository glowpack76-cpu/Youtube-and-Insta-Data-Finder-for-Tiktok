import streamlit as st
import requests
import pandas as pd
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from io import BytesIO

# =========================================================
# CONFIG
# =========================================================
YOUTUBE_API_KEY = st.secrets.get("AIzaSyBzDL7Q8bOCidXPtNHDJN8q3CRV6a-W7Uk", "")
APIFY_TOKEN = st.secrets.get("APIFY_TOKEN", "")

# Recommended Apify actor ids from the repo you shared
APIFY_INSTAGRAM_ACTOR_ID = st.secrets.get("APIFY_INSTAGRAM_ACTOR_ID", "apidojo/instagram-scraper")
# Optional if you want scraper-based YouTube too
APIFY_YOUTUBE_ACTOR_ID = st.secrets.get("APIFY_YOUTUBE_ACTOR_ID", "apidojo/youtube-scraper")

YOUTUBE_SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
YOUTUBE_VIDEO_URL = "https://www.googleapis.com/youtube/v3/videos"

REQUEST_TIMEOUT = 30
YOUTUBE_SEARCH_PAGE_SIZE = 50
MAX_YT_FETCH_PAGES = 8  # increase if needed

# =========================================================
# HELPERS
# =========================================================
def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def parse_iso_datetime(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        # Handles ISO 8601 with Z
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None

def parse_duration_iso8601(duration_iso: str) -> int:
    import re
    if not duration_iso:
        return 0
    pattern = re.compile(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?")
    match = pattern.match(duration_iso)
    if not match:
        return 0
    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    return hours * 3600 + minutes * 60 + seconds

def safe_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default

def exact_age_seconds(published_at: datetime, ref_now: datetime) -> int:
    return int((ref_now - published_at).total_seconds())

def within_inclusive(value: int, min_value: int, max_value: int) -> bool:
    return min_value <= value <= max_value

def make_excel_file(df: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="results")
    output.seek(0)
    return output.getvalue()

def request_json(url: str, params: dict) -> dict:
    resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise RuntimeError(data["error"].get("message", "Unknown API error"))
    return data

# =========================================================
# STRICT FILTER ENGINE
# =========================================================
def strict_match_common(
    created_at: datetime,
    views: int,
    min_age_sec: int,
    max_age_sec: int,
    min_views: int,
    max_views: int,
    ref_now: datetime
) -> Optional[int]:
    if not created_at:
        return None

    age_sec = exact_age_seconds(created_at, ref_now)

    # Strict inclusive range:
    # if max_age_sec = 60, then 61 sec MUST be rejected
    if not within_inclusive(age_sec, min_age_sec, max_age_sec):
        return None

    if not within_inclusive(views, min_views, max_views):
        return None

    return age_sec

# =========================================================
# YOUTUBE STRICT SEARCH
# =========================================================
def search_youtube_strict(
    keywords: List[str],
    result_limit: int,
    min_age_sec: int,
    max_age_sec: int,
    min_views: int,
    max_views: int,
    language_code: str = "",
    video_duration_filter: str = "any",   # any / short / medium / long
    sort_order: str = "date"              # date preferred for exact recency
) -> List[Dict[str, Any]]:

    if not YOUTUBE_API_KEY:
        raise RuntimeError("Missing YOUTUBE_API_KEY in Streamlit secrets.")

    now_utc = utc_now()
    published_after = (now_utc - timedelta(seconds=max_age_sec)).isoformat().replace("+00:00", "Z")
    published_before = (now_utc - timedelta(seconds=min_age_sec)).isoformat().replace("+00:00", "Z")

    dedupe = {}
    results = []

    for keyword in keywords:
        next_page_token = None
        pages_fetched = 0

        while pages_fetched < MAX_YT_FETCH_PAGES and len(results) < result_limit:
            pages_fetched += 1

            search_params = {
                "part": "snippet",
                "q": keyword,
                "type": "video",
                "order": sort_order,  # IMPORTANT: date for recency-sensitive discovery
                "publishedAfter": published_after,
                "publishedBefore": published_before,
                "maxResults": YOUTUBE_SEARCH_PAGE_SIZE,
                "key": YOUTUBE_API_KEY,
            }

            if next_page_token:
                search_params["pageToken"] = next_page_token
            if language_code:
                search_params["relevanceLanguage"] = language_code
            if video_duration_filter != "any":
                search_params["videoDuration"] = video_duration_filter

            search_data = request_json(YOUTUBE_SEARCH_URL, search_params)
            items = search_data.get("items", [])
            if not items:
                break

            video_ids = []
            for item in items:
                video_id = item.get("id", {}).get("videoId")
                if video_id and video_id not in dedupe:
                    video_ids.append(video_id)

            if not video_ids:
                next_page_token = search_data.get("nextPageToken")
                if not next_page_token:
                    break
                continue

            video_data = request_json(YOUTUBE_VIDEO_URL, {
                "part": "snippet,statistics,contentDetails",
                "id": ",".join(video_ids),
                "key": YOUTUBE_API_KEY
            })

            for video in video_data.get("items", []):
                video_id = video.get("id")
                if not video_id or video_id in dedupe:
                    continue

                snippet = video.get("snippet", {})
                stats = video.get("statistics", {})
                content = video.get("contentDetails", {})

                published_at = parse_iso_datetime(snippet.get("publishedAt"))
                views = safe_int(stats.get("viewCount", 0), 0)
                likes = safe_int(stats.get("likeCount", 0), 0)
                comments = safe_int(stats.get("commentCount", 0), 0)
                age_sec = strict_match_common(
                    created_at=published_at,
                    views=views,
                    min_age_sec=min_age_sec,
                    max_age_sec=max_age_sec,
                    min_views=min_views,
                    max_views=max_views,
                    ref_now=now_utc
                )

                if age_sec is None:
                    continue

                duration_sec = parse_duration_iso8601(content.get("duration", ""))

                record = {
                    "platform": "youtube",
                    "keyword": keyword,
                    "id": video_id,
                    "url": f"https://www.youtube.com/watch?v={video_id}",
                    "title": snippet.get("title", ""),
                    "description": snippet.get("description", ""),
                    "published_at_utc": snippet.get("publishedAt", ""),
                    "age_seconds": age_sec,
                    "views": views,
                    "likes": likes,
                    "comments": comments,
                    "duration_seconds": duration_sec,
                    "channel_id": snippet.get("channelId", ""),
                    "channel_name": snippet.get("channelTitle", ""),
                    "thumbnail": (
                        snippet.get("thumbnails", {}).get("high", {}).get("url")
                        or snippet.get("thumbnails", {}).get("default", {}).get("url", "")
                    ),
                    "raw_metadata": video
                }

                dedupe[video_id] = True
                results.append(record)

                if len(results) >= result_limit:
                    break

            next_page_token = search_data.get("nextPageToken")
            if not next_page_token:
                break

    # Final strict sort
    results.sort(key=lambda x: (x["age_seconds"], -x["views"]))
    return results[:result_limit]

# =========================================================
# APIFY HELPERS
# =========================================================
def run_apify_actor(actor_id: str, input_payload: dict) -> List[dict]:
    if not APIFY_TOKEN:
        raise RuntimeError("Missing APIFY_TOKEN in Streamlit secrets.")

    run_url = f"https://api.apify.com/v2/acts/{actor_id}/runs?token={APIFY_TOKEN}"
    run_resp = requests.post(run_url, json=input_payload, timeout=REQUEST_TIMEOUT)
    run_resp.raise_for_status()
    run_data = run_resp.json()
    run_id = run_data["data"]["id"]

    # Poll run
    for _ in range(120):
        status_url = f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_TOKEN}"
        status_resp = requests.get(status_url, timeout=REQUEST_TIMEOUT)
        status_resp.raise_for_status()
        status_data = status_resp.json()["data"]
        status = status_data["status"]

        if status == "SUCCEEDED":
            dataset_id = status_data["defaultDatasetId"]
            items_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?clean=true&format=json"
            items_resp = requests.get(items_url, timeout=REQUEST_TIMEOUT)
            items_resp.raise_for_status()
            return items_resp.json()

        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            raise RuntimeError(f"Apify actor failed with status: {status}")

        time.sleep(2)

    raise TimeoutError("Apify actor polling timed out.")

# =========================================================
# INSTAGRAM STRICT SEARCH (APIFY ADAPTER)
# NOTE:
# Actor input schema may evolve. Keep this function isolated.
# =========================================================
def search_instagram_strict(
    keywords: List[str],
    result_limit: int,
    min_age_sec: int,
    max_age_sec: int,
    min_views: int,
    max_views: int
) -> List[Dict[str, Any]]:
    """
    Uses Apify actor listed in the repo:
    apidojo/instagram-scraper

    IMPORTANT:
    For exact filtering, we fetch broader results, then filter locally.
    Depending on actor input schema, you may need to adapt payload.
    """
    now_utc = utc_now()
    broader_until = (now_utc - timedelta(seconds=max_age_sec)).isoformat().replace("+00:00", "Z")

    final_results = []
    dedupe = set()

    for keyword in keywords:
        # ---- ADAPT THIS PAYLOAD IF YOUR SELECTED ACTOR INPUT CHANGES ----
        input_payload = {
            # The actor docs mention search URLs / keywords support.
            # If your actor version prefers startUrls, map keyword to supported search URLs here.
            "search": keyword,
            "until": broader_until,
            "maxItems": max(result_limit * 10, 50)
        }

        items = run_apify_actor(APIFY_INSTAGRAM_ACTOR_ID, input_payload)

        for item in items:
            # Flexible mapping because actor outputs can vary by version
            item_id = str(item.get("id", ""))
            if not item_id or item_id in dedupe:
                continue

            created_raw = item.get("createdAt") or item.get("timestamp")
            created_at = parse_iso_datetime(created_raw)
            if not created_at:
                continue

            # For videos/reels, playCount/viewCount may differ by actor
            views = safe_int(
                item.get("viewCount",
                item.get("playCount",
                item.get("video", {}).get("playCount", 0))), 0
            )

            age_sec = strict_match_common(
                created_at=created_at,
                views=views,
                min_age_sec=min_age_sec,
                max_age_sec=max_age_sec,
                min_views=min_views,
                max_views=max_views,
                ref_now=now_utc
            )
            if age_sec is None:
                continue

            is_video = bool(item.get("isVideo")) or item.get("media_type") == "VIDEO"
            if not is_video:
                continue

            video_obj = item.get("video", {}) if isinstance(item.get("video"), dict) else {}

            record = {
                "platform": "instagram",
                "keyword": keyword,
                "id": item_id,
                "url": item.get("url") or item.get("permalink", ""),
                "title": item.get("caption", "")[:100],
                "description": item.get("caption", ""),
                "published_at_utc": created_raw,
                "age_seconds": age_sec,
                "views": views,
                "likes": safe_int(item.get("likeCount", item.get("like_count", 0)), 0),
                "comments": safe_int(item.get("commentCount", item.get("comments_count", 0)), 0),
                "duration_seconds": safe_int(video_obj.get("duration", 0), 0),
                "channel_id": str(item.get("owner", {}).get("id", "")),
                "channel_name": item.get("owner", {}).get("username", ""),
                "thumbnail": (
                    item.get("displayUrl")
                    or item.get("image", {}).get("url", "")
                    or video_obj.get("thumbnailUrl", "")
                ),
                "raw_metadata": item
            }

            dedupe.add(item_id)
            final_results.append(record)

            if len(final_results) >= result_limit:
                break

        if len(final_results) >= result_limit:
            break

    final_results.sort(key=lambda x: (x["age_seconds"], -x["views"]))
    return final_results[:result_limit]

# =========================================================
# UI
# =========================================================
st.set_page_config(page_title="Strict Social Video Finder", layout="wide")
st.title("🎯 Strict Social Video Finder")
st.caption("YouTube + Instagram results with strict local filtering by seconds and views.")

with st.sidebar:
    st.header("Settings")

    platforms = st.multiselect(
        "Platforms",
        options=["YouTube", "Instagram"],
        default=["YouTube", "Instagram"]
    )

    niche = st.text_input("Niche", placeholder="e.g. AI tools")
    keyword_text = st.text_area(
        "Keywords (comma separated)",
        placeholder="e.g. chatgpt, ai automation, ai agents"
    )

    result_limit = st.number_input("Number of videos", min_value=1, max_value=500, value=25)

    st.subheader("Exact Time Filter")
    age_range = st.slider(
        "Posted age range in seconds",
        min_value=0,
        max_value=86400,    # 24h
        value=(0, 3600),
        step=1
    )
    min_age_sec, max_age_sec = age_range

    st.subheader("Exact Views Filter")
    views_range = st.slider(
        "Views range",
        min_value=0,
        max_value=10_000_000,
        value=(0, 300),
        step=1
    )
    min_views, max_views = views_range

    yt_language = st.text_input("YouTube relevance language code", value="")
    yt_duration = st.selectbox("YouTube duration filter", ["any", "short", "medium", "long"], index=0)

search_clicked = st.button("🚀 Search Strictly", type="primary")

# Build keywords
keywords = []
if niche.strip():
    keywords.append(niche.strip())
if keyword_text.strip():
    keywords.extend([k.strip() for k in keyword_text.split(",") if k.strip()])

# Dedupe keywords preserving order
seen_kw = set()
keywords = [k for k in keywords if not (k.lower() in seen_kw or seen_kw.add(k.lower()))]

if search_clicked:
    if not keywords:
        st.error("Please enter a niche or at least one keyword.")
        st.stop()

    try:
        all_results = []

        progress = st.progress(0)
        status = st.empty()

        if "YouTube" in platforms:
            status.info("Fetching YouTube results...")
            yt_results = search_youtube_strict(
                keywords=keywords,
                result_limit=result_limit,
                min_age_sec=min_age_sec,
                max_age_sec=max_age_sec,
                min_views=min_views,
                max_views=max_views,
                language_code=yt_language.strip(),
                video_duration_filter=yt_duration,
                sort_order="date"
            )
            all_results.extend(yt_results)

        progress.progress(50)

        if "Instagram" in platforms:
            status.info("Fetching Instagram results...")
            ig_results = search_instagram_strict(
                keywords=keywords,
                result_limit=result_limit,
                min_age_sec=min_age_sec,
                max_age_sec=max_age_sec,
                min_views=min_views,
                max_views=max_views
            )
            all_results.extend(ig_results)

        progress.progress(100)
        status.empty()
        progress.empty()

        # Final strict safety pass
        # (Even after platform functions, we validate again centrally)
        now_ref = utc_now()
        verified = []
        seen_ids = set()

        for r in all_results:
            uid = f'{r["platform"]}:{r["id"]}'
            if uid in seen_ids:
                continue

            created_at = parse_iso_datetime(r["published_at_utc"])
            if not created_at:
                continue

            age_sec = exact_age_seconds(created_at, now_ref)

            if not within_inclusive(age_sec, min_age_sec, max_age_sec):
                continue

            if not within_inclusive(safe_int(r["views"], 0), min_views, max_views):
                continue

            r["age_seconds"] = age_sec
            seen_ids.add(uid)
            verified.append(r)

        verified.sort(key=lambda x: (x["platform"], x["age_seconds"], -x["views"]))

        st.success(f"Strictly matched results: {len(verified)}")

        if not verified:
            st.warning("No results matched your exact filter window.")
            st.stop()

        df = pd.DataFrame([{
            "platform": r["platform"],
            "keyword": r["keyword"],
            "title": r["title"],
            "url": r["url"],
            "published_at_utc": r["published_at_utc"],
            "age_seconds": r["age_seconds"],
            "views": r["views"],
            "likes": r["likes"],
            "comments": r["comments"],
            "duration_seconds": r["duration_seconds"],
            "channel_name": r["channel_name"],
            "channel_id": r["channel_id"],
            "thumbnail": r["thumbnail"],
            "raw_metadata": str(r["raw_metadata"])
        } for r in verified])

        st.dataframe(df, use_container_width=True, height=500)

        for item in verified:
            with st.container():
                c1, c2 = st.columns([1, 3])
                with c1:
                    if item["thumbnail"]:
                        st.image(item["thumbnail"], use_container_width=True)
                with c2:
                    st.markdown(f"### [{item['title'] or 'Untitled'}]({item['url']})")
                    st.write(
                        f"**Platform:** {item['platform']}  |  "
                        f"**Keyword:** {item['keyword']}  |  "
                        f"**Channel/User:** {item['channel_name']}"
                    )
                    st.write(
                        f"**Posted (UTC):** {item['published_at_utc']}  |  "
                        f"**Age (sec):** {item['age_seconds']}  |  "
                        f"**Views:** {item['views']}  |  "
                        f"**Likes:** {item['likes']}  |  "
                        f"**Comments:** {item['comments']}"
                    )
                    if item["duration_seconds"]:
                        st.write(f"**Duration (sec):** {item['duration_seconds']}")
                st.divider()

        excel_bytes = make_excel_file(df)
        st.download_button(
            label="📥 Download all results as Excel",
            data=excel_bytes,
            file_name="strict_social_video_results.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        st.error(f"Error: {e}")
