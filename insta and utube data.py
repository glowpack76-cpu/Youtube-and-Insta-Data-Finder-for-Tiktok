import streamlit as st
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from io import BytesIO

YOUTUBE_API_KEY = st.secrets["AIzaSyBzDL7Q8bOCidXPtNHDJN8q3CRV6a-W7Uk"]

YOUTUBE_SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
YOUTUBE_VIDEO_URL = "https://www.googleapis.com/youtube/v3/videos"

PREFERRED_LANGUAGES = ["hi", "ur", "en", ""]

def parse_iso_datetime(value: str):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))

def parse_duration(duration_iso):
    import re
    if not duration_iso:
        return 0
    pattern = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
    match = pattern.match(duration_iso)
    if not match:
        return 0
    h = int(match.group(1) or 0)
    m = int(match.group(2) or 0)
    s = int(match.group(3) or 0)
    return h * 3600 + m * 60 + s

def make_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="results")
    output.seek(0)
    return output.getvalue()

st.title("YouTube Shorts Strict Finder")

keyword = st.text_input("Keyword / niche", placeholder="e.g. ai tools")
num_videos = st.number_input("Number of videos", min_value=1, max_value=50, value=5, step=1)
time_range_minutes = st.slider("Video public time (minutes)", 0, 60, (0, 60), step=1)
views_range = st.slider("Views range", 0, 300, (0, 300), step=1)

if st.button("Search Shorts"):
    if not keyword.strip():
        st.error("Keyword required")
        st.stop()

    min_minute, max_minute = time_range_minutes
    min_age_sec = min_minute * 60
    max_age_sec = max_minute * 60
    min_views, max_views = views_range

    now_utc = datetime.now(timezone.utc)
    published_after = (now_utc - timedelta(seconds=max_age_sec)).isoformat().replace("+00:00", "Z")
    published_before = (now_utc - timedelta(seconds=min_age_sec)).isoformat().replace("+00:00", "Z")

    all_results = []
    seen = set()

    for lang_code in PREFERRED_LANGUAGES:
        if len(all_results) >= num_videos:
            break

        params = {
            "part": "snippet",
            "q": keyword,
            "type": "video",
            "order": "date",
            "publishedAfter": published_after,
            "publishedBefore": published_before,
            "videoDuration": "short",
            "maxResults": 50,
            "key": YOUTUBE_API_KEY,
        }

        if lang_code:
            params["relevanceLanguage"] = lang_code

        r = requests.get(YOUTUBE_SEARCH_URL, params=params, timeout=30)
        data = r.json()

        if "error" in data:
            st.error(data["error"].get("message", "YouTube API Error"))
            st.stop()

        ids = []
        for item in data.get("items", []):
            video_id = item.get("id", {}).get("videoId")
            if video_id and video_id not in seen:
                ids.append(video_id)

        if not ids:
            continue

        vr = requests.get(YOUTUBE_VIDEO_URL, params={
            "part": "snippet,statistics,contentDetails",
            "id": ",".join(ids),
            "key": YOUTUBE_API_KEY
        }, timeout=30)

        vdata = vr.json()

        if "error" in vdata:
            st.error(vdata["error"].get("message", "YouTube Videos API Error"))
            st.stop()

        for item in vdata.get("items", []):
            video_id = item["id"]
            if video_id in seen:
                continue

            snippet = item.get("snippet", {})
            stats = item.get("statistics", {})
            content = item.get("contentDetails", {})

            published_at = parse_iso_datetime(snippet["publishedAt"])
            age_seconds = int((now_utc - published_at).total_seconds())

            views = int(stats.get("viewCount", 0))
            likes = int(stats.get("likeCount", 0))
            comments = int(stats.get("commentCount", 0))
            duration_seconds = parse_duration(content.get("duration", ""))

            # STRICT FILTERS
            if not (min_age_sec <= age_seconds <= max_age_sec):
                continue

            if not (min_views <= views <= max_views):
                continue

            if duration_seconds > 60:
                continue

            all_results.append({
                "title": snippet.get("title", ""),
                "url": f"https://www.youtube.com/shorts/{video_id}",
                "published_at_utc": snippet.get("publishedAt", ""),
                "age_seconds": age_seconds,
                "views": views,
                "likes": likes,
                "comments": comments,
                "duration_seconds": duration_seconds,
                "channel_name": snippet.get("channelTitle", ""),
                "thumbnail": snippet.get("thumbnails", {}).get("high", {}).get("url", "")
            })

            seen.add(video_id)

            if len(all_results) >= num_videos:
                break

    if not all_results:
        st.warning("No shorts found with exact filters.")
    else:
        df = pd.DataFrame(all_results)
        st.dataframe(df, use_container_width=True)

        excel_data = make_excel(df)
        st.download_button(
            "Download Excel",
            data=excel_data,
            file_name="youtube_shorts_results.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
