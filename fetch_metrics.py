"""
============================================================
YouTube Metrics Fetcher
============================================================
Fetches all 7 podcast metrics and writes metrics.json.
Runs automatically via GitHub Actions on a daily schedule.

Metrics fetched:
  - Subscribers (current total) ........... YouTube Data API v3
  - Subscriber growth since Jan 1 ......... YouTube Data API v3
  - Average views per long-form video ..... YouTube Data API v3
  - Average comments per long-form video .. YouTube Data API v3
  - Average view duration ................. YouTube Analytics API
  - Average retention rate ................ YouTube Analytics API
  - Average CTR ........................... YouTube Analytics API

All "since Jan 2026" metrics exclude Shorts (videos ≤ 3 min).
============================================================
"""

import json
import os
from datetime import datetime, timedelta

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build


# ============================================================
#  CONFIG
# ============================================================

CHANNEL_HANDLE = 'imtiagoferreira'
START_DATE = '2026-01-01'
SUBSCRIBERS_ON_JAN1 = int(os.environ.get('SUBSCRIBERS_ON_JAN1', '0'))

# Shorts filter: exclude videos ≤ this many seconds
MAX_SHORT_DURATION_SEC = 180  # 3 minutes

# Output file
OUTPUT_FILE = 'metrics.json'


# ============================================================
#  AUTHENTICATION
# ============================================================

def get_credentials():
    """Build credentials from environment variables (GitHub Secrets)."""
    creds = Credentials(
        token=None,
        refresh_token=os.environ['YOUTUBE_REFRESH_TOKEN'],
        token_uri='https://oauth2.googleapis.com/token',
        client_id=os.environ['YOUTUBE_CLIENT_ID'],
        client_secret=os.environ['YOUTUBE_CLIENT_SECRET'],
    )
    creds.refresh(Request())
    return creds


# ============================================================
#  YOUTUBE DATA API v3 — Public metrics
# ============================================================

def parse_duration(iso_duration):
    """Parse ISO 8601 duration (e.g., PT12M34S) to seconds."""
    import re
    match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', iso_duration)
    if not match:
        return 0
    h = int(match.group(1) or 0)
    m = int(match.group(2) or 0)
    s = int(match.group(3) or 0)
    return h * 3600 + m * 60 + s


def fetch_data_api_metrics(creds):
    """Fetch subscribers, avg views, and avg comments from YouTube Data API v3."""
    yt = build('youtube', 'v3', credentials=creds)
    start_dt = datetime.fromisoformat(START_DATE)

    # 1. Get channel info
    ch_resp = yt.channels().list(
        forHandle=CHANNEL_HANDLE,
        part='statistics,contentDetails'
    ).execute()

    channel = ch_resp['items'][0]
    subscribers = int(channel['statistics']['subscriberCount'])
    uploads_playlist = channel['contentDetails']['relatedPlaylists']['uploads']

    # 2. Get all video IDs from uploads playlist
    all_video_ids = []
    all_publish_dates = {}
    next_page = None

    while True:
        pl_resp = yt.playlistItems().list(
            playlistId=uploads_playlist,
            part='snippet,contentDetails',
            maxResults=50,
            pageToken=next_page
        ).execute()

        for item in pl_resp['items']:
            vid_id = item['contentDetails']['videoId']
            pub_date = item['contentDetails'].get('videoPublishedAt',
                         item['snippet']['publishedAt'])
            all_video_ids.append(vid_id)
            all_publish_dates[vid_id] = pub_date

        next_page = pl_resp.get('nextPageToken')
        if not next_page:
            break

    # 3. Get video details and filter long-form videos since start date
    long_form_videos = []

    for i in range(0, len(all_video_ids), 50):
        batch = all_video_ids[i:i + 50]
        vid_resp = yt.videos().list(
            id=','.join(batch),
            part='statistics,contentDetails'
        ).execute()

        for video in vid_resp['items']:
            duration_sec = parse_duration(video['contentDetails']['duration'])
            pub_date = datetime.fromisoformat(
                all_publish_dates[video['id']].replace('Z', '+00:00')
            ).replace(tzinfo=None)

            # Filter: long-form only (> 3 min) AND published after start date
            if duration_sec > MAX_SHORT_DURATION_SEC and pub_date >= start_dt:
                long_form_videos.append({
                    'id': video['id'],
                    'views': int(video['statistics'].get('viewCount', 0)),
                    'comments': int(video['statistics'].get('commentCount', 0)),
                })

    # 4. Calculate averages
    count = len(long_form_videos)
    total_views = sum(v['views'] for v in long_form_videos)
    total_comments = sum(v['comments'] for v in long_form_videos)

    return {
        'subscribers': subscribers,
        'avgViews': round(total_views / count, 1) if count > 0 else 0,
        'avgComments': round(total_comments / count, 1) if count > 0 else 0,
        'videoCount': count,
        'videoIds': [v['id'] for v in long_form_videos],
    }


# ============================================================
#  YOUTUBE ANALYTICS API — Private metrics
# ============================================================

def fetch_analytics_metrics(creds):
    """Fetch avg watch time, retention rate, and CTR from YouTube Analytics API."""
    yta = build('youtubeAnalytics', 'v2', credentials=creds)

    # End date = 3 days ago (data latency buffer)
    end_date = (datetime.utcnow() - timedelta(days=3)).strftime('%Y-%m-%d')

    response = yta.reports().query(
        ids='channel==MINE',
        startDate=START_DATE,
        endDate=end_date,
        metrics='views,averageViewDuration,averageViewPercentage,videoThumbnailImpressions,videoThumbnailImpressionsClickRate',
        filters='creatorContentType==VIDEO_ON_DEMAND',
    ).execute()

    # The response returns a single aggregated row when no dimensions are specified
    rows = response.get('rows', [])
    if not rows:
        return {
            'avgWatchTimeSec': 0,
            'avgRetentionPercent': 0,
            'avgCTRPercent': 0,
        }

    # Column order matches the metrics parameter order
    # [views, averageViewDuration, averageViewPercentage, impressions, CTR]
    row = rows[0]

    return {
        'avgWatchTimeSec': round(row[1], 1),            # seconds
        'avgRetentionPercent': round(row[2], 1),         # percentage
        'avgCTRPercent': round(row[4] * 100, 2) if row[4] < 1 else round(row[4], 2),
    }


# ============================================================
#  MAIN
# ============================================================

def main():
    print("🔐 Authenticating...")
    creds = get_credentials()

    print("📊 Fetching YouTube Data API metrics...")
    data_metrics = fetch_data_api_metrics(creds)
    print(f"   Found {data_metrics['videoCount']} long-form videos since {START_DATE}")
    print(f"   Subscribers: {data_metrics['subscribers']}")
    print(f"   Avg views: {data_metrics['avgViews']}")
    print(f"   Avg comments: {data_metrics['avgComments']}")

    print("📈 Fetching YouTube Analytics API metrics...")
    analytics_metrics = fetch_analytics_metrics(creds)
    print(f"   Avg watch time: {analytics_metrics['avgWatchTimeSec']}s")
    print(f"   Avg retention: {analytics_metrics['avgRetentionPercent']}%")
    print(f"   Avg CTR: {analytics_metrics['avgCTRPercent']}%")

    # Calculate subscriber growth
    sub_growth = 0
    if SUBSCRIBERS_ON_JAN1 > 0:
        sub_growth = round(
            ((data_metrics['subscribers'] - SUBSCRIBERS_ON_JAN1) / SUBSCRIBERS_ON_JAN1) * 100,
            1
        )
    print(f"   Subscriber growth: {sub_growth}%")

    # Combine into output
    output = {
        'subscribers': data_metrics['subscribers'],
        'subscriberGrowth': sub_growth,
        'avgViews': data_metrics['avgViews'],
        'avgWatchTimeSec': analytics_metrics['avgWatchTimeSec'],
        'avgRetentionPercent': analytics_metrics['avgRetentionPercent'],
        'avgCTRPercent': analytics_metrics['avgCTRPercent'],
        'avgComments': data_metrics['avgComments'],
        'videoCount': data_metrics['videoCount'],
        'updatedAt': datetime.utcnow().isoformat() + 'Z',
        'startDate': START_DATE,
    }

    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\n✅ Metrics written to {OUTPUT_FILE}")
    print(json.dumps(output, indent=2))


if __name__ == '__main__':
    main()
