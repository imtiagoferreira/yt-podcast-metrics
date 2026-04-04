"""
============================================================
YouTube Metrics Fetcher v3
============================================================
Fetches all podcast metrics, tracks growth from baseline,
and writes metrics.json. Runs via GitHub Actions daily.

Metrics (12 total):
  - Subscribers + growth
  - Average views + growth
  - Average watch time + growth
  - Average retention rate + growth
  - Average CTR + growth
  - Average comments + growth

All averages are for long-form videos (> 3 min) since Jan 1, 2026.
Growth is calculated against a stored baseline snapshot.
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

# Shorts filter: exclude videos <= this many seconds
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

def fetch_analytics_metrics(creds, video_ids):
    """Fetch avg watch time, retention rate, and CTR from YouTube Analytics API."""
    yta = build('youtubeAnalytics', 'v2', credentials=creds)

    # End date = 3 days ago (data latency buffer)
    end_date = (datetime.utcnow() - timedelta(days=3)).strftime('%Y-%m-%d')

    if not video_ids:
        return {
            'avgWatchTimeSec': 0,
            'avgRetentionPercent': 0,
            'avgCTRPercent': 0,
        }

    # --- CALL 1: Watch time + retention per video ---
    durations = []
    retentions = []

    for vid_id in video_ids:
        try:
            resp = yta.reports().query(
                ids='channel==MINE',
                startDate=START_DATE,
                endDate=end_date,
                metrics='views,averageViewDuration,averageViewPercentage',
                filters='video==' + vid_id,
            ).execute()
            rows = resp.get('rows', [])
            if rows:
                durations.append(rows[0][1])
                retentions.append(rows[0][2])
        except Exception as e:
            print(f'   Warning: Could not fetch watch time for {vid_id}: {e}')

    # Calculate averages
    avg_duration = sum(durations) / len(durations) if durations else 0
    avg_retention = sum(retentions) / len(retentions) if retentions else 0

    # --- CALL 2: CTR at channel level with day dimension ---
    # The videoThumbnailImpressions metrics require a dimension.
    # Query by day, then calculate weighted average CTR.
    avg_ctr = 0
    try:
        resp = yta.reports().query(
            ids='channel==MINE',
            startDate=START_DATE,
            endDate=end_date,
            metrics='videoThumbnailImpressions,videoThumbnailImpressionsClickRate',
            dimensions='day',
            sort='day',
        ).execute()
        rows = resp.get('rows', [])
        if rows:
            # Column order per row: [day, impressions, CTR]
            total_impressions = sum(r[1] for r in rows)
            total_clicks = sum(r[1] * r[2] for r in rows)
            if total_impressions > 0:
                raw_ctr = total_clicks / total_impressions
                avg_ctr = round(raw_ctr * 100, 2) if raw_ctr < 1 else round(raw_ctr, 2)
            print(f'   CTR calculated from {len(rows)} days of data')
            print(f'   Total impressions: {total_impressions}')
            print(f'   Weighted avg CTR: {avg_ctr}%')
        else:
            print('   No CTR data returned for date range')
    except Exception as e:
        print(f'   Warning: Could not fetch CTR: {e}')

    return {
        'avgWatchTimeSec': round(avg_duration, 1),
        'avgRetentionPercent': round(avg_retention, 1),
        'avgCTRPercent': avg_ctr,
    }


# ============================================================
#  BASELINE & GROWTH
# ============================================================

def load_baseline():
    """Load baseline values from environment variables (GitHub Secrets)."""
    baseline = {
        'subscribers': SUBSCRIBERS_ON_JAN1,
        'avgViews': float(os.environ.get('BASELINE_AVG_VIEWS', '0')),
        'avgComments': float(os.environ.get('BASELINE_AVG_COMMENTS', '0')),
        'avgWatchTimeSec': float(os.environ.get('BASELINE_AVG_WATCH_TIME', '0')),
        'avgRetentionPercent': float(os.environ.get('BASELINE_AVG_RETENTION', '0')),
        'avgCTRPercent': float(os.environ.get('BASELINE_AVG_CTR', '0')),
    }
    return baseline


def calc_growth(current, baseline):
    """Calculate percentage growth: ((current - baseline) / baseline) * 100."""
    if baseline is None or baseline == 0:
        return 0
    return round(((current - baseline) / baseline) * 100, 1)


# ============================================================
#  MAIN
# ============================================================

def main():
    print("Authenticating...")
    creds = get_credentials()

    print("Fetching YouTube Data API metrics...")
    data_metrics = fetch_data_api_metrics(creds)
    print(f"   Found {data_metrics['videoCount']} long-form videos since {START_DATE}")
    print(f"   Subscribers: {data_metrics['subscribers']}")
    print(f"   Avg views: {data_metrics['avgViews']}")
    print(f"   Avg comments: {data_metrics['avgComments']}")

    print("Fetching YouTube Analytics API metrics...")
    analytics_metrics = fetch_analytics_metrics(creds, data_metrics['videoIds'])
    print(f"   Avg watch time: {analytics_metrics['avgWatchTimeSec']}s")
    print(f"   Avg retention: {analytics_metrics['avgRetentionPercent']}%")
    print(f"   Avg CTR: {analytics_metrics['avgCTRPercent']}%")

    # Current metrics snapshot
    current = {
        'subscribers': data_metrics['subscribers'],
        'avgViews': data_metrics['avgViews'],
        'avgWatchTimeSec': analytics_metrics['avgWatchTimeSec'],
        'avgRetentionPercent': analytics_metrics['avgRetentionPercent'],
        'avgCTRPercent': analytics_metrics['avgCTRPercent'],
        'avgComments': data_metrics['avgComments'],
    }

    # Load baseline from environment variables (GitHub Secrets)
    baseline = load_baseline()
    print(f"\n   Baseline values (Jan 2026):")
    print(f"   Subscribers: {baseline['subscribers']}")
    print(f"   Avg views: {baseline['avgViews']}")
    print(f"   Avg watch time: {baseline['avgWatchTimeSec']}s")
    print(f"   Avg retention: {baseline['avgRetentionPercent']}%")
    print(f"   Avg CTR: {baseline['avgCTRPercent']}%")
    print(f"   Avg comments: {baseline['avgComments']}")

    # Calculate all growth rates
    subscriber_growth = calc_growth(current['subscribers'], baseline['subscribers'])

    views_growth = calc_growth(current['avgViews'], baseline['avgViews'])
    watch_time_growth = calc_growth(current['avgWatchTimeSec'], baseline['avgWatchTimeSec'])
    retention_growth = calc_growth(current['avgRetentionPercent'], baseline['avgRetentionPercent'])
    ctr_growth = calc_growth(current['avgCTRPercent'], baseline['avgCTRPercent'])
    comments_growth = calc_growth(current['avgComments'], baseline['avgComments'])

    print(f"\nGrowth rates:")
    print(f"   Subscriber growth: {subscriber_growth}%")
    print(f"   Avg views growth: {views_growth}%")
    print(f"   Watch time growth: {watch_time_growth}%")
    print(f"   Retention growth: {retention_growth}%")
    print(f"   CTR growth: {ctr_growth}%")
    print(f"   Comments growth: {comments_growth}%")

    # Combine into output
    output = {
        'subscribers': current['subscribers'],
        'subscriberGrowth': subscriber_growth,
        'avgViews': current['avgViews'],
        'avgViewsGrowth': views_growth,
        'avgWatchTimeSec': current['avgWatchTimeSec'],
        'avgWatchTimeGrowth': watch_time_growth,
        'avgRetentionPercent': current['avgRetentionPercent'],
        'avgRetentionGrowth': retention_growth,
        'avgCTRPercent': current['avgCTRPercent'],
        'avgCTRGrowth': ctr_growth,
        'avgComments': current['avgComments'],
        'avgCommentsGrowth': comments_growth,
        'videoCount': data_metrics['videoCount'],
        'updatedAt': datetime.utcnow().isoformat() + 'Z',
        'startDate': START_DATE,
    }

    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"\nMetrics written to {OUTPUT_FILE}")
    print(json.dumps(output, indent=2))


if __name__ == '__main__':
    main()
