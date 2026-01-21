"""
scrape youtube comments from ALL videos in channels using youtube api.
includes checkpointing and exact dates.

usage:
    python scrape_all_comments.py --key-file key.txt
    python scrape_all_comments.py --key-file key.txt --resume
    python scrape_all_comments.py --key-file key.txt --skip-dates --resume
"""

import os
import json
import argparse
import pandas as pd
from datetime import datetime, timedelta
import re
from youtube_comment_downloader import YoutubeCommentDownloader
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# channel urls
channels = {
    # major hagwons
    '대성마이맥': 'https://www.youtube.com/@mimac_study/videos',
    '메가스터디': 'https://www.youtube.com/@theMEGASTUDY/videos', 
    '이투스': 'https://www.youtube.com/@ETOOS_edu/videos',
    '파고다': 'https://www.youtube.com/@pagodastar/videos',  
    '해커스어학원': 'https://www.youtube.com/@HackersEnglish/videos',  
    '엠베스트': 'https://www.youtube.com/@megastudymbest/videos', 
    '이근갑국어': 'https://www.youtube.com/@이근갑국어/videos',  
    '빡공시대': 'https://www.youtube.com/@ppakong-com/videos',  
    '강남인강': 'https://www.youtube.com/@gangnamingang/videos',  
    # star teachers
    '정승제': 'https://www.youtube.com/@seungje_tube/videos',
    '이지영': 'https://www.youtube.com/@leejiyoung_official/videos',  
    '이다지': 'https://www.youtube.com/@2dajido/videos',
    '프랭크쌤': 'https://www.youtube.com/@frankssem_basic/videos',
    '설민석': 'https://www.youtube.com/@seolminseok/videos'
}

# keywords to search for
keywords = ['학원', '강사', '선생님', '교사', '학생']

CHECKPOINT_FILE = 'scrape_checkpoint.json'


def load_checkpoint():
    """load checkpoint if exists"""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {'processed_videos': [], 'comments': []}


def save_checkpoint(processed_videos, comments):
    """save checkpoint"""
    with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
        json.dump({
            'processed_videos': processed_videos,
            'comments': comments,
            'last_saved': datetime.now().isoformat()
        }, f, ensure_ascii=False)


def get_all_video_urls(youtube, channel_url):
    """get ALL videos from channel using api"""
    handle = channel_url.split('@')[1].split('/')[0]
    
    try:
        # search for channel
        response = youtube.search().list(
            part='snippet',
            q=handle,
            type='channel',
            maxResults=1
        ).execute()
        
        if not response['items']:
            print(f"    channel not found: {handle}")
            return []
        
        channel_id = response['items'][0]['snippet']['channelId']
        
        # get uploads playlist
        channel_response = youtube.channels().list(
            part='contentDetails',
            id=channel_id
        ).execute()
        
        uploads_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        
        # paginate through ALL videos
        video_urls = []
        next_page = None
        
        while True:
            playlist_response = youtube.playlistItems().list(
                part='contentDetails',
                playlistId=uploads_id,
                maxResults=50,
                pageToken=next_page
            ).execute()
            
            for item in playlist_response['items']:
                vid = item['contentDetails']['videoId']
                video_urls.append(f'https://www.youtube.com/watch?v={vid}')
            
            next_page = playlist_response.get('nextPageToken')
            if not next_page:
                break
        
        return video_urls
        
    except HttpError as e:
        print(f"    api error: {e}")
        return []


def get_exact_dates(youtube, video_id):
    """get comment dates from youtube api"""
    comments = []
    
    try:
        next_page = None
        while True:
            response = youtube.commentThreads().list(
                part='snippet,replies',
                videoId=video_id,
                maxResults=100,
                pageToken=next_page,
                textFormat='plainText'
            ).execute()
            
            for item in response['items']:
                # top-level comment
                snippet = item['snippet']['topLevelComment']['snippet']
                comments.append({
                    'author': snippet['authorDisplayName'],
                    'text': snippet['textDisplay'],
                    'date': snippet['publishedAt'],
                    'likes': snippet['likeCount']
                })
                
                # replies
                if 'replies' in item:
                    for reply in item['replies']['comments']:
                        r_snippet = reply['snippet']
                        comments.append({
                            'author': r_snippet['authorDisplayName'],
                            'text': r_snippet['textDisplay'],
                            'date': r_snippet['publishedAt'],
                            'likes': r_snippet['likeCount']
                        })
            
            next_page = response.get('nextPageToken')
            if not next_page:
                break
        
        return comments
        
    except HttpError as e:
        if 'commentsDisabled' not in str(e):
            print(f'api error: {e}')
        return []


def normalize_text(text):
    """normalize text for matching"""
    if not text:
        return ""
    return ''.join(text.split()).lower()


def parse_relative_date(time_str):
    """convert '2 days ago' to approximate date"""
    if not time_str:
        return None
    
    now = datetime.now()
    time_str = time_str.lower().strip().replace('(edited)', '').strip()
    
    match = re.match(r'(\d+)\s+(second|minute|hour|day|week|month|year)s?\s+ago', time_str)
    if not match:
        if 'just now' in time_str or 'moment' in time_str:
            return now.strftime('%Y-%m-%dT%H:%M:%SZ')
        return None
    
    num = int(match.group(1))
    unit = match.group(2)
    
    if unit == 'second':
        approx = now - timedelta(seconds=num)
    elif unit == 'minute':
        approx = now - timedelta(minutes=num)
    elif unit == 'hour':
        approx = now - timedelta(hours=num)
    elif unit == 'day':
        approx = now - timedelta(days=num)
    elif unit == 'week':
        approx = now - timedelta(weeks=num)
    elif unit == 'month':
        approx = now - timedelta(days=num * 30)
    elif unit == 'year':
        approx = now - timedelta(days=num * 365)
    else:
        return None
    
    return approx.strftime('%Y-%m-%dT%H:%M:%SZ') + ' (approx)'


def find_exact_date(comment_text, comment_author, api_comments):
    """find exact date for a comment"""
    text_norm = normalize_text(comment_text)
    
    for c in api_comments:
        if c['author'] == comment_author:
            c_norm = normalize_text(c['text'])
            if text_norm[:30] in c_norm or c_norm[:30] in text_norm:
                return c['date']
    
    # try without exact author match
    for c in api_comments:
        c_norm = normalize_text(c['text'])
        if text_norm[:50] == c_norm[:50]:
            return c['date']
    
    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--api-key', help='youtube api key')
    parser.add_argument('--key-file', help='file containing api key')
    parser.add_argument('--output', default='hagwon_comments.csv', help='output file')
    parser.add_argument('--max-comments', type=int, default=100, help='max matching comments per video')
    parser.add_argument('--skip-dates', action='store_true', help='skip exact dates (faster, keep relative dates)')
    parser.add_argument('--resume', action='store_true', help='resume from checkpoint')
    parser.add_argument('--checkpoint-every', type=int, default=10, help='save checkpoint every n videos')
    args = parser.parse_args()
    
    # get api key
    api_key = args.api_key
    if not api_key and args.key_file:
        with open(args.key_file, 'r') as f:
            api_key = f.read().strip()
    if not api_key:
        api_key = os.environ.get('YOUTUBE_API_KEY')
    
    if not api_key:
        print("api key required")
        return
    
    youtube = build('youtube', 'v3', developerKey=api_key)
    downloader = YoutubeCommentDownloader()
    
    # load checkpoint if resuming
    if args.resume:
        checkpoint = load_checkpoint()
        processed_videos = set(checkpoint['processed_videos'])
        all_comments = checkpoint['comments']
        print(f"resuming from checkpoint: {len(processed_videos)} videos processed, {len(all_comments)} comments")
    else:
        processed_videos = set()
        all_comments = []
    
    videos_since_checkpoint = 0
    
    for name, channel_url in channels.items():
        print(f'\nscraping {name}...')
        
        # get ALL videos using api
        video_urls = get_all_video_urls(youtube, channel_url)
        print(f'  found {len(video_urls)} videos')
        
        if not video_urls:
            continue
        
        for i, video_url in enumerate(video_urls):
            # skip if already processed
            if video_url in processed_videos:
                continue
            
            print(f'  video {i+1}/{len(video_urls)}...', end=' ')
            
            try:
                # get comments with downloader (fast, no quota)
                comments = downloader.get_comments_from_url(video_url)
                
                # filter by keywords
                filtered = []
                for comment in comments:
                    text = comment['text']
                    if any(kw in text for kw in keywords):
                        filtered.append(comment)
                        if len(filtered) >= args.max_comments:
                            break
                
                if not filtered:
                    print('0 comments')
                    processed_videos.add(video_url)
                    videos_since_checkpoint += 1
                    continue
                
                # get exact dates from api (unless skipped)
                if args.skip_dates:
                    api_comments = []
                else:
                    video_id = video_url.split('v=')[1].split('&')[0]
                    api_comments = get_exact_dates(youtube, video_id)
                
                # match and save
                for comment in filtered:
                    if args.skip_dates:
                        date = comment['time']  # keep relative date
                    else:
                        exact_date = find_exact_date(comment['text'], comment['author'], api_comments)
                        if not exact_date:
                            exact_date = parse_relative_date(comment['time'])
                        date = exact_date
                    
                    all_comments.append({
                        'channel': name,
                        'video_url': video_url,
                        'text': comment['text'],
                        'author': comment['author'],
                        'date': date,
                        'likes': comment['votes']
                    })
                
                print(f'{len(filtered)} comments')
                processed_videos.add(video_url)
                videos_since_checkpoint += 1
                
                # save checkpoint periodically
                if videos_since_checkpoint >= args.checkpoint_every:
                    save_checkpoint(list(processed_videos), all_comments)
                    print(f'    [checkpoint saved: {len(all_comments)} comments]')
                    videos_since_checkpoint = 0
                
            except Exception as e:
                print(f'error: {e}')
    
    # final save
    if all_comments:
        df = pd.DataFrame(all_comments)
        df.to_csv(args.output, index=False, encoding='utf-8-sig')
        print(f'\ntotal: {len(all_comments)} comments saved to {args.output}')
        
        # clean up checkpoint
        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)
            print('checkpoint file removed')
    else:
        print('\nno comments found')


if __name__ == '__main__':
    main()