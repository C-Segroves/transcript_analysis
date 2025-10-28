import psycopg2
from psycopg2 import sql
import pandas as pd
from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, TranscriptsDisabled, IpBlocked
import re
import os
import logging
from yt_dlp import YoutubeDL
from datetime import datetime
import time
import json

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load database configuration from file
def load_db_config():
    with open('config/db_config.json', 'r') as f:
        return json.load(f)

# Execute SQL query
def execute_query(db_params, query, params=None):
    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            conn.commit()
            return cur

# Get VID_IDs with exactly one row and START = -1
def get_failed_transcript_vids(db_params):
    sql_script = """
    SELECT VID_ID
    FROM VID_TRANSCRIPT_TABLE
    WHERE START = -1
    AND INSERT_AT < '2025-10-18T00:00:00Z'
    GROUP BY VID_ID
    HAVING COUNT(*) = 1;
    """
    with psycopg2.connect(**db_params) as conn:
        return pd.read_sql_query(sql_script, conn)

# Fetch transcript using youtube_transcript_api with fallback to yt-dlp
def fetch_transcript(video_id, logger,ytt_api):
    # Try youtube_transcript_api first
    try:
        #ytt_api = YouTubeTranscriptApi()
        # Optional: Add proxies to bypass IP blocks
        # proxies = {'http': 'http://your-proxy:port', 'https': 'http://your-proxy:port'}
        # ytt_api = YouTubeTranscriptApi(proxies=proxies)
        
        transcript_info_list = ytt_api.fetch(video_id, languages=['en'], preserve_formatting=True)
        return [(snippet.text, snippet.start, snippet.duration) for snippet in transcript_info_list.snippets]
    
    except IpBlocked as e:
        logger.error(f"IP blocked by YouTube for {video_id}: {str(e)}. Stopping transcript fetching.")
        raise  # Re-raise to break the loop in update_failed_transcripts
    except (NoTranscriptFound, TranscriptsDisabled, Exception) as e:
        logger.error(f"youtube_transcript_api failed for {video_id}: {str(e)}")
        # Fallback to yt-dlp
        try:
            url = f"https://www.youtube.com/watch?v={video_id}"
            ydl_opts = {
                'writeautomaticsub': True,
                'subtitleslangs': ['en'],
                'skip_download': True,
                'outtmpl': f"{video_id}.%(ext)s",
                'quiet': True,
                # 'proxy': 'http://your-proxy:port',  # Add if needed
                'extractor_retries': 3,  # Retry on transient errors
            }
            
            with YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])
            
            vtt_filename = f"{video_id}.en.vtt"
            if not os.path.exists(vtt_filename):
                raise FileNotFoundError("No transcript found.")
            
            # Parse VTT
            with open(vtt_filename, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            transcript = []
            current_text = []
            current_start = None
            current_duration = None
            for line in lines:
                line = line.strip()
                if line and '-->' in line:
                    if current_text:
                        text = ' '.join(current_text).strip()
                        if text and current_start is not None:
                            transcript.append((text, current_start, current_duration))
                        current_text = []
                    start_str, end_str = line.split(' --> ')
                    start_time = parse_vtt_time(start_str)
                    end_time = parse_vtt_time(end_str)
                    current_start = start_time
                    current_duration = end_time - start_time
                elif line and not (re.match(r'^\d+$', line) or line.startswith('WEBVTT') or line.startswith('Kind:')):
                    current_text.append(line)
            
            if current_text and current_start is not None:
                text = ' '.join(current_text).strip()
                if text:
                    transcript.append((text, current_start, current_duration))
            
            os.remove(vtt_filename)
            return transcript if transcript else None
        
        except Exception as e:
            logger.error(f"yt-dlp fallback failed for {video_id}: {str(e)}")
            return None

# Helper to parse VTT timestamp to seconds
def parse_vtt_time(time_str):
    # Format: "00:00:01.000" or "00:01:01.000"
    parts = time_str.split(':')
    hours = int(parts[0]) if len(parts) == 3 else 0
    minutes = int(parts[-2])
    seconds = float(parts[-1].replace(',', '.'))
    return hours * 3600 + minutes * 60 + seconds

# Update VID_TRANSCRIPT_TABLE
def update_failed_transcripts(db_params, logger):
    # Get failed transcript VID_IDs
    vid_ids_df = get_failed_transcript_vids(db_params)
    if vid_ids_df.empty:
        logger.info("No failed transcripts (START = -1 with single row) found.")
        return

    # SQL queries
    delete_query = sql.SQL("DELETE FROM VID_TRANSCRIPT_TABLE WHERE VID_ID = %s AND START = -1")
    insert_query = sql.SQL("""
    INSERT INTO VID_TRANSCRIPT_TABLE(VID_ID, TEXT, START, DURATION, TEXT_FORMATTED, WORD_COUNT, CUM_WORD_COUNT, INSERT_AT)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (VID_ID, START) DO NOTHING;
    """)

    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cur:
            ytt_api = YouTubeTranscriptApi()
            for vid_id in vid_ids_df['vid_id']:
                try:
                    time.sleep(60)  # To avoid rate limiting
                    logger.info(f"Attempting to update transcript for {vid_id}")
                    
                    transcript_data = fetch_transcript(vid_id, logger,ytt_api)
                    
                    if transcript_data:
                        # Delete the failed row
                        cur.execute(delete_query, (vid_id,))
                        
                        # Insert new transcript data
                        cum_word_count = 0
                        for text, start, duration in transcript_data:
                            text_formatted = text.lower()
                            text_formatted = re.sub(r"\[.*\]|\{.*\}", "", text_formatted)
                            text_formatted = re.sub(r'[^\w\s]', "", text_formatted)
                            word_count = len(text_formatted.split())
                            cum_word_count += word_count
                            insert_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                            cur.execute(insert_query, (
                                vid_id, text, start, duration, text_formatted, word_count, cum_word_count, insert_time
                            ))
                        conn.commit()
                        logger.info(f"Successfully updated transcript for {vid_id}")
                    else:
                        logger.warning(f"No transcript available for {vid_id}. Keeping existing failed row. Updating INSERT_AT Timestamp.")
                        cur.execute(delete_query, (vid_id,))
                        insert_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                        cur.execute(insert_query, (
                            vid_id, "No transcript available", -1, 0, "", 0, 0, insert_time
                        ))
                        conn.commit()
                
                except IpBlocked:
                    logger.error(f"IP block detected. Stopping transcript updates to avoid further blocks.")
                    conn.commit()  # Commit any successful updates before breaking
                    break  # Exit the loop to prevent further requests
                except Exception as e:
                    logger.error(f"Error processing {vid_id}: {str(e)}")
                    conn.rollback()

if __name__ == "__main__":
    db_params = load_db_config()
    update_failed_transcripts(db_params, logger)