import psycopg2
from psycopg2 import sql
import pandas as pd
import urllib.request
import json
import time
from datetime import datetime
import re
from youtube_transcript_api import YouTubeTranscriptApi
import os
import logging

# Logging is configured in server.py, so we'll rely on it being passed or set up there
logger = logging.getLogger(__name__)

# Load database configuration from file
def load_db_config():
    with open('config/db_config.json', 'r') as f:
        return json.load(f)

def load_db_params():
    return load_db_config()

# Load API key from file
def get_api_key(api_key_path=None):
    if api_key_path and os.path.exists(api_key_path):
        with open(api_key_path, 'r') as f:
            return f.read().strip()
    return os.getenv("YOUTUBE_API_KEY", "")

def execute_query(db_params, query, params=None):
    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            conn.commit()
            return cur

def get_vid_json_list_from_channel(channel_id, after_date, api_key):
    if after_date is None:
        first_url = f'https://www.googleapis.com/youtube/v3/search?key={api_key}&channelId={channel_id}&part=snippet,id&order=date&maxResults=25'
    else:
        first_url = f'https://www.googleapis.com/youtube/v3/search?key={api_key}&channelId={channel_id}&part=snippet,id&order=date&maxResults=25&publishedAfter={after_date}'
    url = first_url
    vid_json_list = []
    while True:
        try:
            time.sleep(1)  # Increased delay to reduce throttling risk
            with urllib.request.urlopen(url, timeout=1) as response:
                resp = json.load(response)
                vid_json_list.append(resp)
                next_page_token = resp.get('nextPageToken')
                if next_page_token:
                    url = f'{first_url}&pageToken={next_page_token}'
                else:
                    break
        except urllib.error.HTTPError as e:
            if e.code == 403:
                logger.error(f"HTTP 403 Forbidden for {channel_id}. Likely API quota exhausted.")
                raise Exception("API quota likely exhausted (HTTP 403).")
            else:
                logger.error(f"Error fetching videos for {channel_id}: {e}")
                break
    return vid_json_list

def get_new_channels_to_pull_vid_data_for(db_params):
    sql_script = """
    SELECT DISTINCT CHANNEL_ID 
    FROM CHANNEL_TABLE 
    WHERE CHANNEL_ID NOT IN (SELECT DISTINCT CHANNEL_ID FROM VID_TABLE);
    """
    with psycopg2.connect(**db_params) as conn:
        return pd.read_sql_query(sql_script, conn)

def insert_vid_data_for_new_channels(db_params, api_key):
    channel_id_df = get_new_channels_to_pull_vid_data_for(db_params)
    
    insert_query_vid_table = sql.SQL("""
    INSERT INTO VID_TABLE(CHANNEL_ID, VID_ID, INSERT_AT)
    VALUES (%s, %s, %s)
    ON CONFLICT (VID_ID) DO NOTHING;
    """)
    
    insert_query_vid_data = sql.SQL("""
    INSERT INTO VID_DATA_TABLE(VID_ID, TITLE, DESCRIPTION, PUBLISHTIME, SNIPPET, INSERT_AT)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (VID_ID) DO NOTHING;
    """)

    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cur:
            for channel_id in channel_id_df['channel_id']:
                logger.info(f"Processing video data for new channel {channel_id}")
                try:
                    test_vid_list = get_vid_json_list_from_channel(channel_id, None, api_key)
                    for page in test_vid_list:
                        for vid in page['items']:
                            if vid['id']['kind'] == 'youtube#video':
                                vid_id = vid['id']['videoId']
                                snippet = vid['snippet']
                                title = snippet['title']
                                description = snippet['description']
                                publishtime = snippet['publishedAt']
                                insert_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                                
                                cur.execute(insert_query_vid_table, (channel_id, vid_id, insert_time))
                                cur.execute(insert_query_vid_data, (vid_id, title, description, publishtime, json.dumps(snippet), insert_time))
                    conn.commit()
                except Exception as e:
                    logger.error(f"Error pulling data for {channel_id}: {e}")
                    if "API quota likely exhausted" in str(e):
                        logger.info("Stopping video data collection due to quota limit.")
                        conn.commit()
                        raise
                    conn.rollback()

def get_date_of_last_vid_by_channel_df(db_params):
    sql_script = """
    SELECT 
        VID.CHANNEL_ID,
        MAX(DAT.PUBLISHTIME) as PUBLISHTIME
    FROM
        VID_TABLE AS VID
    JOIN
        VID_DATA_TABLE as DAT
    ON
        VID.VID_ID = DAT.VID_ID
    GROUP BY
        VID.CHANNEL_ID;
    """
    with psycopg2.connect(**db_params) as conn:
        return pd.read_sql_query(sql_script, conn)

def insert_new_vid_data_for_current_channels(db_params, api_key):
    vid_tab_df = get_date_of_last_vid_by_channel_df(db_params)
    
    insert_query_vid_table = sql.SQL("""
    INSERT INTO VID_TABLE(CHANNEL_ID, VID_ID, INSERT_AT)
    VALUES (%s, %s, %s)
    ON CONFLICT (VID_ID) DO NOTHING;
    """)
    
    insert_query_vid_data = sql.SQL("""
    INSERT INTO VID_DATA_TABLE(VID_ID, TITLE, DESCRIPTION, PUBLISHTIME, SNIPPET, INSERT_AT)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (VID_ID) DO NOTHING;
    """)

    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cur:
            for _, row in vid_tab_df.iterrows():
                channel_id = row['channel_id']
                latest_date = row['publishtime']
                logger.info(f"Processing new video data for existing channel {channel_id}")
                try:
                    if isinstance(latest_date, str):
                        latest_date = datetime.strptime(latest_date, "%Y-%m-%d %H:%M:%S")
                    latest_date_rfc3339 = latest_date.strftime("%Y-%m-%dT%H:%M:%SZ")
                    test_vid_list = get_vid_json_list_from_channel(channel_id, latest_date_rfc3339, api_key)
                    for page in test_vid_list:
                        for vid in page['items']:
                            if vid['id']['kind'] == 'youtube#video':
                                vid_id = vid['id']['videoId']
                                snippet = vid['snippet']
                                title = snippet['title']
                                description = snippet['description']
                                publishtime = snippet['publishedAt']
                                insert_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                                
                                cur.execute(insert_query_vid_table, (channel_id, vid_id, insert_time))
                                cur.execute(insert_query_vid_data, (vid_id, title, description, publishtime, json.dumps(snippet), insert_time))
                    conn.commit()
                except Exception as e:
                    logger.error(f"Error for {channel_id}: {e}")
                    if "API quota likely exhausted" in str(e):
                        logger.info("Stopping video updates due to quota limit.")
                        conn.commit()
                        raise
                    conn.rollback()

def get_transcripts_to_be_pulled(db_params):
    sql_script = """
    SELECT DISTINCT VID_ID
    FROM VID_TABLE
    WHERE VID_ID NOT IN (SELECT VID_ID FROM VID_TRANSCRIPT_TABLE);
    """
    with psycopg2.connect(**db_params) as conn:
        return pd.read_sql_query(sql_script, conn)

def insert_new_transcripts(db_params):
    needed_transcript_vid_id_df = get_transcripts_to_be_pulled(db_params)
    
    insert_query = sql.SQL("""
    INSERT INTO VID_TRANSCRIPT_TABLE(VID_ID, TEXT, START, DURATION, TEXT_FORMATTED, WORD_COUNT, CUM_WORD_COUNT, INSERT_AT)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (VID_ID, START) DO NOTHING;
    """)

    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cur:
            for vid_id in needed_transcript_vid_id_df['vid_id']:
                logger.info(f"Processing transcript for {vid_id}")
                try:
                    time.sleep(0.125)
                    transcript_info_list = YouTubeTranscriptApi.get_transcript(vid_id)
                    cum_word_count = 0
                    for transcript_line in transcript_info_list:
                        text = transcript_line['text']
                        start = transcript_line['start']
                        duration = transcript_line['duration']
                        text_formatted = text.lower()
                        text_formatted = re.sub(r"\[.*\]|\{.*\}", "", text_formatted)
                        text_formatted = re.sub(r'[^\w\s]', "", text_formatted)
                        word_count = len(text_formatted.split())
                        cum_word_count += word_count
                        cur.execute(insert_query, (vid_id, text, start, duration, text_formatted, word_count, cum_word_count, datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")))
                    conn.commit()
                except Exception as e:
                    logger.error(f"Error for {vid_id}: {e}")
                    cur.execute("SELECT COUNT(*) FROM VID_TRANSCRIPT_TABLE WHERE VID_ID = %s", (vid_id,))
                    row_count = cur.fetchone()[0]
                    if row_count == 0:
                        cur.execute(insert_query, (vid_id, "No transcript available", -1, 0, "", 0, 0, datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")))
                    conn.commit()

def insert_initial_vid_model_states(db_params):
    logger.info("Getting new VID_IDs with transcripts that lack model states")
    sql_script = """
    SELECT DISTINCT v.VID_ID
    FROM VID_TABLE v
    LEFT JOIN VID_MODEL_STATE vms ON v.VID_ID = vms.VID_ID
    WHERE vms.VID_ID IS NULL
    AND EXISTS (SELECT 1 FROM VID_TRANSCRIPT_TABLE vt WHERE vt.VID_ID = v.VID_ID AND vt.START IS NOT NULL);
    """
    with psycopg2.connect(**db_params) as conn:
        vid_ids_df = pd.read_sql_query(sql_script, conn)
    
    model_keys_query = "SELECT MODEL_KEY FROM MODEL_TABLE;"
    with psycopg2.connect(**db_params) as conn:
        model_keys_df = pd.read_sql_query(model_keys_query, conn)
    
    if vid_ids_df.empty or model_keys_df.empty:
        logger.info("No new VID_IDs or MODEL_KEYs to process for VID_MODEL_STATE.")
        return
    
    insert_query = sql.SQL("""
    INSERT INTO VID_MODEL_STATE(VID_ID, MODEL_KEY, STATE)
    VALUES (%s, %s, NULL)
    ON CONFLICT (VID_ID, MODEL_KEY) DO NOTHING;
    """)
    
    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cur:
            assignments = [
                (vid_id, model_key)
                for vid_id in vid_ids_df['vid_id']
                for model_key in model_keys_df['model_key']
            ]
            logger.info(f"Inserting {len(assignments)} initial states into VID_MODEL_STATE.")
            cur.executemany(insert_query, assignments)
            conn.commit()

def maintain_database(api_key_path):
    db_params = load_db_params()
    api_key = get_api_key(api_key_path)

    # Add video data for new channels, quit on 403
    try:
        insert_vid_data_for_new_channels(db_params, api_key)
    except Exception as e:
        if "API quota likely exhausted" in str(e):
            logger.info("Quota exhausted. Skipping further API calls and moving to transcripts.")
        else:
            raise

    # Update video data for existing channels
    try:
        insert_new_vid_data_for_current_channels(db_params, api_key)
    except Exception as e:
        if "API quota likely exhausted" in str(e):
            logger.info("Quota exhausted. Skipping further API calls and moving to transcripts.")
        else:
            raise

    # Add transcripts
    insert_new_transcripts(db_params)

    # Add initial states for new VID_IDs with transcripts
    insert_initial_vid_model_states(db_params)

    logger.info("Database maintenance completed.")

if __name__ == "__main__":
    api_key_path = "config/YouTube.txt"
    maintain_database(api_key_path)