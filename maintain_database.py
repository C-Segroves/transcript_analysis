# maintain_database.py

import psycopg2
from psycopg2 import sql
import pandas as pd
import urllib.request
import json
import time
from datetime import datetime
import re
from youtube_transcript_api import YouTubeTranscriptApi
import pickle
import os

# Load database configuration (assumes Docker environment variables)

# Load database configuration from file
def load_db_config():
    print(os.listdir('../'))
    with open('config/db_config.json', 'r') as f:
        return json.load(f)

def load_db_params():
    db_params = load_db_config()
    return db_params

# Load API key from environment or file
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

def get_channel_seed_df(channel_seed_list_location):
    channel_seed_df = pd.read_csv(channel_seed_list_location)
    channel_seed_df['channel_handle'] = channel_seed_df['channel_url'].apply(lambda x: x.split('@')[1])
    return channel_seed_df

def get_current_channels(db_params):
    sql_script = """
    SELECT CHANNEL_HANDLE, CHANNEL_ID FROM CHANNEL_TABLE;
    """
    with psycopg2.connect(**db_params) as conn:
        return pd.read_sql_query(sql_script, conn)

def get_channel_id_from_seed_list(channel_seed_df, api_key, db_params):
    current_channels_df = get_current_channels(db_params)
    channel_seed_df = channel_seed_df.loc[~channel_seed_df['channel_handle'].isin(current_channels_df['channel_handle'])]

    channel_handle, channel_id, channel_snippet = [], [], []
    for index, row in channel_seed_df.iterrows():
        time.sleep(0.25)
        try:
            row_channel_handle = row['channel_handle']
            url = f'https://www.googleapis.com/youtube/v3/channels?key={api_key}&forHandle={row_channel_handle}'
            with urllib.request.urlopen(url, timeout=1) as response:
                resp = json.load(response)
                row_channel_id = resp['items'][0]['id']

            channel_url = f'https://www.googleapis.com/youtube/v3/channels?key={api_key}&id={row_channel_id}&part=snippet'
            with urllib.request.urlopen(channel_url, timeout=1) as response:
                resp2 = json.load(response)
                snippet = resp2['items'][0]['snippet']

            channel_handle.append(row_channel_handle)
            channel_id.append(row_channel_id)
            channel_snippet.append(snippet)
        except Exception as e:
            print(f"Error fetching channel {row_channel_handle}: {e}")
            break
    
    return pd.DataFrame({'channel_handle': channel_handle, 'channel_id': channel_id, 'channel_snippet': channel_snippet})

def insert_new_channels_into_channel_table(new_channels_df, db_params):
    insert_query = sql.SQL("""
    INSERT INTO CHANNEL_TABLE(CHANNEL_HANDLE, CHANNEL_ID, CHANNEL_SNIPPET, INSERT_AT)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (CHANNEL_ID) DO NOTHING;
    """)
    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cur:
            for _, row in new_channels_df.iterrows():
                cur.execute(insert_query, (row['channel_handle'], row['channel_id'], json.dumps(row['channel_snippet']), datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")))

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
                print(f"HTTP 403 Forbidden for {channel_id}. Likely API quota exhausted.")
                raise Exception("API quota likely exhausted (HTTP 403).")  # Signal to quit
            else:
                print(f"Error fetching videos for {channel_id}: {e}")
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
                print(f"Processing video data for {channel_id}")
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
                    print(f"Error pulling data for {channel_id}: {e}")
                    if "API quota likely exhausted" in str(e):
                        print("Stopping video data collection due to quota limit.")
                        conn.commit()  # Save what we’ve got
                        raise  # Re-raise to exit the function
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
                latest_date = row['publishtime']  # lowercase from SQL query
                print(f"Processing new video data for {channel_id}")
                try:
                    # Convert to RFC 3339 format (e.g., "2025-03-06T10:26:40Z")
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
                    print(f"Error for {channel_id}: {e}")
                    if "API quota likely exhausted" in str(e):
                        print("Stopping video updates due to quota limit.")
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
                print(f"Processing transcript for {vid_id}")
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
                    print(f"Error for {vid_id}: {e}")
                    # Insert single "no transcript" row with START = -1 only if no rows exist
                    cur.execute("SELECT COUNT(*) FROM VID_TRANSCRIPT_TABLE WHERE VID_ID = %s", (vid_id,))
                    row_count = cur.fetchone()[0]
                    if row_count == 0:
                        cur.execute(insert_query, (vid_id, "No transcript available", -1, 0, "", 0, 0, datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")))
                    conn.commit()

def insert_initial_vid_model_states(db_params):
    # Get new VID_IDs with transcripts that lack model states
    print("Get new VID_IDs with transcripts that lack model states")
    sql_script = """
    SELECT DISTINCT v.VID_ID
    FROM VID_TABLE v
    LEFT JOIN VID_MODEL_STATE vms ON v.VID_ID = vms.VID_ID
    WHERE vms.VID_ID IS NULL
    AND EXISTS (SELECT 1 FROM VID_TRANSCRIPT_TABLE vt WHERE vt.VID_ID = v.VID_ID AND vt.START IS NOT NULL);
    """
    with psycopg2.connect(**db_params) as conn:
        vid_ids_df = pd.read_sql_query(sql_script, conn)
    
    # Get all MODEL_KEYs
    model_keys_query = "SELECT MODEL_KEY FROM MODEL_TABLE;"
    with psycopg2.connect(**db_params) as conn:
        model_keys_df = pd.read_sql_query(model_keys_query, conn)
    
    if vid_ids_df.empty or model_keys_df.empty:
        print("No new VID_IDs or MODEL_KEYs to process for VID_MODEL_STATE.")
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
            print(f"Inserting {len(assignments)} initial states into VID_MODEL_STATE.")
            cur.executemany(insert_query, assignments)
            conn.commit()


def maintain_database(channel_seed_list_location, api_key_path):
    db_params = load_db_params()
    api_key = get_api_key(api_key_path)

    # Add new channels
    channel_seed_df = get_channel_seed_df(channel_seed_list_location)
    new_channels_df = get_channel_id_from_seed_list(channel_seed_df, api_key, db_params)
    insert_new_channels_into_channel_table(new_channels_df, db_params)

    # Add video data for new channels, quit on 403
    try:
        insert_vid_data_for_new_channels(db_params, api_key)
    except Exception as e:
        if "API quota likely exhausted" in str(e):
            print("Quota exhausted. Skipping further API calls and moving to transcripts.")
        else:
            raise  # Re-raise unexpected errors

    # Skip new video data for current channels if quota is hit
    try:
        insert_new_vid_data_for_current_channels(db_params, api_key)
    except Exception as e:
        if "API quota likely exhausted" in str(e):
            print("Quota exhausted. Skipping further API calls and moving to transcripts.")
        else:
            raise

    # Add transcripts (no API key needed here)
    insert_new_transcripts(db_params)

    # Add initial states for new VID_IDs with transcripts
    insert_initial_vid_model_states(db_params)

    print("Database maintenance completed.")

if __name__ == "__main__":
    channel_seed_list_location = "config\church channel seed list.csv" 
    api_key_path = "config\YouTube.txt"  
      
    maintain_database(channel_seed_list_location, api_key_path)