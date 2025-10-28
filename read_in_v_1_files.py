import psycopg2
from psycopg2 import sql
import pandas as pd
import os
import pickle
import json
from datetime import datetime
import re
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load database configuration from file
def load_db_config():
    with open('config/db_config.json', 'r') as f:
        return json.load(f)

# Function to check if transcript needs update/insert
def needs_transcript_update(db_params, vid_id):
    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cur:
            # Check if any valid transcript rows exist (START != -1)
            cur.execute("SELECT COUNT(*) FROM VID_TRANSCRIPT_TABLE WHERE VID_ID = %s AND START != -1", (vid_id,))
            has_valid_trans = cur.fetchone()[0] > 0
            if has_valid_trans:
                return False, False  # No need to insert, has valid transcript
            
            # Check if failed row exists (START = -1)
            cur.execute("SELECT COUNT(*) FROM VID_TRANSCRIPT_TABLE WHERE VID_ID = %s AND START = -1", (vid_id,))
            has_failed = cur.fetchone()[0] > 0
            
            return True, has_failed  # Needs insert, and delete if failed row exists

if __name__ == "__main__":
    db_params = load_db_config()
    csv_path = "Z:/Project Data/You Tube/Church List.csv"
    channel_files_base = "Z:/Project Data/You Tube/Channel Files/"
    transcripts_base = "Z:/Project Data/You Tube/Raw Transcripts/"
    
    # Read the CSV
    df_churches = pd.read_csv(csv_path, usecols=['Church', 'Channel ID', 'Channel Title'])
    
    for _, row in df_churches.iterrows():
        church = row['Church']
        channel_id = row['Channel ID']
        channel_title = row['Channel Title']
        
        church_folder = os.path.join(channel_files_base, channel_title)
        if not os.path.isdir(church_folder):
            logger.info(f"No folder found for church: {channel_title}")
            continue
        
        channel_pickle_path = os.path.join(church_folder, 'channel_data.pickle')
        vid_pickle_path = os.path.join(church_folder, 'vid_dict.pickle')
        
        if not (os.path.exists(channel_pickle_path) and os.path.exists(vid_pickle_path)):
            logger.info(f"Missing pickle files for church: {church}")
            continue
        
        # Load pickle files
        with open(channel_pickle_path, 'rb') as f:
            channel_data = pickle.load(f)
        
        with open(vid_pickle_path, 'rb') as f:
            vid_dict = pickle.load(f)
        
        # Insert channel if not exists
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM CHANNEL_TABLE WHERE CHANNEL_ID = %s", (channel_id,))
                if cur.fetchone()[0] == 0:
                    insert_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                    channel_snippet = json.dumps(channel_data)  # Assuming channel_data is a dict suitable for JSON
                    cur.execute(
                        "INSERT INTO CHANNEL_TABLE (CHANNEL_HANDLE, CHANNEL_ID, CHANNEL_SNIPPET, INSERT_AT) VALUES (%s, %s, %s, %s)",
                        (None, channel_id, channel_snippet, insert_at)  # CHANNEL_HANDLE set to None if unknown
                    )
                    logger.info(f"Inserted channel: {channel_id}")
                conn.commit()
        
        # Check transcripts folder
        trans_folder = os.path.join(transcripts_base, channel_title)
        if not os.path.isdir(trans_folder):
            logger.info(f"No transcripts folder for church: {church}")
            continue
        
        # Process each video
        num_vids = len(vid_dict['Vid_ID'])
        for i in range(num_vids):
            vid_id = vid_dict['Vid_ID'][i]
            title = vid_dict['Title'][i]
            publish_time_str = vid_dict['PublishTime'][i]
            
            # Insert VID_TABLE and VID_DATA_TABLE if missing
            with psycopg2.connect(**db_params) as conn:
                with conn.cursor() as cur:
                    # Insert VID_TABLE if not exists
                    cur.execute("SELECT COUNT(*) FROM VID_TABLE WHERE VID_ID = %s", (vid_id,))
                    if cur.fetchone()[0] == 0:
                        insert_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                        cur.execute(
                            "INSERT INTO VID_TABLE (CHANNEL_ID, VID_ID, INSERT_AT) VALUES (%s, %s, %s)",
                            (channel_id, vid_id, insert_at)
                        )
                        logger.info(f"Inserted VID_TABLE entry for: {vid_id}")
                    
                    # Insert VID_DATA_TABLE if not exists
                    cur.execute("SELECT COUNT(*) FROM VID_DATA_TABLE WHERE VID_ID = %s", (vid_id,))
                    if cur.fetchone()[0] == 0:
                        try:
                            publish_time = datetime.strptime(publish_time_str, "%Y-%m-%dT%H:%M:%SZ")
                        except ValueError:
                            logger.warning(f"Invalid publish time format for {vid_id}: {publish_time_str}. Skipping VID_DATA insert.")
                            continue
                        description = ''  # Missing in vid_dict, set to empty
                        snippet = json.dumps({
                            'publishedAt': publish_time_str,
                            'channelId': channel_id,
                            'title': title,
                            'description': description,
                            'channelTitle': channel_title
                        })
                        insert_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                        cur.execute(
                            "INSERT INTO VID_DATA_TABLE (VID_ID, TITLE, DESCRIPTION, PUBLISHTIME, SNIPPET, INSERT_AT) VALUES (%s, %s, %s, %s, %s, %s)",
                            (vid_id, title, description, publish_time, snippet, insert_at)
                        )
                        logger.info(f"Inserted VID_DATA_TABLE entry for: {vid_id}")
                    conn.commit()
            
            # Check for transcript CSV
            trans_path = os.path.join(trans_folder, f"{vid_id}.csv")
            if not os.path.exists(trans_path):
                continue
            
            # Check if transcript needs to be inserted/updated
            needs_insert, needs_delete = needs_transcript_update(db_params, vid_id)
            if not needs_insert:
                logger.info(f"Transcript already exists for {vid_id}. Skipping.")
                continue
            
            # Read transcript CSV
            try:
                df_trans = pd.read_csv(trans_path)
                required_cols = {'text', 'start', 'duration'}
                if set(df_trans.columns) != required_cols:
                    logger.warning(f"Invalid columns in transcript CSV for {vid_id}: {df_trans.columns}. Skipping.")
                    continue
            except Exception as e:
                logger.error(f"Error reading transcript CSV for {vid_id}: {e}")
                continue
            
            # Prepare insert query
            insert_query = sql.SQL("""
            INSERT INTO VID_TRANSCRIPT_TABLE(VID_ID, TEXT, START, DURATION, TEXT_FORMATTED, WORD_COUNT, CUM_WORD_COUNT, INSERT_AT)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (VID_ID, START) DO NOTHING;
            """)
            
            # Insert transcript
            with psycopg2.connect(**db_params) as conn:
                with conn.cursor() as cur:
                    if needs_delete:
                        cur.execute("DELETE FROM VID_TRANSCRIPT_TABLE WHERE VID_ID = %s", (vid_id,))
                        logger.info(f"Deleted failed transcript row for {vid_id}")
                    
                    cum_word_count = 0
                    insert_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                    for _, r in df_trans.iterrows():
                        text = r['text']
                        start = float(r['start'])
                        duration = float(r['duration'])
                        text_formatted = text.lower()
                        text_formatted = re.sub(r"\[.*\]|\{.*\}", "", text_formatted)
                        text_formatted = re.sub(r'[^\w\s]', "", text_formatted)
                        word_count = len(text_formatted.split())
                        cum_word_count += word_count
                        cur.execute(insert_query, (vid_id, text, start, duration, text_formatted, word_count, cum_word_count, insert_at))
                    
                    conn.commit()
                    logger.info(f"Successfully inserted transcript for {vid_id}")