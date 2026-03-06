import psycopg2
from psycopg2 import sql
import pandas as pd
import urllib.request
import json
import time
from datetime import datetime
import re
from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, TranscriptsDisabled, IpBlocked
import os
import logging

# Load database configuration from file
def load_db_config():
    with open('config/db_config.json', 'r') as f:
        return json.load(f)

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

def get_vid_json_list_from_channel(channel_id, after_date, api_key, logger:logging.Logger):
    """Get videos from channel using search API (date-based)."""
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

def get_all_video_ids_from_channel_uploads(channel_id, api_key, logger:logging.Logger):
    """
    Get ALL video IDs from a channel using the 'Uploads' playlist.
    This is more comprehensive than date-based search and helps find gaps.
    The uploads playlist ID is 'UU' + channel_id (without the 'UC' prefix).
    """
    # Convert channel ID to uploads playlist ID: UC... -> UU...
    if channel_id.startswith('UC'):
        uploads_playlist_id = 'UU' + channel_id[2:]
    else:
        logger.warning(f"Unexpected channel ID format: {channel_id}")
        uploads_playlist_id = None
        return []
    
    video_ids = []
    url = f'https://www.googleapis.com/youtube/v3/playlistItems?key={api_key}&playlistId={uploads_playlist_id}&part=contentDetails&maxResults=50'
    
    while True:
        try:
            time.sleep(1)  # Rate limiting
            with urllib.request.urlopen(url, timeout=10) as response:
                resp = json.load(response)
                
                for item in resp.get('items', []):
                    video_id = item['contentDetails']['videoId']
                    video_ids.append(video_id)
                
                next_page_token = resp.get('nextPageToken')
                if next_page_token:
                    url = f'https://www.googleapis.com/youtube/v3/playlistItems?key={api_key}&playlistId={uploads_playlist_id}&part=contentDetails&maxResults=50&pageToken={next_page_token}'
                else:
                    break
        except urllib.error.HTTPError as e:
            if e.code == 403:
                logger.error(f"HTTP 403 Forbidden for playlist {uploads_playlist_id}. Likely API quota exhausted.")
                raise Exception("API quota likely exhausted (HTTP 403).")
            elif e.code == 404:
                logger.warning(f"Uploads playlist not found for channel {channel_id}. May need to use search API instead.")
                return []
            else:
                logger.error(f"Error fetching playlist items for {channel_id}: {e}")
                break
        except Exception as e:
            logger.error(f"Unexpected error fetching playlist: {e}")
            break
    
    logger.info(f"Found {len(video_ids)} total videos in channel {channel_id} uploads playlist")
    return video_ids

def get_missing_video_ids_for_channel(db_params, channel_id, api_key, logger:logging.Logger):
    """
    Find video IDs that exist on YouTube but are missing from the database.
    Returns a list of missing video IDs.
    """
    # Get all video IDs from YouTube
    all_youtube_vids = get_all_video_ids_from_channel_uploads(channel_id, api_key, logger)
    if not all_youtube_vids:
        logger.warning(f"Could not get video list from YouTube for channel {channel_id}")
        return []
    
    # Get all video IDs we have in the database for this channel
    get_channel_internal_id_query = sql.SQL("""
    SELECT id FROM channel_table WHERE yt_channel_id = %s
    """)
    
    get_existing_vids_query = sql.SQL("""
    SELECT v.yt_vid_id 
    FROM vid_table v
    WHERE v.channel_id = %s
    """)
    
    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cur:
            # Get internal channel_id
            cur.execute(get_channel_internal_id_query, (channel_id,))
            channel_row = cur.fetchone()
            if not channel_row:
                logger.warning(f"Channel {channel_id} not found in database")
                return all_youtube_vids  # Return all as missing
            
            internal_channel_id = channel_row[0]
            
            # Get existing video IDs
            cur.execute(get_existing_vids_query, (internal_channel_id,))
            existing_vids = {row[0] for row in cur.fetchall()}
    
    # Find missing videos
    missing_vids = [vid_id for vid_id in all_youtube_vids if vid_id not in existing_vids]
    
    logger.info(f"Channel {channel_id}: {len(all_youtube_vids)} total videos on YouTube, {len(existing_vids)} in database, {len(missing_vids)} missing")
    
    return missing_vids

def get_video_details_by_ids(video_ids, api_key, logger:logging.Logger):
    """
    Get video details for a list of video IDs using the Videos API.
    This is more efficient than individual searches.
    """
    if not video_ids:
        return []
    
    # YouTube API allows up to 50 video IDs per request
    all_videos = []
    batch_size = 50
    
    for i in range(0, len(video_ids), batch_size):
        batch = video_ids[i:i+batch_size]
        video_ids_str = ','.join(batch)
        
        url = f'https://www.googleapis.com/youtube/v3/videos?key={api_key}&id={video_ids_str}&part=snippet,contentDetails'
        
        try:
            time.sleep(1)  # Rate limiting
            with urllib.request.urlopen(url, timeout=10) as response:
                resp = json.load(response)
                all_videos.extend(resp.get('items', []))
        except urllib.error.HTTPError as e:
            if e.code == 403:
                logger.error(f"HTTP 403 Forbidden. Likely API quota exhausted.")
                raise Exception("API quota likely exhausted (HTTP 403).")
            else:
                logger.error(f"Error fetching video details: {e}")
                break
        except Exception as e:
            logger.error(f"Unexpected error fetching video details: {e}")
            break
    
    return all_videos

def get_new_channels_to_pull_vid_data_for(db_params):
    sql_script = """
    SELECT DISTINCT c.yt_channel_id as channel_id
    FROM channel_table c
    WHERE c.yt_channel_id NOT IN (
        SELECT DISTINCT c2.yt_channel_id
        FROM channel_table c2
        JOIN vid_table v ON v.channel_id = c2.id
    );
    """
    with psycopg2.connect(**db_params) as conn:
        return pd.read_sql_query(sql_script, conn)

def insert_vid_data_for_new_channels(db_params, api_key, logger:logging.Logger):
    channel_id_df = get_new_channels_to_pull_vid_data_for(db_params)
    
    # Get internal channel_id for the external yt_channel_id
    get_channel_internal_id_query = sql.SQL("""
    SELECT id FROM channel_table WHERE yt_channel_id = %s
    """)
    
    # Insert or get existing id - use DO UPDATE with a real change to ensure RETURNING works
    insert_query_vid_table = sql.SQL("""
    INSERT INTO vid_table(channel_id, yt_vid_id, insert_at)
    VALUES (%s, %s, %s)
    ON CONFLICT (yt_vid_id) DO UPDATE SET insert_at = EXCLUDED.insert_at
    RETURNING id;
    """)
    
    # Fallback: get id if video already exists (in case RETURNING doesn't work on conflict)
    get_vid_id_query = sql.SQL("""
    SELECT id FROM vid_table WHERE yt_vid_id = %s
    """)
    
    # Check if vid_data already exists for this vid_id
    check_vid_data_exists_query = sql.SQL("""
    SELECT COUNT(*) FROM vid_data_table WHERE vid_id = %s
    """)
    
    insert_query_vid_data = sql.SQL("""
    INSERT INTO vid_data_table(vid_id, title, description, publishtime, snippet, insert_at)
    VALUES (%s, %s, %s, %s, %s, %s);
    """)

    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cur:
            for yt_channel_id in channel_id_df['channel_id']:
                logger.info(f"Processing video data for new channel {yt_channel_id}")
                try:
                    # Get internal channel_id
                    cur.execute(get_channel_internal_id_query, (yt_channel_id,))
                    channel_row = cur.fetchone()
                    if not channel_row:
                        logger.warning(f"Channel {yt_channel_id} not found in database, skipping")
                        continue
                    internal_channel_id = channel_row[0]
                    
                    test_vid_list = get_vid_json_list_from_channel(yt_channel_id, None, api_key,logger)
                    for page in test_vid_list:
                        for vid in page['items']:
                            if vid['id']['kind'] == 'youtube#video':
                                yt_vid_id = vid['id']['videoId']
                                snippet = vid['snippet']
                                title = snippet['title']
                                description = snippet['description']
                                publishtime = snippet['publishedAt']
                                insert_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                                
                                # Insert into vid_table and get the internal vid_id
                                # Use DO UPDATE instead of DO NOTHING so we always get the id back
                                cur.execute(insert_query_vid_table, (internal_channel_id, yt_vid_id, insert_time))
                                vid_row = cur.fetchone()
                                
                                # If RETURNING didn't work (shouldn't happen with DO UPDATE), get id manually
                                if not vid_row:
                                    cur.execute(get_vid_id_query, (yt_vid_id,))
                                    vid_row = cur.fetchone()
                                
                                if vid_row:
                                    internal_vid_id = vid_row[0]
                                    # Check if vid_data already exists, if not, insert it
                                    cur.execute(check_vid_data_exists_query, (internal_vid_id,))
                                    exists = cur.fetchone()[0]
                                    if exists == 0:
                                        # Insert into vid_data_table using internal vid_id
                                        cur.execute(insert_query_vid_data, (internal_vid_id, title, description, publishtime, json.dumps(snippet), insert_time))
                                        logger.debug(f"Inserted vid_data for {yt_vid_id}")
                                    else:
                                        logger.debug(f"vid_data already exists for {yt_vid_id}")
                                else:
                                    logger.warning(f"Could not get vid_id for {yt_vid_id}")
                    conn.commit()
                except Exception as e:
                    logger.error(f"Error pulling data for {yt_channel_id}: {e}")
                    if "API quota likely exhausted" in str(e):
                        logger.info("Stopping video data collection due to quota limit.")
                        conn.commit()
                        raise
                    conn.rollback()

def get_date_of_last_vid_by_channel_df(db_params):
    sql_script = """
    SELECT 
        c.yt_channel_id as channel_id,
        MAX(vd.publishtime) as publishtime
    FROM
        vid_table AS v
    JOIN
        vid_data_table as vd
    ON
        v.id = vd.vid_id
    JOIN
        channel_table as c
    ON
        v.channel_id = c.id
    GROUP BY
        c.yt_channel_id;
    """
    with psycopg2.connect(**db_params) as conn:
        return pd.read_sql_query(sql_script, conn)

def insert_new_vid_data_for_current_channels(db_params, api_key, logger:logging.Logger):
    vid_tab_df = get_date_of_last_vid_by_channel_df(db_params)
    
    # Get internal channel_id for the external yt_channel_id
    get_channel_internal_id_query = sql.SQL("""
    SELECT id FROM channel_table WHERE yt_channel_id = %s
    """)
    
    # Insert or get existing id - use DO UPDATE with a real change to ensure RETURNING works
    insert_query_vid_table = sql.SQL("""
    INSERT INTO vid_table(channel_id, yt_vid_id, insert_at)
    VALUES (%s, %s, %s)
    ON CONFLICT (yt_vid_id) DO UPDATE SET insert_at = EXCLUDED.insert_at
    RETURNING id;
    """)
    
    # Fallback: get id if video already exists (in case RETURNING doesn't work on conflict)
    get_vid_id_query = sql.SQL("""
    SELECT id FROM vid_table WHERE yt_vid_id = %s
    """)
    
    # Check if vid_data already exists for this vid_id
    check_vid_data_exists_query = sql.SQL("""
    SELECT COUNT(*) FROM vid_data_table WHERE vid_id = %s
    """)
    
    insert_query_vid_data = sql.SQL("""
    INSERT INTO vid_data_table(vid_id, title, description, publishtime, snippet, insert_at)
    VALUES (%s, %s, %s, %s, %s, %s);
    """)

    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cur:
            for _, row in vid_tab_df.iterrows():
                yt_channel_id = row['channel_id']
                latest_date = row['publishtime']
                logger.info(f"Processing new video data for existing channel {yt_channel_id}")
                try:
                    # Get internal channel_id
                    cur.execute(get_channel_internal_id_query, (yt_channel_id,))
                    channel_row = cur.fetchone()
                    if not channel_row:
                        logger.warning(f"Channel {yt_channel_id} not found in database, skipping")
                        continue
                    internal_channel_id = channel_row[0]
                    
                    if isinstance(latest_date, str):
                        latest_date = datetime.strptime(latest_date, "%Y-%m-%d %H:%M:%S")
                    latest_date_rfc3339 = latest_date.strftime("%Y-%m-%dT%H:%M:%SZ")
                    logger.info(f"Searching for videos published after {latest_date_rfc3339} for channel {yt_channel_id}")
                    test_vid_list = get_vid_json_list_from_channel(yt_channel_id, latest_date_rfc3339, api_key,logger)
                    logger.info(f"API returned {len(test_vid_list)} pages of results for channel {yt_channel_id}")
                    videos_processed = 0
                    videos_inserted = 0
                    for page in test_vid_list:
                        for vid in page['items']:
                            if vid['id']['kind'] == 'youtube#video':
                                videos_processed += 1
                                yt_vid_id = vid['id']['videoId']
                                snippet = vid['snippet']
                                title = snippet['title']
                                description = snippet['description']
                                publishtime = snippet['publishedAt']
                                insert_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                                
                                # Insert into vid_table and get the internal vid_id
                                # Use DO UPDATE instead of DO NOTHING so we always get the id back
                                cur.execute(insert_query_vid_table, (internal_channel_id, yt_vid_id, insert_time))
                                vid_row = cur.fetchone()
                                
                                # If RETURNING didn't work (shouldn't happen with DO UPDATE), get id manually
                                if not vid_row:
                                    cur.execute(get_vid_id_query, (yt_vid_id,))
                                    vid_row = cur.fetchone()
                                
                                if vid_row:
                                    internal_vid_id = vid_row[0]
                                    # Check if vid_data already exists, if not, insert it
                                    cur.execute(check_vid_data_exists_query, (internal_vid_id,))
                                    exists = cur.fetchone()[0]
                                    if exists == 0:
                                        # Insert into vid_data_table using internal vid_id
                                        cur.execute(insert_query_vid_data, (internal_vid_id, title, description, publishtime, json.dumps(snippet), insert_time))
                                        videos_inserted += 1
                                        logger.info(f"Inserted new video data: {yt_vid_id} - {title[:50]}")
                                    else:
                                        logger.debug(f"Video {yt_vid_id} already has data in vid_data_table")
                                else:
                                    logger.warning(f"Could not get vid_id for {yt_vid_id}")
                    logger.info(f"Channel {yt_channel_id}: Processed {videos_processed} videos, inserted {videos_inserted} new vid_data records")
                    if videos_processed == 0:
                        logger.warning(f"Channel {yt_channel_id}: No videos were processed - check if API returned any results")
                    conn.commit()
                    logger.info(f"Committed changes for channel {yt_channel_id}")
                except Exception as e:
                    logger.error(f"Error for {yt_channel_id}: {e}")
                    if "API quota likely exhausted" in str(e):
                        logger.info("Stopping video updates due to quota limit.")
                        conn.commit()
                        raise
                    conn.rollback()

def get_transcripts_to_be_pulled(db_params):
    sql_script = """
    SELECT DISTINCT v.yt_vid_id as vid_id
    FROM vid_table v
    WHERE v.id NOT IN (SELECT DISTINCT vid_id FROM vid_transcript_table);
    """
    with psycopg2.connect(**db_params) as conn:
        return pd.read_sql_query(sql_script, conn)

def insert_new_transcripts(db_params, logger:logging.Logger):
    needed_transcript_vid_id_df = get_transcripts_to_be_pulled(db_params)
    
    # Get internal vid_id for the external yt_vid_id
    get_vid_internal_id_query = sql.SQL("""
    SELECT id FROM vid_table WHERE yt_vid_id = %s
    """)
    
    # Get all existing transcript start times for a video (batch check)
    get_existing_starts_query = sql.SQL("""
    SELECT start FROM vid_transcript_table WHERE vid_id = %s
    """)
    
    insert_query = sql.SQL("""
    INSERT INTO vid_transcript_table(vid_id, text, start, duration, text_formatted, word_count, cum_word_count, insert_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """)

    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cur:
            for yt_vid_id in needed_transcript_vid_id_df['vid_id']:
                logger.info(f"Processing transcript for {yt_vid_id}")
                try:
                    # Get internal vid_id
                    cur.execute(get_vid_internal_id_query, (yt_vid_id,))
                    vid_row = cur.fetchone()
                    if not vid_row:
                        logger.warning(f"Video {yt_vid_id} not found in database, skipping")
                        continue
                    internal_vid_id = vid_row[0]
                    
                    time.sleep(30)
                    logger.info(f"Fetching transcript for {yt_vid_id}...")
                    transcript_info_list = YouTubeTranscriptApi().fetch(yt_vid_id,languages=['en'])#get_transcript(yt_vid_id)
                    
                    # Get all existing start times for this video in one query (much faster)
                    cur.execute(get_existing_starts_query, (internal_vid_id,))
                    existing_starts = {row[0] for row in cur.fetchall()}
                    logger.debug(f"Video {yt_vid_id} already has {len(existing_starts)} transcript lines in database")
                    
                    # Process all transcript lines
                    cum_word_count = 0
                    lines_to_insert = []
                    insert_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                    
                    for transcript_line in transcript_info_list.snippets:
                        text = transcript_line.text#  ['text']
                        start = transcript_line.start #['start']
                        duration = transcript_line.duration #['duration']
                        text_formatted = text.lower()
                        text_formatted = re.sub(r"\[.*\]|\{.*\}", "", text_formatted)
                        text_formatted = re.sub(r'[^\w\s]', "", text_formatted)
                        word_count = len(text_formatted.split())
                        cum_word_count += word_count
                        
                        # Only add to insert list if this line doesn't already exist
                        if start not in existing_starts:
                            lines_to_insert.append((
                                internal_vid_id, text, start, duration, text_formatted, 
                                word_count, cum_word_count, insert_time
                            ))
                    
                    # Batch insert all new lines at once (much faster)
                    if lines_to_insert:
                        logger.info(f"Inserting {len(lines_to_insert)} new transcript lines for video {yt_vid_id}...")
                        cur.executemany(insert_query, lines_to_insert)
                        logger.info(f"Inserted {len(lines_to_insert)} new transcript lines for video {yt_vid_id}")
                    else:
                        logger.info(f"Video {yt_vid_id} already has all transcript lines in database")
                    
                    conn.commit()
                except TranscriptsDisabled:
                    # Video doesn't have transcripts available - insert marker
                    logger.warning(f"Transcripts disabled for video {yt_vid_id}. Inserting 'No transcript available' marker.")
                    # Make sure we have internal_vid_id
                    if 'internal_vid_id' not in locals():
                        cur.execute(get_vid_internal_id_query, (yt_vid_id,))
                        vid_row = cur.fetchone()
                        if not vid_row:
                            logger.warning(f"Video {yt_vid_id} not found in database, skipping")
                            conn.rollback()
                            continue
                        internal_vid_id = vid_row[0]
                    
                    # Check if we already have any transcript entries for this video
                    cur.execute("SELECT COUNT(*) FROM vid_transcript_table WHERE vid_id = %s", (internal_vid_id,))
                    row_count = cur.fetchone()[0]
                    if row_count == 0:
                        # Check if the "No transcript available" marker already exists
                        cur.execute("SELECT COUNT(*) FROM vid_transcript_table WHERE vid_id = %s AND start = %s", (internal_vid_id, -1))
                        marker_exists = cur.fetchone()[0]
                        if marker_exists == 0:
                            cur.execute(insert_query, (internal_vid_id, "No transcript available", -1, 0, "", 0, 0, datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")))
                            logger.info(f"Inserted 'No transcript available' marker for video {yt_vid_id}")
                    conn.commit()
                except IpBlocked:
                    logger.error(f"IP block detected. Stopping transcript updates to avoid further blocks.")
                    conn.commit()  # Commit any successful updates before breaking
                    break  # Exit the loop to prevent further requests
                except Exception as e:
                    logger.error(f"Error for {yt_vid_id}: {e}", exc_info=True)
                    # Check if we already have a transcript entry (using internal vid_id)
                    # Need to get internal_vid_id first if we don't have it
                    if 'internal_vid_id' not in locals():
                        cur.execute(get_vid_internal_id_query, (yt_vid_id,))
                        vid_row = cur.fetchone()
                        if vid_row:
                            internal_vid_id = vid_row[0]
                        else:
                            logger.warning(f"Video {yt_vid_id} not found, cannot insert 'No transcript available' marker")
                            conn.rollback()
                            continue
                    # Check if we already have any transcript entries for this video
                    cur.execute("SELECT COUNT(*) FROM vid_transcript_table WHERE vid_id = %s", (internal_vid_id,))
                    row_count = cur.fetchone()[0]
                    if row_count == 0:
                        # Check if the "No transcript available" marker already exists
                        cur.execute("SELECT COUNT(*) FROM vid_transcript_table WHERE vid_id = %s AND start = %s", (internal_vid_id, -1))
                        marker_exists = cur.fetchone()[0]
                        if marker_exists == 0:
                            cur.execute(insert_query, (internal_vid_id, "No transcript available", -1, 0, "", 0, 0, datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")))
                            logger.info(f"Inserted 'No transcript available' marker for video {yt_vid_id} due to error")
                    conn.commit()

def insert_initial_vid_model_states(db_params, logger:logging.Logger):
    """No-op function - state column removed from vid_model_state table.
    Tasks are now determined dynamically by checking transcripts and scores.
    """
    logger.info("insert_initial_vid_model_states is no longer needed - state column removed.")
    logger.info("Pending tasks are determined by checking which video/model combinations")
    logger.info("have transcripts but no scores in vid_score_table.")
    pass

def reset_pending_states_and_cluster(db_params, logger: logging.Logger):
    """
    No-op function - state column removed. 
    Optionally clusters vid_model_state table if it still exists.
    """
    logger.info("reset_pending_states_and_cluster: state column removed, skipping state reset.")
    
    # Optionally cluster the table if it exists (even without state column)
    # Try to find an appropriate index for clustering
    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cur:
            try:
                # Check if vid_model_state table exists and find an index on vid_id
                cur.execute("""
                    SELECT indexname 
                    FROM pg_indexes 
                    WHERE tablename = 'vid_model_state' 
                    AND indexname LIKE '%vid%'
                    LIMIT 1
                """)
                index_row = cur.fetchone()
                if index_row:
                    index_name = index_row[0]
                    logger.info(f"Attempting to cluster vid_model_state table using index {index_name}.")
                    cur.execute(f"CLUSTER vid_model_state USING {index_name};")
                    conn.commit()
                    logger.info("Successfully clustered vid_model_state table.")
                else:
                    logger.info("No suitable index found for clustering vid_model_state table, skipping.")
            except Exception as e:
                logger.warning(f"Could not cluster table (may not exist or index missing): {e}")
                conn.rollback()

def fill_missing_videos_for_channel(db_params, channel_id, api_key, logger:logging.Logger):
    """
    Find and insert missing videos for a channel by comparing YouTube uploads with database.
    This helps fill gaps that date-based queries might miss.
    """
    logger.info(f"Checking for missing videos in channel {channel_id}")
    
    # Get missing video IDs
    missing_vids = get_missing_video_ids_for_channel(db_params, channel_id, api_key, logger)
    
    if not missing_vids:
        logger.info(f"No missing videos found for channel {channel_id}")
        return
    
    logger.info(f"Found {len(missing_vids)} missing videos for channel {channel_id}. Fetching details...")
    
    # Get video details in batches
    video_details = get_video_details_by_ids(missing_vids, api_key, logger)
    
    if not video_details:
        logger.warning(f"Could not fetch details for missing videos in channel {channel_id}")
        return
    
    # Get internal channel_id
    get_channel_internal_id_query = sql.SQL("""
    SELECT id FROM channel_table WHERE yt_channel_id = %s
    """)
    
    # Insert or get existing id - use DO UPDATE with a real change to ensure RETURNING works
    insert_query_vid_table = sql.SQL("""
    INSERT INTO vid_table(channel_id, yt_vid_id, insert_at)
    VALUES (%s, %s, %s)
    ON CONFLICT (yt_vid_id) DO UPDATE SET insert_at = EXCLUDED.insert_at
    RETURNING id;
    """)
    
    # Fallback: get id if video already exists (in case RETURNING doesn't work on conflict)
    get_vid_id_query = sql.SQL("""
    SELECT id FROM vid_table WHERE yt_vid_id = %s
    """)
    
    check_vid_data_exists_query = sql.SQL("""
    SELECT COUNT(*) FROM vid_data_table WHERE vid_id = %s
    """)
    
    insert_query_vid_data = sql.SQL("""
    INSERT INTO vid_data_table(vid_id, title, description, publishtime, snippet, insert_at)
    VALUES (%s, %s, %s, %s, %s, %s);
    """)
    
    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cur:
            # Get internal channel_id
            cur.execute(get_channel_internal_id_query, (channel_id,))
            channel_row = cur.fetchone()
            if not channel_row:
                logger.warning(f"Channel {channel_id} not found in database, skipping")
                return
            internal_channel_id = channel_row[0]
            
            inserted_count = 0
            for video in video_details:
                try:
                    yt_vid_id = video['id']
                    snippet = video['snippet']
                    title = snippet['title']
                    description = snippet['description']
                    publishtime = snippet['publishedAt']
                    insert_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                    
                    # Insert into vid_table
                    cur.execute(insert_query_vid_table, (internal_channel_id, yt_vid_id, insert_time))
                    vid_row = cur.fetchone()
                    if vid_row:
                        internal_vid_id = vid_row[0]
                        # Check if vid_data already exists
                        cur.execute(check_vid_data_exists_query, (internal_vid_id,))
                        exists = cur.fetchone()[0]
                        if exists == 0:
                            cur.execute(insert_query_vid_data, (internal_vid_id, title, description, publishtime, json.dumps(snippet), insert_time))
                            inserted_count += 1
                except Exception as e:
                    logger.error(f"Error inserting video {video.get('id', 'unknown')}: {e}")
                    continue
            
            conn.commit()
            logger.info(f"Inserted {inserted_count} missing videos for channel {channel_id}")

def fill_missing_videos_for_all_channels(db_params, api_key, logger:logging.Logger):
    """
    Fill missing videos for all channels. This is a gap-filling operation that should
    be run periodically (e.g., weekly) to catch videos that date-based queries missed.
    """
    # Get all channels that have videos
    sql_script = """
    SELECT DISTINCT c.yt_channel_id as channel_id
    FROM channel_table c
    JOIN vid_table v ON v.channel_id = c.id
    """
    
    with psycopg2.connect(**db_params) as conn:
        channel_df = pd.read_sql_query(sql_script, conn)
    
    logger.info(f"Checking for missing videos across {len(channel_df)} channels")
    
    for _, row in channel_df.iterrows():
        channel_id = row['channel_id']
        try:
            fill_missing_videos_for_channel(db_params, channel_id, api_key, logger)
        except Exception as e:
            if "API quota likely exhausted" in str(e):
                logger.info("Quota exhausted. Stopping gap-fill operation.")
                break
            else:
                logger.error(f"Error filling missing videos for channel {channel_id}: {e}")
                continue

def maintain_database(api_key_path, logger:logging.Logger):
    db_params = load_db_config()
    api_key = get_api_key(api_key_path)

    # Add video data for new channels, quit on 403
    try:
        insert_vid_data_for_new_channels(db_params, api_key,logger)
    except Exception as e:
        if "API quota likely exhausted" in str(e):
            logger.info("Quota exhausted. Skipping further API calls and moving to transcripts.")
        else:
            raise

    # Update video data for existing channels (date-based, fast)
    try:
        insert_new_vid_data_for_current_channels(db_params, api_key,logger)
    except Exception as e:
        if "API quota likely exhausted" in str(e):
            logger.info("Quota exhausted. Skipping further API calls and moving to transcripts.")
        else:
            raise

    # Fill missing videos (gap-filling, more comprehensive but slower)
    # This checks for videos that date-based queries might have missed
    # Run this less frequently to avoid quota issues
    try:
        logger.info("Running gap-fill check for missing videos...")
        fill_missing_videos_for_all_channels(db_params, api_key, logger)
    except Exception as e:
        if "API quota likely exhausted" in str(e):
            logger.info("Quota exhausted during gap-fill. Continuing with other maintenance.")
        else:
            logger.warning(f"Error during gap-fill operation: {e}")

    # Add transcripts
    insert_new_transcripts(db_params,logger)

    # Add initial states for new VID_IDs with transcripts
    insert_initial_vid_model_states(db_params,logger)

    # Reset pending states and cluster the table
    reset_pending_states_and_cluster(db_params,logger)

    logger.info("Database maintenance completed.")

if __name__ == "__main__":
    api_key_path = "config/YouTube.txt"
    maintain_database(api_key_path)