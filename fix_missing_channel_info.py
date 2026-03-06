"""
Script to find and update missing channel handles and information from YouTube API.
"""
import psycopg2
import json
import urllib.request
import time
from datetime import datetime
from psycopg2 import sql

def load_db_config():
    with open('config/db_config.json', 'r') as f:
        return json.load(f)

def get_api_key():
    try:
        with open('config/YouTube.txt', 'r') as f:
            return f.read().strip()
    except:
        return None

def get_channel_info_from_youtube(channel_id, api_key):
    """Get channel handle and title from YouTube API."""
    try:
        # Get channel details
        url = f'https://www.googleapis.com/youtube/v3/channels?key={api_key}&id={channel_id}&part=snippet'
        time.sleep(1)  # Rate limiting
        with urllib.request.urlopen(url, timeout=10) as response:
            resp = json.load(response)
            
            if not resp.get('items'):
                print(f"  ⚠ No channel found for ID: {channel_id}")
                return None, None, None
            
            item = resp['items'][0]
            snippet = item.get('snippet', {})
            
            # Get handle from customUrl or try to extract from snippet
            handle = None
            custom_url = snippet.get('customUrl', '')
            if custom_url:
                # customUrl format is usually "@handle" or "handle"
                handle = custom_url.replace('@', '').replace('https://www.youtube.com/', '').replace('c/', '')
            
            # If no customUrl, try to get from snippet (some channels don't have handles)
            if not handle:
                # Channels might not have a handle, that's okay
                pass
            
            title = snippet.get('title', '')
            full_snippet = snippet
            
            return handle, title, full_snippet
            
    except Exception as e:
        print(f"  ✗ Error fetching channel info: {e}")
        return None, None, None

def fix_missing_channel_info():
    """Find and update channels with missing handles or titles."""
    db_params = load_db_config()
    api_key = get_api_key()
    
    if not api_key:
        print("[ERROR] Could not load API key from config/YouTube.txt")
        return
    
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    
    # Find channels with NULL handles or NULL titles in snippet
    cur.execute("""
        SELECT yt_channel_id, channel_handle, channel_snippet
        FROM channel_table 
        WHERE channel_handle IS NULL 
           OR channel_handle = 'nan'
           OR channel_snippet IS NULL
           OR channel_snippet->>'title' IS NULL
    """)
    
    channels_to_fix = cur.fetchall()
    
    if not channels_to_fix:
        print("[OK] No channels with missing information found!")
        conn.close()
        return
    
    print(f"Found {len(channels_to_fix)} channel(s) with missing information:")
    print("=" * 80)
    
    update_query = sql.SQL("""
        UPDATE channel_table 
        SET channel_handle = %s,
            channel_snippet = %s
        WHERE yt_channel_id = %s
    """)
    
    updated_count = 0
    for channel_id, current_handle, current_snippet in channels_to_fix:
        print(f"\nProcessing channel: {channel_id}")
        print(f"  Current handle: {repr(current_handle)}")
        
        # Get info from YouTube
        handle, title, snippet = get_channel_info_from_youtube(channel_id, api_key)
        
        if snippet is None:
            print(f"  ⚠ Could not fetch channel info, skipping")
            continue
        
        # Merge with existing snippet if it exists, otherwise use new one
        if current_snippet:
            try:
                existing_snippet = json.loads(current_snippet) if isinstance(current_snippet, str) else current_snippet
                # Update with new info, keeping existing data
                existing_snippet.update(snippet)
                final_snippet = existing_snippet
            except:
                final_snippet = snippet
        else:
            final_snippet = snippet
        
        # Use existing handle if we have one and new one is None
        final_handle = handle if handle else current_handle
        
        # Update database
        snippet_json = json.dumps(final_snippet)
        cur.execute(update_query, (final_handle, snippet_json, channel_id))
        
        print(f"  [OK] Updated:")
        print(f"    Handle: {repr(final_handle)}")
        print(f"    Title: {final_snippet.get('title', 'N/A')}")
        updated_count += 1
    
    conn.commit()
    conn.close()
    
    print("\n" + "=" * 80)
    print(f"[OK] Updated {updated_count} channel(s)")

if __name__ == "__main__":
    fix_missing_channel_info()
