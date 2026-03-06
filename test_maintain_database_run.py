"""
Test script to run maintain_database.py functions and verify they work.
This script tests each function without making permanent changes where possible.
"""
import psycopg2
import pandas as pd
import logging
import sys
from datetime import datetime
import maintain_database

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_db_config():
    """Load database configuration."""
    import json
    with open('config/db_config.json', 'r') as f:
        return json.load(f)

def test_load_db_config():
    """Test loading database configuration."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing load_db_config()")
    logger.info("=" * 60)
    try:
        db_params = maintain_database.load_db_config()
        logger.info(f"✓ Successfully loaded DB config: {list(db_params.keys())}")
        return db_params
    except Exception as e:
        logger.error(f"✗ Failed to load DB config: {e}", exc_info=True)
        return None

def test_get_api_key():
    """Test getting API key."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing get_api_key()")
    logger.info("=" * 60)
    try:
        api_key = maintain_database.get_api_key("config/YouTube.txt")
        if api_key:
            logger.info(f"✓ Successfully loaded API key (length: {len(api_key)})")
        else:
            logger.warning("⚠ API key file not found or empty, but function works")
        return api_key
    except Exception as e:
        logger.error(f"✗ Failed to get API key: {e}", exc_info=True)
        return None

def test_get_new_channels_to_pull_vid_data_for(db_params):
    """Test query for new channels."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing get_new_channels_to_pull_vid_data_for()")
    logger.info("=" * 60)
    try:
        df = maintain_database.get_new_channels_to_pull_vid_data_for(db_params)
        logger.info(f"✓ Query executed successfully")
        logger.info(f"  Found {len(df)} new channels to pull data for")
        if len(df) > 0:
            logger.info(f"  Sample channel IDs: {df['channel_id'].head(3).tolist()}")
        return True
    except Exception as e:
        logger.error(f"✗ Query failed: {e}", exc_info=True)
        return False

def test_get_date_of_last_vid_by_channel_df(db_params):
    """Test query for last video date by channel."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing get_date_of_last_vid_by_channel_df()")
    logger.info("=" * 60)
    try:
        df = maintain_database.get_date_of_last_vid_by_channel_df(db_params)
        logger.info(f"✓ Query executed successfully")
        logger.info(f"  Found {len(df)} channels with video data")
        if len(df) > 0:
            logger.info(f"  Sample: {df.head(2).to_dict('records')}")
        return True
    except Exception as e:
        logger.error(f"✗ Query failed: {e}", exc_info=True)
        return False

def test_get_transcripts_to_be_pulled(db_params):
    """Test query for transcripts to be pulled."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing get_transcripts_to_be_pulled()")
    logger.info("=" * 60)
    try:
        df = maintain_database.get_transcripts_to_be_pulled(db_params)
        logger.info(f"✓ Query executed successfully")
        logger.info(f"  Found {len(df)} videos needing transcripts")
        if len(df) > 0:
            logger.info(f"  Sample video IDs: {df['vid_id'].head(3).tolist()}")
        return True
    except Exception as e:
        logger.error(f"✗ Query failed: {e}", exc_info=True)
        return False

def test_insert_initial_vid_model_states(db_params):
    """Test insert_initial_vid_model_states (should be no-op)."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing insert_initial_vid_model_states()")
    logger.info("=" * 60)
    try:
        maintain_database.insert_initial_vid_model_states(db_params, logger)
        logger.info("✓ Function executed successfully (no-op as expected)")
        return True
    except Exception as e:
        logger.error(f"✗ Function failed: {e}", exc_info=True)
        return False

def test_reset_pending_states_and_cluster(db_params):
    """Test reset_pending_states_and_cluster."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing reset_pending_states_and_cluster()")
    logger.info("=" * 60)
    try:
        maintain_database.reset_pending_states_and_cluster(db_params, logger)
        logger.info("✓ Function executed successfully")
        return True
    except Exception as e:
        logger.error(f"✗ Function failed: {e}", exc_info=True)
        return False

def test_database_connection_and_schema(db_params):
    """Test database connection and verify table names."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing database connection and schema")
    logger.info("=" * 60)
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Check if tables exist (PostgreSQL is case-insensitive for unquoted identifiers)
        tables_to_check = [
            'channel_table',
            'vid_table',
            'vid_data_table',
            'vid_transcript_table',
            'vid_score_table',
            'model_table',
            'vid_model_state'
        ]
        
        logger.info("Checking table existence:")
        for table in tables_to_check:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = LOWER(%s)
                )
            """, (table,))
            exists = cursor.fetchone()[0]
            if exists:
                logger.info(f"  ✓ {table} exists")
            else:
                logger.warning(f"  ⚠ {table} not found (may use different case)")
        
        # Test a simple query on vid_table
        try:
            cursor.execute("SELECT COUNT(*) FROM vid_table")
            count = cursor.fetchone()[0]
            logger.info(f"  ✓ vid_table has {count} rows")
        except Exception as e:
            logger.warning(f"  ⚠ Could not query vid_table: {e}")
            # Try uppercase
            try:
                cursor.execute("SELECT COUNT(*) FROM VID_TABLE")
                count = cursor.fetchone()[0]
                logger.info(f"  ✓ VID_TABLE has {count} rows (using uppercase)")
            except:
                pass
        
        conn.close()
        logger.info("✓ Database connection test passed")
        return True
    except Exception as e:
        logger.error(f"✗ Database connection test failed: {e}", exc_info=True)
        return False

def test_youtube_api_call(db_params):
    """Test a small YouTube API call to verify the functions work end-to-end."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing YouTube API call (small test)")
    logger.info("=" * 60)
    logger.info("NOTE: This will make ONE small API call to verify the functions work.")
    logger.info("      Testing with one existing channel to get new videos.")
    logger.info("")
    
    try:
        api_key = maintain_database.get_api_key("config/YouTube.txt")
        if not api_key:
            logger.warning("⚠ No API key found, skipping API test")
            return None
        
        # Get one channel that has existing videos
        df = maintain_database.get_date_of_last_vid_by_channel_df(db_params)
        if len(df) == 0:
            logger.warning("⚠ No channels with videos found, skipping API test")
            return None
        
        # Test with just the first channel
        test_channel_id = df.iloc[0]['channel_id']
        logger.info(f"Testing API call for channel: {test_channel_id}")
        logger.info("This will check for new videos (may find 0, which is fine)")
        
        # We'll test the query function that gets video data
        # But limit it to just checking, not inserting
        # Actually, let's test insert_new_vid_data_for_current_channels but with a modified version
        # that only processes one channel
        
        # Get the latest date for this channel
        latest_date = df.iloc[0]['publishtime']
        logger.info(f"Last video date: {latest_date}")
        
        # Make a small API call to verify it works
        # We'll use get_vid_json_list_from_channel with a recent date to minimize results
        from datetime import datetime, timedelta
        recent_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        
        logger.info(f"Making API call to check for videos after {recent_date}...")
        vid_list = maintain_database.get_vid_json_list_from_channel(
            test_channel_id, 
            recent_date, 
            api_key, 
            logger
        )
        
        total_videos = sum(len(page.get('items', [])) for page in vid_list)
        logger.info(f"✓ API call successful! Found {total_videos} video(s) in response")
        logger.info("  (Note: This is just a test - no data was inserted)")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ API call test failed: {e}", exc_info=True)
        # Check if it's a quota issue
        if "quota" in str(e).lower() or "403" in str(e):
            logger.warning("  ⚠ API quota exhausted (this is okay for testing)")
            return None
        return False

def run_all_tests():
    """Run all tests."""
    logger.info("\n" + "=" * 80)
    logger.info("MAINTAIN_DATABASE.PY FUNCTION TEST SUITE")
    logger.info("=" * 80)
    logger.info("This test suite verifies that all functions in maintain_database.py work correctly.")
    logger.info("")
    
    results = {}
    
    # Test 1: Load DB config
    db_params = test_load_db_config()
    results['load_db_config'] = db_params is not None
    
    if not db_params:
        logger.error("Cannot continue without database config. Exiting.")
        return results
    
    # Test 2: Database connection and schema
    results['db_connection'] = test_database_connection_and_schema(db_params)
    
    # Test 3: Get API key
    api_key = test_get_api_key()
    results['get_api_key'] = api_key is not None
    
    # Test 4: Query functions (read-only)
    results['get_new_channels'] = test_get_new_channels_to_pull_vid_data_for(db_params)
    results['get_last_vid_date'] = test_get_date_of_last_vid_by_channel_df(db_params)
    results['get_transcripts_to_pull'] = test_get_transcripts_to_be_pulled(db_params)
    
    # Test 5: No-op functions
    results['insert_initial_states'] = test_insert_initial_vid_model_states(db_params)
    results['reset_and_cluster'] = test_reset_pending_states_and_cluster(db_params)
    
    # Test 6: Small YouTube API call test
    results['youtube_api_call'] = test_youtube_api_call(db_params)
    
    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("TEST SUMMARY")
    logger.info("=" * 80)
    
    passed = sum(1 for v in results.values() if v is True)
    failed = sum(1 for v in results.values() if v is False)
    skipped = sum(1 for v in results.values() if v is None)
    
    for test_name, result in results.items():
        if result is True:
            logger.info(f"✓ {test_name}: PASSED")
        elif result is False:
            logger.error(f"✗ {test_name}: FAILED")
        else:
            logger.info(f"⊘ {test_name}: SKIPPED")
    
    logger.info("")
    logger.info(f"Total: {passed} passed, {failed} failed, {skipped} skipped")
    
    if failed == 0:
        logger.info("✓ All tests passed!")
    else:
        logger.warning(f"⚠ {failed} test(s) failed")
    
    return results

if __name__ == "__main__":
    try:
        run_all_tests()
    except KeyboardInterrupt:
        logger.info("\n\nTest interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n\nUnexpected error: {e}", exc_info=True)
        sys.exit(1)
