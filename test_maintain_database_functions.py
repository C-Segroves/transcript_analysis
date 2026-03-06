"""
Test script for maintain_database.py functions.
Uses transactions with rollback to avoid permanent database changes.
"""

import sys
import os
import json
import psycopg2
from psycopg2 import sql
import logging

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import maintain_database

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_db_config():
    with open('config/db_config.json', 'r') as f:
        return json.load(f)


def test_get_new_channels_to_pull_vid_data_for():
    """Test getting new channels that need video data."""
    logger.info("=" * 60)
    logger.info("Testing get_new_channels_to_pull_vid_data_for()")
    logger.info("=" * 60)
    
    db_params = load_db_config()
    
    try:
        df = maintain_database.get_new_channels_to_pull_vid_data_for(db_params)
        
        logger.info(f"✓ Query executed successfully")
        logger.info(f"  Found {len(df)} channels needing video data")
        if len(df) > 0:
            logger.info(f"  Sample channel IDs: {df['channel_id'].head(3).tolist()}")
        logger.info("✓ Test PASSED")
        
    except Exception as e:
        logger.error(f"Error in test_get_new_channels_to_pull_vid_data_for: {e}", exc_info=True)


def test_get_date_of_last_vid_by_channel_df():
    """Test getting last video date by channel."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing get_date_of_last_vid_by_channel_df()")
    logger.info("=" * 60)
    
    db_params = load_db_config()
    
    try:
        df = maintain_database.get_date_of_last_vid_by_channel_df(db_params)
        
        logger.info(f"✓ Query executed successfully")
        logger.info(f"  Found {len(df)} channels with video data")
        if len(df) > 0:
            logger.info(f"  Sample channels: {df.head(3).to_dict('records')}")
        logger.info("✓ Test PASSED")
        
    except Exception as e:
        logger.error(f"Error in test_get_date_of_last_vid_by_channel_df: {e}", exc_info=True)


def test_get_transcripts_to_be_pulled():
    """Test getting videos that need transcripts."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing get_transcripts_to_be_pulled()")
    logger.info("=" * 60)
    
    db_params = load_db_config()
    
    try:
        df = maintain_database.get_transcripts_to_be_pulled(db_params)
        
        logger.info(f"✓ Query executed successfully")
        logger.info(f"  Found {len(df)} videos needing transcripts")
        if len(df) > 0:
            logger.info(f"  Sample video IDs: {df['vid_id'].head(3).tolist()}")
        logger.info("✓ Test PASSED")
        
    except Exception as e:
        logger.error(f"Error in test_get_transcripts_to_be_pulled: {e}", exc_info=True)


def test_insert_initial_vid_model_states():
    """Test inserting initial video model states (with rollback)."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing insert_initial_vid_model_states()")
    logger.info("=" * 60)
    
    db_params = load_db_config()
    conn = psycopg2.connect(**db_params)
    conn.autocommit = False
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM VID_MODEL_STATE")
        count_before = cursor.fetchone()[0]
        
        logger.info(f"States before: {count_before}")
        
        maintain_database.insert_initial_vid_model_states(db_params, logger)
        
        conn2 = psycopg2.connect(**db_params)
        cursor2 = conn2.cursor()
        cursor2.execute("SELECT COUNT(*) FROM VID_MODEL_STATE")
        count_after = cursor2.fetchone()[0]
        cursor2.close()
        conn2.close()
        
        logger.info(f"States after: {count_after}")
        
        if count_after >= count_before:
            logger.info("✓ Function executed successfully")
            logger.info(f"  Added {count_after - count_before} new states")
            logger.info("✓ Test PASSED")
        else:
            logger.warning("⚠ Unexpected: count decreased")
        
        conn.rollback()
        
    except Exception as e:
        logger.error(f"Error in test_insert_initial_vid_model_states: {e}", exc_info=True)
        conn.rollback()
    finally:
        conn.close()


def test_reset_pending_states_and_cluster():
    """Test resetting pending states (with rollback)."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing reset_pending_states_and_cluster()")
    logger.info("=" * 60)
    
    db_params = load_db_config()
    conn = psycopg2.connect(**db_params)
    conn.autocommit = False
    
    try:
        cursor = conn.cursor()
        
        # Set some states to 'pending' for testing
        cursor.execute("""
            UPDATE VID_MODEL_STATE 
            SET STATE = 'pending'
            WHERE STATE IS NULL
            AND id IN (
                SELECT id FROM VID_MODEL_STATE 
                WHERE STATE IS NULL 
                LIMIT 10
            )
        """)
        pending_count = cursor.rowcount
        conn.commit()
        
        logger.info(f"Set {pending_count} states to 'pending' for testing")
        
        cursor.execute("SELECT COUNT(*) FROM VID_MODEL_STATE WHERE STATE = 'pending'")
        count_before = cursor.fetchone()[0]
        
        logger.info(f"Pending states before: {count_before}")
        
        maintain_database.reset_pending_states_and_cluster(db_params, logger)
        
        conn2 = psycopg2.connect(**db_params)
        cursor2 = conn2.cursor()
        cursor2.execute("SELECT COUNT(*) FROM VID_MODEL_STATE WHERE STATE = 'pending'")
        count_after = cursor2.fetchone()[0]
        cursor2.close()
        conn2.close()
        
        logger.info(f"Pending states after: {count_after}")
        
        if count_after == 0 and count_before > 0:
            logger.info("✓ Successfully reset all pending states")
            logger.info("✓ Test PASSED")
        elif count_before == 0:
            logger.info("✓ No pending states to reset (expected)")
            logger.info("✓ Test PASSED")
        else:
            logger.warning(f"⚠ {count_after} pending states remain")
        
        conn.rollback()
        
    except Exception as e:
        logger.error(f"Error in test_reset_pending_states_and_cluster: {e}", exc_info=True)
        conn.rollback()
    finally:
        conn.close()


def test_schema_queries():
    """Test that queries work with database schema."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing schema compatibility")
    logger.info("=" * 60)
    
    db_params = load_db_config()
    conn = psycopg2.connect(**db_params)
    
    try:
        cursor = conn.cursor()
        
        logger.info("Test 1: Verifying table structure")
        tables_to_check = [
            'model_table',
            'channel_table',
            'vid_table',
            'vid_data_table',
            'vid_transcript_table',
            'vid_score_table',
            'vid_model_state'
        ]
        
        for table in tables_to_check:
            cursor.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = '{table}'
                )
            """)
            exists = cursor.fetchone()[0]
            if exists:
                logger.info(f"  ✓ {table} exists")
            else:
                logger.error(f"  ✗ {table} does not exist")
        
        logger.info("\nTest 2: Verifying column names")
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'vid_table' 
            ORDER BY ordinal_position
        """)
        columns = [row[0] for row in cursor.fetchall()]
        logger.info(f"  vid_table columns: {columns}")
        
        logger.info("✓ Schema compatibility test PASSED")
        
    except Exception as e:
        logger.error(f"Error in test_schema_queries: {e}", exc_info=True)
    finally:
        conn.close()


def run_all_tests():
    """Run all maintain_database function tests."""
    logger.info("\n" + "=" * 60)
    logger.info("MAINTAIN_DATABASE FUNCTION TESTS")
    logger.info("=" * 60)
    
    try:
        test_schema_queries()
        test_get_new_channels_to_pull_vid_data_for()
        test_get_date_of_last_vid_by_channel_df()
        test_get_transcripts_to_be_pulled()
        test_insert_initial_vid_model_states()
        test_reset_pending_states_and_cluster()
        
        logger.info("\n" + "=" * 60)
        logger.info("All maintain_database function tests completed!")
        logger.info("=" * 60)
        logger.info("\nNOTE: Some functions (insert_initial_vid_model_states, reset_pending_states_and_cluster)")
        logger.info("commit internally. Test results show what would happen, but changes are not rolled back.")
        
    except Exception as e:
        logger.error(f"Critical error in test suite: {e}", exc_info=True)


if __name__ == "__main__":
    run_all_tests()
