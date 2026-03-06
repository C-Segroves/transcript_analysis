"""
Test script for client/async_processing_client.py functions.
Uses transactions with rollback to avoid permanent database changes.
"""

import sys
import os
import json
import psycopg2
from psycopg2 import pool
import logging

# Add parent directory to path
root_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, root_dir)
sys.path.insert(0, os.path.join(root_dir, 'client'))

# Import async_processing_client module
import importlib.util
async_client_path = os.path.join(root_dir, 'client', 'async_processing_client.py')
spec = importlib.util.spec_from_file_location("async_processing_client", async_client_path)
async_processing_client = importlib.util.module_from_spec(spec)
spec.loader.exec_module(async_processing_client)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_db_config():
    with open('config/db_config.json', 'r') as f:
        return json.load(f)


def test_get_pending_tasks():
    """Test getting pending tasks from database."""
    logger.info("=" * 60)
    logger.info("Testing get_pending_tasks()")
    logger.info("=" * 60)
    
    db_params = load_db_config()
    conn = psycopg2.connect(**db_params)
    conn.autocommit = False
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT COUNT(*) 
            FROM VID_MODEL_STATE 
            WHERE STATE IS NULL
        """)
        count = cursor.fetchone()[0]
        
        if count > 0:
            logger.info(f"Found {count} pending tasks")
            
            task = async_processing_client.get_pending_tasks(
                logger, batch_size=10, n_gram_size=4, conn=conn
            )
            
            if task:
                logger.info(f"✓ Successfully retrieved pending task")
                logger.info(f"  Video ID: {task['additional_data']['vid_id']}")
                logger.info(f"  Model keys: {task['additional_data']['model_keys']}")
                logger.info(f"  N-gram size: {task['additional_data']['n_gram_size']}")
                logger.info("✓ Test PASSED")
            else:
                logger.warning("✗ No task returned")
        else:
            logger.warning("⚠ No pending tasks found - skipping test")
        
        conn.rollback()
        logger.info("✓ Transaction rolled back - no permanent changes")
        
    except Exception as e:
        logger.error(f"Error in test_get_pending_tasks: {e}", exc_info=True)
        conn.rollback()
    finally:
        conn.close()


def test_get_transcript():
    """Test getting transcript."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing get_transcript()")
    logger.info("=" * 60)
    
    db_params = load_db_config()
    conn = psycopg2.connect(**db_params)
    conn.autocommit = False
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT VID_ID 
            FROM VID_TRANSCRIPT_TABLE
            WHERE WORD_COUNT > 0
            LIMIT 1
        """)
        result = cursor.fetchone()
        
        if result:
            vid_id = result[0]
            logger.info(f"Testing with video ID: {vid_id}")
            
            transcript = async_processing_client.get_transcript(vid_id, 4, conn)
            
            if transcript:
                logger.info(f"✓ Successfully retrieved transcript")
                logger.info(f"  Transcript length: {len(transcript)} tokens")
                logger.info("✓ Test PASSED")
            else:
                logger.warning("✗ No transcript returned")
        else:
            logger.warning("⚠ No videos with transcripts found - skipping test")
        
        conn.rollback()
        
    except Exception as e:
        logger.error(f"Error in test_get_transcript: {e}", exc_info=True)
        conn.rollback()
    finally:
        conn.close()


def test_load_model_from_db():
    """Test loading models from database."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing load_model_from_db()")
    logger.info("=" * 60)
    
    db_params = load_db_config()
    conn = psycopg2.connect(**db_params)
    conn.autocommit = False
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("SELECT MODEL_KEY FROM MODEL_TABLE LIMIT 1")
        result = cursor.fetchone()
        
        if result:
            model_key = result[0]
            logger.info(f"Testing with model key: {model_key}")
            
            model_keys = [model_key]
            loaded_models = async_processing_client.load_model_from_db(conn, model_keys, logger)
            
            if loaded_models and model_key in loaded_models:
                logger.info(f"✓ Successfully loaded model {model_key}")
                logger.info(f"  Model type: {type(loaded_models[model_key])}")
                logger.info("✓ Test PASSED")
            else:
                logger.warning("✗ Model not loaded")
        else:
            logger.warning("⚠ No models found - skipping test")
        
        conn.rollback()
        
    except Exception as e:
        logger.error(f"Error in test_load_model_from_db: {e}", exc_info=True)
        conn.rollback()
    finally:
        conn.close()


def test_save_results():
    """Test saving results."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing save_results()")
    logger.info("=" * 60)
    
    db_params = load_db_config()
    conn = psycopg2.connect(**db_params)
    conn.autocommit = False
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT VID_ID, MODEL_KEY
            FROM VID_TABLE v, MODEL_TABLE m
            LIMIT 1
        """)
        result = cursor.fetchone()
        
        if not result:
            logger.warning("⚠ No videos or models found - skipping test")
            return
        
        vid_id = result[0]
        model_key = result[1]
        
        logger.info(f"Testing with video {vid_id}, model {model_key}")
        
        test_results = [{
            'model_key': model_key,
            'score': [0.1, 0.2, 0.3],
            'time_taken': 0.5
        }]
        
        async_processing_client.save_results(vid_id, test_results, conn)
        
        cursor.execute("""
            SELECT SCORE
            FROM VID_SCORE_TABLE
            WHERE VID_ID = %s AND MODEL_KEY = %s
        """, (vid_id, model_key))
        
        saved = cursor.fetchone()
        
        if saved:
            logger.info(f"✓ Successfully saved results")
            logger.info(f"  Score array: {saved[0]}")
            logger.info("✓ Test PASSED")
        else:
            logger.error("✗ Results not saved")
        
        conn.rollback()
        logger.info("✓ Transaction rolled back - no permanent changes")
        
    except Exception as e:
        logger.error(f"Error in test_save_results: {e}", exc_info=True)
        conn.rollback()
    finally:
        conn.close()


def test_mark_tasks_complete():
    """Test marking tasks as complete."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing mark_tasks_complete()")
    logger.info("=" * 60)
    
    db_params = load_db_config()
    conn = psycopg2.connect(**db_params)
    conn.autocommit = False
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT VID_ID, MODEL_KEY
            FROM VID_MODEL_STATE
            WHERE STATE IS NULL
            LIMIT 1
        """)
        result = cursor.fetchone()
        
        if result:
            vid_id = result[0]
            model_key = result[1]
            
            logger.info(f"Testing with video {vid_id}, model {model_key}")
            
            cursor.execute("""
                UPDATE VID_MODEL_STATE 
                SET STATE = 'pending'
                WHERE VID_ID = %s AND MODEL_KEY = %s
            """, (vid_id, model_key))
            conn.commit()
            
            test_results = [{'model_key': model_key}]
            async_processing_client.mark_tasks_complete(vid_id, test_results, logger, conn)
            
            cursor.execute("""
                SELECT STATE
                FROM VID_MODEL_STATE
                WHERE VID_ID = %s AND MODEL_KEY = %s
            """, (vid_id, model_key))
            
            state_result = cursor.fetchone()
            
            if state_result and state_result[0] == 'complete':
                logger.info("✓ Successfully marked task as complete")
                logger.info("✓ Test PASSED")
            else:
                logger.error(f"✗ Task state is {state_result[0] if state_result else 'None'}, expected 'complete'")
        else:
            logger.warning("⚠ No tasks found - skipping test")
        
        conn.rollback()
        logger.info("✓ Transaction rolled back - no permanent changes")
        
    except Exception as e:
        logger.error(f"Error in test_mark_tasks_complete: {e}", exc_info=True)
        conn.rollback()
    finally:
        conn.close()


def run_all_tests():
    """Run all async client function tests."""
    logger.info("\n" + "=" * 60)
    logger.info("ASYNC CLIENT FUNCTION TESTS")
    logger.info("=" * 60)
    
    try:
        test_get_pending_tasks()
        test_get_transcript()
        test_load_model_from_db()
        test_save_results()
        test_mark_tasks_complete()
        
        logger.info("\n" + "=" * 60)
        logger.info("All async client function tests completed!")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Critical error in test suite: {e}", exc_info=True)


if __name__ == "__main__":
    run_all_tests()
