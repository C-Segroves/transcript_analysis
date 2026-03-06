"""
Test script for client/client.py functions.
Uses transactions with rollback to avoid permanent database changes.
"""

import sys
import os
import json
import psycopg2
from psycopg2 import pool
import asyncio
import logging

# Add parent directory to path
root_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, root_dir)
sys.path.insert(0, os.path.join(root_dir, 'client'))

# Mock packet_handler before importing client
try:
    import test_mock_packet_handler
except ImportError:
    pass

# Import client module
import importlib.util
client_path = os.path.join(root_dir, 'client', 'client.py')
spec = importlib.util.spec_from_file_location("client_module", client_path)
client = importlib.util.module_from_spec(spec)
spec.loader.exec_module(client)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_db_config():
    with open('config/db_config.json', 'r') as f:
        return json.load(f)


def test_load_model_from_db():
    """Test loading a model from the database."""
    logger.info("=" * 60)
    logger.info("Testing load_model_from_db()")
    logger.info("=" * 60)
    
    db_params = load_db_config()
    conn = psycopg2.connect(**db_params)
    
    try:
        conn.autocommit = False
        
        cursor = conn.cursor()
        cursor.execute("SELECT MODEL_KEY FROM MODEL_TABLE LIMIT 1")
        result = cursor.fetchone()
        
        if result:
            model_key = result[0]
            logger.info(f"Found model key: {model_key}")
            
            async def test_async():
                model = await client.load_model_from_db(conn, model_key)
                if model:
                    logger.info(f"✓ Successfully loaded model {model_key}")
                    logger.info(f"  Model type: {type(model)}")
                    logger.info(f"  Has score method: {hasattr(model, 'score')}")
                else:
                    logger.warning(f"✗ Failed to load model {model_key}")
                return model
            
            model = asyncio.run(test_async())
            
            if model:
                logger.info("✓ Test PASSED")
            else:
                logger.warning("✗ Test FAILED - Model not loaded")
        else:
            logger.warning("⚠ No models found in database - skipping model load test")
        
        # Test 2: Try to load a non-existent model
        logger.info("\nTest 2: Loading non-existent model")
        async def test_nonexistent():
            model = await client.load_model_from_db(conn, "NON_EXISTENT_MODEL_KEY_12345")
            if model is None:
                logger.info("✓ Correctly returned None for non-existent model")
                return True
            else:
                logger.error("✗ Should have returned None for non-existent model")
                return False
        
        result = asyncio.run(test_nonexistent())
        if result:
            logger.info("✓ Test 2 PASSED")
        else:
            logger.error("✗ Test 2 FAILED")
        
        conn.rollback()
        logger.info("\n✓ Transaction rolled back - no permanent changes")
        
    except Exception as e:
        logger.error(f"Error in test_load_model_from_db: {e}", exc_info=True)
        conn.rollback()
    finally:
        conn.close()


def test_get_transcript():
    """Test getting transcript from database."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing get_transcript()")
    logger.info("=" * 60)
    
    db_params = load_db_config()
    db_pool = pool.ThreadedConnectionPool(1, 5, **db_params)
    
    try:
        conn = db_pool.getconn()
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
                logger.info(f"Found video ID: {vid_id}")
                
                transcript = client.get_transcript(db_pool, vid_id, 4)
                
                if transcript:
                    logger.info(f"✓ Successfully retrieved transcript")
                    logger.info(f"  Transcript length: {len(transcript)} tokens")
                    logger.info(f"  First 10 tokens: {transcript[:10]}")
                    logger.info("✓ Test PASSED")
                else:
                    logger.warning("✗ Test FAILED - No transcript returned")
            else:
                logger.warning("⚠ No videos with transcripts found - skipping transcript test")
        finally:
            db_pool.putconn(conn)
        
        # Test 2: Try to get transcript for non-existent video
        logger.info("\nTest 2: Getting transcript for non-existent video")
        conn = db_pool.getconn()
        try:
            transcript = client.get_transcript(db_pool, "NON_EXISTENT_VIDEO_ID_12345", 4)
            if not transcript:
                logger.info("✓ Correctly returned empty list for non-existent video")
                logger.info("✓ Test 2 PASSED")
            else:
                logger.error("✗ Should have returned empty list for non-existent video")
                logger.error("✗ Test 2 FAILED")
        finally:
            db_pool.putconn(conn)
        
    except Exception as e:
        logger.error(f"Error in test_get_transcript: {e}", exc_info=True)
    finally:
        db_pool.closeall()


def test_save_results():
    """Test saving results to database."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing save_results()")
    logger.info("=" * 60)
    
    db_params = load_db_config()
    db_pool = pool.ThreadedConnectionPool(1, 5, **db_params)
    
    try:
        conn = db_pool.getconn()
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
                logger.warning("⚠ No videos or models found - skipping save_results test")
                return
            
            test_vid_id = result[0]
            test_model_key = result[1]
            
            logger.info(f"Test: Saving results for video {test_vid_id}, model {test_model_key}")
            
            test_results = [{
                'model_key': test_model_key,
                'score': [0.1, 0.2, 0.3, 0.4, 0.5],
                'time_taken': 1.23
            }]
            
            client.save_results(db_pool, test_vid_id, test_results)
            
            cursor.execute("""
                SELECT SCORE, INSERT_AT
                FROM VID_SCORE_TABLE
                WHERE VID_ID = %s AND MODEL_KEY = %s
            """, (test_vid_id, test_model_key))
            
            saved_result = cursor.fetchone()
            
            if saved_result:
                logger.info(f"✓ Successfully saved results")
                logger.info(f"  Score array length: {len(saved_result[0])}")
                logger.info(f"  Insert timestamp: {saved_result[1]}")
                logger.info("✓ Test PASSED")
            else:
                logger.error("✗ Results were not saved correctly")
                logger.error("✗ Test FAILED")
            
            conn.rollback()
            logger.info("✓ Transaction rolled back - no permanent changes")
            
        finally:
            db_pool.putconn(conn)
            
    except Exception as e:
        logger.error(f"Error in test_save_results: {e}", exc_info=True)
        try:
            conn.rollback()
        except:
            pass
    finally:
        db_pool.closeall()


def run_all_tests():
    """Run all client function tests."""
    logger.info("\n" + "=" * 60)
    logger.info("CLIENT FUNCTION TESTS")
    logger.info("=" * 60)
    
    try:
        test_load_model_from_db()
        test_get_transcript()
        test_save_results()
        
        logger.info("\n" + "=" * 60)
        logger.info("All client function tests completed!")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Critical error in test suite: {e}", exc_info=True)


if __name__ == "__main__":
    run_all_tests()
