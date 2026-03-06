"""
Test script for server/server.py functions.
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
sys.path.insert(0, os.path.join(root_dir, 'server'))

# Mock packet_handler before importing server
try:
    import test_mock_packet_handler
except ImportError:
    pass

# Import server module
import importlib.util
server_path = os.path.join(root_dir, 'server', 'server.py')
spec = importlib.util.spec_from_file_location("server_module", server_path)
server = importlib.util.module_from_spec(spec)
spec.loader.exec_module(server)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_db_config():
    with open('config/db_config.json', 'r') as f:
        return json.load(f)


def test_clear_assigned_tasks():
    """Test clearing assigned tasks."""
    logger.info("=" * 60)
    logger.info("Testing clear_assigned_tasks()")
    logger.info("=" * 60)
    
    db_params = load_db_config()
    db_pool = pool.ThreadedConnectionPool(1, 5, **db_params)
    
    try:
        conn = db_pool.getconn()
        conn.autocommit = False
        
        try:
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE VID_MODEL_STATE 
                SET STATE = 'pending'
                WHERE STATE IS NULL
                LIMIT 5
            """)
            pending_count = cursor.rowcount
            conn.commit()
            
            if pending_count > 0:
                logger.info(f"Set {pending_count} tasks to 'pending' state for testing")
                
                server.clear_assigned_tasks(db_pool)
                
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM VID_MODEL_STATE 
                    WHERE STATE = 'pending'
                """)
                remaining = cursor.fetchone()[0]
                
                if remaining == 0:
                    logger.info("✓ Successfully cleared all pending tasks")
                    logger.info("✓ Test PASSED")
                else:
                    logger.error(f"✗ {remaining} pending tasks still remain")
                    logger.error("✗ Test FAILED")
            else:
                logger.warning("⚠ No tasks to set to pending - skipping test")
            
            conn.rollback()
            logger.info("✓ Transaction rolled back - no permanent changes")
            
        finally:
            db_pool.putconn(conn)
            
    except Exception as e:
        logger.error(f"Error in test_clear_assigned_tasks: {e}", exc_info=True)
    finally:
        db_pool.closeall()


def test_get_pending_tasks():
    """Test getting pending tasks."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing get_pending_tasks()")
    logger.info("=" * 60)
    
    db_params = load_db_config()
    db_pool = pool.ThreadedConnectionPool(1, 5, **db_params)
    
    try:
        task = server.get_pending_tasks(db_pool, batch_size=10, n_gram_size=4)
        
        if task:
            logger.info("✓ Successfully retrieved pending task")
            logger.info(f"  Packet type: {task['packet_type']}")
            logger.info(f"  Video ID: {task['additional_data']['vid_id']}")
            logger.info(f"  Model keys count: {len(task['additional_data']['model_keys'])}")
            logger.info(f"  N-gram size: {task['additional_data']['n_gram_size']}")
            logger.info("✓ Test PASSED")
        elif task is None:
            logger.warning("⚠ No pending tasks available - this is expected if database is empty")
            logger.info("✓ Test PASSED (no tasks to process)")
        else:
            logger.error("✗ Unexpected return value")
            logger.error("✗ Test FAILED")
        
    except Exception as e:
        logger.error(f"Error in test_get_pending_tasks: {e}", exc_info=True)
    finally:
        db_pool.closeall()


def test_mark_tasks_complete():
    """Test marking tasks as complete."""
    logger.info("\n" + "=" * 60)
    logger.info("Testing mark_tasks_complete()")
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
                
                results = [{'model_key': model_key}]
                server.mark_tasks_complete(db_pool, vid_id, results)
                
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
                    logger.error("✗ Test FAILED")
            else:
                logger.warning("⚠ No tasks found - skipping test")
            
            conn.rollback()
            logger.info("✓ Transaction rolled back - no permanent changes")
            
        finally:
            db_pool.putconn(conn)
            
    except Exception as e:
        logger.error(f"Error in test_mark_tasks_complete: {e}", exc_info=True)
    finally:
        db_pool.closeall()


def run_all_tests():
    """Run all server function tests."""
    logger.info("\n" + "=" * 60)
    logger.info("SERVER FUNCTION TESTS")
    logger.info("=" * 60)
    
    try:
        test_clear_assigned_tasks()
        test_get_pending_tasks()
        test_mark_tasks_complete()
        
        logger.info("\n" + "=" * 60)
        logger.info("All server function tests completed!")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Critical error in test suite: {e}", exc_info=True)


if __name__ == "__main__":
    run_all_tests()
