import psycopg2
from psycopg2 import pool
import socket
import json
import time
from datetime import datetime
import subprocess
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load database configuration from file
def load_db_config():
    print(os.listdir('../'))
    with open('config/db_config.json', 'r') as f:
        return json.load(f)

DB_CONFIG = load_db_config()

def clear_assigned_tasks(db_pool):
    with db_pool.getconn() as conn:
        logger.info('Server: clearing out old assigned tasks.')
        cursor = conn.cursor()
        cursor.execute("UPDATE VID_MODEL_STATE SET STATE = NULL WHERE STATE = 'pending';")
        conn.commit()
        db_pool.putconn(conn)

def get_pending_tasks(db_pool, batch_size=1, n_gram_size=4):
    with db_pool.getconn() as conn:
        cursor = conn.cursor()
        get_vid_id_query = "SELECT VID_ID FROM VID_MODEL_STATE WHERE STATE IS NULL LIMIT 1"
        cursor.execute(get_vid_id_query)
        vid_ids = [row[0] for row in cursor.fetchall()]
        if not vid_ids:
            db_pool.putconn(conn)
            return None
        vid_id = vid_ids[0]
        
        get_model_keys_query = "SELECT MODEL_KEY FROM VID_MODEL_STATE WHERE VID_ID = %s AND STATE IS NULL LIMIT %s"
        cursor.execute(get_model_keys_query, (vid_id, batch_size))
        model_keys = [row[0] for row in cursor.fetchall()]
        num_model_keys = len(model_keys)
        model_keys = model_keys[0:min(batch_size, num_model_keys)]
        assignments = [(vid_id, model_key) for model_key in model_keys]
        
        assigned_task_query = "UPDATE VID_MODEL_STATE SET STATE = 'pending' WHERE VID_ID=%s AND MODEL_KEY=%s"
        cursor.executemany(assigned_task_query, assignments)
        conn.commit()
        db_pool.putconn(conn)

        pending_task = {
            'packet_type': 'task_packet',
            'additional_data': {
                'vid_id': vid_id,
                'model_keys': model_keys,
                'n_gram_size': n_gram_size
            }
        }
        return pending_task

def mark_task_complete(db_pool, vid_id, model_key):
    with db_pool.getconn() as conn:
        cursor = conn.cursor()
        update_query = """
            UPDATE VID_MODEL_STATE 
            SET STATE = 'complete'
            WHERE VID_ID = %s AND MODEL_KEY = %s
        """
        cursor.execute(update_query, (vid_id, model_key))
        conn.commit()
        db_pool.putconn(conn)

def run_maintenance():
    logger.info("Running maintenance script...")
    try:
        # Adjust path if maintain_database.py is elsewhere
        result = subprocess.run(
            ["python", "maintain_database.py"],
            check=True,
            capture_output=True,
            text=True
        )
        logger.info("Maintenance completed successfully.")
        logger.debug(f"Maintenance output: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Maintenance failed: {e}")
        logger.error(f"Error output: {e.stderr}")
    except Exception as e:
        logger.error(f"Unexpected error during maintenance: {e}")

def main():
    # Initialize database connection pool
    try:
        db_pool = psycopg2.pool.ThreadedConnectionPool(1, 20, **DB_CONFIG)
    except psycopg2.Error as e:
        logger.error(f"Failed to initialize database pool: {e}")
        return

    clear_assigned_tasks(db_pool)
    
    # Set up socket server
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server_socket.bind(('0.0.0.0', 5000))
        server_socket.listen(1)
        logger.info("Server started, waiting for connections...")
    except socket.error as e:
        logger.error(f"Socket binding failed: {e}")
        return
    
    last_maintenance_day = None
    while True:
        now = datetime.now()
        current_day = now.date()

        # Check if it's after midnight and we haven’t run maintenance today
        if now.hour >= 0 and (last_maintenance_day is None or last_maintenance_day != current_day):
            logger.info("Midnight reached. Stopping task assignment for maintenance.")
            run_maintenance()
            clear_assigned_tasks(db_pool)  # Reset pending tasks after maintenance
            last_maintenance_day = current_day
            logger.info("Resuming task assignment post-maintenance.")
            continue  # Skip to next loop iteration to accept clients

        try:
            client_socket, addr = server_socket.accept()
            logger.info(f"Connected to {addr}")
            
            # Get and send pending task
            task = get_pending_tasks(db_pool)
            if task:
                client_socket.send(json.dumps(task).encode())
                
                # Wait for completion response
                response = json.loads(client_socket.recv(1024).decode())
                if response.get('packet_type') == 'task_complete':
                    vid_id = response['vid_id']
                    model_key = response['model_key']
                    mark_task_complete(db_pool, vid_id, model_key)
                    logger.info(f"Task completed for VID_ID: {vid_id}, MODEL_KEY: {model_key}")
            else:
                logger.info("No pending tasks available. Waiting for clients...")
                time.sleep(5)  # Brief pause to avoid tight loop
            
            client_socket.close()

        except json.JSONDecodeError as e:
            logger.error(f"Invalid response from client {addr}: {e}")
        except socket.error as e:
            logger.error(f"Socket error with client {addr}: {e}")
        except psycopg2.Error as e:
            logger.error(f"Database error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error with client {addr}: {e}")
        finally:
            try:
                client_socket.close()
            except:
                pass

if __name__ == "__main__":
    main()