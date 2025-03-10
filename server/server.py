import psycopg2
from psycopg2 import pool
import socket
import json
import time
from datetime import datetime
import subprocess
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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

def get_pending_tasks(db_pool, batch_size=66, n_gram_size=4):
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

def mark_tasks_complete(db_pool, vid_id, results):
    with db_pool.getconn() as conn:
        cursor = conn.cursor()
        update_query = """
            UPDATE VID_MODEL_STATE 
            SET STATE = 'complete'
            WHERE VID_ID = %s AND MODEL_KEY = %s
        """
        for result in results:
            model_key = result['model_key']
            cursor.execute(update_query, (vid_id, model_key))
            #logger.info(f"Task completed for VID_ID: {vid_id}, MODEL_KEY: {model_key}")
        conn.commit()
        db_pool.putconn(conn)

def run_maintenance():
    logger.info("Running maintenance script...")
    try:
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
    try:
        db_pool = psycopg2.pool.ThreadedConnectionPool(1, 20, **DB_CONFIG)
    except psycopg2.Error as e:
        logger.error(f"Failed to initialize database pool: {e}")
        return

    clear_assigned_tasks(db_pool)
    
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

        if now.hour >= 0 and (last_maintenance_day is None or last_maintenance_day != current_day):
            logger.info("Midnight reached. Stopping task assignment for maintenance.")
            run_maintenance()
            clear_assigned_tasks(db_pool)
            last_maintenance_day = current_day
            logger.info("Resuming task assignment post-maintenance.")
            continue

        try:
            client_socket, addr = server_socket.accept()
            logger.info(f"Connected to {addr}")
            client_socket.settimeout(60)
            
            # Wait for initial 'ready' packet
            initial_data = client_socket.recv(1024).decode()
            initial_packet = json.loads(initial_data)
            if initial_packet.get('packet_type') != 'ready':
                logger.warning(f"Unexpected initial packet from {addr}: {initial_packet}")
                client_socket.close()
                continue
            
            # Send pending task
            task = get_pending_tasks(db_pool)
            if task:
                client_socket.send(json.dumps(task).encode())
            else:
                no_task_response = {
                    'packet_type': 'no_tasks',
                    'message': 'No pending tasks available at this time.'
                }
                client_socket.send(json.dumps(no_task_response).encode())
                logger.info("No pending tasks available. Sent no_tasks response.")
                client_socket.close()
                continue

            # Wait for batch completion
            response_data = client_socket.recv(4096).decode()  # Larger buffer for batch data
            response = json.loads(response_data)
            if response.get('packet_type') == 'batch_complete':
                vid_id = response['vid_id']
                results = response['results']
                mark_tasks_complete(db_pool, vid_id, results)
                logger.info(f"Batch completed for VID_ID: {vid_id} with {len(results)} models")
            else:
                logger.warning(f"Expected batch_complete, got: {response}")

            client_socket.close()

        except socket.timeout:
            logger.info(f"Timeout waiting for client {addr}. Closing connection.")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON from client {addr}: {e}, data: {response_data}")
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