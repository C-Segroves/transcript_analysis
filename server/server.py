import psycopg2
from psycopg2 import pool
import socket
import json
import time
import os

# Load database configuration from file
def load_db_config():
    print(os.listdir('../'))
    with open('../config/db_config.json', 'r') as f:
        return json.load(f)

DB_CONFIG = load_db_config()

def clear_assigned_tasks(db_pool):
    with db_pool.getconn() as conn:
        print('Server: clearing out old assigned tasks.')
        cursor = conn.cursor()
        cursor.execute("UPDATE VID_MODEL_STATE SET STATE = NULL WHERE STATE = 'pending';")
        conn.commit()

def get_pending_tasks(db_pool, batch_size=1, n_gram_size=4):
    with db_pool.getconn() as conn:
            cursor = conn.cursor()
            get_vid_id_query = "SELECT VID_ID FROM VID_MODEL_STATE WHERE STATE IS NULL LIMIT 1"
            cursor.execute(get_vid_id_query)
            vid_ids = [row[0] for row in cursor.fetchall()]
            if not vid_ids:
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
                SET STATE = 'complete', INSERT_AT = CURRENT_TIMESTAMP 
                WHERE VID_ID = %s AND MODEL_KEY = %s
            """
            cursor.execute(update_query, (vid_id, model_key))
            conn.commit()

def main():
    # Initialize database connection pool
    db_pool = psycopg2.pool.ThreadedConnectionPool(1, 20, **DB_CONFIG)

    clear_assigned_tasks(db_pool)
    
    # Set up socket server
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('0.0.0.0', 5000))
    server_socket.listen(1)
    
    print("Server started, waiting for connections...")
    
    while True:
        client_socket, addr = server_socket.accept()
        print(f"Connected to {addr}")
        
        # Get and send pending task
        task = get_pending_tasks(db_pool)
        if task:
            client_socket.send(json.dumps(task).encode())
            
            # Wait for completion response
            response = json.loads(client_socket.recv(1024).decode())
            if response.get('packet_type') == 'task_complete':
                vid_id = response['vid_id']
                model_key = response['model_key']
                #mark_task_complete(db_pool, vid_id, model_key)
                print(f"Task completed for VID_ID: {vid_id}, MODEL_KEY: {model_key}")
        
        client_socket.close()

if __name__ == "__main__":
    main()