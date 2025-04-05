import asyncio
import json

import socket
import logging
import os
import platform
from collections import deque
import threading
import psycopg2
from psycopg2 import pool

from async_server import BaseServer


def load_config():
    try:
        with open('config/db_config.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError as e:
        logger.error(f"Database config file not found: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in db_config.json: {e}")
        raise

DB_CONFIG = load_config()
task_queue = []

def setup_logger(log_file_path):
    # Ensure the directory exists
    #os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

    logger = logging.getLogger('ProcessingServer')
    logger.setLevel(logging.DEBUG)

    # Create console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # Create file handler
    #fh = logging.FileHandler(log_file_path)
    #fh.setLevel(logging.DEBUG)

    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Add formatter to handlers
    ch.setFormatter(formatter)
    #fh.setFormatter(formatter)

    # Add handlers to logger
    logger.addHandler(ch)
    #logger.addHandler(fh)

    return logger

def clear_assigned_tasks(db_pool):
    with db_pool.getconn() as conn:
        logger.info('Server: clearing out old assigned tasks.')
        cursor = conn.cursor()
        cursor.execute("UPDATE VID_MODEL_STATE SET STATE = NULL WHERE STATE = 'pending';")
        conn.commit()
        db_pool.putconn(conn)

def generate_client_acknowledgement_packet():
    init_packet={"packet_type":"client_acknowledgement","additional_data":{}}
    return init_packet

def generate_received_process_task_packet():
    received_process_task_packet={'packet_type':'received_processed_task','additional_data':{}}
    return received_process_task_packet

def mark_tasks_complete(db_pool, vid_id, results,logger):
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
        conn.commit()
        num_models=len(results)
        db_pool.putconn(conn)
        logger.info(f"Task completed for VID_ID: {vid_id} for {num_models} models.")

class ProcessingServer(BaseServer):
    def __init__(self, host, port,logger,config):
        super().__init__(host, port, logger)
        #self.db_path=config['path_to_server']
        self.db_pool=db_pool
        self.task_que= deque()
        self.completed_task_que=deque()

    async def send_data(self, data, writer):
        try:
            json_data = json.dumps(data).encode()
            length = len(json_data)
            writer.write(length.to_bytes(4, 'big'))
            writer.write(json_data)
            await writer.drain()
            #self.logger.info(f"Data sent successfully. Length: {length}, Data: {data}")
        except Exception as e:
            self.logger.error(f"Error sending data: {e}")

    async def process_data(self, packet, writer):
        addr = writer.get_extra_info('peername')
        sock = writer.get_extra_info('socket')
        packet_type=packet['packet_type']

        match packet_type:
            case    'init_packet':
                self.logger.info(f"Received init packet from {addr}")
                ack_packet=generate_client_acknowledgement_packet()
                await self.send_data(ack_packet, writer)
            case    'task_request':
                self.logger.info(f"Received task request from {addr}")
                if len(self.task_que) == 0:
                    logger.info(f'No tasks in queue. Attempting to reload task queue.{db_pool}')
                    self.reload_task_queue(batch_size=66, n_gram_size=4, target_amount=20)
                tasks=self.task_que.popleft()
                await self.send_data(tasks, writer)
            case    'results':
                self.logger.info(f"Received results from {addr}")
                #self.completed_task_que.appendleft(packet)
                received_process_task_packet=generate_received_process_task_packet()
                await asyncio.to_thread(mark_tasks_complete, db_pool, packet['vid_id'], packet['results'],logger)
                await self.send_data(received_process_task_packet,writer)
            case    _:
                self.logger.error(f'Server: Received {packet_type} packet')
                pass

    def reload_task_queue(self, batch_size=66, n_gram_size=4, target_amount=20):
        self.logger.info('Attempting to reload task queue.')
        for _ in range(target_amount):
            task_packet = self.get_pending_tasks(batch_size, n_gram_size)
            self.task_que.append(task_packet)
            if not task_packet:
                break

    def get_pending_tasks(self, batch_size=66, n_gram_size=4):
        with self.db_pool.getconn() as conn:
            self.logger.info('Server: Attempting to get pending tasks from database.')
            cursor = conn.cursor()
            get_vid_id_query = "SELECT VID_ID FROM VID_MODEL_STATE WHERE STATE IS NULL LIMIT 1"
            cursor.execute(get_vid_id_query)
            vid_ids = [row[0] for row in cursor.fetchall()]
            if not vid_ids:
                self.db_pool.putconn(conn)
                return None
            vid_id = vid_ids[0]

            self.logger.info(f'Server: VID_ID {vid_id} has been selected for processing.')
            
            get_model_keys_query = "SELECT MODEL_KEY FROM VID_MODEL_STATE WHERE VID_ID = %s AND STATE IS NULL LIMIT %s"
            cursor.execute(get_model_keys_query, (vid_id, batch_size))
            model_keys = [row[0] for row in cursor.fetchall()]
            num_model_keys = len(model_keys)
            model_keys = model_keys[0:min(batch_size, num_model_keys)]
            assignments = [(vid_id, model_key) for model_key in model_keys]

            self.logger.info(f'Server: VID_ID {vid_id} has been assigned the following model keys: {model_keys}')
            
            assigned_task_query = "UPDATE VID_MODEL_STATE SET STATE = 'pending' WHERE VID_ID=%s AND MODEL_KEY=%s"
            cursor.executemany(assigned_task_query, assignments)
            conn.commit()
            self.db_pool.putconn(conn)

            pending_task = {
                'packet_type': 'task_packet',
                'additional_data': {
                    'vid_id': vid_id,
                    'model_keys': model_keys,
                    'n_gram_size': n_gram_size
                }
            }
            self.logger.info(f'Server: Pending task created: {pending_task}')
            return pending_task

    async def start(self):
        server = await asyncio.start_server(
            self.handle_client, self.host, self.port)
        
        addr = server.sockets[0].getsockname()
        self.logger.info(f'Serving on {addr}')

        # Create tasks
        #db_task = asyncio.create_task(self.db_operations())
        server_task = asyncio.create_task(server.serve_forever())

        try:
            # Run both tasks concurrently
            await asyncio.gather( server_task)
        except asyncio.CancelledError:
            self.logger.info("Server is shutting down...")
        finally:
            #db_task.cancel()
            server_task.cancel()
            self.logger.info("Server shut down complete.")
 
if __name__ == "__main__":

    #Set Host and port number
    host = socket.gethostbyname(socket.gethostname())
    port = 5000

    db_pool = pool.ThreadedConnectionPool(1, 20, **DB_CONFIG)  # Global pool

    #load config file variables
    config=load_config()
    #print(config)

    #Set up logger
    logger = setup_logger('')
    print(logger)

    #initalize server
    server = ProcessingServer(host, port,logger,config)
    print(server)

    #clear out any tasks that didn't get complete
    clear_assigned_tasks(db_pool)
    
    logger.info("Starting ProcessingServer")
    asyncio.run(server.start())