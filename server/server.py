import psycopg2
from psycopg2 import pool
import asyncio
import json
from datetime import datetime
import logging
from maintain_database import maintain_database
from packet_handler.packet_handler import send_packet, receive_packet


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

task_queue = []
assigned_tasks = set()
task_lock = asyncio.Lock()
MAX_CLIENTS = 10
client_semaphore = asyncio.Semaphore(MAX_CLIENTS)

def load_db_config():
    with open('config/db_config.json', 'r') as f:
        return json.load(f)

DB_CONFIG = load_db_config()
db_pool = psycopg2.pool.ThreadedConnectionPool(1, 20, **DB_CONFIG)  # Global pool


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

async def run_maintenance( api_key_path):
    while True:
        now = datetime.utcnow()
        logger.info(f'Time is::{now.hour}:{now.minute}')
        if now.hour == 0 and now.minute == 0:  # Reverted to midnight UTC
            logger.info("Midnight reached. Stopping task assignment for maintenance.")
            logger.info("Running maintenance script...")
            try:
                await asyncio.to_thread(maintain_database, api_key_path, logger)
            except Exception as e:
                logger.error(f"Maintenance failed: {e}")
            await asyncio.sleep(60)
        await asyncio.sleep(30)

async def reload_task_queue(db_pool, batch_size=66, n_gram_size=4,target_amount=20):
    logger.info(f'Attempting to reload task queue.')
    for _ in range(target_amount):
        task_packet=get_pending_tasks(db_pool, batch_size, n_gram_size)
        task_queue.append(task_packet)
        if not task_packet:
            break

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    addr = writer.get_extra_info('peername')
    logger.info(f"Connection from {addr}")
    
    async with client_semaphore:
        try:
            while True:
                packet = await receive_packet(reader, writer, logger=logger)  # Pass writer
                if packet.get('packet_type') != 'ready':
                    logger.error(f"Expected 'ready' packet from {addr}, got {packet}")
                    return
                
                logger.info(f"Received 'ready' from {addr}")
                
                async with task_lock:
                    if len(task_queue)==0:
                        no_tasks_packet = {'packet_type': 'no_tasks'}
                        
                        await send_packet(writer, no_tasks_packet, reader, logger=logger)
                        logger.info(f"Sent 'no_tasks' to {addr} : {datetime.now().hour}")
                        if len(task_queue) < 10:
                            await reload_task_queue(db_pool, batch_size=66, n_gram_size=4,target_amount=20)
                        task_queue_size=len(task_queue)
                        logger.info(f'task_queue is now of size: {task_queue_size}')
                    elif datetime.now().hour == 0:
                        no_tasks_packet = {'packet_type': 'no_tasks'}
                        
                        await send_packet(writer, no_tasks_packet, reader, logger=logger)
                        logger.info(f"Sent 'wait for maintanance' to {addr} : {datetime.now().hour} ")
                    else:
                        task_packet = task_queue.pop(0)
                        """task_packet = {
                            'packet_type': 'task_packet',
                            'additional_data': task
                        }"""
                        await send_packet(writer, task_packet, reader, logger=logger)
                        logger.info(f"Sent task {task_packet['additional_data']['vid_id']} to {addr}")
                        assigned_tasks.add(task_packet['additional_data']['vid_id'])
                        
                        result_packet = await receive_packet(reader, writer, logger=logger)  # Pass writer
                        if result_packet.get('packet_type') == 'batch_complete':
                            logger.info(f"Received batch_complete for {result_packet['vid_id']} from {addr}")
                            await asyncio.to_thread(mark_tasks_complete, db_pool, result_packet['vid_id'], result_packet['results'])
                            assigned_tasks.discard(result_packet['vid_id'])
                        else:
                            logger.error(f"Expected 'batch_complete', got {result_packet}")
                logger.info(f'Attempting to restart com loop with client {addr}.')
        
        except Exception as e:
            logger.error(f"Error handling client {addr}: {e}")
        finally:
            logger.info(f"Closing connection from {addr}")
            writer.close()
            await writer.wait_closed()

async def main():
    try:
        db_pool = psycopg2.pool.ThreadedConnectionPool(1, 20, **DB_CONFIG)
    except psycopg2.Error as e:
        logger.error(f"Failed to initialize database pool: {e}")
        return
    try:
        clear_assigned_tasks(db_pool)
    except Exception as e:
        logger.error(f'Failed to clear assigned tasks:{e}')
        return
    
    if len(task_queue) < 10:
        await reload_task_queue(db_pool, batch_size=66, n_gram_size=4,target_amount=20)

    asyncio.create_task(run_maintenance("/app/config/YouTube.txt"))

    #asyncio.create_task()#TODO add code to refil tasks

    server = await asyncio.start_server(handle_client, '0.0.0.0', 5000)
    logger.info("Server started, waiting for connections...")
    
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        db_pool.closeall()
        logger.info("Database connection pool closed")