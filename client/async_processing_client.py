import asyncio
import json
import os
import platform
import logging
import pickle
import time
import socket
import multiprocessing as mp
from psycopg2 import pool
import psycopg2
import sys
import signal

import platform

import task_processor as tp

from aiomultiprocess import Pool

def check_platform_and_import_wmi():
    print(platform.system())
    if platform.system() == "Windows":
        try:
            import wmi
            print("WMI library loaded successfully.")
        except ImportError as e:
            print(f"Failed to import WMI: {e}")
    else:
        print("WMI is not required on non-Windows platforms.")



import psutil

from nltk.util import ngrams, pad_sequence, everygrams
from nltk.tokenize import word_tokenize

from async_client import BaseClient

model_dict = {}
current_task = None

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

def setup_logger(log_file_path):
    logger = logging.getLogger('ProcessingClient')
    logger.setLevel(logging.INFO)#logging.DEBUG#logging.INFO
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

def get_cpu_temp_linux():
    try:
        # psutil.sensors_temperatures() returns a dictionary of temperature sensors
        temps = psutil.sensors_temperatures()
        if "coretemp" in temps:  # 'coretemp' is common for CPU temperature
            for entry in temps["coretemp"]:
                if entry.label == "Package id 0":  # CPU package temperature
                    return entry.current
        elif "cpu-thermal" in temps:  # Some systems use 'cpu-thermal'
            return temps["cpu-thermal"][0].current
        else:
            return "CPU temperature sensor not found."
    except Exception as e:
        return f"Error reading CPU temperature: {e}"

def get_cpu_temp_windows_wmi():
    try:
        w = wmi.WMI(namespace="root\OpenHardwareMonitor")
        temperature_info = w.Sensor()
        for sensor in temperature_info:
            if sensor.SensorType == "Temperature":
                return sensor.Value  # Return the first temperature value
    except Exception as e:
        return f"Error reading CPU temperature: {e}"

def get_cpu_temp():
    system = platform.system()
    if system == "Linux":
        return get_cpu_temp_linux()
    elif system == "Windows":
        return get_cpu_temp_windows_wmi()
    else:
        return "Unsupported OS"

def generate_client_init_packet(machine_name):
    return {"packet_type": "init_packet", "additional_data": {"machine_name": machine_name}}

def generate_task_request_packet():
    return {'packet_type': 'task_request', 'additional_data': {}}

def get_pending_tasks(logger, batch_size=100, n_gram_size=4, conn=None):
    try:
        logger.info('Attempting to get pending tasks from database.')
        cursor = conn.cursor()
        get_vid_id_query = "SELECT VID_ID FROM VID_MODEL_STATE WHERE STATE IS NULL LIMIT 1"
        cursor.execute(get_vid_id_query)
        vid_ids = [row[0] for row in cursor.fetchall()]
        if not vid_ids:
            logger.info('No pending tasks found in database.')
            return None
        vid_id = vid_ids[0]

        logger.info(f'VID_ID {vid_id} has been selected for processing.')
        
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
        logger.info(f'Pending task created:')
        return pending_task
    except psycopg2.Error as e:
        logger.error(f"Database error fetching transcript for VID_ID {vid_id}: {e}")
        if "current transaction is aborted" in str(e).lower():
            logger.critical("Transaction aborted detected. Aborting client.")
            conn.rollback()
            sys.exit(1)
        conn.rollback()
        return []
    finally:
        cursor.close()

def get_transcript(vid_id, n_gram_size, conn=None):
    try:
        cursor = conn.cursor()
        query = """
            SELECT TEXT 
            FROM VID_TRANSCRIPT_TABLE 
            WHERE VID_ID = %s AND WORD_COUNT > 0 
            ORDER BY CUM_WORD_COUNT
        """
        cursor.execute(query, (vid_id,))
        transcript_bits = [str(row[0]) for row in cursor.fetchall()]
        transcript = ' '.join(transcript_bits)
        output = prep_transcript(transcript, n_gram_size)
        return output
    except psycopg2.Error as e:
        logger.error(f"Database error fetching transcript for VID_ID {vid_id}: {e}")
        if "current transaction is aborted" in str(e).lower():
            logger.critical("Transaction aborted detected. Aborting client.")
            conn.rollback()
            sys.exit(1)
        conn.rollback()
        return []
    finally:
        cursor.close()

def prep_transcript(transcript, n_gram_size):
    try:
        return list(pad_sequence(word_tokenize(transcript), n_gram_size, pad_left=True, left_pad_symbol="<s>"))
    except Exception as e:
        logger.error(f"Error preparing transcript: {e}")
        return []


from aiomultiprocess import Pool

async def handle_task_packet(db_pool, packet, logger, times):
    log_pool_stats(db_pool, logger)
    results = []
    conn = db_pool.getconn()

    try:
        # Fetch pending tasks if no packet is provided
        get_packet_start = time.time()
        if packet is None:
            packet = get_pending_tasks(logger, n_gram_size=4, conn=conn, batch_size=100)
        get_packet_time = time.time() - get_packet_start
        times['get_packet_time'] = get_packet_time

        vid_id = packet.get('additional_data').get('vid_id')
        model_keys = packet.get('additional_data').get('model_keys')
        n_gram_size = packet.get('additional_data').get('n_gram_size')
        logger.debug(f"Acquired connection for VID_ID {vid_id}: {id(conn)}")

        # Get transcript and prepare transcript items
        get_transcript_start = time.time()
        transcript = get_transcript(vid_id, n_gram_size, conn)
        get_transcript_time = time.time() - get_transcript_start
        times['get_transcript_time'] = get_transcript_time

        transcript_items = [
            (item, transcript[j:j + n_gram_size - 1])
            for j, item in enumerate(transcript[n_gram_size - 1:])
            if j + n_gram_size - 1 < len(transcript)
        ]

        if not transcript_items:
            logger.info(f"No transcript found for VID_ID {vid_id}. Assigning empty scores.")
            results = [{'model_key': model_key, 'score': [], 'time_taken': 0} for model_key in model_keys]
        else:
            # Load missing models outside of process_task
            missing_models = [key for key in model_keys if key not in model_dict]
            if missing_models:
                logger.info(f"Loading missing models: {missing_models}")
                loaded_models = load_model_from_db(conn, missing_models, logger)
                model_dict.update(loaded_models)

            # Use aiomultiprocess to process tasks in parallel
            async with Pool() as pool:
                tasks = [
                    #process_task(transcript_items, model_key,model, vid_id, n_gram_size, logger)
                    pool.apply(
                        tp.process_task,
                        args=(transcript_items, model_key, model_dict[model_key], vid_id, n_gram_size, logger)
                    )
                    for model_key in model_keys
                ]
                results = await asyncio.gather(*tasks)

        # Save results to the database
        get_save_results_start = time.time()
        save_results(vid_id, results, conn)
        get_save_results_time = time.time() - get_save_results_start
        times['save_results_time'] = get_save_results_time

        get_mark_tasks_complete_start = time.time()
        mark_tasks_complete(vid_id, results, logger, conn)
        get_mark_tasks_complete_time= time.time() - get_mark_tasks_complete_start
        times['mark_tasks_complete_time'] = get_mark_tasks_complete_time

    except Exception as e:
        logger.error(f"Error processing task for VID_ID {vid_id}: {e}")
        conn.rollback()
    finally:
        logger.debug(f"Returning connection for VID_ID {vid_id}: {id(conn)}")
        db_pool.putconn(conn)

    results_packet = generate_results_packet(vid_id, results)
    total_task_time = time.time() - times['start']
    times['total_task_time'] = total_task_time
    logger.info(f"Task processing times: {times}")

    return results_packet

def mark_tasks_complete(vid_id, results, logger, conn=None):
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
        num_models = len(results)
        db_pool.putconn(conn)
        logger.info(f"Task completed for VID_ID: {vid_id} for {num_models} models.")

def generate_results_packet(vid_id, results):
    return {
        'packet_type': 'results',
        'vid_id': vid_id,
        'results': [{'model_key': r['model_key']} for r in results]
    }

def process_task(transcript_items, model_key, vid_id, n_gram_size, logger):
    try:
        start_time = time.time()

        # Ensure the model is loaded
        model = model_dict.get(model_key)
        if not model:
            raise ValueError(f"Model {model_key} not found in model_dict.")

        # Process transcript items and calculate scores
        score = [model.score(item, ngram) for item, ngram in transcript_items if ngram]
        time_taken = time.time() - start_time

        return {'model_key': model_key, 'score': score, 'time_taken': time_taken}
    except Exception as e:
        logger.error(f"Processing error for VID_ID {vid_id}, MODEL_KEY {model_key}: {e}")
        return {'model_key': model_key, 'score': [], 'time_taken': 0}

def load_model_from_db(conn, model_keys, logger):
    loaded_models = {}
    if not model_keys:
        logger.warning("No model keys provided to load.")
        return loaded_models
    
    try:
        cursor = conn.cursor()
        query = """
            SELECT model_key, model_data 
            FROM model_table 
            WHERE model_key IN %s
        """
        cursor.execute(query, (tuple(model_keys),))
        results = cursor.fetchall()
        
        if not results:
            logger.error(f"No models found for keys: {model_keys}")
            return loaded_models
        
        for model_key, model_data in results:
            try:
                if isinstance(model_data, memoryview):
                    model_data = model_data.tobytes()
                    logger.debug(f"Converted memoryview to bytes for {model_key}")
                elif not isinstance(model_data, bytes):
                    logger.error(f"Model data for {model_key} is not bytes or memoryview: {type(model_data)}")
                    continue
                
                if not model_data:
                    logger.error(f"Model data for {model_key} is empty")
                    continue
                
                model = pickle.loads(model_data)
                if model is None:
                    logger.error(f"Deserialized model for {model_key} is None")
                    continue
                
                loaded_models[model_key] = model
                logger.debug(f"Successfully loaded NLTK model {model_key} from database. Type: {type(model)}")
            
            except pickle.PickleError as e:
                logger.error(f"Pickle deserialization error for model {model_key}: {e}")
                continue
            except Exception as e:
                logger.error(f"Unexpected error processing model {model_key}: {e}")
                continue
        
        missing_keys = set(model_keys) - set(loaded_models.keys())
        if missing_keys:
            logger.warning(f"Failed to load models for keys: {missing_keys}")
        
        return loaded_models
    
    except psycopg2.Error as e:
        logger.error(f"Database error fetching models {model_keys}: {e}")
        conn.rollback()
        return loaded_models
    finally:
        cursor.close()

def save_results(vid_id, results, conn=None):
    try:
        cursor = conn.cursor()
        score_query = """
            INSERT INTO VID_SCORE_TABLE (VID_ID, MODEL_KEY, SCORE, INSERT_AT) 
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (VID_ID, MODEL_KEY) 
            DO UPDATE SET SCORE = EXCLUDED.SCORE, INSERT_AT = CURRENT_TIMESTAMP
        """
        for result in results:
            cursor.execute(score_query, (vid_id, result['model_key'], result['score']))
        conn.commit()
    except psycopg2.Error as e:
        logger.error(f"Database error saving results for VID_ID {vid_id}: {e}")
        if "current transaction is aborted" in str(e).lower():
            logger.critical("Transaction aborted detected. Aborting client.")
            conn.rollback()
            sys.exit(1)
        logger.info("Rolling back transaction in save_results")
        conn.rollback()
    finally:
        cursor.close()

def generate_shutdown_packet(machine_name, reason):
    return {'packet_type': 'client_shutdown', 'additional_data': {'machine_name': machine_name, 'reason': reason}}

def log_pool_stats(db_pool, logger):
    logger.info(f"Connection pool stats - min: {db_pool.minconn}, max: {db_pool.maxconn}, "
                f"open: {len(db_pool._used)}, free: {len(db_pool._pool)}")

def generate_health_packet(machine_name):
    cpu_temp=get_cpu_temp()
    cpu_usage = psutil.cpu_percent(interval=1)
    memory_info = psutil.virtual_memory()
    memory_usage = memory_info.percent
    #todo if memory is above 50% dump some models
    if memory_usage > 50:
        logger.warning(f"Memory usage is high: {memory_usage}%")
        model_dict.clear()  # Clear the model cache if memory usage is high

    return {'packet_type': 'health_packet', 'additional_data': {'machine_name': machine_name,'cpu_temp': cpu_temp, 'cpu_usage': cpu_usage, 'memory_usage': memory_usage}}

class ProcessingClient(BaseClient):
    def __init__(self, host,machine_name, port, logger, config):
        super().__init__(host, port, logger, config)
        self.db_pool = db_pool
        self.client_state = 'pause'
        self.task_task = None  # Track the task processing coroutine
        self.machine_name = machine_name
        logger.info(f"Client initialized with machine name: {self.machine_name}")

    async def handle_error(self):
        self.logger.info('Attempting to reconnect...')
        await self.reconnect()

    async def process_received_data(self, packet):
        packet_type = packet['packet_type']
        self.logger.info(f'Processing packet of type: {packet_type} (current state: {self.client_state})')
        try:
            match packet_type:
                case 'play':
                    self.logger.info('Received play packet. Resuming client.')
                    self.client_state = 'play'
                    if not self.task_task or self.task_task.done():
                        self.task_task = asyncio.create_task(self.process_tasks())
                case 'pause':
                    self.logger.info('Received pause packet. Pausing client.')
                    self.client_state = 'pause'
                    if self.task_task and not self.task_task.done():
                        self.task_task.cancel()
                        try:
                            await self.task_task
                        except asyncio.CancelledError:
                            self.logger.info("Task processing cancelled due to pause")
                case 'health_check':
                    health_packet=generate_health_packet(self.machine_name)
                    await self.send_data(health_packet)
                case 'shutdown_request':
                    await self.graceful_shutdown()
                case _:
                    self.logger.error(f'Client: Received unknown packet type: {packet_type}')
        except Exception as e:
            self.logger.error(f'Error while processing packet: {e}')
            await self.handle_error()

    async def initialize_client(self):
        self.config = load_config()
        #self.machine_name = socket.gethostname()
        self.task_time_tracker_last_time = time.time()
        
        init_packet = generate_client_init_packet(self.machine_name)
        self.logger.info(f'Initializing client with machine name: {self.machine_name} : {init_packet}')
        await self.send_data(init_packet)

    async def process_tasks(self):
        while self.client_state == 'play':
            times = {}
            times['start'] = time.time()
            
            # Await the asynchronous handle_task_packet function
            results_packet = await handle_task_packet(self.db_pool, current_task, self.logger, times)
            
            self.logger.info(f'Processing complete. Got results packet with {len(results_packet["results"])} results.')
            
            # Yield to allow other tasks to run
            await asyncio.sleep(0.1)
        
        self.logger.info('Client paused or stopped. Exiting task processing.')

    async def reconnect(self):
        max_retries = 5
        retry_delay = 5
        for attempt in range(max_retries):
            try:
                await self.connect()
                self.logger.info('Reconnected successfully.')
                await self.initialize_client()
                return
            except Exception as e:
                self.logger.error(f'Reconnection attempt {attempt + 1} failed: {e}')
                if attempt < max_retries - 1:
                    self.logger.info(f'Retrying in {retry_delay} seconds...')
                    await asyncio.sleep(retry_delay)
                else:
                    self.logger.error('Max retries reached. Unable to reconnect.')

    async def run(self):
        try:
            await self.connect()
            receive_task = asyncio.create_task(self.receive_data())
            init_task = asyncio.create_task(self.initialize_client())
            await asyncio.gather(receive_task, init_task)
        except asyncio.CancelledError:
            self.logger.info("Tasks cancelled. Shutting down.")
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")
        finally:
            if hasattr(self, 'writer') and self.writer:
                self.writer.close()
                await self.writer.wait_closed()

    async def graceful_shutdown(self):
        self.logger.info('Received shutdown request. Initiating graceful shutdown.')
        if self.task_task and not self.task_task.done():
            self.task_task.cancel()
            try:
                await self.task_task
            except asyncio.CancelledError:
                self.logger.info("Task processing cancelled during shutdown")
        shutdown_packet = generate_shutdown_packet(self.machine_name, 'Received shutdown request')
        await self.send_data(shutdown_packet)
        self.client_state = 'shutdown'

if __name__ == "__main__":
    check_platform_and_import_wmi()
    config = load_config()
    logger = setup_logger('')

    db_pool = pool.ThreadedConnectionPool(1, 20, **DB_CONFIG)
    server_host = os.environ.get('SERVER_HOST', 'localhost')
    machine_name = os.environ.get('MACHINE_NAME', 'localhost')
    logger.info(f"Server host: {server_host}")

    client = ProcessingClient(server_host, machine_name, 5000, logger, config)

    # Signal handling for graceful shutdown
    def handle_signal(signal_number, frame):
        logger.warning(f"Received signal {signal_number}. Initiating shutdown.")
        asyncio.run(client.graceful_shutdown())

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        asyncio.run(client.run())
    except Exception as e:
        logger.critical(f"Unhandled exception in main: {e}", exc_info=True)
    finally:
        logger.info("Client has exited.")