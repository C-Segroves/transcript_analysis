
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
from nltk.util import ngrams, pad_sequence, everygrams
from nltk.tokenize import word_tokenize

from async_client import BaseClient

model_dict = {}



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
    # Ensure the directory exists
    #os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

    logger = logging.getLogger('ProcessingClient')
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

def generate_client_init_packet():
    init_packet={"packet_type":"init_packet","additional_data":{}}
    return init_packet

def generate_task_request_packet():
    request_packet={'packet_type':'task_request','additional_data':{}}
    return request_packet

def get_transcript(vid_id, n_gram_size,conn=None):
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
        output=prep_transcript(transcript, n_gram_size)
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

def handle_task_packet(db_pool,packet,logger):
    log_pool_stats(db_pool, logger)

    results = []

    vid_id=packet.get('additional_data').get('vid_id')
    model_keys=packet.get('additional_data').get('model_keys')
    n_gram_size=packet.get('additional_data').get('n_gram_size')

    # Manually acquire connection
    conn = db_pool.getconn()
    logger.debug(f"Acquired connection for VID_ID {vid_id}: {id(conn)}")

    try:

        transcript = get_transcript(vid_id, n_gram_size,conn)
        transcript_items = [(item, transcript[j:j+n_gram_size-1]) for j, item in enumerate(transcript[n_gram_size-1:]) if j + n_gram_size - 1 < len(transcript)]

        if not transcript_items:
            logger.info(f"No transcript found for VID_ID {vid_id}. Assigning empty scores.")
            for model_key in model_keys:
                results.append({
                    'model_key': model_key,
                    'score': [],
                    'time_taken': 0
                })
        else:
            for model_key in model_keys:
                score, time_taken = process_task(transcript_items, model_key, vid_id, n_gram_size,logger,conn)
                results.append({
                    'model_key': model_key,
                    'score': score,
                    'time_taken': time_taken
                })
        
        save_results(vid_id, results,conn)
    
    except Exception as e:
        logger.error(f"Error processing task for VID_ID {vid_id}: {e}")
        # Ensure connection is in a good state before returning
        conn.rollback()
    
    finally:
        # Always return the connection to the pool
        logger.debug(f"Returning connection for VID_ID {vid_id}: {id(conn)}")
        db_pool.putconn(conn)
        # Op

    results_packet=generate_results_packet(vid_id,results)

    return results_packet

def generate_results_packet(vid_id,results):
    batch_complete_packet = {
                    'packet_type': 'results',
                    'vid_id': vid_id,
                    'results': [{'model_key': r['model_key']} for r in results]
                }
    return batch_complete_packet

def process_task(transcript_items, model_key, vid_id, n_gram_size,logger,conn=None):
    try:
        start_time = time.time()
        
        # Check if the model is already in model_dict, if not, attempt to load it
        models_to_load = []
        if model_key not in model_dict:
            models_to_load.append(model_key)
        
        if models_to_load:
            loaded_models = load_model_from_db(conn, models_to_load, logger)
            if not loaded_models or model_key not in loaded_models:
                logger.error(f"Skipping processing for model {model_key} due to load failure")
                return [], 0
            # Update model_dict with all loaded models
            model_dict.update(loaded_models)
        
        model = model_dict[model_key]
        score = [model.score(item, ngram) for item, ngram in transcript_items if ngram]
        time_taken = time.time() - start_time
        return score, time_taken
    except Exception as e:
        logger.error(f"Processing error for VID_ID {vid_id}, MODEL_KEY {model_key}: {e}")
        return [], 0
    #finally:
    #    logger.info(f"Processing completed for VID_ID {vid_id}, MODEL_KEY {model_key}. Time taken: {time_taken:.2f} seconds")

def load_model_from_db(conn, model_keys, logger):
    """Load multiple models from the model_table in PostgreSQL with error handling."""
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
        # Convert model_keys list to a tuple for psycopg2
        cursor.execute(query, (tuple(model_keys),))
        results = cursor.fetchall()
        
        if not results:
            logger.error(f"No models found for keys: {model_keys}")
            return loaded_models
        
        for model_key, model_data in results:
            try:
                # Convert memoryview to bytes if necessary
                if isinstance(model_data, memoryview):
                    model_data = model_data.tobytes()
                    logger.info(f"Converted memoryview to bytes for {model_key}")
                elif not isinstance(model_data, bytes):
                    logger.error(f"Model data for {model_key} is not bytes or memoryview: {type(model_data)}")
                    continue
                
                if not model_data:
                    logger.error(f"Model data for {model_key} is empty")
                    continue
                
                # Deserialize the model
                model = pickle.loads(model_data)
                if model is None:
                    logger.error(f"Deserialized model for {model_key} is None")
                    continue
                
                loaded_models[model_key] = model
                logger.info(f"Successfully loaded NLTK model {model_key} from database. Type: {type(model)}")
            
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
    except Exception as e:
        logger.error(f"Unexpected error loading models {model_keys}: {e}")
        return loaded_models
    finally:
        try:
            cursor.close()
        except:
            pass

def save_results(vid_id, results,conn=None):
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

def generate_shutdown_packet(machine_name,reason):
    shut_down_packet={'packet_type':'client_shutdown','additional_data':{'machine_name':machine_name,'reason':reason}}
    return shut_down_packet

def log_pool_stats(db_pool, logger):
    logger.info(f"Connection pool stats - min: {db_pool.minconn}, max: {db_pool.maxconn}, "
                f"open: {len(db_pool._used)}, free: {len(db_pool._pool)}")

class ProcessingClient(BaseClient):
    def __init__(self,host,port,logger,config):
        super().__init__(host, port,logger,config)
        #self.db_path=config['path_to_server']
        self.db_pool=db_pool
    
    async def handle_error(self):
        self.logger.info('Attempting to reconnect...')
        await self.reconnect()
  
    async def process_received_data(self, packet):
        packet_type=packet['packet_type']
        logger.info(f'Processing packet of type: {packet_type}')
        try:
            match packet_type:
                case    'task_packet':
                    logger.info(f'Received task packet.')
                    results_packet=handle_task_packet(self.db_pool,packet,logger)

                    await self.send_data(results_packet)
                    num_models=len(results_packet['results'])
                    logger.info(f'Processing complete. Sending task results packet with {num_models} results.')
                case    'client_acknowledgement':
                    task_request_packet=generate_task_request_packet()
                    await self.send_data(task_request_packet)
                    pass
                case    'received_processed_task':
                    task_request_packet=generate_task_request_packet()
                    await self.send_data(task_request_packet)
                case 'shutdown_request':
                    await self.graceful_shutdown()
                case    _:
                    self.logger.error(f'Client: Received {packet_type} packet')
                    pass
        except Exception as e:
            self.logger.error(f'Error while processing packets. {e}')
            await self.handle_error()

    async def initialize_client(self):
        self.config = load_config()
        self.machine_name=socket.gethostname()
        self.task_time_tracker_last_time=time.time()

        init_packet=generate_client_init_packet()
        await self.send_data(init_packet)

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
                if attempt < max_retries-1:
                    self.logger.info(f'Retrying in {retry_delay} seconds...')
                    await asyncio.sleep(retry_delay)
                else:
                    self.logger.error('Max retries reached. Unable to reconnect.')

    async def run(self):
        try:
            await self.connect()
            receive_task = asyncio.create_task(self.receive_data())
            input_task = asyncio.create_task(self.initialize_client())
            
            await asyncio.gather(receive_task, input_task)
        except asyncio.CancelledError:
            self.logger.info("Tasks cancelled. Shutting down.")
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")
        finally:
            if hasattr(self, 'writer') and self.writer:
                self.writer.close()
                await self.writer.wait_closed()

async def graceful_shutdown(self):
    self.logger.info('Received shutdown request. Initiatin graceful shutdown.')
    shutdown_packet=generate_shutdown_packet(self.machine_name,'Received shutdown request')
    await self.send_data(shutdown_packet)


if __name__ == "__main__":
    # Read server info from the file
    config=load_config()
    print(config)
    logger = setup_logger('')

    db_pool = pool.ThreadedConnectionPool(1, 20, **DB_CONFIG) 
    server_host = os.environ.get('SERVER_HOST', 'localhost')
    logger.info(f"Server host: {server_host}")
    
    client = ProcessingClient(server_host , 5000,logger,config)
    asyncio.run(client.run())