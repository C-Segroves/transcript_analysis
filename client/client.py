import psycopg2
import socket
import json
import time
import pickle
import os
from nltk.util import ngrams, pad_sequence, everygrams
from nltk.tokenize import word_tokenize
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_db_config():    
    try:
        with open('config/db_config.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError as e:
        logger.error(f"Database config file not found: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in db_config.json: {e}")
        raise

DB_CONFIG = load_db_config()

def load_models():
    try:
        logger.info("LOADING NLTK MODELS::")
        with open('config/models/models.pkl', 'rb') as f:
            model_dict = pickle.load(f)
        logger.info("DONE LOADING NLTK MODELS::")
        return model_dict
    except FileNotFoundError as e:
        logger.error(f"Models file not found: {e}")
        raise
    except pickle.PickleError as e:
        logger.error(f"Error loading models.pkl: {e}")
        raise

NLTK_MODELS = load_models()

def prep_transcript(transcript, n_gram_size):
    try:
        return list(pad_sequence(word_tokenize(transcript), n_gram_size, pad_left=True, left_pad_symbol="<s>"))
    except Exception as e:
        logger.error(f"Error preparing transcript: {e}")
        return []

def get_transcript(conn, vid_id, n_gram_size):
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
        return prep_transcript(transcript, n_gram_size)
    except psycopg2.Error as e:
        logger.error(f"Database error fetching transcript for VID_ID {vid_id}: {e}")
        return []
    finally:
        cursor.close()

def process_task(transcript_items, model_key, vid_id, n_gram_size):
    try:
        start_time = time.time()
        model = NLTK_MODELS.get(model_key)
        if not model:
            raise KeyError(f"Model key {model_key} not found")
        score = [model.score(item, ngram) for item, ngram in transcript_items if ngram]
        time_taken = time.time() - start_time
        return score, time_taken
    except KeyError as e:
        logger.error(f"Model error: {e}")
        return [], 0
    except Exception as e:
        logger.error(f"Processing error for VID_ID {vid_id}, MODEL_KEY {model_key}: {e}")
        return [], 0

def save_results(conn, vid_id, results):
    try:
        cursor = conn.cursor()
        score_query = """
            INSERT INTO VID_SCORE_TABLE (VID_ID, MODEL_KEY, SCORE, INSERT_AT) 
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
        """
        for result in results:
            cursor.execute(score_query, (vid_id, result['model_key'], result['score']))
        conn.commit()
    except psycopg2.Error as e:
        logger.error(f"Database error saving results for VID_ID {vid_id}: {e}")
    finally:
        cursor.close()

def main():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except psycopg2.Error as e:
        logger.error(f"Failed to connect to database: {e}")
        return

    while True:
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_host = os.environ.get('SERVER_HOST', 'localhost')
            client_socket.connect((server_host, 5000))
            client_socket.settimeout(60)

            # Send initial 'ready' packet
            ready_packet = {'packet_type': 'ready'}
            client_socket.send(json.dumps(ready_packet).encode())
            logger.info("Sent 'ready' packet to server")

            # Receive task
            task_data = json.loads(client_socket.recv(1024).decode())
            logger.info(f"Received task: {task_data}")

            if task_data['packet_type'] == 'task_packet':
                vid_id = task_data['additional_data']['vid_id']
                n_gram_size = task_data['additional_data']['n_gram_size']
                model_keys = task_data['additional_data']['model_keys']
                
                transcript = get_transcript(conn, vid_id, n_gram_size)
                transcript_items = [(item, transcript[j:j+n_gram_size-1]) for j, item in enumerate(transcript[n_gram_size-1:]) if j + n_gram_size - 1 < len(transcript)]

                # Process all models and collect results
                results = []
                if not transcript_items:  # Handle empty transcript
                    logger.info(f"No transcript found for VID_ID {vid_id}. Assigning empty scores.")
                    for model_key in model_keys:
                        results.append({
                            'model_key': model_key,
                            'score': [],  # Empty list for no transcript
                            'time_taken': 0
                        })
                else:
                    for model_key in model_keys:
                        score, time_taken = process_task(transcript_items, model_key, vid_id, n_gram_size)
                        #logger.info(f"Processed task for VID_ID {vid_id}, MODEL_KEY {model_key}, time taken: {time_taken}")
                        results.append({
                            'model_key': model_key,
                            'score': score,
                            'time_taken': time_taken
                        })

                # Save all results to database
                save_results(conn, vid_id, results)

                # Send single batch_complete packet
                batch_complete_packet = {
                    'packet_type': 'batch_complete',
                    'vid_id': vid_id,
                    'results': [{'model_key': r['model_key']} for r in results]  # Server only needs model_keys
                }
                client_socket.send(json.dumps(batch_complete_packet).encode())
                logger.info(f"Sent batch_complete for VID_ID {vid_id} with {len(results)} models")

            elif task_data['packet_type'] == 'no_tasks':
                logger.info("No tasks available from server. Waiting 60s...")
                time.sleep(60)

            client_socket.close()

        except socket.timeout:
            logger.info("No task received (server may be in maintenance). Retrying in 60s...")
            time.sleep(60)
        except socket.error as e:
            logger.error(f"Socket error: {e}. Retrying in 60s...")
            time.sleep(60)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid task data: {e}. Retrying in 5s...")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            time.sleep(5)
        finally:
            try:
                client_socket.close()
            except:
                pass

if __name__ == "__main__":
    main()