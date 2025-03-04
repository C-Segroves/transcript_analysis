import psycopg2
import socket
import json
import time
import pickle
import os
import multiprocessing as mp
from nltk.util import ngrams, pad_sequence, everygrams
from nltk.tokenize import word_tokenize

# Load database configuration from file
def load_db_config():
    with open('config/db_config.json', 'r') as f:
        return json.load(f)

DB_CONFIG = load_db_config()

def load_models():
    print("LOADING NLTK MODELS::")
    with open('config/models/models.pkl','rb') as f:
        model_dict=pickle.load(f)
    print("DONE LOADING NLTK MODELS::")
    return model_dict

NLTK_MODELS=load_models()

def prep_transcript(transcript,n_gram_size):
    return list(pad_sequence(word_tokenize(transcript), n_gram_size, pad_left=True,left_pad_symbol="<s>"))

def get_transcript(conn, vid_id,n_gram_size):
    cursor = conn.cursor()
    query = """
        SELECT TEXT 
        FROM VID_TRANSCRIPT_TABLE 
        WHERE VID_ID = %s AND WORD_COUNT > 0 
        ORDER BY CUM_WORD_COUNT
    """
    cursor.execute(query, (vid_id,))
    transcript_bits = [str(row[0]) for row in cursor.fetchall()]
    transcript=' '.join(transcript_bits)
    transcript_prepped=prep_transcript(transcript,n_gram_size)
    return transcript_prepped

def process_task(transcript_items,model_key,vid_id,n_gram_size):
    # Dummy processing function - replace with your actual processing logic
    start_time = time.time()
    # Simulate some processing
    score = len(transcript_items) * 0.5  # Dummy score calculation

    model=NLTK_MODELS[model_key]
    score=[model.score(word,ngram) for word,ngram in transcript_items]

    time_taken = time.time() - start_time
    #results_dict={'vid_id':vid_id,'model_key':model_key,'num_n_grams':n_gram_size,'scores':scores,'time_taken':time_taken}
    return score, time_taken

def save_results(conn, vid_id, model_key, score, time_taken):
    cursor = conn.cursor()
    
    # Insert into VID_SCORE_TABLE
    score_query = """
        INSERT INTO VID_SCORE_TABLE (VID_ID, MODEL_KEY, SCORE, INSERT_AT) 
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
    """
    cursor.execute(score_query, (vid_id, model_key, score))
    
    # Insert into VID_SCORE_ANALYTICS_TABLE
    analytics_query = """
        INSERT INTO VID_SCORE_ANALYTICS_TABLE (VID_ID, MODEL_KEY, MACHINE_NAME, TIME_TAKEN, INSERT_AT) 
        VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
    """
    cursor.execute(analytics_query, (vid_id, model_key, socket.gethostname(), time_taken))
    
    conn.commit()

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    
    while True:
        # Connect to server
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_host = os.environ.get('SERVER_HOST', 'localhost')  # Use environment variable
        client_socket.connect((server_host, 5000))
        
        # Receive task
        task_data = json.loads(client_socket.recv(1024).decode())
        print(task_data)
        if task_data['packet_type'] == 'task_packet':
            vid_id = task_data['additional_data']['vid_id']
            n_gram_size=task_data['additional_data']['n_gram_size']
            transcript = get_transcript(conn, vid_id,n_gram_size)
            transcript_items=[(item,transcript[j:j+n_gram_size-1]) for j,item in enumerate(transcript[n_gram_size-1:])]
            model_keys = task_data['additional_data']['model_keys']
            
            for model_key in model_keys:
                # Get transcript and process
                
                score, time_taken = process_task(transcript_items,model_key,vid_id,n_gram_size)
                print(time_taken)
                
                # Save results
                #save_results(conn, vid_id, model_key, score, time_taken)
                
                # Send completion signal
                completion_packet = {
                    'packet_type': 'task_complete',
                    'vid_id': vid_id,
                    'model_key': model_key,
                    'n_gram_size':n_gram_size
                }
                print("Sending completion signal: ",completion_packet)
                client_socket.send(json.dumps(completion_packet).encode())
        
        client_socket.close()
        time.sleep(1)  # Prevent tight loop

if __name__ == "__main__":
    main()