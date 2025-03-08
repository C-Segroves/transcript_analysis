# setup_database.py

import psycopg2
from psycopg2 import sql
import json
import os
from datetime import datetime
import pickle

# Load database configuration (assumes a config file or environment variables in Docker)
# Load database configuration from file
def load_db_config():
    print(os.listdir('../'))
    with open('config/db_config.json', 'r') as f:
        return json.load(f)

def load_db_params():
    db_params = load_db_config()
    return db_params

def execute_postgres_script(db_params, sql_script):
    connection = None
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()
        cursor.execute(sql_script)
        connection.commit()
        print("SQL script executed successfully.")
    except (Exception, psycopg2.Error) as error:
        print(f"Error while connecting to PostgreSQL or executing script: {error}")
        if connection:
            connection.rollback()
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

def create_channel_table(db_params):
    sql_script = """
    DROP TABLE IF EXISTS CHANNEL_TABLE;
    CREATE TABLE CHANNEL_TABLE (
        CHANNEL_HANDLE VARCHAR(255),
        CHANNEL_ID VARCHAR(255) PRIMARY KEY,
        CHANNEL_SNIPPET JSON,
        INSERT_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    execute_postgres_script(db_params, sql_script)

def create_vid_table(db_params):
    sql_script = """
    DROP TABLE IF EXISTS VID_TABLE;
    CREATE TABLE VID_TABLE (
        CHANNEL_ID VARCHAR(255),
        VID_ID VARCHAR(255) PRIMARY KEY,
        INSERT_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (CHANNEL_ID) REFERENCES CHANNEL_TABLE(CHANNEL_ID)
    );
    """
    execute_postgres_script(db_params, sql_script)

def create_vid_data_table(db_params):
    sql_script = """
    DROP TABLE IF EXISTS VID_DATA_TABLE;
    CREATE TABLE VID_DATA_TABLE (
        VID_ID VARCHAR(255) PRIMARY KEY,
        TITLE VARCHAR(255),
        DESCRIPTION TEXT,
        PUBLISHTIME TIMESTAMP,
        SNIPPET JSON,
        INSERT_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (VID_ID) REFERENCES VID_TABLE(VID_ID)
    );
    """
    execute_postgres_script(db_params, sql_script)

def create_vid_transcript_table(db_params):
    sql_script = """
    DROP TABLE IF EXISTS VID_TRANSCRIPT_TABLE;
    CREATE TABLE VID_TRANSCRIPT_TABLE (
        VID_ID VARCHAR(255),
        TEXT TEXT,
        START FLOAT,
        DURATION FLOAT,
        TEXT_FORMATTED TEXT,
        WORD_COUNT INTEGER,
        CUM_WORD_COUNT INTEGER,
        INSERT_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (VID_ID, START),
        FOREIGN KEY (VID_ID) REFERENCES VID_TABLE(VID_ID)
    );
    """
    execute_postgres_script(db_params, sql_script)

def create_model_table(db_params):
    sql_script = """
    DROP TABLE IF EXISTS MODEL_TABLE;
    CREATE TABLE MODEL_TABLE (
        MODEL_KEY VARCHAR(255) PRIMARY KEY,
        MODEL_DATA BYTEA,
        INSERT_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    execute_postgres_script(db_params, sql_script)

def create_score_table(db_params):
    sql_script = """
    DROP TABLE IF EXISTS VID_SCORE_TABLE;
    CREATE TABLE VID_SCORE_TABLE (
        VID_ID VARCHAR(255),
        MODEL_KEY VARCHAR(255),
        SCORE BYTEA,
        INSERT_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (VID_ID, MODEL_KEY),
        FOREIGN KEY (VID_ID) REFERENCES VID_TABLE(VID_ID),
        FOREIGN KEY (MODEL_KEY) REFERENCES MODEL_TABLE(MODEL_KEY)
    );
    """
    execute_postgres_script(db_params, sql_script)

def create_score_analytics_table(db_params):
    sql_script = """
    DROP TABLE IF EXISTS VID_SCORE_ANALYTICS_TABLE;
    CREATE TABLE VID_SCORE_ANALYTICS_TABLE (
        VID_ID VARCHAR(255),
        MODEL_KEY VARCHAR(255),
        MACHINE_NAME VARCHAR(255),
        TIME_TAKEN INTEGER,
        INSERT_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (VID_ID) REFERENCES VID_TABLE(VID_ID),
        FOREIGN KEY (MODEL_KEY) REFERENCES MODEL_TABLE(MODEL_KEY)
    );
    """
    execute_postgres_script(db_params, sql_script)

def create_assigned_tasks_table(db_params):
    sql_script = """
    DROP TABLE IF EXISTS ASSIGNED_TASKS;
    CREATE TABLE ASSIGNED_TASKS (
        VID_ID TEXT,
        MODEL_KEY TEXT,
        PRIMARY KEY (VID_ID, MODEL_KEY),
        FOREIGN KEY (VID_ID) REFERENCES VID_TABLE(VID_ID),
        FOREIGN KEY (MODEL_KEY) REFERENCES MODEL_TABLE(MODEL_KEY)
    );
    """
    execute_postgres_script(db_params, sql_script)

def create_vid_model_state_table(db_params):
    sql_script = """
    DROP TABLE IF EXISTS VID_MODEL_STATE;
    CREATE TABLE VID_MODEL_STATE (
        VID_ID VARCHAR(255),
        MODEL_KEY VARCHAR(255),
        STATE VARCHAR(10),
        PRIMARY KEY (VID_ID, MODEL_KEY),
        FOREIGN KEY (VID_ID) REFERENCES VID_TABLE(VID_ID),
        FOREIGN KEY (MODEL_KEY) REFERENCES MODEL_TABLE(MODEL_KEY)
    );
    """
    execute_postgres_script(db_params, sql_script)

def create_indexes(db_params):
    indexes = [
        "CREATE INDEX IF NOT EXISTS transcript_table_id_wordcount_index ON VID_TRANSCRIPT_TABLE (VID_ID, WORD_COUNT);",
        "CREATE INDEX IF NOT EXISTS idx_vid_score_vid_id ON VID_SCORE_TABLE (VID_ID, MODEL_KEY);",
        "CREATE INDEX IF NOT EXISTS idx_vid_score_analytics ON VID_SCORE_ANALYTICS_TABLE (VID_ID, MODEL_KEY);",
        "CREATE INDEX IF NOT EXISTS idx_assigned_tasks ON ASSIGNED_TASKS (VID_ID, MODEL_KEY);",
        "CREATE INDEX IF NOT EXISTS idx_model_model_key ON MODEL_TABLE (MODEL_KEY);",
        "CREATE INDEX IF NOT EXISTS idx_vid_model_state ON VID_MODEL_STATE (VID_ID, MODEL_KEY);"
    ]
    for index_query in indexes:
        execute_postgres_script(db_params, index_query)

def pull_model_files_dict(model_files_location):
    with open(model_files_location, 'rb') as f:
        models = pickle.load(f)
    return models

def insert_models(db_params, model_files_location):
    model_dict = pull_model_files_dict(model_files_location)

    insert_query = sql.SQL("""
    INSERT INTO MODEL_TABLE(MODEL_KEY, MODEL_DATA, INSERT_AT)
    VALUES (%s, %s, %s)
    ON CONFLICT (MODEL_KEY) DO NOTHING;
    """)
    
    with psycopg2.connect(**db_params) as conn:
        with conn.cursor() as cur:
            for key, value in model_dict.items():
                print(f"Inserting model {key}")
                pickle_data = pickle.dumps(value, pickle.HIGHEST_PROTOCOL)
                cur.execute(insert_query, (key, psycopg2.Binary(pickle_data), datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")))

def master_database_setup():
    db_params = load_db_params()
    create_channel_table(db_params)
    create_vid_table(db_params)
    create_vid_data_table(db_params)
    create_vid_transcript_table(db_params)
    create_model_table(db_params)
    create_score_table(db_params)
    create_score_analytics_table(db_params)
    create_assigned_tasks_table(db_params)

    model_files_location = "config\models\models.pkl"
    # Insert models
    insert_models(db_params, model_files_location)

    create_vid_model_state_table(db_params)
    create_indexes(db_params)
    print("Database setup completed.")

if __name__ == "__main__":
    master_database_setup()