# create_new_tables.py

import psycopg2
from psycopg2 import sql
import json
import os
from datetime import datetime
import pickle

# Load database configuration from file
def load_db_config():
    #print(os.listdir('../'))
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

def create_new_channel_table(db_params):
    sql_script = """
    DROP TABLE IF EXISTS new_channel_table;
    CREATE TABLE new_channel_table (
        id SERIAL PRIMARY KEY,
        external_id VARCHAR(255) UNIQUE NOT NULL,
        handle VARCHAR(255),
        snippet JSON,
        insert_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    execute_postgres_script(db_params, sql_script)

def create_new_vid_table(db_params):
    sql_script = """
    DROP TABLE IF EXISTS new_vid_table;
    CREATE TABLE new_vid_table (
        id SERIAL PRIMARY KEY,
        channel_id INTEGER NOT NULL,
        external_id VARCHAR(255) UNIQUE NOT NULL,
        insert_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (channel_id) REFERENCES new_channel_table(id)
    );
    """
    execute_postgres_script(db_params, sql_script)

def create_new_vid_data_table(db_params):
    sql_script = """
    DROP TABLE IF EXISTS new_vid_data_table;
    CREATE TABLE new_vid_data_table (
        id SERIAL PRIMARY KEY,
        vid_id INTEGER NOT NULL,
        title VARCHAR(255),
        description TEXT,
        publish_time TIMESTAMP,
        snippet JSON,
        insert_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (vid_id) REFERENCES new_vid_table(id)
    );
    """
    execute_postgres_script(db_params, sql_script)

def create_new_vid_transcript_table(db_params):
    sql_script = """
    DROP TABLE IF EXISTS new_vid_transcript_table;
    CREATE TABLE new_vid_transcript_table (
        id SERIAL PRIMARY KEY,
        vid_id INTEGER NOT NULL,
        text TEXT,
        start FLOAT NOT NULL,
        duration FLOAT,
        text_formatted TEXT,
        word_count INTEGER,
        cum_word_count INTEGER,
        insert_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (vid_id) REFERENCES new_vid_table(id),
        UNIQUE (vid_id, start)
    );
    """
    execute_postgres_script(db_params, sql_script)

def create_new_model_table(db_params):
    sql_script = """
    DROP TABLE IF EXISTS new_model_table;
    CREATE TABLE new_model_table (
        id SERIAL PRIMARY KEY,
        external_key VARCHAR(255) UNIQUE NOT NULL,
        model_data BYTEA,
        insert_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    execute_postgres_script(db_params, sql_script)

def create_new_vid_score_table(db_params):
    sql_script = """
    DROP TABLE IF EXISTS new_vid_score_table;
    CREATE TABLE new_vid_score_table (
        id SERIAL PRIMARY KEY,
        vid_id INTEGER NOT NULL,
        model_id INTEGER NOT NULL,
        score FLOAT[],
        insert_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (vid_id) REFERENCES new_vid_table(id),
        FOREIGN KEY (model_id) REFERENCES new_model_table(id),
        UNIQUE (vid_id, model_id)
    );
    """
    execute_postgres_script(db_params, sql_script)

def create_new_vid_score_analytics_table(db_params):
    sql_script = """
    DROP TABLE IF EXISTS new_vid_score_analytics_table;
    CREATE TABLE new_vid_score_analytics_table (
        id SERIAL PRIMARY KEY,
        vid_id INTEGER NOT NULL,
        model_id INTEGER NOT NULL,
        machine_name VARCHAR(255),
        time_taken INTEGER,
        insert_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (vid_id) REFERENCES new_vid_table(id),
        FOREIGN KEY (model_id) REFERENCES new_model_table(id)
    );
    """
    execute_postgres_script(db_params, sql_script)

def create_new_assigned_tasks_table(db_params):
    sql_script = """
    DROP TABLE IF EXISTS new_assigned_tasks;
    CREATE TABLE new_assigned_tasks (
        id SERIAL PRIMARY KEY,
        vid_id INTEGER NOT NULL,
        model_id INTEGER NOT NULL,
        FOREIGN KEY (vid_id) REFERENCES new_vid_table(id),
        FOREIGN KEY (model_id) REFERENCES new_model_table(id),
        UNIQUE (vid_id, model_id)
    );
    """
    execute_postgres_script(db_params, sql_script)

def create_new_vid_model_state_table(db_params):
    sql_script = """
    DROP TABLE IF EXISTS new_vid_model_state;
    CREATE TABLE new_vid_model_state (
        id SERIAL PRIMARY KEY,
        vid_id INTEGER NOT NULL,
        model_id INTEGER NOT NULL,
        state VARCHAR(10),
        FOREIGN KEY (vid_id) REFERENCES new_vid_table(id),
        FOREIGN KEY (model_id) REFERENCES new_model_table(id),
        UNIQUE (vid_id, model_id)
    );
    """
    execute_postgres_script(db_params, sql_script)

def create_new_threshold_table(db_params):
    sql_script = """
    DROP TABLE IF EXISTS new_threshold_table;
    CREATE TABLE new_threshold_table (
        id SERIAL PRIMARY KEY,
        threshold_value FLOAT NOT NULL,
        description VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (threshold_value)
    );
    """
    execute_postgres_script(db_params, sql_script)

def create_new_score_island_table(db_params):
    sql_script = """
    DROP TABLE IF EXISTS new_score_island_table;
    CREATE TABLE new_score_island_table (
        id SERIAL PRIMARY KEY,
        vid_id INTEGER NOT NULL,
        model_id INTEGER NOT NULL,
        threshold_id INTEGER NOT NULL,
        start_index INTEGER NOT NULL,
        end_index INTEGER NOT NULL,
        island_length INTEGER GENERATED ALWAYS AS (end_index - start_index + 1) STORED,
        insert_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (vid_id, model_id) REFERENCES new_vid_score_table (vid_id, model_id),
        FOREIGN KEY (threshold_id) REFERENCES new_threshold_table (id),
        CHECK (start_index <= end_index)
    );
    """
    execute_postgres_script(db_params, sql_script)

def create_new_indexes(db_params):
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_new_transcript_vid_id_wordcount ON new_vid_transcript_table (vid_id, word_count);",
        "CREATE INDEX IF NOT EXISTS idx_new_transcript_vid_id ON new_vid_transcript_table (vid_id);",
        "CREATE INDEX IF NOT EXISTS idx_new_vid_score_vid_id ON new_vid_score_table (vid_id);",
        "CREATE INDEX IF NOT EXISTS idx_new_vid_score_model_id ON new_vid_score_table (model_id);",
        "CREATE INDEX IF NOT EXISTS idx_new_vid_score_analytics_vid_id ON new_vid_score_analytics_table (vid_id);",
        "CREATE INDEX IF NOT EXISTS idx_new_vid_score_analytics_model_id ON new_vid_score_analytics_table (model_id);",
        "CREATE INDEX IF NOT EXISTS idx_new_assigned_tasks_vid_id ON new_assigned_tasks (vid_id);",
        "CREATE INDEX IF NOT EXISTS idx_new_assigned_tasks_model_id ON new_assigned_tasks (model_id);",
        "CREATE INDEX IF NOT EXISTS idx_new_vid_model_state_vid_id ON new_vid_model_state (vid_id);",
        "CREATE INDEX IF NOT EXISTS idx_new_vid_model_state_model_id ON new_vid_model_state (model_id);",
        "CREATE INDEX IF NOT EXISTS idx_new_score_island_vid_id ON new_score_island_table (vid_id);",
        "CREATE INDEX IF NOT EXISTS idx_new_score_island_model_id ON new_score_island_table (model_id);",
        "CREATE INDEX IF NOT EXISTS idx_new_score_island_threshold_id ON new_score_island_table (threshold_id);",
        "CREATE INDEX IF NOT EXISTS idx_new_channel_external_id ON new_channel_table (external_id);",
        "CREATE INDEX IF NOT EXISTS idx_new_vid_external_id ON new_vid_table (external_id);",
        "CREATE INDEX IF NOT EXISTS idx_new_vid_channel_id ON new_vid_table (channel_id);",
        "CREATE INDEX IF NOT EXISTS idx_new_model_external_key ON new_model_table (external_key);",
        "CREATE INDEX IF NOT EXISTS idx_new_threshold_value ON new_threshold_table (threshold_value);"
    ]
    for index_query in indexes:
        execute_postgres_script(db_params, index_query)

def master_new_database_setup():
    db_params = load_db_params()
    print(db_params)
    create_new_channel_table(db_params)
    create_new_vid_table(db_params)
    create_new_vid_data_table(db_params)
    create_new_vid_transcript_table(db_params)
    create_new_model_table(db_params)
    create_new_vid_score_table(db_params)
    create_new_vid_score_analytics_table(db_params)
    create_new_assigned_tasks_table(db_params)
    create_new_vid_model_state_table(db_params)
    create_new_threshold_table(db_params)
    create_new_score_island_table(db_params)
    create_new_indexes(db_params)
    print("New database tables setup completed.")

if __name__ == "__main__":
    master_new_database_setup()