# setup_vector_db.py

import json
import psycopg2
from datetime import datetime

def load_config():
    """Load database config exactly like in your ProcessingServer."""
    try:
        with open('config/db_config.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError as e:
        print(f"ERROR: Database config file not found: {e}")
        raise
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON in db_config.json: {e}")
        raise

def execute_postgres_script(db_params, sql_script, description=""):
    connection = None
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()
        cursor.execute(sql_script)
        connection.commit()
        if description:
            print(f"✓ {description}")
        else:
            print("SQL executed successfully.")
        return True
    except (Exception, psycopg2.Error) as error:
        print(f"✗ Error during {description}: {error}")
        if connection:
            connection.rollback()
        return False
    finally:
        if connection:
            if 'cursor' in locals():
                cursor.close()
            connection.close()


def master_vector_setup():
    db_params = load_config()   # ← Same loading method as your server

    print("=== Vector Database Setup for RAG (Transcript Chunks) ===\n")
    print(f"Connecting to database: {db_params.get('dbname', 'unknown')} on {db_params.get('host', 'localhost')}\n")

    # 1. Enable pgvector extension
    print("1. Enabling pgvector extension...")
    success = execute_postgres_script(
        db_params,
        "CREATE EXTENSION IF NOT EXISTS vector;",
        "pgvector extension enabled"
    )

    if not success:
        print("\nCRITICAL ERROR:")
        print("   The 'vector' extension is not available in your PostgreSQL installation.")
        print("   This usually means you are running a standard 'postgres:17' image without pgvector.")
        print("\nSOLUTION (Recommended):")
        print("   Change your Docker image to:")
        print("       image: pgvector/pgvector:pg17")
        print("   or")
        print("       image: pgvector/pgvector:0.8.2-pg17")
        print("\n   Then restart the container and re-run this script.")
        print("   (Your current setup is using a plain PostgreSQL 17 without the vector extension.)")
        return

    # 2. Create the chunk table for RAG
    print("\n2. Creating TRANSCRIPT_CHUNK_TABLE...")
    create_table_sql = """
    DROP TABLE IF EXISTS TRANSCRIPT_CHUNK_TABLE CASCADE;

    CREATE TABLE TRANSCRIPT_CHUNK_TABLE (
        CHUNK_ID SERIAL PRIMARY KEY,
        VID_ID VARCHAR(255) NOT NULL,
        CHUNK_INDEX INTEGER NOT NULL,           -- Sequential order in the video
        TEXT TEXT NOT NULL,                     -- Chunked transcript text (recommended 400-700 tokens)
        EMBEDDING VECTOR(768) NOT NULL,         -- ← Change to 1024 if you use mxbai-embed-large
        START_TIME FLOAT,                       -- Approx. start timestamp of chunk
        END_TIME FLOAT,                         -- Approx. end timestamp
        WORD_COUNT INTEGER,
        TOKEN_COUNT INTEGER,                    -- Helpful for LLM context management
        METADATA JSONB,                         -- Store original segment IDs, etc.
        INSERT_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

        FOREIGN KEY (VID_ID) REFERENCES VID_TABLE(VID_ID),
        UNIQUE (VID_ID, CHUNK_INDEX)
    );
    """
    execute_postgres_script(db_params, create_table_sql, "TRANSCRIPT_CHUNK_TABLE created")

    # 3. Create useful indexes (especially HNSW for fast similarity search)
    print("\n3. Creating indexes for fast RAG retrieval...")
    indexes = [
        """CREATE INDEX IF NOT EXISTS idx_chunk_embedding_hnsw 
           ON TRANSCRIPT_CHUNK_TABLE 
           USING hnsw (EMBEDDING vector_cosine_ops);""",

        "CREATE INDEX IF NOT EXISTS idx_chunk_vid_id ON TRANSCRIPT_CHUNK_TABLE (VID_ID);",
        "CREATE INDEX IF NOT EXISTS idx_chunk_vid_index ON TRANSCRIPT_CHUNK_TABLE (VID_ID, CHUNK_INDEX);",
        "CREATE INDEX IF NOT EXISTS idx_chunk_metadata ON TRANSCRIPT_CHUNK_TABLE USING GIN (METADATA);"
    ]

    for idx_sql in indexes:
        execute_postgres_script(db_params, idx_sql, "Index created")

    print("\n=== Setup completed successfully! ===")
    print("You can now proceed to write the chunking + embedding script.")
    print("\nRecommended Ollama embedding models:")
    print("   • nomic-embed-text      → VECTOR(768)  (fast, good quality)")
    print("   • mxbai-embed-large     → VECTOR(1024) (better quality, slightly slower)")
    print("\nAfter choosing your model, update the `VECTOR(768)` line in this file if needed and re-run it.")


if __name__ == "__main__":
    master_vector_setup()