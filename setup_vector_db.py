# setup_vector_db.py

import json
import psycopg2

CONFIG_PATH = "config/pg_vector_db_config.json"


def load_config():
    """Load pgvector setup config (Postgres + embedding server endpoints + chunking hints)."""
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError as e:
        print(f"ERROR: Vector DB config file not found: {e}")
        raise
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON in {CONFIG_PATH}: {e}")
        raise


def postgres_params(config):
    """Connection kwargs for psycopg2 from pg_vector_db_config.json."""
    pg = config.get("postgres")
    if not isinstance(pg, dict):
        raise ValueError(f"{CONFIG_PATH} must contain a 'postgres' object with db connection fields.")
    return pg

def execute_postgres_script(db_params, sql_script, description=""):
    connection = None
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()
        cursor.execute(sql_script)
        connection.commit()
        if description:
            print(f"[OK] {description}")
        else:
            print("SQL executed successfully.")
        return True, None
    except (Exception, psycopg2.Error) as error:
        err_msg = str(error)
        print(f"[ERROR] {description}: {err_msg}")
        if connection:
            connection.rollback()
        return False, err_msg
    finally:
        if connection:
            if 'cursor' in locals():
                cursor.close()
            connection.close()


def master_vector_setup():
    config = load_config()
    db_params = postgres_params(config)

    print("=== Vector Database Setup for RAG (Transcript Chunks) ===\n")
    print(f"Connecting to database: {db_params.get('dbname', 'unknown')} on {db_params.get('host', 'localhost')}\n")

    servers = config.get("embedding_servers") or []
    if servers:
        print("Embedding servers (Ollama / llama.cpp) from config:")
        for s in servers:
            host = s.get("host", "?")
            port = s.get("port", "?")
            kind = s.get("kind", "")
            suffix = f" ({kind})" if kind else ""
            print(f"   - {host}:{port}{suffix}")
        print()

    chunking = config.get("chunking") or {}
    tmin = chunking.get("target_tokens_min")
    tmax = chunking.get("target_tokens_max")
    if tmin is not None and tmax is not None:
        print(f"Chunking target (from config): ~{tmin}-{tmax} tokens per chunk\n")

    # 1. Enable pgvector extension
    print("1. Enabling pgvector extension...")
    success, ext_err = execute_postgres_script(
        db_params,
        "CREATE EXTENSION IF NOT EXISTS vector;",
        "pgvector extension enabled"
    )

    if not success:
        low = (ext_err or "").lower()
        if any(
            s in low
            for s in (
                "connection",
                "authentication",
                "password",
                "could not connect",
                "could not translate",
                "timeout",
                "refused",
            )
        ):
            print(
                "\nCould not connect to PostgreSQL. "
                "Check host, port, user, and password in config/pg_vector_db_config.json."
            )
            return
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
        VID_ID VARCHAR(255) NOT NULL,           -- Logical id; video catalog may live on another DB/server (no FK)
        CHUNK_INDEX INTEGER NOT NULL,           -- Sequential order in the video
        TEXT TEXT NOT NULL,                     -- Chunked transcript text (recommended 400-700 tokens)
        EMBEDDING VECTOR(768) NOT NULL,         -- Change to VECTOR(1024) if you use mxbai-embed-large
        START_TIME FLOAT,                       -- Approx. start timestamp of chunk
        END_TIME FLOAT,                         -- Approx. end timestamp
        WORD_COUNT INTEGER,
        TOKEN_COUNT INTEGER,                    -- Helpful for LLM context management
        METADATA JSONB,                         -- Store original segment IDs, etc.
        INSERT_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

        UNIQUE (VID_ID, CHUNK_INDEX)
    );
    """
    ok_table, _ = execute_postgres_script(
        db_params, create_table_sql, "TRANSCRIPT_CHUNK_TABLE created"
    )
    if not ok_table:
        print("\nTable creation failed; fix the error above, then re-run this script.")
        return

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
        ok_idx, _ = execute_postgres_script(db_params, idx_sql, "Index created")
        if not ok_idx:
            print("\nIndex creation failed; fix the error above, then re-run this script.")
            return

    print("\n=== Setup completed successfully! ===")
    print("You can now proceed to write the chunking + embedding script.")
    print("\nRecommended Ollama embedding models:")
    print("   - nomic-embed-text      -> VECTOR(768)  (fast, good quality)")
    print("   - mxbai-embed-large     -> VECTOR(1024) (better quality, slightly slower)")
    print("\nAfter choosing your model, update the `VECTOR(768)` line in this file if needed and re-run it.")


if __name__ == "__main__":
    master_vector_setup()