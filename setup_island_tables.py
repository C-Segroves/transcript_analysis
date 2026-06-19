# setup_island_tables.py
"""
Create the tables that back the per-model island pipeline:

  island_table       one row per island found for a (vid_id, model_id):
                     start_index / end_index are positions in that model's
                     per-word smoothed score sequence (as returned by
                     find_islands on the smoothed score array).

  island_task_table  work tracking, one row per (vid_id, model_id) that has a
                     score array. status moves pending -> in_progress -> done
                     (or -> error). DB-coordinated workers claim rows from here
                     with SELECT ... FOR UPDATE SKIP LOCKED.

Both follow the same conventions as create_new_tables.py: SERIAL id primary
keys, integer foreign keys to vid_table(id) / model_table(id), insert_at
timestamps.

Usage:
    python setup_island_tables.py            # create tables + indexes (idempotent)
    python setup_island_tables.py --refresh  # also seed pending tasks from vid_score_table
    python setup_island_tables.py --drop     # DROP and recreate (destroys island data)
"""

import argparse
import json
import os

import psycopg2
from psycopg2.extras import execute_values


def load_db_config(config_path=None):
    path = config_path or os.path.join(os.path.dirname(__file__), "config", "db_config.json")
    with open(path, "r") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

CREATE_ISLAND_TABLE = """
CREATE TABLE IF NOT EXISTS island_table (
    id SERIAL PRIMARY KEY,
    vid_id INTEGER NOT NULL,
    model_id INTEGER NOT NULL,
    start_index INTEGER NOT NULL,
    end_index INTEGER NOT NULL,
    insert_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (vid_id) REFERENCES vid_table(id),
    FOREIGN KEY (model_id) REFERENCES model_table(id),
    UNIQUE (vid_id, model_id, start_index, end_index),
    CHECK (start_index <= end_index)
);
"""

CREATE_ISLAND_TASK_TABLE = """
CREATE TABLE IF NOT EXISTS island_task_table (
    id SERIAL PRIMARY KEY,
    vid_id INTEGER NOT NULL,
    model_id INTEGER NOT NULL,
    status VARCHAR(12) NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    claimed_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    insert_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (vid_id) REFERENCES vid_table(id),
    FOREIGN KEY (model_id) REFERENCES model_table(id),
    UNIQUE (vid_id, model_id)
);
"""

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_island_vid_id ON island_table (vid_id);",
    "CREATE INDEX IF NOT EXISTS idx_island_model_id ON island_table (model_id);",
    "CREATE INDEX IF NOT EXISTS idx_island_vid_model ON island_table (vid_id, model_id);",
    "CREATE INDEX IF NOT EXISTS idx_island_task_status ON island_task_table (status);",
    "CREATE INDEX IF NOT EXISTS idx_island_task_vid_model ON island_task_table (vid_id, model_id);",
]

DROP_TABLES = [
    "DROP TABLE IF EXISTS island_table;",
    "DROP TABLE IF EXISTS island_task_table;",
]


# ---------------------------------------------------------------------------
# Task seeding
# ---------------------------------------------------------------------------

def refresh_tasks(conn, batch_size=50000, logger=None):
    """
    Seed a pending island task for every (vid_id, model_id) in vid_score_table.

    Done in committed batches using keyset pagination over the
    (vid_id, model_id) index, so:
      * each batch is its own small transaction (no giant 128M-row transaction),
      * it's restartable -- it resumes after the highest (vid_id, model_id)
        already in island_task_table, so re-running continues where it left off,
      * memory stays bounded.

    Pairs with empty score arrays are seeded too; the worker handles them as
    0-island no-ops, which keeps the seed an index-only scan (no heap lookups).

    Re-running after more scoring correctly tops up: it scans the whole table in
    key order (ON CONFLICT skips pairs that already have a task). It does NOT
    "resume from the max key", because model-major scoring adds a new model's
    rows across the entire video range -- those sort below the old max key and a
    resume-from-max would miss them.

    Returns the number of scored pairs scanned this run.
    """
    def log(msg):
        if logger:
            logger.info(msg)
        else:
            print(msg)

    last_vid, last_model = -1, -1
    total = 0
    while True:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT vs.vid_id, vs.model_id
                FROM vid_score_table vs
                WHERE (vs.vid_id, vs.model_id) > (%s, %s)
                  AND cardinality(vs.score) > 0
                ORDER BY vs.vid_id, vs.model_id
                LIMIT %s
                """,
                (last_vid, last_model, batch_size),
            )
            page = cur.fetchall()
            if not page:
                break
            execute_values(
                cur,
                "INSERT INTO island_task_table (vid_id, model_id, status) VALUES %s "
                "ON CONFLICT (vid_id, model_id) DO NOTHING",
                [(v, m, "pending") for (v, m) in page],
                template="(%s, %s, %s)",
            )
        conn.commit()
        total += len(page)
        last_vid, last_model = page[-1]
        log(f"Seeded {total:,} island task(s) (through vid_id={last_vid})...")

    log(f"Island-task seeding complete: {total:,} pair(s) processed this run.")
    return total


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

def ensure_tables(conn):
    """Create island_table and island_task_table (and indexes) if absent. Idempotent."""
    with conn.cursor() as cur:
        cur.execute(CREATE_ISLAND_TABLE)
        cur.execute(CREATE_ISLAND_TASK_TABLE)
        for stmt in CREATE_INDEXES:
            cur.execute(stmt)
    conn.commit()


def setup(db_params, drop=False, do_refresh=False):
    conn = psycopg2.connect(**db_params)
    try:
        if drop:
            with conn.cursor() as cur:
                for stmt in DROP_TABLES:
                    cur.execute(stmt)
            conn.commit()
            print("Dropped existing island tables.")
        ensure_tables(conn)
        print("island_table and island_task_table are ready.")

        if do_refresh:
            added = refresh_tasks(conn)
            print(f"Seeded {added} new pending island task(s) from vid_score_table.")
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Create island_table and island_task_table.")
    parser.add_argument("--config", default=None, help="Path to db_config.json")
    parser.add_argument("--drop", action="store_true", help="Drop and recreate the island tables (destroys island data)")
    parser.add_argument("--refresh", action="store_true", help="Seed pending tasks from vid_score_table after creating tables")
    args = parser.parse_args()

    db_params = load_db_config(args.config)
    setup(db_params, drop=args.drop, do_refresh=args.refresh)


if __name__ == "__main__":
    main()
