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

# Insert a pending task for every (vid_id, model_id) that has a non-empty score
# array and does not already have a task row. Existing tasks are left untouched,
# so completed/in-progress work is never reset.
REFRESH_TASKS_SQL = """
INSERT INTO island_task_table (vid_id, model_id, status)
SELECT vs.vid_id, vs.model_id, 'pending'
FROM vid_score_table vs
WHERE vs.score IS NOT NULL
  AND COALESCE(array_length(vs.score, 1), 0) > 0
ON CONFLICT (vid_id, model_id) DO NOTHING;
"""


def refresh_tasks(conn):
    """Seed pending island tasks from vid_score_table. Returns rows added."""
    with conn.cursor() as cur:
        cur.execute(REFRESH_TASKS_SQL)
        added = cur.rowcount
    conn.commit()
    return added


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
