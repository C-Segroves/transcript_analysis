"""
Read-only schema dump. For each table the scoring/islands pipelines touch,
prints columns, an approximate row count, indexes, and constraints, so the code
can be matched exactly to the live database. Makes no changes.

Row counts use pg_class.reltuples (a fast estimate from the last ANALYZE) so
re-running this never full-scans the big tables.

Run:  python inspect_schema.py
"""

import json
import psycopg2


def load_db_config():
    with open("config/db_config.json", "r") as f:
        return json.load(f)


TABLES = [
    "channel_table",
    "vid_table",
    "vid_data_table",
    "vid_transcript_table",
    "model_table",
    "vid_score_table",
    "island_table",
    "island_task_table",
]


def fetch_columns(cur, table):
    cur.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position
        """,
        (table,),
    )
    return cur.fetchall()


def approx_rows(cur, table):
    try:
        cur.execute("SELECT reltuples::bigint FROM pg_class WHERE relname = %s", (table,))
        row = cur.fetchone()
        return row[0] if row else "?"
    except Exception:
        return "?"


def fetch_indexes(cur, table):
    cur.execute(
        """
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE schemaname = 'public' AND tablename = %s
        ORDER BY indexname
        """,
        (table,),
    )
    return cur.fetchall()


def fetch_constraints(cur, table):
    # Primary key / unique / foreign key definitions for the table.
    try:
        cur.execute(
            """
            SELECT conname, pg_get_constraintdef(c.oid) AS def
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE t.relname = %s AND n.nspname = 'public'
            ORDER BY c.contype
            """,
            (table,),
        )
        return cur.fetchall()
    except Exception:
        return []


def main():
    conn = psycopg2.connect(**load_db_config())
    conn.autocommit = True
    cur = conn.cursor()
    for t in TABLES:
        cols = fetch_columns(cur, t)
        if not cols:
            print(f"\n=== {t}: (table not found) ===")
            continue
        n = approx_rows(cur, t)
        print(f"\n=== {t}  (~{n} rows, estimate) ===")
        print("  columns:")
        for name, typ in cols:
            print(f"    {name}: {typ}")

        idxs = fetch_indexes(cur, t)
        print("  indexes:")
        if not idxs:
            print("    (none)")
        for name, definition in idxs:
            # show just the interesting part of the index definition
            short = definition.split(" USING ", 1)[-1] if " USING " in definition else definition
            unique = "UNIQUE " if "UNIQUE INDEX" in definition else ""
            print(f"    {name}: {unique}{short}")

        cons = fetch_constraints(cur, t)
        print("  constraints:")
        if not cons:
            print("    (none)")
        for name, definition in cons:
            print(f"    {name}: {definition}")
    conn.close()


if __name__ == "__main__":
    main()
