"""
island_worker.py

DB-coordinated worker that finds per-model islands of consecutive high-scoring
words and stores them in island_table.

For each (vid_id, model_id) task:
  1. Claim a pending/error task from island_task_table atomically
     (SELECT ... FOR UPDATE SKIP LOCKED), so any number of workers can run in
     parallel against the same database without doing the same work twice.
  2. Read that model's score array straight from vid_score_table (FLOAT[]).
     Islands don't need the NLTK models, just the scores, so no socket server
     and no 480MB model load is involved.
  3. Gaussian-smooth the scores and run find_islands (same math as
     build_islands_from_scores.py / "island finder 2.ipynb").
  4. Replace island_table rows for that (vid_id, model_id) with the new islands.
  5. Mark the task 'done' (or 'error' with the message, leaving it to be retried).

Scaling: run this image on N machines/containers; each one just claims whatever
is pending. Seed the task table first with:  python setup_island_tables.py --refresh
(or pass --refresh to this worker to seed before processing).

Usage examples:
    python island_worker.py --once                 # drain the queue then exit
    python island_worker.py                         # run forever, polling for work
    python island_worker.py --refresh --once        # seed tasks from scores, then drain
    python island_worker.py --threshold 0.6 --min-length 8
"""

import argparse
import json
import logging
import os
import socket
import sys
import time

import psycopg2

# Reuse the exact smoothing + island-finding math already in the repo.
from build_islands_from_scores import build_smoother, smooth, find_islands

# Defaults mirror build_islands_from_scores.py (island finder 2 settings).
DEFAULT_SCORE_THRESHOLD = 0.6
DEFAULT_MIN_ISLAND_LENGTH = 8
DEFAULT_SMOOTH_SIZE = 10
DEFAULT_SMOOTH_SIGMA = 5
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_POLL_SECONDS = 10


def load_db_config(config_path=None):
    path = config_path or os.path.join(os.path.dirname(__file__), "config", "db_config.json")
    with open(path, "r") as f:
        return json.load(f)


def setup_logger(worker_name):
    logger = logging.getLogger("IslandWorker")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter(f"%(asctime)s - {worker_name} - %(levelname)s - %(message)s"))
        logger.addHandler(h)
    return logger


# ---------------------------------------------------------------------------
# Task claiming / completion
# ---------------------------------------------------------------------------

CLAIM_SQL = """
WITH claimed AS (
    SELECT id
    FROM island_task_table
    WHERE status = 'pending'
       OR (status = 'error' AND attempts < %(max_attempts)s)
    ORDER BY id
    FOR UPDATE SKIP LOCKED
    LIMIT %(batch)s
)
UPDATE island_task_table t
SET status = 'in_progress',
    attempts = t.attempts + 1,
    claimed_at = now(),
    updated_at = now()
FROM claimed
WHERE t.id = claimed.id
RETURNING t.id, t.vid_id, t.model_id;
"""


def claim_tasks(conn, batch, max_attempts):
    """Atomically claim up to `batch` tasks. Returns list of (task_id, vid_id, model_id)."""
    with conn.cursor() as cur:
        cur.execute(CLAIM_SQL, {"batch": batch, "max_attempts": max_attempts})
        rows = cur.fetchall()
    conn.commit()  # release the row locks; claimed rows are now 'in_progress'
    return rows


def get_score_array(conn, vid_id, model_id):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT score FROM vid_score_table WHERE vid_id = %s AND model_id = %s",
            (vid_id, model_id),
        )
        row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return list(row[0])


def store_islands(conn, vid_id, model_id, islands):
    """Replace island_table rows for this (vid_id, model_id) with `islands`."""
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM island_table WHERE vid_id = %s AND model_id = %s",
            (vid_id, model_id),
        )
        if islands:
            cur.executemany(
                "INSERT INTO island_table (vid_id, model_id, start_index, end_index) "
                "VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (vid_id, model_id, start_index, end_index) DO NOTHING",
                [(vid_id, model_id, int(s), int(e)) for (s, e) in islands],
            )


def mark_done(conn, task_id):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE island_task_table SET status = 'done', error = NULL, updated_at = now() WHERE id = %s",
            (task_id,),
        )


def mark_error(conn, task_id, message):
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE island_task_table SET status = 'error', error = %s, updated_at = now() WHERE id = %s",
            (str(message)[:1000], task_id),
        )


# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------

def process_task(conn, task, the_smoother, params, logger):
    """Compute and store islands for one claimed task. Returns number of islands."""
    task_id, vid_id, model_id = task
    try:
        scores = get_score_array(conn, vid_id, model_id)
        if not scores:
            # No usable score array: nothing to do, but the task is "done".
            store_islands(conn, vid_id, model_id, [])
            mark_done(conn, task_id)
            conn.commit()
            logger.info("vid_id=%s model_id=%s: no scores, marked done (0 islands).", vid_id, model_id)
            return 0

        smoothed = smooth(scores, the_smoother, params["smooth_size"])
        islands = find_islands(
            smoothed,
            params["threshold"],
            params["min_length"],
            above_threshold=params["above_threshold"],
        )
        store_islands(conn, vid_id, model_id, islands)
        mark_done(conn, task_id)
        conn.commit()
        logger.info("vid_id=%s model_id=%s: %d island(s) stored.", vid_id, model_id, len(islands))
        return len(islands)
    except Exception as e:
        conn.rollback()
        try:
            mark_error(conn, task_id, e)
            conn.commit()
        except Exception as e2:
            conn.rollback()
            logger.error("Failed to record error for task %s: %s", task_id, e2)
        logger.warning("vid_id=%s model_id=%s failed: %s", vid_id, model_id, e)
        return 0


def run(db_params, params, logger, batch=10, once=False, poll_seconds=DEFAULT_POLL_SECONDS,
        max_attempts=DEFAULT_MAX_ATTEMPTS, refresh_first=False):
    the_smoother = build_smoother(params["smooth_size"], params["smooth_sigma"])
    conn = psycopg2.connect(**db_params)
    total_tasks = 0
    total_islands = 0
    try:
        # Make sure the island tables exist (idempotent), so a fresh machine can
        # run the worker without a separate setup step.
        from setup_island_tables import ensure_tables, refresh_tasks
        ensure_tables(conn)

        if refresh_first:
            added = refresh_tasks(conn, logger=logger)
            logger.info("Island-task seeding pass done (%d pair(s) processed).", added)

        idle_logged = False
        while True:
            tasks = claim_tasks(conn, batch, max_attempts)
            if not tasks:
                if once:
                    logger.info("Queue empty. Processed %d task(s), %d island(s). Exiting (--once).",
                                total_tasks, total_islands)
                    break
                if not idle_logged:
                    logger.info("No pending tasks; polling every %ss...", poll_seconds)
                    idle_logged = True
                time.sleep(poll_seconds)
                continue

            idle_logged = False
            for task in tasks:
                total_islands += process_task(conn, task, the_smoother, params, logger)
                total_tasks += 1
            if total_tasks % 100 == 0:
                logger.info("Progress: %d tasks processed, %d islands stored.", total_tasks, total_islands)
    finally:
        conn.close()
    logger.info("Done. %d task(s) processed, %d island(s) stored.", total_tasks, total_islands)
    return total_tasks, total_islands


def main():
    parser = argparse.ArgumentParser(description="DB-coordinated island worker: find per-model islands and store them.")
    parser.add_argument("--config", default=None, help="Path to db_config.json")
    parser.add_argument("--batch", type=int, default=10, help="Tasks to claim per round")
    parser.add_argument("--once", action="store_true", help="Drain the queue and exit instead of polling forever")
    parser.add_argument("--poll-seconds", type=int, default=DEFAULT_POLL_SECONDS, help="Seconds to wait when no work is available")
    parser.add_argument("--refresh", action="store_true", help="Seed pending tasks from vid_score_table before processing")
    parser.add_argument("--max-attempts", type=int, default=DEFAULT_MAX_ATTEMPTS, help="Max attempts before an errored task is left alone")
    parser.add_argument("--threshold", type=float, default=DEFAULT_SCORE_THRESHOLD, help="Smoothed-score threshold for an island")
    parser.add_argument("--min-length", type=int, default=DEFAULT_MIN_ISLAND_LENGTH, help="Minimum island length (end - start >= this)")
    parser.add_argument("--smooth-size", type=int, default=DEFAULT_SMOOTH_SIZE, help="Gaussian smoother half-window size")
    parser.add_argument("--smooth-sigma", type=float, default=DEFAULT_SMOOTH_SIGMA, help="Gaussian smoother sigma")
    parser.add_argument("--islands-below-threshold", action="store_true", help="Island = runs where smoothed score <= threshold (default: >= threshold)")
    args = parser.parse_args()

    worker_name = os.getenv("WORKER_NAME", socket.gethostname())
    logger = setup_logger(worker_name)
    db_params = load_db_config(args.config)

    params = {
        "threshold": args.threshold,
        "min_length": args.min_length,
        "smooth_size": args.smooth_size,
        "smooth_sigma": args.smooth_sigma,
        "above_threshold": not args.islands_below_threshold,
    }
    logger.info("Island worker starting (threshold=%s, min_length=%s, smooth_size=%s, sigma=%s).",
                params["threshold"], params["min_length"], params["smooth_size"], params["smooth_sigma"])
    run(
        db_params,
        params,
        logger,
        batch=args.batch,
        once=args.once,
        poll_seconds=args.poll_seconds,
        max_attempts=args.max_attempts,
        refresh_first=args.refresh,
    )


if __name__ == "__main__":
    main()
