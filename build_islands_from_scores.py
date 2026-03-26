"""
Build islands from vid_score_table scores and write to vid_island_table.

Derives contiguous high-score ranges (islands) from per-video, per-model score arrays
using Gaussian smoothing and find_islands() from "island finder 2.ipynb",
maps them to word ranges and then to start/end times using vid_transcript_table,
and inserts into vid_island_table. Creates vid_island_table if it does not exist.

Based on Analyze_islands.ipynb and island finder 2.ipynb (smoothing + find_islands).

Efficiency: smoothing is vectorized (np.convolve); score rows are streamed (server-side cursor);
inserts are batched (executemany every commit_every). For more throughput: increase
fetch_batch (e.g. 1000), ensure vid_score_table/vid_transcript_table have indexes on
vid_id, and run with sufficient Docker memory.

Multi-machine: run the same image on N machines with --worker-index 0..N-1 and --total-workers N.
Each worker processes rows where (vid_id %% total_workers) == worker_index. Use the same
config/db_config.json (DB must be reachable from all machines). Optionally run with --clear
on worker 0 only once to reset the table before a full run.
"""

import json
import logging
import os
import sys

import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor

# Default n-gram size used when mapping score indices to word ranges (must match scoring)
DEFAULT_N_GRAM_SIZE = 4
# Island finder 2 defaults: threshold and min length (end - start >= this)
DEFAULT_SCORE_THRESHOLD = 0.6
DEFAULT_MIN_ISLAND_LENGTH = 8
# Gaussian smoother (island finder 2): smooth_size=10, sigma=5
DEFAULT_SMOOTH_SIZE = 10
DEFAULT_SMOOTH_SIGMA = 5
# Seconds to pad around island time range for YouTube link
TIME_PAD_SECONDS = 5


def load_db_config(config_path=None):
    path = config_path or os.path.join(os.path.dirname(__file__), "config", "db_config.json")
    with open(path, "r") as f:
        return json.load(f)


def setup_logger():
    logger = logging.getLogger("BuildIslands")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        logger.addHandler(h)
    return logger


def build_smoother(smooth_size, sigma):
    """Gaussian kernel for smoothing (island finder 2.ipynb)."""
    mu = 0
    x = np.array(range(-1 * smooth_size, smooth_size + 1), dtype=float)
    the_smoother = np.exp(-((x - mu) ** 2) / (2 * sigma ** 2))
    return the_smoother


def smooth(values, the_smoother, smooth_size):
    """
    Smooth a 1D score array with Gaussian kernel; output length = input length.
    Vectorized with np.convolve for speed.
    """
    values = np.asarray(values, dtype=float)
    values = np.where(np.isfinite(values), values, 0.0)
    padded = np.concatenate((np.zeros(smooth_size), values, np.zeros(smooth_size)))
    smoothed = np.convolve(padded, the_smoother, mode="valid")
    max_smoothed = np.max(smoothed)
    if max_smoothed > 0:
        smoothed = smoothed / max_smoothed
    return smoothed


def find_islands(array, min_allowed_score, min_allowed_island_size, above_threshold=True):
    """
    Find contiguous runs that form an "island".
    above_threshold=True (default): island = run where array[i] >= min_allowed_score (high scores).
    above_threshold=False: island = run where array[i] <= min_allowed_score (for "lower = better" scores).
    Keeps only runs where (island_end - island_start) >= min_allowed_island_size.
    """
    islands = []
    island_init = True
    island_start = 0
    island_end = 0
    if above_threshold:
        # Island = scores AT OR ABOVE threshold (e.g. >= 0.6)
        in_island = lambda v: v >= min_allowed_score
        out_island = lambda v: v < min_allowed_score
    else:
        # Island = scores AT OR BELOW threshold (e.g. <= 0.6, for "lower = better")
        in_island = lambda v: v <= min_allowed_score
        out_island = lambda v: v > min_allowed_score
    for i in range(len(array)):
        if island_init:
            if in_island(array[i]):
                island_init = False
                island_start = i
        else:
            if out_island(array[i]):
                island_init = True
                island_end = i - 1
                if island_end - island_start >= min_allowed_island_size:
                    islands.append((island_start, island_end))
    if not island_init:
        island_end = len(array) - 1
        if island_end - island_start >= min_allowed_island_size:
            islands.append((island_start, island_end))
    return islands


def score_index_to_word_range(start_idx, end_idx, n_gram_size):
    """
    Map score (ngram) index range to 1-based word range.
    Score index i corresponds to n-gram covering words [i+1, i+n_gram_size].
    """
    word_start = start_idx + 1
    word_end = end_idx + n_gram_size
    return word_start, word_end


def get_transcript_segments(conn, vid_id):
    """
    Return list of (start_sec, duration_sec, word_count, cum_word_count) for vid_id,
    ordered by start. Excludes 'No transcript available' marker (start=-1).
    """
    q = """
        SELECT start, duration, word_count, cum_word_count
        FROM vid_transcript_table
        WHERE vid_id = %s AND start >= 0 AND word_count > 0
        ORDER BY start
    """
    with conn.cursor() as cur:
        cur.execute(q, (vid_id,))
        return cur.fetchall()


def word_range_to_time(segments, word_start, word_end, pad_sec=5):
    """
    Map 1-based word range (word_start, word_end) to (time_start_sec, time_end_sec)
    using transcript segments. Returns (start_sec, end_sec) or (None, None) if no coverage.
    """
    if not segments:
        return None, None
    prev_cum = 0
    min_start_sec = None
    max_end_sec = None
    for row in segments:
        start_sec, duration_sec, word_count, cum_word_count = row
        seg_start_word = prev_cum + 1
        seg_end_word = cum_word_count
        # overlap if segment [seg_start_word, seg_end_word] overlaps [word_start, word_end]
        if seg_end_word >= word_start and seg_start_word <= word_end:
            if min_start_sec is None:
                min_start_sec = start_sec
            max_end_sec = start_sec + duration_sec
        prev_cum = cum_word_count
    if min_start_sec is None:
        return None, None
    return max(0, min_start_sec - pad_sec), max_end_sec + pad_sec


def average_score_in_range(scores, start_idx, end_idx):
    """Average of scores between start_idx and end_idx (inclusive)."""
    segment = scores[start_idx : end_idx + 1]
    valid = [s for s in segment if s is not None]
    if not valid:
        return None
    return sum(valid) / len(valid)


def ensure_vid_island_table(conn, logger):
    """Create vid_island_table if it does not exist, or recreate if it has model_key (old schema)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'vid_island_table'
            );
        """)
        if cur.fetchone()[0]:
            # Table exists: ensure it has model_id (not model_key)
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'vid_island_table'
                AND column_name IN ('model_id', 'model_key');
            """)
            cols = {row[0] for row in cur.fetchall()}
            if "model_id" in cols:
                logger.info("vid_island_table already exists with model_id.")
                return
            if "model_key" in cols:
                logger.info("Recreating vid_island_table with model_id (replacing model_key schema).")
                cur.execute("DROP TABLE vid_island_table;")
                conn.commit()
            else:
                logger.info("vid_island_table already exists.")
                return
        logger.info("Creating vid_island_table.")
        cur.execute("""
            CREATE TABLE vid_island_table (
                id SERIAL PRIMARY KEY,
                vid_id INTEGER NOT NULL,
                model_id INTEGER NOT NULL,
                word_start INTEGER NOT NULL,
                word_end INTEGER NOT NULL,
                time_start_sec FLOAT,
                time_end_sec FLOAT,
                average_score FLOAT,
                insert_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (vid_id) REFERENCES vid_table(id)
            );
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_vid_island_vid_id ON vid_island_table (vid_id);
            CREATE INDEX IF NOT EXISTS idx_vid_island_model_id ON vid_island_table (model_id);
        """)
        conn.commit()
    logger.info("vid_island_table created.")


def get_yt_vid_id(conn, internal_vid_id):
    """Return YouTube video ID for internal vid_id."""
    with conn.cursor() as cur:
        cur.execute("SELECT yt_vid_id FROM vid_table WHERE id = %s", (internal_vid_id,))
        row = cur.fetchone()
        return row[0] if row else None


def build_youtube_link(yt_vid_id, time_start_sec):
    """Return YouTube watch URL with start time in seconds."""
    if not yt_vid_id or time_start_sec is None:
        return None
    return f"https://www.youtube.com/watch?v={yt_vid_id}&t={int(time_start_sec)}"


def run(db_params, logger, n_gram_size=DEFAULT_N_GRAM_SIZE, score_threshold=DEFAULT_SCORE_THRESHOLD,
        min_island_length=DEFAULT_MIN_ISLAND_LENGTH, smooth_size=DEFAULT_SMOOTH_SIZE,
        smooth_sigma=DEFAULT_SMOOTH_SIGMA, time_pad=TIME_PAD_SECONDS, clear_existing=False,
        islands_above_threshold=True, worker_index=0, total_workers=1):
    """
    Load scores from vid_score_table, smooth with Gaussian kernel, find islands (island finder 2),
    resolve times from transcripts, and insert into vid_island_table.
    When total_workers > 1, each run processes only rows where (vid_id % total_workers) = worker_index,
    so multiple machines can run with different --worker-index and the same --total-workers.
    """
    the_smoother = build_smoother(smooth_size, smooth_sigma)
    commit_every = 100  # batch size for executemany and commit frequency
    log_every = 50      # log progress every N score rows
    fetch_batch = 500   # server-side cursor batch size

    if total_workers > 1:
        logger.info("Worker %s of %s (partition by vid_id %% %s = %s).", worker_index, total_workers, total_workers, worker_index)
    else:
        logger.info("Single-worker mode (all score rows).")

    # Use two connections: commits on write_conn would invalidate a named cursor, so we
    # stream from read_conn (no commits) and insert/commit on write_conn only.
    with psycopg2.connect(**db_params) as write_conn:
        ensure_vid_island_table(write_conn, logger)
        if clear_existing:
            if total_workers > 1 and worker_index != 0:
                logger.info("Skipping --clear (only worker 0 clears when using multiple workers).")
            else:
                with write_conn.cursor() as cur:
                    cur.execute("DELETE FROM vid_island_table")
                    write_conn.commit()
                logger.info("Cleared existing vid_island_table rows.")

        insert_sql = """
            INSERT INTO vid_island_table
            (vid_id, model_id, word_start, word_end, time_start_sec, time_end_sec, average_score)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        transcript_cache = {}
        inserted = 0
        errors = 0
        idx = 0
        insert_batch = []  # batch rows for executemany()

        with psycopg2.connect(**db_params) as read_conn:
            with read_conn.cursor(name="score_stream", cursor_factory=RealDictCursor) as read_cur:
                read_cur.itersize = fetch_batch
                if total_workers > 1:
                    read_cur.execute("""
                        SELECT vs.vid_id, vs.model_id, vs.score
                        FROM vid_score_table vs
                        WHERE vs.score IS NOT NULL AND COALESCE(array_length(vs.score, 1), 0) > 0
                        AND (vs.vid_id %% %s) = %s
                    """, (total_workers, worker_index))
                else:
                    read_cur.execute("""
                        SELECT vs.vid_id, vs.model_id, vs.score
                        FROM vid_score_table vs
                        WHERE vs.score IS NOT NULL AND COALESCE(array_length(vs.score, 1), 0) > 0
                    """)
                insert_cur = write_conn.cursor()
                try:
                    for row in read_cur:
                        idx += 1
                        if idx % log_every == 0 or idx == 1:
                            logger.info("Progress: %d score rows, %d islands inserted so far.", idx, inserted)
                        vid_id = row["vid_id"]
                        model_id = row["model_id"]
                        score_list = list(row["score"]) if row["score"] else []
                        if not score_list:
                            continue
                        if vid_id not in transcript_cache:
                            transcript_cache[vid_id] = get_transcript_segments(write_conn, vid_id)
                        segments = transcript_cache[vid_id]
                        if not segments:
                            logger.debug("No transcript segments for vid_id %s, skipping islands.", vid_id)
                            continue
                        smoothed = smooth(score_list, the_smoother, smooth_size)
                        islands = find_islands(smoothed, score_threshold, min_island_length, above_threshold=islands_above_threshold)
                        for (si, ei) in islands:
                            word_start, word_end = score_index_to_word_range(si, ei, n_gram_size)
                            time_start_sec, time_end_sec = word_range_to_time(
                                segments, word_start, word_end, pad_sec=time_pad
                            )
                            avg_score = average_score_in_range(score_list, si, ei)
                            insert_batch.append((
                                vid_id, model_id, word_start, word_end,
                                time_start_sec, time_end_sec, avg_score,
                            ))
                        if len(insert_batch) >= commit_every:
                            try:
                                insert_cur.executemany(insert_sql, insert_batch)
                                inserted += len(insert_batch)
                                write_conn.commit()
                                logger.info("Committed batch: %d islands in table so far.", inserted)
                            except Exception as e:
                                for t in insert_batch:
                                    try:
                                        insert_cur.execute(insert_sql, t)
                                        inserted += 1
                                    except Exception as e1:
                                        logger.warning("Insert failed for vid_id=%s model_id=%s: %s", t[0], t[1], e1)
                                        errors += 1
                                write_conn.commit()
                            insert_batch = []
                    if insert_batch:
                        try:
                            insert_cur.executemany(insert_sql, insert_batch)
                            inserted += len(insert_batch)
                        except Exception as e:
                            for t in insert_batch:
                                try:
                                    insert_cur.execute(insert_sql, t)
                                    inserted += 1
                                except Exception as e1:
                                    logger.warning("Insert failed: %s", e1)
                                    errors += 1
                finally:
                    insert_cur.close()
        write_conn.commit()

    logger.info("Processed %d score rows; inserted %d island rows (errors: %d).", idx, inserted, errors)
    return inserted


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Build islands from scores (smooth + find_islands) and write to vid_island_table.")
    parser.add_argument("--config", default=None, help="Path to db_config.json")
    parser.add_argument("--n-gram-size", type=int, default=DEFAULT_N_GRAM_SIZE, help="N-gram size used for scoring")
    parser.add_argument("--threshold", type=float, default=DEFAULT_SCORE_THRESHOLD, help="Score threshold for island (after smoothing)")
    parser.add_argument("--min-length", type=int, default=DEFAULT_MIN_ISLAND_LENGTH, help="Min island length (island_end - island_start >= this)")
    parser.add_argument("--smooth-size", type=int, default=DEFAULT_SMOOTH_SIZE, help="Gaussian smoother half-window size")
    parser.add_argument("--smooth-sigma", type=float, default=DEFAULT_SMOOTH_SIGMA, help="Gaussian smoother sigma")
    parser.add_argument("--clear", action="store_true", help="Clear vid_island_table before inserting (with multiple workers, only worker 0 clears)")
    parser.add_argument("--islands-below-threshold", action="store_true", help="Treat islands as runs where score <= threshold (default: islands = runs where score >= threshold)")
    parser.add_argument("--worker-index", type=int, default=0, help="This worker's index (0 to total_workers-1). Use with --total-workers for multi-machine runs.")
    parser.add_argument("--total-workers", type=int, default=1, help="Total number of workers; this run processes rows where vid_id %% total_workers == worker-index.")
    args = parser.parse_args()

    logger = setup_logger()
    config_path = args.config or os.path.join(os.path.dirname(__file__), "config", "db_config.json")
    if not os.path.exists(config_path):
        logger.error("Config not found: %s", config_path)
        sys.exit(1)
    db_params = load_db_config(config_path)
    if args.total_workers < 1 or args.worker_index < 0 or args.worker_index >= args.total_workers:
        logger.error("Require total_workers >= 1 and 0 <= worker_index < total_workers.")
        sys.exit(1)
    run(
        db_params,
        logger,
        n_gram_size=args.n_gram_size,
        score_threshold=args.threshold,
        min_island_length=args.min_length,
        smooth_size=args.smooth_size,
        smooth_sigma=args.smooth_sigma,
        clear_existing=args.clear,
        islands_above_threshold=not args.islands_below_threshold,
        worker_index=args.worker_index,
        total_workers=args.total_workers,
    )


if __name__ == "__main__":
    main()
