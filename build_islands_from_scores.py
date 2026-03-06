"""
Build islands from vid_score_table scores and write to vid_island_table.

Derives contiguous high-score ranges (islands) from per-video, per-model score arrays
using Gaussian smoothing and find_islands() from "island finder 2.ipynb",
maps them to word ranges and then to start/end times using vid_transcript_table,
and inserts into vid_island_table. Creates vid_island_table if it does not exist.

Based on Analyze_islands.ipynb and island finder 2.ipynb (smoothing + find_islands).
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
    Matches island finder 2.ipynb logic (zero-padded, then normalize by max).
    """
    values = np.asarray(values, dtype=float)
    n = len(values)
    # Replace None/nan with 0 for smoothing
    values = np.where(np.isfinite(values), values, 0.0)
    # Pad so we have full window for each index; window has 2*smooth_size+1 elements
    padded = np.concatenate((np.zeros(smooth_size), values, np.zeros(smooth_size)))
    smoothed = []
    for i in range(n):
        window = padded[i : i + 2 * smooth_size + 1]
        smoothed.append(np.dot(window, the_smoother))
    smoothed = np.array(smoothed)
    max_smoothed = np.max(smoothed)
    if max_smoothed > 0:
        smoothed = smoothed / max_smoothed
    return smoothed


def find_islands(array, min_allowed_score, min_allowed_island_size):
    """
    Find contiguous runs where array[i] >= min_allowed_score.
    Keeps only runs where (island_end - island_start) >= min_allowed_island_size.
    From island finder 2.ipynb.
    """
    islands = []
    island_init = True
    island_start = 0
    island_end = 0
    for i in range(len(array)):
        if island_init:
            if array[i] >= min_allowed_score:
                island_init = False
                island_start = i
        else:
            if array[i] < min_allowed_score:
                island_init = True
                island_end = i - 1
                if island_end - island_start >= min_allowed_island_size:
                    islands.append((island_start, island_end))
    # If array ends while still in an island, close it
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
    """Create vid_island_table if it does not exist."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'vid_island_table'
            );
        """)
        if cur.fetchone()[0]:
            logger.info("vid_island_table already exists.")
            return
        logger.info("Creating vid_island_table.")
        cur.execute("""
            CREATE TABLE vid_island_table (
                id SERIAL PRIMARY KEY,
                vid_id INTEGER NOT NULL,
                model_key VARCHAR(255) NOT NULL,
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
            CREATE INDEX IF NOT EXISTS idx_vid_island_model_key ON vid_island_table (model_key);
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
        smooth_sigma=DEFAULT_SMOOTH_SIGMA, time_pad=TIME_PAD_SECONDS, clear_existing=False):
    """
    Load scores from vid_score_table, smooth with Gaussian kernel, find islands (island finder 2),
    resolve times from transcripts, and insert into vid_island_table.
    """
    the_smoother = build_smoother(smooth_size, smooth_sigma)

    with psycopg2.connect(**db_params) as conn:
        ensure_vid_island_table(conn, logger)
        if clear_existing:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM vid_island_table")
                conn.commit()
            logger.info("Cleared existing vid_island_table rows.")

        # All (vid_id, model_key, score) that have scores
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT vs.vid_id, vs.model_key, vs.score
                FROM vid_score_table vs
                WHERE vs.score IS NOT NULL AND COALESCE(array_length(vs.score, 1), 0) > 0
            """)
            rows = cur.fetchall()

    logger.info("Loaded %d score rows from vid_score_table.", len(rows))

    # Cache transcript segments per vid_id
    transcript_cache = {}
    inserted = 0
    errors = 0

    with psycopg2.connect(**db_params) as conn:
        insert_cur = conn.cursor()
        insert_sql = """
            INSERT INTO vid_island_table
            (vid_id, model_key, word_start, word_end, time_start_sec, time_end_sec, average_score)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        for row in rows:
            vid_id = row["vid_id"]
            model_key = row["model_key"]
            score_list = list(row["score"]) if row["score"] else []
            if not score_list:
                continue
            if vid_id not in transcript_cache:
                transcript_cache[vid_id] = get_transcript_segments(conn, vid_id)
            segments = transcript_cache[vid_id]
            if not segments:
                logger.debug("No transcript segments for vid_id %s, skipping islands.", vid_id)
                continue
            # Island finder 2: smooth then find_islands(smoothed, threshold, min_length)
            smoothed = smooth(score_list, the_smoother, smooth_size)
            islands = find_islands(smoothed, score_threshold, min_island_length)
            for (si, ei) in islands:
                word_start, word_end = score_index_to_word_range(si, ei, n_gram_size)
                time_start_sec, time_end_sec = word_range_to_time(
                    segments, word_start, word_end, pad_sec=time_pad
                )
                avg_score = average_score_in_range(score_list, si, ei)
                try:
                    insert_cur.execute(
                        insert_sql,
                        (vid_id, model_key, word_start, word_end,
                         time_start_sec, time_end_sec, avg_score),
                    )
                    inserted += 1
                except Exception as e:
                    logger.warning("Insert failed for vid_id=%s model_key=%s: %s", vid_id, model_key, e)
                    errors += 1
        conn.commit()
        insert_cur.close()

    logger.info("Inserted %d island rows (errors: %d).", inserted, errors)
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
    parser.add_argument("--clear", action="store_true", help="Clear vid_island_table before inserting")
    args = parser.parse_args()

    logger = setup_logger()
    config_path = args.config or os.path.join(os.path.dirname(__file__), "config", "db_config.json")
    if not os.path.exists(config_path):
        logger.error("Config not found: %s", config_path)
        sys.exit(1)
    db_params = load_db_config(config_path)
    run(
        db_params,
        logger,
        n_gram_size=args.n_gram_size,
        score_threshold=args.threshold,
        min_island_length=args.min_length,
        smooth_size=args.smooth_size,
        smooth_sigma=args.smooth_sigma,
        clear_existing=args.clear,
    )


if __name__ == "__main__":
    main()
