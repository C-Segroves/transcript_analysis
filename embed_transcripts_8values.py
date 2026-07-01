"""
embed_transcripts_8values.py

Chunk transcripts and embed the chunks (via Ollama) into transcript_chunk_table
on the MAIN database, for the 8values RAG retrieval step.

For each video that has a real transcript and isn't chunked yet:
  - join its transcript segments in order,
  - split into overlapping word chunks,
  - embed each chunk with the Ollama embedding model,
  - insert (vid_id, chunk_index, text, token_count, embedding[], embedding_model).

It's incremental/restartable: videos already present in transcript_chunk_table
are skipped. Scope to a channel with --channel to start small.

Config / env:
  OLLAMA_HOST       default http://localhost:11434
  EMBED_MODEL       default nomic-embed-text

Usage:
  python embed_transcripts_8values.py --channel desiringGod
  python embed_transcripts_8values.py --limit 500
  python embed_transcripts_8values.py            # all un-chunked videos
"""

import argparse
import json
import logging
import os
import sys
import urllib.request

import psycopg2
from psycopg2.extras import execute_values

ROOT = os.path.dirname(os.path.abspath(__file__))
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434").rstrip("/")
EMBED_MODEL = os.getenv("EMBED_MODEL", "nomic-embed-text")

# Word-based chunking (cheap, no tokenizer dependency). ~200 words ≈ a few
# sentences of a sermon; 40-word overlap keeps context across boundaries.
CHUNK_WORDS = int(os.getenv("CHUNK_WORDS", "200"))
CHUNK_OVERLAP = int(os.getenv("CHUNK_OVERLAP", "40"))


def load_db_config():
    with open(os.path.join(ROOT, "config", "db_config.json"), "r") as f:
        return json.load(f)


def setup_logger():
    logger = logging.getLogger("Embed8v")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        logger.addHandler(h)
    return logger


def chunk_words(text, size, overlap):
    words = text.split()
    if not words:
        return []
    step = max(1, size - overlap)
    chunks = []
    for start in range(0, len(words), step):
        piece = words[start:start + size]
        if piece:
            chunks.append(" ".join(piece))
        if start + size >= len(words):
            break
    return chunks


def embed(text, logger):
    """Return the embedding vector (list of floats) for text via Ollama, or None."""
    payload = json.dumps({"model": EMBED_MODEL, "prompt": text}).encode("utf-8")
    req = urllib.request.Request(
        f"{OLLAMA_HOST}/api/embeddings",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.load(resp)
        vec = data.get("embedding")
        if not vec:
            logger.error("Ollama returned no embedding (model=%s).", EMBED_MODEL)
            return None
        return vec
    except Exception as e:
        logger.error("Embedding request failed: %s", e)
        return None


def get_pending_vids(conn, channel, limit):
    where = [
        "EXISTS (SELECT 1 FROM vid_transcript_table t WHERE t.vid_id = v.id AND t.word_count > 0)",
        "NOT EXISTS (SELECT 1 FROM transcript_chunk_table tc WHERE tc.vid_id = v.id)",
    ]
    params = []
    if channel:
        where.append(
            "v.channel_id IN (SELECT id FROM channel_table WHERE yt_channel_id = %s OR channel_handle = %s)"
        )
        params.extend([channel, channel.lstrip("@")])
    sql = f"SELECT v.id FROM vid_table v WHERE {' AND '.join(where)} ORDER BY v.id"
    if limit:
        sql += f" LIMIT {int(limit)}"
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [r[0] for r in cur.fetchall()]


def get_transcript_text(conn, vid_id):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT text FROM vid_transcript_table WHERE vid_id = %s AND word_count > 0 ORDER BY cum_word_count",
            (vid_id,),
        )
        return " ".join(str(r[0]) for r in cur.fetchall())


def store_chunks(conn, vid_id, rows):
    """rows: list of (chunk_index, text, token_count, embedding_list)."""
    with conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO transcript_chunk_table "
            "(vid_id, chunk_index, text, token_count, embedding, embedding_model) VALUES %s "
            "ON CONFLICT (vid_id, chunk_index) DO NOTHING",
            [(vid_id, ci, txt, tc, emb, EMBED_MODEL) for (ci, txt, tc, emb) in rows],
        )
    conn.commit()


def main():
    parser = argparse.ArgumentParser(description="Chunk + embed transcripts into transcript_chunk_table.")
    parser.add_argument("--channel", help="Only this channel (yt_channel_id or @handle)")
    parser.add_argument("--limit", type=int, help="Max videos to process this run")
    args = parser.parse_args()

    logger = setup_logger()
    logger.info("Ollama=%s model=%s chunk_words=%s overlap=%s", OLLAMA_HOST, EMBED_MODEL, CHUNK_WORDS, CHUNK_OVERLAP)

    conn = psycopg2.connect(**load_db_config())
    try:
        vids = get_pending_vids(conn, args.channel, args.limit)
        logger.info("%d video(s) to embed.", len(vids))
        done = 0
        for vid_id in vids:
            text = get_transcript_text(conn, vid_id)
            chunks = chunk_words(text, CHUNK_WORDS, CHUNK_OVERLAP)
            if not chunks:
                continue
            rows = []
            ok = True
            for ci, chunk in enumerate(chunks):
                vec = embed(chunk, logger)
                if vec is None:
                    ok = False
                    break
                rows.append((ci, chunk, len(chunk.split()), vec))
            if ok and rows:
                store_chunks(conn, vid_id, rows)
                done += 1
                if done % 25 == 0:
                    logger.info("Embedded %d/%d videos (last vid_id=%s, %d chunks).", done, len(vids), vid_id, len(rows))
            elif not ok:
                logger.warning("Stopping on vid_id=%s (embedding failed) -- check Ollama.", vid_id)
                break
        logger.info("Done. Embedded %d video(s).", done)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
