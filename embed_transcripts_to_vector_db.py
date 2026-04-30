"""
Read transcript segments (TEXT_FORMATTED) from the source Postgres DB, chunk them,
embed via Ollama /api/embeddings, and upsert into TRANSCRIPT_CHUNK_TABLE on the vector DB.

With --all, loads every vid_id from the source that has formatted transcripts, compares
to the vector DB (first chunk metadata per vid), and only queues vids that are missing
or out of date for the current settings. Use --vid-id to force a specific list of vids
regardless; --limit then caps how many of that list run. Use --force to re-embed even
when metadata already matches.

Each chunk's metadata JSON stores target_tokens_min/max, embedding_model, embedding_dim,
and chunking_token_counter so you can change settings later and only re-embed mismatches.

Requires: psycopg2-binary
Optional: pip install tiktoken  (more accurate token counts vs. whitespace heuristic)

With multiple reachable Ollama hosts, embeddings run in parallel (one worker thread per host)
unless embedding_parallel is false in config or you pass --no-embedding-parallel.
"""

from __future__ import annotations

import argparse
import json
import logging
import queue
import threading
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import psycopg2
from psycopg2.extras import Json, execute_values

ROOT = Path(__file__).resolve().parent
VECTOR_CONFIG_PATH = ROOT / "config" / "pg_vector_db_config.json"
DEFAULT_SOURCE_CONFIG = "config/db_config.json"

logger = logging.getLogger(__name__)


def _resolve_path(rel_or_abs: str) -> Path:
    p = Path(rel_or_abs)
    return p if p.is_absolute() else (ROOT / p)


def load_json_config(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _make_token_counter() -> Tuple[Callable[[str], int], str]:
    """Returns (count_fn, label stored in chunk metadata for skip / reproducibility)."""
    try:
        import tiktoken

        enc = tiktoken.get_encoding("cl100k_base")

        def count_tokens(text: str) -> int:
            return len(enc.encode(text or ""))

        logger.info("Using tiktoken (cl100k_base) for chunk sizing.")
        return count_tokens, "tiktoken_cl100k_base"
    except ImportError:
        logger.warning(
            "tiktoken not installed; using whitespace word count as a token proxy. "
            "Install tiktoken for closer alignment to target_tokens_* in config."
        )

        def count_tokens(text: str) -> int:
            return len((text or "").split())

        return count_tokens, "whitespace_word_proxy"


def vector_literal(vec: Sequence[float]) -> str:
    return "[" + ",".join(f"{float(x):.8f}" for x in vec) + "]"


def _ollama_reachable(host: str, port: int, timeout_sec: float) -> bool:
    """Cheap check: Ollama exposes GET /api/tags on the same HTTP port."""
    url = f"http://{host}:{port}/api/tags"
    try:
        with urllib.request.urlopen(url, timeout=timeout_sec) as resp:
            return 200 <= resp.getcode() < 300
    except Exception:
        return False


class OllamaEmbeddingPool:
    """
    Embedding across configured Ollama hosts.
    Default: parallel — one worker thread per reachable host, shared task queue.
    Sequential round-robin with failover when parallel is off or only one host.
    """

    def __init__(
        self,
        servers: List[dict],
        model: str,
        timeout_sec: int = 120,
        *,
        probe: bool = True,
        probe_timeout_sec: float = 3.0,
        parallel: bool = True,
    ):
        if not servers:
            raise ValueError("embedding_servers is empty in pg_vector_db_config.json")
        self._endpoints: List[Tuple[str, int]] = [
            (str(s["host"]), int(s["port"])) for s in servers if s.get("host") is not None
        ]
        if not self._endpoints:
            raise ValueError("No valid host/port in embedding_servers")
        if probe:
            live: List[Tuple[str, int]] = []
            for h, p in self._endpoints:
                if _ollama_reachable(h, p, probe_timeout_sec):
                    live.append((h, p))
                else:
                    logger.warning(
                        "Embedding server %s:%s not reachable (Ollama probe); skipping",
                        h,
                        p,
                    )
            if not live:
                raise ValueError(
                    "No embedding servers passed the reachability probe. "
                    "Start Ollama, fix firewall/network, edit embedding_servers in "
                    "pg_vector_db_config.json, or pass --no-embedding-probe to try anyway."
                )
            logger.info("Using %s reachable embedding server(s)", len(live))
            self._endpoints = live
        self.model = model
        self.timeout_sec = timeout_sec
        self.parallel = parallel and len(self._endpoints) > 1
        if self.parallel:
            logger.info(
                "Parallel embedding: up to %s concurrent requests (one worker per server)",
                len(self._endpoints),
            )
        self._lock = threading.Lock()
        self._next = 0

    def embed(self, prompt: str) -> List[float]:
        """Single text; round-robin across hosts with failover (thread-safe)."""
        last_err: Optional[BaseException] = None
        with self._lock:
            start = self._next
        for i in range(len(self._endpoints)):
            idx = (start + i) % len(self._endpoints)
            host, port = self._endpoints[idx]
            try:
                vec = _ollama_embeddings(host, port, self.model, prompt, self.timeout_sec)
                with self._lock:
                    self._next = (idx + 1) % len(self._endpoints)
                return vec
            except Exception as e:
                last_err = e
                logger.warning("Embedding failed on %s:%s: %s", host, port, e)
        with self._lock:
            self._next = (start + 1) % len(self._endpoints)
        raise RuntimeError("All embedding servers failed") from last_err

    def embed_indexed_parallel(self, indexed: List[Tuple[int, str]]) -> Dict[int, List[float]]:
        """
        Embed many texts; return {chunk_index: vector}.
        With multiple hosts and parallel=True, uses one worker thread per host and a shared queue.
        Failed (idx, text) pairs are retried sequentially with full round-robin failover.
        """
        if not indexed:
            return {}
        if not self.parallel:
            return {idx: self.embed(text) for idx, text in indexed}

        out: Dict[int, List[float]] = {}
        out_lock = threading.Lock()
        failed: List[Tuple[int, str]] = []
        fail_lock = threading.Lock()
        task_q: queue.Queue = queue.Queue()
        for item in indexed:
            task_q.put(item)
        for _ in self._endpoints:
            task_q.put(None)

        def worker(host: str, port: int) -> None:
            while True:
                item = task_q.get()
                if item is None:
                    return
                idx, text = item
                try:
                    vec = _ollama_embeddings(host, port, self.model, text, self.timeout_sec)
                    with out_lock:
                        out[idx] = vec
                except Exception as e:
                    logger.warning(
                        "Parallel embed failed on %s:%s for chunk_index=%s: %s",
                        host,
                        port,
                        idx,
                        e,
                    )
                    with fail_lock:
                        failed.append((idx, text))

        threads = [
            threading.Thread(target=worker, args=(h, p), daemon=True)
            for h, p in self._endpoints
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        if failed:
            logger.info("Retrying %s failed chunk(s) sequentially on all hosts", len(failed))
        for idx, text in failed:
            out[idx] = self.embed(text)
        return out


def _ollama_embeddings(host: str, port: int, model: str, prompt: str, timeout_sec: int) -> List[float]:
    url = f"http://{host}:{port}/api/embeddings"
    body = json.dumps({"model": model, "prompt": prompt}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    emb = data.get("embedding")
    if not isinstance(emb, list):
        raise ValueError(f"Unexpected Ollama response (no embedding list): {repr(data)[:200]}")
    return [float(x) for x in emb]


def fetch_formatted_segments(source_conn, vid_id: Any) -> List[Dict[str, Any]]:
    q = """
        SELECT start, duration, text_formatted, word_count
        FROM vid_transcript_table
        WHERE vid_id = %s
          AND start >= 0
          AND COALESCE(TRIM(text_formatted), '') <> ''
        ORDER BY start
    """
    with source_conn.cursor() as cur:
        cur.execute(q, (vid_id,))
        rows = cur.fetchall()
    out = []
    for start, duration, text_formatted, word_count in rows:
        out.append(
            {
                "start": float(start),
                "duration": float(duration) if duration is not None else 0.0,
                "text": (text_formatted or "").strip(),
                "word_count": int(word_count) if word_count is not None else len((text_formatted or "").split()),
            }
        )
    return out


def split_oversized_text(text: str, max_tokens: int, count_tokens: Callable[[str], int]) -> List[str]:
    """If a single segment exceeds max_tokens, split on words."""
    if count_tokens(text) <= max_tokens:
        return [text]
    words = text.split()
    if not words:
        return []
    parts: List[str] = []
    buf: List[str] = []
    for w in words:
        buf.append(w)
        piece = " ".join(buf)
        if count_tokens(piece) >= max_tokens and len(buf) > 1:
            buf.pop()
            parts.append(" ".join(buf))
            buf = [w]
    if buf:
        parts.append(" ".join(buf))
    return parts


def build_chunks_simple(
    segments: List[Dict[str, Any]],
    min_tokens: int,
    max_tokens: int,
    count_tokens: Callable[[str], int],
) -> List[Dict[str, Any]]:
    """
    Simpler greedy chunker: accumulate segments until adding the next would exceed max_tokens,
    then emit the buffer. Merges undersized tail into previous chunk when possible.
    """
    expanded: List[Dict[str, Any]] = []
    for seg in segments:
        for piece in split_oversized_text(seg["text"], max_tokens, count_tokens):
            expanded.append({**seg, "text": piece})

    if not expanded:
        return []

    chunks: List[Dict[str, Any]] = []
    buf: List[Dict[str, Any]] = []

    def emit_buffer() -> None:
        if not buf:
            return
        text = " ".join(s["text"] for s in buf)
        chunks.append(
            {
                "text": text,
                "start_time": buf[0]["start"],
                "end_time": buf[-1]["start"] + buf[-1]["duration"],
                "word_count": len(text.split()),
                "token_count": count_tokens(text),
                "metadata": {
                    "segment_starts": [s["start"] for s in buf],
                    "segment_count": len(buf),
                },
            }
        )
        buf.clear()

    for seg in expanded:
        if not buf:
            buf.append(seg)
            continue
        trial = " ".join(s["text"] for s in buf) + " " + seg["text"]
        if count_tokens(trial) <= max_tokens:
            buf.append(seg)
        else:
            emit_buffer()
            buf.append(seg)

    emit_buffer()

    i = 0
    while i < len(chunks) - 1 and chunks[i + 1]["token_count"] < min_tokens:
        merged_text = chunks[i]["text"] + " " + chunks[i + 1]["text"]
        if count_tokens(merged_text) <= max_tokens:
            chunks[i] = {
                "text": merged_text,
                "start_time": chunks[i]["start_time"],
                "end_time": chunks[i + 1]["end_time"],
                "word_count": len(merged_text.split()),
                "token_count": count_tokens(merged_text),
                "metadata": {
                    "segment_starts": chunks[i]["metadata"]["segment_starts"]
                    + chunks[i + 1]["metadata"]["segment_starts"],
                    "segment_count": chunks[i]["metadata"]["segment_count"]
                    + chunks[i + 1]["metadata"]["segment_count"],
                },
            }
            del chunks[i + 1]
        else:
            i += 1

    return chunks


def fetch_all_source_vid_ids(source_conn) -> List[Any]:
    """All DISTINCT vids with usable formatted transcript rows, ORDER BY vid_id."""
    q = """
        SELECT DISTINCT vid_id
        FROM vid_transcript_table
        WHERE start >= 0
          AND COALESCE(TRIM(text_formatted), '') <> ''
        ORDER BY vid_id
    """
    with source_conn.cursor() as cur:
        cur.execute(q)
        return [r[0] for r in cur.fetchall()]


def fetch_vid_first_metadata_map(vector_conn) -> Dict[str, Any]:
    """
    For each vid_id in transcript_chunk_table, first chunk's metadata (by chunk_index).
    Keys are vid_id as str for stable lookup against source ids.
    """
    q = """
        SELECT DISTINCT ON (vid_id::text)
            vid_id::text,
            metadata
        FROM transcript_chunk_table
        ORDER BY vid_id::text, chunk_index ASC
    """
    out: Dict[str, Any] = {}
    with vector_conn.cursor() as cur:
        cur.execute(q)
        for vkey, metadata in cur.fetchall():
            out[str(vkey)] = metadata
    return out


def chunk_metadata_matches_current(
    metadata: Any,
    min_tokens: int,
    max_tokens: int,
    model_name: str,
    embedding_dim: int,
    token_counter_label: str,
) -> bool:
    """
    True if first-chunk metadata dict matches current run settings (vid fully up to date).
    Missing or legacy rows -> False.
    """
    if metadata is None or not isinstance(metadata, dict):
        return False
    if metadata.get("embedding_model") != model_name:
        return False
    if int(metadata.get("embedding_dim", -1)) != int(embedding_dim):
        return False
    if metadata.get("chunking_token_counter") != token_counter_label:
        return False
    if metadata.get("target_tokens_min") is None or metadata.get("target_tokens_max") is None:
        return False
    if int(metadata["target_tokens_min"]) != int(min_tokens) or int(
        metadata["target_tokens_max"]
    ) != int(max_tokens):
        return False
    return True


def upsert_chunks(
    vector_conn,
    vid_id_str: str,
    chunks: List[Dict[str, Any]],
    pool: OllamaEmbeddingPool,
    embedding_dim: int,
    model_name: str,
    min_tokens: int,
    max_tokens: int,
    token_counter_label: str,
) -> None:
    for c in chunks:
        c["metadata"] = dict(c["metadata"])
        c["metadata"]["embedding_model"] = model_name
        c["metadata"]["embedding_dim"] = embedding_dim
        c["metadata"]["target_tokens_min"] = min_tokens
        c["metadata"]["target_tokens_max"] = max_tokens
        c["metadata"]["chunking_token_counter"] = token_counter_label

    indexed = [(idx, ch["text"]) for idx, ch in enumerate(chunks)]
    emb_by_idx = pool.embed_indexed_parallel(indexed)
    rows = []
    for idx, ch in enumerate(chunks):
        emb = emb_by_idx[idx]
        if len(emb) != embedding_dim:
            raise ValueError(
                f"Embedding dim {len(emb)} != expected {embedding_dim} (check model vs VECTOR(...) in DB)"
            )
        rows.append(
            (
                vid_id_str,
                idx,
                ch["text"],
                vector_literal(emb),
                ch["start_time"],
                ch["end_time"],
                ch["word_count"],
                ch["token_count"],
                Json(ch["metadata"]),
            )
        )

    try:
        with vector_conn.cursor() as cur:
            cur.execute("DELETE FROM transcript_chunk_table WHERE vid_id = %s", (vid_id_str,))
            if not rows:
                return
            insert_sql = """
                INSERT INTO transcript_chunk_table (
                    vid_id, chunk_index, text, embedding, start_time, end_time,
                    word_count, token_count, metadata
                ) VALUES %s
                ON CONFLICT (vid_id, chunk_index) DO UPDATE SET
                    text = EXCLUDED.text,
                    embedding = EXCLUDED.embedding,
                    start_time = EXCLUDED.start_time,
                    end_time = EXCLUDED.end_time,
                    word_count = EXCLUDED.word_count,
                    token_count = EXCLUDED.token_count,
                    metadata = EXCLUDED.metadata
            """
            template = "(%s, %s, %s, %s::vector, %s, %s, %s, %s, %s)"
            execute_values(cur, insert_sql, rows, template=template, page_size=50)
        vector_conn.commit()
    except Exception:
        vector_conn.rollback()
        raise


def process_one_video(
    source_conn,
    vector_conn,
    vid_id: Any,
    pool: OllamaEmbeddingPool,
    min_tokens: int,
    max_tokens: int,
    count_tokens: Callable[[str], int],
    embedding_dim: int,
    model_name: str,
    token_counter_label: str,
    force: bool,
    meta_by_vid: Dict[str, Any],
) -> Tuple[int, bool]:
    """
    Returns (chunks_written, skipped_already_vectorized).
    skipped is True when existing DB rows match current chunking/embedding settings.
    meta_by_vid maps str(vid_id) -> first chunk metadata (preloaded; updated map not required mid-run).
    """
    vid_id_str = str(vid_id)
    prec = meta_by_vid.get(vid_id_str)
    if not force and chunk_metadata_matches_current(
        prec,
        min_tokens,
        max_tokens,
        model_name,
        embedding_dim,
        token_counter_label,
    ):
        logger.debug(
            "Skip %s: already vectorized with same params (use --force to rebuild)",
            vid_id_str,
        )
        return 0, True

    logger.info("Embedding VID %s", vid_id_str)
    segments = fetch_formatted_segments(source_conn, vid_id)
    if not segments:
        logger.info("Skip %s: no formatted segments", vid_id_str)
        return 0, False
    chunks = build_chunks_simple(segments, min_tokens, max_tokens, count_tokens)
    if not chunks:
        return 0, False
    upsert_chunks(
        vector_conn,
        vid_id_str,
        chunks,
        pool,
        embedding_dim,
        model_name,
        min_tokens,
        max_tokens,
        token_counter_label,
    )
    logger.info("VID %s: %s chunks stored", vid_id_str, len(chunks))
    return len(chunks), False


def main() -> None:
    parser = argparse.ArgumentParser(description="Embed transcripts into TRANSCRIPT_CHUNK_TABLE")
    parser.add_argument(
        "--vid-id",
        action="append",
        dest="vid_ids",
        help="Process only these vid_ids (repeatable); no source-vs-vector diff, order as given. Combine with --limit to cap.",
    )
    parser.add_argument("--all", action="store_true", help="Process all vids with formatted transcripts")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max vids to process this run: after --all diff (missing/outdated only), or first N of --vid-id list",
    )
    parser.add_argument(
        "--embedding-dim",
        type=int,
        default=768,
        help="Expected embedding size (must match VECTOR(n) in DB, e.g. 768 for nomic-embed-text)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-embed even when stored metadata matches current chunking model and token bounds",
    )
    parser.add_argument(
        "--no-embedding-probe",
        action="store_true",
        help="Do not pre-check embedding_servers with HTTP GET /api/tags (may waste time on dead hosts each chunk)",
    )
    parser.add_argument(
        "--no-embedding-parallel",
        action="store_true",
        help="Embed one chunk at a time (round-robin). Default: one worker thread per Ollama host.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    vcfg = load_json_config(VECTOR_CONFIG_PATH)
    source_rel = vcfg.get("source_postgres_config") or DEFAULT_SOURCE_CONFIG
    source_path = _resolve_path(source_rel)
    scfg = load_json_config(source_path)
    source_params = scfg if "dbname" in scfg else scfg.get("postgres") or scfg

    vector_params = vcfg.get("postgres")
    if not isinstance(vector_params, dict):
        raise SystemExit("pg_vector_db_config.json missing postgres section")

    chunking = vcfg.get("chunking") or {}
    min_tokens = int(chunking.get("target_tokens_min", 400))
    max_tokens = int(chunking.get("target_tokens_max", 700))
    if min_tokens > max_tokens:
        min_tokens, max_tokens = max_tokens, min_tokens

    model = vcfg.get("embedding_model") or "nomic-embed-text"
    servers = vcfg.get("embedding_servers") or []
    raw_probe = vcfg.get("embedding_server_probe", True)
    if isinstance(raw_probe, str):
        cfg_probe_on = raw_probe.strip().lower() in ("1", "true", "yes", "on")
    else:
        cfg_probe_on = bool(raw_probe)
    probe_timeout = float(vcfg.get("embedding_server_probe_timeout_sec", 3))
    raw_par = vcfg.get("embedding_parallel", True)
    if isinstance(raw_par, str):
        cfg_parallel = raw_par.strip().lower() in ("1", "true", "yes", "on")
    else:
        cfg_parallel = bool(raw_par)
    pool = OllamaEmbeddingPool(
        servers,
        model,
        probe=cfg_probe_on and not args.no_embedding_probe,
        probe_timeout_sec=probe_timeout,
        parallel=cfg_parallel and not args.no_embedding_parallel,
    )
    count_tokens_fn, token_counter_label = _make_token_counter()

    if not args.all and not args.vid_ids:
        parser.error("Specify --vid-id ... or --all")
    source_conn = psycopg2.connect(**source_params)
    vector_conn = psycopg2.connect(**vector_params)
    try:
        meta_by_vid = fetch_vid_first_metadata_map(vector_conn)

        if args.vid_ids:
            to_run = list(args.vid_ids)
            if args.limit is not None:
                to_run = to_run[: max(0, args.limit)]
            logger.info("Explicit --vid-id list: %s vids to visit", len(to_run))
        else:
            source_all = fetch_all_source_vid_ids(source_conn)
            pending: List[Any] = []
            for vid in source_all:
                if args.force or not chunk_metadata_matches_current(
                    meta_by_vid.get(str(vid)),
                    min_tokens,
                    max_tokens,
                    model,
                    args.embedding_dim,
                    token_counter_label,
                ):
                    pending.append(vid)
            need_count = len(pending)
            if args.limit is not None:
                pending = pending[: max(0, args.limit)]
            to_run = pending
            logger.info(
                "Source vids with transcripts: %s; need embedding (missing or stale): %s; running this run: %s",
                len(source_all),
                need_count,
                len(to_run),
            )

        total_chunks = 0
        total_skipped = 0
        for vid in to_run:
            try:
                n, skipped = process_one_video(
                    source_conn,
                    vector_conn,
                    vid,
                    pool,
                    min_tokens,
                    max_tokens,
                    count_tokens_fn,
                    args.embedding_dim,
                    model,
                    token_counter_label,
                    args.force,
                    meta_by_vid,
                )
                total_chunks += n
                if skipped:
                    total_skipped += 1
            except urllib.error.URLError as e:
                logger.error("Network error on VID %s: %s", vid, e)
                vector_conn.rollback()
            except Exception as e:
                logger.exception("Failed VID %s: %s", vid, e)
                vector_conn.rollback()

        logger.info(
            "Done. Chunks written this run: %s; vids skipped (already up to date): %s",
            total_chunks,
            total_skipped,
        )
    finally:
        source_conn.close()
        vector_conn.close()


if __name__ == "__main__":
    main()
