"""
async_processing_client.py  (model-major worker)

A scoring client that works one MODEL at a time:

  1. Ask the server for a model to work on, reporting which models we already
     have loaded so the server can prefer one of those (affinity) and never hand
     the same model to two clients at once.
  2. Load that model (from model_table) if it isn't already cached. The cache is
     a small RAM-aware LRU, so big-RAM machines keep several models hot and
     small ones evict the least-recently-used to stay under budget.
  3. Find every video whose transcript still lacks a score for this model and
     score them, saving results in batches. ("Pending" is always derived from
     missing scores, so the work is resumable after a restart.)
  4. Tell the server the model is complete and ask for the next one.

Use one client process per core you want to use: each process owns a different
model (the server enforces that), so N processes on a box use N cores without
any of them duplicating work.

Schema: this targets the migrated DB (integer ids) --
  vid_table(id, yt_vid_id), vid_transcript_table(vid_id -> vid_table.id, word_count, ...),
  model_table(id, external_key, model_data), vid_score_table(vid_id, model_id, score).
"""

import asyncio
import json
import os
import platform
import logging
import pickle
import time
import signal
from collections import OrderedDict

import psutil
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values

from nltk.tokenize import word_tokenize
from nltk.util import pad_sequence

from async_client import BaseClient

# ---------------------------------------------------------------------------
# Config / logging
# ---------------------------------------------------------------------------

N_GRAM_SIZE = int(os.getenv("N_GRAM_SIZE", "4"))
# Max models to keep cached in this process (LRU). 1 is fine for model-major;
# a couple gives reuse across maintenance re-pending. Tune to the box's RAM.
MODEL_CACHE_SIZE = int(os.getenv("MODEL_CACHE_SIZE", "3"))
# Evict LRU models when system memory usage exceeds this percent.
MEMORY_HIGH_PERCENT = float(os.getenv("MEMORY_HIGH_PERCENT", "85"))
# How many videos to score before flushing scores to the DB.
SAVE_BATCH = int(os.getenv("SAVE_BATCH", "10"))
# Seconds to wait when the server has no work for us.
NO_WORK_POLL_SECONDS = int(os.getenv("NO_WORK_POLL_SECONDS", "15"))


def load_config():
    try:
        with open('config/db_config.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError as e:
        logging.getLogger('ProcessingClient').error(f"Database config file not found: {e}")
        raise
    except json.JSONDecodeError as e:
        logging.getLogger('ProcessingClient').error(f"Invalid JSON in db_config.json: {e}")
        raise


DB_CONFIG = load_config()


def setup_logger(log_file_path):
    logger = logging.getLogger('ProcessingClient')
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(ch)
    return logger


# ---------------------------------------------------------------------------
# CPU temperature helpers (kept from the original client, used in health packet)
# ---------------------------------------------------------------------------

def get_cpu_temp_linux():
    try:
        temps = psutil.sensors_temperatures()
        if "coretemp" in temps:
            for entry in temps["coretemp"]:
                if entry.label == "Package id 0":
                    return entry.current
        elif "cpu-thermal" in temps:
            return temps["cpu-thermal"][0].current
        return "CPU temperature sensor not found."
    except Exception as e:
        return f"Error reading CPU temperature: {e}"


def get_cpu_temp_windows_wmi():
    try:
        import wmi
        w = wmi.WMI(namespace="root\\OpenHardwareMonitor")
        for sensor in w.Sensor():
            if sensor.SensorType == "Temperature":
                return sensor.Value
    except Exception as e:
        return f"Error reading CPU temperature: {e}"


def get_cpu_temp():
    system = platform.system()
    if system == "Linux":
        return get_cpu_temp_linux()
    elif system == "Windows":
        return get_cpu_temp_windows_wmi()
    return "Unsupported OS"


# ---------------------------------------------------------------------------
# Packet builders
# ---------------------------------------------------------------------------

def generate_client_init_packet(machine_name):
    return {"packet_type": "init_packet", "additional_data": {"machine_name": machine_name}}


def generate_work_request_packet(machine_name, loaded_models):
    return {"packet_type": "work_request",
            "additional_data": {"machine_name": machine_name, "loaded_models": list(loaded_models)}}


def generate_model_complete_packet(model_id, scored_count):
    return {"packet_type": "model_complete",
            "additional_data": {"model_id": model_id, "scored_count": scored_count}}


def generate_health_packet(machine_name, model_cache):
    cpu_temp = get_cpu_temp()
    cpu_usage = psutil.cpu_percent(interval=1)
    memory_usage = psutil.virtual_memory().percent
    return {"packet_type": "health_packet",
            "additional_data": {"machine_name": machine_name, "cpu_temp": cpu_temp,
                                "cpu_usage": cpu_usage, "memory_usage": memory_usage,
                                "loaded_models": list(model_cache.keys())}}


# ---------------------------------------------------------------------------
# Transcript prep + scoring (pure, runs off the event loop via to_thread)
# ---------------------------------------------------------------------------

def prep_transcript(transcript, n_gram_size):
    return list(pad_sequence(word_tokenize(transcript), n_gram_size,
                             pad_left=True, left_pad_symbol="<s>"))


def build_transcript_items(transcript, n_gram_size):
    """Each item is (word, preceding (n_gram_size-1) context words)."""
    return [
        (item, transcript[j:j + n_gram_size - 1])
        for j, item in enumerate(transcript[n_gram_size - 1:])
        if j + n_gram_size - 1 < len(transcript)
    ]


def score_transcript_items(model, transcript_items):
    score = model.score  # local bind for the hot loop
    return [score(item, ngram) for item, ngram in transcript_items if ngram]


# ---------------------------------------------------------------------------
# DB access (all called via asyncio.to_thread so the event loop stays responsive)
# ---------------------------------------------------------------------------

def db_get_pending_video_ids(db_pool, model_id, logger):
    """Internal vid_table.id of every video whose transcript lacks a score for model_id."""
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            # EXISTS/NOT EXISTS (semi-joins) instead of JOIN+GROUP BY so the planner
            # can satisfy this with index probes: one per video against the
            # transcript index and the (vid_id, model_id) score index, rather than
            # scanning the whole transcript/score tables.
            cur.execute("""
                SELECT v.id
                FROM vid_table v
                WHERE EXISTS (
                        SELECT 1 FROM vid_transcript_table vt
                        WHERE vt.vid_id = v.id AND vt.word_count > 0
                      )
                  AND NOT EXISTS (
                        SELECT 1 FROM vid_score_table vs
                        WHERE vs.vid_id = v.id AND vs.model_id = %s
                          AND cardinality(vs.score) > 0
                      )
                ORDER BY v.id
            """, (model_id,))
            return [r[0] for r in cur.fetchall()]
    except psycopg2.Error as e:
        logger.error(f"Error fetching pending videos for model {model_id}: {e}")
        conn.rollback()
        return []
    finally:
        db_pool.putconn(conn)


def db_get_transcript_text(db_pool, vid_id, logger):
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT text FROM vid_transcript_table
                WHERE vid_id = %s AND word_count > 0
                ORDER BY cum_word_count
            """, (vid_id,))
            return ' '.join(str(r[0]) for r in cur.fetchall())
    except psycopg2.Error as e:
        logger.error(f"Error fetching transcript for vid {vid_id}: {e}")
        conn.rollback()
        return ""
    finally:
        db_pool.putconn(conn)


def db_load_models(db_pool, model_ids, logger):
    """Return {model_id: model_object} for the given ids, unpickled from model_table."""
    loaded = {}
    if not model_ids:
        return loaded
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, yt_model_key, model_data FROM model_table WHERE id = ANY(%s)",
                (list(model_ids),),
            )
            rows = cur.fetchall()
        for model_id, yt_model_key, model_data in rows:
            try:
                if isinstance(model_data, memoryview):
                    model_data = model_data.tobytes()
                if not model_data:
                    logger.error(f"Model {model_id} ({yt_model_key}) has empty data.")
                    continue
                loaded[model_id] = pickle.loads(model_data)
            except Exception as e:
                logger.error(f"Failed to unpickle model {model_id} ({yt_model_key}): {e}")
        return loaded
    except psycopg2.Error as e:
        logger.error(f"DB error loading models {list(model_ids)}: {e}")
        conn.rollback()
        return loaded
    finally:
        db_pool.putconn(conn)


def db_save_scores(db_pool, rows, logger):
    """rows: list of (vid_id, model_id, score_list). One batched upsert."""
    if not rows:
        return 0
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            # vid_score_table has no unique key, so we can't upsert. Delete any
            # existing rows for these (vid_id, model_id) pairs first, then insert.
            # This replaces stale empty-score rows being re-scored and prevents
            # duplicate rows for a pair (which would break the island reader).
            execute_values(cur, """
                DELETE FROM vid_score_table vs
                USING (VALUES %s) AS d(vid_id, model_id)
                WHERE vs.vid_id = d.vid_id AND vs.model_id = d.model_id
            """, [(vid_id, model_id) for (vid_id, model_id, score) in rows],
                template="(%s, %s)")
            execute_values(cur, """
                INSERT INTO vid_score_table (vid_id, model_id, score, insert_at)
                VALUES %s
            """, [(vid_id, model_id, score) for (vid_id, model_id, score) in rows],
                template="(%s, %s, %s, CURRENT_TIMESTAMP)")
        conn.commit()
        return len(rows)
    except psycopg2.Error as e:
        logger.error(f"DB error saving {len(rows)} score row(s): {e}")
        conn.rollback()
        return 0
    finally:
        db_pool.putconn(conn)


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class ProcessingClient(BaseClient):
    def __init__(self, host, machine_name, port, logger, config, db_pool):
        super().__init__(host, port, logger, config)
        self.db_pool = db_pool
        self.machine_name = machine_name
        self.client_state = 'pause'
        self.task_task = None
        self.model_cache = OrderedDict()  # model_id -> model object (LRU; most-recent at end)
        self.total_scored = 0
        # Server replies (assignment / no_work) are delivered to the worker loop here.
        self.assignment_queue = asyncio.Queue()
        logger.info(f"Model-major client initialized: {machine_name} "
                    f"(cache={MODEL_CACHE_SIZE}, n_gram={N_GRAM_SIZE})")

    # ----- model cache (RAM-aware LRU) -----

    def _evict_if_needed(self):
        while len(self.model_cache) > MODEL_CACHE_SIZE:
            old_id, _ = self.model_cache.popitem(last=False)
            self.logger.info(f"Evicted model {old_id} (cache over size {MODEL_CACHE_SIZE}).")
        # Pressure-based eviction: drop LRU models while memory is high (keep >=1).
        try:
            while len(self.model_cache) > 1 and psutil.virtual_memory().percent > MEMORY_HIGH_PERCENT:
                old_id, _ = self.model_cache.popitem(last=False)
                self.logger.warning(f"Memory high; evicted model {old_id}.")
        except Exception:
            pass

    async def ensure_model(self, model_id):
        """Return the model object for model_id, loading + caching it if needed."""
        if model_id in self.model_cache:
            self.model_cache.move_to_end(model_id)  # mark most-recently-used
            return self.model_cache[model_id]
        self.logger.info(f"Loading model {model_id} from DB...")
        loaded = await asyncio.to_thread(db_load_models, self.db_pool, [model_id], self.logger)
        model = loaded.get(model_id)
        if model is None:
            return None
        self.model_cache[model_id] = model
        self.model_cache.move_to_end(model_id)
        self._evict_if_needed()
        return model

    # ----- packet handling -----

    async def process_received_data(self, packet):
        packet_type = packet.get('packet_type')
        self.logger.info(f"Received packet: {packet_type} (state: {self.client_state})")
        try:
            match packet_type:
                case 'play':
                    self.client_state = 'play'
                    if not self.task_task or self.task_task.done():
                        self.task_task = asyncio.create_task(self.worker_loop())
                case 'pause':
                    self.client_state = 'pause'
                    # The worker loop checks client_state between videos and stops.
                case 'assignment' | 'no_work':
                    await self.assignment_queue.put(packet)
                case 'health_check':
                    await self.send_data(generate_health_packet(self.machine_name, self.model_cache))
                case 'shutdown_request':
                    await self.graceful_shutdown()
                case _:
                    self.logger.error(f"Unknown packet type: {packet_type}")
        except Exception as e:
            self.logger.error(f"Error processing packet {packet_type}: {e}")

    async def initialize_client(self):
        self.config = load_config()
        await self.send_data(generate_client_init_packet(self.machine_name))
        self.logger.info(f"Sent init packet for {self.machine_name}")

    # ----- the model-major worker loop -----

    async def request_model(self):
        """Ask the server for a model; return model_id or None (no work)."""
        # Drain any stale replies first.
        while not self.assignment_queue.empty():
            self.assignment_queue.get_nowait()
        await self.send_data(generate_work_request_packet(self.machine_name, self.model_cache.keys()))
        reply = await self.assignment_queue.get()
        if reply.get('packet_type') == 'assignment':
            return reply.get('additional_data', {}).get('model_id')
        return None

    async def process_model(self, model_id):
        """Score every pending video for model_id. Returns count scored."""
        model = await self.ensure_model(model_id)
        if model is None:
            self.logger.error(f"Could not load model {model_id}; reporting complete to avoid stalling.")
            return 0

        pending = await asyncio.to_thread(db_get_pending_video_ids, self.db_pool, model_id, self.logger)
        self.logger.info(f"Model {model_id}: {len(pending)} pending video(s).")

        scored = 0
        batch = []
        for vid_id in pending:
            if self.client_state != 'play':
                self.logger.info("Paused mid-model; flushing and stopping.")
                break
            text = await asyncio.to_thread(db_get_transcript_text, self.db_pool, vid_id, self.logger)
            if not text:
                continue
            transcript = await asyncio.to_thread(prep_transcript, text, N_GRAM_SIZE)
            items = build_transcript_items(transcript, N_GRAM_SIZE)
            if not items:
                # No usable items: store empty score so it isn't retried forever.
                batch.append((vid_id, model_id, []))
            else:
                score = await asyncio.to_thread(score_transcript_items, model, items)
                batch.append((vid_id, model_id, score))
            if len(batch) >= SAVE_BATCH:
                saved = await asyncio.to_thread(db_save_scores, self.db_pool, batch, self.logger)
                scored += saved
                self.total_scored += saved
                batch = []
            await asyncio.sleep(0)  # yield to the event loop (health/pause stay responsive)

        if batch:
            saved = await asyncio.to_thread(db_save_scores, self.db_pool, batch, self.logger)
            scored += saved
            self.total_scored += saved
        return scored

    async def worker_loop(self):
        self.logger.info("Worker loop started.")
        while self.client_state == 'play':
            model_id = await self.request_model()
            if model_id is None:
                self.logger.info(f"No work available; retrying in {NO_WORK_POLL_SECONDS}s.")
                await asyncio.sleep(NO_WORK_POLL_SECONDS)
                continue
            scored = await self.process_model(model_id)
            if self.client_state != 'play':
                # Paused before finishing: keep the model (server still owns it for us);
                # we'll resume it on the next play.
                break
            await self.send_data(generate_model_complete_packet(model_id, scored))
            self.logger.info(f"Model {model_id} done (+{scored}, total {self.total_scored}).")
            await asyncio.sleep(0.05)
        self.logger.info("Worker loop exiting (paused/stopped).")

    # ----- lifecycle -----

    async def reconnect(self):
        for attempt in range(5):
            try:
                await self.connect()
                await self.initialize_client()
                self.logger.info("Reconnected.")
                return
            except Exception as e:
                self.logger.error(f"Reconnect attempt {attempt + 1} failed: {e}")
                await asyncio.sleep(5)
        self.logger.error("Max reconnect retries reached.")

    async def run(self):
        try:
            await self.connect()
            receive_task = asyncio.create_task(self.receive_data())
            init_task = asyncio.create_task(self.initialize_client())
            await asyncio.gather(receive_task, init_task)
        except asyncio.CancelledError:
            self.logger.info("Tasks cancelled. Shutting down.")
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")
        finally:
            if getattr(self, 'writer', None):
                self.writer.close()
                await self.writer.wait_closed()

    async def graceful_shutdown(self):
        self.logger.info("Graceful shutdown requested.")
        self.client_state = 'shutdown'
        if self.task_task and not self.task_task.done():
            self.task_task.cancel()
            try:
                await self.task_task
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    config = load_config()
    logger = setup_logger('')

    db_pool = pool.ThreadedConnectionPool(1, 20, **DB_CONFIG)
    server_host = os.environ.get('SERVER_HOST', 'localhost')
    machine_name = os.environ.get('MACHINE_NAME', platform.node() or 'client')
    logger.info(f"Server host: {server_host}, machine: {machine_name}")

    client = ProcessingClient(server_host, machine_name, 5000, logger, config, db_pool)

    def handle_signal(signal_number, frame):
        logger.warning(f"Received signal {signal_number}. Shutting down.")
        try:
            asyncio.run(client.graceful_shutdown())
        except Exception:
            pass

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        asyncio.run(client.run())
    except Exception as e:
        logger.critical(f"Unhandled exception in main: {e}", exc_info=True)
    finally:
        logger.info("Client has exited.")
