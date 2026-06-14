import asyncio
import json
import socket
import logging
import os
import platform
from collections import deque
import threading
import psycopg2
from psycopg2 import pool
from datetime import datetime, time

from async_server import BaseServer
import maintain_database
import dashboard

def load_config():
    try:
        with open('config/db_config.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError as e:
        logger.error(f"Database config file not found: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in db_config.json: {e}")
        raise

DB_CONFIG = load_config()
task_queue = []

def setup_logger(log_file_path):
    logger = logging.getLogger('ProcessingServer')
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

def clear_assigned_tasks(db_pool):
    """No-op function - state column removed. Tasks are determined by checking transcripts and scores."""
    logger.info('Server: clear_assigned_tasks called (no-op - state column removed)')
    pass

def generate_client_play_packet():
    return {"packet_type": "play", "additional_data": {}}

def generate_client_pause_packet():
    return {"packet_type": "pause", "additional_data": {}}

class ProcessingServer(BaseServer):
    def __init__(self, host, port, logger, config):
        super().__init__(host, port, logger)
        self.db_pool = db_pool
        self.task_que = deque()
        self.completed_task_que = deque()
        self.writers = set()  # Track connected clients
        self.clients ={}
        self.server_state = 'running'
        self.last_maintenance_hour = None  # Track last maintenance hour to prevent re-triggering

        # ----- model-major work coordination (in-memory) -----
        # all_model_ids: every model_id that exists (loaded from model_table)
        # completed_models: models with no remaining pending videos (per clients)
        # model_owner: model_id -> writer currently assigned that model (at most one)
        # client_info: writer -> {machine_name, loaded:set, assigned:set, scored:int}
        self.all_model_ids = []
        self.completed_models = set()
        self.model_owner = {}
        self.client_info = {}

    def _client_record(self, writer):
        rec = self.client_info.get(writer)
        if rec is None:
            rec = {"machine_name": "?", "loaded": set(), "assigned": set(), "scored": 0}
            self.client_info[writer] = rec
        return rec

    def load_all_model_ids(self):
        """Load every model_id from the DB. Safe to call again to pick up new models."""
        conn = self.db_pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM model_table ORDER BY id")
                self.all_model_ids = [r[0] for r in cur.fetchall()]
        finally:
            self.db_pool.putconn(conn)
        self.logger.info(f"Loaded {len(self.all_model_ids)} model id(s) for assignment.")

    def pick_model_for(self, writer, loaded_models):
        """
        Choose a model for this client. Rules:
          1. If the client already owns an unfinished model, hand the same one back
             (so a client resuming after a pause continues its model).
          2. Otherwise assign a model that no other client owns and that isn't
             complete, preferring one the client already has loaded (affinity).
        Returns a model_id or None if nothing is available.
        """
        rec = self._client_record(writer)
        for m in rec["assigned"]:
            if m not in self.completed_models:
                return m  # resume already-owned model
        loaded_set = set(loaded_models or [])
        available = [m for m in self.all_model_ids
                     if m not in self.model_owner and m not in self.completed_models]
        if not available:
            return None
        choice = next((m for m in available if m in loaded_set), available[0])
        self.model_owner[choice] = writer
        rec["assigned"].add(choice)
        return choice

    def handle_model_complete(self, writer, model_id, scored_count=0):
        if model_id is None:
            return
        self.completed_models.add(model_id)
        if self.model_owner.get(model_id) is writer:
            self.model_owner.pop(model_id, None)
        rec = self._client_record(writer)
        rec["assigned"].discard(model_id)
        rec["scored"] += int(scored_count or 0)
        self.logger.info(
            f"Model {model_id} complete from {rec['machine_name']} "
            f"(+{scored_count} scored). {len(self.completed_models)}/{len(self.all_model_ids)} models done."
        )

    def release_client(self, writer):
        """Free a disconnected client's model assignments so others can pick them up."""
        rec = self.client_info.pop(writer, None)
        if not rec:
            return
        for m in list(rec["assigned"]):
            if self.model_owner.get(m) is writer:
                self.model_owner.pop(m, None)
        self.logger.info(f"Released assignments for {rec.get('machine_name', '?')}: {sorted(rec['assigned'])}")

    def get_worker_snapshot(self):
        """Read-only snapshot for the dashboard (runs in another thread)."""
        clients = []
        for rec in list(self.client_info.values()):
            clients.append({
                "machine_name": rec.get("machine_name", "?"),
                "loaded": sorted(rec.get("loaded", [])),
                "assigned": sorted(rec.get("assigned", [])),
                "scored": rec.get("scored", 0),
            })
        return {
            "server_state": self.server_state,
            "total_models": len(self.all_model_ids),
            "completed_models": len(self.completed_models),
            "busy_models": len(self.model_owner),
            "active_clients": len(self.writers),
            "clients": clients,
        }

    async def send_data(self, data, writer):
        try:
            json_data = json.dumps(data).encode()
            length = len(json_data)
            writer.write(length.to_bytes(4, 'big'))
            writer.write(json_data)
            await writer.drain()
            self.logger.debug(f"Sent {data} to client")
        except Exception as e:
            self.logger.error(f"Error sending data: {e}")

    async def process_data(self, packet, writer):
        addr = writer.get_extra_info('peername')
        packet_type = packet['packet_type']
        self.logger.debug(f"Processing packet of type {packet_type} from {addr} (state: {self.server_state})")
        match packet_type:
            case 'init_packet':
                #{"packet_type": "init_packet", "additional_data": {"machine_name": machine_name}}
                writer_name = packet['additional_data'].get('machine_name', 'Unknown')
                self.logger.info(f"Received init packet from {addr}, machine name: {writer_name}")
                self.writers.add(writer)  # Add client to tracked writers
                self._client_record(writer)["machine_name"] = writer_name
                self.logger.info(f"Active clients: {len(self.writers)}")
                if self.server_state == 'running':
                    ack_packet = generate_client_play_packet()
                else:
                    ack_packet = generate_client_pause_packet()
                logger.info(f"Sending {ack_packet} packet to {addr}")
                await self.send_data(ack_packet, writer)
            case 'work_request':
                # Client asks for a model to work on. It reports the models it
                # currently has loaded so we can prefer one it already holds.
                ad = packet.get('additional_data', {}) or {}
                rec = self._client_record(writer)
                if ad.get('machine_name'):
                    rec['machine_name'] = ad['machine_name']
                rec['loaded'] = set(ad.get('loaded_models', []) or [])
                if self.server_state != 'running':
                    await self.send_data({'packet_type': 'no_work', 'additional_data': {}}, writer)
                else:
                    model_id = self.pick_model_for(writer, rec['loaded'])
                    if model_id is None:
                        await self.send_data({'packet_type': 'no_work', 'additional_data': {}}, writer)
                    else:
                        self.logger.info(f"Assigned model {model_id} to {rec['machine_name']} ({addr}).")
                        await self.send_data(
                            {'packet_type': 'assignment', 'additional_data': {'model_id': model_id}}, writer
                        )
            case 'model_complete':
                ad = packet.get('additional_data', {}) or {}
                self.handle_model_complete(writer, ad.get('model_id'), ad.get('scored_count', 0))
            case 'model_progress':
                ad = packet.get('additional_data', {}) or {}
                rec = self._client_record(writer)
                rec['scored'] = int(ad.get('total_scored', rec['scored']) or rec['scored'])
            case 'health_packet':
                self.logger.info(f"Received health packet from {addr} , packet: {packet}")
            case _:
                self.logger.error(f'Server: Received unknown packet type: {packet_type} packet')

    async def handle_client(self, reader, writer):
        await super().handle_client(reader, writer)
        self.writers.discard(writer)  # Remove client when disconnected
        self.release_client(writer)   # Free its model assignments for others
        self.logger.info(f"Client disconnected. Active clients: {len(self.writers)}")

    async def _run_maintenance(self, now):
        """Helper method to run the maintenance process."""
        self.logger.info(f"Maintenance triggered at {now}")
        self.last_maintenance_hour = now.hour  # Mark that we've run maintenance this hour
        self.server_state = 'maintenance'

        # Send pause packets to all clients
        self.logger.info(f"Sending pause packets to {len(self.writers)} clients")
        pause_packet = generate_client_pause_packet()
        for writer in self.writers.copy():
            try:
                logger.info(f"Sending {pause_packet} packet to {writer.get_extra_info('peername')}")
                await self.send_data(pause_packet, writer)
            except Exception as e:
                self.logger.error(f"Failed to send pause to client: {e}")
                self.writers.discard(writer)

        # Run maintenance
        self.logger.info("Running maintenance")
        try:
            await asyncio.to_thread(maintain_database.maintain_database, "/app/config/YouTube.txt", self.logger)
        except Exception as e:
            self.logger.error(f"Maintenance failed: {e}")

        # New videos may have been added, so models that were 'complete' can have
        # pending work again. Reset completion and refresh the model list.
        self.completed_models.clear()
        try:
            await asyncio.to_thread(self.load_all_model_ids)
        except Exception as e:
            self.logger.error(f"Failed to reload model ids after maintenance: {e}")

        # Send play packets to all clients
        self.logger.info(f"Sending play packets to {len(self.writers)} clients")
        self.server_state = 'running'  # Fixed typo from 'run' to 'running'
        play_packet = generate_client_play_packet()
        for writer in self.writers.copy():
            try:
                await self.send_data(play_packet, writer)
            except Exception as e:
                self.logger.error(f"Failed to send play to client: {e}")
                self.writers.discard(writer)
        
        self.logger.info(f"Maintenance completed at {datetime.now()}")

    async def maintenance_task(self):
        """Runs at 4:00 AM local time: pauses clients, runs maintenance, resumes clients"""
        self.logger.info("Maintenance task started - will run at 4:00 AM local time")
        startup_check_done = False
        try:
            while True:
                now = datetime.now()
                current_hour = now.hour
                current_minute = now.minute
                
                # On first startup, check if we should run maintenance immediately
                # (if it's past 4:00 AM and we haven't run today)
                if not startup_check_done:
                    startup_check_done = True
                    self.logger.info(f"Startup check: Current time is {current_hour}:{current_minute:02d}, last_maintenance_hour={self.last_maintenance_hour}")
                    # Run maintenance if:
                    # 1. It's 4:00 AM or later (any time after 4:00 AM)
                    # 2. We haven't run maintenance for hour 4 today
                    if current_hour >= 4 and self.last_maintenance_hour != 4:
                        self.logger.info(f"Startup check: It's {current_hour}:{current_minute:02d}, past 4:00 AM. Running maintenance now (missed scheduled time).")
                        # Run maintenance immediately
                        await self._run_maintenance(now)
                        continue  # Skip to next iteration after running maintenance
                    elif current_hour == 4 and current_minute < 2:
                        # We're in the 4:00-4:02 window, let the normal check handle it
                        self.logger.info(f"Startup check: It's {current_hour}:{current_minute:02d}, in the 4:00 AM window. Normal maintenance check will handle it.")
                    else:
                        self.logger.info(f"Startup check: Current time {current_hour}:{current_minute:02d}, maintenance will run at next 4:00 AM")
                
                # Log every hour on the hour to show the task is running
                if current_minute == 0 and now.second < 30:
                    self.logger.info(f"Maintenance task active - current time: {now.strftime('%Y-%m-%d %H:%M:%S')}, waiting for 4:00 AM")
                
                # Check if it's 4:00 AM (within the first 2 minutes) and we haven't run maintenance this hour
                if current_hour == 4 and current_minute < 2 and self.last_maintenance_hour != current_hour:
                    await self._run_maintenance(now)
                elif current_hour != 4:
                    # Reset the flag when we're past 4 AM to allow next day's run
                    if self.last_maintenance_hour == 4:
                        self.last_maintenance_hour = None
                        self.logger.debug("Reset maintenance flag - ready for next day")

                # Sleep to prevent tight loop
                # Check more frequently around 4 AM to catch the trigger
                if current_hour == 3 and current_minute >= 58:
                    await asyncio.sleep(10)  # Check every 10 seconds when approaching 4 AM
                elif current_hour == 4 and current_minute < 5:
                    await asyncio.sleep(10)  # Check every 10 seconds during the first 5 minutes of 4 AM
                else:
                    await asyncio.sleep(30)  # Check every 30 seconds otherwise
        except Exception as e:
            self.logger.error(f"Critical error in maintenance_task: {e}", exc_info=True)
            # Re-raise to let the caller know
            raise

    async def health_check_task(self):
        """Runs every {some period} to send health_check packets to all clients."""
        while True:
            self.logger.info("Starting health check for all clients.")
            health_check_packet = {"packet_type": "health_check", "additional_data": {}}

            for writer in self.writers.copy():
                addr = writer.get_extra_info('peername')
                try:
                    self.logger.info(f"Sending health_check packet to {addr}")
                    await self.send_data(health_check_packet, writer)
                except Exception as e:
                    self.logger.error(f"Error sending health_check to client {addr}: {e}")
                    self.writers.discard(writer)  # Remove client on error

            self.logger.info("Hourly health check completed.")
            await asyncio.sleep(3600)  # Wait for <- that amount over there before the next health check #3600

    async def start(self):
        server = await asyncio.start_server(
            self.handle_client, self.host, self.port)

        addr = server.sockets[0].getsockname()
        self.logger.info(f'Serving on {addr}')

        # Load the set of models we can hand out before accepting work requests.
        try:
            await asyncio.to_thread(self.load_all_model_ids)
        except Exception as e:
            self.logger.error(f"Failed to load model ids at startup: {e}")

        # Create tasks
        maintenance_task = asyncio.create_task(self.maintenance_task())
        health_check_task = asyncio.create_task(self.health_check_task())
        server_task = asyncio.create_task(server.serve_forever())

        try:
            await asyncio.gather(maintenance_task, server_task)
        except asyncio.CancelledError:
            self.logger.info("Server is shutting down...")
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")
        finally:
            maintenance_task.cancel()
            health_check_task.cancel()
            server_task.cancel()
            self.logger.info("Server shut down complete.")

if __name__ == "__main__":
    host = socket.gethostbyname(socket.gethostname())
    port = 5000

    db_pool = pool.ThreadedConnectionPool(1, 20, **DB_CONFIG)
    config = load_config()
    logger = setup_logger('')

    server = ProcessingServer(host, port, logger, config)

    clear_assigned_tasks(db_pool)

    # Start the web dashboard (stats + add-channel) in a background thread so a
    # single container serves both the task server (5000) and the web UI.
    dashboard_port = int(os.getenv("DASHBOARD_PORT", "8080"))
    try:
        dashboard.start_dashboard_in_thread(
            DB_CONFIG,
            api_key_path="config/YouTube.txt",
            logger=logger,
            host="0.0.0.0",
            port=dashboard_port,
            status_provider=server.get_worker_snapshot,
        )
    except Exception as e:
        logger.error(f"Failed to start dashboard web UI: {e}")

    logger.info("Starting ProcessingServer")
    asyncio.run(server.start())