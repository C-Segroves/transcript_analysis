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
    with db_pool.getconn() as conn:
        logger.info('Server: clearing out old assigned tasks.')
        cursor = conn.cursor()
        cursor.execute("UPDATE VID_MODEL_STATE SET STATE = NULL WHERE STATE = 'pending';")
        conn.commit()
        db_pool.putconn(conn)

def generate_client_play_packet():
    return {"packet_type": "play", "additional_data": {}}

def generate_client_pause_packet():
    return {"packet_type": "pause", "additional_data": {}}

def mark_tasks_complete(db_pool, vid_id, results, logger):
    with db_pool.getconn() as conn:
        cursor = conn.cursor()
        update_query = """
            UPDATE VID_MODEL_STATE 
            SET STATE = 'complete'
            WHERE VID_ID = %s AND MODEL_KEY = %s
        """
        for result in results:
            model_key = result['model_key']
            cursor.execute(update_query, (vid_id, model_key))
        conn.commit()
        num_models = len(results)
        db_pool.putconn(conn)
        logger.info(f"Task completed for VID_ID: {vid_id} for {num_models} models.")

class ProcessingServer(BaseServer):
    def __init__(self, host, port, logger, config):
        super().__init__(host, port, logger)
        self.db_pool = db_pool
        self.task_que = deque()
        self.completed_task_que = deque()
        self.writers = set()  # Track connected clients
        self.server_state = 'running'

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
        self.logger.info(f"Processing packet of type {packet_type} from {addr} (state: {self.server_state})")
        match packet_type:
            case 'init_packet':
                self.logger.info(f"Received init packet from {addr}")
                self.writers.add(writer)  # Add client to tracked writers
                self.logger.info(f"Active clients: {len(self.writers)}")
                if self.server_state == 'running':
                    ack_packet = generate_client_play_packet()
                else:
                    ack_packet = generate_client_pause_packet()
                logger.info(f"Sending {ack_packet} packet to {addr}")
                await self.send_data(ack_packet, writer)
            case _:
                self.logger.error(f'Server: Received {packet_type} packet')

    async def handle_client(self, reader, writer):
        await super().handle_client(reader, writer)
        self.writers.discard(writer)  # Remove client when disconnected
        self.logger.info(f"Client disconnected. Active clients: {len(self.writers)}")

    async def maintenance_task(self):
        while True:
            now = datetime.now()
            self.logger.debug(f"Current time: {now.hour}:{now.minute}")
            if now.hour == 3 and now.minute == 12:
                self.logger.info(f"Maintenance triggered at {now}")
                self.server_state = 'maintenance'

                # Send pause packets to all clients
                self.logger.info(f"Sending pause packets to {len(self.writers)} clients")
                pause_packet = generate_client_pause_packet()
                for writer in self.writers.copy():
                    try:
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

            # Sleep to prevent tight loop
            await asyncio.sleep(30)  # Check every 30 seconds

    async def start(self):
        server = await asyncio.start_server(
            self.handle_client, self.host, self.port)
        
        addr = server.sockets[0].getsockname()
        self.logger.info(f'Serving on {addr}')

        # Create tasks
        maintenance_task = asyncio.create_task(self.maintenance_task())
        server_task = asyncio.create_task(server.serve_forever())

        try:
            await asyncio.gather(maintenance_task, server_task)
        except asyncio.CancelledError:
            self.logger.info("Server is shutting down...")
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")
        finally:
            maintenance_task.cancel()
            server_task.cancel()
            self.logger.info("Server shut down complete.")

if __name__ == "__main__":
    host = socket.gethostbyname(socket.gethostname())
    port = 5000

    db_pool = pool.ThreadedConnectionPool(1, 20, **DB_CONFIG)
    config = load_config()
    logger = setup_logger('')
    print(logger)

    server = ProcessingServer(host, port, logger, config)
    print(server)

    clear_assigned_tasks(db_pool)
    
    logger.info("Starting ProcessingServer")
    asyncio.run(server.start())