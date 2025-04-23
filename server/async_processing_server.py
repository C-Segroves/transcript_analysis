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

class ProcessingServer(BaseServer):
    def __init__(self, host, port, logger, config):
        super().__init__(host, port, logger)
        self.db_pool = db_pool
        self.task_que = deque()
        self.completed_task_que = deque()
        self.writers = set()  # Track connected clients
        self.clients ={}
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
        self.logger.debug(f"Processing packet of type {packet_type} from {addr} (state: {self.server_state})")
        match packet_type:
            case 'init_packet':
                #{"packet_type": "init_packet", "additional_data": {"machine_name": machine_name}}
                writer_name = packet['additional_data'].get('machine_name', 'Unknown')
                self.logger.info(f"Received init packet from {addr}, machine name: {writer_name}")
                self.writers.add(writer)  # Add client to tracked writers
                self.logger.info(f"Active clients: {len(self.writers)}")
                if self.server_state == 'running':
                    ack_packet = generate_client_play_packet()
                else:
                    ack_packet = generate_client_pause_packet()
                logger.info(f"Sending {ack_packet} packet to {addr}")
                await self.send_data(ack_packet, writer)
            case 'health_packet':
                self.logger.info(f"Received health packet from {addr} , packet: {packet}")
            case _:
                self.logger.error(f'Server: Received unknown packet type: {packet_type} packet')

    async def handle_client(self, reader, writer):
        await super().handle_client(reader, writer)
        self.writers.discard(writer)  # Remove client when disconnected
        self.logger.info(f"Client disconnected. Active clients: {len(self.writers)}")

    async def maintenance_task(self):
        """Runs at 22:56 local time: pauses clients, runs maintenance, resumes clients"""
        while True:
            now = datetime.now()
            if now.hour == 4 and now.minute == 0:
                self.logger.info(f"Maintenance triggered at {now}")
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
            await asyncio.sleep(600)  # Wait for <- that amount over there before the next health check #3600

    async def start(self):
        server = await asyncio.start_server(
            self.handle_client, self.host, self.port)
        
        addr = server.sockets[0].getsockname()
        self.logger.info(f'Serving on {addr}')

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
    
    logger.info("Starting ProcessingServer")
    asyncio.run(server.start())