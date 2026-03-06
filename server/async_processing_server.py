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