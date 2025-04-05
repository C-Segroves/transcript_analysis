import asyncio
import json
import logging
from datetime import datetime, time
import pytz
from psycopg2 import pool
import aiopg  # Using aiopg for async PostgreSQL support
from async_server import BaseServer
from maintain_database import maintain_database

class ServerState:
    RUNNING = "running"
    MAINTENANCE = "maintenance"
    PAUSED = "paused"

class TaskServer(BaseServer):
    def __init__(self, host, port, logger):
        super().__init__(host, port, logger)
        self.state = ServerState.RUNNING
        self.db_pool = None
        self.tasks = asyncio.Queue()
        self.maintenance_tasks = []
        
    async def initialize_db(self):
        """Initialize the async database connection pool"""
        try:
            db_config = self.load_config()
            # Using aiopg for async PostgreSQL connections
            self.db_pool = await aiopg.create_pool(
                host=db_config['host'],
                port=db_config['port'],
                dbname=db_config['dbname'],
                user=db_config['user'],
                password=db_config['password'],
                minsize=1,
                maxsize=20
            )
            self.logger.info("Database connection pool initialized")
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {e}")
            raise

    def load_config(self):
        """Load database configuration from JSON file"""
        try:
            with open('config/db_config.json', 'r') as f:
                return json.load(f)
        except FileNotFoundError as e:
            self.logger.error(f"Database config file not found: {e}")
            raise
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in db_config.json: {e}")
            raise

    async def start(self):
        """Start the server with database initialization"""
        await self.initialize_db()
        await super().start()

    async def process_data(self, data, writer):
        """Process incoming client messages"""
        message_type = data.get('type')
        
        if self.state == ServerState.MAINTENANCE:
            await self.send_data({"status": "maintenance", "message": "Server in maintenance mode"}, writer)
            return

        if message_type == "request_task":
            await self.handle_task_request(writer)
        elif message_type == "task_complete":
            await self.handle_task_completion(data, writer)

    async def handle_task_request(self, writer):
        """Handle client task requests"""
        if not self.tasks.empty():
            task = await self.tasks.get()
            await self.send_data({"status": "task_assigned", "task": task}, writer)
        else:
            await self.send_data({"status": "no_tasks", "message": "No tasks available"}, writer)

    async def handle_task_completion(self, data, writer):
        """Handle completed tasks from clients"""
        async with self.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "UPDATE tasks SET status = %s, completed_at = %s WHERE task_id = %s",
                    ("completed", datetime.now(), data["task_id"])
                )
        await self.send_data({"status": "task_received"}, writer)

    async def server_main_loop(self):
        """Main server loop with maintenance scheduling"""
        while True:
            now = datetime.now(pytz.timezone('US/Eastern'))
            midnight = datetime.combine(now.date(), time(0, 0)).replace(tzinfo=pytz.timezone('US/Eastern'))
            
            if now.hour == 0 and self.state != ServerState.MAINTENANCE:
                await self.start_maintenance()
            elif now.hour == 1 and self.state == ServerState.MAINTENANCE:
                await self.end_maintenance()
                
            await asyncio.sleep(60)  # Check every minute

    async def start_maintenance(self):
        """Start maintenance mode"""
        self.state = ServerState.MAINTENANCE
        self.logger.info("Starting maintenance mode")
        
        # Notify all clients to pause
        for client in self.clients:
            await self.send_data({"status": "pause", "message": "Server maintenance starting"}, client)
        
        # Perform maintenance tasks
        async with self.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                # Example maintenance task: clean up old completed tasks
                await cur.execute(
                    "DELETE FROM tasks WHERE status = 'completed' AND completed_at < NOW() - INTERVAL '30 days'"
                )
                self.logger.info("Maintenance: Cleaned up old tasks")
        
        await self.end_maintenance()



    async def end_maintenance(self):
        """End maintenance mode"""
        self.state = ServerState.RUNNING
        self.logger.info("Ending maintenance mode")
        
        # Notify all clients to resume
        for client in self.clients:
            await self.send_data({"status": "resume", "message": "Server maintenance complete"}, client)

    async def add_task(self, task_data):
        """Add a new task to the queue"""
        async with self.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO tasks (data, status) VALUES (%s, %s) RETURNING task_id",
                    (json.dumps(task_data), "pending")
                )
                task_id = await cur.fetchone()
                await self.tasks.put({"task_id": task_id[0], "data": task_data})

    async def run(self):
        """Run the server with maintenance loop"""
        await self.initialize_db()
        server = await asyncio.start_server(
            self.handle_client, self.host, self.port)
        
        addr = server.sockets[0].getsockname()
        self.logger.info(f'Serving on {addr}')

        async with server:
            await asyncio.gather(
                server.serve_forever(),
                self.server_main_loop()
            )

# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("TaskServer")
    
    server = TaskServer("localhost", 8888, logger)
    
    asyncio.run(server.run())