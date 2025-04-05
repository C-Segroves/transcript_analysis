
import asyncio
import json

class BaseClient:
    def __init__(self, host, port,logger,config):
        self.host = host
        self.port = port
        self.reader = None
        self.writer = None
        self.logger = logger
        self.config=config

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        print(f'Connected to {self.host}:{self.port}')
        self.logger.info(f'Connected to {self.host}:{self.port}')

    async def receive_data(self):
        try:
            while True:
                # Read the length of the message
                length_bytes = await self.reader.readexactly(4)
                length = int.from_bytes(length_bytes, 'big')
                
                # Read the JSON data
                data = await self.reader.readexactly(length)
                message = json.loads(data.decode())
                await self.process_received_data(message)
        except asyncio.CancelledError:
            pass

    async def send_data(self, data):
        # Serialize the dictionary to JSON
        json_data = json.dumps(data).encode()
        
        # Send the length of the message
        self.writer.write(len(json_data).to_bytes(4, 'big'))
        
        # Send the JSON data
        self.writer.write(json_data)
        await self.writer.drain()

    async def process_received_data(self, data):
        # This method should be overridden by subclasses
        pass

    async def initialize_client(self):
        # This method should be overridden by subclasses
        pass

    async def run(self):
        await self.connect()
        receive_task = asyncio.create_task(self.receive_data())
        input_task = asyncio.create_task(self.initialize_client())
        
        try:
            await asyncio.gather(receive_task, input_task)
        finally:
            receive_task.cancel()
            input_task.cancel()
            self.writer.close()
            await self.writer.wait_closed()