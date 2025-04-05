import asyncio
import json
import logging

class BaseServer:
    def __init__(self, host, port,logger):
        self.host = host
        self.port = port
        self.clients = set()
        self.logger = logger

    async def start(self):
        server = await asyncio.start_server(
            self.handle_client, self.host, self.port)
        
        addr = server.sockets[0].getsockname()
        print(f'Serving on {addr}')
        self.logger.info(f'Serving on {addr}')

        async with server:
            await server.serve_forever()

    async def handle_client(self, reader, writer):
        addr = writer.get_extra_info('peername')
        print(f"New connection from {addr}")
        self.logger.info(f"New connection from {addr}")
        self.clients.add(writer)
        
        try:
            while True:
                data = await self.receive_data(reader)
                if not data:
                    break
                await self.process_data(data, writer)
        except asyncio.CancelledError:
            pass
        finally:
            writer.close()
            await writer.wait_closed()
            self.clients.remove(writer)
            print(f"Connection closed for {addr}")
            self.logger.info(f"Connection closed for {addr}")

    async def receive_data(self, reader):
        # Read the length of the message
        length_bytes = await reader.readexactly(4)
        length = int.from_bytes(length_bytes, 'big')
        
        # Read the JSON data
        data = await reader.readexactly(length)
        return json.loads(data.decode())

    async def send_data(self, data, writer):
        # Serialize the dictionary to JSON
        json_data = json.dumps(data).encode()
        
        # Send the length of the message
        writer.write(len(json_data).to_bytes(4, 'big'))
        
        # Send the JSON data
        writer.write(json_data)
        await writer.drain()

    async def process_data(self, data, writer):
        # This method should be overridden by subclasses
        pass

    async def run(self):
        server = await asyncio.start_server(
            self.handle_client, self.host, self.port)
        
        addr = server.sockets[0].getsockname()
        self.logger.info(f'Serving on {addr}')

        async with server:
            await self.server_main_loop()

    async def server_main_loop(self):
        # This method can be overridden by subclasses to add additional tasks
        await asyncio.Future()  # Run forever