# server/server.py

import asyncio
import logging

from connection.connection_handler import MQTTConnectionHandler
from utils.singleton import Singleton

from connection.broker import Broker

class Server(metaclass=Singleton):
    def __init__(self, host='127.0.0.1', port=1884, authentication=False):
        self.host = host
        self.port = port
        self.authentication = authentication
        self.broker = Broker(authentication=authentication)
        self.server = None
        self.logger = logging.getLogger("Server")

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        handler = MQTTConnectionHandler(reader, writer, self.broker)
        await handler.run()

    async def start_server(self):
        self.server = await asyncio.start_server(
            self.handle_client,
            self.host,
            self.port
        )
        addr = self.server.sockets[0].getsockname()
        self.logger.info(f"Server listening on {addr}. Authentication: {self.authentication}")

        async with self.server:
            await self.server.serve_forever()

    def run(self):
        # Configure logging for the server if not already configured
        if not logging.getLogger().hasHandlers():
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
        try:
            asyncio.run(self.start_server())
        except KeyboardInterrupt:
            self.logger.info("Server shut down via KeyboardInterrupt.")
