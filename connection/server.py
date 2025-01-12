import asyncio
import logging
from .connection_handler import MQTTConnectionHandler
from .mqtt_broker import MQTTBroker 

log = logging.getLogger(__name__)

class MQTTServer:
    def __init__(self, host='0.0.0.0', port=1883):
        self.host = host
        self.port = port
        self.server = None
        self.logger = logging.getLogger("MQTTServer")
        self.broker = MQTTBroker()

    async def start(self):
        self.server = await asyncio.start_server(
            self._handle_connection, 
            self.host, 
            self.port
        )
        self.logger.info(f"MQTT server listening on {self.host}:{self.port}")
        async with self.server:
            await self.server.serve_forever()

    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        address = writer.get_extra_info('peername')
        self.logger.info(f"New connection from {address}")
        handler = MQTTConnectionHandler(reader, writer, self.broker)
        await handler.run()

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.logger.info("MQTTServer stopped")
    
    def run(self):
        try:
            asyncio.run(self.start())
        except KeyboardInterrupt:
            self.logger.info("Shutting down...")
            asyncio.run(self.stop())
