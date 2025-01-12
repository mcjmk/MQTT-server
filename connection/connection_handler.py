import asyncio
import logging
from messages.base import Message
from messages.connect import ConnectMessage
from messages.constants import MessageType
from authentication.user_auth import UserAuthenticator
from authentication.device_pairing_manager import DevicePairingManager

class MQTTConnectionHandler:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, broker):
        self.reader = reader
        self.writer = writer
        self.broker = broker
        self.logger = logging.getLogger("MQTTConnectionHandler")
        self.authenticator = UserAuthenticator() 
        self.device_manager = DevicePairingManager()

    async def run(self):
        while True:
            try:
                message = await Message.from_reader(self.reader)
                await self.handle_message(message)
            except asyncio.IncompleteReadError:
                self.logger.info("Client disconnected.")
                break
            except Exception as e:
                self.logger.error(f"Error handling message: {e}")
                break 