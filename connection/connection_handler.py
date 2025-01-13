# server/connection_handler.py

import asyncio
import logging
from messages.base import Message
from messages.constants import MessageType
from authentication.user_auth import UserAuthenticator
from authentication.device_pairing_manager import DevicePairingManager

from connection.broker import Broker
from connection.commands import (
    Command,
    ConnectCommand,
    SubscribeCommand,
    UnsubscribeCommand,
    PublishCommand,
    PubRecCommand,
    PubRelCommand,
    PingReqCommand,
    DisconnectCommand
)

from dataclasses import dataclass, field
from typing import List, Set

from messages import (
    Message,
    ConnectMessage, ConnAckMessage,
    UnsubAckMessage, UnsubscribeMessage,
    SubAckMessage, SubscribeMessage,
    PublishMessage, PingReqMessage, PingRespMessage,
    PubAckMessage, PubRecMessage, PubRelMessage, PubCompMessage,
    DisconnectMessage,
    Header
)

@dataclass
class SessionData:
    subscriptions: Set[str] = field(default_factory=set)
    queued_messages: List[PublishMessage] = field(default_factory=list)
    queued_message_ids: Set[str] = field(default_factory=set)

class MQTTConnectionHandler:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, broker: Broker):
        self.reader = reader
        self.writer = writer
        self.broker = broker
        self.logger = logging.getLogger("MQTTConnectionHandler")
        self.authenticator = self.broker.authenticator  # Use Broker's authenticator
        self.device_manager = self.broker.device_manager
        self.client_id = None
        self.username = None
        self.clean_session = True

    async def dispatch_command(self, message: Message):
        msg_type = message.header.message_type

        command = None

        if msg_type == MessageType.CONNECT:
            command = ConnectCommand(self, message)
        elif msg_type == MessageType.SUBSCRIBE:
            command = SubscribeCommand(self, message)
        elif msg_type == MessageType.UNSUBSCRIBE:
            command = UnsubscribeCommand(self, message)
        elif msg_type == MessageType.PUBLISH:
            command = PublishCommand(self, message)
        elif msg_type == MessageType.PUBREC:
            command = PubRecCommand(self, message)
        elif msg_type == MessageType.PUBREL:
            command = PubRelCommand(self, message)
        elif msg_type == MessageType.PINGREQ:
            command = PingReqCommand(self, message)
        elif msg_type == MessageType.DISCONNECT:
            command = DisconnectCommand(self, message)
        else:
            self.logger.warning(f"Unknown message type: {msg_type}")
            return

        if command:
            await command.execute()

    async def run(self):
        while True:
            try:
                message: Message = await Message.from_reader(self.reader)
                await self.dispatch_command(message)
            except asyncio.IncompleteReadError:
                self.logger.info("Client disconnected unexpectedly.")
                break
            except Exception as e:
                self.logger.error(f"Error handling message: {e}")
                break

        # Cleanup after disconnection
        async with self.broker.lock:
            if self.client_id:
                if not self.clean_session:
                    # Persist session: subscriptions already handled during SUBSCRIBE
                    self.logger.info(f"Persisted session for client_id={self.client_id}")
                else:
                    # Clean session: remove session data
                    if self.client_id in self.broker.sessions:
                        del self.broker.sessions[self.client_id]
                        self.logger.info(f"Removed session for client_id={self.client_id}")

            # Remove writer from subscription registry
            if self.writer in self.broker.client_subscriptions:
                for topic in self.broker.client_subscriptions[self.writer]:
                    self.broker.subscriptions[topic].discard(self.writer)
                    self.logger.info(f"Removed from topic '{topic}' subscriptions")
                del self.broker.client_subscriptions[self.writer]

            # Remove client from connected clients
            if self.client_id in self.broker.connected_clients:
                del self.broker.connected_clients[self.client_id]
            if self.writer in self.broker.writer_to_client_id:
                del self.broker.writer_to_client_id[self.writer]

            self.writer.close()
            await self.writer.wait_closed()
            self.logger.info(f"Connection with {self.client_id} closed.")
