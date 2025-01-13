# server/connection_handler.py

import asyncio
import logging
from messages.base import Message
from messages.constants import MessageType
from authentication.user_auth import UserAuthenticator
from authentication.device_pairing_manager import DevicePairingManager

from connection.broker import Broker
from messages.base import MessageFactory
from messages.constants import MessageType

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
        

    async def handle_connect(self, connect_msg: ConnectMessage):
        async with self.broker.lock:
            self.client_id = connect_msg.client_id
            self.username = connect_msg.username
            self.clean_session = connect_msg.clean_session
            self.logger.info(f"CONNECT: client_id={self.client_id}, clean_session={self.clean_session}")

            # Extract username and password if authentication is enabled
            if self.broker.authentication_enabled:
                username = connect_msg.username
                password = connect_msg.password
                if not self.authenticator.login(username, password):
                    # Authentication failed
                    connack_header = Header(MessageType.CONNACK)
                    # Return code 4: bad username or password (per MQTT spec)
                    connack = ConnAckMessage(connack_header, session_present=0, return_code=4)
                    self.writer.write(connack.pack())
                    await self.writer.drain()
                    self.logger.warning(f"Authentication failed for client_id={self.client_id}, username={username}")
                    self.writer.close()
                    await self.writer.wait_closed()
                    return
                else:
                    self.logger.info(f"Authentication successful for client_id={self.client_id}, username={username}")

            if not self.clean_session:
                # Resume or create session
                if self.client_id in self.broker.sessions:
                    session = self.broker.sessions[self.client_id]
                    self.logger.info(f"Resuming session for client_id={self.client_id}")
                else:
                    session = SessionData()
                    self.broker.sessions[self.client_id] = session
                    self.logger.info(f"Creating new session for client_id={self.client_id}")
            else:
                # Clean session: remove existing session if any
                if self.client_id in self.broker.sessions:
                    del self.broker.sessions[self.client_id]
                    self.logger.info(f"Cleared session for client_id={self.client_id}")
                session = SessionData()
                self.broker.sessions[self.client_id] = session

            # Check for existing connection and close it
            if not self.clean_session and self.client_id in self.broker.connected_clients:
                old_writer = self.broker.connected_clients[self.client_id]
                old_peer = old_writer.get_extra_info('peername')
                self.logger.info(f"Closing existing connection for client_id={self.client_id} from {old_peer}")

                # Remove old writer from all subscriptions
                if old_writer in self.broker.client_subscriptions:
                    for topic in self.broker.client_subscriptions[old_writer]:
                        self.broker.subscriptions[topic].discard(old_writer)
                        self.logger.info(f"Removed from topic '{topic}' subscriptions")
                    del self.broker.client_subscriptions[old_writer]

                # Remove from writer_to_client_id
                if old_writer in self.broker.writer_to_client_id:
                    del self.broker.writer_to_client_id[old_writer]

                # Close the old connection
                old_writer.close()
                await old_writer.wait_closed()
                self.logger.info(f"Closed previous connection for client_id={self.client_id}")

            # Mark client as connected
            self.broker.connected_clients[self.client_id] = self.writer
            self.broker.writer_to_client_id[self.writer] = self.client_id

            # Send CONNACK
            connack_header = Header(MessageType.CONNACK)
            connack = ConnAckMessage(connack_header, session_present=0, return_code=0)
            self.writer.write(connack.pack())
            await self.writer.drain()
            self.logger.info(f"Sent CONNACK to {self.client_id}")

            # Queued messages will be delivered after SUBSCRIBE

    async def handle_subscribe(self, subscribe_msg: SubscribeMessage):
        async with self.broker.lock:
            self.logger.info(f"SUBSCRIBE: packet_id={subscribe_msg.packet_id}, topics={subscribe_msg.subscriptions}")
            granted_qos = []
            for topic, qos in subscribe_msg.subscriptions:
                # If authentication is enabled, check if the client is authorized to subscribe to the topic
                if self.broker.authentication_enabled:
                    username = self.username  # Assuming client_id is the username
                    if not self.broker.device_manager.is_topic_authorized(username, topic):
                        granted_qos.append(0x80)  # 0x80 indicates failure to subscribe
                        self.logger.warning(f"Subscription to topic '{topic}' failed for user '{username}'")
                        continue
                if topic not in self.broker.client_subscriptions[self.writer]:
                    self.broker.subscriptions[topic].add(self.writer)
                    self.broker.client_subscriptions[self.writer].add(topic)
                    granted_qos.append(qos)
                    self.logger.info(f"Subscribed to topic '{topic}' with QoS {qos}")

                    # Add to session subscriptions
                    if self.client_id and not self.clean_session:
                        self.broker.sessions[self.client_id].subscriptions.add(topic)
                else:
                    # Already subscribed, update QoS if needed
                    granted_qos.append(qos)
                    self.logger.info(f"Already subscribed to topic '{topic}', updated QoS to {qos}")

            # Send SUBACK
            suback_header = Header(MessageType.SUBACK)
            suback = SubAckMessage(suback_header, subscribe_msg.packet_id, granted_qos)
            self.writer.write(suback.pack())
            await self.writer.drain()
            self.logger.info(f"Sent SUBACK for packet_id={subscribe_msg.packet_id}")

            # Deliver queued messages after SUBACK
            if self.client_id and not self.clean_session:
                session = self.broker.sessions.get(self.client_id)
                if session and session.queued_messages:
                    for queued_msg in session.queued_messages:
                        self.writer.write(queued_msg.pack())
                        await self.writer.drain()
                        self.logger.info(f"Delivered queued message to topic '{queued_msg.topic}'")
                    # Clear queued messages and their IDs after delivery
                    session.queued_messages.clear()
                    session.queued_message_ids.clear()

    async def handle_unsubscribe(self, unsubscribe_msg: UnsubscribeMessage):
        async with self.broker.lock:
            self.logger.info(f"UNSUBSCRIBE: packet_id={unsubscribe_msg.packet_id}, topics={unsubscribe_msg.topics}")

            for topic in unsubscribe_msg.topics:
                if topic in self.broker.client_subscriptions[self.writer]:
                    self.broker.subscriptions[topic].discard(self.writer)
                    self.broker.client_subscriptions[self.writer].discard(topic)
                    self.logger.info(f"Unsubscribed from topic '{topic}'")

                    # Remove from session subscriptions
                    if self.client_id and not self.clean_session:
                        self.broker.sessions[self.client_id].subscriptions.discard(topic)
                else:
                    self.logger.info(f"Attempted to unsubscribe from non-subscribed topic '{topic}'")

            # Send UNSUBACK
            unsuback_header = Header(MessageType.UNSUBACK)
            unsuback = UnsubAckMessage(unsuback_header, unsubscribe_msg.packet_id)
            self.writer.write(unsuback.pack())
            await self.writer.drain()
            self.logger.info(f"Sent UNSUBACK for packet_id={unsubscribe_msg.packet_id}")

    async def handle_publish(self, publish_msg: PublishMessage):
        async with self.broker.lock:
            topic = publish_msg.topic
            payload = publish_msg.payload
            qos = publish_msg.header.qos

            try:
                payload_str = payload.decode('utf-8')
            except UnicodeDecodeError:
                payload_str = payload.decode('utf-8', 'ignore')

            self.logger.info(f"PUBLISH: topic='{topic}', payload='{payload_str}', QoS={qos}")

            # Forward the PUBLISH to all connected subscribers except the sender
            if topic in self.broker.subscriptions:
                for subscriber in list(self.broker.subscriptions[topic]):
                    if subscriber != self.writer:
                        subscriber_client_id = self.broker.writer_to_client_id.get(subscriber)

                        if subscriber_client_id and subscriber_client_id in self.broker.connected_clients:
                            # Subscriber is connected
                            try:
                                subscriber.write(publish_msg.pack())
                                await subscriber.drain()
                                self.logger.info(f"Forwarded PUBLISH to {subscriber.get_extra_info('peername')}")
                            except Exception as e:
                                self.logger.error(f"Error forwarding PUBLISH to {subscriber.get_extra_info('peername')}: {e}")
            else:
                self.logger.info(f"No connected subscribers for topic '{topic}'")

            # Iterate through all sessions to find offline subscribers subscribed to the topic
            for offline_client_id, session in self.broker.sessions.items():
                if offline_client_id not in self.broker.connected_clients and topic in session.subscriptions:
                    # Queue the message based on QoS
                    if qos in [1, 2]:
                        # Generate a unique message_id
                        message_id = f"{publish_msg.packet_id}-{topic}-{payload_str}"
                        # Check if the message is already queued
                        if message_id not in session.queued_message_ids:
                            session.queued_messages.append(publish_msg)
                            session.queued_message_ids.add(message_id)
                            self.logger.info(f"Queued PUBLISH for offline client_id={offline_client_id}, topic='{topic}'")
                        else:
                            self.logger.info(f"PUBLISH already queued for client_id={offline_client_id}, topic='{topic}'")

            # Handle QoS acknowledgments
            if qos == 1:
                # Respond with PUBACK
                puback_header = Header(MessageType.PUBACK)
                puback = PubAckMessage(puback_header, publish_msg.packet_id)
                self.writer.write(puback.pack())
                await self.writer.drain()
                self.logger.info(f"Sent PUBACK for packet_id={publish_msg.packet_id}")

            elif qos == 2:
                # Respond with PUBREC
                pubrec_header = Header(MessageType.PUBREC)
                pubrec = PubRecMessage(pubrec_header, publish_msg.packet_id)
                self.writer.write(pubrec.pack())
                await self.writer.drain()
                self.logger.info(f"Sent PUBREC for packet_id={publish_msg.packet_id}")

    async def handle_pubrec(self, pubrec_msg: PubRecMessage):
        async with self.broker.lock:
            self.logger.info(f"PUBREC: packet_id={pubrec_msg.packet_id}")

            # Respond with PUBREL
            pubrel_header = Header(MessageType.PUBREL, qos=1)  # PUBREL must have fixed-header QoS=1
            pubrel = PubRelMessage(pubrel_header, pubrec_msg.packet_id)
            self.writer.write(pubrel.pack())
            await self.writer.drain()
            self.logger.info(f"Sent PUBREL for packet_id={pubrec_msg.packet_id}")

    async def handle_pubrel(self, pubrel_msg: PubRelMessage):
        async with self.broker.lock:
            self.logger.info(f"PUBREL: packet_id={pubrel_msg.packet_id}")

            # Respond with PUBCOMP
            pubcomp_header = Header(MessageType.PUBCOMP)
            pubcomp = PubCompMessage(pubcomp_header, pubrel_msg.packet_id)
            self.writer.write(pubcomp.pack())
            await self.writer.drain()
            self.logger.info(f"Sent PUBCOMP for packet_id={pubrel_msg.packet_id}")

    async def handle_pingreq(self):
        async with self.broker.lock:
            self.logger.info("PINGREQ received")

            # Respond with PINGRESP
            pingresp_header = Header(MessageType.PINGRESP)
            pingresp = PingRespMessage(pingresp_header)
            self.writer.write(pingresp.pack())
            await self.writer.drain()
            self.logger.info("Sent PINGRESP")

    async def handle_pingresp(self):
        self.logger.info("PINGRESP received (unexpected as server)")

    async def handle_pubcomp(self, pubcomp_msg: PubCompMessage):
        self.logger.info(f"PUBCOMP: packet_id={pubcomp_msg.packet_id}")

    async def handle_disconnect(self):
        async with self.broker.lock:
            self.logger.info(f"DISCONNECT from client_id={self.client_id}")
            self.writer.close()
            await self.writer.wait_closed()

    async def handle_message(self, message: Message):
        msg_type = message.header.message_type

        if msg_type == MessageType.CONNECT:
            await self.handle_connect(message)
        elif msg_type == MessageType.SUBSCRIBE:
            await self.handle_subscribe(message)
        elif msg_type == MessageType.UNSUBSCRIBE:
            await self.handle_unsubscribe(message)
        elif msg_type == MessageType.PUBLISH:
            await self.handle_publish(message)
        elif msg_type == MessageType.PUBREC:
            await self.handle_pubrec(message)
        elif msg_type == MessageType.PUBREL:
            await self.handle_pubrel(message)
        elif msg_type == MessageType.PUBCOMP:
            await self.handle_pubcomp(message)
        elif msg_type == MessageType.PINGREQ:
            await self.handle_pingreq()
        elif msg_type == MessageType.PINGRESP:
            await self.handle_pingresp()
        elif msg_type == MessageType.DISCONNECT:
            await self.handle_disconnect()
        else:
            self.logger.warning(f"Unknown message type: {msg_type}")

    async def run(self):
        while True:
            try:
                message: Message = await Message.from_reader(self.reader)
                await self.handle_message(message)
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
