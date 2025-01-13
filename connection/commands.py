# connection/commands.py

from abc import ABC, abstractmethod
import logging

from messages.constants import MessageType
from connection.broker import Broker, SessionData
from messages import (
    ConnectMessage, ConnAckMessage,
    SubscribeMessage, SubAckMessage,
    UnsubscribeMessage, UnsubAckMessage,
    PublishMessage, PubAckMessage, PubRecMessage,
    PubRelMessage, PubCompMessage,
    PingRespMessage,
    Header
)

class Command(ABC):
    def __init__(self, handler, message):
        self.handler = handler
        self.message = message
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    async def execute(self):
        pass

class ConnectCommand(Command):
    async def execute(self):
        connect_msg: ConnectMessage = self.message
        broker: Broker = self.handler.broker

        async with broker.lock:
            self.handler.client_id = connect_msg.client_id
            self.handler.username = connect_msg.username
            self.handler.clean_session = connect_msg.clean_session
            self.logger.info(f"CONNECT: client_id={self.handler.client_id}, clean_session={self.handler.clean_session}")

            # Authentication
            if broker.authentication_enabled:
                username = connect_msg.username
                password = connect_msg.password
                if not broker.authenticator.login(username, password):
                    # Authentication failed
                    connack = ConnAckMessage(
                        header=Header(MessageType.CONNACK),
                        session_present=0,
                        return_code=4  # Bad Username or Password
                    )
                    self.handler.writer.write(connack.pack())
                    await self.handler.writer.drain()
                    self.logger.warning(f"Authentication failed for client_id={self.handler.client_id}, username={username}")
                    self.handler.writer.close()
                    await self.handler.writer.wait_closed()
                    return

                self.logger.info(f"Authentication successful for client_id={self.handler.client_id}, username={username}")

            # Session Handling
            if not self.handler.clean_session:
                if self.handler.client_id in broker.sessions:
                    session = broker.sessions[self.handler.client_id]
                    self.logger.info(f"Resuming session for client_id={self.handler.client_id}")
                else:
                    session = SessionData()
                    broker.sessions[self.handler.client_id] = session
                    self.logger.info(f"Creating new session for client_id={self.handler.client_id}")
            else:
                if self.handler.client_id in broker.sessions:
                    del broker.sessions[self.handler.client_id]
                    self.logger.info(f"Cleared session for client_id={self.handler.client_id}")
                session = SessionData()
                broker.sessions[self.handler.client_id] = session

            # Handle existing connections
            if not self.handler.clean_session and self.handler.client_id in broker.connected_clients:
                old_writer = broker.connected_clients[self.handler.client_id]
                old_peer = old_writer.get_extra_info('peername')
                self.logger.info(f"Closing existing connection for client_id={self.handler.client_id} from {old_peer}")

                # Remove old writer from subscriptions
                if old_writer in broker.client_subscriptions:
                    for topic in broker.client_subscriptions[old_writer]:
                        broker.subscriptions[topic].discard(old_writer)
                        self.logger.info(f"Removed from topic '{topic}' subscriptions")
                    del broker.client_subscriptions[old_writer]

                # Remove from writer_to_client_id
                if old_writer in broker.writer_to_client_id:
                    del broker.writer_to_client_id[old_writer]

                # Close old connection
                old_writer.close()
                await old_writer.wait_closed()
                self.logger.info(f"Closed previous connection for client_id={self.handler.client_id}")

            # Mark as connected
            broker.connected_clients[self.handler.client_id] = self.handler.writer
            broker.writer_to_client_id[self.handler.writer] = self.handler.client_id

            # Send CONNACK
            connack = ConnAckMessage(
                header=Header(MessageType.CONNACK),
                session_present=0,
                return_code=0  # Success
            )
            self.handler.writer.write(connack.pack())
            await self.handler.writer.drain()
            self.logger.info(f"Sent CONNACK to {self.handler.client_id}")

class SubscribeCommand(Command):
    async def execute(self):
        subscribe_msg: SubscribeMessage = self.message
        broker: Broker = self.handler.broker

        async with broker.lock:
            packet_id = subscribe_msg.packet_id
            topics_qos = subscribe_msg.subscriptions  # List of (topic, qos)
            granted_qos = []

            for topic, qos in topics_qos:
                # Authorization check
                if broker.authentication_enabled:
                    username = self.handler.username
                    if not broker.device_manager.is_topic_authorized(username, topic):
                        granted_qos.append(0x80)  # Failure QoS
                        self.logger.warning(f"User '{username}' is not authorized to subscribe to '{topic}'")
                        continue

                # Subscribe
                if topic not in broker.client_subscriptions[self.handler.writer]:
                    broker.subscriptions[topic].add(self.handler.writer)
                    broker.client_subscriptions[self.handler.writer].add(topic)
                    broker.sessions[self.handler.client_id].subscriptions.add(topic)
                    granted_qos.append(qos)
                    self.logger.info(f"Subscribed to topic '{topic}' with QoS {qos}")

                    # Deliver queued messages if any
                    session = broker.sessions.get(self.handler.client_id)
                    if session and session.queued_messages:
                        for queued_msg in session.queued_messages:
                            self.handler.writer.write(queued_msg.pack())
                            await self.handler.writer.drain()
                            self.logger.info(f"Delivered queued message to topic '{queued_msg.topic}'")
                        session.queued_messages.clear()
                        session.queued_message_ids.clear()
                else:
                    # Already subscribed, append current QoS
                    granted_qos.append(qos)
                    self.logger.info(f"Already subscribed to topic '{topic}', updated QoS to {qos}")

            # Send SUBACK
            suback = SubAckMessage(
                header=Header(MessageType.SUBACK),
                packet_id=packet_id,
                return_codes=granted_qos
            )
            self.handler.writer.write(suback.pack())
            await self.handler.writer.drain()
            self.logger.info(f"Sent SUBACK for packet_id={packet_id}")

class UnsubscribeCommand(Command):
    async def execute(self):
        unsubscribe_msg: UnsubscribeMessage = self.message
        broker: Broker = self.handler.broker

        async with broker.lock:
            packet_id = unsubscribe_msg.packet_id
            topics = unsubscribe_msg.topics

            for topic in topics:
                if topic in broker.client_subscriptions[self.handler.writer]:
                    broker.subscriptions[topic].discard(self.handler.writer)
                    broker.client_subscriptions[self.handler.writer].discard(topic)
                    broker.sessions[self.handler.client_id].subscriptions.discard(topic)
                    self.logger.info(f"Unsubscribed from topic '{topic}'")
                else:
                    self.logger.info(f"Attempted to unsubscribe from non-subscribed topic '{topic}'")

            # Send UNSUBACK
            unsuback = UnsubAckMessage(
                header=Header(MessageType.UNSUBACK),
                packet_id=packet_id
            )
            self.handler.writer.write(unsuback.pack())
            await self.handler.writer.drain()
            self.logger.info(f"Sent UNSUBACK for packet_id={packet_id}")

class PublishCommand(Command):
    async def execute(self):
        publish_msg: PublishMessage = self.message
        broker: Broker = self.handler.broker

        async with broker.lock:
            topic = publish_msg.topic
            payload = publish_msg.payload
            qos = publish_msg.header.qos

            try:
                payload_str = payload.decode('utf-8')
            except UnicodeDecodeError:
                payload_str = payload.decode('utf-8', 'ignore')

            self.logger.info(f"PUBLISH: topic='{topic}', payload='{payload_str}', QoS={qos}")

            # Authorization check
            if broker.authentication_enabled:
                username = self.handler.username
                if not broker.device_manager.is_topic_authorized(username, topic):
                    self.logger.warning(f"User '{username}' is not authorized to publish to '{topic}'")
                    # Optionally, send an error or ignore
                    return

            # Forward to subscribers
            if topic in broker.subscriptions:
                for subscriber in list(broker.subscriptions[topic]):
                    if subscriber != self.handler.writer:
                        try:
                            subscriber.write(publish_msg.pack())
                            await subscriber.drain()
                            self.logger.info(f"Forwarded PUBLISH to subscriber {subscriber.get_extra_info('peername')}")
                        except Exception as e:
                            self.logger.error(f"Error forwarding PUBLISH to subscriber: {e}")
            else:
                self.logger.info(f"No subscribers for topic '{topic}'")

            # Handle offline subscribers
            for offline_client_id, session in broker.sessions.items():
                if offline_client_id not in broker.connected_clients and topic in session.subscriptions:
                    # Queue the message based on QoS
                    if qos in [1, 2]:
                        message_id = f"{publish_msg.packet_id}-{topic}-{payload_str}"
                        if message_id not in session.queued_message_ids:
                            session.queued_messages.append(publish_msg)
                            session.queued_message_ids.add(message_id)
                            self.logger.info(f"Queued PUBLISH for offline client_id={offline_client_id}, topic='{topic}'")
                        else:
                            self.logger.info(f"PUBLISH already queued for client_id={offline_client_id}, topic='{topic}'")

            # Handle QoS acknowledgments
            if qos == 1:
                puback = PubAckMessage(
                    header=Header(MessageType.PUBACK),
                    packet_id=publish_msg.packet_id
                )
                self.handler.writer.write(puback.pack())
                await self.handler.writer.drain()
                self.logger.info(f"Sent PUBACK for packet_id={publish_msg.packet_id}")
            elif qos == 2:
                pubrec = PubRecMessage(
                    header=Header(MessageType.PUBREC),
                    packet_id=publish_msg.packet_id
                )
                self.handler.writer.write(pubrec.pack())
                await self.handler.writer.drain()
                self.logger.info(f"Sent PUBREC for packet_id={publish_msg.packet_id}")

class PubRecCommand(Command):
    async def execute(self):
        pubrec_msg: PubRecMessage = self.message
        broker: Broker = self.handler.broker

        async with broker.lock:
            packet_id = pubrec_msg.packet_id
            self.logger.info(f"PUBREC: packet_id={packet_id}")

            # Respond with PUBREL
            pubrel = PubRelMessage(
                header=Header(MessageType.PUBREL, qos=1),
                packet_id=packet_id
            )
            self.handler.writer.write(pubrel.pack())
            await self.handler.writer.drain()
            self.logger.info(f"Sent PUBREL for packet_id={packet_id}")

class PubRelCommand(Command):
    async def execute(self):
        pubrel_msg: PubRelMessage = self.message
        broker: Broker = self.handler.broker

        async with broker.lock:
            packet_id = pubrel_msg.packet_id
            self.logger.info(f"PUBREL: packet_id={packet_id}")

            # Respond with PUBCOMP
            pubcomp = PubCompMessage(
                header=Header(MessageType.PUBCOMP),
                packet_id=packet_id
            )
            self.handler.writer.write(pubcomp.pack())
            await self.handler.writer.drain()
            self.logger.info(f"Sent PUBCOMP for packet_id={packet_id}")

class PingReqCommand(Command):
    async def execute(self):
        broker: Broker = self.handler.broker

        async with broker.lock:
            self.logger.info("PINGREQ received")

            # Respond with PINGRESP
            pingresp = PingRespMessage(
                header=Header(MessageType.PINGRESP)
            )
            self.handler.writer.write(pingresp.pack())
            await self.handler.writer.drain()
            self.logger.info("Sent PINGRESP")

class DisconnectCommand(Command):
    async def execute(self):
        broker: Broker = self.handler.broker

        async with broker.lock:
            self.logger.info(f"DISCONNECT from client_id={self.handler.client_id}")
            self.handler.writer.close()
            await self.handler.writer.wait_closed()
