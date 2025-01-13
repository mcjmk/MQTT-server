# server.py (Comprehensive Revision)

import asyncio
from collections import defaultdict
import logging
from typing import Set, Dict, List

from messages.base import MessageFactory
from messages.constants import MessageType

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

from dataclasses import dataclass, field

# Configure logging to include the timestamp, log level, and message
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Global subscription registry: topic -> set of StreamWriter objects
subscriptions: defaultdict[str, Set[asyncio.StreamWriter]] = defaultdict(set)
# Track each client's subscribed topics: StreamWriter -> set of topics
client_subscriptions: defaultdict[asyncio.StreamWriter, Set[str]] = defaultdict(set)

# Session registry: client_id -> SessionData
@dataclass
class SessionData:
    subscriptions: Set[str] = field(default_factory=set)
    queued_messages: List[PublishMessage] = field(default_factory=list)
    queued_message_ids: Set[str] = field(default_factory=set)

sessions: Dict[str, SessionData] = {}

# Connected clients: client_id -> StreamWriter
connected_clients: Dict[str, asyncio.StreamWriter] = {}

# Reverse mapping: StreamWriter -> client_id
writer_to_client_id: Dict[asyncio.StreamWriter, str] = {}

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    peer = writer.get_extra_info('peername')
    logging.info(f"New connection from {peer}")

    connected = True
    client_id = None
    clean_session = True  # Default to True

    while connected:
        try:
            # Parse the next MQTT packet
            message: Message = await Message.from_reader(reader)
        except asyncio.IncompleteReadError:
            logging.info(f"Client {peer} disconnected unexpectedly.")
            break
        except (ConnectionError, EOFError):
            logging.info(f"Connection ended with {peer}")
            break
        except Exception as e:
            logging.error(f"Error while reading from {peer}: {e}")
            break

        if not message:
            # No message returned (error or closed socket)
            break

        msg_type = message.header.message_type

        # -------------------------
        # Handle each MQTT msg type
        # -------------------------
        if msg_type == MessageType.CONNECT:
            # Handle CONNECT message
            connect_msg = message  # type: ConnectMessage
            client_id = connect_msg.client_id
            clean_session = connect_msg.clean_session
            logging.info(f"[{peer}] CONNECT: client_id={client_id}, clean_session={clean_session}")

            if not clean_session:
                # Resume or create session
                if client_id in sessions:
                    session = sessions[client_id]
                    logging.info(f"[{peer}] Resuming existing session for client_id={client_id}")
                else:
                    session = SessionData()
                    sessions[client_id] = session
                    logging.info(f"[{peer}] Creating new session for client_id={client_id}")
            else:
                # Clean session: remove existing session if any
                if client_id in sessions:
                    del sessions[client_id]
                    logging.info(f"[{peer}] Cleared existing session for client_id={client_id}")
                session = SessionData()

            # Check for existing connection and close it
            if not clean_session and client_id in connected_clients:
                old_writer = connected_clients[client_id]
                old_peer = old_writer.get_extra_info('peername')
                logging.info(f"[{peer}] Closing existing connection for client_id={client_id} from {old_peer}")

                # Remove old writer from all subscriptions
                if old_writer in client_subscriptions:
                    for topic in client_subscriptions[old_writer]:
                        subscriptions[topic].discard(old_writer)
                        logging.info(f"[{old_peer}] Removed from topic '{topic}' subscriptions")
                    del client_subscriptions[old_writer]

                # Remove from writer_to_client_id
                if old_writer in writer_to_client_id:
                    del writer_to_client_id[old_writer]

                # Close the old connection
                old_writer.close()
                await old_writer.wait_closed()
                logging.info(f"[{peer}] Closed previous connection for client_id={client_id}")

            # Mark client as connected
            connected_clients[client_id] = writer
            writer_to_client_id[writer] = client_id

            # Send CONNACK
            header = Header(MessageType.CONNACK)
            connack = ConnAckMessage(header, session_present=0, return_code=0)
            writer.write(connack.pack())
            await writer.drain()
            logging.info(f"[{peer}] Sent CONNACK (session_present=0, return_code=0)")

            # **Removed Queued Message Delivery from CONNECT**

        elif msg_type == MessageType.SUBSCRIBE:
            # Handle SUBSCRIBE message
            sub_msg = message  # type: SubscribeMessage
            logging.info(f"[{peer}] SUBSCRIBE: packet_id={sub_msg.packet_id}, topics={sub_msg.subscriptions}")

            granted_qos = []
            for topic, qos in sub_msg.subscriptions:
                if topic not in client_subscriptions[writer]:
                    subscriptions[topic].add(writer)
                    client_subscriptions[writer].add(topic)
                    granted_qos.append(qos)
                    logging.info(f"[{peer}] Subscribed to topic '{topic}' with QoS {qos}")

                    # Add to session subscriptions
                    if client_id and not clean_session:
                        sessions[client_id].subscriptions.add(topic)
                else:
                    # Already subscribed, update QoS if needed
                    granted_qos.append(qos)
                    logging.info(f"[{peer}] Already subscribed to topic '{topic}', updated QoS to {qos}")

            # Send SUBACK
            header = Header(MessageType.SUBACK)
            suback = SubAckMessage(header, sub_msg.packet_id, granted_qos)
            writer.write(suback.pack())
            await writer.drain()
            logging.info(f"[{peer}] Sent SUBACK for packet_id={sub_msg.packet_id}")

            # **Deliver Queued Messages After SUBACK**
            if client_id and not clean_session:
                session = sessions.get(client_id)
                if session and session.queued_messages:
                    for queued_msg in session.queued_messages:
                        writer.write(queued_msg.pack())
                        await writer.drain()
                        logging.info(f"[{peer}] Delivered queued message to topic '{queued_msg.topic}'")
                    # Clear queued messages and their IDs after delivery
                    session.queued_messages.clear()
                    session.queued_message_ids.clear()

        elif msg_type == MessageType.UNSUBSCRIBE:
            # Handle UNSUBSCRIBE message
            unsub_msg = message  # type: UnsubscribeMessage
            logging.info(f"[{peer}] UNSUBSCRIBE: packet_id={unsub_msg.packet_id}, topics={unsub_msg.topics}")

            for topic in unsub_msg.topics:
                if topic in client_subscriptions[writer]:
                    subscriptions[topic].discard(writer)
                    client_subscriptions[writer].discard(topic)
                    logging.info(f"[{peer}] Unsubscribed from topic '{topic}'")

                    # Remove from session subscriptions
                    if client_id and not clean_session:
                        sessions[client_id].subscriptions.discard(topic)
                else:
                    logging.info(f"[{peer}] Attempted to unsubscribe from non-subscribed topic '{topic}'")

            # Send UNSUBACK
            header = Header(MessageType.UNSUBACK)
            unsuback = UnsubAckMessage(header, unsub_msg.packet_id)
            writer.write(unsuback.pack())
            await writer.drain()
            logging.info(f"[{peer}] Sent UNSUBACK for packet_id={unsub_msg.packet_id}")

        elif msg_type == MessageType.PUBLISH:
            # Handle PUBLISH message
            publish_msg = message  # type: PublishMessage
            topic = publish_msg.topic
            payload = publish_msg.payload
            qos = publish_msg.header.qos

            try:
                payload_str = payload.decode('utf-8')
            except UnicodeDecodeError:
                payload_str = payload.decode('utf-8', 'ignore')

            logging.info(f"[{peer}] PUBLISH: topic='{topic}', payload='{payload_str}', QoS={qos}")

            # Forward the PUBLISH to all connected subscribers except the sender
            if topic in subscriptions:
                for subscriber in list(subscriptions[topic]):
                    if subscriber != writer:
                        subscriber_client_id = writer_to_client_id.get(subscriber)

                        if subscriber_client_id and subscriber_client_id in connected_clients:
                            # Subscriber is connected
                            try:
                                subscriber.write(publish_msg.pack())
                                await subscriber.drain()
                                logging.info(f"[{peer}] Forwarded PUBLISH to {subscriber.get_extra_info('peername')}")
                            except Exception as e:
                                logging.error(f"Error forwarding PUBLISH to {subscriber.get_extra_info('peername')}: {e}")
            else:
                logging.info(f"No connected subscribers for topic '{topic}'")

            # Iterate through all sessions to find offline subscribers subscribed to the topic
            for offline_client_id, session in sessions.items():
                if offline_client_id not in connected_clients and topic in session.subscriptions:
                    # Queue the message based on QoS
                    if qos in [1, 2]:
                        # Generate a unique message_id
                        message_id = f"{publish_msg.packet_id}-{topic}-{payload_str}"
                        # Check if the message is already queued
                        if not any(
                            f"{msg.packet_id}-{msg.topic}-{msg.payload.decode('utf-8', 'ignore')}" == message_id
                            for msg in session.queued_messages
                        ):
                            session.queued_messages.append(publish_msg)
                            session.queued_message_ids.add(message_id)
                            logging.info(f"[{peer}] Queued PUBLISH for offline client_id={offline_client_id}, topic='{topic}'")
                        else:
                            logging.info(f"[{peer}] PUBLISH already queued for client_id={offline_client_id}, topic='{topic}'")

            # Handle QoS acknowledgments
            if qos == 1:
                # Respond with PUBACK
                header = Header(MessageType.PUBACK)
                puback = PubAckMessage(header, publish_msg.packet_id)
                writer.write(puback.pack())
                await writer.drain()
                logging.info(f"[{peer}] Sent PUBACK for packet_id={publish_msg.packet_id}")

            elif qos == 2:
                # Respond with PUBREC
                header = Header(MessageType.PUBREC)
                pubrec = PubRecMessage(header, publish_msg.packet_id)
                writer.write(pubrec.pack())
                await writer.drain()
                logging.info(f"[{peer}] Sent PUBREC for packet_id={publish_msg.packet_id}")

        elif msg_type == MessageType.PUBACK:
            # Handle PUBACK message
            puback_msg = message  # type: PubAckMessage
            logging.info(f"[{peer}] PUBACK: packet_id={puback_msg.packet_id}")

        elif msg_type == MessageType.PUBREC:
            # Handle PUBREC message
            pubrec_msg = message  # type: PubRecMessage
            logging.info(f"[{peer}] PUBREC: packet_id={pubrec_msg.packet_id}")

            # Respond with PUBREL
            header = Header(MessageType.PUBREL, qos=1)  # PUBREL must have fixed-header QoS=1
            pubrel = PubRelMessage(header, pubrec_msg.packet_id)
            writer.write(pubrel.pack())
            await writer.drain()
            logging.info(f"[{peer}] Sent PUBREL for packet_id={pubrec_msg.packet_id}")

        elif msg_type == MessageType.PUBREL:
            # Handle PUBREL message
            pubrel_msg = message  # type: PubRelMessage
            logging.info(f"[{peer}] PUBREL: packet_id={pubrel_msg.packet_id}")

            # Respond with PUBCOMP
            header = Header(MessageType.PUBCOMP)
            pubcomp = PubCompMessage(header, pubrel_msg.packet_id)
            writer.write(pubcomp.pack())
            await writer.drain()
            logging.info(f"[{peer}] Sent PUBCOMP for packet_id={pubrel_msg.packet_id}")

        elif msg_type == MessageType.PUBCOMP:
            # Handle PUBCOMP message
            pubcomp_msg = message  # type: PubCompMessage
            logging.info(f"[{peer}] PUBCOMP: packet_id={pubcomp_msg.packet_id}")

        elif msg_type == MessageType.SUBACK:
            # Log unexpected SUBACK messages
            suback_msg = message  # type: SubAckMessage
            logging.info(f"[{peer}] SUBACK (unexpected as server): {suback_msg}")

        elif msg_type == MessageType.UNSUBACK:
            # Log unexpected UNSUBACK messages
            unsuback_msg = message  # type: UnsubAckMessage
            logging.info(f"[{peer}] UNSUBACK (unexpected as server): {unsuback_msg}")

        elif msg_type == MessageType.PINGREQ:
            # Respond with PINGRESP
            logging.info(f"[{peer}] PINGREQ")
            header = Header(MessageType.PINGRESP)
            pingresp = PingRespMessage(header)
            writer.write(pingresp.pack())
            await writer.drain()
            logging.info(f"[{peer}] Sent PINGRESP")

        elif msg_type == MessageType.PINGRESP:
            # Log unexpected PINGRESP messages
            logging.info(f"[{peer}] PINGRESP (unexpected as server)")

        elif msg_type == MessageType.DISCONNECT:
            # Handle DISCONNECT message
            logging.info(f"[{peer}] DISCONNECT")
            connected = False

        else:
            # Handle unknown/unsupported message types
            logging.info(f"[{peer}] Received unknown/unsupported message type={msg_type}")

    # After the while loop: handle session persistence and cleanup
    if client_id:
        if not clean_session:
            # Persist session: update session data with current subscriptions
            # Do NOT overwrite session.subscriptions
            sessions[client_id] = sessions.get(client_id, SessionData())
            logging.info(f"[{peer}] Persisted session for client_id={client_id}")
        else:
            # Clean session: remove any session data
            if client_id in sessions:
                del sessions[client_id]
                logging.info(f"[{peer}] Removed session for client_id={client_id}")

    # Remove writer from subscription registry
    if writer in client_subscriptions:
        for topic in client_subscriptions[writer]:
            subscriptions[topic].discard(writer)
            logging.info(f"[{peer}] Removed from topic '{topic}' subscriptions")
        del client_subscriptions[writer]

    # Remove client from connected clients
    if client_id in connected_clients:
        del connected_clients[client_id]
    if writer in writer_to_client_id:
        del writer_to_client_id[writer]

    writer.close()
    await writer.wait_closed()
    logging.info(f"Connection with {peer} closed.")

async def main_server(host='127.0.0.1', port=1884):
    server = await asyncio.start_server(handle_client, host, port)
    addr = server.sockets[0].getsockname()
    logging.info(f"Server listening on {addr}")
    async with server:
        await server.serve_forever()

if __name__ == '__main__':
    try:
        asyncio.run(main_server())
    except KeyboardInterrupt:
        logging.info("Server shut down via KeyboardInterrupt.")
