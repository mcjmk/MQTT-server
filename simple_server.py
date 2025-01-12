import asyncio
from collections import defaultdict
import logging
from typing import Set

from messages.base import MessageFactory
from messages.constants import MessageType

from messages import (
    Message,  
    ConnectMessage, ConnAckMessage, UnsubAckMessage, UnsubscribeMessage, SubAckMessage, SubscribeMessage,
    PublishMessage, PingReqMessage, PingRespMessage, PubAckMessage, PubRecMessage, PubRelMessage, PubCompMessage,
    DisconnectMessage,
    Header
)

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


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    peer = writer.get_extra_info('peername')
    logging.info(f"New connection from {peer}")

    connected = True
    client_id = None  # Will be set after CONNECT

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
            # CONNECT → CONNACK
            connect_msg = message  # type: ConnectMessage
            client_id = connect_msg.client_id
            logging.info(f"[{peer}] CONNECT: client_id={client_id}")

            # Build a CONNACK (return_code=0 => success)
            header = Header(MessageType.CONNACK)
            connack = ConnAckMessage(header, session_present=0, return_code=0)
            writer.write(connack.pack())
            await writer.drain()
            logging.info(f"[{peer}] Sent CONNACK (session_present=0, return_code=0)")

        elif msg_type == MessageType.SUBSCRIBE:
            # SUBSCRIBE → SUBACK
            sub_msg = message  # type: SubscribeMessage
            logging.info(f"[{peer}] SUBSCRIBE: packet_id={sub_msg.packet_id}, topics={sub_msg.subscriptions}")

            # Update the subscription registry
            for topic, qos in sub_msg.subscriptions:
                subscriptions[topic].add(writer)
                client_subscriptions[writer].add(topic)
                logging.info(f"[{peer}] Subscribed to topic '{topic}' with QoS {qos}")

            # Respond with SUBACK (granted QoS same as requested for simplicity)
            header = Header(MessageType.SUBACK)
            return_codes = [qos for _, qos in sub_msg.subscriptions]
            suback = SubAckMessage(header, sub_msg.packet_id, return_codes)
            writer.write(suback.pack())
            await writer.drain()
            logging.info(f"[{peer}] Sent SUBACK for packet_id={sub_msg.packet_id}")

        elif msg_type == MessageType.UNSUBSCRIBE:
            # UNSUBSCRIBE → UNSUBACK
            unsub_msg = message  # type: UnsubscribeMessage
            logging.info(f"[{peer}] UNSUBSCRIBE: packet_id={unsub_msg.packet_id}, topics={unsub_msg.topics}")

            # Update the subscription registry
            for topic in unsub_msg.topics:
                subscriptions[topic].discard(writer)
                client_subscriptions[writer].discard(topic)
                logging.info(f"[{peer}] Unsubscribed from topic '{topic}'")

            # Respond with UNSUBACK
            header = Header(MessageType.UNSUBACK)
            unsuback = UnsubAckMessage(header, unsub_msg.packet_id)
            writer.write(unsuback.pack())
            await writer.drain()
            logging.info(f"[{peer}] Sent UNSUBACK for packet_id={unsub_msg.packet_id}")

        elif msg_type == MessageType.PUBLISH:
            # PUBLISH → Forward to subscribers based on topic
            publish_msg = message  # type: PublishMessage
            topic = publish_msg.topic
            payload = publish_msg.payload
            qos = publish_msg.header.qos

            try:
                payload_str = payload.decode('utf-8')
            except UnicodeDecodeError:
                payload_str = payload.decode('utf-8', 'ignore')

            logging.info(f"[{peer}] PUBLISH: topic='{topic}', payload='{payload_str}', QoS={qos}")

            # Forward the PUBLISH to all subscribed clients except the sender
            if topic in subscriptions:
                for subscriber in subscriptions[topic]:
                    if subscriber != writer:
                        try:
                            # Reconstruct the PUBLISH message to send
                            # You might want to modify the header (e.g., set DUP flag if needed)
                            # For simplicity, using the same header and packet_id
                            forwarded_publish = PublishMessage(
                                header=publish_msg.header,
                                topic=topic,
                                packet_id=publish_msg.packet_id,
                                payload=payload
                            )
                            subscriber.write(forwarded_publish.pack())
                            await subscriber.drain()
                            logging.info(f"[{peer}] Forwarded PUBLISH to {subscriber.get_extra_info('peername')}")
                        except Exception as e:
                            logging.error(f"Error forwarding PUBLISH to {subscriber.get_extra_info('peername')}: {e}")
            else:
                logging.info(f"No subscribers for topic '{topic}'")

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
            # Client is acknowledging a QoS=1 Publish we might have sent
            puback_msg = message  # type: PubAckMessage
            logging.info(f"[{peer}] PUBACK: packet_id={puback_msg.packet_id}")

        elif msg_type == MessageType.PUBREC:
            # Client is acknowledging a QoS=2 Publish (PUBREC)
            pubrec_msg = message  # type: PubRecMessage
            logging.info(f"[{peer}] PUBREC: packet_id={pubrec_msg.packet_id}")

            # Respond with PUBREL
            header = Header(MessageType.PUBREL, qos=1)  # PUBREL must have fixed-header QoS=1
            pubrel = PubRelMessage(header, pubrec_msg.packet_id)
            writer.write(pubrel.pack())
            await writer.drain()
            logging.info(f"[{peer}] Sent PUBREL for packet_id={pubrec_msg.packet_id}")

        elif msg_type == MessageType.PUBREL:
            # Client sending PUBREL for QoS=2
            pubrel_msg = message  # type: PubRelMessage
            logging.info(f"[{peer}] PUBREL: packet_id={pubrel_msg.packet_id}")

            # Respond with PUBCOMP
            header = Header(MessageType.PUBCOMP)
            pubcomp = PubCompMessage(header, pubrel_msg.packet_id)
            writer.write(pubcomp.pack())
            await writer.drain()
            logging.info(f"[{peer}] Sent PUBCOMP for packet_id={pubrel_msg.packet_id}")

        elif msg_type == MessageType.PUBCOMP:
            # Final QoS=2 acknowledgment
            pubcomp_msg = message  # type: PubCompMessage
            logging.info(f"[{peer}] PUBCOMP: packet_id={pubcomp_msg.packet_id}")

        elif msg_type == MessageType.SUBACK:
            # Typically client → server is unusual, just log
            suback_msg = message  # type: SubAckMessage
            logging.info(f"[{peer}] SUBACK (unexpected as server): {suback_msg}")

        elif msg_type == MessageType.UNSUBACK:
            # Typically client → server is unusual, just log
            unsuback_msg = message  # type: UnsubAckMessage
            logging.info(f"[{peer}] UNSUBACK (unexpected as server): {unsuback_msg}")

        elif msg_type == MessageType.PINGREQ:
            # PINGREQ → PINGRESP
            logging.info(f"[{peer}] PINGREQ")
            header = Header(MessageType.PINGRESP)
            pingresp = PingRespMessage(header)
            writer.write(pingresp.pack())
            await writer.drain()
            logging.info(f"[{peer}] Sent PINGRESP")

        elif msg_type == MessageType.PINGRESP:
            # Typically client → server is unusual, just log
            logging.info(f"[{peer}] PINGRESP (unexpected as server)")

        elif msg_type == MessageType.DISCONNECT:
            # DISCONNECT → Close connection
            logging.info(f"[{peer}] DISCONNECT")
            connected = False

        else:
            logging.info(f"[{peer}] Received unknown/unsupported message type={msg_type}")

    # End of while loop: clean up subscriptions and close writer
    if client_id and writer in client_subscriptions:
        for topic in client_subscriptions[writer]:
            subscriptions[topic].discard(writer)
            logging.info(f"[{peer}] Removed from topic '{topic}' subscriptions")
        del client_subscriptions[writer]

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
