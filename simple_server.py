import asyncio
import logging

from messages.base import MessageFactory
from messages.constants import MessageType

from messages import (
    Message,  
    ConnectMessage, ConnAckMessage,
    PublishMessage, PingReqMessage, PingRespMessage,
    DisconnectMessage,
    Header
)


logging.basicConfig(level=logging.INFO)


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    peer = writer.get_extra_info('peername')
    logging.info(f"New connection from {peer}")

    connected = True
    while connected:
        try:
            # Attempt to parse a message. This will raise if the client disconnects or sends invalid data.
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
            # No message returned, likely an error
            break

        # Handle each message type (minimal example)
        msg_type = message.header.message_type

        if msg_type == MessageType.CONNECT:
            # We have a ConnectMessage, respond with ConnAck
            connect_msg = message  # type: ConnectMessage
            logging.info(f"Received CONNECT from client_id={connect_msg.client_id}")
            
            # Build a CONNACK (session_present=0, return_code=0 => Connection Accepted)
            header = Header(MessageType.CONNACK)
            connack = ConnAckMessage(header, session_present=0, return_code=0)
            packet_bytes = connack.pack()
            writer.write(packet_bytes)
            await writer.drain()

        elif msg_type == MessageType.PUBLISH:
            publish_msg = message  # type: PublishMessage
            logging.info(
                f"Received PUBLISH topic='{publish_msg.topic}', payload='{publish_msg.payload.decode('utf-8', 'ignore')}'"
            )
            # Minimal server: no further ack for QoS=0, 1, or 2 in this snippet.

        elif msg_type == MessageType.PINGREQ:
            logging.info("Received PINGREQ")
            # Respond with PINGRESP
            header = Header(MessageType.PINGRESP)
            pingresp = PingRespMessage(header)
            packet_bytes = pingresp.pack()
            writer.write(packet_bytes)
            await writer.drain()

        elif msg_type == MessageType.DISCONNECT:
            logging.info("Received DISCONNECT. Closing connection.")
            connected = False

        else:
            # For brevity, ignoring other message types here,
            # but you can add SUBSCRIBE, UNSUBSCRIBE, etc.
            logging.info(f"Received unsupported message type={msg_type}. Ignoring...")

    writer.close()
    await writer.wait_closed()
    logging.info(f"Connection with {peer} closed.")


async def main_server(host='127.0.0.1', port=1884):
    server = await asyncio.start_server(handle_client, host, port)
    logging.info(f"Server listening on {host}:{port}")
    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    try:
        asyncio.run(main_server())
    except KeyboardInterrupt:
        logging.info("Server shut down via KeyboardInterrupt.")
