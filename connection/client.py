# client/client.py

import asyncio
import logging

from messages.base import MessageType
from messages import (
    Message,
    Header,
    ConnectMessage, ConnAckMessage,
    SubscribeMessage, SubAckMessage,
    PublishMessage,
    PubAckMessage, PubRecMessage, PubRelMessage, PubCompMessage,
    PingReqMessage, PingRespMessage,
    DisconnectMessage
)

class Client:
    def __init__(self, client_id: str, username: str = None, password: str = None, host='127.0.0.1', port=1884):
        self.client_id = client_id
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.reader = None
        self.writer = None
        self.logger = logging.getLogger(f"Client-{self.client_id}")

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        peer = self.writer.get_extra_info('peername')
        self.logger.info(f"Connected to server at {peer}")

        # Send CONNECT message
        connect_header = Header(MessageType.CONNECT)
        connect_msg = ConnectMessage(
            header=connect_header,
            protocol_name="MQTT",
            protocol_level=4,
            username_flag=1 if self.username else 0,
            password_flag=1 if self.password else 0,
            will_retain=0,
            will_qos=0,
            will_flag=0,
            clean_session=False,  # Persistent session
            keep_alive=60,
            client_id=self.client_id,
            username=self.username,
            password=self.password
        )
        self.writer.write(connect_msg.pack())
        await self.writer.drain()
        self.logger.info("Sent CONNECT message")

        # Wait for CONNACK
        connack: ConnAckMessage = await Message.from_reader(self.reader)
        self.logger.info(f"Received CONNACK: session_present={connack.session_present}, return_code={connack.return_code}")

        if connack.return_code != 0:
            self.logger.error(f"Connection refused by server with return code {connack.return_code}")
            self.writer.close()
            await self.writer.wait_closed()
            return False

        return True

    async def subscribe(self, topic: str, qos: int = 0):
        packet_id = 1  # This should be unique per SUBSCRIBE in a real implementation
        subscribe_header = Header(MessageType.SUBSCRIBE, qos=1)
        subscribe_msg = SubscribeMessage(
            header=subscribe_header,
            packet_id=packet_id,
            subscriptions=[(topic, qos)]
        )
        self.writer.write(subscribe_msg.pack())
        await self.writer.drain()
        self.logger.info(f"Sent SUBSCRIBE to topic '{topic}' with QoS {qos}")

        # Wait for SUBACK
        suback: SubAckMessage = await Message.from_reader(self.reader)
        self.logger.info(f"Received SUBACK: packet_id={suback.packet_id}, return_codes={suback.return_codes}")

    async def publish(self, topic: str, payload: str, qos: int = 0):
        packet_id = 2  # This should be unique per PUBLISH in a real implementation
        publish_header = Header(MessageType.PUBLISH, qos=qos)
        publish_msg = PublishMessage(
            header=publish_header,
            topic=topic,
            packet_id=packet_id,
            payload=payload.encode('utf-8')
        )
        self.writer.write(publish_msg.pack())
        await self.writer.drain()
        self.logger.info(f"Published to '{topic}': {payload}")

        # Handle QoS acknowledgments
        if qos == 1:
            # Wait for PUBACK
            puback: PubAckMessage = await Message.from_reader(self.reader)
            self.logger.info(f"Received PUBACK for packet_id={puback.packet_id}")
        elif qos == 2:
            # Wait for PUBREC
            pubrec: PubRecMessage = await Message.from_reader(self.reader)
            self.logger.info(f"Received PUBREC for packet_id={pubrec.packet_id}")

            # Send PUBREL
            pubrel_header = Header(MessageType.PUBREL, qos=1)
            pubrel = PubRelMessage(pubrel_header, pubrec.packet_id)
            self.writer.write(pubrel.pack())
            await self.writer.drain()
            self.logger.info(f"Sent PUBREL for packet_id={pubrec.packet_id}")

            # Wait for PUBCOMP
            pubcomp: PubCompMessage = await Message.from_reader(self.reader)
            self.logger.info(f"Received PUBCOMP for packet_id={pubcomp.packet_id}")

    async def listen(self):
        while True:
            try:
                message: Message = await Message.from_reader(self.reader)
                await self.handle_message(message)
            except asyncio.IncompleteReadError:
                self.logger.info("Server closed the connection")
                break
            except Exception as e:
                self.logger.error(f"Error while reading message: {e}")
                break

    async def handle_message(self, message: Message):
        msg_type = message.header.message_type

        if msg_type == MessageType.PUBLISH:
            publish_msg: PublishMessage = message
            topic = publish_msg.topic
            payload = publish_msg.payload.decode('utf-8', 'ignore')
            qos = publish_msg.header.qos
            self.logger.info(f"Received PUBLISH: topic='{topic}', payload='{payload}', QoS={qos}")

            # Handle QoS acknowledgments
            if qos == 1:
                # Send PUBACK
                puback_header = Header(MessageType.PUBACK)
                puback = PubAckMessage(puback_header, publish_msg.packet_id)
                self.writer.write(puback.pack())
                await self.writer.drain()
                self.logger.info(f"Sent PUBACK for packet_id={publish_msg.packet_id}")

            elif qos == 2:
                # Send PUBREC
                pubrec_header = Header(MessageType.PUBREC)
                pubrec = PubRecMessage(pubrec_header, publish_msg.packet_id)
                self.writer.write(pubrec.pack())
                await self.writer.drain()
                self.logger.info(f"Sent PUBREC for packet_id={publish_msg.packet_id}")

        elif msg_type == MessageType.PUBREL:
            # Handle PUBREL for QoS=2
            pubrel_msg: PubRelMessage = message
            self.logger.info(f"Received PUBREL: packet_id={pubrel_msg.packet_id}")

            # Send PUBCOMP
            pubcomp_header = Header(MessageType.PUBCOMP)
            pubcomp = PubCompMessage(pubcomp_header, pubrel_msg.packet_id)
            self.writer.write(pubcomp.pack())
            await self.writer.drain()
            self.logger.info(f"Sent PUBCOMP for packet_id={pubrel_msg.packet_id}")

        elif msg_type == MessageType.PINGRESP:
            self.logger.info("Received PINGRESP")

        else:
            self.logger.info(f"Received unexpected message type={msg_type}")

    async def disconnect(self):
        # Send DISCONNECT message
        disconnect_header = Header(MessageType.DISCONNECT)
        disconnect_msg = DisconnectMessage(disconnect_header)
        self.writer.write(disconnect_msg.pack())
        await self.writer.drain()
        self.logger.info("Sent DISCONNECT")
        self.writer.close()
        await self.writer.wait_closed()
        self.logger.info("Disconnected from server")

    async def run(self):
        if not await self.connect():
            return

        # Start listening for incoming messages
        listen_task = asyncio.create_task(self.listen())

        # Example: Subscribe to a topic and publish messages
        await self.subscribe("topic1", qos=1)
        await self.publish("topic1", "Hello MQTT!", qos=1)

        # Wait for the listen task to complete
        await listen_task
