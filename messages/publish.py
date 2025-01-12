from io import BytesIO
from dataclasses import dataclass

from .base import Message, MessageFactory
from .constants import MessageType
from .header import Header
from .utils import unpack_string, pack_string, pack_remaining_length

@dataclass
class PublishMessage(Message):
    """
    PUBLISH packet. Variable header includes:
      - Topic Name (UTF-8 string)
      - Packet Identifier (if QoS > 0)
    Payload is the remainder of the bytes.
    """
    header: Header
    topic: str
    packet_id: int
    payload: bytes

    @classmethod
    def from_data(cls, header: Header, data: BytesIO) -> 'PublishMessage':
        # 1) Topic Name
        topic = unpack_string(data)
        packet_id = 0

        # 2) If QoS > 0, packet identifier is next 2 bytes
        if header.qos > 0:
            pid_bytes = data.read(2)
            if len(pid_bytes) < 2:
                raise ValueError("Not enough bytes for packet identifier.")
            packet_id = int.from_bytes(pid_bytes, 'big')

        # 3) Payload is whatever remains
        payload = data.read()
        return cls(header, topic, packet_id, payload)

    def pack(self) -> bytes:
        variable = b""
        variable += pack_string(self.topic)
        if self.header.qos > 0:
            variable += self.packet_id.to_bytes(2, 'big')
        variable += self.payload

        fixed_header = self.header.pack()
        remaining_length = len(variable)
        return fixed_header + pack_remaining_length(remaining_length) + variable


@MessageFactory.register(MessageType.PUBLISH)
class _PublishMessageFactory(PublishMessage):
    pass
