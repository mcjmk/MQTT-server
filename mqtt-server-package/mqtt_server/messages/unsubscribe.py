from io import BytesIO
from dataclasses import dataclass

from .base import Message, MessageFactory
from .constants import MessageType
from .header import Header
from .utils import unpack_string, pack_string, pack_remaining_length


@dataclass
class UnsubscribeMessage(Message):
    """
    UNSUBSCRIBE packet. Variable header:
      - Packet Identifier (2 bytes)
    Payload:
      - List of Topic Filters
    """
    header: Header
    packet_id: int
    topics: list  # list of topic strings to unsubscribe from

    @classmethod
    def from_data(cls, header: Header, data: BytesIO) -> 'UnsubscribeMessage':
        pid_bytes = data.read(2)
        if len(pid_bytes) < 2:
            raise ValueError("Not enough bytes for UNSUBSCRIBE packet ID.")
        packet_id = int.from_bytes(pid_bytes, 'big')

        topics = []
        while True:
            if data.tell() >= len(data.getbuffer()):
                break
            topic = unpack_string(data)
            topics.append(topic)

        return cls(header, packet_id, topics)

    def pack(self) -> bytes:
        variable = b""
        variable += self.packet_id.to_bytes(2, 'big')
        for topic in self.topics:
            variable += pack_string(topic)

        fixed_header = self.header.pack()
        remaining_length = len(variable)
        return fixed_header + pack_remaining_length(remaining_length) + variable


@MessageFactory.register(MessageType.UNSUBSCRIBE)
class _UnsubscribeMessageFactory(UnsubscribeMessage):
    pass
