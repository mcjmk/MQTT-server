from io import BytesIO
from dataclasses import dataclass

from .base import Message, MessageFactory
from .constants import MessageType
from .header import Header
from .utils import pack_remaining_length

@dataclass
class PubRecMessage(Message):
    """
    PUBREC is the first step in acknowledging a QoS=2 PUBLISH.
    Variable header: 2-byte Packet Identifier (no payload).
    """
    header: Header
    packet_id: int

    @classmethod
    def from_data(cls, header: Header, data: BytesIO) -> 'PubRecMessage':
        pid_bytes = data.read(2)
        if len(pid_bytes) < 2:
            raise ValueError("Not enough bytes for PUBREC packet_id.")
        packet_id = int.from_bytes(pid_bytes, 'big')
        return cls(header, packet_id)

    def pack(self) -> bytes:
        variable = self.packet_id.to_bytes(2, 'big')
        fixed_header = self.header.pack()
        remaining_length = len(variable)
        return fixed_header + pack_remaining_length(remaining_length) + variable


@MessageFactory.register(MessageType.PUBREC)
class _PubRecMessageFactory(PubRecMessage):
    pass
