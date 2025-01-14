from io import BytesIO
from dataclasses import dataclass

from .base import Message, MessageFactory
from .constants import MessageType
from .header import Header
from .utils import pack_remaining_length

@dataclass
class PubAckMessage(Message):
    """
    PUBACK is the acknowledgment for a QoS=1 PUBLISH.
    Variable header: 2-byte Packet Identifier (no payload).
    """
    header: Header
    packet_id: int

    @classmethod
    def from_data(cls, header: Header, data: BytesIO) -> 'PubAckMessage':
        pid_bytes = data.read(2)
        if len(pid_bytes) < 2:
            raise ValueError("Not enough bytes for PUBACK packet_id.")
        packet_id = int.from_bytes(pid_bytes, 'big')
        return cls(header, packet_id)

    def pack(self) -> bytes:
        variable = self.packet_id.to_bytes(2, 'big')
        fixed_header = self.header.pack()
        remaining_length = len(variable)
        return fixed_header + pack_remaining_length(remaining_length) + variable


@MessageFactory.register(MessageType.PUBACK)
class _PubAckMessageFactory(PubAckMessage):
    """
    Registers the PubAckMessage in the factory.
    """
    pass
