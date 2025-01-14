from io import BytesIO
from dataclasses import dataclass

from .base import Message, MessageFactory
from .constants import MessageType
from .header import Header
from .utils import pack_remaining_length


@dataclass
class SubAckMessage(Message):
    """
    SUBACK packet. Variable header:
      - Packet Identifier (2 bytes)
    Payload:
      - Return codes for each topic requested
    """
    header: Header
    packet_id: int
    return_codes: list  # list of bytes, e.g. [0x00, 0x01, 0x80]

    @classmethod
    def from_data(cls, header: Header, data: BytesIO) -> 'SubAckMessage':
        pid_bytes = data.read(2)
        if len(pid_bytes) < 2:
            raise ValueError("Not enough bytes for SUBACK packet ID.")
        packet_id = int.from_bytes(pid_bytes, 'big')

        return_codes = list(data.read())
        return cls(header, packet_id, return_codes)

    def pack(self) -> bytes:
        variable = b""
        variable += self.packet_id.to_bytes(2, 'big')
        variable += bytes(self.return_codes)

        fixed_header = self.header.pack()
        remaining_length = len(variable)
        return fixed_header + pack_remaining_length(remaining_length) + variable


@MessageFactory.register(MessageType.SUBACK)
class _SubAckMessageFactory(SubAckMessage):
    pass
