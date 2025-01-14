from io import BytesIO
from dataclasses import dataclass

from .base import Message, MessageFactory
from .constants import MessageType
from .header import Header
from .utils import pack_remaining_length


@dataclass
class PingRespMessage(Message):
    """
    PINGRESP has no variable header or payload (MQTT 3.1.1).
    """
    header: Header

    @classmethod
    def from_data(cls, header: Header, data: BytesIO) -> 'PingRespMessage':
        return cls(header)

    def pack(self) -> bytes:
        fixed_header = self.header.pack()
        return fixed_header + pack_remaining_length(0)


@MessageFactory.register(MessageType.PINGRESP)
class _PingRespMessageFactory(PingRespMessage):
    pass
