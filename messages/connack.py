from io import BytesIO
from dataclasses import dataclass

from .base import Message, MessageFactory
from .constants import MessageType
from .header import Header
from .utils import pack_remaining_length

@dataclass
class ConnAckMessage(Message):
    """
    CONNACK packet: variable header = 2 bytes
      Byte 1: sessionPresent (0 or 1)
      Byte 2: connectReturnCode
    """
    header: Header
    session_present: int
    return_code: int  # 0 = success, 1..5 various connection errors in MQTT 3.1.1

    @classmethod
    def from_data(cls, header: Header, data: BytesIO) -> 'ConnAckMessage':
        b = data.read(2)
        if len(b) < 2:
            raise ValueError("Not enough bytes for CONNACK variable header.")
        session_present = b[0] & 0x01
        return_code = b[1]
        return cls(header, session_present, return_code)

    def pack(self) -> bytes:
        fixed_header = self.header.pack()
        variable = bytes([self.session_present & 0x01, self.return_code & 0xFF])
        remaining_length = len(variable)
        return fixed_header + pack_remaining_length(remaining_length) + variable


@MessageFactory.register(MessageType.CONNACK)
class _ConnAckMessageFactory(ConnAckMessage):
    pass
