from dataclasses import dataclass

from connection.constants import MessageType
from .bitstructs import FIXED_HEADER


@dataclass
class Header:
    message_type: MessageType
    dup: int = 0
    qos: int = 0
    retain: int = 0

    @classmethod
    def from_bytes(cls, data: bytes) -> 'Header':
        """Creates a header object from a bytes buffer."""

        message_type, dup, qos, retain = FIXED_HEADER.unpack(data)

        return cls(MessageType(message_type), dup, qos, retain)

    def pack(self) -> bytes:
        """Packs the header into a bytes buffer."""

        return FIXED_HEADER.pack(self.message_type, self.dup, self.qos, self.retain)