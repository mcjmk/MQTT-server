import bitstruct
from dataclasses import dataclass

from .constants import MessageType

# We'll compile a single byte that contains:
#   - 4 bits for message type
#   - 1 bit for DUP
#   - 2 bits for QoS
#   - 1 bit for RETAIN
#
# bitstruct.compile('u4u1u2u1') maps:
#   - first 4 bits  -> messageType
#   - next 1 bit    -> DUP
#   - next 2 bits   -> QoS
#   - next 1 bit    -> RETAIN
FIXED_HEADER = bitstruct.compile('u4u1u2u1')


@dataclass
class Header:
    message_type: MessageType
    dup: int = 0
    qos: int = 0
    retain: int = 0

    @classmethod
    def from_bytes(cls, data: bytes) -> 'Header':
        """
        data is the first byte (8 bits) of the fixed header.
        We'll unpack it using the bitstruct pattern above.
        """
        if len(data) < 1:
            raise ValueError("Not enough bytes to parse header.")

        (message_type_raw,
         dup,
         qos,
         retain) = FIXED_HEADER.unpack(bytes([data[0]]))

        message_type_enum = MessageType(message_type_raw)
        return cls(
            message_type=message_type_enum,
            dup=dup,
            qos=qos,
            retain=retain
        )

    def pack(self) -> bytes:
        """
        Packs the first byte of the fixed header according to
        message_type, dup, qos, retain bits.
        """
        packed_byte = FIXED_HEADER.pack(
            self.message_type.value,
            self.dup,
            self.qos,
            self.retain
        )
        return packed_byte
