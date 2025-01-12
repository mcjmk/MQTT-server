from io import BytesIO
import bitstruct
from dataclasses import dataclass

from .base import Message, MessageFactory
from .constants import MessageType
from .header import Header
from .utils import pack_remaining_length, unpack_string, pack_string

# CONNECT Flags layout (per MQTT 3.1.1) - one byte:
# bit 7: usernameFlag
# bit 6: passwordFlag
# bit 5: willRetain
# bit 4-3: willQoS
# bit 2: willFlag
# bit 1: cleanSession
# bit 0: reserved (must be 0)
CONNECT_FLAGS = bitstruct.compile('u1u1u1u2u1u1u1')


@dataclass
class ConnectMessage(Message):
    """
    Represents a CONNECT packet with basic fields. Simplified, but enough to illustrate logic.
    """
    header: Header
    protocol_name: str
    protocol_level: int
    username_flag: int
    password_flag: int
    will_retain: int
    will_qos: int
    will_flag: int
    clean_session: int
    keep_alive: int
    client_id: str
    username: str = ""
    password: str = ""

    @classmethod
    def from_data(cls, header: Header, data: BytesIO) -> 'ConnectMessage':
        # 1) Protocol Name (string)
        protocol_name = unpack_string(data)

        # 2) Protocol Level (1 byte)
        protocol_level = data.read(1)
        if len(protocol_level) < 1:
            raise ValueError("Not enough bytes for protocol level.")
        protocol_level = protocol_level[0]

        # 3) Connect Flags (1 byte)
        raw_flags = data.read(1)
        if len(raw_flags) < 1:
            raise ValueError("Not enough bytes for connect flags.")
        (username_flag, password_flag, will_retain,
         will_qos, will_flag, clean_session, reserved) = CONNECT_FLAGS.unpack(raw_flags)

        if reserved != 0:
            raise ValueError("Malformed CONNECT flags: reserved bit != 0")

        # 4) Keep Alive (2 bytes)
        ka_raw = data.read(2)
        if len(ka_raw) < 2:
            raise ValueError("Not enough bytes for keep alive.")
        keep_alive = int.from_bytes(ka_raw, 'big')

        # 5) Client ID (MQTT spec requires at least one char if cleanSession=0)
        client_id = unpack_string(data)

        # (Optional) If will_flag=1, you'd parse will topic/message here. Omitted for brevity.

        # (Optional) If username_flag=1, parse username
        username = ""
        if username_flag == 1:
            username = unpack_string(data)

        # (Optional) If password_flag=1, parse password
        password = ""
        if password_flag == 1:
            password = unpack_string(data)

        return cls(
            header=header,
            protocol_name=protocol_name,
            protocol_level=protocol_level,
            username_flag=username_flag,
            password_flag=password_flag,
            will_retain=will_retain,
            will_qos=will_qos,
            will_flag=will_flag,
            clean_session=clean_session,
            keep_alive=keep_alive,
            client_id=client_id,
            username=username,
            password=password
        )

    def pack(self) -> bytes:
        # Construct variable header + payload:
        variable = b""
        variable += pack_string(self.protocol_name)
        variable += bytes([self.protocol_level])

        # Re-pack the connect flags
        flags_byte = CONNECT_FLAGS.pack(
            self.username_flag,
            self.password_flag,
            self.will_retain,
            self.will_qos,
            self.will_flag,
            self.clean_session,
            0  # reserved bit
        )
        variable += flags_byte
        variable += self.keep_alive.to_bytes(2, 'big')

        # Client ID
        variable += pack_string(self.client_id)

        # If username_flag=1, add username
        if self.username_flag == 1:
            variable += pack_string(self.username)

        # If password_flag=1, add password
        if self.password_flag == 1:
            variable += pack_string(self.password)

        # Fixed header + Remaining length + variable+payload
        fixed_header = self.header.pack()
        remaining_length = len(variable)
        return fixed_header + \
               pack_remaining_length(remaining_length) + \
               variable


@MessageFactory.register(MessageType.CONNECT)
class _ConnectMessageFactory(ConnectMessage):
    """
    This wrapper ensures the ConnectMessage is registered in the factory.
    In practice, you could just decorate ConnectMessage directly.
    """
    pass
