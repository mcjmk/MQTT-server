from io import BytesIO
from dataclasses import dataclass

from .base import Message, MessageFactory
from .constants import MessageType
from .header import Header
from .utils import unpack_string, pack_string, pack_remaining_length


@dataclass
class SubscribeMessage(Message):
    """
    SUBSCRIBE packet. Variable header:
      - Packet Identifier (2 bytes)
    Payload:
      - Repeated pairs of (Topic Filter, Requested QoS)
    """
    header: Header
    packet_id: int
    subscriptions: list  # list of (topic, qos)

    @classmethod
    def from_data(cls, header: Header, data: BytesIO) -> 'SubscribeMessage':
        # 1) Packet Identifier
        pid_bytes = data.read(2)
        if len(pid_bytes) < 2:
            raise ValueError("Not enough bytes for SUBSCRIBE packet ID.")
        packet_id = int.from_bytes(pid_bytes, 'big')

        # 2) Then, parse topic + QoS pairs until no more data
        subscriptions = []
        while True:
            if data.tell() >= len(data.getbuffer()):
                break
            topic = unpack_string(data)
            req_qos_bytes = data.read(1)
            if len(req_qos_bytes) < 1:
                raise ValueError("Not enough bytes for requested QoS.")
            req_qos = req_qos_bytes[0] & 0x03  # only the lowest 2 bits matter
            subscriptions.append((topic, req_qos))

        return cls(header, packet_id, subscriptions)

    def pack(self) -> bytes:
        variable = b""
        # Packet identifier
        variable += self.packet_id.to_bytes(2, 'big')

        for topic, qos in self.subscriptions:
            variable += pack_string(topic)
            variable += bytes([qos & 0x03])

        fixed_header = self.header.pack()
        remaining_length = len(variable)
        return fixed_header + pack_remaining_length(remaining_length) + variable


@MessageFactory.register(MessageType.SUBSCRIBE)
class _SubscribeMessageFactory(SubscribeMessage):
    pass
