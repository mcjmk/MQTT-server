import asyncio
from abc import ABC, abstractmethod
from io import BytesIO

import bitstruct

from .constants import MessageType
from .utils import read_remaining_length, pack_remaining_length
from .header import Header


class Message(ABC):
    """
    Abstract base class for all MQTT messages.
    Each subclass must implement from_data() and pack() to parse/emit bytes.
    """
    header: Header

    @classmethod
    @abstractmethod
    def from_data(cls, header: Header, data: BytesIO) -> 'Message':
        """
        Parse a message's variable header & payload from a BytesIO, given an already-decoded fixed header.
        """
        pass

    @abstractmethod
    def pack(self) -> bytes:
        """
        Return the full MQTT packet (fixed header + variable header + payload) as bytes.
        """
        pass

    @classmethod
    async def from_reader(cls, reader: asyncio.StreamReader) -> 'Message':
        """
        Read an entire MQTT packet from the stream:
        1. Read the first byte -> decode fixed header bits
        2. Read the remaining length
        3. Read that many bytes -> parse the message
        4. Dispatch to the right subclass based on message type.
        """
        first_byte = await reader.readexactly(1)
        if not first_byte:
            raise ConnectionError("Stream ended unexpectedly (no first byte).")

        # Parse the fixed header's control bits:
        header = Header.from_bytes(first_byte)

        # Read 'Remaining Length'
        remaining_len = await read_remaining_length(reader)
        if remaining_len > 0:
            packet_body = await reader.readexactly(remaining_len)
        else:
            packet_body = b''

        # Turn the body into a BytesIO for convenient parsing
        data_stream = BytesIO(packet_body)

        # Dynamically get the correct message class from the type
        msg_class = MessageFactory.get_message_class(header.message_type)
        if msg_class is None:
            raise NotImplementedError(f"Message type {header.message_type} not supported.")

        return msg_class.from_data(header, data_stream)


class MessageFactory:
    """
    Simple registry to map a MessageType to a concrete subclass of Message.
    """
    registry = {}

    @classmethod
    def register(cls, msg_type: MessageType):
        def decorator(message_class):
            cls.registry[msg_type] = message_class
            return message_class
        return decorator

    @classmethod
    def get_message_class(cls, msg_type: MessageType):
        return cls.registry.get(msg_type, None)
