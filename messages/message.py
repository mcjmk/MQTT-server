import asyncio
from abc import ABC, abstractmethod
from io import BytesIO

from connection.reader_handler import HeaderHandler, RemainingLengthHandler, DataHandler, MessageHandler
from messages.header import Header

#TODO: potrzebne metody z connection


class Message(ABC):
    header: Header

    @classmethod
    async def from_reader(cls, reader: asyncio.StreamReader, keep_alive: int = None) -> 'Message':
        """Creates a message object from a reader stream."""

        header_handler = HeaderHandler()
        length_handler = RemainingLengthHandler()
        data_handler = DataHandler()
        message_handler = MessageHandler()

        header_handler.set_next(length_handler).set_next(data_handler).set_next(message_handler)
        return await header_handler.handle(reader, keep_alive)

    @classmethod
    @abstractmethod
    def from_data(cls, header: Header, data: BytesIO) -> 'Message':
        """Creates a message object from the given header and data."""

    @abstractmethod
    def pack(self) -> bytes:
        """Packs the message into a bytes object."""
