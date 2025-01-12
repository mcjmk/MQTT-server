import asyncio
from io import BytesIO
from typing import Literal

import bitstruct

# from exceptions.connection import MalformedPacketError

FIXED_HEADER = bitstruct.compile('u4u1u2u1')
CONNECT_FLAGS = bitstruct.compile('u1u1u1u2u1u1u1')
BYTE_ORDER: Literal['little', 'big'] = 'big'


def unpack_string(data: BytesIO) -> str:
    """Unpacks a string from the BytesIO object."""

    length = int.from_bytes(data.read(2), BYTE_ORDER)

    try:
        return data.read(length).decode()
    except UnicodeDecodeError:
        raise Exception('Invalid UTF-8 encoded string.')  #TODO: Dodać osobbny error


def pack_string(data: str) -> bytes:
    """Packs a string into a bytes object."""

    packed = len(data).to_bytes(2, BYTE_ORDER)
    return packed + data.encode()


async def read_remaining_length(reader: asyncio.StreamReader) -> int:
    """Reads the remaining length of the message."""

    remaining_length = 0
    multiplier = 1
    digit = 128

    while digit & 128:
        try:
            digit = (await reader.readexactly(1))[0]
        except asyncio.IncompleteReadError: 
            raise Exception('Header incomplete')    #TODO: Dodać osobbny error

        remaining_length += (digit & 127) * multiplier
        multiplier *= 128

    return remaining_length


def pack_remaining_length(remaining_length: int) -> bytes:
    """Packs the remaining length into a bytes object."""

    if remaining_length == 0:
        return b'\x00'

    packed = b''

    while remaining_length > 0:
        digit = remaining_length & 127
        remaining_length >>= 7

        if remaining_length > 0:
            digit |= 128

        packed += digit.to_bytes(1, BYTE_ORDER)

    return packed
