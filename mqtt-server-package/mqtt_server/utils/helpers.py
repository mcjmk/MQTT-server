# utils/helpers.py

from io import BytesIO
from typing import Literal
import asyncio

BYTE_ORDER: Literal['little', 'big'] = 'big'

def unpack_string(data: BytesIO) -> str:
    length = int.from_bytes(data.read(2), BYTE_ORDER)
    raw = data.read(length)
    return raw.decode('utf-8')

def pack_string(text: str) -> bytes:
    encoded = text.encode('utf-8')
    length = len(encoded).to_bytes(2, BYTE_ORDER)
    return length + encoded

async def read_remaining_length(reader: asyncio.StreamReader) -> int:
    remaining_length = 0
    multiplier = 1
    digit = 128

    while digit & 128:
        digit = (await reader.readexactly(1))[0]
        remaining_length += (digit & 127) * multiplier
        multiplier *= 128

    return remaining_length
