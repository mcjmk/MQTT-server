import asyncio
from typing import Literal
from io import BytesIO

BYTE_ORDER: Literal['little', 'big'] = 'big'

async def read_remaining_length(reader: asyncio.StreamReader) -> int:
    """
    Reads the 'Remaining Length' field of an MQTT packet.
    It's a variable-length scheme where each byte uses 7 bits of info + 1 continuation bit.
    """
    multiplier = 1
    value = 0

    while True:
        byte = await reader.readexactly(1)
        encoded_byte = byte[0]

        value += (encoded_byte & 127) * multiplier
        multiplier *= 128

        # If continuation bit not set, stop
        if (encoded_byte & 128) == 0:
            break

    return value


def pack_remaining_length(length: int) -> bytes:
    """
    Encodes the 'Remaining Length' field using the same variable-length scheme.
    """
    encoded = bytearray()
    while True:
        digit = length % 128
        length //= 128
        # if there are more digits to encode, set the top bit of this digit
        if length > 0:
            digit |= 128
        encoded.append(digit)
        if length == 0:
            break
    return bytes(encoded)


def unpack_string(data: BytesIO) -> str:
    """
    Reads a 2-byte length followed by that many bytes of UTF-8 text from the BytesIO object.
    """
    raw_len = data.read(2)
    if len(raw_len) < 2:
        raise ValueError("Not enough bytes to unpack string length.")
    str_len = int.from_bytes(raw_len, BYTE_ORDER)
    raw_str = data.read(str_len)
    if len(raw_str) < str_len:
        raise ValueError("Not enough bytes to read full string.")
    try:
        return raw_str.decode('utf-8')
    except UnicodeDecodeError:
        raise ValueError("Invalid UTF-8 string.")

def pack_string(s: str) -> bytes:
    """
    Converts a string into MQTT's length-prefixed UTF-8 format.
    """
    encoded = s.encode('utf-8')
    length_bytes = len(encoded).to_bytes(2, BYTE_ORDER)
    return length_bytes + encoded
