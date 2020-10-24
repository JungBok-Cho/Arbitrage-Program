"""
CPSC 5520-01, Seattle University
This is free and unencumbered software released into the public domain.
:Authors: JungBok Cho
:Version: 1.0
"""
import struct
from datetime import datetime, timedelta
from typing import List

MAX_QUOTES_PER_MESSAGE = 50
MICROS_PER_SECOND = 1_000_000
MESSAGE_LENGTH = 32


def deserialize_price(x: bytes) -> float:
    """
    Convert a byte array to a float used in the price feed messages.

    :param x: Number to be converted
    :return: Float for price ratio
    """
    p = struct.unpack('<d', x)
    return p[0]


def serialize_address(host: str, port: int) -> bytes:
    """
    Serialize the host, port address that we want to get published

    :param host: Host address
    :param port: Port number
    :return: Bytes of ip address and port pair
    """
    ip = bytes(map(int, host.split('.')))
    p = struct.pack('>H', port)
    return ip + p


def deserialize_utcdatetime(utc: bytes) -> datetime:
    """
    Convert a byte stream tp a UTC datetime

    :param utc: 8-byte stream
    :return: Datetime for timestamp
    """
    epoch = datetime(1970, 1, 1)
    p = struct.unpack('>Q', utc)[0]
    p = p / MICROS_PER_SECOND
    return epoch + timedelta(seconds=p)


def unmarshal_message(byte_sequence: bytes) -> List[str]:
    """
    Construct the list of quote structures from the byte stream

    :param byte_sequence: Byte stream that received in UDP message
    :return: List of quote structures
    """
    messageArray = []  # List of quotes
    length = len(byte_sequence) / 32

    # Iterate through the byte_sequence
    for x in range(int(length)):
        message = ''
        timestamp = deserialize_utcdatetime(byte_sequence[
                                            (0 + (x * MESSAGE_LENGTH)):(8 + (
                                                        x * MESSAGE_LENGTH))])
        currencyName = byte_sequence[(8 + (x * MESSAGE_LENGTH)):(
                    14 + (x * MESSAGE_LENGTH))].decode("utf-8")
        price = deserialize_price(byte_sequence[(14 + (x * MESSAGE_LENGTH)):(
                    22 + (x * MESSAGE_LENGTH))])

        message += str(timestamp) + ' '
        message += str(currencyName[0:3]) + ' '
        message += str(currencyName[3:]) + ' '
        message += str(price)
        messageArray.append(message)
    return messageArray
