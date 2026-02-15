"""Binary framing protocol for multiplexing sessions over unidirectional channels.

Frame format:
    +----------+----------+----------+----------+
    | Type (1) | SessID(4)| Length(4)| Data (N) |
    +----------+----------+----------+----------+
"""

import struct
import asyncio
from enum import IntEnum

HEADER_SIZE = 9  # 1 + 4 + 4
MAX_DATA_LEN = 64 * 1024  # 64KB
HEADER_FMT = "!BII"  # big-endian: uint8, uint32, uint32


class FrameType(IntEnum):
    OPEN = 0x01   # Open a new session (data = service name)
    DATA = 0x02   # Session data payload
    CLOSE = 0x03  # Close a session
    PING = 0x04   # Heartbeat request
    PONG = 0x05   # Heartbeat response


class Frame:
    __slots__ = ("type", "sess_id", "data")

    def __init__(self, frame_type: int, sess_id: int = 0, data: bytes = b""):
        self.type = frame_type
        self.sess_id = sess_id
        self.data = data

    def encode(self) -> bytes:
        header = struct.pack(HEADER_FMT, self.type, self.sess_id, len(self.data))
        return header + self.data

    @staticmethod
    def make_open(sess_id: int, service_name: str) -> "Frame":
        return Frame(FrameType.OPEN, sess_id, service_name.encode())

    @staticmethod
    def make_data(sess_id: int, payload: bytes) -> "Frame":
        return Frame(FrameType.DATA, sess_id, payload)

    @staticmethod
    def make_close(sess_id: int) -> "Frame":
        return Frame(FrameType.CLOSE, sess_id)

    @staticmethod
    def make_ping() -> "Frame":
        return Frame(FrameType.PING)

    @staticmethod
    def make_pong() -> "Frame":
        return Frame(FrameType.PONG)


async def read_frame(reader: asyncio.StreamReader) -> Frame:
    """Read a single frame from an asyncio StreamReader."""
    header = await reader.readexactly(HEADER_SIZE)
    frame_type, sess_id, data_len = struct.unpack(HEADER_FMT, header)
    if data_len > MAX_DATA_LEN:
        raise ValueError(f"frame data length {data_len} exceeds max {MAX_DATA_LEN}")
    frame_data = b""
    if data_len > 0:
        frame_data = await reader.readexactly(data_len)
    return Frame(frame_type, sess_id, frame_data)


async def write_frame(writer: asyncio.StreamWriter, frame: Frame) -> None:
    """Write a single frame to an asyncio StreamWriter."""
    writer.write(frame.encode())
    await writer.drain()
