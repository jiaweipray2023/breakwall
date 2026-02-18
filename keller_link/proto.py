"""二进制帧协议模块 - 用于在单向通道上复用多个会话。

本模块实现了一个简单的二进制帧协议，将多个 SSH/VNC 会话的数据封装成帧，
在正向通道（内→外）和反向通道（外→内）上传输。

帧格式 (固定 9 字节头 + 可变长数据):
    +----------+----------+----------+----------+
    | Type (1) | SessID(4)| Length(4)| Data (N) |
    +----------+----------+----------+----------+

    - Type:   1 字节，帧类型（OPEN/DATA/CLOSE/PING/PONG）
    - SessID: 4 字节，会话 ID（大端序），标识属于哪个 SSH/VNC 连接
    - Length: 4 字节，数据长度（大端序），最大 64KB
    - Data:   可变长度，实际负载数据
"""

import struct
import asyncio
from enum import IntEnum

# --- 协议常量 ---
HEADER_SIZE = 9          # 帧头固定长度: 1(类型) + 4(会话ID) + 4(数据长度)
MAX_DATA_LEN = 64 * 1024  # 单帧最大数据长度 64KB，防止内存滥用
HEADER_FMT = "!BII"      # struct 格式: 网络字节序(大端)，uint8 + uint32 + uint32


class FrameType(IntEnum):
    """帧类型枚举。

    OPEN  - 打开新会话，Data 字段携带目标服务名称（如 "ssh-server1"）
    DATA  - 传输会话数据，即 SSH/VNC 的原始 TCP 字节流
    CLOSE - 关闭会话，表示连接断开
    PING  - 心跳探测请求
    PONG  - 心跳探测响应
    """
    OPEN = 0x01
    DATA = 0x02
    CLOSE = 0x03
    PING = 0x04
    PONG = 0x05


class Frame:
    """单个协议帧。

    使用 __slots__ 减少内存开销（可能同时有大量帧在队列中）。

    属性:
        type:    帧类型（FrameType 枚举值）
        sess_id: 会话 ID，用于区分不同的 SSH/VNC 连接
        data:    帧负载数据
    """
    __slots__ = ("type", "sess_id", "data")

    def __init__(self, frame_type: int, sess_id: int = 0, data: bytes = b""):
        self.type = frame_type
        self.sess_id = sess_id
        self.data = data

    def encode(self) -> bytes:
        """将帧编码为二进制字节串，用于网络发送。"""
        header = struct.pack(HEADER_FMT, self.type, self.sess_id, len(self.data))
        return header + self.data

    # --- 帧构造工厂方法 ---

    @staticmethod
    def make_open(sess_id: int, service_name: str) -> "Frame":
        """构造 OPEN 帧 - 通知对端打开新会话，data 为服务名称的 UTF-8 编码。"""
        return Frame(FrameType.OPEN, sess_id, service_name.encode())

    @staticmethod
    def make_data(sess_id: int, payload: bytes) -> "Frame":
        """构造 DATA 帧 - 携带 SSH/VNC 的原始 TCP 数据。"""
        return Frame(FrameType.DATA, sess_id, payload)

    @staticmethod
    def make_close(sess_id: int) -> "Frame":
        """构造 CLOSE 帧 - 通知对端关闭指定会话。"""
        return Frame(FrameType.CLOSE, sess_id)

    @staticmethod
    def make_ping() -> "Frame":
        """构造 PING 帧 - 心跳探测，检测通道是否存活。"""
        return Frame(FrameType.PING)

    @staticmethod
    def make_pong() -> "Frame":
        """构造 PONG 帧 - 心跳响应。"""
        return Frame(FrameType.PONG)


async def read_frame(reader: asyncio.StreamReader) -> Frame:
    """从 asyncio StreamReader 中读取并解码一个完整的帧。

    该函数会阻塞直到收到完整的帧头和数据。如果连接断开会抛出
    asyncio.IncompleteReadError。

    Args:
        reader: asyncio 的流读取器（来自 TCP 连接）

    Returns:
        解码后的 Frame 对象

    Raises:
        asyncio.IncompleteReadError: 连接在读取过程中断开
        ValueError: 数据长度超过 MAX_DATA_LEN
    """
    # 先读取固定 9 字节的帧头
    header = await reader.readexactly(HEADER_SIZE)
    frame_type, sess_id, data_len = struct.unpack(HEADER_FMT, header)

    # 安全检查：防止恶意或错误的超大帧耗尽内存
    if data_len > MAX_DATA_LEN:
        raise ValueError(f"frame data length {data_len} exceeds max {MAX_DATA_LEN}")

    # 读取帧数据部分
    frame_data = b""
    if data_len > 0:
        frame_data = await reader.readexactly(data_len)
    return Frame(frame_type, sess_id, frame_data)


async def write_frame(writer: asyncio.StreamWriter, frame: Frame) -> None:
    """将一个帧编码后写入 asyncio StreamWriter。

    写入后立即调用 drain() 确保数据被刷出到内核缓冲区，
    实现 TCP 反压（backpressure）控制。

    Args:
        writer: asyncio 的流写入器（来自 TCP 连接）
        frame:  要发送的帧
    """
    writer.write(frame.encode())
    await writer.drain()
