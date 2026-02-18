"""单向通道传输层 - 管理正向/反向隔离装置之间的持久 TCP 连接。

本模块提供两个核心类：
  - Sender:   发送端，持续从内部队列取帧并写入 TCP 连接
  - Receiver: 接收端，持续从 TCP 连接读帧并回调处理

两者均支持自动重连（指数退避），适应隔离装置网络不稳定的场景。

典型部署：
  内网代理 (bw-inner):
    - Sender   连接正向隔离装置 → 发送客户端请求数据
    - Receiver 监听反向隔离装置 → 接收服务器响应数据

  外网代理 (bw-outer):
    - Receiver 监听正向隔离装置 → 接收客户端请求数据
    - Sender   连接反向隔离装置 → 发送服务器响应数据

连接模式：
  - "connect": 主动发起 TCP 连接（适用于隔离装置提供透传端口）
  - "listen":  被动监听 TCP 连接（适用于隔离装置主动推送数据）
"""

import asyncio
import logging
from typing import Callable, Awaitable

from .proto import Frame, read_frame, write_frame

logger = logging.getLogger("keller_link.transport")

# --- 重连策略常量 ---
RECONNECT_BASE_DELAY = 2.0   # 首次重连等待时间（秒）
RECONNECT_MAX_DELAY = 30.0   # 最大重连等待时间（秒），指数退避的上限


class Sender:
    """发送端 - 管理一条持久的出站单向通道。

    工作流程：
      1. 建立 TCP 连接（connect 或 listen 模式）
      2. 循环从内部队列取帧 → 编码 → 写入 TCP
      3. 连接断开时自动重连，队列中的帧不会丢失

    帧队列 (maxsize=1024):
      - send() 方法是非阻塞的，将帧放入队列
      - 如果队列满（通道不畅），新帧会被丢弃并打印警告
    """

    def __init__(self, mode: str, address: str):
        """
        Args:
            mode:    连接模式，"connect"（主动连接）或 "listen"（被动监听）
            address: 目标地址，格式为 "host:port" 或 ":port"
        """
        self._mode = mode
        self._address = address
        self._queue: asyncio.Queue[Frame] = asyncio.Queue(maxsize=1024)
        self._task: asyncio.Task | None = None
        self._stopped = False

    def start(self):
        """启动发送循环（在后台协程中运行）。"""
        self._task = asyncio.ensure_future(self._loop())

    def send(self, frame: Frame):
        """将帧放入发送队列（非阻塞）。

        如果队列已满说明通道不畅，帧会被丢弃。
        这是设计决策：宁可丢帧也不阻塞调用方的客户端处理逻辑。
        """
        try:
            self._queue.put_nowait(frame)
        except asyncio.QueueFull:
            logger.warning("sender queue full, dropping frame")

    async def stop(self):
        """停止发送循环并清理资源。"""
        self._stopped = True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _loop(self):
        """主循环：建立连接 → 发送帧 → 断开后重连。"""
        while not self._stopped:
            # 第一步：建立 TCP 连接（失败会自动重试）
            try:
                reader, writer = await self._establish()
            except asyncio.CancelledError:
                return
            except Exception:
                continue

            logger.info("sender channel established: %s (%s)", self._address, self._mode)

            # 第二步：持续发送帧，直到连接断开
            try:
                await self._write_loop(writer)
            except (ConnectionError, OSError, asyncio.CancelledError) as e:
                if isinstance(e, asyncio.CancelledError) and self._stopped:
                    return
                logger.warning("sender write error: %s, reconnecting...", e)
            finally:
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass

    async def _write_loop(self, writer: asyncio.StreamWriter):
        """从队列取帧并写入 TCP 连接，直到连接断开或被停止。"""
        while not self._stopped:
            frame = await self._queue.get()
            await write_frame(writer, frame)

    async def _establish(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """建立 TCP 连接，失败时指数退避重试。

        重试策略：2s → 4s → 8s → 16s → 30s → 30s → ...
        """
        delay = RECONNECT_BASE_DELAY
        while not self._stopped:
            try:
                if self._mode == "connect":
                    # 主动连接模式：拨号到隔离装置的端口
                    host, port = _parse_address(self._address)
                    return await asyncio.open_connection(host, port)
                else:
                    # 被动监听模式：等待隔离装置发起连接
                    return await _accept_one(self._address)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(
                    "sender establish failed (%s %s): %s, retrying in %.1fs",
                    self._mode, self._address, e, delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, RECONNECT_MAX_DELAY)
        raise asyncio.CancelledError()


class Receiver:
    """接收端 - 管理一条持久的入站单向通道。

    工作流程：
      1. 建立 TCP 连接（connect 或 listen 模式）
      2. 循环从 TCP 读帧 → 调用 handler 回调处理
      3. 连接断开时自动重连

    handler 回调函数：
      接收一个 Frame 参数，由上层代码根据帧类型做分发处理。
      例如内网代理的 handler 会根据 DATA/CLOSE 帧类型
      将数据分发到对应的会话队列。
    """

    def __init__(self, mode: str, address: str,
                 handler: Callable[[Frame], Awaitable[None]]):
        """
        Args:
            mode:    连接模式，"connect" 或 "listen"
            address: 地址，格式为 "host:port" 或 ":port"
            handler: 收到帧时的异步回调函数
        """
        self._mode = mode
        self._address = address
        self._handler = handler
        self._task: asyncio.Task | None = None
        self._stopped = False
        self._writer: asyncio.StreamWriter | None = None  # 保存当前连接，供 send_frame 使用
        self._server: asyncio.AbstractServer | None = None

    def start(self):
        """启动接收循环（在后台协程中运行）。"""
        self._task = asyncio.ensure_future(self._loop())

    async def stop(self):
        """停止接收循环并清理资源。"""
        self._stopped = True
        if self._writer:
            self._writer.close()
        if self._server:
            self._server.close()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def send_frame(self, frame: Frame):
        """通过接收端的连接回送帧（用于回复 PONG 心跳响应）。

        注意：这是在接收通道上反向发送，仅用于心跳响应等少量控制帧。
        主要的数据传输应走对应的 Sender。
        """
        if self._writer and not self._writer.is_closing():
            await write_frame(self._writer, frame)

    async def _loop(self):
        """主循环：建立连接 → 读取帧并处理 → 断开后重连。"""
        while not self._stopped:
            # 第一步：建立 TCP 连接
            try:
                reader, writer = await self._establish()
            except asyncio.CancelledError:
                return
            except Exception:
                continue

            self._writer = writer
            logger.info("receiver channel established: %s (%s)", self._address, self._mode)

            # 第二步：持续读帧并回调处理
            try:
                while not self._stopped:
                    frame = await read_frame(reader)
                    await self._handler(frame)
            except (ConnectionError, OSError, asyncio.IncompleteReadError,
                    asyncio.CancelledError) as e:
                if isinstance(e, asyncio.CancelledError) and self._stopped:
                    return
                logger.warning("receiver read error: %s, reconnecting...", e)
            finally:
                self._writer = None
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass

    async def _establish(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """建立 TCP 连接，失败时指数退避重试。"""
        delay = RECONNECT_BASE_DELAY
        while not self._stopped:
            try:
                if self._mode == "connect":
                    host, port = _parse_address(self._address)
                    return await asyncio.open_connection(host, port)
                else:
                    return await _accept_one(self._address)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(
                    "receiver establish failed (%s %s): %s, retrying in %.1fs",
                    self._mode, self._address, e, delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, RECONNECT_MAX_DELAY)
        raise asyncio.CancelledError()


def _parse_address(address: str) -> tuple[str, int]:
    """解析地址字符串为 (host, port) 元组。

    支持两种格式：
      - ":9001"          → ("0.0.0.0", 9001)  监听所有接口
      - "10.0.1.100:9001" → ("10.0.1.100", 9001)
    """
    if address.startswith(":"):
        return "0.0.0.0", int(address[1:])
    host, port_str = address.rsplit(":", 1)
    return host, int(port_str)


async def _accept_one(address: str) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """在指定地址监听并接受一个 TCP 连接后立即关闭监听。

    这种 "接受一个就关" 的模式适用于隔离装置场景：
    每次通道重建时，对端（隔离装置）会发起一个新连接。
    我们只需要一个连接用于帧传输。
    """
    host, port = _parse_address(address)
    connected: asyncio.Future[tuple[asyncio.StreamReader, asyncio.StreamWriter]] = (
        asyncio.get_event_loop().create_future()
    )

    async def on_connect(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        if not connected.done():
            connected.set_result((reader, writer))

    server = await asyncio.start_server(on_connect, host, port)
    try:
        result = await connected
        return result
    finally:
        # 拿到连接后立即关闭监听，释放端口
        server.close()
        await server.wait_closed()
