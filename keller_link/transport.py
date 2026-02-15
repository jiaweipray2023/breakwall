"""Persistent unidirectional TCP channels with automatic reconnection."""

import asyncio
import logging
from typing import Callable, Awaitable

from .proto import Frame, read_frame, write_frame

logger = logging.getLogger("keller_link.transport")

RECONNECT_BASE_DELAY = 2.0  # seconds
RECONNECT_MAX_DELAY = 30.0


class Sender:
    """Manages a persistent outbound channel that sends frames.

    Reconnects automatically on failure. Frames are queued internally.
    """

    def __init__(self, mode: str, address: str):
        self._mode = mode
        self._address = address
        self._queue: asyncio.Queue[Frame] = asyncio.Queue(maxsize=1024)
        self._task: asyncio.Task | None = None
        self._stopped = False

    def start(self):
        self._task = asyncio.ensure_future(self._loop())

    def send(self, frame: Frame):
        try:
            self._queue.put_nowait(frame)
        except asyncio.QueueFull:
            logger.warning("sender queue full, dropping frame")

    async def stop(self):
        self._stopped = True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _loop(self):
        while not self._stopped:
            try:
                reader, writer = await self._establish()
            except asyncio.CancelledError:
                return
            except Exception:
                continue

            logger.info("sender channel established: %s (%s)", self._address, self._mode)
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
        while not self._stopped:
            frame = await self._queue.get()
            await write_frame(writer, frame)

    async def _establish(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
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
                    "sender establish failed (%s %s): %s, retrying in %.1fs",
                    self._mode, self._address, e, delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, RECONNECT_MAX_DELAY)
        raise asyncio.CancelledError()


class Receiver:
    """Manages a persistent inbound channel that reads frames.

    Dispatches received frames via an async callback.
    """

    def __init__(self, mode: str, address: str,
                 handler: Callable[[Frame], Awaitable[None]]):
        self._mode = mode
        self._address = address
        self._handler = handler
        self._task: asyncio.Task | None = None
        self._stopped = False
        self._writer: asyncio.StreamWriter | None = None
        self._server: asyncio.AbstractServer | None = None

    def start(self):
        self._task = asyncio.ensure_future(self._loop())

    async def stop(self):
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
        """Send a frame back through the receiver's connection (e.g., for PONG)."""
        if self._writer and not self._writer.is_closing():
            await write_frame(self._writer, frame)

    async def _loop(self):
        while not self._stopped:
            try:
                reader, writer = await self._establish()
            except asyncio.CancelledError:
                return
            except Exception:
                continue

            self._writer = writer
            logger.info("receiver channel established: %s (%s)", self._address, self._mode)
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
    """Parse 'host:port' or ':port' into (host, port) tuple."""
    if address.startswith(":"):
        return "0.0.0.0", int(address[1:])
    host, port_str = address.rsplit(":", 1)
    return host, int(port_str)


async def _accept_one(address: str) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """Listen on address and accept a single connection."""
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
        server.close()
        await server.wait_closed()
