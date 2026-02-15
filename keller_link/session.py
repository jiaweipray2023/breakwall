"""Session management for multiplexed connections."""

import asyncio
import logging
import threading

logger = logging.getLogger("keller_link.session")


class Session:
    """Represents a single proxied connection (e.g., one SSH or VNC session)."""

    __slots__ = ("id", "name", "data_queue", "_closed")

    def __init__(self, sess_id: int, name: str):
        self.id = sess_id
        self.name = name
        self.data_queue: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=256)
        self._closed = False

    async def put_data(self, data: bytes) -> bool:
        """Enqueue data for this session. Returns False if session is closed."""
        if self._closed:
            return False
        try:
            self.data_queue.put_nowait(data)
            return True
        except asyncio.QueueFull:
            return False

    def close(self):
        """Signal session termination by enqueuing None sentinel."""
        if not self._closed:
            self._closed = True
            try:
                self.data_queue.put_nowait(None)
            except asyncio.QueueFull:
                pass


class SessionManager:
    """Tracks all active sessions and dispatches incoming data."""

    def __init__(self):
        self._sessions: dict[int, Session] = {}
        self._lock = asyncio.Lock()
        self._next_id = 0

    async def new_session(self, name: str) -> Session:
        """Create and register a new session with an auto-assigned ID."""
        async with self._lock:
            self._next_id += 1
            sess = Session(self._next_id, name)
            self._sessions[sess.id] = sess
            logger.info("new session %d for service %r", sess.id, name)
            return sess

    async def register_session(self, sess_id: int, name: str) -> Session:
        """Register a session with a specific ID (used by outer proxy)."""
        async with self._lock:
            sess = Session(sess_id, name)
            self._sessions[sess_id] = sess
            logger.info("registered session %d for service %r", sess_id, name)
            return sess

    async def get(self, sess_id: int) -> Session | None:
        async with self._lock:
            return self._sessions.get(sess_id)

    async def remove(self, sess_id: int):
        async with self._lock:
            sess = self._sessions.pop(sess_id, None)
        if sess:
            sess.close()
            logger.info("removed session %d", sess_id)

    async def dispatch(self, sess_id: int, data: bytes) -> bool:
        """Send data to a session's queue. Returns False if session not found."""
        async with self._lock:
            sess = self._sessions.get(sess_id)
        if sess is None:
            return False
        return await sess.put_data(data)

    async def close_all(self):
        async with self._lock:
            for sess in self._sessions.values():
                sess.close()
            self._sessions.clear()
        logger.info("all sessions closed")
