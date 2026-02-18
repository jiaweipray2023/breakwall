"""会话管理模块 - 管理所有通过代理的 SSH/VNC 会话。

每个 SSH/VNC 客户端连接对应一个 Session 对象，拥有唯一的会话 ID。
SessionManager 负责：
  1. 分配和注册会话
  2. 根据会话 ID 路由数据（从通道帧分发到对应的会话队列）
  3. 清理已关闭的会话

数据流向（以内网代理为例）：
  反向通道收到 DATA 帧 → SessionManager.dispatch() → Session.data_queue → 写回客户端
"""

import asyncio
import logging

logger = logging.getLogger("keller_link.session")


class Session:
    """单个代理会话，对应一个 SSH 或 VNC 连接。

    属性:
        id:         会话 ID（全局唯一，由内网代理分配）
        name:       关联的服务名称（如 "ssh-server1"），用于路由到目标服务器
        data_queue: 异步队列，存放从对端收到的数据
                    - 队列中的 bytes 是正常数据
                    - 队列中的 None 是结束信号（哨兵值）
        _closed:    标记会话是否已关闭，防止重复关闭
    """

    __slots__ = ("id", "name", "data_queue", "_closed")

    def __init__(self, sess_id: int, name: str):
        self.id = sess_id
        self.name = name
        # maxsize=256: 限制队列深度，防止数据积压导致内存溢出
        # 当队列满时，新数据会被丢弃（put_nowait 抛出 QueueFull）
        self.data_queue: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=256)
        self._closed = False

    async def put_data(self, data: bytes) -> bool:
        """将数据放入会话队列，供消费端（客户端或目标服务器）读取。

        Args:
            data: 要入队的数据

        Returns:
            True 表示成功入队，False 表示会话已关闭或队列已满
        """
        if self._closed:
            return False
        try:
            self.data_queue.put_nowait(data)
            return True
        except asyncio.QueueFull:
            return False

    def close(self):
        """关闭会话，向队列投放 None 哨兵值通知消费端退出循环。"""
        if not self._closed:
            self._closed = True
            try:
                self.data_queue.put_nowait(None)
            except asyncio.QueueFull:
                pass


class SessionManager:
    """会话管理器 - 跟踪所有活跃会话，分发数据到正确的会话。

    线程安全：使用 asyncio.Lock 保护内部字典，支持多个协程并发操作。

    在内网代理中：
      - new_session() 在客户端连接时调用，自动分配递增的会话 ID
      - dispatch() 在反向通道收到 DATA 帧时调用，将数据路由到对应会话

    在外网代理中：
      - register_session() 在收到 OPEN 帧时调用，使用内网代理分配的会话 ID
      - dispatch() 在正向通道收到 DATA 帧时调用，将数据路由到对应会话
    """

    def __init__(self):
        self._sessions: dict[int, Session] = {}  # 会话 ID → Session 映射
        self._lock = asyncio.Lock()               # 保护 _sessions 的异步锁
        self._next_id = 0                          # 自增 ID 计数器（仅内网代理使用）

    async def new_session(self, name: str) -> Session:
        """创建新会话并自动分配 ID（由内网代理调用）。

        Args:
            name: 服务名称，如 "ssh-server1"

        Returns:
            新创建的 Session 对象
        """
        async with self._lock:
            self._next_id += 1
            sess = Session(self._next_id, name)
            self._sessions[sess.id] = sess
            logger.info("new session %d for service %r", sess.id, name)
            return sess

    async def register_session(self, sess_id: int, name: str) -> Session:
        """以指定 ID 注册会话（由外网代理在收到 OPEN 帧时调用）。

        外网代理不自行分配 ID，而是沿用内网代理在 OPEN 帧中携带的会话 ID，
        保证两端的会话 ID 一致。

        Args:
            sess_id: 内网代理分配的会话 ID
            name:    服务名称

        Returns:
            新注册的 Session 对象
        """
        async with self._lock:
            sess = Session(sess_id, name)
            self._sessions[sess_id] = sess
            logger.info("registered session %d for service %r", sess_id, name)
            return sess

    async def get(self, sess_id: int) -> Session | None:
        """根据 ID 获取会话，不存在则返回 None。"""
        async with self._lock:
            return self._sessions.get(sess_id)

    async def remove(self, sess_id: int):
        """移除并关闭会话。会向该会话的队列投放 None 哨兵值。"""
        async with self._lock:
            sess = self._sessions.pop(sess_id, None)
        if sess:
            sess.close()
            logger.info("removed session %d", sess_id)

    async def dispatch(self, sess_id: int, data: bytes) -> bool:
        """将数据分发到指定会话的队列。

        这是数据路由的核心方法。当通道收到 DATA 帧时，
        根据帧中的会话 ID 将数据投递到对应会话的队列。

        Args:
            sess_id: 目标会话 ID
            data:    要投递的数据

        Returns:
            True 表示成功投递，False 表示会话不存在
        """
        async with self._lock:
            sess = self._sessions.get(sess_id)
        if sess is None:
            return False
        return await sess.put_data(data)

    async def close_all(self):
        """关闭所有活跃会话（在程序退出时调用）。"""
        async with self._lock:
            for sess in self._sessions.values():
                sess.close()
            self._sessions.clear()
        logger.info("all sessions closed")
