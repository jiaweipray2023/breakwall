"""Keller-Link 集成测试。

测试覆盖三个层次：
  1. TestFrameProtocol - 帧协议的编解码正确性
  2. TestSessionManager - 会话管理器的增删查分发
  3. TestEndToEnd - 完整链路端到端往返测试

端到端测试模拟了以下完整数据流：
  测试客户端 → 内网代理组件 → 正向通道 → 外网代理组件 → 模拟目标服务器
  测试客户端 ← 内网代理组件 ← 反向通道 ← 外网代理组件 ← 模拟目标服务器
"""

import asyncio
import struct
import unittest

from keller_link.proto import Frame, FrameType, read_frame, write_frame, HEADER_FMT
from keller_link.session import SessionManager
from keller_link.transport import Sender, Receiver


def get_free_port() -> int:
    """获取一个可用的 TCP 端口（绑定后立即释放）。"""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestFrameProtocol(unittest.TestCase):
    """测试二进制帧协议的编码和解码。"""

    def test_encode_decode_roundtrip(self):
        """测试所有帧类型的编码→解码往返一致性。"""
        frames = [
            Frame.make_open(1, "ssh-server"),
            Frame.make_data(1, b"hello world"),
            Frame.make_close(1),
            Frame.make_ping(),
            Frame.make_pong(),
        ]

        async def run():
            for expected in frames:
                # 将帧编码后喂入 StreamReader，再解码出来比较
                reader = asyncio.StreamReader()
                reader.feed_data(expected.encode())
                reader.feed_eof()
                got = await read_frame(reader)
                self.assertEqual(got.type, expected.type)
                self.assertEqual(got.sess_id, expected.sess_id)
                self.assertEqual(got.data, expected.data)

        asyncio.run(run())

    def test_frame_header_format(self):
        """测试帧头的二进制格式是否符合协议规范。"""
        frame = Frame.make_data(42, b"test")
        encoded = frame.encode()
        # 手动解析帧头的 9 个字节
        frame_type, sess_id, data_len = struct.unpack(HEADER_FMT, encoded[:9])
        self.assertEqual(frame_type, FrameType.DATA)
        self.assertEqual(sess_id, 42)
        self.assertEqual(data_len, 4)
        self.assertEqual(encoded[9:], b"test")  # 帧头之后是原始数据

    def test_empty_data_frame(self):
        """测试无数据负载的帧（如 CLOSE 帧）能正确编解码。"""
        frame = Frame.make_close(99)
        self.assertEqual(len(frame.data), 0)

        async def run():
            reader = asyncio.StreamReader()
            reader.feed_data(frame.encode())
            reader.feed_eof()
            got = await read_frame(reader)
            self.assertEqual(got.type, FrameType.CLOSE)
            self.assertEqual(got.sess_id, 99)
            self.assertEqual(got.data, b"")

        asyncio.run(run())


class TestSessionManager(unittest.TestCase):
    """测试会话管理器的核心功能。"""

    def test_new_session_and_dispatch(self):
        """测试创建会话后，数据能正确分发到会话队列。"""
        async def run():
            mgr = SessionManager()
            sess = await mgr.new_session("ssh")
            self.assertEqual(sess.name, "ssh")
            self.assertTrue(sess.id > 0)

            # 向会话分发数据
            ok = await mgr.dispatch(sess.id, b"hello")
            self.assertTrue(ok)

            # 从会话队列取出数据并验证
            data = await sess.data_queue.get()
            self.assertEqual(data, b"hello")

        asyncio.run(run())

    def test_dispatch_unknown_session(self):
        """测试向不存在的会话分发数据应返回 False。"""
        async def run():
            mgr = SessionManager()
            ok = await mgr.dispatch(999, b"data")
            self.assertFalse(ok)

        asyncio.run(run())

    def test_remove_session(self):
        """测试移除会话后，再分发数据应返回 False。"""
        async def run():
            mgr = SessionManager()
            sess = await mgr.new_session("vnc")
            await mgr.remove(sess.id)
            ok = await mgr.dispatch(sess.id, b"data")
            self.assertFalse(ok)

        asyncio.run(run())


class TestEndToEnd(unittest.TestCase):
    """端到端集成测试 - 验证完整的代理数据链路。

    测试架构：
      测试客户端 ──→ 内网服务监听 ──→ 正向通道 ──→ 外网帧处理 ──→ 模拟目标服务器
      测试客户端 ←── 内网帧处理   ←── 反向通道 ←── 外网帧处理 ←── 模拟目标服务器

    模拟目标服务器是一个简单的 echo 服务，收到数据后加 "echo:" 前缀返回。
    """

    def test_roundtrip(self):
        """测试数据从客户端经全链路往返后内容正确。"""
        asyncio.run(self._run_roundtrip())

    async def _run_roundtrip(self):
        # ========== 1. 启动模拟目标服务器（代替 DMZ 中的 SSH/VNC 服务器）==========
        target_port = get_free_port()

        async def echo_handler(reader: asyncio.StreamReader,
                               writer: asyncio.StreamWriter):
            """模拟目标服务器：收到什么就加 "echo:" 前缀回传。"""
            try:
                while True:
                    data = await reader.read(4096)
                    if not data:
                        break
                    writer.write(b"echo:" + data)
                    await writer.drain()
            except Exception:
                pass
            finally:
                writer.close()

        target_server = await asyncio.start_server(
            echo_handler, "127.0.0.1", target_port)

        # ========== 2. 分配测试用端口 ==========
        fwd_port = get_free_port()   # 正向通道端口
        rev_port = get_free_port()   # 反向通道端口
        svc_port = get_free_port()   # 内网服务监听端口

        service_name = "test-svc"

        # ========== 3. 启动外网代理组件 ==========
        outer_mgr = SessionManager()
        # 反向通道 Sender：外网 → 内网
        outer_rev_sender = Sender("connect", f"127.0.0.1:{rev_port}")
        outer_rev_sender.start()

        outer_fwd_receiver: Receiver | None = None

        async def outer_handler(f: Frame):
            """外网代理的正向通道帧处理器。"""
            if f.type == FrameType.OPEN:
                await outer_mgr.register_session(f.sess_id, f.data.decode())
                asyncio.create_task(
                    self._handle_target(f.sess_id, f"127.0.0.1:{target_port}",
                                        outer_mgr, outer_rev_sender))
            elif f.type == FrameType.DATA:
                await outer_mgr.dispatch(f.sess_id, f.data)
            elif f.type == FrameType.CLOSE:
                await outer_mgr.remove(f.sess_id)

        # 正向通道 Receiver：内网 → 外网
        outer_fwd_receiver = Receiver("listen", f"127.0.0.1:{fwd_port}", outer_handler)
        outer_fwd_receiver.start()

        # ========== 4. 启动内网代理组件 ==========
        inner_mgr = SessionManager()
        # 正向通道 Sender：内网 → 外网
        inner_fwd_sender = Sender("connect", f"127.0.0.1:{fwd_port}")
        inner_fwd_sender.start()

        async def inner_handler(f: Frame):
            """内网代理的反向通道帧处理器。"""
            if f.type == FrameType.DATA:
                await inner_mgr.dispatch(f.sess_id, f.data)
            elif f.type == FrameType.CLOSE:
                await inner_mgr.remove(f.sess_id)

        # 反向通道 Receiver：外网 → 内网
        inner_rev_receiver = Receiver("listen", f"127.0.0.1:{rev_port}", inner_handler)
        inner_rev_receiver.start()

        # 内网服务监听器（简化版 bw-inner 的客户端处理逻辑）
        async def svc_handler(client_reader: asyncio.StreamReader,
                              client_writer: asyncio.StreamWriter):
            sess = await inner_mgr.new_session(service_name)
            inner_fwd_sender.send(Frame.make_open(sess.id, service_name))

            async def relay():
                while True:
                    data = await sess.data_queue.get()
                    if data is None:
                        break
                    client_writer.write(data)
                    await client_writer.drain()

            relay_task = asyncio.create_task(relay())
            try:
                while True:
                    data = await client_reader.read(32 * 1024)
                    if not data:
                        break
                    inner_fwd_sender.send(Frame.make_data(sess.id, data))
            except Exception:
                pass
            inner_fwd_sender.send(Frame.make_close(sess.id))
            relay_task.cancel()
            try:
                await relay_task
            except asyncio.CancelledError:
                pass
            await inner_mgr.remove(sess.id)
            client_writer.close()

        svc_server = await asyncio.start_server(svc_handler, "127.0.0.1", svc_port)

        # 等待所有通道建立连接
        await asyncio.sleep(0.5)

        # ========== 5. 作为客户端连接并验证往返数据 ==========
        client_reader, client_writer = await asyncio.open_connection(
            "127.0.0.1", svc_port)

        test_msg = b"hello keller-link"
        client_writer.write(test_msg)
        await client_writer.drain()

        # 读取响应并验证（应为 "echo:hello keller-link"）
        response = await asyncio.wait_for(client_reader.read(4096), timeout=5.0)
        self.assertEqual(response, b"echo:" + test_msg)

        # ========== 6. 清理所有资源 ==========
        client_writer.close()
        await asyncio.sleep(0.2)

        svc_server.close()
        await svc_server.wait_closed()
        target_server.close()
        await target_server.wait_closed()
        await inner_fwd_sender.stop()
        await inner_rev_receiver.stop()
        await outer_rev_sender.stop()
        await outer_fwd_receiver.stop()
        await inner_mgr.close_all()
        await outer_mgr.close_all()

    async def _handle_target(self, sess_id: int, target: str,
                             mgr: SessionManager, rev_sender: Sender):
        """外网代理的目标服务器处理（测试用简化版）。"""
        sess = await mgr.get(sess_id)
        if sess is None:
            return

        host, port_str = target.rsplit(":", 1)
        try:
            tr, tw = await asyncio.wait_for(
                asyncio.open_connection(host, int(port_str)), timeout=5.0)
        except Exception:
            rev_sender.send(Frame.make_close(sess_id))
            await mgr.remove(sess_id)
            return

        async def relay():
            while True:
                data = await sess.data_queue.get()
                if data is None:
                    break
                tw.write(data)
                await tw.drain()

        relay_task = asyncio.create_task(relay())
        try:
            while True:
                data = await tr.read(32 * 1024)
                if not data:
                    break
                rev_sender.send(Frame.make_data(sess_id, data))
        except Exception:
            pass

        rev_sender.send(Frame.make_close(sess_id))
        relay_task.cancel()
        try:
            await relay_task
        except asyncio.CancelledError:
            pass
        tw.close()
        await mgr.remove(sess_id)


if __name__ == "__main__":
    unittest.main()
