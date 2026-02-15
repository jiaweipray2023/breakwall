"""Integration tests for the Keller-Link proxy system."""

import asyncio
import struct
import unittest

from keller_link.proto import Frame, FrameType, read_frame, write_frame, HEADER_FMT
from keller_link.session import SessionManager
from keller_link.transport import Sender, Receiver


def get_free_port() -> int:
    """Find a free TCP port."""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestFrameProtocol(unittest.TestCase):
    """Test the binary frame encoding/decoding."""

    def test_encode_decode_roundtrip(self):
        frames = [
            Frame.make_open(1, "ssh-server"),
            Frame.make_data(1, b"hello world"),
            Frame.make_close(1),
            Frame.make_ping(),
            Frame.make_pong(),
        ]

        async def run():
            for expected in frames:
                reader = asyncio.StreamReader()
                reader.feed_data(expected.encode())
                reader.feed_eof()
                got = await read_frame(reader)
                self.assertEqual(got.type, expected.type)
                self.assertEqual(got.sess_id, expected.sess_id)
                self.assertEqual(got.data, expected.data)

        asyncio.run(run())

    def test_frame_header_format(self):
        frame = Frame.make_data(42, b"test")
        encoded = frame.encode()
        frame_type, sess_id, data_len = struct.unpack(HEADER_FMT, encoded[:9])
        self.assertEqual(frame_type, FrameType.DATA)
        self.assertEqual(sess_id, 42)
        self.assertEqual(data_len, 4)
        self.assertEqual(encoded[9:], b"test")

    def test_empty_data_frame(self):
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
    """Test session management."""

    def test_new_session_and_dispatch(self):
        async def run():
            mgr = SessionManager()
            sess = await mgr.new_session("ssh")
            self.assertEqual(sess.name, "ssh")
            self.assertTrue(sess.id > 0)

            ok = await mgr.dispatch(sess.id, b"hello")
            self.assertTrue(ok)

            data = await sess.data_queue.get()
            self.assertEqual(data, b"hello")

        asyncio.run(run())

    def test_dispatch_unknown_session(self):
        async def run():
            mgr = SessionManager()
            ok = await mgr.dispatch(999, b"data")
            self.assertFalse(ok)

        asyncio.run(run())

    def test_remove_session(self):
        async def run():
            mgr = SessionManager()
            sess = await mgr.new_session("vnc")
            await mgr.remove(sess.id)
            ok = await mgr.dispatch(sess.id, b"data")
            self.assertFalse(ok)

        asyncio.run(run())


class TestEndToEnd(unittest.TestCase):
    """End-to-end integration test through the full proxy pipeline."""

    def test_roundtrip(self):
        asyncio.run(self._run_roundtrip())

    async def _run_roundtrip(self):
        # 1. Start a mock target server (simulates SSH/VNC server in DMZ).
        target_port = get_free_port()

        async def echo_handler(reader: asyncio.StreamReader,
                               writer: asyncio.StreamWriter):
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

        # 2. Find free ports.
        fwd_port = get_free_port()
        rev_port = get_free_port()
        svc_port = get_free_port()

        service_name = "test-svc"

        # 3. Start bw-outer components.
        outer_mgr = SessionManager()
        outer_rev_sender = Sender("connect", f"127.0.0.1:{rev_port}")
        outer_rev_sender.start()

        outer_fwd_receiver: Receiver | None = None

        async def outer_handler(f: Frame):
            if f.type == FrameType.OPEN:
                await outer_mgr.register_session(f.sess_id, f.data.decode())
                asyncio.create_task(
                    self._handle_target(f.sess_id, f"127.0.0.1:{target_port}",
                                        outer_mgr, outer_rev_sender))
            elif f.type == FrameType.DATA:
                await outer_mgr.dispatch(f.sess_id, f.data)
            elif f.type == FrameType.CLOSE:
                await outer_mgr.remove(f.sess_id)

        outer_fwd_receiver = Receiver("listen", f"127.0.0.1:{fwd_port}", outer_handler)
        outer_fwd_receiver.start()

        # 4. Start bw-inner components.
        inner_mgr = SessionManager()
        inner_fwd_sender = Sender("connect", f"127.0.0.1:{fwd_port}")
        inner_fwd_sender.start()

        async def inner_handler(f: Frame):
            if f.type == FrameType.DATA:
                await inner_mgr.dispatch(f.sess_id, f.data)
            elif f.type == FrameType.CLOSE:
                await inner_mgr.remove(f.sess_id)

        inner_rev_receiver = Receiver("listen", f"127.0.0.1:{rev_port}", inner_handler)
        inner_rev_receiver.start()

        # Service listener (inline version of bw-inner's service listener).
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

        # Give channels time to establish.
        await asyncio.sleep(0.5)

        # 5. Connect as a client and test roundtrip.
        client_reader, client_writer = await asyncio.open_connection(
            "127.0.0.1", svc_port)

        test_msg = b"hello keller-link"
        client_writer.write(test_msg)
        await client_writer.drain()

        response = await asyncio.wait_for(client_reader.read(4096), timeout=5.0)
        self.assertEqual(response, b"echo:" + test_msg)

        # Cleanup.
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
        """Outer proxy target handler for testing."""
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
