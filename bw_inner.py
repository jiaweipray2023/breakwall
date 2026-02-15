#!/usr/bin/env python3
"""bw-inner: Internal network proxy.

Accepts SSH/VNC client connections on the internal network, multiplexes them
through the forward isolation channel, and delivers responses from the reverse
isolation channel back to the clients.
"""

import argparse
import asyncio
import logging
import signal

from keller_link.config import load_inner_config
from keller_link.proto import Frame, FrameType
from keller_link.session import SessionManager
from keller_link.transport import Sender, Receiver

logger = logging.getLogger("bw-inner")


async def handle_client(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    service_name: str,
    mgr: SessionManager,
    fwd_sender: Sender,
):
    """Handle a single client connection."""
    peer = client_writer.get_extra_info("peername")
    sess = await mgr.new_session(service_name)
    logger.info("client connected: %s -> session %d (%s)", peer, sess.id, service_name)

    try:
        # Tell the outer proxy to open a new session.
        fwd_sender.send(Frame.make_open(sess.id, service_name))

        async def relay_to_client():
            """Read data from reverse channel and write to client."""
            while True:
                data = await sess.data_queue.get()
                if data is None:
                    break
                try:
                    client_writer.write(data)
                    await client_writer.drain()
                except (ConnectionError, OSError):
                    break

        relay_task = asyncio.create_task(relay_to_client())

        try:
            while True:
                data = await client_reader.read(32 * 1024)
                if not data:
                    break
                fwd_sender.send(Frame.make_data(sess.id, data))
        except (ConnectionError, OSError):
            pass

        # Client disconnected — tell outer proxy.
        fwd_sender.send(Frame.make_close(sess.id))
        relay_task.cancel()
        try:
            await relay_task
        except asyncio.CancelledError:
            pass
    finally:
        await mgr.remove(sess.id)
        client_writer.close()
        try:
            await client_writer.wait_closed()
        except Exception:
            pass
        logger.info("session %d closed", sess.id)


async def start_service_listener(
    svc_name: str,
    listen_addr: str,
    mgr: SessionManager,
    fwd_sender: Sender,
):
    """Start listening for client connections for a specific service."""
    if listen_addr.startswith(":"):
        host, port = "0.0.0.0", int(listen_addr[1:])
    else:
        host, port_str = listen_addr.rsplit(":", 1)
        host, port = host, int(port_str)

    async def on_connect(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        await handle_client(reader, writer, svc_name, mgr, fwd_sender)

    server = await asyncio.start_server(on_connect, host, port)
    logger.info("service %r listening on %s", svc_name, listen_addr)
    return server


async def main():
    parser = argparse.ArgumentParser(description="Keller-Link inner proxy")
    parser.add_argument("-c", "--config", default="configs/inner.yaml",
                        help="path to inner proxy config file")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
    )

    cfg = load_inner_config(args.config)
    mgr = SessionManager()

    # Forward channel: sends client data to the outer proxy (inner -> outer).
    fwd_sender = Sender(cfg.forward.mode, cfg.forward.address)
    fwd_sender.start()
    logger.info("forward channel: %s %s", cfg.forward.mode, cfg.forward.address)

    # Reverse channel: receives server responses from the outer proxy.
    rev_receiver: Receiver | None = None

    async def on_reverse_frame(frame: Frame):
        if frame.type == FrameType.DATA:
            if not await mgr.dispatch(frame.sess_id, frame.data):
                logger.warning("dropped data for unknown session %d", frame.sess_id)
        elif frame.type == FrameType.CLOSE:
            await mgr.remove(frame.sess_id)
        elif frame.type == FrameType.PING:
            if rev_receiver:
                await rev_receiver.send_frame(Frame.make_pong())

    rev_receiver = Receiver(cfg.reverse.mode, cfg.reverse.address, on_reverse_frame)
    rev_receiver.start()
    logger.info("reverse channel: %s %s", cfg.reverse.mode, cfg.reverse.address)

    # Start service listeners.
    servers = []
    for svc in cfg.services:
        server = await start_service_listener(svc.name, svc.listen, mgr, fwd_sender)
        servers.append(server)

    # Wait for shutdown signal.
    stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    logger.info("bw-inner running, press Ctrl+C to stop")
    await stop_event.wait()

    logger.info("shutting down...")
    for server in servers:
        server.close()
    await mgr.close_all()
    await fwd_sender.stop()
    await rev_receiver.stop()
    logger.info("shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
