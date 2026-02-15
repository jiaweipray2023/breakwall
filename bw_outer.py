#!/usr/bin/env python3
"""bw-outer: External DMZ proxy.

Receives session data from the inner proxy through the forward isolation
channel, connects to actual SSH/VNC servers in the DMZ, and sends responses
back through the reverse isolation channel.
"""

import argparse
import asyncio
import logging
import signal

from keller_link.config import load_outer_config
from keller_link.proto import Frame, FrameType
from keller_link.session import SessionManager
from keller_link.transport import Sender, Receiver

logger = logging.getLogger("bw-outer")

TARGET_DIAL_TIMEOUT = 10.0  # seconds


async def handle_target(
    sess_id: int,
    target: str,
    mgr: SessionManager,
    rev_sender: Sender,
):
    """Connect to the actual SSH/VNC server and relay data."""
    sess = await mgr.get(sess_id)
    if sess is None:
        return

    # Parse target address.
    host, port_str = target.rsplit(":", 1)
    port = int(port_str)

    try:
        target_reader, target_writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=TARGET_DIAL_TIMEOUT,
        )
    except Exception as e:
        logger.error("session %d: failed to connect to target %s: %s", sess_id, target, e)
        rev_sender.send(Frame.make_close(sess_id))
        await mgr.remove(sess_id)
        return

    logger.info("session %d: connected to target %s", sess_id, target)

    async def relay_to_target():
        """Read data from session queue and write to target server."""
        while True:
            data = await sess.data_queue.get()
            if data is None:
                break
            try:
                target_writer.write(data)
                await target_writer.drain()
            except (ConnectionError, OSError):
                break

    relay_task = asyncio.create_task(relay_to_target())

    try:
        while True:
            data = await target_reader.read(32 * 1024)
            if not data:
                break
            rev_sender.send(Frame.make_data(sess_id, data))
    except (ConnectionError, OSError) as e:
        logger.warning("session %d: read from target error: %s", sess_id, e)

    # Target disconnected.
    rev_sender.send(Frame.make_close(sess_id))
    relay_task.cancel()
    try:
        await relay_task
    except asyncio.CancelledError:
        pass

    target_writer.close()
    try:
        await target_writer.wait_closed()
    except Exception:
        pass

    await mgr.remove(sess_id)
    logger.info("session %d: target connection closed", sess_id)


async def main():
    parser = argparse.ArgumentParser(description="Keller-Link outer proxy")
    parser.add_argument("-c", "--config", default="configs/outer.yaml",
                        help="path to outer proxy config file")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
    )

    cfg = load_outer_config(args.config)

    # Build service name -> target address mapping.
    targets: dict[str, str] = {}
    for svc in cfg.services:
        targets[svc.name] = svc.target
        logger.info("service %r -> %s", svc.name, svc.target)

    mgr = SessionManager()

    # Reverse channel: sends server responses to the inner proxy (outer -> inner).
    rev_sender = Sender(cfg.reverse.mode, cfg.reverse.address)
    rev_sender.start()
    logger.info("reverse channel: %s %s", cfg.reverse.mode, cfg.reverse.address)

    # Forward channel: receives client data from the inner proxy (inner -> outer).
    fwd_receiver: Receiver | None = None

    async def on_forward_frame(frame: Frame):
        if frame.type == FrameType.OPEN:
            service_name = frame.data.decode()
            target = targets.get(service_name)
            if target is None:
                logger.warning("unknown service %r for session %d", service_name, frame.sess_id)
                rev_sender.send(Frame.make_close(frame.sess_id))
                return
            await mgr.register_session(frame.sess_id, service_name)
            asyncio.create_task(handle_target(frame.sess_id, target, mgr, rev_sender))
        elif frame.type == FrameType.DATA:
            if not await mgr.dispatch(frame.sess_id, frame.data):
                logger.warning("dropped data for unknown session %d", frame.sess_id)
        elif frame.type == FrameType.CLOSE:
            await mgr.remove(frame.sess_id)
        elif frame.type == FrameType.PING:
            if fwd_receiver:
                await fwd_receiver.send_frame(Frame.make_pong())

    fwd_receiver = Receiver(cfg.forward.mode, cfg.forward.address, on_forward_frame)
    fwd_receiver.start()
    logger.info("forward channel: %s %s", cfg.forward.mode, cfg.forward.address)

    # Wait for shutdown signal.
    stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    logger.info("bw-outer running, press Ctrl+C to stop")
    await stop_event.wait()

    logger.info("shutting down...")
    await mgr.close_all()
    await rev_sender.stop()
    await fwd_receiver.stop()
    logger.info("shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
