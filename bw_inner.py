#!/usr/bin/env python3
"""bw-inner: 内网代理程序。

部署在内网（安全区），负责：
  1. 对内网用户暴露 TCP 端口，接受标准 SSH/VNC 客户端连接
  2. 将客户端数据通过正向通道（经正向隔离装置）发送到外网代理
  3. 从反向通道（经反向隔离装置）接收服务器响应数据，写回客户端

数据流向：
  SSH/VNC 客户端 → [bw-inner] → 正向隔离装置 → [bw-outer] → SSH/VNC 服务器
  SSH/VNC 客户端 ← [bw-inner] ← 反向隔离装置 ← [bw-outer] ← SSH/VNC 服务器

用法:
  python bw_inner.py -c configs/inner.yaml
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
    """处理单个 SSH/VNC 客户端连接。

    为每个客户端连接创建一个会话，然后启动两个并行的数据流：
      - 上行：客户端 → 读取数据 → 封装为 DATA 帧 → 正向通道 → 外网代理
      - 下行：外网代理 → 反向通道 → DATA 帧 → 解帧 → 写回客户端

    Args:
        client_reader: 客户端 TCP 读取流
        client_writer: 客户端 TCP 写入流
        service_name:  对应的服务名称（如 "ssh-server1"），用于在外网代理端路由到目标服务器
        mgr:           会话管理器
        fwd_sender:    正向通道发送器
    """
    peer = client_writer.get_extra_info("peername")
    sess = await mgr.new_session(service_name)
    logger.info("client connected: %s -> session %d (%s)", peer, sess.id, service_name)

    try:
        # 通知外网代理：为此客户端打开一个新的到目标服务器的连接
        fwd_sender.send(Frame.make_open(sess.id, service_name))

        # --- 下行协程：从反向通道接收数据 → 写回客户端 ---
        async def relay_to_client():
            while True:
                # 从会话队列取数据（由反向通道的帧处理器投递）
                data = await sess.data_queue.get()
                if data is None:  # None 哨兵值 = 会话结束
                    break
                try:
                    client_writer.write(data)
                    await client_writer.drain()
                except (ConnectionError, OSError):
                    break

        relay_task = asyncio.create_task(relay_to_client())

        # --- 上行：从客户端读取数据 → 通过正向通道发送 ---
        try:
            while True:
                data = await client_reader.read(32 * 1024)
                if not data:  # 客户端断开连接
                    break
                fwd_sender.send(Frame.make_data(sess.id, data))
        except (ConnectionError, OSError):
            pass

        # 客户端已断开，通知外网代理关闭对应的目标连接
        fwd_sender.send(Frame.make_close(sess.id))

        # 取消下行协程
        relay_task.cancel()
        try:
            await relay_task
        except asyncio.CancelledError:
            pass
    finally:
        # 清理：移除会话、关闭客户端连接
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
    """为一个服务启动 TCP 监听器。

    每个配置的服务对应一个监听端口。例如：
      - "ssh-server1" 监听 :2222
      - "vnc-server1" 监听 :5900

    Args:
        svc_name:    服务名称
        listen_addr: 监听地址（如 ":2222" 或 "0.0.0.0:2222"）
        mgr:         会话管理器
        fwd_sender:  正向通道发送器

    Returns:
        asyncio 的 Server 对象
    """
    # 解析监听地址
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
    """内网代理主函数。

    启动流程：
      1. 加载配置文件
      2. 启动正向通道 Sender（发送客户端请求数据到外网代理）
      3. 启动反向通道 Receiver（接收外网代理发回的服务器响应数据）
      4. 为每个配置的服务启动 TCP 监听器
      5. 等待 Ctrl+C 退出信号
    """
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

    # --- 正向通道：内网 → 外网（经正向隔离装置）---
    # 发送客户端的 SSH/VNC 请求数据
    fwd_sender = Sender(cfg.forward.mode, cfg.forward.address)
    fwd_sender.start()
    logger.info("forward channel: %s %s", cfg.forward.mode, cfg.forward.address)

    # --- 反向通道：外网 → 内网（经反向隔离装置）---
    # 接收 SSH/VNC 服务器的响应数据
    rev_receiver: Receiver | None = None

    async def on_reverse_frame(frame: Frame):
        """反向通道帧处理回调。

        根据帧类型分发处理：
          - DATA:  将数据投递到对应会话的队列（最终写回客户端）
          - CLOSE: 移除并关闭会话（目标服务器断开了连接）
          - PING:  回复 PONG 心跳
        """
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

    # --- 为每个服务启动 TCP 监听器 ---
    servers = []
    for svc in cfg.services:
        server = await start_service_listener(svc.name, svc.listen, mgr, fwd_sender)
        servers.append(server)

    # --- 等待退出信号 (Ctrl+C / SIGTERM) ---
    stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    logger.info("bw-inner running, press Ctrl+C to stop")
    await stop_event.wait()

    # --- 优雅关闭 ---
    logger.info("shutting down...")
    for server in servers:
        server.close()
    await mgr.close_all()
    await fwd_sender.stop()
    await rev_receiver.stop()
    logger.info("shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
