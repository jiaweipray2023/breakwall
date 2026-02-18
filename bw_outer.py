#!/usr/bin/env python3
"""bw-outer: 外网 DMZ 代理程序。

部署在外网 DMZ 区，负责：
  1. 从正向通道（经正向隔离装置）接收内网代理转发的客户端请求
  2. 连接 DMZ 区内实际的 SSH/VNC 服务器，转发请求数据
  3. 读取服务器响应，通过反向通道（经反向隔离装置）发回内网代理

数据流向：
  SSH/VNC 客户端 → [bw-inner] → 正向隔离装置 → [bw-outer] → SSH/VNC 服务器
  SSH/VNC 客户端 ← [bw-inner] ← 反向隔离装置 ← [bw-outer] ← SSH/VNC 服务器

用法:
  python bw_outer.py -c configs/outer.yaml
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

TARGET_DIAL_TIMEOUT = 10.0  # 连接目标 SSH/VNC 服务器的超时时间（秒）


async def handle_target(
    sess_id: int,
    target: str,
    mgr: SessionManager,
    rev_sender: Sender,
):
    """连接 DMZ 中的实际 SSH/VNC 服务器并双向中继数据。

    被 OPEN 帧触发调用。为指定会话建立到目标服务器的 TCP 连接，
    然后启动两个并行的数据流：
      - 上行：正向通道 DATA 帧 → 会话队列 → 写入目标服务器
      - 下行：目标服务器响应 → 封装为 DATA 帧 → 反向通道 → 内网代理

    Args:
        sess_id:    会话 ID（由内网代理在 OPEN 帧中分配）
        target:     目标服务器地址（如 "192.168.1.10:22"）
        mgr:        会话管理器
        rev_sender: 反向通道发送器
    """
    sess = await mgr.get(sess_id)
    if sess is None:
        return

    # 解析并连接目标服务器
    host, port_str = target.rsplit(":", 1)
    port = int(port_str)

    try:
        target_reader, target_writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=TARGET_DIAL_TIMEOUT,
        )
    except Exception as e:
        logger.error("session %d: failed to connect to target %s: %s", sess_id, target, e)
        # 连接失败，通知内网代理关闭此会话
        rev_sender.send(Frame.make_close(sess_id))
        await mgr.remove(sess_id)
        return

    logger.info("session %d: connected to target %s", sess_id, target)

    # --- 上行协程：从会话队列取数据（来自正向通道）→ 写入目标服务器 ---
    async def relay_to_target():
        while True:
            data = await sess.data_queue.get()
            if data is None:  # None 哨兵值 = 会话结束
                break
            try:
                target_writer.write(data)
                await target_writer.drain()
            except (ConnectionError, OSError):
                break

    relay_task = asyncio.create_task(relay_to_target())

    # --- 下行：从目标服务器读取响应 → 通过反向通道发回内网代理 ---
    try:
        while True:
            data = await target_reader.read(32 * 1024)
            if not data:  # 目标服务器断开连接
                break
            rev_sender.send(Frame.make_data(sess_id, data))
    except (ConnectionError, OSError) as e:
        logger.warning("session %d: read from target error: %s", sess_id, e)

    # 目标服务器已断开，通知内网代理关闭此会话
    rev_sender.send(Frame.make_close(sess_id))

    # 取消上行协程
    relay_task.cancel()
    try:
        await relay_task
    except asyncio.CancelledError:
        pass

    # 清理目标连接
    target_writer.close()
    try:
        await target_writer.wait_closed()
    except Exception:
        pass

    await mgr.remove(sess_id)
    logger.info("session %d: target connection closed", sess_id)


async def main():
    """外网代理主函数。

    启动流程：
      1. 加载配置文件，构建服务名称 → 目标地址映射表
      2. 启动反向通道 Sender（发送服务器响应数据到内网代理）
      3. 启动正向通道 Receiver（接收内网代理转发的客户端请求数据）
      4. 等待 Ctrl+C 退出信号
    """
    parser = argparse.ArgumentParser(description="Keller-Link outer proxy")
    parser.add_argument("-c", "--config", default="configs/outer.yaml",
                        help="path to outer proxy config file")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
    )

    cfg = load_outer_config(args.config)

    # 构建服务名称到目标地址的映射表
    # 例如: {"ssh-server1": "192.168.1.10:22", "vnc-server1": "192.168.1.10:5900"}
    targets: dict[str, str] = {}
    for svc in cfg.services:
        targets[svc.name] = svc.target
        logger.info("service %r -> %s", svc.name, svc.target)

    mgr = SessionManager()

    # --- 反向通道：外网 → 内网（经反向隔离装置）---
    # 发送 SSH/VNC 服务器的响应数据
    rev_sender = Sender(cfg.reverse.mode, cfg.reverse.address)
    rev_sender.start()
    logger.info("reverse channel: %s %s", cfg.reverse.mode, cfg.reverse.address)

    # --- 正向通道：内网 → 外网（经正向隔离装置）---
    # 接收内网代理转发的客户端请求数据
    fwd_receiver: Receiver | None = None

    async def on_forward_frame(frame: Frame):
        """正向通道帧处理回调。

        根据帧类型分发处理：
          - OPEN:  查找目标服务器地址，建立到目标的 TCP 连接
          - DATA:  将数据投递到对应会话的队列（最终写入目标服务器）
          - CLOSE: 移除并关闭会话（客户端断开了连接）
          - PING:  回复 PONG 心跳
        """
        if frame.type == FrameType.OPEN:
            # 解码服务名称，查找目标地址
            service_name = frame.data.decode()
            target = targets.get(service_name)
            if target is None:
                logger.warning("unknown service %r for session %d", service_name, frame.sess_id)
                rev_sender.send(Frame.make_close(frame.sess_id))
                return
            # 注册会话并异步启动目标连接处理
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

    # --- 等待退出信号 (Ctrl+C / SIGTERM) ---
    stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    logger.info("bw-outer running, press Ctrl+C to stop")
    await stop_event.wait()

    # --- 优雅关闭 ---
    logger.info("shutting down...")
    await mgr.close_all()
    await rev_sender.stop()
    await fwd_receiver.stop()
    logger.info("shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
