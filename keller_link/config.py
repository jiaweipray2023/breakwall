"""配置模块 - 加载和校验 YAML 配置文件。

内网代理 (bw-inner) 和外网代理 (bw-outer) 各有独立的配置结构。
两者的 services 中的 name 字段必须一一对应，用于建立会话到目标服务器的路由。

配置文件示例见 configs/ 目录。
"""

from dataclasses import dataclass, field
from typing import Any

import yaml


@dataclass
class ChannelConfig:
    """通道配置 - 描述一条单向 TCP 通道的连接方式。

    属性:
        mode:    "connect"（主动连接）或 "listen"（被动监听）
        address: 地址，格式 "host:port" 或 ":port"
    """
    mode: str = ""
    address: str = ""


@dataclass
class ServiceConfig:
    """服务配置 - 描述一个被代理的 SSH/VNC 服务。

    属性:
        name:   服务名称（唯一标识，inner/outer 配置中必须一致）
        listen: 仅 inner 使用，内网监听地址（如 ":2222"）
        target: 仅 outer 使用，DMZ 中实际服务器地址（如 "192.168.1.10:22"）
    """
    name: str = ""
    listen: str = ""
    target: str = ""


@dataclass
class InnerConfig:
    """内网代理配置。

    属性:
        services: 服务列表，每个服务定义 name 和 listen 端口
        forward:  正向通道配置（内→外，经过正向隔离装置）
        reverse:  反向通道配置（外→内，经过反向隔离装置）
    """
    services: list[ServiceConfig] = field(default_factory=list)
    forward: ChannelConfig = field(default_factory=ChannelConfig)
    reverse: ChannelConfig = field(default_factory=ChannelConfig)


@dataclass
class OuterConfig:
    """外网代理配置。

    属性:
        services: 服务列表，每个服务定义 name 和 target 地址
        forward:  正向通道配置（内→外）
        reverse:  反向通道配置（外→内）
    """
    services: list[ServiceConfig] = field(default_factory=list)
    forward: ChannelConfig = field(default_factory=ChannelConfig)
    reverse: ChannelConfig = field(default_factory=ChannelConfig)


# --- 内部解析函数 ---

def _parse_channel(data: dict[str, Any]) -> ChannelConfig:
    """从 YAML 字典解析通道配置。"""
    return ChannelConfig(
        mode=data.get("mode", ""),
        address=data.get("address", ""),
    )


def _parse_services(data: list[dict[str, Any]]) -> list[ServiceConfig]:
    """从 YAML 列表解析服务配置列表。"""
    return [
        ServiceConfig(
            name=s.get("name", ""),
            listen=s.get("listen", ""),
            target=s.get("target", ""),
        )
        for s in data
    ]


# --- 公开加载函数 ---

def load_inner_config(path: str) -> InnerConfig:
    """加载并校验内网代理配置文件。

    Args:
        path: YAML 配置文件路径

    Returns:
        校验通过的 InnerConfig 对象

    Raises:
        FileNotFoundError: 配置文件不存在
        ValueError: 配置内容不合法
    """
    with open(path) as f:
        raw = yaml.safe_load(f)

    cfg = InnerConfig(
        services=_parse_services(raw.get("services", [])),
        forward=_parse_channel(raw.get("forward", {})),
        reverse=_parse_channel(raw.get("reverse", {})),
    )
    _validate_inner(cfg)
    return cfg


def load_outer_config(path: str) -> OuterConfig:
    """加载并校验外网代理配置文件。

    Args:
        path: YAML 配置文件路径

    Returns:
        校验通过的 OuterConfig 对象

    Raises:
        FileNotFoundError: 配置文件不存在
        ValueError: 配置内容不合法
    """
    with open(path) as f:
        raw = yaml.safe_load(f)

    cfg = OuterConfig(
        services=_parse_services(raw.get("services", [])),
        forward=_parse_channel(raw.get("forward", {})),
        reverse=_parse_channel(raw.get("reverse", {})),
    )
    _validate_outer(cfg)
    return cfg


# --- 校验函数 ---

def _validate_inner(cfg: InnerConfig):
    """校验内网配置：每个服务必须有 name 和 listen，通道配置必须完整。"""
    if not cfg.services:
        raise ValueError("config: at least one service is required")
    for i, svc in enumerate(cfg.services):
        if not svc.name:
            raise ValueError(f"config: service[{i}] missing name")
        if not svc.listen:
            raise ValueError(f"config: service[{i}] {svc.name!r} missing listen address")
    _validate_channel("forward", cfg.forward)
    _validate_channel("reverse", cfg.reverse)


def _validate_outer(cfg: OuterConfig):
    """校验外网配置：每个服务必须有 name 和 target，通道配置必须完整。"""
    if not cfg.services:
        raise ValueError("config: at least one service is required")
    for i, svc in enumerate(cfg.services):
        if not svc.name:
            raise ValueError(f"config: service[{i}] missing name")
        if not svc.target:
            raise ValueError(f"config: service[{i}] {svc.name!r} missing target address")
    _validate_channel("forward", cfg.forward)
    _validate_channel("reverse", cfg.reverse)


def _validate_channel(name: str, ch: ChannelConfig):
    """校验通道配置：mode 必须是 connect/listen，address 不能为空。"""
    if ch.mode not in ("connect", "listen"):
        raise ValueError(
            f"config: {name} channel mode must be 'connect' or 'listen', got {ch.mode!r}"
        )
    if not ch.address:
        raise ValueError(f"config: {name} channel address is required")
