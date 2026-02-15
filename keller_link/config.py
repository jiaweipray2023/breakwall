"""Configuration structures and loading logic."""

from dataclasses import dataclass, field
from typing import Any

import yaml


@dataclass
class ChannelConfig:
    mode: str = ""      # "connect" or "listen"
    address: str = ""   # host:port


@dataclass
class ServiceConfig:
    name: str = ""
    listen: str = ""    # inner: address to listen on
    target: str = ""    # outer: address to connect to


@dataclass
class InnerConfig:
    services: list[ServiceConfig] = field(default_factory=list)
    forward: ChannelConfig = field(default_factory=ChannelConfig)
    reverse: ChannelConfig = field(default_factory=ChannelConfig)


@dataclass
class OuterConfig:
    services: list[ServiceConfig] = field(default_factory=list)
    forward: ChannelConfig = field(default_factory=ChannelConfig)
    reverse: ChannelConfig = field(default_factory=ChannelConfig)


def _parse_channel(data: dict[str, Any]) -> ChannelConfig:
    return ChannelConfig(
        mode=data.get("mode", ""),
        address=data.get("address", ""),
    )


def _parse_services(data: list[dict[str, Any]]) -> list[ServiceConfig]:
    return [
        ServiceConfig(
            name=s.get("name", ""),
            listen=s.get("listen", ""),
            target=s.get("target", ""),
        )
        for s in data
    ]


def load_inner_config(path: str) -> InnerConfig:
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
    with open(path) as f:
        raw = yaml.safe_load(f)

    cfg = OuterConfig(
        services=_parse_services(raw.get("services", [])),
        forward=_parse_channel(raw.get("forward", {})),
        reverse=_parse_channel(raw.get("reverse", {})),
    )
    _validate_outer(cfg)
    return cfg


def _validate_inner(cfg: InnerConfig):
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
    if ch.mode not in ("connect", "listen"):
        raise ValueError(
            f"config: {name} channel mode must be 'connect' or 'listen', got {ch.mode!r}"
        )
    if not ch.address:
        raise ValueError(f"config: {name} channel address is required")
