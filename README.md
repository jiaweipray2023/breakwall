# Breakwall

通过正反向隔离装置，远程运维 DMZ 区设备的 SSH/VNC 代理系统。

## 架构

```
内网 (安全区)                                                      外网 (DMZ 区)
┌──────────┐      ┌──────────┐                               ┌──────────┐      ┌──────────┐
│ SSH/VNC  │─────→│ bw-inner │───[正向隔离装置]──────────────→│ bw-outer │─────→│ SSH/VNC  │
│ 客户端    │←─────│ 内网代理  │←──[反向隔离装置]──────────────│ 外网代理  │←─────│ 服务器    │
└──────────┘      └──────────┘                               └──────────┘      └──────────┘
```

- **bw-inner** 部署在内网，对内网用户暴露标准的 TCP 端口（如 SSH :2222, VNC :5900）
- **bw-outer** 部署在外网 DMZ 区，连接 DMZ 区内的实际 SSH/VNC 服务器
- 客户端请求数据通过**正向隔离装置**（内→外单向）传输到 bw-outer
- 服务器响应数据通过**反向隔离装置**（外→内单向）传输回 bw-inner
- 内网用户使用标准 SSH/VNC 客户端即可访问，无需任何特殊软件

## 原理

正反向隔离装置是物理单向传输设备，只允许数据单向流动：

- **正向隔离**：仅允许内网→外网方向的数据
- **反向隔离**：仅允许外网→内网方向的数据

Breakwall 将双向的 SSH/VNC 会话拆分为两条单向数据流，分别通过正向和反向隔离装置传输，从而在满足安全隔离要求的前提下实现远程运维。

多个 SSH/VNC 会话通过自定义的二进制帧协议在通道上复用，支持并发访问。

## 构建

```bash
go build -o bw-inner ./cmd/bw-inner
go build -o bw-outer ./cmd/bw-outer
```

## 配置

复制示例配置文件并按实际环境修改：

```bash
cp configs/inner.example.yaml configs/inner.yaml
cp configs/outer.example.yaml configs/outer.yaml
```

### 内网代理配置 (inner.yaml)

```yaml
services:
  - name: "ssh-server1"    # 服务名称，需与 outer 配置对应
    listen: ":2222"         # 内网监听地址
  - name: "vnc-server1"
    listen: ":5900"

forward:                    # 正向通道 (内→外)
  mode: "connect"           # 主动连接正向隔离装置
  address: "10.0.1.100:9001"

reverse:                    # 反向通道 (外→内)
  mode: "listen"            # 等待反向隔离装置连接
  address: ":9002"
```

### 外网代理配置 (outer.yaml)

```yaml
services:
  - name: "ssh-server1"
    target: "192.168.1.10:22"    # DMZ 区实际 SSH 服务器
  - name: "vnc-server1"
    target: "192.168.1.10:5900"  # DMZ 区实际 VNC 服务器

forward:                         # 正向通道 (内→外)
  mode: "listen"                 # 等待正向隔离装置连接
  address: ":9001"

reverse:                         # 反向通道 (外→内)
  mode: "connect"                # 主动连接反向隔离装置
  address: "10.0.2.100:9002"
```

## 运行

```bash
# 在内网服务器上运行
./bw-inner -config configs/inner.yaml

# 在外网 DMZ 服务器上运行
./bw-outer -config configs/outer.yaml
```

内网用户使用标准客户端连接即可：

```bash
# SSH 连接
ssh user@inner-proxy-host -p 2222

# VNC 连接
vncviewer inner-proxy-host:5900
```

## 帧协议

会话在通道上通过二进制帧协议复用：

```
+----------+----------+----------+----------+
| Type (1) | SessID(4)| Length(4)| Data (N) |
+----------+----------+----------+----------+
```

| Type | 含义 | 说明 |
|------|------|------|
| 0x01 | OPEN | 打开会话，Data 为服务名称 |
| 0x02 | DATA | 会话数据 |
| 0x03 | CLOSE | 关闭会话 |
| 0x04 | PING | 心跳请求 |
| 0x05 | PONG | 心跳响应 |

## 通道模式

正向和反向通道各支持两种模式：

- **connect**：代理主动发起 TCP 连接（适用于隔离装置提供透传端口的场景）
- **listen**：代理被动监听 TCP 连接（适用于隔离装置主动推送数据的场景）

根据实际的隔离装置网络拓扑选择合适的模式。典型配置：

| 通道 | bw-inner 侧 | bw-outer 侧 |
|------|-------------|-------------|
| 正向 | connect | listen |
| 反向 | listen | connect |
