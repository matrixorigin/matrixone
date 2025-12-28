# EOF 延迟问题复现工具

## 问题描述

当 mysql 客户端通过代理连接 MO 执行 LOAD DATA LOCAL 时，如果客户端断开连接，
MO 可能需要很长时间（几十分钟甚至几小时）才能检测到连接断开并返回 EOF。

## 复现步骤

### 1. 启动 MO

确保 MO 正常运行在 6001 端口。

### 2. 启动模拟代理

```bash
# 延迟 60 秒后才关闭后端连接
python proxy.py --backend-host 127.0.0.1 --backend-port 6001 --listen-port 16001 --delay 60
```

### 3. 运行测试脚本

```bash
chmod +x test_load_local.sh
./test_load_local.sh
```

### 4. 观察结果

- mysql 客户端会在 10 秒后超时退出
- 代理会等待 60 秒后才关闭与 MO 的连接
- MO 日志中的 `readThenWrite error (attempt 1): EOF` 会在 mysql 退出 60 秒后才出现

## 参数说明

### proxy.py

- `--listen-port`: 代理监听端口，默认 16001
- `--backend-host`: MO 地址，默认 127.0.0.1
- `--backend-port`: MO 端口，默认 6001
- `--delay`: 客户端断开后，延迟多少秒才断开后端连接，默认 60

### 环境变量

- `PROXY_HOST`: 代理地址
- `PROXY_PORT`: 代理端口
- `MO_USER`: MO 用户名
- `MO_PASSWORD`: MO 密码
- `DATABASE`: 测试数据库名
