# 测试环境配置

## 本地单机部署

```bash
# 编译
make build

# 启动（单机集群：LogService + TN + CN）
./mo-service -launch etc/launch/launch.toml
```

默认端口：`6001`（MySQL 协议）

## Docker 部署

```bash
# 使用 docker compose
cd etc/launch-tae-compose/
docker compose up -d
```

## 本地多 CN 部署

配置目录：`etc/launch-multi-cn/`

```bash
# 启动集群（1 LogService + 1 TN + 多 CN）
./mo-service -launch etc/launch-multi-cn/launch.toml
```

每个 CN 使用不同端口基数，共享同一个 TN 和 LogService。

## 加速 Flush / GC（测试调参）

在 TN 配置中调整：

```toml
# tn.toml - 加快 flush
[tn.Txn.Storage]
# 减小 checkpoint 间隔
checkpoint-flush-interval = "1s"
# 减小 GC 间隔
gc-check-interval = "1s"
```

**目的：** 测试时加快数据落盘和垃圾回收，缩短测试等待时间。

## BVT 测试运行

```bash
# 需要 mo-tester 工具
cd mo-tester/
./run.sh -p /path/to/matrixone/test/distributed/cases/optimizer/

# 跑单个 .test 文件
./run.sh -p /path/to/test/distributed/cases/window/window_basic.test
```

## 稳定性/Chaos/大数据测试

这些测试通过 mo-nightly-regression 仓库的 GitHub Actions Workflow 运行：

```bash
# 触发方式：手动或定时（nightly）
# 仓库：matrixorigin/mo-nightly-regression
# 分支：main (稳定性/chaos/PITR/snapshot), big_data (大数据量)
```

## 环境变量

| 变量 | 说明 |
|------|------|
| `MO_WORKSPACE` | MO 工作目录 |
| `MO_LOG_LEVEL` | 日志级别 (debug/info/warn/error) |
| `CGO_CFLAGS` | CGO 编译标志（含 thirdparties 路径）|
