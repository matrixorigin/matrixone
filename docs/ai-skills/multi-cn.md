# 多 CN 架构

## 概述

MO 的 CN 是无状态的，可以水平扩展。Proxy 负责将客户端连接路由到合适的 CN。

## Proxy（`pkg/proxy/`）

- `Router` 接口 — CN 选择策略
- `RefreshableRouter` — 动态刷新集群拓扑
- 连接路由 + 负载均衡
- 支持基于标签（label）的路由

## ClusterService（`pkg/clusterservice/`）

- `MOCluster` 接口 — 集群拓扑和元数据
- `ClusterClient` 接口 — CN 间通信
- `labelSupportedClient` — 基于标签路由的客户端

## QueryService（`pkg/queryservice/`）

- `QueryService` 接口 — 分布式查询处理
- `Session` 接口 — 会话管理
- 节点间消息路由

## Gossip（`pkg/gossip/`）

- 基于 memberlist 的节点发现
- 节点心跳 + 元数据交换

## 多 CN 部署配置

参考 `etc/launch-multi-cn/`，每个 CN 独立配置：
- 不同端口
- 相同的 TN 和 LogService 地址
- 可配置不同标签用于路由

## 与测试的关联

| 变更范围 | 影响的测试 |
|---------|----------|
| Proxy 路由逻辑 | BVT: tenant（多租户路由）|
| 连接均衡 | 稳定性: sysbench（高并发连接）|
| 杀 CN 恢复 | Chaos: 杀 CN 场景 |
| 节点发现 | Chaos: 网络分区场景 |
| 跨 CN 查询 | 大数据量测试（分布式执行）|
