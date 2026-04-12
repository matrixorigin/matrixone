# 问题背景与业内参考

## 1. 问题背景

MO 是云原生架构，数据存储在对象存储中，计算层由多个 CN 节点组成，请求经 Proxy 分发到各 CN。每个 CN 拥有独立的 Memory Cache。

当前问题：在表数量多、单表数据量不大、高频访问的场景下，由于 Proxy 没有做亲和性路由，同一张表的请求会被分发到不同 CN，导致：

- 每个 CN 都独立从对象存储加载并缓存相同的数据
- N 个 CN 的等效 Cache 容量从 Total 退化为 Total/N
- Cache 冗余放大，Miss Rate 居高不下
- Disk Cache 作为二级缓存在高频场景下延迟仍然不可接受

## 2. 业内参考

### 2.1 云原生数仓

| 系统 | 策略 | 核心思路 |
|------|------|----------|
| Snowflake | Consistent Hashing | Micro-Partition 通过一致性哈希映射到固定节点，消除 Cache 冗余 |
| Databricks | Soft Affinity | Task 调度优先分配到上次缓存过该文件的节点，非强绑定 |
| ClickHouse Cloud | Primary Owner | 每个 Part 有 Owner 节点，其他节点优先从 Owner 拉取 |
| PolarDB | Buffer Pool Extension | 共享存储 + 分布式 Buffer Pool，节点间可互相读取缓存页 |

### 2.2 传统分布式系统

| 系统/协议 | 策略 | 核心思路 |
|-----------|------|----------|
| Memcached 集群 | Consistent Hashing | 客户端按 Key 哈希决定数据存放在哪个节点，天然无冗余 |
| Redis Cluster | Hash Slot | 16384 个 Slot 分配到各节点，数据按 Slot 路由，扩缩容时只迁移 Slot |
| Cassandra/Dynamo | Virtual Nodes | 虚拟节点保证负载均衡，每个 Key 有 Primary + N 副本 |
| NUMA 架构 | Local-First | CPU 优先访问本地内存，远程可达但有额外延迟 |
| CDN | 多级回源 | 边缘 Miss 后向上级节点请求，而非直接回源站 |
| CPU Cache | MESI 协议 | 多核间协议维护一致性，某核 Miss 可从其他核 Cache 获取 |

传统分布式系统的核心经验：**数据分片 + 路由亲和 + 多级回退 + 协同淘汰**。

## 3. 方案总览

| 方案 | 核心改动点 | 改动量 | 效果 | 适用阶段 |
|------|-----------|--------|------|----------|
| 一：Proxy Consistent Hash 路由 | Proxy | 中 | 高 | 短期 |
| 二：CN 分组 + 表绑定 | Proxy + 运维 | 低 | 中 | 短期 |
| 三：独立分布式 Cache 池 | 新增组件 | 高 | 最高 | 长期 |
| 四：Cache-Aware 查询调度 | 计算层 | 中 | 高 | 中期 |
| 五：Predictive Prefetch + 协同淘汰 | CN Cache | 低 | 中低 | 短期 |
| 六：分层 Cache + MESI 式协同 | CN + Proxy | 中高 | 高 | 中期 |
