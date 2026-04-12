# 方案详细设计

## 方案一：Proxy Consistent Hash 路由（推荐短期落地）

借鉴 Snowflake + Memcached 的经典一致性哈希思路。

**架构：**

```
Client → Proxy (Consistent Hash Router) → CN₁ (负责 Table A, D, G ...)
                                        → CN₂ (负责 Table B, E, H ...)
                                        → CN₃ (负责 Table C, F, I ...)
```

**设计要点：**

- Proxy 对查询做轻量 SQL 解析，提取主表名作为 Hash Key
- 通过 Consistent Hash Ring（每 CN 128~256 虚拟节点）映射到目标 CN
- 单表查询：按表名 Hash 路由
- 多表 Join：按主表路由，或退化为 Session 亲和
- DDL / 系统查询：保持原有轮询策略

**CN 间 Peer Fetch 协议（借鉴 NUMA Local-First + CDN 回源）：**

```
查询到达 CN₂ → L1 本地 Memory Cache 查找
                → Miss → 计算 Owner CN（Hash Ring）
                → 向 Owner CN₁ 发起 RPC Fetch（内网，~ms 级）
                → Owner 也 Miss → 回退对象存储
```

**扩缩容（借鉴 Redis Cluster Hash Slot 迁移）：**

- 新增 CN：加入 Hash Ring，仅影响相邻虚拟节点，Cache 逐步预热
- 缩减 CN：移除后表自动迁移到 Ring 上下一个节点
- 预热窗口期允许从原 Owner Peer Fetch 加速

**热点处理（借鉴 Dynamo Power of Two Choices）：**

- 监控各 CN 的 QPS 和 Cache 命中率
- 热点表允许扩散到多个 CN（接受冗余换负载均衡）

---

## 方案二：CN 分组 + 表绑定

不改 Proxy 路由逻辑，在 CN 层面做逻辑分组。

**设计要点：**

- 将 CN 分成若干组，每组负责一批表或一个业务域
- 例如：交易表 → CN 组 A（3 节点），报表 → CN 组 B（2 节点）
- Proxy 按目标表所属分组路由
- 组内仍可轮询，但 Cache 冗余从 N 降低到组内节点数

**优点：** 改动小，运维可控，可按业务域隔离资源
**缺点：** 静态绑定，灵活性差，需要人工规划，负载不均时调整成本高

---

## 方案三：独立分布式 Cache 池（Disaggregated Cache）

借鉴 Memcached/Redis 集群的独立缓存层思路，将 Cache 从 CN 中抽离。

**架构：**

```
CN₁ ─┐
CN₂ ─┼─→ Distributed Memory Cache Pool ─→ 对象存储
CN₃ ─┘
        (RDMA / 高速内网互联)
```

**设计要点：**

- Cache Pool 由独立的缓存节点组成，内部用 Consistent Hashing 分片
- 所有 CN 共享统一 Cache，彻底消除冗余
- Cache 容量 = 缓存池总内存，与 CN 数量解耦
- CN 本地保留小量 L1 Cache（热点数据），Cache Pool 作为 L2
- 缓存节点间用 RDMA 或高速网络互联，延迟控制在几十微秒

**参考实现：** Alluxio、Facebook CacheLib、PolarDB Buffer Pool Extension

**优点：** 最彻底的方案，Cache 利用率最高，CN 可以完全无状态
**缺点：** 引入新组件，运维复杂度增加，多一跳网络开销

---

## 方案四：Cache-Aware 查询调度

不改 Proxy，改查询执行器。借鉴 Presto/Trino 的 Soft Affinity Scheduling。

**设计要点：**

- 每个 CN 维护 Cache Catalog（记录缓存了哪些 Block）
- 定期向轻量元数据服务汇报 Cache 状态
- 查询调度时，Coordinator 查 Cache Catalog
- 将 Scan 算子优先分配给已缓存对应数据块的 CN
- 未命中时退化为负载均衡调度

**优点：** 对 Proxy 完全透明，多表 Join 场景也能受益
**缺点：** 需要在计算层加调度逻辑，Cache Catalog 同步有延迟

---

## 方案五：Predictive Prefetch + 协同淘汰

不改路由，让 Cache 本身更聪明。借鉴 CPU Cache 的 Prefetch 和 MESI 协议思想。

**Predictive Prefetch：**

- 基于查询历史和访问模式，预测即将被访问的数据块
- 在查询到达前提前将数据加载到 Memory Cache
- 可结合时间序列模型或简单的 LRU-K 频率统计

**协同淘汰（借鉴 MESI 协议）：**

- CN 之间维护轻量的 Cache 目录信息（类似 MESI 的 Directory-Based Protocol）
- CN₁ 要淘汰某 Block 时，检查其他 CN 是否也缓存了该 Block
  - 如果有其他副本 → 放心淘汰
  - 如果是全局唯一副本 → 提高保留优先级，或通知其他 CN 接管
- 避免所有 CN 同时淘汰同一个 Block 导致集体 Miss

**优点：** 改动最小，可以独立上线
**缺点：** 只是优化而非根治，效果有限

---

## 方案六：分层 Cache + MESI 式协同（传统分布式思路的完整移植）

将传统分布式系统的经典架构完整移植到 MO 的多 CN 场景。

**核心思想：把多个 CN 的 Memory Cache 看作一个分布式缓存系统，用传统分布式缓存的全套机制来管理。**

**分层设计（借鉴 CPU L1/L2/L3 + NUMA）：**

| 层级 | 介质 | 延迟 | 角色 |
|------|------|------|------|
| L1 | CN 本地 Memory（热区） | ~μs | 高频热点数据，小容量，LRU 淘汰 |
| L2 | CN 本地 Memory（温区） | ~μs | 亲和路由命中的数据，主容量区 |
| L3 | Peer CN Memory（RPC） | ~ms | 本地 Miss 时从 Owner CN 获取 |
| L4 | 本地 Disk Cache | ~ms | 冷数据兜底 |
| L5 | 对象存储 | ~100ms | 最终数据源 |

**Cache 一致性协议（借鉴 MESI）：**

每个 Block 在全局有以下状态：
- **Exclusive**：仅一个 CN 缓存，可直接读写
- **Shared**：多个 CN 缓存（冗余），只读
- **Invalid**：数据已更新，缓存失效

状态转换：
- CN₁ 加载 Block X → 标记为 Exclusive
- CN₂ 也加载 Block X → 两者都转为 Shared
- Block X 底层数据更新 → 所有持有者标记为 Invalid

**全局 Cache Directory（借鉴 Directory-Based Coherence）：**

- 维护一个轻量的全局目录，记录每个 Block 被哪些 CN 缓存
- 可以嵌入到现有的 TN（Transaction Node）或独立的元数据服务中
- 目录信息不需要强一致，允许短暂的过期（最终一致即可）

**数据分片（借鉴 Redis Cluster Hash Slot）：**

- 将所有表的数据块映射到 N 个 Slot
- 每个 Slot 有一个 Primary CN 和可选的 Secondary CN
- Primary 负责该 Slot 数据的缓存，Secondary 作为热备
- 扩缩容时只迁移 Slot，不影响全局

**优点：** 体系完整，兼顾性能、一致性和可扩展性
**缺点：** 实现复杂度较高，需要引入全局目录和状态管理
