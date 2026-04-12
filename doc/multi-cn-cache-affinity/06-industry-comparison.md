# 业界方案对比分析：哪个最适合改造 MO

## 1 四大系统方案概述

| 系统 | 核心机制 | 架构特点 |
|------|---------|---------|
| Snowflake | Consistent Hashing in Compute Layer | Micro-Partition 通过一致性哈希映射到 Virtual Warehouse 中的节点 |
| Databricks | Soft Affinity Scheduling | Task 调度器优先将 Scan 分配给上次缓存过该文件的节点 |
| ClickHouse Cloud | Distributed Cache Service | 独立的分布式缓存服务，计算节点通过 RPC 访问 |
| PolarDB | Shared Buffer Pool Extension | 共享存储 + 分布式 Buffer Pool，节点间可互相读取缓存页 |

## 2 逐一分析

#### Snowflake：一致性哈希在计算层，不在 Proxy

Snowflake 的一致性哈希发生在 Virtual Warehouse 内部的 Task Scheduler，不是在接入层。其流程：

```
Query → Cloud Services Layer（优化器）→ 生成 Scan Task
→ Task Scheduler 按 Micro-Partition 的一致性哈希分配到 Worker Node
→ Worker Node 本地 SSD Cache 命中 → 直接读取
→ Miss → 从 S3 加载
```

**不适合 MO 的原因：**
- Snowflake 的哈希发生在查询执行计划的 Task 分配阶段，MO 对应的是 `compile.go` 中的 `compileTableScan()` → `generateNodes()`
- 这不是 Proxy 层的改动，而是计算层调度器的改动
- MO 的 `generateNodes()` 当前按 CN 负载分配 Scan 节点，要改成按 Cache 亲和分配需要引入 Cache Catalog 元数据，改动量大
- Snowflake 是单写多读架构（一个 VW 内部），MO 是对等 CN 架构，模型不同

#### Databricks：方向正确，但改动在计算层

Databricks 的 Soft Affinity 思路：

```
Query → Optimizer → Scan Task
→ Scheduler 查询 Cache Catalog：哪个节点缓存了目标文件？
→ 优先分配给有缓存的节点（Soft Affinity）
→ 如果该节点负载过高 → 退化为负载均衡分配
```

**对 MO 的启示：**
- 方向正确：在查询调度时考虑 Cache 状态
- 但改动点在 `pkg/sql/compile/compile.go` 的 `generateNodes()` 函数，不在 Proxy
- 需要一个全局的 Cache Catalog（哪个 CN 缓存了哪些文件），MO 的 Gossip `keyTarget` map 本质上就是这个 Catalog，但粒度太细（block 级）且有 Bug
- 如果修复 Gossip 并改为文件级粒度，可以作为 Cache Catalog 使用
- 改动量中等，但涉及计算层核心调度逻辑，风险较高

#### ClickHouse Cloud Distributed Cache：最佳匹配

ClickHouse Cloud 的方案：

```
Compute Node₁ ─┐
Compute Node₂ ─┼─→ Distributed Cache Service ─→ Object Storage
Compute Node₃ ─┘
                    (独立进程，内网 RPC)
```

核心特点：
- 独立的分布式缓存服务，不嵌入计算节点
- 计算节点本地 Miss 后，先查 Distributed Cache，再回 Object Storage
- Cache Service 内部用一致性哈希分片，每个 Key 有 Primary Owner
- 计算节点扩缩容不影响 Cache（Cache 是独立服务）

**为什么最适合 MO：**

1. **MO 已有原型**：Remote Cache 就是 Distributed Cache 的雏形版本。每个 CN 的 MemCache 通过 Gossip 广播 Key 位置，其他 CN 通过 RPC 读取 — 这就是 ClickHouse Cloud Distributed Cache 的简化版
2. **改动路径最短**：不需要新增组件，只需修复现有 Remote Cache 的缺陷（8 个 Bug + 可扩展性问题）
3. **不涉及 Proxy 和计算层**：改动完全在 `fileservice` 和 `gossip` 包内，不影响路由和查询调度
4. **数据安全**：Cache 层的改动不会导致数据正确性问题。最坏情况是 Cache Miss 回退到 S3，查询变慢但不会出错
5. **渐进式启用**：可以先在测试环境启用 `remote-cache-enabled = true`，验证稳定后再推生产

**与 MO Remote Cache 的对应关系：**

| ClickHouse Cloud | MO Remote Cache | 差距 |
|-----------------|-----------------|------|
| Distributed Cache Service | 每个 CN 的 MemCache + Gossip 路由 | MO 没有独立 Cache 进程，但效果等价 |
| 一致性哈希分片 | Gossip `keyTarget` map | MO 是全量广播，不是哈希分片 |
| RPC 读取 | `QueryService.GetCacheData` | 已实现，但有 2s 超时和只查 memCache 的问题 |
| Cache 独立扩缩容 | N/A | MO 的 Cache 绑定在 CN 上，无法独立扩缩 |

**结论：修复 MO 的 Remote Cache = 实现 ClickHouse Cloud Distributed Cache 的核心功能。**

#### PolarDB：架构不匹配

PolarDB 的方案：
- 一写多读架构（1 个 RW 节点 + N 个 RO 节点）
- 共享存储（RDMA 互联的分布式块存储）
- Buffer Pool Extension：RO 节点 Miss 时可以从 RW 节点的 Buffer Pool 读取

**不适合 MO 的原因：**
- PolarDB 是一写多读，MO 是对等 CN（每个 CN 都可以读写）
- PolarDB 依赖共享存储（RDMA），MO 用对象存储（S3），网络模型完全不同
- Buffer Pool Extension 假设有一个"权威"节点（RW），MO 没有这个角色
- 架构差异太大，无法借鉴

## 3 对比总结

| 维度 | Snowflake | Databricks | ClickHouse Cloud | PolarDB |
|------|-----------|------------|-----------------|---------|
| 改动层 | 计算层调度器 | 计算层调度器 | Cache 层 | 存储层 |
| MO 对应改动点 | `compile.go` | `compile.go` | `fileservice` + `gossip` | 无对应 |
| 改动量 | 大 | 中 | 小（修复现有代码） | 不可行 |
| 数据安全风险 | 中（改调度逻辑） | 中（改调度逻辑） | 低（只改 Cache） | N/A |
| 是否需要新组件 | 需要 Cache Catalog | 需要 Cache Catalog | 不需要（已有 Gossip） | 需要共享存储 |
| 与 MO 架构匹配度 | 低 | 中 | 高 | 极低 |

**AP 结论：ClickHouse Cloud 的 Distributed Cache 方案最适合 MO 的 AP 工作负载，且 MO 已有 Remote Cache 作为基础，修复缺陷即可实现核心功能。**

**但上述四个系统都是 AP 数据库。MO 是 HTAP 系统，TP 工作负载需要不同的策略。详见 [09-tp-ap-analysis.md](09-tp-ap-analysis.md)。**

## 4 云原生 TP 数据库方案对比

上述 AP 系统的方案核心是"Miss 后补救"（Remote Cache / Distributed Cache），这对 AP 有效但对 TP 不够。以下是云原生 TP 数据库的 Cache 策略：

| 系统 | 架构类型 | Cache 策略 | Cache 冗余问题 |
|------|---------|-----------|---------------|
| Aurora | 共享存储 + 单写多读 | 独立 Buffer Pool + WAL replay 同步 | 不严重（单写控制） |
| PolarDB | 共享存储 + 单写多读 | 独立 Buffer Pool + WAL replay | 不严重（单写控制） |
| Neon | 共享存储 + 单写多读 | Local File Cache（NVMe）+ Pageserver | 不严重（LFC 覆盖工作集） |
| CockroachDB | Shared-Nothing 分片 | Leaseholder 本地 Block Cache | 无（分片天然亲和） |
| TiDB | Shared-Nothing 分片 | TiKV 本地 Block Cache | 无（分片天然亲和） |
| PolarDB-MP | 多主共享内存 | RDMA Buffer Fusion | 无（RDMA 共享，μs 级延迟） |

#### Aurora / PolarDB / Neon：单写多读，Cache 由写节点控制

这类系统的 Cache 冗余问题天然较轻：
- 只有 1 个 RW 节点负责写入，RO 节点通过 WAL replay 获取变更
- 每个节点的 Cache 内容由其接收的读请求决定，不存在"所有节点缓存相同数据"的问题
- Neon 的 LFC 方案特别值得借鉴：在内存 Cache 和远程存储之间加一层大容量 NVMe Cache，只要 LFC 覆盖工作集，性能等同本地磁盘数据库

#### CockroachDB / TiDB：数据分片 = 天然 Cache 亲和

Shared-Nothing 架构通过数据分片从根本上消除了 Cache 冗余：
- 每个数据 Range 有一个 Leaseholder/Leader 节点
- 所有读请求被路由到 Leaseholder
- Leaseholder 的 Cache 自然包含该 Range 的热点数据
- **这是"路由亲和"的极致形式 — 数据分片决定路由，路由决定 Cache**

#### PolarDB-MP：RDMA 让 Remote Cache 延迟降到微秒级

PolarDB-MP（SIGMOD 最佳论文）用 RDMA 实现了 Buffer Fusion：
- 任何节点可以通过 RDMA 直接读取其他节点的 Buffer Pool
- RDMA 延迟 ~1-5μs，比 RPC（~0.5-1ms）快 100-1000x
- 本质上就是"Remote Cache"，但延迟足够低，对 TP 也有效

## 5 MO 的架构独特性与方案选择

MO 不完全属于上述任何一类架构：

| 特征 | MO | Aurora/PolarDB | CockroachDB/TiDB | PolarDB-MP |
|------|-----|---------------|-------------------|------------|
| 计算节点 | 对等 CN | 1 RW + N RO | 分片 Leader | 对等多主 |
| 存储 | 共享 S3 | 共享块存储 | 本地 + Raft | 共享块存储 + RDMA |
| 数据分区 | 无 | 单写控制 | Range 分片 | 无 |
| Cache 冗余 | 严重 | 不严重 | 无 | 无 |

**MO 的 Cache 冗余问题是架构独特性导致的** — 共享存储 + 对等计算 + 无数据分区。

**方案选择：**
- AP 优化：借鉴 ClickHouse Cloud → 修复并启用 Remote Cache（路径 A）
- TP 优化：借鉴 Shared-Nothing 思路 → 引入软亲和路由（路径 B），减少 Cache Miss
- 长期：借鉴 Neon LFC → 增大本地 diskCache 容量，覆盖工作集

**最终结论：MO 作为 HTAP 系统，不能只用 AP 的方案（Remote Cache），也需要 TP 的方案（路由亲和）。两条路径应并行推进。**


