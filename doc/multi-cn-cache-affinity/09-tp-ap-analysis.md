# TP vs AP 工作负载分析：Remote Cache 的根本局限

## 1 问题本质

之前的方案分析主要参考了 Snowflake、Databricks、ClickHouse Cloud 等系统。这些系统有一个共同特征：**它们都是 AP（分析型）数据库**。AP 工作负载的 Cache 读取特征与 TP（事务型）工作负载有本质区别，而 MO 是 HTAP（TP+AP 混合）系统，必须同时考虑两种场景。

## 2 AP vs TP 的 Cache 读取特征对比

| 维度 | AP 工作负载 | TP 工作负载 |
|------|-----------|-----------|
| 单次读取数据量 | 大（MB~GB 级，全表扫描/大范围扫描） | 小（KB 级，点查/小范围查询） |
| 读取模式 | 顺序扫描大量 block | 精确定位少量 block |
| S3 延迟占比 | 高（~100ms vs 总查询时间数秒~数分钟） | 极高（~100ms vs 总查询时间 <10ms） |
| RPC 开销占比 | 低（~0.5-1ms vs 100ms S3 延迟，可忽略） | 高（~0.5-1ms vs 4KB 数据读取，不可忽略） |
| Cache Miss 策略 | "Miss 后补救"有效（Remote Cache RPC 比 S3 快 100x） | "不要 Miss"更重要（RPC 开销接近 S3） |

### 关键数据分析

**AP 场景（Remote Cache 有效）：**
```
查询：SELECT SUM(amount) FROM orders WHERE date > '2024-01-01'
读取数据量：~100MB（数千个 block）
S3 延迟：~100ms per request（可并行，但总延迟仍高）
Remote Cache RPC：~1ms per request
收益：100ms → 1ms，约 100x 提升
RPC 开销占比：1ms / 100ms = 1%，可忽略
```

**TP 场景（Remote Cache 收益有限）：**
```
查询：SELECT * FROM users WHERE id = 12345
读取数据量：~4KB（1-2 个 block）
S3 延迟：~100ms（单次请求）
Remote Cache RPC：~0.5-1ms（网络往返 + 序列化/反序列化）
本地 memCache 命中：~0.01ms（微秒级）
收益：100ms → 1ms（如果 Remote Cache 命中）
问题：本地 memCache 命中只需 0.01ms，Remote Cache 慢 100x
```

**TP 场景的核心矛盾：**

对于 TP 点查，最优路径是本地 memCache 命中（~0.01ms）。Remote Cache 虽然比 S3 快（1ms vs 100ms），但比本地 memCache 慢 100 倍。如果通过路由亲和能让 TP 请求命中本地 memCache，就完全不需要 Remote Cache。

**换言之：TP 的正确策略是"不要 Miss"（路由亲和），而非"Miss 后补救"（Remote Cache）。**

## 3 MO 代码中的 TP/AP 区分证据

MO 代码已经在 Cache 层面区分了 TP 和 AP 行为：

```go
// pkg/vm/engine/readutil/reader.go
type reader struct {
    readBlockCnt uint64 // 已读取的 block 计数
    threshHold   uint64 // 超过阈值后跳过 memCache 写入
}

// 读取 block 时的策略判断
var policy fileservice.Policy
if r.readBlockCnt > r.threshHold {
    policy = fileservice.SkipMemoryCacheWrites  // AP 大扫描：跳过 memCache 写入
}
r.readBlockCnt++
```

这段代码的含义：
- 当一个 reader 读取的 block 数超过 `threshHold` 时，后续 block 不再写入 memCache
- 这是为了防止 AP 大扫描"污染" memCache，把 TP 热点数据挤出去
- **说明 MO 团队已经意识到 TP 和 AP 对 memCache 的需求不同**

## 4 Remote Cache 对 TP/AP 的实际效果评估

### 4.1 AP 工作负载：Remote Cache 有效

| 场景 | 无 Remote Cache | 有 Remote Cache | 提升 |
|------|----------------|-----------------|------|
| 全表扫描（100MB） | 100ms × N requests | 1ms × N requests | ~100x |
| 范围扫描（10MB） | 100ms × N requests | 1ms × N requests | ~100x |
| 聚合查询（大量 block） | 秒级 | 百毫秒级 | ~10x |

AP 场景下，Remote Cache 的 RPC 开销（~1ms）相对于 S3 延迟（~100ms）可以忽略。即使有 Gossip 状态滞后导致的 false positive（RPC 到目标 CN 但 Cache Miss），额外的 1ms 开销也不影响整体性能。

### 4.2 TP 工作负载：Remote Cache 收益有限

| 场景 | 本地 memCache 命中 | Remote Cache 命中 | S3 回源 |
|------|-------------------|-------------------|---------|
| 点查（4KB） | 0.01ms | 0.5-1ms | 100ms |
| 小范围查询（40KB） | 0.05ms | 1-2ms | 100ms |
| 索引查找（8KB） | 0.02ms | 0.5-1ms | 100ms |

TP 场景的延迟分布：
- **最优**：本地 memCache 命中 → 0.01ms（微秒级）
- **次优**：Remote Cache 命中 → 0.5-1ms（毫秒级，比本地慢 50-100x）
- **最差**：S3 回源 → 100ms

Remote Cache 确实比 S3 快 100x，但对于 TP 来说，0.5-1ms 的延迟仍然不理想。一个 TP 事务可能涉及多次点查，每次 0.5-1ms 的 Remote Cache 延迟会累积。

**更关键的问题：** 如果 N 个 CN 各自缓存相同的热点数据（当前无亲和的状态），TP 请求大概率能命中本地 memCache（因为热点数据在每个 CN 上都有副本）。这种情况下 Remote Cache 完全没有用武之地。

**Remote Cache 对 TP 真正有价值的场景：** 当 Cache 容量不足以容纳所有热点数据时，部分热点数据被淘汰，此时 Remote Cache 可以从其他 CN 获取。但这恰恰说明了问题 — 如果通过路由亲和减少冗余，每个 CN 的有效 Cache 容量增大，热点数据被淘汰的概率降低，从根本上减少了 TP 的 Cache Miss。

## 5 修订后的方案优先级

基于 TP/AP 分析，方案优先级需要调整：

### 原方案（路径 A 优先）

```
路径 A（Remote Cache 修复）→ 路径 B（Proxy 亲和）
理由：Remote Cache 的 ROI 最高，Miss 延迟从 100ms → 1ms
```

### 修订方案（TP/AP 分治）

```
TP 优化：路由亲和（减少 Miss）> Remote Cache（补救 Miss）
AP 优化：Remote Cache（补救 Miss）> 路由亲和（减少 Miss）
```

**修订后的优先级：**

| 优先级 | 方案 | 目标工作负载 | 理由 |
|--------|------|-------------|------|
| P0 | Remote Cache Bug 修复（Phase 1） | AP + TP 兜底 | 低风险，快速见效 |
| P0 | Proxy 路由亲和（路径 B） | TP | TP 的核心优化手段 |
| P1 | Remote Cache 层级调整（Phase 2） | AP | AP 的核心优化手段 |
| P2 | Gossip 可扩展性（Phase 3） | AP 大规模 | 大规模 AP 场景 |

**关键变化：Proxy 路由亲和从 P2 提升到 P0，与 Remote Cache Bug 修复并行推进。**

理由：
1. TP 工作负载对延迟极其敏感，Remote Cache 的 RPC 开销（~1ms）对 TP 不可接受
2. TP 的正确策略是通过路由亲和提高本地 memCache 命中率，而非依赖 Remote Cache
3. AP 工作负载对 RPC 开销不敏感，Remote Cache 是 AP 的有效优化
4. 两条路径正交，可以并行推进

## 6 云原生 TP 数据库的解决方案

为了验证上述分析，我们调研了主流云原生 TP 数据库如何解决类似的 Cache 问题。

### 6.1 三大架构类别

#### 类别一：共享存储 + 单写多读（Aurora、PolarDB、Neon、AlloyDB）

**架构特征：**
- 1 个 RW（读写）节点 + N 个 RO（只读）节点
- 共享存储层（Aurora 用自研分布式存储，PolarDB 用 RDMA 共享块存储，Neon 用 Pageserver）
- 每个节点有独立的本地 Buffer Pool / Cache

**Cache 策略：**
- 每个节点维护独立的本地 Cache，不存在"Remote Cache"概念
- RO 节点通过 WAL replay 从 RW 节点获取数据变更，保持 Cache 一致性
- 不存在 Cache 冗余问题，因为数据写入由单一 RW 节点控制

**Neon 的 Local File Cache（LFC）方案：**

Neon 在 PostgreSQL 的 `shared_buffers` 和远程 Pageserver 之间引入了一层 NVMe 本地缓存（Local File Cache）：

```
shared_buffers（内存，GB 级）
    ↓ miss
Local File Cache（NVMe SSD，数十~数百 GB）
    ↓ miss
Pageserver（远程存储）
```

关键设计原则：
- LFC 大小应覆盖工作集（working set），而非整个数据库
- 只要 LFC 能容纳工作集，性能与本地磁盘 PostgreSQL 相当
- 不需要节点间 Cache 协调，因为每个节点的 LFC 独立且足够大

**对 MO 的启示：** 如果 MO 的 diskCache（本地 SSD）足够大，能覆盖每个 CN 的工作集，那么 Remote Cache 的需求会大幅降低。问题在于当前多 CN 冗余缓存导致有效容量不足。

**Aurora 的 Cluster Cache Management：**

Aurora 有一个"指定读副本"（Designated Reader）机制：
- 指定一个 RO 节点作为 failover 目标
- 该节点的 Buffer Pool 会"预热"以匹配 RW 节点的 Cache 内容
- Failover 时新的 RW 节点已有热 Cache，避免冷启动

这本质上是一种 Cache 亲和 — 通过预热确保特定节点有正确的 Cache 内容。

#### 类别二：Shared-Nothing 分片架构（CockroachDB、TiDB、YugabyteDB、Spanner）

**架构特征：**
- 数据通过 Raft/Paxos 分片到不同节点
- 每个节点"拥有"自己的数据分片
- 路由层将请求发送到数据所在的节点

**Cache 策略：**
- Cache 亲和是天然的 — 请求被路由到数据所在节点，该节点的 Cache 自然包含相关数据
- 不存在 Cache 冗余问题，因为每个数据分片只在一个节点上被访问（Leader）
- 不需要 Remote Cache，因为数据本身就在本地

**CockroachDB 的 Leaseholder 机制：**
- 每个 Range（数据分片）有一个 Leaseholder 节点
- 所有读请求被路由到 Leaseholder
- Leaseholder 的 Block Cache 自然包含该 Range 的热点数据
- 这就是"路由亲和"的极致形式 — 数据分片决定了路由，路由决定了 Cache

**TiDB 的演进（TiDB X / Serverless）：**
- 传统 TiDB 是 Shared-Nothing（TiKV 分片存储）
- 新一代 TiDB X 正在向共享对象存储演进，本地保留热数据缓存
- 这与 MO 的架构方向类似，值得持续关注

#### 类别三：多主共享内存（PolarDB-MP）

**架构特征：**
- 所有节点对等，都可以读写
- 使用 RDMA 构建分布式共享内存（Disaggregated Shared Memory）
- 节点间通过 RDMA 协议实现 Cache 一致性（类似硬件 MESI 协议）

**Cache 策略：**
- Buffer Fusion：任何节点都可以通过 RDMA 直接读取其他节点的 Buffer Pool
- RDMA 延迟 ~1-5μs，比 RPC（~0.5-1ms）快 100-1000x
- 这是 SIGMOD 最佳论文级别的方案，性能是同类系统的 3x

**对 MO 的启示：** PolarDB-MP 的 Buffer Fusion 本质上就是"Remote Cache"，但用 RDMA 替代了 RPC，延迟从毫秒级降到微秒级。这说明 Remote Cache 的方向是对的，但 RPC 的延迟是瓶颈。MO 在云环境下无法使用 RDMA，因此 Remote Cache 对 TP 的效果受限于网络延迟。

### 6.2 MO 的架构独特性

MO 的架构不完全属于上述任何一类：

| 特征 | MO | Aurora/PolarDB | CockroachDB/TiDB | PolarDB-MP |
|------|-----|---------------|-------------------|------------|
| 计算节点关系 | 对等 CN | 1 RW + N RO | 分片 Leader | 对等多主 |
| 存储 | 共享对象存储（S3） | 共享块存储 | 本地 + Raft 复制 | 共享块存储 + RDMA |
| 数据分区 | 无（任何 CN 读任何数据） | 单写控制 | Raft Range 分片 | 无（RDMA 共享） |
| Cache 冗余 | 严重（N 份副本） | 不严重（单写控制） | 无（分片天然亲和） | 无（RDMA 共享） |
| Cache 协调 | Gossip + RPC（未启用） | WAL replay | 不需要（分片路由） | RDMA Buffer Fusion |

**MO 的 Cache 冗余问题是架构独特性导致的：**
- 像 Aurora/PolarDB 一样用共享存储，但没有单写节点来控制数据分布
- 像 CockroachDB/TiDB 一样有对等计算节点，但没有数据分片来天然实现 Cache 亲和
- 像 PolarDB-MP 一样所有节点可以访问所有数据，但没有 RDMA 来实现低延迟 Cache 共享

**这意味着 MO 需要在软件层面"模拟"某种形式的数据亲和，才能从根本上解决 Cache 冗余问题。**

### 6.3 对 MO 的方案启示

综合云原生 TP 数据库的经验，MO 的 Cache 优化应该分两个方向：

**方向一：TP 优化 — 引入"软亲和"（借鉴 Shared-Nothing 思路）**

不需要像 CockroachDB 那样做硬分片，但可以在路由层引入"软亲和"：
- Proxy 层：基于 username / session 特征做 Consistent Hash，同一用户的请求倾向于路由到同一 CN
- 计算层：借鉴 Databricks 的 Soft Affinity，在 `generateNodes()` 中优先将 Scan Task 分配给缓存了目标文件的 CN
- 效果：减少 TP 的 Cache Miss，提高本地 memCache 命中率

**方向二：AP 优化 — 启用 Remote Cache（借鉴 ClickHouse Cloud 思路）**

AP 工作负载对 RPC 延迟不敏感，Remote Cache 是有效的优化：
- 修复现有 Remote Cache 的 Bug
- 调整缓存层级（Remote Cache 在 Disk Cache 之前）
- 优化 Gossip 可扩展性

**方向三：长期 — 扩大本地 Cache 容量（借鉴 Neon LFC 思路）**

如果本地 Cache（memCache + diskCache）足够大，能覆盖工作集，那么 Cache Miss 本身就会减少：
- 评估当前 diskCache 的大小配置是否合理
- 考虑增大 diskCache 容量（NVMe SSD 成本远低于内存）
- 结合路由亲和，每个 CN 的有效 Cache 容量 = Total（而非 Total/N）

## 7 结论

| 工作负载 | 核心问题 | 正确策略 | 对应方案 |
|---------|---------|---------|---------|
| TP | Cache Miss 延迟敏感，RPC 开销不可忽略 | 减少 Miss（路由亲和） | Proxy 亲和 + 计算层 Soft Affinity |
| AP | 数据量大，S3 延迟是瓶颈 | 降低 Miss 代价（Remote Cache） | 修复并启用 Remote Cache |
| 通用 | Cache 容量不足 | 扩大有效容量 | 路由亲和消除冗余 + 增大 diskCache |

**Remote Cache 不是银弹。它是 AP 优化的有效手段，但对 TP 的价值有限。MO 作为 HTAP 系统，需要 TP 和 AP 两条优化路径并行推进。**
