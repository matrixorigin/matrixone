# 当前状态评估与修订方案

## 1 核心矛盾

经过对 MO 代码的深入分析，当前面临的核心矛盾是：

**Proxy 路由和 Cache 是两个完全独立的系统，互相不感知。**

- Proxy 按 Least-Connection 分发 Session，不关心 Cache
- 每个 CN 独立维护 Cache，不关心请求从哪来
- Remote Cache（唯一的 CN 间缓存协调机制）从未在生产环境启用
- 结果：N 个 CN 各自缓存相同数据，等效容量 = Total/N

## 2 Proxy 层现状

| 特征 | 现状 | 影响 |
|------|------|------|
| 路由粒度 | Session 级（MySQL 连接） | 无法做 Query 级或表级路由 |
| 选择策略 | Least-Connection（`selectOne`） | 连接均匀分布，Cache 无亲和 |
| SQL 解析 | 无 | 无法提取表名做 Hash |
| Label 路由 | 按 tenant + label 筛选候选 CN | 同 tenant 的所有 Session 在候选 CN 间轮转 |
| Rebalancer | 定期检查，超过 avg*(1+0.3) 时迁移 | 会主动打破任何亲和性 |
| Scaling | 检测 Draining CN，迁移 tunnel | 缩容时已有优雅机制 |

**关键发现：`LabelHash` 是 tenant + labels 的 hash，同一个 tenant 的所有用户共享同一个 LabelHash。这意味着如果在 `selectOne` 中用 LabelHash 做 Consistent Hash，效果是同一个 tenant 的所有 Session 都路由到同一个 CN — 这在单 tenant 多 CN 场景下会导致严重的负载不均。**

## 3 Cache 层现状

| 层级 | 实现 | 状态 | 问题 |
|------|------|------|------|
| Memory Cache | FIFO 双队列（类 2Q） | 正常运行 | 每个 CN 独立，冗余严重 |
| Disk Cache | FIFO + 异步加载 | 正常运行 | 高频场景延迟不可接受 |
| Remote Cache | Gossip + RPC | **未启用** | 8 个设计缺陷（见第 6 章） |

## 4 之前推荐方案的问题

之前推荐的「Proxy Session 级 Consistent Hash」方案存在一个关键问题：

**LabelHash 粒度太粗。** 同一个 tenant 下所有用户的 LabelHash 相同，用它做 Consistent Hash 等于把整个 tenant 的流量绑定到一个 CN，这不是亲和性，是单点。

要做有效的 Session 亲和，需要更细粒度的 Hash Key，比如 `tenant + username` 或 `tenant + session_id`。但这又带来新问题：不同用户可能访问相同的表，用户级亲和不等于表级亲和。

**结论：在 Proxy 不解析 SQL 的前提下，Session 级亲和对 Cache 命中率的提升有限。真正的杠杆在 Cache 层本身。**

---

# 修订后的实现方案

## 1 方案重新定位

基于上述评估，将方案分为两条独立的优化路径，可以并行推进：

**路径 A：修复并启用 Remote Cache（Cache 层优化，核心路径）**
**路径 B：Proxy 轻量亲和（路由层优化，辅助路径）**

## 2 路径 A：修复并启用 Remote Cache（优先级最高）

Remote Cache 是 MO 已有的、最接近可用的 CN 间缓存协调机制。修复缺陷后启用，可以直接将所有 CN 的 Memory Cache 变成一个逻辑上的分布式缓存池。

**即使没有路由亲和，Remote Cache 也能显著降低 Cache Miss 的代价：本地 Miss 后从其他 CN 的内存获取（~ms），而非回对象存储（~100ms）。**

#### Phase 1：Bug 修复（1 周）

| Bug | 文件 | 修复方案 |
|-----|------|----------|
| broadcastData 零值 | `pkg/gossip/delegate.go` | `make([]gossip.GossipData, 0, len(items))` |
| statsInfoState 错误锁 | `pkg/gossip/delegate.go` | 改为 `d.statsInfoKey.mu.Lock()` |
| NotifyLeave 不完整 | `pkg/gossip/delegate.go` | 增加 `statsInfoKey` 清理 |
| ReadCache 不查 diskCache | `pkg/fileservice/s3_fs.go` | `ReadCache()` 增加 diskCache 查询 |

#### Phase 2：缓存层级调整 + 启用（1 周）

1. `s3_fs.go` 的 `Read()` 方法中，将 `remoteCache` 读取提前到 `diskCache` 之前：

```
当前：memCache → diskCache → remoteCache → S3
改为：memCache → remoteCache → diskCache → S3
```

2. 配置文件中启用 `remote-cache-enabled = true`

3. 广播队列满时增加 warn 日志和 metric counter

#### Phase 3：Gossip 可扩展性优化（2~4 周）

这是 Remote Cache 能否在大规模场景下工作的关键。两个可选方案：

**方案 A：文件级粒度路由（推荐，改动小）**

将 Gossip 广播的粒度从 `(Path, Offset, Size)` 改为 `Path`（文件级）：

```go
// 当前：每个 block 一个 key
CacheKey{Path: "xxx/yyy.blk", Offset: 0, Size: 4096}
CacheKey{Path: "xxx/yyy.blk", Offset: 4096, Size: 4096}

// 改为：每个文件一个 key
CacheKey{Path: "xxx/yyy.blk"}  // 只广播文件路径
```

- 广播量降低 1~2 个数量级
- `keyTarget` map 内存开销大幅减少
- Remote Cache 查询时，先按 Path 找到目标 CN，再发 RPC 请求具体 offset
- 代价：可能出现 false positive（文件在目标 CN 但具体 offset 不在），但 RPC 失败后回退到 diskCache/S3 即可

**方案 B：Bloom Filter 摘要（改动大，效果更好）**

每个 CN 定期（如每 5 秒）将自己 Cache 的 Bloom Filter 广播给其他 CN：

- 广播量固定（Bloom Filter 大小固定，如 1MB），与 Cache 条目数无关
- 查询时先用 Bloom Filter 判断哪个 CN 可能有数据
- 有一定 false positive rate，但可以通过调整 Bloom Filter 参数控制

## 3 路径 B：Proxy 轻量亲和（辅助路径）

在 `selectOne()` 中引入轻量的亲和性，但不是用 LabelHash（太粗），而是用更细粒度的 key。

**方案：基于 username 的 Consistent Hash**

```go
func (m *connManager) selectOne(hash LabelHash, cns []*CNServer, username string) *CNServer {
    // 1. 用 hash(tenant + username) 做 Consistent Hash，选出首选 CN
    // 2. 如果首选 CN 连接数 < avg * (1 + tolerance)，返回首选
    // 3. 否则退化为 Least-Connection
}
```

原理：同一个用户的多个 Session 通常访问相似的表集合，将同一用户的 Session 聚集到同一个 CN，可以提升该 CN 的 Cache 命中率。

**注意：这需要在 `router.Route()` 中将 `clientInfo.username` 传递到 `selectOne()`，当前 `selectOne` 只接收 `LabelHash`，需要扩展接口。**

改动范围：
- `pkg/proxy/conn_manager.go`：`selectOne()` 增加 username 参数
- `pkg/proxy/router.go`：`Route()` 传递 username

## 4 改动范围总结

| 阶段 | 文件 | 改动 | 复杂度 | 优先级 |
|------|------|------|--------|--------|
| A-Phase1 | `pkg/gossip/delegate.go` | 修复 3 个 Bug | 低 | P0 |
| A-Phase1 | `pkg/fileservice/s3_fs.go` | ReadCache 增加 diskCache | 低 | P0 |
| A-Phase2 | `pkg/fileservice/s3_fs.go` | 调整 remoteCache 优先级 | 低 | P0 |
| A-Phase2 | 配置文件 | 启用 remote-cache-enabled | 低 | P0 |
| A-Phase2 | `pkg/gossip/base_item.go` | 队列满时增加日志 | 低 | P1 |
| A-Phase3 | `pkg/gossip/delegate.go` | 文件级粒度路由 | 中 | P1 |
| A-Phase3 | `pkg/fileservice/cache.go` | 调整 CacheKey 广播粒度 | 中 | P1 |
| A-Phase3 | `pkg/fileservice/remote_cache.go` | 适配文件级路由查询 | 中 | P1 |
| B | `pkg/proxy/conn_manager.go` | username Consistent Hash | 中 | P2 |
| B | `pkg/proxy/router.go` | 传递 username | 低 | P2 |

## 5 预期效果

| 阶段 | Memory Cache 命中率 | Cache Miss 延迟 | 改动量 |
|------|---------------------|-----------------|--------|
| 当前 | 30~50%（冗余严重） | ~100ms（回 S3） | - |
| A-Phase1+2 完成 | 30~50%（不变） | ~1ms（Remote Cache 兜底） | 小 |
| A-Phase3 完成 | 30~50%（不变） | ~1ms（可扩展） | 中 |
| B 完成 | 50~70%（用户级亲和） | ~1ms | 中 |
| A+B 全部完成 | 60~80% | ~1ms | 中 |

**关键洞察：路径 A（Remote Cache）解决的是 Miss 代价问题（100ms → 1ms），路径 B（Proxy 亲和）解决的是 Miss 频率问题。两者正交，效果叠加。**

> **⚠️ 重要修订（详见 [09-tp-ap-analysis.md](09-tp-ap-analysis.md)）：**
> 上述分析主要适用于 AP 工作负载。对于 TP 工作负载，Remote Cache 的 RPC 开销（~0.5-1ms）相对于本地 memCache（~0.01ms）不可忽略。TP 的正确策略是"不要 Miss"（路由亲和），而非"Miss 后补救"（Remote Cache）。因此路径 B 的优先级应提升到与路径 A Phase 1 并行（P0），而非之前建议的 P2。

## 6 路径 B 风险分析：Proxy 亲和方案的深层问题

通过深入分析 Proxy 代码（`conn_manager.go`、`router.go`、`rebalancer.go`、`scaling.go`、`config.go`），在 Proxy 层引入 Cache 亲和路由存在以下显著风险：

#### 风险一：负载不均风险

当前 `selectOne()` 使用 Least-Connection 策略保证连接均匀分布。引入 Consistent Hash 亲和后：

- **亲和性与负载均衡的根本冲突**：亲和性要求同一用户/表的请求固定到某个 CN，但 Least-Connection 的核心目标是均匀分布。两者天然矛盾。
- **Rebalancer 会主动打破亲和**：`rebalancer` 每 10 秒检查一次，连接数超过 `avg * (1 + 0.3)` 时会迁移 tunnel。如果亲和导致某个 CN 连接偏多，rebalancer 会把连接迁走，亲和失效。
- **LabelHash 粒度太粗**：`LabelHash` 是 `tenant + labels` 的 hash，同一个 tenant 的所有用户共享同一个 LabelHash。用它做 Consistent Hash 等于把整个 tenant 绑定到一个 CN，造成单点而非亲和。

#### 风险二：扩缩容连锁反应

- **Hash Ring 变化导致大面积重映射**：CN 扩缩容时 Hash Ring 变化，已建立的 Session 仍在旧 CN 上，新 Session 被路由到新 CN，同一用户的不同 Session 分散到不同 CN，亲和性在扩缩容窗口期完全失效。
- **与现有 Scaling 机制的交互**：`scaling.go` 中的 `doScaling()` 检测 Draining CN 并迁移 tunnel，但迁移目标是按 Least-Connection 选择的，不会考虑亲和性。缩容后的 Session 重分布会打破之前建立的亲和关系。

#### 风险三：多 Proxy 实例一致性

- **独立的 connManager**：每个 Proxy 实例有自己独立的 `connManager`，维护独立的连接计数。
- **CN 列表刷新时差**：各 Proxy 通过 `MOCluster` 获取 CN 列表，`RefreshInterval` 默认 2 秒。不同 Proxy 实例的 CN 列表可能存在短暂不一致，导致同一用户的请求在不同 Proxy 上被路由到不同 CN。
- **Hash Ring 不同步**：如果用 Consistent Hash，各 Proxy 实例的 Hash Ring 在 CN 变更时的更新时机不同，会出现短暂的路由分裂。

#### 风险四：Session 亲和 ≠ 表亲和

- **同一用户可能访问不同表**：Session 级亲和将同一用户的连接聚集到同一个 CN，但用户在不同 Session 中可能访问完全不同的表集合。
- **不同用户可能访问相同表**：多个用户可能频繁访问同一张热点表，但被亲和到不同 CN，热点表仍然在多个 CN 上冗余缓存。
- **Proxy 不解析 SQL**：Proxy 工作在 MySQL 协议层，不解析 SQL，无法提取表名。要做表级亲和需要增加 SQL 解析，这会显著增加 Proxy 延迟和复杂度。

#### 风险五：对现有行为的破坏性

- **CN 性能问题的放大效应**：如果某个 CN 出现性能问题（GC、磁盘慢、网络抖动），亲和绑定在该 CN 上的所有 Session 都会受影响。当前 Least-Connection 策略下，新 Session 会自然避开高负载 CN，具有天然的故障隔离能力。
- **连接池行为变化**：应用层通常使用连接池，连接池中的连接可能被不同业务逻辑复用。亲和性假设"同一连接访问相似数据"，但连接池打破了这个假设。

#### 风险六：改动 `selectOne()` 核心逻辑的风险

`selectOne()` 是 Proxy 路由的核心函数，所有 Session 建立都经过它。修改此函数：

- 影响面极大，任何 Bug 都会导致全量流量受影响
- 需要充分的灰度和回滚机制
- 与 rebalancer、scaling 等机制的交互需要仔细验证

#### 建议：如需 Proxy 层改动，使用 Plugin 机制

MO Proxy 已有 `PluginConfig` 机制（`pkg/proxy/config.go`），支持通过外部插件影响路由决策：

```go
// Config 中已有 Plugin 配置
Plugin *PluginConfig `toml:"plugin"`

type PluginConfig struct {
    Backend string        `toml:"backend"`
    Timeout time.Duration `toml:"timeout"`
}
```

如果未来确实需要在 Proxy 层做路由优化，建议通过 Plugin 机制实现外部路由决策，而非直接修改 `selectOne()` 核心逻辑。这样可以：

- 将路由策略与 Proxy 核心解耦
- 支持灰度发布和快速回滚
- 不影响 Proxy 的稳定性

