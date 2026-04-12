# 关键指标与结论

## 1 预期效果指标

| 指标 | 当前（预估） | 路径 A 落地后（AP 优化） | 路径 B 落地后（TP 优化） | A+B 全部落地后 |
|------|-------------|----------------------|----------------------|-----------|
| AP Cache Miss 延迟 | ~100ms（回 S3） | ~1ms（Remote Cache 兜底） | ~100ms（不变） | ~1ms |
| TP Cache Miss 延迟 | ~100ms（回 S3） | ~1ms（Remote Cache，但 RPC 开销对 TP 不理想） | 大幅降低（本地 memCache 命中率提升） | 最优 |
| TP 本地 memCache 命中率 | 30~50%（冗余严重） | 30~50%（不变） | 60~80%（亲和路由） | 60~80% |
| AP Cache Miss 代价 | ~100ms | ~1ms | ~100ms（不变） | ~1ms |
| 等效 Cache 容量 | Total/N | ~Total（CN 间 Cache 互通） | ~Total（亲和消除冗余） | Total + 互通 |
| 对象存储访问量 | 高 | 降低 60~80%（AP 场景） | 降低 40~60%（TP 场景） | 降低 90%+ |
| 扩缩容期间延迟抖动 | 严重 | 可控（AP） | 可控（TP） | 无感知 |

## 2 核心结论

### TP 和 AP 需要不同的优化策略

经过深入分析（详见 [09-tp-ap-analysis.md](09-tp-ap-analysis.md)），之前"路径 A 优先、路径 B 辅助"的结论需要修订：

**Remote Cache（路径 A）是 AP 优化的有效手段，但对 TP 的价值有限。**

原因：
- AP 工作负载读取数据量大（MB~GB），RPC 开销（~1ms）相对于 S3 延迟（~100ms）可忽略
- TP 工作负载读取数据量小（KB），RPC 开销（~0.5-1ms）相对于本地 memCache（~0.01ms）不可忽略
- TP 的正确策略是"不要 Miss"（路由亲和提高本地命中率），而非"Miss 后补救"（Remote Cache）
- 业界云原生 TP 数据库（CockroachDB、TiDB、Aurora）都通过数据分片或单写控制实现天然 Cache 亲和，而非 Remote Cache

### 修订后的方案定位

| 路径 | 目标工作负载 | 核心价值 | 优先级 |
|------|-------------|---------|--------|
| 路径 A：Remote Cache 修复 | AP | 将 AP Cache Miss 延迟从 ~100ms 降到 ~1ms | P0（Phase 1 Bug 修复） |
| 路径 B：Proxy 路由亲和 | TP | 提高 TP 本地 memCache 命中率，从 30-50% 到 60-80% | P0（与 Phase 1 并行） |
| 路径 A Phase 2：缓存层级调整 | AP | 优化 Remote Cache 在读取路径中的位置 | P1 |
| 路径 A Phase 3：Gossip 可扩展性 | AP 大规模 | 支持 >10 CN 的大规模 AP 场景 | P2 |
| 长期：增大 diskCache | TP + AP | 借鉴 Neon LFC，本地 NVMe Cache 覆盖工作集 | P2 |

### 关键变化

1. **路径 B（Proxy 亲和）从 P2 提升到 P0**，与路径 A Phase 1 并行推进
2. **路径 A 的定位从"核心路径"调整为"AP 优化路径"**
3. **新增长期方向**：借鉴 Neon 的 Local File Cache 思路，增大本地 diskCache 容量

## 3 推荐落地顺序

### 第一阶段（并行，2 周）

| 任务 | 目标 | 成功率 | 工作负载 |
|------|------|--------|---------|
| 路径 A Phase 1：修复 4 个 Bug | 启用 Remote Cache 基本功能 | 95% | AP |
| 路径 B：Proxy username 亲和 | 同一用户 Session 路由到同一 CN | 85% | TP |

路径 A Phase 1 的 Bug 修复（缺陷 2/3/4/8）是低风险改动，可以快速完成。

路径 B 的 Proxy 亲和需要注意：
- 通过 Plugin 机制实现，不直接修改 `selectOne()` 核心逻辑
- 亲和是"软"的 — 首选 CN 负载过高时退化为 Least-Connection
- 需要处理与 Rebalancer 的交互

### 第二阶段（1 周）

| 任务 | 目标 | 成功率 |
|------|------|--------|
| 路径 A Phase 2：缓存层级调整 | Remote Cache 提前到 Disk Cache 之前 | 90% |
| RPC 超时可配置 | 默认 500ms，AP 场景可调大 | 95% |
| 增加监控 metric | Gossip 队列、Remote Cache 命中率 | 95% |

### 第三阶段（2-4 周，按需）

| 任务 | 目标 | 成功率 |
|------|------|--------|
| 路径 A Phase 3：Gossip 可扩展性 | 文件级粒度路由，支持大规模场景 | 75% |
| 评估 diskCache 容量扩展 | 借鉴 Neon LFC，评估 NVMe 缓存方案 | N/A |

### 关键决策点

1. 第一阶段完成后，分别在 TP 和 AP 场景下验证效果
2. 如果 TP 场景下 Proxy 亲和效果显著（memCache 命中率 >60%），可以降低 Remote Cache 后续优化的优先级
3. 如果 AP 场景下 Remote Cache 效果显著（Miss 延迟 <5ms），Phase 3 可以延后
4. 每个改动独立可回滚：路径 A 关闭 `remote-cache-enabled`，路径 B 关闭 Plugin

## 4 数据安全保障

所有方案都满足用户的核心目标："数据不要出问题，其他可以接受"。

- 路径 A（Remote Cache）：Cache 层改动，最坏情况是 Cache Miss 回退到 S3，查询变慢但数据正确
- 路径 B（Proxy 亲和）：路由层改动，最坏情况是亲和失效退化为 Least-Connection，不影响数据
- 两条路径都不涉及数据写入路径，不会导致数据损坏或丢失

## 5 文档索引

完整分析详见各子文档：
- [01-background.md](01-background.md) — 问题背景
- [02-solution-design.md](02-solution-design.md) — 六个方案设计
- [03-scaling.md](03-scaling.md) — 扩缩容分析
- [04-remote-cache-analysis.md](04-remote-cache-analysis.md) — Remote Cache 深度分析（8 个缺陷）
- [05-revised-plan.md](05-revised-plan.md) — 修订方案（路径 A + 路径 B）
- [06-industry-comparison.md](06-industry-comparison.md) — 业界对比（AP + TP 数据库）
- [07-code-feasibility.md](07-code-feasibility.md) — 代码可行性（15 个缺陷 + 开发风险）
- [09-tp-ap-analysis.md](09-tp-ap-analysis.md) — TP vs AP 工作负载分析（核心修订）
