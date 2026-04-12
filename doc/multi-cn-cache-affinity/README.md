# 多 CN 架构下 Cache 优化方案设计

## 文档导航

| 文档 | 内容 | 说明 |
|------|------|------|
| [01-background.md](01-background.md) | 问题背景、业内参考、方案总览 | 为什么要做，业界怎么做 |
| [02-solution-design.md](02-solution-design.md) | 六个方案的详细设计 | 每个方案的架构、优缺点 |
| [03-scaling.md](03-scaling.md) | 动态扩缩容场景分析 | 扩缩容对 Cache 亲和的影响及应对 |
| [04-remote-cache-analysis.md](04-remote-cache-analysis.md) | Remote Cache 深度分析 | 现有代码的架构、状态、8 个设计缺陷 |
| [05-revised-plan.md](05-revised-plan.md) | 当前状态评估 + 修订后的实现方案 | 路径 A（Remote Cache）+ 路径 B（Proxy 亲和） |
| [06-industry-comparison.md](06-industry-comparison.md) | 业界方案对比（AP + TP 数据库） | Snowflake/Databricks/ClickHouse + Aurora/CockroachDB/Neon/PolarDB-MP |
| [07-code-feasibility.md](07-code-feasibility.md) | 代码可行性深度分析 | 15 个缺陷清单、开发风险、代码改动详情 |
| [08-conclusion.md](08-conclusion.md) | 关键指标 + 结论与建议 | 修订后的落地顺序、决策点 |
| [09-tp-ap-analysis.md](09-tp-ap-analysis.md) | TP vs AP 工作负载分析 | Remote Cache 对 TP 的局限性、云原生 TP 数据库方案 |

## 核心结论（修订版）

**Remote Cache 不是银弹。** 它是 AP 优化的有效手段，但对 TP 的价值有限。MO 作为 HTAP 系统，需要 TP 和 AP 两条优化路径并行推进。

| 工作负载 | 核心问题 | 正确策略 | 对应方案 |
|---------|---------|---------|---------|
| TP | Cache Miss 延迟敏感，RPC 开销不可忽略 | 减少 Miss（路由亲和） | 路径 B：Proxy 亲和（P0） |
| AP | 数据量大，S3 延迟是瓶颈 | 降低 Miss 代价（Remote Cache） | 路径 A：修复 Remote Cache（P0） |

落地顺序：
1. 第一阶段（并行，2 周）：路径 A Phase 1 Bug 修复 + 路径 B Proxy 亲和
2. 第二阶段（1 周）：路径 A Phase 2 缓存层级调整 + 监控
3. 第三阶段（2-4 周，按需）：Gossip 可扩展性 + diskCache 容量评估
