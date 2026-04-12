# 多 CN 架构下 Cache 优化方案设计

> 本文档已拆分为多个子文档，请查看 [doc/multi-cn-cache-affinity/](multi-cn-cache-affinity/README.md) 目录。

| 文档 | 内容 |
|------|------|
| [README.md](multi-cn-cache-affinity/README.md) | 总览与导航 |
| [01-background.md](multi-cn-cache-affinity/01-background.md) | 问题背景、业内参考、方案总览 |
| [02-solution-design.md](multi-cn-cache-affinity/02-solution-design.md) | 六个方案的详细设计 |
| [03-scaling.md](multi-cn-cache-affinity/03-scaling.md) | 动态扩缩容场景分析 |
| [04-remote-cache-analysis.md](multi-cn-cache-affinity/04-remote-cache-analysis.md) | Remote Cache 深度分析（8 个缺陷） |
| [05-revised-plan.md](multi-cn-cache-affinity/05-revised-plan.md) | 当前状态评估 + 修订方案（路径 A/B） |
| [06-industry-comparison.md](multi-cn-cache-affinity/06-industry-comparison.md) | 业界对比（Snowflake/Databricks/ClickHouse/PolarDB） |
| [07-code-feasibility.md](multi-cn-cache-affinity/07-code-feasibility.md) | 代码可行性（15 个缺陷 + 开发风险） |
| [08-conclusion.md](multi-cn-cache-affinity/08-conclusion.md) | 关键指标与结论 |
