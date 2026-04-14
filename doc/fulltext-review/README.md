# MatrixOne 全文索引 / 全文检索调研

本目录是对 MatrixOne 当前全文索引实现的代码级分析，以及面向后续演进的方案设计。

## 快速结论

- **当前实现不是成熟搜索引擎内核，而是“隐藏倒排表 + SQL 改写 + table function + 查询时内存打分”的数据库内拼装方案。**
- **功能验证、小规模数据、演示型用例是能跑的；但在大数据量、频繁写入、高并发查询下，不适合作为生产级全文检索方案。**
- **`IN NATURAL LANGUAGE MODE` 的当前实现并不像 MySQL / Elasticsearch 那样做 bag-of-words 相关性检索，而是被实现成了强位置约束的 `SqlPhrase` 路径。**
- **建索引回填、增量维护、查询统计都带有明显的同步开销和额外 SQL / 内存聚合开销。**
- **如果明确要求只在 MO 内部实现，不依赖外部引擎或外挂搜索模块，更务实的路线是：放弃继续扩展 hidden-table bridge，实现 storage-coupled native FTS。**

## 文档索引

1. [01-current-implementation.md](./01-current-implementation.md): 当前 MO 全文索引实现全链路拆解
2. [02-industry-comparison.md](./02-industry-comparison.md): MySQL / PostgreSQL / Elasticsearch-Lucene 对照
3. [03-mo-fulltext-v2-plan.md](./03-mo-fulltext-v2-plan.md): 面向 MO 的全文索引演进方案
4. [04-path-selection-and-confidence.md](./04-path-selection-and-confidence.md): 路线选择、实现难度、把握和针对大数据用户的建议
5. [05-current-vs-v2lite-vs-native.md](./05-current-vs-v2lite-vs-native.md): 当前实现、hidden-table FTS v2-lite、storage-coupled native FTS 的原理与优劣对照
6. [06-native-core-status-and-test-plan.md](./06-native-core-status-and-test-plan.md): 当前 native 代码已落地范围、未完成边界与测试方案
7. [07-code-review-and-improvement-proposals.md](./07-code-review-and-improvement-proposals.md): 第一轮代码级 Review 与改进建议
8. [08-second-review-and-architecture-fit.md](./08-second-review-and-architecture-fit.md): 第二/三轮 Review、架构适配性分析、风险评估
9. [09-fulltext-summary.md](./09-fulltext-summary.md): **总结文档 — 旧版本问题、新版本原理与优势、已知限制、风险评估**
10. [10-industry-engines-vs-mo-native-fts.md](./10-industry-engines-vs-mo-native-fts.md): **主流全文引擎实现原理对比与 MO native FTS 定位分析**
11. [11-test-plan-and-cases.md](./11-test-plan-and-cases.md): **Native FTS 测试方案、nightly FULLTEXT job 分析、单机脚本与 CI 集成建议**
12. [12-benchmark-1m-report.md](./12-benchmark-1m-report.md): **1M 行 Benchmark 测试报告（含发现的 Boolean mode BUG）**
13. [13-current-issues-analysis.md](./13-current-issues-analysis.md): 当前问题分析（UPDATE 残留根因、tail segment、混合查询）
14. [14-native-only-switch-plan.md](./14-native-only-switch-plan.md): native-only 开关方案设计
15. [15-native-only-change-record.md](./15-native-only-change-record.md): native-only 修改记录（4 文件 +25/-7 行）
16. [16-native-only-test-report.md](./16-native-only-test-report.md): native-only 2M 行测试报告
17. [17-production-roadmap.md](./17-production-roadmap.md): **商用路线图 — 三档优先级、时间线、已完成清单**

## 推荐阅读顺序

如果只想快速抓住重点，建议按下面顺序看：

1. **README**
2. **05-current-vs-v2lite-vs-native.md** 的“一页结论”和“三方案总对比”
3. **01-current-implementation.md** 的“总体结论”和“为什么大数据量体验会很差”
4. **04-path-selection-and-confidence.md** 的“推荐路线”和“我的把握”
5. **03-mo-fulltext-v2-plan.md** 的“目标架构”和“分阶段落地”
