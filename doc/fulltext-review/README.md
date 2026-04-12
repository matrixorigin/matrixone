# MatrixOne 全文索引 / 全文检索调研

本目录是对 MatrixOne 当前全文索引实现的代码级分析，以及面向后续演进的方案设计。

## 快速结论

- **当前实现不是成熟搜索引擎内核，而是“隐藏倒排表 + SQL 改写 + table function + 查询时内存打分”的数据库内拼装方案。**
- **功能验证、小规模数据、演示型用例是能跑的；但在大数据量、频繁写入、高并发查询下，不适合作为生产级全文检索方案。**
- **`IN NATURAL LANGUAGE MODE` 的当前实现并不像 MySQL / Elasticsearch 那样做 bag-of-words 相关性检索，而是被实现成了强位置约束的 `SqlPhrase` 路径。**
- **建索引回填、增量维护、查询统计都带有明显的同步开销和额外 SQL / 内存聚合开销。**
- **如果明确要求只在 MO 内部实现，更务实的路线是：第一阶段先做纯内部的 FTS v2-lite（隐藏 delta 表 + compacted segment 对象 + refresh/merge + 专用 FTS scan executor），长期再评估是否继续下沉到完全专用的 engine-native FTS 存储。**

## 文档索引

1. [01-current-implementation.md](./01-current-implementation.md): 当前 MO 全文索引实现全链路拆解
2. [02-industry-comparison.md](./02-industry-comparison.md): MySQL / PostgreSQL / Elasticsearch-Lucene 对照
3. [03-mo-fulltext-v2-plan.md](./03-mo-fulltext-v2-plan.md): 面向 MO 的全文索引演进方案
4. [04-path-selection-and-confidence.md](./04-path-selection-and-confidence.md): 路线选择、实现难度、把握和针对大数据用户的建议

## 推荐阅读顺序

如果只想快速抓住重点，建议按下面顺序看：

1. **README**
2. **01-current-implementation.md** 的“总体结论”和“为什么大数据量体验会很差”
3. **04-path-selection-and-confidence.md** 的“推荐路线”和“我的把握”
4. **03-mo-fulltext-v2-plan.md** 的“目标架构”和“分阶段落地”
