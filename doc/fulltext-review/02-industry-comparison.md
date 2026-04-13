# MySQL / PostgreSQL / Elasticsearch-Lucene 对照

## 1. 先回答一个直接问题：全文索引最流行的是不是 ES？

**如果问题指的是“专用全文检索 / 搜索引擎领域最主流的工业方案”，答案基本是：是，Elasticsearch/Lucene 家族是最主流的一类。**

但这句话需要补充两个限定：

1. **ES 的定位是专用搜索系统**，不只是数据库里的一个 index type。
2. **数据库内置全文能力同样非常常见**，MySQL InnoDB FTS、PostgreSQL `tsvector + GIN` 都是成熟路径，只是目标边界和 ES 不一样。

所以对 MO 来说，更合理的问题不是：

- “要不要直接照抄 ES？”

而是：

- **“MO 作为数据库内建全文能力，应该借鉴 ES/Lucene 的哪些核心结构，同时保留数据库内一体化体验？”**

---

## 2. 三类成熟路线

### 2.1 MySQL InnoDB FTS

MySQL 代表的是：

- **数据库内建全文索引**
- 但已经不是简单 hidden table，而是带有专门辅助表、DOC_ID、删除表、缓存与并行 build 的 FTS 子系统

从官方文档可以提炼出这些关键点：

- inverted index 设计
- 为每个 fulltext index 创建辅助 index tables
- 额外维护删除表和 config 表
- 通过 cache 批量 flush，避免每次写都直接打到主倒排结构
- 支持 CJK ngram parser

这说明即使是“数据库内 FTS”，成熟实现也不会停留在“一张 token 表 + 查询时拼 SQL”的层面。

### 2.2 PostgreSQL text search

PostgreSQL 代表的是：

- **数据库内搜索表达能力很强**
- 但核心对象是 **`tsvector` / `tsquery`**
- 依赖 parser、dictionary、stopword、stemming、phrase operator、GIN/GiST

它的价值在于提醒 MO：

- analyzer 不是一个 parser name 就够了
- 文本需要先变成标准化的 document representation
- query 也需要做分析与规范化
- 倒排索引应当建立在“预处理后的文档表示”之上，而不是查询时临时拼很多 SQL

### 2.3 Elasticsearch / Lucene

ES/Lucene 代表的是：

- **面向搜索而设计的专用全文内核**
- 核心结构是 **segment-based inverted index**
- 支持 near real-time refresh
- 通过后台 merge 维持长期读写平衡
- 默认 BM25
- 有完善 analyzer 链路、phrase/slop、filter/query 分层、explain/highlight 等能力

ES 真正值得 MO 学的，不是它的 REST DSL，而是这些底层机制：

- segment
- refresh
- merge
- postings + positions
- analyzer pipeline
- top-k / ranking 作为一等公民

### 2.4 ClickHouse text index / MergeTree

ClickHouse 代表的是另一条很值得 MO 参考的路：

- **全文/文本索引尽量挂到存储 part 的生命周期上**
- insert 先形成 immutable part
- 后台 merge 统一整理
- 更偏向**批量写、分析型、低更新** workload 的稳定运行

它的优点不在于“比 Lucene 更先进”，而在于：

- 不需要外部独立搜索集群
- 索引、merge、恢复都跟着数据库自己的存储对象走
- 对日更、批量导入、分析查询这类场景很稳

但它也不等于完整搜索引擎：

- analyzer 生态通常不如 Lucene 丰富
- relevance / phrase / slop / search UX 不一定做到专用搜索引擎的成熟度

所以对 MO 来说，更合理的学习方式不是“二选一”，而是：

- **检索内核借 Lucene/ES**
- **存储生命周期借 ClickHouse / MergeTree**
- **系统集成层保持数据库原生**

---

## 3. 横向对比

| 维度 | MatrixOne 当前实现 | MySQL InnoDB FTS | PostgreSQL | Elasticsearch / Lucene |
| --- | --- | --- | --- | --- |
| 索引表示 | 隐藏 token 表：`doc_id,pos,word` | 多张辅助倒排表 + DOC_ID + delete/config/cache | `tsvector` + GIN/GiST | segment-based inverted index |
| 建索引 | 同步回填，重新扫表并切词 | 支持专门 FTS index build 机制 | 对 `tsvector` 建索引 | 批量建段，后台 merge |
| 增量写入 | 同步 delete+retokenize+insert | 有 cache / flush / delete handling | 更新 `tsvector` 后更新索引 | 写 buffer -> refresh -> searchable segment |
| 查询执行 | SQL 召回 + Go map 聚合 + 打分 | FTS 子系统执行 | `@@` + GIN/GiST 加速 | posting 级执行与 top-k 优化 |
| 文本分析 | simple tokenizer，能力弱 | 有内建 parser / ngram parser | parser + dictionary + stemming + stopword | analyzer chain，可高度定制 |
| 相关性 | TF-IDF 默认，BM25 可选 | 成熟 FTS relevance | `ts_rank` 等 | BM25 默认，可配置 similarity |
| phrase/proximity | 候选过滤有位置，但 ranking 简化 | 完整支持 | `<->` 等短语语义 | phrase/slop 非常成熟 |
| 删除处理 | 按 `doc_id` 直接删 token 行 | 专门 delete/deleted_cache 表 | 交给 index 机制 | tombstone + segment merge |
| 在线性 | 弱 | 中等到强 | 强 | 很强 |
| 适合场景 | 小规模能力验证 | 数据库内置全文 | 数据库内中高能力文本检索 | 大规模专用搜索 |

---

## 4. 从 ES/Lucene 应该借什么

## 4.1 必须借的

### A. 段式倒排（segment-based inverted index）

不要再把全文索引建模成“逐 token 一行”的普通表。  
应当升级为：

- term dictionary
- postings list
- positions / offsets
- per-segment stats

### B. refresh / near real-time search

ES 的关键体验之一是：

- 写入不需要等一次完整 commit / merge
- refresh 后就能查到

对 MO 来说，应该把全文索引写入拆成：

- 事务提交成功
- delta segment 可见
- 后台 merge 整理

### C. 后台 merge

没有 merge，就没有长期稳定性能。  
MO 当前最大的问题之一就是所有 token 都散在逻辑 token 表里，没有 segment 生命周期。

### D. analyzer pipeline

ES 的 analyzer 体系说明了全文检索需要明确区分：

- char filter
- tokenizer
- token filter

这对 MO 至关重要，因为当前 parser 只是一个很薄的字符串枚举，不是真正 analyzer 能力。

### E. BM25 默认化

ES 默认 BM25，不是偶然，而是长期工业实践选择。  
MO 现在虽然支持 BM25，但默认仍是 TF-IDF，而且 BM25 统计还是查询时动态拿，离成熟实现差很远。

## 4.2 不建议直接照搬的

### A. 不建议把 MO 的答案直接变成“外置 ES”

原因不是 ES 不好，而是这会改变产品定位：

- 一体化 SQL 体验会丢
- 事务一致性更难
- 运维复杂度陡增
- 用户要同时维护数据库和搜索集群

### B. 不建议一开始就追求 ES 全特性

MO 的第一目标应该是：

- 做出**数据库内可生产使用的全文索引**

而不是：

- 一次性做到搜索引擎平台全部能力

---

## 5. 从 MySQL / PostgreSQL 应该借什么

## 5.1 从 MySQL 借

### A. 数据库内 FTS 的系统表设计

MySQL 证明了数据库内 FTS 不是不能做，而是要把它当成独立子系统做：

- DOC_ID
- auxiliary index tables
- deletion tables
- config tables
- cache / flush

### B. 在线写入优化

MySQL 不会每次写一个 token 就直接粗暴打到底层结构，而是有专门 cache / batch flush 机制。

这对 MO 很重要，因为 MO 当前写放大很明显。

## 5.2 从 PostgreSQL 借

### A. “文档表示”和“查询表示”分离

PostgreSQL 的 `tsvector` / `tsquery` 思路非常值得借鉴：

- 文档先标准化
- query 也先标准化
- 再做索引与匹配

### B. dictionary / stemming / stopword

这类能力对于中文/英文混合文本、多语言场景非常关键。  
MO 当前 analyzer 能力太薄，必须补这层。

### C. phrase operator 和语义清晰性

PostgreSQL 的文本检索语义是清楚的：

- 普通匹配
- AND/OR/NOT
- phrase/followed-by

而 MO 当前最大的问题之一，是 `natural language mode` 和实际执行语义不对齐。

---

## 6. 对 MO 的直接启示

### 6.1 目标不应该是“做一个 SQL 版 ES”

而应该是：

- **做一个数据库原生、可事务集成、可 explain、可在线维护、可扩展 analyzer 的全文索引子系统**

### 6.2 目标也不应该停留在“当前实现加强一点”

只在现有 token 表方案上小修小补，最多是缓解症状，不能解决根因。

根因是：

- 索引结构不对
- 执行模型不对
- 统计方式不对
- analyzer 能力不对

### 6.3 最合理的路线

对 MO 来说，最合理的是一条混合路线：

1. **检索内核借 Lucene/ES**
   - segment
   - refresh
   - merge
   - postings/positions
   - BM25
   - analyzer pipeline

2. **存储生命周期借 ClickHouse / MergeTree**
   - part-local / object-local 辅助索引
   - immutable data part + background merge
   - append new version + async compaction

3. **系统集成层借 MySQL / PostgreSQL**
   - SQL 原生语法
   - 数据库内元数据与权限体系
   - explain / optimizer / 事务联动
   - 在线 build / rebuild / inspect

---

## 7. 一个明确结论

**ES/Lucene 必须参考，而且应该作为 MO 下一代全文内核设计的最重要参考对象。**

但最佳方案不是：

- “MO 放弃内建全文，直接建议用户外接 ES”

而是：

- **MO 内部做一个检索内核借鉴 Lucene/ES、存储生命周期借鉴 ClickHouse 的 FTS v2**

这样既能保住数据库内置全文的一体化体验，也能真正跨过当前实现的大规模瓶颈。
