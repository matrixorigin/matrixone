# 主流全文引擎实现原理对比与 MO native FTS 定位分析

## 1. 主流全文引擎实现原理

### 1.1 Lucene / Elasticsearch — 专用搜索引擎

Lucene 是全文检索领域的工业标准。核心架构：

- **immutable segment**：文档写入后形成不可变的 segment，每个 segment 是一个独立的倒排索引
- **segment 内部结构**：term dictionary（FST 压缩）→ postings list（delta + varint + PFOR 压缩）→ positions / offsets → doc values / norms
- **near real-time refresh**：新文档先写入内存 buffer，定期 refresh 成新 segment 后可搜索
- **background merge**：后台合并小 segment 为大 segment，控制 segment 数量（通常 < 几十个）
- **delete 处理**：删除只标记 `.del` bitmap，merge 时物理清理
- **BM25 默认**：segment 级统计持久化，查询时汇总

**关键特点**：segment 是独立文件，有完整的 term dictionary + postings + positions + norms + stored fields。查询时按需读取，不需要全量加载。

### 1.2 Tantivy — Rust 版 Lucene

Tantivy 是 Lucene 的 Rust 重新实现，被 ParadeDB 和 Quickwit 使用。架构几乎相同：

- immutable segment + background merge
- term dictionary（FST）+ postings（bitpacking / PFOR）
- positions / offsets 一等公民
- BM25 默认

**和 Lucene 的区别**：更轻量，没有 Java GC 问题，适合嵌入式场景。ParadeDB 把 Tantivy 嵌入 PostgreSQL 作为扩展，实现了数据库内的搜索引擎级全文能力。

### 1.3 ClickHouse — 数据库内 per-part 倒排索引

ClickHouse 的全文索引（2026 年 GA）是最值得 MO 参考的方案：

- **per-part 倒排索引**：每个 MergeTree data part 内部维护一个 text index，跟随 part 生命周期
- **三文件结构**：`.dct`（dictionary blocks，sorted terms + front coding 压缩）+ `.idx`（sparse index header）+ `.pst`（postings lists，roaring bitmap / bitpacking）
- **merge 时合并而非重建**：part merge 时，text index 也做 merge（合并 sorted dictionary + 重映射 row numbers），不需要重新切词
- **direct read 优化**：查询可以直接从 text index 读取结果，跳过底层列数据扫描（45x 加速）
- **三级缓存**：header cache + tokens cache + postings cache
- **preprocessor**：支持 lower/caseFold/normalize 等预处理链

**关键特点**：text index 是 data part 的附属结构，和 part 同生同灭。merge 时做 index merge 而非重建。有 dictionary block + sparse index 支持按需加载。

### 1.4 PostgreSQL — tsvector + GIN

PostgreSQL 的全文检索走的是另一条路：

- **tsvector**：文本先经过 parser → dictionary → stemming → stopword 处理，变成标准化的 `tsvector` 表示
- **GIN index**：在 tsvector 列上建 GIN（Generalized Inverted Index），存储 lexeme → 压缩 posting list
- **tsquery**：查询也做分析和规范化，支持 AND/OR/NOT/phrase/prefix
- **ts_rank / ts_rank_cd**：基于 TF 的排序

**关键特点**：analyzer 是一等公民（parser + dictionary + stopword + stemming），GIN 是通用倒排索引框架（不只用于全文）。但没有 BM25，没有 segment merge，GIN 更新有 pending list 机制。

### 1.5 MySQL InnoDB FTS — 数据库内辅助表

MySQL 的全文索引是数据库内实现的经典案例：

- **辅助 index tables**：每个 FULLTEXT index 创建 6 张辅助表（按 term 首字母分片）
- **FTS cache**：内存中的倒排缓存，批量 flush 到辅助表，避免每次写都打到磁盘
- **DOC_ID**：内部文档 ID 系统
- **delete 表**：单独维护删除记录
- **OPTIMIZE TABLE**：手动触发 merge / 清理

**关键特点**：比 MO v1 成熟很多（有 cache、有 delete 表、有辅助表分片），但本质仍然是"隐藏表方案"。没有 segment merge，OPTIMIZE TABLE 是全量重建。

### 1.6 Quickwit — 对象存储上的分布式搜索

Quickwit 是最新一代的搜索引擎，专为对象存储（S3）设计：

- **split = immutable segment on S3**：每个 split 是一个 Tantivy segment + hotcache 文件，存储在 S3 上
- **compute-storage 分离**：indexer 和 searcher 分离，searcher 完全无状态
- **hotcache**：每个 split 附带一个小的 hotcache 文件，让 searcher 在 60ms 内打开 S3 上的 split
- **metastore**：用 PostgreSQL 存储 split 元数据（状态、时间范围等）
- **rendezvous hashing**：searcher 之间用 rendezvous hashing 分配 split，实现高效缓存

**关键特点**：证明了"immutable segment on object storage + stateless searcher"的可行性。和 MO 的存储计算分离架构高度相似。

### 1.7 DuckDB FTS — 轻量级隐藏表

DuckDB 的全文检索是最简单的实现：

- 本质上是创建隐藏的倒排表（类似 SQLite FTS5）
- 支持 BM25 排序
- 支持 stemming / stopword
- 没有增量更新，索引需要重建

**关键特点**：功能验证级别，不适合大规模生产。和 MO v1 的定位类似。

---

## 2. MO native FTS 和主流方案的对比

| 维度 | Lucene/ES | ClickHouse | PostgreSQL GIN | MySQL InnoDB FTS | MO native FTS |
|------|-----------|------------|----------------|------------------|---------------|
| **索引粒度** | per-segment | per-part | per-table (GIN) | per-table (辅助表) | per-object |
| **索引和数据的关系** | 独立文件 | part 内附属 | 独立 GIN 结构 | 独立辅助表 | 独立 sidecar 文件 |
| **term dictionary** | FST 压缩 | sorted blocks + front coding | GIN B-tree | 辅助表分片 | Go map（无压缩） |
| **postings 压缩** | delta + varint + PFOR | roaring bitmap / bitpacking | 压缩 posting list | 无明确压缩 | 无压缩 |
| **按需加载** | ✅ 按 term 读取 | ✅ sparse index + dictionary block | ✅ B-tree 查找 | ✅ 辅助表查询 | ❌ 全量反序列化 |
| **merge 策略** | segment merge（合并 postings） | part merge（合并 index） | vacuum | OPTIMIZE TABLE（重建） | object merge（重新切词） |
| **delete 处理** | .del bitmap | roaring bitmap | HOT / vacuum | delete 表 | MO tombstone |
| **统计持久化** | ✅ per-segment | ✅ per-part | ❌ 查询时计算 | ✅ cache | ✅ per-sidecar |
| **缓存** | OS page cache + Lucene cache | header/tokens/postings 三级缓存 | shared buffers | FTS cache | 无 |
| **analyzer** | 完整 pipeline | tokenizer + preprocessor | parser + dictionary + stemming | ngram / 内置 parser | simple tokenizer |

### 2.1 MO 做得好的地方

1. **per-object sidecar 和 MO 的 immutable object 模型天然契合**——这和 ClickHouse 的 per-part index、Lucene 的 per-segment index 是同一个思路
2. **tombstone 复用 MO 原生机制**——不需要像 MySQL 那样维护独立的 delete 表
3. **降级策略干净**——这是其他方案都没有的优势（因为它们没有 fallback 路径）
4. **统计已持久化**——和 Lucene/ClickHouse 对齐

### 2.2 MO 的主要差距

1. **没有按需加载**——这是和所有成熟方案最大的差距。Lucene 有 FST + 按 term 读取，ClickHouse 有 dictionary block + sparse index，MO 是全量反序列化到内存 map
2. **postings 没有压缩**——Lucene 用 delta+varint+PFOR，ClickHouse 用 roaring bitmap/bitpacking，MO 是原始 binary.Write
3. **merge 时重新切词**——ClickHouse 的 text index merge 是合并 sorted dictionary + 重映射 row numbers，不需要重新切词。MO 是重新对原始文本切词
4. **没有缓存**——ClickHouse 有三级缓存（header/tokens/postings），MO 没有任何缓存
5. **analyzer 太简单**——只有 simple tokenizer + 3-gram，没有 stemming/stopword/synonym/preprocessor chain

---

## 3. 是否有更好的方案？

### 3.1 最值得借鉴的方案：ClickHouse text index

ClickHouse 的 text index 是当前最值得 MO 借鉴的方案，原因：

1. **架构最相似**：都是 columnar database + immutable data part/object + background merge
2. **索引和数据的关系最相似**：都是 per-part/per-object 的附属索引
3. **merge 策略最相似**：都跟随 data part/object 的 compaction

ClickHouse 比 MO 多做的关键事情：

| ClickHouse 已有 | MO 当前状态 | 建议 |
|-----------------|------------|------|
| dictionary block + sparse index（按需加载） | 全量反序列化 | **P1：segment 格式加 term index** |
| roaring bitmap / bitpacking（postings 压缩） | 无压缩 | **P1：至少做 delta + varint** |
| merge 时合并 index（不重新切词） | 重新切词 | **P2：实现 segment merge** |
| header/tokens/postings 三级缓存 | 无缓存 | **P2：利用 MO file service cache** |
| tokenizer + preprocessor chain | simple tokenizer | **P2：加 preprocessor 支持** |
| direct read（跳过列数据） | 仍走 table function 框架 | **P3：独立 FTS scan 算子** |

### 3.2 不建议照搬的方案

1. **不建议照搬 Lucene/ES 的完整 segment 架构**——太重，MO 不需要独立的 segment 管理（已经有 object 管理）
2. **不建议嵌入 Tantivy**——引入 Rust FFI 复杂度太高，和 MO 的 Go 技术栈不匹配
3. **不建议走 PostgreSQL GIN 路线**——GIN 是通用倒排框架，对全文检索不够专用
4. **不建议走 MySQL 辅助表路线**——这就是 MO v1 的思路，已经证明不适合

### 3.3 推荐的演进路径

**当前方案（per-object sidecar）方向正确，不需要换方案，需要的是沿着 ClickHouse text index 的方向做格式和能力升级。**

具体来说：

**第一步（当前 → 能跑稳）**：
- segment 格式加 term index + 按需加载
- postings 做 delta + varint 压缩
- sidecar GC
- 更细粒度的 native/v1 混合查询

**第二步（能跑稳 → 能跑快）**：
- merge 时合并 sidecar 而非重新切词
- sidecar 缓存（利用 MO file service cache）
- preprocessor chain（lower/normalize）
- per-sidecar term bloom filter

**第三步（能跑快 → 能跑好）**：
- 独立 FTS scan 算子（替代 table function）
- segment 级 early top-k
- 更丰富的 analyzer（stemming/stopword）
- hybrid search（FTS + vector fusion）

---

## 4. 结论

MO 的 native FTS 走的是和 ClickHouse text index 相同的大方向——**per-data-unit 附属倒排索引，跟随存储对象生命周期**。这个方向是对的。

当前的差距不在架构层面，而在工程成熟度：按需加载、压缩、merge 优化、缓存。这些都是可以在当前架构上逐步补齐的，不需要推翻重来。

最不应该做的事情是：因为看到 Lucene/ES 很成熟就想照搬它的完整架构，或者因为看到 Tantivy 很快就想嵌入它。MO 已经有了自己的存储对象模型和 compaction 体系，全文索引应该长在这个体系上，而不是在旁边再建一个独立的搜索引擎。

References:
[1] Elasticsearch Segment Merges - https://shivamagarwal7.medium.com/elasticsearch-navigating-lucene-segment-merges-9ed775bd45cb
[2] ClickHouse Full-text Search GA - https://clickhouse.com/blog/full-text-search-ga-release
[3] ClickHouse Text Indexes - https://clickhouse.com/docs/en/guides/developer/full-text-search
[4] Quickwit Architecture - https://quickwit.io/docs/overview/architecture
[5] ParadeDB Block Storage for FTS - https://www.paradedb.com/blog/block-storage-part-one
[6] Tantivy Architecture - https://github.com/quickwit-oss/tantivy/blob/main/ARCHITECTURE.md
[7] PostgreSQL GIN Indexes - https://postgresql.org/docs/current/textsearch-indexes.html
[8] MySQL InnoDB FTS - http://dev.mysql.com/doc/en/innodb-fulltext-index.html
