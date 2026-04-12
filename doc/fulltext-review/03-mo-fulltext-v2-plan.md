# MatrixOne 全文索引演进方案（FTS v2）

## 1. 目标定位

## 1.1 目标

给 MO 规划一套**数据库原生、可规模化、可在线维护、具备主流搜索体验的全文索引能力**。

这套能力应当满足：

- 保留 SQL 原生体验：`FULLTEXT` / `MATCH ... AGAINST`
- 具备数据库内一体化能力：权限、事务、元数据、explain、回表
- 在大数据量下仍能保持可接受的 build / write / search 行为
- 逐步具备接近主流搜索系统的 analyzer / ranking / phrase / prefix 能力

## 1.2 非目标

第一阶段不追求：

- 全量复刻 Elasticsearch 全功能 DSL
- 一次性做成搜索平台
- 一开始就支持所有语言学高级能力

## 1.3 重要修正

这份文档描述的是 **MO 全文能力的目标终态**。  
如果把“近期有更大数据用户要接入”、“实现难度/交付风险”以及“必须在 MO 内部实现”一起纳入考虑，那么**不建议把“完全专用的 engine-native FTS 存储对象”作为第一落地形态**。

更务实的第一落地建议是：

1. **第一阶段**：先做 **FTS v2-lite**，即纯内部的 `delta + segment` 架构，复用 MO 现有元数据、对象存储、后台任务体系。
2. **第二阶段**：补齐 analyzer、phrase/proximity、在线 build、top-k 优化等能力。
3. **长期**：当 native FTS 的使用量、查询规模和功能诉求都被验证后，再考虑继续下沉为完全专用的 engine-native FTS 存储。

---

## 2. 设计原则

1. **索引应是引擎能力，不应再是“普通隐藏表 + 查询时聚合”的拼装逻辑**
2. **写入应支持 refresh / merge，不应要求同步重建整份 token 数据**
3. **查询应尽量在倒排层完成召回、交并、位置约束、top-k，而不是先吐大候选再在 Go map 聚合**
4. **全局统计应预计算/增量维护，而不是每次查询 `COUNT(*)`**
5. **分析器应成为一等公民**
6. **语义必须和用户直觉一致，尤其是 natural language / boolean / phrase**

---

## 3. 目标架构

## 3.1 总体结构

```text
写入路径
  base row commit
    -> analyzer
    -> mutable delta segment
    -> refresh
    -> searchable delta segment
    -> background merge
    -> compact immutable segments

查询路径
  MATCH ... AGAINST
    -> query parser + analyzer
    -> segment readers
    -> postings intersection / union / phrase / prefix
    -> BM25 scoring + early top-k
    -> merge local top-k
    -> join doc_id back to base table
```

## 3.2 核心对象

建议新增一组 FTS 元数据与存储对象：

### A. Index metadata

记录：

- index id
- base table id
- indexed fields
- analyzer config
- similarity config
- refresh policy
- build state

### B. Segment metadata

每个 segment 记录：

- segment id
- state（building/searchable/merging/deleted）
- min/max doc id
- doc count
- total token count
- avg doc length
- term count
- object/table location

### C. Segment storage

每个 segment 持有：

- term dictionary
- postings list
- positions / offsets
- doc length / norm
- segment-level df / tf stats

### D. Tombstone / delete bitmap

用于：

- DELETE
- UPDATE 的旧版本屏蔽
- merge 时物理清理

## 3.3 第一阶段推荐落地形态：内部 `delta + segment`

这部分是我当前认为**最适合作为 MO 内部第一代可生产全文方案**的具体形态。

### A. Delta 层：小而受控的隐藏增量表

最近写入先进入一个**受控规模**的隐藏 delta 层，用于：

- 刚提交数据的近实时可见
- CREATE INDEX / REBUILD 时的 catch-up
- merge 前的临时写缓冲

这一层可以继续使用 MO 易于落地的隐藏系统表表达，但必须满足：

- 有大小/时间阈值
- 不作为长期主索引形态
- 查询时只处理“最近增量”，不能无限膨胀

### B. Segment 层：压缩后的不可变倒排段对象

后台任务把 delta 数据合并成不可变 segment，对每个 segment 保存：

- term dictionary
- postings
- positions / offsets
- doc length / norm
- segment stats

这层建议优先基于 **隐藏系统对象 + segment metadata** 实现，而不是第一步就发明全新的独立存储子引擎。

### C. Delete 层：tombstone / delete bitmap

删除和更新旧版本不直接重写大段索引，而是：

- 先记录 tombstone / delete bitmap
- merge 时再做物理清理

### D. Query 层：同时查询 segment 和 delta

查询执行时：

1. 先读 segment metadata
2. 用专用 FTS scan 在 segment postings 上做召回与 top-k
3. 再合并小规模 delta 结果
4. 最后回表 JOIN 原表

这会比“全量 token 行扫描 + Go map 聚合”稳定得多，同时实现难度又显著低于完整专用存储子系统。

---

## 4. 写入路径设计

## 4.1 CREATE INDEX / ALTER ADD INDEX

当前同步回填必须改成：

1. 创建 index metadata
2. 创建 build job
3. 以某个 snapshot 启动 backfill
4. backfill 期间把新提交写入 delta log / catch-up 队列
5. 回填完成后切到 searchable
6. 后台持续 merge / optimize

这样可以避免：

- DDL 强阻塞
- 大表 build 期间不可控资源峰值

## 4.2 INSERT

INSERT 不再直接写“最终索引结构”，而是：

1. 事务提交时把 analyzer 后的 token 写入 mutable delta segment
2. refresh 后对查询可见
3. 后台 merge 进更大 immutable segment

## 4.3 UPDATE

UPDATE 不再 `delete all tokens + rebuild all tokens` 到最终结构，而是：

1. 旧 doc version 打 tombstone
2. 新版本写入新的 delta segment
3. merge 时统一清理旧版本

## 4.4 DELETE

DELETE 只追加 tombstone / delete bitmap，不立即重写大段索引。

---

## 5. 查询路径设计

## 5.1 语法保持兼容

继续支持：

- `FULLTEXT (...)`
- `CREATE FULLTEXT INDEX`
- `MATCH(...) AGAINST(...)`

但执行层改成真正的 FTS scan 节点，例如：

- `Node_FTS_SCAN`
- 或 engine-native index reader

而不是 `fulltext_index_scan` table function 拼 SQL。

## 5.2 查询语义重定义

### A. default / natural language mode

应改成主流语义：

- query analyzer
- bag-of-words 召回
- BM25 默认打分
- phrase / proximity 作为额外加分，不是默认强位置约束

### B. boolean mode

继续支持：

- MUST
- MUST NOT
- SHOULD
- prefix
- group
- weight hints

但执行方式改成 posting list 运算。

### C. phrase query

显式 phrase 必须使用 positions 做精确约束，并支持：

- exact phrase
- configurable slop
- ordered proximity

## 5.3 查询执行优化

目标执行模型：

1. analyzer 后得到 query terms
2. 从 term dictionary 找到对应 postings
3. 对 MUST term 做交集
4. 对 SHOULD term 做并集或打分补充
5. 对 phrase/proximity 使用 positions 验证
6. 做 segment-local top-k
7. merge 成 global top-k
8. 再回表 JOIN

关键目标是：

- **先在索引层把候选集压小**
- **再回表**

而不是先把大量候选拉出来再在上层聚合。

---

## 6. 打分与统计设计

## 6.1 BM25 作为默认

建议把 BM25 设为默认相关性算法。

原因：

- 工业实践更成熟
- 对短字段/长字段更稳
- 和 ES/Lucene 默认一致

## 6.2 统计必须预计算

下面这些统计不能再查询时动态扫：

- doc count
- avg doc length
- term doc freq
- term total term freq

建议维护：

- segment-level stats
- global merged stats

查询时只读 metadata，不跑额外 `COUNT(*)`。

## 6.3 字段权重

后续建议支持：

- field boost
- title/body 不同权重
- 可选的 field norm

这会明显改善多字段检索体验。

---

## 7. Analyzer 体系设计

## 7.1 把 parser 升级成 analyzer

建议从 “parser name” 升级到完整 analyzer 配置：

```text
analyzer:
  char_filters
  tokenizer
  token_filters
```

## 7.2 首批内建 analyzer

建议第一批提供：

- `simple`
- `cjk_3gram`
- `standard`
- `json_text`
- `json_value`
- `english`

后续再加：

- stopword
- stemmer
- synonym
- custom dictionary

## 7.3 索引时 analyzer 与查询时 analyzer 分离

需要支持：

- index analyzer
- search analyzer

这样才能做到更合理的召回与排序。

---

## 8. 存储结构建议

## 8.1 不建议继续用“逐 token 一行”

建议从以下两个方向中选一个：

### 方向 A：专用 FTS 存储对象（长期终态）

优点：

- 更适合 postings 压缩和 segment lifecycle
- 不受普通表扫描模型约束
- 更接近搜索引擎内核

缺点：

- 实现难度最高
- 需要更多 storage/executor/metadata 新能力
- 不适合作为“下一个大用户快要上量时”的第一交付方案

### 方向 B：隐藏系统表 + segment 对象（第一阶段最推荐）

把 segment 数据组织成系统表/对象，但列里存：

- term dictionary blob
- posting blob
- position blob
- stats blob

优点：

- 更容易复用 MO 现有对象管理、元数据、后台任务体系
- 能自然承载第一阶段推荐的 `delta + segment` 架构
- 比“完全专用 FTS 存储对象”更容易落地
- 已经能把当前的逐 token 行模型升级为 segment/postings 模型
- 更适合作为 native FTS 的第一代可生产方案

缺点：

- 终态性能天花板可能低于完全专用 FTS 存储对象
- 后续若继续深挖极致性能，仍可能需要二次下沉

## 8.2 建议的数据粒度

每个 searchable unit 建议是：

- immutable segment

不要再以单条 token row 作为核心查询粒度。

---

## 9. Optimizer / Executor 集成

## 9.1 新增 FTS 原生算子

建议新增原生计划节点，而不是 table function：

- `FTS_SCAN`
- `FTS_TOPK`
- `FTS_MERGE`

## 9.2 limit / top-k pushdown

要支持：

- segment-local top-k
- shard-local top-k
- coordinator merge top-k

这样 `LIMIT 10` 才真正有意义。

## 9.3 explain 能力

需要在 explain 中清晰展示：

- index used
- analyzer used
- query terms
- postings ops
- phrase / prefix checks
- local top-k
- merge top-k

---

## 10. 运维与可观测性

建议提供：

- `SHOW FULLTEXT INDEX STATUS`
- `SHOW FULLTEXT SEGMENTS`
- `SHOW FULLTEXT ANALYZERS`
- `ALTER FULLTEXT INDEX ... REBUILD`
- `ALTER FULLTEXT INDEX ... REFRESH`
- `ALTER FULLTEXT INDEX ... OPTIMIZE`

并暴露指标：

- refresh latency
- merge backlog
- pending tombstones
- index size
- token count
- query candidate count
- top-k prune ratio

---

## 11. 与 ES 的关系

## 11.1 结论

**要重度参考 ES/Lucene 的底层设计，但 MO 的交付路线应保持为内部原生实现，而不是依赖外部搜索系统。**

## 11.2 更合理的产品策略

建议坚持单主线：

### 主线：MO 原生 FTS v2

覆盖：

- SQL 内全文检索
- 事务一致性
- explain / optimizer 集成
- 中到大规模在线检索

而 ES/Lucene 的价值主要体现在：

- 作为底层结构和执行模型的参考
- 帮助 MO 避免重复踩搜索系统已经踩过的坑
- 为 analyzer、refresh、merge、postings、top-k 设计提供成熟样板

---

## 12. 分阶段落地建议

## Phase 0：现有实现止血

目标：在不推翻当前实现的前提下，先解决最明显的问题。

建议项：

1. 把 `natural language mode` 改成真正 bag-of-words 语义
2. 让 BM25 成为默认，且把全局统计持久化
3. 把 `default/ngram` 明确成 analyzer 配置，不再只是名字
4. 把 query expansion 标成显式 unsupported，避免语义误导
5. 加完整 explain / metrics

## Phase 1：原生 FTS Scan

目标：去掉 `table function + 动态 SQL + Go map 聚合` 主路径。

建议项：

1. 引入 engine-native FTS scan
2. 支持 postings 级布尔运算
3. 支持 positions 级 phrase 校验
4. 支持 local top-k

## Phase 2：Segment / Refresh / Merge

目标：真正解决写入与长期性能问题。

建议项：

1. mutable delta segment
2. refresh 使新写入近实时可见
3. immutable segment
4. background merge
5. delete bitmap / tombstone

## Phase 3：Analyzer / Relevance 强化

目标：把“能查”提升到“好用”。

建议项：

1. analyzer pipeline
2. stopword / stemmer / synonym
3. field boost
4. slop / proximity ranking
5. snippet / highlight

## Phase 4：外部生态桥接

目标：补齐超大规模搜索平台能力。

建议项：

1. ES 同步器
2. 双写/异步同步策略
3. 联邦查询或 fallback

---

## 13. 方案收益

如果按上述方向演进，MO 会得到：

1. **更合理的写路径**：不再每次同步重建整份 token 数据
2. **更合理的查询路径**：posting 级执行，减少大候选拉取与内存聚合
3. **更合理的语义**：natural language、boolean、phrase 与用户认知对齐
4. **更合理的相关性**：BM25 默认化，统计预计算
5. **更合理的可运维性**：refresh/merge/rebuild/status 成为正式能力

---

## 14. 最终建议

**建议把当前实现定位为 FTS v1（功能型），并尽快启动 FTS v2。**

但更具体地说：

1. **如果下一批用户很快就会上较大数据，不要把当前 v1 直接作为大规模方案承诺出去。**
2. **第一阶段优先落地纯内部的 FTS v2-lite（隐藏 delta 表 + segment 对象 + refresh/merge + 专用 scan executor）。**
3. **把“完全专用 engine-native FTS 存储”作为长期终态，而不是第一阶段交付目标。**

从架构价值看，这不是“小优化”，而是一项值得单独立项、按阶段分解的基础能力升级。
