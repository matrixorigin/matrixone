# MatrixOne 全文三方案对照：当前实现 vs hidden-table FTS v2-lite vs storage-coupled native FTS

## 1. 文档目标

这份文档专门回答一个更聚焦的问题：

**如果明确要求“只在 MO 内部实现全文能力，不接外部搜索引擎，也不依赖外挂搜索模块”，那当前三条路径分别是怎么工作的、各自有什么短板、为什么更推荐 storage-coupled native FTS。**

这里对比的三条路径是：

1. **当前 MO 全文实现（v1）**
2. **当前 hidden-table FTS v2-lite**
3. **推荐方向：storage-coupled native FTS**

这不是在讨论外接 ES、OpenSearch、ClickHouse 或其它独立搜索服务。  
这里的目标始终是：**在 MatrixOne 内部把全文能力做成数据库原生能力。**

---

## 2. 一页结论

### 2.1 最短结论

1. **当前 MO v1** 能证明功能链路可用，但结构太“表化”，更像“隐藏 token 表 + SQL 改写 + Go 聚合”，不适合作为大数据和日更场景的长期方案。
2. **hidden-table FTS v2-lite** 比 v1 更接近倒排索引，但它仍然依赖隐藏表和 post-DML SQL 维护；它只适合作为历史桥接思路，不适合作为当前继续推进的主线。
3. **storage-coupled native FTS** 更适合你的目标，因为它把全文索引的 segment、delete bitmap、merge 生命周期直接挂到 MO 自己的 object / part / compaction 流程上，系统行为更稳、维护更自然，也更适合每天固定时间更新一批数据。

### 2.2 为什么最终不推荐继续押 hidden-table 路线

hidden-table 路线最大的问题不是“功能不通”，而是**系统边界不自然**：

- 全文索引长期表现成一组独立隐藏表
- 更新/删除仍然要额外拼 SQL 去维护这些表
- merge / compact / refresh 没有真正和 base data 的生命周期绑定
- 查询执行也还在依赖 table function + 动态 SQL + Go 聚合框架

这会导致两个结果：

1. **运行稳定性依赖很多补丁式逻辑**
2. **可维护性会随着功能增加越来越差**

所以更合理的做法是：

- **当前代码主线**：直接推进 storage-coupled native FTS
- **v2-lite**：仅保留为方案对比与历史经验，不再继续扩展

---

## 3. 三方案总对比

| 维度 | 当前 MO v1 | hidden-table FTS v2-lite | storage-coupled native FTS |
| --- | --- | --- | --- |
| 物理表示 | 单隐藏 token 表：`doc_id,pos,word` | 4 张隐藏表：`fts_meta / fts_docs / fts_segment / fts_delta` | FTS segment 附着在 base object / part 生命周期上 |
| 建索引 | 同步建隐藏表并整表回填 | feature flag 下创建 4 张隐藏表并回填 `docs + segment` | 建立 metadata + build job，按 snapshot backfill 并 catch-up |
| INSERT | 直接切词写 token 表 | 写 `docs + delta` | 写 delta / micro-segment，refresh 后可见 |
| UPDATE | `delete old tokens + re-tokenize + insert new` | 删除旧 `docs/segment/delta` 后重写 `docs + delta` | 旧版本打 tombstone / delete bitmap，新版本追加写入 |
| DELETE | `DELETE FROM hidden_index WHERE doc_id IN (...)` | `DELETE FROM docs/segment/delta WHERE doc_id IN (...)` | 先记 tombstone / delete bitmap，compaction 时物理清理 |
| 查询执行 | `MATCH` 改写成 `fulltext_index_scan`，动态 SQL 读 token 表，Go 里聚合和打分 | 仍走 `fulltext_index_scan` 框架，但读 `docs + UNION(segment, delta)` | 专用 `FTS_SCAN` 直接读 postings / positions，完成召回、top-k、回表 |
| 统计/打分 | 查询时动态统计，TF-IDF 默认，BM25 可选 | 有 `docs` 表，统计形态更清晰，但仍未完全原生化 | segment/global stats 持久化，BM25 默认更自然 |
| phrase/proximity | 依赖位置列，语义与主流实现仍有偏差 | 当前明确不支持 phrase | positions 一等公民，可做 phrase / slop / proximity |
| 后台 merge | 没有 | 还没有真正落地 | 是核心生命周期的一部分 |
| 日更适配 | 差 | 中 | 好 |
| 运维与恢复 | 偏脆弱，索引和数据关系松散 | 比 v1 好，但仍然像外挂隐藏结构 | 最自然，跟随 MO 自身对象与 compaction 体系 |
| 长期可维护性 | 差 | 中 | 好 |
| 推荐结论 | 只能止血 | 适合作为桥接 | 最适合作为 internal-only 主线 |

---

## 4. 当前 MO 全文实现（v1）：实现原理与劣势

## 4.1 实现原理

当前 v1 的核心思想很直接：

1. 把原表文本列切词
2. 把每个 token 作为一行写进隐藏索引表
3. 查询时把 `MATCH ... AGAINST` 改写成对隐藏索引表的 SQL
4. 再在 Go 里做候选聚合、TF-IDF/BM25 计算和回表

它的逻辑图可以概括成：

```text
原表文本
  -> fulltext_index_tokenize
  -> 隐藏 token 表(doc_id, pos, word)

MATCH ... AGAINST
  -> planner rewrite
  -> fulltext_index_scan(...)
  -> 动态 SQL 读 token 表
  -> Go 内存聚合/打分
  -> doc_id 回表 JOIN 原表
```

### 关键代码落点

- DDL / 隐藏表定义：`pkg/sql/plan/build_ddl.go`
- 建索引回填：`pkg/sql/compile/ddl_index_algo.go`、`pkg/sql/compile/util.go`
- planner 改写：`pkg/sql/plan/apply_indices_fulltext.go`
- 查询执行：`pkg/sql/colexec/table_function/fulltext.go`
- DML 维护：`pkg/sql/plan/build_dml_util.go`

## 4.2 它为什么能工作

从功能上看，这条链路是完整的：

- 有 SQL 语法
- 有隐藏索引表
- 有增量维护
- 有查询改写
- 有相关性打分
- 能回表拿原始行

所以对小规模数据、功能验证、演示型场景，它不是完全不能用。

## 4.3 它的结构性劣势

### A. 索引结构太表化

它的物理核心仍然是一张“逐 token 一行”的隐藏表，而不是成熟全文系统里的：

- term dictionary
- postings list
- positions block
- delete bitmap
- segment metadata

这会直接导致：

- 行数膨胀快
- 查询要读很多细碎 token 行
- 很难做真正的 posting 级优化

### B. 建索引是同步整表回填

当前建索引本质上是：

```sql
INSERT INTO hidden_index
SELECT f.*
FROM src
CROSS APPLY fulltext_index_tokenize(...) AS f;
```

也就是说：

- 需要重扫原表
- 需要同步切词
- 需要同步写隐藏表

对大表 build 很不友好。

### C. UPDATE/DELETE 写放大严重

当前更新文本列时，本质动作是：

1. 删掉旧 `doc_id` 的全部 token
2. 重新切词
3. 再全部写回去

这意味着只改一行文本，也可能触发整篇文档 token 重建。

### D. 查询时做了太多动态工作

`MATCH ... AGAINST` 并不是直接走原生倒排 scan，而是：

1. 动态组 SQL
2. 扫 token 行
3. 在 Go map 中聚合
4. 再算分数
5. 最后回表

这条查询链路在大数据下很容易出现：

- 候选集过大
- 内存聚合偏重
- `LIMIT` 不能充分提前剪枝

### E. 没有 refresh / merge / compaction 生命周期

这是 v1 最大的系统性短板之一。  
它没有真正意义上的：

- delta segment
- immutable segment
- tombstone
- background merge

所以长期运行后，索引结构不会变得更“整齐”，只会继续依赖 SQL 维护和表扫描式思维。

## 4.4 对你的目标意味着什么

如果你的目标是：

- 不外接引擎
- 支持更大数据
- 每天固定时间更新一批
- 低到中等并发查询

那么 v1 **不应该作为长期承诺方案**。

---

## 5. hidden-table FTS v2-lite：实现原理与劣势

## 5.1 它是什么

当前 FTS v2-lite 是在 v1 基础上的一次重要修正。  
它不是完整 native FTS，但也不再是单 hidden token 表。

它引入了 4 张隐藏表：

- `fts_meta`
- `fts_docs`
- `fts_segment`
- `fts_delta`

并通过 `experimental_fulltext_v2_index` 开关启用。

### 关键代码落点

- catalog / table type：`pkg/catalog/types.go`
- 开关：`pkg/frontend/variables.go`
- DDL 构建：`pkg/sql/plan/build_ddl.go`
- v2 build：`pkg/sql/compile/ddl_index_algo.go`、`pkg/sql/compile/util.go`
- v2 SQL 生成：`pkg/fulltext/sql.go`
- planner 参数传递：`pkg/sql/plan/apply_indices_fulltext.go`、`pkg/sql/plan/fulltext.go`
- 执行器复用：`pkg/sql/colexec/table_function/fulltext.go`
- DML 维护：`pkg/sql/plan/build_dml_util.go`、`pkg/sql/colexec/postdml/postdml.go`

## 5.2 实现原理

### A. 建索引

建索引时，v2-lite 会：

1. 创建 4 张隐藏表
2. 初始化 metadata
3. 回填 `docs`
4. 回填 `segment`

也就是说，它已经开始把“文档信息”和“posting 信息”拆开，而不是所有东西全塞进一张 token 表。

### B. 查询

查询时，planner 仍然保留 `MATCH ... AGAINST` 入口，但会把 v2 的表名和实现标记传入执行层。  
随后执行层基于 `docs + UNION ALL(segment, delta)` 生成 SQL，再在现有 table function 框架里完成聚合和打分。

相比 v1，它的改进在于：

- 文档统计被单独显式化
- segment 和 delta 被区分出来
- 更接近“主段 + 增量段”的思路

### C. 写入

INSERT/UPDATE/DELETE 不再完全复用 v1 的单表逻辑，而是走 post-DML 维护：

1. 对旧 `doc_id` 删除 `segment / delta / docs` 里的旧数据
2. 对新版本重新切词
3. 写入 `docs + delta`

这说明 v2-lite 已经尝试从“最终索引直接重写”转向“文档表 + 增量表”的思路。

## 5.3 它相对 v1 的真实提升

### A. 索引结构更接近倒排

至少现在已经有：

- 文档表
- 主段表
- 增量表
- metadata

这比 v1 的单 token 表明显进了一步。

### B. 更容易继续演进

v2-lite 让后面的这些事更容易接：

- 增量 refresh
- 后台 merge
- 持久化统计
- postings/segment 优化
- 专用 scan 节点

### C. 更适合做 bridge

它保留了：

- 当前 SQL 入口
- 当前 planner/executor 主体框架
- 当前元数据体系

所以工程上是一个很好的过渡层。

## 5.4 它为什么仍然不适合做长期主线

### A. 它本质上还是 hidden-table 方案

虽然从 1 张表变成了 4 张表，但本质仍然是：

- 用隐藏表模拟全文索引内部结构
- 用 SQL 维护这些隐藏表

这比 v1 好，但系统边界仍不够自然。

### B. UPDATE/DELETE 仍然偏重

当前 v2-lite 不是 tombstone-first，而是：

1. 删旧数据
2. 重建新数据

而且删除还是按 `doc_id` 发起；如果 posting 数据本身更偏 `word` 组织，这类删除并不友好。

### C. post-DML 仍然会形成串行尾巴

当前维护逻辑是把补充 SQL 放进 `PostDmlSqlList`，主流水线完成后再执行。  
这对低频更新还行，但对大批量日更会形成明显尾部开销。

### D. 还没有真正的 merge / refresh / compact

这是最关键的问题。  
如果没有这层，`delta` 会长期存在，查询就要一直：

- 查 `segment`
- 查 `delta`
- 再做合并

时间一长，结构会继续变重。

### E. 查询执行仍未原生化

即使 v2-lite 的存储形态更合理，它目前仍然复用了：

- table function
- 动态 SQL
- Go 端聚合/打分

这意味着它还没有获得 native FTS 真正该有的：

- posting 级布尔执行
- positions 级 phrase 校验
- 早期 top-k 剪枝

### F. phrase 还不支持

当前 v2-lite 明确不支持 phrase/proximity，这是它在搜索体验上的一个明显短板。

## 5.5 对你的目标意味着什么

如果你的目标是“只在 MO 内部实现，先要能接一批数据更大的用户，更新不多但每天可能定时更新”，那 v2-lite 的合适定位是：

- **可以保留**
- **可以继续用来承接当前代码**
- **但最好只当 bridge，不要当长期终态**

---

## 6. storage-coupled native FTS：实现原理与优势

## 6.1 它是什么

storage-coupled native FTS 的核心不是“把 v2-lite 再拆几张隐藏表”，而是：

**让全文索引真正跟随 MO 自己的存储对象生命周期。**

具体说，就是让下面这些能力变成 MO 内部原生能力：

- FTS metadata
- per-part / per-object FTS segment
- delta / micro-segment
- delete bitmap / tombstone
- background merge / compaction
- 原生 `FTS_SCAN`

这里的“native”仍然是 **MO 内部 native**，不是外接独立搜索服务。

## 6.2 实现原理

### A. 建索引

建索引不再是“立即把全文索引最终结构同步写完”，而是：

1. 创建 FTS index metadata
2. 建立 build job
3. 以某个 snapshot 启动 backfill
4. backfill 期间把新提交的数据写入 delta / catch-up
5. 回填完成后切换为 searchable

### B. 写入

INSERT/UPDATE/DELETE 都不再直接大改 posting 主体，而是：

- **INSERT**：写 delta / micro-segment
- **UPDATE**：旧版本打 tombstone，新版本写新段
- **DELETE**：只写 tombstone / delete bitmap

真正的物理整理交给后台 merge / compaction。

### C. 查询

查询执行不再拼大 SQL，而是：

1. 读 segment metadata
2. 找 term dictionary
3. 读 postings / positions
4. 在 FTS scan 节点里做交并、phrase、prefix、top-k
5. 最后回表

### D. 合并

后台任务负责：

- 合并小段
- 清理 tombstone
- 维护 segment stats
- 控制 delta 大小

这一点对于长期稳定运行非常关键。

## 6.3 它为什么更适合“internal-only + 日更”目标

### A. 系统边界最自然

它不再长期依赖隐藏表去“模拟”全文引擎内部结构，而是让全文索引真正成为：

- MO metadata 的一部分
- MO object / part 生命周期的一部分
- MO compaction / merge 体系的一部分

这是稳定性和可维护性的关键。

### B. UPDATE/DELETE 成本更可控

最重要的改进是：

- 不再每次都同步删大段 posting
- 先写 tombstone / 新版本
- 后台统一 merge 清理

这会让每天固定时间更新一批数据时，写路径更短、更平滑。

### C. 查询性能更容易做稳

因为查询直接针对：

- term dictionary
- postings
- positions
- segment stats

所以可以更自然地做：

- candidate pruning
- local top-k
- phrase/proximity
- BM25 默认化

### D. 恢复和可观测性更自然

当全文索引跟 base object / part 同步演进时，下面这些事情会更好做：

- snapshot 一致性
- repair / inspect
- merge backlog 观测
- rebuild / optimize

### E. 更符合你的约束

你明确要求的是：

- 不用外部引擎
- 不用外挂模块
- 只在 MO 内把这件事做成

那 storage-coupled native FTS 正是沿着这个方向走的：

- 不外接 ES
- 不需要独立搜索集群
- 不需要旁路同步服务成为主路径
- 仍然保留 SQL、事务、权限、optimizer、向量链路解耦

## 6.4 它的代价

它不是没有代价。  
它比当前 v2-lite 更难做，因为会同时涉及：

- 存储层对象表示
- compaction 生命周期
- executor / planner
- metadata
- 后台任务
- 运维接口

但这个代价买到的是：

- 长期稳定性
- 长期可维护性
- 更自然的日更能力

这笔账对你的目标是划算的。

---

## 7. 为什么 storage-coupled native FTS 比 hidden-table v2-lite 更适合你的目标

如果把目标明确成下面这组约束：

- **internal-only**
- **不接外部引擎**
- **不靠外挂模块**
- **未来数据量更大**
- **并发不是特别高**
- **每天固定时间会更新一批**
- **不要影响当前向量查询**

那么三条路线里最合适的是：

1. **v1**：排除，能力验证可以，不能作为长期承诺
2. **hidden-table v2-lite**：保留，作为 bridge
3. **storage-coupled native FTS**：主推

原因很简单：

- 你的问题核心不是“先把 SQL 跑通”
- 而是“系统能不能长期稳定地跑”

而长期稳定这件事，最终取决于：

- 写路径是否平滑
- merge/compaction 是否自然
- 查询是否原生
- 索引生命周期是否跟 base data 生命周期一致

这些恰恰都是 storage-coupled native FTS 的强项。

---

## 8. 推荐落地顺序

### 第一步：继续保留当前 v2-lite，但明确它只是 bridge

它的价值主要是：

- 兼容现有 SQL 入口
- 验证 metadata 和参数协议
- 承接已有代码和测试

### 第二步：尽快把写路径改成 tombstone-first

优先做：

- delete bitmap / tombstone
- delta append
- 后台 merge / compact

先把“同步删除大段 posting”这个最伤稳定性的点干掉。

### 第三步：把查询从 table function 下沉成原生 FTS scan

目标是去掉：

- 动态 SQL
- Go map 大聚合

把召回、交并、top-k、phrase 校验放到原生算子里。

### 第四步：让 FTS 生命周期真正挂到 object / part / compaction 上

这是最终把 hidden-table 方案升级成 storage-coupled native FTS 的关键一步。

---

## 9. 最终结论

**如果坚持 internal-only，不使用外部搜索引擎或外挂搜索模块，那么最合理的路线不是继续把当前 hidden-table 方案做大，而是把它当桥接，最终转向 storage-coupled native FTS。**

再压缩成一句话就是：

- **当前 MO v1**：结构太原始，不适合作为长期方案
- **当前 hidden-table FTS v2-lite**：值得保留，但只适合作为 bridge
- **storage-coupled native FTS**：最适合你当前目标的内部主线
