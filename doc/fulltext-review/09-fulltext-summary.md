# MatrixOne 全文索引总结：问题、演进与当前状态

## 结论先行

**当前 native FTS 实现方向正确，可以正常开发推进。** 核心改动对 TAE 稳定性无风险，所有 sidecar 相关失败都是静默降级而非硬失败。

---

## 1. 旧版本（v1）的问题

### 1.1 它是什么

v1 的全文检索本质上是一个**拼装方案**：

```
原表文本 → 切词 → 写入隐藏 token 表(doc_id, pos, word)
MATCH ... AGAINST → planner 改写 → 动态 SQL 读 token 表 → Go 内存聚合打分 → 回表
```

它不是搜索引擎，而是"隐藏表 + SQL 改写 + table function + 查询时内存打分"。

### 1.2 核心问题

#### A. 索引结构太原始

物理核心是一张"逐 token 一行"的隐藏表，没有：
- term dictionary / postings list / positions block
- delete bitmap / segment metadata
- 任何形式的压缩

**后果**：行数膨胀快，查询要读大量细碎 token 行，无法做 posting 级优化。

#### B. 写入全部同步

- **建索引**：同步整表回填（`INSERT INTO hidden_index SELECT ... CROSS APPLY fulltext_index_tokenize`）
- **UPDATE**：删掉旧 doc_id 的全部 token → 重新切词 → 全部写回
- **DELETE**：`DELETE FROM hidden_index WHERE doc_id IN (...)`

没有异步 refresh、没有 delta segment、没有 tombstone。改一行文本就要重建整篇文档的全部 token。

#### C. 查询做了太多动态工作

每次 `MATCH ... AGAINST` 的执行链路：

1. 先执行 `SELECT COUNT(*), AVG(pos) FROM index_table WHERE word = '__DocLen'` 获取全局统计
2. 动态生成 SQL 读 token 表
3. 流式拉候选到 Go 内存
4. 在 Go map 中按 doc_id 聚合
5. 计算 TF-IDF / BM25
6. top-k heap 排序
7. 回表 JOIN 原表

**后果**：候选集过大时内存聚合偏重，LIMIT 不能充分提前剪枝。

#### D. 没有 merge / compaction 生命周期

索引结构不会随时间变得更"整齐"，只会继续依赖 SQL 维护和表扫描式思维。长期运行后性能持续退化。

#### E. 语义偏离

`IN NATURAL LANGUAGE MODE` 实际走的是 `SqlPhrase` 路径（强位置约束），不是主流全文系统的 bag-of-words 相关性检索。用户以为在做"自然语言搜索"，实际更接近"按 token 顺序严格匹配"。

### 1.3 v1 适合什么

- 数据量不大、文本不长、写入不频繁、并发不高、对召回/排序质量要求不高的场景
- 功能验证、Demo、小规模应用

**不适合**：大量文档、高频更新、高并发查询、要求稳定 top-k latency 的场景。

---

## 2. 新版本（native FTS）的实现原理

### 2.1 核心思想

**把全文索引从"独立维护的隐藏表"变成"跟随 MO object 生命周期的 sidecar 文件"。**

每个 MO data object 旁边挂一个 FTS sidecar 文件，包含该 object 内所有文本列的倒排索引。sidecar 和 object 同生同灭——flush 时生成，merge 时重建，GC 时清理。

当前实现里还增加了一个 **per-object locator 文件**，显式记录这个 object 对应的 sidecar 路径。GC 会优先读 locator，再把已存在的 sidecar 一并删除；如果 locator 坏掉，object GC 仍然继续，不会被 sidecar metadata 卡住。

### 2.2 数据结构

```go
type Segment struct {
    Terms    map[string][]Posting  // term → postings 列表
    DocCount int64                 // 该 object 的文档数
    TokenSum int64                 // 该 object 的总 token 数
}

type Posting struct {
    Ref       RowRef    // (Block, Row, PK) — 定位到 object 内的具体行
    DocLen    int32     // 文档长度（用于 BM25）
    Positions []int32   // 该 term 在文档中的位置列表（用于 phrase 查询）
}
```

这已经是一个真正的倒排索引结构——有 term dictionary、有 postings、有 positions、有文档统计。

### 2.3 写入路径

#### flush 时生成 sidecar

```
内存 appendable object
  → flush 持久化为 immutable object 文件
  → 对 object 内每行文本切词
  → 构建 Segment（term → postings）
  → 序列化写入 sidecar 文件（{objectName}.fts.{hash}.seg）
```

代码位置：`pkg/vm/engine/tae/tables/jobs/flushobj.go`

关键安全保证：
- 在 `writer.Sync()` **之后**执行，object 数据已完整落盘
- 失败只记日志，不阻塞 flush
- sidecar 写完后再写 locator，失败同样只降级不阻塞

#### merge 时重建 sidecar

```
多个旧 object → compaction 合并 → 输出新 object
  → 对新 object 的每个 block 切词
  → 构建新 Segment
  → 写入新 sidecar
```

代码位置：`pkg/vm/engine/tae/tables/jobs/mergeobjects.go`

关键安全保证：
- `OnBlockWritten` 和 `OnObjectSynced` **永远 return nil**，不可能导致 merge 失败
- 两级状态机：`nativeObjectFailed`（per-object 隔离）+ `nativeIndexingClosed`（schema 级熔断）
- 没有 FULLTEXT 索引的表，所有回调直接跳过

### 2.4 查询路径

```
MATCH ... AGAINST
  → planner 改写为 fulltext_index_scan
  → 检查是否满足 native 条件
    → 所有 visible persisted object 都有 sidecar
    → 查询模式被 native 支持（NL / boolean / phrase）
  → 满足：走 native 路径
    → 枚举所有 visible object 的 sidecar
    → 汇总 persisted sidecar 的 DocCount / TokenSum（不再执行 runCountStar）
    → 通过 `Relation.Ranges + BuildReaders` 读取 committed in-memory / uncommitted tail
    → 把 tail rows 构建成一个临时 native segment，并把它和 persisted sidecar 一起参与召回
    → 在 sidecar / tail segment 中 lookup term → 收集 postings
    → 对 persisted sidecar 命中做 tombstone 过滤（tail segment 由 reader 自身保证可见性）
    → 布尔交并 / phrase 位置校验
    → 填充到 v1 的打分框架做 BM25 / top-k
  → 不满足：fallback 到 v1 路径（和之前完全一样）
```

代码位置：`pkg/sql/colexec/table_function/fulltext_native.go`

这里最关键的一点是：**appendable tail 不是通过 object metadata 暴露给查询层的，而是通过 row-level reader 路径暴露。**  
所以 native tail/delta 的正确实现方式，不是去强行枚举 appendable object，而是：

1. persisted immutable object 继续走 sidecar；
2. committed in-memory / workspace / uncommitted persisted data 通过 reader path 读出来；
3. 查询期现场构建一个临时 segment；
4. 这个临时 segment 不再额外做 tombstone 过滤，因为 reader 本身已经做了可见性裁剪。

### 2.5 序列化格式（V3 extensible header）

```
[magic "MOFTSN3\0" 8B]
[headerLen uint32 4B]        ← 后续加字段只需增大这个值
[DocCount int64 8B]
[TokenSum int64 8B]
[termCount uint32 4B]
[term1: len + bytes + postings...]
[term2: len + bytes + postings...]
...
```

向后兼容：能读取 V1（无统计，从 postings 重建）、V2（有统计但无 extensible header）、V3 三种格式。

### 2.6 sidecar locator / GC

```
object file
  + object.fts.locator
      -> [{index_table, file_path}, ...]
```

- locator 是显式 sidecar metadata 的第一步；
- GC 删除 object 时会 best-effort 读取 locator，并把 locator 自己和仍然存在的 sidecar 一并删除；
- 这避免了在 MO 当前 flat object namespace 上做昂贵的目录扫描；
- locator 解析失败不会阻塞 object GC，最坏情况只是 sidecar 泄漏。

### 2.7 降级策略

这是整个设计最关键的安全网：

| 场景 | 行为 |
|------|------|
| 表没有 FULLTEXT 索引 | flush/merge 的 sidecar 逻辑完全跳过，零开销 |
| sidecar 生成失败（flush） | 记日志，跳过 sidecar，flush 正常完成 |
| sidecar 生成失败（merge） | 记日志，标记 nativeObjectFailed，merge 正常完成 |
| schema 解析失败（merge） | 标记 nativeIndexingClosed，后续所有 object 跳过 sidecar |
| 查询时有 appendable / in-memory / uncommitted tail | 通过 query-time tail segment 纳入 native 结果 |
| 查询时某个 object 缺 sidecar | 整体 fallback 到 v1 |
| 查询模式不被 native 支持 | fallback 到 v1 |
| GC 时 locator 缺失/损坏 | object 正常删除，sidecar 清理 best-effort |

**sidecar 是纯加速层，不是正确性依赖。最坏情况就是退回到 v1 的行为。**

---

## 3. 新版本相比旧版本的优势

| 维度 | v1（隐藏表） | native（per-object sidecar） |
|------|-------------|---------------------------|
| **索引结构** | 逐 token 一行的隐藏表 | 真正的倒排索引（term → postings + positions） |
| **写入** | 同步整表回填 / 同步 delete + re-tokenize + insert | flush 时生成，merge 时重建，失败不阻塞 |
| **全局统计** | 每次查询执行 `COUNT(*)` | 持久化在 sidecar 中，查询时直接汇总 |
| **查询执行** | 动态 SQL → Go map 聚合 → 算分 | 直接读 postings → 布尔交并 → phrase 校验 → 算分 |
| **DELETE 可见性** | 依赖隐藏表的 SQL 维护 | 结合 MO 原生 tombstone 过滤 |
| **生命周期** | 隐藏表和 base data 脱节 | sidecar 和 object 同生同灭，并有 locator 辅助 GC |
| **merge / compaction** | 没有 | 跟随 MO 的 compaction 自动重建 |
| **对 TAE 的影响** | 无（纯 SQL 层） | 极低（flush/merge 末尾追加，失败只降级） |
| **对非 FTS 表的影响** | 无 | 无（Empty() 检查直接跳过） |
| **格式演进** | 无版本管理 | V1/V2/V3 向后兼容，V3 extensible header |

### 最核心的三个优势

1. **不再依赖隐藏表维护**：索引生命周期和 object 绑定，不需要额外的 SQL 去维护索引一致性。
2. **查询不再需要动态 SQL + 全局 count**：统计持久化在 sidecar 中，postings 直接读取，省掉了 v1 最重的两个开销。
3. **降级策略干净**：任何环节失败都不影响数据写入和查询正确性，最坏退回 v1。

---

## 4. 当前仍存在的限制与后续计划

### 4.1 已知限制

| 限制 | 影响 | 优先级 |
|------|------|--------|
| persisted object 仍是 all-or-nothing | 只要一个 visible persisted object 缺 sidecar，就整表 fallback v1 | P1 |
| locator 只接到了 GC best-effort | replay / inspect / repair 还不完整 | P1 |
| fallback 是全表级的 | 一个 object 缺 sidecar 就整表退化 | P1 |
| segment 全量反序列化到内存 | 大 object 场景内存和性能瓶颈 | P1 |
| postings 没有压缩 | 存储和 I/O 开销偏大 | P1 |
| 查询路径仍主要靠 deterministic path | locator 价值还没完全释放 | P1 |
| merge 时重新切词 | CPU 开销可优化 | P2 |
| prefix 查询 O(N) 遍历 | 大 segment 慢 | P2 |
| sidecar 串行读取 | 多 object 时 I/O 串行 | P2 |
| native 查询复用 v1 打分框架 | 没有 segment 级 early top-k | P2 |

### 4.2 推荐的下一步

1. **locator / replay / inspect 继续补强**：把 locator 从 GC 起点继续扩展到 inspect / repair / replay
2. **更细粒度的 mixed query**：有 sidecar 的 persisted object 走 native，没有的走 v1，合并结果
3. **segment 按需加载**：头部加 term index，查询时只读目标 term 的 postings

这三件事做完，native FTS 就从"能跑通"变成"能跑稳"。

---

## 5. 风险评估

| 改动点 | 风险 | 把握 | 最坏情况 |
|--------|------|------|----------|
| flush 加 sidecar 生成 | 极低 | 很高 | sidecar 没生成，查询走 v1 |
| merge 加 sidecar 重建 | 极低 | 很高 | sidecar 没生成，查询走 v1 |
| 查询加 native 分支 | 低 | 高 | native 条件不满足，走 v1 |
| segment 格式演进 | 极低 | 很高 | TAE 零改动，纯 sidecar 内部变化 |

**可以正常开发推进。**
