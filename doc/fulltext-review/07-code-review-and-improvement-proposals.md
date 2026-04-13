# MatrixOne 全文索引代码级 Review 与改进建议

## 0. Review 范围

本文基于以下代码和文档的逐行分析：

- `doc/fulltext-review/01` ~ `06` 全部文档
- `pkg/fulltext/native/segment.go` — native FTS 内核
- `pkg/fulltext/native/object.go` — sidecar 生成与读取
- `pkg/fulltext/tokenize.go` — 统一切词
- `pkg/sql/colexec/table_function/fulltext_native.go` — native 查询执行
- `pkg/sql/colexec/table_function/fulltext.go` — v1 查询执行与 native/v1 切换
- `pkg/vm/engine/tae/tables/jobs/flushobj.go` — flush 时 sidecar 生成
- `pkg/vm/engine/tae/tables/jobs/mergeobjects.go` — merge 时 sidecar 重建
- `pkg/vm/engine/tae/mergesort/task.go` — MergeTaskHost 接口
- `pkg/objectio/writer.go` — object 写入器
- `pkg/objectio/metav2.go` — object 元数据类型

---

## 1. 文档体系 Review

### 1.1 整体评价

六篇文档的分析方向正确，结论合理。v1 → v2-lite → storage-coupled native FTS 的演进路线判断是对的。

### 1.2 文档存在的问题

#### A. 对 native FTS 的风险描述严重不对称

v1 和 v2-lite 的劣势分析都有代码级论据支撑，每个点都展开了。但 05 文档对 native FTS 的代价只用了一小段话（第 6.4 节），列了几个涉及面就结束了。这会让决策者低估 native 路线的实际工程难度。

#### B. 05 文档的落地顺序和 06 文档的实际代码状态不匹配

05 文档建议的落地顺序是：保留 v2-lite → tombstone-first → 原生 FTS scan → 挂到 object/part/compaction。但 06 文档显示代码已经跳过了前两步，直接做了 sidecar 接入 flush/merge 和 native query。这说明 05 是写文档时的规划，和实际推进路径已经脱节。

#### C. 缺少对几个关键问题的讨论

- appendable tail 问题（刚写入数据无法走 native）
- v1 fallback 的长期维护成本
- sidecar 元数据 GC / 自愈
- datalink 列在 native 方案下的长期策略

---

## 2. 当前 native FTS 代码 Review

### 2.1 Segment 数据结构

```go
type Segment struct {
    Terms map[string][]Posting
}
```

**问题 1：整个 segment 反序列化到内存 map**

当前 `ReadSidecar` 会把整个 sidecar 文件反序列化成 `map[string][]Posting`。对于一个包含大量 term 的 object，这意味着：

- 查询一个 term 也要加载全部 term 的 postings
- 内存占用和 object 中文本量成正比
- 无法做 term 级别的按需加载

这在小 object 上没问题，但当 object 变大（MO 默认 object 可以到 128MB+），一个 sidecar 可能包含数十万甚至上百万 term，全量反序列化会成为瓶颈。

**建议**：segment 格式应该支持 term dictionary + offset index，允许只读取目标 term 的 postings 而不加载整个 segment。可以参考 Lucene 的 `.tip` / `.tim` / `.doc` 分层，但不需要做到那么复杂——一个简单的 `sorted term array + offset table + postings blocks` 就够了。

**问题 2：postings 没有压缩**

当前 postings 是逐字段 binary.Write，每个 posting 包含 Block(2B) + Row(4B) + PK(变长) + DocLen(4B) + Positions(变长)。没有任何压缩。

对于高频 term（比如中文 3-gram 里的常见片段），一个 term 可能有数万条 posting，未压缩的存储和传输开销会很大。

**建议**：至少对 positions 做 delta encoding + varint 压缩。postings 本身也可以按 block 分组后做 delta encoding。这是全文索引最基本的空间优化，实现成本很低。

**问题 3：Terms 用 Go map，查询时无法做 prefix scan**

当前 `nativeLookupLeaf` 对 `STAR`（前缀查询）的实现是遍历整个 `seg.Terms` map：

```go
for term, termPostings := range seg.Terms {
    if strings.HasPrefix(term, prefix) {
        postings = append(postings, termPostings...)
    }
}
```

这是 O(N) 遍历所有 term，对大 segment 很慢。

**建议**：如果要支持 prefix 查询，term dictionary 应该用有序结构（sorted slice 或 trie），而不是 hash map。

### 2.2 Sidecar 生命周期

**问题 4：sidecar 通过 deterministic path 定位，没有显式元数据**

```go
func SidecarPath(objectName string, indexName string) string {
    sum := sha1.Sum([]byte(indexName))
    return fmt.Sprintf("%s.fts.%x.seg", objectName, sum[:8])
}
```

这意味着：

- 没有地方记录"哪些 object 有 sidecar、哪些没有"
- 判断 sidecar 是否存在只能靠 `fs.Read` 然后看是否 `ErrFileNotFound`
- 无法做 sidecar 级别的 inspect / repair / GC
- checkpoint / replay 时无法确认 sidecar 完整性

**建议**：在 object metadata 或 catalog 中显式记录 sidecar 存在性。可以是 ObjectStats 里加一个 bit flag，也可以是 catalog 层面的 index metadata。

**问题 5：旧 object 被 merge 后，sidecar 文件没有显式清理**

06 文档也提到了这个问题。当前依赖"旧 object 不可见所以 sidecar 不会被查询到"，但 sidecar 文件本身会一直留在 file service 里。长期运行后会积累大量孤儿 sidecar 文件。

**建议**：在 object GC 流程中同步清理对应的 sidecar 文件。或者在 sidecar 元数据治理中定期扫描清理。

**问题 6：sidecar 生成失败时没有重试或标记机制**

`flushobj.go` 和 `mergeobjects.go` 中，sidecar 生成失败会直接返回 error，导致整个 flush/merge 任务失败。但 sidecar 生成失败不应该阻塞数据写入——数据本身是完整的，只是全文索引暂时不可用。

```go
// flushobj.go
if idxErr = indexer.Write(ctx, task.fs, task.name); idxErr != nil {
    return idxErr  // 这里会导致整个 flush 失败
}
```

**建议**：sidecar 生成失败应该降级处理（记录日志 + 标记该 object 缺少 sidecar），而不是阻塞 flush/merge。查询时已经有 fallback 到 v1 的机制，所以降级是安全的。

### 2.3 查询执行

**问题 7：native 查询仍然复用 v1 的 fulltextState 和打分框架**

`fulltext_native.go` 里的 `populatePhraseCompat` 和 `populateBooleanNative` 最终都是往 `u.agghtab` / `u.aggcnt` / `u.docLenMap` 里填数据，然后由 v1 的 `evaluate()` / `sort_topk()` 来做最终打分和排序。

这意味着：

- native 查询只替换了"候选召回"阶段
- 打分和 top-k 仍然走 v1 的 Go map 聚合路径
- 没有实现真正的 segment 级 early top-k 剪枝

**建议**：这不是当前必须改的，但应该在文档中明确说明"当前 native 查询的原生化程度"，避免给人"已经完全原生化"的错觉。05 文档第 6.2.C 节描述的理想查询路径和实际实现有差距。

**问题 8：每个 object 的 sidecar 都要单独读取，没有批量 I/O**

```go
for i := range stats {
    seg, exists, err := ftnative.ReadSidecar(proc.Ctx, proc.Base.FileService, name, indexTableName)
    // ...
}
```

如果一个表有 100 个 object，就要发 100 次 file service read。每次 read 都是一次完整的 I/O 操作。

**建议**：可以考虑并行读取 sidecar，或者在 file service 层面做批量 read。

**问题 9：tombstone 过滤是逐行检查的**

```go
func isNativeDeleted(...) (bool, error) {
    rows := []int64{int64(ref.Row)}
    rows = scan.tombstones.ApplyInMemTombstones(&bid, rows, nil)
    // ...
}
```

虽然有 `nativeDeleteCache` 做了 block 级缓存，但对于每个候选 posting 仍然是逐行调用 `isNativeDeleted`。如果候选集很大，这会产生大量的 tombstone 查询。

**建议**：可以在 block 级别批量获取 deleted rows bitmap，然后对整个 block 的候选一次性过滤。

**问题 10：`prepareNativeScan` 的 fallback 判断过于保守**

```go
stats, err := rel.GetNonAppendableObjectStats(proc.Ctx)
if len(visible) != len(stats) {
    return nil, false, nil  // 有任何不一致就 fallback
}
```

只要有一个 object 的 sidecar 不存在，或者 visible object 数量和 non-appendable object 数量不一致，就整体 fallback 到 v1。这意味着：

- 只要表里还有 appendable object（刚写入还没 flush 的数据），就不走 native
- 只要有一个 object 的 sidecar 丢失或损坏，整个表的查询都退化到 v1

**建议**：可以做更细粒度的 fallback——对有 sidecar 的 object 走 native，对没有的走 v1，最后合并结果。这样可以在大部分数据走 native 的同时，只对少量数据走 fallback。

### 2.4 写入路径

**问题 11：merge 时 sidecar 重建需要重新切词**

```go
func (task *mergeObjectsTask) OnBlockWritten(bat *batch.Batch, _ uint16) error {
    return task.nativeIndexer.AddBatch(bat, []uint32{uint32(bat.RowCount())})
}
```

merge 时对每个输出 block 都要重新调用 `AddBatch`，而 `AddBatch` 内部会对每行数据重新切词（`fulltext.TokenizeIndexValues`）。但被 merge 的 object 已经有 sidecar 了，理论上可以直接合并已有的 sidecar 而不需要重新切词。

**建议**：实现 segment merge 能力——直接合并多个 sidecar 的 postings，只需要重新映射 block/row 引用。这会显著降低 merge 时的 CPU 开销，尤其是对大文本列。

---

## 3. 结合 MO 存储体系的改进建议

### 3.1 把 sidecar 嵌入 object 文件本身

当前 sidecar 是独立文件（`{objectName}.fts.{hash}.seg`），和 object 文件分离。这带来了前面提到的生命周期不一致、GC 困难、元数据缺失等问题。

MO 的 object 文件格式（`objectWriterV1`）已经支持 sub-block 和多种 DataMetaType（`SchemaData`、`SchemaTombstone`、checkpoint 类型等）。可以考虑：

**方案 A：新增 FTS DataMetaType**

在 `DataMetaType` 中新增一个类型（比如 `SchemaFTS`），把 FTS segment 数据作为 object 文件的一个 sub-block 写入。这样：

- sidecar 和 object 天然同生命周期
- GC 自动跟随 object 清理
- checkpoint / replay 自动覆盖
- 不需要额外的 sidecar 元数据治理

代价是需要改 object writer/reader，但改动量不大——`WriteSubBlock` 和 `AddSubBlock` 已经支持按 DataMetaType 写入不同类型的数据。

**方案 B：保持独立文件，但加显式 locator**

如果不想改 object 格式，至少应该在 ObjectStats 或 catalog 中记录 sidecar 的存在性和位置。

**推荐方案 A**，因为它从根本上解决了生命周期问题，而且 MO 的 object 格式已经有扩展机制。

### 3.2 利用 MO 的 block 级 ZoneMap 做 term 级剪枝

MO 的 object 已经为每个 block 的每个列维护了 ZoneMap（min/max）。虽然 ZoneMap 对全文检索的直接价值有限（文本列的 min/max 意义不大），但可以借鉴这个机制：

- 在 sidecar 中为每个 block 维护一个 term bloom filter
- 查询时先用 bloom filter 跳过不包含目标 term 的 block
- 这对 boolean AND 查询特别有效——如果某个 block 不包含所有必须 term，可以直接跳过

### 3.3 利用 MO 的 compaction 调度做 FTS segment 合并

当前 sidecar 是 per-object 的，merge 时重建。但可以更进一步：

- 在 compaction 调度中考虑 FTS segment 的碎片化程度
- 如果某个表的 FTS sidecar 过于碎片化（太多小 object），可以优先触发 merge
- 这样 FTS 的 merge 策略就自然融入了 MO 的 compaction 体系

### 3.4 appendable tail 的 native 化路径

当前 appendable object 没有 sidecar，查询 fallback 到 v1。这是最影响用户体验的 gap。建议的解决路径：

**方案：在 CN 侧维护 in-memory delta segment**

- 当数据写入 appendable object 时，同时在 CN 内存中维护一个临时的 FTS segment
- 查询时合并 persisted sidecar 结果和 in-memory delta 结果
- flush 时 in-memory delta 随 object 一起持久化为 sidecar

这比"等 flush 后才能走 native"体验好很多，而且实现上可以复用现有的 `Builder` / `Segment` 代码。

### 3.5 全局统计的持久化

当前 native 查询仍然依赖 v1 的 `runCountStar` 来获取全局文档数和平均文档长度。这是一个查询时的额外开销。

**建议**：在 sidecar 中持久化 per-object 的统计信息（doc count、total token count），查询时汇总所有 visible object 的统计即可得到全局统计。这样就不需要额外的 count 查询了。

具体做法：在 `Segment` 结构中增加：

```go
type Segment struct {
    Terms    map[string][]Posting
    DocCount int64
    TokenSum int64
}
```

查询时：

```go
totalDocs := int64(0)
totalTokens := int64(0)
for _, obj := range scan.objects {
    totalDocs += obj.segment.DocCount
    totalTokens += obj.segment.TokenSum
}
avgDocLen := float64(totalTokens) / float64(totalDocs)
```

---

## 4. 优先级排序

如果要继续推进 native FTS，建议按以下优先级：

### P0：必须尽快做

1. **sidecar 生成失败不阻塞 flush/merge**（问题 6）——这是生产稳定性问题
2. **全局统计持久化到 sidecar**（3.5）——去掉每次查询的 count 开销
3. **sidecar 元数据显式化**（问题 4）——至少记录存在性

### P1：近期应该做

4. **segment 格式支持按需加载 term**（问题 1）——大 object 场景的性能关键
5. **postings 压缩**（问题 2）——空间和 I/O 优化
6. **sidecar GC**（问题 5）——长期运行的存储治理
7. **更细粒度的 native/v1 混合查询**（问题 10）——减少不必要的 fallback

### P2：中期改进

8. **sidecar 嵌入 object 文件**（3.1 方案 A）——根本解决生命周期问题
9. **merge 时直接合并 sidecar 而非重新切词**（问题 11）——降低 merge CPU 开销
10. **appendable tail in-memory delta**（3.4）——消除 appendable fallback
11. **并行 sidecar 读取**（问题 8）——查询 I/O 优化
12. **block 级 term bloom filter**（3.2）——查询剪枝

### P3：长期演进

13. **独立 FTS_SCAN 算子替代 table function 框架**（问题 7）
14. **segment 级 early top-k**
15. **prefix 查询用有序结构替代 map 遍历**（问题 3）

---

## 5. 对文档体系的修正建议

1. **05 文档**应该补充 native FTS 的具体风险和代价分析，和 v1/v2-lite 的分析深度对齐。
2. **05 文档**的落地顺序应该更新为和实际代码状态一致。
3. **06 文档**应该补充本 review 中发现的代码级问题。
4. 建议新增一篇文档专门讨论 **sidecar 与 object 生命周期的绑定策略**，这是 native FTS 最核心的架构决策。

---

## 6. 总结

当前 native FTS 的方向是对的——把全文索引挂到 MO 的 object 生命周期上，比继续维护 hidden-table 方案更有前途。代码已经走出了关键的第一步：flush/merge 时生成 sidecar，查询时读取 sidecar，不匹配时 fallback v1。

但当前实现仍然处于"能跑通"阶段，距离"能跑稳"还有明显差距。最关键的几个问题是：

- **sidecar 生成失败会阻塞 flush/merge**——这是生产环境的定时炸弹
- **整个 segment 全量反序列化到内存**——大 object 场景会成为瓶颈
- **sidecar 没有显式元数据**——无法做治理、无法做 inspect
- **fallback 判断过于保守**——一个 object 缺 sidecar 就整表退化

最值得投入的改进方向是：把 sidecar 嵌入 object 文件格式（利用已有的 DataMetaType 扩展机制），从根本上解决生命周期问题。这比维护独立 sidecar 文件 + 补丁式 GC 更干净。
