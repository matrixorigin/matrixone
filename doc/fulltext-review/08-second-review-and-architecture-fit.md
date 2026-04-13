# MatrixOne native FTS 第二轮 Review：代码变化确认、架构适配性分析与改进建议

## 0. 本轮 Review 范围

基于第一轮 review（07 文档）后的代码和文档更新，重新审视：

1. 上一轮提出的问题哪些已经修复
2. 修复质量如何
3. 当前实现是否是最适合 MO 的方案
4. 还有哪些新发现的问题
5. 更具体的改进建议

---

## 1. 上一轮问题修复确认

### 已修复

| 问题 | 修复方式 | 评价 |
| --- | --- | --- |
| **P0-1: sidecar 生成失败阻塞 flush/merge** | flush 和 merge 都改成了 `logNativeSidecarError` + 跳过，不再 return error | ✅ 修复正确。merge 侧还加了 `nativeObjectFailed` / `nativeIndexingClosed` 两级状态机，单 object 失败不影响后续 object，schema 级失败直接关闭整个 task 的 indexing。设计合理。 |
| **P0-2: 全局统计持久化到 sidecar** | `Segment` 新增 `DocCount` / `TokenSum`，`Builder.Add` 累加，`MarshalBinary` 写入 V2 magic 后，`prepareNativeScan` 汇总，`applyNativeSegmentStats` 直接设置 `s.Nrow` / `s.AvgDocLen`，跳过 `runCountStar` | ✅ 修复正确。还做了 V1 magic 向后兼容（`rebuildStats` 从 postings 重建统计），测试也覆盖了 legacy 格式。 |

### 未修复（仍然存在）

| 问题 | 状态 | 优先级建议 |
| --- | --- | --- |
| **segment 全量反序列化到内存 map** | 未修复 | P1 |
| **postings 没有压缩** | 未修复 | P1 |
| **prefix 查询 O(N) 遍历 map** | 未修复 | P2 |
| **sidecar 没有显式元数据** | 未修复，06 文档已标注 | P1 |
| **旧 sidecar 没有 GC** | 未修复，06 文档已标注 | P1 |
| **native 查询复用 v1 打分框架** | 未修复 | P2 |
| **sidecar 串行读取** | 未修复 | P2 |
| **tombstone 逐行检查** | 未修复 | P2 |
| **fallback 判断过于保守** | 未修复 | P1 |
| **merge 时重新切词** | 未修复 | P2 |

---

## 2. 新发现的问题

### 2.1 merge 状态机有一个语义漏洞

```go
func (task *mergeObjectsTask) OnBlockWritten(bat *batch.Batch, _ uint16) error {
    if task.isTombstone || task.nativeIndexingClosed || task.nativeObjectFailed ||
        task.nativeIndexer == nil || task.nativeIndexer.Empty() {
        return nil
    }
    if err := task.nativeIndexer.AddBatch(bat, []uint32{uint32(bat.RowCount())}); err != nil {
        task.nativeObjectFailed = true
        task.logNativeSidecarError("merge-add", err)
    }
    return nil
}
```

当 `nativeObjectFailed = true` 后，后续同一个 output object 的 `OnBlockWritten` 调用会被跳过（因为检查了 `nativeObjectFailed`），这是对的。但 `OnObjectSynced` 里：

```go
func (task *mergeObjectsTask) OnObjectSynced(ctx context.Context, stats *objectio.ObjectStats) error {
    defer task.resetNativeIndexer()
    if task.isTombstone || task.nativeIndexingClosed || task.nativeObjectFailed || ...
```

`resetNativeIndexer` 会把 `nativeObjectFailed` 重置为 false。这意味着如果一个 merge task 输出多个 object，第一个 object 的 sidecar 构建失败后，第二个 object 会重新尝试。这个行为本身是合理的（per-object 隔离），但有一个边界情况：

如果 `AddBatch` 失败的原因是 schema 解析问题或 tokenizer 初始化问题（而不是数据问题），那么 `resetNativeIndexer` 会重新调用 `NewObjectIndexer`，如果这次也失败，就会设置 `nativeIndexingClosed = true`，后续所有 object 都跳过。这个逻辑是对的。

但如果 `AddBatch` 失败的原因是某个特定行的数据问题（比如 JSON 解析失败），那么 `nativeObjectFailed` 只影响当前 object，下一个 object 会重新尝试。这也是合理的。

**结论**：状态机设计是正确的，没有漏洞。之前的担心是多余的。

### 2.2 `rebuildStats` 对 V1 sidecar 的重建有重复计数风险

```go
func (s *Segment) rebuildStats() {
    docLens := make(map[string]int32)
    for _, postings := range s.Terms {
        for _, posting := range postings {
            key := encodeRowKey(rowKey{...})
            if _, ok := docLens[key]; ok {
                continue
            }
            docLens[key] = posting.DocLen
            s.DocCount++
            s.TokenSum += int64(posting.DocLen)
        }
    }
}
```

这里用 `encodeRowKey` 做去重，逻辑是对的——同一个文档在多个 term 的 postings 里出现，只计一次。但 `encodeRowKey` 用的是 `fmt.Sprintf("%d/%d/%x", ...)` 格式化，如果 PK 的 hex 表示恰好包含 `/` 字符（不可能，hex 只有 0-9a-f），或者 block/row 组合恰好和另一个文档的 PK hex 前缀冲突（极低概率），理论上可能误判。

**实际风险**：极低，因为 `%x` 编码不会产生 `/`，而且 block+row+pk 三元组本身就是唯一的。可以忽略。

### 2.3 `applyNativeSegmentStats` 没有扣除 tombstone 的文档数

```go
func applyNativeSegmentStats(u *fulltextState, s *fulltext.SearchAccum, scan *nativePreparedScan) {
    s.Nrow = scan.totalDocs
    ...
}
```

`totalDocs` 是所有 sidecar 的 `DocCount` 之和，但没有减去已经被 tombstone 删除的文档数。这意味着 BM25 的 `N`（总文档数）会偏大。

**影响**：对 BM25 打分有轻微影响——IDF 分量会略微偏高（因为分母偏大）。在删除量不大的场景下影响可以忽略。但如果表经历了大量 DELETE 后还没 merge，偏差会变大。

**建议**：短期可以接受，长期应该在 merge/compaction 后统计准确的活跃文档数。或者在查询时用 `len(candidates after tombstone filter)` 做修正。

### 2.4 V2 格式没有预留扩展字段

当前 V2 格式是：

```
[magic V2 8B] [DocCount 8B] [TokenSum 8B] [termCount 4B] [terms...]
```

如果后续需要加更多 segment 级元数据（比如 per-term DF、bloom filter、min/max term 等），就需要再升级到 V3。

**建议**：在 magic 后加一个 `headerLen uint32`，这样后续加字段只需要扩展 header 区域，不需要改 magic。这是一个很小的改动但能避免频繁升级格式版本。

---

## 3. 当前实现是否是最适合 MO 的方案？

### 3.1 先回答核心问题

**当前 per-object sidecar 的方案方向是对的，是目前最适合 MO 的实现路线。** 原因：

1. **和 MO 的 immutable object 模型天然契合**。MO 的数据一旦 flush 成 object 就不可变，sidecar 也是 per-object 不可变的。这比 hidden-table 方案（需要持续维护可变的隐藏表）更自然。

2. **和 MO 的 merge/compaction 生命周期绑定**。merge 时重建 sidecar，旧 object 不可见后 sidecar 也不参与查询。这比独立维护全文索引的 merge 策略简单得多。

3. **不侵入 MO 的核心存储格式**。sidecar 是独立文件，不需要改 object writer/reader。这降低了引入风险。

4. **fallback 机制保证了正确性**。appendable tail 和 sidecar 缺失场景都有 v1 兜底。

### 3.2 但它不是终态，有几个结构性限制

#### A. per-object sidecar 的查询扇出问题

一个表如果有 200 个 object，查询时就要读 200 个 sidecar 文件，每个都要全量反序列化。这是当前方案最大的性能瓶颈。

对比：
- Lucene 的 segment 数量通常控制在几十个以内（通过 merge policy）
- ClickHouse 的 part 数量也有 merge 控制
- 当前 MO 的 object 数量取决于 compaction 策略，可能很多

**这不是说方案错了，而是说需要配合 MO 的 compaction 策略来控制 object 数量。** 如果 compaction 能把 object 数量控制在合理范围内（比如 < 50），per-object sidecar 的查询扇出是可以接受的。

#### B. 没有跨 object 的全局 term dictionary

当前每个 sidecar 都有自己的 term map，查询时要在每个 sidecar 里分别 lookup。如果一个 term 在 200 个 sidecar 里都存在，就要做 200 次 lookup + 200 次 postings 合并。

对比 Lucene：虽然也是 per-segment 的 term dictionary，但 segment 数量少，而且有 segment 级 bloom filter 可以快速跳过不包含目标 term 的 segment。

**建议**：短期不需要全局 term dictionary，但应该加 per-sidecar 的 term bloom filter，让查询可以快速跳过不包含目标 term 的 sidecar。

#### C. appendable tail 的 gap 是用户体验的最大短板

当前只要表里有 appendable object，整个查询就 fallback 到 v1。对于持续写入的表，这意味着 native 路径几乎永远不会被命中。

这是当前方案最需要优先解决的问题。

### 3.3 和其他可能方案的对比

#### 方案 1：把 FTS 数据嵌入 object 文件（DataMetaType 扩展）

上一轮 review 建议的方案。重新评估：

**优点**：
- 生命周期完全一致
- GC 自动
- checkpoint/replay 自动

**缺点**：
- 需要改 object writer/reader，这是 MO 最核心的存储路径
- object 文件变大，影响非 FTS 查询的 I/O（即使只读数据列也要跳过 FTS 区域）
- 如果 FTS 数据很大（大文本列），object 文件膨胀严重
- 回滚难度大——一旦嵌入就很难再拆出来

**重新评估后的结论**：**不建议现阶段做这个改动。** 当前独立 sidecar 文件的方案虽然有 GC 问题，但侵入性低、回滚容易、不影响非 FTS 路径。GC 问题可以通过在 object GC 流程中加一行 sidecar 清理来解决，不需要改 object 格式。

#### 方案 2：用 MO 的隐藏表存储全局 term dictionary + postings

回到 hidden-table 思路，但不是 v1/v2-lite 那种"每个 token 一行"，而是存储压缩后的 postings block。

**优点**：
- 可以有全局 term dictionary
- 可以利用 MO 的 SQL 引擎做 term lookup

**缺点**：
- 又回到了 hidden-table 的生命周期问题
- 更新/删除维护复杂
- 和 base data 的 compaction 不同步

**结论**：不推荐。这条路已经被 v1/v2-lite 的经验证明了不适合。

#### 方案 3：当前方案 + 增量改进

保持 per-object sidecar 的基本架构，通过以下改进逐步提升：

1. sidecar 格式升级（按需加载、压缩、bloom filter）
2. sidecar GC 接入 object GC 流程
3. appendable tail in-memory delta
4. 更细粒度的 native/v1 混合查询
5. merge 时合并 sidecar 而非重新切词

**结论**：**这是当前最推荐的路线。** 方向正确，风险可控，每一步都是独立可交付的。

---

## 4. 具体改进建议（按优先级）

### P0：影响正确性或生产稳定性

#### 4.1 sidecar GC 接入 object GC 流程

当前 MO 的 GC 流程（`GCWindow.ExecuteGlobalCheckpointBasedGC`）基于 object 文件名的 bloom filter 来判断哪些文件可以删除。sidecar 文件名（`{obj}.fts.{hash}.seg`）不在这个 bloom filter 里，所以永远不会被 GC。

**方向上应该接入 GC，但结合 MO 当前实现，不能简单靠“按 object 名前缀列 sidecar”补上。**

原因：

1. `FileService.List` 是按目录列举，不是任意 prefix 扫描。
2. 当前 object 名字是 flat namespace（`segment_uuid_num`），不是按 object 分目录。
3. 如果对每个待删 object 都去列根目录再过滤 `{obj}.fts.*.seg`，在 object 很多时会非常低效。

因此，下面这个“最小改动伪代码”只说明 **GC 需要把 sidecar 一起删** 这个方向，不应直接按字面实现。

```go
// 伪代码：在 GC 收集 filesToGC 时
for file := range filesToGCSet {
    filesToGC = append(filesToGC, file)
    // 同时清理可能存在的 FTS sidecar
    for _, sidecarSuffix := range ftnative.KnownSidecarSuffixes(file) {
        filesToGC = append(filesToGC, sidecarSuffix)
    }
}
```

**更贴合 MO 的实现路线** 有两个：

1. **显式 locator / replay / GC 元数据**：让 GC 直接知道 object 对应哪些 sidecar，而不是运行时去扫文件服务。
2. **后续如果调整 sidecar 布局**，把 sidecar 放进可按 object 局部枚举的分层路径，再考虑 prefix-based 清理。

结论：**sidecar GC 仍然是 P0/P1 级重要问题，但更推荐和 locator 元数据一起做。**

#### 4.2 segment 格式加 header length 预留扩展

在 V2 magic 后加一个 `uint32 headerLen`，后续加字段不需要改 magic。改动量极小。

### P1：影响性能但不影响正确性

#### 4.3 segment 格式支持按需加载

当前格式是线性的：`[header] [term1 postings] [term2 postings] ...`。要支持按需加载，需要在头部加一个 term index：

```
[header]
[term_count uint32]
[term_index: (term_hash uint64, offset uint32, length uint32) × term_count]
[postings_data...]
```

查询时：
1. 先读 header + term_index（固定大小，可以用一次 I/O）
2. 根据 term_hash 找到目标 term 的 offset
3. 只读目标 term 的 postings block

这样即使 sidecar 很大，查询一个 term 也只需要两次 I/O。

#### 4.4 更细粒度的 native/v1 混合查询

当前 `prepareNativeScan` 的逻辑是"全有或全无"——所有 object 都有 sidecar 才走 native，否则整体 fallback。

改进方案：

```go
// 伪代码
nativeObjects := []nativeObjectSegment{}
fallbackNeeded := false
for _, stat := range stats {
    seg, exists, err := ReadSidecar(...)
    if exists {
        nativeObjects = append(nativeObjects, ...)
    } else {
        fallbackNeeded = true
    }
}
if hasAppendableObjects {
    fallbackNeeded = true
}

if !fallbackNeeded {
    // 纯 native 路径（当前已有）
    return nativeScan(nativeObjects)
} else if len(nativeObjects) > 0 {
    // 混合路径：native 处理有 sidecar 的 object，v1 处理剩余
    nativeResults := nativeScan(nativeObjects)
    v1Results := v1Scan(remainingObjects)
    return merge(nativeResults, v1Results)
} else {
    // 纯 v1 路径（当前已有）
    return v1Scan(allObjects)
}
```

这样即使有少量 object 缺 sidecar 或有 appendable tail，大部分数据仍然走 native。

#### 4.5 postings 压缩

最简单的改进：对 positions 数组做 delta encoding + varint。

```go
// 当前：每个 position 固定 4 字节
// 改进后：delta + varint，平均每个 position 1-2 字节
func encodePositions(positions []int32) []byte {
    var buf bytes.Buffer
    prev := int32(0)
    for _, pos := range positions {
        delta := pos - prev
        writeVarint(&buf, delta)
        prev = pos
    }
    return buf.Bytes()
}
```

对于高频 term，positions 数组可能很长，压缩率可以达到 50-70%。

### P2：中期改进

#### 4.6 appendable tail in-memory delta

这是消除 v1 fallback 最关键的一步。具体方案：

1. 在 `txnTable` 或 CN 侧维护一个 per-table 的 `*native.Builder`
2. 当数据写入 appendable object 时，同时调用 `builder.Add()`
3. 查询时，`prepareNativeScan` 除了读 persisted sidecar，还合并 in-memory builder 的结果
4. flush 时，in-memory builder 的内容随 object 一起持久化为 sidecar，然后清空 builder

关键难点：
- 多 CN 场景下，每个 CN 只能看到自己写入的 appendable 数据
- 需要和 MO 的 workspace / txn 机制对齐
- 内存管理——builder 不能无限增长

**建议先在单 CN 场景下验证可行性。**

#### 4.7 merge 时合并 sidecar 而非重新切词

当前 merge 时对每个输出 block 重新切词。如果能直接合并已有 sidecar 的 postings，只需要重新映射 block/row 引用，CPU 开销会大幅降低。

实现思路：

```go
func MergeSegments(segments []*Segment, blockMapping []BlockMapping) *Segment {
    merged := &Segment{Terms: make(map[string][]Posting)}
    for i, seg := range segments {
        for term, postings := range seg.Terms {
            for _, posting := range postings {
                newRef := blockMapping[i].Remap(posting.Ref)
                merged.Terms[term] = append(merged.Terms[term], Posting{
                    Ref: newRef,
                    DocLen: posting.DocLen,
                    Positions: posting.Positions,
                })
            }
        }
    }
    return merged
}
```

难点在于 merge 时 block/row 的重新映射——需要知道旧 object 的每一行在新 object 中的位置。当前 `mergesort` 框架是否提供这个映射需要进一步确认。

#### 4.8 per-sidecar term bloom filter

在 sidecar 头部加一个 bloom filter，记录该 sidecar 包含哪些 term。查询时先检查 bloom filter，如果目标 term 不在 bloom filter 里，直接跳过该 sidecar，不需要反序列化。

对于 boolean AND 查询特别有效——如果某个 sidecar 不包含所有必须 term，可以直接跳过。

---

## 5. 对"是否最适合 MO"的最终回答

### 5.1 方向判断

**当前 per-object sidecar + v1 fallback 的方案是目前最适合 MO 的实现路线。**

理由：
1. 和 MO 的 immutable object 模型天然契合
2. 侵入性低，不改核心存储路径
3. fallback 保证正确性
4. 每一步改进都是独立可交付的
5. 回滚容易——最坏情况下去掉 sidecar 生成，全量走 v1

### 5.2 不建议做的事

1. **不建议现阶段把 sidecar 嵌入 object 文件格式**——侵入性太大，收益不够明显
2. **不建议回到 hidden-table 路线**——已经被 v1/v2-lite 证明不适合
3. **不建议引入独立的 FTS 存储引擎**——复杂度太高，和 MO 的存储体系脱节
4. **不建议现阶段追求完美的 analyzer 体系**——先把核心链路跑稳

### 5.3 最应该优先做的三件事

1. **sidecar GC**（4.1）——不做的话长期运行会积累大量孤儿文件
2. **更细粒度的 native/v1 混合查询**（4.4）——大幅减少不必要的 fallback
3. **segment 格式支持按需加载**（4.3）——大 object 场景的性能关键

这三件事做完后，native FTS 就从"能跑通"变成"能跑稳"了。

---

## 6. 为什么 fulltext 需要改 TAE 的 flush / merge / GC

这是一个经常被问到的问题，值得专门说清楚。

### 6.1 根本原因

当前方案选择了 **per-object sidecar** 路线——每个 MO data object 旁边挂一个全文索引文件。这个选择决定了全文索引的生命周期必须和 object 的生命周期绑定，而 object 的生命周期恰恰是由 TAE 的 flush / merge / GC 控制的。

### 6.2 为什么要改 flush

MO 的数据先写到内存的 appendable object，flush 时才持久化成 immutable object 文件。全文索引的 sidecar 必须在这个时机生成——因为只有 flush 的时候才知道这个 object 最终包含哪些行、哪些文本。

`flushobj.go` 的改动：flush 完 object → 对这个 object 的文本列切词 → 生成 sidecar 文件。

### 6.3 为什么要改 merge

MO 的 compaction 会把多个小 object 合并成大 object。合并后旧 object 消失，新 object 产生。旧 object 的 sidecar 就作废了，新 object 需要新的 sidecar。

`mergeobjects.go` 的改动：merge 输出新 object 时 → 对新 object 的数据重新切词 → 生成新 sidecar。

### 6.4 为什么要改 GC

旧 object 被 merge 后变成不可见，MO 的 GC 会清理旧 object 文件。但 GC 只认识 object 文件，不认识旁边的 sidecar 文件。如果不改 GC，sidecar 就会变成孤儿文件永远留在存储里。

### 6.5 为什么不能不改这些

之前的 v1 方案就是不改这些——它用隐藏表存全文索引，完全走 SQL 层维护。但那个方案的问题是：

- 隐藏表和 base data 的生命周期是脱节的
- 更新/删除要额外拼 SQL 去维护隐藏表
- 没有 merge/compaction，索引结构会越来越碎
- 长期运行后稳定性差

选择 per-object sidecar 就是为了让全文索引不再是一个独立维护的外挂结构，而是跟着 object 一起生、一起合并、一起清理。代价就是要在 TAE 的 flush / merge / GC 三个关键生命周期节点上加钩子。

---

## 7. 改动风险分析

### 7.1 flush 改动 — 风险极低

```go
// flushobj.go，在 writer.Sync 之后、返回之前
if !task.meta.IsTombstone {
    indexer, idxErr := ftnative.NewObjectIndexer(task.meta.GetSchema())
    if idxErr != nil {
        task.logNativeSidecarError("flush-init", idxErr)
    } else if !indexer.Empty() {
        // ... AddBatch + Write，失败都只 log 不 return
    }
}
```

关键安全保证：

- 在 `writer.Sync` **之后**才执行，object 数据已经完整落盘
- 任何一步失败都只记日志，不 return error，**不影响 flush 主流程**
- `NewObjectIndexer` 如果表没有 FULLTEXT 索引，`Empty()` 返回 true，直接跳过——**对没有全文索引的表零影响**
- tombstone object 直接跳过

最坏情况：sidecar 生成失败，查询 fallback 到 v1，数据不受影响。

### 7.2 merge 改动 — 风险极低

改动稍多但设计更谨慎，加了两级状态机：

- `nativeObjectFailed`：当前 output object 的 sidecar 构建失败，跳过这一个 object 的 sidecar，下一个 object 重新尝试
- `nativeIndexingClosed`：schema 级别的问题（比如 `NewObjectIndexer` 失败），永久关闭整个 merge task 的 indexing

```go
func (task *mergeObjectsTask) OnBlockWritten(bat *batch.Batch, _ uint16) error {
    if task.isTombstone || task.nativeIndexingClosed || task.nativeObjectFailed || ... {
        return nil  // 永远 return nil，不影响 merge
    }
    if err := task.nativeIndexer.AddBatch(...); err != nil {
        task.nativeObjectFailed = true
        task.logNativeSidecarError(...)
    }
    return nil  // 即使失败也 return nil
}
```

关键安全保证：

- `OnBlockWritten` 和 `OnObjectSynced` **永远 return nil**，不可能导致 merge 失败
- 没有 FULLTEXT 索引的表，所有回调直接跳过
- 状态机保证：单 object 数据问题不扩散，schema 问题快速熔断

最坏情况：sidecar 没生成，查询 fallback 到 v1。

### 7.3 GC 改动 — 尚未实现

当前 sidecar 文件不会被 GC 清理，会慢慢积累孤儿文件。但：

- 不影响正确性（查询只枚举 visible object 的 sidecar）
- 不影响性能（孤儿文件不会被读取）
- 只影响存储空间

后续实现时，如果只是在 GC 输出删除列表时追加 sidecar 文件名，风险也很低。

### 7.4 查询侧 — 风险低

```go
if u.param.UseNative() {
    used, err := fulltextIndexMatchNative(...)
    if used { return nil }
}
// 走到这里就是 v1 路径，和之前完全一样
```

- `UseNative()` 只有显式标记为 native 的索引才会尝试 native 路径
- native 路径内部，任何条件不满足都 return `false`，自动 fallback
- v1 路径完全没改

对现有 v1 用户零影响。native 路径是纯增量的 opt-in。

### 7.5 风险总结

| 改动点 | 风险 | 把握 | 最坏情况 |
|--------|------|------|----------|
| flush 加 sidecar 生成 | 极低 | 很高 | sidecar 没生成，查询走 v1 |
| merge 加 sidecar 重建 | 极低 | 很高 | sidecar 没生成，查询走 v1 |
| GC 加 sidecar 清理 | 未做 | — | 孤儿文件积累，不影响正确性 |
| 查询加 native 分支 | 低 | 高 | native 条件不满足，走 v1 |

整体设计的核心安全网是：**所有 sidecar 相关的失败都是静默降级，不是硬失败。** 数据写入路径（flush / merge）不会因为 sidecar 问题中断，查询路径不会因为 sidecar 缺失报错。最坏情况就是退回到 v1 的行为，和没有这些改动之前一样。

把握很高的原因不是因为代码简单，而是因为**降级策略设计得很干净**——sidecar 是一个纯粹的加速层，不是正确性依赖。

---

## 8. 总结

上一轮 review 提出的两个 P0 问题（sidecar 失败阻塞 flush/merge、全局统计持久化）都已经正确修复，修复质量很好——flush 和 merge 都做了降级处理，V2 格式做了向后兼容，测试覆盖了 legacy 格式。

当前实现的方向是对的，per-object sidecar 是目前最适合 MO 的 native FTS 实现路线。它不是终态，但它是一个正确的起点，后续的每一步改进都可以在这个基础上独立推进。

对 TAE flush / merge / GC 的改动风险极低——所有改动都在主流程之后追加，失败只降级不阻塞，对没有全文索引的表零影响。这是因为 sidecar 被设计成纯加速层而非正确性依赖。

最关键的下一步不是追求更复杂的架构，而是把当前方案的几个实际短板补上：sidecar GC、混合查询、按需加载。这三件事做完，native FTS 就可以在生产环境中稳定运行了。
