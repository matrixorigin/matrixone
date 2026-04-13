# MatrixOne 全文路线选择、实现难度与我的把握

## 1. 修正后的总体结论

前面的 `03-mo-fulltext-v2-plan.md` 给的是**架构终态**，方向本身没有问题。  
但如果把下面三个现实因素一起考虑进去：

- **实现难度**
- **交付风险**
- **下一批用户可能很快带来更大数据量**

那么更合适的建议不是“直接上完整的 engine-native FTS v2”，也不是把**当前这版 hidden-table FTS v2-lite**继续扩成长期主线，而是：

1. **短期止血**：当前 v1 只做必要修正，不作为大规模方案承诺。
2. **过渡桥接**：当前 FTS v2-lite 继续保留，但定位成**低更新 workload 的过渡实现**，主要用于验证 SQL 语义、元数据、查询执行和 DML 维护闭环。
3. **中期主线**：尽快转向 **storage-coupled native FTS**，让全文 segment、delete bitmap 和 merge 生命周期跟随 MO 的 base object / part / compaction 流程，而不是长期依赖独立隐藏表 + post-DML SQL 维护。
4. **长期演进**：在 storage-coupled native FTS 已经跑稳后，再决定是否继续下沉到更专用的 FTS 存储/编码层。

换句话说：

- **03 文档里的终态方向是对的**
- **但第一步不应该走得那么重**

---

## 2. 三条可选路线

| 路线 | 方案描述 | 实现难度 | 交付风险 | 我当前的把握 | 对大数据的适配性 | 结论 |
| --- | --- | --- | --- | --- | --- | --- |
| 路线 A | 在当前 v1 上继续修补：缓存统计、BM25 默认、异步 build、少量 optimizer 改进 | 低到中 | 低 | 高 | 差 | 只能止血，不能作为大数据方案 |
| 路线 B | **当前 FTS v2-lite**：隐藏 `docs/segment/delta` 表 + post-DML 维护，优先解决兼容性和闭环问题 | 中到高 | 中 | 中高 | 中 | 适合作为桥接实现，不适合作为长期主线 |
| 路线 C | **storage-coupled native FTS（修正后主推）**：全文 segment 与 base object / part 生命周期绑定，更新删除走 tombstone / bitmap，查询走专用 FTS scan | 高 | 中到高 | 中高 | 很好 | **更稳、更好维护、也更适合日更的内部主线** |

---

## 3. 我对这些判断的把握

## 3.1 把握高的部分

### A. 当前 v1 不适合承接更大数据量

这点我的把握是**高**。

原因很直接，当前实现的几个核心问题都已经在代码里坐实：

- 逐 token 行式隐藏表
- 同步回填
- 同步 delete + re-tokenize + insert
- 查询时动态 SQL + 内存聚合 + 运行时统计

这些问题不是“参数调优”能解决的。

### B. 仅在 v1 上打补丁，不足以解决大数据问题

这点我的把握也是**高**。

即使做了：

- BM25 默认化
- 统计缓存
- explain 增强
- analyzer 稍微加强

它仍然没有改变最根本的结构问题：

- 不是段式倒排
- 没有 refresh/merge
- 没有真正的 posting 级执行

所以最多是止血，不是翻盘。

## 3.2 把握中高的部分

### A. Lucene/ES 的 `segment + tombstone + merge` 本身不是“原始方案”

这点我的把握是**高**。

很多人对 ES 的负面体验，主要来自：

- 独立搜索集群的运维复杂度
- 分布式副本、rebalance、mapping 演进、容量治理
- Java heap / GC / cache / shard 运维细节

但如果只看全文索引内核，Lucene 这套：

- immutable segment
- delete/tombstone
- background merge
- postings/positions

恰恰是**长期稳定性和可恢复性很强**的一条工业路线，并不“原始”。

### B. 当前 FTS v2-lite 更适合作为桥接第一阶段

这点我的把握是**中高**。

原因是它在三件事之间平衡得还不错：

1. **比 v1 强很多**
2. **比完整 engine-native FTS 更容易落地**
3. **还能保留数据库内一体化体验**

但这版实现的几个现实问题也已经很清楚：

- 更新/删除仍然有明显的同步维护尾巴
- 依赖隐藏表与 post-DML SQL，运维模型不够“原生”
- 还没有把 segment 生命周期真正挂到存储层 compaction/merge 上

所以它更适合当**桥接方案**，不适合当长期主线。

### C. 对“下一个更大数据用户”，更稳妥的是 storage-coupled native FTS

这点我的把握也是**中高**。

如果业务上很快就会遇到：

- 更大的文档量
- 更频繁的更新
- 更严格的查询 SLA

那么在只能内部实现的前提下，**让全文 segment 与 base object / part 的 flush、checkpoint、compaction 生命周期绑定**，会比“长期维护独立隐藏全文表”更稳妥。

这条路线本质上是：

1. **检索内核借 Lucene**
   - segment
   - postings / positions
   - tombstone
   - merge
   - BM25 / top-k

2. **存储生命周期借 ClickHouse / LSM 风格**
   - part-local / object-local 辅助索引
   - immutable data part + background merge
   - append new version + async compaction

3. **系统集成保持数据库原生**
   - SQL 语法
   - 事务/快照
   - explain / optimizer
   - 与向量检索解耦

### D. 如果目标 workload 是“约 500 次请求/分钟、更新较少、且要与向量共存”，我的把握会进一步提高

这点我的把握可以从**中高**再往上走一些。

原因是这组约束会明显降低第一阶段的工程压力：

1. **查询压力不算高。**  
   如果按 500 次/分钟理解，总体量级大约是 8 QPS 出头，这不是“极高并发搜索平台”的问题，更像是“中等吞吐下要把 1TB 级文本做对”的问题。

2. **更新压力不大。**  
在更新较少的前提下，refresh、merge、tombstone、bulk catch-up 都可以保守设计，不需要一开始就追求很激进的 near-real-time 写入路径。

3. **可以把重点放在读路径而不是写路径。**  
   第一阶段真正要啃下来的核心，是 postings/positions、segment metadata、BM25、top-k 和回表，而不是高频 UPDATE/DELETE 下的极限写放大控制。

4. **可以更稳地和向量检索共存。**  
   在这个 workload 下，没有必要把全文和向量揉成一个全新大系统。更合理的做法是：
   - 向量查询继续走现有向量索引链路
   - 全文查询走独立的 FTS 索引链路
   - 只有在显式混合检索时，才在上层做候选集合并与重排

所以如果目标画像是：

- 数据量可能到 1TB
- 查询量中等
- 更新较少
- 希望与向量能力一起使用

那我对 **internal `FTS v2-lite` 作为第一阶段主线** 的把握会比“面向高并发、高更新搜索平台”这个假设下更高。

## 3.3 把握相对低一些的部分

### A. 精确的性能阈值

这点我的把握**不高**，因为这次是代码级分析，不是基于目标业务语料和硬件环境做的实测。

也就是说，我可以高把握地说：

- 当前方案在更大数据下会出问题

但我不应该在没有基准测试的情况下武断给出：

- “到多少文档一定不行”
- “延迟一定会是多少”

### B. 完全专用 engine-native FTS 的第一阶段交付可控性

我对它作为**终态方向**是认可的，但对它作为**第一阶段交付方案**的把握明显低于 v2-lite。

原因不是方向错，而是它会同时撬动：

- 存储
- 索引生命周期
- 后台任务
- 查询执行器
- 元数据
- 运维接口

第一步就这样做，项目风险会偏高。

---

## 4. 为什么我现在更推荐 FTS v2-lite

## 4.1 它本质上是什么

FTS v2-lite 不是“继续修 v1”，也不是“一步到位重写整个引擎”。  
它更像是：

- 在 MO 内做一个 **LSM/segment 风格的倒排索引层**
- 但优先复用现有基础设施

可以把它理解成：

```text
隐藏系统元数据
  + 隐藏 delta 表
  + 隐藏 segment 对象/表
  + analyzer
  + refresh
  + merge
  + tombstone
  + 专用 FTS scan executor
```

## 4.2 它为什么更适合作为第一步

### A. 对现有系统侵入更可控

它可以复用：

- 现有元数据体系
- 现有对象存储/隐藏表能力
- 现有后台任务体系
- 现有 SQL 语法与 optimizer 接口

这意味着第一阶段不必发明一个全新的独立存储子引擎。

### B. 已经足以跨过当前最大瓶颈

只要从“逐 token 行”升级到“segment + postings”，就已经可以解决当前最核心的问题：

- 写入放大
- 查询时大候选聚合
- 统计运行时扫描
- 无 merge 的长期退化

### C. 仍能继续向终态演进

如果后续 native FTS 被证明价值很大，v2-lite 的很多资产仍然能复用：

- analyzer
- query parser
- refresh/merge 框架
- postings/positions 编码
- metadata 设计

所以它不是死路，而是更低风险的第一阶段。

---

## 5. 为什么不建议直接把“完整 engine-native FTS”作为第一步

## 5.1 不是因为方向不好

而是因为第一阶段这样做，往往会同时踩中两类风险：

### A. 范围过大

第一阶段就要同时交付：

- 新存储表示
- 新 segment 生命周期
- 新 scan/executor
- 新 merge 机制
- 新运维接口
- 新 analyzer 体系

### B. 很难快速兜住大用户

如果下一个用户很快就会上较大数据，那么最怕的是：

- native 大工程还没落地
- v1 又顶不住

这时产品就会处在一个很尴尬的窗口期。

---

## 6. 如果下一批用户很快要上更大数据，且必须内部实现，应该怎么选

## 6.1 我给的实际建议

### 短期

**不要把当前 v1 当成大规模方案承诺出去。**

建议：

1. 明确 current fulltext 的适用边界
2. 当前 v2-lite 只当桥接，不把它包装成长期终态
3. 先把最危险的写路径问题收掉：停止按 `doc_id` 直接清理大段 posting，优先转向 tombstone / delete bitmap
4. 在 native 侧先做止血改进，但不把它当最终答案

### 中期

**把主要研发资源放到 storage-coupled native FTS，而不是继续把 hidden-table v2-lite 越做越重。**

重点优先级建议是：

1. 让全文 segment 跟随 base object / part 生命周期
2. delete bitmap / tombstone + version-aware compaction
3. async build / catch-up / daily refresh orchestration
4. dedicated FTS scan
5. cached stats + BM25 default
6. analyzer framework

### 长期

等 storage-coupled native FTS 跑稳后，再决定是否继续投入到：

- 更深的专用 FTS 编码/压缩层
- 更复杂的 hybrid retrieval / rerank 能力

## 6.2 一个很现实的判断

如果业务上真的“很快就会来一个大数据用户”，那就更应该：

1. **明确放弃继续把 v1 当大规模方案扩展**
2. **把当前 v2-lite 当桥接，同时尽快切 storage-coupled native FTS 主线**
3. **严格控制首批功能范围，只先交付最必要的检索闭环**

这样才不会因为第一阶段范围过大，导致内部方案长期落不下来。

## 6.3 如果还要求“结合向量，但不要影响当前向量查询”

我建议把这条要求作为**明确设计边界**写死：

1. **向量单查路径不动。**  
   现有 HNSW / IVF 的索引构建、planner rewrite、search table function 和回表链路保持原样，不为了接全文而重写。

2. **全文单查路径独立建设。**  
   FTS v2-lite 自己有 metadata、segment、scan executor、merge 和统计体系，不复用向量索引存储格式。

3. **混合检索只做“上层融合”，不做“底层强耦合”。**  
   第一阶段只建议两种模式：
   - **FTS first, vector rerank**：全文先召回一批候选，再用向量排序/重排
   - **dual retrieval + fusion**：全文 top-N 和向量 top-N 分别召回，最后按 RRF 或加权分数融合

4. **资源隔离。**  
   FTS 的 cache、merge 任务、内存预算、后台 build 预算都要与向量索引隔离，避免全文 merge 抢占向量查询资源。

这条边界非常重要，因为它意味着：

- 当前向量能力可以继续稳定演进
- 全文能力可以独立交付
- 混合检索可以作为增量能力接上去

这样做的把握，明显高于“第一阶段就把全文和向量做成一个统一底层索引引擎”。

---

## 7. 不建议做的事

1. **不建议继续把 v1 当成“再优化一点就能撑大数据”的方案。**
2. **不建议在大用户逼近时，第一步直接开做完整 engine-native FTS。**
3. **不建议现在就承诺复杂 analyzer / query expansion / 高级 search UX 全部一起交付。**

---

## 8. 最终推荐

**最终推荐路线：**

1. **当前 v1**：只做止血，不做承诺。
2. **当前 v2-lite**：保留，但明确只作为桥接实现；它适合低更新、可控日更窗口，不适合作为长期主线。
3. **internal 主线**：优先做 **storage-coupled native FTS**，让 segment / tombstone / merge 直接挂到 MO 的 object / part / compaction 生命周期上；这条路更稳、更易维护，也更适合日更。
4. **长期终态**：再评估是否继续下沉到更专用的 FTS 存储/编码层。

## 9. 一句话版本

**如果下一个用户可能会上较大数据，而且必须内部实现，我不建议把赌注押在当前 v1，也不建议把现在这版 hidden-table v2-lite 当长期答案；更合适的是把它当桥接，同时尽快转到 storage-coupled native FTS。**
