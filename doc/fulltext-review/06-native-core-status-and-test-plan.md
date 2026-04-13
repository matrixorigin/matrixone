# MatrixOne native FTS 当前落地状态与测试方案

## 1. 当前代码状态

当前仓库里的全文实现已经进入 **native sidecar + v1 fallback** 的混合阶段：

1. **保留现有 FULLTEXT v1 hidden-table 路径**，作为正确性兜底与兼容路径。
2. **hidden-table `FTS v2-lite` 已从主代码路径移除**，不再继续作为主线。
3. **native FTS kernel 已落地**：
   - `pkg/fulltext/tokenize.go`
     - 抽出统一 document tokenization helper；
     - `default / ngram / json / json_value` 统一复用；
     - `fulltext_index_tokenize` 与 native build 共享语义。
   - `pkg/fulltext/native/segment.go`
     - `term -> postings(block,row,pk,positions,doc_len)`；
     - term lookup / prefix scan 组合、AND、phrase offset match；
     - deterministic sidecar path；
     - binary codec。
4. **storage-coupled sidecar 生成已经接到 object 生命周期**：
   - flush object 后生成 per-object native sidecar；
   - merge / compaction 输出新 object 时同步重建 sidecar。
   - sidecar 构建失败只记日志并跳过 sidecar，不阻塞 flush / merge 主流程。
5. **`MATCH ... AGAINST` 已接入 native 查询分支**：
   - 当当前快照下所有 visible object 都是 non-appendable，且 sidecar 覆盖完整时，走 native sidecar；
   - 否则显式回退到 v1 hidden-table 路径。
   - native 查询直接汇总 sidecar 中的 `DocCount` / `TokenSum`，不再额外执行 `runCountStar()`。
6. **native 查询已经接 tombstone 过滤**：
   - native postings 命中的 `(block,row)` 会结合 `CollectTombstones()` 做删除可见性过滤。

这意味着当前不是“只有内核，没有用户路径”，而是已经有 **steady-state native 查询路径**，只是 appendable tail 仍然通过 fallback 保证正确性。

## 2. 当前边界：哪些已经完成，哪些还没有

### 2.1 已完成

1. v2-lite bridge 从 DDL / compile / planner / post-DML / executor 主路径移除。
2. tokenization 语义统一，v1 与 native 不再各自维护一份。
3. native postings / positions / codec / phrase kernel 已完成。
4. flush sidecar 生成已接入。
5. merge / compaction sidecar 重建已接入。
6. native query 已接回 `fulltext_index_scan`。
7. native query 已接 visible object 枚举与 tombstone 过滤。
8. native query 对 sidecar 覆盖不全、appendable object 存在、或当前模式不支持时，会明确回退 v1。
9. flush / merge 的 sidecar 生成失败不再放大成主数据写入失败。
10. sidecar 已持久化 indexed doc count / token sum，native 查询不再先跑全局 count。

### 2.2 仍未完成

1. **appendable tail native delta**  
   还没有单独实现 native tail/delta；当前依赖 “覆盖不完整则回退 v1”。
2. **显式 locator 元数据**  
   sidecar 仍然通过 deterministic path 定位，没有扩展 catalog/checkpoint 显式记录 locator。
3. **query mode 覆盖仍是分阶段的**  
   当前 native 查询主要覆盖 steady-state 的 NL / phrase / boolean persisted-object 路径；不适合的模式会回退 v1。
4. **datalink 列不在 storage path 直接取外部文本**  
   这类 FULLTEXT index 当前不产 native sidecar，查询会回退 v1。
5. **sidecar 生命周期的显式 GC / replay 元数据**  
   旧 object sidecar 因为对象不可见而不会参与查询，但还没有独立的 sidecar 元数据治理机制。

## 3. 当前实现的核心取舍

这轮实现没有冒进到“强行让所有查询全量切 native”，而是采取了更稳的策略：

1. **persisted steady-state 走 native**
2. **appendable / sidecar 缺失 / 不支持模式 走 v1 fallback**
3. **flush / merge 期间 sidecar 与 object 同生命周期生成**
4. **查询时只从当前 visible object 枚举 sidecar，不依赖额外桥接表**
5. **sidecar 失败降级为“少一个 native sidecar”，而不是“整个 flush / merge 失败”**

这样做的好处是：

1. 不会因为 appendable tail 尚未 native 化就牺牲正确性。
2. 不会把 flush / merge 路径绑到外部模块或外部引擎。
3. merge 后旧 object sidecar 即使还在文件服务里，也不会再被查询枚举到。
4. vector-only 路径没有被接入或改写，不影响现有向量查询链路。
5. native steady-state 查询少了一次额外的全局 count 开销。

## 4. 当前可执行测试

可以直接跑下面脚本：

```bash
./scripts/test_fulltext_native.sh
```

当前脚本覆盖的包级回归：

1. `./pkg/fulltext`
2. `./pkg/fulltext/native`
3. `./pkg/sql/colexec/table_function`
4. `./pkg/sql/colexec/postdml`
5. `./pkg/sql/plan`
6. `./pkg/sql/compile`
7. `./pkg/vm/engine/tae/mergesort`
8. `./pkg/vm/engine/disttae`

另外已经补了一个 native sidecar unit test：

1. `pkg/fulltext/native/object_test.go`
   - 构造带 FULLTEXT constraint 的 schema；
   - 用 `ObjectIndexer` 生成 sidecar；
   - 从 file service 读回 segment，验证 term lookup 结果。

## 5. 建议的系统级测试方案

### 5.1 基础正确性

1. **tokenization parity**
   - `default / ngram / json / json_value`
   - native 与 `fulltext_index_tokenize` 一致
2. **segment / sidecar codec**
   - 单文档
   - 多文档
   - prefix / repeated term
   - phrase positions

### 5.2 object 生命周期

1. **flush sidecar**
   - 写入并 flush 后 sidecar 文件存在
   - sidecar term/postings 与 object 数据一致
2. **merge / compaction sidecar**
   - merge 生成的新 object 有 sidecar
   - 查询只枚举新的 visible object
   - 被 merge 掉的旧 object sidecar 即使存在也不再参与结果

### 5.3 查询正确性

1. **NL / term**
2. **boolean**
3. **phrase**
4. **top-k / score**
5. **sidecar 覆盖完整时 native 命中**
6. **appendable object 或缺 sidecar 时自动回退 v1**

### 5.4 删除与可见性

1. **tombstone 过滤**
   - delete 后 native 命中立即不可见
2. **update 场景**
   - 旧版本 row 被 tombstone 过滤
   - 新版本在 flush 前通过 fallback 保持正确
   - flush / merge 后重新回到 native steady-state

### 5.5 稳定性与回归

1. **restart / replay**
   - deterministic path 下 sidecar 可重新读取
2. **vector regression**
   - vector-only 查询路径无回归
3. **datalink fallback**
   - datalink FULLTEXT index 不生成 native sidecar
   - 查询仍能通过 v1 返回正确结果

## 6. 现阶段结论

当前仓库已经不再沿着 hidden-table v2-lite 往前推，而是进入了 **native sidecar 为主、v1 为 fallback 的可运行阶段**。  
对用户语义来说，steady-state persisted 数据已经可以走 storage-coupled native 路线；对系统正确性来说，appendable tail、datalink、以及 sidecar 覆盖不完整的情况仍然由 v1 路径兜底。

下一阶段如果继续推进，优先级应该是：

1. native tail/delta
2. sidecar locator / replay / GC 元数据
3. 扩大 native query mode 覆盖面
