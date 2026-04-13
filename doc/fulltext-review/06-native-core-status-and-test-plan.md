# MatrixOne native FTS 当前代码状态与测试方案

## 1. 当前代码状态

当前仓库里的全文实现状态已经调整为：

1. **保留现有 FULLTEXT v1 路径**，保证现有功能不被这轮 native 重构直接打断。
2. **移除 hidden-table `FTS v2-lite` 的主代码路径**，不再继续把它作为主线推进。
3. **新增 native FTS core foundation**，为后续 storage-coupled 路线提供统一内核：
   - `pkg/fulltext/tokenize.go`
     - 抽出 document tokenization helper；
     - `default / ngram / json / json_value` 统一走一套可复用逻辑；
     - `fulltext_index_tokenize` 已复用这套 helper，避免 v1 与 native 语义漂移。
   - `pkg/fulltext/native/segment.go`
     - 建模 `term -> postings(block,row,pk,positions,doc_len)`；
     - 支持 term lookup、AND 交集、phrase offset 匹配；
     - 支持 native sidecar segment 二进制编解码；
     - 提供 deterministic sidecar path helper。

这意味着：

- **已经有 native postings/positions 内核**
- **还没有完整 storage-coupled 用户路径**

## 2. 已完成与未完成边界

### 2.1 已完成

1. v2-lite bridge 从 DDL / compile / planner / post-DML / executor 主路径移除。
2. tokenization 语义抽象成通用 helper，可被 v1 与 native 复用。
3. native segment builder 可以从 document values 构造 postings / positions。
4. phrase 所需的 offset matching 已经能在 native kernel 层工作。
5. sidecar segment 已能做 deterministic path + binary codec。

### 2.2 还没完成

1. **flush / merge sidecar 生成**  
   还没把 native builder 挂到 object 写出流程。
2. **查询执行接管**  
   `MATCH ... AGAINST` 还没切到 visible object sidecar 扫描。
3. **tombstone 过滤**  
   还没把 `CollectTombstones()` 接进 native query。
4. **appendable tail / delta**  
   persisted object 之外的最新 appendable 数据还没补齐 native 可见性路径。
5. **catalog/checkpoint/replay 显式 locator**  
   当前只有 deterministic path helper，还没做显式元数据扩展。

## 3. 这轮代码对后续 native 实现的价值

这轮不是把最终 FTS_SCAN 一步做完，而是先把最容易失控的底层共性问题收住：

1. **切词语义只有一份**
2. **倒排物理结构已经落地**
3. **phrase 需要的 positions 模型已经落地**
4. **sidecar 文件已经有稳定编码形态**

后面 flush/merge/query/tombstone 再往上接，不需要继续依赖 hidden-table 结构。

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

## 5. 后续系统测试方案

native 完整接线后，建议按下面顺序补系统级测试：

1. **tokenization parity**
   - `default / ngram / json / json_value`
   - datalink 文本展开
2. **segment build / codec**
   - 单文档、多文档、重复 term、phrase positions
3. **flush sidecar**
   - flush 后 sidecar 文件存在
   - sidecar 内容与 object 内数据一致
4. **merge / compaction sidecar**
   - merge 后 sidecar 重建正确
   - 被 merge 掉的旧 object sidecar 不再参与查询
5. **query correctness**
   - term
   - boolean AND
   - phrase
   - top-k / score
6. **delete visibility**
   - tombstone 后结果立即不可见
   - compaction 后物理清理仍正确
7. **appendable tail**
   - persisted + tail 混合可见性
8. **replay / restore**
   - checkpoint / restart 后 sidecar 可恢复
9. **vector regression**
   - vector-only 查询路径无回归

## 6. 现阶段结论

当前仓库已经不再沿着 hidden-table v2-lite 往前推，而是切到了 **native FTS 内核先行** 的路线。  
这条路线现在已经具备继续往 flush/merge/query/tombstone 上接的代码基础，但还没有完成最终用户可见的 storage-coupled 端到端能力。
