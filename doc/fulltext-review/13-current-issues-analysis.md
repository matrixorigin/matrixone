# Native FTS 当前问题分析

基于 `fulltext_single_node_report_cn.md` 的测试结果和代码分析。

---

## 1. 已修复的问题

### 1.1 Boolean mode 大表报错（已修复）

- 之前：`ERROR 20422: service not found`，>= 5000 行时触发
- 现在：1M 行 boolean 查询正常通过

---

## 2. 当前最严重的问题：大批量 UPDATE 后旧词残留

### 2.1 现象

从报告中的数据：

| 操作 | 期望旧词命中 | 实际旧词命中 | 状态 |
|------|----------:|----------:|------|
| 100K persisted UPDATE + checkpoint | 0 | 53,277 | ❌ |
| 100K tail UPDATE + checkpoint | 0 | 24,894 | ❌ |

新词（`hotupdated` / `tailupdated`）都能正确返回 100,000，说明新版本文档已经进入索引。但旧词没有完全消失。

### 2.2 根因分析

UPDATE 在 MO 中的执行路径是 **DELETE old row + INSERT new row**。对于全文索引，涉及两个层面：

**层面 1：v1 隐藏表维护（PostDML）**

```go
// postdml.go
if postdml.PostDmlCtx.IsDelete {
    sql = fmt.Sprintf("DELETE FROM %s WHERE doc_id IN (%s)", indextbl, values)
    proc.Base.PostDmlSqlList.Append(sql)
}
if postdml.PostDmlCtx.IsInsert {
    sql = fmt.Sprintf("INSERT INTO %s SELECT f.* FROM %s ...", indextbl, sourcetbl, ...)
    proc.Base.PostDmlSqlList.Append(sql)
}
```

UPDATE 会生成两条 PostDML SQL：先 DELETE 旧 token，再 INSERT 新 token。这些 SQL 在主 pipeline 完成后执行。

**问题**：当 UPDATE 100K 行时，`DELETE FROM hidden_index WHERE doc_id IN (100000 个值)` 这条 SQL 可能因为 IN 列表过大而执行不完整或超时。如果 DELETE 没有完全执行，旧 token 就会残留在隐藏表中。

**层面 2：native sidecar 路径**

native 路径通过 tombstone 过滤旧版本。但 UPDATE 后：
- 旧行被标记为 tombstone ✅
- 新行写入 appendable object ✅
- 旧 object 的 sidecar 中仍然包含旧行的 postings
- 查询时 `isNativeDeleted` 应该过滤掉旧行

**但是**：如果查询走的是混合路径（native + v1 fallback），native 部分正确过滤了旧行，但 v1 fallback 部分从隐藏表读到了残留的旧 token，就会出现旧词残留。

### 2.3 验证推测

报告中的关键线索：

1. **残留量不是 100K，而是 53,277 和 24,894** — 说明部分旧 token 被成功删除了，但不是全部
2. **checkpoint 后残留仍然存在** — 说明不是 flush 时序问题
3. **DELETE 场景完全正常** — DELETE 只需要从隐藏表删除，不需要 INSERT 新 token，所以 PostDML 更简单
4. **新词能查到 100K** — INSERT 新 token 的 PostDML 执行成功了

**最可能的根因**：v1 的 PostDML `DELETE FROM hidden_index WHERE doc_id IN (...)` 在处理大批量 UPDATE 时，IN 列表被分批执行或部分失败，导致隐藏表中残留了部分旧 token。native sidecar 路径本身的 tombstone 过滤是正确的，但查询最终走的是混合路径，v1 部分的隐藏表残留污染了结果。

### 2.4 建议修复方向

1. **短期**：排查 PostDML 的 DELETE SQL 在大 IN 列表下是否有截断或分批问题
2. **中期**：当 native 路径 `complete = true` 时，完全跳过 v1 fallback，不依赖隐藏表
3. **长期**：去掉 v1 隐藏表维护，UPDATE/DELETE 完全依赖 native tombstone

---

## 3. Sidecar 文件增量为 NA

### 3.1 现象

报告中所有阶段的 sidecar 增量都是 `NA`：

```
| *.fts.*.seg  | NA |
| *.fts.locator | NA |
```

### 3.2 原因

报告中 `MO_DATA_DIR` 标注为 `not provided`，说明测试脚本没有配置 MO 数据目录路径，无法扫描文件系统来统计 sidecar 文件增量。这不是 bug，只是测试配置缺失。

### 3.3 建议

提供 `MO_DATA_DIR` 后重新跑一次，确认 sidecar 文件确实在 flush/merge 后生成。或者通过 file service 的 API 来检查。

---

## 4. 查询延迟分析

### 4.1 数据

从报告中提取的查询延迟：

| 场景 | 数据量 | 命中量 | 延迟 |
|------|--------|--------|------|
| NL 查询 alpha | 1M | 1M | 956ms |
| checkpoint 后 NL alpha | 1M | 1M | 1009ms |
| 基线 hotupdate | 2M | 100K | 299ms |
| 大 boolean +stablegamma | 2M | 500K | 544ms |
| 查询 legacytoken | 600K | 400K | 337ms |
| 查询 newtoken | 600K | 200K | 277ms |

### 4.2 分析

1. **checkpoint 前后延迟几乎相同**（956ms vs 1009ms）— 说明当前 native 路径和 v1 fallback 路径的查询延迟差距不大。这是因为 native 路径虽然跳过了 `runCountStar`，但仍然全量反序列化 sidecar 到内存 map。
2. **延迟和命中量基本成正比** — 100K 命中 ~300ms，500K 命中 ~550ms，1M 命中 ~1000ms。说明瓶颈在结果集处理而非索引查找。
3. **2M 表的 100K 命中查询（299ms）比 1M 表的 1M 命中查询（956ms）快很多** — 进一步证实瓶颈在结果集大小而非表大小。

### 4.3 优化方向

当前延迟的主要组成：
- sidecar 全量反序列化（和 object 数量成正比）
- postings 遍历和 tombstone 过滤（和命中量成正比）
- 结果集聚合和打分（和命中量成正比）

最有效的优化是 **segment 按需加载**（只读目标 term 的 postings）和 **early top-k**（不需要遍历所有命中文档）。

---

## 5. 其他观察

### 5.1 tail segment 已实现

代码中 `buildNativeTailSegment` 已经实现了 appendable tail 的 native 化——通过 `rel.Ranges` 读取 uncommitted/committed in-memory 数据，构建临时 segment。这解决了之前 review 中提到的"appendable object 无法走 native"的问题。

tail segment 的 `applyTombstones = false`，因为 tail 数据是从 reader 直接读取的，已经经过了可见性过滤。

### 5.2 混合查询已实现

代码中 `incomplete` 标志和 `nativeOwned` 去重机制说明混合查询（native + v1）已经实现：
- native 处理有 sidecar 的 persisted object + tail segment
- 如果 `incomplete = true`（有 object 缺 sidecar），v1 处理剩余部分
- `nativeOwned` 防止同一个文档被 native 和 v1 重复计算

这解决了之前 review 中提到的"fallback 判断过于保守"的问题。

### 5.3 mixed-coverage 场景正确

旧数据（索引创建前已存在）和新数据（索引创建后插入）都能正确查询，说明 native + v1 混合路径在这个场景下工作正常。

---

## 6. 问题优先级总结

| 优先级 | 问题 | 影响 | 状态 |
|--------|------|------|------|
| **P0** | 大批量 UPDATE 后旧词残留 | 正确性 | 未修复，最可能是 v1 PostDML DELETE 大 IN 列表问题 |
| P1 | sidecar 全量反序列化 | 性能 | 未修复 |
| P1 | postings 无压缩 | 存储/IO | 未修复 |
| P1 | sidecar GC | 存储泄漏 | 未修复 |
| P2 | BM25 统计未扣除 tombstone | 打分精度 | 未修复 |
| ✅ | Boolean mode 大表报错 | 功能 | 已修复 |
| ✅ | sidecar 失败阻塞 flush/merge | 稳定性 | 已修复 |
| ✅ | 全局统计持久化 | 性能 | 已修复 |
| ✅ | V3 extensible header | 格式演进 | 已修复 |
| ✅ | appendable tail native 化 | 功能 | 已实现（buildNativeTailSegment） |
| ✅ | 混合查询（native + v1） | 功能 | 已实现（nativeOwned 去重） |
| ✅ | DELETE 大批量正确性 | 正确性 | 测试通过 |
