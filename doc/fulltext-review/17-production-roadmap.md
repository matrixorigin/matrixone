# Native FTS 商用路线图

基于全部 review、benchmark 和边界测试的结论。

---

## 当前状态

- ✅ native-only 模式已实现（跳过 v1 隐藏表）
- ✅ 大批量 UPDATE 旧词残留问题已解决
- ✅ Boolean mode 大表已修复
- ✅ DELETE/INSERT/UPDATE 正确性已验证（2M 行）
- ✅ V3 extensible header 已实现
- ✅ sidecar 失败不阻塞 flush/merge
- ✅ tail segment 已实现（appendable 数据可查）
- ✅ 混合查询已实现（native + v1 去重）

---

## 第一档：阻塞商用（2 周内必须完成）

不做就不能上生产。

### 1.1 多列索引 NULL 列导致整行不可搜索

| 项目 | 内容 |
|------|------|
| 现状 | `collectIndexValues` 遇到任意 NULL 列就跳过整行，`(NULL, 'cherry')` 完全不被索引 |
| 影响 | 任何有可选字段的表都会丢数据，这在业务中极其常见 |
| 根因 | `pkg/fulltext/native/object.go` 第 351 行 `return nil, false, nil` |
| 修复 | 跳过 NULL 值而不是跳过整行，对非 NULL 列正常切词 |
| 改动量 | ~5 行 |
| 预计 | 0.5 天 |

### 1.2 sidecar GC

| 项目 | 内容 |
|------|------|
| 现状 | 旧 object 被 merge 后，sidecar 文件永远留在 file service 里 |
| 影响 | 存储持续增长，长期运行后耗尽磁盘/S3 空间 |
| 根因 | GC 流程只认识 object 文件，不认识 sidecar 文件 |
| 修复 | 在 GC 删除 object 文件时，同步删除对应的 sidecar 文件 |
| 改动量 | ~20 行 |
| 预计 | 1-2 天 |

### 1.3 现有 SQL 测试全量回归

| 项目 | 内容 |
|------|------|
| 现状 | native-only 改动后，现有 7 个 fulltext 测试文件还没跑过 |
| 影响 | 可能有未发现的回归 |
| 修复 | 跑全部测试 + 更新 .result 基线文件 |
| 文件 | fulltext.sql / fulltext_bm25.sql / fulltext1.sql / fulltext2.sql / jsonvalue.sql / datalink.sql / pessimistic_transaction/fulltext.sql |
| 预计 | 1 天 |

---

## 第二档：影响生产稳定性（1 个月内完成）

不做能跑但跑不稳，大数据量或长期运行会出问题。

### 2.1 segment 按需加载

| 项目 | 内容 |
|------|------|
| 现状 | `ReadSidecar` 把整个 sidecar 反序列化到内存 `map[string][]Posting` |
| 影响 | 大 object（128MB+ 文本）场景内存暴涨、查询变慢 |
| 修复 | segment 格式加 term index + offset table，读取时只加载目标 term 的 postings |
| 参考 | ClickHouse text index 的 `.dct` + `.idx` 分层 |
| 预计 | 3-5 天 |

### 2.2 postings 压缩

| 项目 | 内容 |
|------|------|
| 现状 | 每个 posting 原始 `binary.Write`，无压缩 |
| 影响 | sidecar 文件偏大，I/O 开销高，尤其高频 term |
| 修复 | positions 做 delta + varint 压缩，postings 按 block 分组 |
| 参考 | Lucene 的 delta + PFOR，ClickHouse 的 roaring bitmap |
| 预计 | 2-3 天 |

### 2.3 BM25 统计扣除 tombstone

| 项目 | 内容 |
|------|------|
| 现状 | `totalDocs` 是 sidecar 原始 DocCount 之和，没减去已删除文档 |
| 影响 | 大量 DELETE 后 BM25 打分偏差（IDF 偏高），排序不准 |
| 修复 | 查询时用实际候选数修正，或在 merge 后更新统计 |
| 预计 | 1 天 |

### 2.4 sidecar 并行读取

| 项目 | 内容 |
|------|------|
| 现状 | `prepareNativeScan` 逐个串行读 sidecar |
| 影响 | 100 个 object 就要 100 次串行 I/O，查询延迟和 object 数量成正比 |
| 修复 | 用 goroutine pool 并行读取 |
| 预计 | 1 天 |

### 2.5 merge 时合并 sidecar 而非重新切词

| 项目 | 内容 |
|------|------|
| 现状 | merge 时 `OnBlockWritten` 对每行重新 tokenize |
| 影响 | 大文本列 merge 时 CPU 开销高 |
| 修复 | 实现 segment merge（合并 sorted terms + 重映射 row refs） |
| 参考 | ClickHouse text index merge 策略 |
| 预计 | 3-5 天 |

---

## 第三档：影响用户体验（按需排期）

不做能用但体验差，影响长期竞争力。

### 3.1 analyzer 增强

| 项目 | 内容 |
|------|------|
| 现状 | 只有 simple tokenizer + 3-gram，没有 stopword/stemming/synonym |
| 影响 | 英文搜 "running" 找不到 "run"，常见词（the/is/a）污染结果 |
| 修复 | 加 preprocessor chain（至少 lower + stopword），参考 ClickHouse 的 preprocessor 参数 |
| 预计 | 3-5 天 |

### 3.2 NL 模式语义修正

| 项目 | 内容 |
|------|------|
| 现状 | NL 模式走 phrase 路径（强位置约束），`'matrix cloud'` 返回 0 |
| 影响 | 用户期望 bag-of-words 召回，实际是严格短语匹配，和 MySQL/ES 行为不一致 |
| 修复 | NL 模式改为 OR 召回 + BM25 排序 |
| 预计 | 2-3 天 |

### 3.3 sidecar 显式元数据

| 项目 | 内容 |
|------|------|
| 现状 | 通过 deterministic path 定位，无法 inspect/repair |
| 影响 | 运维无法查看索引状态、无法手动修复损坏的 sidecar |
| 修复 | catalog 或 ObjectStats 中记录 sidecar 存在性和位置 |
| 预计 | 2-3 天 |

### 3.4 独立 FTS_SCAN 算子

| 项目 | 内容 |
|------|------|
| 现状 | native 查询仍复用 v1 的 table function 框架和打分逻辑 |
| 影响 | 无法做 segment 级 early top-k，大结果集性能差 |
| 修复 | 新增 executor 算子，直接在 postings 层做交并、top-k、回表 |
| 参考 | ClickHouse 的 direct read 优化（45x 加速） |
| 预计 | 5-10 天 |

---

## 时间线总结

```
Week 1-2:  第一档（NULL 修复 + sidecar GC + 全量回归）
           → 可上有限场景生产（低更新、中等数据量）

Week 3-6:  第二档（按需加载 + 压缩 + 统计修正 + 并行读取 + merge 优化）
           → 可上大数据量生产

Week 7+:   第三档（analyzer + NL 语义 + 元数据 + FTS_SCAN 算子）
           → 长期竞争力
```

---

## 已完成清单

| 完成时间 | 项目 | 文档 |
|---------|------|------|
| Round 1 | sidecar 失败不阻塞 flush/merge | 08 §1 |
| Round 1 | 全局统计持久化到 sidecar | 08 §1 |
| Round 2 | V3 extensible header | 08 §8 |
| Round 3 | native-only 模式（跳过 v1 隐藏表） | 15 |
| Round 3 | 大批量 UPDATE 旧词残留修复 | 16 |
| 已有 | tail segment（appendable 数据可查） | 13 §5.1 |
| 已有 | 混合查询（native + v1 去重） | 13 §5.2 |
| 已有 | Boolean mode 大表修复 | 12 §4 |
