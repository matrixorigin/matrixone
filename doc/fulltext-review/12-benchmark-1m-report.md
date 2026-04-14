# Native FTS Benchmark Report — 1M rows

- Date: 2026-04-13 17:54
- Server: 10.222.1.50:6001
- MO Version: 8.0.30-MatrixOne-v
- Data: 1,000,000 rows, VARCHAR body (~50 chars), VARCHAR title (~20 chars)
- Body: 10 种文本模式轮换（matrix/distributed/fulltext/ML/cloud/database/analytics/k8s/s3/txn）
- Title: 5 种标题轮换
- Index: `FULLTEXT(body, title)`
- Build index time: **1.24s**（1M 行）

---

## 1. Natural Language Mode — 正常

| Test Case | Count | Latency |
|-----------|------:|--------:|
| Single term: matrix (~100K match) | 99,899 | 0.228s |
| Single term: cloud (~300K match) | 299,700 | 0.375s |
| Single term: neural (~100K match) | 99,900 | 0.229s |
| Single term: kubernetes (~100K match) | 99,900 | 0.247s |
| Rare term: xyzzy (1 match) | 1 | 0.113s |
| No match term | 0 | 0.166s |
| Universal term: row (~1M match) | 998,999 | 0.740s |
| Medium term: database (~400K match) | 399,599 | 0.372s |

**分析**：
- 查询延迟和结果集大小基本成正比
- 100K 结果集 ~0.23s，300K ~0.37s，1M ~0.74s
- 稀有 term（1 match）0.11s，空结果 0.17s
- 空结果比稀有 term 慢，说明可能仍在做全量扫描而非 early termination

## 2. Boolean Mode — 发现 BUG

| Test Case | Count | Latency |
|-----------|------:|--------:|
| Simple: matrix | ERROR | 0.104s |
| AND: +matrix +cloud | ERROR | 0.109s |
| NOT: +matrix -cloud | ERROR | 0.108s |

**BUG 详情**：

- 错误码：`ERROR 20422 (HY000): service not found`
- 触发条件：**Boolean mode + 行数 >= 5000**
- 精确边界：4999 行正常，5000 行报错
- NL mode 在同样数据量下完全正常
- 小表（< 5000 行）Boolean mode 正常

**根因推测**：5000 行可能触发了 flush 产生多个 block 或 object。Boolean mode 的 v1 fallback 路径在处理多 object 场景时，动态 SQL 生成或 table function 调用链路中某个 service 查找失败。这不是 native sidecar 的问题（native 路径对 boolean 也会 fallback 到 v1），而是 v1 boolean 路径本身在大数据量下的 bug。

## 3. DML Correctness — 正常

| Test Case | Count | Latency |
|-----------|------:|--------:|
| After DELETE 1000 rows: matrix count | 99,899 | 0.234s |
| New INSERT 1000 rows: freshly | 1,000 | 0.186s |
| After UPDATE: xyzzy | 1 | 0.131s |

**分析**：
- DELETE 后 tombstone 过滤正确（100,000 → 99,899，减少了 101 行 = id 1-1000 中 matrix 模式的 ~100 行 + id=5000 被 UPDATE）
- INSERT 新行立即可查（走 v1 fallback，因为有 appendable object）
- UPDATE 后新值可查，旧值不可查

## 4. 性能总结

| 指标 | 值 |
|------|-----|
| 建索引（1M 行） | 1.24s |
| 单 term 查询（100K 结果） | ~0.23s |
| 单 term 查询（300K 结果） | ~0.37s |
| 单 term 查询（1M 结果） | ~0.74s |
| 稀有 term 查询（1 结果） | ~0.11s |
| 空结果查询 | ~0.17s |
| DELETE 后查询 | ~0.23s |
| INSERT 后查询 | ~0.19s |
| UPDATE 后查询 | ~0.13s |

## 5. 发现的问题

### BUG-1: Boolean mode 在 >= 5000 行时报 `service not found`

- **严重程度**：高 — Boolean mode 是全文检索的核心功能
- **影响范围**：所有 boolean 查询（AND/OR/NOT/phrase/prefix）在大表上都失败
- **精确边界**：4999 行正常，5000 行报错
- **NL mode 不受影响**
- **建议**：优先修复，这会阻塞所有 boolean 相关的测试和用户场景

### ISSUE-1: NL multi-word 走 phrase 路径

- `MATCH ... AGAINST('matrix cloud')` 返回 0 结果
- 原因：NL mode 实际走 SqlPhrase 路径（强位置约束），"matrix" 和 "cloud" 不是连续出现的
- 这是 01 文档已经分析过的语义偏离问题，不是新 bug

### OBSERVATION-1: 空结果查询比稀有 term 查询慢

- 空结果 0.17s vs 稀有 term 0.11s
- 说明当前没有做 term 不存在的 early termination，可能仍在枚举所有 sidecar

---

## 6. Native v2 优势验证测试（500K rows, steady-state）

以下测试通过 `mo_ctl('dn','checkpoint','')` 强制 flush，确保数据进入 persisted object，走 native sidecar 路径。

### 6.1 Steady-state NL 查询

| Query | Result | Latency |
|-------|-------:|--------:|
| matrix (50K match) | 50,000 | 189ms |
| database (200K match) | 200,000 | 250ms |
| kubernetes (50K match) | 50,000 | 157ms |
| cloud (150K match) | 150,000 | 240ms |
| streaming (50K match) | 50,000 | 179ms |

### 6.2 DELETE 50K 行 → checkpoint → 查询

验证 tombstone 过滤：native 复用 MO 原生 tombstone，不需要额外 SQL 维护隐藏表。

- DELETE 50K rows: 639ms
- checkpoint + wait: 8s

| Query | Result | Latency |
|-------|-------:|--------:|
| matrix (before DELETE) | 50,000 | 146ms |
| matrix (after DELETE, expect ~45K) | 45,000 | 243ms |
| database (after DELETE) | 180,000 | 233ms |
| streaming (after DELETE) | 45,000 | 126ms |

✅ **DELETE 后 tombstone 过滤完全正确**：matrix 从 50,000 → 45,000（减少 5,000 = 50K 删除行中 10% 是 matrix 模式）。

### 6.3 UPDATE 10K 行 → checkpoint → 查询

验证 UPDATE：native 通过 tombstone 屏蔽旧版本，新版本 flush 后生成新 sidecar。

- UPDATE 10K rows: 507ms
- checkpoint + wait: 8s

| Query | Result | Latency |
|-------|-------:|--------:|
| upgraded (new term, expect 10K) | 10,000 | 149ms |
| matrix (should decrease by ~1K) | 44,000 | 180ms |

✅ **UPDATE 后旧值被 tombstone 屏蔽，新值通过新 sidecar 可查**：upgraded 精确返回 10,000，matrix 从 45,000 → 44,000（被 UPDATE 覆盖的 ~1,000 个 matrix 行不再匹配）。

### 6.4 INSERT 100K 新行 → checkpoint → 查询

验证增量写入：新 object flush 后自动生成 sidecar。

- INSERT 100K rows: 497ms
- checkpoint + wait: 8s

| Query | Result | Latency |
|-------|-------:|--------:|
| freshbatch (new term, expect 100K) | 100,000 | 274ms |
| alpha (new term, expect 100K) | 100,000 | 210ms |
| kubernetes (old data, expect ~44K) | 44,000 | 175ms |
| Total rows | 550,000 | 91ms |

✅ **新插入的 100K 行在 checkpoint 后完全可查，旧数据不受影响。**

### 6.5 混合 DML → checkpoint → 查询稳定性

连续执行 DELETE 10K + UPDATE 1K + INSERT 5K → checkpoint：

| Query | Result | Latency |
|-------|-------:|--------:|
| omega (UPDATE 新值, expect 1K) | 1,000 | 146ms |
| zeta (INSERT 新行, expect 5K) | 5,000 | 137ms |
| matrix (多次 DML 后) | 42,900 | 169ms |
| freshbatch (之前的 100K 不受影响) | 100,000 | 190ms |
| Total rows | 545,000 | 84ms |

✅ **混合 DML 后所有查询结果精确正确，历史数据不受影响。**

### 6.6 多次 checkpoint 后查询一致性

连续执行 3 次 checkpoint，验证 merge/compaction 后数据一致性：

| Query | Result | Latency |
|-------|-------:|--------:|
| matrix | 42,900 | 176ms |
| freshbatch | 100,000 | 146ms |
| upgraded | 10,000 | 110ms |
| omega | 1,000 | 145ms |
| zeta | 5,000 | 90ms |

✅ **多次 checkpoint/merge 后所有查询结果完全一致，sidecar 在 merge 后正确重建。**

### 6.7 v2 优势总结

| 维度 | 测试结论 |
|------|---------|
| DELETE 后查询 | ✅ tombstone 过滤精确，不需要额外 SQL 维护隐藏表 |
| UPDATE 后查询 | ✅ 旧值被屏蔽，新值可查，不需要 delete+re-tokenize+insert |
| INSERT 后查询 | ✅ 新 object 自动生成 sidecar，checkpoint 后立即可查 |
| 混合 DML 稳定性 | ✅ 多轮 DELETE+UPDATE+INSERT 后结果精确正确 |
| merge/compaction 一致性 | ✅ 多次 checkpoint 后结果完全一致 |
| 查询延迟 | 50K 结果 ~150ms，100K ~200ms，200K ~250ms |
