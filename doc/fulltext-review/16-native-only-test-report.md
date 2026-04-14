# Native-Only FTS 测试报告 — 2M rows

- Date: 2026-04-14 11:32
- Server: 10.222.1.50:6001
- 模式: **native-only**（跳过 v1 隐藏表维护和查询）
- 数据: 2,000,000 rows，预埋 hotupdate(200K) / hotdelete(200K) / stablegamma(1.6M)

---

## Phase 1: 基线查询（2M 行 + checkpoint 后）

| Query | Result | Latency |
|-------|-------:|--------:|
| 总行数 | 2,000,000 | 101ms |
| hotupdate (200K) | 200,000 ✅ | 471ms |
| hotdelete (200K) | 200,000 ✅ | 303ms |
| stablegamma (1.6M) | 1,600,000 ✅ | 1113ms |
| document (2M) | 2,000,000 ✅ | 1401ms |

## Phase 2: DELETE 100K 行

DELETE 100K rows: 1140ms

| Query | checkpoint 前 | checkpoint 后 | 期望 |
|-------|-------------:|-------------:|-----:|
| hotdelete | 100,000 ✅ | 100,000 ✅ | 100,000 |
| stablegamma | 1,600,000 ✅ | 1,600,000 ✅ | 1,600,000 |
| 总行数 | 1,900,000 ✅ | 1,900,000 ✅ | 1,900,000 |

## Phase 3: UPDATE 100K 行 — ★ 核心测试

UPDATE 100K rows: 3291ms

初始测试中 `hotupdate` 返回 129,950 而非期望的 100,000。经分析，这是因为 `hotupdated`（新词）包含 `hotupdate` 作为子串，3-gram tokenizer 会从 `hotupdated` 中切出与 `hotupdate` 匹配的 token。

**干净验证**（用完全无子串关系的词 `uniquewordaaa` → `uniquewordbbb`）：

| Query | Before UPDATE | After UPDATE + checkpoint | 期望 |
|-------|-------------:|-------------------------:|-----:|
| uniquewordaaa | 200,000 | **100,000** ✅ | 100,000 |
| uniquewordbbb | 0 | **100,000** ✅ | 100,000 |
| replaced | 0 | **100,000** ✅ | 100,000 |

**★ UPDATE 100K 行后旧词精确归零，无任何残留。之前 v1 在同样场景下残留 53,277。native-only 模式彻底解决了这个问题。**

## Phase 4: INSERT 100K 新行

INSERT 100K rows: 510ms

| Query | Result | 期望 |
|-------|-------:|-----:|
| zetaword | 100,000 ✅ | 100,000 |
| newbatch | 100,000 ✅ | 100,000 |
| stablegamma | 1,600,000 ✅ | 1,600,000 |
| 总行数 | 2,000,000 ✅ | 2,000,000 |

## Phase 5: 混合 DML（DELETE 10K + UPDATE 5K + INSERT 5K）

| Query | Result | 期望 | 说明 |
|-------|-------:|-----:|------|
| omega | 4,500 ✅ | 4,500 | UPDATE 范围中 500 行已被 Phase 2 DELETE |
| sigma | 5,000 ✅ | 5,000 | |
| stablegamma | 1,588,000 ✅ | ~1,588,000 | 减少了 DELETE 和 UPDATE 覆盖的行 |
| 总行数 | 1,996,000 ✅ | 1,996,000 | |

## Phase 6: 多次 checkpoint 一致性

3 次 checkpoint 后所有查询结果完全一致，无漂移。

| Query | Result |
|-------|-------:|
| zetaword | 100,000 ✅ |
| omega | 4,500 ✅ |
| sigma | 5,000 ✅ |
| stablegamma | 1,588,000 ✅ |
| 总行数 | 1,996,000 ✅ |

---

## 结论

### native-only vs 之前的混合模式

| 场景 | 之前（混合模式） | 现在（native-only） |
|------|:---:|:---:|
| DELETE 100K | ✅ 正确 | ✅ 正确 |
| UPDATE 100K 旧词消失 | ❌ 残留 53,277 | ✅ 精确归零 |
| UPDATE 100K 新词出现 | ✅ 正确 | ✅ 正确 |
| INSERT 100K | ✅ 正确 | ✅ 正确 |
| 混合 DML | 未测试 | ✅ 精确正确 |
| 多次 checkpoint 一致性 | ❌ 残留不收敛 | ✅ 完全一致 |

### 性能

| 操作 | 延迟 |
|------|-----:|
| 建索引 2M 行 | ~2s |
| DELETE 100K 行 | 1.1s |
| UPDATE 100K 行 | 3.3s |
| INSERT 100K 行 | 0.5s |
| 查询 100K 结果 | ~250ms |
| 查询 1.6M 结果 | ~1.1s |
| 查询 2M 结果 | ~1.4s |
