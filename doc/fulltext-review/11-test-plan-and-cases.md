# Native FTS 测试方案

## 1. 测试目标

1. **正确性**：native sidecar 路径和 v1 fallback 路径返回相同结果
2. **TAE 稳定性**：FTS 改动不影响非 FTS 表的 flush/merge/compaction
3. **生命周期**：sidecar 在 flush/merge/delete 场景下行为正确
4. **回归**：现有 fulltext 测试全部通过，向量查询无回归

---

## 2. 测试层次

### 2.1 单元测试（Go test）

已有覆盖，通过 `go test` 执行：

| 包 | 覆盖内容 |
|----|---------|
| `pkg/fulltext` | tokenization parity（default/ngram/json/json_value） |
| `pkg/fulltext/native` | segment build/lookup/phrase、marshal V1/V2/V3 兼容、sidecar 读写 |
| `pkg/sql/colexec/table_function` | native query 执行、v1 fallback |
| `pkg/sql/plan` | planner 改写 |
| `pkg/sql/compile` | DDL 编译 |
| `pkg/vm/engine/tae/mergesort` | merge 框架 |

执行命令：
```bash
cd /path/to/matrixone
go test ./pkg/fulltext/... -v -count=1
go test ./pkg/fulltext/native/... -v -count=1
go test ./pkg/sql/colexec/table_function/... -v -count=1 -run TestFulltext
```

### 2.2 分布式 SQL 测试（mo-tester 框架）

新增两个测试文件，放在 `test/distributed/cases/fulltext/` 下：

| 文件 | 覆盖内容 |
|------|---------|
| `fulltext_native.sql` | native FTS 正确性：基础查询、DELETE/tombstone、UPDATE、merge 后查询、中文、JSON、复合主键、空表、多索引、BM25 打分、并发压力 |
| `fulltext_tae_regression.sql` | TAE 回归：非 FTS 表不受影响、FTS 和非 FTS 共存、tombstone 对象跳过、DDL 操作、向量索引共存 |

执行方式（和现有 nightly regression 一致）：
```bash
# 单独跑 native FTS 测试
./mo-tester -p test/distributed/cases/fulltext/fulltext_native.sql

# 单独跑 TAE 回归测试
./mo-tester -p test/distributed/cases/fulltext/fulltext_tae_regression.sql

# 跑全部 fulltext 测试（包括现有 v1 测试）
./mo-tester -p test/distributed/cases/fulltext/
```

### 2.3 现有测试回归

必须确保以下现有测试全部通过：

| 文件 | 说明 |
|------|------|
| `fulltext.sql` | v1 核心功能（TF-IDF） |
| `fulltext_bm25.sql` | v1 BM25 打分 |
| `fulltext1.sql` | DDL、多类型列 |
| `fulltext2.sql` | char/varchar/text 列类型 |
| `jsonvalue.sql` | json_value parser |
| `datalink.sql` | datalink 列 |
| `pessimistic_transaction/fulltext.sql` | 事务场景 |

---

## 3. 测试用例矩阵

### 3.1 native sidecar 正确性

| 编号 | 场景 | 验证点 | 测试文件 |
|------|------|--------|---------|
| N-01 | 基础 insert → flush → NL 查询 | native 路径返回正确结果 | fulltext_native.sql §1 |
| N-02 | boolean AND/OR/NOT/phrase/prefix | 所有布尔操作正确 | fulltext_native.sql §1 |
| N-03 | score 投影 | BM25 分数正确排序 | fulltext_native.sql §1, §10 |
| N-04 | NULL 列处理 | 不崩溃，不返回 NULL 行 | fulltext_native.sql §1 |
| N-05 | DELETE 后查询 | 被删行不出现在结果中 | fulltext_native.sql §2 |
| N-06 | UPDATE 后查询 | 旧值不匹配，新值匹配 | fulltext_native.sql §3 |
| N-07 | 大批量 insert → merge | merge 后两批数据都可查 | fulltext_native.sql §4 |
| N-08 | 中文 3-gram | 中文分词和查询正确 | fulltext_native.sql §5 |
| N-09 | JSON parser | JSON 值可搜索 | fulltext_native.sql §6 |
| N-10 | 复合主键 | 多列主键下查询和删除正确 | fulltext_native.sql §7 |
| N-11 | 空表查询 | 不报错，返回空 | fulltext_native.sql §8 |
| N-12 | 多索引共存 | 不同索引返回不同结果 | fulltext_native.sql §9 |
| N-13 | 并发 insert + 查询 | 不崩溃，结果最终一致 | fulltext_native.sql §11 |

### 3.2 TAE 稳定性回归

| 编号 | 场景 | 验证点 | 测试文件 |
|------|------|--------|---------|
| T-01 | 非 FTS 表大批量 insert | flush/merge 正常，count 正确 | fulltext_tae_regression.sql §1 |
| T-02 | 非 FTS 表 UPDATE/DELETE | DML 正常 | fulltext_tae_regression.sql §1 |
| T-03 | FTS 表和非 FTS 表交替操作 | 互不影响 | fulltext_tae_regression.sql §2 |
| T-04 | 大量 DELETE 生成 tombstone | tombstone 对象不影响 FTS | fulltext_tae_regression.sql §3 |
| T-05 | DROP INDEX | 删除后 FTS 查询报错 | fulltext_tae_regression.sql §4 |
| T-06 | 已有数据表加 FTS 索引 | 回填正确 | fulltext_tae_regression.sql §4 |
| T-07 | TRUNCATE + FTS | 清空后查询返回空 | fulltext_tae_regression.sql §4 |
| T-08 | 向量索引和 FTS 共存 | 两种查询互不干扰 | fulltext_tae_regression.sql §5 |

---

## 4. 接入 nightly regression 的建议

### 4.1 CI 集成

在 nightly regression workflow 中加入：

```yaml
- name: Run fulltext native tests
  run: |
    ./mo-tester -p test/distributed/cases/fulltext/fulltext_native.sql
    ./mo-tester -p test/distributed/cases/fulltext/fulltext_tae_regression.sql
```

### 4.2 性能基准（可选，后续加入）

建议后续加入性能基准测试：

| 指标 | 基准场景 | 目标 |
|------|---------|------|
| 建索引时间 | 100 万行 × 200 字符 | 记录基线 |
| 单 term 查询延迟 | 100 万行，单 term NL 查询 | < 500ms |
| boolean AND 查询延迟 | 100 万行，2 term AND | < 1s |
| flush 额外耗时 | 对比有/无 FTS 索引的 flush 时间 | < 10% 额外开销 |
| merge 额外耗时 | 对比有/无 FTS 索引的 merge 时间 | < 20% 额外开销 |

---

## 5. 测试通过标准

1. **所有现有 fulltext 测试通过**（fulltext.sql / fulltext_bm25.sql / fulltext1.sql / fulltext2.sql / jsonvalue.sql / datalink.sql）
2. **新增 native 测试通过**（fulltext_native.sql）
3. **TAE 回归测试通过**（fulltext_tae_regression.sql）
4. **单元测试通过**（`go test ./pkg/fulltext/... ./pkg/fulltext/native/...`）
5. **无 panic / 无 data race**（`go test -race`）
