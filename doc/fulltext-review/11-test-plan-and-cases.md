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

### 2.3 单机版验证脚本（推荐给 10.222.1.50）

当前仓库已经补了一个单机验证脚本：

```bash
scripts/test_fulltext_single_node.sh
```

它不是只跑一个 SQL 文件，而是分成三段：

1. **nightly-compatible**：复刻当前 `mo-nightly-regression` 的 FULLTEXT job 形状  
   `load data -> create fulltext index -> MATCH/DELETE/UPDATE`
2. **native-ready**：为单机 native 路径专门设计  
   `create index first -> load after index -> query tail -> checkpoint -> query persisted sidecar`
3. **mixed-coverage**：模拟 rollout 过渡表  
   `old persisted rows before index + new rows after index`

推荐在单机 MO 已经启动后执行：

```bash
MO_HOST=10.222.1.50 \
MO_PORT=6001 \
MO_USER=dump \
MO_PASSWORD=111 \
MO_DATA_DIR=/path/to/mo-data \
bash scripts/test_fulltext_single_node.sh
```

如果要验证**默认配置**是否已经足够支持新版 FULLTEXT（不额外 `set global experimental_fulltext_index=1`，并且建索引时省略 `WITH PARSER`），可以显式这样运行：

```bash
AUTO_ENABLE_FULLTEXT_INDEX=0 \
TEXT_PARSER= \
MO_HOST=10.222.1.50 \
MO_PORT=6001 \
MO_USER=root \
MO_PASSWORD=111 \
ROW_COUNT=1000000 \
MIXED_PRE_ROWS=400000 \
MIXED_POST_ROWS=200000 \
WAIT_SECONDS=10 \
bash scripts/test_fulltext_single_node.sh
```

脚本会生成 Markdown 报告，默认输出到当前目录：

```bash
fulltext_single_node_report.md
```

如果设置了 `MO_DATA_DIR`，脚本还会统计 `*.fts.*.seg` 和 `*.fts.locator` 的增量，用来辅助确认 sidecar / locator 确实被写出来。

### 2.4 现有测试回归

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

## 4. 当前 nightly FULLTEXT job 分析

用户当前提到的 job：

- workflow run: `24309229789`
- job: `70999366143`
- 名称：`FULLTEXT INDEX TEST`

从 workflow 与 job step 可以看出，这条 job 当前做的是：

1. 建表
2. 先 `LOAD DATA`
3. 再 `CREATE FULLTEXT INDEX`
4. 跑 `MATCH`
5. 跑 delete / update / boolean 查询

按 step 时间看，这次运行大致是：

| Step | 用时 |
|------|------|
| Load data into table | ~8s |
| Select before create fulltext index | ~1s |
| Create fulltext index on table column | ~39s |
| Select after create fulltext index | ~1s |
| Delete test on fulltext index table | ~6s |
| Update test on fulltext index table | ~2s |

这个 job 的定位应该明确成：

- **它是 FULLTEXT 功能回归 / 兼容性回归**
- **它不是 native sidecar 路径覆盖验证**

原因很简单：它的数据先落进表，再建索引；这种形状主要覆盖的是当前用户语义与 v1 hidden-table 行为。要在单机上稳定看到 native sidecar 的效果，必须至少满足下面一种：

1. **先建 FULLTEXT 索引，再导入数据，再 checkpoint**
2. **或让数据在索引创建后重新经历 flush / merge，产出 sidecar**

---

## 5. 接入 nightly regression 的建议

### 5.1 CI 集成

在 nightly regression workflow 中加入：

```yaml
- name: Run fulltext native tests
  run: |
    ./mo-tester -p test/distributed/cases/fulltext/fulltext_native.sql
    ./mo-tester -p test/distributed/cases/fulltext/fulltext_tae_regression.sql
```

### 5.2 单机 / 预发环境建议

建议把 `scripts/test_fulltext_single_node.sh` 放到单机或预发环境的 smoke test 里，原因是：

1. 它能同时覆盖 nightly-compatible 回归和 native-ready 路径
2. 它可以输出一份可读的 Markdown 报告
3. 它支持通过 `MO_DATA_DIR` 直接观察 sidecar / locator 文件增量

### 5.3 性能基准（可选，后续加入）

建议后续加入性能基准测试：

| 指标 | 基准场景 | 目标 |
|------|---------|------|
| 建索引时间 | 100 万行 × 200 字符 | 记录基线 |
| 单 term 查询延迟 | 100 万行，单 term NL 查询 | < 500ms |
| boolean AND 查询延迟 | 100 万行，2 term AND | < 1s |
| flush 额外耗时 | 对比有/无 FTS 索引的 flush 时间 | < 10% 额外开销 |
| merge 额外耗时 | 对比有/无 FTS 索引的 merge 时间 | < 20% 额外开销 |

---

## 6. 测试通过标准

1. **所有现有 fulltext 测试通过**（fulltext.sql / fulltext_bm25.sql / fulltext1.sql / fulltext2.sql / jsonvalue.sql / datalink.sql）
2. **新增 native 测试通过**（fulltext_native.sql）
3. **TAE 回归测试通过**（fulltext_tae_regression.sql）
4. **单元测试通过**（`go test ./pkg/fulltext/... ./pkg/fulltext/native/...`）
5. **无 panic / 无 data race**（`go test -race`）
