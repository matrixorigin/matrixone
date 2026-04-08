# AI Agent 驱动的测试基础设施方案（V2）

## 一、背景与目标

### 背景
MO 的测试体系分布在多个仓库、多种测试类型中。当开发者提交 PR 后，需要人工判断该变更可能影响哪些功能，以及现有的测试是否充分覆盖了这些变更。这个过程依赖经验，容易遗漏。

### 核心目标
**手动选择一个 PR → AI 分析 diff + 阅读 skill 文档（MO 底层实现知识库）→ 判断 6 类测试中缺少哪些 case → 自动提 PR 补充到对应仓库。**

### 与 V1 方案的区别

| | V1（已废弃） | V2（当前） |
|---|------------|---------|
| 触发方式 | PR 自动触发 | **手动选择 PR** |
| 分析依据 | 22 条硬编码路径映射规则 | **AI + skill 文档** |
| 分析工具 | Go 代码 `mo-testplan` | **Copilot Agent** |
| 测试范围 | UT + BVT + SCA | **6 类测试**（BVT/稳定性/chaos/大数据/PITR/snapshot） |
| 输出 | TestPlan JSON | **自动提 PR 补充 case** |
| 目标仓库 | 只有 matrixone | **matrixone + mo-nightly-regression** |

---

## 二、整体架构

```
                    ┌────────────────────┐
                    │   手动选择 PR 编号   │
                    └────────┬───────────┘
                             │
                             ▼
                    ┌────────────────────┐
                    │  获取 diff (vs main)│
                    └────────┬───────────┘
                             │
                ┌────────────┼────────────┐
                ▼            ▼            ▼
        ┌──────────┐  ┌──────────┐  ┌──────────────┐
        │ diff 内容 │  │skill 文档│  │ 已有 case 库  │
        │          │  │(MO 知识库)│  │ (6类测试)     │
        └────┬─────┘  └────┬─────┘  └──────┬───────┘
             │             │               │
             └─────────────┼───────────────┘
                           │
                           ▼
                ┌──────────────────────┐
                │    Copilot Agent     │
                │                      │
                │  1. 理解 diff 改了什么 │
                │  2. 对照 skill 文档    │
                │  3. 判断影响哪些功能域  │
                │  4. 检查 6 类测试覆盖   │
                │  5. 生成缺失的 case    │
                │  6. 去重              │
                └──────────┬───────────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
      ┌──────────┐  ┌──────────────┐  ┌────────────┐
      │ PR → MO  │  │ PR → nightly │  │ PR → nightly│
      │ (BVT)    │  │ main 分支     │  │ big_data    │
      └──────────┘  └──────────────┘  └────────────┘
```

---

## 三、6 类测试详解

### 3.1 BVT 测试
- **仓库：** matrixone
- **路径：** `test/distributed/cases/`
- **类别（70+）：**

```
analyze, array, auto_increment, benchmark, charset_collation, comment,
cte, database, ddl, distinct, disttae, dml, dtype, expression, fake_pk,
feature_limit, foreign_key, fulltext, function, hint, join, keyword,
load_data, log, metadata, mo_cloud, operator, optimistic, optimizer,
pessimistic_transaction, pg_cast, pitr, plan_cache, plugin, prepare,
procedure, query_result, recursive_cte, replace_statement, result_count,
sample, save_query_result, security, sequence, set, snapshot, sql_inject,
sql_source_type, stage, statement_query_type, subquery, system,
system_variable, table, temporary, tenant, time_window, udf, union,
util, vector, view, window, zz_accesscontrol, ...
```

- **case 格式：** `.test` 文件（SQL 语句 + mo-tester 标签）
- **补充方式：** 在对应类别目录下新增/修改 `.test` 文件 → 提 PR 到 matrixone

### 3.2 稳定性测试
- **仓库：** mo-nightly-regression (main 分支)
- **Job：** [stability workflow](https://github.com/matrixorigin/mo-nightly-regression/actions/runs/23894864579/workflow)
- **测试项：**
  - TPCH — 标准分析型基准测试
  - TPCC — 标准 OLTP 基准测试
  - Sysbench — 高并发压测
  - Fulltext-vector — 全文+向量检索
  - Vector IVF + DML concurrency — 向量索引并发写入
- **目的：** 测试 MO 在长时间运行下的稳定程度
- **补充方式：** 提 PR 到 mo-nightly-regression 修改 workflow 或配置

### 3.3 Chaos 测试
- **仓库：** mo-nightly-regression (main 分支)
- **Job：** [chaos workflow](https://github.com/matrixorigin/mo-nightly-regression/actions/runs/24126717463/workflow)
- **配置目录：** `mo-chaos-config/`
- **测试项：** sysbench、tpcc、fulltext
- **故障注入类型：**
  - 杀 CN（计算节点）
  - 杀 DN/TN（数据节点）
  - 杀 LogService
  - 其他（网络分区、磁盘故障等）
- **补充方式：** 提 PR 到 mo-nightly-regression 修改 `mo-chaos-config/` 或 workflow

### 3.4 大数据量测试
- **仓库：** mo-nightly-regression (**big_data 分支**)
- **路径：** `tools/mo-regression-test/cases/big_data_test/`
- **特点：** 从云上 load 大规模数据，验证大数据量下的正确性和性能
- **补充方式：** 提 PR 到 mo-nightly-regression 的 big_data 分支

### 3.5 PITR 测试
- **仓库：** mo-nightly-regression (main 分支)
- **Workflow：** [pitr-backup-restore-regression-main.yml](https://github.com/matrixorigin/mo-nightly-regression/actions/workflows/pitr-backup-restore-regression-main.yml)
- **测试内容：** Point-In-Time Recovery 备份恢复功能
- **补充方式：** 提 PR 增加 PITR 场景 case

### 3.6 Snapshot 测试
- **仓库：** mo-nightly-regression (main 分支)
- **Workflow：** [snapshot_backup_restore_main.yml](https://github.com/matrixorigin/mo-nightly-regression/actions/workflows/snapshot_backup_restore_main.yml)
- **测试内容：** Snapshot 备份恢复功能
- **补充方式：** 提 PR 增加 snapshot 场景 case

---

## 四、Skill 文档（MO 知识库）

### 4.1 定位
Skill 文档是 **AI 理解 MO 底层实现的知识来源**。AI 不是靠路径映射规则，而是靠阅读这个文档来理解"改了某段代码意味着什么、可能影响什么"。

### 4.2 内容结构（建议）

```
docs/ai-skills/
├── architecture.md          # MO 整体架构（CN/TN/LogService/Proxy）
├── storage-engine.md        # 存储引擎（TAE、DisttaE、对象存储）
├── transaction.md           # 事务模型（乐观/悲观、MVCC、锁服务）
├── sql-engine.md            # SQL 引擎（parser → plan → compile → execute）
├── backup-restore.md        # 备份恢复（PITR、Snapshot 实现原理）
├── cdc.md                   # CDC 实现
├── fulltext-vector.md       # 全文检索 + 向量索引
├── multi-cn.md              # 多 CN 架构 + 负载均衡
├── fileservice.md           # FileService 对象存储抽象层
├── testing-guide.md         # 测试体系总览（6 类测试怎么跑）
├── test-env-setup.md        # 测试环境配置（docker 启动、多 CN、flush/gc 加速）
└── module-test-mapping.md   # 各模块与测试类型的关联
```

### 4.3 关键文档示例：`module-test-mapping.md`

```markdown
# 模块 → 测试关联

## 事务模块 (pkg/txn/, pkg/lockservice/)
- **BVT:** pessimistic_transaction, optimistic
- **稳定性:** tpcc (长事务场景), sysbench (高并发事务)
- **Chaos:** 杀 TN 后事务恢复, 杀 CN 后事务回滚
- **PITR:** 事务一致性点恢复

## 存储引擎 (pkg/vm/engine/tae/, pkg/vm/engine/disttae/)
- **BVT:** disttae
- **稳定性:** tpch (大量扫描), tpcc (频繁写入)
- **Chaos:** 杀 TN 后数据一致性
- **大数据:** 大规模 load + 查询
- **Snapshot:** 存储层 snapshot 一致性

## SQL 计划 (pkg/sql/plan/)
- **BVT:** optimizer, join, subquery, cte, window, hint, plan_cache
- **稳定性:** tpch (复杂查询计划)
- **大数据:** 大数据量下查询计划选择

## CDC (pkg/cdc/)
- **BVT:** cdc
- **稳定性:** 长时间 CDC 同步
- **Chaos:** 杀 CN/TN 后 CDC 恢复

## 备份恢复 (pkg/frontend/ snapshot/pitr 相关)
- **BVT:** pitr, snapshot
- **PITR:** 全量 PITR 测试
- **Snapshot:** 全量 snapshot 测试
```

### 4.4 维护方式
- **初始版本：** 根据 MO 架构和代码编写
- **持续补充：** 随着对 MO 理解加深，不断在 skill 文档中补充新知识
- **内容几乎固定：** 底层架构不会频繁变动，文档更新频率低

---

## 五、AI Agent 工作流程

### 5.1 输入
```
用户指定: PR #24088
```

### 5.2 步骤

```
Step 1: 获取 diff
    gh pr diff 24088 --repo matrixorigin/matrixone

Step 2: 解析变更
    变更文件:
    - pkg/sql/plan/function/func_compare_fix.go (修改了 getAsFloat64Slice, isNumericType)
    - pkg/sql/plan/opt_misc.go (修改了 remapHavingClause, remapWindowClause)
    - pkg/sql/plan/query_builder.go (修改了 remapAllColRefs)

Step 3: 阅读 skill 文档
    → 读 sql-engine.md → 了解 plan 模块在 SQL 执行流程中的位置
    → 读 module-test-mapping.md → 了解 pkg/sql/plan/ 关联的测试类型
    → 读 testing-guide.md → 了解各类测试怎么跑、case 格式是什么

Step 4: 分析影响
    → 这个改动涉及：类型比较(decimal vs integer) + 窗口函数列引用 remap
    → 功能域：SQL 类型系统、窗口函数、HAVING 子句

Step 5: 检查已有 case 覆盖情况
    → BVT optimizer/ 目录: 有窗口函数基础 case，但没有 decimal vs integer 混合比较
    → BVT window/ 目录: 有基础窗口函数 case，没有子查询 filter 下推场景
    → 稳定性 tpch: tpch 查询不涉及 decimal/integer 混合比较，不需要补
    → 大数据/chaos/PITR/snapshot: 不直接相关

Step 6: 生成缺失 case
    → 需要补充 BVT case:
    1. window/ 目录: decimal 列窗口函数 + integer filter
    2. optimizer/ 目录: HAVING 子句列 remap 场景
    → 其他 5 类测试: 不需要补充

Step 7: 去重
    → 与 window/ 目录已有 .test 文件比对，确认不重复

Step 8: 自动提 PR
    → 提 PR 到 matrixone，在 test/distributed/cases/window/ 下新增 case
```

### 5.3 输出

#### 分析报告（评论到 PR 或输出到终端）
```
## PR #24088 测试覆盖分析

### 变更摘要
修复 valueDec128Compare 在 decimal 与 integer 类型比较时的 panic。
涉及模块: SQL 类型比较、窗口函数列引用 remap、HAVING 子句。

### 6 类测试覆盖情况

| 测试类型 | 覆盖状态 | 说明 |
|---------|---------|------|
| BVT | ⚠️ 需补充 | window/ 缺少 decimal vs integer 混合比较场景 |
| 稳定性 | ✅ 已覆盖 | tpch/tpcc 间接覆盖类型比较 |
| Chaos | ➖ 不相关 | 非故障注入相关改动 |
| 大数据 | ➖ 不相关 | 非数据量相关改动 |
| PITR | ➖ 不相关 | 非备份恢复相关改动 |
| Snapshot | ➖ 不相关 | 非快照相关改动 |

### 自动补充
已提 PR #XXXXX 到 matrixone，补充 2 个 BVT case:
- test/distributed/cases/window/decimal_filter.test
- test/distributed/cases/optimizer/having_remap.test
```

---

## 六、实现路径

### Phase 1: Skill 文档编写
- [ ] 编写 `architecture.md` — MO 整体架构概览
- [ ] 编写 `sql-engine.md` — SQL 引擎模块详解
- [ ] 编写 `storage-engine.md` — 存储引擎详解
- [ ] 编写 `transaction.md` — 事务模型
- [ ] 编写 `backup-restore.md` — 备份恢复原理
- [ ] 编写 `testing-guide.md` — 6 类测试操作指南
- [ ] 编写 `module-test-mapping.md` — 模块→测试关联表
- [ ] 编写 `test-env-setup.md` — 测试环境配置（docker/多CN/flush/gc）

### Phase 2: Agent 基础能力
- [ ] 实现 diff 获取（`gh pr diff`）
- [ ] 实现已有 case 扫描（遍历 `test/distributed/cases/` + mo-nightly-regression）
- [ ] 实现 AI 调用（Copilot API / GitHub Models）
- [ ] 实现 prompt 工程（diff + skill 文档 + 已有 case → 分析 + 生成）

### Phase 3: 自动提 PR
- [ ] 实现 BVT case 生成 → 提 PR 到 matrixone
- [ ] 实现 nightly case 生成 → 提 PR 到 mo-nightly-regression
- [ ] SQL 去重（复用 V1 的 dedup 模块）

### Phase 4: 迭代优化
- [ ] 持续补充 skill 文档
- [ ] 根据实际效果调优 prompt
- [ ] 支持更多测试类型的 case 生成

---

## 七、Copilot 集成方式

### 方案 A: Copilot Agent（推荐）
在仓库中配置 `.github/copilot-instructions.md` + skill 文件，通过 Copilot Chat 直接交互：

```
用户: @workspace 分析 PR #24088 的测试覆盖情况，参考 docs/ai-skills/ 下的文档

Copilot: 
  1. 获取 diff...
  2. 阅读 skill 文档...
  3. 分析结果: BVT 缺少 window/decimal 场景...
  4. 生成 case...
```

### 方案 B: GitHub Actions + LLM API
在 workflow 中调用 Copilot API，全自动执行：

```yaml
- name: Analyze test coverage
  run: |
    DIFF=$(gh pr diff $PR_NUMBER)
    SKILLS=$(cat docs/ai-skills/*.md)
    # 调用 Copilot API
    curl -X POST $COPILOT_API \
      -d "{\"messages\": [{\"role\":\"system\", \"content\":\"$SKILLS\"}, 
                          {\"role\":\"user\", \"content\":\"分析以下diff的测试覆盖: $DIFF\"}]}"
```

### 方案 C: 本地 CLI + LLM
扩展 `mo-testplan` CLI，加入 LLM 调用：

```bash
mo-testplan analyze --pr 24088 --skills docs/ai-skills/ --model copilot
```

**建议先走方案 A**（Copilot Agent），最轻量，不需要写多少代码，主要工作量在 skill 文档上。验证效果后再考虑 B 或 C 做自动化。

---

## 八、与 V1 代码的关系

| V1 模块 | V2 中的去留 |
|--------|-----------|
| `pkg/testinfra/types/` | ⚠️ 可能需要扩展（增加 6 类测试类型） |
| `pkg/testinfra/planner/diff.go` | ❌ 不需要（直接用 `gh pr diff` 原文喂给 AI） |
| `pkg/testinfra/planner/mapping.go` | ❌ 废弃（用 skill 文档替代静态映射） |
| `pkg/testinfra/planner/planner.go` | ❌ 废弃（AI 直接生成分析结果） |
| `pkg/testinfra/dedup/` | ✅ 保留（生成 case 后仍需去重） |
| `pkg/testinfra/executor/` | ❌ 暂不需要（V2 只管生成 case，不管执行） |
| `cmd/mo-testplan/` | ⚠️ 如果走方案 C 则需要重写 |
| `.github/workflows/testplan.yaml` | ⚠️ 如果走方案 B 则需要重写 |
