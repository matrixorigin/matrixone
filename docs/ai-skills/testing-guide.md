# 测试体系总览

## 6 类测试

### 1. BVT 测试（轻量级回归）
- **仓库：** matrixone
- **路径：** `test/distributed/cases/{category}/`
- **工具：** mo-tester
- **Case 格式：** `.test`/`.sql` + 对应 `.result` 文件
- **运行时机：** 每个 PR 的 CI
- **运行命令：** `cd mo-tester && ./run.sh -n -g -p /path/to/test.test`
- **生成 result：** `./run.sh -m genrs -n -g -p /path/to/test.test`

### 2. 稳定性测试（长时间运行）
- **仓库：** mo-nightly-regression (main)
- **Workflow：** `stability-test-on-distributed.yaml`
- **测试项：** TPCH, TPCC, Sysbench, Fulltext-vector, Vector IVF+DML
- **配置：** `cases/sysbench/{scenario}/run.yml` — mo-load YAML 配置
- **参数：** TPCHScale, LoadDataScale, TPCC_WarehouseNum, OLTPThreads, RunMinutes
- **目的：** 验证长时间运行下的稳定性

### 3. Chaos 测试（故障注入）
- **仓库：** mo-nightly-regression (main)
- **配置：** `mo-chaos-config/chaos_regression.yaml` — Chaos Mesh YAML
- **工作负载：** `mo-chaos-config/chaos_test_case.yaml` — 测试任务列表
- **故障类型：**
  - `PodChaos/pod-kill` — 杀 CN/TN/LogService Pod
  - `StressChaos/memory` — 内存压力
  - `NetworkChaos` — 网络分区
- **测试任务：** mo-tpcc, mo-load(sysbench), mo-cdc-test, mo-vector-fulltext-test
- **工作负载配置格式：** `mo-chaos-config/mo-load-insert-case/run.yml`

### 4. 大数据量测试
- **仓库：** mo-nightly-regression (main)
- **Workflow：** `big-data-test.yml`
- **路径：** `tools/mo-regression-test/cases/{customer_name}/`
- **配置格式：** `mo_ddl.yaml`（DDL source）, `mo_load_*.yaml`（数据加载）, `q00.sql`（查询）
- **Suite：** suite_1y（1年数据）, suite_10y（10年数据）, all
- **数据来源：** COS 对象存储 + load data

### 5. PITR 测试
- **仓库：** matrixone（BVT 级别）+ mo-nightly-regression（完整流程）
- **BVT 路径：** `test/distributed/cases/pitr/` — pitr.sql, pitr_basic.sql, pitr_inherit.sql
- **内容：** CREATE PITR → 操作数据 → RESTORE → 验证数据一致性

### 6. Snapshot 测试
- **仓库：** matrixone（BVT 级别）+ mo-nightly-regression（完整流程）
- **BVT 路径：** `test/distributed/cases/snapshot/` — 多层级 snapshot 测试
- **场景：** cluster/account/database/table 级别的 snapshot 创建和恢复
- **内容：** CREATE SNAPSHOT → 操作数据 → RESTORE ACCOUNT → 验证

## BVT Case 格式详解

### 文件结构
- `.test`/`.sql` — 测试文件（SQL + mo-tester 标签）
- `.result` — 期望输出（含列元数据 `column[type,precision,scale]`）
- 文件对应关系：`func_sum.test` ↔ `func_sum.result`

### 标签语法

**文件级标签：**
```sql
-- @skip:issue#16438        -- 跳过整个文件
--- @metacmp(false)         -- 关闭元数据比较（三个 -）
```

**SQL 级标签：**
```sql
-- @bvt:issue#3185          -- 开始跳过区块
SELECT * FROM t1;
-- @bvt:issue               -- 结束跳过区块

-- @ignore:0                -- 忽略第 0 列（0-indexed）
select time(now());

-- @ignore:5,6              -- 忽略多列
show publications;

-- @sortkey:0,1             -- 按第 0,1 列排序后比较
SELECT col1, col2 FROM t1;

-- @regex("pattern", true)  -- 结果必须匹配 pattern
show accounts;

-- @regex("error", false)   -- 结果不能匹配 pattern
SHOW TABLES;

-- @metacmp(true)           -- 单条 SQL 开启元数据比较
SELECT * FROM t1;
```

**会话标签（并发测试）：**
```sql
begin;
select * from t1;
-- @session:id=1&user=acc:admin&password=111{
insert into t1 values (100);
-- @wait:0:commit           -- 等待 session 0 commit
select * from t1;
-- @session}
commit;
```

### BVT Case 编写规范
1. **自包含** — 每个 test 文件独立运行
2. **资源清理** — 结尾 drop 创建的表/库
3. **确定性** — 不确定列用 `@ignore`，不确定顺序用 `@sortkey`
4. **复用库** — 避免创建过多临时 database

## 稳定性测试 Case 格式（mo-load）

```yaml
# cases/sysbench/simple_insert_10_100000/run.yml
duration: 10          # 运行分钟数
stdout: "console"
transaction:
  - name: "simple_insert"
    vuser: 10          # 并发数
    mode: 0            # 0=顺序执行, 1=封装为事务
    prepared: "false"
    script:
      - sql: "insert into sbtest{tbx} values({i_id},{kvalue},'...');"
```

## Chaos 测试配置格式

### 故障定义 (chaos_regression.yaml)
```yaml
chaos:
  cm-chaos:
    - name: task_kill_cn
      kubectl_yaml: |
        apiVersion: chaos-mesh.org/v1alpha1
        kind: PodChaos
        spec:
          action: pod-kill
          selector:
            labelSelectors:
              'matrixorigin.io/component': 'CNSet'  # 或 DNSet/LogSet
      times: 1
      interval: 30
      is_delete_after_apply: true
```

### 工作负载定义 (chaos_test_case.yaml)
```yaml
tasks:
  - name: mo-tpcc
    work-path: mo-tpcc
    run-steps:
      - command: ./runBenchmark.sh props.mo > tpcc.log 2>&1
    verify:
      - command: ./runVerify.sh props.mo >> check.log 2>&1
    verify-mode: parallel   # parallel(边跑边验)/after(跑完再验)
```

## 大数据测试 Case 格式

```yaml
# tools/mo-regression-test/cases/tpch_1g/mo_ddl.yaml
source_path: "/data/customer/tpch_1g/ddl/mo.sql"
load_type: "ddl"

# tools/mo-regression-test/cases/tpch_1g/mo_load_server_serial.yaml
s3_path: "cos://bucket/path/to/data.csv"
load_type: "load_server"
count: "6001215"

# tools/mo-regression-test/cases/tpch_1g/q00.sql
-- 标准 SQL 查询文件
SELECT * FROM lineitem WHERE l_shipdate <= '1998-09-02';
```
