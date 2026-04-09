# 测试体系总览

## 6 类测试

### 1. BVT 测试（轻量级回归）
- **仓库：** matrixone
- **路径：** `test/distributed/cases/`
- **工具：** mo-tester
- **Case 格式：** `.test` 文件（SQL + 标签）
- **运行时机：** 每个 PR 的 CI

### 2. 稳定性测试（长时间运行）
- **仓库：** mo-nightly-regression (main)
- **测试项：** TPCH, TPCC, Sysbench, Fulltext-vector, Vector IVF+DML
- **目的：** 验证长时间运行下的稳定性
- **运行时机：** Nightly

### 3. Chaos 测试（故障注入）
- **仓库：** mo-nightly-regression (main)
- **配置：** `mo-chaos-config/`
- **测试项：** Sysbench, TPCC, Fulltext（叠加故障注入）
- **故障类型：** 杀 CN、杀 TN、杀 LogService、网络分区
- **运行时机：** Nightly

### 4. 大数据量测试
- **仓库：** mo-nightly-regression (**big_data 分支**)
- **路径：** `tools/mo-regression-test/cases/big_data_test/`
- **特点：** 从云端 load 大规模数据后执行测试
- **运行时机：** 定期

### 5. PITR 测试
- **仓库：** mo-nightly-regression (main)
- **Workflow：** `pitr-backup-restore-regression-main.yml`
- **内容：** Point-In-Time Recovery 完整流程验证
- **运行时机：** Nightly

### 6. Snapshot 测试
- **仓库：** mo-nightly-regression (main)
- **Workflow：** `snapshot_backup_restore_main.yml`
- **内容：** Snapshot 备份恢复完整流程验证
- **运行时机：** Nightly

## BVT Case 格式

`.test` 文件示例：
```sql
-- @bvt:issue#12345
SELECT 1;
-- @bvt:issue#12345

-- @session:id=1
BEGIN;
INSERT INTO t1 VALUES (1);
-- @session

-- @sortkey:0,1
SELECT * FROM t1 ORDER BY id;

-- @ignore:1
SELECT NOW(), COUNT(*) FROM t1;
```

**常用标签：**
| 标签 | 说明 |
|------|------|
| `@bvt:issue#N` | 跳过指定区块 |
| `@skip:issue#N` | 跳过整个文件 |
| `@session:id=N` | 并发会话 |
| `@wait:session:commit/rollback` | 等待会话提交/回滚 |
| `@sortkey:cols` | 结果排序（消除不确定顺序）|
| `@ignore:cols` | 忽略指定列（如时间戳）|

## BVT 类别（70+）

核心 SQL: `dml`, `ddl`, `database`, `table`, `view`, `sequence`
查询: `expression`, `function`, `subquery`, `cte`, `recursive_cte`, `window`, `join`
事务: `pessimistic_transaction`, `optimistic`, `snapshot`
高级功能: `fulltext`, `vector`, `udf`, `procedure`
特性: `partition`, `charset_collation`, `foreign_key`, `temporary`, `tenant`
数据操作: `load_data`, `replace_statement`, `prepare`, `hint`
基础设施: `pitr`, `disttae`, `log`, `metadata`, `stage`
