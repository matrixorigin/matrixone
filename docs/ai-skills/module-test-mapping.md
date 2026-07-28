# 模块 → 测试关联

本文档描述 MO 各代码模块与 6 类测试的关联关系。AI 分析 diff 时，根据变更文件所属模块查找需要关注的测试类型。

## SQL 引擎

### pkg/sql/plan/
- **BVT:** optimizer, plan_cache, join, subquery, cte, recursive_cte, hint, window
- **稳定性:** tpch（复杂查询计划）
- **大数据:** 大数据量下查询计划选择

### pkg/sql/compile/
- **BVT:** ddl, dml, function, expression
- **稳定性:** tpch, tpcc

### pkg/sql/colexec/
- **BVT:** function, expression, join, window

### pkg/sql/parsers/
- **BVT:** ddl, dml, prepare

## 存储引擎

### pkg/vm/engine/disttae/
- **BVT:** disttae, pessimistic_transaction, optimistic
- **稳定性:** tpcc, sysbench
- **Chaos:** 杀 TN 后数据一致性
- **Snapshot:** 存储层快照一致性
- **PITR:** 存储层恢复

### pkg/vm/engine/tae/
- **BVT:** disttae, pessimistic_transaction
- **稳定性:** tpcc（频繁写入 flush）
- **Chaos:** 杀 TN 后恢复
- **PITR:** checkpoint 恢复

### pkg/objectio/
- **BVT:** load_data, disttae
- **大数据:** 大规模数据读写

## 事务

### pkg/txn/
- **BVT:** pessimistic_transaction, optimistic
- **稳定性:** tpcc, sysbench（高并发事务）
- **Chaos:** 杀 TN 后事务恢复

### pkg/lockservice/
- **BVT:** pessimistic_transaction
- **Chaos:** 死锁场景、杀节点后锁恢复

## 前端

### pkg/frontend/
- **BVT:** security, tenant, zz_accesscontrol, snapshot, pitr, system_variable, set
- **PITR:** 前端 PITR 命令处理
- **Snapshot:** 前端 snapshot 命令处理

## 数据类型

### pkg/container/
- **BVT:** dtype, array, vector

## 全文 / 向量

### pkg/fulltext/
- **BVT:** fulltext
- **稳定性:** fulltext-vector

### pkg/vectorindex/
- **BVT:** vector
- **稳定性:** vector IVF+DML concurrency
- **Chaos:** fulltext 故障场景

## CDC

### pkg/cdc/
- **BVT:** cdc
- **稳定性:** 长时间 CDC 同步
- **Chaos:** 杀 CN/TN 后 CDC 恢复

## 备份恢复

### pkg/backup/
- **PITR:** 备份元数据
- **Snapshot:** 快照管理

## 基础设施

### pkg/fileservice/
- **BVT:** stage, load_data
- **大数据:** 云端数据 load

### pkg/partition/
- **BVT:** ddl, dml（分区表）

### pkg/proxy/
- **BVT:** tenant（路由）
- **Chaos:** 杀 CN 后连接恢复

### pkg/bootstrap/
- **BVT:** system

### pkg/catalog/
- **BVT:** ddl, database, table, system

### pkg/udf/
- **BVT:** udf

### pkg/stage/
- **BVT:** stage

### pkg/logservice/
- **Chaos:** 杀 LogService 场景
- **PITR:** WAL 日志完整性

## BVT 测试目录索引

```
test/distributed/cases/
├── ddl/                # CREATE/ALTER/DROP 语句
├── dml/                # INSERT/UPDATE/DELETE
├── optimizer/          # 查询优化器
├── join/               # JOIN 语法和优化
├── subquery/           # 子查询
├── window/             # 窗口函数
├── function/           # 内置函数
├── expression/         # 表达式计算
├── pessimistic_transaction/  # 悲观事务
├── optimistic/         # 乐观事务
├── disttae/            # 分布式存储引擎
├── fulltext/           # 全文检索
├── vector/             # 向量索引
├── cdc/                # CDC
├── pitr/               # PITR
├── snapshot/           # Snapshot
├── load_data/          # 数据加载
├── tenant/             # 多租户
├── security/           # 安全/权限
└── ...                 # 其他 50+ 类别
```
