# SQL 引擎

## 执行流水线

```
SQL 文本 → Parser → AST → Planner → Plan Tree → Compiler → Scope/Pipeline → Executor → 结果
```

## 各阶段详解

### Parser（`pkg/sql/parsers/`）
- SQL → AST 转换（基于 bison/flex）
- MySQL 方言兼容
- 输出：语法树节点

### Planner（`pkg/sql/plan/`）
- 逻辑计划生成
- 类型推导与验证
- 查询优化（代价估算、谓词下推、连接重排）
- 分区裁剪集成
- 关键子模块：
  - `function/` — 函数注册与类型匹配
  - `opt_misc.go` — 各种优化 pass（HAVING remap、窗口函数 remap 等）

### Compiler（`pkg/sql/compile/`）
- Plan Tree → 可执行 Pipeline
- 核心类型：`Compile` 结构体
- `Scope` — 执行单元（本地/远程/Load/表函数）
- 负责跨 CN 的远程执行规划
- DDL/DML/TCL 翻译

### Executor（`pkg/sql/colexec/`）
- 列式执行引擎
- `ExpressionExecutor` 接口
- 实现：`FixedVectorExpressionExecutor`, `FunctionExpressionExecutor`, `ColumnExpressionExecutor`
- 批量向量化处理

## VM Pipeline（`pkg/vm/`）

```
Scope → Operator → Operator → ... → Output
```

- 每个 SQL 算子对应一个 Operator（scan, filter, join, agg, sort, limit...）
- Pipeline 模式执行（pull-based）

## 与测试的关联

| 变更范围 | 影响的测试 |
|---------|----------|
| Parser | BVT: ddl, dml, prepare |
| Plan 优化 | BVT: optimizer, hint, plan_cache, join, subquery, cte, window |
| Compile/Scope | BVT: expression, function; 稳定性: tpch |
| 类型系统 | BVT: dtype, pg_cast, array, vector |
| 执行引擎 | 大数据量测试（大规模扫描/聚合） |
