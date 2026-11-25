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

## Vector 内存管理模型（`pkg/container/vector/`）

以下是 Vector 内存的所有权与生命周期约定，所有执行算子、存储读取路径都必须遵守。

### 所有权

- **每个 Vector 都有一个所有者（owner）**。查询执行期间，所有者通常是某个算子（Operator）。
- 所有者持有一个 mpool，它创建的 Vector 都用这个 mpool 创建（`vector.NewVec(typ, mp)`）。Vector 记住这个 mpool（`v.MPool()`），之后它的全部内存分配都来自这个 mpool，并且都是 off-heap。因此，没有 Free 的 Vector 就是真实的内存泄漏，不会被 GC 回收。
- **mpool 严格匹配**：对 Vector 的调用如果传入 `mp`，它必须与 Vector 创建时的 mpool 是同一个指针。即使是同一个池的 view（例如 `WithAccount`），也算不匹配。不匹配时返回错误，`Free` 中直接 panic。不要为了让调用通过而放宽检查，应当修正调用方，让 Vector 在创建时就使用正确的 mpool。
- **nil mpool 表示只读引用**：如果 Vector 只是引用一块不属于它的内存（例如 FileService cache、Arrow buffer），创建时传 nil mpool。这样的 Vector 保证只读：任何修改或分配都会报错，`Free` 只释放引用。

### 算子与 mpool

- 需要缓存大量数据的算子（sort/order、top、group、hash build 等）创建**自己的 mpool**（`colexec.NewOperatorMPool`），用于记账（accounting）和整体 reset。其他算子直接使用进程的 mpool（`proc.Mp()`）。
- **一个算子创建多少个 Vector 由查询（计划）决定，与处理的数据量无关**。Vector 只创建一次，数据从中流过（每次 `Next` 复用，例如 `CleanOnlyData` 后重新填充）。在每个 batch 上创建新的 Vector（例如每次 `Call` 都 `Dup`/复制 window）是坏味道。

### Batch 生命周期与修改规则

- Vector 通过 Batch 在算子之间传递。生命周期承诺：**`Next` 返回的 batch 一直有效，直到下一次调用 `Next`**。
- 算子**可以**把输入 batch 中的 Vector 取出、直接放进自己的输出 batch（pass-through），因为上面的承诺保证它活得足够久。
- 算子**只释放自己创建的 Vector**，不释放 pass-through 进来的 Vector。
- 算子如果需要**修改** Vector，必须创建自己的新 Vector，把修改后的数据写进去。**禁止原地修改输入 Vector。**
- `Window`（vector 和 batch）返回只读引用（nil mpool），不分配、不复制，与 pass-through 一样只在源 Vector 不变期间有效（对输入来说，直到下一次 `Next`）。释放 window 只是丢弃引用（`Free(nil)`、`batch.FreeWindow()`）。需要修改这些行，或者需要保留得更久时，用 `CloneWindow`/`CloneWindowTo` 复制到自己的 Vector 中。
- 不要用 `vec.Free(vec.MPool())` 或 `batch.CleanOwned()` 这类“按 Vector 自己的池释放”的写法绕过 mpool 检查（`CleanOwned` 只给测试用）。
- 写入事务 workspace（`Transaction.WriteBatch`）时，workspace 复制数据，调用方仍然拥有并负责释放自己的 batch。

### 表扫描

- 从 buffer pool（cache）读出的 Vector，要么使用 nil mpool（只读引用），要么使用 buffer pool 专用的 mpool。
- 这保证查询执行无法修改这些 Vector：任何算子的 mpool 都与它们不同，严格的 mpool 检查会拒绝修改。

### ⚠️ 修改 Vector 结构体需要格外谨慎

从现在开始，修改 `vector.Vector` 必须格外谨慎。**除非确有必要，不要给 Vector 增加任何成员（字段）。** Vector 应当是一个简单的容器：类型、长度、数据、area、nulls、以及唯一的所有者 mpool。借用标记、租约、计数、偏移、额外的分配状态等，都不应该放进 Vector。这类状态应当由所有者（算子、cache、lender）持有。

**如果确实必须给 Vector 增加成员，必须停下来，请人类（human）做仔细的 review，得到确认后才能继续。** AI agent 不得自行决定给 Vector 增加字段。
