# #25347 UPDATE IGNORE 重复键修复方案

## 结论

[#25347](https://github.com/matrixorigin/matrixone/issues/25347) 是真问题，根因跨 parser 和 planner 两层：

- MySQL grammar 接受 `UPDATE ... IGNORE`，但 `ignore_opt` 的结果没有写入 AST；
- `tree.Update` 没有 `Ignore` 字段，format/prepare round-trip 也无法保留；
- `pkg/sql/plan/bind_update.go` 对 PK/UK dedup join 始终使用 `plan.Node_FAIL`。

因此用户写了 IGNORE，执行计划仍按普通 UPDATE 在唯一键冲突时报错。

## 本期语义边界

本期只承诺 duplicate-key 语义：会产生 PK/UK 冲突的目标行保持原值，其他不冲突目标行正常更新。

MySQL 的 UPDATE IGNORE 还可能把部分类型转换/截断错误降为 warning。除非另行实现完整 warning 转换，本 PR 不应宣称完全支持所有 MySQL IGNORE 行为。

## 正确性不变量

1. 被忽略的目标行既不能删除旧行，也不能插入新行或更新 hidden index。
2. 多个目标行更新到同一个 key 时，赢家选择必须确定，且与 pipeline 调度无关。
3. PK、每个 UNIQUE key、复合 key 和 NULL 语义与 INSERT IGNORE 一致。
4. 普通 UPDATE 行为完全不变。
5. prepared statement、format/parse 和 multi-table/update-from 路径不能丢失 IGNORE。
6. 受影响行数只计算实际更新的行，warning 计数与产品定义一致。

## 实施方案

### 1. AST 和 parser 贯通

- 给 `tree.Update` 增加 `Ignore bool`。
- 让 `ignore_opt` 返回布尔值，并在所有 UPDATE grammar production 中写入 AST。
- `tree.Update.Format` 在字段为 true 时输出 `IGNORE`。
- 按仓库生成流程重新生成 MySQL parser 文件，不手改生成物。
- 补 parse→format→parse 和 prepared statement 测试。

### 2. Planner 选择冲突动作

在 `bind_update.go` 构建 PK/UK dedup join 时：

```text
stmt.Ignore == false -> Node_FAIL
stmt.Ignore == true  -> Node_IGNORE
```

所有 unique key 必须共享同一个 surviving-row set。不能让某行先通过主键检查并修改主表，之后才被 unique index 检查忽略。

### 3. 保证 old-row 原子更新

UPDATE 物理上可能是 delete old + insert new。冲突过滤必须位于这两者共同的上游：

```text
candidate rows -> compute new values -> all-key conflict resolution
               -> surviving rows -> delete old + insert new + maintain indexes
```

对于多个 candidate 相互冲突，使用稳定 row identity/order 选择赢家；禁止依赖并行到达顺序。

## 预计改动

- `pkg/sql/parsers/tree/update.go`
- `pkg/sql/parsers/dialect/mysql/mysql_sql.y` 及生成物
- `pkg/sql/plan/bind_update.go`
- parser、plan 和 distributed DML tests

## 测试计划

1. 单行更新与已有 PK/UK 冲突：该行不变，语句成功。
2. 一条语句多个目标行收敛到同一个 key：只有确定赢家更新。
3. 同一行分别与两个 unique index 中不同已有行冲突：整行忽略。
4. 复合 UK 和 NULL 语义。
5. secondary/hidden index、generated column、FK。
6. multi-table/update-from、带 ORDER/LIMIT 的支持路径、prepared statement。
7. 普通 UPDATE 保持 duplicate error 和全语句 rollback。
8. 小 batch、多 pipeline、race test 下结果稳定。
9. parse/format/explain 保留 `IGNORE`。

## 验收标准

- issue 最小复现通过，冲突行的所有列和 index 均未变化。
- 普通 UPDATE 无语义变化。
- AST、prepared statement、explain 不再丢失 IGNORE。
- 同一数据在不同并行度和 batch size 下结果一致。
- 文档明确本期仅覆盖 duplicate-key IGNORE。

## 实现复盘与后续治理

本次修复后续又暴露出多轮问题，并非单个条件判断遗漏，而是 dedup join 的数据模型依赖了多项没有被显式表达和统一维护的协议：

- `hash group g` 被隐式当作物理行 `g-1`。过滤、收缩或重建 batch 后，这个映射会失效；nullable unique key 尤其容易触发 group 与 row 错位。
- `Node_IGNORE` 同时服务 INSERT 和 UPDATE，但 UPDATE 还需要排除当前旧行、延迟释放旧 key，并确保 delete/insert/index maintenance 使用同一 survivor set；复用 action 枚举并没有表达这些额外语义。
- `OldColList`、`DelColIdx` 等通过列位置传递协议，缺少结构化校验，planner 与 executor 任一侧改列布局都可能静默破坏旧行识别。
- survivor、batch shrink、hash map、`DelRows` 和 `GroupSels` 是同一份派生状态，却曾被分散修改，导致收缩后继续沿用旧下标或旧 hash group。
- 多个唯一约束是分阶段检查的。前一约束淘汰的行不能在后一阶段被误认为仍会释放旧 key；最终决策必须基于最终 survivor 集合，而不是局部阶段状态。
- shuffle 能力判断和 iterator cache 的重置依赖调用方自觉，生命周期契约不够集中，增加了 uncommon pipeline 路径上的风险。

实际发现并修复的失败模式包括：跨约束错误释放旧 key、unchanged-key owner 顺序依赖、batch shrink 后 build-row/group 错位、以及 nullable unique key 的 group-to-row 映射错误。

后续应优先进行以下治理：

1. 用显式 row identity / group-to-row 映射代替 `g-1` 约定，并在 shrink 后原子重建所有派生状态。
2. 将 UPDATE IGNORE 的 old-row/self-exclusion 和 survivor 语义建模为独立、可校验的执行契约，而不是仅依赖通用 `Node_IGNORE`。
3. 将位置型列协议封装为具名结构，并在 plan build、prepare 和 executor 初始化阶段校验。
4. 增加跨多个唯一约束、NULL、收缩、shuffle、不同 batch size 与 pipeline 顺序的性质测试；断言最终表与索引状态，不只断言内部数组。
