# StringSource 边界清单

## 不变量

透明边界逐行保留 `StringSource`；选择边界保留被选中 arm 的来源；组合边界使用其显式 merge 规则；所有改变行数、顺序或表示的边界必须同步处理来源 metadata。无来源 metadata 的 hot path 不分配 sidecar，也不增加 wire payload。

## 边界与 owner

| 边界 | Owner | 规则 | 验证 |
|---|---|---|---|
| Literal、user variable、SQL PREPARE、COM_STMT 创建 | `pkg/sql/colexec/evalExpression.go`、`pkg/frontend/computation_wrapper.go` | 创建对应 uniform source；NULL 同样携带来源 | expression/frontend UT；公共 SQL fresh/reuse |
| vector→scalar literal | `pkg/sql/plan/rule/constant_fold.go` | 目标 row 来源编码进 `Literal.StringSource`；NULL 不例外 | rule UT；remote expression protobuf round-trip |
| scalar/list literal→vector | `pkg/sql/colexec/evalExpression.go` | 解码 uniform 或逐项来源；非法值确定性报错 | colexec UT；constant-list UT |
| CAST | `pkg/sql/colexec/evalExpression.go`、`evalExpressionReset.go` | implicit CAST 透明；explicit CAST 生成 Expression | ordinary/selection/fold/reset UT |
| COALESCE | `pkg/sql/colexec/evalExpression.go` | 每行保留 selected arm 来源 | 独立 colexec UT；公共 SQL first/second arm |
| IF、CASE、IFNULL | `pkg/sql/colexec/evalExpression.go` | 按 common-domain flow-control 规则逐行传播 | colexec UT；公共 SQL control |
| append、union、window、shrink、clone | `pkg/container/vector/vector.go` | 与值相同的选择/复制映射；失败不发布部分 metadata | vector lifecycle/allocation UT |
| sort、compact、distinct constant list | `pkg/container/vector/vector.go`、`pkg/sql/plan/rule/constant_fold.go` | 排序交换完整 row metadata；仅完整语义相等才去重；uniform source 不分配 sidecar | fixed/varlen/mixed vector UT；planner list UT |
| batch transport | `pkg/container/batch/batch.go`、dispatch/remote result | MORPC v25 保留来源；旧 peer 丢弃 source-only metadata，旧 prepared metadata 仍拒绝 | batch/dispatch/remoterun UT |
| selected-row spill | `pkg/container/vector/selection_codec.go` | 编解码选中 row 的精确来源；未知值拒绝 | selection codec UT |
| aggregate/group/window state | `pkg/sql/colexec/aggexec`、`group` | state 编码/merge 使用显式 source merge；旧协议降级 | agg/group/window UT |
| stable vector decode reuse | `pkg/container/vector/vector.go` | stable 格式不携带来源；decode 前清除旧来源 | vector decode reuse UT |
| public local/derived/materialized/remote scan | SQL planner、compile remote scope、storage materialization | 每次只加入一个边界，结果 bytes 与本地控制等价 | `string_source_provenance.test` |

## 公共 SQL 矩阵

| 对照 | 唯一变化边界 | Oracle |
|---|---|---|
| direct `@var` vs derived-table `@var` | derived projection/materialization | `HEX` 完全相等 |
| direct value vs stored table scan | storage materialization；multi-CN BVT 下包含 remote scan | id 对应 bytes/NULL 不变 |
| COALESCE first vs second arm | selected arm | 被选中 bytes 正确 |
| COALESCE vs IF/CASE/IFNULL | flow-control merge policy | 相同输入得到相同 bytes，COALESCE 单独执行 |
| prepared fresh vs repeated execute | executor reset/reuse | non-NULL 与 NULL 两组均逐次等价 |
