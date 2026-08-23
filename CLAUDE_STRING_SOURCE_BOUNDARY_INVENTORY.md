# StringSource 边界清单

## 总不变量与 merge rule

`StringSource` 的合法集合为 Expression、Literal、UserVariable、SQLPrepare、COMStmt。透明边界逐行保持来源；选择边界采用实际被选中值的来源；多个值共同产生一个新值时，相同来源保持，否则合并为 Expression；显式语义 owner（例如 explicit CAST、一般函数结果、算术结果）产生 Expression。NULL 也携带来源。所有改变行数、顺序或表示的边界必须同步处理来源 metadata；未知枚举在发布状态前确定性失败。无 metadata 的 hot path 不分配 sidecar、不写 trailer。

## Expression 与 planner 边界

| 边界 | Owner | merge rule | Typed / public oracle |
|---|---|---|---|
| Literal、user variable、SQL PREPARE、COM_STMT 创建 | `pkg/sql/colexec/evalExpression.go`、`pkg/frontend/computation_wrapper.go` | 创建对应 uniform source，NULL 不例外 | colexec/frontend UT；prepared BVT fresh/reuse |
| projection / column wrapper | `ColumnExpressionExecutor`、projection operators | 透明逐行传播 | expression/vector UT；derived-table BVT |
| implicit / explicit CAST | `evalExpression.go`、`evalExpressionReset.go` | overload 0 透明；overload 1 为 Expression；ordinary/selection/fold 一致 | CAST ordinary/selection/fold/reset UT |
| arithmetic / string / general function wrapper | function executor 与各 kernel | kernel 生成新值时为 Expression；仅明确声明的透明 wrapper 传播 | expression consumer UT；BVT wrapper controls |
| COALESCE | `evalExpression.go` | 每行采用 selected arm 来源 | 独立 selected-arm UT/BVT |
| IF、CASE、IFNULL | `evalExpression.go` | common-domain：结果 source 为 Expression；runtime domain仍按被选中值相对公共静态类型计算；IFNULL rewrite 为 CASE | normal/selection/fold/reset flow-control UT；独立 BVT controls |
| vector → scalar literal / VALUE_SCAN scalar materialization | `rule.GetConstantValue`、`GetConstantValue2`、`constructValueScan` | raw-width校验后目标row来源编码/写入，NULL不例外；constant与dynamic RowsetExpr等价 | rule合法/invalid UT；VALUE_SCAN constant/dynamic/reset UT |
| scalar/list literal → vector | `GenerateConstExpressionExecutor`、`GenerateConstListExpressionExecutor` | 解码 uniform/逐项来源；非法值拒绝 | colexec UT；list-fold UT |
| LiteralVec → scalar/list | `decodeLiteralVec`、`inRHSValues` | 先验证并恢复外层 uniform source，再拆行；stable `Data` 不提供来源 | Expression/Literal/COMStmt/NULL/invalid UT；OR-IN/composite rewrite UT |
| scalar/list constant fold | `pkg/sql/plan/rule/constant_fold.go`、`pkg/sql/plan/utils.go` | 折叠前后 executable owner 相同；mixed-owner list 不降为只能表示 uniform source 的 LiteralVec | planner fold/deepcopy/hash/protobuf UT |
| 一般 planner rewrite / expression dedup | `expr_hash.go`、`expr_opt.go`、rewrite owners | executable identity 包含来源；仅 SQL value-domain 比较显式忽略来源 | structural hash/equality UT；filter-domain controls |

## Relational边界

| 边界 | Owner | merge rule | Oracle |
|---|---|---|---|
| derived table / projection materialization | planner projection、colexec materialization | 透明逐行传播 | derived-table BVT；materialization UT |
| CTE | CTE planning/materialization owner | producer→consumer 透明；多引用不共享可变 sidecar owner | planner/clone/materialization UT；inventory coverage control |
| join | join output vector append/union owner | copied columns透明；新计算表达式为 Expression | vector union/join package UT |
| SQL UNION / UNION ALL | set operator与group/vector owner | UNION ALL逐行透明；UNION仍按SQL值去重，不改变行数，同值代表来源按同源保持/异源→Expression合并 | group正反序与spill/reload typed UT；UNION public reachability BVT |
| DISTINCT | distinct/hash/group owner | SQL key identity忽略来源；重复key的代表来源按所有贡献行确定性合并 | group正反序、same/mixed source UT |
| GROUP BY | `pkg/sql/colexec/group` | key按SQL值分组；重复key同源保持、异源→Expression；与到达顺序、spill/reload无关 | hash-group正反序、spill/reload、reset UT |
| aggregate | `pkg/sql/colexec/aggexec` | MIN/MAX/MAX_BY winner replacement保留代表来源；equal candidate同源保持、异源→Expression；source merge独立于字符串runtime-domain | 五类 state round-trip；fixed numeric/date/decimal MIN/MAX、fixed/JSON/array MAX_BY、partial merge UT |
| window | window operator / aggregate state | value window按实际 lag/lead/first/last/nth row透明；窗口聚合使用 aggregate merge | `TestValueWindowExecPreservesRowStringSources`；window/agg UT |

## Vector 边界

| 边界 | Owner | merge rule | Oracle |
|---|---|---|---|
| append / append-list / append-null | `pkg/container/vector/vector.go` | payload 发布前预检来源；普通追加贡献 Expression | append rollback/allocation UT |
| clone / copy / window | vector clone/window helpers | 与值相同的 row/window 映射 | lifecycle/allocation UT |
| union / union-one / union-batch | vector union helpers | 与 selection/flags 相同映射；const 物理 row 正确展开 | union fast-path/selection UT |
| shrink / selection | vector shrink、selection codec | 按 sels/mask 精确映射 | row-transform/selection-codec UT |
| sort | metadata-aware sorter | 值、NULL、grouping、domain、kind、source 使用同一 swap | fixed/varlen/JSON/array UT |
| compact / DISTINCT-like dedup | metadata-aware compact | 仅值与全部逐行语义状态相同才合并；uniform source 保持 scalar fast path | uniform/mixed compact UT |
| shuffle | vector shuffle owner | 按 shuffle permutation 精确映射 | shuffle/selection UT |
| reset / reuse / clean / decode reuse | Vector、expression executor、Process reset owners | 旧 sidecar 释放/清零；下一代等价于 fresh | reset/reuse/allocation UT；prepared BVT |

## Transport 与持久化边界

| 边界 | Owner | merge rule / version | Oracle |
|---|---|---|---|
| batch marshal/unmarshal | `pkg/container/batch/batch.go` | MORPC v27 trailer保留来源；旧 peer 对 source-only batch降级丢弃，旧 prepared metadata仍按旧 gate拒绝 | 五类来源 buffered round-trip；invalid/const-mixed/reuse UT |
| CN dispatch / remote result | dispatch sender、remote-run receiver | 双向使用 MORPC v27 source gate | dispatch/remoterun version-matrix UT |
| grouping batch codec | batch grouping codec | grouping bitmap与五类来源同时 round-trip | 五类来源 grouping streaming UT |
| aggregate/group state encode/decode/merge | `aggexec.SaveIntermediateResult*`、`group.saveAggregateChunkForProtocol` | MORPC v27携带来源；旧协议显式省略；merge使用贡献规则；未知值拒绝且decoder可复用 | 五类 full/chunk round-trip、group protocol gate、same/mixed merge、invalid/reuse UT |
| Process parameters | Process/frontend codecs | SQLPrepare/COMStmt/UserVariable保留至 Param executor；v27远端携带 | process/frontend UT |
| spill / selected-row materialization | selection/grouping codecs、spill owners | 精确行来源随payload写入/恢复 | selection/grouping/spill consumer UT |
| storage / derived materialization | compile/storage operators | materialized vector逐行透明；重新计算表达式为Expression | local/materialized/remote BVT |

## Consumer totality

| Consumer | Owner rule | Oracle |
|---|---|---|
| numeric conversions | CAST/numeric kernels读取独立 conversion/domain axis，不按 source 猜类型；五类合法 source均可达 | `TestStringSourceConsumerTotality/numeric`；prepared conversion UT |
| BIT | BIT CAST/kernel同上，来源仅随明确透明边界传播 | `TestStringSourceConsumerTotality/bit`；bit-cast row-lineage UT |
| JSON | JSON kernels对五类来源 total；输出新 JSON 值为Expression，透明存取保持来源 | `TestStringSourceConsumerTotality/json`；JSON/vector UT |
| string functions | string kernels不把 source 当作binary/domain；新值为Expression | `TestStringSourceConsumerTotality/string`；function UT |
| protocol result consumers | frontend/remote result codec只编码合法来源；未知值拒绝，旧版本按v27降级 | frontend/process/dispatch/remoterun UT |

## 公共 SQL 矩阵

| 对照 | 唯一变化边界 | Oracle |
|---|---|---|
| direct `@var` vs projection/string wrapper | projection/wrapper | `HEX` 完全相等 |
| direct `@var` vs derived/CTE/join/UNION ALL | 单一 relational boundary | id与bytes/NULL相等 |
| base rows vs DISTINCT/GROUP BY/aggregate | set/group boundary | 稳定结果集合与bytes |
| local value vs stored/materialized/remote scan | materialization/remote | id对应bytes/NULL不变 |
| COALESCE first vs second arm | selected arm | 被选中bytes正确 |
| COALESCE vs IF/CASE/IFNULL | merge policy | 相同输入bytes，COALESCE独立执行 |
| prepared fresh vs repeated execute | reset/reuse | non-NULL与NULL逐次等价 |

## 验收状态

- 五类合法来源：普通 batch 与 grouping codec typed round-trip。
- 未知来源：vector、batch、Literal/LiteralVec planner入口确定性拒绝。
- public BVT：证明 projection、CTE、join、UNION、GROUP BY、DISTINCT、aggregate、materialization、flow-control、prepare-reuse 的 SQL 可达性和结果 bytes/NULL 等价；`HEX`/`COUNT`/`MAX` 不读取 source，因此不作为 source 传播 oracle。
- source 传播由 typed owner oracle观察：planner rewrite、operator/vector、aggregate/group-state、value-window、dispatch sender、remote-result receiver分别直接断言 `GetStringSourceAt`。remote typed tests直接调用远端专用 `marshalRemoteBatch` 与 `messageReceiverOnServer.sendBatch`，从而确定性经过 v27 remote wire owner，不依赖本地 planner placement 或 BVT 调度概率。
- 当前独立 StringSource wire capability：`MORPCVersion27`。
