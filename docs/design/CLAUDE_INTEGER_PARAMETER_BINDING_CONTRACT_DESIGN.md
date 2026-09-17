# 整数参数绑定与兼容性契约设计

- 状态：Proposed，等待 maintainer 对精确 revision 独立审批
- Parent issue：[matrixorigin/matrixone#28893](https://github.com/matrixorigin/matrixone/issues/28893)
- Foundation issue：[matrixorigin/matrixone#28977](https://github.com/matrixorigin/matrixone/issues/28977)
- Implementation PR：[matrixorigin/matrixone#28989](https://github.com/matrixorigin/matrixone/pull/28989)
- 后续迁移：[#28978](https://github.com/matrixorigin/matrixone/issues/28978)、[#28979](https://github.com/matrixorigin/matrixone/issues/28979)、[#28980](https://github.com/matrixorigin/matrixone/issues/28980)、[#28981](https://github.com/matrixorigin/matrixone/issues/28981)
- 设计 owner：function registry（参数契约与稳定 execution identity）、planner/binder（source-domain coercion）、function executor（逐行转换）、compile/remote pipeline（发送与接收准入）、catalog expression publisher/reader（持久化准入）
- 设计门禁：跨越 function registry、planner、remote pipeline 和 catalog persistence；改变 MORPC、混合版本、持久化表达式及 rollback compatibility contract

## 2026-09-17 合并基线后的协议编号修订

最新 `main` 已将 MORPC v84 分配给扩展离散百分位输入类型。本设计下文原有的整数转换 v84 门槛统一迁移至 **v85**，原有“v83/旧节点”边界相应扩展至 **v84 及以下**；CAST 5–8 identity、转换语义与准入机制不变。代码与协议测试以此修订为准，v84 节点必须拒绝整数转换 identity，不能因为支持百分位功能而获得整数转换能力。

本次 prepared 修复同时明确：无域 NULL 只在表达式公共类型推导期间使用 ANY，输出到 PROJECT/GROUP BY/UNION 前必须具体化；NULL peer 的 `T_any=0` 由显式 provenance 标志确认，不能当作缺失类型；嵌套位聚合必须从实际协议源类型重新绑定其私有转换。

## 1. 问题、证据与成功标准

MatrixOne 中 count、offset、length、position、precision 等参数在 SQL 语义上要求整数，但历史 function registry 往往用多个 FLOAT、DECIMAL、UINT 或 INT overload 兼任“输入转换”和“函数执行”。通用 conversion-cost resolver 因而可以先把精确 DECIMAL 转成 DOUBLE，再由某个 executor 截断；相同值来自 literal、column、CASE、SQL `EXECUTE` 或 binary prepared parameter 时还可能选择不同 identity 或转换规则。

`SUBSTRING_INDEX(source, delimiter, count)` 是本 PR 的代表性反例：旧注册保留 FLOAT64、UINT64、INT64 count execution identities。DECIMAL count 可能先丢失精度或进入与普通 DECIMAL-to-integer 不同的舍入路径。#28893 还记录了 `PERIOD_ADD`、`PERIOD_DIFF`、`HEX` 等同类路径；逐函数 matcher、binder name check 或 executor-local conversion 只能把同一策略复制到更多 owner，不能形成共享契约。

### 1.1 核心不变量

1. **参数 owner 不变量**：参数是否为整数、目标 signedness/width、允许的 source domain 和特殊语义由 function metadata 声明；全局 conversion cost 不得覆盖该声明。
2. **source-domain 不变量**：INTEGER、UINT/BIT、DECIMAL、FLOAT、text、binary literal、temporal、UUID、NULL 的原始 domain 保持到参数转换执行点；精确值不得经过隐式 `float64` 中间态。
3. **统一路径不变量**：literal、column、selector branch、SQL `EXECUTE`、COM_STMT 和持久化表达式在给定相同 source type/value 时必须到达同一个 private conversion identity 和同一个结果/错误。
4. **语义隔离不变量**：本契约只用于 function argument coercion；不改变用户显式 `CAST`、assignment、普通 overload resolution 或 unrelated arithmetic semantics。
5. **identity 不变量**：旧 function overload 和 CAST 0–4 的编号、含义及可执行性不变；新 binding 可以停止选择 compatibility-only overload，但不得删除、重排或复用旧 identity。
6. **范围不变量**：转换先保留完整 signed/unsigned magnitude，再对目标 `INT64`/`UINT64` 检查；overflow 是错误，不 wrap、saturate 或仅发 warning。
7. **分布式/持久化不变量**：任何包含 private CAST 5–8 的 plan 都要求 MORPC v84。旧 CN 不得接收、执行、发布或读取其无法解释的新 identity。
8. **inactive-row 不变量**：转换自身的 NULL source 或未被 select mask 选中的 row 不读取、不舍入、不解析，也不产生 overflow；输出保持 NULL。外层函数的其它参数为 NULL 不等价于该转换已被 mask：例如 `SUBSTRING_INDEX(NULL, '.', 9223372036854775808)` 的非 NULL count 仍须报范围错误。

### 1.2 可度量成功标准

- `SUBSTRING_INDEX` 新 binding 只选择 canonical INT64 count executor；旧 identities 仍可执行旧 plan。
- source-to-target matrix 在 constants、columns、CASE/IF/NULLIF selector、SQL `EXECUTE`、COM_STMT、NULL、boundary 和 overflow 上有 typed oracle。
- private CAST 5–8 在 local、remote、protobuf round-trip、DEFAULT/generated/CHECK catalog round-trip 和 SQL-origin rebind 上保持 identity 与行为。
- MORPC <= v83 对新 identity fail closed；v84 sender/receiver 和持久化 floor 才允许执行。
- ordinary path 不增加逐行额外副本、缓存、goroutine 或共享可变状态；每个 row 只做一次 source conversion 和 target range check。
- 当前 foundation owning-package tests 通过；公开 SQL BVT 覆盖代表 consumer。

## 2. 范围、迁移清单与非目标

### 2.1 当前 foundation PR 范围

- 在 `FuncNew` metadata 中增加 integer parameter declaration 和 binding-only overload subset。
- 增加 append-only private CAST execution identities 5–8。
- 在 AST binding、plan-expression rebinding、prepared specialization、constant folding和 expression visitor 中保留 integer context。
- 仅把 `SUBSTRING_INDEX` count（position 2）迁移到 fixed signed INT64 contract，并将旧 FLOAT/UINT identities保留为 execution-only compatibility entries。
- 增加 worker placement、send-time destination recheck、receiver validation 和 persisted-expression v84 admission。

### 2.2 完整迁移 inventory

后续 PR 只能复用本文已经定义的 mode、转换矩阵和兼容机制；若需要新的 source semantics、identity、MORPC version 或 persistence mechanism，必须回到设计 review。

| Issue | 参数 family / consumer | 预期 contract |
| --- | --- | --- |
| #28977 / 当前 PR | `SUBSTRING_INDEX` count | fixed signed INT64；代表端到端 foundation |
| #28978 | `LEFT`、`RIGHT`、`SUBSTRING/SUBSTR/MID`、`LPAD/RPAD`、`INSERT`、`LOCATE`、`REPEAT`、`SPACE`、`ELT` | ordinary signed position/count/length；不得改变 string/result domain |
| #28979 | `PERIOD_ADD/DIFF`、`CEIL/FLOOR/ROUND/TRUNCATE` precision、`FROM_DAYS/WEEK/YEARWEEK`、`TIMESTAMPADD` count、`SUB_VECTOR` bounds、`LAST_QUERY_ID` offset、`RANDOM_BYTES` length、`SHA2` selector、`SPLIT_PART`、REGEXP optional controls | 每个位置显式 signedness/range；temporal/value operands 不迁移 |
| #28980 | `HEX`、`CHAR`、`MAKE_SET`、`EXPORT_SET`、`CONV` | bit-pattern / numeric-only modes；保留 UINT/BIT、text bits、ENUM 和 radix-string 语义 |
| #28981 | `FORMAT` precision、`MAKEDATE` year/day、`MAKETIME` hour/minute | special consumer；非整数 operand 与 fractional seconds 保持原域 |

### 2.3 非目标

- 不全局修改 DECIMAL-to-INT 或 DECIMAL-to-DOUBLE conversion cost。
- 不改变 explicit CAST、assignment、warning 或 charset/collation policy。
- 不把真正允许 fractional value 的 operand 标记为 integer parameter。
- 不在当前 PR 批量迁移剩余 inventory，也不删除 legacy execution entries。
- 不为该契约增加 background worker、cache、queue、retry layer、用户配置或 SQL-visible function。
- 不声称完全复制 MySQL warning/saturation 行为；MatrixOne 对本契约的 overflow 选择 error。

## 3. 参数 metadata 与 binding identity

Function registry 是整数参数 contract 的第一 owner。每个 declaration 包含：

```text
position | target(INT64/UINT64) | mode | variadic | temporal permission | UUID permission
```

### 3.1 Mode

| Mode | 用途 | 规则 |
| --- | --- | --- |
| `fixedIntegerParameter` | ordinary count/offset/length/position | 所有允许 source 转到 metadata 指定 target；source unsigned 不改变 target |
| `integerBitPatternParameter` | bit-pattern consumer | text/binary source 和 genuine unsigned/BIT 可选择 UINT64；不得把普通 signed count 扩成 UINT64 |
| `numericOnlyIntegerParameter` | `HEX` 等同时有 string/array semantics 的 consumer | 仅 numeric/bool/BIT/year source 使用整数 contract；其他 source 留给原 function semantics |

`temporal` 和 `uuid` 是逐参数 permission，不是全局 CAST 能力。只有历史 signature 已允许这些 source 的角色才能打开；普通 integer count 继续拒绝它们。

### 3.2 Binding overload 与 execution overload 分离

`Overloads` 保留所有可执行 identity，包括历史 plan 仍可能引用的 entries。`bindingOverloads` 仅列出新 resolver 可以选择的 canonical entries。Type checker 先按 integer metadata 归一化参数 type，再在 binding subset 中选择，并把 subset index 映射回稳定 execution-table index。

该分离保证：

- 新表达式不会再选择仅用于历史 source conversion 的 FLOAT/UINT overload；
- 旧 serialized/persisted plan 仍能按原 identity 执行；
- overload array index 不因过滤而写入错误的 plan identity；
- 一个 function 是否进入新 contract 由 metadata 决定，不靠名称白名单或全局 cost hack。

## 4. Source-to-target coercion matrix

以下矩阵是 function-argument contract，不是 public CAST specification。

| Source | Normalization | INT64 target | UINT64 target |
| --- | --- | --- | --- |
| signed INT / YEAR | 保存 sign + uint64 magnitude | `[-2^63, 2^63-1]` 接受 | 负的非零值拒绝 |
| unsigned INT / ENUM / BIT | 保存完整 uint64，不先转 int64 | `<= MaxInt64` 接受 | 全域接受 |
| BOOL | `false=0`、`true=1` | 接受 | 接受 |
| DECIMAL64/128/256 | 按 decimal 自身 scale 做精确整数舍入；不经过 DOUBLE | target range 内接受 | target range 内且非负接受 |
| implicit FLOAT32/64 | IEEE `RoundToEven` 后检查 NaN/Inf/范围 | target range 内接受 | target range 内且非负接受 |
| explicit REAL value boundary | `Trunc` 后检查 NaN/Inf/范围 | target range 内接受 | target range 内且非负接受 |
| ordinary text | 解析 leading signed decimal integer prefix；无 prefix 为 0 | range 内接受 | 负的非零值拒绝 |
| binary literal / text-bits role | 最多 8 bytes，按 big-endian bit pattern；leading zero 可去除 | 仅声明该 target 的角色使用 | 全域接受；text-bits 特例可保留合法 signed pattern |
| DATE/TIME/DATETIME/TIMESTAMP | 仅 temporal-enabled role；先在 calendar/clock domain 舍入再编码 | 接受并检查 | 不用于当前 temporal contract |
| UUID | 仅 UUID-enabled role；按既有 leading-decimal text policy | 接受并检查 | 不默认接受 |
| NULL / masked row | 不读取 source；输出 NULL | NULL | NULL |
| unsupported domain | bind/signature validation error | 拒绝 | 拒绝 |

### 4.1 精确数值与范围

内部 `integerArgumentValue` 用 `magnitude uint64 + negative + overflow` 表示完整 source range。`uint64 -> int64` 检查发生在最终 target 边界，禁止 wrap；DECIMAL128/256 的高 limb 非零直接形成 overflow proof；FLOAT 的 NaN、Inf 或绝对值 `>= 2^64` 拒绝。

DECIMAL 使用类型自己的 scaled-integer operation，避免 DOUBLE 精度丢失与 double rounding。FLOAT 是近似 domain，保留其已经表示的近似值，再依据 source 是否由显式 REAL value boundary 选择 round-to-even 或 truncation identity。

### 4.2 Text 与 binary

ordinary text 只消费 leading decimal integer prefix；它不把本任务扩展为完整 numeric parser。binary-literal/bit-pattern 语义只属于 metadata 明确声明的 bit role，不得因为 source 是 string 就改变 ordinary signed count。

### 4.3 Temporal

TIME 在 clock domain 处理 fractional carry 后编码 `HHMMSS`；DATETIME/TIMESTAMP 在 calendar/timezone domain 舍入后编码；最大 datetime carry 保留既有边界策略。禁止直接把 packed `HHMMSS` 当普通 decimal 做进位，因为这会产生非法 minute/hour。

## 5. Private CAST execution identities

CAST function ID 保持 21；本设计 append-only 保留以下 planner-owned overload：

| Overload | 名称 | 语义 |
| ---: | --- | --- |
| 5 | `IntegerArgumentCastOverload` | ordinary argument conversion；DECIMAL exact，implicit FLOAT round-to-even |
| 6 | `TruncatedIntegerArgumentCastOverload` | explicit REAL value boundary 的 truncation |
| 7 | `TextIntegerBitsCastOverload` | text/binary bit-pattern 到 UINT64 |
| 8 | `TemporalIntegerArgumentCastOverload` | temporal/UUID-enabled parameter conversion |

这些 identity：

- 只能由 planner integer-argument path 指定；ordinary type checker 和用户显式 CAST 不选择；
- signature 必须是两参数（source、target marker），且 target/source 满足对应 allowlist；
- 由 plan visitor、deep copy、constant fold、prepared rebind、remote feature scanner 识别；
- 不得格式化成用户 SQL。Catalog 同时保存 origin SQL，restart/rebuild 可以从 public spelling 重新绑定，而 serialized expression 可以直接执行稳定 private identity。

CAST 0–4 仍是 legacy/public contract，不能因新增 identity 改号或改变行为。

## 6. Binder、selector 与 prepared execution

### 6.1 AST binding

Binder 在 function metadata 标记的位置进入 integer source context。普通 expression 先保留 source type，再追加 private conversion。CASE、IF/IFF、NULLIF 属于选择边界：每个可能被选中的 branch 在自己的 source domain 中转换，然后 selector 只调和已经验证的 integer result。

这防止 CASE 的通用 common-type reconciliation 先把 DECIMAL/INT branch 提升为 DOUBLE。IFNULL/COALESCE 和 arithmetic 是 value-producing boundary，不透明恢复内部 source；用户 explicit CAST 同样是 authoritative value boundary。

ENUM/SET 等特殊存储值只有在已证明 reversible provenance 时恢复 storage numeric source；任意 string expression 不得被猜回 storage ordinal。

### 6.2 Plan-expression binding、fold 与 rewrite

由 AST 以外路径构造的 function expression也必须调用同一 integer metadata helper。Binder-owned reconciliation 可以剥离；user CAST 不剥离。Constant fold：

- 保留 private CAST identity及 target；
- NULL private cast 可安全折叠为 typed NULL；
- selector 未选 branch 的 conversion error 不能提前成为 query error；
- plan visitor/deep copy 必须遍历 conversion source 和 target marker。

### 6.3 Prepared specialization

PREPARE 时 ParamRef 的 transport type 可能只是 provisional TEXT，但真实 source domain 属于 EXECUTE。Plan metadata缓存哪些 parameter positions 位于 private integer CAST 中；普通 execution无需每次扫描整棵 plan。

EXECUTE 时 rebind：

1. 从 SQL `EXECUTE` 或 COM_STMT packet 取得 typed runtime source；
2. 保留 explicit CAST envelope；
3. 仅替换 private CAST source，不把 identity 重建为 CAST0；
4. source type/value变化时按同一 declaration 重新 specialization；
5. 不修改 cached template plan，重复执行和 NULL-first execution 不污染下一 generation。

peer provenance 只记录源类型，不允许把优化前的可执行表达式快照恢复到最终计划。临时 TEXT 调和的撤销必须作用于已经完成列重映射、子查询展开的当前表达式；被折叠的 peer 使用其当前值及保存的源类型恢复。显式 CHAR、DOUBLE 等 CAST 都保持权威边界。值 lineage 覆盖 PROJECT、UNION 各输入、GROUP BY、AGG 和 WINDOW 的真实生产者；裸参数的真实 TEXT 类型也不得通过字符串内容推断改为数值。

## 7. Remote protocol、混合版本与 rollback

### 7.1 Version allocation

MORPC v76–v83 已由 `main` 中其他兼容契约占用；本设计最终分配：

```text
MORPC v84 = shared integer-parameter coercion execution identities (CAST 5–8)
```

任何旧 issue/PR 文本中的 v76 都由本设计和当前实现中的 v84 supersede。版本 ownership 以 `pkg/defines/const.go` 的最终 allocation 和本文批准 revision 为准。

### 7.2 Feature detection

`RequiredRemoteExpressionFeatures` 遍历 expression owner，以稳定 function ID 21 + overload 5–8 检测 `IntegerParameterCoercion`，不依据函数名或 source type 猜测。legacy CAST 0–4 不触发 v84。

### 7.3 Placement、send 与 receive

| 阶段 | v83/unknown destination | v84 destination |
| --- | --- | --- |
| AP MULTI-CN placement | 若 query 含 private identity，收缩到可用 ONE-CN/local placement | 可保持 MULTI-CN |
| remote serialization | 对实际 destination 再查询；reject | encode |
| receiver decode/admission | reject pipeline | decode/execute |

Placement 不是最终安全边界：worker 可在 placement 后降级、替换或 capability discovery 变化，所以 sender 在 serialization 前必须 recheck actual destination。未知 service、nil destination、version lookup error 或 cancellation 均 fail closed；不能把“未发现版本”当 latest。

### 7.4 升级、降级与回滚

- **滚动升级**：在 common deployment floor 达到 v84 前，新 identity只在支持它的本地 CN 执行，不发往旧 worker。
- **升级完成**：所有目标 v84 后允许 remote execution和新 persisted expression publication。
- **worker rollback/replacement**：send-time recheck 拒绝已变成 v83 的 destination；不会发送不可解释的 identity。
- **binary rollback**：旧 binary仍可执行 legacy plans；包含 private CAST 5–8 的 remote/persisted expression必须被 admission gate 拒绝，而不是误解释。
- **无编号复用**：回滚后 v84 identity保留，不把 5–8 或 protocol 84 分配给其他语义。

本设计不修改 protobuf schema；兼容风险来自现有 function/overload identity 的新值，因此 version barrier 仍是强制条件。

## 8. Persisted expression、restart 与 restore

DEFAULT、generated column、CHECK、view/其他 catalog-bound expression 可能在任一 CN 本地执行，不能仅依靠 remote sender gate。`RequiredPersistedExpressionProtocolVersion` 汇总 owner 中所有 feature 的最大 floor；private integer identity将 floor提升到 v84，并复用 deployment-managed read/authoring admission：

- read/rebind path 要求 runtime protocol和 durable persisted-expression floor均达到 v84；
- authoring path还要求 local catalog admission floor已启用并 fenced，避免 phase-one rollout提前发布 metadata；
- publication helper在 expression fold 后及 TableDef最终发布边界检查，mixed owner取最高 requirement；
- restore/background service 没有 `Process` 时使用 service-level floor，未知状态 fail closed。

Catalog 同时保留 public origin SQL。Restart有两条独立有效路径：

1. protobuf expression保留 CAST 5–8 并在 v84 admission后直接执行；
2. 从 origin SQL重新 parse/bind，按相同 metadata重新生成 private identity。

恢复不能依赖 private CAST 的 SQL formatter spelling，也不能把它降级为 ordinary CAST0。旧 CN 读取 v84 catalog metadata 时必须收到 not-supported error，而不是静默执行不同舍入。

## 9. Ownership、失败路径、资源与性能

### 9.1 Owner/consumer closure

| Contract | 第一 owner | 主要 consumer / reverse arc |
| --- | --- | --- |
| 参数 role/target/mode | function registry | AST binder、plan binder、prepared rebind |
| private identity与逐行转换 | CAST registry/executor | constant fold、expression executor、serialized plan |
| runtime source type | EXECUTE parameter materialization | ResetParamRefRule、cached prepare metadata |
| remote capability | MORPC version + destination discovery | placement、sender、receiver |
| persisted floor | deployment/runtime admission | DDL author、catalog reader、restore/rebind |

### 9.2 失败与生命周期

- bind-time unsupported source 在 plan publication前失败；不产生半绑定 function identity。
- row conversion只 append到现有 `FunctionResult` owner；allocation/append error原样返回，由既有 result lifecycle清理。
- overflow、NaN、Inf、invalid signature在输出该 row前失败；masked/NULL row不触发 conversion。
- prepared specialization deep-copy template再修改；错误不会污染 cached plan，下一次 execution可重试。
- placement fallback不创建新 worker pool；send validation获取的 client/release由既有 discovery owner配对。
- 本设计不新增 goroutine、lock、channel、retry、queue、cache或无界容器，因此没有新的 wait-for chain 或 background cleanup owner。

### 9.3 性能预算

- registry normalization：bind-time `O(argument count)`，只在首次需要修改时复制 argument type slice。
- expression wrapping：每个声明的 integer parameter最多一个 private CAST；selector只递归可达 branch，不扫描 runtime rows。
- runtime：`O(rows)`，每 row一次 source读取、normalization、target check和append；无完整 vector中间副本。
- prepared ordinary execution：eligible parameter positions在 plan建立时缓存；不为每次 execution扫描整棵 plan。
- protocol detection：plan/DDL/serialization boundary traversal，不进入 scalar per-row hot path。
- 额外状态受 plan argument count、parameter count和batch length既有上界约束；不新增永久缓存或高基数 metrics。

## 10. 安全、租户与可观测性

private identity不包含用户文本、secret或tenant identifier，不改变 authentication、authorization或tenant routing。Remote/cached plan属于内部 trust boundary，但 decoder仍须验证 function ID、overload signature、target type及protocol floor，避免 malformed plan绕过source allowlist或造成类型混淆。

错误使用现有 typed `invalid input`、`out of range` 和 `not supported` classes。运维可通过 negotiated MORPC version、destination identity、persisted-expression floor及稳定的“requires protocol version 84”错误诊断；不在逐行转换路径增加日志或高基数指标。

## 11. 被拒绝方案

1. **逐函数 matcher/binder名称判断**：策略分散，prepared/persisted/selector路径会继续漂移，也无法形成统一 compatibility owner。
2. **在 FLOAT executor内部 round/truncate**：DECIMAL在进入executor前已经可能丢精度，且会改变 genuine FLOAT overload语义。
3. **全局提高 DECIMAL-to-INT conversion preference**：会误绑定真正接受fractional argument的函数，影响范围无法由参数owner控制。
4. **统一走 public CAST0**：混淆function argument、explicit CAST和assignment的rounding/overflow policy，也无法稳定标识remote capability。
5. **把所有source先转DOUBLE**：对大整数、高精度DECIMAL和bit pattern不正确。
6. **删除旧overload或重用ID**：破坏serialized/persisted plan、rollback和mixed-version execution。
7. **只在remote sender检查**：catalog expression可在旧CN本地执行，绕过remote边界。
8. **仅在placement检查**：destination可能在send前替换/降级，存在TOCTOU compatibility gap。
9. **以warning/saturation模拟MySQL**：与#28893已决定的MatrixOne overflow-error contract冲突，并扩展到本任务排除的warning compatibility。

## 12. 验证地图

| 契约 | 最小证明 |
| --- | --- |
| metadata declaration与binding subset | registry/type-checker UT：position、variadic、source allowlist、legacy identity不可被新binding选中 |
| exact integer conversion | executor table UT：signed/unsigned/bool/BIT、DECIMAL64/128/256 half/boundary、FLOAT implicit/explicit、text prefix、NULL、mask、overflow/NaN/Inf |
| selector preservation | planner UT：CASE/IF/NULLIF branch；nearest controls为IFNULL/COALESCE、arithmetic、user CAST |
| prepared source | SQL EXECUTE + COM_STMT typed parameter UT/BVT：source type变化、typed NULL、repeat/cache reuse、overflow后重试 |
| representative consumer | `SUBSTRING_INDEX` binder/executor UT：literal、column、fractional boundary、UINT/BIT boundary、NULL和legacy identity |
| constant fold/rewrite | typed plan UT：selected failing branch不提前报错、NULL fold、deep copy/visitor保留identity |
| protocol feature scan | CAST 0–4 negative、5–8 positive；nested pipeline/TableDef owner |
| placement/send/receive | v83 reject/v84 accept、unknown/nil、placement后downgrade、cancellation、client acquire/release平衡 |
| persistence/restart | DEFAULT/generated/CHECK protobuf round-trip + origin SQL independent rebind；read/authoring floor v83 reject/v84 accept |
| public SQL | `integer_parameter_coercion.sql/result` normal comparison；最小rows覆盖constant/column/prepared/overflow |
| delivery | owning package tests、incremental vet/lint、build、changed-block coverage >=75%、`git diff --check`（BVT表格格式例外需单独记录） |

BVT证明公开SQL路径；private identity、source type、protocol floor和catalog state由typed UT作为独立oracle。该设计不增加shared mutable state或goroutine，因此race/topology专项不自动要求；remote/persistence compatibility测试不可由普通local function UT替代。

## 13. Rollout、fallback 与开放问题

### 13.1 Rollout

1. 审批本文精确 revision，确认v84 allocation和conversion matrix；
2. 合入foundation及代表consumer，但在deployment common floor <81时依赖placement/local fallback和catalog authoring fence；
3. 所有CN达到v84并完成catalog fence后，允许distributed execution和新persisted expressions；
4. 后续migration PR逐项启用metadata，不再新增identity/version；
5. 若发现correctness问题，先停止新增consumer migration；remote/persistence gate保持fail closed，回退binary不会误执行v84 plan。

### 13.2 已接受权衡

- private CAST增加内部identity数量，但用稳定capability marker换取明确mixed-version safety。
- text prefix与implicit FLOAT规则可能不同于某些public CAST路径，这是有意的argument contract隔离。
- old CN无法执行含private identity的persisted expression；选择明确拒绝而非silent semantic downgrade。
- 当前PR只迁移一个consumer，减少blast radius；完整收益需后续inventory按本文逐项迁移。

### 13.3 开放问题

无实现前可延后的设计阻塞问题。若maintainer改变以下任一项，必须先修订本文并重新审批受影响章节：

- implicit FLOAT round-to-even与explicit REAL truncation的分界；
- text-prefix或bit-pattern规则；
- overflow error policy；
- CAST 5–8或MORPC v84 allocation；
- persisted authoring/read floor；
- 后续consumer需要本文之外的新mode、source permission或compatibility mechanism。

## 14. 设计审批与实现符合性

### 14.1 审批请求

请maintainer对本文所在的精确commit revision明确确认：

- 参数metadata owner及三种mode；
- source-to-target coercion matrix和overflow policy；
- private CAST 5–8的append-only identity；
- prepared selector/rebind语义；
- MORPC v84 placement/send/receive和mixed-version rollback；
- persisted expression read/authoring/restart closure；
- 验证地图及后续migration不得扩展机制的边界。

### 14.2 Approval record

- 设计 revision：待本文提交后，由PR正文中的精确commit permalink锁定
- 审批者：待maintainer
- 决定：Pending
- 日期：待填写
- 实现偏差：当前implementation按本文描述实现；正式approval后仍需继续完整implementation review，本文件本身不替代代码审查

本文在当前implementation PR中提交，但设计与实现审批保持两个阶段。只有maintainer对精确revision给出可追溯approval后，才将状态改为`Approved`并解除design gate。
