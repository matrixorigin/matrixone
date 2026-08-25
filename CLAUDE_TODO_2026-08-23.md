# PR #27467 review comments 修复计划

## 范围判断

三条意见都直接修正本 PR 新增的字符串来源传播、兼容传输和表达式身份语义，属于当前 PR 的必要闭环，应在本 PR 中修复。

## 不变量与根因

1. CAST 来源所有权：仅 overload 0（隐式 CAST）是透明边界；overload 1（显式 CAST）必须生成 `StringSourceExpression`。当前普通与 selection 路径无条件透明传播，破坏显式 CAST 的语义所有权。
2. 旧协议降级：MORPC v11 只能拒绝既有 prepared-parameter metadata；仅含 v12 新增 `StringSource` 的 batch 必须丢弃该字段后继续传输。当前协议门禁复用了包含 `StringSource` 的总判断，导致编码降级之前提前拒绝。
3. LiteralVec 结构身份：影响执行结果来源的 `LiteralVec.StringSource` 必须参与 hash/equality；诊断字段 `IsSerialized` 继续忽略。当前 hash/equality 仅考虑 `Len`、`Data`。

## 实施步骤

1. 合并最新 `mo/main`，如有冲突使用 merge 方式解决。
2. 将普通与 selection CAST 的透明来源传播限制为 overload 0，检查 reset/reuse 后不会残留上次来源。
3. 为 batch 暴露/复用“排除 StringSource 的旧 prepared metadata”判断，并在 CN dispatch 与 remote result 两个 MORPC v12 门禁中使用；保留编码器 `includeStringSources=false` 的降级行为。
4. 将 `LiteralVec.StringSource` 纳入结构 hash 与 equality，同时保持 `IsSerialized` 不参与身份。
5. 增加单测覆盖：
   - 隐式/显式 CAST 的普通、selection、reset/reuse 来源；
   - source-only batch 在 MORPC v11 的双向降级，以及旧 metadata 仍被拒绝；
   - LiteralVec 仅 StringSource 不同则 hash/equality 不同，IsSerialized 不同仍相等。
6. 运行改动所属包和跨边界消费者的 list/build/vet/test；检查完整 diff，并执行 pre-push 自审。
7. 提交并 push 到 `origin/issue-27215-main`；若存在可 resolve 的 inline threads，逐个 resolve，全部修复后请求 re-review。

## 性能与设计约束

仅增加常量条件、复用已有 batch 向量扫描以及在表达式 hash/equality 中处理一个标量字段；不引入新分配、状态机或额外热路径遍历。修复保持在现有所有权与协议边界内。

---

# PR #27467 第二轮 review comments 修复计划

## 范围判断

四项均是本 PR 新增 `StringSource` 在既有执行、planner、vector 排序和反序列化路径上的功能闭环缺口，应在当前 PR 修复。

## 不变量、反例与方案

1. 运行时 folding：同一个 overload 0 implicit CAST，无论普通执行、selection 或 `finishFolding`，结果来源都必须等于输入来源；overload 1 始终为 Expression。最小反例是 literal/运行时 prepared parameter 被 `doFold` 后来源变为 Expression。方案是在 folding 完成边界复用 implicit CAST 透明传播，并验证 reset/reuse。
2. Planner scalar fold：折叠前后可执行表达式的来源所有权必须相同。显式 CAST 折叠成普通 Literal 后会被执行器误标 Literal。优先为 scalar folded literal 增加可执行来源字段并贯通 protobuf、deepcopy、hash/equality、fold、执行与 round-trip；若现有表达式身份模型无法安全承载，再限制改变来源身份的折叠。
3. Vector sort/compact：任何可排序物理类型的值、NULL、domain、prepare kind、StringSource 等逐行语义必须经过同一置换；compact 仅能合并完整语义相等的行。方案是让 fixed、varlen、JSON、array 共用 metadata-aware 行排序/等价边界，避免 `CleanOnlyData` 丢来源。
4. Decoder：const vector 只能携带统一 row source。`SetStringSourcesFromReader` 必须与 `SetStringSourcesWithMP` 使用相同验证，在发布 sidecar 前拒绝 mixed const 输入；buffered/streaming batch decoder 都必须返回错误且不发布部分状态。

## 测试矩阵

- runtime fold：implicit/explicit × literal/prepared runtime parameter × fresh/reset-reuse；来源为直接 typed oracle。
- planner scalar fold：explicit CAST folded/unfolded 的来源相等；protobuf marshal/unmarshal、deepcopy、hash/equality 保留来源。
- sort：fixed/varlen/JSON/array × sort/compact × mixed source；值与所有 metadata 行绑定保持一致，来源不同的同值行不得 compact。
- decoder：uniform const 接受、mixed const 拒绝 × buffered/streaming；普通非 const mixed sidecar 继续接受。

## 验证与性能约束

修改后运行所有直接 owning package 与 planner/colexec/batch 消费者的 list/build/vet/test，并执行完整 diff 自审。排序不得增加第二次值排序或按行分配；优先复用现有 permutation/sorter，metadata 只随既有交换或 compact 扫描同步移动。

---

# PR #27467 第三轮 review comments 修复计划

## 范围判断

三项均直接涉及本 PR 新增 `StringSource` 在远端变量折叠、vector 去重以及 issue #27215 公共验收路径上的闭环，属于当前 PR 必须完成的修复与验证。

## 不变量、根因与方案

1. vector→scalar literal：任何调用 `GetConstantValue()` 的 materialization 边界都必须把目标 row 的来源编码进 `Literal.StringSource`，包括 NULL；零值只表示普通 Literal，其他来源编码为 `source + 1`。当前只有 constant-fold caller 事后补字段，remote user-variable folding 绕过该补丁。方案是把编码收口到 `GetConstantValue()`，删除 caller 的重复写入，并覆盖 remote expression protobuf round-trip 与 fresh/reuse。
2. sort+compact：只要 vector 携带任意 StringSource metadata（uniform scalar 或 mixed sidecar），排序/去重后都必须保留来源；完整行等价性包含来源。当前 metadata fast path 只看 `stringSources != nil`，uniform source 落入旧 fast path后被 `CleanOnlyData()` 清空。方案是以 `HasStringSourceMetadata()` 选择 metadata-aware compact，保持 uniform source 无 sidecar、无新增分配，并覆盖 fixed/varlen uniform duplicate。
3. 公共 SQL 验收：外部可见语义必须同时有本地与 remote/materialized SQL witness，控制组每次只改变一个边界，fresh 与 prepare/reset-reuse 等价，且 COALESCE selected-arm 与 IF/CASE/IFNULL 分开验证。新增专项 BVT `.test/.result` 与一份 `CLAUDE` 前缀 boundary inventory，明确每个边界 owner、传播/merge 规则和对应 oracle。

## 测试矩阵

- remote variable fold：UserVariable × non-NULL/NULL × protobuf round-trip × fresh/reuse；typed literal source 为 oracle。
- sort+compact：fixed/varlen × uniform Literal duplicate；值去重且 `HasStringSourceMetadata()`/row source 保持不变。
- constant-list：`IN (1, 1)` folding 后 `LiteralVec.StringSource` 保持 Literal。
- public SQL：local direct expression 与单一 materialization/remote 边界对照；literal/user variable/prepared 参数；NULL；fresh/re-execute；COALESCE selected arm 独立于 IF/CASE/IFNULL common-domain 用例。

## 性能与兼容性约束

`GetConstantValue()` 仅增加一次 O(1) row metadata 读取；uniform source sort/compact 复用现有 metadata sorter，不分配 sidecar，也不增加第二次排序。protobuf 字段与 wire version 不变；source-free vector 继续走原 fast path。

## 执行步骤

1. 实现统一 scalar literal 来源编码并补 compile/rule 单测。
2. 修正 metadata-aware sort/compact 判定并补 vector 与 constant-list 单测。
3. 增加公共 SQL 专项回归、result 和边界 inventory。
4. 运行差异派生的 owning/dependent package list/build/vet/test 与专项 BVT；执行完整 pre-push 自审。
5. 提交并正常 push 到 `origin/issue-27215-main`；存在 inline thread 时逐个 resolve，全部修复后请求 re-review。

---

# PR #27467 第四轮 review comments 修复计划

## 范围判断

两项均属于 issue #27215 的明确验收条件：第一项是 planner representation rewrite 的来源闭环；第二项是当前 PR 声明的边界清单、codec totality 与协议版本准确性，均应在本 PR 完成。

## 不变量与方案

1. `LiteralVec` 的 stable `Data` 不携带来源，所有拆分/读取 owner 必须先验证外层 `LiteralVec.StringSource`，再恢复到 decoded vector，之后才能调用 `GetConstantValue()`。将恢复收口到 `decodeLiteralVec()`，使 IN RHS、block-filter set、OR-IN/composite rewrite 共用同一验证；非法枚举关闭优化且不产生部分 scalar literals。
2. Boundary inventory 必须逐项覆盖 issue 列出的 expression、relational、vector、transport 和 consumer 边界，明确 owner、透明/选择/贡献 merge rule 和 oracle。协议 gate 按当前 `MORPCVersion27` 更新 inventory 与 PR 描述。
3. 所有五个合法 source category 必须分别通过普通 batch codec 与 grouping codec round-trip；NULL、未知 source 和 decoder 失败继续有独立控制。

## 测试矩阵

- LiteralVec 拆分：Expression/Literal/COMStmt × non-NULL，SQLPrepare × NULL，非法枚举；typed scalar `Literal.StringSource` 为 oracle。
- planner 真实路径：带来源的 folded IN RHS 经 OR-IN/composite-key rewrite 后，生成 scalar/compound predicate 仍保持对应来源；非法来源不触发 rewrite。
- codec：Expression、Literal、UserVariable、SQLPrepare、COMStmt 五行 × batch buffered decode/grouping streaming decode，逐行来源完全相等。
- inventory：逐项映射 projection、wrapper、planner rewrite、CTE/join/UNION/DISTINCT/GROUP BY、shuffle/reset/reuse、numeric/BIT/JSON/string/protocol consumer totality及现有/新增测试。

## 执行步骤

1. 修复 `decodeLiteralVec()` 外层来源验证/恢复并增加 planner 单测。
2. 增加 batch/group codec 全枚举 round-trip 单测。
3. 扩充 `CLAUDE_STRING_SOURCE_BOUNDARY_INVENTORY.md`，将 MORPC gate 更新为 v27；同步 PR description。
4. 运行差异派生的 list/build/vet/test、文档/协议搜索和 pre-push 自审。
5. 提交并 push；存在 inline thread 时 resolve 并请求 re-review。

---

# PR #27467 第五轮 review comments 修复计划

## 根因与不变量

1. protobuf/process 的 `uint32` source 必须先在原始宽度上完成上界校验，再窄化为底层 `uint8` 枚举；256、257、`MaxUint32` 均必须确定性失败，所有入口行为一致。
2. 权威模型保持不变：仅 COALESCE 使用 selected-value source；IF、CASE、由 IFNULL rewrite 得到的 CASE 使用 common-domain，输出 source 必须为 Expression。runtime domain 等其他 selected-value metadata 按既有独立维度处理。
3. aggregate/group-state、value window 和 remote placement 必须由各 owner 的 typed/internal oracle 证明；普通 SQL bytes oracle不能证明 source，BVT仅作为可达性/结果控制，不再声称其直接观测来源。
4. `remoterun_test.go` 运行 gofmt，消除 SCA 风险。

## 测试矩阵

- raw source：LiteralVec 256/257/MaxUint32；Literal encoded 256/257/MaxUint32；ProcessInfo 256/257/MaxUint32，均拒绝；最大合法值作为相邻控制。
- flow control：COALESCE selected SQLPrepare；IF/CASE/IFNULL common-domain Expression；normal、selection、constant folding/reset 路径保持一致。
- aggregate/group-state：五类 source 的 full/chunk encode-decode、同源/异源 merge、未知 trailer、reuse；覆盖 `saveAggregateChunkForProtocol` v27 与旧协议 gate。
- value window：五类 source、NULL/default、lag/lead/first/last/nth 的 typed row-lineage。
- remote：通过 compile/dispatch typed version/placement assertion确定走 remote protocol owner；文档明确 public BVT只能证明可达性和 bytes 等价，source由 typed test观察。

## 执行步骤

1. 修复三个 raw-width 校验入口并补回绕反例。
2. 拆分 COALESCE 与 IF/CASE/IFNULL source policy，修正 normal/fold/reset 测试。
3. 补 aggregate/group-state、window、remote typed coverage并更新 boundary inventory/PR描述。
4. gofmt 全部改动文件，运行差异派生的 list/build/vet/test及 BVT。
5. 完成全 diff 自审和 Q1-Q3，提交、push、处理可见 review thread。

---

# PR #27467 第六轮 review comments 修复计划

## 根因与统一规则

1. VALUE_SCAN 的 constant materialization 是 scalar literal 的独立消费入口，必须复用与 expression executor 相同的 raw-width source 解码；每个成功物化 row（含 NULL）设置来源，失败不得发布部分 batch。constant 与 dynamic RowsetExpr 必须语义等价，reset/reuse 等价于 fresh。
2. UNION、DISTINCT、GROUP BY 不因 provenance 改变 SQL 去重行数；相同 SQL key 的 provenance 按 contributing representatives 合并：同源保持，异源为 Expression。该规则取代 inventory 中“来源不同不能合并”的错误表述，并且与输入顺序、并发到达、spill/reload 无关。
3. MIN/MAX winner replacement 对 fixed/varlen 全类型采用 selected representative source；equal candidate 对所有类型执行 source merge，runtime string-domain merge仅在字符串域上执行。MAX_BY equal candidate同样适用，source merge不得被非字符串 early return 截断。

## 测试矩阵

- VALUE_SCAN：五类合法 source、typed NULL、257/MaxUint32；constant/dynamic等价；reset/reuse；失败无部分发布。
- group key：same bytes + same/mixed source、正反顺序、DISTINCT/UNION等价 shape、spill encode/reload、reset/reuse；row count保持1且source确定。
- MIN/MAX：fixed numeric/date/decimal winner来源；equal same/mixed source正反序；full/chunk partial-state round-trip/merge。
- MAX_BY：fixed、JSON、array equal candidate mixed source正反序；winner replacement与partial merge。

## 执行步骤

1. 抽取/复用 plan scalar source decoder并修复 VALUE_SCAN constant owner。
2. 在 group key duplicate路径加入无额外普通-path sidecar分配的来源合并，并覆盖spill/reuse。
3. 解耦 aggregate source/domain merge，补齐 fixed MIN/MAX winner与MAX_BY equal/partial路径。
4. 更新 boundary inventory，运行差异派生的 list/build/vet/test及相关 BVT。
5. 全 diff 自审、Q1-Q3、提交、push并处理可见 review thread。

---

# PR #27467 第七轮 review comments 与 target conflict 修复计划

## 同步与冲突策略

1. fetch 并 merge 最新 `mo/main`。冲突以 main 的 `5eda149552` 撤销 prepared runtime specialization 为权威：不恢复被撤销的 runtime kind/specialization，只在仍存在的 SQL PREPARE/COM_STMT 参数创建入口叠加独立 `StringSourceSQLPrepare` / `StringSourceCOMStmt`。
2. merge 后先用 diff/搜索确认没有重新引入被撤销字段、分支或测试断言，再开始 review fix。

## 根因与不变量

1. Group same-preview duplicate 的最终来源必须在 hash commit 前按 `preview.values` 聚合；新 group append必须直接发布最终 merged source。所有可能的 sidecar分配均属于 pre-commit preflight，allocation rejection返回原有 spill/retry路径，commit 后不得再出现可失败分配。
2. MAX_BY winner是完整 nullable row state。NULL winner也必须 selected-value复制 source；replacement不得继承旧winner metadata。value/order/tie 的 correlated state在所有分配成功后一次发布。
3. MIN/MAX extra是无显式来源的内部贡献者，source为Expression。extra获胜覆盖winner source；extra相等按contributors merge为Expression。该规则同时适用于fixed与bytes，不依赖当前SQL可达性。

## 测试矩阵

- Group：same-preview `[Literal, UserVariable, Literal]` / inserted `[1,0,1]`，正反来源；reject-next group allocation必须在commit前失败且可spill/retry；成功结果两个SQL group且same来源Expression；reset/reuse。
- MAX_BY NULL：first winner、replacement、same/different source、partial merge；NULL及非NULL controls，fixed/varlen value。
- MIN/MAX extra：fixed/bytes × wins/ties/loses；已有winner为COMStmt时 wins/ties输出Expression，loses保持COMStmt；empty state由extra产生Expression。
- merge冲突：frontend相关 owning package build/vet/test，搜索确认prepared runtime specialization未恢复。

## 执行步骤

1. merge `mo/main`，按上述权威规则解冲突并验证撤销结果。
2. 将group source聚合前移到preview/preflight并增加allocation-failure测试。
3. 修复MAX_BY NULL完整row-state复制与MIN/MAX extra source贡献。
4. 更新inventory，运行差异派生list/build/vet/test和相关BVT。
5. 全diff自审、Q1-Q3、提交merge/fix、push并处理可见review thread。

# PR #27467 review round 8

## 阻塞项与不变量

1. Group 不得修改 borrowed input vector。preview 合并来源只能写入 operator-owned bounded scratch，并在 selected-row append 时作为 source override 发布；hash commit 后不得分配，input source 值和 sidecar MPool ownership 均不变化。
2. Vector staticcheck S1009 必须通过，不保留冗余 nil slice 判断。
3. Changed-code coverage 必须高于 75%，至少补足当前缺失的 8 个 modified lines，并优先覆盖 MAX_BY、MIN/MAX、Vector 新分支。
4. 合并最新 `mo/main`，proto 冲突在源文件解决，`.pb.go` 仅由仓库生成工具产生。

## 反例与测试矩阵

- Group same-preview `[Literal, Expression]`：结果 merged source 正确，input source/sidecar owner/MPool bytes 前后完全不变。
- Group allocation rejection：preflight 可失败且 hash 未 commit；hash commit 后 reject controller 不得观察到新分配。
- group key 与 ANY_VALUE/aggregate argument 共享同一 input vector：aggregate 仍观察原始 row source。
- Vector source override：nil/uniform/mixed destination、selected rows、cancel/error/reuse，source-free路径零额外row scratch。
- MAX_BY 与 MIN/MAX：覆盖当前未命中的 NULL、extra wins/ties/loses、preflight/error分支。
- proto：生成结果可重复，禁止手改 generated files。

## 执行步骤

1. fetch 并 merge 最新 `mo/main`，解决 proto 源冲突并重新生成。
2. 设计并实现 bounded selected-row source override，删除 borrowed input mutation/defer restore。
3. 补 ownership、共享参数、allocation rejection及 coverage UT，修复 S1009。
4. 运行生成检查、SCA、coverage、diff-derived build/vet/UT、BVT和完整自审。
5. 提交、推送、更新 PR，并检查 CI/review thread 状态。

# PR #27467 review round 9

## 不变量与根因

1. Group source publication 的原子边界必须覆盖 new-row append 与 existing-row update；任何中间 uniform 状态不得 normalize/release 已预留 sidecar。
2. preflight 持有状态在成功、pre-commit失败、post-commit错误路径都必须显式 finalize；finalize 不分配、不失败。
3. touched destination 集合必须受 hashmap.UnitLimit × group-key columns 约束，不扫描全部历史 group batches。
4. coverage 必须严格大于75%，至少新增一个确定命中的 changed block并留安全余量。

## 测试矩阵

- 第一批 `a/Literal`；第二批 `a/Expression + b/Literal`，最终 `[Expression, Literal]`。
- allocation controller 仅在 `Hash.GroupCount()==2` 后拒绝：不得观察到任何分配或查询错误。
- 内部 commit与完整Group operator均覆盖；input ownership保持不变。
- preflight失败、append/apply错误及正常完成均清除deferred-normalization状态。
- coverage增加至少两个可由UT稳定命中的changed blocks。

## 执行步骤

1. 实现deferred selected publication与显式finalize。
2. Group以bounded touched-destination scratch跨append/apply保持sidecar。
3. 补两批输入、post-commit rejection和coverage UT。
4. 运行SCA、coverage、diff-derived build/vet/UT、BVT、自审。
5. 提交推送并检查CI/review状态。

# PR #27467 review round 10

## 不变量与根因

1. commitGroupByChunk必须向调用方区分pre-publication与post-publication错误；只有前者允许spill/retry。
2. 最终merged-source sidecar preflight早于CommitPreview，capacity rejection必须进入现有cancel→spill→retry闭环。
3. Group与MergeGroup必须采用同一阶段协议；禁止一个重试、另一个直返。
4. retry必须重放完全相同的input offset/rows，且先取消所有selected/source preflight状态。

## 测试矩阵

- Group完整operator：第一批a/Literal，第二批a/Expression；仅在resident groups=1时拒绝一次source sidecar，必须spill/retry成功，最终a/Expression。
- MergeGroup完整operator：同一resident/source rejection与重试结果。
- pre-publication capacity error可重试；CommitPreview或publication后错误不可重试。
- retry前destination preflight状态已取消，输入ownership与row count不变。

## 执行步骤

1. 增加commit phase/result协议并接通Group/MergeGroup retry。
2. 补完整operator rejection测试与阶段控制测试。
3. 运行coverage、SCA、diff-derived build/vet/UT、BVT和自审。
4. 合并最新mo/main，重新验证，提交推送。

# PR #27467 conflict resolution round 11

1. fetch并merge最新mo/main，枚举冲突文件与双方语义。
2. 源文件按StringSource不变量与main最新行为增量合并；generated文件只通过生成工具更新。
3. 运行冲突涉及package的build/vet/UT、SCA、make与必要BVT。
4. 提交推送并确认GitHub MERGEABLE。

# PR #27467 review round 12

## 阻塞项与不变量

1. Vector.PreExtend成功必须预留所有row-parallel metadata，包括mixed stringSources；后续SetLength不得分配或panic。
2. timewin T_any partition materializer是透明选择边界，NULL也必须复制selected row StringSource。
3. ANY_VALUE、fixed/bytes MIN/MAX、MAX_BY的PreflightBatchFill/Merge必须计算winner/equal source事件，并在hash CommitPreview前为当前与future group rows预留sidecar；commit后GroupGrow/Fill/Merge不得为source分配。

## 测试矩阵

- Vector mixed [Literal, COMStmt]：PreExtend(64)后sidecar capacity覆盖future length，SetLength不分配；allocation rejection发生在PreExtend。
- timewin T_any const/selected NULL：SQLPrepare与COMStmt source保持。
- ANY_VALUE、fixed/bytes MIN/MAX、MAX_BY equal candidate：Group与MergeGroup resident state + new/equal/winning source，拒绝首次sidecar allocation必须发生在hash publication前并spill/retry成功。
- fill/merge、same/mixed source、future group row、NULL、reset/reuse。

## 执行步骤

1. 合并最新mo/main并解决冲突。
2. 修复Vector generic pre-extension与timewin透明NULL路径。
3. 扩展aggregate fill/merge source event preflight并接入future row reservation。
4. 补确定性allocation-account与operator测试。
5. 运行coverage、SCA、diff-derived build/vet/UT、BVT、自审；提交推送并resolve inline threads。

---

# PR #27467 第十三轮 review comments 修复计划

## 范围与不变量

1. selected-batch publication：`PreExtendSelectedBatch` 成功后，预留的 mixed `stringSources` sidecar 必须跨越 `setLengthAfterExtend`，直到 provenance propagation 完成；禁止在 payload/length 发布后重新分配。
2. fixed MIN/MAX admission：outer aggregate preflight 必须覆盖 runtime 对每个非 NULL candidate 执行的 source admission，而不能只覆盖最终 winner。loser、tie 和同一 group 的 transient winner 都必须在 hash commit 前完成容量 admission。
3. 本轮只修复新增 inline comments；完成后 resolve 对应 threads，但按用户要求不请求 re-review。

## 实施与测试

1. 在 selected-batch preflight 成功边界标记 source sidecar ready，由现有 `UnionBatchPreflighted` finalization 统一 normalize；cancel/error 保持逻辑状态不变。
2. 将 fixed MIN/MAX source preflight 简化为按 runtime 顺序记录每个非 NULL candidate 的 `(chunk,row,source)` admission，复用现有 `PreflightSetStringSourceAtLength`，不增加额外扫描或分配。
3. 新增 Expression destination + Literal append 的 reject-after-preflight vector 回归。
4. 新增 fixed MIN fill/merge 的 losing candidate 与 transient winner 回归，断言 outer preflight 后 runtime 零分配并验证最终来源。
5. 运行 diff-derived list/build/vet/UT、SCA、make、BVT、自审；合并最新 `mo/main` 后提交并 push。

---

# PR #27467 第十四轮 review comment 修复计划

## 不变量与根因

aggregate outer preflight 预留的 source sidecar 必须从 preflight 成功一直存活到整个 `BatchFill`/`BatchMerge` 结束；任一中间 same-source mutation 都不能提前 normalize/release，否则后续 mixed-source mutation 会在 hash commit 后重分配。

## 实施与验证

1. 在 aggregate base 上集中 retain/finalize 所有 state vector 的 source preflight；`applyStringSourceEvents` 成功后 retain。
2. ANY_VALUE、fixed/bytes MIN/MAX、MAX_BY 的 BatchFill/BatchMerge 均用 defer 在所有成功/错误出口 finalize。
3. 将 ANY direct regression 改为 Literal→COMStmt 逆序，保持 reject-next-allocation；同步增加/调整 Group 与 MergeGroup 逆序 rejection+spill/retry 回归。
4. 运行 diff-derived list/build/vet/UT、SCA、make、BVT、自审，提交推送并 resolve thread；不请求 re-review。

---

# PR #27467 第十五轮 P1 Group-key preflight 修复计划

## 不变量与根因

Group-key current/standby vector 的 source sidecar 从 preflight 成功到 `groupKeySourcePublication.finalize()` 前必须保持 reservation。`finalLength == Length()` 的 existing-row preflight 不会自动设置 ready，导致 same-source update 提前normalize，后续 mixed update在hash commit后重分配。

## 实施与测试

1. Group-key preflight 对每个成功 admission 的 destination 显式调用 `RetainStringSourcePreflight()`；沿用 publication finalize/cancel 的统一归一化和释放边界。
2. 添加 all-existing Literal→COMStmt 逆序 reject-next-allocation 回归，证明 hash publication 后无分配。
3. 添加 current batch 已满（8192）+ existing Literal/COMStmt + standby new Literal group 回归，拒绝第三次 allocation，断言 publication成功且8193组完整。
4. 运行 diff-derived list/build/vet/UT、SCA、make、BVT、自审；合并最新main，提交推送；不请求re-review。

---

# PR #27467 CI failure 与最新 main 冲突修复计划

## 诊断

- 失败 job：Matrixone CI / UT Test on Ubuntu/x86，run 32834433346，job 97760234017。
- 唯一失败：`TestCloneCommitFailureRollbackKeepsSourceFiles`；直接错误是 logservice/Hakeeper connection reset。失败窗口同时出现 dragonboat tick/propose timeout、heartbeat timeout、broken pipe，未指向本 PR StringSource 改动。
- PR 当前 `CONFLICTING/DIRTY`，`mo/main` 已前进到 `8dd1efc201`。按项目规则先 merge 最新 main，并保留 main 对冲突代码的最新修复，再叠加本 PR StringSource 语义。

## 步骤

1. merge `mo/main`，逐文件按语义解决冲突，不恢复 main 已撤销行为。
2. 检查最新 main 是否已包含该 DML UT/cluster timeout 的修复；在合并后运行精确失败用例验证，不为基础设施超时添加重试或弱化断言。
3. 运行冲突与 diff-derived owning/dependent packages 的 list/build/vet/UT、SCA、make、BVT。
4. 自审、提交、push；不请求 re-review。

---

# PR #27467 第十六轮 review comments 修复计划

## 不变量与根因

1. Vector append/union publication：source sidecar admission 成功后必须保留到值、长度和来源全部发布完成；任何中间 `setLengthAfterExtend` 或逐行 metadata mutation 都不得 normalize/release reservation，错误出口必须统一 finalize，且 payload/length 发布后不得再发生可失败分配。
2. 透明 column projection：const NULL 与非 NULL 一样携带输入 StringSource；缓存新建及复用都必须覆盖当前输入来源，禁止继承上一次缓存来源。
3. LAG/LEAD：显式 default（包括 NULL）是被选中的值，必须通过 Union 路径复制 default row 的来源；仅无 default 的合成 NULL 为 Expression。
4. Buffered batch decode：任一 metadata apply 失败必须原子清理所有已发布 prepare/runtime/source metadata，不能让前序列残留来源。

## 实施与测试

1. 统一 Vector 直接 append/UnionOne/UnionMulti/Union/UnionBatch/GetUnionAllFunction 的 source reservation retain/finalize 生命周期，覆盖全部成功与错误出口，保持 source-free 快路径无额外分配。
2. ColumnExpressionExecutor 的 const NULL cache 每次 Eval 从输入覆盖 StringSource；测试新建、复用及来源变化。
3. LAG/LEAD 显式 NULL default 统一走 `UnionOne`；测试 COMStmt NULL default 与无 default Expression 对照。
4. Batch decoder 两个错误分支复用完整 metadata cleanup；测试第二列 sidecar OOM 后第一列 source/prepare/runtime 均回滚。
5. 合并最新 `mo/main`，运行定向 allocation rejection UT、diff-derived list/build/vet/UT、SCA、make、BVT、自审；提交并 push，不请求 re-review。
