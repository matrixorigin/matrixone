# PR 31944：执行期常量与混合时序索引访问的设计 v2

设计者：`gpt-6.1-terra` / `max`，2026-11-02；设计稿已由独立 gpt-6.1-terra/max 审查通过；实现与验证另行记录。
关联 issue：#31619；实现 PR：#31944。原正确性问题已由 #31624 修复。
设计审查冻结稿 SHA-256：`3270f30f40f8e21efdf7e36b12ec0d98fc918b75c728a9ac62e7c903a64bcd1a`。
补充设计评估批准删除不可达的 witness/legacy translator，保留入口类型及 collation 防线；
成本估计和枚举 metadata 按值携带，保持预算及取消语义。
本文是现行访问合同；数学语义见 [混合时间范围](20261028-temporal-index-ranges.md)。

## 1. 问题、合同与范围

#31619 原始错误结果已由 #31624 的安全回退修复。#31944 是性能跟进：在 SQL 语义要求
DATETIME/TIMESTAMP 跨域比较时，恢复可证明安全且成本合理的剪枝和自动覆盖索引。
完整 SQL 行谓词始终决定结果；这不是所有查询都必须走索引的承诺。

不变量：所有支持和回退路径的结果、错误与 warning 语义等价于原谓词；仅在验证有效的
事务/索引/调度域内，使用包含所有潜在匹配行的必要物理范围。反例是错过一行、增加或
丢失一个行级诊断、使用其他事务版本的隐藏表，或跨执行保留不再合适的物理选择。
公共 SQL 的 AUTO 对 `IGNORE INDEX` 比较和独立的已知值比较是黑盒 oracle；
带类型的 plan/事务状态和实际 reader 路径是各自独立的白盒 oracle。

v2 将三件事分给各自 owner：执行期边界能否安全求值；物理索引键范围能否覆盖 SQL 匹配集；
现有 regular-index 选择器如何选索引并生成扫描。`NOW()` 与其他执行期稳定值共享第一项，
DATETIME/TIMESTAMP 跨域比较另需时区、精度和编码处理。逐行变化的 `RAND()` 不能一次求值。
同域整数继续走现有普通索引逻辑，不因整数由表达式生成而单独分支。

冻结在已有无参数来源的 SELECT 形状、固定时区、本地调度、literal/NOW 准入、
单个混合比较的 AUTO 覆盖索引与回退。不增加 temporal backfill、prepared AUTO、
跨 CN 放宽、命名时区 AUTO、新缓存、后台任务、表达式注册框架或新 wire capability。
普通 prepared、EWM、同域索引和现有 hint 行为维持当前合同。

## 2. 执行期边界：复用既有语义，集合不扩大

表达式稳定性的共享 owner 是现有 `pkg/sql/plan/function` 元数据和检查：
`function.IsRuntimeConstant` 与 `GetFunctionValueUnstableByID`。
`temporalCandidateBoundSafe` 保持如实命名，组合这些原语和其已有正向形状/来源证明；
`evaluateTemporalCostScalar` 仍是当前唯一生产调用方使用的时序成本观察函数。
不把只被时序路径调用的窄白名单改名为通用 `canObserve`，也不为制造复用扩张其准入。
稳定性只是必要条件；不能仅凭非 volatile、`executionStable` 或 `realTimeRelated`
推断无副作用、无诊断、无行依赖或可提前求值。

本 PR 精确维持旧集合：无来源的 literal；已解析的 `CURRENT_TIMESTAMP` overload 0，
无参数或一个证明为合法 0..6 的 literal FSP；类型、scale 和 owner 检查沿用原代码。
参数及其物化 literal 的来源、未知函数、未证明来源、子查询、惰性分支、逐行 volatile、
可能改变 warning/error 次数的表达式一律不扩大准入。原查询级 `ParameterFree`、
`temporalCandidateHasUnprovedSource`、原 owner 和完整谓词诊断证明仍须通过。
在本次时序成本路径，`ScanStorageFilterEligible` 仍用于实际 base block filter 的剪枝抵扣；
它不是“所有稳定表达式均可提前运行”的许可。

`evaluateTemporalCostScalar` 的合同不变：
先验证预算和输入大小，复制表达式，用当前 process 的隔离子进程和 warning sink 求值；
仅接受单值、正确类型与 scale、受限大小的结果。warning、不可证明的转换和不支持情况
放弃可选代价观察；取消、资源及其他既有真实错误按原错误归属传播，不吞掉。
结果只给 CBO 估计使用，绝不把观察到的时钟值固化到执行计划；执行表达式仍读取当前
语句 process 的时钟，`realTimeRelated` 的 prepare 折叠限制不变。

## 3. 类型适配与残余谓词

`pkg/container/types/temporal_index_bounds.go` 的 `BuildTemporalCandidate` 继续负责
DATETIME/TIMESTAMP 跨域逆像：返回覆盖所有可能命中行的必要范围，无法证明时返回 Full。
处理比较方向、strict/inclusive、FSP 舍入、NULL、零值/sentinel、物理范围尾部、
时区及 DST、越界和取消。普通同域比较不经过此适配器。

`temporal_candidate_expr.go` 继续产生运行时 direct/serialized key 表达式和 empty guard；
生产序列化入口仅接收首组件的复合 binary key，并保留类型、Unicode/collation 检查。
实施前的追加源码追踪证明：当前合法生产输入均按精确类型绑定，witness 不能失败；
移除仅服务该不可达 fallback 的 witness 与 legacy translator。函数绑定、资源和取消错误
继续传播；不支持的物理 metadata 放弃可选候选。生产 AUTO 没有非空 equality prefix。
`TemporalCandidateFilter` 标明可选存储必要条件，`TemporalCandidateGuard` 只允许已标记的
内部空范围触发空读；深拷贝、fold 和反序列化不得丢失标记或误认普通 false 表达式。
Full/失败/关闭 narrowing 必须不漏行；候选可能多取行，但不能作为最终 SQL 结果。

覆盖索引 scan 的 `FilterList` 保留完整、重映射后的原始 SQL 谓词；
`BlockFilterList` 保留一个已标记的必要范围。最终投影即使只返回主键，也必须保留
残余谓词所需的隐藏键。storage fail-open 或返回额外块时由 residual 精确过滤。

## 4. 共用候选选择及覆盖扫描发布

在 `pkg/sql/plan/apply_indices.go` 扩展现有 `regularIndexAccessCandidate`，
仅增加包内 transient 信息：按值携带的 `encodedRegularIndexEstimate`、本候选 base work、
以及“需要时序 necessary bound”的标记。信息不进入 plan、协议、cache 或注册表。
现有普通候选的构造、优先级、异常行为和 nil estimate 的评分保持不变。
估计仅在时序评分的同步调用内取地址；普通候选继续传 nil。枚举的借用 metadata 和
剩余预算也按值返回，只有 winner 的应用局部副本可把预算地址传给同步验证。
这避免为最终回退 BASE 的尝试分配访问对象；实际分配收益以基准为准。

将 `temporal_cbo_access.go` 的 `applyCostedTemporalIndex` 拆为已有选择器调用的
候选准备/枚举及选中后的构造；删除其独立 `temporalCBOProposal` 最佳候选循环：

1. 原有普通候选先走 `consider`，完成现有 `best`、`sawScorable` 和 `fallback` 决策。
2. 只有原 late gate 完全成立时才枚举：late stats finalized、原 owner 安全、参数来源、
   固定时区、允许的本地 placement、非 FORCE、非小表/忽略统计、非 sorted preference，
   且 `best.indexPos == -1 && fallback.indexPos == -1`。
3. 保留当前 `temporalBaseReaderShapeSafe`、单个 filter、覆盖列、leading part、
   索引完整性和工作预算检查；计算 scalar、必要范围、stats selectivity、实际 base work，
   并保留 `len(costCtx.unknownRangeLowerSelectivities) != 0` 时直接放弃时序候选的保护。
   否则 `scoreWithEstimate` 会改用 `baseUpperWork` 作内部比较，改变现有评分合同。
   按现有构造预付预算生成和计量动态 hint。每个候选只携带 estimate 与 filter 位置。
4. 同一个 `consider` 调现有 `scoreWithEstimate`：时序候选浅复制 `costCtx` 得到局部 view，
   将 **view.baseWork** 设为 temporal base work 后再评分，因为内部 composite 拒绝也读取
   `ctx.baseWork`；不得改写共享 `costCtx.baseWork`。保留现有同步 scratch 重置合同，
   浅复制共享的 scratch 底层数组不能被并发评分使用。评分结果还必须 `valid && !rejected &&
   work < temporalBaseWork`，再按原迭代顺序取最小值。普通候选仍走原 nil estimate 路径。
5. 时序候选无效/拒绝时不写普通 `fallback`，不改变普通 `sawScorable` 或 hint 优先级。
   winner 验证或构造未应用时清除这个临时 winner，继续原有 spatial/base 路径；
   真实错误按现有路径返回。不能把未计价的时序候选作为 FORCE 或兜底候选。

这保留“普通候选先于时序候选”，不扩大竞争范围。成本仍包括 index block 输入下限、
残余谓词、解码、输出及构造；只有实际可执行且与 base 行过滤匹配的 block filter
才能抵扣 base 成本，不能修改共享统计。

存在实际 BASE block filter 时，先保留原有 shape、谓词一致、存储剪枝、统计、预算
及取消准入，再由原生 relation 同步观察当前快照下已缓存的块元数据。对象 overlap
只参与既有统计准入，不再作为对象内块数的依据。观察复用实际 reader 的混合比较
matcher，先判对象，再判块；完整匹配块的行数用于本次私有 BASE 成本。

缓存入口逐层检查支持的元数据版式、对象身份、列和块边界，只交出复制的 ZM；
没有缓存、未知版式或不完整元数据时放弃。观察不加载文件、不订阅、不等待历史，
不创建 partition，也不保留新的缓存或统计。现有事务、订阅、transport、GS 和不可变
partition 前后复核；内存行、appendable、删除数据、总体不一致、预算耗尽或未知
matcher 均回退 BASE。对象和块遍历共用现有预算，多个候选只观察一次。

本次块输入观察的设计评审版本 SHA-256：
`87c88e3d26ab69879f3483f55a0090014b7c8d5ae6c9c3125a9b59bc9d9f2547`。
缓存短锁和命中计数仍有运行成本；该实现不承诺任意布局最优，实际收益和不回退
要求须由完整结果对照、布局控制及预先固定的性能门槛验证。

把两个覆盖路径共有的最终节点发布代码收敛为 `publishRegularIndexOnlyScan(...)`：
参数是已解析 ref/definition、源扫描、index、tag、调用方准备的 row/block filters 和映射。
它保持 `IndexScanInfo`、`ParentObjRef`、snapshot、绑定 tag、`Limit`/`Offset`、hint 继承、
`appendNode` 和 `forceScanNodeStatsTP` 的原有行为；时序结果设置 `ForceOneCN`。
普通调用仍按现有规则构造 leading filter、必要 residual、映射及其原有错误行为；
时序调用构造完整 residual 和 block hint，不假设普通 leading filter 即可代替原谓词。
这是具体发布 helper，不引入 planner framework 或独立候选注册机制。

## 5. 关系、事务、调度和发布 owner

本次**保留** `TemporalPlanningScope`、`TxnCompilerContext` 的 lexical frame/capture、
`engine.TemporalRead`、`TemporalIndexRelation` 及检查：现有共享 resolver 尚未证明能替代
这些保障，改名或直接调用普通 Resolve 并不等价。

正常 `Resolve` / `ResolveIndexTableByRef` 是关系和完整表定义的唯一来源；scope 只同步借用，
不关闭关系、不创建第二个快照。原 frame 的嵌套、并发、revision、cleanup 和失效协议保留；
operator、transaction ID、workspace、snapshot revision 和 catalog visibility 在捕获前后核验，
stamp 不授权 workspace 并发访问。订阅、临时、历史快照和不受支持关系继续回退。

`ReadTemporalCostStats` 只从相同 account/database/table 的普通 GlobalStats 观察复制标量，
沿用 TryLock、观察指针一致、显式 schema version 不兼容则拒绝等规则；统计只提供估计，
不授权读取行。只为成本胜出者正常解析隐藏表，保留 `VerifyTemporalIndexPair` 对精确
base/hidden 实例、事务域、表版本、parent、index membership 与布局的发布前验证。

时序 winner 先在局部暂存 node、filters、列映射及名字信息，不改变共享 builder 状态。
必要的 tag 在单 owner builder 上暂取 `nextBindTag + 1`；溢出或发布前计数已变化则放弃。
完成精确 pair、scope/process、budget 和 context 校验后，检查暂定 tag 仍是下一个 tag，
才递增分配 tag，并连续提交名字、列映射、`appendNode`/tag map、stats 和 `ForceOneCN`。
提交开始后不再以可选失败或迟到的取消返回 BASE；正常 builder 出错仍走原错误处理。
公共发布 helper 不得提前执行普通构造器原有的全局 map 写入；普通调用保持原有行为。
验证失败、预算耗尽或可选元数据不可用则不发布；取消在发布前返回原 cause，已成功发布的
结果不因稍后取消被追溯撤销。catalog/资源等真实错误按现有合同传播。

原上限继续生效：每 root 最多 64 个关系绑定、64 个 index 定义、16 个 index parts、
1024 列、256 字节名称和 65536 工作单位；原始 owner admission 预算 4096。
表达式深度/节点 64、protobuf 大小 16384、literal payload 4096 及取消检查继续生效。

## 6. 编译、重试、缓存与部署

沿用 `TemporalPlanningIntent` 的语句时钟、timezone 和 scheduler snapshot；
`AllowsFixedTemporalCandidate`、实际 local/`AllowsNamedTemporalCandidate` 和 `ForceOneCN`
的现有组合不变。编译前 `temporalExecutionAllowed` / placement 校验 process、timezone、
时钟、CN、scheduling、Sirius/offload 和标记 scan 的 ForceOneCN。
漂移时返回 `ErrTemporalAccessDrift`，由现有 owner 重建无时序候选的安全计划，按原规则
重新授权、验证结果元数据后编译；retry 不继承旧 narrowing 许可。安全重建或授权失败须
返回错误，不能执行被拒绝的旧拓扑。

在原 owner、任何诊断/成本选择之前保留 `TemporalIndexNeedsReplan` 判定；
潜在混合访问即使最终选择 BASE，也与 INDEX 一样禁止普通缓存插入和 generation 替换。
每次普通执行按新 process、clock、stats 和本次关系快照重新估价。
已知 prepared owner 不进入本 AUTO 路径；其原参数、类型历史、诊断和缓存逻辑不变。
不恢复已删除的时序模板缓存，也不使用 clock epoch、selectivity 门槛或全局清缓存。

维持现有同版本 process/pipeline 时区传递：fixed offset 显式信息和可正确解码的 legacy
timezone bytes 一致，不可表示时沿用既有拒绝，不能静默换成 UTC；命名时区不因此获得
AUTO 准入。现有 plan 字段及折叠标记继续传递；不新增混合二进制 capability gate。
部署/升级仍按项目的停机升级合同，不声称新增滚动混版本兼容性。

## 7. 变更及删除清单

| 位置 | v2 处理 |
| --- | --- |
| `temporal_candidate_admission.go`, `temporal_cbo_scalar.go` | 保留如实命名的时序正向准入和有界观察；准入组合现有通用稳定性原语，接受集合及错误/诊断合同不变。 |
| `apply_indices.go`, `temporal_cbo_access.go` | 扩展既有候选和 `consider`；删 `temporalCBOProposal` 及独立 winner 循环，保留时序 adapter、估计与原 late gate。 |
| `apply_indices.go`, `temporal_candidate_index_only.go` | 抽取共同覆盖节点发布；两个调用方各自准备与原语义一致的 filters/mapping。 |
| `temporal_candidate_index_only.go` | 删除仅测试调用的 `applyTemporalIndexBackfill` 和 `applyTemporalIndexOnlyScan` nil-proof wrapper；生产入口要求已解析且可验证的 pair，移除仅服务这些入口的不可验证分支；用真实选择器/验证发布入口覆盖替换测试。 |
| `temporal_candidate_index.go` | 当前 AUTO 只允许一个 filter，且拒绝非空 equality prefix；删仅服务非空 equality prefix 的发现、证明和参数分支，改为匹配第一个索引组件的当前 mixed filter，保持所有原准入与错误合同。 |
| `temporal_candidate_expr.go` 等 | 删仅服务非空 equality prefix 的拼接和参数及其专属测试；保留 typed adapter、guard、block hint 与零 equality prefix、带 composite suffix 的直接构造；删除经精确类型绑定证明不可达的 witness/fallback。`prefix_between`/`prefix_eq` 对 suffix 仍必需。保留类型、时区及边界证明测试。 |
| `temporal_planning_scope.go`, frontend/native、engine/disttae、compile/schedule | 保留原身份、作用域、成本元数据和漂移防护；本 PR 不做 native compiler owner 重构。 |
| `temporal_candidate_potential.go`, frontend plan cache | 保留 BASE/INDEX 的 replan 标记、插入/替换排除与普通 prepared/EWM 既有合同。 |
| `docs/design/20261030-temporal-cbo.md` | 改写为唯一现行访问合同，列出支持范围及此次 owner；不再追加互相覆盖的 amendment。 |
| `docs/design/20261028-temporal-index-ranges.md` | 保留数学证明，删除或改写与当前 prepared/backfill/cache 能力冲突的历史文字，指向现行访问合同。 |

设计前的 `06c6fed` **已经**删除 `pkg/frontend/temporal_ordinary_cache.go` 及其测试、
`pkg/sql/plan/temporal_cache_proof.go` 及其测试，并清除了对应 cache seal/观察状态。
这些不是 v2 将要再删除的内容；不要误删仍用于安全的 query 标记、当前 scope 和 pair 验证。

## 8. 取舍

维持现状无需变更，但会保留时序私有 winner/覆盖节点构造，以及无生产入口的路径。
v2 消除重复选择/发布、删除无生产调用的 backfill、无验证入口和 equality-prefix 代码；
复用真实的稳定性语义原语，同时明确当前正向求值准入仍专属时序路径。
性能收益必须测量，不能由行数推断。
风险是共用 helper 影响普通索引，因此限定保留既有调用顺序、分支和合同。

完全照搬 DuckDB 的 SEQ 选择会放弃已经证明有价值的 AUTO 覆盖访问；
构建通用动态 prepared recost 系统、替换 native owner 或推广所有稳定表达式则超出本 PR。
这些既不是 v2 的隐含依赖，也不是本次承诺的后续工作。

## 9. 实施后的验收条件

**语义与结构：**测试真实选择入口而非只调用 helper。比较 AUTO 与 `IGNORE INDEX` 的完整
行集合、重复行、NULL、warning/error；覆盖双方列方向、五种比较、不同 FSP、边界舍入、
zero/physical tails、固定时区及 DST 命名时区安全回退。验证实际 block hint、guard、
保留的 residual、隐藏键投影，以及 storage 返回宽块或 fail-open 时的行结果；覆盖可达的
零 equality prefix、composite suffix 及非法物理 metadata 安全拒绝；验证精确注册 overload。
正向测试 literal/NOW，负向测试带来源的参数、RAND、懒分支、会诊断的表达式、
prepared、LIMIT/aggregate、FORCE、非覆盖、named zone 和非本地调度。
同域整数稳定表达式及普通 prepared/EWM 为行为不变的对照。使用既有 fixture 和最小数据，
每个测试只承担不同的语义或状态转换证明；大数据测量只属于独立性能 harness。

**生命周期：**覆盖 DDL/index drop/recreate、stats 更新、表版本和 hidden parent 不一致、
同表不同关系实例、transaction/snapshot/workspace revision ABA、嵌套、竞争、
预算耗尽、取消在发布前后、compile placement/timezone/clock 漂移、授权失败与 retry。
校验借用关系不被关闭、失败不发布映射或节点；固定 offset、标记和 fold 在同版本进程
编码/解码及复制后保持合同。验证当前 main 的 prepared temporal history 测试。

**性能：**冻结 v2 前后同一 main 上的源码 revision 或内容摘要及 frozen main，使用同机、
相同构建参数、同 SQL、相同数据、同连接、相同统计与 warmup；先跑 A/A 确定噪声。
至少三个独立 ABBA 轮次，每 phase 预热 25 次、计时至少 100 次；各轮按相同工作负载
的两个 A phase 与两个 B phase 的中位数计算配对变化，并交替 ABBA/BAAB 顺序。
A/A 的配对 p50 差异
超过 3% 或 p95 差异超过 8% 时先降噪重测，不能报告性能 PASS。
覆盖 1M shuffled 稀疏→稠密和稠密→稀疏、稳定稀疏、稳定稠密、同域整数及既有
prepared 对照；时钟变更后不插入 EXPLAIN，用外部 observer 记录每次实际 BASE/INDEX、
是否重新规划、blocks/rows、结果、p50/p95、进程 CPU 和分配。
候选结果必须正确；稀疏 positive control 必须实际减少扫描量且 p50 优于 main BASE；
v2 相比同 base 的 v1 控制，配对三轮中至少两轮的 p50 回退不得超过 5%，p95 不得超过
10%；超限须归因并修正或明确缩小设计。CPU/分配和 scans 用于解释而非替代延迟。
稠密转换必须真实重估，不得沿用先前稀疏 INDEX。原历史混合 harness 已不存在，
该历史观察无法声称被精确复现。

最终 revision 需运行受影响 owner 的正常与 race 测试、相关公开 SQL/BVT 和配置的增量 SCA，
记录完整命令、非空测试选择、退出码及 revision。已有证据仅在相关代码、模式和依赖
逐项证明不变时复用；既有结果不是当前 rebase 或未来 v2 的交付 PASS。

DuckDB 参考的本地源码 `cc9648d736fd7e4fb2fc813cba6f6d34932c3386` 与实测 wheel
`2.0.0` / `d54cbbe3df` 不是同一构建。wheel 的有索引/无索引 28 例结果相同，
且 prepared 稀疏/稠密/稀疏改变访问方式；这仅支持上述职责划分，不证明 v2 已实现。
