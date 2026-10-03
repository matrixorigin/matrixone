# #29531 v4 最小增量：普通 PRE 外层可选 PK runtime filter

日期：2026-09-30。设计：gpt-6-astra / xhigh。只读诊断，无 source 修改、服务启动或测试执行。

基线 HEAD：`c229fcb77632905c0c9e9cebf5184d57c2c503cc`。本增量依附已批准 v4，原文件 SHA256 `7b3535a3281efd5ebc568b3165a3765c309323979b4807b262b81e3558c9af99`；不替换其 required-domain / CPU-route / protocol / snapshot / lifecycle 合同。须由6.1独立批准本增量后实施。

## 1. 反例与源码拒绝点

父任务 public SQL QA 中，`cast(file_id as bigint)=value` 与 `abs(file_id)=value` 均结果正确，但 readers=1，entries约17/25 ms；同一source数据、同一800 membership domain的普通索引谓词、nullable IN及query-vector cast进入readers=2。产物：

- `rootcause-main-final-16-cn2-staged-qa-challenge-cast_predicate.log`
- `rootcause-main-final-16-cn2-staged-qa-challenge-function_predicate.log`
- `rootcause-main-final-16-cn2-staged-qa-challenges.json`

两条反例已有合法shape：`INNER(rowFetch TABLE_SCAN, SEMI(direct VECTOR_INDEX_SCAN, membership TABLE_SCAN))`；同source、直接整数PK等值、单轮、work Objects=8，ScanWork完整。函数/cast位于table FilterList，不在join PK或query route表达式中。普通索引无法覆盖该谓词导致row fetch成为普通table scan，并触发已有普通INNER runtime-filter生成。

源码链：

1. `pkg/sql/plan/runtime_filter.go:736–752` 为outer INNER生成optional build/probe pair：build挂root，probe挂root.Children[0]的直接TABLE_SCAN。
2. `pkg/sql/plan/vector_pre_placement.go:263–278` 的最终扫描probe校验仅允许`indexTags`；该pair不是INDEXjoin的tag，故`RequiredIVFPlacement`返回false。
3. `stats.go:GetExecType`对未证明required placement恢复query-wide AP_ONECN，`compileVectorIndexScan`同样不放开required分片。日志的AP_ONECN无worker discovery和readers1与源码一致。

这是v4资格闭包遗漏，仍属原decoded-cache工作集集中机制。没有证据表明cast或abs自身要求vector entries扫描限于单CN。不能仅因正确结果已经通过而忽略这个性能反例。

当前因果确认来自源码分支和实际EXPLAIN；实施UT须以“同一完整plan只去掉这对optional RF即可从false变true”作精确判别，避免把其他资格问题混入。

父任务随后对当前保留的同对象fixture测得cast/abs基线中位QPS约31.066/29.582，普通INDEX control约167.4；两条反例仍readers1。服务已停止，下一candidate必须使用`PROBE_EXISTING=1`保留这些8个objects，不重新建索引。不能把早一轮不同fixture的QPS与此轮拼成倍率。

## 2. 最小改变：只接受已证明root INNER的一对optional PK RF

生产owner保持`RequiredIVFPlacement`，预计仅`pkg/sql/plan/vector_pre_placement.go`增加一个小的纯校验helper及最终probe允许分支；不改planner生成规则、metadata calls、Stats、reader、broadcast、protocol、cache或写路径。

在现有完整shape、`pkJoin(root)`、`pkJoin(member)`、source身份及access树检查全部通过之后，额外识别root的optional pair。它不是对所有optional RF的豁免。

### 2.1 资格不变量

1. root是原helper已经证明的唯一outer INNER：非shuffle、非right、恰好一个直接整数PK等值。不能从任意INNER/SINGLE/INDEX节点收集额外tag。
2. root的build list允许0或1项；0沿原行为。1项必须非nil、Tag>0，且不同于required membership tag、全部已证明INDEX tags。
3. probe节点必须是`root.Children[0]`的**直接TABLE_SCAN**。这是已有普通RF生成owner的真实形状（runtime_filter.go目前只向直接table scan生成该pair）。不新增向PROJECT/任意descendant搜索probe的通用逻辑。它的同source/regular hidden index身份已经由access检查证明。
4. 两端必须均`MustApply=false`、`UseMembershipFilter=false`、`ScalarPredicate=false`、`MatchPrefix=false`、`NotOnPk=false`。不接受approximate membership、partial key、prefix、scalar predicate或required filter伪装为optional。
5. 只证明这对RF来自已验证的PK join，不复制wire/encoding合同：
   - 正常main build可以`Expr=nil`，规范表达式在`BuildExpr`。优先检查BuildExpr；若没有则检查现有legacy Expr。实际build表达式必须为直接col `RelPos=-1, ColPos=0`、声明类型为已证明的整数PK。这是该root唯一等值条件的unique-key slot0，**不是**右child普通输出列0。若两种表达式同时存在，两者都必须指向该同一PK slot；computed/serial/另一个slot不在本资格内。
   - probe.Expr为直接col `RelPos=0`，ColPos落在该scan当前TableDef内，列为PK：base scan用source.PkeyColName，已验证covering hidden index用IndexTablePrimaryColName，类型吻合。扫描经过remap与列裁剪，**不能**拿SourceTableDef原始PK ordinal比较当前scan的ColPos。
   - 不复制KeyEncoding、ProbeType、legacy兼容、raw codec版本判定矩阵。已有`makeExactRuntimeFilterPair` / `exactRuntimeFilterPlanEncoding`负责生成正确pair，执行期HashBuild负责payload/type/编码验证和安全PASS；`QueryBuilder.exactRuntimeFilterPairContractValid`是现有完整pair验证owner（目前用于fuzzy planner验证），不能仅为placement重写一份或依赖虚构compCtx调用它。本增量也不把该函数改成通用framework。
   - 这里接受的是出处和依赖关系已证明的pair；运行时不能物化可选exact payload时仍沿已有PASS。tag来源未知/外部、computed key、错误probe列等**placement证明失败**继续local；这与wire合同由其原owner负责没有冲突。
6. 在完整`q.Nodes`内检查该tag恰好一个build且owner就是root，恰好一个probe且owner就是这个直接rowfetch scan。包括非TABLE_SCAN和不可达节点，不能让同tag出现在membership producer、vector、另一个join/scan或query外部来源；重复、缺端、孤立端均不合格。
7. 仅在上述证明成立后，最终TABLE_SCAN probe校验额外允许这个**确切节点和确切pair**。其他probe仍须原INDEX tag证明；不得删除最终orphan检查、把所有optional tags塞进allowlist或按MustApply=false通配。
8. root/build pair不增加任何localScans例外。unexplained ForceOneCN仍使query local；required tag、各INDEX局部tag及协议/backend/work gates原样保留。

可实现为返回validated probe节点ID及tag的小helper，或在现有函数内保存这两个局部值。禁止新的持久proof状态、planner字段或通用runtime-filter分类框架。对只有这对optional RF阻碍的public查询，GetExecType与compile自动经原helper恢复同一分布资格。

## 3. 为什么现有执行可以闭合

两条不同的数据依赖：

```
完整membership table scan → 广播 → 每CN required HashBuild → 本地vector分片
每CN SEMI候选 → 原outer build merge → 完整候选HashBuild → rowfetch可选PK filter
```

required domain仍先于每个entries reader，probe/budget/route不变。outer optional filter来自membership之后的候选并集，用于避免读无关rowfetch PK；它不参与决定vector domain，更不能反向过滤membership producer。

`compileBuildSideForBroadcastJoin:7225+` 已先合并全部right buildScopes。单probe owner在colocation后构造本地完整HashBuild；多probe owner则通过原SendToAll批次分发完整build，每CN本地HashBuild发布optional IN/PASS/DROP。根因修复的required broadcast图校验照旧。新增资格不要求更改广播层或新增optional消息。

等待图无环：membership扫描没有outer tag；只有rowfetch扫描等待候选build；候选build等待vector；vector等待membership。禁止把同tag允许到membership scan，因此不会出现 `membership → vector → outer keys → membership` 环等。

**现有optional禁用语义的源码澄清：**c229没有找到“AP_MULTICN统一删除普通INNER RF”的compile分支。存在的是scalar-predicate topology的`disableScalarRuntimeFilter`（仅scalar合同），以及普通HashBuild在spill、超budget、无可用payload/不支持合同等情况下发布PASS；后者由普通probe接受。此次不移植scalar禁用规则、不把ordinary变required，不发明一个不存在的compile disable作为安全依据。若existing普通pipeline执行某分支选择PASS，保留它。required membership的PASS仍是error。

已有查询或某compile阶段若完整移除optional两端，helper的0项路径自然仍合格；若只剩孤立probe，则必须继续local/error，不以“optional”名义忽略残端。

## 4. 资源、行为与回滚

- 不增加原v4的metadata resolve或Stats调用；新增对已在内存的q.Nodes的一次有界tag计数及常数大小字段校验。
- distributed required vector的cache/CPU policy、每CN1reader、P×B候选与domain内存预算沿已审v4。
- plain rowfetch/membership TABLE_SCAN可能读取更多基表数据，这是缺少普通索引覆盖的原SQL成本；不能宣称补齐资格后必与covering索引谓词同速。但vector entries工作集应恢复稳定多CN cache复用，不能再因optional pair集中到1CN。
- 不降低候选/probe，predicate本身仍由原scan执行，cast、abs、NULL和异常语义不重写。不新增函数白名单；本增量只判断PK RF。v4的其他类型/shape限制不放开。
- 回滚仅撤回这个资格分支，恢复现有单CNfallback；无格式/持久化/cache迁移。mixed version继续v4的整体local gate，无新协议版本需求。

预计1个生产文件约40–80行逻辑，外加现有plan/compile测试文件；如实现需要改执行算法或抽新wire验证层，先返回设计审核。

## 5. 最小验证与验收

### A. Plan UT：真实生成路径和定位实验

复用现有IVF builder fixture，使用普通base-table access（无可用regular index，或已生成cast/abs谓词）；完成generateRuntimeFilters、forceJoinOnOneCN、实际remap/projection阶段后再调用helper。

- cast与abs生成完整root optional pair；保留pair时eligible=true，GetExecType=AP_MULTICN，MustApply membership仍true。
- 同一plan只清除optional两端仍eligible；只清除一端不合格。这也验证源拒绝点，不只手工“看起来像”shape。
- current versioned BuildExpr + Expr=nil，以及合法legacy slot表达式都通过；PK经过scan列裁剪后正确识别。完整wire合同沿用既有owner测试，不复制encoding矩阵。
- 一个表驱动negative set覆盖：same-typed nonPK、computed build、错slot/RelPos、prefix/NotOnPk、membership/scalar/required flags、tag碰required/INDEX、root额外第二build、tag出现在membership/外部scan/第二producer/非TABLE节点、未知orphan及unexplained ForceOneCN。保证未知RF没有变宽。
- 原regular INDEX staged plan与现有nullable IN/query-vector cast仍合格；不重跑全类型/场景笛卡尔积。

### B. Compile UT：证明沿用完整普通广播，避免环等

小scope fixture覆盖rowfetch单owner与2owner：outer build得到全部SEMI候选（来自每vector partition），每rowfetch consumer的普通RF producer在同CN；membership producer无outer tag；required完整broadcast仍通过原validator。以现有broadcast helper生成实际operator树，避免自己镜像实现。

普通outer HashBuild的PASS保持optional/no-prune；required membership PASS仍失败。可复用已有PASS UT，不扩展一个新的通用topology validator。此次没有新资源owner；不因单纯资格变化要求新的大规模race矩阵，保留现有v4生命周期门禁。

### C. 运行验收：只复用原同main对象、16MiB、2CN

重跑已失败的两个public SQL：cast predicate、abs predicate。保持同800 domain/queryvector/K/probe，先warm，再固定小请求数。必须同时得到：

- actual scheduling=AP_MULTICN、required readers=2，非仅结果正确；普通optional RF可存在或沿原合同PASS。
- 相同membership、唯一PK、距离升序、tie-aware TopK/recall对照通过；probe/centroid route和每shard预算未下降。
- 每CN实际entries owner/cache活动符合v4分片，vector entries不再重复约20MB单CN工作集的冷解压路径；报告entries时间和QPS，不只总query时间。
- 已通过的普通索引谓词保留readers2和结果作为control；无需再跑4.2、64MiB反事实或10M全量，已有根因闭包证据复用。

这些是补齐同机制反例的验收，不重开已批准v4设计或扩大资源。完成后独立6.1检查production diff及本增量对应证据。
