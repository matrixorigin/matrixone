# 普通 View 元数据：按需推导的生产接入与演进设计

- 状态：**全新设计草案，待评审；不授权实现或激活。**
- 需求背景：[#29433](https://github.com/matrixorigin/matrixone/issues/29433)；跨组件验收：[#29440](https://github.com/matrixorigin/matrixone/issues/29440)。
- 源码核查基线：`23488e1eb2c50f4d3638dee1d50715bbc1eb1f67`（2026-10-28 的 `mo/main`，含 catalog 4.0.7 合并）。本文中的函数名是导航，不是已实现本方案的声明。
- PR #31282 及其未交付续作不再作为实施方案；本文不移植那些修改，也不继承其批准或运行证据。已经合入 main 的公共推导器、权限和升级基础设施仍是需要评价的现状，不因弃用该 PR 而回退。
- 本稿批准后，实施 PR 必须链接**新设计的批准提交**。下列决策取代旧 S1 的对应条款之前，须在本设计 PR 中明确批准，不能把“从零设计”当成生产修改许可。

## 1. 先定义问题，而不是继续补接线

用户要的是：源 schema 在某条语句中可见后，查询结果、CTAS、DESC/SHOW、I_S 和协议描述遵循同一套输出推导规则；历史读仍描述历史，权限撤回不能被旧数据事务绕过。用户不需要后台任务把每次推导重新写成目录事实。

当前 main 有可复用的部分，但未形成该生产合同：

| 事实 | 当前 owner | 不能由此推断 |
|---|---|---|
| 只读推导器、请求 memo 和显式 metadata request 已存在 | [view_schema_request.go](../../pkg/sql/plan/view_schema_request.go)、[view_metadata_request.go](../../pkg/sql/plan/view_metadata_request.go) | 所有普通 SQL 已选择该路径 |
| 普通构建的 validation selector 返回未选中；测试构建另有 registry | [view_schema_validation.go](../../pkg/frontend/view_schema_validation.go)、[view_schema_validation_integration.go](../../pkg/frontend/view_schema_validation_integration.go) | registry 或测试 tag 是生产模式权威 |
| 数据事务、创建环境、执行资源代由 frontend provider 借用 | [view_schema_provider.go](../../pkg/frontend/view_schema_provider.go) | 父数据事务也能代表当前权限 |
| 现有 authorization epoch 独立于旧数据事务 | [authorization_snapshot.go](../../pkg/frontend/authorization_snapshot.go) | snapshot 名字解析本身完成了跨租户授权 |
| C06 根据 scan snapshot 读取订阅候选与 publisher schema | [subscription_metadata.go](../../pkg/sql/colexec/table_function/subscription_metadata.go) | 历史 publication 资格在今天仍有效 |
| bootstrap 已将未发布的 4.0.8–4.0.17 合并到 4.0.7；旧源升级在普通入场前完成 | [upgrade.go](../../pkg/bootstrap/upgrade.go)、[server_bootstrap_wait.go](../../pkg/cnservice/server_bootstrap_wait.go) | 可以把同一 catalog-version 路由直接当作在线 View 激活接口 |
| `create_version` 与 task/range 状态是既有目录升级权威，4.0.7 枚举依赖输入被冻结 | [service_upgrade.go](../../pkg/bootstrap/service_upgrade.go)、[4.0.7 设计](20261028-catalog-upgrade-407.md) | 允许在线改写它们的语义、恢复旧的未发布版本链或削弱启动 fence |

历史 issue 是需求来源，不是本基线的新复现。本文不附带内核测试通过、性能提升或旧备份恢复成功的结论。

## 2. 核心模型与不变量

刻意区分四种概念：

- **L/N**：旧/新二进制。部署只允许整集群停机替换，运行中不存在受支持的 L/N 混部。
- **C/D**：同一个 N 二进制内的兼容读语义 / 按需推导语义。D 默认不激活。
- **T**：数据可见域，包括事务身份、statement snapshot、workspace/own-DDL 修订、显式历史快照和实际对象身份。
- **A**：该语义语句的当前授权域，包括调用者 account/user/role、授权 snapshot、租户当前元数据语义属性、publication 资格。它不来自旧 T，也不是“每读一个对象就追一次最新权限”。

对一个已准入的根对象：

```text
M = mode(tenant.view_metadata_version visible in A)                 // C 或 D，语句内不变
Visible = Admit(surface, caller@A, namespace@T, snapshot rights@A)
if Visible:
    D_schema = Infer(definition@T, creation environment, T, binding inputs)
    Output = Present(surface, D_schema)
```

`Infer` 返回输出列、默认值/provenance 规则和完整依赖证据，不执行 View 数据、不发布目录写、不读取 refresh 完成标志。数据执行仍由原 query/compile/pipeline 负责。

必须成立的八个不变量：

1. **单语句一致**：一个语义语句只有一个 M/A；每个数据域 T 自身一致。显式跨历史时点的 SQL 可拥有多个 T，不能把它们错误合并为一个最新 snapshot。
2. **当前权限、历史数据**：权限/publication 在 A 判断，schema 在 T 读取。历史授权记录不能复活已撤销资格；A 一旦建立则保持冻结，下一语句取得新 A。
3. **先许可、后推导**：不可见的 bulk 候选不进入 binder；权限查询失败不是“不可见”。SELECT 执行授权不能被 SHOW 许可替代。
4. **没有旧成功兜底**：D 推导失败不能返回持久 Cols、旧 prepared 描述、上次 memo 或某个源表的同名列。
5. **定义不是派生副本**：保存定义、创建环境、冻结 star、所有者/安全属性；源 DDL 不改写这些定义事实。目录派生列不是 D 的权威。
6. **代际隔离**：计划可保留不可变值，不能保留活 session/事务/request。旧结果/cleanup 不能读写下一执行代。
7. **读路径无持久协调**：metadata freshness 不新增日志、队列、lease、HAKeeper 状态或全局 barrier。
8. **错误保真与有界终结**：目录/RPC/取消/预算错误保留原因；任何等待有取消或上界，每个资源有唯一有效释放 owner。

非目标：materialized view 数据刷新、跨请求 schema cache、通用权限框架、新 lineage 规则、滚动升级/降级，以及 partition、存储过程、UDF、warning 兼容性扩展。现有相关能力不因本设计被删除。

## 3. 架构选择与代价

### 3.1 元数据权威的选择

| 方案 | 正确性与迁移 | 成本与运维 | 决定 |
|---|---|---|---|
| 保留当前分散的按需/持久读 | 各入口继续可能采用不同权威；补单点不能关闭全图 | 局部便宜，但定位与兼容矩阵不断扩大 | 不作为目标 |
| DDL 同步更新全部依赖 View | 可以维护物理派生列，但需定义整个依赖图的原子性、坏 View 和跨租户 fan-out | 写放大、锁范围和恢复复杂度进入 DDL | 不选 |
| 异步持久刷新 | 必须再定义 pending/stale 读、任务重试、反向失效和重启 | 把一个读计算变成持久分布式工作流 | 不选 |
| **共同只读推导 + 有界语句 memo** | 直接使用已有 MVCC；各消费者只做展示适配 | bulk 可能更贵，需预算与性能门禁；raw catalog 不承诺派生新鲜度 | **选择** |
| 立即增加跨请求 cache | 仍需完整依赖、历史/角色/订阅隔离证明 | 冷路径与维护路径都变复杂，尚无必要性证明 | 暂不做；另案基于测量 |

### 3.2 语义切换的重新选择

**本稿选择：同版本内按语句冻结模式；租户元数据语义属性提交后，已准入的 C 语句可以完成，新准入的语句按新值选择 D。无需等待所有旧语句退出。**

这不是“用一个配置绕过旧 drain”，而是明确更换切换合同：

| 方案 | 对旧语句的承诺 | 所需机制 | 决定 |
|---|---|---|---|
| 资格提交 → 封口/drain → tenant 提交 | tenant 提交后不允许普通 C 语句继续 | 持久资格、集群 admission、进程代和恢复/未知提交协调 | 不选；本功能不再提出这个更强承诺 |
| 停机完成语义切换 | 不存在活跃旧语句 | 离线升级/启动隔离；无需在线共存证明 | 可行替代，但不选为日常语义激活方式 |
| **租户属性提交 + statement 模式冻结** | 旧语句保留其语义，新语句使用新模式；旧物化 cursor 保留原结果 | 一个持久 account 属性、已有事务提交/授权 snapshot、模式敏感的复用检查 | **选择；需要本 PR 显式批准** |

选择第三种的前提是：N 中两种 reader 同时可用；两者共享同一种持久定义写入规则；D 只改变读取/推导，不产生 C reader 无法解释的新数据格式。新 binary 的 CREATE/ALTER/restore 写入路径须先满足此条件。在线激活只检查定义资格并提交一个 account 属性，不批量重写 View 列，也不把系统模板永久替换成仅 D 能使用的模板；模板选择在本次绑定时按 M 进行。若实现证明存在不可共存的 writer/格式变化，本设计不能继续声称不需要 drain，必须重新评审，而不是偷偷增加协议。

#31287 只决定二进制部署边界，**并不自动批准任何语义切换方案**。这里对旧 S1 的在线 drain/两阶段资格承诺作出了显式替换，也不再申请 P_OD2 或语义 offset 资格回执。

## 4. 单一调用链与责任边界

```text
frontend 语义语句 owner
  ├─ 建立/借用已有 authorization epoch
  ├─ 在首次模式敏感动作前冻结 M/A；再准入 cache/prepared
  ├─ 取得 T、语句资源代和合法 binding inputs
  ├─ surface policy → 已许可的 root/candidate
  ├─ 公共只读 binder → immutable schema + dependency evidence
  ├─ query/CTAS/protocol 或 SHOW/I_S adapter
  └─ pipeline / result / physical compile 终结 → 释放所有请求资源
```

只增加必要的语句内责任，不建立新的常驻服务。以下为**拟定内部契约，不是现成 API 声明**：

| owner | 输入/输出契约 | 不得承担 |
|---|---|---|
| frontend statement scope | 冻结 `{M,A,T,execution generation}`；创建/关闭绑定和授权 reader | 持久 refresh、跨语句成功缓存 |
| frontend surface admission | `Admit(surface, root identity, T, A)` → admitted / invisible / error | 用类型推导成功代替 SELECT/DEFINER 授权 |
| planner common inference | admitted root + T + environment → immutable schema/provenance/dependencies | 决定租户模式、执行数据 SQL、提交借用事务 |
| metadata candidate producer | 从 T 分页枚举、在 A 限定候选、流式调用 inference | 用 root 权限错误当 bulk 过滤器；规划时展开全租户常量 |
| compile/执行 owner | 绑定 retry、物理 consumer、结果释放和 request 终结 | 把 request 放入 prepared/cached plan 或序列化给远端 |
| account DDL | tenant 元数据语义属性的校验、锁定与提交 | 修改 bootstrap 进度或封口整个集群 |
| bootstrap/restore | 离线安装字段及默认值、恢复属性与定义的归一化 | 将旧数据事务、create_version或session登录字段当模式权威 |

模式可以延迟到第一次接触 View/逻辑元数据时冻结，但**必须早于该语义的缓存准入、模板选择和推导**。已读取 cache 指针不等于已准入；含 View 或 metadata dependency 的 entry 在 mode/身份验证前不得安装到执行状态。

不涉及 View/逻辑元数据的计划可以凭经依赖校验的 feature-free 标记跳过模式查询。不能仅凭 SQL 文本或“上一回是普通表”判断；同名对象重建、table→view、嵌套 View 或 schema 依赖变化须使该证明失效。

模式敏感的 reusable plan 使用明确的 `C=0 / D=1` 标记并绑定依赖证据；未知标记拒绝。分布式执行沿用 origin 已选择的模式，不在每个 receiver 重新按当地 catalog 选一次。这个标记不是权限凭据，不存入 View 定义，不需要 feature MORPC gate。

## 5. 可见域与安全先闭合

### 5.1 T 与 A 的所有权

T 借用用户语句的数据事务、workspace 及其历史 clone；不私自 commit/rollback，不提升 snapshot。own-DDL 可见；RC 刷新按现有语义语句边界执行。snapshot、事务/执行代、对象或角色身份变化使旧 binding 失效。

A 使用已有 authorization epoch 的当前 catalog snapshot。根许可、完整角色继承、tenant 元数据语义属性与 publication 当前资格均在同一个 A 中完成。epoch 之外的合法入口须建立独立只读 RC 授权域，不能退回旧用户事务。需要后台 SQL reader 时，只借用 A 的 snapshot/证明；每个 reader 有词法事务 owner，不并发共享一个未经支持的 TxnOperator。

权限缓存只能使用能证明属于 A 的已有缓存项；不能边推导边刷新会改变冻结 role generation 的共享缓存。不引入新的 schema 或授权成功 cache。

### 5.2 入口策略不是一个万能 SELECT 检查

| 入口 | 准入依据 | 后续约束 |
|---|---|---|
| SELECT、CTAS、PREPARE/EXECUTE | 当前调用者根对象执行许可 | 沿现有 DEFINER/INVOKER 链检查底层对象，`(account, role)` 整体切换；仅允许定义不能跳过执行授权 |
| DESC、SHOW [FULL] COLUMNS | 现有 SHOW database/object-visibility 策略 | SHOW-only 用户不额外要求 SELECT；不得泄露不可见对象 |
| I_S 逻辑 columns/订阅 metadata | 该入口的行可见性、快照许可和 publication 范围 | 在推导之前过滤，否定结果与基础设施错误分开；字段级权限仍保留 |
| SHOW CREATE、ALTER/DROP 修复 | 既有定义访问/DDL 权限 | 不强制先成功推导已经损坏的 View |
| raw catalog | 原 catalog 权限 | 不赋予新的逻辑 schema 或 snapshot 权限 |

系统对象只通过已知系统模板/身份适配；数据库名字、`account=0`、嵌套 resolver 或 request marker 不是授权凭据。未知/被改写的系统模板受控失败，不做猜测式 SQL 替换。内部 catalog SQL 去掉派生 marker，但保留调用者、取消与 snapshot；去 marker 不等于提升权限。

### 5.3 历史、跨租户与订阅

- snapshot 解析只是解析，不能单靠 [doResolveSnapshotWithSnapshotName](../../pkg/frontend/snapshot.go) 的返回值进入其它租户。
- 本租户历史查询仍须有入口对象可见性及现有 snapshot 权限。跨租户只能沿系统管理员或已有 snapshot-publication 的合法授权入口；显式的 `Snapshot.Tenant` 不能自行授予资格。
- admission 必须同时携带：调用者身份、获准访问的 namespace、历史数据实际 owner、snapshot 身份/范围。返回只用于本 request 的已许可根；不能把 subscriber 的 role ID 搬到 publisher 查询。
- 订阅 namespace、定义/物理 source 在 T 解析；account generation、subscription 身份、publication 名称/归属、成员与表集合在 A 检查。当前不可见的候选直接过滤；目录/RPC/解码错误终止查询。
- 历史表集合与 A 的合法集合取交集。合法同 publication remap 不改写 T 的历史物理 source；同名 account/database/publication 重建不是同一授权身份。
- 当前 publication 的一张表与历史同名表之间，是否可访问由 snapshot-publication 的覆盖关系决定，不用“名字相同”代替历史 source 许可；没有覆盖证据则拒绝。

bulk 不得先绑定一个已撤权根再让整条 I_S 报权限错误；也不能把所有权限错误吞掉变成零行。这个区别在 admission 的 typed outcome 层表达，不按 error message 或宽泛错误码猜测。

## 6. 定义、推导与所有消费者

### 6.1 持久定义与旧格式

使用已有 ViewData/目录中的定义、DefaultDatabase、SQLMode、LowerCaseTableNames、列命名、所有权与安全类型。现有持久表达式保护不删除、不扩展为本功能的新门禁。需要的创建环境必须来自持久记录或该格式可证明的默认值，不得拿当前会话环境补未知历史。

CREATE/ALTER 在成功绑定时冻结所有投影 star，含子查询/CTE/特殊展开；`COUNT(*)` 不是投影 star。源 ADD COLUMN 不增加既有 View 输出，删除显式依赖使其失效，类型可在 T 中重算。不能从旧 `Cols` 的名字反推丢失的历史星号映射。

旧格式分为：可完整解释、确定依赖失效、缺失创建语义。最后一类返回明确的 legacy repair-required，不猜测、不静默漏行；owner 可用明确 ALTER VIEW 修复，SHOW CREATE/raw/DROP 仍按原权限可用。激活 preflight 对当前定义做只读、有界资格检查；不可完整解释的定义阻止该 tenant 激活，依赖暂时缺失但定义格式完整则按运行时失效规则处理。

预检查至属性提交之间，N 的所有定义写入者必须保证新写入完整；可能导入 legacy 定义的 restore/import 则须与该 tenant 的激活事务使用同一 account 行锁串行化，并在锁内按目标模式复核。不把“预检查曾经扫过一次”当并发安全证明。

memo 只存推导值，不存许可结果。key 至少区分 root 的 account/database/relation 身份、定义指纹、T 的 snapshot/workspace 修订、创建环境及影响推导的参数/会话输入。每次根访问（包括命中）先走相应 surface admission；同语句的 I_S 许可不能经 memo 变成 SELECT 许可。依赖证据覆盖所有中间 View 与基对象，在原 T 中验证，不能只看根 View 自己的 version。不同 snapshot 或同名重建不共享结果。

### 6.2 消费者矩阵

| 消费者 | D 下的权威与迁移义务 |
|---|---|
| query `bindView`、嵌套 View、LIMIT 0/无数据快路 | 公共 binder；无数据不等于可用持久 Cols 描述 View |
| CTAS 与输出 provenance | 复用既有推导/nullability/default remap；不得按结果列名另查源列 |
| MySQL result metadata | 使用本次已绑定计划的输出；不在发送前另开“最新 metadata”查询 |
| PREPARE/EXECUTE、逻辑 plan cache | reusable artifact 记录模式、定义/依赖身份、binding schema；每次执行重做模式/权限/可见性准入 |
| 参数特化/物理 Compile cache | 不持有 request、scope、borrowed vector/txn。第一版对 D 的 request-bound physical compile 禁止跨执行缓存；不借“下次再 rebind”延后释放 |
| cursor FETCH | 物化结果与列头属于建立 cursor 的执行代；FETCH 不重新推导；下一 EXECUTE 新准入 |
| DESC/SHOW [FULL] COLUMNS | 同一 schema，入口格式化；保留 SHOW 专有字段规则 |
| I_S COLUMNS、订阅 provider | 普通表走原行源；View 经候选许可再流式推导；历史系统模板只读适配，不替换历史用户定义 |
| 其它 I_S/SHOW TABLE STATUS 等 | 明确区分身份/物理属性与需要当前输出 schema 的字段；后一类必须进入共同 adapter，不能遗留第二权威 |
| 内部/公开 table function | 原有公开调用仍做对应入口准入；只有受信任编译边界可创建内部 token，函数名字或 relation ID 不构成许可 |
| raw `mo_catalog.mo_columns`、engine Cols、备份 | **保持物理/MVCC 记录语义**，不是当前 View 类型接口；当前类型消费者必须迁到 I_S/DESC/协议 |
| 尚不支持 View 的 FIELD_LIST/DUMP 等 | 保持现有受控不支持，不为这次迁移新增能力 |

raw 不虚拟化：既有 row ID、seqnum、encoded blob 不能与临时“最新类型”拼成伪物理记录。发布说明须展示源 ALTER 后逻辑类型变化而 raw 列副本未变的例子，明确工具迁移路径。这是显式接受的接口界限，不是把 raw freshness 从验收表悄悄删除。

列名/顺序、精度/scale、字符/特殊类型、nullability 来自共同 inference。SHOW 字符串、I_S 列以及协议 type/flags 做各自规范化；CTAS 的可执行默认值不要求等于 View 展示默认值。沿用 #26226/#26232 的独立语义，不在本稿重新定义。

### 6.3 bulk 关系语义

从 T 的目录分页得到候选，按 A 过滤，再推导所需列，最后执行依赖派生属性的残余谓词。只能下推经证明安全的对象名/库名约束，不能越过 outer join/聚合或先 LIMIT 再过滤。ORDER BY、UNION/APPLY、重复引用及早停仍是正常关系执行。

不把整个租户的 View 预先绑定成 VALUES/巨型 UNION；保持现有 producer 的有界队列、分页与 cancel/drain。metadata derivation 固定在持有 request 的 origin CN，远端只接收常规计划/批次/不可变 schema，不接收事务或 context 指针。

## 7. 语句、retry 与结果生命周期

语义语句不是一个网络包：多语句命令逐条准入；隐式提交后的下一语句重建 A/M/T。PREPARE 与每次 EXECUTE 各有 owner。内部 SQL 借用合法域，不成为独立用户模式切换入口。

```text
NEW → ADMITTED(M,A) → BOUND(T,generation) → EXECUTING → FINALIZING → CLOSED
                          │
                          └─ retry：丢 binding/memo/物理消费者 → BOUND(T',generation')
```

- 内部 retry 属于同一语义语句：保留 M/A、累计 root/work 计数与原 deadline；重新取得合法 T，重新绑定逻辑计划。用户重发才是新语句，可选择新 M/A。
- metadata 值可能已嵌入 SHOW 的计划常量；仅清 memo 不够，选中 D 的 retry 必须走逻辑重建，没有 builder 则受控失败。
- 计划产出是 S3 的绑定终点，不是 S4 的终点。执行/result/pipeline/physical compile 完成后再关闭 request；异常与 panic 路径同样遵守。
- 取消是两层：branch 的 LIMIT/早停只结束该 branch；语句取消使全部 branch 结束。不得将正常早停永久取消兄弟消费者或下一执行代。
- terminal 顺序：发取消 → 停止/排空 producer 与 pipeline → 释放结果 lease/物理消费者 → 关闭请求 binder/授权 reader → 恢复 process 的 top/query/pipeline context → 结束 A。不得让 request.Close 等待只能由它之后的代码释放的结果。

| 资源 | owner / 释放 | 等待与上界 |
|---|---|---|
| 用户事务/历史 clone | 原事务 owner；推导仅借用 | 不提交/关闭父事务 |
| A snapshot、私有授权 reader | 语义语句或 producer 词法 owner；建立后立即登记清理 | acquisition/SQL 受 request/branch cancel；不持 session/gate 锁做 I/O |
| isolated binder、变量快照、memo | request，retry 或终结释放 | 一个 active binder；等候必须可取消 |
| 编码结果与复制后的计划列 | handle consumer / 计划 owner分别负责 | 复制前 reserve，返回 plan 不引用 request |
| stream result、batch、channel | 现有 operator/producer | 满队列发送可取消；早停排空并关闭所有结果 |
| prepared 参数与 cursor | 参数只借用；cursor 由既有结果 owner 保留 | 不将参数 vector/旧 cleanup 带入下一代 |

等待图必须是 `Close → cancel → producer/I/O 退出 → result Release → request Close`。不能是 `Close 持 gate → 等 producer → producer 等同一 gate`，也不能通过为每个 object 添加一个 cleanup goroutine 规避问题。

## 8. 错误合同

| 分类 | 单对象/执行 | bulk metadata |
|---|---|---|
| 无权/不可见 | 入口原有的拒绝/隐匿规则 | typed admission 判定 invisible：不推导、不返回行 |
| 根对象不存在 | 缺失对象，不伪装成成功 | 枚举后身份变化按 T/check 受控重试或失败，不用同名新对象替代 |
| 确认的定义依赖缺失/坏列 | invalid-view 错误 | 仅已许可、确证的依赖失效可跳过该 View；普通健康行继续 |
| 未知 legacy 环境/star、未知系统模板 | 明确 unsupported/repair-required | 同样失败，不将不理解的定义伪装为不存在 |
| catalog/RPC/存储/格式损坏/授权读取失败 | 保留 error/cause | 终止查询；不能用 NoDB/invalid/invisible 消化 |
| 取消、超时、资源不足 | 保留根因，无旧值兜底 | 停止全部消费者并清理；不返回伪完整成功 |

若 MySQL 已流出部分行再失败，协议返回失败终态，不能补一个成功 EOF。错误文本不得透露未获授权的 publisher/source 名称。测试分别断言错误类别、未返回旧成功和资源归零。

## 9. 默认关闭：把目录升级与语义激活解耦

### 9.1 一个 account 属性，不复用 catalog 进度

在 `mo_catalog.mo_account` 增加 `view_metadata_version SMALLINT UNSIGNED NOT NULL DEFAULT 0`：`0=C`，`1=D`。未知值受控拒绝。它是每租户一个持久产品语义属性，不是 freshness、任务进度、资格证书或 worker 健康状态。

为什么不用 `create_version`：最新 main 的 4.0.7 升级在冻结输入、关闭 SQL/task/pipeline admission 的前提下推进 catalog；强行把它改为在线模式切换，需要重定义枚举、登录、fresh install和恢复，并削弱已批准的共享启动合同。新增一个明确属性比增加目标选择配置或另一套 HAKeeper 协议更小。

字段安装走 **4.0.7 之后的正常 catalog 版本**，遵守既有停机升级和默认值回填。不得改写已发布 handler、复活未发布的 4.0.8–4.0.17 链、用 offset-only标记产品语义或增加 MORPC gate。新版本号按届时 registry 分配；本稿不分配数字。

fresh install、存量回填、CREATE ACCOUNT 都默认写0；GetFinalVersion、auto-upgrade、MaybeUpgradeTenant只负责已有目录升级，不能把字段变为1。结构安装完成后才开放SQL；原4.0.7 startup fence、snapshot决策与恢复前置条件完全保留，不为本功能新增 bootstrap target 配置。

statement 在 A 中以调用者所属的执行account ID点读该属性并校验account generation；切入publisher或历史source namespace不重新选择模式。缺行、字段损坏/未知值、目录读取错误都失败，不能默认C。`Session.createVersion`、登录缓存、旧T、registry和本地配置都不是模式权威。内部点读只投影已验证调用者的account行，不授予用户任意读取其它account的能力。

### 9.2 显式、同步、单租户的激活命令

拟扩展现有 account DDL：

```sql
ALTER ACCOUNT account_name SET VIEW_METADATA = 'DERIVED';
```

这是**提议的新语法，当前 main 尚不支持**。沿用 ALTER ACCOUNT 的系统管理权限；不新增默认租户权限或暴露通用内部table-function授权。第一版只接受 `DERIVED`；已经为1时幂等成功，不提供写回0的命令。

frontend account DDL owner执行独立的目录事务：

1. 验证操作者，解析account ID/generation；锁定该account行，重核名称/身份和当前值。
2. 若值为1直接返回；为0则在该事务的当前视域中分页检查该tenant当前普通View的定义可解释性。已知系统模板按受信任身份/定义校验可适配性，不套用普通用户View的legacy规则；未知模板失败。只检查固化环境/定义资格，不要求所有依赖当前存在，不执行View数据。
3. 新 binary的普通CREATE/ALTER始终写完整定义；可能引入legacy的restore/import与激活共享account行锁，在锁内按目标模式复核。普通源表DDL不会破坏“定义格式完整”这一单调条件，不需锁全部用户对象。
4. 预检查成功后同一事务只更新该属性为1并提交；不改变account incarnation、owner/role或session身份，不触发强制断连。失败/取消/预算不足则回滚，值仍为0。没有持久中间态、分页进度、后台重试或额外资格提交。

激活事务不借用或提交用户的数据事务；在不支持的显式用户事务上下文中，进入修改前拒绝。顺序为：当前授权/目录admission → restore既有生命周期锁（仅restore需要）→ account行锁。激活不获取restore生命周期锁；持account锁后不得反向申请它们或再次等待新的共享目录admission，内部SQL使用已建立的读域与该事务。

account锁等待返回后，按既有锁定当前读/事务snapshot刷新机制建立preflight视域，必须包含在它之前完成的restore/import；不能沿用等待前的旧数据snapshot来证明定义完整。若无法取得该可见性证明则回滚。preflight可因同时修复/删除旧定义而保守失败，用户可以重试；不能因并发导入不完整定义而错误成功。account锁不等待用户查询退出，控制取消也不排在该锁之后。

preflight不是对任意未来、历史或其它tenant定义的保证。订阅publisher无需同时激活D；其被访问的定义仍按T/A在实际请求中检查，不可解释时受控失败，不能跨租户自动ALTER修复。

legacy失败不会卡住登录或bootstrap：该tenant仍是正常C，owner能够ALTER修复后重新执行命令。并发激活在同一account行串行，最多一次0→1；其它tenant不受这行锁影响。CREATE ACCOUNT始终独立创建0，无“升级扫描遗漏迟到account”的补偿协议。

### 9.3 提交与可见性

```text
N 的正常停机目录升级已完成：字段存在、所有tenant初始为C
  → C正常服务；管理员可逐tenant试点
  → ALTER ACCOUNT：锁account + preflight + 属性0→1，同一事务提交
  → A可见该提交的语句选择D；已有A的C语句允许结束
```

模式的线性化依据是 **A 的 catalog snapshot**，不是命令到达墙钟时间。A在属性提交前已建立的语句可以继续C；A的可见前缀包含该提交则必须D。不能将提交前的授权admission复用给一个新语义语句。命令返回的提交点须能通过既有committed-read/因果可见性机制在验收客户端建立读后写保证；不能以sleep或CN本地cache传播代替证明。

活跃C语句、旧显式事务中的本次语句、旧物化FETCH允许完成；下一语义语句/EXECUTE重新准入。旧T中的新语句选择D仍推导旧T，不强迫它读取最新数据。内部retry保留原M/A，不借重试刷新模式。tenant可长期分别处于C/D，不以cluster升级计数伪装全部激活。

### 9.4 崩溃、未知提交与回退

- 提交前崩溃：事务回滚，属性0；提交后崩溃：属性1。重启不存在需要恢复的View激活阶段。
- 未知提交：重新读取同一account ID/generation的属性；1为已完成，0可重新验证并重试，无法读取则保持未知，不盲目认定失败/成功。旧ID已删除时不能操作同名新account。
- 同UUID的新进程不继承旧request/cache/lease，从当前A读持久属性。配置变更和关闭auto-upgrade不改变已有值。
- 不支持新数据降级给L，不提供D→C在线回滚。故障使用forward fix；激活前备份可在隔离恢复环境恢复，不能覆盖正在服务的D tenant冒充回滚。
- 普通metadata读取永不写该属性。异常未留下View控制面垃圾，也不需要定期清理任务。

激活审计使用既有account DDL审计，记录操作者、目标account ID/generation与提交结果；不把View列表或SQL正文追加为新的持久协议。

## 10. 备份、snapshot、restore 与旧定义

新 binary 必须读取旧持久格式和旧备份；这与“不支持旧 binary 读取新数据”是两回事。

| 场景 | 行为 |
|---|---|
| L 的完整定义备份 → N/C | 按已有格式读取与恢复；不自动激活 D |
| 完整旧备份 → N/D tenant | restore owner保留定义环境/身份映射，在发布前完成preflight；现有目标属性1不能被源属性0或旧create_version覆盖 |
| 旧备份缺失不可恢复的创建语义 | 数据/原定义须保留；D 目标受控拒绝发布。用隔离的 N/C 恢复环境，由 owner 明确 ALTER 修复后再导入；不能猜历史 star |
| D 数据重启/恢复 | 重启读持久属性；新恢复account保留备份中已提交的1，先验证支持与定义资格；重复归一化幂等 |
| 当前 D 查询早于升级的 snapshot | 模式取当前 A，用户 schema 取历史 T；系统模板按已知版本只读适配。历史定义不完整则明确失败，当前 ALTER 不能“修复”过去 |
| clone/PITR/租户恢复 | restore 身份映射、publication 覆盖与目标策略由原 restore owner 完成；新对象发布前校验，半成品不成为正常候选 |

已知旧格式没有该属性时，恢复新account显式投影0；只有确认的旧schema缺列才允许这个默认，目录/RPC错误不能当缺列。恢复到已有目标使用目标属性，恢复新account使用源已提交属性（已知旧格式为0），均在对象发布前验证。4.0.7已规定的CDC/Iceberg及历史恢复限制继续适用，本文不声称消除它们。

恢复不得直接复制 source 角色数字作为目标执行权限；caller 与 source owner 分开。read-only derivation 不替 restore 创建定义，也不把一次当前 SQL 测试当成真实旧备份证据。

## 11. 性能、界限与可观测性

起点沿用 main 的数值限制作为**局部边界**，不把它们之和叫完整内存证明：

| 资源 | 第一版边界 |
|---|---|
| 每个定义/请求输入、每个保留结果 | 分别不超过 16 MiB |
| request memo | 最多 16 MiB、4096 entry；可关闭，关闭时正确性不变 |
| 依赖深度、单 root work、请求累计 work/root admission | 64、4096、65536；重试不重置 |
| 推导 deadline | min(请求 deadline, 首次推导起 30s)，retry 不续期 |
| 解析 token/work | 32768 token、64 MiB parser work；只覆盖解析，不代表整个 binder |
| C06 候选页、表名 chunk、流队列、输出 batch | 沿用 64、256、8、8192；行数上限必须同时受字节预算约束 |
| 环境快照、builder、解码/序列化、队列结果、并行保留结果 | 复制/增长前计入既有 execution resource generation；受同一查询/CN budget 限制 |
| 激活 preflight | keyset分页64个ID，定义逐个读取/处理，单定义16MiB上限；受激活DDL deadline/查询资源预算约束，不保留全租户AST |

多层递归、序列化扩容、哈希表、临时树及 provenance 复制都须纳入计费或明确有界；仅为最终 bytes reserve 不够。不可计费的下层路径是实施 blocker，不用宣传一个无法证明的统一 128 MiB 上限。

期望成本：cache-off 为所访问依赖闭包的绑定工作；memo 只减少本语句重复计算。逻辑元数据查询额外至多一次account属性点读及A建立，不能每 root 重读模式或扫描全租户权限。feature-free 普通计划不得引入该点读。候选查询按页/namespace复用 A，禁止每列创建事务。无 DDL 依赖 fan-out，无新后台 worker。

上线性能门禁（测试前固定口径，不作为已测结果）：相同数据/T/权限、相同 Go/平台/拓扑，D cache-off 与当前功能等价的按需路径比较；选择性 metadata/query 的 p95 增量不超过 20%、分配增量不超过 25%，bulk 吞吐回退不超过 20%，普通无 View 对照 p95 增量不超过 5%。不足时优化 owner/重复工作或提交有测量依据的调整，不自动启动跨请求缓存项目。陈旧物理目录读只能作成本下界，不能当等价性能基线。

激活是O(tenant定义总量)扫描，持有该account目录行锁直到事务终结；只保证有界内存，不保证任意规模在固定时间内完成。失败不留进度，重试重扫；大型tenant需预留DDL时间/资源。普通查询不等待此锁，冲突的account变更和restore按既有锁超时失败。若此代价不可接受，另评持久扫描进度，不能在实现中悄加worker。

采用有限标签：mode、surface、outcome、stage。记录 bind 次数、memo hit/bytes、budget 拒绝、活跃请求、激活成功/失败；不以 view/SQL/user/tenant 名字作无限 metric 标签。失败诊断通过既有受权日志/语句 tracing 关联，不建立第二份持久 View 状态。

## 12. 验收矩阵：每项一个可证伪合同

| 编号 | 最小反例与正负控制 | 证据层与独立 oracle |
|---|---|---|
| V01 类型/DDL | 一张表、嵌套 View；MODIFY/ADD/DROP、metadata-only/COPY、同名重建；own-DDL与未提交外部DDL | inference UT + 公共 query/CTAS/SHOW/I_S/协议类型；期望不由被测 helper 生成 |
| V02 T/A 分离 | 保留旧 T，另一连接撤权；同 A 保持冻结、下一 A拒绝 | 角色/publication UT + 双连接 SQL；验证 schema仍历史、不可见根 binder 次数为零、健康行存在 |
| V03 全权限链 | SHOW-only、无权限、继承角色、DEFINER/INVOKER、同数字不同 account role | 正/负 query与SHOW；当前权限读取失败必须保留错误，不被当 invisible |
| V04 历史/系统/订阅 | 本租户、sys授权跨租户、普通用户伪造跨租户；withdraw/remap/recreate；早期系统模板与未知模板 | 最小真实 snapshot/publication fixture；source身份、显示namespace和覆盖关系分别断言 |
| V05 reuse/retry/cursor | 预热所有 cache层再DDL/撤权/模式提交；参数/零参数；旧FETCH新EXECUTE；多语句中间失败 | 实际 MySQL prepare/execute/fetch 与 typed owner oracle；逻辑重建、无旧compile、无closed context残留 |
| V06 bulk关系/错误 | UNION/APPLY/outer join、过滤/ORDER/LIMIT、早停；坏依赖与catalog故障 | 公共关系结果 + typed producer测试；不提前LIMIT、不吞fatal、不污染兄弟branch |
| V07 生命周期/预算 | 各 acquisition 前后注错，cancel/close/retry/释放竞态，边界±1 | 确定性 channel/failpoint、精确 release计数；聚合预算归零、borrowed父资源仍活；按精确test自适应race |
| V08 默认与激活 | fresh/存量回填/new account/login/restore均默认0或保留合法源属性；管理员/非管理员；legacy失败后仍能登录C修复；并发与重复激活 | account DDL/迁移UT + 无integration tag普通binary SQL；不能由catalog版本、auto-upgrade或登录偷设1 |
| V09 新切换合同 | N/C长语句停在已准入点，tenant提交D；旧语句继续C、后续A选D；旧T中新语句D；不等待长事务 | 两CN同N真实SQL + barrier；记录mode/snapshot/version，不用all-peers mock冒充 |
| V10 崩溃/未知提交 | 激活更新/提交前后、提交应答丢失、重启、重复命令、相同UUID换进程；激活与restore/import/drop-recreate交错 | fault UT + 必要真实持久重启；以持久状态/SQL结果判断，不以日志顺序判断 |
| V11 旧格式/备份 | 真实L产生完整/legacy备份，N/C恢复；完整迁到D，缺失语义受控修复；historical不被当前修复改写 | 有来源哈希的L/N二进制与备份；不能用测试拼JSON代替全部兼容证据 |
| V12 raw与性能 | logical新类型/raw旧副本、普通表对照；memo开关；选择性/bulk/共享DAG | 明确公共兼容断言；单独基准，按第11节口径记录原始数据 |

优先扩展现有 [view cases](../../test/distributed/cases/view)、[object visibility case](../../test/distributed/cases/zz_accesscontrol/information_schema_object_visibility.sql)、[snapshot cases](../../test/distributed/cases/snapshot) 与 owning UT fixtures。普通验证尽量0/1/2行，拓扑只用于跨CN/持久合同；不靠 sleep、重试断言或增大数据撞竞态。

公共 BVT 由 mo-tester 运行/生成结果，不手改期望。覆盖率按实际 CI changed-production-code 算法严格大于75%，full-package单列。当前 integration registry 测试是局部证明，不替代无测试 tag 的生产选路与实际升级。每个失败记录版本、模式、选择与终态；本设计阶段没有任何 V01–V12 的 PASS 记录。

## 13. 实施切片与交付门禁

本稿本身只提交文档。批准后按**端到端合同**拆分，而不是先堆接口、最后再拼激活：

1. **模式与兼容写基础**：在正常停机catalog迁移中安装默认0的account属性；N/C不自动改变读语义，全部定义写入可共存；属性点读与feature-free/cache准入测试先闭合。消费者接完之前不发布可执行激活命令。
2. **一个请求与两种域**：frontend owner、A/T、surface admission、公共 binder/错误分类、lexical资源；query/CTAS及SHOW/I_S最小真实用户路径一起验证。
3. **完整消费者与安全**：补齐订阅/历史/系统、所有reuse/retry/cursor与早停路径；逐个分类剩余 Cols/mo_columns 用户，不以一次搜索代替审计。
4. **升级/恢复贯通**：字段安装与默认值、新切换合同、unknown commit/old backup、真实无tag N/C→N/D；只有前面合同和性能门禁通过才发布激活命令。
5. **独立清理**：通过后再评估删除 refresh-only runtime，这不等于允许删除 C reader。停机只能结束进程内已入场的 C 语句、cursor 等消费者，不能消除持久的 C 语义需求。删除 C reader 须另案评审，并同时满足以下兼容条件：
   - **存量租户**：持久 `view_metadata_version=0` 的租户须已按批准路径完成迁移；仍有0租户需要服务时，目标版本必须保留 C 兼容读取，不能直接把0解释为D或忽略这些租户。
   - **新建入口**：fresh install、CREATE ACCOUNT 的默认0及相关写入入口已有经批准的迁移安排，不再产生无法服务的 C 租户。本稿默认0不因清理计划而自动改变。
   - **恢复入口**：旧备份/快照恢复投影0、隔离 N/C 恢复与 legacy 修复路径仍有受支持的 reader 或经验证的前置迁移流程；不能以删除 reader 为由静默丢弃已有恢复兼容承诺。

   只有这些持久兼容条件成立后，受支持的停机部署才可作为移除不再需要的 reader 代码的边界。共享 persisted-expression/admission/snapshot 保护与 legacy raw catalog不顺带删除。

没有“先称S5完成，后补生产路由”的交付状态。设计批准、实现完成、局部验收、可激活、默认启用、遗留清理是不同结论；每个PR只声明实际达到的一个范围。缓存与默认启用另外评审，不能由局部 benchmark或设计PR自动触发。

## 14. 标准、取舍与评审决定

- MySQL 8.0 的 [CREATE VIEW](https://dev.mysql.com/doc/refman/8.0/en/create-view.html)、[SHOW COLUMNS](https://dev.mysql.com/doc/refman/8.0/en/show-columns.html)、[I_S COLUMNS](https://dev.mysql.com/doc/refman/8.0/en/information-schema-columns-table.html) 用于 frozen definition、展示与安全类型的接口参考；不声称内部实现与MO相同，也不扩大本系列兼容范围。
- [MySQL protocol resultset](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html) 是客户端边界参考；实现仍须用真实driver验证prepare/result/cursor行为。
- 项目 [upgrade compatibility](../../.agents/skills/mo-dev/references/upgrade-compatibility.md)、[4.0.7停机升级指南](../guides/upgrade-4.2-to-main.md) 是二进制部署/持久兼容依据；它们不代替本文的在线产品语义选择，也不允许本功能削弱已存在的startup fence。
- 请求级 memo、不可变结果、MVCC snapshot 与按语句模式冻结是生命周期选择，不是为了metadata另建共享freshness状态。

必须在新设计评审中整体决定：

| 决策 | 本稿提案 | 明确代价 |
|---|---|---|
| D1 | 按需权威 + 请求memo，raw保持物理接口 | bulk读成本、raw工具迁移 |
| D2 | T/A分离，surface typed admission先于推导 | 当前权限读成本；更多明确错误而非旧值成功 |
| D3 | **允许已经准入的C语句在tenant提交后完成；不设新的资格/drain提交** | 不提供“提交即全局无旧reader”保证；删除C代码须先满足存量0租户、新建默认值与旧备份恢复的兼容条件，停机本身不充分 |
| D4 | 一个默认0的account属性；显式管理员DDL原子激活；create_version继续只管catalog升级 | 新字段及恢复投影、新SQL入口；激活全量检查持account行锁，不提供后台进度 |
| D5 | 缺失历史语义不猜；先修复再激活，旧备份保留兼容修复路径 | legacy租户可能不能直接激活，历史快照可能受控失败 |
| D6 | 默认关闭，不承诺跨请求cache，不自动删除已有共享gate | 多阶段交付，保留独立保护的短期维护成本 |

这些是供批准的选择，不是留给实现者猜的开放问题。实际catalog版本号与具体测试文件位置由对应实施owner在首个实现PR分配；不得改变上述语义。若D3或raw/legacy取舍不被接受，应修改本设计后再批准，不能退回对旧PR逐个补丁的流程。
