# 普通 View 按需元数据：完整系列契约与消费者迁移设计

- 修订：**v2.0-draft.1，2026-09-28，待审批**。S2 内存上限与实现方式已由用户于 2026-10-05 调整；以 [v2.1 S2 修订](CLAUDE_20261005-view-s2-scope-revision.md) 为准，原 §11 中逐对象先准入、128 MiB 硬证明及对应性能目标不再是 S2 验收条件。其余语义与安全边界保持。
- 设计所有者：[S1 #29436](https://github.com/matrixorigin/matrixone/issues/29436)；完整系列：[#29433](https://github.com/matrixorigin/matrixone/issues/29433)，S2–S9 见第 15 节。
- 核查基线：`d99187d7b3bfda8744088b3ae8cf16739b690aa0`，包含已合入的 [#29139](https://github.com/matrixorigin/matrixone/pull/29139)。本次仅修改设计，不修改生产行为。
- 精确修订摘要、自审和审批记录：[S1 审查记录](CLAUDE_20260928-view-metadata-s1-review.md)。**同意执行设计整理不等于批准本修订。未经对精确修订的明确批准，后续生产修改仍阻塞。**
- v1 原文和审批历史保留在 [#29139 合入修订](https://github.com/matrixorigin/matrixone/blob/8db6d7ff4f0f38701f6e3154a578e4ce01f4f7ac/docs/design/CLAUDE_20260922-view-metadata-on-demand.md)。本修订整合而非追认旧版未覆盖的契约；不撤销已合入 PR，不把其审批外推到整个系列。

## 1. 问题、证据与范围

[#26227](https://github.com/matrixorigin/matrixone/issues/26227) 的历史复现：源列经 ALTER 后，SELECT/CTAS 已使用新类型，而 DESC/I_S 仍显示旧宽度、scale、default 等。用户需要的是**相同可见 schema 下的输出描述一致**，不是某个恢复任务最终完成。

| 证据 | 本次可确认的结论 | 不能据此声称 |
| --- | --- | --- |
| #26227 的旧提交复现及表达式 default 补充 | 原问题及验收维度 | 当前 head 仍以相同方式复现 |
| #29139 和当前源码 | 已有只读生成器、DESC/SHOW、动态 I_S/订阅路径、部分 CTAS/缓存修复、协议 100 与 4.0.10 迁移 | S2–S9 全部完成；raw catalog 已新鲜；新预算已满足 |
| v1 中 MySQL 8.0.45 对照与历史审批 | 单对象失败、批量跳过确证缺失依赖并 warning 的既有决策 | 本次重新执行了 MySQL 或 MO 测试 |
| #29139 历史 review 的约 6–7ms → 11–34ms 暖读观测 | 按需绑定可能有显著读成本 | 当前 head 的容量、p95、普遍性能提升 |
| 本次源码阅读 | 消费者、字段、版本及共享保护的设计依据 | 完整运行时审计、无 race/leak 或新测试 PASS |

仅覆盖普通 View 输出元数据。物化视图刷新、分区、charset/collation 规则修改、存储过程、UDF 功能及独立 warning 兼容工作不纳入；已有类型/创建环境/错误契约必须保留。普通表继续以持久列为权威。系统视图的固定 schema 由版本化系统定义负责，不按用户 View 全库递归推导。

## 2. 不变量与反例

令 `V` 为可见 View 身份及定义版本，`S` 为解析域的 schema snapshot，`W` 为该语句可见的事务本地目录 overlay，`C` 为创建环境和经授权的绑定上下文：

```text
D(V,S,W,C) = 权威 binder/provenance 对持久语义定义的完整输出描述
当前 schema 消费者的结果 = 该入口的展示规则(D)
原始物理目录结果 = 在 S/W 下可见的持久记录（第 5 节的明确兼容契约）
```

1. 同一 `V/S/W/C` 的推导结果一致。字段不按名字重新猜来源，不用旧 View 列覆盖当前类型。
2. 描述读取可以查询目录，但不执行 View 的数据查询，不写回 ViewSql/Cols/依赖，不排队 refresh，不以 worker/lease/CURRENT 标记为前提。
3. 先做该入口的对象可见性检查，再推导；内部提升权限不能绕过调用者可见集合。失败不得回退陈旧描述。
4. 缓存/请求 memo 被关闭、逐出或随 CN 重启丢失，只影响成本，不改变成功时的字段、错误分类或权限。
5. 保持定义/身份/授权的持久所有权；生成的 ColDef 是请求拥有的输出，不是引擎共享 TableDef 的原地修改。
6. 所有请求状态有容量、取消和终结边界；元数据读取不新建 durable freshness 状态机。

最小反例：`v2 -> v1 -> t`，只 ALTER `t`，View 自身版本不变；重用只验证 v2 的描述会返回旧类型。最近对照是同一历史快照中的读，后者**应当**继续看到旧类型。另一个反例是先绑定全部 View 再执行表名/权限过滤：无关或不可见坏 View 会消耗预算或泄露错误。

## 3. 当前源码地图与剩余闭包

以下是基线上的定位，不是新运行证据。链接均为仓库内源码，评审应在上述基线查看。

| ID / 所有者与消费者 | 当前入口 | 系列要求及迁移归属 |
| --- | --- | --- |
| C01 创建/修改定义 → 推导器 | [build_ddl.go](../../pkg/sql/plan/build_ddl.go) `genViewTableDef`、`stableViewSQLWithExpandedStars`；[view_regeneration.go](../../pkg/sql/plan/view_regeneration.go) | 共用完整输出语义；读路径不发布重生成 SQL/Dependencies；S2 |
| C02 查询、嵌套 View → 结果包 | [query_builder.go](../../pkg/sql/plan/query_builder.go) `bindView`；[build.go](../../pkg/sql/plan/build.go) `GetResultColumnsFromPlan`；[computation_wrapper.go](../../pkg/frontend/computation_wrapper.go) `GetColumns` | 查询仍构造执行计划；其输出契约与 D 一致，不用描述缓存替代执行子树；S3 |
| C03 View → CTAS/default | [output_column_provenance.go](../../pkg/sql/plan/output_column_provenance.go) `markViewCTASDefaultBoundary`；`build_ddl.go` CTAS/default remap | 当前已从绑定边界取 nullability；保留 View 与 CTAS default 的不同策略，不重新按源列名查目录；S3 |
| C04 DESC/SHOW [FULL] COLUMNS | [build_show.go](../../pkg/sql/plan/build_show.go) `buildShowColumns`；[view_description.go](../../pkg/sql/plan/view_description.go) | 已使用生成器及 PrepareSchemas；补齐本设计的 legacy/预算/统一证据；S4 |
| C05 列数及按列构造的 SHOW | `build_show.go` `buildShowColumnNumber`、`buildShowTableValues` | 前者仍 count 原始列；改用 D 的可见列数。后者仍遍历 Cols 构造 min/max，应从 D 选列；它本来就是显式数据查询，不属于“元数据读取不执行数据”的禁止对象；S3/S4 |
| C06 information_schema.COLUMNS | [predefined.go](../../pkg/util/sysview/predefined.go) `informationSchemaCurrentColumnsDDL`；[view_columns.go](../../pkg/sql/colexec/table_function/view_columns.go) | 已有授权候选 + origin-CN APPLY；普通表/系统视图走旧路径，用户 View 不 join 旧列骨架；S4 |
| C07 订阅及历史目录 | [compiler_context.go](../../pkg/frontend/compiler_context.go) `GetSubscriptionMetadata`；[subscription_metadata.go](../../pkg/sql/colexec/table_function/subscription_metadata.go)；C06 | subscriber 可见性先行，publisher 绑定，subscriber 名称展示；保留旧 V58 生产者直到迁移资格满足；S4/S5 |
| C08 普通计划缓存 | [mysql_cmd_executor.go](../../pkg/frontend/mysql_cmd_executor.go) `checkModify`、`cachedPlanForInput`；[plan_cache.go](../../pkg/frontend/plan_cache.go) | 复用现有身份/版本校验，包含中间 View 与源对象，不另建失效系统；S3 |
| C09 文本/二进制 PREPARE、EXECUTE/reprepare | `computation_wrapper.go` `validateCapturedPrepareSchemas`；[subscription_metadata.go](../../pkg/sql/plan/subscription_metadata.go) | 动态候选/授权每次执行重算；冻结的 SHOW VALUES 必须依赖闭包验证或重绑定；S3 |
| C10 server cursor | `mysql_cmd_executor.go` `capturePreparedCursorBatch`、`executeStmtFetch` | 已物化 cursor 的列与行归属 EXECUTE 代；FETCH 不用最新 schema 改写已生成结果；S3/S5 |
| C11 COM_FIELD_LIST | `mysql_cmd_executor.go` `doCmdFieldList` | 当前仅检查默认库，列枚举/发送已注释。保持现状，不声称已支持 View 列；今后实现时必须走 D，不顺手扩功能；S4 的回归控制项 |
| C12 SHOW CREATE、定义 dump/introspection | `build_show.go` `buildShowCreateView`、`buildShowCreateTable` | 返回持久语义定义；坏 View 仍可导出定义/修复，不以 D 成功为前提；S4 |
| C13 DUMP TABLE / 查询结果导出 | [table_dump.go](../../pkg/frontend/table_dump.go) `validateTableDumpSchema`；[query_result.go](../../pkg/frontend/query_result.go) `doDumpQueryResult` | DUMP TABLE 明确拒绝 View，保持拒绝；已有查询结果导出使用该次结果描述，不重新描述当前 View；外部 dump 客户端按实际使用的 SHOW/I_S/结果协议契约验证，不假设仓库有独立 mo-dump 实现；S4/S5 |
| C14 `SELECT * FROM v LIMIT 0` 规划旁路 | `query_builder.go` `bindSelectClause` 的 LIMIT 0 设置及 `isSkipResolveTableDef` 分支；`compiler_context.go` `BuildTableDefByMoColumns`；[frontend/util.go](../../pkg/frontend/util.go) `buildTableDefFromMoColumns` | 当前可直接从持久列构造无 ViewSql 的 TableDef；View 必须绕过该优化，走正常 View 绑定和 D 契约，不能只因不取数据就返回旧结果类型；普通表保留快路；S3 |
| C15 引擎、restore/clone/PITR、升级 | `TableDef.Cols`、`MoColumnsAllQueryFormat`；[snapshot.go](../../pkg/frontend/snapshot.go)、[pitr.go](../../pkg/frontend/pitr.go)、bootstrap versions | 物理记录/定义传输保留；恢复不把派生列当作当前语义。目标侧重新以其合法 schema/权限绑定；S5/S9 |
| C16 原始 `mo_columns` | [catalog/types.go](../../pkg/catalog/types.go) `MoColumnsSchema` | 物理兼容接口，详见第 5 节；要求当前列的内部调用者必须迁出；S3/S4 |
| C17 其他 I_S/SHOW | `predefined.go` TABLES/VIEWS/STATISTICS/约束；`build_show.go` SHOW INDEX/SEQUENCES/TABLE STATUS | 身份/定义字段来自权威目录；序列、索引和普通表元数据不因本系列变成 View 输出。View 不虚构源表索引/约束/auto_increment；含 View 输出列的新增消费者按 C04/C06 接入 |

S3/S4 关闭时须给出最终源码搜索的每个 Cols/mo_columns 消费者归类；上表是功能边界清单，不以一次 rg 代替最终所有调用点审计。不得因为 C11/C13 当前不支持而把未来支持算作本系列已完成。

## 4. 字段权威与展示规则

### 4.1 定义层

`A` = 持久权威事实；`D` = 按需输出；`K` = 仅物理兼容数据。下面的 D/K 分类**只针对普通 View**，不是普通表字段的重新定义。

| 字段组 | 分类 | 规则 |
| --- | --- | --- |
| account/database/relation/logical ID、名称、对象 kind、版本、owner/权限 | A | 按请求可见目录解析；复制对象必须重新鉴权，名称相同不代表身份相同 |
| ViewData.Stmt、DefaultDatabase、sql_mode、lower_case_table_names、security_type、required_protocol_version | A | 已保存值优先；unknown JSON 字段原样保存。缺失值按第 7 节，不使用当前会话猜测 |
| 显式 View 列名单、已经展开的投影和列顺序 | A | 位于稳定 SQL 中；ALTER 源表不能重新展开历史 star |
| ViewData.Dependencies、mo_view_dependencies | K（对 freshness） | 旧维护机制/兼容与其他已确认使用者的数据；不是当前推导/缓存有效性的证明；未经 S9 审计不能删除 |
| 本次解析的完整依赖证据 | D | 包含根和中间 View、源对象及各自 snapshot/identity/version；生命周期不超过请求或已验证计划 |

### 4.2 `ColDef` 与 `Type`（按 proto 当前全部字段分组）

| 字段 | 分类与公共语义 |
| --- | --- |
| `name/origin_name`、有序列位次 | D，受显式名单/冻结投影 A 约束；SELECT 自身 alias 与 View 定义名按入口区分 |
| `typ.id/width/scale/notNullable/enumvalues/charset/pad_space` | D，由同一 binder 输出；保留已有特殊类型、字符串域/填充语义，不新增 charset 规则 |
| `default.expr/origin_string/null_ability` | D；单源 provenance 决定继承资格，外连接 null-extension 在输出边界处理；表达式 default 不是数据执行 |
| `not_null`、`typ.auto_incr/typ.table`、`tbl_name/db_name/origin_tbl_name`、`primary/unique` | 查询结果协议的 D 展示/来源字段；不得反向把这些 flags 当成 View 自身拥有物理键或 auto_increment |
| `comment/on_update/generated_col` | View 目录展示按权威 View 生成器的规则（目前不继承源表这些属性）；旧存储副本 K，不能凭来源推导新增写入属性 |
| `hidden/clusterBy/pkidx/low_card/headers/header` | View 输出不暴露引擎隐藏/物理属性；兼容默认值 K，不重新从源表继承 |
| `col_id/seqnum/alg` | K；引擎兼容标识/布局，不是普通 View 输出语义；生成器中的默认压缩值不意味着 View 有存储 |

View schema 的 nullability 与 SELECT 外层查询的 null-extension 分开：`SELECT * FROM v` 的列描述以 D 为输入，外层 join/表达式可以再改变它。CTAS 用来源策略创建新的表定义，不复制 View 整个 ColDef。

### 4.3 `mo_columns` 当前 27 个字段及隐藏行身份

| 字段 | 普通 View 的分类 |
| --- | --- |
| `account_id, att_database_id, att_database, att_relname_id, att_relname` | 对象身份 A 的物理投影；展示订阅时使用 subscriber 名称映射 |
| `attname, attnum` | 逻辑输出 D，定义名/顺序受 A 约束；raw 行中保存的是 K 副本 |
| `atttyp, att_length, attnotnull, atthasdef, att_default, att_is_unsigned, attr_enum` | 当前值 D；raw 行中的旧快照 K，不可混入 D |
| `att_constraint_type, att_is_auto_increment, att_comment, att_is_hidden, attr_has_update, attr_update, attr_is_clusterby, attr_has_generated, attr_generated` | View 展示的默认/空属性由统一 schema 与入口格式器决定；raw 副本 K，不继承源表物理属性 |
| `att_uniq_name, attisdropped, attr_seqnum, __mo_cpkey_col` 及隐藏 row ID | K，保留引擎/复制协议原义；不伪造当前 schema 对应的物理行身份 |

DESC 的 `Type/Null/Default`、I_S 的数值宽度/scale、MySQL 结果包是不同编码；一致性比较归一化后的类型和 nullability，不要求字面字符串完全相等。View 默认 `7` 与 CTAS 的可执行类型默认 `0` 是已批准的不同契约；表达式 default 的重映射按 #26232，不能强行统一。SHOW 的 Key/Extra 与 SELECT 包来源 flags 也不作逐字等同。

## 5. raw catalog 决策：明确保留物理兼容接口

**D03 推荐并请求显式批准：不虚拟化用户直接 SELECT 的 `mo_catalog.mo_columns`，也不恢复持久刷新。** 原始 SQL（包括投影、JOIN、聚合、快照查询及用户在其上建立的 View）保持物理目录/MVCC 语义。View 行是在 CREATE/ALTER VIEW 或历史维护时写入的列快照，源 DDL 后可能陈旧，失效 View 的原始行也不会因读而消失。普通表行语义不变。

这不是“所有 SQL 都读到新列”的承诺，而是明确区分两个接口：

- **当前 schema 接口**：SELECT 的结果描述、CTAS、DESC/SHOW 输出列、I_S.COLUMNS、订阅及复用计划，统一消费 D。
- **物理兼容接口**：raw mo_columns/引擎 Cols/备份载荷，原样访问存储，不代表当前 View 输出。只要求这些记录自身的 snapshot 一致性，不承诺依赖 DDL 后更新。

直接访问不会按用户名、SQL 文本、列投影或内部/外部调用者偷偷切换语义；不创建管理员白名单，不给 SELECT raw catalog 注入写操作/诊断副作用。raw 权限继续使用原 catalog 规则，不授予额外可见性。

迁移义务：要求当前 schema 的内部/产品化外部工具改用现有 I_S/DESC/协议元数据；需要 attr_seqnum/row ID/物理 blob 的工具继续 raw。S4 必须在发布说明写明兼容界限及迁移示例：

```sql
-- 当前 View 输出列：使用逻辑接口，不将原始 atttyp 作为 freshness 证明。
SELECT column_name, column_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = 'app' AND table_name = 'v'
ORDER BY ordinal_position;
```

为何不选虚拟化：mo_columns 同时承担物理身份、编码列、目录复制/恢复接口；替换其中部分字段会产生不一致的“新类型 + 旧物理身份”，全部替换则需要新的 engine/catalog 适配与升级协议。仅为保持一个未经明确区分的 raw freshness 预期引入该复杂度，不符合此系列的只读、最小所有权边界。

**审批条件**：#26227 当前文字要求持久化刷新，本条是对那部分实现/兼容承诺的明确变更，而不是声称它从未存在。必须由用户/该验收契约 owner 对 D03 及第 16 节协调项明确同意；否则 S1 不完成，S2–S9 不因本文获准，不能把 raw 新鲜性从验收静默删掉。若 owner 要求 raw 也实时，须另修订适配边界并重新审批，不能在实施中自行改选。

## 6. 统一推导边界、memo 与所有权

### 6.1 接口与数据流

以下为语义接口，不要求新增同名框架或绕开现有 binder：

```text
请求/执行代拥有 Visibility(S,W) + CallerAuthorization + 资源预算
  -> 原始目录中的可见对象候选
  -> Definition(V) + 已保存创建环境 + 合法订阅映射
  -> Describe(V, BindingContext, RequestBudget)
  -> immutable {有序输出描述, provenance/default 策略, 完整依赖证据}
  -> planner/CTAS/结果协议 或 DESC/I_S 展示
```

S2 从 `DescribeViewColumns -> RegenerateViewDefinition -> genViewTableDef` 提取/复用读侧内核；允许初期保留必要的绑定/优化验证，但须测量代价。不得从读取调用 `refreshOneView` 或发布 `RegeneratedViewDefinition.TableDef.ViewSql`。目录查询是允许的元数据 I/O；View 数据扫描、执行用户函数来“探测类型”、持久修复和统计收集任务均禁止由描述读触发。

返回值的类型/default/provenance 在发布给调用者前完整确定。需要改写的消费者复制自己的壳和可变字段；不得修改引擎 ColDef、memo 默认表达式或别的绑定位置。涉及 RelPos/ColPos 的 provenance 只能在合法映射后的边界使用，不能把另一个 QueryBuilder 的槽位直接嵌入。

C02 查询绑定依然产生执行计划；共享的是输出语义和合法的描述结果，不把只读描述当成查询执行计划。不能为了“只绑定一次”跨查询节点共享可变 plan/AST。最终完整依赖由本次 resolver 跟踪所有层级；当前 `viewDependencyCaptureContext` 的 depth=0 持久 direct-dependency 集合不够。

### 6.2 请求内复用

S2 增加有界 statement-local memo；不开跨请求缓存，不存失败结果。键至少包含执行代、snapshot 解析域/时间及租户、事务 workspace 可见代、根定义身份/版本、创建环境、有效授权角色/安全链、订阅映射及本地协议能力。无法证明键等价就旁路；授权仍在每个入口进行，不把命中当作授权。

按需复用完整不可变输出及证据；嵌套描述可在其 schema 边界命中，查询执行树仍独立构造。若既有 binder 必须重复做优化/执行计划构造，S6 如实计费，不宣称总工作一定为 O(唯一对象数)。缓存满时逐出/不缓存，不能返回旧值；推导本身的硬预算耗尽则失败。

结构性 work/depth/列槽预算按无 memo 的逻辑展开计费，memo 项附带相应子树用量，命中也扣同样额度；不能靠缓存命中绕过硬上限，或因逐出改变结构性错误。内存优先回收可选 memo，再判断必要工作是否超限，不能让保留缓存挤掉本可成功的绑定。关闭/逐出不改变语义结果；实际 CPU、分配和触达外部 deadline 的机会可能改变，这属于成本/资源终止而非返回不同 schema 的许可。所有性能模式同时验证无资源故障时与 cache-off 的结果、错误类一致。

循环检测使用**当前递归栈**的 account/database/object 身份和 snapshot 域，不能用“曾访问”集合把 DAG 共享误判成循环；进入前检查，离开时释放。重复名字对应不同对象/时间域不混同，深度上限仍阻止跨时间域无穷展开。

### 6.3 生命周期与等待关系

| 资源/状态 | 唯一所有者和成功路径 | 失败、取消、重试、复用路径 |
| --- | --- | --- |
| 借用的父事务/会话 | 原请求所有；描述只借用 | 描述 Close 不提交、回滚或关闭父事务 |
| 隔离 compiler/process、历史 executor | provider 创建成功后立即登记 cleanup；串行使用 | 初始化到哪层就释放哪层；clone 的关闭遵循其真实 API，不把借用者当 owner |
| AST、推导临时对象、memo、依赖集合 | 执行代拥有；完整发布后只读 | error 不发布半成品；终结清空引用；新执行代从空状态开始 |
| 候选 cursor、输出 batch | operator 按现有 pipeline 所有权转移 | EOF/提前 LIMIT/错误/取消都释放；Reset/Free 不能重复释放，也不能等已退出的消费者 |
| cursor 结果 | frontend prepared cursor，已物化的列+行 | FETCH/再次 EXECUTE/RESET/CLOSE/session close 按既有生命周期释放；不持有描述 provider |

```text
入口 -> 短暂复制上下文 -> 串行 binder -> 同一事务的目录 RPC -> 正常返回/取消
取消 -> 请求 context -> 当前目录 RPC/绑定检查点 -> cleanup
```

无 refresh 锁、后台线程或全局 memo 锁参与读路径。不得持 session/context mutex 跨目录 RPC；并行 UNION/APPLY 分支不能并发使用同一个可变 child。若需串行 admission，只在请求内排队且等待可取消，计入同一预算，不阻断 cancel。成功输出后的普通 pipeline 终结协议不变，不从 Call 发终结信号；仅在真实修改该协议时另作 sender/receiver 审查。

Q1：每个获得的资源有同作用域 cleanup；Q2：不依赖 refresh worker，目录等待继承截止时间；Q3：候选、递归、memo、临时分配、输出均受第 11 节限制。这些是实施必须证明的要求，不是当前代码已无泄漏的声明。

## 7. 定义环境、star 和 legacy 决策

### 7.1 新定义及可完整恢复的定义

保存并使用创建时的 DefaultDatabase、SQL parser mode、lower_case_table_names、安全类型及协议标记；用户/definer 身份由可见对象 owner/权限目录解析，不能把当前 caller 变成历史 definer。保持已有 charset/函数解析语义，不引入第二套函数绑定器。

其他具有既有会话语义的绑定变量（例如影响结果描述的 session 设置）不是可凭空恢复的创建历史：沿用其既有 owner 的请求语义，在语句开始捕获不可变绑定环境并纳入 C/memo 键。首版保守按完整绑定变量快照隔离复用，而非遗漏未知依赖；保存的 View 字段覆盖对应 lexical/name/database 设置。某字段若要求创建时固定但没有保存值，按 legacy 信息缺失处理，不偷换成当前变量。SELECT、CTAS 和描述器采用同一环境选择，不能只让 DESC 使用保存值。

CREATE/ALTER VIEW 在同一次成功绑定中固化各查询块可展开的投影 star，包含 alias/显式列名单与顺序；只有列集合可证明稳定才发布新定义。`COUNT(*)` 不是投影 star；`SAMPLE(*)` 等特殊展开必须由现有语义生成显式稳定形式，不能把无法稳定的情况伪装为已冻结。源 ADD COLUMN 不增加 View 输出，删除被显式引用的列使其失效；类型/default 可以随同一可见定义重算。

创建/修改是定义变更的事务提交点，读路径永不补写历史字段。相关正常 DDL 自身仍允许写兼容列，不等于读时刷新。

### 7.2 legacy 分类（D04）

| 已有定义的可证明信息 | v2 行为 |
| --- | --- |
| 完整环境 + 无未稳定投影 star | 正常 D；不要求重写历史 JSON |
| 缺 SQLMode，但属于现有 `legacyViewParserSQLMode = PIPES_AS_CONCAT` 的兼容编码 | 使用这个**既有版本约定**，不是当前会话 mode；有相反/未知 writer 证据则拒绝，不扩展猜测规则 |
| 缺 lower_case_table_names 或必要 DefaultDatabase，无法由已记录且经测试的历史格式规则唯一确定 | `LEGACY_CONTEXT_UNAVAILABLE`，受控不支持；不试探当前会话、同名库或多种 parser mode 直到某个成功 |
| 持久 SQL 仍含未稳定投影 star，只有旧 Cols 而无历史绑定映射 | `LEGACY_STAR_UNAVAILABLE`；旧列名/数量不能证明 join、alias、同名列来源，不据此重建投影 |
| 缺 security_type | 只沿用现有 legacy DEFINER 规则及目录 owner；owner 不存在或权限不合法是错误，不能提升为 sys |
| 未知/损坏 JSON 或不支持的 required_protocol_version | 原错误/协议拒绝，不以 legacy 借口降格为 warning 或回退列 |

这些 legacy 拒绝可能比 v1 严格，是必须显式批准的兼容代价。规则同时适用于 SELECT/CTAS 的 View 绑定和当前元数据入口，不能让数据查询重新展开 star 而 DESC 拒绝另一种定义。激活前执行**只读** inventory，列出需 owner 处理的对象；不能以“扫描到过”作为永久 readiness/freshness 标志。所有写入入口在切换后执行相同规则，避免升级后再次产生未知定义。

修复方式是 owner 提交明确的 ALTER VIEW 定义（若无法找回原语义，则明确选择从现在起采用新的列集合），或从有证据的旧备份取回原定义；不自动替用户作此选择。SHOW CREATE、DROP、ALTER 修复入口及 raw catalog 仍可用。legacy 不在批量可跳过的“确证依赖缺失”集合内：其出现使该候选范围的当前-schema 查询受控失败，不悄悄漏行。支持的完整 legacy 恢复仍可正常绑定；缺失信息的旧备份须先经上述修复才能激活目标上的 v2 保证。

## 8. snapshot、事务、本地 DDL 与对象重建

- `S/W` 来自请求本来的 TxnOperator/目录 workspace。planner、候选集合、根 View、嵌套依赖与格式器使用同一解析域；描述器不新建独立事务读最新目录。
- 当前读在每个 statement snapshot 固定后开始推导；RC 下一语句依法推进 snapshot，同时换执行代/memo。允许事务本地 DDL 的路径必须看到自身 overlay；DDL 若按既有语义隐式提交，则下一语句使用提交后的事务，不能为测试假定它仍在旧事务。
- 重试只能走既有合法事务/语句重试边界；旧输出、警告、memo 与依赖证据全部丢弃后重建。描述器自身不无限重试，不把两次 snapshot 的列拼接。
- 历史/named snapshot 使用该域的用户定义和依赖身份，不能用当前同名对象补缺失项。一个 SQL 显式引用不同快照时，各分支有不同 S，预算仍属于同一请求；不把合法多快照查询强制改成一个时间点。
- B 激活后，公共 I_S 的**接口实现**不能因历史快照中保存 V58 等旧系统模板而退回 stale 列。S4 在已知系统模板的解析边界作只读版本适配：保持该支持版本的字段布局/展示规则，用户 View/候选/基表数据仍全部使用请求的历史 S，列权威改由 D 提供；不写历史目录、不换成当前用户 View 定义。未知系统模板明确不支持，不能猜替换 SQL。SHOW CREATE 对历史系统视图仍返回其原持久定义，raw catalog 仍是物理记录；两者不是当前-schema 行源。S5 必须专门覆盖“快照早于 4.0.10”的控制，现有升级前快照测试不能自动替代它。
- Own-DDL 与显式历史读分开：历史解析域不混入“发生在该历史时间之后”的 workspace 改动。不能证明 overlay 处理的上下文不进入 memo；若 resolver 连基本可见性也不能证明则失败。
- 同名 DROP/CREATE、COPY、restore/clone/PITR：新绑定按保存 SQL 的合法名字解析**可见的新对象**；这不是永久钉死原源表 ID。旧结果/计划必须因实际身份或版本变化重建。LogicalID 相同不足以容忍 PhysicalID/version 改变，反之亦然。
- 新 DDL 提交之后才开始的当前读使用新 schema；已获合法旧 snapshot 的执行/cursor 不被强制升级。不能通过 metadata authority lease 延长或缩短数据库本来的隔离语义。

## 9. 权限、订阅与错误面

### 9.1 授权边界

复用 C06 的角色闭包/可见对象规则和 frontend 的 View security 链；DESC 的 SHOW 权限与 I_S 行可见性不是同一个 predicate，不通过粗暴合并扩大权限。对象可见先于其依赖解析；没有可见性就不绑定、不泄露错误、也不计该对象的推导 work。

输出描述只暴露该入口本来有权暴露的字段。语义解析不是执行权限：允许 SHOW 的用户不必被额外要求直接 SELECT 每张基表；当入口需要执行授权时（SELECT/CTAS），继续按 `resolveViewChainPrivilegeContext` 的 DEFINER/INVOKER 链检查源对象。内部 catalog resolver 可在已授权目标内获取推导必需信息，但不得执行数据或泄露额外源名/权限信息。不能缓存一张全租户“可见 View”列表供其他用户复用。

订阅先解析 subscriber 的授权/有效 publication 集合，再以 publisher account、持久定义库和匹配 snapshot 绑定；结果的数据库名保持 subscriber 本地名。不得把 subscriber role ID 放到 publisher 授权查询。withdraw/remap/recreate 在新执行代生效；同名 publisher/subscriber 数据库不能互为缺失依赖的 fallback。

历史 schema 不是授权穿越：仍需当前请求合法的 snapshot/tenant 访问资格；不能仅凭旧 owner/旧授权记录恢复已撤销的访问权。目录历史可见性沿用既有快照策略，任何需要修改授权策略的偏差必须重新审查，而不能由描述器附带实施。

### 9.2 错误分类与展示

| 情况 | 单对象当前 schema（DESC/SHOW 等） | 批量 I_S（包括等值筛选某个 View） |
| --- | --- | --- |
| 合法完整定义 | 完整描述 | 完整列行 |
| 可见且进入候选集，确证源库/表/列缺失 | 受控依赖错误，不用旧列 | 跳过该 View 的**所有**列，沿用现有 warning 1356；其余合法行可返回 |
| 不可见或被安全候选约束排除 | 按入口原有不可见错误 | 不绑定、不返回、无该对象诊断 |
| 存储/RPC/权限/协议/格式错误、legacy 信息不可恢复、循环、深度/工作/内存上限 | 失败 | 失败，不包装成缺依赖 warning |
| ctx 取消/超时 | 原取消原因优先，清理 | 同左，不能把部分结果标成成功 |
| 可重试 schema 冲突 | 交还原请求重试 owner | 同左；已发送结果后不透明重放 |

缺失分类使用能返回原错误的权威目录接口，不把布尔 DatabaseExists 的 false 一律视为缺失；未来代码不得把所有 `NoDB` 等错误不分来源地吞掉。warning sink/保留上限沿用既有机制，不扩展 warning 文本或数量兼容工作。批量 fatal 可在流式发送部分行后以错误结束，不能发送成功终结包或继续返回旧列。

## 10. 关系语义、计划复用与协议

1. C06 继续是私有关系行源，不公开可任意传 publisher ID 的特权 TVF；system-view 固定输出 schema 的绑定不触发全库枚举，避免用户 View 引用 I_S 时自省递归。
2. 可以下推的只有原谓词的安全必要条件，例如租户/对象 ID、schema/table 等值或有界 IN 以及已绑定参数。AND/OR、NULL、大小写模式必须保持 SQL 原义；volatile、可能报错的 cast/表达式不提前求值。
3. 输出列类型/default 条件在 D 后执行；不跨外连接、聚合或 LIMIT 错误下推。保留完整残余 predicate。`ORDER BY/LIMIT` 不允许先截候选再过滤；全库查询按需流式处理，不做规划期全库 UNION 常量展开。
4. DESC 当前生成的 VALUES 是结果数据，不是长期真相。普通缓存/PrepareSchemas 必须捕获根、中间 View 和基表的身份/版本、订阅映射及快照；不能只看顶层 View 版本。无法证明的路径按执行重绑/禁用复用，不回退旧 Cols。
5. I_S 的候选集合和授权每次 EXECUTE 重新建立；缓存计划本身可以复用，但不能固化旧候选。历史依赖只对相同历史身份/域复用，仍检查当前访问资格及对象是否可被合法访问。
6. 二进制 PREPARE 的描述属于 prepare 当时；EXECUTE 在新语义下重绑定并按既有协议发出本次描述。若某客户端路径不支持该变更，返回现有 reprepare/协议错误，而不是编码新行却沿用旧包。S3 必须以实际客户端验证，不能只验证 SQL PREPARE。
7. Cursor 在 EXECUTE 时固定描述和已物化行；FETCH 是同一次结果的延续，不重新读取 schema 或启动推导。后续 EXECUTE 是新代。保留既有 cursor 内存上限，不把 D memo 留在 cursor 中。
8. SELECT/CTAS/结果包来源 flags 可有不同展示，但不可存在独立“按结果列名查旧 catalog”的类型/default 算法。保留 #26226 的特殊类型值传输与来源清除规则，不将本系列扩大为新的 lineage 引擎。

## 11. 数值资源预算与性能验收

以下是 **v2 拟批准的硬约束/验收预算，不是实测达标声明**。实际请求上限取本表与已有更严格的 query/tenant admission 限制的较小者。放宽任何上限须记录新证据并重审，不能超限返回截断的“完整”结果。

### 11.1 请求内资源上限

| 维度 | 上限/规则 | owner 与计费点 |
| --- | --- | --- |
| 持久单 View 定义 | 16 MiB | parse 前检查；沿用当前上限 |
| 一个根描述的输出列、累计绑定列槽 | 各 4,096 | 生成/扩大容器前扣费；不能只在返回后检查 |
| 嵌套 View 深度 | 64（包含根） | 递归进入前检查；不得将当前 expandedViews 限制误报成已有深度 64 保护 |
| 单根 View 展开 | 4,096 次 | 每次进入扣费；循环先检查 |
| 全请求 View 候选描述及展开工作 | 各 65,536 次 | 共享给所有 UNION/APPLY/订阅分支，不按每算子重置；memo 命中按缓存的逻辑子树用量计费，与无缓存展开一致 |
| 全请求依赖证据条目 | 65,536 | 含根/中间/源对象及不同 snapshot 域；插入前检查 |
| memo | 4,096 项且 16 MiB | 仅成功不可变输出；满时逐出/旁路，不存活事务/AST |
| 单个完整描述的保留内容 | 16 MiB | 含列/default/依赖；构建中计费 |
| 全请求描述工作内存 | 128 MiB | 候选、定义副本、AST/binder/必要优化器临时数据、memo、依赖及当前输出一并计费；不是仅 mpool batch 限额 |
| 候选分页 / 输出 | 候选每页至多 32；batch 目标至多 1,024 行或 1 MiB，硬上限 16 MiB | 单行大于目标阈值可单独 batch，仍受硬上限；不得持有全部目录/全部输出 |
| 绑定并发 / 新后台工作 | 每请求 1 个活动描述 binder；新 worker/queue/goroutine 数 0 | 请求内等待可取消；跨请求并发使用既有 admission |
| 描述请求截止时间 | 从首次描述开始累计最多 30s，或调用者更早的 deadline | 所有目录 RPC/分支共享截止时间，不因换 View 重置；不改变父事务生命周期 |

128 MiB 是需要实现和验证的**描述子系统存活内存**上限，不是声称 Go RSS 可精确等于它。Go heap/arena 中 AST、表达式和临时拷贝须有分配前的收费或保守预留；缺少这种证明不能靠“16MiB SQL + 最后一次 len 检查”宣称防 OOM。排序/聚合/客户端已消费结果另归现有查询 admission，不免除其限制。

### 11.2 固定基准合同（S6）

统一使用 release/非 race 构建、go.mod 的工具链、8 个固定 CPU 配额与 16 GiB 内存、SSD/本地目录的单 CN+TN 基准环境。记录 OS/CPU 型号/配置/拓扑/原生库；两实现跑同一实例规模和数据分布，不比较不同机器的绝对值。变更此基准环境须在运行前审批等价性，不能跑完再放宽预算。

三个对照：B0 = 本文源码基线的现有按需路径；B1 = v2 cache-off；B2 = v2 statement-memo。可以附加 V58 的陈旧物理目录读成本，但它不满足相同 freshness，不能被标成完全等价更优方案。没有激活的 durable worker 不得列为实测方案。

深链场景为 32 层逐层直接投影；共享场景为 128 个根共同引用同一条 16 层中间 View 链；浅 View 全部直接投影同一基表，以便区分唯一依赖和根对象枚举成本。另以最小菱形 DAG 验证复用不是循环，不能拿链基准替代该正确性测试。

| 场景（每表/View 默认 8 列，零数据即可） | 预算（并发 1，warm p95） | 其他验收 |
| --- | --- | --- |
| 普通表定向读及普通表-only 1,000 表目录 | 相对 B0 增加不超过 `max(10%, 1ms)` | alloc/op 增加不超过 10%；View binder 调用 0 |
| 一张 View 定向 DESC/I_S；目录含 10,000 个无关 View | ≤25ms | warm 分配 ≤2 MiB/op；无关 View 绑定 0，记录候选目录扫描代价而非假定 O(1) |
| 深度 32；128 个根共享 16 个中间 View | 单根 ≤100ms；128 根集合 ≤2s | 分别记录 unique 依赖数、实际 bind/memo 命中、总分配；不允许按输出列重复完整描述 |
| 全库 1,000 / 10,000 个浅 View | ≤5s / ≤25s | 峰值描述内存 ≤128 MiB；总分配 ≤1 MiB/选中 View；全部行校验 |
| 单 View 4,096 列（简单投影） | ≤1s | 边界成功；4,097 用最小 UT 验证受控拒绝 |
| 一次源 DDL，依赖扇出 0 / 1,000 / 10,000 | S8 后各 p95 相对 0 扇出增加 ≤`max(5%, 2ms)` | refresh-only 写/锁/队列为 0；与普通 catalog/表达式检查的成本区分 |

相对 latency 表达式指 `T_new <= T_B0 + max(比例*T_B0, 绝对增量)`。另跑并发 8：单对象 p95 ≤100ms，吞吐不少于并发 1 的 4 倍，无新增无界排队；目录规模不扩大来替代并发维度。

warm 单对象场景至少 1,000 次测量，批量/大列至少 30 次；独立服务重启至少 5 次记录 cold 最大值（小样本不称 cold p99）。cold 单对象 ≤100ms、1,000 View ≤10s、10,000 View ≤30s，测启动就绪后的首次读，不把服务启动时间混入查询。计时包含结果读取/关闭，不能仅测首行。

另测：并发 DDL 的首读、授权稀疏目录、订阅、预算边界、取消中断的 AST/RPC/输出三个阶段。取消请求在受控可取消依赖下 ≤1s 退出；cleanup 后请求账户保留字节为 0，经 GC 后没有请求对象被长期 owner 引用。故障注入后的 termination 不是用 sleep 猜测；真实性能采样与功能 UT 分开。

记录 p50/p95/p99（样本足够时）、CPU、allocs/B/op、峰值 heap/mpool、目录解析次数、bind 次数、DDL 开销和误差范围。优先去掉无关扫描、每列重复绑定、无必要优化和请求内重复工作。预算失败阻塞 S8：优化后重测或由 owner 审批新预算；**不得以 stale fallback 达标**。S7 默认不选，仅在验证成本扣除后确有收益且独立设计批准时成为可选优化/具名 rollout 前提。

## 12. 混合版本、激活与回滚

### 12.1 明确区分三种能力

- **L**：#29139 前的读取能力，例如持久 V58 COLUMNS；不保证当前 View schema。
- **A**：基线已合入的按需能力，协议 100 + 租户 4.0.10 是当前已分配边界，不等于满足 v2 全部契约。
- **B**：完成 v2 全部消费者、legacy/安全/预算检查的能力；命名 `P_OD2` 表示将来实现分配的唯一新协议能力值，**不是复用或重新解释 100**。实际常量和新语义版本号由 S2/S8 在合入时从当前注册表分配并记录；这是编码分配，不留待实施决定行为。

选用现有 cluster capability/版本迁移及 admission 机制承载 B 的资格，不新增 View freshness epoch/lease。新边界有必要：A/L 不能遵守严格 legacy 策略及退役后的保护语义，单 CN flag 不足以阻止旧进程重入。涉及新持久表达式语义的 floor 继续单独保护，不能用“refresh 已关闭”降低它。

### 12.2 状态转换

| 阶段 | 允许行为 | 进入/退出条件与失败处理 |
| --- | --- | --- |
| R0：现有兼容运行 | 按实际 L/A 与已提交租户模板运行，保留旧表/写路径 | 不声称 v2 一致性；B binary 默认兼容模式，不能抢先切换共享定义 |
| R1：全 B 就绪、尚未激活 | 只读 inventory、S5/S6 验证；仍保留兼容维护 | 所有可服务 CN、升级执行者及相关 proxy/control-plane 路由满足能力；重启/同 UUID 替换重新校验，不能仅看 CN 数量 |
| R2：v2 消费者激活 | 同一请求选择完整 v2 契约；继续保留兼容 catalog 结构 | 先证明旧进程不能再 ingress/执行新语义；既有旧代请求 drain 后提交版本化切换。inventory 中不可恢复定义需修复或获得明确拒绝策略批准；新读不按分支混选权威 |
| R3：refresh-only runtime 退役（S8） | 停依赖 fan-out/revalidation/recovery/仅 refresh 的锁 | S5/S6 达标，共享用户已迁离，旧进程禁入成立；不得先删除表/锁行 |
| R4：兼容保留或清理（S9） | 默认保留旧表 stub、字段与历史升级路径 | 只有全部支持的 reader/writer/restore/control-plane 不再引用，且版本门禁/回滚窗口允许，才另提清理迁移 |

切换顺序固定，不能假定 HAKeeper 与租户 catalog 有跨系统原子提交：

1. 用现有能力/admission 机制先持久化 B 所需的单调版本资格，封住旧 binary 新入口，验证已有旧执行代及连接已按既有 drain/cancel 协议退出；未能 drain 就不进入下一步。floor 不因后续失败而下降。
2. 在全 B 保持兼容模式的前提下，按事务提交新的租户语义版本/系统模板；**这个提交是该租户新请求切到 v2 的线性化点**。运行模式取当前已激活资格，而不是用户 SQL 指定的历史 snapshot；历史用户 schema 始终按第 8 节读取。
3. 所有 B 入口在选择新请求执行代时遵循已提交资格；现存 session 的计划/memo 不能跨模式直接复用。不能证明资格/版本时拒绝该元数据请求，不随机选 A/B。迁移中断于第 1 步后时继续使用全 B 的兼容读侧并重试事务，第 2 步后则幂等继续，不能让旧 reader 再服务。
4. 完成所有目标租户切换及 S5/S6 证明后才进入 R3。刷新状态的 CURRENT/COMPLETE 不参与这些资格判断。

本文不预设当前 floor 代码已能直接完成这条转换：S5 必须验证其 ingress/重启闭包，S8 保留共享安全逻辑后才移除 refresh-only 副作用。不能为消除最后一个 revalidation 写而提前删除 admission；如需改变共享协议含义而非复用既有机制，必须补充设计并重新审批，不能静默搭建第二套 barrier。

### 12.3 回滚与恢复规则（D08）

- R2 前允许回到先前 binary/模板组合，仅当所有现存持久协议要求和 catalog 版本兼容。此时可能仍有 #26227 的旧语义，不能宣传为 v2 保证下的回滚。
- R2 后，不支持在线退回 L/A 或降 floor。旧 reader 可能读陈旧列或接受被 v2 拒绝的定义，**必须在服务入口拒绝**。可以回退到仍遵守 B 契约且理解 catalog 的前一个已验证 binary；否则 forward-fix 或停服恢复完整切换前备份，并明确新写入数据损失/迁移风险，由运维 owner 决定，不能自动执行。
- 升级中断：未提交的迁移回滚，已提交的能力/模板按版本继续；重跑幂等，不能旧 worker 以相同版本号但不同 offset 假完成新任务。保留 4.0.9 → 4.0.10 的现有顺序。
- Fresh install 直接建立匹配 binary 的模板及兼容结构；不得跳过持久表达式/权限门禁。restart 只丢请求 memo，不清 durable 协议 floor。
- Legacy backup/restore/clone/PITR 保留定义、环境、owner/授权映射与未知字段；目标服务在开放读取前校验其协议、snapshot 范围和第 7 节 legacy 资格。不能以源环境的旧 Cols 修复目标当前 schema，也不能为了恢复退回已不支持的 reader。
- 兼容 View 列仍在 CREATE/ALTER/restore 正常写入，本系列不授权删掉这些结构。停止源 DDL 的 refresh-only 写与停止创建时兼容列写是不同决策；后者不在 S8 中实施。

## 13. 共享 admission/fence 保留清单

此表基于真实调用关系，而非文件前缀。状态为 v2 的删除规则，不声称已退役。

| 资产与消费者 | 必须保留的独立契约 | 可退役部分及证明 owner |
| --- | --- | --- |
| [persisted_ip.go](../../pkg/sql/plan/persisted_ip.go) `RequirePersisted*`；base binder、CREATE/ALTER、View 重绑定、table dump/load、snapshot restore | 持久表达式 authoring/read floor，旧 binary 重入保护 | 不因 metadata 按需而删除；planner/协议 owner |
| [server_view_metadata_admission.go](../../pkg/cnservice/server_view_metadata_admission.go)；CN heartbeat/ingress | 持久表达式 read/authoring floor、generation 和 ingress handoff | `fenceViewMetadataCatalog -> RequireViewMetadataRevalidation` 的旧 refresh 副作用仅在替代资格证明后移除；CN/HAKeeper owner |
| [view_metadata_admission.go](../../pkg/hakeeper/view_metadata_admission.go)、catalog_metadata_*、snapshot_codec、logservice/proxy/clusterservice | 持久能力/成员代、旧 heartbeat、同 UUID 替换、快照/恢复兼容、路由拒绝 | 保留共享 wire 字段和历史解码；只删不再服务其他能力的 refresh 完成状态，不能整文件删除 |
| [catalog/view_metadata.go](../../pkg/catalog/view_metadata.go) SNAPSHOT gate / feature-registry 身份锁 | snapshot、restore、账户生命周期一致性及稳定锁序 | 保留外层保护；View 表锁是滚动兼容内层，须在所有旧用户退出后才能去掉 |
| [snapshot.go](../../pkg/frontend/snapshot.go)、[pitr.go](../../pkg/frontend/pitr.go) | 全目录恢复、PITR、对象身份与 snapshot 范围 | `prepareViewMetadataMutation` 等 refresh 标记单独迁出；不能删整个 restore 临界区 |
| [authenticate.go](../../pkg/frontend/authenticate.go) `inheritViewMetadataRevalidation`、账户 shared-gate admission | CREATE ACCOUNT 与全局 restore 的 writer-fair/同事务保护 | 继承 refresh marker 可停；外层账户/SNAPSHOT admission 留存，按 owner-atomic 语义测试 |
| [publication_subscription.go](../../pkg/frontend/publication_subscription.go) | publication/subscriber 生命周期、权限及 snapshot 映射 | refresh 反向失效写可停，publication 本身的锁/校验不可跟着删 |
| [view_metadata.go](../../pkg/sql/compile/view_metadata.go)、[view_metadata_recovery.go](../../pkg/sql/compile/view_metadata_recovery.go) | 保留被其他调用者使用的身份解析/协议约束 | S8 才停 reverse closure、revalidation、RefreshViewMetadata 控制处理、retry/lease/scan；读取不调用它们 |
| bootstrap 历史迁移、mo_view_refresh/mo_view_dependencies、嵌入依赖 | 支持的升级/备份/恢复旧格式；无状态副作用的兼容存根 | S9 按逐项 reader/writer 清单决定，默认 retain；不把旧表改作新权威 |

已确认的全局保护链是 feature-registry 身份（需要全目录恢复时）→ SNAPSHOT → 兼容 View gate；它不是对所有账户/对象行锁的新增全局总序。账户创建在既有事务中的重入、writer-fair admission 与旧远端 owner 回退见 [account_lifecycle_lock.go](../../pkg/frontend/account_lifecycle_lock.go)，须保留其 owner-atomic 协议，不能在删除 marker 时一并删除。删除内层锁不授权颠倒剩余顺序。S8 最终证据须区分“无 refresh-only 锁/写”和“仍存在合法共享锁/能力保护”。

## 14. 方案比较与可选缓存

| 方案 | 正确性/成本 | 决策 |
| --- | --- | --- |
| 当前持久副本 + 部分按需 | 单路径可用但消费者不完整、raw 语义易混淆；历史维护仍有 runtime 耦合 | 迁移基线，不视为 S1–S9 完成 |
| DDL 同步维护全部依赖 | 可原子维持持久 schema，但 DDL fan-out/锁持有、坏 View 与递归成本放在写路径；restore 和跨租户复杂 | 不选；除非 raw 实时物理契约成为不可撤销要求，否则成本不合理 |
| durable 异步 refresh | 扫描已物化列便宜；要 pending-read 策略、依赖失效、重试/租约/恢复/版本协议才能保证 freshness | 不选；存在“最终会刷新”不等于当前语句一致 |
| 每读绑定再写回 | 同时承担读取绑定与写事务/失效成本，且可能污染不同 snapshot | 拒绝 |
| 按需 + 请求 memo | 把成本移到实际读取者，MVCC 可见性直接由请求拥有；bulk 读 CPU/内存更高，接受 raw 物理兼容界限 | 选择；以第 11 节真实预算而非普遍更快为依据 |
| 立即引入全局/跨请求缓存 | 需证明完整传递依赖与 snapshot/权限/生命周期，可能重建旧恢复系统 | 首版不选 |

S7 若经 S6 选择，另审 CN-local immutable cache：硬 byte/entry 限额、无 txn/session/AST handle、成功结果限定、无负缓存、完整传递依赖重验、每请求授权。历史/订阅/own-DDL/legacy 无法证明则旁路。通知仅用于加速逐出，不能是唯一失效证据；view-own-version/TTL 不够。并发 fill/取消/逐出须有独立所有者和边界；不得为缓存新增 durable queue、lease、worker 或 cluster freshness 协议。本文不预先批准该缓存实现。

## 15. S2–S9 执行分工与门禁

| 阶段 | 当前可复用成果 | 剩余交付与退出条件 |
| --- | --- | --- |
| [S2 #29437](https://github.com/matrixorigin/matrixone/issues/29437) | 生成器、隔离 context/provider | 完整 immutable 输出/依赖契约、legacy 分类、请求 memo、统一资源/深度计费；新契约在 B 激活前不改变默认行为 |
| [S3 #29438](https://github.com/matrixorigin/matrixone/issues/29438) | bindView、CTAS nullable 边界、CatalogDependencies、PrepareSchemas | C02/C03/C08–C10/C14 全消费者与全部依赖闭包；数据/描述独立验证；不重复修已落地部分 |
| [S4 #29439](https://github.com/matrixorigin/matrixone/issues/29439) | DESC/I_S/订阅行源及 V58 控制 | C04–C07/C11–C13/C16–C17；raw 兼容说明、SHOW 旁路、任意关系 SQL/授权/取消；不新增 COM_FIELD_LIST/View DUMP 能力 |
| [S5 #29440](https://github.com/matrixorigin/matrixone/issues/29440) | public/privilege/two-CN/subscription 测试入口 | 第 17 节跨组件缺口；old/mixed/new、恢复、own-DDL、激活/回滚拒绝；复用已核实证据，不把测试存在当作通过 |
| [S6 #29441](https://github.com/matrixorigin/matrixone/issues/29441) | 历史小规模观测仅作背景 | 批准预算下的 B0/B1/B2 基准、成本模型、S7 go/no-go；工具准备可在 S1 后，最终结果须为迁移后消费者 |
| [S7 #29442](https://github.com/matrixorigin/matrixone/issues/29442) | 无默认跨请求缓存要求 | 仅经测量及独立缓存设计审批后启动；“不需要”是有效结果且不阻塞 S8 |
| [S8 #29443](https://github.com/matrixorigin/matrixone/issues/29443) | 协议 100/4.0.10 为 A 的既有边界 | B 的分期激活、零 refresh-only runtime 耦合、共享保护保留、S5/S6 精确 head 证据及 runbook；不 drop table |
| [S9 #29444](https://github.com/matrixorigin/matrixone/issues/29444) | 兼容表/旧升级/restore 载荷 | 每项 retain/remove 的 reader/writer 证明；可保留 stub 完成父目标，物理删除不优先于兼容 |

顺序：S1 → S2 → S3/S4 → S5/S6 → S8 → S9；S7 仅在 S6 明确将其定为 rollout 前提时加入关键路径。不能以 issue 名称/旧状态机械重复 #29139 的实现。

## 16. 旧任务验收协调

| 任务 | 保留的不变量 | 被替代/需要 owner 明确同意的内容 |
| --- | --- | --- |
| #26226（当前已关闭） | ENUM/SET 单源 provenance、表达式/集合边界清除、SQL 可见值和 raw 值传输分离 | 无语义重定义；本系列不得重新按名字猜来源 |
| #26232（当前已关闭） | View default 与 CTAS 可执行 default 的独立规则、精确 source/remap、nullability | 无语义重定义 |
| #26227（原 bug） | 同 snapshot 的 SELECT/CTAS/DESC/I_S 一致、完整列推导、身份/权限保存、DDL/订阅/历史边界与受控失败 | “刷新持久列”“原子替换派生 catalog”“恢复分页公平”等旧实现验收改为只读推导/请求预算；raw 物理契约按 D03 显式批准。未获 owner 同意且未有当前公共证据前，不称该 bug 已全部解决 |
| #29003/#29004（先前已关闭任务） | 仍有共享用户的 wire/snapshot/admission/持久表达式保护 | 不按 View 前缀删除；最终组件 owner 签署第 13 节清单 |
| #29005（本次核查已 CLOSED，#29139 合入） | 其中有用的上下文/受控成本目标 | durable generations/recovery 不再是目标；不重开、不重新实现，也不把关闭理解为恢复机制现已开启 |
| #29006（OPEN） | reusable/cursor 不陈旧、取消/生命周期安全、无关 SQL 可用 | refresh authority lease、终结包/COMMIT 新鲜性 fencing 路线被 snapshot+依赖校验替代；独立已有安全门禁保留；需 frontend/control-plane owner 协调任务处置 |
| #29007（OPEN） | 混合版本、重启/恢复、rollback denial、公共一致性证据 | required generation → recovery COMPLETE → authority reopened 激活序列被 R0–R4 替代；owner 明确重定向后才修改任务 |

#28079/#28317/#29396/#29400 仅为生命周期/升级场景来源，#29095 为元数据可见性协调项；不声明同根因或已修复。本任务不擅自修改、关闭或评论任何旧 issue。

## 17. 契约到验证映射

本次文档改动 R0，仅校验文档/源码引用和决策闭合，不运行无关 Go suite。下表是后续生产实现的必需证据；“已有入口”不是新 PASS。

| 契约 | 最便宜的独立证明 / 最近反例 | 已有入口及后续 owner |
| --- | --- | --- |
| 类型、default、特殊类型、outer join | typed D/provenance UT + 公共 SELECT/CTAS/DESC/I_S 的独立预期；表达式/UNION 为清除对照，不用生产 helper 生成期望 | `pkg/sql/plan/view_description_test.go`、`pkg/tests/issues/issue_26232_test.go`、`pkg/tests/upgrade/view_description_test.go`；S2/S3 |
| frozen star、显式名、legacy | 1 个源表，ADD/DROP/MODIFY；完整环境、缺 mode、缺 name-mode、raw star 分别测试；未知历史不得猜测 | planner view/dependency/stable-star 相关 UT；S2/S5 |
| 当前/own-DDL/历史快照 | 两会话 barrier：同一旧 snapshot 对照新 snapshot；本地已完成 DDL 的可见路径与 rollback；named snapshot 后同名重建 | PublicSQL/Subscription 扩展最小场景；S5 |
| raw 兼容 | 源 ALTER 后 D 新值、raw 旧值被**明确断言**；普通表 raw 控制；写计数为 0 | S4 新兼容断言；只有 D03 获批才可将此当正确预期 |
| LIMIT 0 结果描述 | 源 MODIFY 后 `SELECT * FROM v LIMIT 0` 与显式列 LIMIT 0/正常 View 绑定的类型一致；普通表快路保持；无行不等于无需绑定 | C14 的 planner/frontend seam 与真实 client 列描述；S3 |
| prepared/cache/cursor | 改变中间 View 或基表但根 View 版本不变；二进制 client 读类型；已开 cursor 保留旧结果，再 EXECUTE 取新描述 | 现有 public prepared 与 frontend cursor/cache fixtures；S3/S5 |
| 权限、角色、订阅 | 先过滤再绑定的计数 oracle；不可见坏 View 不影响定向读；definer/invoker、撤销 publication、同名 publisher/subscriber、snapshot 控制 | Privileges/Subscription 与既有 auth UT；S4/S5 |
| 任意元数据 SQL | 定向/全库、参数、OR/IN、JOIN/外连接、聚合、ORDER/LIMIT；筛除坏 View 与真正进入候选的坏 View 对照 | PublicSQL + sysview/planner/table_function UT；S4 |
| 不写/不执行数据 | 注入 catalog-write/执行器调用计数，应为 0；允许目录解析；取消后下一请求重新成功 | provider/generator seam；S2/S4 |
| Q1–Q3 | partial-init、递归循环与共享 DAG、64/65、预算 N/N+1；取消于解析/RPC/输出、提前消费停止、Reset/再执行；假依赖+barrier，不靠 sleep | `view_description_context_test.go`、`view_binding_process_test.go`、table_function UT；S2/S4 |
| origin CN/共享 context | 双 CN、AP/TP、多个 UNION 分支、非管理员订阅；记录实际 placement，normal + 风险对应 focused race | `TestViewDescriptionTwoCN` 及现有 process/provider fixtures；S5 |
| 升级/恢复/回滚 | L/A/B 真实 binary 组合、V58 持久定义、早于 4.0.10 的历史 I_S 适配、4.0.9→4.0.10、资格提交与租户提交之间中断、旧 UUID 重入、R2 后旧 reader 拒绝；restore/clone/PITR 的身份/权限 | bootstrap/upgrade、catalog admission、snapshot suites；S5/S8/S9 |
| 无 refresh-only DDL 成本 | 同 SQL/扇出变化的锁/写计数与延迟；共享保护反向控制仍有效 | compile/frontend/catalog fixtures + S6 基准；S8 |
| 无数据类型的接口 | COM_FIELD_LIST 当前行为、View DUMP 拒绝、坏 View SHOW CREATE 可读、查询结果 dump 不重绑定 | frontend/show/table_dump fixtures；S4 |
| 性能/容量 | 第 11 节完整环境、cardinality、采样、取消释放；cache-off/memo 结果一致 | S6 专用 benchmark；不是 BVT 压测 |

公共回归优先扩展 [view_metadata_on_demand.sql](../../test/distributed/cases/view/view_metadata_on_demand.sql) 及其 result，使用 mo-tester 生成、人工审查和正常比较；新增隔离场景才另建 fixture。数据通常 0–2 行，UT 用可配置小预算覆盖边界，不在 BVT 建 10,000 View。每项提供 exact head、选择非空、终态、工具链/模式/拓扑；可复用仅语义输入未变的历史证据。生产覆盖率及 owning-package/static/race 要求依仓库规范，不能用文档检查代替。

## 18. 运维、可观测性与接受的代价

- 新增观测限于低基数：`surface`、`result_class`、`phase` 的枚举，记录描述耗时/数目、bind 次数、请求内存峰值、预算拒绝、兼容拒绝和 memo 命中；不把租户名/View 名/SQL/任意错误文本作为 metric label，不保留 per-view durable 状态。
- 调试身份仅在已有受权限控制的请求 trace 中按需记录；使用现有采样/日志容量，不新增无界 per-view error 日志。
- B 上线先只读 inventory 和离线/测试双读，不在用户请求中双执行数据或双发 warning。单租户/分批模板切换也必须满足 cluster B 能力与请求代 drain 边界，不能局部撤销全局旧 binary 禁入。
- 告警以连续 5 分钟的采样窗口为起点：预算/兼容拒绝比例 >1% 或单对象 p95 超过相应预算触发调查；低流量场景报告实际计数，不把少量样本当可靠百分位。
- 读取变慢、legacy 拒绝、raw 不新鲜是公开接受的代价，不以 silent fallback 掩盖。运维处理是缩小查询候选、修复明确定义、调优/扩容或 forward-fix，不是启动旧 refresh worker。
- S8 runbook 必须列实际版本号/能力值、支持 binary 清单、各阶段检查、退役前后锁/写计数、回滚拒绝及恢复演练；S9 必须给出 retain/remove inventory。没有这些交付即不能激活/删除。

## 19. 决策日志与审批门禁

| 决策 | 本修订选择及代价 | 批准要求 |
| --- | --- | --- |
| D01 | 基于 #29139 增量闭合全系列；不重做已有能力、不复活异步刷新 | 整体设计审批 |
| D02 | 复用 binder/provenance，一个当前输出契约；不改 CTAS 独立默认规则 | planner/SQL owner |
| D03 | raw mo_columns 明确物理兼容，当前 schema 消费者迁出；协调 #26227 持久化承诺 | **用户/原验收 owner 显式批准** |
| D04 | 丢失历史上下文/star 不猜测，legacy 受控拒绝+owner ALTER 修复 | **兼容策略显式批准** |
| D05 | 同 snapshot/overlay 一致，身份重建触发重绑；历史读/cursor 不强制最新 | frontend/txn/SQL owner |
| D06 | 延续既有 single error / bulk 缺依赖 skip+warning；其他错误 fatal | 延续 v1 已有决策；新增 legacy/预算拒绝随本修订审批 |
| D07 | 请求 memo 有界、cache-off 是正确性参考；跨请求缓存另审 | 整体设计审批；S6 另给 go/no-go |
| D08 | R2 后拒绝在线降到 L/A；保留共享 floors/锁；S9 默认兼容 stub | **运维/兼容 owner 显式批准** |
| D09 | 第 11 节数字是提前确定的验收预算；不假设 on-demand 更快 | **性能/容量预算显式批准** |
| D10 | COM_FIELD_LIST/View DUMP 不扩功能；其它列消费者按契约迁移 | 整体范围审批 |

没有留给实现者的“二选一”架构决策；上述选择仍是**待批准提案**，特别是 D03/D04/D08/D09。若任何必需 owner 不接受，应修改设计并生成新精确修订，不在代码中悄悄偏离。

审批记录必须包含：修订摘要、批准人/身份、日期、明确范围、批准或拒绝、未接受条款的处理。仅“方向可以”、执行计划授权、旧 PR approval 或本地自审不够。S1 完成条件是精确版本获批且无阻塞决策；本文自审可提交评审，不等于已满足该条件。后续实现每个 PR 引用获批修订与对应契约/证据，不以 issue roadmap 代替设计。

## 20. 参考与兼容依据

- MySQL 8.0 [CREATE VIEW](https://dev.mysql.com/doc/refman/8.0/en/create-view.html)：定义冻结等作为兼容参照；不是 MO 内部实现要求。
- MySQL 8.0 [INFORMATION_SCHEMA COLUMNS](https://dev.mysql.com/doc/refman/8.0/en/information-schema-columns-table.html)：字段展示参照。
- MySQL client/server [COM_STMT_EXECUTE](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html) 与 [COM_STMT_FETCH](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_fetch.html)：执行/游标协议边界参照；实际 MO 支持范围以 frontend 源码和客户端验证为准。
- v1 保留的 MySQL 8.0.45 invalid-view 对照及 #29139 reviews 是历史证据。本次没有重新测量，也不从其局部成功推导整个系列已经正确。
