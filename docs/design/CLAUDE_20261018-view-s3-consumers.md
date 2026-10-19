# 普通 View 元数据 S3：显式查询消费者接线

## 1. 范围与依据

- 任务：[#29438](https://github.com/matrixorigin/matrixone/issues/29438)，父任务 [#29433](https://github.com/matrixorigin/matrixone/issues/29433)。
- PR 交付同步基线：`57fb9530859d92791100098d312ea8cacc671910`，以 merge 纳入；此前 PR 基线为 `def799e1260dd547cae2e0e54323ea20ec1b7685`，本地验收基线为 `8f600318beffd82185ee8fdd92a8f0b625b8e5e7`，最初核查/反例基线为 `68e642fdc533e96ed8f83ac44659119ad8be97af`。上游同步不改变 S1/S2 契约和本次默认未激活的边界；主输出步与 correlation 采用上游共同 binder/复制语义，不拷贝另一套实现。
- 依据：[系列契约](CLAUDE_20260922-view-metadata-on-demand.md)、[S1 精确批准记录](CLAUDE_20260928-view-metadata-s1-review.md)、[已批准的 S2 v2.1 修订](CLAUDE_20261005-view-s2-scope-revision.md)。S1/S2 分别通过 #29456/#29633 合入。
- 本文记录上述设计的 S3 接线、边界和验证要求，不是新的上线批准。默认 SQL、PREPARE、cursor 路由不激活新模式；S4、S5/S6、S8/S9 的退出条件没有被本次测试替代。

## 2. 增量而非重做

当前基线已包含 #29139、#29521 等变更：

- `markViewCTASDefaultBoundary()` 已不读取持久化 View 列，类型、来源、default 和 null-extension 由绑定输出决定。
- LIMIT 0/空结果优化前已捕获 `CatalogDependencies`；当前 `query_builder.go` 已无 columns-only 快路，普通表也解析完整 metadata。
- `checkModify` 已校验扫描 snapshot、数据库/关系 ID、版本与订阅身份；`validateCapturedPrepareSchemas` 复用既有 catalog change cache。
- Cursor 已拥有 EXECUTE 代的物化列与行，FETCH 不重新推导。

因此不恢复旧快路，不重写 CTAS default 算法，不添加持久失效系统。S2 的缺口是显式描述请求尚未提供真实可执行查询消费者；而其 resolver 仍按旧 View 列数收费/复制，可能以无关的物理快照拒绝有效定义。

## 3. 不变量与接口

相同的 View 定义身份、可见 schema/snapshot、事务本地 overlay、创建环境与授权域，产生同一权威绑定输出。Query/CTAS 允许有已批准的展示/default 差异，但不能让旧 View `Cols` 决定当前输出。

显式接口：

```text
ViewSchemaRequest.BuildPlan(statement, isPrepare)
TxnCompilerContext.BuildPlanWithViewSchema(ctx, statement, isPrepare, rootAuthorizer)
```

接收 SELECT、括号 SELECT、CTAS，以及包装这些 source 的 typed/binary `PrepareStmt` 和文本 `PrepareString`。不将其它 DDL/DML/SHOW 或 EXECUTE/FETCH 扩大为本次迁移范围；不支持的 PREPARE body 在既有 `getPreparePlan` 的内层语句边界拒绝。SQL PREPARE 仍由已有 parser 和 `buildPrepare` 解析一次。

调用者先建立 statement/transaction/snapshot，提供根对象授权策略。接口仅构造 plan，不执行语句；执行前仍由 frontend owner 调用原有 statement/plan 授权和 View security-chain 校验。schema 可见性检查不是完整执行权限。`isPrepare` 沿用 planner 的语义，不替换 frontend 的 force-prepare 参数归一化、统计/trace 或协议发送流程；后续接线必须保留这些 owner，不能把 helper 当作整个 `buildPlanWithPrepareMode` 的无条件替代。

### 3.1 共享语义，不共享执行树

```text
请求可见性 + frozen provider
  -> 普通查询 binder
  -> persisted View 的创建环境、严格 legacy/protocol、深度/工作检查
  -> 正常绑定嵌套执行子树及 builder-local provenance
  -> SELECT 结果 / CTAS 策略 / 既有 PREPARE 元数据
```

使用与 S2 同一 resolver、绑定上下文、来源/default 和资源规则。不先对每层 View 独立 Describe 再重复绑定，也不把另一个 QueryBuilder 的 RelPos/ColPos 拼入当前执行树。

S2 nested memo 中的 synthetic View scan 只服务 schema 描述，不能执行。规划状态显式禁止加载/生成这种 memo；即使同一请求已通过 Describe 预热，规划仍构造真实基表扫描。描述模式的 memo 能力保留。

### 3.2 身份与持久列隔离

View 定义、环境、身份、owner/权限和版本继续来自可见目录；resolver 先创建无 `Cols`/`Name2ColIndex` 的 View 壳，再进行请求副本的计费和复制。旧列数量、default、类型不进入推导准入。普通表完整 schema 仍须经过既有兼容/协议及资源检查。

物理来源的实际 catalog account 同时进入请求私有的扫描 `ObjectRef.PubInfo` 与依赖副本，不能让扫描按 subscriber 解析。View 的 PubInfo 则表示已解析的订阅入口身份；普通跨库中间 View 继承 publisher 绑定域，不能仅凭账户不同合成 PubInfo、将它误当新订阅入口。其依赖副本仍独立记录实际账户；`ViewReferences` 保留 publisher snapshot tenant 和原始空 SubscriptionName。原目录对象不被修改，已有 PubInfo 及未知字节保留。

这是只读隔离，不更新目录列、ViewSql、依赖表或刷新状态。raw catalog 和备份载荷仍保持物理快照语义。

## 4. 授权、名称域与错误

- 每个用户查询根在数据库/订阅元数据探测及关系解析前授权；嵌套来源的执行权限由计划 `ViewReferences` 保留给现有安全链。
- 授权函数可以执行内部 catalog SQL，因此只接收 request context，**不能接收私有 derivation/planning marker**；否则内部授权查询会被误当新用户根，递归授权直至资源拒绝。真实订阅/RBAC 测试覆盖此边界。
- CTAS 等 planner 内部目录 SQL 在 `runSqlWithSnapshot` 边界屏蔽私有 derivation marker，保留账户、事务、快照、取消及其他 context 值；不按系统表名跳过授权。用户显式查询系统表仍是用户根，必须授权。
- 一次规划从 caller entry 域开始；持久化 publisher SQL 进入真实发布账户/库域，退出恢复，不能把继承的 subscription 当作根身份。
- 数据库存在性使用与 source resolver 一致的错误返回型 `GetDatabaseId`，包括 publisher 和 historical snapshot。不能通过 bool-only `DatabaseExists` 丢失 RPC 错误或切回 subscriber 的同名库。
- 明确缺失源、legacy 环境不足、未稳定 star、损坏定义、协议拒绝、取消和可见性变化均返回错误；没有旧列 fallback。

## 5. 依赖与复用

继续使用 `CatalogDependencies`/`Prepare.Schemas`，不新增 schema cache 或 durable invalidation。

- scoped resolver 捕获根、中间 View、基对象及按 ID/index/subscription 解析的来源。证据包括数据库/关系身份、版本、实际 publisher 账户、订阅与 snapshot。S3 planning 的 View 依赖以该捕获为唯一来源，普通 builder 不再从 View 执行入口 ref 重复推断账户；普通表、默认 binder 与 Describe 的捕获不变。
- 数据子树被 LIMIT 0、where false 或裁剪移除，依赖证据仍存在。
- 对有完整捕获的 S3 PREPARE，`collectPrepareViewSchemas` 使用这些证据，不在 subscriber 名称域重新解析 publisher 的中间 View。
- source 在 publisher 域解析但没有可供 caller 按名访问的 subscription 别名时，保留实际账户证据；既有 name-only validator 无法证明其有效性就重绑，不借用同名 subscriber 对象。S6 再测这种保守旁路的成本。
- `DeepCopyObjectRef` 同时复制 `PubInfo` 及未知字节，计划/派生结果不能与目录共享可变 publisher 身份。
- 原 `checkModify` 与 `validateCapturedPrepareSchemas` 模型不变：同根版本但传递基对象改变，或中间 View/COPY/同名重建，仍必须重绑或旁路。

协议约束不变：binary PREPARE 的描述属于准备时；后续 EXECUTE/reprepare 需要合法的新语句计划与其列描述。已物化 cursor 的 FETCH 沿用原代，不调用描述或规划 API。S8 选用入口时必须将 compile retry/reprepare 一起路由，并按批准的模式转换清空旧计划；本 API 的存在不等于默认服务已切换。

## 6. CTAS 独立契约

保留既有行为，而非强行把所有元数据字段统一：

- View 的源 literal default 可保留为描述 default；CTAS 根据来源策略产生类型默认值。
- expression default 按现有精确来源/槽位 remap 处理，表达式列不继承普通源 default。
- 外连接 null-extension 保留为 nullable；nullable 输出不被强行写成 NOT NULL 或赋予非空类型默认值。
- 普通 CTAS 保留既有 follow-up INSERT SQL、丢弃临时 Query；prepared CTAS 保留 Query 用于参数及依赖处理。没有为了新接口改变持久化/执行模型。
- 显式 View 列名来自稳定语义定义，输出 SELECT alias 与 CTAS 目标规则各由已有 owner 决定。

## 7. 消费者审计分类

功能闭包覆盖源码中的调用点，而不是按 `Cols` 字面命中次数改代码。S3 涉及的直接 View 列在 scoped resolver 统一移除，后续 binder/optimizer 只能取得基表真实列或本次绑定/合成输出。

| 消费者/调用点组 | 权威与处理 | 归属 |
| --- | --- | --- |
| `buildTable` 的 `Name2ColIndex`、列映射、`addBinding` | View 不带物理列进入；转为 `bindView` 的 subquery 输出；普通表仍用完整 schema | S3 |
| `bindView`、显式列名单、`appendMySQLSpecialTypeBoundary` | 真实执行子树、已有来源边界与 names；描述 memo 不进入执行 | S3 |
| optimizer 的 scan pruning/remap、row carrier、CTE/value/function scan 列 | 执行扫描的基表列或请求生成列；不是持久 View 列 | S3 控制项 |
| `genAsSelectCols`、`remapCTASSourceDefaultsToOutput`、`finalizeCTASDefaults`、目标显式列覆盖 | 绑定结果和当前 builder 的来源坐标；不回查旧 View 列 | S3 |
| `BuildTableDefByMoColumns`/`buildTableDefFromMoColumns` | 历史物理 helper；当前 query binder 不调用，不恢复快路 | 保留/非当前 authority |
| `CatalogDependencies`、复制/序列化、`ResetPreparePlan`/`buildPrepare` | 请求捕获的全闭包，经既有复用模型校验 | S3 |
| `GetColumns`、prepared 描述、cursor capture/FETCH、已有结果导出 | 本次计划或物化执行代的列，不重新读取 View `Cols` | S3/S5 |
| CREATE/ALTER VIEW 的 `TableDef.Cols` 赋值 | 定义 owner 的正常兼容列发布；不是读取时维护 | 保留 |
| CREATE TABLE LIKE/CLONE、CHECK/FK/index、ALTER COPY/存储布局 | 物理表定义/约束；相关路径拒绝 View，不转为按需 View 列消费者 | 非当前 authority |
| ANALYZE 的 visible-column helper | `bindAnalyzeTables` 最终明确拒绝非 owned physical table 和 `ViewSql`；不新增 View ANALYZE 能力 | 非当前 authority |
| SHOW/DESC/I_S、订阅 metadata TVF | 列描述展示迁移及旧系统模板适配，不借本次改完 | S4 |
| SHOW CREATE、raw `mo_columns`、backup/restore/clone/PITR 载荷 | 持久语义定义或物理目录，不承诺依赖 DDL 后物理副本新鲜 | S4/S5/S9 保留契约 |
| COM_FIELD_LIST、View DUMP | 不扩展既有未实现/拒绝的接口 | S4 控制项 |

## 8. 所有权、等待与预算

| 资源/状态 | owner 与终态 |
| --- | --- |
| AST、返回计划 | 调用者持有 AST；计划不持有 request/compiler；Close 后仍可编译执行 |
| compiler/冻结环境/输入租约 | lazy Open 的 request；成功/error/panic 都先退出局部 derivation，再归还 gate；wrapper defer Close |
| 父事务、父执行资源代 | 仅借用；child Close 不提交/回滚、也不关闭父资源代 |
| 描述 result handle | 仍遵循 S2 的 Release-before-Close；BuildPlan 本身不创建 result handle |
| 规划依赖/stack/work | derivation 的有界请求状态；跨语句不保留 |

等待图：`Close.cancel -> 当前 catalog RPC/授权查询取消 -> 绑定 defer 释放 gate -> Close 取得 gate -> 清理 child`。竞争 BuildPlan/Describe 在 gate 忙时直接拒绝，不排队等待当前绑定。取消/关闭不依赖持有同一 mutex 后才能传播。没有新 goroutine、worker、queue、全局缓存或持久 freshness 状态。

沿用已批准的 S2 v2.1 输入/解析/深度/工作/列槽/依赖/结果/memo 和既有查询资源上限；真实执行计划继续由原查询 owner 管理，不将 S2 的描述结果编码预算冒称为整个可执行计划的 RSS 硬上限。

## 9. 验证与交付门禁

最小矩阵：

- typed UT：预热 Describe 后仍是真实基表扫描；显式列名、嵌套、CTAS default/表达式/null-extension；文本与 typed PREPARE 的全依赖；物理 View 列 0/1/4097 的独立反例；parent/child 身份复制；上游 correlation 多程序主输出步及 Close 后计划复制的兼容控制。
- 拒绝和控制：缺少创建环境、未冻结 star、丢失 source、损坏定义、授权失败；默认入口保留 legacy 兼容；授权 context 不带私有 marker；数据库错误/snapshot 原样传播。
- 生命周期：失败/panic 后下一次规划成功；blocked catalog lookup 与 Close/BUSY 的确定性 barrier；资源归零，父事务/执行代可继续使用。
- frontend：真正由新 binder 生成的空结果计划，在基表 metadata-only、COPY physical ID、同名重建、中间 View 变化后由既有 cache validator 拒绝。
- native：复用 S2 的两个单 CN fixture；真实 own-DDL/历史/订阅/权限域、严格源 View authorizer 下的无写 CTAS 规划，以及 request 关闭后实际编译执行嵌套/跨库订阅 View 的行与类型。publisher/subscriber 同名源采用不同类型/行值；直接/嵌套/跨库中间 View/历史、普通与 prepared 计划均实际执行，各自使用独立父语句资源代；跨库中间 View 的普通与 prepared CTAS 另做无写规划控制。
- 公共协议控制：已有 `TestViewDescriptionPublicSQL` 的独立 reader/writer、SQL/binary prepare 和 empty-result schema 变化；它验证默认路径无回归，**不是宣称默认 binary 服务已激活 S3**。
- 定向 UT 后执行 owning packages、风险对应 race、增量 gofmt/vet/CI 同版 lint及修改代码覆盖率 ≥75%。未通过的门禁不得标记 PASS。

具体命令、工具链、exact revision、终态与覆盖率保留在本次交付记录；本文不把测试存在或一轮局部通过当作全部验收。S3 不授权省略 S5 的 cursor/多 CN/版本交叉验证，不授权省略 S6 性能或 S8 rollout 证据。
