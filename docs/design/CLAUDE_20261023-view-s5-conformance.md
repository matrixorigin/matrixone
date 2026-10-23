# 普通 View 元数据 S5：合同、恢复修复与验收证据

所属：[#29440](https://github.com/matrixorigin/matrixone/issues/29440)，父任务 [#29433](https://github.com/matrixorigin/matrixone/issues/29433)，Draft PR [#30953](https://github.com/matrixorigin/matrixone/pull/30953)。

**完整 S5 尚未验收，S8 rollout 为 NO-GO。** 默认 S3/S4 激活仍关闭。独立可做的协议、授权、历史、恢复与多 CN 验证不能笼统归为“等待激活”；另一方面，真实 L/A/B 切换不能由 mock、同 binary 多 CN 或 integration 接线代替。

## 1. 基线与批准边界

当前合并基线为 `mo/main@5fb586e1429386260b900171f65ffa844f49c981`，本地 merge `04db167d87409c8524d7a84ea27faf76583792e3`。首次部分交付 `22b48b77dffaaeeb9f773f47e561ae1d3d0a1728` 只有测试/文档；**续作已经包含生产恢复修复，不再适用“无生产改动”或“覆盖率门禁不适用”的描述。**

批准依据：

- [S1 设计及精确审批](CLAUDE_20260922-view-metadata-on-demand.md#s1-批准与-e1-勘误)。
- [S2 v2.1](CLAUDE_20261005-view-s2-scope-revision.md)：不恢复已撤销的全链路 128 MiB 逐对象证明。
- [S3](CLAUDE_20261018-view-s3-consumers.md)、[S4](CLAUDE_20261019-view-metadata-s4-consumers.md)、[S4 对账](CLAUDE_2026-10-20-view-metadata-s4-reconciliation.md)。
- [既有生命周期验证范围](CLAUDE_VIEW_METADATA_LIFECYCLE_CONFORMANCE_29007.md)。
- [独立 RC 生命周期升级约束](20260929-rc-lifecycle-protocol.md)：停服、drain、成员替换和旧进程禁入要求不能被 View 测试豁免。

默认排除 partition、stored procedure、UDF 功能及 warning 文案/数量兼容。没有 S8 部署、refresh 退役、S9 catalog 清理或新激活状态机。既有协议/授权检查不能为了通过测试而降低。

## 2. 判定规则

同一身份、合法 catalog/statement snapshot、workspace、创建环境和授权域产生相同 schema；并非任何历史查询都必须得到“最新 schema”。

- 验证类型、宽度、顺序、default 和 nullability 的语义。View 的 default `7` 与 CTAS 按现有规则生成的 `0` 可以不同。
- 固定旧 statement 与已物化 cursor 保持旧代；新 statement、重绑及 retry 使用其合法的新代。FETCH 不重新推导。
- 根 View ID/version 不变不能证明传递来源未变；COPY、同名重建、内层 View 变化仍须失效。
- RC snapshot 推进属于真实 statement owner；旧请求必须拒绝跨 stamp 复用。
- 根授权与查找每次执行；失败不能回退 warm memo 的成功。固定合法 snapshot 下，memo 命中无需重新读每个传递叶子。
- 描述只读，不执行 View 数据、不刷新目录、不接管父事务。`mo_catalog.mo_columns` 仍是 D03 物理/MVCC 表面，不是唯一派生 schema oracle。
- 历史 schema 的解析与当前访问资格是两件事；caller authorizer 不能只检查角色而忽略 publication 当前资格。

## 3. 发现的恢复缺陷及最小修复

### 3.1 不能用恢复管理员的会话重造定义属性

真实跨账户恢复反例：reader 只有 View grant，没有 underlying table grant；源 View 为 INVOKER，管理员恢复后却按当前会话的 DEFINER 重建，从拒绝变为允许。原失败保留于 `remaining-account-restore.*`。

`WithRestoredViewDefinition` 仅绑定恢复 owner 的目标库、View 名和原始 replay SQL。CREATE 的既有 admission、语法、授权及持久协议检查照常执行。新生成目标 SQL/definition/dependencies，保留原 security、check option、SQL mode、大小写环境与未知 JSON 字段；required protocol 取原始/新生成的较大值。

原始 replay SQL 在 star 展开前保留：不能把旧 `*` 伪造成创建时已冻结的投影；缺失、null、空 legacy payload 的环境也不能被恢复会话补成“已知”。

### 3.2 创建数据库不等于 View 所在数据库

反例：`USE creation_env; CREATE VIEW app.v AS SELECT id FROM origin`。`creation_env.origin` 为 bigint，`app.origin` 为 int。只修补最终 JSON 不足够：恢复时首次生成的物理列和依赖也必须在正确环境绑定。

- 通用恢复在目标账户中复用 `viewRegenerationContext`，在列/依赖生成前使用原始创建数据库和标识符策略。
- snapshot、PITR、timestamp restore 的三个依赖排序 owner 同样使用该环境。临时 compiler context 在成功、错误时均恢复；只操作既有 isolated binder。
- 后台 parser 仅在精确 replay SQL 匹配时使用保存的大小写策略；不改 session/global 变量，不污染内层目录查询。
- **clone 是独立的映射 owner，但会间接调用 restoreViews。** `rewriteCloneViewInfos` 按明确的 source→target 映射重写匹配的创建数据库，外部创建数据库不变；SQL 重写使用保存的 SQL mode/大小写策略。不能把所有非空 DefaultDatabase 都改为 View 所在库，也不能完全忽略 clone 映射。

真实失败分别保留于 `acceptance-native.*`（创建域错绑）和 `delivery-native.*`（clone 错指原库，期望宽度 20，得到 80）。修复加入同名 decoy、跨库 nested View、原账户后续变化不污染目标等反例。BVT 使用两个不同值的最小数据行验证真实读取域及 metadata，不只比较空结果。

## 4. 默认关闭的验证接线

普通构建的两个 frontend hook 返回常量 false，没有 registry、配置或 SQL 开关。`integration` 构建也默认关闭；仅 `_test.go` 能为测试拥有的 CN/数据库注册 route，background session 不选中。

- 通用 SELECT/PREPARE 经过原统计、prepare normalization 与最终授权 owner，再显式使用 S3。
- 参数化 EXECUTE/compile-retry 另走 `buildPreparedBoundQuery`，不能只接通用入口就声称每个执行代使用 S3。该 owner 也有默认关闭的验证 hook。
- S3 的 admission、root authorization、依赖收集、取消、代号检查和 cleanup 保留一个公共内部壳；integration 的 prepared bridge 调用既有参数域 binder，不复制其类型/优化语义。
- private binding process 只在绑定期间借用当前参数向量，并复制其类型/二进制来源属性；返回前撤销借用，不能释放父载荷。S3 请求在返回 plan 前关闭，不进入 cursor 或 reusable-plan 生命周期。
- S4 的直接执行 fixture 持有请求直到 Compile/Run、结果读取和释放完成，再关闭请求；父事务/执行代仍存活。

这只是可执行候选的局部验证入口，不是 durable capability、tenant activation 或生产 authorizer 的完整实现。

## 5. 合同与证据入口

源码入口存在不等于当前修订已经跑完；终态单独列于第 7 节。

| 合同 | 入口与独立 oracle | 边界 |
| --- | --- | --- |
| INPLACE/COPY/同名重建、frozen star | `testNativeViewMetadataDDLTransitions`；真实物理 ID、根 View ID、20/40/60 宽度、default、固定 snapshot | 不靠 SQL 拼写猜 DDL 算法，不强迫旧 snapshot 读新 schema |
| S3/S4 各表面 | 同一父事务中的 SELECT/CTAS typed plan、DESC/SHOW/FULL、I_S 实际执行；memo-on/off | CTAS 该组证明的是规划，不冒充其真实执行 |
| RC 语句推进 | 真实 SQLExecutor statement owner 更新 snapshot；旧请求 `ErrViewSchemaChanged`，新请求正确 | 不是伪造 parent stamp 的 mock |
| 授权链 | `view_metadata_security_conformance_test.go`；DEFINER/INVOKER、基表 grant/revoke/recovery、不可见根、真实最终授权 owner | 测试 authorizer 不是全部生产 SHOW/安全策略 |
| publication 变化 | 当前 membership withdrawal/restoration、drop/recreate、同名 remap、历史资格拒绝、来源 ID 变化 | 当前资格与历史解析分别处理；完整历史 remap/table-list/生产权限组合尚不能声称全部闭合 |
| clone/PITR/snapshot/account restore | `view_metadata_recovery_conformance_test.go`；target independence、table/database PITR、database snapshot、跨账户 security/创建域/nested View | PITR 用实际数据库时钟跨边界，不用固定 sleep；全版本恢复矩阵仍有缺口 |
| 真实 wire/cursor | `view_schema_protocol_conformance_test.go`；认证后的独占 TCP COM_QUERY/PREPARE/EXECUTE/FETCH/CLOSE、参数、列描述、状态位、provider 计数 | cursor 旧行与 EXECUTE 新 schema 分开断言；参数化 EXECUTE 自身必须调用 provider |
| compile retry/DDL 竞争 | 同协议 fixture 的 post-bind 确定性 checkpoint 与 prepared retry owner | 已通过真实 COPY 竞争：绑定后、执行前提交，prepared binding 次数至少增加两次；不注入假 retry 错误 |
| 两 CN origin/placement | `TestNativeViewMetadataTwoCNOriginOwnership`；两个 origin、TP/AP、memo-on/off、UNION 元数据和真实来源行；Compile/Run physical report | AP 确认真实 remote worker，metadata APPLY 只在 origin；不是 EXPLAIN 空输出或同 binary 升级证据 |
| 历史系统模板 | V58 COLUMNS 历史 snapshot 与当前模板、来源 DDL 后的不同合法宽度；真实旧模板拒绝 UT | 支持的 V58 fixture 与真实协议 10 模板明确区分 |
| warm memo 错误/取消 | `TestViewMetadataWarmMemoFailureRecovery`、`TestViewMetadataWarmMemoCancellationDuringRootRead` | channel barrier；错误不能 bulk-skip/回退；资源归零、新请求恢复 |
| legacy 原始数据 | `view_legacy_artifact_test.go`、`testdata/view_metadata_legacy_v4_2_6.json`；恢复前后资格仍拒绝，memo-on/off 不污染下一合法请求 | 只读 inventory/qualification 证据，不是完整 binary 升级与 owner 修复流程 |
| 默认路径 | PublicSQL/Privileges/Subscription、`TestViewDescriptionTwoCN`、mo-tester | 真实公开兼容控制；不能代替 opt-in provider 证据 |
| admission 组件 | 既有 v4_0_10 Columns migration、CN/proxy generation/撤销/ingress 测试 | 组件模型，不能当真实旧进程重入或 B 版本切换 |

### 真实 L artifact

实际执行官方 v4.2.6 linux/arm64 binary，而不是用新 binary 模拟旧标签：

- release SHA：`71498157cc21f833b8e717f45d50d1f926be3134`。
- archive SHA-256：`1cae542eca54b582b079cf7b0d1ac3e97e8198ede950e6b57747644433c852a5`。
- Go 1.26.4，catalog 4.0.6 / offset 5，最高协议 10，因此是 **L，不是 A**。
- 在具备所需动态库的 `golang:1.27.1` 容器运行，真实创建 explicit/star View、读取存储 JSON 与 SQL 结果，并捕获当时 COLUMNS 定义。较早缺少 libgomp 的启动失败也保留。
- 原始定义没有 `lower_case_table_names`；恢复不得补齐并宣称可由 S2 正确解释。容器已停止删除，artifact/日志保留。

## 6. 完整验收与上线阻塞

`MORPCVersion100` 是既有 A 的描述能力，`MORPCVersion126` 关联 parser-derived I_S.VIEWS；均不能凭数字借作新的 `P_OD2`。当前 main 的默认兼容路径不自动等于完整 B。

必须分别补齐：

1. 冻结支持矩阵与精确 L/A/B binary/config/artifact 清单；不能把开发 OS/版本的任意拼接称为受支持升级。
2. 完整、默认关闭且经批准的 B 可执行候选：所有 consumer/retry 入口、生产授权资格、旧会话/计划失效和真实能力准入闭包。
3. 单调持久资格/旧 ingress 封锁与 drain，然后 tenant semantic version 提交；验证两个提交点的中断、重启和幂等。不得另外发明激活状态机。
4. 真实 all-old/mixed/all-new（仅运行独立 RC 协议允许的组合）、R2 前合法回退、R2 后 L/A 拒绝、同 UUID 旧进程重入保护。
5. 仍未覆盖的历史授权组合、完整失败/重试矩阵及受支持旧备份到目标的 owner 修复证据。

**S5/S6 验证先于 S8 rollout。** 缺候选实现/产物不等于要求先上线 S8；必要候选可以在隔离验证环境开发。任何安全、持久化、激活或回滚合同变更仍需单独设计批准。#29440 保持 open，PR 保持 Draft，关联使用 `Related to #29440`。

共享保护不能以“View 前缀”删除：persisted-expression/remote decode floor、CN/HAKeeper/proxy admission、catalog barrier、feature registry、SNAPSHOT/account lifecycle owner 与 lock order 均保留。refresh-only 逆向 fan-out/worker/revalidation 的退役属于后续 S8；旧目录/字段默认 S9 retain。

## 7. 验证状态与可复现性

环境为 darwin/arm64、Go 1.27.1、配套 gofmt、golangci-lint 2.14.0 / Go 1.27.1。CGo 首次复用因 provenance 不匹配被拒绝后，以本 worktree `make -j8 cgo` 构建；没有绕过校验。测试使用 `.agents/skills/mo-dev/scripts/mo-cgo-test`；BVT 使用 `~/Develop/matrixorigin/mo-tester` 的工具与自动生成结果，不手改 `.result`。

**当前续作的下列验证已收敛通过；不等于第 6 节完整 S5 已验收。** `closure-*` 对应上述合并基线及当前生产源码。自审最后仅修改 plan 取消测试的 deferred join，随后补跑完整 plan 包及精确 race/static；frontend/native 与服务构建不编译 plan `_test.go`，其源码与 fixture 未变。

| 验证 | 当前终态 |
| --- | --- |
| 恢复/legacy 精确 UT | 7 个顶层选择、50 个命名结果，2.471s |
| 普通 owning packages | plan 补验 19.483s；frontend 23.867s |
| 显式 opt-in native | Catalog/Subscription/TwoCN 三个精确顶层测试，136 个命名结果，24.298s |
| 默认公开控制 | PublicSQL/Privileges/Subscription 9.713s；默认两 CN 8.425s |
| static / 构建 | 配套 gofmt、diff-check、integration vet/lint 通过（lint 0 issues）；普通 mo-service 构建通过 |
| BVT | 最新普通 binary 经 mo-tester genrs 生成扩展 result；restore_account_privileges 与 view_metadata_on_demand strict run 通过；grant_view `-n` 补充授权/行比较通过 |
| owning race | integration 模式 plan 116.576s、frontend 187.168s，均通过 |

所有最终精确 race stress 独立运行同一测试、同一进程，`B=30s`，`N=clamp(floor(B/T),1,100)`，`-timeout=120s`；T 来自测量运行的 terminal JSON，包含实际选中顶层 fixture 的执行时间，不包括构建：

| 测试 | T / N | 结果 |
| --- | --- | --- |
| TestViewMetadataWarmMemoCancellationDuringRootRead | 0.01s / 100 | PASS |
| TestViewSchemaPlanningCancelAndClose | 0.01s / 100 | PASS |
| TestNativeViewSchemaCatalogOwnDDLHistoryAndSharedReads | 34.92s / 1 | PASS |
| TestNativeViewSchemaSubscriptionHistoryAndRoleDenial | 31.60s / 1 | PASS |
| TestNativeViewMetadataTwoCNOriginOwnership | 12.10s / 2 | PASS |

**生产增量覆盖门禁通过**：以 PR 基线新增/修改行与 Go cover blocks 相交，剔除空行、注释和纯括号；不是全包覆盖率。普通生产代码 **141/149 = 94.63%**，integration-only **43/48 = 89.58%**，合计 **184/197 = 93.40%**。另列受修改影响 block 的语句加权覆盖为普通生产 90.56%、合计 90.35%；不把它冒充精确新增语句计数。13 个修改生产文件均有覆盖信息，未覆盖行也保留在报告中，不能因此声称所有恢复/失败分支都已覆盖。

证据边界与留存：

- `CLAUDE_FINAL_EVIDENCE.json` 验证精确选择非空、命名与 package 终态、退出码、源码/日志/生成结果/binary SHA；`CLAUDE_changed_coverage.json` 保存分文件分母、命中和未覆盖行。覆盖汇总不用旧 combined profile 的 plan 部分，替换为 join 修正后的完整 plan profile。
- 原 `pr-*`、早期 `final-*` 及各次失败仍保留，不替代当前结果。空 legacy payload 的过时 UT oracle 已修正，首次 closure 失败保留。早期 180s stress 日志不当作最终规范命令，另有上述 120s 命令。
- grant_view 曾有 11 个 strict metadata 宽度差异；当时未加本修复的 baseline overlay binary 重现相同 11 个差异，并多出两个 INVOKER 恢复失败。当前 `-n` 不替代 strict metadata；没有宣称该补充用例 strict 通过。
- 两次 Actions artifact inventory 请求均返回 `unexpected end of JSON input`。这是查询失败，不是“没有 A/B artifact”的证明，更不是支持矩阵。
- 不相关任务占用 TCP 10060 的失败仅发生于 cluster-start，没有终止它们。独占 TMPDIR 的端口 lease 保留进程使 allocator 从 40000 段选择；验证完成后释放。BVT 普通服务已停止。
- 空间不足时等待活动构建结束，仅以 `go clean -cache` 清理可再生构建缓存，可用空间 4.4→46 GiB；未删源码、数据库、模块缓存、原生产物或验证证据。

本地证据与历史失败保留在未提交的 `CLAUDE_validation/`。没有据本地运行声称 CI 已全绿。

## 8. 自审闭包

| 闭包 | 风险/owner | 终结与上界 |
| --- | --- | --- |
| 持久恢复属性/clone 映射 | R2，restore/clone/create-plan owners | scoped immutable payload；无全局开关；原 parser/admission/权限保留；未知字段、legacy 缺失和协议下限不被弱化 |
| 依赖排序 | R2，三个 isolated compiler owners | 原 context 必须 defer 恢复；temporary plan/AST 释放；不污染下个 View |
| S3/参数化验证路由 | R3，编译期隔离 registry、每次 request | request 在返回 plan 前 Close；参数向量借用撤销；旧成功不能掩盖错误；ordinary build 常量关闭 |
| S4/多 CN | R3，origin request/Compile/results | 先结果释放/执行终结，后 request Close；父事务不被关闭；远端只执行可分布的来源部分 |
| cursor/retry fixture | R3，真实 protocol/statement owner | 独占连接，显式 statement close，固定 checkpoint；无客户端重试掩盖错误 |
| warm cancel | R3，单 worker/request | entered→cancel→ctx.Done→resolver 返回→done→Close；两个 barrier，无 sleep 同步 |
| artifact/文档 | R0/R2，证据与兼容 owner | 真 binary、模型、同 binary、合成模板分开；部分交付不等于上线许可 |

已分段完整检查相对基线的 committed/unstaged/untracked Go、SQL、JSON 变更及自动生成结果，核对各 owner 的 Q1–Q3、选择/终态/指纹；自审还修正了取消测试失败路径的 deferred join。没有将 DetachPrepareParams 用于此只读借用，因为它会临时改变父状态；独立 BaseProcess 与不转移 ownership 的借用已沿真实构造链核实。提交前再次检查暂存文件清单、格式与当前 PR 模板。TODO、日志、二进制不提交；保留未合并 worktree。完整验收阻塞不因本次增加用例而自动消失。
