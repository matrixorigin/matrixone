# 普通 View 元数据 S5：跨组件合同、证据与上线阻塞

所属：[#29440](https://github.com/matrixorigin/matrixone/issues/29440)，父任务 [#29433](https://github.com/matrixorigin/matrixone/issues/29433)。

**结论：本交付补充激活前验证，不关闭 S5，不批准 S8 上线。** 当前生产入口未创建 S3/S4 的 opt-in 请求；真实 B 激活、混合 binary 和回滚拒绝仍缺实现/运行证据。本文明确保留所有未完成验收，不能据部分测试通过将其改记为已完成。

## 1. 设计与版本边界

PR 基线：`bc84bd142600b745503e225cb89cb1526df8d35e`（`mo/main`）；初始验证基线为 `91639500503a301742e0c84c8aa6c524f1e70792`。交付前已 pull 上游新增的 #30828（COUNT 子查询排序消除），相关复验见第 6 节。本 PR 相对基线只增加测试及本文，不修改生产路由、协议、目录、权限策略、全局配置或缓存。

依据：

- [S1 完整设计与精确审批](CLAUDE_20260922-view-metadata-on-demand.md#s1-批准与-e1-勘误)。
- [S2 v2.1 修订](CLAUDE_20261005-view-s2-scope-revision.md)：不重新要求已撤销的全链路 128 MiB 逐对象证明。
- [S3 消费者](CLAUDE_20261018-view-s3-consumers.md)、[S4 消费者](CLAUDE_20261019-view-metadata-s4-consumers.md)、[S4 关闭性对账](CLAUDE_2026-10-20-view-metadata-s4-reconciliation.md)。
- [既有升级/恢复验证的范围与缺口](CLAUDE_VIEW_METADATA_LIFECYCLE_CONFORMANCE_29007.md)。
- [当前 RC 生命周期升级约束](20260929-rc-lifecycle-protocol.md)：新旧锁协议不能被假定可安全在线混跑，升级/回滚须遵守独立的停服、drain、成员替换与旧进程禁入要求。

本基线的实际生产调用搜索只找到 `BuildPlanWithViewSchema`、`NewViewMetadataRequest` 的定义/构造包装，没有默认 frontend 的生产接线。因此必须区分三种证据：

1. **opt-in**：测试显式创建请求、传递 context，并断言 provider 确实被调用。
2. **默认路径控制**：公共 MySQL 协议、BVT、同 binary 多 CN/重启。能证明现有行为，不能证明默认请求已采用 S2 的新所有权和 legacy 合同。
3. **兼容组件模型**：注入能力响应、复现 migration 事务或 admission 状态机。不是多个真实版本进程的兼容性证明。

`MORPCVersion100` 是既有 A 的 View 列描述边界；当前 `MORPCVersion126` 注释为 parser-derived I_S.VIEWS 定义。不能仅因版本号更大就把当前 binary 叫作已完成 B，也不能复用任意已有常量充当 `P_OD2`。

## 2. 判定规则和可见性

同一个定义身份、事务/statement snapshot、workspace overlay、创建环境及合法授权域，应导出同一 schema。比较类型/宽度/顺序/default/nullability 的语义，而不是不同表面编码的逐字相同。

- literal default `7` 在 View 描述中为 `7`，CTAS 按既有策略生成 `0`，这是正确差异。
- 新 statement 依法取得 DDL 后的 schema；已固定 snapshot 的旧 statement 不被迫读取之后提交的字段。描述器不得以读取最新目录替代其原解析域。
- RC 的推进属于外层 statement owner；请求不能跨推进后的 catalog stamp/执行代继续复用。当前新增 native 控制证明的是“固定旧 statement 与新 statement”的边界，**不是**完整公开 RC 多语句重试矩阵。
- 历史解析必须同时使用历史定义与来源身份。当前同名 source 的存在不能修补历史域。
- own-DDL 只在实际同一事务/workspace 的路径验证；公共 DDL 的隐式提交不能被测试改写成可 rollback 的假合同。
- raw `mo_catalog.mo_columns` 按 D03 保持物理快照。查询其旧 View 列不是当前逻辑 schema 的 freshness 证明；描述读不得写回使其“变新”。
- 在固定合法 snapshot 内，statement memo 命中不必重新读取每个基表；不能用一个未改变 snapshot/overlay 的假 resolver 突变，要求合法旧缓存读取最新 schema。每根授权与根身份解析仍必须执行，发生失败就不能回退已缓存的成功。

## 3. 合同到证据矩阵

“现有入口”表示源码中有相应断言，不自动代表本 head 已运行，也不表示该行的全部合同已闭合。运行终态单独见第 6 节。

| 合同 | 现有/新增入口与最小 oracle | 证据边界或剩余缺口 |
| --- | --- | --- |
| metadata-only、COPY、同名重建 | 新增 `testNativeViewMetadataDDLTransitions`：显式 INPLACE 改 default，显式 COPY 扩宽并加列，DROP/CREATE 同名 source；分别断言物理 ID 不变/改变，根 View ID 不变 | 同一真实目录 fixture；不以语句拼写推断实际 DDL 算法 |
| 同 statement 旧 snapshot | 新 native 子场景先完成 Describe，再在独立事务提交 DDL，再次 Describe 的完整列/来源/依赖仍等于旧结果 | 提交完成是确定性相位边界；无需 sleep；不声称并发所有阶段都覆盖 |
| 各表面一致 | 新 helper 在一个父事务中运行 opt-in SELECT LIMIT 0 与 CTAS 规划、DESC/SHOW/FULL、I_S SQL；独立期待宽度 20/40/60 与 default，断言 S4 授权回调非零 | SELECT/CTAS 新增证据是 typed plan，不是假装实际 CTAS 已执行；实际查询执行沿用原 native fixture |
| frozen star 与嵌套 | `source -> middle -> root`，创建时 `*` 固化；COPY 新增 `extra` 后 View 仍两列 | 两层嵌套，零行数据，不用大量 View 代替正确性 |
| 历史对象身份 | 新 native 历史 caller snapshot：同名重建之后依然使用旧宽度/default；原 `prepared_caller_snapshot` 覆盖 SELECT/CTAS/PREPARE 形状 | 尚不补齐“早于 4.0.10 的所有系统模板 + named snapshot + 权限撤销”组合 |
| restore source | 新 native 公共 snapshot/RESTORE TABLE 之后，源物理 ID 改变、View ID 不变，再用 opt-in 各消费者验证 | source-table restore，不是全账户/全目录恢复或 clone/PITR 全矩阵 |
| own-DDL/rollback | 原 `TestNativeViewSchemaCatalogOwnDDLHistoryAndSharedReads`：同事务 CREATE/DROP View、stamp/overlay、借用事务存活、fixture rollback 后对象不存在 | 扩展已有 fixture，不重复建第二套集群；公共隐式提交另按其合同 |
| 预热失败，不返回旧成功 | 新 `TestViewMetadataWarmMemoFailureRecovery`：memo-hit 与 cache-off 对照，根查找缺失/临时解析失败/撤销授权，nil result、原错误、非 bulk skip；随后合法请求成功 | 根查找失败不是 binder 确证的依赖缺失；原 `TestViewMetadataCandidateFailureProvenance` 保留该差别 |
| 取消/缺失竞争 | 新 `TestViewMetadataWarmMemoCancellationDuringRootRead`：预热后用 channel 卡在根读取，取消后 resolver 返回缺失；必须返回取消、nil result、非 bulk skip，资源归零，新请求可用 | barrier 确认相位，正常及 focused race；不靠重试或时间猜测 |
| 请求资源/父事务 | native 检查 CatalogReadStamp、visibility、WriteOffset 不变，关闭后保留字节为 0，父 generation 未关闭；UT 验证 Close/Release | 描述/规划不写目录，不把父事务当成子请求所有物 |
| 完整传递依赖与缓存 | 原 `TestViewSchemaPlansInvalidateOnTransitiveCatalogChanges`、S3 PREPARE tests、公开 prepared SHOW | 新请求与同名对象的身份验证；binary/cursor 的完整 opt-in 执行接线仍未完成 |
| 租户/订阅同名隔离 | 原 `TestNativeViewSchemaSubscriptionHistoryAndRoleDenial`：publisher 与 subscriber 同名库不同类型/行值、跨库/历史来源、拒绝角色、先过滤再推导 | fixture 的单 active-role authorizer 不能冒充全部生产 SHOW/DEFINER/INVOKER 策略 |
| publication withdraw/remap、安全链 | 现有默认 subscription/public privilege tests 与安全 owner 的 UT | 仍需真实 opt-in 下撤销/remap/recreate，以及当前 snapshot 访问资格与 definer/invoker 的组合证据 |
| binary 描述/cursor | 默认 frontend cursor 生命周期及默认真实 binary prepare 控制 | 仍缺 opt-in 的 COM_STMT_EXECUTE/reprepare/FETCH 完整客户端证据，FETCH 旧代与再次 EXECUTE 新代必须分别验证 |
| 两 CN/restart | 原 `TestViewDescriptionTwoCN`：两个真实 CN、源 restore、prepared SHOW、同目录/UUID 重启后读取 | 默认兼容路径、同 binary；不是 B 接线、多版本或 opt-in AP/TP/UNION placement 证明 |
| legacy definition | S2/S3 的 missing context、raw star、协议拒绝、历史 parser 规则与恢复 UT | 仍需支持版本真实备份样本的只读 inventory、目标恢复及拒绝/owner 修复流程 |
| all-old/mixed/all-new、迁移中断、回滚 | `TestColumnsUpgradeAdmission` 的 11 个能力/失败/幂等场景；CN/proxy admission 的 floor、旧 generation、ingress handoff tests | 都是组件证明；真实 L/A/B binary 序列及 R2 后旧 reader 拒绝为阻塞项 |

## 4. 共享保护与 refresh-only 状态

本交付不删除下列任何资产；该表用于防止 S8 把“View 前缀”误作删除依据。

| owner/资产 | 独立用途，须保留 | refresh-only 内容的退役条件 |
| --- | --- | --- |
| `pkg/sql/plan/persisted_ip.go`、表达式 destination checks | 持久表达式 authoring/read protocol、远端解码能力 | 与列 freshness 无关，不降低 floor |
| CN / HAKeeper / proxy admission 与 catalog metadata barrier | durable 能力、成员 incarnation、旧 UUID 重入、路由/ingress readiness、snapshot 解码 | 先证明新资格、drain 与旧 binary 禁入；不能靠停 refresh worker 宣称已满足 |
| catalog feature-registry / SNAPSHOT / account lifecycle 保护 | restore、tenant 生命周期、锁序及 owner-atomic/writer-fair 语义 | 只有不再有共享用户的内层 refresh 副作用可在 S8 单独退役 |
| compile refresh/recovery、revalidation 标记 | 旧兼容维护及尚存调用 | S8 才审计关闭 reverse fan-out、重验证、worker 状态；读取不依赖它们 |
| bootstrap 4.0.9→4.0.10、旧表和定义载荷 | 历史升级、备份恢复及仍支持的 reader/writer | S9 默认 retain；没有支持版本完整清单不能删表/字段 |

## 5. 上线/回滚决定与明确阻塞

**本 head 的决定为 NO-GO：保持默认兼容路径，不进行 B 激活，不退役 refresh runtime，不删 catalog。** 这是根据当前可执行接线和缺失证据作出的实施门禁判断，不声称已取得新的运维 rollout 审批。

| 场景 | 已批准要求 | 当前判定与 owner |
| --- | --- | --- |
| all-old L / 现有 A | 按各真实支持版本运行，不声称 v2 保证 | 历史版本产物及支持组合须冻结到 SHA；S5/运维 owner 补证据 |
| mixed L/A/B | 不得让局部新 CN 抢先切换共享定义；先满足所有协议的兼容条件 | 尚无 B 生产接线；禁止从 mock 或同 binary 拓扑推导支持；S5/S8 |
| all-B、未激活 R1 | 只读 inventory，兼容模式，旧进程重入重新鉴别 | B 能力/租户版本的实际分配与模式选择尚待 S8，不用 `100` 或任意当前版本代替 |
| R1→R2 | 先持久化资格/封旧入口并 drain，再提交租户语义版本；后者是租户切换点 | 需要实现接线及切换前后计划/会话复用验证；S8 实现，S5 复核 |
| 切换中断/restart | 资格不得回退；未提交租户事务回滚，已提交幂等继续；同 UUID 新进程重新准入 | 现有 admission 组件测试不等于 B 两提交点的故障演练；S5/S8 |
| R2 前退回旧 reader | 仅在全部 durable floor、catalog、其它生命周期协议都允许时 | 需支持 binary 实测，不承诺任意旧版可回滚 |
| R2 后退回 L/A | 服务入口拒绝，不降 floor；只能兼容 B 的已验证回退、forward-fix 或经运维批准的停服恢复 | 拒绝链当前缺 B 激活闭包，仍 BLOCKED；不能用 stale persisted columns 充当后备 |
| restore/clone/PITR | 定义/环境/owner/授权/未知字段保持，目标重新按合法 schema 与访问资格绑定 | source restore 有 focused 证明，其余完整对象/版本组合仍由 S5 补齐 |

这些缺口不是以“归 S8”从 S5 验收删除：S8 可以开发必要激活接线，但在 S5/S6 精确修订证据和 rollout/rollback review 完成前不得上线。若要改变该依赖/退出条件，须 owner 明确修订设计。

## 6. 本次验证记录

环境：darwin/arm64，Go 1.27.1（当前 `go.mod`），golangci-lint 2.14.0 / Go 1.27.1（与 Makefile 相符）。原生库由本 worktree `make -j8 cgo` 构建，测试经仓库 `mo-cgo-test`，短 TMPDIR 为测试独占目录。

### 6.1 初始基线运行与失败归因

- 原生库复用首次因 provenance 不匹配被拒绝，按 wrapper 要求本地重新构建并成功；未绕过校验或复用未知产物。
- 测试开发中纠正了两个 fixture 假设：`ALTER COLUMN SET DEFAULT` 实际走 COPY，改用显式且已验证身份的 INPLACE MODIFY；CREATE SNAPSHOT/RESTORE 属于 frontend，改走真实 MySQL 连接而非内部 SQLExecutor。这两次失败不是生产缺陷证据。
- 新 warm-memo 正常测试已通过；取消 barrier 的 race 首次测量 T=0.01s，B=30s，按上限取 N=100，100 次命名运行全部通过。
- 最终 native 两个命名测试及子场景均通过，包耗时 13.653s；新增跨消费者子组 0.33s。不新增集群，新增成本限于同 fixture 的小型 DDL/描述序列。
- `pkg/sql/plan` 与 `pkg/frontend` 全包 normal 分别 16.298s、22.365s，退出码均为 0。原有 3 个 skip（HNSW cosine_similarity、ProjectionBinder interval fixture、RuleCacheDoubleCheckLocking）不作为本次证据；新增测试没有 skip。
- 既有 `v4_0_10` 的 3 个升级测试（含 11 个 admission 子场景）、CN 的 read/authoring floor、final ingress handoff、higher-generation 撤销，以及 proxy 的 higher-generation 撤销均通过；这是组件模型而非真实 B 升级。
- integration 标签下两 owning package 的 `go vet` 和增量 golangci-lint 均退出 0，lint 为 `0 issues`；gofmt、`git diff --check` 通过。
- 默认公开控制首次因无关 `issues.test` 占用 TCP 10060，在 cluster-start 阶段退出 1，未执行 SQL 断言；该环境失败与产品证据分开保留，等待端口释放而未杀其它任务。释放后 `TestViewDescriptionPublicSQL`、`Privileges`、`Subscription` 分别 5.62s/0.19s/1.76s PASS；`TestViewDescriptionTwoCN` 7.54s PASS，两条命令均退出 0。后者包含同目录及 UUID 重启；仍只证明默认路径、同 binary。
- 不新增 BVT：本次无生产行为或 BVT 文件变化；新增 opt-in 证明通过现有 native 真实 SQL fixture，默认 mo-tester 不能激活该入口。不会把默认 BVT 的通过当作新入口证明。
- 无生产语句修改，修改生产代码覆盖率 ≥75% 的门禁不适用；不是声称全包覆盖率达到 75%。

### 6.2 更新到 PR 基线后的终态

上游唯一新增生产变更进入 planner 的 `createQuery`，所以重跑全部 planner-dependent 检查，没有直接沿用初始包/集成证据。以下均在 PR 基线加本交付工作区执行，命名选择非空、实际退出码为 0；这是本地证据，不是 CI 全绿声明。

| 检查 | 模式/选择 | 终态及耗时 |
| --- | --- | --- |
| 新增 warm-memo tests | normal，`^TestViewMetadataWarmMemo` | PASS，8 个含子场景命名结果，包 2.061s |
| 根读取取消 | `-race -count=100`，精确单测试 | PASS，100 个命名结果，包 4.791s |
| owning packages | normal，`-p=1 -count=1`，plan/frontend 全包 | PASS，15.542s / 18.382s；仍仅上述 3 个已有 skip |
| native 两个测试 | `integration`，catalog/DDL 与 subscription/role | PASS，96 个含子场景命名结果，包 13.521s；新增子组 0.29s |
| 默认公共控制 | PublicSQL / Privileges / Subscription | PASS，5.52s / 0.18s / 1.68s，包 9.655s |
| 默认双 CN | `TestViewDescriptionTwoCN`，同 binary restore/restart | PASS，测试 6.55s，包 10.174s |
| 增量静态 | 两 owning package，integration 标签；lint 相对 PR 基线 | vet/lint 退出 0，lint `0 issues`；gofmt/diff-check 通过 |
| admission 组件 | 复用第 6.1 节 18 个含子场景命名 PASS | 对应实现、mock SQL fixture、配置及工具链未变；只调用状态机/mock executor，不经过上游新增 COUNT 规划规则 |

测试源码未改变，原生源码/Makefile/依赖及 wrapper 也与初始验证一致。最终提交仅封装下列源码与文档，不改变验证输入。三个 Go 文件 SHA256 为：

| 文件 | 验证源码 SHA256 |
| --- | --- |
| `pkg/frontend/view_metadata_conformance_test.go` | `bc8ab5a430173a3c231ba596467fca93cf949c217862cd9762409077705d9b64` |
| `pkg/frontend/view_schema_native_test.go` | `204b948f790548d17cb4b24eefaf0312a2f914b6c77fb3a2bc4bd7d2cff5df1d` |
| `pkg/sql/plan/view_metadata_request_test.go` | `5f17c4dd349ece04e709085040ba739eeaaa5605b65334f60f20f8b20161571f` |

本地命令、日志、退出码和指纹记录位于未提交的 `CLAUDE_validation/`。初始记录保留 `native-final.log`、`owning-normal.log`、`warm-memo-race-stress.json`、`existing-admission.log` 及 `CLAUDE_evidence_manifest.json`；PR 基线复验为 `pr-{focused,race,owning,native,public,two-cn,vet,lint}.log/.command/.exit` 及 `CLAUDE_PR_EVIDENCE.json`。提交后可按文件语义未变复用；S8 前仍须链接最终目标修订的完整必需证据。

复现入口（每项须检查非空命名测试和实际退出码）：

```sh
export GOTOOLCHAIN=go1.27.1
# 使用已建立的测试独占短 TMPDIR；CGo 产物必须通过 wrapper provenance 检查。
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=180s \
  -run '^TestViewMetadataWarmMemo' ./pkg/sql/plan
.agents/skills/mo-dev/scripts/mo-cgo-test -race -count=100 -timeout=180s \
  -run '^TestViewMetadataWarmMemoCancellationDuringRootRead$' ./pkg/sql/plan
.agents/skills/mo-dev/scripts/mo-cgo-test -tags integration -count=1 -timeout=600s \
  -run '^TestNativeViewSchema(CatalogOwnDDLHistoryAndSharedReads|SubscriptionHistoryAndRoleDenial)$' ./pkg/frontend
.agents/skills/mo-dev/scripts/mo-cgo-test -p=1 -count=1 -timeout=600s ./pkg/sql/plan ./pkg/frontend
```

## 7. 自审范围与资源闭包

| 闭包 | Q1 所有权 | Q2 终结 | Q3 上界 |
| --- | --- | --- | --- |
| 新取消 UT | 测试拥有 goroutine/request，结果在 worker 发布完成前 Release；provider 只借 generation | `entered -> cancel -> ctx.Done -> resolver 返回 -> done -> Close`；失败清理同样 cancel，无 sleep 同步 | 一个 worker、两个 barrier、两层 View，无后台状态 |
| native DDL 子场景 | 复用原单 CN fixture；对象/快照获取后立即登记清理；每次 reader 由原 helper rollback/free | 独立 DDL 同步提交后才下一相位；SQL/fixture context 有界；失败不继续依赖其状态的 DDL 链 | 零数据行、一个 source、两个 View、一个 snapshot，不新增集群 |
| 派生结果与规划 | Describe helper 释放 handle；S3 wrapper 自行 Close；S4 在全部消费者退出后 Close | 不关闭父事务；失败由 defer 清理；旧请求终结后才新请求 | 保留字节归零，parent generation 仍打开 |

风险：新单元测试 R1/R3（局部确定性并发），native 跨 SQL/事务测试 R2/R3，文档 R0。生产协议、部署和热路径性能无变更；不需要新 benchmark 或整个仓库 race。最终自审已读取全部 4 个交付文件的新增/修改 hunk，核对测试选择非空、正常/race/static 终态、fixture 清理和源码指纹；本次测试/文档闭包无未处理代码 blocker。TODO、日志、构建产物不交付，没有修改生产路径或 issue 状态。结论为：**本次部分测试交付 PASS；#29440 完整验收 BLOCKED，S8 rollout NO-GO**。第 3/5 节缺口仍须完成，不以文档或本轮通过替代。
