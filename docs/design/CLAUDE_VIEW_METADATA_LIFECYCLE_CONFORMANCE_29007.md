# View 元数据：迁移幂等性、恢复与重启验证

所属：#29007 / #26227。研究基线：`466eb8ff4f65752ad25d7e19274770459f5ef934`。
本记录描述当前按需实现的修复与证据，不是 v2 激活批准，也不表示 #29007 的全部历史验收已完成。

## 1. 范围与架构依据

#29139 已合入按需描述器。对同一可见快照和绑定上下文，当前 View 列来自语义定义的绑定，而非持久派生列或恢复任务的完成状态。现有设计见 [按需设计](CLAUDE_20260922-view-metadata-on-demand.md)。

#29007 的旧 `required generation → recovery COMPLETE → authority reopened` 序列不再是本任务的实现目标。用户在本次任务中明确同意按现有按需架构调查升级、恢复和兼容缺口，不重新引入旧恢复机制。

完整 v2 合同及其独立审批记录在 [#29456 已审阅精确修订](https://github.com/ck89119/matrixone/blob/371e3c1761dab22dff95d0e830caab31aa46eeeb/docs/design/CLAUDE_20260922-view-metadata-on-demand.md)。该设计区分现有 A（protocol 100 / 4.0.10）与未来 B；本次不启动 B，不接管 #29437–#29444 的全部消费者迁移、性能预算或运行时退役。

## 2. 已证实的缺陷与最小修复

`v4_0_10` 使用持久 `rel_createsql == InformationSchemaColumnsDDL` 判定迁移已完成。这是精确 readiness 检查，不能以“包含新函数名”代替。

旧模板的私有 `__mo_visible_subscription_views` CTE 使用 `SELECT mt.*`。真实 CREATE VIEW 会经 `stableViewSQLWithExpandedStars` 固化该投影并格式化整个 CREATE SQL。因此迁移成功后写入的 SQL 与迁移器自己的模板不同，重放 handler 会再次执行 DROP/CREATE；原来的内存测试直接返回模板字符串，没有覆盖这一点。

真实存储回归先验证 CREATE 失败时 DROP 被回滚，再运行成功迁移；在修复前，持久定义精确相等断言失败。修复只将私有候选 CTE 改为显式投影下游使用的八个字段，保留原授权 predicate、APPLY、输出列及协议门禁。planner 冻结用户 View star 的规则不变。

### 兼容边界

- 不改变公共 I_S 字段、类型或权限规则，不新增协议、状态、锁、后台任务或缓存。
- 不放松大小写兼容查找或精确定义判定。
- 旧 4.0.10 模板仍语义可用，已完成升级的租户无需仅因 SQL 表示不同强制重迁移；这不是新能力激活，故不新增版本/offset。
- 原迁移被重放时，旧表示会更新一次；新模板创建成功后精确匹配，后续重放不再 DDL。
- 不移除共享 admission、持久表达式 floor、snapshot/账户锁或兼容 catalog 表。

## 3. 变更与证据地图

| 闭包 | 所有者 / 消费者 | 风险与测试 |
| --- | --- | --- |
| 私有候选显式投影 | sysview → bootstrap / frontend / subscription 行源 | R2；sysview 所属包、真实订阅 SQL、三处 SHOW CREATE 结果 |
| 精确 readiness 与能力检查 | v4_0_10 → UpgradeEntry | 11 个确定性内存场景：all-old、mixed、all-new、空响应/RPC 错误、缺失目录、大小写、非精确定义、DROP/CREATE 错误、幂等 |
| 真实迁移事务 | 既有 Subscription fixture / SQLExecutor | V58 → 注入 CREATE 失败 → 精确恢复 V58 → 成功提交 → 在禁止 CREATE 的 executor 下重放成功 |
| 源表恢复、跨 CN、重启 | 既有独立 TwoCN fixture | 一表一行、直接和嵌套 View；两 CN 的 DESC/I_S/SELECT、CTAS、远端 prepared SHOW；物理源身份改变、View 身份不变；保留目录及 UUID 的整群停启 |
| 公共 SQL 回归 | 既有 view BVT / mo-tester | 第二 session prepared SHOW 在源宽度 180→restore 120 后重绑定；I_S/CTAS 同为 120；snapshot 和数据库清理 |

BVT 的结果是独立、明确的宽度/列定义预期；CTAS 是另一消费者，不以描述器生成期望值。原有权限、订阅及历史快照场景保留，不扩展 warning、UDF 或 charset/collation 兼容规则。

测试成本：复用原双 CN fixture，不再建第二套集群；新增 restore/restart 后该测试 normal 从约 17 秒增至约 25 秒。整群重启用于实际目录持久化边界，不能由 mock 代替；这些数字不是产品性能基准。

## 4. 验证记录

执行环境：macOS arm64，Go 1.26.4（go.mod），原生库由当前 worktree 的 `make -j8 cgo` 构建，CGo 测试使用仓库 `mo-cgo-test`。静态检查使用 golangci-lint 2.6.2 / Go 1.26.4，与 CI 工具版本一致。

2026-09-29 执行结果（全部为最终代码语义；后续仅更新文档）：

| 检查 | 终态 / 说明 |
| --- | --- |
| 三个所属包 normal | `sysview`、`v4_0_10`、`tests/upgrade` 均 PASS；最终测试 helper 清理后再跑整个 upgrade 包，50.377 秒 PASS |
| 精确回归选择 | 11 个 `TestColumnsUpgradeAdmission` 子场景、`TestViewDescriptionSubscription`（最终 1.42 秒）、`TestViewDescriptionTwoCN`（最终 normal 20.65 秒）均实际运行并 PASS |
| 聚焦 race | `^TestViewDescriptionTwoCN$`，测量 T=28.57 秒，B=30 秒，N=1；最终 helper 下单独 `-race -count=1`，命名测试 30.28 秒 PASS，进程退出码 0 |
| 修改生产语句覆盖 | `predefined.go:302–304` 是一个字符串拼接语句，所在 `282.50–310.2` 块的 10 个语句计数为 1；改动语句 1/1，即 100%，满足 ≥75%。sysview 全包为 69.0%，不混淆为全包达到 75% |
| 静态检查 | Go 1.26.4 gofmt 无输出；增量 vet 退出码 0；golangci-lint 2.6.2：0 issues |
| view BVT | mo-tester genrs 后正常比较 3 次，每次 42/42，零失败 |
| SHOW BVT | `dml/show/show.test` 正常比较 2 次，每次 189/189，零失败 |
| account BVT | `zz_accesscontrol/account_restricted.sql` 正常比较 2 次，每次 125 成功、1 个既有 ignored、零失败 |
| teardown | view 两次复跑后立即检查数据库及 snapshot 均为 0；账户用例后相关账户为 0；独占服务收到 TERM 后退出 |

upgrade 包中四个既有框架测试（`TestUpgradeFrameworkInit`、`TestUpgradeFrameworkInitWithHighVersion`、`TestUpgrade`、`TestUpgradeCrossVersions`）仍由原测试跳过；不将其计为本次升级矩阵的有效证据。新修改的命名测试均未跳过。生产没有新增共享状态或生命周期实现；两 CN 测试使用独立集群，race 针对新增停启步骤，不宣称全包 race。

三处 SHOW CREATE 的 `.result` 只保留当前 COLUMNS 模板及返回字符串长度的生成变化，丢弃整文件 genrs 的无关漂移；完整原用例随后正常比较通过。view `.result` 保留 mo-tester 原生列分隔与末列空格，不手工改写预期。

本地日志保留在工作区 `.claude-validation/`（不提交）：`owning-packages.json`、`upgrade-final.json`、`two-cn-race-final.json`、`sysview.cover`、`vet-delivery.log`、`lint-delivery.log`、`bvt-*-compare*.log` 与 `teardown-*.log`。

常用复现入口：

```sh
export GOTOOLCHAIN=go1.26.4
# macOS 使用短的、测试独占的 TMPDIR，避免 Unix socket 路径超长。
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=600s \
  ./pkg/util/sysview ./pkg/bootstrap/versions/v4_0_10 ./pkg/tests/upgrade
.agents/skills/mo-dev/scripts/mo-cgo-test -race -count=1 -timeout=600s \
  -run '^TestViewDescriptionTwoCN$' ./pkg/tests/upgrade
```

正常模式与 race 都必须看到命名测试的终态；旧模板运行在新二进制上的测试，及注入的版本响应，均不能标成真实混合二进制验证。

## 5. 当前版本的上线、观测与回滚边界

1. 核对所有实际服务 CN 的 binary/catalog 版本，而非仅看升级执行者；保留现有 protocol 100 检查。无法得到能力响应或仍有旧 CN 时，升级不得先执行 DROP。
2. 使用既有升级框架按 4.0.9 → 4.0.10 顺序提交租户迁移，不手工更新版本行为“完成”。失败由原事务回滚，重试原 handler；不启动派生元数据恢复 worker。
3. 在同一可见快照下检查 DESC、I_S、SELECT 与 CTAS。直接 raw `mo_columns` 是兼容存储，不能作为当前 View schema 或恢复完成的证明。
4. 观测既有升级日志的版本、租户、entry 与错误；对迁移完成后仍反复 DROP/CREATE 的现象，检查持久定义与实际部署模板，不仅搜索函数名。复现升级失败时同时保留事务错误和迁移前后目录定义。
5. 恢复检查以恢复语句提交为界，在另一 CN 发起新语句并验证源对象身份及 schema；合法旧 snapshot 不要求看到恢复后的状态。
6. 重启须保持原目录并重新通过已有 admission。清数据后的启动不是 restart 证据；进程 UUID 相同也不是沿用旧 incarnation 资格的理由。
7. 本修复不新增自动降级能力。已发布的新模板含旧 binary 不理解的私有行源，不能仅因旧派生列仍存在就允许旧 reader 重入。任何降级须符合既有目录/表达式协议 floor，并有实际支持 binary 的验证；没有证据时采取 forward-fix，不降低 floor 或手改版本号。

不为此次局部修复新增指标。使用现有升级日志、SQL 可见结果、服务 readiness 和测试 teardown 作为证据；不将租户/View/SQL 文本引入高基数 metric label。

## 6. 原验收逐项归属及明确缺口

| #29007 原要求 | 本次结论 |
| --- | --- |
| 接通 durable recovery / authority | 旧方案已被按需推导替代；本次不实现、不启用 |
| 目录就绪和能力谓词 | 修复现有 A 迁移精确 readiness 的幂等性；版本响应 UT 不证明真实成员切换或 ingress drain |
| 依赖 DDL 原子重写全部 View schema | 按需读取替代持久重写；本次覆盖 ALTER/restore 后公开结果，不更改 raw catalog 语义 |
| 双 CN 共享目录恢复 | 新增源表 restore 提交后的直接/嵌套描述、prepared SHOW 与源/View 身份验证；不是完整 cluster/account restore、PITR/clone 矩阵 |
| 立即重启、同 UUID 替换 | 同二进制整群停启，验证相同 UUID、新 service 实例及恢复后 schema；不声称覆盖未退出旧实例/迟到 heartbeat 的所有竞争 |
| all-old / partial / all-new / rollback denial | 本次只有组件能力矩阵；真实 L/A/B binary 序列、B 禁入与回滚拒绝仍归 #29440/#29443，不能关闭为已验证 |
| 公开 BVT、normal/race、覆盖率、teardown | 见第 4 节精确执行记录；不以历史 #29139 结果冒充本次运行 |
| 最终语义 checkpoint / conformance head 审批 | 本记录及实现 PR 待 review；不冒充 GitHub 审批或 v2 发布批准 |

本 PR 应关联 #29007 而非自动关闭它，也不关闭 #26227。完整 rollout 仍受 #29433 系列消费者迁移、兼容和性能证据门禁约束。

## 7. 自审边界

生产补丁是局部 SQL 模板幂等性修复，不改变架构，适用既有按需设计；不需要另建一个恢复/激活设计。

- Q1：查询在 helper 内获取 rows，立即 defer Close；prepared statement 在局部作用域关闭；数据库句柄先于测试集群关闭。重启失败不通过已关闭句柄做目录清理，隔离集群仍有最终 Close owner。
- Q2：SQL 受 fixture context 限制，所有恢复/重启步骤串行；未新增 goroutine、锁或等待协议。测试整体 timeout 是 hang guard，不用 sleep 作为正确性同步。
- Q3：一表一行、两个 View、有限断言与一次重启，不增加产品缓存/队列/持久状态。
- 平台与交付：不改生成代码；BVT `.result` 必须经 mo-tester 生成及比较；本地 TODO、日志、二进制、native 产物不提交。

最终自审：针对上述有界修复 PASS，无未关闭阻塞项；不是对 #29007 整体或 B/v2 rollout 的批准。

- 范围：交付前重新 fetch `mo/main`，仍为研究基线 `466eb8ff4f65752ad25d7e19274770459f5ef934`；已核对全部四个 Go 文件、SQL、三个生成结果及本文。基线没有漂移；本地 TODO/构建产物/日志不交付。
- 正确性/消费者/兼容安全：显式投影包含全部下游输入；授权条件仍在 APPLY 之前，无权限、tenant、错误语义或公共 schema 的变更。读路径、真实迁移事务、生成 SHOW CREATE 和跨 CN 消费者均有对应证据。
- 状态/活性/资源：生产不新增状态、等待或清理 owner；新增测试的事务回滚/重放及停启属于验证闭包，Q1–Q3 如上。未变动的 production admission/generation 竞争不冒充本次已覆盖。
- 规模/性能：仅改变有限私有投影和 SQL 表示，不增加递归描述、查询次数或后台工作；无物质性的热路径或容量契约变化，不据测试耗时宣称性能优化。
- 平台/测试架构：仅 macOS arm64 本地验证，无 Linux/实际混合 binary 运行声明；复用既有 fixture，具备独立公开 oracle、确定性 CREATE 故障与实际 teardown，未通过重试/睡眠等待制造通过。
- 已接受边界：用户批准当前按需架构的升级/恢复缺口修复；完整混合版本及 B 激活属于 #29440/#29443，本文第 6 节保留所有未完成验收，不关闭父任务。
