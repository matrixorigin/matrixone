# #27287/#27288/#27289 租户升级失败链路修复计划

- 状态：实现、本地代码验证、真实数据逐级升级、account BVT 和 DDL BVT 完成；subagent 独立审阅的 blocker/high 已关闭
- Tracking issues：
  - [#27287](https://github.com/matrixorigin/matrixone/issues/27287) tenant upgrade 失败后无退避热循环
  - [#27288](https://github.com/matrixorigin/matrixone/issues/27288) `TABLE_CONSTRAINTS` 升级未覆盖历史 base table 状态
  - [#27289](https://github.com/matrixorigin/matrixone/issues/27289) `DROP VIEW IF EXISTS` 遇到 base table 错误
- 证据基线：`origin/main` at `548c4122f0f8005a6cfefda23dc88e34645ba7f8`
- 4.2 分支基线：`origin/4.2-dev` at `aaac5956fdbcee0d43aed91c150fe3f2c05b90c1`
- PR 实现基线：`origin/main` at `294d6361b03ecccc9e0172105a09a9412c3ba27d`
- 审阅：subagent 两轮只读审阅；首轮 2 个 blocker 已修正，复审无 blocker/high
- 最后更新：2026-08-19

## 1. 结论

这不是三个互相独立的 S-1，而是一条事故链上的三个缺陷：

```text
历史租户的 information_schema.TABLE_CONSTRAINTS 仍是 base table
  -> v4_0_5 新升级项执行 DROP VIEW IF EXISTS
  -> 类型不匹配返回 ErrBadView，升级事务回滚（#27288/#27289）
  -> tenant upgrade worker 在错误路径立即重试（#27287）
  -> 多 CN × 每 CN 4 worker 持续建事务、抢锁、回滚和刷日志
  -> 升级停止，节点内存耗尽，CN 被 OOM kill
```

修复拆成三个彼此独立、可分别合入和回滚的变更：

1. **重试安全边界**：一次 tenant upgrade round 出错后立即结束本轮，回到已有 10 秒外层定时器；健康状态下仍连续 drain 待升级租户。
2. **升级项收敛性**：让已确认存在历史类型迁移的 information_schema 对象通过 qualified `DROP TABLE`、qualified `DROP VIEW` 和 canonical `CREATE VIEW`，从 absent/base table/stale view/target view 四种状态都收敛到目标 view，并保持重复执行幂等。
3. **DDL 兼容性**：`DROP VIEW IF EXISTS` 遇到同名 base table 时返回 no-op plan，不删除 table；不带 `IF EXISTS` 时继续返回 `ErrBadView`。

发布顺序必须是重试安全边界优先、升级项修复随后。DDL 兼容修复可以独立发布，且不能替代升级项修复：即使 `DROP VIEW IF EXISTS` 跳过 base table，后续普通 `CREATE VIEW` 仍会与同名 table 冲突。

## 2. 历史归因与影响边界

### 2.1 新增触发器

`6227c18da7`（PR #26298，2026-07-30）在 4.2 开发周期给 `v4_0_5` 新增了四个 tenant view upgrade，其中包括 `TABLE_CONSTRAINTS`。该提交存在于 `origin/4.2-dev`，不存在于 `origin/4.1-dev`/`v4.1.4`。

### 2.2 存量放大器

- tenant upgrade 的错误热循环从 `6e6302c84c`（PR #14302，2024-02-18）引入升级框架时已经存在；默认 10 秒检查周期和每 CN 4 worker 同样来自该提交。
- `DROP VIEW IF EXISTS` 的类型不匹配分支至少从 2022 年已经无条件返回 `ErrBadView`。
- `8caa563b58`（PR #21443，2025-02-26）把 `TABLE_CONSTRAINTS` 的 table-to-view 转换放在 cluster upgrade，未覆盖普通历史租户，留下 base table 状态。

因此 #27288 是 4.2-dev 新增的直接回归；#27287 和 #27289 是被该回归激活的存量缺陷。

## 3. 正确性契约

### 3.1 Tenant upgrade 调度不变量（#27287）

一次 worker round 中：

1. `fn() == (true, nil)` 表示成功处理了一批且仍可能有待处理租户，worker 应立即继续，不能给健康升级路径增加固定延迟。
2. `fn() == (false, nil)` 表示本轮已经 drain 完成，worker 应退出内层 round，回到外层检查周期。
3. `fn() == (_, err)` 表示本轮不能安全继续；worker 必须退出内层 round，不能在没有 timer/backoff 边界的情况下再次调用 `fn()`。
4. context 取消后不得启动新一轮；已经进入 `fn()` 的取消仍由现有 transaction/context 链路负责。
5. 无论错误是否永久存在，单个 worker 的失败尝试频率都受 `checkUpgradeTenantDuration` 上界约束。

最小否定见证：`fn()` 每次返回错误时，在一个 `checkUpgradeTenantDuration` 窗口内被同一 worker 调用两次或更多次。

### 3.2 View upgrade 收敛不变量（#27288）

对确实有历史 table/view 类型迁移的目标对象，升级成功返回后必须满足：对象是目标定义的 view。升级必须从下列四种输入状态收敛：

| 输入状态 | `PreSql` | `UpgSql` | `PostSql`/结果 |
|---|---|---|---|
| absent | `DROP TABLE IF EXISTS` no-op | `DROP VIEW IF EXISTS` no-op | canonical `CREATE VIEW` 创建目标 view |
| base table | 删除 base table | `DROP VIEW IF EXISTS` no-op | canonical `CREATE VIEW` 转成目标 view |
| stale view | `DROP TABLE IF EXISTS` no-op | 删除 stale view | canonical `CREATE VIEW` 创建目标 view |
| target view | `CheckFunc` 返回 true | 不执行 | 不执行，保持目标 view |

第五个状态是再次执行：第一次成功后的第二次执行必须由 `CheckFunc` 跳过，不能删除或重建对象。

升级项只能对有历史证据的对象采用“允许删除同名 base table”的策略。不能把 `v4_0_5` 的通用 `upgradeInformationSchemaView` 无条件改成删除任意同名 table，否则会扩大 destructive scope。

### 3.3 `DROP VIEW IF EXISTS` 不变量（#27289）

| 对象状态 | `DROP VIEW name` | `DROP VIEW IF EXISTS name` |
|---|---|---|
| absent | `ErrBadView` | 成功 no-op |
| base table | `ErrBadView` | 成功 no-op，table 保留 |
| view | 删除 view | 删除 view |

最小否定见证：创建 base table 后执行 `DROP VIEW IF EXISTS` 返回错误，或者语句成功但 base table 被删除。

公共路径是 SQL DDL；黑盒 oracle 是错误类别和语句后的对象类型/数据仍然存在。typed planner unit test 只证明 no-op plan 构造，不能代替 SQL 级副作用验证。

## 4. 修复设计

### 4.1 #27287：错误结束当前 drain round

涉及文件：

- `pkg/bootstrap/service_upgrade_tenant.go`
- `pkg/bootstrap/service_upgrade_tenant_test.go`

将当前组合条件拆成三个显式状态：

```go
for {
    select {
    case <-ctx.Done():
        return
    default:
    }

    hasUpgradeTenants, err := fn()
    if err != nil {
        return
    }
    if hasUpgradeTenants {
        continue
    }
    return
}
```

为使状态转换可被无时间竞争地测试，可提取一个仅负责单轮 drain 的窄 helper，例如：

```go
func drainUpgradeTenants(ctx context.Context, fn func() (bool, error))
```

helper 不拥有 timer、goroutine、失败计数或日志；这些继续由 `asyncUpgradeTenantTask` 和现有 `fn` 所有。它在每次调用 `fn()` 前以 non-blocking select 检查 `ctx.Done()`，从而满足“第一次成功返回 `hasUpgradeTenants=true` 后发生 cancel 时不再进入第二次调用”的契约。除此之外它只表达上述三态协议，避免用毫秒 sleep 或概率性计数测试调度行为。

本次不新增指数退避器、jitter、配置项、后台状态机或高基数 metrics。错误后的固定 10 秒边界复用现有 `checkUpgradeTenantDuration`，已经把单 worker 的无界热循环降为有界周期重试。指数退避、聚合 metrics 和同类日志限流若仍需要，应作为独立可观测性增强，不阻塞 S-1 止血。

性能影响：健康路径只增加显式分支，不增加 allocation、timer、锁、atomic 或日志；错误路径显著减少事务、锁请求和日志。

### 4.2 #27288：只对确认对象使用全状态收敛策略

涉及文件：

- `pkg/bootstrap/versions/v4_0_5/tenant_upgrade_list.go`
- `pkg/bootstrap/versions/v4_0_5/upgrade_test.go`
- `pkg/bootstrap/versions/v4_0_6/tenant_upgrade_list.go`
- `pkg/bootstrap/versions/v4_0_6/upgrade_test.go`

`UpgradeEntry.Upgrade` 已支持 `PreSql -> UpgSql -> PostSql` 三段在同一 transaction 中顺序执行。`v4_0_5.TABLE_CONSTRAINTS` 使用：

```text
PreSql: DROP TABLE IF EXISTS information_schema.TABLE_CONSTRAINTS
UpgSql: DROP VIEW IF EXISTS information_schema.TABLE_CONSTRAINTS
PostSql: canonical CREATE VIEW information_schema.TABLE_CONSTRAINTS AS ...
```

实现应保留现有 target-definition `CheckFunc`。不要改变 entry 数量、顺序、handler version 或 version offset：卡在该 entry 的租户会重试修正后的 SQL；已经完成且定义匹配的租户继续跳过。

必须使用 fully-qualified SQL，不能依赖 transaction 当前默认库。不能用 `CREATE OR REPLACE VIEW` 代替上述三段：当前 compile replace 路径的内部 drop 没有 schema qualification，并且 `CheckViewDefinition` 会把持久化的 `CREATE OR REPLACE VIEW ...` 与 canonical `CREATE VIEW ...` 常量作精确比较，导致重复执行无法跳过。

不要把所有 `TABLES/COLUMNS/STATISTICS` entry 一并改为先删 table。推荐为 `TABLE_CONSTRAINTS` 使用命名明确的窄构造函数，或者直接构造该 entry。最终创建必须直接复用 canonical `sysview.InformationSchemaTableConstraintsDDL`，保证 `rel_createsql` 与 `CheckFunc` 的比较值一致。

同一 PR 审计 `v4_0_6` 的 information_schema view entry：

- `KEY_COLUMN_USAGE` 当前是 `DROP TABLE IF EXISTS + CREATE VIEW`，不能覆盖 stale view；改为 qualified `DROP TABLE IF EXISTS`、qualified `DROP VIEW IF EXISTS`、canonical `CREATE VIEW` 三段式。
- `REFERENTIAL_CONSTRAINTS`、`COLUMNS` 当前是 `DROP VIEW IF EXISTS + CREATE VIEW`。只有发现历史 base-table 可达证据时才允许加入删除 table 的步骤；否则保留非破坏性的 view-only 路径。
- 在代码或测试中记录每个允许删除 base table 的对象对应的历史来源，避免以后把该策略误用为通用 view refresh。

### 4.3 #27289：类型不匹配时构造明确 no-op plan

涉及文件：

- `pkg/sql/plan/build_ddl.go`
- `pkg/sql/plan/build_ddl_test.go`
- `test/distributed/cases/ddl/drop_if_exists.sql`
- `test/distributed/cases/ddl/drop_if_exists.result`

在 `buildDropView` 的 `tableDef.ViewSql == nil` 分支：

1. 不带 `IF EXISTS`：保持当前 `ErrBadView`。
2. 带 `IF EXISTS`：构造明确 no-op，与 `buildDropTableSingle` 遇到同名 view 的镜像行为一致；设置 `dropTable.Table = ""` 并保持 `dropTable.IsView = true`。`TableDef` 当前本来就没有被 `buildDropView` 填充，真正的 executor no-op 契约是空 `Table`。
3. subscription database 的限制只对真实 view drop 生效；类型不匹配且 `IF EXISTS` 的 no-op 不应进入删除路径。

本次不实现 MySQL NOTE/warning，不改变 multi-view 当前“不支持多个名称”的行为，也不修改 `DROP TABLE IF EXISTS <view>` 的既有语义。

## 5. 测试矩阵

### 5.1 调度协议

| case | `fn()` 序列 | oracle | 未修复行为 | 修复后行为 |
|---|---|---|---|---|
| healthy drain | `(true,nil)`, `(true,nil)`, `(false,nil)` | 单轮调用 3 次后返回 | 通过 | 通过 |
| empty round | `(false,nil)` | 调用 1 次后返回 | 通过 | 通过 |
| immediate error | `(false,err)` | 调用 1 次后返回 | 无界调用 | 返回当前 round |
| error with stale has flag | `(true,err)` | 错误优先，调用 1 次 | 无界调用 | 返回当前 round |
| error then next timer round succeeds | round 1 error；round 2 `(false,nil)` | 两轮之间经过可控 timer 边界 | 热循环 | 第二轮成功 |
| cancellation while waiting | timer 等待时 cancel | bounded return，无新事务 | 通过 | 通过 |
| cancellation during drain | 第一次返回 `(true,nil)` 后 cancel | 不发生第二次 `fn()` 调用 | 可能额外调用 | helper 在调用前退出 |

纯 helper 测试不得使用 sleep。外层 timer 集成测试使用短但明确的注入 duration、channel/atomic 同步和 bounded context；不以“运行一段时间后次数大概不多”作为 oracle。

### 5.2 Upgrade entry 状态

| entry | 初始状态 | oracle |
|---|---|---|
| `v4_0_5.TABLE_CONSTRAINTS` | absent | 最终是目标 view |
| 同上 | historical base table | table 被转换为目标 view |
| 同上 | stale view | view 定义被替换 |
| 同上 | target view | 不执行 PreSql/UpgSql |
| 同上 | 第一次升级后的状态 | 第二次执行无 DDL，状态不变 |
| `v4_0_6.KEY_COLUMN_USAGE` | stale view | 不发生 name collision，最终定义匹配 |
| 同上 | base table/absent/target view | 分别收敛/创建/跳过 |

package unit test 负责检查 entry 的 fully-qualified SQL、执行顺序、version offset 不变，以及 `UpgradeEntry.Upgrade` 的 `PreSql -> UpgSql -> PostSql` 序列和第二次执行跳过。DDL BVT 独立验证两个关键执行语义：`DROP VIEW IF EXISTS` 遇到 base table 时保留 table/data，以及 `DROP TABLE IF EXISTS` 遇到 view 时保留 view/data。两组证据合在一起覆盖升级 entry 的三段式协议，但发布前仍应在升级环境执行第 7.2 节的端到端验收。

### 5.3 DDL 行为

| 初始状态 | SQL | 黑盒 oracle | 白盒 oracle |
|---|---|---|---|
| absent | `DROP VIEW v` | `ErrBadView` | planner 返回错误 |
| absent | `DROP VIEW IF EXISTS v` | 成功 | 可安全执行的 `IfExists` plan；不要求 `Table == ""` |
| base table with row | `DROP VIEW v` | `ErrBadView`，row 保留 | planner 返回错误 |
| base table with row | `DROP VIEW IF EXISTS v` | 成功，table 和 row 保留 | `Table == ""`、`IsView == true` |
| view | `DROP VIEW v` | view 消失 | 正常 drop-view plan |
| view | `DROP VIEW IF EXISTS v` | view 消失 | 正常 drop-view plan |

BVT 用 `SHOW TABLES` 加 `SELECT` 证明 base table 及数据保留；不能只断言 SQL 没报错。planner unit test 明确断言 base-table mismatch no-op 的 `Table == ""`；BVT 中 table 和 row 均保留是独立的 executor 副作用 oracle，因此不再增加重复的 compile-layer unit test。

## 6. 实施与提交拆分

### PR A：#27287 retry safety

- 只改 `pkg/bootstrap` 调度与测试。
- 不依赖另外两个 PR，可最先合入和 backport。
- commit/PR 说明量化修复前后的单 worker 最大错误尝试频率。

### PR B：#27288 convergent view upgrades

- 修复 `v4_0_5.TABLE_CONSTRAINTS`。
- 同时关闭 `v4_0_6.KEY_COLUMN_USAGE` 已确认的同类状态缺口并完成其他 view entry 审计。
- 不依赖 #27289；升级 SQL 自身必须能处理 base table。
- 不改变 entry 数量、顺序或 offset。
- 修复覆盖尚未完成当前 handler/offset 的 tenant；已经完成相同 version/offset 的 tenant 不会仅因 SQL 内容变化自动重跑。本事故中已完成者已经是 target view，因此无需补跑；发布前必须验证这一前提。

### PR C：#27289 `DROP VIEW IF EXISTS`

- planner 修复、unit test 和 DDL BVT。
- 独立兼容性修复，不把它声明为 #27288 的完整解决方案。

每个 PR 先合入 `main`，再按发布负责人确认的目标 cherry-pick 到 `4.2-dev`。PR body 使用当时仓库中的 `.github/PULL_REQUEST_TEMPLATE.md`。如果修复 review feedback 后请求复审，必须使用 GitHub review-request API，并验证 reviewer 出现在 pending `reviewRequests` 中。

## 7. 验证计划

### 7.1 受影响 package

```text
./pkg/bootstrap
./pkg/bootstrap/versions/v4_0_5
./pkg/bootstrap/versions/v4_0_6
./pkg/sql/plan
```

先验证 package 选择非空，再执行 build/vet/test。由于 MatrixOne 的 Go 测试可能传递依赖 usearch，按仓库要求准备 thirdparties 和 CGO 环境：

```sh
make thirdparties
export CGO_ENABLED=1
export CGO_CFLAGS="-I$(pwd)/thirdparties/install/include"
export CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib -Wl,-rpath,$(pwd)/thirdparties/install/lib"
export LD_LIBRARY_PATH="$(pwd)/thirdparties/install/lib:${LD_LIBRARY_PATH}"

GOWORK=off go list -mod=readonly \
  ./pkg/bootstrap \
  ./pkg/bootstrap/versions/v4_0_5 \
  ./pkg/bootstrap/versions/v4_0_6 \
  ./pkg/sql/plan

GOWORK=off go build -mod=readonly \
  ./pkg/bootstrap \
  ./pkg/bootstrap/versions/v4_0_5 \
  ./pkg/bootstrap/versions/v4_0_6 \
  ./pkg/sql/plan

GOWORK=off go vet -mod=readonly \
  ./pkg/bootstrap \
  ./pkg/bootstrap/versions/v4_0_5 \
  ./pkg/bootstrap/versions/v4_0_6 \
  ./pkg/sql/plan

GOWORK=off go test -mod=readonly -v -count=1 -timeout 120s \
  ./pkg/bootstrap \
  ./pkg/bootstrap/versions/v4_0_5 \
  ./pkg/bootstrap/versions/v4_0_6 \
  ./pkg/sql/plan
```

若本机采用技能提供的 controlled wrapper，则对 test 阶段使用：

```sh
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=120s \
  ./pkg/bootstrap \
  ./pkg/bootstrap/versions/v4_0_5 \
  ./pkg/bootstrap/versions/v4_0_6 \
  ./pkg/sql/plan
```

### 7.2 SQL/升级集成验证

1. 运行更新后的 `test/distributed/cases/ddl/drop_if_exists.sql`，确认 result 文件只包含预期语义变化。
2. 在最小 service fixture 中构造 tenant 的四种 `TABLE_CONSTRAINTS` 状态，执行相同 handler/entry 公共路径，验证最终 catalog 类型和 view definition。
3. 从 `v4.0.4` 状态模拟失败中的 tenant task，升级到修复后的 `v4.0.5`，确认 `ready_tenant` 最终推进且没有同一 worker 的无界错误循环。
4. 对已经完成目标 entry 的 tenant 再启动升级，确认 `CheckFunc` 跳过 DDL，version offset 和完成状态不回退。
5. 真实持久化数据升级已执行：用 `7686f340df`（`TABLE_CONSTRAINTS` 改成 view 的提交之前）启动单机集群，执行 account BVT 并创建两个保留租户；随后按发布版本 `v2.1.0 -> v2.2.0 -> v3.0.0 -> v4.1.4` 顺序升级同一份 `mo-data`，最后以本 PR 的 `mo-service` 启动并完成 `4.0.6` 升级。

### 7.3 本地验证记录（2026-08-19）

- 受影响 package 的 `go list`、`go build`、`go vet` 全部通过。
- 四个受影响 package 的新增/相关定向测试全部通过。
- `pkg/bootstrap` 新增调度测试在 race detector 下重复 100 次通过，随后该 package 的完整 race test 通过。
- `pkg/bootstrap`、`v4_0_5`、`v4_0_6`、`pkg/sql/plan` 的完整普通测试均在上述 PR 实现基线上通过。
- account BVT `zz_accesscontrol/create_account.sql` 在历史二进制和升级后的 PR 二进制上各执行一次；两次均为 150/150 成功。
- 真实升级前，两个保留租户的 `information_schema.table_constraints` 均为 `relkind='r'` 的历史 base table，业务 marker 行分别为 101 和 202；升级后两个租户的 `create_version` 均为 `4.0.6`，`TABLE_CONSTRAINTS` 和 `KEY_COLUMN_USAGE` 均为 `relkind='v'`，canonical view SQL 已落盘，view 可查询，marker 行保持不变。
- 真实升级中所有 version/tenant step 都完成：`2.0.3 -> 2.1.0 -> 2.2.0 -> 3.0.0 -> 4.0.0 -> 4.0.1 -> 4.0.2 -> 4.0.3 -> 4.0.4 -> 4.0.5 -> 4.0.6` 的 `state=2`，各步 `ready_tenant=total_tenant=3`（system tenant 加两个保留租户）。
- 直接从 `v2.0.3` 跳到当前二进制会先遇到与本 PR 无关的历史 catalog 依赖（例如缺少 `mo_merge_settings`/`mo_feature_registry`），因此端到端验证采用发布版本逐级升级；这也避免把不受支持的跨代跳跃误当成本修复失败。
- DDL BVT `ddl/drop_if_exists.sql` 在升级后的 PR 二进制上通过：47 条命令中 35 条执行成功、12 条按 case 标记忽略、0 失败，成功率 100%。

## 8. 发布与运行时验收

发布时按以下信号验收，不以“升级最终完成”作为唯一判断：

1. unit test 证明错误会结束当前 drain round，外层调度代码保证下一次调用经过 timer 边界；线上聚合错误速率应受 `CN 数 × 每 CN worker 数 / checkUpgradeTenantDuration` 约束。不能用多 worker 聚合日志中相邻两条同文案的间隔直接证明单 worker 不变量。
2. `sql_executor exec`、remote lock failure 和 rollback 速率不再随 `CN 数 × worker 数` 形成热循环。
3. `v4.0.4 -> v4.0.5` 的 `ReadyTenant` 能越过原 736/1072 停点并最终等于 `TotalTenant`。
4. 随机抽查曾为 base table 的租户：`information_schema.TABLE_CONSTRAINTS` 是目标 view，而不是对象缺失或仍为 table。
5. CN RSS/working set 不再与升级错误同步陡升；没有新的 OOM kill。

若升级项仍因其他确定性错误失败，#27287 应保证集群保持可服务，但升级仍会停住并周期重试；这属于预期的 fail-visible 状态，不能把“没有 OOM”误判为升级成功。

## 9. 非目标与风险控制

- 不重构整个 upgrade framework。
- 不把单次事故扩展成通用 retry framework。
- 不在失败热路径增加无界标签 cardinality、逐 tenant metrics 或重复日志。
- 不批量删除所有 information_schema 同名 base table；只有具备历史迁移证据的对象进入 destructive conversion。
- 不依赖 #27289 的兼容性修复来保证 version entry 收敛。
- 不通过缩短/拉长 sleep 让并发测试概率性通过。
- 不修改历史 tenant 数据，除非目标 entry 已确认该同名 base table 是应被 view 替代的遗留对象。

主要风险是对对象历史状态判断过宽，误删仍有语义的 base table。控制手段是：对象白名单、历史来源记录、entry 三段式协议测试、DDL 类型不匹配 BVT、升级后对象类型与 definition 双重校验，以及保持其他通用 view refresh entry 的非破坏性策略。
