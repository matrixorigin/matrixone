# UT 执行模型与 fixture 生命周期优化设计

- 状态：Proposed for this PR，revision 10
- 适用范围：`optools/run_ut.sh`、Go test package 分组、embedded/shared cluster fixture、CI UT 执行模型与资源预算
- 约束：UT CI 始终只使用现有的一个 runner；不增加 shard matrix、并发 job 或临时 runner。只考虑该 runner 内有界调度和有证据的测试/fixture 优化
- 设计 owner：UT runner 与测试基础设施；各测试 package 对自己的 fixture reset/cleanup 契约负责
- 设计门禁：跨 package、跨进程 admission、runner 取消和集群生命周期，命中 execution、ownership、resource 和 public test-contract 多个边界
- 决策记录：当前 `UT_OVERLAP_PLAN=1` 复用已释放 slot；`UT_OVERLAP_LIGHT=0` 默认关闭。用户明确要求不得增加 runner，因此不采用四 shard 并行 CI；在单 runner 上顺序运行 shard 不能减少总工作量，也不是本 revision 的优化。compile-only prebuild 默认关闭；跨进程 cluster 共享、动态调度、任意缩减测试数据均不在本 revision。

## Revision 10: race-UT timeout root cause and single-runner overlap

### 现场与根因

PR #28818 的 GitHub run `34746731207` / job `103695929664` 在 race UT 的外层
`70m` timeout 收到 TERM；test2json 记录到 43,564 个 pass、180 个 skip、零个失败事件，
但 heavy/plan 阶段尚未完成，因此结果是不完整而非通过或断言失败。checkpoint 显示
light 用时 21m24s、HNSW 50s、issues 21m42s、embedded 21m12s；约 69m32s 后才进入
plan。根因是完整套件的阶段关键路径在串行 caller 上超过硬时限，现有证据不能把它归因
为某条失败 case、过大测试数据或 UT 断言错误。

当前 runner label 是 `amd64-mo-shanghai-8c16g`。同一失败现场的 cgroup memory 峰值约
17.18 GB（16 GiB 量级）；light 的 `-p6` 阶段已几乎占满预算。已有
`UT_OVERLAP_LIGHT=1` 会在同一 runner 上重叠 light 与 issues，但组合内存尚未测量，故
本 revision 不打开它。`UT_OVERLAP_PLAN=1` 是现有默认，继续复用 engine 释放的进程槽，
不改变测试范围或默认 runner 数。

reusable `matrixorigin/CI` 虽提供四个静态 shard，但使用它们会增加并发 runner，违反本任务
的硬约束；即使把 shard 顺序放到同一 runner，也不会减少总 UT 工作量。本 revision 不改
`ut_sharded: false`，不添加 PR selector、manual workflow 或额外 job。

同一 runner 上已有的 default-off `UT_OVERLAP_LIGHT=1` 路径是唯一已实现、可直接用现有 CI
job 评估的跨阶段候选：HNSW 仍先独占；随后 light 使用至多两个 package worker，并与
`pkg/tests/issues` 的独占 package 阶段重叠；两边各自结束后才进入 embedded。报告私有化、
合并、失败传播和 TERM 取消已有 scheduler contract tests。它保持 runner 数为一，但总进程
并发提高、light 的 package 并行度降低；真实 wall-time 和合并内存峰值仍需在同一
`8c/16GiB` runner 上测量，不能从串行两阶段时间直接推断净收益。

慢 case 排名也没有给出任意缩小数据或再合并 fixture 的证据：`pkg/tests/issues` 已共享
canonical fixture；Arrow LOAD fanout 已是略高于 1 MiB 产品阈值的最小输入；长耗时
Arrow BVT 的第一个子测试在 parent 开始约 4m17s 后才启动，指向启动/准入成本（这是基于
事件顺序和源码的推断，尚非分阶段实测）。并发 branch reclamation、quota race、不同
topology/config/restart/global hook 的 case 必须保留各自压力和隔离语义。没有新的
fixture 兼容矩阵、reset oracle 和 A/B 数据前，不缩小这些数据、不把隔离 suite 强行合并。

### 本 revision 的安全交付

- 用同一现有 required UT job 做可复核的顺序 A/B，不新增 workflow/job/runner，也不并发重跑：
  先在只含诊断改动、`UT_OVERLAP_LIGHT=0` 的 parent SHA 上收集 3 个完整 control；再创建
  child SHA，唯一行为差异是 `UT_OVERLAP_LIGHT` 默认值从 `0` 改为 `1`，并收集 3 个完整
  treatment。runner label、`ut_parallel=6`、`ut_sharded=false`、race/tags/timeout、测试选择
  及 reusable CI resolved SHA 必须相同；逐个记录 workflow run id/head SHA/CI SHA/时间/内存。
  任一 treatment 失败、超时、取消或 OOM，立即恢复默认 `0`，停止后续 treatment，不推广。
- 为审计内存边界，诊断日志需同时记录 cgroup `memory.max`、`memory.peak` 及
  `memory.events` 的 `oom`/`oom_kill`；本 revision 不以 CPU throttling 作门槛（当前未采集）。
  70m timeout 是 censored 样本，不能作完整 control。包和 test 选择由 `UT_SHARD=all` 的
  源码命令比较与现有 complete/disjoint partition 和 scheduler contract tests 证明相同；
  两边完整 required UT job 必须通过，报告必须合并且无丢失/重复。
- 只有 3 个 treatment 均完整通过、race-UT job 均不超过 60 分钟、每次
  `memory.peak <= 0.9 * memory.max`，且结束时 `memory.events` 的 `oom`/`oom_kill` 均为 0，
  并且 treatment 中位 wall-time 比 3 个完整 serial control 中位数至少改善 15%，才允许
  把默认值保留为 `1`。
  证据收齐前 PR 保持 Draft/不可合并；未达门槛则保持默认 `0`，继续按 stage/case profile
  定位瓶颈，而不靠加 runner 或提高 package 并发补救。
- issue #28538 已跟踪 70m race UT 超时且目前由 `gouhongshen` 负责；本 revision 不重复
  建 issue、不改 assignee、不关闭 issue。单 runner overlap 的配置/耗时结果由该 issue 的跟进更新
  维护。

## Revision 9: branch SQL regression fixture consolidation

最近两天的变更横跨 SQL function/aggregate、planner/execution、向量与索引、Arrow
LOAD/DDL/catalog、事务与服务生命周期，以及 UT/BVT runner 基建。测试优化先按生产
边界和 oracle 分组；本 revision 只处理其中同一普通 SQL 单 CN 合约的两个 branch 回归：
`TestIssue26111DataBranchDatabaseWithCyclicForeignKeys` 和
`TestIssue26114CrossAccountBranchQuotaAndOwnership`。它们从
`pkg/tests/issues/isolated` 迁入已有的 `pkg/tests/sqlintegration` canonical fixture，
不改变生产代码、runner 分组、race/coverage/timeout 或 BVT 文件。

两组回归的业务 oracle 原样保留：

- 26111 继续验证循环外键的数据、元数据、非法写入、已有目标拒绝、跨账号可见性、
  session `foreign_key_checks` 和 branch 可删除性；
- 26114 继续验证 table/database branch 禁用、无残留、并发 quota 的一成一败、quota
  提高后的 metadata 数量，以及旧版 `creator=0` 元数据仍计入 quota。

共享 fixture 引入了新的 reset oracle，而不是删掉业务断言。每个 26114 子场景保存并
恢复 `mo_feature_registry` 的 `BRANCH` 完整行（包括 description、JSON scope、enabled、
created/updated timestamp 以及原行不存在的情况），并复查恢复结果。创建的 account、
feature-limit、branch metadata、用户 snapshot 和 branch protect snapshot 在退出时有界
清理并检查。quota 竞争的两个 goroutine 必须先全部返回，随后才能做 catalog cleanup。
共享 fixture 只有在场景成功且清理成功时复用；失败或恢复失败由外层 cleanup 在 fixture
锁释放后销毁，避免 callback 内自锁和脏状态串入下一场景。restore、预算、multi-CN、
global hook、Arrow/partition/shard/upgrade 等不兼容场景继续留在 `isolated`。

删除的业务 case 数量为零；删除的只是跨 package 的重复生命周期 wrapper 和旧文件定义。
现有 feature-limit branch BVT 仍负责真实 frontend/public SQL contract，因此本 revision
不新增重复 BVT。收益必须以同一 CGo、race、tag、topology 和 runner 配置的 before/after
测量确认；CI admission 累计等待不能直接计为 wall-time 节省。

## Revision 8: timeout observability for partial UT runs

超时现场必须同时回答“现在卡在哪里”和“此前哪些 case 已经异常慢”。`run_ut.sh` 在
收到 TERM、停止并回收自己创建的进程组后，从已经写入的 Go `-json` 前缀生成已完成
case 和 package 的耗时排行；仍在运行的 case 继续由 active-case 分析单独列出，不能
把它误报成已完成结果。JSON 报告按二进制行读取，尾部截断的 UTF-8 或半行只计为损坏
行，不影响此前完整事件的输出。排行写入 `ut-report/top.txt`，因此现有 CI 的 always-run
“Print the Top 10 Time-Consuming Tests”步骤会在超时后直接打印它。

UT 运行期间默认每 60 秒写一条 heartbeat，记录最近 stage/label、active case 数、进程
数、报告路径和 checkpoint；取消时再把原始 JSON、checkpoint、stderr 和排行复制到
`ut-report/`。active-case 扫描会跳过不改变状态的 `output` 事件；主报告在同一文件
系统上优先用 hard link 快照，避免失败时复制一份完整 JSON。CI artifact 只上传快照和
尚未合并的 helper report，避免再次上传主报告。这些文件是诊断输入，不参与测试结论，
解析失败也不能覆盖原始退出码。
当前 reusable `matrixorigin/CI` workflow 只打印 `top.txt`，没有上传 `ut-report`；要在
GitHub UI 下载原始现场，需要在该 workflow 增加 always-run 的 `actions/upload-artifact`
步骤。这个上传步骤属于 CI 基础设施 PR，不能由 MatrixOne 的 `make ut` 单独完成。
CI PR 合并后，失败或取消的 job 会生成
`ut-diagnostics-<run-id>-<attempt>-<shard>` artifact；可用
`gh run download <run-id> -n <artifact-name>` 下载，再运行
`python3 optools/summarize_ut_slow_cases.py ut-report/ut-report.json` 查看已完成的慢
case，结合 `ut-report/ut-checkpoint.log` 和 helper report 判断卡点。
CI 上传副本设置 250 MiB 总预算和 5 分钟超时；超出预算的文件保留带截断标记的头尾，
`manifest.txt` 记录原始与保存字节数。该预算只约束诊断 artifact，不改变测试输入、
coverage 或失败结论。
成功的 UT job 只上传 `top.txt` 和 checkpoint 作为 1 天的轻量 baseline；原始 JSON 和
helper report 仅在失败或取消时上传，便于比较正常 run 的阶段耗时而不复制完整测试流。

## Revision 7: bounded light/issues overlap on one runner

最近成功的单 runner UT 记录显示，`light race-test` 约占 10--20 分钟，
`pkg/tests/issues` 的独占阶段约占 6--11 分钟；两者当前完全串行。依赖图分组已经把
所有会启动 embedded cluster 的 package 从 light 组移除，因此可以在不增加 runner、
不改变测试参数和不共享 fixture 的前提下，让两个阶段重叠。

调度保持 HNSW 的 native worker pool 独占约束：先完成 HNSW，再启动 light helper，随后
前台执行 issues。light helper 使用 `-race`、相同的 tags/timeout 和完整的 light scope，
但将自己的 JSON 写入私有文件；issues 的 writer 结束后，父进程再原子合并 helper 报告。
两条路径都执行完毕后才进入 embedded 阶段。helper 的 PID、进程组、退出状态和报告消费
纳入同一 TERM 取消路径，任何一边失败都不会跳过另一边。

并发是有界的：`UT_OVERLAP_LIGHT=0` 默认保持顺序基线，`UT_OVERLAP_LIGHT=1` 提供受限 A/B，
`UT_OVERLAP_LIGHT_PARALLEL=2` 默认限制 light 的 package 并行度，并在小于该值的
`UT_PARALLEL` 下自动收窄。开启 overlap 时不同时启动 compile-only embedded prebuild，
避免叠加第二条编译通道。所有 package 仍只执行一次，测试函数、subtest、race detector、
coverage 事件和失败断言都保持不变；这里改变的是阶段顺序和 package 调度，而不是测试
强度。预期收益是被重叠的 issues wall time，约 6--11 分钟只是基于阶段长度的上限，
必须在同资源 Linux runner 上用 cgroup memory/OOM、CPU throttling、长尾和失败率的 A/B
结果确认净收益后再调整并行度。

## Revision 6: reuse released engine capacity on one runner

The CI caller selects `ut_sharded: false`. Returning to one runner is a resource
constraint, not a speedup relative to the earlier single-runner baseline.

The measured single-runner trace has resource-heavy work from 06:19:04 to
06:26:04, while both engine processes finish by 06:22:22. Plan then extends the
critical path by about 2m20s. The runner now starts plan after joining engine,
while the existing resource-heavy command continues. With the default budget
of three, `engine(2) + resource(1)` becomes `plan(1) + resource(1)`. Plan compilation
retains `-p1`; cases, data, race flags, cluster admission, and coverage jobs are
unchanged. This scheduling change is enabled by default (`UT_OVERLAP_PLAN=1`);
`0` provides the previous sequential baseline. Budgets of one or two retain
sequential execution, as do paths without a concurrent engine helper.

The foreground command retains one parent-owned PID and stage/label. Engine's
status is saved even on failure, and plan still runs. Engine JSON is merged only
after the foreground writer has exited: renaming the report before that barrier
would discard later writes to the old inode. Plan retains its private report.
Cancellation signals every owned group before bounded waits and report merging.

The scheduling contract tests use a blocked heavy writer that only plan can
release, prove engine is joined first, and require every report exactly once.
They also exercise failures of each stage, low budgets, sequential mode, and
TERM during overlap. No public SQL behavior changes, so new BVT cases are not
needed. Actual CI savings depend on the remaining heavy tail and resource
contention; the historical 2m20s is potential overlap, not a measured improvement.

The earlier proposed logservice companion was removed. That package finished
several minutes before light completed in the observed trace; its own elapsed
time was not evidence of critical-path savings.

The sections below record the earlier fixture/runner design. Revision 6 replaces
the earlier opt-in, extra-slot plan-overlap policy with released-slot scheduling.

## 1. 问题与不变量

最近 8 个相同模式的成功 UT job 约为 45--51 分钟；描述性 P80 约 50.7 分钟。stage 中位数约为：embedded-cluster 16.1 分钟、light 11.6 分钟、issues 9.0 分钟、engine/test 6.6 分钟。一个带 setup 账本的运行记录了 22 个 embedded cluster admission acquire，等待时间总和 13.71 分钟，最大等待 4.73 分钟；这个总和是多个等待者的重叠时间，不能直接当作可节省 wall time。当前 `SharedTestCluster` 只在一个 Go test 进程内共享，不同 test binary 仍会各自启动 cluster。

优化的目标是减少真正的关键路径工作，同时让超时和取消可以定位到等待者、持有者、阶段和资源，而不是把排队时间换成不可解释的并发。以下不变量必须保持：

1. 每个测试只使用与自己的 topology、配置、global hook/failpoint、内存预算、restart/destructive 行为兼容的 fixture。
2. 共享 fixture 的 body、`t.Cleanup` 和 admission lease 在同一生命周期内完成。清理失败后 fixture 标记为 dirty，不得静默交给下一个 scenario。
3. 每个场景保持独立 session、database、事务和 named oracle；合并 package 不得删除唯一的失败路径，也不得失去 `-run` 定位能力。
4. admission 只有一个权威所有权记录。诊断中的 owner、waiter 和 stage 信息不能成为第二套锁，也不能绕过资源边界。
5. 取消必须停止派发，终止本 runner 创建的进程组，并在有界时间内保留原始测试状态和已产生的报告。报告解析失败不能覆盖测试失败。
6. 任何并行度改变都必须检查 cgroup memory/OOM、runner queue、fixture 启停次数和 job critical path；CPU throttling 只有在现有 runner 指标可用时才纳入，本 revision 不以未采集的 throttling 指标作门槛。`-p1` 或增加 admission slot 不是默认优化结论。

## 2. 现状证据与根因假设

当前 runner 已按 light、issues、embedded、heavy、plan 分组，但阶段在一个 runner 上顺序执行；embedded 组允许两个 test package 进程，cluster lifecycle 通过跨进程文件锁串行。旧 setup 输出只有 phase 的累计统计和模糊的 `admission_unreleased`，没有稳定的 run/package/owner 归因。取消发生在报告目录创建之前时，后置报告步骤可能因目录不存在而失败，且前台 `go test` 的子进程可能只由外层清理。compile-only prebuild 和 plan overlap 即使不启动第二个 cluster，也会消耗编译、内存和 linker 资源，不能因为“只编译”就默认开启。

因此，第一阶段只把下面两件事作为已确认的工程问题：

- 关键路径和取消现场没有足够的阶段账本；
- 可兼容 fixture 的合并机会尚未以矩阵和 reset 契约证明。

`admission_wait(total=...)` 的累计值、`admission_unreleased_observed` 的非零值以及 light 阶段中未显示的时间都只能作为待归因信号。没有 holder 生命周期、进程退出状态和 cgroup 数据前，不把它们命名为泄漏、纯排队浪费或编译瓶颈。

## 3. 分阶段方案

### Phase 1：关键路径账本、取消和 70 分钟兜底

runner 在任何可能失败或被取消的工作前创建报告目录、原始 Go JSON 文件和 checkpoint 文件。每个 stage、package command、cluster fixture 关键事件写入结构化 checkpoint，至少包含 run id、stage、label、pid、开始/结束时间、状态和退出码。终端摘要只打印结果、最长阶段、最长等待、首个错误、当前 holder 和 artifact 路径；详细事件保留在 artifact，并限制摘要行数。

TERM 路径停止新的 command，抓取当前 checkpoint 和 active test，向本 runner 创建的进程组发送 TERM，有界等待后再 KILL，并保留测试原始退出状态。正常 CI UT 的外层硬兜底设为 70 分钟，另留诊断和上传预算；外部 SIGKILL 或 runner 丢失时不承诺事后生成 artifact，只依赖已刷新的 checkpoint。

### Phase 2：fixture 兼容矩阵与同进程合并

为 cluster 用例维护兼容矩阵：CN/TN 拓扑、启动配置、global hook/failpoint、内存限制、restart/destructive 行为、SQL/session 状态和 cleanup 要求。只把矩阵兼容且通过 reset 验证的公共 API 用例移动到明确领域 suite；保留原 named test/subtest 和独立 oracle。不同 Go test binary 不能共享内存中的 `SharedTestCluster` 指针，因此先通过有意的 suite 合并减少 fixture 启停，不引入跨进程 cluster daemon。

不得把所有 issue 用例倒入已经约 9 分钟的 package。若 body 占据主要时间，继续合并 fixture 只会增加失败耦合；应先缩小数据、移除无契约的 sleep、合并重复 setup，或把真正的压力/稳定性场景迁移到合适的 BVT/perf 层，并保留最小 public SQL oracle。

本 revision 迁移了 `pkg/tests/issues/isolated` 中配置兼容的 `TestIssue26111...` 和
`TestIssue26114...` 到 canonical `RunSingleCNBaseClusterTests`。26111 原先依赖关集群
丢弃 catalog 状态，因此迁移时补了 snapshot、database 和 account 的进入前/退出后
清理；清理失败时只在 callback 返回后丢弃 dirty fixture，避免在共享锁内自锁。26114
汇总并报告每条清理语句的错误，并在 testcase 生命周期边界丢弃 fixture，避免脏 catalog
被下一次 `-count` 或重排执行复用。单 CN fixture 的显式 release 在成功关闭后重置
初始化代数，因此 `-count` 和 shuffle 不依赖顶层测试顺序。该包仍包含必须使用专用
内存、双 CN、restore 或 global hook 的场景；这些场景在启动前显式释放已经完成的单 CN
fixture，避免同时持有两个 complete cluster，且不改变现有 admission 保护。

### Phase 3：reset/cleanup 契约和低质量测试清理

每个共享 scenario 在进入时注册 cleanup，退出时恢复 session/transaction、catalog/account、runtime hook/failpoint、cache/provider 等状态。初始化部分成功、`FailNow`、cleanup error、cancel、close error 和 restart 都要有 deterministic fault test。dirty fixture 只能被丢弃，不能透明重试或 skip。

对每个慢 case 建立“独特 oracle -> 新位置”映射，证明删除、合并或缩小数据没有降低 coverage。固定 sleep 必须改为可观察条件或 deterministic injection；大数据只保留能证明边界的最小规模。

### Phase 4：有界并行、静态分片和跨进程共享的重新评估

compile-only embedded prebuild 保持显式 opt-in（`UT_PREBUILD_EMBEDDED=0`），只有同一
checkout、race/tags、CPU/memory 和缓存模式的 A/B 证明关键路径收益后，才可在 CI 打开。
当前 `UT_OVERLAP_PLAN=1` 复用已释放的 engine slot；`UT_OVERLAP_LIGHT=0`。reusable CI
虽然提供静态四分片，但本任务固定一个 runner，不调用并行 shard matrix；同 runner 上只
评估现有有界 light/issues overlap，且必须记录其资源竞争和取消/报告行为。

只有当同进程合并、reset 和静态分片后仍有实测的 cluster startup 瓶颈，才另行设计跨进程 cluster service。那时必须补租约、generation、失联、reset、server crash、内部 hook 不可复用和权限隔离契约；本设计不把 daemon、动态 scheduler 或无界 admission slot 作为第一批改动。

## 4. 观测与度量

事件阶段至少区分 discovery、build/link、process startup、admission wait、service start、readiness、test body 和 cleanup/close。统计使用非重叠区间，避免把父测试、子测试和等待者时长重复相加。admission 诊断区分 `released`、`process-exited/lower-bound`、`still-live` 和 `unknown`；缺少 release 记录不能直接判定泄漏。

每次 A/B 需固定 checkout、race/coverage 模式、runner class 与 `UT_PARALLEL`；单 runner overlap 另记录 light helper 实际 `-p`、两阶段重叠区间、stage critical path、fixture start/close 次数、admission 持有/等待区间、cgroup `memory.max`/`memory.peak`/`memory.events` 和失败诊断完整性。timeout 是被截断的样本，不能当完整 wall time。当前 8 个历史样本仅用于描述性基线；是否保留 overlap 由 revision 10 的重复完整 run 与量化门槛决定，不能用资源风险或失败率换取表面 wall time。

## 5. 已否决或暂缓的替代方案

| 方案 | 决定 | 原因 |
| --- | --- | --- |
| 删除 admission 或盲目增加 slot | 暂缓 | 可能把 HAKeeper/内存竞争重新变成 flaky；没有 cgroup 和服务健康证据 |
| 通过改锁路径绕过跨进程串行 | 否决 | 不会删除 cluster 生命周期，只会破坏资源边界 |
| 跨进程 cluster daemon/pool | 暂缓 | 内部对象、global hook 和 reset 无法透明共享，新增租约、generation 和失联故障面 |
| 把所有 SQL/issue 测试合并到一个大 package | 否决 | 增大 failure blast radius，可能让 body 继续串行，失去定位性 |
| 任意动态 scheduler | 暂缓 | 在静态 partition、fixture 合并和资源预算未稳定前，所有权和取消复杂度不值得 |

## 6. 验收标准

- 第一阶段在 TERM、包失败、报告截断、parser error 和子进程不退出时都能产生有界摘要，且保留原始失败状态；helper 已完成但父进程尚未接管报告时，必须按完成标记或 shard 备份选择唯一报告来源。报告转移先写同目录临时文件并原子发布，复制被 TERM/KILL 打断时不得删除源或发布前缀。
- admission 的 `complete` 只允许由当前 generation 的 acquire -> release 闭合得到；只有 hold/release 或旧 generation 的事件必须保持 `unknown`/`partial`，不能升级为完整证据。
- 每个迁移、删除或缩小的测试都有独特 oracle 映射；无新增固定 sleep、盲目 retry 或 skip。
- 被合并 suite 的 fixture start/close 次数与兼容矩阵预期一致，dirty fixture 不会被复用。
- 同配置 A/B 能同时给出 wall time、资源成本、排队和失败率；不把累计 wait 当成 wall-time 收益。
- 新增并发路径必须证明 ownership、generation、cleanup、取消独立性和 boundedness；`go test -race`、定向 failure injection 和原有 UT/BVT 矩阵保持通过。
- CI 调用方必须继续 `ut_sharded: false`；不得增加并发 runner/job。UT shard 仅能用于选择性诊断，不能把多个 shard 并发执行作为提速方案。
- 单 runner overlap 必须满足 revision 10 的顺序 A/B、coverage、wall-time、`memory.max`/peak 和 OOM 门槛；否则保持 `UT_OVERLAP_LIGHT=0` 并将结果作为不推广证据。

## 7. 交付拆分与当前决策

1. 已落地的前序 revision/PR：70 分钟执行阶段兜底、进程组取消与报告诊断、兼容 fixture 复用、计划阶段释放 slot 重叠；当前 `UT_OVERLAP_PLAN=1`、`UT_OVERLAP_LIGHT=0`、caller `ut_sharded: false`。
2. 本 revision：补充 cgroup memory limit 诊断；随后在同一个现有 UT check 上顺序收集 default-off control 与 default-on overlap 样本。完整保留 race tests、报告、失败传播和 TERM 取消。不新增 job/runner，不改测试断言、测试数据、race 覆盖或 shard 分配。
3. 只有 3+3 个完整样本通过 revision 10 门槛，才保留 overlap 默认并把 PR 标为可合并；任何失败即恢复 `UT_OVERLAP_LIGHT=0`。本 revision 不启用四 shard 并行模式。
4. 后续仅按 profiling 结果提出重复 setup/fixture merge/data 最小化；每项保留 case oracle、reset 证明和同资源前后测量。跨进程 cluster service 仍需独立设计 ownership、租约、generation、reset、crash 和取消契约。

### Compatible SQL fixture consolidation

`pkg/tests/sqlintegration` owns the former DDL and transaction-executor tests.
Both packages used the same canonical single-CN fixture, but separate Go test
processes initialized it twice. Keeping these seven test functions in one
package removes one complete startup per package invocation. Test names,
assertions, data sizes, topology, race instrumentation, and the existing CDC
external-MySQL CI skip are preserved. The embedded lane discovers the new
package through its transitive dependency on `pkg/embed`; no explicit package
allowlist or additional runner is required. Coverage continues to execute these
tests through normal Go package discovery.

Scenarios execute sequentially under the canonical fixture lock. Test-created
databases are dropped through the CN's MySQL frontend with a fresh bounded
context before the callback releases that lock. A failed test (including cleanup
failure) closes the fixture before the next scenario. `TestMain` closes the
successful fixture after all repetitions. Existing PITR and role assertions and
cleanup remain in place. The `scenario-body` setup event separates callback and
cleanup time from the first test's shared startup cost; it is nested timing,
not an independent wall-time saving.

Configuration compatibility limits consolidation: Arrow LOAD cases retain
different gates, materialization settings, topologies and restart ownership;
partition and shard suites retain different service/heartbeat configurations;
upgrade tests retain catalog-mutation isolation. Sharing those merely because
they use embedded clusters would change the test contract.

The report installer reuses an executable only when Go build metadata matches
the pinned module version and contains no replacement. Stale, unreadable or
missing metadata takes the existing bounded installation/retry path. The CI
builder also warms that version's module/build caches. Installation savings are
conditional on a reusable binary or restored caches; refreshing the builder is
required to gain its new cache contents. Neither optimization changes report
retention or failure handling. CI critical-path savings must be measured on the
PR; overlapping admission waits must never be added to the claimed saving.
