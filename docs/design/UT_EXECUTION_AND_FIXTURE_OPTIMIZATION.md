# UT 执行模型与 fixture 生命周期优化设计

- 状态：Proposed for this PR，revision 8
- 适用范围：`optools/run_ut.sh`、Go test package 分组、embedded/shared cluster fixture、CI UT 资源预算
- 约束：不增加 runner 数量；收益必须来自单 runner 的工作删除、fixture 复用或资源有界的阶段重叠
- 设计 owner：UT runner 与测试基础设施；各测试 package 对自己的 fixture reset/cleanup 契约负责
- 设计门禁：跨 package、跨进程 admission、runner 取消和集群生命周期，命中 execution、ownership、resource 和 public test-contract 多个边界
- 决策记录：revision 2 接受本 PR 的 runner 账本、取消/报告所有权和有界分片；compile-only prebuild 保留为显式 opt-in，plan overlap 复用已释放 slot 并默认开启；两者都不扩大默认 heavy 资源预算。跨进程 cluster 共享和动态调度不在本 PR；本 revision 按兼容矩阵落地了一个同进程单 CN fixture 合并，并为后续专用 fixture 增加显式释放边界。

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
6. 任何并行度改变都必须同时检查 cgroup memory、CPU throttling、runner queue、fixture 启停次数和 job critical path；`-p1` 或增加 admission slot 不是默认优化结论。

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

复用现有完整且不重叠的 UT shard partition。单 runner 内可以先用 `go test -c` 做不执行测试 binary 的预编译，把 embedded 包的 build/link 与 issues 的共享 cluster body 重叠；预编译失败仍由正式测试命令给出权威结果，预编译进程必须接受同一取消路径。本 PR 提供这个路径，但 `UT_PREBUILD_EMBEDDED=0` 是默认值；只有同一 checkout、race/tags、CPU/memory 和缓存模式的 A/B 证明关键路径收益后，才可在 CI 显式打开。再在相同条件下 A/B embedded `-p1` 与 `-p2`，决定是否扩大 package 并发。plan shards 只有在显式消耗一个 heavy 进程 slot、证明非空、全集覆盖、不重复和可清理后才并行；`UT_OVERLAP_PLAN=0` 默认保持顺序。每个 runner 默认一个 active complete cluster。

只有当同进程合并、reset 和静态分片后仍有实测的 cluster startup 瓶颈，才另行设计跨进程 cluster service。那时必须补租约、generation、失联、reset、server crash、内部 hook 不可复用和权限隔离契约；本设计不把 daemon、动态 scheduler 或无界 admission slot 作为第一批改动。

## 4. 观测与度量

事件阶段至少区分 discovery、build/link、process startup、admission wait、service start、readiness、test body 和 cleanup/close。统计使用非重叠区间，避免把父测试、子测试和等待者时长重复相加。admission 诊断区分 `released`、`process-exited/lower-bound`、`still-live` 和 `unknown`；缺少 release 记录不能直接判定泄漏。

每次 A/B 需固定 checkout、UT shard、race/coverage 模式、runner 类型和资源限制，比较：job/stage critical path、runner-minutes、queue、fixture start/close 次数、admission 持有/等待区间、cgroup memory peak/OOM/CPU throttling 和失败诊断完整性。当前 8 个样本只用于建立基线，不预先承诺“50 分钟降到某个固定数值”；后续目标是由多次同配置样本验证关键路径下降，且不能用 runner 成本或失败率换取表面 wall time。

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

## 7. 交付拆分

1. 本 revision：runner checkpoint、报告早创建、取消进程组清理、`run_ut.sh` 执行阶段的 70 分钟外层兜底、helper 报告所有权和诊断测试。`make ut` 的 `cgo/config` prerequisites 在该 timeout 之前运行；它们仍由各自命令负责失败和重试，不把 70 分钟描述成整个 make job 的硬上限。
2. 本 revision 提供 compile-only embedded prebuild，但默认关闭（`UT_PREBUILD_EMBEDDED=0`）；plan overlap 同样默认关闭（`UT_OVERLAP_PLAN=0`）。打开任一开关前必须补同资源 A/B 和负向取消证据。
3. 本 revision：按兼容矩阵迁移一小批可复用 fixture，并提供 fixture/关键路径 before-after；后续继续逐组验证，不跨越不同 topology 或 global hook 合并。
4. 后续 PR：清理慢测试的重复 setup、无契约等待和过大数据，逐项保留 oracle 证明。
5. 后续 PR：静态 shard/runner 资源 A/B；只有证据支持时再考虑跨进程共享 cluster。
