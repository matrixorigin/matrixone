# UT 执行模型与 fixture 生命周期优化设计

- 状态：Accepted for this PR，revision 2
- 适用范围：`optools/run_ut.sh`、Go test package 分组、embedded/shared cluster fixture、CI UT 资源预算
- 约束：不增加 runner 数量；收益必须来自单 runner 的工作删除、fixture 复用或资源有界的阶段重叠
- 设计 owner：UT runner 与测试基础设施；各测试 package 对自己的 fixture reset/cleanup 契约负责
- 设计门禁：跨 package、跨进程 admission、runner 取消和集群生命周期，命中 execution、ownership、resource 和 public test-contract 多个边界
- 决策记录：revision 2 接受本 PR 的 runner 账本、取消/报告所有权和有界分片；compile-only prebuild 保留为显式 opt-in，plan overlap 仍为显式 opt-in；两者都不改变默认资源预算。跨进程 cluster 共享、fixture reset 合并和动态调度不在本 PR。

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

- 第一阶段在 TERM、包失败、报告截断、parser error 和子进程不退出时都能产生有界摘要，且保留原始失败状态；helper 已完成但父进程尚未接管报告时，必须按完成标记或 shard 备份选择唯一报告来源。
- 每个迁移、删除或缩小的测试都有独特 oracle 映射；无新增固定 sleep、盲目 retry 或 skip。
- 被合并 suite 的 fixture start/close 次数与兼容矩阵预期一致，dirty fixture 不会被复用。
- 同配置 A/B 能同时给出 wall time、资源成本、排队和失败率；不把累计 wait 当成 wall-time 收益。
- 新增并发路径必须证明 ownership、generation、cleanup、取消独立性和 boundedness；`go test -race`、定向 failure injection 和原有 UT/BVT 矩阵保持通过。

## 7. 交付拆分

1. 本 revision：runner checkpoint、报告早创建、取消进程组清理、`run_ut.sh` 执行阶段的 70 分钟外层兜底、helper 报告所有权和诊断测试。`make ut` 的 `cgo/config` prerequisites 在该 timeout 之前运行；它们仍由各自命令负责失败和重试，不把 70 分钟描述成整个 make job 的硬上限。
2. 本 revision 提供 compile-only embedded prebuild，但默认关闭（`UT_PREBUILD_EMBEDDED=0`）；plan overlap 同样默认关闭（`UT_OVERLAP_PLAN=0`）。打开任一开关前必须补同资源 A/B 和负向取消证据。
3. 后续 PR：按兼容矩阵迁移一小批可复用 fixture，并提供 fixture/关键路径 before-after。
4. 后续 PR：清理慢测试的重复 setup、无契约等待和过大数据，逐项保留 oracle 证明。
5. 后续 PR：静态 shard/runner 资源 A/B；只有证据支持时再考虑跨进程共享 cluster。
