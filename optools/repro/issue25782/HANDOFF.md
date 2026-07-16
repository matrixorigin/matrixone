# Issue #25782 多进程复现 Harness 交接说明

更新时间：2026-07-16 18:47（Asia/Shanghai）

## 0. 当前修复进度（2026-07-16 23:40）

历史复现结论仍有效，但内核状态已经变化：当前 worktree 基于 `82f0cc2fb9c7bbb1fa2da161e204b7873647f80a` 实现了第一阶段 no-OOM hard-fail safety slice。它对 retained build batches、HashMap resize peak、主要 HashBuild scratch 和 runtime-filter serialization 做 admission；Broadcast build failure 使用 typed terminal dependency 广播给全部消费者；尚未预算化的 Shuffle spill 和 expression join-key 路径会在进入大分配前返回受控 query error。

代码安全切片已通过包级、race、vet 和 build 验证。首轮 fixed E2E 使用 `/tmp/mo-25782-fixed2-20260716235218`：目标 Broadcast SQL 成功返回 `132096`，PHYPLAN 总内存 `291164058B`、spill `0B`；CN1/CN2 后续查询均成功，cgroup `oom=0, oom_kill=0`，峰值分别为 `346140672` 和 `310169600` bytes，运行随后完整停止。超预算 query error 已由 operator/Broadcast 多消费者单测验证；专用 fixed classifier 和 over-budget cluster E2E 仍待补齐。

## 1. 最终结论

业务验证已经闭合，最终分类为：

```text
classification=REPRODUCED
```

最终使用全新 runtime 完成一轮自包含验证：

```text
runtime=/tmp/mo-25782-harness-final6-20260716184209
result=/tmp/mo-25782-harness-final6-20260716184209/results/20260716T184239-1679970
```

结果目录中的三个分类文件分别为：

```text
classification.overall   REPRODUCED
classification.broadcast REPRODUCED / broadcast_hashbuild_spill_bypassed
classification.shuffle   REPRODUCED / shuffle_positive_control_spilled
```

## 2. 关键证据

两阶段使用相同的物理数据、相同的 `LEFT JOIN`、相同的 `join_spill_mem=1000`，每次查询结果均为 `132096`。

Broadcast/non-shuffle 阶段的两个独立 execution attempt：

```text
attempt-1 / CN1  InRows=132096  is_shuffle=false  SpillRows=0  SpillSize=0
attempt-2 / CN2  InRows=132096  is_shuffle=false  SpillRows=0  SpillSize=0
```

Shuffle 正对照：

```text
CN1  instances=16  eligible=16  input=132096  spill_instances=16  spill_rows=132096  spill_size=1102848
CN2  instances=16  eligible=9   input=132096  spill_instances=9   spill_rows=132096  spill_size=1082688
```

CN2 的其余 7 个实例是 `InRows=0, SpillRows=0, SpillSize=0` 的空 shuffle partition，不是阈值以上却未 spill 的反例。两个 attempt 都存在阈值以上实例，且这些实例在两个 CN 上均产生了 spill。

重要证据边界：这是相同数据和相同 SQL 的两个独立执行。当前 `EXPLAIN PHYPLAN ANALYZE` 只在 coordinator 输出本地实际执行算子的运行时计数器，远端 scope 只是无计数器占位。Harness 因此给两个 CN 设置私有路由标签，经 Proxy 分别执行 attempt 1/2；extractor 把 attempt 写入 instance ID，classifier 强制 attempt 1→CN1、attempt 2→CN2。这里不声称“单次 PHYPLAN 输出同时含两个 CN 的运行时计数器”。

计划门禁也已闭合：

```text
broadcast: AP QUERY PLAN ON MULTICN, Join Type LEFT, Join Cond 无 shuffle
shuffle:   AP QUERY PLAN ON MULTICN, Join Type LEFT, Join Cond 带 shuffle: range(p.k)
```

对应文件：

- `execution/broadcast_plan.out`
- `execution/shuffle_plan.out`
- `evidence/operators.broadcast.tsv`
- `evidence/operators.shuffle.tsv`
- `evidence/operators.{broadcast,shuffle}.provenance.tsv`
- `classification.{broadcast,shuffle,overall}`

## 3. 安全与清理结果

最终运行未发生 timeout、cancel、watchdog failure、OOM 或 swap 增长。

停止前审计值：

```text
CN1 memory.peak=671641600, memory.max=2684354560, swap.current=0, oom=0, oom_kill=0
CN2 memory.peak=611315712, memory.max=2684354560, swap.current=0, oom=0, oom_kill=0
```

`manifest.final_telemetry` 为：

```text
final_telemetry=valid
stop_rc=0
```

运行结束后所有 harness unit、PID 和监听均已清理；停止后的 manifest 保留了原始进程/cgroup 身份、启动基线和停止前 telemetry。

## 4. 已完成的 Harness 修复

- PHYPLAN extractor 不再依赖 `idx`，使用 CN/scope/HashBuild ordinal 生成稳定 ID；只接受带运行时 `CallNum` 和完整计数器的实例，避免远端占位 plan 被误判。
- Evidence 使用当前 run 的只读 snapshot、SHA256 provenance 和完成标记；classifier 会重新验证路径、phase、run ID 和内容 hash。
- 启动和运行门禁把已锁定 source commit、二进制报告的 commit、二进制 SHA256 与每个实际运行进程的 `/proc/<pid>/exe` hash 绑定；最终二进制 SHA256 为 `b34b6c25aa34e3fd59c5c791f1c8d26ccafd0435b93a9624492a34b96bb3b246`。
- Classifier 拒绝重复实例、未知 CN、字段不完整、attempt/CN 路由错配和错误 shuffle flag；broadcast 必须覆盖两个 CN。Shuffle 允许零输入且零 spill 的自然空 partition，但每个 attempt 都必须有阈值以上实例并实际 spill。
- SQL 使用固定 `LEFT JOIN`，物理行数固定为每表 `132096`；通过分块 flush 产生多个持久对象。
- planner stats 同步到两个 CN，避免 Proxy 后端切换后使用不同的本地 stats cache。
- 每个 phase 通过两个有显式私有 CN label 的独立 Proxy 会话采集两个 coordinator 的本地运行时计数器；不再依赖负载均衡时序碰运气。
- 运行期 watchdog 持续验证 systemd invocation、进程 generation、cgroup memory/OOM/swap、宿主内存和 PSI；采样失败或覆盖间隔异常均判为 `INCONCLUSIVE`。
- `stop.sh` 保留完整 manifest，并在停止前记录最终 telemetry；正常停止、TERM、KILL 都仅作用于 manifest 绑定的 exact unit。

## 5. 验证记录

最新静态验证：

```text
bash -n optools/repro/issue25782/*.sh optools/repro/issue25782/tests/run.sh  # PASS
optools/repro/issue25782/tests/run.sh                                        # 22 passed
git diff --check                                                            # PASS
```

`shellcheck` 在当前主机未安装，因此没有 shellcheck 结果。此次未修改 Go 代码，也没有执行直接 `go test`。

## 6. 工作区状态与约束

- worktree：`/home/mo/worktrees/mo-25782-main`
- 分支：`repro/25782-main-harness`
- 锁定代码：`cd741923cd847ae26faacffc5b6adb101bb6dcb7`
- Harness 文件仍全部未提交；未经用户指示不要提交、推送或创建 PR。
- 原工作区 `/home/mo/matrixone` 当前 HEAD 已变为 `8f3aee9cef5d667c6c7686d83cb155d0ba321e1f`，与最初记录的 `c883c48...` 不同。此次工作没有修改、stash 或清理原工作区，因此只能记录该外部漂移，不能再声称它自 harness 创建以来未变化。

## 7. 重新运行

```bash
cd /home/mo/worktrees/mo-25782-main
runtime=/tmp/mo-25782-harness-$(date +%Y%m%d%H%M%S)
./optools/repro/issue25782/prepare.sh --runtime "$runtime"
./optools/repro/issue25782/start.sh --runtime "$runtime"
REPRO_ALLOWED=1 ./optools/repro/issue25782/run.sh --runtime "$runtime"
```

默认 `run.sh` 会清理 SQL 数据并停止整个 runtime；最终以结果目录中的分类和 `manifest.final_telemetry` 为准。

## 8. 内核修复计划

当前已完成第一阶段内核 hard-fail safety slice；完整方案仍按单 PR、多 commit 推进。尚未完成的核心项是 pre-dispatch query lease、Shuffle 有界 spill/re-spill、observability 和 fixed E2E acceptance。详见 [`FIX_PLAN.md`](FIX_PLAN.md)。
