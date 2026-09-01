
## P1 provisional/committed 持久化（追加）

- 在发布 Fenced 前持久化 provisional v38；失败则绝不发布 Fenced。
- 全体 provisional Fence 收敛后持久化 committed v38；失败保持 ingress/DDL fail-closed。
- restart 对 committed 执行 startup frontier fence 后开放；对 provisional 恢复 v38 capability 但保持 ingress/public DDL 关闭，等待完整 activation retry。
- restart regression 必须创建新 service 并调用 initMetadata()，另覆盖 provisional/committed persist failure。

## 2026-08-30：最新主干冲突与 review comments

1. Merge 最新 `mo/main`，保留主干 v38/v39 语义，将 DDL visibility protocol 完整迁移到下一个版本 v40。
2. 在 ctl dispatch 前读取 raw CN inventory，要求 activation target 精确覆盖全部仍可能公开产出 DDL 的 CN，并校验 generation/address 与 receiver capability。
3. 在 startup ingress publication 前重新读取权威 cluster epoch，关闭 markerless join 与最终 epoch commit 之间的 TOCTOU；若 cut 已提交则转为 v40 fail-closed，不能开放 ingress。
4. 增加版本兼容、遗漏 legacy producer、markerless join/commit 交错回归；执行相关 UT、race、vet、自审后 push，并 resolve threads/request re-review。

## 2026-08-30：再次解决主干冲突

1. Merge 最新 `mo/main`，按主干协议版本与生成文件保留双方语义。
2. 重新生成受影响生成文件，运行冲突闭包测试与 `git diff --check`。
3. Push PR 分支并确认恢复 mergeable。

## 2026-08-30：原子 membership/epoch commit

1. 将 activation target 的 service ID、generation、query address 作为 epoch commit proof 传入 HAKeeper heartbeat transition。
2. HAKeeper RSM 在同一 transition 内按 raw CNState 精确校验 membership/capability tuple；仅校验成功时推进 cluster epoch。
3. CN 必须先获得 RSM 返回的 committed epoch，再持久化本地 committed marker 和开放 ingress；拒绝时保持 provisional/fail-closed。
4. 增加 final scan 后、epoch commit 前 markerless join 的确定性回归，并运行相关 UT/race/vet。

## 2026-08-30：frontend txn 测试冲突

1. 合并最新 `mo/main`，在 `pkg/frontend/txn_test.go` 同时保留 DDL sync 回归与主干 panic rollback 回归。
2. 运行 frontend 冲突闭包测试、vet 和 diff check，随后 push。

## 2026-08-30：最新 deep-review P1 与设计 gate

1. markerless 首次升级 CN 保留当前主干已部署协议 baseline（至少 v40），仅将 v41 DDL activation 状态保持未完成/fail-closed，不回退共享 scalar 到 v37。
2. HAKeeper monotonic epoch 已达 v41 后拒绝任何局部低版本 downgrade，避免现有会话继续作为无 fan-out DDL producer。
3. 增加 baseline preservation、post-cut downgrade rejection 回归。
4. 新增稳定版本化设计文档，说明状态机、rollout/rollback、direct/proxy ingress、复杂度/延迟、观测与恢复；补充确定性 two-CN 自动 DDL create/read 公共路径测试。
5. 合并最新主干，执行相关 UT/race/vet/self-review，push 并请求 re-review。

## 2026-08-30：v41 公共路径回归与最新 review

1. 查询并分析 PR 最新 inline/general review comment，更新变更闭包与验收条件。
2. 修复 two-CN 公共路径测试发现的生产激活协调阻塞：保持 activation/startup/shutdown 串行所有权，同时允许心跳在长阶段等待期间发布 Prepared/Fenced 状态，避免用重试或弱化断言掩盖问题。
3. 通过生产 `mo_ctl SetProtocolVersion` 激活精确 CN 集合，并用故障注入证明 CREATE DDL 实际经过 v41 `SyncCommitV2`，随后验证另一 CN 首次读取可见。
4. 补齐心跳/RSM/超时/并发 UT，运行生成、focused/owning package、race、vet、embedded topology 与 self-review；通过后 commit/push、处理 review thread 并请求 re-review。

## 2026-08-30：处理新一轮 review comments

1. 获取 exact-head 最新 review 与 unresolved threads，逐条复现并确认根因。
2. 对确认问题做最小完整修复，补充对应确定性 UT/拓扑回归。
3. 运行受影响 owning package、race、vet、生成文件与 diff 检查。
4. commit/push，逐条英文回复并 resolve，重新请求 review。

进度（2026-08-31）：
- 已将 DDL frontier 提升为 HAKeeper RSM 中独立于 CN store 生命周期的单调集群最大值；activation/restart 先从 txn client 重建 durable high-water mark，再发布并同步该全局值。
- 已增加 producer replacement/store removal 不得清空 frontier 的 RSM 回归，以及 replacement 后 activation 仍等待旧 frontier 的 CN 回归。
- 按用户决定不再等待其他并发 PR：本 PR 固定使用当前无人占用的 MORPC v43，并为两个已在 review 的并发 owner 预留 v41/v42；同步更新实现、测试、设计与生产路径回归。

## 2026-08-31：解决最新主干冲突

1. fetch 并 merge 最新 `mo/main`，逐文件确认双方语义，保留主干新增能力与本 PR v43 DDL fence。
2. 重新生成必要生成文件，运行冲突相关 owning package、embedded 双 CN、race/vet 与 diff 检查。
3. commit/push merge 结果，确认 PR mergeability 与 unresolved thread 状态。

## 2026-08-31：修复最新 CI failure

1. 获取 PR exact head 的失败 check/job 原始日志，区分代码回归与基础设施问题并定位最小根因。
2. 对代码问题补充确定性回归并修复；不使用重试或弱化断言掩盖失败。
3. 运行 owning package、相关 race/vet/embedded 验证，commit/push 后确认新 CI 状态。

## 2026-09-01：frontier publication generation fence

1. 同步 frontier heartbeat 必须验证 `CommandBatch.ViewMetadataAdmission.Generation` 与本地 incarnation generation 完全一致。
2. HAKeeper 返回更高 authoritative generation 时立即 revoke 旧 incarnation 并向 frontend 返回错误，禁止继续 fan-out/成功确认。
3. 增加 generation N commit → N+1 takeover → N publication rejected → N crash → restart 仅同步旧 durable frontier 的确定性回归，并运行 owning package/race/vet。

## 2026-09-01：generation revocation TaskRunner self-wait

1. 将 revocation 拆为同步不可逆 admission seal 与异步 frontend/TaskRunner/full-service drain。
2. 同步 frontier rejection 只等待 seal，不得等待可能包含当前 SQL DDL 调用栈的 TaskRunner.Stop。
3. 增加 TaskRunner SQL DDL 自身发现 stale generation、rejection 先返回、task 退出后异步 Stop 才完成的确定性 Q2 回归，并运行 race/vet。

## 2026-09-01：revoked TaskRunner 与正常 Close 协调

1. 在 `s.task` 下建立 detached revoked-runner 的共享 ownership/completion；异步 revocation drain 是唯一 Stop owner。
2. 正常 `stopTask` 在锁内转移普通 runner ownership，锁外执行 holder.Close/runner.Stop；若 runner 已由 revocation 摘除，则等待共享 completion 后才允许关闭下游依赖。
3. 分别覆盖 stopTask 先取得 runner 与 revocation 先摘除 runner 两种顺序，证明无持锁等待、无 double Stop、无依赖提前关闭。

## 2026-09-01：新增 review blockers（frontier ack / heartbeat response / v44）

1. 将 DDL capability 从与 #27553 冲突的 v43 移至下一未占用 v44，并迁移全部 gate、method map、测试、`mo_ctl` 路径及设计文档。
2. 在同一 HAKeeper RSM heartbeat 响应中返回应用后的 authoritative frontier；CN 仅在 generation 精确匹配且 frontier >= T 时承认同步发布，旧 RSM 静默丢字段时 fail closed。
3. 所有 direct CN heartbeat 将 CommandBatch 交给现有 command mutex/dedupe owner，避免 command-delivery-disabled 阶段的 destructive schedule command 丢失。
4. 增加旧 HAKeeper 丢 frontier 与 legacy command delivery 的确定性回归，运行 affected package、race、vet 和 protobuf 测试。
