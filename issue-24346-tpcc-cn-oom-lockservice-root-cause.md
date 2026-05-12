# Issue #24346 TPCC consistency failure 根因分析

## 结论

这次 `mo-main-commit-95f75847d-20260510` TPCC 100W/1000 terminals 的 consistency failure，根因可以基本敲死：

> **三个 TP CN 在同一个故障窗口内发生 OOMKilled/restart，导致 CN lockservice endpoint 短暂不可达。重启期间 remote lock unlock/cleanup 失败，TN lock table allocator 随后把相关表的 lock table bind 更新到新 service version。已有事务仍携带旧 bind 继续执行或提交，虽然 TN 在 commit 阶段能拒绝一部分旧 bind 事务，但 CN 侧 remote lock、orphan cleanup、事务重试/回滚之间出现不一致窗口，破坏了 TPCC 关键行锁的串行化，最终同一 district 重复分配 `o_id`，并引发 warehouse/district ytd 不一致。**

这不是 DN/TAE 静默写坏，也不是单纯客户端断连导致的校验误报。真正的数据一致性破坏发生在 CN OOM/restart 引起的 **lockservice remote bind 迁移窗口**。

---

## 一、已确认事实

### 1. 三个 TP CN 都发生了 OOMKilled

Grafana Crash Analysis 已确认三个 `nightly-regression-dis-tp-cn-*` pod 的 `Terminated Reason` 均为 `OOMKilled`。

受影响 CN：

- `nightly-regression-dis-tp-cn-zph8b`
- `nightly-regression-dis-tp-cn-czn58`
- `nightly-regression-dis-tp-cn-s7xmn`

这说明本次不是 MO 自己主动退出，也不是普通网络抖动，而是容器被 cgroup/memcg 直接杀死后重启。

### 2. DN 没有 OOM，也没有发现 TAE/WAL/commit/rollback 层面的异常

`Explore-logs-dn-1.txt` 和 `Explore-logs-dn-2.txt` 覆盖了关键窗口：

- `2026-05-11 01:43:03 +08` 到 `2026-05-11 01:49:30 +08`

DN 日志中没有看到：

- DN OOM/restart；
- TAE panic/fatal；
- WAL/checkpoint 异常；
- commit/rollback 失败导致的数据层错误；
- MVCC visibility 相关明显错误。

DN 的业务表相关日志主要是正常的 flusher、merge、checkpoint、WAL truncate、`CN-COMMIT-S3-Data` 和 logtail subscription。

因此 DN/TAE 不是第一故障点。

### 3. DN 侧真正有价值的证据是 lock table bind 变化

DN lockservice allocator 明确记录了 CN lockservice 不可达和 bind 迁移。

关键日志：

```text
01:46:23 +08
tn-service.lock-client init remote connection failed
remote=10.143.206.253:6003
error="connect: connection refused"
```

这说明 DN 也确认 `zph8b` 的 lockservice endpoint 已不可达。

随后：

```text
01:46:25 +08
bind updated with newer service version
table=272693
service=177843518455603480761366236-3132-3563-3639-623066383162

01:46:25 +08
bind changed
table=272693
version=1778425409166972664
```

表 ID 映射：

```text
272693 = bmsql_oorder
```

也就是说，`bmsql_oorder` 的 lock table bind 已经从旧 zph8b service version 切到了重启后的新 zph8b service version。

紧接着：

```text
01:46:26 +08
table and service bind changed
current=0-272693(...)-1778435184556034807...-1778425409166972664
received=0-272693(...)-1778425459353781269...-1778425409166972663

01:46:26 +08
service_cn_handler.go:214
error: lock table bind changed
```

这说明有事务仍携带旧 `bmsql_oorder` bind 去 commit，TN 在 commit 阶段识别到 bind changed 并拒绝提交。

再之后：

```text
01:46:40 +08
get timeout services timeout=10s count=1
bind disabled lockservice=1778425459353781269...
service removed service=1778425459353781269...
```

旧 zph8b lockservice 被 allocator 判定超时并移除。

同类现象也出现在 `czn58`：

```text
01:48:34 +08
tn-service.lock-client init remote connection failed
remote=10.143.206.251:6003
error="connect: connection refused"

01:48:35 +08
bind updated with newer service version
table=272596

01:48:35 +08
bind changed
table=272596
```

表 ID 映射：

```text
272596 = bmsql_warehouse
```

这与 consistency check 中 warehouse/district ytd 不一致能对应上。

---

## 二、故障时间线

### 01:43:33 +08 左右：TPCC 100W/1000 terminals 开始

GitHub Actions TPCC 日志显示：

- 100W/100 terminals 先通过 consistency check；
- 随后启动 100W/1000 terminals；
- 01:46:21 +08 前吞吐仍正常。

### 01:46:20~01:46:24 +08：`zph8b` OOMKilled/restart

CN 日志显示：

- `Explore-logs-zph8b-oom-bf.txt` 中旧进程日志在 `01:46:20 +08` 左右结束；
- `Explore-logs-zph8b-restart.txt` 在 `01:46:24 +08` 出现 `MO logger init`；
- Grafana Crash Analysis 确认该 CN terminated reason 为 `OOMKilled`。

与此同时，其他节点开始大量报到 `zph8b:6003` 的连接错误：

```text
remote=10.143.206.253:6003
connection reset by peer
connect: connection refused
```

### 01:46:23~01:46:24 +08：remote lock/unlock 大面积失败

`czn58` 日志中，针对旧 zph8b bind 的 remote unlock 大量失败：

```text
txn failed to unlock table on remote
binding=0-272693(272693)-1778425459353781269...-1778425409166972663
error="backend connection closed"
```

这里的 `272693` 是 `bmsql_oorder`。

同时还能看到：

```text
found orphans txns
failed to lock on remote
connect to remote failed
init remote connection failed, retry later
```

这说明旧 zph8b 上仍存在 remote lock/remote txn 状态需要清理，但 owner CN 已经被 OOMKilled，remote unlock 和正常清理路径都不可靠。

### 01:46:25~01:46:40 +08：TN allocator 切换并移除旧 zph8b lockservice

DN lockservice allocator 在 01:46:25 更新了 `bmsql_oorder` 的 bind：

```text
table=272693=bmsql_oorder
old service timestamp=1778425459353781269
new service timestamp=1778435184556034807
```

01:46:26，有事务仍带旧 bind 提交，被 TN 返回：

```text
ErrLockTableBindChanged
```

01:46:40，旧 zph8b service 超时后被 disabled/removed。

这证明当时确实处在 lock table bind 新旧切换窗口。

### 01:46:32 +08 开始：业务执行期已经出现 ACID 异常

关键点：duplicate key 不是 consistency check 时才发现，而是在 workload 仍在执行时就已经出现。

日志中 `bmsql_oorder` 开始出现重复：

```text
Duplicate entry '(35,8,3141)'
Duplicate entry '(35,8,3142)'
Duplicate entry '(35,8,3143)'
Duplicate entry '(35,8,3144)'
Duplicate entry '(35,8,3145)'
Duplicate entry '(35,8,3148)'
```

这些错误来自：

```text
dedupjoin/join.go:625
frontend/txn.go:63
frontend/util.go:469
```

这说明多个 NewOrder 事务拿到了同一个 `(w_id=35, d_id=8)` 的 `d_next_o_id`。

正常情况下，TPCC NewOrder 需要通过 district 行锁串行化：

1. 锁住对应 `bmsql_district` 行；
2. 读取 `d_next_o_id`；
3. 插入 `bmsql_oorder` / `bmsql_new_order` / `bmsql_order_line`；
4. 更新 `d_next_o_id = d_next_o_id + 1`；
5. 提交后下一个事务才能拿到下一个 id。

同一个 `(w_id,d_id)` 连续出现重复 `o_id`，说明这个串行化保护已经被破坏。

### 01:48:34~01:48:40 +08：`czn58` 也出现 lockservice 不可达和 bind 切换

DN 侧记录：

```text
remote=10.143.206.251:6003
connect: connection refused
```

随后：

```text
table=272596=bmsql_warehouse
bind updated with newer service version
bind changed
```

这与后续 consistency check 中的 warehouse/district ytd mismatch 对应。

### 01:48:45 +08：consistency check 失败

最终 TPCC consistency check 报：

- `(w_id=35,d_id=8)` 的 order/new_order/district counter 不一致；
- 多个 warehouse 的 `w_ytd` 与 `sum(district.d_ytd)` 不一致；
- 还有 duplicate visible warehouse rows 相关异常。

这不是校验误报，而是 01:46:32 起 workload 执行期已经产生的数据一致性问题在最终校验阶段暴露。

---

## 三、为什么不是 DN/TAE 根因

### 1. DN 没有对应的数据层错误

在 01:43~01:49 的 DN 日志中，没有看到：

- TAE panic/fatal；
- WAL append/truncate failed；
- checkpoint failed；
- commit/rollback failed；
- MVCC visibility wait/abort 异常；
- duplicate/dedup 异常。

DN 只是正常执行后台 flush/merge/checkpoint/logtail，并记录 lockservice allocator 的 bind 状态变化。

### 2. TN commit 阶段反而有保护动作

`service_cn_handler.go` commit 逻辑会在 commit 前校验事务携带的 lock table bind：

```go
invalidBinds, err := s.allocator.Valid(
    request.Txn.LockService,
    request.Txn.ID,
    request.Txn.LockTables,
)
if len(invalidBinds) > 0 {
    response.CommitResponse.InvalidLockTables = invalidBinds
    response.TxnError = txn.WrapError(moerr.NewLockTableBindChanged(ctx), 0)
    return nil
}
```

DN 日志中 01:46:26 的 `error: lock table bind changed` 就是这个保护生效的证据。

因此不能说 TN 完全没有发现旧 bind；问题在于：

> **在 CN OOM/restart 和 bind 切换期间，部分事务在 commit 前的执行阶段已经基于异常的 remote lock 状态继续推进，造成了业务层序列化破坏。**

### 3. Duplicate 发生时间早于最终校验

如果是 DN/TAE 在最终读取时可见性错乱，duplicate 更可能只在校验查询里暴露。

但当前日志显示：

- 01:46:32 开始，业务 SQL 执行期已经出现 `Duplicate entry '(35,8,3141)'`；
- 01:48:45 才做 consistency check。

这说明异常已经在业务执行阶段形成，不是最后校验读错。

---

## 四、真正的故障链路

可以把链路归纳为：

```text
高并发 TPCC
  -> 三个 TP CN 内存打满
  -> CN 被 OOMKilled
  -> lockservice endpoint 6003 不可达
  -> remote lock/unlock 失败
  -> old lock table bind 上残留 remote lock/txn 状态
  -> CN 重启后以同 UUID + 新 timestamp 注册新 service version
  -> TN allocator 更新 bind
  -> 部分事务携带旧 bind 继续执行/提交
  -> commit 阶段部分事务被 ErrLockTableBindChanged 拒绝
  -> 但执行阶段的行锁串行化窗口已经被破坏
  -> NewOrder 重复分配 d_next_o_id
  -> Payment/warehouse 相关锁也在后续 CN OOM 中受影响
  -> consistency check 失败
```

核心不是“连接断了所以客户端报错”，而是：

> **连接断裂发生在 lockservice owner CN 被 OOMKilled 的窗口，remote lock 的 owner 状态、requester 状态、TN allocator bind 状态、事务重试/回滚状态没有形成一个完全原子的切换协议。**

---

## 五、关键证据汇总

### OOM 证据

- Grafana Crash Analysis 显示三个 TP CN `Terminated Reason = OOMKilled`。
- zph8b 旧进程日志在 01:46:20 左右结束，新进程 01:46:24 `MO logger init`。
- cleanup snapshot 中 TP CN restart count 非 0。

### CN lockservice 证据

`czn58` 在 zph8b OOM 后大量报：

```text
connect to remote failed 10.143.206.253:6003
init remote connection failed, retry later
txn failed to unlock table on remote
found orphans txns
failed to lock on remote
```

旧 bind 指向：

```text
0-272693(272693)-1778425459353781269...-1778425409166972663
```

即旧 zph8b 上的 `bmsql_oorder` lock table bind。

### DN lock table allocator 证据

DN 确认 zph8b lockservice 不可达：

```text
remote=10.143.206.253:6003
connect: connection refused
```

DN 更新 `bmsql_oorder` bind：

```text
table=272693=bmsql_oorder
bind updated with newer service version
bind changed
```

DN 拒绝旧 bind commit：

```text
table and service bind changed
error: lock table bind changed
```

DN 10s 后移除旧 service：

```text
get timeout services
bind disabled
service removed
```

### 业务异常证据

NewOrder 业务执行期已经出现：

```text
Duplicate entry '(35,8,3141)'
Duplicate entry '(35,8,3142)'
Duplicate entry '(35,8,3143)'
Duplicate entry '(35,8,3144)'
Duplicate entry '(35,8,3145)'
Duplicate entry '(35,8,3148)'
```

这说明同一 `(w_id=35,d_id=8)` 的 `d_next_o_id` 被重复分配。

---

## 六、最终判断

最终根因可以写成：

> **TPCC 高并发下三个 CN 发生 OOMKilled/restart，导致 CN lockservice remote owner 短暂丢失。旧 service version 上的 remote lock/unlock/orphan cleanup 与新 service version 的 lock table bind 生效之间存在不一致窗口。虽然 TN commit 阶段能够检测并拒绝部分旧 bind 事务，但 CN 执行阶段的锁状态已经出现错乱，导致 TPCC district/warehouse 行锁串行化失效，进而产生重复 `o_id` 和 ytd 不一致。**

其中：

- **触发因素**：三个 TP CN OOMKilled。
- **直接故障点**：CN lockservice endpoint 不可达、remote lock/unlock 失败、bind changed。
- **正确性破坏点**：lockservice bind 切换窗口内，旧 bind 事务和新 bind 事务之间未能保持严格互斥/原子切换。
- **表现结果**：NewOrder 重复分配 `o_id`，Payment/warehouse 更新不一致，最终 TPCC consistency check failed。

---

## 七、建议后续排查/修复方向

### 1. lockservice bind 切换必须成为强一致屏障

当某个 CN lockservice service version 被判定失效并切到新 version 时，需要确保：

- 旧 bind 上仍持有/等待的 remote locks 不会和新 bind 上的新锁并行生效；
- 携带旧 bind 的事务不能在执行阶段继续产生业务写入；
- requester 侧遇到 `BackendClosed` / `BackendCannotConnect` / `ErrLockTableBindChanged` 后，必须可靠地终止当前语句或事务，而不是仅局部 retry。

### 2. remote unlock 失败不能只依赖异步重试

日志中大量：

```text
txn failed to unlock table on remote
error="backend connection closed"
```

说明 requester 已经无法清理 owner 上的旧 remote lock。

如果 owner 已 OOM，远端状态实际已经丢失；如果 owner 未完全退出，则可能留下 stale holder。

这里需要确认：

- old bind disabled 后，所有旧 bind holder 是否有强制失效路径；
- orphan cleanup 是否以 bind version 为粒度，而不是只以 remote txn/service alive 为判断；
- unlock 失败时是否会影响事务后续状态机。

### 3. commit 阶段保护不够，执行阶段也要保护

TN commit 阶段已经能返回 `ErrLockTableBindChanged`，但 duplicate 在 workload 执行期已经出现。

因此只在 commit 校验 bind 不够，还要检查：

- lock 获取失败后的 SQL 是否可能继续执行；
- lock table bind changed 是否一定导致语句/事务 rollback；
- 显式事务内是否存在局部 retry 后继续使用旧 snapshot/旧 lock 状态的路径；
- `ErrBackendClosed` / `ErrBackendCannotConnect` 是否在所有 lock path 上都被视为强制终止事务的错误。

### 4. OOM 仍需要单独治理

本次一致性问题由 OOM 触发，但 OOM 本身也是必须解决的稳定性问题。

建议同时保留两个方向：

- **稳定性治理**：降低 TPCC 高并发下 CN 内存峰值，避免三个 CN 接连 OOM。
- **正确性治理**：即使 CN OOM/restart，也不能破坏事务/锁的串行化语义。

---

## 八、一句话版本

这次 issue #24346 不是 DN 写坏，而是 **三个 CN OOMKilled 后 lockservice remote bind 迁移期间的锁一致性漏洞**：旧 bind 的 remote lock 清理失败，新 bind 很快生效，部分事务在新旧 bind 切换窗口内破坏了 TPCC 关键行锁串行化，导致 `d_next_o_id` 重复分配和 ytd 不一致。
