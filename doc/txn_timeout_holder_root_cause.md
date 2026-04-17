# 基于 `Explore-logs-txn.txt` / `Explore-logs-txn-detail.txt` / `Explore-logs-tn.txt` 的超时后 holder 泄漏根因分析

## 分析范围

这份文档最初的主结论来自：

- `Explore-logs-txn.txt`
- `Explore-logs-txn-detail.txt`

这次补充修订又加入了：

- `Explore-logs-tn.txt`

用于解释同一时间窗口内 **CN logtail pause / resume** 的上游原因。

仍然**不依赖** proxy 日志、其它 CN 日志或额外 profile 文件。

---

## 结论先行

这次问题的**根因不是 lock retry 本身**，也不能简单归因为“业务没有 commit”。

更准确的根因链是：

1. 业务 SQL `INSERT INTO task_log ...` 在执行过程中进入了 **auto-increment 元数据更新** 路径；
2. 这条内部路径在 `doUpdate` 上触发了 **10 秒超时**；
3. 同一时间窗口内，TN 侧旧 logtail stream 对这台 CN **长时间没有 forward progress**，随后 CN 侧出现 **logtail consumer / txn client pause -> resume**；
4. TN 侧这次旧流卡死不是单条连接的偶发抖动，而是和 **多条 logtail blocked、commit 超慢、WAL client timeout、cleanCommitState timeout** 同时出现的系统性退化；
5. 同一 session / connection 上的后续 txn（`e594...0478`）在恢复后继续变成 Active，并最终拿到锁；
6. 但在 CN 这条会话链上，**始终没有看到该 session / connection 对应的最终 cleanup / rollback 收尾日志**；
7. 结合 frontend 代码，cleanup 依赖 `Conn.Close() -> rt.cleanup()`，而 frontend 读包和执行请求是**同一个串行 goroutine**。如果执行线程卡在中间路径，没有返回到读 socket 的位置，就可能**感知不到对端断开，也进不到 cleanup**；
8. 于是 txn context 继续存活，holder 不释放，最终表现为长时间泄漏。

所以：

- **`doUpdate 10s timeout` 是触发点**
- **TN / logtail 路径在同一窗口内存在明显系统性退化**
- **`disconnect / cleanup` 没有真正打到这条卡住的执行链，才是 holder 长时间不释放的根本原因**

---

## 关键对象

- 会话：`019d91c4-706e-7a70-848a-022b3ec59335`
- 连接：`3442154071`
- CN：`64333364-6638-6334-6362-656134303537`
- TN：`33376565-3237-3162-3439-323631326537`
- 可见失败语句：`execute __mo_stmt_id_120 // INSERT INTO task_log ...`
- 失败时前台 txn：`e5948d2b756a9dff18a660ffec0a045e`
- 同窗口提交/解锁异常 txn：`e5948d2b756a9dff18a660ffec0a0460`
- 最终 leaked holder txn：`e5948d2b756a9dff18a660ffec0a0478`
- 旧 logtail stream：`172.20.117.8:57242`
- 新 logtail stream：`172.20.117.8:39090`

---

## 时间线

| 时间点 | 证据 | 说明 |
| --- | --- | --- |
| `15:31:03.410475` | `Explore-logs-txn.txt:6` / `Explore-logs-txn-detail.txt:6` | 连接 `3442154071` 被挂到账户 routine map。 |
| `15:31:03.411063` | `Explore-logs-txn.txt:7` / `Explore-logs-txn-detail.txt:7` | CN 接收该连接，会话 `019d91c4-706e-7a70-848a-022b3ec59335` 建立。 |
| `15:32:31.821973` | `Explore-logs-tn.txt:74` | TN 对这台 CN 的旧 logtail stream `172.20.117.8:57242` 报 `send logtail channel blocked`。 |
| `15:32:41.822100` | `Explore-logs-tn.txt:85` | 同一旧 stream 再次 blocked。 |
| `15:32:51.822756` | `Explore-logs-tn.txt:108` | 同一旧 stream 再次 blocked。 |
| `15:32:51.686919` | `Explore-logs-tn.txt:106` | TN 报 `WAL-Replay failed to create log service client (attempt 1/300): timeout`。 |
| `15:33:01.823721` | `Explore-logs-tn.txt:134` | 同一旧 stream 继续 blocked。 |
| `15:33:03.179195` | `Explore-logs-tn.txt:143` | `cleanCommitState` 再次 `context deadline exceeded`。 |
| `15:33:11.840005` | `Explore-logs-tn.txt:153` | 同一旧 stream 继续 blocked。 |
| `15:33:13.640523` | `Explore-logs-txn-detail.txt:65` | txn `...0460` 在 `Commit` 发送请求时超时。 |
| `15:33:13.640580` | `Explore-logs-txn-detail.txt:66` | txn `...0460` 在 `wait committed log applied` 阶段再次报 `context deadline exceeded`。 |
| `15:33:13.640864` | `Explore-logs-txn.txt:8` / `Explore-logs-txn-detail.txt:67` | 前台明确报错：`context deadline exceeded / internal error: doUpdate`，txn 为 `...045e`。 |
| `15:33:13.640899` | `Explore-logs-txn.txt:9` / `Explore-logs-txn-detail.txt:68` | 失败语句被明确记录为 `INSERT INTO task_log ...`。 |
| `15:33:13.642107` | `Explore-logs-txn-detail.txt:69` | frontend 发起 `Transaction.Rollback`，txn 为 `...045e`。 |
| `15:33:13.726207` | `Explore-logs-txn.txt:15` | leak checker 回填：txn `...0478` 的 create-at 是 `15:33:13.726207`。 |
| `15:33:13.179319` | `Explore-logs-tn.txt:161` | TN 仍在报 `cleanCommitState` 超时。 |
| `15:33:18.198344` | `Explore-logs-tn.txt:163` | 还有其它 remote 的 logtail channel blocked。 |
| `15:33:21.821955 ~ 15:33:21.822024` | `Explore-logs-txn-detail.txt:71-75` | CN 侧 `ReceiveOneLogtail` 超时，receiver paused，随后 `txn client status changed to paused`。 |
| `15:33:21.841125` | `Explore-logs-tn.txt:167` | 旧 stream `172.20.117.8:57242` 在 TN 侧仍 blocked。 |
| `15:33:21.864480` | `Explore-logs-tn.txt:189` | TN 为同一 CN 建立新 logtail stream `172.20.117.8:39090`。 |
| `15:33:21.865107` | `Explore-logs-tn.txt:193` | TN 立即开始给新 stream 回 `mo_database`。 |
| `15:33:21.923644` | `Explore-logs-tn.txt:213` | TN 新 stream 回 `mo_tables`。 |
| `15:33:21.925348` | `Explore-logs-tn.txt:223` | TN 新 stream 回 `mo_columns`。 |
| `15:33:22.625397` | `Explore-logs-tn.txt:235` | TN 再次报 `WAL-Replay failed to create log service client (attempt 2/300): timeout`。 |
| `15:33:23.001518` | `Explore-logs-txn-detail.txt:100` | txn `...047b` 因 txn client pause 被阻塞。 |
| `15:33:23.628994 ~ 15:33:23.633735` | `Explore-logs-tn.txt:237-239` | TN `Wal-Get-Client` / `commit txn too slow` 达到 **1 分钟级别**。 |
| `15:33:23.634365` | `Explore-logs-tn.txt:240` | TN 清理旧 stream `172.20.117.8:57242`。 |
| `15:33:25.397686` | `Explore-logs-txn-detail.txt:110` | txn `...047c` 同样因 txn client pause 被阻塞。 |
| `15:33:31.902864` | `Explore-logs-txn-detail.txt:125` | CN `txn client` 恢复到 normal。 |
| `15:33:31.902914 ~ 15:33:31.903040` | `Explore-logs-txn-detail.txt:127-128` | `...047b` / `...047c` 被唤醒。 |
| `15:33:31.906397` | `Explore-logs-txn.txt:10` / `Explore-logs-txn-detail.txt:137` | `...0478` 首次在 detail 日志里以 Active 形态出现。 |
| `15:33:31.906907` | `Explore-logs-tn.txt:538` | TN 新 stream 给这台 CN 回 `mo_increment_columns`。 |
| `15:33:31.909342` | `Explore-logs-tn.txt:540` | TN 新 stream 给这台 CN 回 `task`。 |
| `15:33:31.909965 ~ 15:33:31.910015` | `Explore-logs-txn-detail.txt:150-151` | `table-id=1563507` 被映射为 `task`。 |
| `15:33:31.922203` | `Explore-logs-tn.txt:546` | TN 新 stream 给这台 CN 回 `task_log`。 |
| `15:33:31.922879 ~ 15:33:31.922920` | `Explore-logs-txn-detail.txt:183-184` | `table-id=1400770` 被映射为 `task_log`。 |
| `15:33:39.374674` | `Explore-logs-txn.txt:15` | leak checker 首次报 `...0478` 为 leak txn。 |
| `15:35:06.014925` | `Explore-logs-txn.txt:20` | lockservice 报 wait-too-long：holder 是 `...0478`，table-id 是 `1563507`，即 `task`。 |

---

## 从日志能直接确定的事实

### 1. 可见失败点是 `doUpdate`，不是 lock retry

`Explore-logs-txn.txt:8-9` / `Explore-logs-txn-detail.txt:67-68` 已经把失败语句和错误原因写死了：

- 错误：`context deadline exceeded`
- 内层原因：`internal error: doUpdate`
- 语句：`INSERT INTO task_log ...`

这说明最早暴露出来的失败点是**内部元数据更新路径超时**，不是 lock retry 自己在无限重试。

### 2. 前台确实尝试过 rollback，但这不等于最终 cleanup 已经完成

`Explore-logs-txn-detail.txt:69` 有：

- `Transaction.Rollback` for txn `...045e`

但在 CN 这条 session / connection 上，没有看到最终收尾日志：

- `rollback the txn.`
- `routine cleanup`
- `clean resource of the connection`
- `the io session was closed`

也就是说，**“报错后发起过 rollback”** 和 **“这条连接最终完成了 cleanup”** 不是一回事。

### 3. `...0478` 不是后来无关的新连接，而是同一条 session / connection 上的 holder

`Explore-logs-txn.txt:15-19,32` 反复报 leak txn `...0478`，并且 options 里明确写着：

- `SessionID:"019d91c4-706e-7a70-848a-022b3ec59335"`
- `ConnectionID:3442154071`

所以 leaked holder `...0478` 就是同一条会话链上的问题，不是别的连接串进来的。

### 4. `...0478` 的锁落在 `task`，而不是表面上失败的 `task_log`

这两个日志一起能看出一个重要细节：

- `Explore-logs-txn-detail.txt:150-151`：`table-id=1563507` 对应 `task`
- `Explore-logs-txn-detail.txt:183-184`：`table-id=1400770` 对应 `task_log`
- `Explore-logs-txn.txt:20-31`：wait-too-long 的 bind 里 table-id 是 `1563507`

也就是说，**前台可见失败 SQL 是 `INSERT INTO task_log`，但最终 leaked holder 卡住的是 `task` 的锁**。

这说明：

- 这条执行链不只是简单插入 `task_log`
- 它在同一故障窗口内还进入了 `task` 相关的数据路径
- 但这件事只解释“锁为什么落在 task 上”，**不改变真正根因仍然是 cleanup 没有走完**

---

## TN 侧补充证据：旧 logtail stream 为什么会卡死

### 1. `send logtail channel blocked` 的含义

`pkg/vm/engine/tae/logtail/service/session.go:284-300,344-349`

- sender 会监控一个 logtail session 的发送进度；
- 如果在观察窗口内没有成功发送，就打印 `send logtail channel blocked`；
- 每次成功发送后，timer 会被重置为 `10s`。

所以：

- **单次、短时间** 的 `send logtail channel blocked`，只能说明瞬时 backpressure 或 send 停顿；
- 像这次这样，同一个 remote `172.20.117.8:57242` 在 `15:32:31 / 15:32:41 / 15:32:51 / 15:33:01 / 15:33:11 / 15:33:21` 连续出现，并且 `sendRound` 一直不变，表示这条旧 stream **持续没有 forward progress**。

这不是健康 steady-state 下的正常表现。

### 2. 这种现象不是“完全不会发生”，但长时间连续出现通常意味着异常退化

从代码语义上讲，短暂 blocked 可能出现在：

- 瞬时网络抖动
- 下游 CN 接收/消费突然变慢
- 短时负载尖峰导致 logtail 发送排队

所以：

- **单次、只影响个别 stream** 的 blocked，可以是可恢复的瞬时背压；
- **持续 50+ 秒、同一 `sendRound` 不前进、并且多条 remote 同时 blocked**，就已经不是“正常慢”，而是明显异常信号。

### 3. 这次不是单条旧流抖动，而是 TN / logservice 路径的系统性退化

`Explore-logs-tn.txt` 同一窗口还出现了：

- 多个 remote 同时报 `send logtail channel blocked`（`71-78`, `166-173`）
- `WAL-Replay failed to create log service client ... timeout`（`106`, `235`）
- `cleanCommitState` 持续 `context deadline exceeded`（`93`, `116`, `143`, `161`, `236`）
- `Wal-Get-Client duration` 与 `commit txn too slow` 达到 **1 分钟级别**（`237-239`, `253-255`）

所以更准确的判断是：

> TN / logservice 路径在当时出现了明显系统性退化；  
> 旧 logtail stream 卡死只是这个退化在 CN->TN logtail 链路上的一个外在表现。

### 4. 这次更像“系统退化”，不是“明确重启 / 切主”

在 `Explore-logs-tn.txt` 里，没有看到明确的：

- TN restart
- leader 切换
- hakeeper 切主

因此，这份日志更支持：

- **严重卡顿 / 资源压力 / 依赖路径超时**

而不是：

- 明确的一次 TN 重启事件

### 5. 为什么旧流没有更早被立刻杀掉

`pkg/vm/engine/tae/options/cfg.go:27-29,97-117` 给出的默认阈值是：

- `ResponseSendTimeout = 1m`
- `RPCStreamPoisonTime = 5s`

另外 `pkg/vm/engine/tae/logtail/service/session.go:566-598` 说明：

- `poison` 检测针对的是**响应无法塞入 sendChan**
- 而这次 `send logtail channel blocked` 反映的是 **sender 已经很久没有成功往外发**

所以这次旧流的实际表现是：

- TN 一直检测到“没有成功发送”
- CN 侧随后先感知到 `ReceiveOneLogtail` 超时并主动重连
- TN 直到收到 error/cleanup 链条后，才把旧流真正清掉

这也解释了为什么你会看到：

- 旧流连续 blocked 很久
- 新流已经建起来了
- 旧流稍后才被 `clean session for morpc stream` 清掉

---

## 对应代码点

### 1. `doUpdate` 的真实含义：在同事务里更新 auto-increment 元数据

`pkg/incrservice/allocator.go:207-214`

```go
ctx, cancel := context.WithTimeoutCause(ctx, time.Second*10, moerr.CauseDoUpdate)
err := a.store.UpdateMinValue(ctx, act.tableID, act.col, act.minValue, act.txnOp)
```

`pkg/incrservice/store_sql.go:259-289`

```go
opts := executor.Options{}.WithDatabase(database).WithTxn(txnOp)
...
res, err := s.exec.Exec(
    ctx,
    fmt.Sprintf("update %s set offset = %d where table_id = %d and col_name = '%s' and offset < %d", ...),
    opts)
```

这里说明两件事：

1. `doUpdate` 确实有 **10 秒超时**
2. 当 `txnOp != nil` 时，这个更新是**在当前业务事务里执行的内部 SQL**，不是独立后台任务

所以日志里的 `internal error: doUpdate`，本质上是在说：

> 这条业务 SQL 在执行过程中，卡在了 auto-increment 元数据更新上。

### 2. frontend 读包与执行请求是串行的

`pkg/frontend/server.go:480-530`

- `handleMessage()` 循环里先 `rs.Read()`
- 然后同步调用 `mo.rm.Handler(rs, msg)`

`pkg/frontend/routine_manager.go:360-385`

- `Handler()` 里同步调用 `routine.handleRequest(req)`

这说明：

> 同一个 frontend goroutine 同时承担“继续读 socket”和“执行当前请求”两件事。

如果当前请求卡住，它就不会再回到 `rs.Read()` 那里去感知对端是否已经断开。

### 3. request ctx 继承自 `txnCtx`，而 `txnCtx` 的生命周期跟连接绑定

`pkg/frontend/txn.go:195-206`

```go
ret.txnCtx, ret.txnCtxCancel = context.WithCancel(connCtx)
```

`pkg/frontend/routine.go:275-278`

```go
cancelRequestCtx, cancelRequestFunc :=
    context.WithTimeoutCause(ses.GetTxnHandler().GetTxnCtx(),
        parameters.SessionTimeout.Duration,
        moerr.CauseHandleRequest)
```

这说明：

- request ctx 不是脱离连接的短生命周期 ctx
- 它直接挂在 `txnCtx` 之下
- 如果连接侧 cleanup 没走到，`txnCtx` 就不会及时被 cancel

### 4. 真正的 cleanup / rollback 依赖请求返回或连接关闭

`pkg/frontend/routine.go:341-365`

只有在 `handleRequest()` 尾部判定 `quit` 时，才会执行：

- `rollback the txn.`
- `TxnHandler.Rollback(...)`
- `proto.Close()`

`pkg/frontend/routine.go:419-474`

真正完整的 `routine cleanup` 发生在：

- `Conn.Close()`
- `RoutineManager.Closed()`
- `rt.cleanup()`

而 `rt.cleanup()` 内部才会：

- 回滚 txn
- 打 `routine cleanup` 日志
- `releaseRoutineCtx()`
- 释放 session

所以只要执行线程没有返回到这个 cleanup 链，holder 就可能继续留着。

### 5. 这个 cleanup gap 之所以能拖很久，是因为默认 `SessionTimeout` 是 24 小时

`pkg/config/configuration.go:126-127`

```go
defaultSessionTimeout = 24 * time.Hour
```

`pkg/config/configuration.go:433-434`

```go
if fp.SessionTimeout.Duration == 0 {
    fp.SessionTimeout.Duration = defaultSessionTimeout
}
```

也就是说，如果执行线程卡住且没有真正走到 cleanup，ctx 并不会在几秒内自然收敛，而是可能拖到非常久。

### 6. `15:33:21 -> 15:33:31` 的 pause/resume 解释了为什么 `0478` 在失败后还能继续活过来

`pkg/vm/engine/disttae/logtail_consumer.go:641-654`

- `logtail receiver paused`
- `logtail receiver resumed`

`pkg/vm/engine/disttae/logtail_consumer.go:888-945`

- reconnect
- clean memory table
- `connected to server`

`pkg/txn/client/client.go:509-526`

- paused 时，新 txn 会 `wait for it to be ready`
- resume 后会打印 `txn client is in ready state`

`pkg/txn/client/client.go:605-623`

- `Pause()` 把 client 状态改成 paused
- `Resume()` 改回 normal，并 `Broadcast()`

这和 `Explore-logs-txn-detail.txt:125-139` 完全对上：

- `15:33:21`：txn client paused
- `15:33:31.902864`：txn client normal
- `15:33:31.902914/.903040`：`047b/047c` ready
- `15:33:31.906397`：`0478` 变成 Active

所以 `0478` 不是凭空冒出来的，而是**同一故障窗口里遗留/排队的 txn，在恢复后继续推进**。

### 7. TN 侧旧流 blocked 与 CN 侧 pause/resume 是同一条链

`pkg/vm/engine/disttae/logtail_consumer.go:657-660`

- `receiveOneLogtail()` 出错后，CN 会触发 `c.pause(...)`

`pkg/vm/engine/disttae/engine.go:855-866`

- push client not ready 时，engine 会 `e.cli.Pause()`
- 恢复 ready 后再 `e.cli.Resume()`

结合 TN 日志，这次链路是：

1. TN 旧流 `172.20.117.8:57242` 长时间 blocked
2. CN 侧随后报 `ReceiveOneLogtail` timeout
3. CN pause txn client
4. TN 建立新流 `172.20.117.8:39090`
5. CN 订阅 / replay 完成后 resume txn client

因此，CN 的 pause/resume 不是独立事件，而是 TN 侧旧 logtail stream 卡死后的直接结果。

---

## 为什么我认为“根因不是 lock retry”

因为这组日志里最关键的失败点和时间顺序都不支持这个结论：

1. **第一个明确失败点是 `doUpdate`**，不是 lock retry 的报错
2. `...045e` 已经被标成 statement fail，`...045e` 也已经尝试 rollback
3. 真正变成 holder 的是**后续同 session 的 `...0478`**
4. `...0478` 之所以能继续活着并持锁，不是因为 retry 一直转，而是因为**cleanup 没有把这条会话真正收掉**
5. retry 逻辑是否退出，前提都是 **ctx 已经 done**；而这里最核心的问题是 **ctx 没有被及时打断**

所以 lock retry 可能只是症状放大器，不是这次案例的第一根因。

---

## 最终判断

结合 `Explore-logs-txn.txt`、`Explore-logs-txn-detail.txt` 和 `Explore-logs-tn.txt`，我认为这次问题最准确的判断是：

> `INSERT INTO task_log ...` 在 auto-increment 的 `doUpdate` 内部路径触发了 10 秒超时；  
> 同时 TN / logtail 路径出现系统性退化，旧 logtail stream 卡死，CN 因此发生了 pause / resume；  
> 同 session 上的后续 txn（`...0478`）在恢复后继续进入 Active；  
> 但该 session / connection 没有在日志中出现最终 cleanup / rollback 收尾证据；  
> 结合 frontend 串行 read+execute 以及 cleanup 依赖 `Conn.Close()` 的代码实现，可以判断 holder 长时间不释放的真正根因仍然是**超时窗口内的连接/会话 cleanup 没有真正打到卡住的执行链**。

换句话说：

- **触发点**：`doUpdate` 10 秒超时
- **上游背景**：TN / logtail 路径当时处于异常退化，而不是正常 steady-state 的轻微慢
- **根本原因**：`disconnect / cleanup gap`
- **最终表现**：同 session 的 `...0478` 变成 leaked holder 并卡住 `task` 的锁

---

## 修复抓手

如果要“一次修好”，修复重点不该只放在 lock retry，而应该放在下面这条链：

1. **客户端/代理侧断连后，必须保证 CN 上对应 connection 能被 out-of-band 地 kill / cleanup**
2. **不能把 frontend cleanup 只绑定在“执行线程自己返回后再处理”**
3. **要保证卡在锁等待 / 远端 RPC / logtail 故障窗口里的会话，也能被异步打断并触发 rollback / unlock**

否则：

- 即使 retry 逻辑已经修过
- 只要 `ctx` 还没被真正 cancel
- holder 仍然会继续活着

---

## 当前修复的影响面

当前已经落地的修复思路是：**仅在 proxy 识别到 client disconnect 时，定向对同一个 backend CN 发 `KILL CONNECTION <原 connection_id>`**。

这条修复的影响面是收敛的：

- 只在 **client disconnect** 路径触发，不影响正常执行路径
- 只打到 **当前正在服务该连接的 backend CN**
- 使用的是 **原始 connection_id**，不会扫全局，也不会误伤其它 session
- 如果 kill 失败，只记 warning，不改变正常连接关闭语义

所以它更像是：

- 对 `disconnect / cleanup gap` 的**定向兜底**

而不是：

- 对所有请求路径增加新的通用逻辑分支
