# Checkpoint 模块可靠性问题

## 1. 崩溃恢复问题

### 1.1 🔴 检查点日志条目可能丢失

**位置**: `executor.go` - `RunICKP`

```go
defer func() {
    runner.store.CommitICKPIntent(entry)
    runner.postCheckpointQueue.Enqueue(entry)
    runner.TryTriggerExecuteGCKP(&gckpContext{...})
}()

v2.TaskCkpEntryPendingDurationHistogram.Observe(entry.Age().Seconds())

files = append(files, file)

// PXU TODO: if crash here, the checkpoint log entry will be lost
var logEntry wal.LogEntry
if logEntry, err = runner.wal.RangeCheckpoint(1, lsnToTruncate, files...); err != nil {
    errPhase = "wal-ckp"
    fatal = true
    return
}
```

**问题**:
- 在 `saveCheckpoint` 之后、`RangeCheckpoint` 之前崩溃
- 检查点数据已写入存储，但 WAL 中没有记录
- 恢复时可能导致数据不一致

**影响**:
- 检查点元数据文件存在但 WAL 不知道
- 可能导致重复的检查点或数据丢失

**建议**: 使用两阶段提交

```go
func (job *checkpointJob) RunICKP(ctx context.Context) (err error) {
    // Phase 1: Prepare
    // 写入检查点数据，但标记为 "pending"
    if file, err = runner.saveCheckpointPending(entry.start, entry.end); err != nil {
        return
    }
    
    // Phase 2: WAL
    // 写入 WAL 日志
    if logEntry, err = runner.wal.RangeCheckpoint(1, lsnToTruncate, files...); err != nil {
        // 回滚：删除 pending 文件
        runner.removePendingCheckpoint(file)
        return
    }
    
    // Phase 3: Commit
    // 将 pending 文件重命名为正式文件
    if err = runner.commitCheckpoint(file); err != nil {
        return
    }
    
    // 等待 WAL 完成
    if err = logEntry.WaitDone(); err != nil {
        return
    }
}
```

### 1.2 Intent 状态不持久化

**位置**: `store.go`

```go
type runnerStore struct {
    incrementalIntent atomic.Pointer[CheckpointEntry]  // 仅内存
}
```

**问题**:
- `incrementalIntent` 仅存在于内存中
- 崩溃后丢失正在进行的检查点意图
- 可能导致检查点间隔不一致

**建议**: 持久化 Intent 状态

```go
func (s *runnerStore) UpdateICKPIntent(...) {
    // 更新内存
    s.incrementalIntent.Store(newIntent)
    
    // 异步持久化（可选，用于调试和恢复）
    go s.persistIntent(newIntent)
}
```

---

## 2. 数据一致性问题

### 2.1 检查点与 WAL 不同步

**位置**: `executor.go`

```go
// 先保存检查点
if file, err = runner.saveCheckpoint(entry.start, entry.end); err != nil {
    // ...
}

// 后写 WAL
if logEntry, err = runner.wal.RangeCheckpoint(1, lsnToTruncate, files...); err != nil {
    // 检查点已保存，但 WAL 失败
    // 没有回滚检查点文件！
}
```

**问题**: 检查点文件和 WAL 记录不是原子操作。

### 2.2 GC 与检查点竞争

**位置**: `store.go`

```go
func (s *runnerStore) TryGC() (gdeleted, ideleted int) {
    s.Lock()
    defer s.Unlock()
    
    // GC 可能删除正在被读取的检查点
    return s.doGC(&intent)
}
```

**问题**:
- GC 和检查点读取可能并发执行
- 可能删除正在被使用的检查点条目

**建议**: 引用计数或快照隔离

```go
type CheckpointEntry struct {
    refCount atomic.Int32
    // ...
}

func (e *CheckpointEntry) Acquire() {
    e.refCount.Add(1)
}

func (e *CheckpointEntry) Release() {
    if e.refCount.Add(-1) == 0 {
        // 可以安全删除
    }
}
```

---

## 3. 错误恢复问题

### 3.1 Rollback 使用 Fatal

**位置**: `store.go`

```go
func (s *runnerStore) RollbackICKPIntent(intent *CheckpointEntry) {
    expect := s.incrementalIntent.Load()
    if intent != expect || !intent.IsRunning() {
        logutil.Fatal(  // 🔴 导致进程退出
            "ICKP-Rollback",
            zap.Any("expected", expect),
            zap.Any("actual", intent),
        )
    }
}
```

**问题**:
- 使用 `Fatal` 会导致进程立即退出
- 没有机会进行清理或恢复
- 可能导致数据处于不一致状态

**建议**: 返回错误而非 Fatal

```go
func (s *runnerStore) RollbackICKPIntent(intent *CheckpointEntry) error {
    expect := s.incrementalIntent.Load()
    if intent != expect || !intent.IsRunning() {
        logutil.Error(
            "ICKP-Rollback-Mismatch",
            zap.Any("expected", expect),
            zap.Any("actual", intent),
        )
        return ErrIntentMismatch
    }
    // ...
    return nil
}
```

### 3.2 PrepareCommit 失败处理不完整

**位置**: `executor.go`

```go
if prepared := runner.store.PrepareCommitICKPIntent(entry); !prepared {
    errPhase = "prepare"
    rollback()
    err = moerr.NewInternalErrorNoCtxf("cannot prepare ickp")
    return
}

if file, err = runner.saveCheckpoint(entry.start, entry.end); err != nil {
    errPhase = "save-ckp"
    runner.store.RollbackICKPIntent(entry)  // 回滚
    rollback()
    return
}
```

**问题**:
- `PrepareCommitICKPIntent` 已将 entry 加入 incrementals
- 如果 `saveCheckpoint` 失败，需要从 incrementals 中删除
- `RollbackICKPIntent` 确实做了这个，但错误处理路径复杂

---

## 4. 超时和重试问题

### 4.1 缺少操作超时

**位置**: 多处

```go
// executor.go - doGlobalCheckpoint
if data, err = factory(runner.catalog); err != nil {
    // 没有超时控制
}

// flusher.go - ForceFlushWithInterval
if err = common.RetryWithInterval(ctx, op, flushInterval); err != nil {
    // 依赖外部 context 超时
}
```

**问题**:
- 某些操作没有超时控制
- 可能导致无限等待
- 影响系统响应性

**建议**: 添加操作级别的超时

```go
func (job *checkpointJob) doGlobalCheckpoint(...) error {
    ctx, cancel := context.WithTimeout(job.executor.ctx, job.executor.cfg.GCKPTimeout)
    defer cancel()
    
    if data, err = factory(runner.catalog); err != nil {
        if ctx.Err() != nil {
            return ErrGCKPTimeout
        }
        return err
    }
    // ...
}
```

### 4.2 重试策略不一致

**位置**: 多处

```go
// store.go - UpdateICKPIntent
for {
    // 无限重试，无退避
}

// flusher.go - ForceFlushWithInterval
err = common.RetryWithInterval(ctx, op, flushInterval)
// 使用固定间隔重试
```

**建议**: 统一重试策略

```go
type RetryConfig struct {
    MaxRetries     int
    InitialBackoff time.Duration
    MaxBackoff     time.Duration
    Multiplier     float64
}

func RetryWithBackoff(ctx context.Context, cfg RetryConfig, op func() error) error {
    backoff := cfg.InitialBackoff
    for i := 0; i < cfg.MaxRetries; i++ {
        if err := op(); err == nil {
            return nil
        }
        
        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-time.After(backoff):
        }
        
        backoff = time.Duration(float64(backoff) * cfg.Multiplier)
        if backoff > cfg.MaxBackoff {
            backoff = cfg.MaxBackoff
        }
    }
    return ErrMaxRetriesExceeded
}
```

---

## 5. 资源泄漏问题

### 5.1 文件句柄泄漏风险

**位置**: `executor.go`

```go
func (job *checkpointJob) doGlobalCheckpoint(...) {
    // ...
    location, files, err := data.Sync(job.executor.ctx, runner.rt.Fs)
    if err != nil {
        runner.store.RemoveGCKPIntent()
        errPhase = "flush"
        return  // files 可能已部分创建
    }
}
```

**问题**: 如果 `Sync` 部分成功，可能留下孤立文件。

### 5.2 队列资源未释放

**位置**: `executor.go`

```go
func (executor *checkpointExecutor) StopWithCause(cause error) {
    // ...
    executor.ickpQueue.Stop()
    executor.gckpQueue.Stop()
    // 队列中未处理的任务怎么办？
}
```

**问题**: 停止时队列中可能还有未处理的任务。

---

## 6. 监控和告警缺失

### 6.1 缺少健康检查

**建议**: 添加健康检查接口

```go
type HealthStatus struct {
    LastICKPTime     time.Time
    LastGCKPTime     time.Time
    PendingICKPCount int
    PendingGCKPCount int
    Errors           []error
}

func (r *runner) HealthCheck() HealthStatus {
    // 返回当前健康状态
}
```

### 6.2 缺少关键事件告警

**建议**: 添加告警机制

```go
type AlertLevel int

const (
    AlertInfo AlertLevel = iota
    AlertWarning
    AlertCritical
)

type Alert struct {
    Level   AlertLevel
    Message string
    Time    time.Time
    Context map[string]interface{}
}

func (r *runner) Alert(alert Alert) {
    // 发送告警
}
```
