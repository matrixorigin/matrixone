# Checkpoint Executor 执行器

## 概述

`checkpointExecutor` 是检查点的执行引擎，负责实际执行增量检查点（ICKP）和全局检查点（GCKP）。

## 结构定义

```go
type checkpointExecutor struct {
    cfg CheckpointCfg

    // 检查点策略
    incrementalPolicy *timeBasedPolicy  // 基于时间的策略
    globalPolicy      *countBasedPolicy // 基于计数的策略

    ctx         context.Context
    cancel      context.CancelCauseFunc
    active      atomic.Bool
    runningICKP atomic.Pointer[checkpointJob]  // 正在运行的增量检查点
    runningGCKP atomic.Pointer[checkpointJob]  // 正在运行的全局检查点

    runner      *runner
    runICKPFunc func(context.Context, *runner) error
    runGCKPFunc func(context.Context, *gckpContext, *runner) error

    ickpQueue sm.Queue  // 增量检查点队列
    gckpQueue sm.Queue  // 全局检查点队列
}
```

## 策略定义

### 基于时间的策略

```go
type timeBasedPolicy struct {
    interval time.Duration
}

func (p *timeBasedPolicy) Check(last types.TS) bool {
    physical := last.Physical()
    return physical <= time.Now().UTC().UnixNano()-p.interval.Nanoseconds()
}
```

### 基于计数的策略

```go
type countBasedPolicy struct {
    minCount int
}

func (p *countBasedPolicy) Check(current int) bool {
    return current >= p.minCount
}
```

## 初始化

```go
func newCheckpointExecutor(
    runner *runner,
    cfg *CheckpointCfg,
) *checkpointExecutor {
    ctx := context.Background()
    if runner != nil {
        ctx = runner.ctx
    }
    if cfg == nil {
        cfg = new(CheckpointCfg)
    }
    ctx, cancel := context.WithCancelCause(ctx)
    
    e := &checkpointExecutor{
        runner: runner,
        ctx:    ctx,
        cancel: cancel,
        cfg:    *cfg,
    }
    e.fillDefaults()

    // 初始化策略
    e.incrementalPolicy = &timeBasedPolicy{interval: e.cfg.IncrementalInterval}
    e.globalPolicy = &countBasedPolicy{minCount: int(e.cfg.GlobalMinCount)}

    // 初始化队列
    e.ickpQueue = sm.NewSafeQueue(1000, 100, e.onICKPEntries)
    e.gckpQueue = sm.NewSafeQueue(1000, 100, e.onGCKPEntries)
    e.ickpQueue.Start()
    e.gckpQueue.Start()

    e.active.Store(true)
    logutil.Info("CKP-Executor-Started", zap.String("cfg", e.cfg.String()))
    return e
}
```

## 停止执行器

```go
func (executor *checkpointExecutor) StopWithCause(cause error) {
    if updated := executor.active.CompareAndSwap(true, false); !updated {
        return
    }
    if cause == nil {
        cause = ErrCheckpointDisabled
    }
    executor.cancel(cause)
    
    // 等待正在运行的任务完成
    job := executor.runningGCKP.Load()
    if job != nil {
        <-job.WaitC()
    }
    executor.runningGCKP.Store(nil)
    
    job = executor.runningICKP.Load()
    if job != nil {
        <-job.WaitC()
    }
    executor.runningICKP.Store(nil)
    
    executor.ickpQueue.Stop()
    executor.gckpQueue.Stop()
    logutil.Info("CKP-Executor-Stopped", zap.Error(cause), zap.String("cfg", executor.cfg.String()))
}
```

## Checkpoint Job

```go
type checkpointJob struct {
    doneCh   chan struct{}
    executor *checkpointExecutor

    runICKPFunc func(context.Context, *runner) error

    gckpCtx     *gckpContext
    runGCKPFunc func(context.Context, *gckpContext, *runner) error

    err error
}

func (job *checkpointJob) WaitC() <-chan struct{} {
    return job.doneCh
}

func (job *checkpointJob) Err() error {
    return job.err
}

func (job *checkpointJob) Done(err error) {
    job.err = err
    close(job.doneCh)
}
```

## 调度检查点

### TryScheduleCheckpoint

```go
func (executor *checkpointExecutor) TryScheduleCheckpoint(
    ts types.TS, force bool,
) (ret Intent, err error) {
    if !force {
        return executor.softScheduleCheckpoint(&ts)
    }

    // 强制调度
    intent, updated := executor.runner.store.UpdateICKPIntent(&ts, true, true)
    if intent == nil {
        return
    }

    now := time.Now()
    defer func() {
        logger := logutil.Info
        if err != nil {
            logger = logutil.Error
        }
        logger(
            "ICKP-Schedule-Force",
            zap.String("intent", intent.String()),
            zap.String("ts", ts.ToString()),
            zap.Bool("updated", updated),
            zap.Duration("cost", time.Since(now)),
            zap.Error(err),
        )
    }()

    executor.TriggerExecutingICKP()

    if intent.end.LT(&ts) {
        err = ErrPendingCheckpoint
        return
    }

    return intent, nil
}
```

### softScheduleCheckpoint

软调度检查点，会检查各种条件：

```go
func (executor *checkpointExecutor) softScheduleCheckpoint(
    ts *types.TS,
) (ret *CheckpointEntry, err error) {
    var updated bool
    intent := executor.runner.store.GetICKPIntent()

    // 检查脏数据是否已刷盘
    check := func() (done bool) {
        start, end := intent.GetStart(), intent.GetEnd()
        ready := IsAllDirtyFlushed(
            executor.runner.source, 
            executor.runner.catalog, 
            start, end, 
            intent.TooOld(),
        )
        if !ready {
            intent.DeferRetirement()
        }
        return ready
    }

    // 如果没有 intent，检查是否需要创建
    if intent == nil {
        start := executor.runner.store.GetCheckpointed()
        if ts.LT(&start) {
            return
        }
        if !executor.incrementalPolicy.Check(start) {
            return
        }
        _, count := executor.runner.source.ScanInRange(start, *ts)
        if count < int(executor.cfg.MinCount) {
            return
        }
        intent, updated = executor.runner.store.UpdateICKPIntent(ts, true, false)
        if updated {
            logutil.Info("ICKP-Schedule-Soft-Updated",
                zap.String("intent", intent.String()),
                zap.String("ts", ts.ToString()),
            )
        }
    }

    if intent == nil {
        return
    }

    // 检查策略和刷盘状态
    var policyChecked, flushedChecked bool
    policyChecked = intent.IsPolicyChecked()
    if !policyChecked {
        if !executor.incrementalPolicy.Check(intent.GetStart()) {
            return
        }
        _, count := executor.runner.source.ScanInRange(intent.GetStart(), intent.GetEnd())
        if count < int(executor.cfg.MinCount) {
            return
        }
        policyChecked = true
    }

    flushedChecked = intent.IsFlushChecked()
    if !flushedChecked && check() {
        flushedChecked = true
    }

    // 更新 intent 状态
    if policyChecked != intent.IsPolicyChecked() || flushedChecked != intent.IsFlushChecked() {
        endTS := intent.GetEnd()
        intent, updated = executor.runner.store.UpdateICKPIntent(&endTS, policyChecked, flushedChecked)
        if updated {
            logutil.Info("ICKP-Schedule-Soft-Updated",
                zap.String("intent", intent.String()),
                zap.String("endTS", ts.ToString()),
            )
        }
    }

    if intent == nil {
        return
    }

    if intent.end.LT(ts) {
        err = ErrPendingCheckpoint
        executor.TriggerExecutingICKP()
        return
    }

    if intent.AllChecked() {
        executor.TriggerExecutingICKP()
    }
    return
}
```

## 执行增量检查点

### 触发执行

```go
func (executor *checkpointExecutor) TriggerExecutingICKP() (err error) {
    if !executor.active.Load() {
        err = ErrExecutorClosed
        return
    }
    _, err = executor.ickpQueue.Enqueue(struct{}{})
    return
}
```

### 执行逻辑

```go
func (executor *checkpointExecutor) RunICKP() (err error) {
    if !executor.active.Load() {
        err = ErrCheckpointDisabled
        return
    }
    if executor.runningICKP.Load() != nil {
        err = ErrPendingCheckpoint
    }
    
    job := &checkpointJob{
        doneCh:      make(chan struct{}),
        executor:    executor,
        runICKPFunc: executor.runICKPFunc,
    }
    
    if !executor.runningICKP.CompareAndSwap(nil, job) {
        err = ErrPendingCheckpoint
        return
    }
    
    defer func() {
        job.Done(err)
        executor.runningICKP.Store(nil)
    }()
    
    err = job.RunICKP(executor.ctx)
    return
}
```

### doIncrementalCheckpoint

```go
func (executor *checkpointExecutor) doIncrementalCheckpoint(
    entry *CheckpointEntry,
) (fields []zap.Field, files []string, err error) {
    // 创建数据工厂
    factory := logtail.IncrementalCheckpointDataFactory(
        entry.start, entry.end, 0, executor.runner.rt.Fs,
    )
    
    // 收集数据
    data, err := factory(executor.runner.catalog)
    if err != nil {
        return
    }
    defer data.Close()
    
    // 同步到存储
    var location objectio.Location
    location, files, err = data.Sync(executor.ctx, executor.runner.rt.Fs)
    fields = data.ExportStats("")
    if err != nil {
        return
    }
    
    files = append(files, location.Name().String())
    entry.SetLocation(location, location)

    perfcounter.Update(executor.ctx, func(counter *perfcounter.CounterSet) {
        counter.TAE.CheckPoint.DoIncrementalCheckpoint.Add(1)
    })
    return
}
```

## 执行全局检查点

### 触发执行

```go
func (executor *checkpointExecutor) TriggerExecutingGCKP(ctx *gckpContext) (err error) {
    if !executor.active.Load() {
        err = ErrExecutorClosed
        return
    }
    _, err = executor.gckpQueue.Enqueue(ctx)
    return
}
```

### 队列处理

```go
func (executor *checkpointExecutor) onGCKPEntries(items ...any) {
    var mergedCtx *gckpContext
    var fromCheckpointed, toCheckpointed types.TS
    now := time.Now()
    
    defer func() {
        // 日志记录
    }()

    // 合并所有上下文
    for _, item := range items {
        oneCtx := item.(*gckpContext)
        if mergedCtx == nil {
            mergedCtx = oneCtx
        } else {
            mergedCtx.Merge(oneCtx)
        }
    }
    
    if mergedCtx == nil {
        return
    }

    if mergedCtx.histroyRetention == 0 {
        mergedCtx.histroyRetention = executor.cfg.GlobalHistoryDuration
    }

    fromEntry := executor.runner.store.MaxGlobalCheckpoint()
    if fromEntry != nil {
        fromCheckpointed = fromEntry.GetEnd()
    }

    if mergedCtx.end.LE(&fromCheckpointed) {
        logutil.Info("GCKP-Execute-Skip",
            zap.String("have", fromCheckpointed.ToString()),
            zap.String("want", mergedCtx.end.ToString()),
        )
        return
    }

    // 检查策略
    if !mergedCtx.force {
        ickpCount := executor.runner.store.GetIncrementalCountAfterGlobal()
        if !executor.globalPolicy.Check(ickpCount) {
            logutil.Debug("GCKP-Execute-Skip",
                zap.Int("pending-ickp", ickpCount),
                zap.String("want", mergedCtx.end.ToString()),
            )
            return
        }
    }

    err = executor.RunGCKP(mergedCtx)
}
```

## GCKP Context

```go
type gckpContext struct {
    force            bool           // 是否强制执行
    end              types.TS       // 结束时间戳
    histroyRetention time.Duration  // 历史保留时间
    truncateLSN      uint64         // 截断 LSN
    ckpLSN           uint64         // 检查点 LSN
}

func (g *gckpContext) Merge(other *gckpContext) {
    if other == nil {
        return
    }
    if other.force {
        g.force = true
    }
    if other.end.LE(&g.end) {
        return
    }
    g.end = other.end
    g.histroyRetention = other.histroyRetention
    g.truncateLSN = other.truncateLSN
    g.ckpLSN = other.ckpLSN
}
```

## 执行流程图

```
┌─────────────────────────────────────────────────────────────────┐
│                    ICKP 执行流程                                 │
├─────────────────────────────────────────────────────────────────┤
│  1. TriggerExecutingICKP()                                      │
│     │                                                           │
│     ▼                                                           │
│  2. ickpQueue.Enqueue()                                         │
│     │                                                           │
│     ▼                                                           │
│  3. onICKPEntries() -> RunICKP()                               │
│     │                                                           │
│     ▼                                                           │
│  4. TakeICKPIntent() - 获取 Intent                              │
│     │                                                           │
│     ▼                                                           │
│  5. doIncrementalCheckpoint() - 收集并写入数据                   │
│     │                                                           │
│     ▼                                                           │
│  6. SyncTableIDBatch() - 同步 TableID                           │
│     │                                                           │
│     ▼                                                           │
│  7. PrepareCommitICKPIntent() - 准备提交                        │
│     │                                                           │
│     ▼                                                           │
│  8. saveCheckpoint() - 保存元数据                               │
│     │                                                           │
│     ▼                                                           │
│  9. CommitICKPIntent() - 提交 Intent                            │
│     │                                                           │
│     ▼                                                           │
│  10. TryTriggerExecuteGCKP() - 尝试触发 GCKP                    │
└─────────────────────────────────────────────────────────────────┘
```
