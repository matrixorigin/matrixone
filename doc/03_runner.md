# Checkpoint Runner 详解

## 概述

`runner` 是 Checkpoint 模块的核心组件，负责组织和管理所有与检查点相关的行为。

## 结构定义

```go
type runner struct {
    ctx context.Context

    // logtail 数据源
    source    logtail.Collector
    catalog   *catalog.Catalog
    rt        *dbutils.Runtime
    observers *observers
    wal       wal.Store

    // 检查点条目的内存存储
    store *runnerStore

    // 执行器
    executor atomic.Pointer[checkpointExecutor]

    // 任务队列
    postCheckpointQueue sm.Queue  // 检查点完成后处理
    gcCheckpointQueue   sm.Queue  // GC 处理
    replayQueue         sm.Queue  // 恢复处理

    onceStart sync.Once
    onceStop  sync.Once
}
```

## 初始化

```go
func NewRunner(
    ctx context.Context,
    rt *dbutils.Runtime,
    catalog *catalog.Catalog,
    source logtail.Collector,
    wal wal.Store,
    cfg *CheckpointCfg,
    opts ...Option,
) *runner {
    r := &runner{
        ctx:       ctx,
        rt:        rt,
        catalog:   catalog,
        source:    source,
        observers: new(observers),
        wal:       wal,
    }
    
    // 应用选项
    for _, opt := range opts {
        opt(r)
    }
    
    r.fillDefaults()
    r.StartExecutor(cfg)
    cfg = r.GetCfg()

    // 初始化存储
    r.store = newRunnerStore(
        r.rt.SID(), 
        cfg.GlobalHistoryDuration, 
        time.Minute*2,
    )

    // 初始化队列
    r.gcCheckpointQueue = sm.NewSafeQueue(100, 100, r.onGCCheckpointEntries)
    r.postCheckpointQueue = sm.NewSafeQueue(1000, 1, r.onPostCheckpointEntries)
    r.replayQueue = sm.NewSafeQueue(100, 10, r.onReplayCheckpoint)

    return r
}
```

## 生命周期管理

### 启动

```go
func (r *runner) Start() {
    r.onceStart.Do(func() {
        r.postCheckpointQueue.Start()
        r.gcCheckpointQueue.Start()
        r.replayQueue.Start()
        logutil.Info(
            "CKPRunner-Started",
            zap.String("mode", r.ModeString()),
        )
    })
}
```

### 停止

```go
func (r *runner) Stop() {
    r.onceStop.Do(func() {
        mode := r.ModeString()
        r.replayQueue.Stop()
        r.StopExecutor(ErrStopRunner)
        r.gcCheckpointQueue.Stop()
        r.postCheckpointQueue.Stop()
        logutil.Info(
            "CKPRunner-Stopped",
            zap.String("mode", mode),
        )
    })
}
```

## 执行器管理

### 启动执行器

```go
func (r *runner) StartExecutor(cfg *CheckpointCfg) {
    for {
        executor := r.executor.Load()
        if executor != nil {
            executor.StopWithCause(ErrExecutorRestarted)
        }
        newExecutor := newCheckpointExecutor(r, cfg)
        if r.executor.CompareAndSwap(executor, newExecutor) {
            break
        }
    }
}
```

### 停止执行器

```go
func (r *runner) StopExecutor(err error) (cfg *CheckpointCfg) {
    for {
        running := r.executor.Load()
        if running == nil {
            return
        }
        running.StopWithCause(err)
        if stopped := r.executor.CompareAndSwap(running, nil); stopped {
            cfg = running.GetCfg()
            break
        }
    }
    return
}
```

## 调度检查点

### TryScheduleCheckpoint

这是调度检查点的主入口：

```go
func (r *runner) TryScheduleCheckpoint(
    ts types.TS, force bool,
) (ret Intent, err error) {
    executor := r.executor.Load()
    if executor == nil {
        err = ErrExecutorClosed
        return
    }
    return executor.TryScheduleCheckpoint(ts, force)
}
```

### 触发增量检查点

```go
func (r *runner) TryTriggerExecuteICKP() (err error) {
    executor := r.executor.Load()
    if executor == nil {
        err = ErrExecutorClosed
        return
    }
    return executor.TriggerExecutingICKP()
}
```

### 触发全局检查点

```go
func (r *runner) TryTriggerExecuteGCKP(ctx *gckpContext) (err error) {
    executor := r.executor.Load()
    if executor == nil {
        err = ErrExecutorClosed
        return
    }
    return executor.TriggerExecutingGCKP(ctx)
}
```

## 队列处理

### 检查点完成后处理

```go
func (r *runner) onPostCheckpointEntries(entries ...any) {
    for _, e := range entries {
        entry := e.(*CheckpointEntry)

        // 1. 广播事件
        r.observers.OnNewCheckpoint(entry.GetEnd())

        // 2. 可以在这里移除旧检查点
        logutil.Debugf("Post %s", entry.String())
    }
}
```

### GC 处理

```go
func (r *runner) onGCCheckpointEntries(items ...any) {
    r.store.TryGC()
}
```

### 恢复处理

```go
func (r *runner) onReplayCheckpoint(entries ...any) {
    for _, e := range entries {
        entry := e.(*CheckpointEntry)
        r.replayOneEntry(entry)
    }
}

func (r *runner) replayOneEntry(entry *CheckpointEntry) {
    defer entry.Done()
    if !entry.IsFinished() {
        logutil.Warn("Replay-CKP-NoFinished", zap.String("entry", entry.String()))
        return
    }

    if entry.IsGlobal() {
        if ok := r.store.AddGCKPFinishedEntry(entry); !ok {
            logutil.Warn("Replay-GCKP-AddFailed", zap.String("entry", entry.String()))
        }
    } else if entry.IsIncremental() {
        if ok := r.store.AddICKPFinishedEntry(entry); !ok {
            logutil.Warn("Replay-ICKP-AddFailed", zap.String("entry", entry.String()))
        }
    } else if entry.IsBackup() {
        if ok := r.store.AddBackupCKPEntry(entry); !ok {
            logutil.Warn("Replay-Backup-AddFailed", zap.String("entry", entry.String()))
        }
    } else if entry.IsCompact() {
        if ok := r.store.UpdateCompacted(entry); !ok {
            logutil.Warn("Replay-Compact-AddFailed", zap.String("entry", entry.String()))
        }
    }
}
```

## 元数据保存

```go
func (r *runner) saveCheckpoint(
    start, end types.TS,
) (name string, err error) {
    // 检查注入错误（用于测试）
    if injectErrMsg, injected := objectio.CheckpointSaveInjected(); injected {
        return "", moerr.NewInternalErrorNoCtx(injectErrMsg)
    }
    
    // 收集检查点元数据
    bat := r.collectCheckpointMetadata(start, end)
    defer bat.Close()
    
    // 编码文件名
    name = ioutil.EncodeCKPMetadataFullName(start, end)
    
    // 创建写入器
    writer, err := objectio.NewObjectWriterSpecial(
        objectio.WriterCheckpoint, name, r.rt.Fs,
    )
    if err != nil {
        return
    }
    
    // 写入数据
    if _, err = writer.Write(containers.ToCNBatch(bat)); err != nil {
        return
    }

    // 同步写入
    _, err = writer.WriteEnd(r.ctx)
    if err != nil {
        return
    }
    
    // 记录元数据文件
    fileName := ioutil.EncodeCKPMetadataName(start, end)
    r.store.AddMetaFile(fileName)
    return
}
```

## 脏数据检查

```go
func IsAllDirtyFlushed(
    source logtail.Collector, 
    cata *catalog.Catalog, 
    start, end types.TS, 
    doPrint bool,
) bool {
    tree, _ := source.ScanInRange(start, end)
    ready := true
    var notFlushed *catalog.ObjectEntry
    
    for _, table := range tree.GetTree().Tables {
        db, err := cata.GetDatabaseByID(table.DbID)
        if err != nil {
            continue
        }
        table, err := db.GetTableEntryByID(table.ID)
        if err != nil {
            continue
        }
        ready, notFlushed = table.IsTableTailFlushed(start, end)
        if !ready {
            break
        }
    }
    
    if !ready && doPrint {
        table := notFlushed.GetTable()
        tableDesc := fmt.Sprintf("%d-%s", table.ID, table.GetLastestSchemaLocked(false).Name)
        logutil.Info("waiting for dirty tree %s", 
            zap.String("table", tableDesc), 
            zap.String("obj", notFlushed.StringWithLevel(2)))
    }
    return ready
}
```

## 查询接口

### 收集检查点范围

```go
func (r *runner) CollectCheckpointsInRange(
    ctx context.Context, start, end types.TS,
) (locations string, checkpointed types.TS, err error) {
    return r.store.CollectCheckpointsInRange(ctx, start, end)
}
```

### 获取脏数据收集器

```go
func (r *runner) GetDirtyCollector() logtail.Collector {
    return r.source
}
```

## 运行模式

Runner 支持两种运行模式：

```go
func (r *runner) String() string {
    cfg := r.GetCfg()
    if cfg == nil {
        return "<RO-CKPRunner>"  // 只读模式
    }
    return fmt.Sprintf("<RW-CKPRunner[%s]>", cfg.String())  // 读写模式
}

func (r *runner) ModeString() string {
    cfg := r.GetCfg()
    if cfg == nil {
        return "<RO>"
    }
    return "<RW>"
}
```

## 错误处理

```go
var ErrPendingCheckpoint = moerr.NewPrevCheckpointNotFinished()
var ErrCheckpointDisabled = moerr.NewInternalErrorNoCtxf("checkpoint disabled")
var ErrExecutorRestarted = moerr.NewInternalErrorNoCtxf("executor restarted")
var ErrExecutorClosed = moerr.NewInternalErrorNoCtxf("executor closed")
var ErrBadIntent = moerr.NewInternalErrorNoCtxf("bad intent")
var ErrStopRunner = moerr.NewInternalErrorNoCtxf("runner stopped")
```
