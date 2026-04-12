# Flusher 刷盘机制

## 概述

Flusher 负责将内存中的脏数据（dirty data）刷新到对象存储。它是 Checkpoint 能够执行的前提条件——只有当所有脏数据都已刷盘后，才能创建检查点。

## 架构

```
┌─────────────────────────────────────────────────────────────────┐
│                         Flusher                                  │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                      flushImpl                               ││
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  ││
│  │  │ cronTrigger │  │flushRequestQ│  │  objMemSizeList     │  ││
│  │  │  (定时触发)  │  │  (请求队列)  │  │  (内存大小列表)      │  ││
│  │  └─────────────┘  └─────────────┘  └─────────────────────┘  ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │  Object Storage │
                    └─────────────────┘
```

## 接口定义

```go
type Flusher interface {
    IsAllChangesFlushed(start, end types.TS, doPrint bool) bool
    FlushTable(ctx context.Context, dbID, tableID uint64, ts types.TS) error
    ForceFlush(ctx context.Context, ts types.TS) error
    ForceFlushWithInterval(ctx context.Context, ts types.TS, flushInterval time.Duration) error
    ChangeForceFlushTimeout(timeout time.Duration)
    ChangeForceCheckInterval(interval time.Duration)
    GetCfg() FlushCfg
    Restart(opts ...FlusherOption)
    IsNoop() bool
    Start()
    Stop()
}
```

## 配置

```go
type FlushCfg struct {
    ForceFlushTimeout       time.Duration  // 强制刷盘超时
    ForceFlushCheckInterval time.Duration  // 强制刷盘检查间隔
    FlushInterval           time.Duration  // 刷盘间隔
    CronPeriod              time.Duration  // 定时任务周期
}

type FlushMutableCfg struct {
    ForceFlushTimeout       time.Duration
    ForceFlushCheckInterval time.Duration
}
```

### 默认值

```go
func (flusher *flushImpl) fillDefaults() {
    if flusher.cronPeriod <= 0 {
        flusher.cronPeriod = time.Second * 5
    }
    if cfg.ForceFlushTimeout <= 0 {
        cfg.ForceFlushTimeout = time.Second * 90
    }
    if cfg.ForceFlushCheckInterval <= 0 {
        cfg.ForceFlushCheckInterval = time.Millisecond * 500
    }
    if flusher.flushInterval <= 0 {
        flusher.flushInterval = time.Minute
    }
    if flusher.flushLag <= 0 {
        if flusher.flushInterval < time.Second {
            flusher.flushLag = 0
        } else {
            flusher.flushLag = time.Second * 3
        }
    }
    if flusher.flushQueueSize <= 0 {
        flusher.flushQueueSize = 1000
    }
}
```

## 核心结构

```go
type flushImpl struct {
    mutableCfg     atomic.Pointer[FlushMutableCfg]
    flushInterval  time.Duration
    cronPeriod     time.Duration
    flushLag       time.Duration
    flushQueueSize int

    sourcer            logtail.Collector
    catalogCache       *catalog.Catalog
    checkpointSchduler CheckpointScheduler
    rt                 *dbutils.Runtime

    cronTrigger   *tasks.CancelableJob
    flushRequestQ sm.Queue

    objMemSizeList []tableAndSize

    // 日志节流
    logThrottleMu   sync.Mutex
    lastLogTime     time.Time
    lastLogPressure float64
    lastLogSize     int64

    onceStart sync.Once
    onceStop  sync.Once
}

type tableAndSize struct {
    tbl   *catalog.TableEntry
    asize int  // 追加对象大小
    dsize int  // 删除对象大小
}
```

## 定时触发

```go
func (flusher *flushImpl) triggerJob(ctx context.Context) {
    if flusher.sourcer == nil {
        return
    }
    
    // 运行收集器
    flusher.sourcer.Run(flusher.flushLag)
    
    // 获取合并后的脏数据
    entry := flusher.sourcer.GetAndRefreshMerged()
    if !entry.IsEmpty() {
        request := new(FlushRequest)
        request.tree = entry
        flusher.flushRequestQ.Enqueue(request)
    }
    
    // 尝试调度检查点
    _, ts := entry.GetTimeRange()
    flusher.checkpointSchduler.TryScheduleCheckpoint(ts, false)
}
```

## 刷盘请求处理

### FlushRequest

```go
type FlushRequest struct {
    force bool                    // 是否强制刷盘
    tree  *logtail.DirtyTreeEntry // 脏数据树
}
```

### 请求处理

```go
func (flusher *flushImpl) onFlushRequest(items ...any) {
    fromCrons := logtail.NewEmptyDirtyTreeEntry()
    fromForce := logtail.NewEmptyDirtyTreeEntry()
    
    // 分类请求
    for _, item := range items {
        e := item.(*FlushRequest)
        if e.force {
            fromForce.Merge(e.tree)
        } else {
            fromCrons.Merge(e.tree)
        }
    }
    
    // 分别处理
    flusher.scheduleFlush(fromForce, true)
    flusher.scheduleFlush(fromCrons, false)
}
```

### 调度刷盘

```go
func (flusher *flushImpl) scheduleFlush(
    entry *logtail.DirtyTreeEntry,
    force bool,
) {
    if entry.IsEmpty() {
        return
    }
    
    // 获取最后一个检查点
    lastCkp := types.TS{}
    if ckp := flusher.checkpointSchduler.MaxIncrementalCheckpoint(); ckp != nil {
        if ckp.IsFinished() {
            lastCkp = ckp.GetEnd()
        } else if ckp.GetStart().Physical() > 0 {
            lastCkp = ckp.GetStart().Prev()
        }
    }
    
    // 收集表内存使用情况
    pressure := flusher.collectTableMemUsage(entry, lastCkp)
    
    // 检查条件并触发刷盘
    flusher.checkFlushConditionAndFire(entry, force, pressure, lastCkp)
}
```

## 内存使用收集

```go
func (flusher *flushImpl) collectTableMemUsage(
    entry *logtail.DirtyTreeEntry, 
    lastCkp types.TS,
) (memPressureRate float64) {
    flusher.objMemSizeList = flusher.objMemSizeList[:0]
    sizevisitor := new(model.BaseTreeVisitor)
    var totalSize int
    _, end := entry.GetTimeRange()
    
    sizevisitor.TableFn = func(did, tid uint64) error {
        db, err := flusher.catalogCache.GetDatabaseByID(did)
        if err != nil {
            panic(err)
        }
        table, err := db.GetTableEntryByID(tid)
        if err != nil {
            panic(err)
        }
        table.Stats.Init(flusher.flushInterval)
        
        var asize, dsize int
        df := func(obj *catalog.ObjectEntry) {
            asize += obj.GetObjectData().EstimateMemSize()
        }
        tf := func(obj *catalog.ObjectEntry) {
            dsize += obj.GetObjectData().EstimateMemSize()
        }
        
        foreachAobjBefore(context.Background(), table, end, lastCkp, df, tf)
        totalSize += asize + dsize
        flusher.objMemSizeList = append(flusher.objMemSizeList, tableAndSize{table, asize, dsize})
        return moerr.GetOkStopCurrRecur()
    }
    
    if err := entry.GetTree().Visit(sizevisitor); err != nil {
        panic(err)
    }

    // 按大小降序排序
    slices.SortFunc(flusher.objMemSizeList, func(a, b tableAndSize) int {
        return b.asize - a.asize
    })

    // 计算内存压力
    pressure := float64(totalSize) / float64(common.RuntimeOverallFlushMemCap.Load())
    if pressure > 1.0 {
        pressure = 1.0
    }

    return pressure
}
```

## 刷盘条件检查

```go
func (flusher *flushImpl) checkFlushConditionAndFire(
    entry *logtail.DirtyTreeEntry, 
    force bool, 
    pressure float64, 
    lastCkp types.TS,
) {
    count := 0
    _, end := entry.GetTimeRange()
    
    for _, ticket := range flusher.objMemSizeList {
        table, asize, dsize := ticket.tbl, ticket.asize, ticket.dsize

        // 强制刷盘
        if force {
            logutil.Info("flusher.force",
                zap.Uint64("id", table.ID),
                zap.String("name", table.GetLastestSchemaLocked(false).Name),
            )
            if err := flusher.fireFlushTabletail(table, end, lastCkp); err == nil {
                table.Stats.ResetDeadline(flusher.flushInterval)
            }
            continue
        }

        // 检查是否需要刷盘
        flushReady := func() bool {
            // 已删除的表，资源允许时立即刷盘
            if !table.IsActive() {
                count++
                if pressure < 0.5 || count < 200 {
                    return true
                }
                return false
            }
            // 到达刷盘时间
            if table.Stats.GetFlushDeadline().Before(time.Now()) {
                return true
            }
            // 表太大
            if asize+dsize > int(common.FlushMemCapacity.Load()) {
                return true
            }
            // 未刷盘数据太多
            if asize > common.Const1MBytes && rand.Float64() < pressure {
                return true
            }
            return false
        }

        ready := flushReady()

        if ready {
            if err := flusher.fireFlushTabletail(table, end, lastCkp); err == nil {
                table.Stats.ResetDeadline(flusher.flushInterval)
            }
        }
    }
}
```

## 触发表尾刷盘

```go
func (flusher *flushImpl) fireFlushTabletail(
    table *catalog.TableEntry,
    end, lastCkp types.TS,
) error {
    tableDesc := fmt.Sprintf("%d-%s", table.ID, table.GetLastestSchemaLocked(false).Name)
    metas := make([]*catalog.ObjectEntry, 0, 10)
    tombstoneMetas := make([]*catalog.ObjectEntry, 0, 10)
    
    // 收集需要刷盘的对象
    df := func(obj *catalog.ObjectEntry) {
        metas = append(metas, obj)
    }
    tf := func(obj *catalog.ObjectEntry) {
        tombstoneMetas = append(tombstoneMetas, obj)
    }
    foreachAobjBefore(context.Background(), table, end, lastCkp, df, tf)
    
    if len(metas) == 0 && len(tombstoneMetas) == 0 {
        return nil
    }

    // 分块处理
    metaChunks := slices.Chunk(metas, 400)
    firstChunk := true

nextChunk:
    for chunk := range metaChunks {
        scopes := make([]common.ID, 0, len(chunk))
        for _, meta := range chunk {
            if !meta.GetObjectData().PrepareCompact() {
                logutil.Info("flusher.data.prepare.compact.false",
                    zap.String("table", tableDesc),
                    zap.String("obj", meta.ID().String()),
                )
                break nextChunk
            }
            scopes = append(scopes, *meta.AsCommonID())
        }
        
        // 第一个块包含 tombstone
        if firstChunk {
            for _, meta := range tombstoneMetas {
                if !meta.GetObjectData().PrepareCompact() {
                    return moerr.GetOkExpectedEOB()
                }
                scopes = append(scopes, *meta.AsCommonID())
            }
        }
        
        // 创建刷盘任务
        var factory tasks.TxnTaskFactory
        if firstChunk {
            factory = jobs.FlushTableTailTaskFactory(chunk, tombstoneMetas, flusher.rt)
            firstChunk = false
        } else {
            factory = jobs.FlushTableTailTaskFactory(chunk, nil, flusher.rt)
        }

        // 调度任务
        _, err := flusher.rt.Scheduler.ScheduleMultiScopedTxnTask(
            nil, tasks.FlushTableTailTask, scopes, factory)
        if err != nil {
            if err != tasks.ErrScheduleScopeConflict {
                logutil.Error("flusher.table.tail.sched.failure",
                    zap.String("table", tableDesc),
                    zap.Error(err),
                )
            }
            return moerr.GetOkExpectedEOB()
        }
    }

    return nil
}
```

## 强制刷盘

```go
func (flusher *flushImpl) ForceFlush(ctx context.Context, ts types.TS) (err error) {
    return flusher.ForceFlushWithInterval(ctx, ts, 0)
}

func (flusher *flushImpl) ForceFlushWithInterval(
    ctx context.Context,
    ts types.TS,
    flushInterval time.Duration,
) (err error) {
    makeRequest := func() *FlushRequest {
        tree := flusher.sourcer.ScanInRangePruned(types.TS{}, ts)
        if tree.IsEmpty() {
            return nil
        }
        entry := logtail.NewDirtyTreeEntry(types.TS{}, ts, tree.GetTree())
        request := new(FlushRequest)
        request.tree = entry
        request.force = true
        return request
    }
    
    op := func() (ok bool, err error) {
        request := makeRequest()
        if request == nil {
            return true, nil
        }
        if _, err = flusher.flushRequestQ.Enqueue(request); err != nil {
            return true, nil
        }
        return false, nil
    }

    cfg := flusher.mutableCfg.Load()

    if flushInterval <= 0 {
        flushInterval = cfg.ForceFlushCheckInterval
    }
    
    if err = common.RetryWithInterval(ctx, op, flushInterval); err != nil {
        return moerr.NewInternalErrorf(ctx, "force flush failed: %v", err)
    }
    return
}
```

## 遍历追加对象

```go
func foreachAobjBefore(
    _ context.Context,
    table *catalog.TableEntry, 
    ts types.TS, 
    lastCkp types.TS,
    df func(*catalog.ObjectEntry),  // 数据对象处理函数
    tf func(*catalog.ObjectEntry),  // 墓碑对象处理函数
) {
    var ok bool
    key := &catalog.ObjectEntry{EntryMVCCNode: catalog.EntryMVCCNode{DeletedAt: ts.Next()}}

    // 遍历数据对象
    data := table.MakeDataObjectIt()
    defer data.Release()
    if ok = data.Seek(key); !ok {
        ok = data.Last()
    }
    for ; ok; ok = data.Prev() {
        item := data.Item()
        // 在最后检查点之前创建的 C 条目，跳出
        if item.IsCEntry() && item.CreatedAt.LT(&lastCkp) {
            break
        }
        if item.IsAppendable() && item.IsCEntry() && !item.HasDCounterpart() && item.CreatedAt.LE(&ts) {
            df(item)
        }
    }

    // 遍历墓碑对象
    tomb := table.MakeTombstoneObjectIt()
    defer tomb.Release()
    if ok = tomb.Seek(key); !ok {
        ok = tomb.Last()
    }
    for ; ok; ok = tomb.Prev() {
        item := tomb.Item()
        if item.IsCEntry() && item.CreatedAt.LT(&lastCkp) {
            break
        }
        if item.IsAppendable() && item.IsCEntry() && !item.HasDCounterpart() && item.CreatedAt.LE(&ts) {
            tf(item)
        }
    }
}
```

## 生命周期

### 启动

```go
func (flusher *flushImpl) Start() {
    flusher.onceStart.Do(func() {
        flusher.flushRequestQ.Start()
        flusher.cronTrigger.Start()
        cfg := flusher.mutableCfg.Load()
        logutil.Info("flusher.start",
            zap.Duration("cron-period", flusher.cronPeriod),
            zap.Duration("flush-interval", flusher.flushInterval),
            zap.Duration("flush-lag", flusher.flushLag),
            zap.Duration("force-flush-timeout", cfg.ForceFlushTimeout),
            zap.Duration("force-flush-check-interval", cfg.ForceFlushCheckInterval),
        )
    })
}
```

### 停止

```go
func (flusher *flushImpl) Stop() {
    flusher.onceStop.Do(func() {
        flusher.cronTrigger.Stop()
        flusher.flushRequestQ.Stop()
        logutil.Info("flusher.stop")
    })
}
```
