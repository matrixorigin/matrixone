# 增量检查点（ICKP）详解

## 概述

增量检查点（Incremental Checkpoint，ICKP）记录自上次检查点以来的所有数据变更。它是 TAE 中最常执行的检查点类型，具有执行频率高、开销小的特点。

## 时间范围

增量检查点的时间范围是 `[start, end]`：

- `start`：上一个检查点的 `end.Next()`
- `end`：当前检查点的结束时间戳

```
ICKP1: [0, t1]
ICKP2: [t1+1, t2]
ICKP3: [t2+1, t3]
...
```

## 触发条件

增量检查点的触发需要满足以下条件：

### 1. 时间间隔检查

```go
type timeBasedPolicy struct {
    interval time.Duration  // 默认 1 分钟
}

func (p *timeBasedPolicy) Check(last types.TS) bool {
    physical := last.Physical()
    return physical <= time.Now().UTC().UnixNano()-p.interval.Nanoseconds()
}
```

### 2. 最小事务数检查

```go
_, count := executor.runner.source.ScanInRange(start, *ts)
if count < int(executor.cfg.MinCount) {  // 默认 10000
    return
}
```

### 3. 脏数据刷盘检查

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
    return ready
}
```

## Intent 管理

### 创建/更新 Intent

```go
func (s *runnerStore) UpdateICKPIntent(
    ts *types.TS, policyChecked, flushChecked bool,
) (intent *CheckpointEntry, updated bool) {
    for {
        old := s.incrementalIntent.Load()
        
        // 场景1：已有 intent 且满足条件，无需更新
        if old != nil && (old.end.GT(ts) || !old.IsPendding() || old.Age() > s.intentOldAge) {
            intent = old
            return
        }

        // 场景2：需要扩展现有 intent 的结束时间
        // 场景3：需要创建新的 intent
        
        var start types.TS
        if old != nil {
            start = old.start
        } else {
            start = s.GetCheckpointed()
        }

        if start.GE(ts) {
            intent = old
            return
        }
        
        var newIntent *CheckpointEntry
        if old == nil {
            newIntent = NewCheckpointEntry(
                s.sid, start, *ts, ET_Incremental,
                WithCheckedEntryOption(policyChecked, flushChecked),
            )
        } else {
            newIntent = InheritCheckpointEntry(
                old,
                WithEndEntryOption(*ts),
                WithCheckedEntryOption(policyChecked, flushChecked),
            )
        }
        
        if s.incrementalIntent.CompareAndSwap(old, newIntent) {
            intent = newIntent
            updated = true
            return
        }
    }
}
```

### 获取 Intent 执行

```go
func (s *runnerStore) TakeICKPIntent() (taken *CheckpointEntry, rollback func()) {
    for {
        old := s.incrementalIntent.Load()
        if old == nil || !old.IsPendding() || !old.AllChecked() {
            return
        }
        taken = InheritCheckpointEntry(
            old,
            WithStateEntryOption(ST_Running),
        )
        if s.incrementalIntent.CompareAndSwap(old, taken) {
            rollback = func() {
                s.incrementalIntent.Store(nil)
                taken.Done()
            }
            break
        }
        taken = nil
        rollback = nil
    }
    return
}
```

## 执行流程

### Job 执行

```go
func (job *checkpointJob) RunICKP(ctx context.Context) (err error) {
    if job.runICKPFunc != nil {
        return job.runICKPFunc(ctx, job.executor.runner)
    }
    
    select {
    case <-ctx.Done():
        return context.Cause(ctx)
    default:
    }

    runner := job.executor.runner

    // 1. 获取 Intent
    entry, rollback := runner.store.TakeICKPIntent()
    if entry == nil {
        return
    }

    var (
        errPhase      string
        lsnToTruncate uint64
        lsn           uint64
        fatal         bool
        fields        []zap.Field
        now           = time.Now()
    )

    logutil.Info("ICKP-Execute-Start", zap.String("entry", entry.String()))

    defer func() {
        // 错误处理和日志记录
    }()

    // 2. 执行增量检查点
    var files []string
    var file string
    if fields, files, err = job.executor.doIncrementalCheckpoint(entry); err != nil {
        errPhase = "do-ckp"
        rollback()
        return
    }

    // 3. 同步 TableID 批次
    tableIDLocation, err := logtail.SyncTableIDBatch(
        job.executor.ctx,
        entry.start,
        entry.end,
        job.executor.cfg.TableIDHistoryDuration,
        job.executor.cfg.TableIDSinkerThreshold,
        entry.GetLocation(),
        entry.GetVersion(),
        preTableIDLocation,
        common.CheckpointAllocator,
        runner.rt.Fs,
    )
    if err != nil {
        errPhase = "sync-table-id"
        rollback()
        return
    }
    entry.SetTableIDLocation(tableIDLocation)

    // 4. 计算 LSN
    lsn = runner.source.GetMaxLSN(entry.start, entry.end)
    if lsn == 0 {
        if maxICKP := runner.store.MaxIncrementalCheckpoint(); maxICKP != nil {
            lsn = maxICKP.LSN()
        }
    }
    if lsn > job.executor.cfg.IncrementalReservedWALCount {
        lsnToTruncate = lsn - job.executor.cfg.IncrementalReservedWALCount
    }
    entry.SetLSN(lsn, lsnToTruncate)

    // 5. 准备提交
    if prepared := runner.store.PrepareCommitICKPIntent(entry); !prepared {
        errPhase = "prepare"
        rollback()
        err = moerr.NewInternalErrorNoCtxf("cannot prepare ickp")
        return
    }

    // 6. 保存检查点元数据
    if file, err = runner.saveCheckpoint(entry.start, entry.end); err != nil {
        errPhase = "save-ckp"
        runner.store.RollbackICKPIntent(entry)
        rollback()
        return
    }

    defer func() {
        // 7. 提交 Intent
        runner.store.CommitICKPIntent(entry)
        runner.postCheckpointQueue.Enqueue(entry)
        
        // 8. 尝试触发全局检查点
        runner.TryTriggerExecuteGCKP(&gckpContext{
            end:              entry.end,
            histroyRetention: job.executor.cfg.GlobalHistoryDuration,
            ckpLSN:           lsn,
            truncateLSN:      lsnToTruncate,
        })
    }()

    // 9. 记录 WAL
    files = append(files, file)
    var logEntry wal.LogEntry
    if logEntry, err = runner.wal.RangeCheckpoint(1, lsnToTruncate, files...); err != nil {
        errPhase = "wal-ckp"
        fatal = true
        return
    }
    if err = logEntry.WaitDone(); err != nil {
        errPhase = "wait-wal-ckp-done"
        fatal = true
        return
    }

    return nil
}
```

### 数据收集

```go
func (executor *checkpointExecutor) doIncrementalCheckpoint(
    entry *CheckpointEntry,
) (fields []zap.Field, files []string, err error) {
    // 创建增量检查点数据工厂
    factory := logtail.IncrementalCheckpointDataFactory(
        entry.start, entry.end, 0, executor.runner.rt.Fs,
    )
    
    // 从 catalog 收集数据
    data, err := factory(executor.runner.catalog)
    if err != nil {
        return
    }
    defer data.Close()
    
    // 同步到对象存储
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

## 数据收集器

### BaseCollector_V2

```go
type BaseCollector_V2 struct {
    *catalog.LoopProcessor
    data       *CheckpointData_V2
    objectSize int
    start, end types.TS
    packer     *types.Packer
}

func NewBaseCollector_V2(start, end types.TS, size int, fs fileservice.FileService) *BaseCollector_V2 {
    collector := &BaseCollector_V2{
        LoopProcessor: &catalog.LoopProcessor{},
        data:          NewCheckpointData_V2(common.CheckpointAllocator, size, fs),
        start:         start,
        end:           end,
        objectSize:    size,
        packer:        types.NewPacker(),
    }
    if collector.objectSize == 0 {
        collector.objectSize = DefaultCheckpointSize  // 512MB
    }
    collector.ObjectFn = collector.visitObject
    collector.TombstoneFn = collector.visitObject
    return collector
}
```

### 访问对象

```go
func (collector *BaseCollector_V2) visitObject(entry *catalog.ObjectEntry) error {
    return entry.ForeachMVCCNodeInRange(collector.start, collector.end, func(node *txnbase.TxnMVCCNode) error {
        if node.IsAborted() {
            return nil
        }
        isObjectTombstone := !node.End.Equal(&entry.CreatedAt)
        
        // 收集对象数据
        if err := collectObjectBatch(
            collector.data.batch, entry, isObjectTombstone, collector.packer, collector.data.allocator,
        ); err != nil {
            return err
        }
        
        // 检查是否需要刷盘
        if collector.data.batch.Size() >= collector.objectSize || 
           collector.data.batch.RowCount() >= DefaultCheckpointBlockRows {
            collector.data.sinker.Write(context.Background(), collector.data.batch)
            collector.data.batch.CleanOnlyData()
        }
        return nil
    })
}
```

## 状态转换

```
┌──────────────────────────────────────────────────────────────────┐
│                    ICKP Intent 状态转换                          │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  nil ──────────────────────────────────────────────────────────▶ │
│   │                                                              │
│   │ UpdateICKPIntent()                                           │
│   ▼                                                              │
│  Pending ─────────────────────────────────────────────────────▶  │
│   │  │                                                           │
│   │  │ UpdateICKPIntent() - 扩展 end                             │
│   │  └──────────────────────────────────────────────────────────▶│
│   │                                                              │
│   │ TakeICKPIntent()                                             │
│   ▼                                                              │
│  Running ────────────────────────────────────────────────────▶   │
│   │                                                              │
│   │ CommitICKPIntent()                                           │
│   ▼                                                              │
│  Finished ───────────────────────────────────────────────────▶   │
│   │                                                              │
│   │ (存入 incrementals btree)                                    │
│   ▼                                                              │
│  nil (intent 清空)                                               │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

## 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `MinCount` | 10000 | 最小事务数 |
| `IncrementalInterval` | 1 分钟 | 增量检查点间隔 |
| `IncrementalReservedWALCount` | 0 | 保留的 WAL 条目数 |

## 监控指标

```go
v2.TaskCkpEntryPendingDurationHistogram.Observe(entry.Age().Seconds())

perfcounter.Update(executor.ctx, func(counter *perfcounter.CounterSet) {
    counter.TAE.CheckPoint.DoIncrementalCheckpoint.Add(1)
})
```
