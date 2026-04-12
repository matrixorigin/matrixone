# 全局检查点（GCKP）详解

## 概述

全局检查点（Global Checkpoint，GCKP）包含系统在某个时间点的完整快照。它的时间范围是 `[0, end]`，包含了从系统启动到 `end` 时间点的所有数据。

## 特点

- **完整性**：包含系统的完整状态
- **执行频率低**：相比增量检查点，执行频率较低
- **开销大**：需要扫描所有数据
- **支持 GC**：全局检查点是 GC 的基础

## 触发条件

### 基于计数的策略

```go
type countBasedPolicy struct {
    minCount int  // 默认 40
}

func (p *countBasedPolicy) Check(current int) bool {
    return current >= p.minCount
}
```

全局检查点在以下情况下触发：

1. **增量检查点数量达到阈值**：当未被全局检查点覆盖的增量检查点数量达到 `GlobalMinCount`（默认 40）时
2. **强制触发**：通过 `force=true` 参数强制触发

### 检查逻辑

```go
func (executor *checkpointExecutor) onGCKPEntries(items ...any) {
    // ...
    
    // 检查是否已有更新的全局检查点
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

    // 非强制模式下检查增量检查点数量
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

func (g gckpContext) String() string {
    return fmt.Sprintf(
        "GCTX[%v][%s][%d,%d][%s]",
        g.force,
        g.end.ToString(),
        g.truncateLSN,
        g.ckpLSN,
        g.histroyRetention.String(),
    )
}

// 合并多个上下文
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

## 执行流程

### Job 执行

```go
func (job *checkpointJob) RunGCKP(ctx context.Context) (err error) {
    if job.runGCKPFunc != nil {
        return job.runGCKPFunc(ctx, job.gckpCtx, job.executor.runner)
    }

    // 用于混沌测试的等待点
    objectio.WaitInjected(objectio.FJ_GCKPWait1)
    objectio.WaitInjected(objectio.FJ_GCKPWait1)

    _, err = job.doGlobalCheckpoint(
        job.gckpCtx.end,
        job.gckpCtx.ckpLSN,
        job.gckpCtx.truncateLSN,
        job.gckpCtx.histroyRetention,
    )

    return
}
```

### doGlobalCheckpoint

```go
func (job *checkpointJob) doGlobalCheckpoint(
    end types.TS, ckpLSN, truncateLSN uint64, interval time.Duration,
) (entry *CheckpointEntry, err error) {
    var (
        errPhase string
        fields   []zap.Field
        now      = time.Now()
        runner   = job.executor.runner
    )

    // 1. 创建全局检查点条目
    entry = NewCheckpointEntry(
        runner.rt.SID(),
        types.TS{},  // start = 0
        end.Next(),
        ET_Global,
    )
    entry.ckpLSN = ckpLSN
    entry.truncateLSN = truncateLSN

    logutil.Info("GCKP-Execute-Start", zap.String("entry", entry.String()))

    defer func() {
        if err != nil {
            logutil.Error("GCKP-Execute-Error",
                zap.String("entry", entry.String()),
                zap.String("phase", errPhase),
                zap.Error(err),
                zap.Duration("cost", time.Since(now)),
            )
        } else {
            fields = append(fields, zap.Duration("cost", time.Since(now)))
            fields = append(fields, zap.String("entry", entry.String()))
            logutil.Info("GCKP-Execute-End", fields...)
        }
    }()

    // 2. 添加 GCKP Intent
    if ok := runner.store.AddGCKPIntent(entry); !ok {
        err = ErrBadIntent
        return
    }

    // 3. 收集全局检查点数据
    var data *logtail.CheckpointData_V2
    factory := logtail.GlobalCheckpointDataFactory(entry.end, interval, runner.rt.Fs)

    if data, err = factory(runner.catalog); err != nil {
        runner.store.RemoveGCKPIntent()
        errPhase = "collect"
        return
    }
    defer data.Close()

    // 4. 同步数据到存储
    location, files, err := data.Sync(job.executor.ctx, runner.rt.Fs)
    fields = data.ExportStats("")
    if err != nil {
        runner.store.RemoveGCKPIntent()
        errPhase = "flush"
        return
    }

    entry.SetLocation(location, location)

    // 5. 同步 TableID 批次
    ickps := runner.store.GetAllIncrementalCheckpoints()
    var preICKP *CheckpointEntry
    ickpEnd := entry.end.Prev()
    for _, ckp := range ickps {
        if ckp.end.EQ(&ickpEnd) {
            preICKP = ckp
            break
        }
    }

    var emptyLocation objectio.Location
    tableIDLocation, err := logtail.SyncTableIDBatch(
        job.executor.ctx,
        entry.start,
        entry.end,
        job.executor.cfg.TableIDHistoryDuration,
        job.executor.cfg.TableIDSinkerThreshold,
        emptyLocation,
        preICKP.GetVersion(),
        preICKP.GetTableIDLocation(),
        common.CheckpointAllocator,
        runner.rt.Fs,
    )
    if err != nil {
        runner.store.RemoveGCKPIntent()
        errPhase = "sync-table-id"
        return
    }
    entry.SetTableIDLocation(tableIDLocation)

    // 6. 保存检查点元数据
    files = append(files, location.Name().String())
    var name string
    if name, err = runner.saveCheckpoint(entry.start, entry.end); err != nil {
        runner.store.RemoveGCKPIntent()
        errPhase = "save"
        return
    }
    
    defer func() {
        entry.SetState(ST_Finished)
    }()

    // 7. 记录 WAL
    files = append(files, name)
    fileEntry, err := wal.BuildFilesEntry(files)
    if err != nil {
        return
    }
    _, err = runner.wal.AppendEntry(wal.GroupFiles, fileEntry)
    if err != nil {
        return
    }
    
    perfcounter.Update(job.executor.ctx, func(counter *perfcounter.CounterSet) {
        counter.TAE.CheckPoint.DoGlobalCheckPoint.Add(1)
    })
    return
}
```

## 全局数据收集器

### GlobalCollector_V2

```go
type GlobalCollector_V2 struct {
    BaseCollector_V2
    // 不收集在 versionThershold 之前删除的对象
    versionThershold types.TS
}

func NewGlobalCollector_V2(
    fs fileservice.FileService,
    end types.TS,
    versionInterval time.Duration,
) *GlobalCollector_V2 {
    versionThresholdTS := types.BuildTS(
        end.Physical()-versionInterval.Nanoseconds(), 
        end.Logical(),
    )
    collector := &GlobalCollector_V2{
        BaseCollector_V2: *NewBaseCollector_V2(types.TS{}, end, 0, fs),
        versionThershold: versionThresholdTS,
    }
    collector.ObjectFn = collector.visitObjectForGlobal
    collector.TombstoneFn = collector.visitObjectForGlobal

    return collector
}
```

### 访问对象（全局模式）

```go
func (collector *GlobalCollector_V2) visitObjectForGlobal(entry *catalog.ObjectEntry) error {
    // 跳过在阈值之前删除的对象
    if entry.DeleteBefore(collector.versionThershold) {
        return nil
    }
    // 跳过已删除的表
    if collector.isEntryDeletedBeforeThreshold(entry.GetTable().BaseEntryImpl) {
        return nil
    }
    // 跳过已删除的数据库
    if collector.isEntryDeletedBeforeThreshold(entry.GetTable().GetDB().BaseEntryImpl) {
        return nil
    }
    return collector.BaseCollector_V2.visitObject(entry)
}

func (collector *GlobalCollector_V2) isEntryDeletedBeforeThreshold(entry catalog.BaseEntry) bool {
    entry.RLock()
    defer entry.RUnlock()
    return entry.DeleteBeforeLocked(collector.versionThershold)
}
```

## Intent 管理

### 添加 GCKP Intent

```go
func (s *runnerStore) AddGCKPIntent(intent *CheckpointEntry) (success bool) {
    if intent == nil || intent.entryType != ET_Global || intent.IsFinished() {
        return false
    }

    s.Lock()
    defer s.Unlock()

    maxEntry, _ := s.globals.Max()
    if maxEntry != nil && (maxEntry.end.GE(&intent.end) || !maxEntry.IsFinished()) {
        return false
    }

    s.globals.Set(intent)
    return true
}
```

### 移除 GCKP Intent

```go
func (s *runnerStore) RemoveGCKPIntent() (ok bool) {
    s.Lock()
    defer s.Unlock()
    intent, _ := s.globals.Max()
    if intent == nil || intent.IsFinished() {
        return false
    }
    s.globals.Delete(intent)
    return true
}
```

## 与增量检查点的关系

```
时间线:
─────────────────────────────────────────────────────────────────▶
    │         │         │         │         │         │
  ICKP1     ICKP2     ICKP3     ...     ICKP40      GCKP
  [0,t1]   [t1+1,t2] [t2+1,t3]         [t39+1,t40] [0,t40]
    │         │         │         │         │         │
    └─────────┴─────────┴─────────┴─────────┴─────────┘
                        │
                        ▼
              当 ICKP 数量达到 40 时
              触发 GCKP，覆盖所有 ICKP
```

## 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `GlobalMinCount` | 40 | 触发全局检查点的最小增量检查点数 |
| `GlobalHistoryDuration` | 0 | 全局历史保留时间 |

## 监控指标

```go
perfcounter.Update(job.executor.ctx, func(counter *perfcounter.CounterSet) {
    counter.TAE.CheckPoint.DoGlobalCheckPoint.Add(1)
})
```

## 执行流程图

```
┌─────────────────────────────────────────────────────────────────┐
│                    GCKP 执行流程                                 │
├─────────────────────────────────────────────────────────────────┤
│  1. ICKP 完成后触发 TryTriggerExecuteGCKP()                     │
│     │                                                           │
│     ▼                                                           │
│  2. gckpQueue.Enqueue(gckpContext)                              │
│     │                                                           │
│     ▼                                                           │
│  3. onGCKPEntries() - 合并上下文，检查策略                       │
│     │                                                           │
│     ▼                                                           │
│  4. RunGCKP() -> doGlobalCheckpoint()                           │
│     │                                                           │
│     ▼                                                           │
│  5. AddGCKPIntent() - 添加 Intent                               │
│     │                                                           │
│     ▼                                                           │
│  6. GlobalCheckpointDataFactory() - 收集全局数据                 │
│     │                                                           │
│     ▼                                                           │
│  7. data.Sync() - 同步到存储                                    │
│     │                                                           │
│     ▼                                                           │
│  8. SyncTableIDBatch() - 同步 TableID                           │
│     │                                                           │
│     ▼                                                           │
│  9. saveCheckpoint() - 保存元数据                               │
│     │                                                           │
│     ▼                                                           │
│  10. WAL 记录                                                   │
│     │                                                           │
│     ▼                                                           │
│  11. SetState(ST_Finished)                                      │
└─────────────────────────────────────────────────────────────────┘
```
