# Checkpoint 模块代码质量问题

## 1. 函数复杂度过高

### 1.1 UpdateICKPIntent 函数过于复杂

**位置**: `store.go` - `UpdateICKPIntent` 方法

```go
func (s *runnerStore) UpdateICKPIntent(
	ts *types.TS, policyChecked, flushChecked bool,
) (intent *CheckpointEntry, updated bool) {
	for {
		old := s.incrementalIntent.Load()
		// 场景1: 检查是否需要减少 end ts
		if old != nil && !old.AllChecked() && policyChecked && flushChecked {
			checkpointed := s.GetCheckpointed()
			if checkpointed.GT(&old.end) {
				s.incrementalIntent.CompareAndSwap(old, nil)
				continue
			}
			// ... 更多嵌套逻辑
		}
		// 场景2: 检查是否可以更新
		if old != nil && (old.end.GT(ts) || !old.IsPendding() || old.Age() > s.intentOldAge) {
			intent = old
			return
		}
		// ... 更多场景处理
	}
}
```

**问题**:
- 函数超过 80 行，包含多个嵌套的 CAS 循环
- 多个场景分支，逻辑难以理解
- 注释虽然存在但不足以解释复杂的状态转换

**建议**:
```go
// 拆分为多个小函数
func (s *runnerStore) UpdateICKPIntent(...) (intent *CheckpointEntry, updated bool) {
    for {
        old := s.incrementalIntent.Load()
        
        if result := s.tryDecreaseIntent(old, ts, policyChecked, flushChecked); result.shouldContinue {
            continue
        } else if result.done {
            return result.intent, result.updated
        }
        
        if result := s.tryExtendIntent(old, ts, policyChecked, flushChecked); result.done {
            return result.intent, result.updated
        }
        
        // ... 其他场景
    }
}
```

### 1.2 collectTableMemUsage 函数职责过多

**位置**: `flusher.go` - `collectTableMemUsage` 方法

```go
func (flusher *flushImpl) collectTableMemUsage(entry *logtail.DirtyTreeEntry, lastCkp types.TS) (memPressureRate float64) {
    // 1. 重置列表
    flusher.objMemSizeList = flusher.objMemSizeList[:0]
    
    // 2. 遍历收集内存使用
    sizevisitor := new(model.BaseTreeVisitor)
    // ...
    
    // 3. 排序
    slices.SortFunc(flusher.objMemSizeList, ...)
    
    // 4. 计算压力
    pressure := float64(totalSize) / float64(common.RuntimeOverallFlushMemCap.Load())
    
    // 5. 日志节流逻辑
    flusher.logThrottleMu.Lock()
    // ... 复杂的节流判断
    flusher.logThrottleMu.Unlock()
    
    return pressure
}
```

**问题**:
- 单个函数承担了收集、排序、计算、日志等多个职责
- 日志节流逻辑与核心功能混杂

**建议**:
- 将日志节流逻辑抽取为独立的 `logThrottler` 结构
- 将内存收集和压力计算分离

---

## 2. 错误处理问题

### 2.1 使用 panic 而非错误返回

**位置**: `flusher.go` - `collectTableMemUsage`

```go
sizevisitor.TableFn = func(did, tid uint64) error {
    db, err := flusher.catalogCache.GetDatabaseByID(did)
    if err != nil {
        panic(err)  // 🔴 不应该使用 panic
    }
    table, err := db.GetTableEntryByID(tid)
    if err != nil {
        panic(err)  // 🔴 不应该使用 panic
    }
    // ...
}
if err := entry.GetTree().Visit(sizevisitor); err != nil {
    panic(err)  // 🔴 不应该使用 panic
}
```

**问题**:
- 使用 `panic` 处理可恢复的错误
- 可能导致整个进程崩溃
- 难以进行错误追踪和恢复

**建议**:
```go
func (flusher *flushImpl) collectTableMemUsage(...) (memPressureRate float64, err error) {
    sizevisitor.TableFn = func(did, tid uint64) error {
        db, err := flusher.catalogCache.GetDatabaseByID(did)
        if err != nil {
            // 记录日志并跳过该表
            logutil.Warn("skip table due to db not found", zap.Uint64("dbID", did), zap.Error(err))
            return moerr.GetOkStopCurrRecur()
        }
        // ...
    }
    // ...
}
```

### 2.2 错误被静默忽略

**位置**: `executor.go` - `RunICKP`

```go
func (executor *checkpointExecutor) RunICKP() (err error) {
    if !executor.active.Load() {
        err = ErrCheckpointDisabled
        return
    }
    if executor.runningICKP.Load() != nil {
        err = ErrPendingCheckpoint  // 🟠 这里设置了 err 但没有 return
    }
    // 继续执行...
}
```

**问题**: 设置了错误但没有立即返回，可能导致意外行为。

### 2.3 Fatal 日志使用不当

**位置**: `store.go` - `RollbackICKPIntent`

```go
func (s *runnerStore) RollbackICKPIntent(intent *CheckpointEntry) {
    expect := s.incrementalIntent.Load()
    if intent != expect || !intent.IsRunning() {
        logutil.Fatal(  // 🔴 使用 Fatal 会导致进程退出
            "ICKP-Rollback",
            zap.Any("expected", expect),
            zap.Any("actual", intent),
        )
    }
    // ...
}
```

**问题**: `logutil.Fatal` 会导致进程退出，应该考虑更优雅的错误处理。

---

## 3. 魔法数字和硬编码

### 3.1 硬编码的数值

**位置**: 多处

```go
// flusher.go
metaChunks := slices.Chunk(metas, 400)  // 🟡 为什么是 400？

// flusher.go - checkFlushConditionAndFire
if pressure < 0.5 || count < 200 {  // 🟡 0.5 和 200 的含义？
    return true
}

// flusher.go - collectTableMemUsage
const (
    logInterval           = 1 * time.Minute
    changeThreshold       = 0.2
    absoluteSizeThreshold = 256 * 1024 * 1024
    pressureThreshold     = 0.5
)

// store.go
s.intentOldAge = time.Minute * 2  // 🟡 硬编码的超时时间
```

**建议**: 将这些值移到配置结构中：

```go
type FlushConfig struct {
    ChunkSize             int           `default:"400"`
    PressureThreshold     float64       `default:"0.5"`
    MaxFlushCountPerRound int           `default:"200"`
    LogInterval           time.Duration `default:"1m"`
    IntentOldAge          time.Duration `default:"2m"`
}
```

### 3.2 硬编码的队列大小

**位置**: `executor.go`

```go
e.ickpQueue = sm.NewSafeQueue(1000, 100, e.onICKPEntries)
e.gckpQueue = sm.NewSafeQueue(1000, 100, e.onGCKPEntries)
```

**位置**: `runner.go`

```go
r.gcCheckpointQueue = sm.NewSafeQueue(100, 100, r.onGCCheckpointEntries)
r.postCheckpointQueue = sm.NewSafeQueue(1000, 1, r.onPostCheckpointEntries)
r.replayQueue = sm.NewSafeQueue(100, 10, r.onReplayCheckpoint)
```

**问题**: 队列大小硬编码，无法根据负载调整。

---

## 4. 代码重复

### 4.1 检查点条目查找逻辑重复

**位置**: `executor.go` - `RunICKP` 和 `doGlobalCheckpoint`

```go
// RunICKP 中
gckps := runner.store.GetAllGlobalCheckpoints()
var prevCkp *CheckpointEntry
for _, ckp := range gckps {
    if ckp.end.EQ(&entry.start) {
        prevCkp = ckp
        break
    }
}
if prevCkp == nil {
    ickps := runner.store.GetAllIncrementalCheckpoints()
    prevEnd := entry.start.Prev()
    for _, ckp := range ickps {
        if ckp.end.EQ(&prevEnd) {
            prevCkp = ckp
            break
        }
    }
}

// doGlobalCheckpoint 中类似的逻辑
ickps := runner.store.GetAllIncrementalCheckpoints()
var preICKP *CheckpointEntry
ickpEnd := entry.end.Prev()
for _, ckp := range ickps {
    if ckp.end.EQ(&ickpEnd) {
        preICKP = ckp
        break
    }
}
```

**建议**: 抽取公共方法：

```go
func (s *runnerStore) FindPreviousCheckpoint(ts types.TS) *CheckpointEntry {
    // 统一的查找逻辑
}
```

### 4.2 foreachAobjBefore 回调模式重复

**位置**: `flusher.go`

```go
// collectTableMemUsage 中
df := func(obj *catalog.ObjectEntry) {
    asize += obj.GetObjectData().EstimateMemSize()
}
tf := func(obj *catalog.ObjectEntry) {
    dsize += obj.GetObjectData().EstimateMemSize()
}
foreachAobjBefore(context.Background(), table, end, lastCkp, df, tf)

// fireFlushTabletail 中
df := func(obj *catalog.ObjectEntry) {
    metas = append(metas, obj)
}
tf := func(obj *catalog.ObjectEntry) {
    tombstoneMetas = append(tombstoneMetas, obj)
}
foreachAobjBefore(context.Background(), table, end, lastCkp, df, tf)
```

**建议**: 考虑使用更通用的迭代器模式或返回切片。

---

## 5. 注释问题

### 5.1 TODO 注释未处理

**位置**: `executor.go`

```go
// PXU TODO: if crash here, the checkpoint log entry will be lost
var logEntry wal.LogEntry
if logEntry, err = runner.wal.RangeCheckpoint(1, lsnToTruncate, files...); err != nil {
    errPhase = "wal-ckp"
    fatal = true
    return
}
```

**问题**: 这是一个严重的可靠性问题，但只有 TODO 注释，没有实际解决方案。

### 5.2 注释与代码不一致

**位置**: `runner.go`

```go
// Q: How to do incremental checkpoint?
// A: 1. Decide a checkpoint timestamp
//    2. Wait all transactions before timestamp were committed
//    3. Wait all dirty blocks before the timestamp were flushed
//    ...
```

**问题**: 注释描述的流程与实际代码实现可能存在差异，需要验证和更新。

---

## 6. 命名问题

### 6.1 缩写不一致

```go
// 有时使用完整名称
incrementalIntent
incrementals
globals

// 有时使用缩写
ICKP  // Incremental Checkpoint
GCKP  // Global Checkpoint
ckp   // checkpoint
```

**建议**: 统一命名规范，在代码中添加缩写说明。

### 6.2 变量命名不清晰

```go
// flusher.go
for _, ticket := range flusher.objMemSizeList {
    table, asize, dsize := ticket.tbl, ticket.asize, ticket.dsize
    // asize = append size? active size?
    // dsize = delete size? data size?
}
```

**建议**: 使用更清晰的命名如 `appendableSize`, `tombstoneSize`。
