# Checkpoint 模块架构设计问题

## 1. 模块耦合问题

### 1.1 Runner 与 Executor 职责边界模糊

**位置**: `runner.go` 和 `executor.go`

```go
// runner.go
type runner struct {
    executor atomic.Pointer[checkpointExecutor]
    store    *runnerStore
    // ...
}

func (r *runner) TryScheduleCheckpoint(ts types.TS, force bool) (ret Intent, err error) {
    executor := r.executor.Load()
    return executor.TryScheduleCheckpoint(ts, force)  // 直接委托给 executor
}

// executor.go
type checkpointExecutor struct {
    runner *runner  // 反向引用 runner
    // ...
}

func (executor *checkpointExecutor) TryScheduleCheckpoint(...) {
    // 又调用 runner.store 的方法
    intent, updated := executor.runner.store.UpdateICKPIntent(...)
}
```

**问题**:
- Runner 和 Executor 相互引用，形成循环依赖
- 职责划分不清晰：调度逻辑分散在两个组件中
- 难以独立测试和替换

**建议**:
```go
// 明确职责划分
// Runner: 生命周期管理、外部接口
// Executor: 检查点执行逻辑
// Store: 状态存储

type CheckpointScheduler interface {
    Schedule(ts types.TS, force bool) (Intent, error)
}

type CheckpointExecutor interface {
    ExecuteICKP(ctx context.Context, entry *CheckpointEntry) error
    ExecuteGCKP(ctx context.Context, entry *CheckpointEntry) error
}

type CheckpointStore interface {
    GetIntent() *CheckpointEntry
    UpdateIntent(entry *CheckpointEntry) bool
    // ...
}
```

### 1.2 Flusher 与 CheckpointScheduler 耦合

**位置**: `flusher.go`

```go
type flushImpl struct {
    checkpointSchduler CheckpointScheduler  // 拼写错误: Schduler -> Scheduler
    // ...
}

func (flusher *flushImpl) triggerJob(ctx context.Context) {
    // Flusher 直接触发 checkpoint 调度
    flusher.checkpointSchduler.TryScheduleCheckpoint(ts, false)
}
```

**问题**:
- Flusher 不应该直接调度 checkpoint
- 应该通过事件或回调机制解耦

**建议**:
```go
type FlushCompleteHandler func(ts types.TS)

type Flusher interface {
    OnFlushComplete(handler FlushCompleteHandler)
    // ...
}
```

---

## 2. 状态管理问题

### 2.1 Intent 状态机不明确

**位置**: `store.go`

```go
// CheckpointEntry 的状态转换
const (
    ST_Pending  = iota  // 等待执行
    ST_Running          // 正在执行
    ST_Finished         // 执行完成
)
```

**问题**:
- 状态转换规则分散在多个方法中
- 没有明确的状态机定义
- 某些状态转换可能不合法但没有检查

**当前状态转换**:
```
                    UpdateICKPIntent
    nil ─────────────────────────────────> Pending
                                              │
                    TakeICKPIntent            │
    Pending ──────────────────────────────> Running
                                              │
                    CommitICKPIntent          │
    Running ──────────────────────────────> Finished
                                              │
                    RollbackICKPIntent        │
    Running ──────────────────────────────> (deleted)
```

**建议**: 使用显式的状态机：

```go
type IntentStateMachine struct {
    current State
    transitions map[State][]State
}

func (sm *IntentStateMachine) CanTransition(to State) bool {
    allowed := sm.transitions[sm.current]
    return slices.Contains(allowed, to)
}

func (sm *IntentStateMachine) Transition(to State) error {
    if !sm.CanTransition(to) {
        return ErrInvalidStateTransition
    }
    sm.current = to
    return nil
}
```

### 2.2 多个原子变量管理复杂

**位置**: `store.go`

```go
type runnerStore struct {
    sync.RWMutex
    
    incrementalIntent atomic.Pointer[CheckpointEntry]  // 原子指针
    compacted         atomic.Pointer[CheckpointEntry]  // 原子指针
    gcWatermark       atomic.Value                     // 原子值
    gcIntent          types.TS                         // 普通字段，需要锁保护
    
    incrementals *btree.BTreeG[*CheckpointEntry]       // 需要锁保护
    globals      *btree.BTreeG[*CheckpointEntry]       // 需要锁保护
}
```

**问题**:
- 混合使用原子操作和锁
- 难以保证多个字段之间的一致性
- 增加了并发编程的复杂度

**建议**: 统一并发控制策略：

```go
// 方案1: 全部使用锁
type runnerStore struct {
    mu sync.RWMutex
    
    incrementalIntent *CheckpointEntry
    compacted         *CheckpointEntry
    gcWatermark       types.TS
    // ...
}

// 方案2: 使用不可变数据结构
type storeSnapshot struct {
    incrementalIntent *CheckpointEntry
    compacted         *CheckpointEntry
    // ...
}

type runnerStore struct {
    snapshot atomic.Pointer[storeSnapshot]
}
```

---

## 3. 接口设计问题

### 3.1 CheckpointScheduler 接口过大

**位置**: `types.go` (推断)

```go
type CheckpointScheduler interface {
    TryScheduleCheckpoint(ts types.TS, force bool) (Intent, error)
    MaxIncrementalCheckpoint() *CheckpointEntry
    // 可能还有更多方法...
}
```

**问题**: Flusher 只需要调度功能，但可能暴露了过多的方法。

**建议**: 接口隔离

```go
type CheckpointTrigger interface {
    TryScheduleCheckpoint(ts types.TS, force bool) (Intent, error)
}

type CheckpointQuery interface {
    MaxIncrementalCheckpoint() *CheckpointEntry
    MaxGlobalCheckpoint() *CheckpointEntry
    GetAllCheckpoints() []*CheckpointEntry
}

type CheckpointScheduler interface {
    CheckpointTrigger
    CheckpointQuery
}
```

### 3.2 Collector 接口职责不清

**位置**: `collector.go`

```go
type Collector interface {
    String() string
    Run(lag time.Duration)
    ScanInRange(from, to types.TS) (*DirtyTreeEntry, int)
    ScanInRangePruned(from, to types.TS) *DirtyTreeEntry
    GetAndRefreshMerged() *DirtyTreeEntry
    Merge() *DirtyTreeEntry
    GetMaxLSN(from, to types.TS) uint64
    Init(maxts types.TS)
}
```

**问题**:
- `Run` 方法暗示这是一个主动组件，但其他方法是被动查询
- `String()` 方法不应该在业务接口中
- `Init` 方法暗示对象创建后需要额外初始化

**建议**:
```go
type DirtyDataScanner interface {
    ScanInRange(from, to types.TS) (*DirtyTreeEntry, int)
    ScanInRangePruned(from, to types.TS) *DirtyTreeEntry
    GetMaxLSN(from, to types.TS) uint64
}

type DirtyDataCollector interface {
    DirtyDataScanner
    Run(lag time.Duration)
    GetMerged() *DirtyTreeEntry
}
```

---

## 4. 数据流问题

### 4.1 检查点数据流复杂

```
                                    ┌─────────────┐
                                    │   Flusher   │
                                    └──────┬──────┘
                                           │ triggerJob
                                           ▼
┌─────────────┐    ScanInRange    ┌─────────────┐
│  Collector  │◄──────────────────│   Runner    │
└─────────────┘                   └──────┬──────┘
                                         │ TryScheduleCheckpoint
                                         ▼
                                  ┌─────────────┐
                                  │  Executor   │
                                  └──────┬──────┘
                                         │ UpdateICKPIntent
                                         ▼
                                  ┌─────────────┐
                                  │    Store    │
                                  └─────────────┘
```

**问题**:
- 数据流方向不一致
- Flusher 既是数据生产者又是调度触发者
- 难以追踪检查点的完整生命周期

### 4.2 回调和队列混用

**位置**: `runner.go`

```go
type runner struct {
    observers           *observers              // 观察者模式
    postCheckpointQueue sm.Queue                // 队列模式
    gcCheckpointQueue   sm.Queue                // 队列模式
    replayQueue         sm.Queue                // 队列模式
}
```

**问题**:
- 同时使用观察者模式和队列模式
- 事件处理方式不统一
- 难以理解事件的处理顺序

---

## 5. 配置管理问题

### 5.1 配置分散

**位置**: 多处

```go
// runner.go
r.store = newRunnerStore(r.rt.SID(), cfg.GlobalHistoryDuration, time.Minute*2)

// executor.go
type CheckpointCfg struct {
    IncrementalInterval        time.Duration
    GlobalMinCount             uint64
    MinCount                   uint64
    // ...
}

// flusher.go
type FlushCfg struct {
    ForceFlushTimeout       time.Duration
    ForceFlushCheckInterval time.Duration
    FlushInterval           time.Duration
    CronPeriod              time.Duration
}
```

**问题**:
- 配置分散在多个结构中
- 部分配置硬编码
- 配置之间可能存在依赖关系但没有验证

**建议**: 统一配置管理

```go
type CheckpointConfig struct {
    // 增量检查点配置
    Incremental struct {
        Interval    time.Duration
        MinCount    uint64
        ReservedWAL uint64
    }
    
    // 全局检查点配置
    Global struct {
        MinCount        uint64
        HistoryDuration time.Duration
    }
    
    // 刷盘配置
    Flush struct {
        Interval      time.Duration
        ForceTimeout  time.Duration
        CheckInterval time.Duration
    }
    
    // 队列配置
    Queue struct {
        ICKPSize int
        GCKPSize int
        // ...
    }
}

func (c *CheckpointConfig) Validate() error {
    // 验证配置之间的依赖关系
}
```

### 5.2 运行时配置变更支持不完整

**位置**: `flusher.go`

```go
func (f *flusher) ChangeForceFlushTimeout(timeout time.Duration) {
    impl := f.impl.Load()
    if impl == nil {
        logutil.Warn("flusher.stopped")
        return
    }
    impl.ChangeForceFlushTimeout(timeout)
}
```

**问题**:
- 只有部分配置支持运行时变更
- 变更后没有通知机制
- 没有配置变更的审计日志

---

## 6. 错误传播问题

### 6.1 错误信息丢失

**位置**: `executor.go`

```go
func (job *checkpointJob) RunICKP(ctx context.Context) (err error) {
    // ...
    if fields, files, err = job.executor.doIncrementalCheckpoint(entry); err != nil {
        errPhase = "do-ckp"
        rollback()
        return  // 只返回 err，丢失了 errPhase 信息
    }
    // ...
}
```

**问题**:
- 错误阶段信息只用于日志，不包含在返回的错误中
- 调用者无法知道具体失败的阶段

**建议**:
```go
type CheckpointError struct {
    Phase   string
    Cause   error
    Entry   *CheckpointEntry
    Context map[string]interface{}
}

func (e *CheckpointError) Error() string {
    return fmt.Sprintf("checkpoint failed at phase %s: %v", e.Phase, e.Cause)
}

func (e *CheckpointError) Unwrap() error {
    return e.Cause
}
```

### 6.2 异步错误处理不完整

**位置**: `runner.go`

```go
func (r *runner) onPostCheckpointEntries(entries ...any) {
    for _, e := range entries {
        entry := e.(*CheckpointEntry)
        r.observers.OnNewCheckpoint(entry.GetEnd())
        // 如果 observer 处理失败怎么办？
    }
}
```

**问题**: 队列处理函数中的错误没有被妥善处理。
