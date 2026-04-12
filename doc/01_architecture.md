# Checkpoint 架构设计

## 整体架构

TAE Checkpoint 模块采用分层架构设计，主要包含以下层次：

```
┌────────────────────────────────────────────────────────────────────┐
│                        Application Layer                            │
│                    (TryScheduleCheckpoint API)                      │
└────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌────────────────────────────────────────────────────────────────────┐
│                        Runner Layer                                 │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │                    Checkpoint Runner                          │  │
│  │  - 生命周期管理                                                │  │
│  │  - 调度协调                                                    │  │
│  │  - 观察者通知                                                  │  │
│  └──────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌────────────────────────────────────────────────────────────────────┐
│                       Executor Layer                                │
│  ┌─────────────────────┐    ┌─────────────────────┐                │
│  │   ICKP Executor     │    │   GCKP Executor     │                │
│  │   (增量检查点执行)    │    │   (全局检查点执行)    │                │
│  └─────────────────────┘    └─────────────────────┘                │
└────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌────────────────────────────────────────────────────────────────────┐
│                        Data Layer                                   │
│  ┌─────────────────────┐    ┌─────────────────────┐                │
│  │   Collector         │    │   Writer            │                │
│  │   (数据收集)         │    │   (数据写入)         │                │
│  └─────────────────────┘    └─────────────────────┘                │
└────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌────────────────────────────────────────────────────────────────────┐
│                       Storage Layer                                 │
│  ┌─────────────────────┐    ┌─────────────────────┐                │
│  │   Object Storage    │    │   WAL               │                │
│  │   (对象存储)         │    │   (预写日志)         │                │
│  └─────────────────────┘    └─────────────────────┘                │
└────────────────────────────────────────────────────────────────────┘
```

## 核心接口定义

### Runner 接口

```go
type Runner interface {
    ReplayClient
    CheckpointScheduler
    TestRunner
    RunnerWriter
    RunnerReader

    Start()
    Stop()

    BuildReplayer(string) *CkpReplayer
    GCByTS(ctx context.Context, ts types.TS) error
}
```

### CheckpointScheduler 接口

```go
type CheckpointScheduler interface {
    TryScheduleCheckpoint(types.TS, bool) (Intent, error)
    RunnerReader
}
```

### Flusher 接口

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

## 组件交互流程

### 1. 增量检查点流程

```
┌─────────┐     ┌──────────┐     ┌──────────┐     ┌─────────┐
│ Trigger │────▶│ Schedule │────▶│ Execute  │────▶│ Persist │
└─────────┘     └──────────┘     └──────────┘     └─────────┘
     │               │                │                │
     │               │                │                │
     ▼               ▼                ▼                ▼
  定时/手动      创建 Intent      收集数据         写入存储
  触发检查点     检查条件         生成 Batch       更新元数据
```

### 2. 全局检查点流程

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ ICKP Count  │────▶│ Check Policy│────▶│ Execute GCKP│
│ Threshold   │     │             │     │             │
└─────────────┘     └─────────────┘     └─────────────┘
       │                  │                   │
       │                  │                   │
       ▼                  ▼                   ▼
  增量检查点数量      检查是否满足        执行全局检查点
  达到阈值           全局检查点条件       合并所有数据
```

## 状态机

### CheckpointEntry 状态

```
┌──────────┐     ┌──────────┐     ┌──────────┐
│ Pending  │────▶│ Running  │────▶│ Finished │
└──────────┘     └──────────┘     └──────────┘
     │                │                │
     │                │                │
     ▼                ▼                ▼
  等待执行          正在执行          执行完成
  可被更新          不可更新          不可更新
```

```go
type State int8

const (
    ST_Running State = iota
    ST_Pending
    ST_Finished
)
```

## 并发控制

### 1. Intent 机制

Checkpoint 使用 Intent（意图）机制来协调并发：

- **ICKP Intent**：增量检查点意图，使用原子指针管理
- **GCKP Intent**：全局检查点意图，存储在 globals btree 中

### 2. 锁策略

```go
type runnerStore struct {
    sync.RWMutex
    // ...
    incrementalIntent atomic.Pointer[CheckpointEntry]
    incrementals *btree.BTreeG[*CheckpointEntry]
    globals      *btree.BTreeG[*CheckpointEntry]
}
```

- 读操作使用 RLock
- 写操作使用 Lock
- Intent 更新使用 CAS（Compare-And-Swap）

## 队列系统

Runner 使用多个队列来处理不同类型的任务：

```go
type runner struct {
    // ...
    postCheckpointQueue sm.Queue  // 检查点完成后的处理
    gcCheckpointQueue   sm.Queue  // GC 相关处理
    replayQueue         sm.Queue  // 恢复相关处理
}

type checkpointExecutor struct {
    // ...
    ickpQueue sm.Queue  // 增量检查点队列
    gckpQueue sm.Queue  // 全局检查点队列
}
```

## 观察者模式

Checkpoint 支持观察者模式，允许其他组件监听检查点事件：

```go
type Observer interface {
    OnNewCheckpoint(ts types.TS)
}

type observers struct {
    os []Observer
}

func (os *observers) OnNewCheckpoint(ts types.TS) {
    for _, o := range os.os {
        o.OnNewCheckpoint(ts)
    }
}
```

## 与其他模块的关系

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Catalog   │◀───▶│ Checkpoint  │◀───▶│    WAL      │
└─────────────┘     └─────────────┘     └─────────────┘
       │                  │                   │
       │                  │                   │
       ▼                  ▼                   ▼
  元数据管理          数据持久化           日志管理
  
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Logtail   │◀───▶│ Checkpoint  │◀───▶│     GC      │
└─────────────┘     └─────────────┘     └─────────────┘
       │                  │                   │
       │                  │                   │
       ▼                  ▼                   ▼
  变更收集            数据持久化           空间回收
```
