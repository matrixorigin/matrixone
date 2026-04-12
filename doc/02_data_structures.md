# 核心数据结构

## CheckpointEntry

`CheckpointEntry` 是 Checkpoint 的核心数据结构，代表一个检查点条目。

### 结构定义

```go
type CheckpointEntry struct {
    sync.RWMutex
    sid             string           // 服务 ID
    start, end      types.TS         // 时间范围 [start, end]
    state           State            // 状态：Pending/Running/Finished
    entryType       EntryType        // 类型：Global/Incremental/Backup/Compacted
    cnLocation      objectio.Location // CN 位置
    tnLocation      objectio.Location // TN 位置
    tableIDLocation objectio.LocationSlice // TableID 位置
    version         uint32           // 版本号

    ckpLSN      uint64  // Checkpoint LSN
    truncateLSN uint64  // 截断 LSN

    policyChecked bool   // 策略检查通过
    flushChecked  bool   // 刷盘检查通过

    bornTime   time.Time  // 创建时间
    refreshCnt uint32     // 刷新计数

    doneC chan struct{}   // 完成通道
}
```

### 关键方法

#### 时间戳比较

```go
// 检查是否在另一个 entry 之后
func (e *CheckpointEntry) AllGE(o *CheckpointEntry) bool {
    return e.start.GE(&o.end)
}

// 检查是否在指定时间戳之前
func (e *CheckpointEntry) LEByTS(ts *types.TS) bool {
    return e.end.LE(ts)
}

// 检查是否与时间范围重叠
func (e *CheckpointEntry) HasOverlap(from, to types.TS) bool {
    if e.start.GT(&to) || e.end.LT(&from) {
        return false
    }
    return true
}

// 检查是否是相邻的年轻邻居
func (e *CheckpointEntry) IsYoungNeighbor(o *CheckpointEntry) bool {
    next := e.end.Next()
    return o.start.Equal(&next)
}
```

#### 状态管理

```go
// 等待完成
func (e *CheckpointEntry) Wait() <-chan struct{} {
    return e.doneC
}

// 标记完成
func (e *CheckpointEntry) Done() {
    close(e.doneC)
}

// 设置状态
func (e *CheckpointEntry) SetState(state State) (ok bool) {
    e.Lock()
    defer e.Unlock()
    if e.state == ST_Finished {
        return
    }
    if state == ST_Running && e.state == ST_Running {
        return
    }
    e.state = state
    ok = true
    return
}
```

#### 年龄管理

```go
// 延迟退休
func (e *CheckpointEntry) DeferRetirement() {
    e.Lock()
    defer e.Unlock()
    e.refreshCnt++
}

// 获取年龄
func (e *CheckpointEntry) Age() time.Duration {
    e.RLock()
    defer e.RUnlock()
    return time.Since(e.bornTime)
}

// 检查是否过老
func (e *CheckpointEntry) TooOld() bool {
    e.RLock()
    defer e.RUnlock()
    return time.Since(e.bornTime) > time.Minute*3*time.Duration(e.refreshCnt+1)
}
```

## RunnerStore

`runnerStore` 管理所有检查点条目的内存存储。

### 结构定义

```go
type runnerStore struct {
    sync.RWMutex
    sid string

    globalHistoryDuration time.Duration  // 全局历史保留时间
    intentOldAge          time.Duration  // Intent 过期时间

    incrementalIntent atomic.Pointer[CheckpointEntry]  // 增量检查点意图

    incrementals *btree.BTreeG[*CheckpointEntry]  // 增量检查点 B-Tree
    globals      *btree.BTreeG[*CheckpointEntry]  // 全局检查点 B-Tree
    compacted    atomic.Pointer[CheckpointEntry]  // 压缩检查点
    metaFiles    map[string]struct{}              // 元数据文件集合

    gcIntent    types.TS   // GC 意图时间戳
    gcCount     int        // GC 计数
    gcTime      time.Time  // 最后 GC 时间
    gcWatermark atomic.Value  // GC 水位线
}
```

### B-Tree 排序规则

```go
// 按 end 时间戳升序排列
incrementals = btree.NewBTreeGOptions(
    func(a, b *CheckpointEntry) bool {
        return a.end.LT(&b.end)
    }, btree.Options{
        NoLocks: true,
    },
)
```

### 关键方法

#### Intent 管理

```go
// 更新增量检查点 Intent
func (s *runnerStore) UpdateICKPIntent(
    ts *types.TS, policyChecked, flushChecked bool,
) (intent *CheckpointEntry, updated bool)

// 获取增量检查点 Intent
func (s *runnerStore) TakeICKPIntent() (taken *CheckpointEntry, rollback func())

// 准备提交 Intent
func (s *runnerStore) PrepareCommitICKPIntent(intent *CheckpointEntry) (ok bool)

// 提交 Intent
func (s *runnerStore) CommitICKPIntent(intent *CheckpointEntry)
```

#### 查询方法

```go
// 获取已检查点的时间戳
func (s *runnerStore) GetCheckpointed() types.TS

// 获取最大增量检查点
func (s *runnerStore) MaxIncrementalCheckpoint() *CheckpointEntry

// 获取最大全局检查点
func (s *runnerStore) MaxGlobalCheckpoint() *CheckpointEntry

// 获取全局检查点之后的增量检查点数量
func (s *runnerStore) GetIncrementalCountAfterGlobal() int
```

## CheckpointCfg

检查点配置结构。

```go
type CheckpointCfg struct {
    // 增量检查点配置
    MinCount                    int64          // 最小事务数
    IncrementalReservedWALCount uint64         // 保留的 WAL 条目数
    IncrementalInterval         time.Duration  // 增量检查点间隔

    // 全局检查点配置
    GlobalMinCount        int64          // 触发全局检查点的最小增量检查点数
    GlobalHistoryDuration time.Duration  // 全局历史保留时间

    // TableID 配置
    TableIDHistoryDuration time.Duration  // TableID 历史保留时间
    TableIDSinkerThreshold int            // TableID Sinker 阈值
}
```

### 默认值

```go
func (cfg *CheckpointCfg) FillDefaults() {
    if cfg.IncrementalInterval <= 0 {
        cfg.IncrementalInterval = time.Minute
    }
    if cfg.MinCount <= 0 {
        cfg.MinCount = 10000
    }
    if cfg.GlobalMinCount <= 0 {
        cfg.GlobalMinCount = 40
    }
    if cfg.TableIDHistoryDuration <= 0 {
        cfg.TableIDHistoryDuration = time.Hour * 24
    }
    if cfg.TableIDSinkerThreshold <= 0 {
        cfg.TableIDSinkerThreshold = 64 * mpool.MB
    }
}
```

## DirtyTreeEntry

脏数据树条目，用于跟踪需要刷盘的数据。

```go
type DirtyTreeEntry struct {
    sync.RWMutex
    start, end types.TS   // 时间范围
    tree       *model.Tree  // 脏数据树
}
```

### 脏数据树结构

```
DirtyTreeEntry [t1, t5]
        /           \
     [TB1]         [TB2]
       |             |
      [1]        [2,3,4]    <- 叶子节点是脏块
```

## CheckpointData_V2

检查点数据结构（V2 版本）。

```go
type CheckpointData_V2 struct {
    batch     *batch.Batch    // 数据批次
    sinker    *ioutil.Sinker  // 数据写入器
    rows      int             // 行数
    size      int             // 大小
    allocator *mpool.MPool    // 内存分配器
}
```

## Checkpoint Schema

检查点元数据 Schema。

```go
var CheckpointSchemaAttr = []string{
    CheckpointAttr_StartTS,         // 开始时间戳
    CheckpointAttr_EndTS,           // 结束时间戳
    CheckpointAttr_MetaLocation,    // 元数据位置
    CheckpointAttr_EntryType,       // 条目类型
    CheckpointAttr_Version,         // 版本
    CheckpointAttr_AllLocations,    // 所有位置
    CheckpointAttr_CheckpointLSN,   // 检查点 LSN
    CheckpointAttr_TruncateLSN,     // 截断 LSN
    CheckpointAttr_Type,            // 类型
    CheckpointAttr_TableIDLocation, // TableID 位置
}

var CheckpointSchemaTypes = []types.Type{
    types.New(types.T_TS, 0, 0),
    types.New(types.T_TS, 0, 0),
    types.New(types.T_varchar, types.MaxVarcharLen, 0),
    types.New(types.T_bool, 0, 0),
    types.New(types.T_uint32, 0, 0),
    types.New(types.T_varchar, types.MaxVarcharLen, 0),
    types.New(types.T_uint64, 0, 0),
    types.New(types.T_uint64, 0, 0),
    types.New(types.T_int8, 0, 0),
    types.New(types.T_varchar, types.MaxVarcharLen, 0),
}
```

## 版本演进

```go
const (
    CheckpointVersion12 uint32 = 12  // 旧版本
    CheckpointVersion13 uint32 = 13  // 当前版本

    CheckpointCurrentVersion = CheckpointVersion13
)
```

### 版本差异

| 特性 | V12 | V13 |
|------|-----|-----|
| 数据格式 | 按表分块存储 | 统一对象存储 |
| 元数据 | BlockLocations | ObjectStats |
| 读取方式 | 按块读取 | 按对象读取 |
