# Checkpoint 模块并发与性能问题

## 1. 锁竞争问题

### 1.1 runnerStore 单一 RWMutex 瓶颈

**位置**: `store.go`

```go
type runnerStore struct {
    sync.RWMutex  // 单一锁保护所有数据
    
    incrementals *btree.BTreeG[*CheckpointEntry]
    globals      *btree.BTreeG[*CheckpointEntry]
    metaFiles    map[string]struct{}
    gcIntent     types.TS
}
```

**问题**:
- 所有操作共享同一把锁
- 读操作会阻塞写操作
- 高并发场景下可能成为瓶颈

**建议**: 细粒度锁

```go
type runnerStore struct {
    incrementalsMu sync.RWMutex
    incrementals   *btree.BTreeG[*CheckpointEntry]
    
    globalsMu sync.RWMutex
    globals   *btree.BTreeG[*CheckpointEntry]
    
    metaFilesMu sync.RWMutex
    metaFiles   map[string]struct{}
}
```

### 1.2 Flusher 日志节流锁

**位置**: `flusher.go`

```go
flusher.logThrottleMu.Lock()
// 复杂的节流判断逻辑
flusher.logThrottleMu.Unlock()
```

**问题**: 日志节流逻辑在热路径上，每次收集内存使用都需要获取锁。

**建议**: 使用原子操作替代锁。

---

## 2. CAS 循环问题

### 2.1 UpdateICKPIntent 无限循环风险

**位置**: `store.go`

```go
func (s *runnerStore) UpdateICKPIntent(...) {
    for {  // 无限循环
        old := s.incrementalIntent.Load()
        if s.incrementalIntent.CompareAndSwap(old, newIntent) {
            return
        }
        // CAS 失败，重试
    }
}
```

**问题**:
- 高并发下可能导致长时间自旋
- 没有退避策略
- 没有最大重试次数限制

**建议**: 添加退避和超时

```go
func (s *runnerStore) UpdateICKPIntent(...) error {
    const maxRetries = 100
    backoff := time.Microsecond
    
    for i := 0; i < maxRetries; i++ {
        if s.incrementalIntent.CompareAndSwap(old, newIntent) {
            return nil
        }
        time.Sleep(backoff)
        backoff = min(backoff*2, time.Millisecond*10)
    }
    return ErrTooManyRetries
}
```

---

## 3. 性能瓶颈

### 3.1 GetAllCheckpoints 全量复制

**位置**: `store.go`

```go
func (s *runnerStore) GetAllIncrementalCheckpoints() []*CheckpointEntry {
    s.Lock()
    snapshot := s.incrementals.Copy()  // 复制整个 B-Tree
    s.Unlock()
    return snapshot.Items()
}
```

**问题**: 每次调用都复制整个 B-Tree，内存分配开销大。

**建议**: 提供迭代器接口。

### 3.2 collectTableMemUsage 遍历开销

**位置**: `flusher.go`

每次刷盘周期都要遍历所有脏表，对每个表都要查找数据库和表条目。

**建议**: 缓存表引用，增量更新内存统计。

### 3.3 随机决策导致不可预测的行为

**位置**: `flusher.go`

```go
if asize > common.Const1MBytes && rand.Float64() < pressure {
    return true
}
```

**问题**: 使用随机数决定是否刷盘，行为不可预测。

**建议**: 使用确定性算法，基于优先级队列的刷盘决策。

---

## 4. 队列处理问题

### 4.1 队列处理无并行化

```go
func (executor *checkpointExecutor) onICKPEntries(items ...any) {
    executor.RunICKP()  // 串行处理
}
```

**问题**: 队列中的任务串行处理，无法利用多核优势。

### 4.2 队列大小固定

```go
e.ickpQueue = sm.NewSafeQueue(1000, 100, e.onICKPEntries)
```

**问题**: 队列大小硬编码，无法根据负载动态调整。

---

## 5. 内存使用问题

### 5.1 B-Tree 内存开销

对于检查点数量较少的场景，B-Tree 可能不如排序切片高效。

### 5.2 DirtyTreeEntry 内存泄漏风险

`merged` 指向的旧对象可能不会被及时回收。

---

## 6. 监控指标缺失

**缺失的指标**:
- CAS 重试次数
- 锁等待时间
- 队列深度和等待时间
- 内存使用量
- 刷盘延迟分布

**建议**: 添加 Prometheus 指标监控关键性能数据。
