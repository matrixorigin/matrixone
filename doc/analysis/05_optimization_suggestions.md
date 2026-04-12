# Checkpoint 模块优化建议

## 1. 高优先级优化（可靠性相关）

### 1.1 修复崩溃恢复问题

**问题**: `executor.go` 中存在 TODO 注释，表明崩溃时可能丢失检查点日志条目。

**优化方案**: 实现两阶段提交

```go
// 阶段1: 写入 pending 状态的检查点
func (r *runner) saveCheckpointPending(start, end types.TS) (string, error) {
    name := ioutil.EncodeCKPMetadataFullName(start, end) + ".pending"
    // 写入文件...
    return name, nil
}

// 阶段2: 提交检查点（重命名文件）
func (r *runner) commitCheckpoint(pendingName string) error {
    finalName := strings.TrimSuffix(pendingName, ".pending")
    return os.Rename(pendingName, finalName)
}

// 恢复时清理未完成的 pending 文件
func (r *runner) cleanupPendingCheckpoints() error {
    // 扫描并删除所有 .pending 文件
}
```

**预期收益**:
- 消除检查点丢失风险
- 提高系统可靠性
- 简化故障恢复流程

### 1.2 改进错误处理

**问题**: 多处使用 `panic` 和 `Fatal`。

**优化方案**: 统一错误处理策略

```go
// 定义检查点错误类型
type CheckpointError struct {
    Phase   string
    Op      string
    Entry   *CheckpointEntry
    Cause   error
    Fatal   bool
}

func (e *CheckpointError) Error() string {
    return fmt.Sprintf("checkpoint error at %s/%s: %v", e.Phase, e.Op, e.Cause)
}

// 错误处理中间件
func (executor *checkpointExecutor) handleError(err error) {
    var ckpErr *CheckpointError
    if errors.As(err, &ckpErr) {
        if ckpErr.Fatal {
            // 记录并触发告警，但不退出
            executor.alertFatalError(ckpErr)
        }
        // 记录指标
        v2.CheckpointErrorCounter.WithLabelValues(ckpErr.Phase, ckpErr.Op).Inc()
    }
}
```

---

## 2. 中优先级优化（性能相关）

### 2.1 优化锁粒度

**问题**: `runnerStore` 使用单一 RWMutex。

**优化方案**: 分离锁

```go
type runnerStore struct {
    // 增量检查点相关
    ickpMu       sync.RWMutex
    incrementals *btree.BTreeG[*CheckpointEntry]
    
    // 全局检查点相关
    gckpMu  sync.RWMutex
    globals *btree.BTreeG[*CheckpointEntry]
    
    // 元数据文件相关
    metaMu    sync.RWMutex
    metaFiles map[string]struct{}
    
    // GC 相关
    gcMu        sync.Mutex
    gcIntent    types.TS
    gcWatermark atomic.Value
    
    // Intent 使用原子操作，无需锁
    incrementalIntent atomic.Pointer[CheckpointEntry]
    compacted         atomic.Pointer[CheckpointEntry]
}
```

**预期收益**:
- 减少锁竞争
- 提高并发性能
- 读写操作可以并行

### 2.2 优化 CAS 循环

**问题**: `UpdateICKPIntent` 可能无限循环。

**优化方案**: 添加退避和限制

```go
func (s *runnerStore) UpdateICKPIntent(
    ts *types.TS, policyChecked, flushChecked bool,
) (intent *CheckpointEntry, updated bool, err error) {
    const maxRetries = 50
    backoff := &exponentialBackoff{
        initial: time.Microsecond * 10,
        max:     time.Millisecond * 5,
        factor:  1.5,
    }
    
    for i := 0; i < maxRetries; i++ {
        old := s.incrementalIntent.Load()
        
        // ... 原有逻辑 ...
        
        if s.incrementalIntent.CompareAndSwap(old, newIntent) {
            return newIntent, true, nil
        }
        
        // CAS 失败，退避重试
        backoff.Wait()
        v2.CASRetryCounter.WithLabelValues("UpdateICKPIntent").Inc()
    }
    
    return nil, false, ErrCASRetryExhausted
}

type exponentialBackoff struct {
    initial time.Duration
    max     time.Duration
    factor  float64
    current time.Duration
}

func (b *exponentialBackoff) Wait() {
    if b.current == 0 {
        b.current = b.initial
    }
    time.Sleep(b.current)
    b.current = time.Duration(float64(b.current) * b.factor)
    if b.current > b.max {
        b.current = b.max
    }
}
```

### 2.3 优化内存收集

**问题**: `collectTableMemUsage` 每次都遍历所有表。

**优化方案**: 增量更新 + 缓存

```go
type memStatsManager struct {
    mu          sync.RWMutex
    stats       map[uint64]*tableMemStats
    totalSize   int64
    lastUpdate  time.Time
    updateChan  chan memUpdate
}

type tableMemStats struct {
    tableID   uint64
    asize     int64
    dsize     int64
    lastCheck time.Time
}

type memUpdate struct {
    tableID uint64
    delta   int64
}

func (m *memStatsManager) Start() {
    go m.processUpdates()
}

func (m *memStatsManager) processUpdates() {
    for update := range m.updateChan {
        m.mu.Lock()
        if stats, ok := m.stats[update.tableID]; ok {
            stats.asize += update.delta
            m.totalSize += update.delta
        }
        m.mu.Unlock()
    }
}

func (m *memStatsManager) GetPressure() float64 {
    m.mu.RLock()
    defer m.mu.RUnlock()
    return float64(m.totalSize) / float64(common.RuntimeOverallFlushMemCap.Load())
}
```

---

## 3. 低优先级优化（代码质量）

### 3.1 配置统一管理

**问题**: 配置分散在多个结构中。

**优化方案**: 统一配置结构

```go
type CheckpointConfig struct {
    // 增量检查点
    ICKP struct {
        Interval        time.Duration `yaml:"interval" default:"1m"`
        MinCount        uint64        `yaml:"min_count" default:"10000"`
        ReservedWALCount uint64       `yaml:"reserved_wal_count" default:"5"`
        IntentOldAge    time.Duration `yaml:"intent_old_age" default:"2m"`
    } `yaml:"ickp"`
    
    // 全局检查点
    GCKP struct {
        MinCount        uint64        `yaml:"min_count" default:"40"`
        HistoryDuration time.Duration `yaml:"history_duration" default:"72h"`
    } `yaml:"gckp"`
    
    // 刷盘
    Flush struct {
        Interval      time.Duration `yaml:"interval" default:"1m"`
        CronPeriod    time.Duration `yaml:"cron_period" default:"5s"`
        ForceTimeout  time.Duration `yaml:"force_timeout" default:"90s"`
        CheckInterval time.Duration `yaml:"check_interval" default:"500ms"`
        ChunkSize     int           `yaml:"chunk_size" default:"400"`
    } `yaml:"flush"`
    
    // 队列
    Queue struct {
        ICKPSize int `yaml:"ickp_size" default:"1000"`
        GCKPSize int `yaml:"gckp_size" default:"1000"`
        GCSize   int `yaml:"gc_size" default:"100"`
    } `yaml:"queue"`
}

func (c *CheckpointConfig) Validate() error {
    if c.ICKP.Interval < time.Second {
        return errors.New("ickp interval too small")
    }
    if c.GCKP.MinCount < 1 {
        return errors.New("gckp min count must be positive")
    }
    // ... 更多验证
    return nil
}
```

### 3.2 重构复杂函数

**问题**: `UpdateICKPIntent` 函数过于复杂。

**优化方案**: 拆分为多个小函数

```go
func (s *runnerStore) UpdateICKPIntent(
    ts *types.TS, policyChecked, flushChecked bool,
) (intent *CheckpointEntry, updated bool) {
    for {
        old := s.incrementalIntent.Load()
        
        // 场景1: 尝试减少 intent
        if result := s.tryDecreaseIntent(old, ts, policyChecked, flushChecked); result.done {
            if result.shouldContinue {
                continue
            }
            return result.intent, result.updated
        }
        
        // 场景2: 检查是否可以更新
        if result := s.checkCanUpdate(old, ts, policyChecked, flushChecked); !result.canUpdate {
            return result.intent, false
        }
        
        // 场景3: 创建或扩展 intent
        newIntent := s.createOrExtendIntent(old, ts, policyChecked, flushChecked)
        if newIntent == nil {
            return old, false
        }
        
        if s.incrementalIntent.CompareAndSwap(old, newIntent) {
            return newIntent, true
        }
    }
}

type updateResult struct {
    intent         *CheckpointEntry
    updated        bool
    done           bool
    shouldContinue bool
    canUpdate      bool
}

func (s *runnerStore) tryDecreaseIntent(...) updateResult { ... }
func (s *runnerStore) checkCanUpdate(...) updateResult { ... }
func (s *runnerStore) createOrExtendIntent(...) *CheckpointEntry { ... }
```

### 3.3 添加监控指标

**问题**: 缺少关键性能指标。

**优化方案**: 添加 Prometheus 指标

```go
var (
    // 检查点计数
    checkpointTotal = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: "tae",
            Subsystem: "checkpoint",
            Name:      "total",
            Help:      "Total number of checkpoints",
        },
        []string{"type", "status"},
    )
    
    // 检查点延迟
    checkpointDuration = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Namespace: "tae",
            Subsystem: "checkpoint",
            Name:      "duration_seconds",
            Help:      "Checkpoint duration in seconds",
            Buckets:   prometheus.ExponentialBuckets(0.1, 2, 15),
        },
        []string{"type"},
    )
    
    // Intent 年龄
    intentAge = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Namespace: "tae",
            Subsystem: "checkpoint",
            Name:      "intent_age_seconds",
            Help:      "Current intent age in seconds",
        },
        []string{"type"},
    )
    
    // 队列深度
    queueDepth = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Namespace: "tae",
            Subsystem: "checkpoint",
            Name:      "queue_depth",
            Help:      "Current queue depth",
        },
        []string{"queue"},
    )
    
    // CAS 重试
    casRetries = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: "tae",
            Subsystem: "checkpoint",
            Name:      "cas_retries_total",
            Help:      "Total CAS retry count",
        },
        []string{"operation"},
    )
    
    // 内存压力
    memoryPressure = prometheus.NewGauge(
        prometheus.GaugeOpts{
            Namespace: "tae",
            Subsystem: "checkpoint",
            Name:      "memory_pressure",
            Help:      "Current memory pressure (0-1)",
        },
    )
)

func init() {
    prometheus.MustRegister(
        checkpointTotal,
        checkpointDuration,
        intentAge,
        queueDepth,
        casRetries,
        memoryPressure,
    )
}
```

---

## 4. 实施路线图

### 第一阶段（1-2 周）：可靠性修复

1. 实现两阶段提交，修复崩溃恢复问题
2. 替换 `panic` 和 `Fatal` 为错误返回
3. 添加基本的健康检查接口

### 第二阶段（2-3 周）：性能优化

1. 实现细粒度锁
2. 优化 CAS 循环，添加退避策略
3. 实现内存统计缓存

### 第三阶段（1-2 周）：代码重构

1. 统一配置管理
2. 重构复杂函数
3. 添加监控指标

### 第四阶段（持续）：监控和调优

1. 基于指标进行性能调优
2. 根据生产环境反馈持续改进
3. 文档更新和知识传承

---

## 5. 风险评估

| 优化项 | 风险等级 | 风险描述 | 缓解措施 |
|--------|----------|----------|----------|
| 两阶段提交 | 中 | 可能影响检查点性能 | 充分测试，灰度发布 |
| 细粒度锁 | 高 | 可能引入新的并发问题 | 代码审查，压力测试 |
| CAS 退避 | 低 | 可能增加延迟 | 可配置参数 |
| 配置重构 | 低 | 兼容性问题 | 保持向后兼容 |
| 函数重构 | 中 | 可能引入 bug | 单元测试覆盖 |

---

## 6. 测试建议

### 6.1 单元测试

```go
func TestUpdateICKPIntent_CASRetry(t *testing.T) {
    store := newRunnerStore(...)
    
    // 模拟高并发场景
    var wg sync.WaitGroup
    for i := 0; i < 100; i++ {
        wg.Add(1)
        go func(i int) {
            defer wg.Done()
            ts := types.BuildTS(int64(i), 0)
            store.UpdateICKPIntent(&ts, true, true)
        }(i)
    }
    wg.Wait()
    
    // 验证最终状态一致
}
```

### 6.2 集成测试

```go
func TestCheckpointCrashRecovery(t *testing.T) {
    // 1. 启动检查点
    // 2. 在特定阶段模拟崩溃
    // 3. 重启并验证恢复
}
```

### 6.3 压力测试

```go
func BenchmarkCheckpointThroughput(b *testing.B) {
    // 测试检查点吞吐量
}

func BenchmarkLockContention(b *testing.B) {
    // 测试锁竞争情况
}
```
