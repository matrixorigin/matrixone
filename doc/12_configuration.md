# 配置与调优

## 概述

本章介绍 Checkpoint 模块的配置参数和调优建议。

## 配置参数

### CheckpointCfg

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

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `MinCount` | 10000 | 触发增量检查点的最小事务数 |
| `IncrementalInterval` | 1 分钟 | 增量检查点最小间隔 |
| `IncrementalReservedWALCount` | 0 | 保留的 WAL 条目数 |
| `GlobalMinCount` | 40 | 触发全局检查点的增量检查点数 |
| `GlobalHistoryDuration` | 0 | 全局历史保留时间 |
| `TableIDHistoryDuration` | 24 小时 | TableID 历史保留时间 |
| `TableIDSinkerThreshold` | 64 MB | TableID Sinker 内存阈值 |

### FlushCfg

```go
type FlushCfg struct {
    ForceFlushTimeout       time.Duration  // 强制刷盘超时
    ForceFlushCheckInterval time.Duration  // 强制刷盘检查间隔
    FlushInterval           time.Duration  // 刷盘间隔
    CronPeriod              time.Duration  // 定时任务周期
}
```

### Flusher 默认值

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `ForceFlushTimeout` | 90 秒 | 强制刷盘超时时间 |
| `ForceFlushCheckInterval` | 500 毫秒 | 强制刷盘检查间隔 |
| `FlushInterval` | 1 分钟 | 普通刷盘间隔 |
| `CronPeriod` | 5 秒 | 定时任务周期 |
| `FlushLag` | 3 秒 | 刷盘滞后时间 |
| `FlushQueueSize` | 1000 | 刷盘队列大小 |

## 常量配置

### Checkpoint 数据常量

```go
const DefaultCheckpointBlockRows = 10000   // 每块最大行数
const DefaultCheckpointSize = 512 * 1024 * 1024  // 512MB，检查点文件大小阈值
```

### 重放工作线程

```go
const DefaultObjectReplayWorkerCount = 10  // 对象重放工作线程数
```

### 协议版本

```go
const (
    CKPProtocolVersion_V1 uint8 = 1
    CKPProtocolVersion_V2 uint8 = 2
    CKPProtocolVersion_Curr = CKPProtocolVersion_V2
)
```

## 运行时配置

### 内存相关

```go
// 全局刷盘内存上限
common.RuntimeOverallFlushMemCap.Load()

// 单表刷盘内存上限
common.FlushMemCapacity.Load()
```

### 动态修改配置

```go
// 修改强制刷盘超时
flusher.ChangeForceFlushTimeout(timeout time.Duration)

// 修改强制刷盘检查间隔
flusher.ChangeForceCheckInterval(interval time.Duration)
```

## 调优建议

### 1. 增量检查点频率

**场景**：高写入负载

```go
cfg := &CheckpointCfg{
    MinCount:            5000,      // 降低阈值，更频繁检查点
    IncrementalInterval: 30 * time.Second,  // 缩短间隔
}
```

**场景**：低写入负载

```go
cfg := &CheckpointCfg{
    MinCount:            20000,     // 提高阈值，减少检查点
    IncrementalInterval: 2 * time.Minute,   // 延长间隔
}
```

### 2. 全局检查点频率

**场景**：需要快速恢复

```go
cfg := &CheckpointCfg{
    GlobalMinCount: 20,  // 更频繁的全局检查点
}
```

**场景**：减少 I/O 开销

```go
cfg := &CheckpointCfg{
    GlobalMinCount: 100,  // 减少全局检查点频率
}
```

### 3. WAL 保留

**场景**：需要更多恢复点

```go
cfg := &CheckpointCfg{
    IncrementalReservedWALCount: 1000,  // 保留更多 WAL
}
```

### 4. 刷盘配置

**场景**：高内存压力

```go
flushCfg := FlushCfg{
    FlushInterval: 30 * time.Second,  // 更频繁刷盘
    CronPeriod:    2 * time.Second,   // 更频繁检查
}
```

**场景**：低延迟要求

```go
flushCfg := FlushCfg{
    ForceFlushTimeout:       30 * time.Second,  // 缩短超时
    ForceFlushCheckInterval: 100 * time.Millisecond,  // 更频繁检查
}
```

## 监控指标

### 性能计数器

```go
// 增量检查点计数
perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
    counter.TAE.CheckPoint.DoIncrementalCheckpoint.Add(1)
})

// 全局检查点计数
perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
    counter.TAE.CheckPoint.DoGlobalCheckPoint.Add(1)
})
```

### Prometheus 指标

```go
// 检查点条目等待时间
v2.TaskCkpEntryPendingDurationHistogram.Observe(entry.Age().Seconds())

// 检查点加载计数
v2.LogtailLoadCheckpointCounter.Inc()

// 检查点加载耗时
v2.LogTailLoadCheckpointDurationHistogram.Observe(time.Since(now).Seconds())
```

## 故障排查

### 1. 检查点执行缓慢

**症状**：检查点执行时间过长

**排查步骤**：
1. 检查脏数据量：`collector.DirtyCount()`
2. 检查内存压力：`collectTableMemUsage()` 返回值
3. 检查 I/O 性能：对象存储延迟

**解决方案**：
- 增加刷盘频率
- 调整内存阈值
- 优化存储性能

### 2. 检查点积压

**症状**：增量检查点数量持续增长

**排查步骤**：
1. 检查全局检查点是否正常执行
2. 检查 `GlobalMinCount` 配置
3. 检查是否有长事务阻塞

**解决方案**：
- 降低 `GlobalMinCount`
- 强制执行全局检查点
- 处理长事务

### 3. 恢复时间过长

**症状**：系统重启恢复时间过长

**排查步骤**：
1. 检查检查点数量
2. 检查全局检查点时间
3. 检查增量检查点数量

**解决方案**：
- 增加全局检查点频率
- 执行检查点压缩
- 优化恢复并行度

### 4. 内存使用过高

**症状**：Flusher 内存使用过高

**排查步骤**：
1. 检查 `objMemSizeList` 大小
2. 检查刷盘队列长度
3. 检查表内存使用分布

**解决方案**：
- 降低 `FlushInterval`
- 增加刷盘并行度
- 调整内存阈值

## 最佳实践

### 1. 生产环境配置

```go
cfg := &CheckpointCfg{
    MinCount:                    10000,
    IncrementalInterval:         time.Minute,
    IncrementalReservedWALCount: 100,
    GlobalMinCount:              40,
    GlobalHistoryDuration:       time.Hour * 24,
    TableIDHistoryDuration:      time.Hour * 24,
    TableIDSinkerThreshold:      64 * 1024 * 1024,
}

flushCfg := FlushCfg{
    ForceFlushTimeout:       90 * time.Second,
    ForceFlushCheckInterval: 500 * time.Millisecond,
    FlushInterval:           time.Minute,
    CronPeriod:              5 * time.Second,
}
```

### 2. 测试环境配置

```go
cfg := &CheckpointCfg{
    MinCount:            1000,
    IncrementalInterval: 10 * time.Second,
    GlobalMinCount:      10,
}

flushCfg := FlushCfg{
    ForceFlushTimeout:       30 * time.Second,
    ForceFlushCheckInterval: 100 * time.Millisecond,
    FlushInterval:           10 * time.Second,
    CronPeriod:              1 * time.Second,
}
```

### 3. 高可用配置

```go
cfg := &CheckpointCfg{
    MinCount:                    5000,
    IncrementalInterval:         30 * time.Second,
    IncrementalReservedWALCount: 500,
    GlobalMinCount:              20,
}
```

## 配置验证

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
    if cfg.GlobalHistoryDuration < 0 {
        cfg.GlobalHistoryDuration = 0
    }
    if cfg.TableIDHistoryDuration <= 0 {
        cfg.TableIDHistoryDuration = time.Hour * 24
    }
    if cfg.TableIDSinkerThreshold <= 0 {
        cfg.TableIDSinkerThreshold = 64 * mpool.MB
    }
}
```
