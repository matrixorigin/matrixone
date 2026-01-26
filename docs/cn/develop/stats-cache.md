# Session Stats Cache

## 核心逻辑

**有效 stats 在 3 秒内直接返回缓存，否则重新计算。**

```txt
Stats(tableID)
    │
    ▼
cache.Get(tableID)
    │
    ▼
w.Exists() && 3秒内访问 && AccurateObjectNumber > 0 ?
    │
    ├─ 是 → 返回缓存
    │
    └─ 否 → doStatsHeavyWork()
                │
                ▼
            cache.Set(tableID, result)
                │
                ▼
            返回 result
```

## doStatsHeavyWork 流程

```txt
doStatsHeavyWork(tableID)
    │
    ▼
ensureDatabaseIsNotEmpty()  ← 重操作
    │
    ▼
getRelation()               ← 重操作
    │
    ▼
table.Stats()
    │
    ▼
返回 stats
```

## 关键设计

1. **有效性判断**：`AccurateObjectNumber > 0` 表示 stats 有效
2. **空表处理**：返回 `nil`，让调用方使用 `DefaultStats()`（Outcnt=1000）
3. **激进重试**：stats 无效时每次都重新获取，确保数据变化后能及时更新。这对 BVT 测试至关重要——测试中表刚创建/插入数据后立即查询，需要及时获取最新 stats 才能生成正确的执行计划（如 LEFT/RIGHT JOIN 选择）
4. **值类型缓存**：`map[uint64]StatsInfoWrapper` 使用值类型，通过 `lastVisit == 0` 判断是否存在，减少小对象分配

## 相关文件

- `pkg/sql/plan/stats.go` - StatsCache, StatsInfoWrapper 定义
- `pkg/frontend/compiler_context.go` - TxnCompilerContext.Stats() 实现
- `pkg/sql/compile/sql_executor_context.go` - compilerContext.Stats() 实现


# Global Stats Cache

GlobalStatsCache 是一个全局缓存，用于存储所有表的统计信息。它由 Logtail 事件驱动异步更新，Session Stats 读取时直接从内存获取。

```txt
Logtail 事件
    │
    ▼
tailC (chan, cap=10000) logtail 消费专用，最小化阻塞 logtail 消费
    │
    ▼
logtailConsumer (1个 goroutine)
    │
    │ 判断入队条件（第一层）：
    │ - keyExists(): key 必须已存在，表示有访问请求
    │ - CkpLocation: checkpoint 时触发
    │ - MetaEntry: object 元数据变更时触发
    │ - 大表降频：
    │   • 小表 (< 500 objects): 有变化就入队
    │   • 大表 (>= 500 objects): 变化率 >= 5% 或超时 30min 才入队
    │
    ▼
updateC (chan, cap=3000)
    │
    ▼
spawnUpdateWorkers (16-27个 goroutine)
    │
    │ 判断执行条件（第二层）：便于统一 debounce force/normal update request
    │ - shouldExecuteUpdate(): 检查 inProgress 和 MinUpdateInterval (15s)
    │
    ▼
coordinateStatsUpdate()
    │
    ├─→ 订阅表获取 PartitionState
    ├─→ 从 CatalogCache 获取 TableDef
    └─→ CollectAndCalculateStats()
            │
            ▼
        ForeachVisibleObjects()
            │ 并发遍历所有**已落盘的 Object** (concurrentExecutor)
            │ 注意：内存中的 dirty blocks 不参与统计
            ▼
        FastLoadObjectMeta() (S3 IO)
            │
            ▼
        累加统计信息 (ZoneMap, NDV, RowCount 等)
```

## 跨 CN 获取 Stats（当前失效）

**设计意图**：在多 CN 环境下，如果某个表的 stats 在其他 CN 上已经计算好，当前 CN 可以通过 gossip + query client 从远程 CN 获取，避免重复计算。

**实现流程**（理论上）：
```txt
CN2.GlobalStats.Get(key)
    │
    ▼
本地缓存中没有
    │
    ▼
KeyRouter.Target(key) → 返回 CN1 地址
    │
    ▼
queryClient.SendMessage(CN1, GetStatsInfo)
    │
    ▼
CN1 返回 stats
    │
    ▼
CN2 缓存 stats 并返回
```

**当前状态：失效**

该功能目前**完全失效**，原因有两个：

1. **QueryClient 未传递**
   - `Engine.qc` 字段在初始化时为 nil

2. **KeyRouter.Target() 查询不到目标**（未修复）
   - 即使传递了 QueryClient，`KeyRouter.Target(key)` 依然返回空字符串


## 关键设计

1. **两级 channel 缓冲**：`tailC`(10000) 最小代价干扰 logtail 的消费流程
2. **两层过滤**：入队条件（keyExists + 事件类型 + 大表降频）和执行条件（inProgress + MinUpdateInterval）减少更新频率。分离是为了满足 force 模式，可以直接跳过入队条件，但是需要受限于执行条件
3. **大表降频**：
   - 从 logtail 的 batch 长度统计实际 object 变化数量
   - 小表（< 500 objects）：有变化就更新，保持响应速度
   - 大表（>= 500 objects）：累积变化率 >= 5% 或超时 30min 才更新
   - 更新完成后重置基准（baseObjectCount）和待处理变化（pendingChanges）
4. **只统计已落盘数据**：内存中 logtail 不参与 stats 统计，所以入队条件只关注元数据类型 logtail entry，内存类型 entry 不关心
5. **并发遍历**：使用 concurrentExecutor 并发加载 Object 元数据

## 相关文件

- `pkg/vm/engine/disttae/stats.go` - GlobalStatsCache 定义和更新逻辑
- `pkg/vm/engine/disttae/txn_table.go` - table.Stats() 读取 GlobalStats
- `pkg/vm/engine/disttae/logtailreplay/partition_state.go` - PartitionState 和 ForeachVisibleObjects


# Global Stats 采样统计

对大表（> 100 objects）采样统计，减少 S3 IO，同时保证统计精度。

## 核心思路

**两阶段统计**：表级精确，列级采样

```
阶段1：遍历所有对象，从 ObjectStats 获取（无 IO）
  ├─ TableRowCount = Σ obj.Rows()        ← 精确
  ├─ BlockNumber = Σ obj.BlkCnt()        ← 精确
  └─ AccurateObjectNumber                ← 精确

阶段2：采样部分对象，从 ObjectMeta 获取（有 IO）
  ├─ ColumnZMs[i]     ← 采样合并
  ├─ ColumnNDVs[i]    ← 采样，交给 AdjustNDV
  ├─ NullCnts[i]      ← 采样 × rowScaleFactor
  └─ ColumnSize[i]    ← 采样 × rowScaleFactor
```

## 采样策略

**采样数计算**：`clamp(sqrt(对象数), 100, 500)`

| 对象数 | 采样数 | 说明 |
|--------|--------|------|
| ≤100   | 全量   | 不采样，保证小表精度 |
| 101~10000 | 100 | 最小采样数 |
| 10001~250000 | sqrt(n) | 平方根增长 |
| >250000 | 500   | 最大采样数，限制 IO 上限 |

**采样方法**：利用 ObjectID（UUIDv7）的随机性直接采样，无需哈希

```go
// UUIDv7 bytes 8-15 是随机部分，均匀分布
randomPart := binary.LittleEndian.Uint64(objName[8:16])
return randomPart < threshold  // threshold = ratio × MaxUint64
```

**优点**：零开销、确定性、无并发问题、与遍历顺序无关

## 列级统计缩放

使用**行数比例**缩放，减少对象大小差异的影响：

```go
rowScaleFactor := exactRowCount / sampledRowCount
info.ColumnSize[i] *= rowScaleFactor
info.NullCnts[i] *= rowScaleFactor
// NDV 不缩放，交给 AdjustNDV 处理
```

## 误差控制

| 统计量 | 精确/采样 | 说明 |
|--------|-----------|------|
| TableRowCount | 精确 | 无误差 |
| BlockNumber | 精确 | 无误差 |
| AccurateObjectNumber | 精确 | 无误差 |
| ColumnSize / NullCnts | 采样×缩放 | 对象间差异大时有偏差 |
| ColumnZMs | 采样合并 | 可能遗漏极值 |
| ColumnNDVs | 采样 | 由 AdjustNDV 综合调整 |

**关键**：表级统计精确，列级统计采样推算，查询优化器主要依赖表级统计（行数）做决策。


# table_stats 表函数

表函数，用于获取/刷新/修改表统计信息，主要用于测试优化器。

## 语法

```sql
SELECT ... FROM table_stats(table_path, command, args) g;
```

**参数：**
- `table_path`: 表路径，格式为 `"db.table"` 或 `"table"`（使用当前数据库）
- `command`: 操作命令
  - `'get'`: 获取统计信息（默认）
  - `'refresh'`: 强制刷新统计信息
  - `'patch'`: 临时修改统计信息（用于测试）
- `args`: JSON 参数（`patch` 命令必需），参数定义见 `pkg/vm/engine/disttae/stats.go` 中的 `PatchArgs` 结构

## 示例

详见 `test/distributed/cases/optimizer/associative.sql`

```sql
-- 设置小表触发优化器规则
SELECT table_cnt FROM table_stats("db.t", 'patch', 
  '{"table_cnt": 9, "accurate_object_number": 1}') g;

-- 调整列 NDV 观察 Shuffle 决策
SELECT table_cnt FROM table_stats("db.lineitem", 'patch',
  '{"ndv_map": {"l_partkey": 1840290}}') g;
```
