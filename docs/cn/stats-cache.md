# Session Stats Cache

## Core Logic

**Valid stats are returned from cache within 3 seconds, otherwise recalculated.**

```txt
Stats(tableID)
    │
    ▼
cache.Get(tableID)
    │
    ▼
w.Exists() && accessed within 3s && AccurateObjectNumber > 0 ?
    │
    ├─ Yes → Return cached
    │
    └─ No → doStatsHeavyWork()
                │
                ▼
            cache.Set(tableID, result)
                │
                ▼
            Return result
```

## doStatsHeavyWork Flow

```txt
doStatsHeavyWork(tableID)
    │
    ▼
ensureDatabaseIsNotEmpty()  ← Heavy operation
    │
    ▼
getRelation()               ← Heavy operation
    │
    ▼
table.Stats()
    │
    ▼
Return stats
```

## Key Design

1. **Validity Check**: `AccurateObjectNumber > 0` indicates valid stats
2. **Empty Table Handling**: Returns `nil`, letting caller use `DefaultStats()` (Outcnt=1000)
3. **Aggressive Retry**: When stats are invalid, re-fetch every time to ensure data changes are captured promptly. This is critical for BVT tests—when tables are just created/inserted and immediately queried, timely stats updates are needed to generate correct execution plans (e.g., LEFT/RIGHT JOIN selection)
4. **Value-Type Cache**: `map[uint64]StatsInfoWrapper` uses value types, checking existence via `lastVisit == 0`, reducing small object allocations

## Related Files

- `pkg/sql/plan/stats.go` - StatsCache, StatsInfoWrapper definitions
- `pkg/frontend/compiler_context.go` - TxnCompilerContext.Stats() implementation
- `pkg/sql/compile/sql_executor_context.go` - compilerContext.Stats() implementation


# Global Stats Cache

GlobalStatsCache is a global cache for storing statistics of all tables. It's asynchronously updated by Logtail events, and Session Stats reads directly from memory.

```txt
Logtail Events
    │
    ▼
tailC (chan, cap=10000) dedicated for logtail consumption, minimizing blocking
    │
    ▼
logtailConsumer (1 goroutine)
    │
    │ Enqueue conditions (first layer):
    │ - keyExists(): key must already exist, indicating access request
    │ - CkpLocation: triggered on checkpoint
    │ - MetaEntry: triggered on object metadata changes
    │ - Large table throttling:
    │   • Small tables (< 500 objects): enqueue on any change
    │   • Large tables (>= 500 objects): enqueue only if change rate >= 5% or timeout 30min
    │
    ▼
updateC (chan, cap=3000)
    │
    ▼
spawnUpdateWorkers (16-27 goroutines)
    │
    │ Execution conditions (second layer): unified debounce for force/normal update requests
    │ - shouldExecuteUpdate(): checks inProgress and MinUpdateInterval (15s)
    │
    ▼
coordinateStatsUpdate()
    │
    ├─→ Get PartitionState for subscribed tables
    ├─→ Get TableDef from CatalogCache
    └─→ CollectAndCalculateStats()
            │
            ▼
        ForeachVisibleObjects()
            │ Concurrently traverse all **persisted Objects** (concurrentExecutor)
            │ Note: dirty blocks in memory don't participate in stats
            ▼
        FastLoadObjectMeta() (S3 IO)
            │
            ▼
        Accumulate statistics (ZoneMap, NDV, RowCount, etc.)
```

## Cross-CN Stats Retrieval (Currently Disabled)

**Design Intent**: In multi-CN environments, if a table's stats are already calculated on another CN, the current CN can fetch them via gossip + query client from the remote CN, avoiding redundant computation.

**Implementation Flow** (theoretical):
```txt
CN2.GlobalStats.Get(key)
    │
    ▼
Not in local cache
    │
    ▼
KeyRouter.Target(key) → Returns CN1 address
    │
    ▼
queryClient.SendMessage(CN1, GetStatsInfo)
    │
    ▼
CN1 returns stats
    │
    ▼
CN2 caches stats and returns
```

**Current Status: Disabled**

This feature is currently **completely disabled** for two reasons:

1. **QueryClient Not Passed**
   - `Engine.qc` field is nil during initialization

2. **KeyRouter.Target() Returns Empty** (unfixed)
   - Even if QueryClient is passed, `KeyRouter.Target(key)` still returns empty string


## Key Design

1. **Two-Level Channel Buffering**: `tailC`(10000) minimizes interference with logtail consumption
2. **Two-Layer Filtering**: Enqueue conditions (keyExists + event type + large table throttling) and execution conditions (inProgress + MinUpdateInterval) reduce update frequency. Separation allows force mode to skip enqueue conditions but still respect execution conditions
3. **Large Table Throttling**:
   - Count actual object changes from logtail batch length
   - Small tables (< 500 objects): update on any change, maintaining responsiveness
   - Large tables (>= 500 objects): update only when accumulated change rate >= 5% or timeout 30min
   - Reset baseline (baseObjectCount) and pending changes after update
4. **Only Persisted Data**: In-memory logtail doesn't participate in stats, so enqueue conditions only care about metadata-type logtail entries, ignoring memory-type entries
5. **Concurrent Traversal**: Use concurrentExecutor to load Object metadata concurrently

## Related Files

- `pkg/vm/engine/disttae/stats.go` - GlobalStatsCache definition and update logic
- `pkg/vm/engine/disttae/txn_table.go` - table.Stats() reads GlobalStats
- `pkg/vm/engine/disttae/logtailreplay/partition_state.go` - PartitionState and ForeachVisibleObjects


# Global Stats Sampling

For large tables (> 100 objects), use sampling to reduce S3 IO while maintaining statistical accuracy.

## Core Approach

**Two-Phase Statistics**: Table-level exact, column-level sampled

```
Phase 1: Traverse all objects, get from ObjectStats (no IO)
  ├─ TableRowCount = Σ obj.Rows()        ← Exact
  ├─ BlockNumber = Σ obj.BlkCnt()        ← Exact
  └─ AccurateObjectNumber                ← Exact

Phase 2: Sample some objects, get from ObjectMeta (with IO)
  ├─ ColumnZMs[i]     ← Sampled merge
  ├─ ColumnNDVs[i]    ← Sampled, defer to AdjustNDV
  ├─ NullCnts[i]      ← Sampled × rowScaleFactor
  └─ ColumnSize[i]    ← Sampled × rowScaleFactor
```

## Sampling Strategy

**Sample Count Calculation**: `clamp(sqrt(object_count), 100, 500)`

| Object Count | Sample Count | Description |
|--------------|--------------|-------------|
| ≤100   | Full scan | No sampling, ensure small table accuracy |
| 101~10000 | 100 | Minimum sample count |
| 10001~250000 | sqrt(n) | Square root growth |
| >250000 | 500   | Maximum sample count, limit IO upper bound |

**Sampling Method**: Leverage ObjectID (UUIDv7) randomness for direct sampling, no hashing needed

```go
// UUIDv7 bytes 8-15 are random part, uniformly distributed
randomPart := binary.LittleEndian.Uint64(objName[8:16])
return randomPart < threshold  // threshold = ratio × MaxUint64
```

**Advantages**: Zero overhead, deterministic, no concurrency issues, independent of traversal order

## Column-Level Statistics Scaling

Use **row count ratio** for scaling, reducing impact of object size differences:

```go
rowScaleFactor := exactRowCount / sampledRowCount
info.ColumnSize[i] *= rowScaleFactor
info.NullCnts[i] *= rowScaleFactor
// NDV not scaled, deferred to AdjustNDV
```

## Error Control

| Statistic | Exact/Sampled | Description |
|-----------|---------------|-------------|
| TableRowCount | Exact | No error |
| BlockNumber | Exact | No error |
| AccurateObjectNumber | Exact | No error |
| ColumnSize / NullCnts | Sampled×Scaled | Bias when object size varies |
| ColumnZMs | Sampled merge | May miss extreme values |
| ColumnNDVs | Sampled | Adjusted by AdjustNDV |

**Key Point**: Table-level stats are exact, column-level stats are sampled estimates. Query optimizer primarily relies on table-level stats (row count) for decisions.


# table_stats Table Function

Table function for getting/refreshing/modifying table statistics, primarily for testing optimizer.

## Syntax

```sql
SELECT ... FROM table_stats(table_path, command, args) g;
```

**Parameters:**
- `table_path`: Table path, format `"db.table"` or `"table"` (uses current database)
- `command`: Operation command
  - `'get'`: Get statistics (default)
  - `'refresh'`: Force refresh statistics
  - `'patch'`: Temporarily modify statistics (for testing)
- `args`: JSON parameters (required for `patch` command), parameter definition see `PatchArgs` struct in `pkg/vm/engine/disttae/stats.go`

## Examples

See `test/distributed/cases/optimizer/associative.sql` for details

```sql
-- Set small table to trigger optimizer rules
SELECT table_cnt FROM table_stats("db.t", 'patch', 
  '{"table_cnt": 9, "accurate_object_number": 1}') g;

-- Adjust column NDV to observe Shuffle decisions
SELECT table_cnt FROM table_stats("db.lineitem", 'patch',
  '{"ndv_map": {"l_partkey": 1840290}}') g;
```
