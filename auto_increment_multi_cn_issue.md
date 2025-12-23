# Auto-Increment Service Multi-CN Issue Analysis

## Problem Summary

The auto-increment service in MatrixOne has architectural limitations when running with multiple Compute Nodes (CNs) that cause false positive duplicate key detection, leading to unnecessary transaction retries and errors.

## Background

### Original Bug Fix History

1. **Commit 9e56400f4** (Oct 2024): Added `GetLastAllocateTS` and `PrimaryKeysMayBeUpserted` check
   - **Purpose**: Detect when users manually insert values that conflict with auto-increment cache
   - **Scenario**: User inserts value 5, then auto-increment allocates range [1,10], causing duplicate
   - **Solution**: Check if PKs exist between `lastAllocateAt` and current `snapshotTS`, retry if found

2. **Commit 053ede3cc** (Nov 2024): Changed from `PrimaryKeysMayBeModified` to `PrimaryKeysMayBeUpserted`
   - **Purpose**: Fix concurrent deletes causing duplicates by checking tombstone files

## Current Issue

### Symptoms

When running TPC-C benchmark with 2+ CNs:

1. **Massive error logs**: ~5000+ "txn need retry in rc mode" errors in CN logs
2. **Duplicate entry errors**: "Duplicate entry for key 'PRIMARY'" for auto-increment columns
3. **Reproduction**: Occurs consistently during `INSERT` operations with auto-increment columns

### Root Cause

The auto-increment service has a **per-CN architecture**:

- Each CN maintains its own `incrservice` instance (initialized in `cnservice/server.go`)
- Each CN has local `tableCache` and `columnCache` with local `lastAllocateAt` timestamp
- Metadata table `mo_catalog.mo_increment_columns` is global and shared
- Value allocation uses SQL with locks for global coordination
- **BUT** `lastAllocateAt` cache is local to each CN and not synchronized

### Why It Fails in Multi-CN

**Example scenario:**

1. **CN1** allocates auto-increment range [1,100] at timestamp `T1`
   - Sets local `CN1.lastAllocateAt = T1`

2. **CN2** allocates auto-increment range [101,200] at timestamp `T2` (T2 > T1)
   - Sets local `CN2.lastAllocateAt = T2`

3. **CN1** inserts rows using range [1,100]
   - These inserts are committed with timestamps between T1 and T2

4. **CN2** tries to insert rows using range [101,200]
   - Calls `GetLastAllocateTS()` → returns `T2` (local to CN2)
   - Checks `PrimaryKeysMayBeUpserted(from=T2, to=currentSnapshotTS)`
   - **Finds CN1's inserts** in the time range `[T2, currentSnapshotTS]`
   - **False positive**: CN1's inserts are legitimate auto-increment values, not manual insertions
   - Triggers `NewTxnNeedRetry` error

**The fundamental issue**: `lastAllocateAt` is local but used for cross-CN conflict detection.

## Current Modifications

### 1. `/pkg/incrservice/service.go`

**Change**: `GetLastAllocateTS` always returns zero timestamp

```go
func (s *service) GetLastAllocateTS(
    ctx context.Context,
    tableID uint64,
    colName string,
) (timestamp.Timestamp, error) {
    // Note: In multi-CN scenario, lastAllocateTS is local to each CN and does not reflect
    // global state. To avoid false positives in PrimaryKeysMayBeUpserted check,
    // we return zero timestamp to effectively disable the cross-CN check.
    // The real PK conflict detection will be handled by the database's uniqueness constraint.
    return timestamp.Timestamp{}, nil
}
```

**Effect**: Disables the `PrimaryKeysMayBeUpserted` check in multi-CN scenarios

### 2. `/pkg/incrservice/store_sql.go`

**Change**: Use `SnapshotTS` as fallback when `CommitTS` is empty (lines 227-240)

```go
commitTs := txnOp.GetOverview().Meta.CommitTS
// When the transaction has not committed yet (e.g., during CREATE TABLE),
// CommitTS is zero. In this case, use SnapshotTS as a fallback.
if commitTs.IsEmpty() {
    commitTs = txnOp.SnapshotTS()
    // ... logging ...
}
```

**Effect**: Prevents `lastAllocateAt` from being set to zero during CREATE TABLE

### 3. `/pkg/sql/colexec/preinsert/preinsert.go`

**Change**: Skip PK conflict check when `from.IsEmpty()` (lines 302-308)

```go
// In multi-CN scenario, GetLastAllocateTS returns zero timestamp because lastAllocateTS
// is local to each CN. Skip the check in this case and rely on database PK constraints.
if from.IsEmpty() {
    logutil.Infof("preinsert: skip PK conflict check (multi-CN scenario) for tableID=%d, col=%s",
        tableID, col)
    continue
}
```

**Effect**: Bypasses the check when zero timestamp is returned

### 4. `/pkg/incrservice/column_cache.go` & `table_cache.go`

**Change**: Added Info-level logging for debugging

**Effect**: Helps diagnose timestamp update behavior (but may cause log spam)

## Analysis

### What Works

✅ Fixes the false positive errors in multi-CN scenarios
✅ TPC-C and TPC-H benchmarks should pass with multiple CNs
✅ Maintains correct `lastAllocateAt` values (via store_sql.go fix)

### What Doesn't Work

❌ **Original protection is lost**: The check for manual PK insertion conflicts is completely disabled
❌ **Dead code**: The check logic in `preinsert.go` (lines 297-322) never executes anymore
❌ **Log spam**: Info-level logs in hot paths will flood production logs
❌ **Incomplete fix**: This is a workaround, not an architectural solution

### Potential Issues

1. **Unprotected manual insertion scenario**:
   ```sql
   -- CN1:
   CREATE TABLE t1 (id INT AUTO_INCREMENT PRIMARY KEY);
   INSERT INTO t1 (id) VALUES (5);  -- Manual insert
   
   -- CN2:
   INSERT INTO t1 VALUES ();  -- Auto-increment might allocate 5
   ```
   - Previously: Detected via `PrimaryKeysMayBeUpserted`, graceful retry
   - Now: Will fail with duplicate key error (caught by DB constraint)
   - Impact: Less graceful error handling, but still correct

2. **Production log volume**:
   - `applyAllocateLocked` logs on every allocation
   - `getLastAllocateTS` logs on every INSERT with auto-increment
   - Violates "Log Level Usage: Info vs Debug" specification
   - Should be Debug level or removed

3. **Code maintainability**:
   - Dead code paths make the codebase harder to understand
   - The `needReCheck` logic is computed but never used
   - Future developers might not understand why the check is disabled

## Recommendations

### Option A: Clean Up the Workaround (Simpler)

Accept that manual PK insertion + auto-increment is rare and errors are acceptable:

1. **Remove dead code**: Delete the `needReCheck` logic from `preinsert.go` since it never runs
2. **Keep store_sql.go fix**: Good practice for maintaining correct timestamps
3. **Change logs to Debug level**: Comply with logging standards, avoid production spam
4. **Document the limitation**: Add comments explaining multi-CN behavior

**Pros**: Simple, low risk, matches current workaround intent
**Cons**: Manual PK insertion will error instead of retry

### Option B: Implement Global Timestamp (Proper Fix)

Make `lastAllocateAt` truly global across all CNs:

1. **Add column to metadata table**: Store `last_allocate_ts` in `mo_increment_columns`
2. **Update atomically**: Set timestamp during allocation SQL
3. **Query from table**: `GetLastAllocateTS` reads from table, not local cache
4. **Cache with TTL**: Local cache with short expiration for performance

**Pros**: Proper architectural fix, restores original protection
**Cons**: More complex, requires schema change, potential performance impact

### Option C: Hybrid Approach

Detect single-CN vs multi-CN deployment:

1. **Single CN**: Use local `lastAllocateAt` (full protection)
2. **Multi-CN**: Return zero timestamp (current workaround)
3. **Detection**: Check CN count via cluster metadata

**Pros**: Best of both worlds
**Cons**: Added complexity, deployment mode detection overhead

## Testing Requirements

Before deployment, verify:

1. **Multi-CN TPC-C**: No "txn need retry" errors, no duplicate errors
2. **Multi-CN TPC-H**: Correct aggregation results (no 4x values)
3. **Manual PK insertion**: Confirm error handling (should fail with duplicate error, not retry)
4. **Log volume**: Ensure Info-level logs don't flood in production
5. **Single CN**: Verify auto-increment still works correctly
6. **CREATE TABLE**: Verify `lastAllocateAt` is not zero after creation

## Conclusion

The current modifications are a **reasonable workaround** for multi-CN false positives, but represent an **incomplete solution** with trade-offs:

- **Solves**: Cross-CN false positive duplicate detection
- **Loses**: Graceful handling of manual PK insertion conflicts  
- **Creates**: Dead code and potential log spam issues

For production deployment, **Option A (Clean Up)** is recommended as the immediate path forward, with **Option B (Global Timestamp)** as a future architectural improvement if manual PK insertion scenarios become problematic.

## References

- **Commit 9e56400f4**: Initial `GetLastAllocateTS` implementation
- **Commit 053ede3cc**: Changed to `PrimaryKeysMayBeUpserted` for tombstone support
- **Commit cf95d916**: Partition state caching (unrelated TPC-H bug)
- **Commit cf8998ccc**: Fixed partition state policy check
