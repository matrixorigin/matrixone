# ORDER BY + LIMIT OOM Issue Analysis

## Issue Summary

CI Job: https://github.com/matrixorigin/mo-nightly-regression/actions/runs/25022702692/job/73291550102

**Failing SQL:**
```sql
insert into big_data_test.insert_into_table_limit 
select * from big_data_test.table_basic_for_load_100M 
order by col4 limit 5000000
```

**Problem:** Out of Memory (OOM) when executing INSERT INTO with ORDER BY + LIMIT on large dataset (100M rows, limiting to 5M rows).

## Root Cause Analysis

### Execution Pipeline

The query execution follows this pipeline:

```
TableScan → Order → Limit → Insert
```

### Key Issue: ORDER Operator Memory Behavior

From code analysis of `pkg/sql/colexec/order/order.go`:

1. **Order operator accumulates ALL input data before sorting** (lines 32-44):
   ```go
   func (ctr *container) appendBatch(proc *process.Process, bat *batch.Batch) (enoughToSend bool, err error) {
       s1, s2 := 0, bat.Size()
       if ctr.batWaitForSort != nil {
           s1 = ctr.batWaitForSort.Size()
       }
       all := s1 + s2
       
       ctr.batWaitForSort, err = ctr.batWaitForSort.AppendWithCopy(proc.Ctx, proc.Mp(), bat)
       if err != nil {
           return false, err
       }
       return all >= maxBatchSizeToSort, nil  // maxBatchSizeToSort = 64MB
   }
   ```

2. **The Order operator enters Build phase** (lines 180-206):
   - Continuously reads ALL batches from child operator (TableScan)
   - Appends them to `batWaitForSort`
   - Only outputs when reaching `maxBatchSizeToSort` (64MB) or all input is consumed
   - Performs full sort on accumulated data

3. **Limit operator cannot prevent upstream processing** (`pkg/sql/colexec/limit/limit.go`):
   - Limit only controls output rows (lines 86-91)
   - Does NOT stop or limit the Order operator from processing all input
   - Order must sort ALL 100M rows even though only 5M are needed

### Memory Consumption

For 100M rows with multiple columns:
- **Input data size**: 100M rows * (estimated 100-200 bytes/row) = 10-20GB
- **Sort auxiliary structures**: 
  - `resultOrderList`: 100M * 8 bytes = 800MB
  - `sortVectors`: Multiple vectors for sort keys
- **Total memory**: Could easily exceed 15-25GB

## Code Flow Details

### 1. Insert Planning (`pkg/sql/plan/bind_insert.go`)

```go
func (builder *QueryBuilder) bindInsert(stmt *tree.Insert, bindCtx *BindContext) (int32, error) {
    // Line 57: Initialize insert/replace statement with SELECT clause
    lastNodeID, colName2Idx, skipUniqueIdx, err := builder.initInsertReplaceStmt(
        bindCtx, stmt.Rows, stmt.Columns, dmlCtx.objRefs[0], dmlCtx.tableDefs[0], false)
    
    // Line 62: Append dedup and multi-update nodes
    return builder.appendDedupAndMultiUpdateNodesForBindInsert(
        bindCtx, dmlCtx, lastNodeID, colName2Idx, skipUniqueIdx, stmt.OnDuplicateUpdate)
}
```

The SELECT with ORDER BY + LIMIT becomes the input to INSERT, but the Order operator doesn't know about the downstream Limit.

### 2. Order Operator State Machine (`pkg/sql/colexec/order/order.go`)

```go
func (order *Order) Call(proc *process.Process) (vm.CallResult, error) {
    if ctr.state == vm.Build {
        for {
            // Line 182: Keep reading from child until exhausted
            input, err := vm.ChildrenCall(order.GetChildren(0), proc, analyzer)
            if input.Batch == nil {
                ctr.state = vm.Eval  // All input consumed
                break
            }
            
            // Line 194: Append batch to waiting buffer
            enoughToSend, err := ctr.appendBatch(proc, input.Batch)
            
            // Line 199: Only send early if accumulated size exceeds 64MB
            if enoughToSend {
                err := ctr.sortAndSend(proc, &input)
                return input, nil
            }
        }
    }
    
    if ctr.state == vm.Eval {
        // Line 211: Sort all accumulated data
        err := ctr.sortAndSend(proc, &result)
        ctr.state = vm.End
        return result, nil
    }
}
```

**Problem**: The loop at line 182 processes ALL input batches regardless of downstream LIMIT.

### 3. Limit Operator (`pkg/sql/colexec/limit/limit.go`)

```go
func (limit *Limit) Call(proc *process.Process) (vm.CallResult, error) {
    if limit.ctr.seen >= limit.ctr.limit {
        result := vm.NewCallResult()
        result.Status = vm.ExecStop  // Line 69: Set ExecStop status
        return result, nil
    }
    
    // Line 75: Pull from upstream (Order)
    result, err := vm.ChildrenCall(limit.GetChildren(0), proc, analyzer)
    
    // Line 86-90: Truncate batch if exceeds limit
    if newSeen >= limit.ctr.limit {
        batch.SetLength(bat, int(limit.ctr.limit-limit.ctr.seen))
        result.Status = vm.ExecStop
    }
}
```

**Issue**: The `ExecStop` status is set AFTER Order has already processed and sorted all data.

## Why This Causes OOM

1. **Full Table Scan**: 100M rows are read from storage
2. **Full Memory Accumulation**: Order operator holds all 100M rows in `batWaitForSort`
3. **Full Sort**: Sorting 100M rows requires significant memory
4. **Late Limit Application**: Limit only takes effect AFTER sorting completes
5. **Insert Overhead**: Additional memory for dedup/lock operations

## Reproduction Strategy

To reproduce with smaller dataset:

```sql
-- Create test with 1M rows (1% of original)
create table source_table (
    col1 int,
    col2 varchar(100),
    col3 double,
    col4 int,  -- ORDER BY column
    col5 varchar(200)
);

-- Insert test data
insert into source_table
select
    generate_series,
    concat('test_', generate_series),
    generate_series * 1.5,
    generate_series % 10000,
    repeat('x', 100)
from generate_series(1, 1000000);

-- Reproduce the issue
create table target_table like source_table;

insert into target_table 
select * from source_table 
order by col4 limit 50000;  -- Limit to 5% of data
```

**Expected behavior**: Should only use memory proportional to 50K rows  
**Actual behavior**: Uses memory for sorting full 1M rows

## Potential Solutions

### Option 1: Top-K Optimization
Implement a heap-based top-K algorithm when ORDER BY is followed by LIMIT:
- Maintain a heap of size K (limit value)
- Process rows incrementally without accumulating all data
- **Memory**: O(K) instead of O(N)

### Option 2: Push Limit Down
In the planner, detect ORDER BY + LIMIT pattern and use a different operator:
- Create a `TopK` or `OrderLimit` combined operator
- Use priority queue to maintain only top K elements
- Avoid full sort

### Option 3: Early Termination Signal
Enhance pipeline execution to propagate LIMIT information:
- Limit operator signals upstream about required rows
- Order operator can use this to optimize (partial sort, early termination)
- Requires pipeline protocol changes

### Option 4: Spill to Disk
Add external sorting capability to Order operator:
- When memory threshold is reached, spill sorted runs to disk
- Merge sorted runs later
- **Tradeoff**: Slower but prevents OOM

## Related Code Locations

- **Order operator**: `pkg/sql/colexec/order/order.go:32-111`
- **Limit operator**: `pkg/sql/colexec/limit/limit.go:66-94`
- **Insert planning**: `pkg/sql/plan/bind_insert.go:33-63`
- **Order constants**: `pkg/sql/colexec/order/types.go:30` (maxBatchSizeToSort)

## Test Plan

1. Create test case with controlled data size (1M-10M rows)
2. Monitor memory usage during execution
3. Verify Order operator processes all rows before Limit
4. Test with EXPLAIN ANALYZE to see operator memory consumption
5. Compare memory usage with and without LIMIT clause

## Next Steps

1. Run reproduction test with smaller dataset
2. Confirm memory behavior matches analysis
3. Evaluate solution options (recommend Option 1 or 2)
4. Implement optimization for ORDER BY + LIMIT pattern
5. Add regression test to prevent future OOM issues
