# DATA BRANCH PICK Design Document

## 1. Overview

DATA BRANCH PICK is a core operation in the MatrixOne Data Branch feature that
**selects specific rows by primary key** from one branch table and merges their
changes into another branch table. It is analogous to Git's `cherry-pick`, but
operates on relational data rows.

### 1.1 Design Goals

- **Precise selection**: Pick rows by specifying their primary key values, not a full merge
- **High performance**: Three-level progressive filtering (object → block → row) avoids full table scans
- **Conflict handling**: Three configurable strategies (FAIL / SKIP / ACCEPT)
- **Broad PK type support**: Single-column PK, composite PK (`__mo_cpkey_col`), various data types
- **Flexible key sources**: Literal values and subqueries
- **Time-window filtering**: Optional BETWEEN SNAPSHOT clause to narrow change collection

### 1.2 Constraints

- The table must have an explicit primary key (tables without a PK are rejected)
- BETWEEN SNAPSHOT requires both snapshots to exist and be resolvable

---

## 2. SQL Syntax

### 2.1 Full Syntax

```sql
DATA BRANCH PICK <src_table> INTO <dst_table>
    [BETWEEN SNAPSHOT <sp1> AND <sp2>]
    KEYS ( <key_specification> )
    [WHEN CONFLICT { FAIL | SKIP | ACCEPT }]
```

### 2.2 KEYS Clause

Two forms are supported:

**Literal values (single-column PK)**:
```sql
KEYS (1, 2, 3, 5)
KEYS ('alice', 'bob', 'charlie')
KEYS ('2025-01-03', '2025-06-15')
```

**Literal values (composite PK)**:
```sql
-- Each tuple must have the same number of elements as PK columns
KEYS ((1, 'alice'), (2, 'bob'), (5, 'eve'))
```

**Subquery**:
```sql
-- The subquery must return as many columns as the PK has
KEYS (SELECT id FROM some_table WHERE condition)
KEYS (SELECT id, name FROM some_table WHERE condition)  -- composite PK
```

### 2.3 BETWEEN SNAPSHOT Clause

```sql
-- Narrow change collection to [sp1, sp2] time window
DATA BRANCH PICK t1 INTO t2 BETWEEN SNAPSHOT sp1 AND sp2 KEYS (1, 2, 3)

-- Quoted snapshot names are also supported
DATA BRANCH PICK t1 INTO t2 BETWEEN SNAPSHOT 'snapshot_1' AND 'snapshot_2' KEYS (1, 2, 3)
```

When specified, only changes within the `[sp1, sp2]` time interval are collected.
The interval is intersected with the original change collection range.

### 2.4 Conflict Strategies

| Strategy | Syntax | Behavior |
|----------|--------|----------|
| FAIL (default) | `WHEN CONFLICT FAIL` | Abort on conflict; return error |
| SKIP | `WHEN CONFLICT SKIP` | Skip conflicting rows; keep destination values |
| ACCEPT | `WHEN CONFLICT ACCEPT` | Accept source values; overwrite destination |

### 2.5 Examples

```sql
-- Basic: pick 3 rows from feature_branch into main
DATA BRANCH PICK feature_branch INTO main KEYS (101, 102, 103);

-- Subquery to select which rows to pick
DATA BRANCH PICK feature_branch INTO main
    KEYS (SELECT id FROM audit_log WHERE status = 'approved');

-- With time window and conflict strategy
DATA BRANCH PICK feature_branch INTO main
    BETWEEN SNAPSHOT sp_before AND sp_after
    KEYS (101, 102)
    WHEN CONFLICT ACCEPT;

-- Composite primary key
DATA BRANCH PICK feature_branch INTO main
    KEYS ((1, 'alice'), (2, 'bob'));
```

---

## 3. AST Definitions

### 3.1 Core Structs

```go
// DataBranchPick represents a PICK statement AST node.
type DataBranchPick struct {
    statementImpl
    SrcTable    TableName    // source branch table
    DstTable    TableName    // destination branch table
    Keys        *PickKeys    // KEYS clause (at least one of Keys or BetweenFrom must be set)
    BetweenFrom string       // starting snapshot name
    BetweenTo   string       // ending snapshot name
    ConflictOpt *ConflictOpt // conflict strategy (default FAIL)
}

// PickKeys represents the KEYS clause.
type PickKeys struct {
    Type     PickKeysType // PickKeysValues or PickKeysSubquery
    KeyExprs []Expr       // literal value list  (valid when Type == PickKeysValues)
    Select   *Select      // subquery            (valid when Type == PickKeysSubquery)
}

type PickKeysType int
const (
    PickKeysValues   PickKeysType = iota // KEYS (1, 2, 3) or KEYS ((a,b), (c,d))
    PickKeysSubquery                     // KEYS (SELECT ...)
)

// ConflictOpt represents a conflict strategy.
type ConflictOpt struct {
    Opt int
}
const (
    CONFLICT_FAIL   = iota // 0 - abort on conflict
    CONFLICT_SKIP          // 1 - skip conflicting rows
    CONFLICT_ACCEPT        // 2 - accept source value
)
```

---

## 4. Architecture

### 4.1 Core Data Flow

```
                        SQL Parsing
                            │
                   DataBranchPick AST
                            │
                  handleBranchPick()
                            │
                   diffMergeAgency()
                       │         │
              ┌────────┘         └────────┐
              │                           │
   materializePickKeys            decideLCABranch
   AndFilter()                    TSFromBranchDAG()
        │                                 │
        ├─→ pickKeyHashmap                │
        │   (row-level filter)            │
        │                                 │
        └─→ pkFilter                      │
            (object/block pruning)        │
                       │                  │
                       └───────┬──────────┘
                               │
                        diffOnBase()
                          │       │
               ┌──────────┘       └──────────┐
               │                             │
    collectChanges(src)           collectChanges(dst)
    + PKFilter pruning            + PKFilter pruning
               │                             │
    buildHashmapForTable()      buildHashmapForTable()
    + pickKeyHashmap row filter  + pickKeyHashmap row filter
               │                             │
               └──────────┬──────────────────┘
                          │
                   handleHashDiff()
                          │
                       retCh  ← diff result batches
                          │
                  pickMergeDiffs()
                          │
                   conflict semantics + SQL generation
                          │
                   INSERT / DELETE execution
```

### 4.2 Three-Level Filtering Architecture

The performance core of PICK is a progressive three-level filter:

| Level | Mechanism | Granularity | Data Structure | Location |
|-------|-----------|-------------|----------------|----------|
| **Object** | ZoneMap segments vs object ZoneMap | ~several MB | `[][]byte` segments | Engine `CollectChanges` |
| **Block** | ZoneMap segments vs block ZoneMap | ~8192 rows | Same | Engine `BlockEvaluator` |
| **Row** | pickKeyHashmap.GetByVectors | Per-row | `BranchHashmap` | `buildHashmapForTable` |

---

## 5. Detailed Implementation

### 5.1 Entry Point and Validation

**Entry**: `handleBranchPick()` (`data_branch.go`)

1. Validate that at least one of KEYS or BETWEEN SNAPSHOT is present
2. Set default conflict strategy to FAIL
3. Call `diffMergeAgency()`

**Table schema loading**: `getTableStuff()`

After loading source and destination table definitions, detect PK kind:

```
if PkeyColName == "__mo_fake_pk_col" → fakeKind      // no PK → PICK rejected
if Pkey.CompPkeyCol != nil           → compositeKind  // composite PK
else                                  → normalKind     // single-column PK
```

Key metadata extracted:
- `pkColIdx`: Logical column index of the PK in `Cols[]`
- `pkSeqnum`: Physical column Seqnum (used for ZoneMap lookup; immutable across DDL changes)
- `pkColIdxes`: Indices of all PK component columns (multiple for composite PK)

**No-PK rejection**: When `fakeKind` is detected, an explicit error is returned immediately:

```
DATA BRANCH PICK requires a table with a primary key; table <name> has no primary key
```

### 5.2 KEYS Materialization and Filter Construction

`materializePickKeysAndFilter()` is the core entry function that produces two key data structures:

1. **pickKeyHashmap** (`BranchHashmap`): For row-level exact matching
2. **pkFilter** (`engine.PKFilter`): ZoneMap segments for object/block pruning

Dispatches to different processing paths based on KEYS type:

#### 5.2.1 Literal Values Path (Single-Column PK)

`materializeValuesUnified()`:

```
for each expr in KEYS:
    appendExprToVec(vec, expr, pkType)   // AST literal → typed vector
vec.InplaceSort()                         // ascending order
pickKeyHashmap.PutByVectors([vec])       // populate hashmap
pkFilter = buildPKFilterFromVec(vec)     // build ZoneMap segments
vec.Free()
```

#### 5.2.2 Literal Values Path (Composite PK)

`materializeCompositeValuesUnified()`:

```
for each tuple in KEYS:
    for each element in tuple:
        appendExprToVec(compVecs[i], element, colTypes[i])

encodedVec = serial(compVecs[0], compVecs[1], ...)  // __mo_cpkey_col encoding
encodedVec.InplaceSort()                              // byte-order sort
pickKeyHashmap.PutByVectors([encodedVec])
pkFilter = buildPKFilterFromVec(encodedVec)
```

A key property of `serial()` encoding: **byte order ≡ composite key logical order**:

```
(a1, a2) < (b1, b2)  ⟺  bytes(serial(a1,a2)) < bytes(serial(b1,b2))
```

This guarantees that `InplaceSort()` on the encoded bytes produces the correct
composite key ordering for ZoneMap segment construction.

#### 5.2.3 Subquery Path

`materializeSubqueryUnified()`:

```
// 1. Construct sorted SQL
orderCols = isComposite ? "1, 2, ..., N" : "1"
orderSQL = "SELECT * FROM (<subquery>) ORDER BY " + orderCols

// 2. Stream execution
go runSql(ctx, ses, bh, orderSQL, streamChan, errChan)

// 3. Process batches
for batch := range streamChan:
    if isComposite:
        pkVec = serial(batch.Vecs[0..N])   // encode composite PK
    else:
        pkVec = batch.Vecs[0]
    pickKeyHashmap.PutByVectors([pkVec])   // populate hashmap
    segmentBuilder.observe(pkVec[0..len])  // feed segment builder

// 4. Finalize
segments = segmentBuilder.finalize()
pkFilter = PKFilter{Segments: segments, PrimarySeqnum: pkSeqnum}
```

**Key design decision**: The subquery path uses `ORDER BY 1, 2, ..., N` for composite
PKs (ordering by all columns), which ensures the `serial()` encoded output is globally
sorted. For single-column PK, `ORDER BY 1` suffices. The SQL engine's Sort operator
handles the sorting (can spill to disk), and results stream back via `streamChan`.
Memory usage is proportional to batch size, not total key count.

### 5.3 Segment Builder (`segmentBuilder`)

The segment builder ingests a **sorted** stream of PK bytes and produces a set of
**sorted, non-overlapping** ZoneMap segments.

#### Algorithm

```
observe(pkBytes):
    if first value:
        curMin = curMax = pk, curCount = 1
        return

    shouldSplit = false

    if curCount >= 8192:              // hard cap
        shouldSplit = true
    else if isNumeric && gapCount >= 4:
        gap = |pk - curMax|           // distance from current max
        avgGap = gapSum / gapCount
        if gap > 5.0 × avgGap:       // sparse region detected
            shouldSplit = true

    if shouldSplit:
        flushSegment()                // emit [curMin, curMax] as 64-byte ZoneMap
        curMin = curMax = pk, curCount = 1
    else:
        curMax = pk, curCount++
        if isNumeric: update gap statistics
```

#### Splitting Strategy

| Strategy | Condition | Applicable To |
|----------|-----------|---------------|
| Hard cap | Segment value count ≥ 8192 | All PK types |
| Adaptive gap | gap > 5× average gap | Numeric types only |
| No split | Neither of the above | Extend current segment |

**String/variable-length types**: Gap splitting is not used (distance metrics are
unreliable for varlen); only the hard cap applies.

#### Segment Invariants

After construction, the segment sequence satisfies:

1. **Sorted**: `segments[i].min ≤ segments[i+1].min`
2. **Non-overlapping**: `segments[i].max < segments[i+1].min`
3. **Corollary**: max values are also monotonically increasing

These invariants enable O(log N) binary search in `AnySegmentOverlaps`.

### 5.4 Object/Block-Level Pruning

`AnySegmentOverlaps(objZM, segments)` executes at the engine level:

```
if segments is empty: return true       // no filter = match all
if objZM not initialized: return false  // invalid ZoneMap
if type mismatch: return true           // conservative include (e.g., tombstone T_Rowid vs PK T_int32)

// Binary search: find first segment where seg.max >= obj.min
i = binarySearch(segments, seg.max >= obj.min)

if i >= len(segments): return false     // all segments below obj.min

// Check overlap: seg.min <= obj.max ?
return segments[i].min <= obj.max
```

**Mathematical basis**: Two intervals `[a_min, a_max]` and `[b_min, b_max]` overlap
if and only if `a_max ≥ b_min ∧ a_min ≤ b_max`.

#### PKFilter Propagation Chain

```
materializePickKeysAndFilter()
    → pkFilter (engine.PKFilter)
        → diffOnBase()
            → CollectChangesWithPKFilter()
                → engine.WithPKFilter(ctx, pkFilter)    // attach to context
                    → rel.CollectChanges(ctx, ...)
                        → engine extracts pkFilter from context
                            → per object: AnySegmentOverlaps(objZM, segments)
                                → per block: AnySegmentOverlaps(blockZM, segments)
```

### 5.5 Row-Level Exact Filtering

Executed in the `putVectors` closure within `buildHashmapForTable()`:

```go
if pickKeyHashmap != nil {
    pkIdx := tblStuff.def.pkColIdx
    if isTombstone {
        pkIdx = 0  // tombstone PK is at Vec[0] after processing
    }

    results = pickKeyHashmap.GetByVectors([batch.Vecs[pkIdx]])

    sels = []  // indices of selected rows
    for i, r := range results {
        if r.Exists {
            sels = append(sels, i)
        }
    }

    if len(sels) == 0: return         // all rows filtered out
    if len(sels) < batch.RowCount():
        batch.Shrink(sels, false)     // keep only matching rows
}
```

Only rows whose PK exists in the hashmap proceed to the downstream hashDiff comparison.

### 5.6 Conflict Handling

`pickMergeDiffs()` is the consumer side that handles diff batches from hashDiff.

hashDiff internally uses ACCEPT (to avoid aborting on non-picked-key conflicts).
The user's actual conflict strategy is enforced here, only for picked rows.

#### Batch Semantics

| Source | Operation | Meaning |
|--------|-----------|---------|
| base DELETE | Destination table has a different/deleted value | Conflict candidate |
| target INSERT | Source table's new value | To be applied to destination |
| base INSERT | Destination-only new row | Ignored by PICK |

#### Strategy Execution

**FAIL**:
```
Receive base DELETE (conflict on picked key)
    → Return error: "conflict: <target> INSERT and <base> INSERT on pk(<key>) with different values"
    → Transaction aborted
```

**SKIP**:
```
Receive base DELETE
    → Add PK to skipSet; do not execute DELETE

Receive target INSERT
    → If PK in skipSet → skip
    → Otherwise → execute INSERT
```

**ACCEPT**:
```
Receive base DELETE → execute DELETE (remove old value)
Receive target INSERT → execute INSERT (write source value)
```

### 5.7 BETWEEN SNAPSHOT

When `BETWEEN SNAPSHOT sp1 AND sp2` is specified:

1. `resolveBetweenSnapshots()` resolves snapshot names to `types.TS` timestamps
2. The change collection range narrows from `[LCA, current]` to its intersection with `[sp1, sp2]`
3. Only changes that occurred within the time window are collected and picked

---

## 6. Supported PK Types

### 6.1 Single-Column PK Types

| Category | Types | Literal Example | Gap Splitting |
|----------|-------|-----------------|---------------|
| Integer | int8/16/32/64, uint8/16/32/64 | `KEYS (1, 2, 3)` | ✓ |
| Float | float32, float64 | `KEYS (1.5, 2.7)` | ✓ |
| Fixed-point | decimal64, decimal128 | `KEYS (99.99, 100.01)` | ✓ |
| String | varchar, char, text, blob | `KEYS ('alice', 'bob')` | ✗ (hard cap only) |
| Date/time | date | `KEYS ('2025-01-03')` | ✗ |
| Date/time | datetime | `KEYS ('2025-01-03 12:30:45')` | ✗ |
| Date/time | timestamp | `KEYS ('2025-01-03 12:30:45')` | ✗ |
| Date/time | time | `KEYS ('12:30:45')` | ✗ |
| UUID | uuid | `KEYS ('12345678-1234-...')` | ✗ |

### 6.2 Composite PK

Composite PKs are encoded via the `serial()` function into `__mo_cpkey_col`
(`T_varchar`). Any combination of the above base types is supported.

The byte ordering of the encoded value is equivalent to the logical dictionary
ordering of the composite key, ensuring correctness of ZoneMap segment construction.

---

## 7. Error Messages

### 7.1 Syntax and Input Validation

| Error | Trigger |
|-------|---------|
| `DATA BRANCH PICK requires a KEYS or BETWEEN SNAPSHOT clause` | Neither specified |
| `DATA BRANCH PICK requires a table with a primary key` | Table has no PK |
| `KEYS contains tuples but table has a single-column primary key` | Tuples for single PK |
| `KEYS expression must be a tuple for composite primary key` | Scalars for composite PK |
| `KEYS tuple has N elements but composite primary key has M columns` | Tuple size mismatch |
| `KEYS subquery returns N columns but ...` | Subquery column count mismatch |
| `unsupported PK type for engine filter: <type>` | Unsupported PK type |

### 7.2 Runtime Errors

| Error | Trigger |
|-------|---------|
| `conflict: ... on pk(<key>) with different values` | Conflict detected under FAIL strategy |
| `cannot resolve snapshot '<name>'` | Snapshot name cannot be resolved |
| `KEYS subquery streaming error` | Subquery execution failure |
| `failed to encode composite keys` | serial() encoding failure |

---

## 8. Key Files and Functions

| File | Core Functions | Responsibility |
|------|---------------|----------------|
| `pkg/frontend/data_branch_pick.go` | `materializePickKeysAndFilter` | KEYS materialization + PKFilter construction |
| | `materializeValuesUnified` | Single-column PK literal processing |
| | `materializeCompositeValuesUnified` | Composite PK literal processing |
| | `materializeSubqueryUnified` | Subquery streaming processing |
| | `segmentBuilder` | ZoneMap segment construction algorithm |
| | `buildPKFilterFromVec` | Sorted vector → PKFilter |
| | `pickMergeDiffs` | Consumer-side conflict semantics |
| | `appendExprToVec` / `appendStrValToVec` | AST literal → typed vector conversion |
| `pkg/frontend/data_branch.go` | `handleBranchPick` | PICK entry validation |
| | `diffMergeAgency` | Unified dispatch for diff/merge/pick |
| | `getTableStuff` | Table schema loading + PK kind detection |
| `pkg/frontend/data_branch_hashdiff.go` | `buildHashmapForTable` | pickKeyHashmap row-level filtering |
| `pkg/frontend/databranchutils/branch_hashmap.go` | `BranchHashmap` | Exact-match hash table |
| `pkg/vm/engine/tae/index/zm_segments.go` | `AnySegmentOverlaps` | ZoneMap segment pruning algorithm |
| `pkg/frontend/databranchutils/branch_change_handle.go` | `CollectChangesWithPKFilter` | PKFilter context propagation |

---

## 9. Testing

### 9.1 BVT Tests (Distributed SQL Tests)

Located in `test/distributed/cases/git4data/branch/pick/`, 12 test files:

| File | Scenario |
|------|----------|
| pick_1.sql | Basic PICK with all LCA topologies |
| pick_2.sql | Conflict handling (FAIL / SKIP / ACCEPT) |
| pick_3.sql | Mixed operations (INSERT + UPDATE) |
| pick_4.sql | VARCHAR primary key and various data types |
| pick_5.sql | KEYS with subquery |
| pick_6.sql | Edge cases (non-existent keys) |
| pick_7.sql | Sequential picks (incremental cherry-pick) |
| pick_8.sql | DELETE scenarios (6 fundamental combinations) |
| pick_9.sql | BETWEEN SNAPSHOT time-window narrowing |
| pick_10.sql | BETWEEN SNAPSHOT edge cases |
| pick_11.sql | Composite primary key (71 assertions) |
| pick_12.sql | DECIMAL primary key (55 assertions) |

### 9.2 Embedded Cluster Integration Tests

Located in `pkg/tests/dml/pick_test.go`, 12 test functions:

| Test Function | Scenario |
|--------------|----------|
| `runPickByKeyValues` | Literal KEYS: select 2 out of 5 rows |
| `runPickAll` | Select all 5 rows |
| `runPickWithDelete` | Source branch deletes rows; verify DELETE propagation |
| `runPickConflictSkip` | SKIP strategy preserves destination value |
| `runPickConflictAccept` | ACCEPT strategy overwrites with source value |
| `runPickConflictFail` | FAIL strategy aborts transaction |
| `runPickSubqueryKeys` | Subquery KEYS |
| `runPickLargeScale` | Large scale: 1000 seed rows + 500 new, pick 50 |
| `runPickVarcharPK` | VARCHAR primary key |
| `runPickConsecutive` | Two consecutive picks |
| `runPickIntoExistingData` | Destination already has non-overlapping data |
| `runPickMixedOperations` | INSERT + UPDATE + DELETE combined |

### 9.3 Unit Tests

Located in `pkg/frontend/data_branch_pick_test.go`:

| Test Function | Scenario |
|--------------|----------|
| `TestSegmentBuilder_SingleValue` | Single-value segment construction |
| `TestSegmentBuilder_HardCapSplit` | 8192 hard cap triggers split |
| `TestSegmentBuilder_FlushEmpty` | Empty segment builder |
| `TestPKFilterPruning_SinglePK_Int32` | int32 single PK pruning |
| `TestPKFilterPruning_SinglePK_Varchar` | varchar single PK pruning |
| `TestPKFilterPruning_Decimal64` | decimal64 PK pruning |
| `TestPKFilterPruning_SeqnumPreserved` | Seqnum correctly propagated |
| `TestPKFilterPruning_LargeKeySet` | Large key set pruning |
| `TestPKFilterPruning_CompositePK_IntVarchar` | (int, varchar) composite PK pruning |
| `TestPKFilterPruning_CompositePK_IntInt` | (int, int) composite PK pruning |
| `TestBuildPKFilterFromVec_ProducesValidFilter` | PKFilter construction validity |
| `TestBuildPKFilterFromVec_NilVec` | nil vector handling |
| `TestAppendStrValToVec_DateTypes` | date/datetime/timestamp/time/uuid string parsing |
| `TestAppendStrValToVec_PrunesCorrectly` | Date PK parsed correctly for pruning |
| `TestAppendNumericStringToVec_DateAsNumber` | Numeric-form date parsing |

Located in `pkg/vm/engine/tae/index/zm_segments_test.go`:

| Test Function | Scenario |
|--------------|----------|
| `TestAnySegmentOverlaps_EmptySegments` | Empty segments match all |
| `TestAnySegmentOverlaps_UninitedObjZM` | Uninitialized ZoneMap |
| `TestAnySegmentOverlaps_SingleSegment` | Single segment overlap |
| `TestAnySegmentOverlaps_MultipleSegments` | Multi-segment binary search |
| `TestAnySegmentOverlaps_StringType` | String type segments |
| `TestAnySegmentOverlaps_TypeMismatch` | Type mismatch conservative include |

---

## 10. Performance Characteristics

### 10.1 Benchmark Results

**Environment**: 32M-row int PK table; branch with 275K DELETE changes

| Scenario | Key Count | Latency |
|----------|-----------|---------|
| Literal KEYS | 5 | ~158ms |
| Literal KEYS | 100 | ~110ms |
| Literal KEYS | 1,000 | ~83ms |
| Literal KEYS | 10,000 | ~118ms |

**Composite PK** (1M-row table): 5 tuple KEYS ~87ms

**BETWEEN SNAPSHOT** (32M rows, 275K changes): ~5–8 seconds

### 10.2 Analysis

- **Object pruning** is the dominant contributor: on a 32M-row table, picking a few keys
  can skip 99%+ of objects
- **Block pruning** further reduces I/O within objects
- **Row-level hashmap filtering** ensures final precision with negligible overhead (hash lookup)
- As key count increases, segments merge and individual pruning efficiency decreases slightly,
  but overall processing is faster due to batch amortization

### 10.3 Memory Profile

- **Literal KEYS**: Memory proportional to key count; freed after processing
- **Subquery KEYS**: Streaming; memory proportional to batch size, not total key count
- **ZoneMap segments**: 64 bytes each; segment count is much smaller than key count
- **BranchHashmap**: Sharded + smart allocator; can spill to disk at 80% memory utilization
