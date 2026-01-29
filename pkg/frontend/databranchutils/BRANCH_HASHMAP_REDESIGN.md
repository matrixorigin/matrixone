# Branch Hashmap Redesign (Bucketized Spill, Streamed Iteration)

## Layered Structure (At a Glance)

```
BranchHashmap
 ├─ Shards[] (N shards)
 │   ├─ InMemoryStore
 │   │   ├─ HashIndex (open addressing)
 │   │   ├─ EntryMeta[] (flat array)
 │   │   └─ EntryArena (allocator-managed byte blocks)
 │   ├─ SpillStore (bucketized on disk)
 │   │   ├─ BucketFiles[B]
 │   │   ├─ BucketRowCount[B]
 │   │   └─ TombstoneBitmap[B] (lazy)
 │   └─ BucketCache (optional, per-shard LRU)
 ├─ Global config (spill, cache, shard count)
 └─ Pools (packers, temp buffers)
```

---

## 1. Goals and Constraints

### 1.1 Goals
- **I/O friendly spill**: sequential disk I/O instead of random `ReadAt` during traversal or batch operations.
- **Memory/GC friendly**: reduce per-entry pointer structures and demonstrating predictable heap usage.
- **Batch/diff optimized**: primary use is bulk comparison/diff; design should favor streaming and bucket locality.
- **Stable mapping**: once spilled, data is never re-hashed or rewritten, ensuring deterministic bucket mapping.
- **Interface compatibility**: keep `BranchHashmap` public interface unchanged; `ForEach` semantics may be revised.

### 1.2 Constraints / Assumptions
- BranchHashmap is **write-once-ish**: after build, no full rebuild or rehash is required.
- Primary keys are often unique, but duplicates must still be supported.
- ForEach is mainly used by `data_branch` diff logic and can tolerate changes in grouping semantics.
- Spill is **append-only**; no in-place updates.
- Spill reads can be sequential; occasional random point lookups are acceptable but not the dominant path.

### 1.3 Non-goals
- Full general-purpose OLTP index performance.
- Perfect efficiency for single-key random access when spill is large.
- Immediate compaction or tombstone reclamation (can be added later).

---

## 2. Core Concepts and Terminology

- **Shard**: independent unit with its own lock, memory store, spill store, cache, and counters.
- **Bucket**: fixed partition of spill files within a shard (`bucket = hash & (B-1)`), **not** the same as in-memory hash buckets.
- **Entry**: a single row (key + full value payload).
- **Rid**: record id in a bucket file (0-based, append order).
- **Tombstone**: deletion marker for spilled rows, stored as bitmap indexed by rid.

---

## 3. Data Structures

### 3.1 InMemoryStore

**Goal**: compact and GC-friendly storage while preserving O(1) lookup.

```
EntryMeta {
  hash      uint64
  keyOff    uint32/uint64
  keyLen    uint32
  valOff    uint32/uint64
  valLen    uint32
}

HashIndex: open addressing array of int32 indexes
EntryArena: large allocator blocks holding concatenated key/value bytes
```

**Design choices**
- Open addressing avoids `map` overhead.
- EntryMeta is a flat array to minimize pointer chasing.
- Allocator blocks reduce GC pressure.

### 3.2 SpillStore

**Goal**: sequential I/O, predictable file count, minimal in-memory index.

```
BucketFiles[B]
BucketRowCount[B]
TombstoneBitmap[B] (optional, created on first delete)
```

**File format** (append-only):
```
[hash(uint64)][keyLen(uint32)][valLen(uint32)][key bytes][value bytes]
```

### 3.3 BucketCache (optional)

**Goal**: reduce repeated sequential scans for hot buckets.

- Per-shard LRU cache.
- Stores `key -> rid` and optionally `rid -> value`.
- Bounded by configurable memory budget.

---

## 4. State Model and Transitions

### 4.1 High-level States
- **EMPTY**: shard has no entries.
- **IN_MEMORY**: entries stored in InMemoryStore only.
- **SPILLING**: InMemoryStore evicts entries into SpillStore.
- **SPILLED**: bucket files exist; tombstones may exist.

### 4.2 State Transitions
1) `EMPTY -> IN_MEMORY`: PutByVectors inserts first entry.
2) `IN_MEMORY -> SPILLING`: allocator pressure triggers spill.
3) `SPILLING -> SPILLED`: spill completes; entries removed from memory.
4) `SPILLED -> SPILLING`: additional spill from memory to disk.
5) `* -> CLOSED`: Close releases memory, closes files, deletes spill dirs.

---

## 5. Algorithms (Detailed)

### 5.1 PutByVectors
**Input**: vectors + keyCols

**Steps**:
1) Encode key/value for each row into batch.
2) Allocate one block for batch payloads.
3) Create EntryMeta and insert into shard InMemoryStore.
4) If allocator fails, invoke `spill(required)`.

**Notes**:
- No spill rehashing; entries are moved to bucket files by hash.

### 5.2 spill(required)
**Goal**: free `required` bytes from InMemoryStore.

**Steps**:
1) For each shard, evict entries in LRU order.
2) For each entry:
   - compute bucket: `hash & (B-1)`
   - append to bucket file (sequential)
   - increment BucketRowCount[bucket]
3) Remove entry from InMemoryStore.

**Logging**:
- log required, freed, pointers/entries, avg entry size.

### 5.3 ForEach (new semantics)
**Goal**: streaming iteration, low memory, bucket-local I/O.

**Semantic change**:
- Previously: `fn(key, rows)` grouped by key.
- Now: `fn(key, rows)` is called **per entry** with `len(rows)==1`.

**Order**:
1) **In-memory first**: iterate EntryMeta array; call fn with in-memory slices.
2) **Spill next**: for each bucket file in order:
   - sequentially scan file
   - maintain rid counter
   - skip if tombstone bit is set
   - call fn per entry

**Copy policy**:
- In-memory: zero-copy slices into arena.
- Spill: one copy into scratch buffer; valid only during callback.

### 5.4 GetByVectors / PopByVectors
**Goal**: batch-friendly, bucket-local access.

**Steps**:
1) Encode keys.
2) Group keys by bucket (hash & (B-1)).
3) For each bucket:
   - if bucket cached, use cache for lookups
   - else sequentially scan bucket file, building temporary map
   - for Pop: set tombstone bit for matching rid
4) Return results in input order.

### 5.5 PopByEncodedKey / PopByEncodedKeyValue
**Goal**: correctness with acceptable cost.

**Steps**:
1) Compute bucket by hash.
2) If bucket cached, lookup rid, set tombstone.
3) Otherwise, sequentially scan bucket file to find key/value.

**Note**: This is slower than current index-based random access but fits diff/batch use.

---

## 6. Tombstone Strategy

**Why tombstone**: no file rewrite; stable rid.

- **Structure**: per-bucket bitmap (`rid -> deleted?`).
- **Lazy allocation**: only allocate when first delete happens.
- **Memory**: 1 bit per record; 1e8 deletions ≈ 12.5MB total (plus metadata).

**Optional**: Trigger compaction when tombstone ratio exceeds threshold.

---

## 7. Concurrency Model

- Shard lock serializes shard operations.
- ForEach requires shard lock; Pop during iteration must be called via ShardCursor.
- Parallelism comes from `ForEachShardParallel`.
- Cache is per-shard to avoid cross-shard contention and pollution.

---

## 8. Failure and Recovery Behavior

- Spill files are created under shard-specific spillDir.
- On error during spill, shard stays consistent; entries remain in memory if spill fails.
- On Close: close files, delete spillDir.
- No crash recovery required (spill files are temp).

---

## 9. Configuration

| Parameter | Description | Default |
|---|---|---|
| `shardCount` | number of shards | `clamp(runtime.NumCPU()*4,4,128)` |
| `bucketCount` | spill buckets per shard (power of 2) | 128 or 256 |
| `spillPartitionMaxBytes` | max bucket file size (optional) | 128MB |
| `cacheBytesPerShard` | bucket cache budget | 64MB |
| `spillMinReleaseBytes` | minimum spill size | 1MB |

---

## 10. Large-Scale Considerations (10TB+)

To support tens of TB scale, the baseline design must add **segmented bucket files** and use **64-bit offsets** end-to-end. Without these, bucket files become too large, offsets overflow, and tombstone bitmaps balloon uncontrollably.

### 10.1 Bucket Segmentation
- Each bucket is split into **fixed-size segments** (e.g. 1–4GB).
- Spill appends to the current segment; when full, rotates to the next.
- Maintain `BucketSegments[bucket] -> []SegmentMeta` including:
  - `path`
  - `rowCount`
  - `byteSize`
  - `minRid` / `maxRid`

### 10.2 64-bit Offsets and RIDs
- Use `uint64` for all offsets and record counters.
- Record id becomes `rid = segmentBaseRid + localRid`.
- Hash index remains 32-bit if entry count < 2^31; otherwise, upgrade to 64-bit indexes.

### 10.3 Tombstone Scaling
- Tombstones are **per-segment bitmaps**, not per-bucket monolithic bitmaps.
- This enables:
  - small, cache-friendly bitmaps
  - partial cleanup or compaction per segment
- With 10B rows fully deleted, bitmap memory remains in the low GB range and can be partitioned per segment.

### 10.4 Cache Boundaries
- Strict per-shard cache limits (e.g. 64–128MB/shard).
- Eviction should drop segment caches cleanly to avoid page cache thrash.

### 10.5 File Descriptor Strategy
- Never keep all bucket/segment files open.
- Maintain a per-shard FD LRU (1–4 active segment files).

### 10.6 Suggested Defaults for 10TB
- `shardCount`: 64–128
- `bucketCount`: 512–1024
- `segmentSize`: 1GB
- `cacheBytesPerShard`: 64–128MB
- `spillMinReleaseBytes`: 8–64MB

---

## 11. Behavioral Differences vs Current Implementation

- ForEach is **streaming per entry** (no grouped rows).
- Spill uses **bucket files** rather than per-entry index.
- Random Get/Pop on spilled data is slower; batch operations are faster.
- Tombstones replace delete-in-file.

---

## 12. Testing Strategy

- **Correctness**: Put/Get/Pop with unique and duplicate keys.
- **Streaming**: ForEach returns all rows; order not guaranteed but count is.
- **Spill**: data survives spill; tombstone hides deleted rows.
- **Batch**: GetByVectors with mixed buckets.
- **Cache**: repeated gets hit cache; cache evictions do not corrupt data.
- **Concurrency**: ForEachShardParallel with mixed Pop calls.

---

## 13. Implementation Checklist

1) Add bucket spill store structures.
2) Update spill path to write bucket files sequentially.
3) Implement tombstone bitmap per bucket.
4) Update ForEach to stream entries (in-memory then spill buckets).
5) Update Get/Pop to bucket-grouped access.
6) Update tests for new ForEach semantics.
7) Update design docs and comments.

---

## 14. Rationale Summary

- **Bucketized spill** enables sequential I/O and avoids giant per-entry indexes.
- **Streaming ForEach** avoids full-map grouping and reduces memory spikes.
- **Tombstone bitmap** provides low-memory deletion marking for spilled rows.
- **Per-shard cache** prevents cross-shard cache pollution under concurrency.

This design optimizes for the dominant workload (bulk diff) while maintaining correctness and concurrency safety. Even with changed ForEach semantics, the interface surface remains compatible and existing callers can be updated with minimal logic changes.
