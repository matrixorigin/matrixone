# Branch Hashmap Design (Current Implementation)

This document describes the current, implemented behavior of `pkg/frontend/databranchutils/branch_hashmap.go`.
It replaces older split docs and avoids forward-looking proposals that are not in code yet.

## 1. Scope

`BranchHashmap` is a shard-based, spill-capable hashmap used by data-branch diff/merge logic. It supports:

- vector batch insert (`PutByVectors`)
- key probing and removal (`Get*`, `Pop*`)
- shard-parallel iteration with in-iteration mutation helpers (`ForEachShardParallel`)
- key re-projection (`Project`) and consuming migration (`Migrate`)

Non-goals in current implementation:

- point-lookups optimized for very large spilled datasets (spill path is scan-based)
- crash recovery for spill files (spill files are temporary process-local artifacts)
- automatic compaction of tombstoned spill data

## 2. High-Level Architecture

```
branchHashmap
  ├─ meta: valueTypes, keyTypes, keyCols, allocator, options
  ├─ shard[0..N-1]
  │    ├─ memStore (open-addressing hash index + flat entries + LRU)
  │    ├─ spillStore (optional; bucketed segmented files + tombstone bitmap)
  │    └─ mutex + iteration gate + item counter
  ├─ packer pool
  └─ prepared-entry batch pool
```

### 2.1 Sharding

- Default shard count: `runtime.NumCPU() * 4`
- Clamped range: `[4, 128]`
- Shard index: `hash(key) % shardCount`
- Each shard serializes operations via shard mutex.

### 2.2 Hashing

- Hash function: `hashtable.BytesBatchGenHashStatesWithSeed`
- Seed: `branchHashSeed = 0x9e3779b97f4a7c15`
- Hash key input is the encoded key bytes.

## 3. Data Model and Encoding

### 3.1 Row Encoding

- Keys and rows are encoded with `types.Packer`.
- Supported types are implemented in `encodeValue` / `encodeDecodedValue`.
- Null values are encoded explicitly.
- On first `PutByVectors`, hashmap captures:
  - `valueTypes` (all columns),
  - `keyCols` and `keyTypes`.
- Later puts must match vector schema and key definition exactly.

### 3.2 Duplicate Keys

- Duplicate keys are supported.
- A key can map to multiple encoded row payloads (`[][]byte`).

## 4. In-Memory Store

Each shard owns a `memStore`:

- hash index: open addressing (`memSlotEmpty = -1`, tombstone slot = `-2`)
- flat `entries []memEntry`
- free list for entry index reuse
- LRU chain (`lruHead`, `lruTail`) for spill eviction candidate selection

`memEntry` stores:

- `hash`
- key/value offsets + lengths into an `entryBlock` buffer
- index slot and LRU links
- `inUse` flag

`entryBlock` keeps one allocator-backed buffer for a batch and an atomic refcount. Entry removal decrements refcount and frees allocator memory when zero.

## 5. Spill Store

Spill is shard-local and optional (created lazily when needed).

### 5.1 Bucket and Segment Layout

- Buckets per shard: default `1024`, normalized to power-of-two
- Bucket id: `hash & (bucketCount-1)`
- Segment max bytes: default `128MB`
- Segment filename: `spill-b%05d-%06d.bin`

### 5.2 On-Disk Record Format

Per record:

- `hash` (uint64, little-endian)
- `keyLen` (uint32, little-endian)
- `valLen` (uint32, little-endian)
- `key bytes`
- `value bytes`

### 5.3 Deletion in Spill

- No in-place rewrite.
- Deletions set tombstones in per-bucket bitmap (`rid -> deleted`).
- Reads/iteration skip tombstoned records.

## 6. Core Operation Semantics

### 6.1 PutByVectors

- Batch size target: `8192` rows (`putBatchSize`).
- Encodes key/value per row, groups by shard, inserts under shard lock.
- Allocation path:
  - try allocator
  - if allocator returns `nil` buffer: trigger global spill and retry
  - if still cannot fit, split chunk recursively
  - for single-entry chunk, fallback to Go heap allocation

### 6.2 Get / Pop by vectors

- Key vectors are encoded in caller goroutine.
- Probes are grouped by shard and processed under each shard lock.
- `GetByVectors`:
  - in-memory hits return read-only slices aliased to internal buffers
  - spill hits return copied payloads
- `PopByVectors(removeAll=true)` removes all rows for each key.
- `PopByVectors(removeAll=false)` removes one row per probed key entry.

### 6.3 Encoded-key operations

- `GetByEncodedKey`
- `PopByEncodedKey`
- `PopByEncodedKeyValue`
- `PopByEncodedFullValue`
- `PopByEncodedFullValueExact`

`PopByEncodedFullValue*` reconstruct key bytes from the full encoded row payload and then delegates to key/key+value removal.

### 6.4 Streaming pop

`PopByVectorsStream` removes like `PopByVectors`, but invokes callback per removed row:

- callback receives `(probeIndex, encodedKey, encodedRow)`
- removed row payload is copied and safe to keep
- key bytes may come from ephemeral scan buffers for spilled rows; copy if retained

## 7. Iteration and Concurrency Model

### 7.1 ForEachShardParallel

- Runs one task per shard via `ants` worker pool.
- `parallelism <= 0` => defaults to `runtime.NumCPU()`, then clamped to `[1, shardCount]`.
- Per shard:
  - `beginIteration` sets `iterating=true`
  - normal lock-based operations wait until iteration ends
  - callback receives `ShardCursor`

### 7.2 ShardCursor guarantees

- `cursor.ForEach` iterates per entry, not grouped by key.
- order:
  - all in-memory entries first (`memStore.forEach`)
  - then spill buckets in bucket order and segment scan order
- mutation helpers (`PopByEncodedKey`, `PopByEncodedKeyValue`) are allowed during cursor lifecycle and operate in shard iteration context.

Data lifetime rule:

- data returned to callback must be treated as transient read-only views unless caller copies it.

## 8. Project and Migrate

Both APIs rebuild a new hashmap with new key columns:

- target keeps source allocator and spill settings
- target shard count equals source shard count
- target value rows keep original encoded row payload

`Project`:

- source remains unchanged

`Migrate`:

- consumes source by popping keys during iteration
- reduces peak memory compared with duplicating full dataset

Edge behavior:

- if source `ItemCount()==0`, current code returns `NewBranchHashmap()` with default options.

## 9. Lifecycle, Errors, and Cleanup

- `Close()` is idempotent.
- Close path:
  - releases in-memory entry blocks
  - closes spill store
  - removes spill files and spill directory
- If spill stats exist, close logs aggregate spill summary.
- On spill write/scan errors, operation returns error; no crash-recovery protocol is implemented.

## 10. Configuration Defaults

| Parameter | Current Default | Notes |
|---|---:|---|
| shard count | `clamp(runtime.NumCPU()*4, 4, 128)` | configurable by option |
| spill bucket count | `1024` | normalized to power-of-two |
| spill segment max bytes | `128MB` | configurable by option |
| put batch size | `8192` | internal constant |
| mem index initial size | `1024` | internal constant |

## 11. Observability

Current implementation records spill statistics and prints aggregate summary on `Close()`, including:

- spill bytes/entries
- bucket scans and scan amplification indicators
- get/pop probe-to-scan ratios
- repeated bucket scan hints

Fine-grained runtime metrics are marked as TODO in code.

## 12. Tested Behavior (Unit/Benchmark Coverage)

`branch_hashmap_test.go` verifies:

- basic put/get/pop
- duplicate-key semantics and partial removal
- composite-key behavior
- spill and retrieval/removal correctness
- `ItemCount` in memory and spilled cases
- shard parallelism bound
- safe pop during `ForEachShardParallel`
- encoded-key and full-value deletion APIs
- schema/key consistency checks across multiple puts
- shard count clamp behavior
- project/migrate correctness and performance benchmarks

## 13. Known Limitations

- Spill lookup/removal uses bucket scan, so lookup latency may rise with large spill volume.
- Spill tombstones are not compacted automatically.
- `DecodeRow` currently returns tuple and error; the `[]types.Type` return value is not populated.
