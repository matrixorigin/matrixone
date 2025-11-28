# Branch Hashmap Optimization Plan

This document captures the design decisions for the sharded `branchHashmap`. All code changes must honor the behavior described here.

## Goals
- Provide predictable high concurrency for `Put`, `Get`, `Pop`, and iteration while keeping the API surface concurrency-safe.
- Keep work per lock bounded to reduce tail latency even with tens of millions of rows.
- Minimize memory copies so `PutByVectors` can ingest 8,192-row batches efficiently.
- Preserve spill-to-disk semantics and leave TODO hooks for later monitoring instrumentation.

## Architecture Overview
1. **Shard-per-lock structure**: The hashmap is split into `N` shards, configured via `NewBranchHashmap` options. `N` must satisfy `4 <= N <= 64`. Defaults use `runtime.NumCPU() * 4` clamped into that range. Each shard owns its buckets, spill state, and accounting.
2. **Strictly serial shard operations**: Each shard exposes `putBatch`, `probe`, `pop`, and `iterate` routines guarded by its mutex. No goroutines run inside shards; serialization happens at the shard boundary while callers gain concurrency by hashing onto different shards.
3. **ForEachShardParallel API**: Callers iterate shard-by-shard by invoking `ForEachShardParallel(fn, parallelism)`. The callback receives a `ShardCursor` that offers read-only iteration and helper mutations (`PopByEncodedKey`). Internally the hashmap fans work out to a goroutine pool so different shards progress concurrently while each shard still executes serially.
4. **Read-only return data**: Data slices returned from public APIs alias internal buffers and must be treated as read-only. Callers copy if they need to retain data after the next hashmap call.

## Key APIs & Semantics
- `PutByVectors` encodes up to 8,192 rows per call, allocates once per chunk, and dispatches entries to shards based on hash.
- `GetByVectors` / `PopByVectors` encode keys sequentially inside the calling goroutine and rely on shard-level parallelism plus caller-driven concurrency rather than spawning worker pools.
- `PopByEncodedKey` remains O(1) when provided a pre-encoded key. Calls targeting the same shard serialize; different shards operate concurrently.
- `PopByEncodedFullValues` reconstructs keys from full row payloads before delegating to key-based pop, allowing callers that only have value encoding to remove entries.
- `ForEachShardParallel` replaces the old `ForEach`. `PopByEncodedKey` must be invoked via `ShardCursor` while holding the shard lock to avoid blocking other shards. `parallelism <= 0` uses `min(runtime.NumCPU(), shardCount)`; positive values are clamped to `[1, shardCount]`.
- `ItemCount` returns the current row count using shard-local counters that are updated during insertions and removals. No iteration is required to compute the total.
- Returned rows still decode via `DecodeRow` without changing tuple order.
- `Project(keyCols, parallelism)` builds a new hashmap by re-encoding keys with the provided column indexes while keeping the original value payloads. Work is fanned out by shard using `ForEachShardParallel`; large shards are processed in batches to cap memory pressure. The returned hashmap is independent of the source (separate storage and locks). `parallelism` follows the same clamping rules as `ForEachShardParallel`.
- `Migrate(keyCols, parallelism)` migrates rows into a new hashmap and removes them from the source during traversal to avoid holding double memory. Key re-encoding and batching semantics match `Project`; removal uses shard-local pop to stay non-blocking for other shards.

## Memory Management
- Each shard pools `entryBlock` arenas so large slabs are reused. Temporary packers, key buffers, and batches sit in global `sync.Pool`s to avoid GC churn.
- Encoding for `PutByVectors` allocates per batch rather than per row to keep allocator pressure low.

## Spill Strategy
- Spills are triggered globally but executed per shard. Each shard tracks its spill manifest and exposes both in-memory and spilled rows when iterating.

## Concurrency & Correctness
- All public APIs are concurrency-safe. Shard locks serialize same-key work while allowing cross-shard parallelism.
- Adding or removing keys within a shard during iteration is only legal via the provided cursor helpers. This keeps iteration deterministic.

## Testing & Benchmarking
- Unit tests cover sharding defaults, concurrent `Put/Get/Pop`, `ForEachShardParallel` semantics, spill handling, and read-only guarantees.
- Benchmarks focus on multi-shard `Put` throughput and `Get` latency to verify batching efficiency.

## Observability
- TODO hooks exist in insertion and spill paths so future monitoring can expose shard-level metrics once instrumentation is ready.

## Implementation Order
1. Introduce configuration, shard structs, and options.
2. Route Put/Get/Pop through shards.
3. Implement `ForEachShardParallel` and update iteration call sites.
4. Add tests/benchmarks for concurrency, spill, and iteration paths.
5. Wire monitoring once metrics infrastructure is available.
