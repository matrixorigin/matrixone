# Chunk-native Top-K for fused INCLUDE reads

- Status: approved in conversation before implementation.
- Implementation branch: `perf/fused-chunk-topk`; one implementation PR.
- Base: main `f0c31cd4b830be32442cf329e0a3fb08aa9c16c3`.
- Related issue: #24097; follows merged #28434 and #28442.

## Contract and decision

Fused INCLUDE currently reconstructs a whole vector column before evaluating
the residual filter and Top-K. Sharing removes repeated reconstruction across
overlapping readers but does not remove reconstruction itself.

Preserve the combined filter/vector/projection storage request. On chunked
misses, retain the compressed vector extent and decode only chunks containing
filter survivors, one at a time. This reduces decoding and copies, not cold-read
storage bytes. Do not change projection decoding, recycling, parallelism, SQL,
storage format, or the primary FileService interface.

The alternatives are the current shared whole-column reconstruction (retains
avoidable copies and whole-column transient memory), and staged filter/chunk
reads (saves bytes but changes I/O latency and batching). The user explicitly
selected preserved combined I/O. Reuse existing chunk encoding, validation,
range cache keys, Top-K accumulator, and bounded decode registry.

## Interfaces and flow

`ReadBlockForTopN` returns a scoped `BlockTopNRead` with `Entry`, `TopN`,
`FromCache`, and `Release`. It owns pinned block sources, not a new cache or
executor. The public fused loader signature stays unchanged. It materializes
filters, invokes its existing selector, ranks through this owner, copies winning
projections, and always releases the scope. Requested positions survive
synthetic-column expansion; siblings keep their existing constructors.

Prefer a decoded whole-column cache hit. Otherwise probe cached chunk directory
and all decoded chunks without storage I/O. A complete hit pins existing chunks
and omits the vector storage read. A partial hit releases probe ownership and
uses the existing combined request. Its selected converter copies and validates
the compressed extent into a private immutable reference-counted result that
rejects memory-cache admission. A distinct codec permits scoped sharing without
polluting the decoded-column cache key. Drop obsolete raw read buffers only after
FileService cache updates complete, releasing allocator ownership as well as
clearing references.

`fileservice.DecodeFromBytes` accepts one physical entry and matching immutable
input. It does cache lookup, conversion, bounded sharing, and memory-cache
admission without storage I/O, disk writes, or I/O merging. S3FS implements the
cache-aware optional interface; FileServices and subpaths forward it; other
implementations use owned conversion. Conversion and updates hold an active-read
guard against Close even when no sharing generation is admitted. Results and
tickets remain owned by the returned complete IOVector.

Cache directory prefixes/headers and chunks under the same physical range keys
as single-column Top-K. Ranking reuses chunk validation and vectorTopAccumulator.
Eligible sorted selections retain block coordinates, selection ordinals, ties,
thresholds, and reader-wide heap state. Legacy/unsupported operation shapes use
the old path. Unsorted selections reconstruct from already-held sources, with
no extra storage I/O. Corruption is an error, never a retry after partial ranking.
Validate the full directory and consumed payloads; empty selections decode no
vector payloads. A scope is query-local and is not used concurrently.

## Ownership and bounds

| Audit | Contract |
|---|---|
| Q1 | The block owner releases parent buffers/cache pins; each temporary chunk vector and ticket release before advancing; every mpool allocation reaches Free. Release is idempotent at the block owner. |
| Q2 | Check cancellation between chunks. Preserve bounded follower waits and nonblocking Close; no new worker, lock, wait, or background cleanup is introduced. |
| Q3 | Cold execution retains compressed input, bounded directory metadata, one newly decoded chunk, and O(K) candidates. Cache hits may pin existing decoded data. Preserve registry byte/generation/participant limits; never accumulate a query-owned decoded chunk collection. |

Rollback removes the fused opt-in; persisted data and ordinary readers remain
compatible. The immutable object path, extent, codec, and existing policy remain
the sharing trust boundary. No tenant/auth/catalog/protocol change is involved.

## Validation and acceptance

Extend miniature persisted chunk and fused-consumer fixtures for dense/sparse/
empty selections, cross-chunk winners, ties, seeded heaps, thresholds, vector
types, synthetic columns, independent overlapping queries, malformed input,
selector/materialization failures, cancellation, Close and update failures.
Prove whole-cache/self-warmed-chunk/partial-cache paths, disabled caches,
forwarding, ordinary/single-column interoperability, and no encoded cache entry.
Owning packages, adaptive focused race stress, coverage and full SCA must pass.
Run existing INCLUDE SQL regression cases twice with teardown verification;
miniature fixtures, not their small SQL data, prove chunk-path execution.

Use five matched main/candidate benchmark samples for chunk misses (dense,
sparse, overlapping), legacy, existing whole hits and self-warmed chunk hits.
Require unchanged cold storage requests/bytes, no eligible whole reconstruction,
bounded decoded workspace and lower chunk-miss CPU. Control medians stay within
5%; a failure requires revisiting the design, not weakening its oracle. No
dataset download, remote benchmark dispatch, or unmeasured SQL/Wiki speedup claim.

Fetch and rebase newest authoritative main before every push, revalidate affected
changes, and publish only to aunjgr. Keep implementation text separate from PR
validation evidence and omit machine-specific notes.
