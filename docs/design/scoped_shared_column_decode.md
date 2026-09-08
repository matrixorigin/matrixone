# Scoped sharing of decoded object columns

- Status: draft; implementation is gated on design approval.
- Base: `585a38efd152fadf216c8675b7b997a35ca8deb1` (newest main when prepared).
- Branch: `fix/shared-column-decompression`.
- Tracking issue: [#24097 — query execution performance improvements](https://github.com/matrixorigin/matrixone/issues/24097), specifically decompression overhead, copying and syscall costs. This PR contributes to the umbrella issue; it does not close it.
- Scope decision: the user requested a separate PR, not an extension of #28002.
- Motivation: the cache-pressure mechanism investigated for #27854. This is
  not a claim that this change alone resolves that incident.

## Evidence and invariants

The local 32K-by-768 experiment showed DOP 2 activating, but no throughput gain
at concurrency 100 with a 32 MiB decoded cache. CPU profiles were dominated by
entries-column decompression/copying and buffer recycling. A 256 MiB cache
removed those dominant costs. These are local, process-wide observations, not
Wiki-10M acceptance evidence.

A deterministic FileService reproducer uses one 64 KiB LZ4 column and eight
overlapping reads from a warmed disk cache. Other active data pins the 128 KiB
memory-cache budget. It observes eight decodes and eight distinct allocations,
all correctly released, without an object-storage GET. FileService code is
unchanged between that reproducer's #28002 revision and this base, so its
evidence is reusable. Main's newer #28139 fused INCLUDE path remains separate.

The missing property is reuse during overlapping consumption when persistent
memory-cache admission is unavailable. It is not missing Free/Release.

1. Share only identical immutable stored bytes with identical conversion and
   validation semantics, within one FileService instance.
2. A decoded allocation remains valid until the final participating read
   releases it. Never resurrect a released buffer or wrap away its ObjectIO
   validated-vector capability.
3. No idle result retention, TTL cache, detached worker, or cross-CN protocol.
4. Sharing admission never waits for memory while a caller holds other data.
5. Cancellation/close has a local terminal path independent of a data-path
   lock held across I/O or conversion.
6. Failed/oversized conversions never publish partial data or relax validation.

## Initial integration boundary

Opt in only the single-column, scoped `LoadColumnDataByTopN` path. Its cached
Vector stays inside ObjectIO; only row coordinates and distances escape.
Do not initially opt in fused INCLUDE reads, arbitrary FileService converters,
mutable paths, multi-entry requests, custom per-vector caches, caller-provided
Data, stream handles or writers. Their existing behavior is unchanged.

Reuse `IOEntry.CachedDataSize` for the decoded-size hint. Add an optional
sharing descriptor containing a stable codec name and conversion parameters;
the ObjectIO column constructor identifies the validated-column codec,
compression algorithm and original extent size. Zero/invalid descriptors
disable sharing, not the read. Do not use a Go function pointer as identity.

The internal key contains FileService-local identity, canonical object path,
offset, compressed length, policy and the full descriptor. ObjectIO's opted-in
object names must identify immutable, non-reused objects. Snapshot visibility,
membership and Top-K remain per-query operations after the shared bytes arrive.

Add an opt-in ObjectIO read option that reaches the entry builder; existing
callers without the option retain the old behavior. FileService implementations
without this capability safely ignore the optional descriptor. There is no SQL,
protobuf, catalog or on-disk format change.

## Admission and lifetime

The S3FS instance owns a bounded registry of loading/actively consumed columns.
Defaults: at most 64 entries, 128 participating reads per entry (including the
leader), and decoded backing reservations totaling at most
`min(configured memory-cache capacity, 64 MiB)`. No memory cache means no
sharing. Reserve the larger of the cached and transient allocators' backing-size
estimates before electing a leader. Check actual returned capacity before
publication; if it exceeds the reservation, deliver the leader's ordinary
result and send followers through the unshared fallback instead.

The registry owns at most one retained data reference per active entry. Each
successful participant receives a normal retained reference to the same
concrete `fscache.Data` object and an explicit release ticket. Attach the ticket
to the read result, preserving it through ObjectIO's entry relocation and
error cleanup. It is valid only for the opted-in scoped lifetime; no consumer
may transfer data outside that lifetime without transferring the ticket.

Normal IOVector release drops the consumer's data reference and its ticket.
The final ticket removes that exact registry generation, releases its retained
data and returns the byte/entry reservation. Persistent cache references, if
any, remain independently owned and charged by the existing cache. There is
no generic fscache.Data wrapper that could lose the sealed-vector marker.

The reservation bounds the active sharing feature, not the pre-existing
unshared fallback's total transient memory. It does not silently increase the
persistent cache capacity. If a byte, entry or participant limit is reached,
execute the existing read without sharing or admission waits.

## State transitions and waiting

| State/event | Action |
|---|---|
| Memory hit | Return normally; do not enter the registry. |
| Missing key with available admission | Register loading generation; this caller is leader. |
| Existing loading generation | Join with a ticket; wait on its completion or caller cancellation. |
| Existing ready generation | Retain its immutable data and acquire a ticket under the registry lock. |
| Leader completes all read/validation/cache-update work | Publish the complete data and wake waiters. |
| Ordinary read/decode failure | Publish the error, remove generation, return its reservation. |
| Leader cancellation | Live followers fall back once to the ordinary unshared read; do not inherit another query's cancellation. |
| Follower cancellation or 200 ms wait bound | Drop its ticket; cancellation returns immediately, timeout falls back once without rejoining. |
| Final participating read releases | Remove the matching generation and release data/reservation. |
| FileService close | Stop admission and wake pending followers; detach generations without freeing live consumer data. |

No I/O, conversion, allocator release or cache update runs under the registry
mutex. Publish only after the existing deferred cache updates have completed,
so followers cannot observe success before a leader's required work fails.
Use generation identity for removal; a late completion cannot delete a newer
generation. Late completion after close releases its result without publishing.

Use the leader's existing read context; do not start a background task or
detach its deadline. Existing allocator arena retirement keeps outstanding
allocations valid after close until their normal final release. Close itself
must not wait for a consumer to finish computing Top-K.

## Alternatives and tradeoffs

- Moving IOMerger ahead of disk-cache lookup is insufficient: it signals cache
  publication rather than transferring an uncached decoded result.
- A second decoded LRU or TTL cache creates idle retention and duplicates cache
  budgeting; rejected for this stage.
- Raising cache capacity proves the local mechanism, but is not a general code
  fix for finite memory and changing working sets.
- Broader conversion sharing risks codec collisions and escaped references;
  postpone it until the scoped consumer contract has evidence.

This registry adds one short lock/ticket operation per eligible cache miss.
It helps overlapping reads of the same columns, not workloads with disjoint
columns. Admission and cancellation fallbacks intentionally permit duplicate
work; the optimization must never trade liveness for perfect coalescing.

## Validation and delivery

Convert the diagnostic into a permanent barrier-driven test proving one
conversion and identical backing for overlapping admitted reads. Cover memory
hits, pinned-cache pressure, independent keys/codecs/policies, oversized or
rejected admission, conversion validation failure, both cancellation roles,
wait timeout, read error, last release, release after close and next generation.
Assert the byte/entry budgets return to zero, and preserve the existing concrete
validated cache-data type. Add a real scoped ObjectIO Top-K consumer test with
independent membership/threshold inputs sharing the same bytes.

Run focused race stress and owning FileService/ObjectIO tests. Repeat the small
32 MiB/256 MiB SQL/profile diagnostic with exact base and candidate, no changes
to inputs or index contents within each comparison. Record decode count,
bytes, retained/native allocation cost, CPU/query, QPS and tails. Require fewer
decodes and lower CPU on the overlapping cache-pressure case; do not accept a
new persistent memory budget overrun or result difference. Require no material
memory-hit/independent-key regression before enabling the opted-in path.

Keep the design and implementation in the same separate PR, with design
approval preceding production implementation. Merge newest authoritative main
before pushing to aunjgr. Chunked Top-K and bounded buffer reuse remain later,
separately evaluated steps. Do not download Wiki data or dispatch remote runs
without the user's request.
