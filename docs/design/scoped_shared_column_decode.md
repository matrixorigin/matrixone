# Scoped sharing of decoded object columns

- Status: in progress; coding design approved in the conversation before implementation.
- Design base: `585a38efd152fadf216c8675b7b997a35ca8deb1`; implementation merges newer main before delivery.
- Branch: `fix/shared-column-decompression`.
- Implementation PR: [#28413](https://github.com/matrixorigin/matrixone/pull/28413).
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

The approved revision shares decompression only, not compressed-byte reads.
Install the opt-in converter wrapper after a memory-cache miss, and join the
registry only when the converter receives its compressed input. Every caller
keeps its ordinary I/O counters and cache-update errors. Restore the converter
before Read returns. This replaces the earlier proposal to share an entire
disk-read/cache-publication operation.

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
`min(configured memory-cache capacity, effective capacity at initialization, 64 MiB)`. No memory cache means no
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
| Leader completes conversion/validation | Publish the complete decoded data and wake waiters; each read owns its subsequent cache updates. |
| Ordinary decode failure | Publish the error and detach the generation; the final ticket returns its reservation. |
| I/O or cache-update failure | Preserve that caller's ordinary error and release its ticket through read-error cleanup. |
| Leader cancellation | Live followers fall back once to the ordinary unshared read; do not inherit another query's cancellation. |
| Follower cancellation or 200 ms wait bound | Drop its ticket; cancellation returns immediately, timeout falls back once without rejoining. |
| Final participating read releases | Remove the matching generation and release data/reservation. |
| FileService close | Stop admission and wake pending followers with a closed-operation error; detach generations without freeing live consumer data. |

No I/O, conversion, allocator release or cache update runs under the registry
mutex. Publication covers conversion/validation only; subsequent per-read
cache updates remain independent and may fail independently.
Use generation identity for removal; a late completion cannot delete a newer
generation. Late completion after close releases its result without publishing.

Use the leader's existing read context; do not start a background task or
detach its deadline. Existing allocator arena retirement keeps outstanding
allocations valid after close until their normal final release. Close itself
must not wait for a consumer to finish computing Top-K. Guard each opted-in
read through conversion and its enclosing read/cache-update cleanup. If Close
finds guarded reads active, the final guard performs cache retirement after
they finish, without starting a detached worker. Keep sharing quota charged
until the registry's final data reference is released, including detached
generations. Deferred abandonment cleanup handles panics without recover.

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

## Implementation and validation record

The approved decompression-only revision is implemented in this PR. Compressed
I/O and cache updates remain per read. No runtime measurement switch is shipped.

| Closure | Risk and ownership proof |
|---|---|
| S3FS registry and scoped read guard (R3) | Admission and publication are under one mutex; I/O, conversion, cache updates and final release are outside it. Close seals admission, wakes followers and defers cache retirement to the final active read guard. |
| IOVector and ObjectIO (R2/R3) | The registry and each consumer retain separate references to the original validated data. The final consumer's ticket removes only its own generation. Top-K exports row coordinates and distances, never the borrowed vector. |
| Boundedness (R3) | Bytes, live generations (including detached cleanup), participants and follower waiting have explicit limits. No worker or idle retention is introduced. Quota remains charged until the registry reference is released. |
| Metrics (R1) | Fixed-label conversion/leader/reuse/bypass counters and active-generation/reserved-byte gauges; no object/query labels. |
| Compatibility (R2) | No persisted, SQL, wire or catalog change. Unscoped reads, other FileService implementations and fused INCLUDE reads do not opt in. |

The permanent tests cover concurrent/late reuse, key and policy isolation,
admission limits, oversized output, ordinary/partial-result errors, cancellation
of either role, bounded fallback, panic abandonment, stale-generation cleanup,
last physical release, close during conversion and deferred cache update, and
real persisted ObjectIO Top-K consumers with different queries/selections/bounds.
The actual disk-cache test proves one conversion, one backing allocation and no
object-store GET for eight overlapping reads under pinned-cache pressure.

Normal owning-package and race validation cover FileService, ObjectIO and
ObjectIO/ioutil. Metrics has normal package validation. Focused lifecycle tests
are measured once under race and then stressed individually with the bounded
100-repetition budget. Dependent block-reader tests cover persisted Top-K,
appendable fallback and residual filtering. No new SQL behavior requires a new
BVT case; a real SQL performance diagnostic additionally compares exact rows
and distances through the frontend.

Local performance evidence (Go 1.26.4, Linux amd64; synthetic, not Wiki-10M):

| Microbenchmark (five-run medians) | Sharing off | Sharing on |
|---|---:|---:|
| Memory hit | 713 ns/read | 721 ns/read |
| Independent read | 114.0 us/read | 115.5 us/read |
| Eight independent keys | 309.9 us/batch | 317.2 us/batch |
| Eight overlapping reads | 182.8 us/batch, 8 decodes | 147.2 us/batch, 1 decode |

All three control medians remain within 5%; the overlapping batch improves by
about 20%. The 512 KiB decode workload also reduces decoded backing allocation
from eight buffers to one; Go allocation accounting alone excludes that native
backing and does not represent the memory saving.

The SQL diagnostic uses 32,768 synthetic 768-dimensional vectors, eight flushed
index objects, two CNs and 32 fixed queries with prefiltering and a distance
bound. Each off/on/on/off comparison uses one unchanged index and query set.
Temporary local instrumentation only disables the opt-in descriptor and counts
actual conversions/backing capacity; it is removed before delivery. This is a
same-binary mechanism control, not a comparison of independently rebuilt ANN
indexes or a claim about the incident-scale dataset.

| SQL, concurrency 100 (mean of two runs per mode) | Sharing off | Sharing on |
|---|---:|---:|
| 32 MiB cache: decodes/query | 7.93 | 6.45 |
| 32 MiB cache: decoded backing bytes/query | 129.2 MB | 104.8 MB |
| 32 MiB cache: CPU/query | 147.1 ms | 122.2 ms |
| 32 MiB cache: QPS | 201.1 | 242.3 |
| 32 MiB cache: p95 / p99 | 655 / 769 ms | 567 / 658 ms |
| 256 MiB cache: decodes/query | 0 | 0 |
| 256 MiB cache: CPU/query | 6.80 ms | 6.89 ms |
| 256 MiB cache: QPS | 4,340 | 4,290 |

The pressure measurement was repeated after merging main `f8a690cb21`, including
its vector-membership planner changes, and is reported above. The microbenchmarks
and large-cache control used main `c51bb4ed86` plus the same sharing mechanism.
The large cache's short high-QPS samples have noisy tails (p95 78/96 ms, p99 142/133 ms);
they are not a tail-latency improvement claim. Sequential queries decoded eight
columns in both modes under pressure and zero with the large cache, as expected.
All compared rows and distances matched. Profiles, raw measurements and the
temporary harness are retained locally; no external dataset was downloaded.
