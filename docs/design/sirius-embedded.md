# Embedded Sirius migration

Design version: 1.

Owner: MatrixOne query execution.

Tracking: [#28966](https://github.com/matrixorigin/matrixone/issues/28966).
Separate numeric compatibility blocker:
[#28968](https://github.com/matrixorigin/matrixone/issues/28968).

Status: implementation in progress. The user approved the ten-PR plan before
implementation. That approval does not claim implementation review, CI, GPU
validation, or production readiness. Each implementation PR must link the
reviewed revision of this document and report its own evidence.

## 1. Decision and scope

Embed Sirius and DuckDB statically in `mo-service` through a C ABI and CGo,
retaining pinned shared GPU dependencies. Use `upstream-dev-merge` synchronized
with `sirius-db/sirius:dev`, not the legacy engine or a from-scratch port.

- MO readers are the default embedded input; explicitly selected direct TAE
  input remains available under its existing admission restrictions.
- One active Sirius query on the selected GPU initially; bounded competing
  requests wait without starting readers. Default GPU stream count is two.
- Bound input and incremental output, including asynchronous ownership.
  Bounded prefetch is allowed; whole-table transport buffering is not.
- CPU-only builds and ordinary unhinted native execution remain unchanged.
- Initial execution is Linux amd64, single selected GPU, single-CN input.
  Multi-CN producer fan-in and concurrent GPU queries are not this release.
- Flight remains the existing deployment default during coexistence.
  Embedded execution is opt-in until all cutover gates pass.
- Wide-decimal support is a separate workstream. Unsupported numeric cases
  remain explicit blockers, not successful CPU fallback or skipped tests.
- Fatal native/CUDA failures may require restarting the whole MO process.

The predecessor issues are #26154 (sidecar), #27586 (streamed-input experiment)
and #26159 (compatibility/E2E evidence). Do not redefine or auto-close them.
MO #27599, Sirius #12 and sidecar #20 remain reference implementations; new
embedding commits belong on dedicated branches. Supersede those PRs only after
their useful behavior and tests have an explicit delivered replacement.

## 2. Evidence and alternatives

The current MO streamed producer already propagates a full transport window
through its output operator and bounded pipeline edges to storage readers.
Reuse this ownership boundary, not a separate eager table-reading goroutine.

The current upstream C++ FFI materializes a query result before wrapping it in
an Arrow stream. Its generic fragment streaming repositories deliberately lack
channel back-pressure. Neither API alone satisfies bounded embedded delivery.

The chosen native-descriptor path removes Flight framing, wire serialization
and its duplicate retained copies, while reusing the established MO conversion
and GPU packing logic. Alternatives considered:

| Alternative | Reason not selected |
| --- | --- |
| Keep Flight indefinitely | Retains transport, deployment and recovery machinery unnecessary in-process. |
| Wrap the current eager FFI | An Arrow stream over a materialized result does not bound result memory. |
| Arrow conversion at both boundaries | Adds representation conversion; reuse MO-native layout first. |
| Fully static dependency-free executable | Not needed; retain the current mixed-linkage deployment model. |
| New legacy-engine fork | Duplicates upstream execution work and prolongs divergence. |

Input's intended ordinary cost is one bulk copy into reusable native staging
followed by H2D conversion. This is a measured target, not an end-to-end
zero-copy claim. Algorithm state for joins, sorting and aggregation remains
separately accounted and spillable; bounded transport does not imply constant
memory for every SQL algorithm.

## 3. End-to-end data and control flow

```text
MO planning + transaction snapshot
             |
        Substrait plan + query-local read bindings
             |
          CGo / C ABI
             |
     Sirius physical GPU execution
       ^                       |
 bounded native input     bounded native results
       ^                       |
   MO readers             MO result writer

Alternative: MO-admitted immutable TAE snapshot -> TAE GPU ingestible
```

MO owns authorization, the statement snapshot, relation handles, native scan
filtering/projection and MySQL result publication. Sirius owns GPU planning,
tasks, reservations, its input/output buffer leases and asynchronous GPU work.
The Go execution owner joins both sides before releasing their shared lifetime.

Preparation validates the complete plan, input bindings and expected output
schema before starting readers. Every binding is scoped to account, query,
snapshot and schema; arbitrary file reads and undeclared bindings are rejected
by the MO-specific entry point. No cross-query MO scan cache is introduced.

Direct TAE preserves schema checks, immutable-object protection and rejection
of unsupported visible tails/tombstones. Linking TAE is not enough: its table
function and converters must be registered in the embedded context. In-process
resolution does not waive storage protection. A deployment lacking the required
storage protection capability rejects direct TAE. MO-reader execution does not
require Flight certificates, an external resolver, or direct-TAE leases.

## 4. Native ABI and ownership

The native header is `src/include/sirius_c.h` in Sirius; the Go adapter belongs
under `pkg/sql/compile/siriusbridge`. Use opaque engine/query/batch handles and
fixed-width descriptors with explicit lengths, never C++ classes or Go slices.

| Group | Required operations |
| --- | --- |
| Runtime | ABI/capabilities, create, health, stop admission, destroy after queries close |
| Query | Prepare once, start once, schema, incremental result retrieval |
| Input | Acquire capacity, fill native buffers, publish, finish, producer failure |
| Control | Cancel independently of data locks, join, release query resources |
| Batch | Release unpublished input or returned result lease |

Every C entry point catches exceptions and returns bounded, owned error
information. Distinguish unsupported plans, invalid arguments/bindings,
resource exhaustion, cancellation, execution failure and GPU unavailable.
EOF and input-not-needed are explicit states, not empty batches.

A successful input publication transfers ownership. Rejection leaves the
lease with the caller. Returned result buffers remain valid until explicit
release. Native code owns asynchronous buffers: ordinary Go memory must not be
retained by C++/CUDA after the synchronous call. Go fills leased C-owned staging
with bulk copies, without Flight frames or `Batch.MarshalBinary` on this path.
Go copies/consumes each returned native batch through the normal output path
and returns its credit only after consumption.

Go wrappers serialize incompatible operations, close once, stop and join their
cancellation callbacks, and never destroy a handle still used by a CGo call.
Cancellation must run concurrently with a blocked native input/result call.
No native callback may touch released Go state.

The new embedding ABI starts at version 1. Flight versions are unchanged
during coexistence. Do not bump a protocol version for each PR in the stack.
Keep existing standalone C++/DuckDB interfaces working; only the new embedded
path replaces materialized collection.

## 5. Capacity and scheduling

| Resource | Initial default or limit |
| --- | --- |
| Active queries | One on the selected GPU |
| Waiting requests | 16; no readers or native input buffers while waiting |
| Reads per query | Existing maximum of 16 |
| Native input per read | 64 MiB including filling, queued and in-flight buffers |
| Source unit descriptors | At most 128 contributing MO batches |
| Coalescing target | 32 MiB, bounded by the 64 MiB hard limit |
| Result window | 64 MiB including results borrowed by Go |
| Host/GPU/spill capacity | Explicit finite configuration, checked by admission |

Account descriptor/bitmap bytes and relevant expansion before allocation.
Constant-encoded or variable-width data cannot bypass the expanded-source
bound. Split oversized batches at row boundaries; reject an individually
unrepresentable row. Account per-read reservations in a process-wide admission
envelope; a query whose minimum progress envelope cannot fit is rejected,
not admitted into a permanent capacity wait. Physical pool allocation and
logical query reservations must not count the same bytes twice.

Input back-pressure:

1. MO acquires native capacity before allocation/copy.
2. Bounded coalescing creates a GPU source unit on downstream demand.
3. Filling, queueing and H2D ownership consume the same credit budget.
4. Credit remains held through asynchronous use and retry ownership.
5. If Sirius stops consuming, publication blocks, MO's existing bounded output
   edge fills, and the reader stops after ordinary DOP-bounded read-ahead.

No second unaccounted repository or eager source-continuation queue is allowed.
Do not replace back-pressure with spillable-but-unbounded transport state.

Output uses an incremental sink with capacity-aware scheduling. A full result
window parks result-producing work without occupying every task-creator/GPU
worker. Include in-flight publication in capacity; wakeups are durable and
cancellation-aware. Never collect the full result and only then expose a stream.
TAE metadata dispatch and prefetch also remain bounded.

## 6. Lifecycle, failure and recovery

The first owner is the Go query execution owner and its native query handle:

```text
prepared -> running -> draining -> closed
   \----------- cancel/error --------^
```

Cancellation, errors, deadlines, partial initialization and shutdown enter
draining from any live state. The first terminal failure wins. Early LIMIT and
pruned inputs retire unneeded producers with a success-valued signal.

Cleanup order:

1. Seal publication and wake input/output/admission waits.
2. Request both MO and native cancellation; stop and join MO producers.
3. Drain native scans, task creation, GPU tasks and asynchronous transfers.
4. Release leases and query-owned repositories.
5. Release TAE protection and MO scan/snapshot resources.
6. Release admission and destroy the query handle.

Waits must have an independently callable cancellation path; a data-path mutex
must not prevent cancellation from delivering the event that wakes that path.
Queued requests allocate no native inputs and can cancel before engine entry.
An error after schema/rows become visible never triggers native-MO replay.
Preparation failures do not start readers; cleanup failures never count as
safe fallback.

Only a cleanly quiesced failure permits runtime reuse. Fatal CUDA failure or
unprovable quiescence seals admission and requires fail-stop/restart of MO.
A timeout does not authorize freeing live buffers, resetting a device in use,
or starting another query on a poisoned runtime. This larger blast radius is
an accepted consequence of embedding and must be visible in readiness/health.

Direct-TAE recovery and old Flight leases remain protected until their owner
is quiescent or process death has been established. Do not remove lease
recovery merely because new calls no longer use a network resolver.

## 7. Packaging and service integration

Add opt-in `MO_SIRIUS=1` and a `sirius` Go build tag, independent of MO's current
GPU vector-index flag. Export a real static embedding target with complete
transitive dependency/device-link metadata. Do not link MO to a loadable DuckDB
extension or hide unresolved/duplicate symbols with broad linker flags.

Package pinned shared dependencies beside MO with relocatable runtime paths.
Pin source commits, compiler/toolchain, architecture and native artifacts in
build provenance. Use Pixi and incremental host builds; no container rebuilds.
Combined Sirius/cuVS builds must use a single compatible CUDA/RAPIDS dependency
set and a lifetime-managed allocator arrangement. No per-query replacement of
process-wide device resources or `cudaDeviceReset` is permitted.

The backend selector is `flight` or `embedded`. Empty selection preserves
Flight during coexistence. Unsupported/uncompiled embedded execution is an
explicit configuration error, not successful native fallback. Separate common
query limits from Flight-only certificates/addresses. Keep ordinary CPU builds
free of a new GPU library requirement. Preserve old hint behavior while adding
embedded selection with MO-reader default and explicit direct TAE.

## 8. Ten-PR delivery map

All PRs reference #28966; no intermediate PR auto-closes the migration.
Numeric #28968 has its own design and PR count outside this table.

| PR | Repository | Closure | Dependencies | Merge evidence |
| --- | --- | --- | --- | --- |
| 1 | Sirius | Sync newest upstream dev into upstream-dev-merge, retain TAE/evidence/quiescence and regenerate Pixi lock | none | Build, TAE fixtures, task-lifetime and multi-stream checks |
| 2 | MO | This design, narrow backend/execution contract, Flight adapter and selector | approved design | CPU contract/config tests, unchanged Flight behavior, fake backend lifecycle |
| 3 | Sirius | Static target, C ABI, native leases, query lifecycle, errors/cancel/join/health | 1 and contract from 2 | Linked native consumer, partial init, cancel-before-start, queued GPU failure, safe reuse |
| 4 | Sirius | Bounded MO-native ingestion on unified scan path | 3 | Capacity/ownership, bitmap/NULL varlena, GPU conversion, full-window cancellation |
| 5 | Sirius | Embedded TAE registration/bindings and strict Substrait admission | 3, 4 | TAE fixtures, schema/identity/plan rejection, projection/filter and bounded preparation |
| 6 | Sirius | Bounded incremental native result sink and capacity wakeups | 3; integrates 4, 5 | Result larger than window, stalled output, full-window cancel, failure is not EOF |
| 7 | MO | CGo bridge, artifact/build/package, service owner, memory/admission/fatal handling | 2, merged 3-6 | CPU/Sirius/combined build and loader checks, callback lifetime, allocation balance, GPU coexistence |
| 8 | MO | Real readers/results, direct TAE admission, execution evidence and parity harness | 7 | Public SQL and lifecycle on supported types; opt-in only while numeric blocker remains |
| 9 | MO | Default cutover, Flight removal and configuration/recovery migration | 8, 28968, every gate | Fresh all-22/GPU/performance, failure/restart/config migration, MO CI |
| 10 | sidecar | Retire MO/Sirius Flight service and its stale deployment/code/tests | 9 and release artifact | Retained tooling tests, current README, explicit replacement mapping |

PR 1 is a synchronization, not embedding code. PR 2 adapts Flight without
changing its wire contract; do not merge the entire #27599 branch. PRs 3-6
target the live upstream architecture, never `src/legacy`. PR 5 must not sneak
numeric narrowing into admission. PR 6 preserves unrelated standalone
materialized interfaces. PR 8 may merge disabled by default with numeric
blockers reported, not skipped. PR 9 cannot be combined with PR 8 to evade
cutover evidence. PR 10 preserves independently useful DuckDB/TAE tools and
historical benchmark records.

Use existing directories and dedicated branches in the user's forks. Sirius
integration PRs target `matrixorigin/sirius:upstream-dev-merge`; MO and sidecar
target their `main` branches. Pin MO artifacts to merged Sirius commits, not
moving branch names. Do not mark dependent PRs ready with unresolved contracts
or cumulative unmerged diffs. Optional later submissions of reusable patches
to `sirius-db/sirius:dev` are outside this ten-PR count.

## 9. Verification and observability

Each behavior-changing PR includes its own focused tests. PR 8 is integration
proof, not a deferred dumping ground for earlier missing ownership tests.

- Deterministic UTs: success/empty/NULL/variable width, transfer rejection,
  exact byte/count limits, oversized rows, blocked consumers, missed wakeups,
  early retirement, cancel before/during/after start, partial init and cleanup.
- GPU ownership: injected failure after asynchronous work starts; input,
  reservation, stream and task owners survive until quiescence. Only clean
  recoverable failures can retry/reuse.
- Public SQL: persisted data, committed unflushed tail, visible deletes,
  snapshot isolation, schema drift, MySQL output failure and session reuse.
- Builds: CPU-only, Sirius-enabled and combined Sirius/cuVS, including real
  executable loadability and native provenance. MO CI must pass; do not repair
  unrelated native-repository CI to claim this migration is validated.
- Memory: deliberately pause consumers and prove reader progress stops, queues
  remain bounded and query-accounted bytes return to baseline after cleanup.

Record query-local backend and scan mode separately, fallback, stage rows and
bytes, first-row/total wall latency, CPU time, native/Go/pinned/GPU peaks,
capacity waits, cancellation origin and terminal health. Keep metric labels
bounded; redact credentials, object paths and row contents from artifacts.

## 10. All-22 and performance cutover gates

Run all 22 canonical SF1 and SF10 queries through embedded MO-reader and TAE
inputs with streams=2 and fallback disabled. Require GPU execution evidence,
correct schema and native-MO results. Also validate streams=1 and a higher-
stream stress configuration. Passing at one stream alone is insufficient.
Agreed floating-point tolerance never excuses decimal/type corruption.

Publish Q1-Q22 and a sum for MO native, Flight+TAE, Flight+MO, embedded+TAE and
embedded+MO. Use identical data/semantics, matching hardware/memory configuration
and exact recorded source/artifact revisions. Distinguish cold/warm runs.

Default campaign: one excluded warm-up and five measured repetitions, routes
alternated and GPU benchmarks never concurrent. Report each query's median and
their sum, plus raw runs and the separately labelled median full-suite time.
Repeat Q9 ten times for its concurrency sensitivity. CPU samples are not wall
time and are not additive stage latencies.

Embedded MO-reader Q9 and full-suite median must each be at most 2x embedded
direct TAE. Neither embedded route may regress against its controlled matching
Flight baseline. Historical 34.16s/15.50s observations are not fresh evidence.

#28968 must inventory current-main numeric failures and provide separately
reviewed exact lowering/execution/result reconstruction. No benchmark SQL
rewrites, decimal-to-float coercion, unchecked narrowing, skipped cases or CPU
fallback may satisfy the all-22 gate. PRs 1-8 can proceed independently; PRs
9-10 cannot.

## 11. Cutover, cleanup and completion

During coexistence rollback uses the backend selector. Before removing Flight,
drain/reconcile its outstanding executions and protection, preserving recovery
until no old consumer can read. After removal rollback uses the previous
release; document configuration compatibility and retained protection state.

Map every retained ownership/semantic test to its replacement before deleting
transport-only cases. Remove tickets, uploads/acks, Flight client/server and
network-resolver configuration only at their retirement PRs. Retain storage GC
protection and independently useful sidecar tools. Do not delete historical
timing comments or relabel old scan-mode evidence.

Completion has two milestones:

1. PRs 1-8: opt-in embedded backend delivered and supported contracts verified.
2. Numeric blocker resolved, every gate passed, PRs 9-10 delivered: migration
   complete, predecessor PRs explicitly superseded, #28966 can close.

No benchmark, CI or readiness claim follows solely from this design approval.
