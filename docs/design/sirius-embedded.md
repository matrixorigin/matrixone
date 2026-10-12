# Embedded Sirius migration

Design version: 2.

Owner: MatrixOne query execution.

Tracking: [#28966](https://github.com/matrixorigin/matrixone/issues/28966).
Separate numeric compatibility blocker:
[#28968](https://github.com/matrixorigin/matrixone/issues/28968).

Status: implementation in progress. The user approved the ten-PR plan before
implementation and narrowed this milestone to MO-reader input on 2026-09-23.
Direct TAE input requires a separately reviewed design and is deferred. These
decisions do not claim implementation review, CI, GPU validation, or production
readiness. Each implementation PR must link the reviewed revision of this
document and report its own evidence.

## 1. Decision and scope

Embed Sirius and DuckDB statically in `mo-service` through a C ABI and CGo,
retaining pinned shared GPU dependencies. Use the Sirius commit pinned at
`third_party/sirius`, integrating the native embedding work with a reviewed
`sirius-db/sirius:main` commit, not the legacy engine or a from-scratch port.

- MO readers are the only embedded input in this milestone. No new TAE or
  directory-lock changes are required for Sirius offload.
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

```

MO owns authorization, the statement snapshot, relation handles, native scan
filtering/projection and MySQL result publication. Sirius owns GPU planning,
tasks, reservations, its input/output buffer leases and asynchronous GPU work.
The Go execution owner joins both sides before releasing their shared lifetime.

Preparation validates the complete plan, input bindings and expected output
schema before starting readers. Every binding is scoped to account, query,
snapshot and schema; arbitrary file reads and undeclared bindings are rejected
by the MO-specific entry point. No cross-query MO scan cache is introduced.

MO-reader execution uses the existing MO statement snapshot and scan ownership.
It does not require Flight certificates, an external resolver, direct-TAE
leases, or a new storage bootstrap hook. An explicit direct-TAE input request
fails configuration before a native query starts. The existing Flight service
retains its own lease and recovery contract during coexistence.

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

The compiler-facing input exposes `Acquire(ctx, payloadBytes)` and returns a
lease with `Capacity`, `Publish`, and `Release`. Acquisition must precede any
additional outgoing payload allocation, expansion, coalescing, or copying;
existing MO reader batches remain governed by their bounded pipeline edge.
Register lease cleanup immediately after acquisition. Successful publication
consumes native ownership and retains no Go buffers after the synchronous call;
failed publication leaves cleanup with the producer. A lease publishes at most
once. Release is idempotent, and a surviving handle after native release failure
transfers exactly once to query cleanup. Query cancellation uses an independent
native control path and interrupts blocked acquisition before joining producers.

1. MO acquires native capacity before allocation/copy.
2. Bounded coalescing creates a GPU source unit on downstream demand.
3. Filling, queueing and H2D ownership consume the same credit budget.
4. Credit remains held through asynchronous use and retry ownership.
5. If Sirius stops consuming, acquiring another lease blocks, MO's existing
   bounded output edge fills, and the reader stops after ordinary DOP-bounded
   read-ahead.

No second unaccounted repository or eager source-continuation queue is allowed.
Do not replace back-pressure with spillable-but-unbounded transport state.

Output uses an incremental sink with capacity-aware scheduling. A full result
window parks result-producing work without occupying every task-creator/GPU
worker. Include in-flight publication in capacity; wakeups are durable and
cancellation-aware. Never collect the full result and only then expose a stream.
MO reader prefetch remains bounded by its existing pipeline and the native
input credit window.

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
4. Release query-owned repositories and credits.
5. Release MO scan/snapshot resources.
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

Old Flight leases remain protected until their owner is quiescent or process
death has been established. The embedded MO-reader path creates no direct-TAE
lease authority. Do not remove Flight recovery merely because the new path
does not use a network resolver.

## 7. Packaging and service integration

Add opt-in `MO_SIRIUS=1` and a `sirius` Go build tag, independent of MO's current
GPU vector-index flag. Export a real static embedding target with complete
transitive dependency/device-link metadata. Do not link MO to a loadable DuckDB
extension or hide unresolved/duplicate symbols with broad linker flags.

Package pinned shared dependencies beside MO with relocatable runtime paths.
Pin source commits, compiler/toolchain, architecture and native artifacts in
build provenance. Use Pixi and incremental host builds; no container rebuilds.
Pixi is the sole GPU dependency provider, and combined Sirius/cuVS builds use
one activated Sirius `mo` prefix, as defined by the
[GPU toolchain design v2](sirius-gpu-toolchain.md). They do not exchange a
separate GPU toolchain manifest. The process needs a lifetime-managed allocator
arrangement. No per-query replacement of
process-wide device resources or `cudaDeviceReset` is permitted.

The backend selector is `flight` or `embedded`. Empty selection preserves
Flight during coexistence. Unsupported/uncompiled embedded execution is an
explicit configuration error, not successful native fallback. Separate common
query limits from Flight-only certificates/addresses. Keep ordinary CPU builds
free of a new GPU library requirement. Preserve old hint behavior while adding
embedded selection with MO-reader input only.

## 8. Ten-PR delivery map

All PRs reference #28966; no intermediate PR auto-closes the migration.
Numeric #28968 has its own design and PR count outside this table.
This original map remains the historical record; the remaining sequence below
incorporates the delivered prerequisites and the owner-approved native split.

| PR | Repository | Closure | Dependencies | Merge evidence |
| --- | --- | --- | --- | --- |
| 1 | Sirius | Sync newest upstream dev into upstream-dev-merge, retain TAE/evidence/quiescence and regenerate Pixi lock | none | Build, TAE fixtures, task-lifetime and multi-stream checks |
| 2 | MO | This design, narrow backend/execution contract, Flight adapter and selector | approved design | CPU contract/config tests, unchanged Flight behavior, fake backend lifecycle |
| 3 | Sirius | Static target, C ABI, native leases, query lifecycle, errors/cancel/join/health | 1 and contract from 2 | Linked native consumer, partial init, cancel-before-start, queued GPU failure, safe reuse |
| 4 | Sirius | Bounded MO-native ingestion on unified scan path | 3 | Capacity/ownership, bitmap/NULL varlena, GPU conversion, full-window cancellation |
| 5 | Sirius | Embedded MO bindings and strict Substrait admission | 3, 4 | Schema/identity/plan rejection and bounded preparation; any existing TAE capability stays inaccessible from MO |
| 6 | Sirius | Bounded incremental native result sink and capacity wakeups | 3; integrates 4, 5 | Result larger than window, stalled output, full-window cancel, failure is not EOF |
| 7 | MO | CGo bridge, artifact/build/package, service owner, memory/admission/fatal handling | 2, merged 3-6 | CPU/Sirius/combined build and loader checks, callback lifetime, allocation balance, GPU coexistence |
| 8 | MO | Real MO readers/results, execution evidence and parity harness | 7 | Public SQL and lifecycle on supported types; opt-in only while numeric blocker remains |
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

### Remaining implementation sequence

Owner-approved on 2026-10-08. MO #29547 delivers the opt-in bounded MO-reader
bridge; Sirius #25 delivers numeric primitives. Importer #3/#4 are merged.
These are prerequisites, not all-22 or production cutover evidence.

| Order | Repository | Complete closure | Required predecessor |
| --- | --- | --- | --- |
| A | Sirius | Scoped exact type/literal import, immutable binding, clone fidelity and masked scalar GPU execution; capability disabled | Merged Sirius #25 and importer #4 |
| B | Sirius | SUM/AVG/MIN/MAX, exact grouping/join/sort keys, spillable states, complete admission, statuses 12/13 and capability 16u | Merged A |
| C | MO | Capability-scoped lowering, Decimal256 publication/reconstruction, public typed errors and query-local evidence; opt-in | Merged and pinned B |
| D | MO | Real native-MO/embedded-MO runner, all-22 SF1/SF10 public/type/error parity, lifecycle/resource and performance acceptance | Merged C |
| E | MO | Embedded default, Flight removal and strict configuration/recovery migration | D and all acceptance gates |
| F | sidecar | Retire superseded MO Flight service/deployment/tests while retaining independent tools and historical records | E and verified available MO release artifact |

Native A may not expose partial numeric support. Native B completes and validates
the entire family before advertising it. C must preserve one export profile
through validation and serialization; ordinary Flight emission remains unchanged
during coexistence. Exact semantic approval remains MO #29449 document blob
`42a89f09a1d168d02b9583cb3ea7b4de6dbb5634`.

D versions the campaign contract around required native-MO and embedded-MO
routes. The obsolete embedded-TAE route is not required. Equivalent Flight
coverage is optional and explicitly recorded; unavailable comparisons are N/A,
not incomplete results presented as full-suite evidence. The matched Flight+MO
ratio limit remains 1.0 for common-suite metrics and Q9 where available. The
numeric regression limit remains 10% against valid equivalent baselines.

E rejects enabled legacy Flight selections and removed transport settings with
actionable migration errors. Disabled Sirius and ordinary CPU behavior remain
unchanged. Cutover requires authoritative replay/readiness and no unresolved
Flight executions; a nil or unready lease manager is not proof of empty state.
Retain GC protection until old consumers are proven quiescent. Reconciliation
and post-removal rollback use the previous release. F waits for the actual MO
release artifact, preserves DuckDB/TAE/HTTP and standalone Sirius tools, and
maps useful tests to retained replacements before transport-only deletion.

Close #28968 only after numeric/public/resource/performance acceptance passes;
close #28966 only after E, release availability and F are delivered. Docker
image/base changes remain a separate later PR.

## 9. Verification and observability

Each behavior-changing PR includes its own focused tests. PR 8 is integration
proof, not a deferred dumping ground for earlier missing ownership tests. Its
parity harness validates each runner result incrementally, stops before later
work on invalid evidence, and writes only a fixed, sanitized `failure.json`;
the CLI rejects a non-empty output directory before invoking the runner and
leaves it untouched. Failure diagnostics can accompany this invocation's partial
artifacts but never overwrite an existing diagnostic. Result validation and
digests use the same schema/rows boundary, ignoring unknown result fields.
Complete campaigns retain the full schedule and existing performance gates.

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

Run all 22 canonical SF1 and SF10 queries through embedded MO-reader input
with streams=2 and fallback disabled. Require GPU execution evidence, correct
schema and native-MO results. Also validate streams=1 and a higher-stream
stress configuration. Passing at one stream alone is insufficient.
Agreed floating-point tolerance never excuses decimal/type corruption.

Publish complete Q1-Q22 tables and sums for MO native and embedded+MO.
Native MO is the all-22 correctness oracle. Flight's existing exporter declines
14 canonical queries; exact numeric support remains embedded-only. The owner
selected native-oracle cutover on 2026-10-01 rather than extending Flight.
Publish Flight+TAE and Flight+MO comparisons only where equivalent execution is
supported, explicitly marking unsupported or unavailable coverage. Never call
a partial sum a full-suite total. Flight+TAE is a retained comparison baseline,
not an embedded route. Use
identical data/semantics, matching hardware/memory configuration and exact
recorded source/artifact revisions. Distinguish cold/warm runs.

Default campaign: one excluded warm-up and five measured repetitions, routes
alternated and GPU benchmarks never concurrent. Report each query's median and
their sum, plus raw runs and the separately labelled median full-suite time.
Repeat Q9 ten times for its concurrency sensitivity. CPU samples are not wall
time and are not additive stage latencies.

Compare embedded MO-reader performance with controlled Flight+MO only on the
same supported queries and report the common coverage. An unavailable exact
Q9/full-suite Flight baseline is not fabricated or obtained through narrowing,
SQL rewrites or fallback. Report ratios to Flight+TAE for context; that route
uses a different reader and is not an embedded acceptance gate. Complete native
MO/embedded timings and the reviewed numeric performance gates remain required.
Historical 34.16s/15.50s observations are not fresh evidence.

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
