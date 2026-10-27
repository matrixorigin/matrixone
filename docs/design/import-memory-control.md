# Bounded import working sets and pipeline retirement

This design applies to the import-memory changes on `4.1.6-hotfix`.
Owner: aptend. Implementation context: [PR #31166](https://github.com/matrixorigin/matrixone/pull/31166).
The original implementation and design are preserved at
[`ef64eb8bea`](https://github.com/matrixorigin/matrixone/tree/ef64eb8bea866b2c5fe457593bd434d573c6ef5f).
This revision preserves the active allocation, admission and retirement contracts
and retires the unenabled distributed admission extension.

## Problem and supported execution

A completed import can leave both live reusable buffers and allocator-resident
free pages. A second import needs allocation headroom before constructing its
next working set; limiting idle caches alone does not bound that working set.
The required behavior is successful repeated import within the process memory
limit, with intact rows/indexes and terminal cleanup on success, error and cancel.

Admission applies only when the complete physical graph has a controlled CSV
source and supported allocation contracts. Prepared executions, unknown variable
length expressions, JSONLINE, retaining/unbounded operators, partition wrappers,
unproven transport and channel layouts keep the whole statement on its existing
execution path before allocating an import ticket. A local nested compile can
borrow an existing process view without replacing or closing its parent's root.

The bounded producer graph rejects both incoming remote receiver descriptors and
a scope that would actually execute on another CN. Therefore every eligible
scope executes at the origin. This is an eligibility boundary, not a new
distributed memory guarantee. Remote pipeline execution continues to use its
existing protocol and must not independently admit a partial statement graph.

## Scope and alternatives

Retain one local manifest and one statement admission entry point, using the
existing compiler graph and resource coordinator. Local planning, admission and
execution must use the same schemas, physical copy counts and output units.

The previous extension had a second MORPC client pool, capability/process-ID
discovery, OPEN/COMMIT/END messages, token owners, late scope attachment and
MessageBoard roots. None could be reached by an eligible physical graph:
the first scope placed remotely by `memoryExecutionLocations` satisfies exactly
the remote predicate rejected by the producer-graph walk. A successful producer
walk cannot therefore produce a multi-CN admission manifest.

Keep that extension only if transport allocations and lifetime are first bounded
and distributed admission is intentionally enabled. Keeping its current dormant
implementation incurs owners, branches, tests and compatibility obligations
without bounding another supported statement. Enabling it now would expand
behavior and require distributed acceptance. The selected change removes the
dormant extension and preserves whole-query legacy fallback for remote graphs.
It does not enable independent per-RPC admission.

Local manifests are internal typed planning values. They have no wire version,
serialized JSON contract or incoming remote batch unit.

The removed capability, token and control interfaces were introduced only in this
unreleased change. Restore the target branch's query protocol and pipeline client
instead of retaining speculative compatibility with intermediate experimental
peers. Broadcast spill topology stays in local operator templates and their shared
process owner. Eligible imports never cross a CN boundary, so no new pipeline
protobuf fields or remote spill admission capability is needed. Remote encoding
keeps the released target protocol; local topology/reuse tests prove that separate
in-process contract.

## Allocation and admission

Use owned allocated capacity, including Go and native buffers, rather than
cumulative processed bytes or a shared-pool allocation delta. Reserve an append,
copy, resize or serialization peak before mutation; reconcile to retained
capacity afterwards. Cross-pool frees and partial failures retain exactly one
effective owner. Returning a grant does not prove that its resident pages were
returned to the operating system.

One process coordinator manages three commitments:

| Commitment | Owner and release |
| --- | --- |
| Input/output slots (I) | Complete graph input, output and producer scratch slots; a lease remains charged until consumption/destruction. |
| Retained data (D) | Private writer/build allocations; acquire before growth, then flush/spill on denial without blocking on their own consumer. |
| IO workspace (W) | One process IO/spill job; queued work holds no workspace, and the active job never waits for D or another W. |

Admit the I sum plus any increase in the shared W maximum atomically. Release
notifications drive FIFO progress; the one-second sampling fallback exists only
while waiters exist. Sampling and full recovery are coalesced across throttlers.
Physical-pressure denial can request recovery below the ordinary RSS soft line.
No per-writer periodic sampling loop is added.

Physical pressure uses `max(RSS, max(0, cgroup.current - inactive_file))`, not
RSS plus cgroup usage. Outstanding commitments remain part of the prediction;
unproven resident coverage cannot be subtracted. Missing/partial samples do not
erase a previously observed container limit. Linux cgroup paths are resolved
relative to the selected controller's exposed mount root, including private
namespaces, rather than by blindly appending membership paths.

Before waiting, compute descending legal units from the target down to one byte.
The target comes from the existing throttler policy, capped at 64 MiB. Merge only
identical complete I/W envelopes, retaining the largest unit. Candidate selection
rechecks actual shared commitments and physical pressure after release/recovery.
For a parallel graph whose target cannot fit even the empty process limit,
recompile a smaller DOP before permanently reducing the unit. At final DOP,
wait cancellably for transient pressure. Transactions entered by BEGIN or not
known to be autocommit use immediate bounded admission, since waiting may depend
on resources held by their own previous statements. This does not force rollback
of an otherwise usable explicit transaction.

Working-set membership is derived from the closed root and actual input/IO
owners. Do not maintain another total reference counter for the same owners.
At the coordinator lock, the old total owner count is exactly the open-root
indicator plus input and IO owners. Both promise-release flags can become true
only after root closure and their respective owner counts reach zero. Deleting
the ticket at that point preserves the same final-owner condition.

Close wakes blocked acquisition; active/transferred leases finish under their
existing owners. Remove the ticket after both promises have been retired.

## Source, writer and join bounds

CSV raw records and typed batches follow one admitted unit. Preserve configured
record limits and a pending crossing row without reusing its parser storage.
Bound sparse retained fields, parser scratch, fixed headers, variable payload,
conversion scratch and allocation growth separately. Compact column bounds count
inline values through their headers and do not double count inline payload.
Separate the maximum legal record from the currently admitted record budget.
Reserve records through the candidate batch unit, with the existing CSV read
block as the minimum and the legal bound as the maximum, including LOCAL. The
candidate's row budget shrinks together with its batch unit. Reserving only one
read block while admitting a much larger batch can leave no room for an ordinary
row inside that batch once execution starts; such a row must already be covered.
A known uncompressed input can tighten the legal bound; its total size must not
force every short row to reserve the maximum.
Before extending record storage beyond its budget, the parser grows the complete
statement's I/W reservation, including producer, conversion and join promises.
Precompute geometric record budgets at the selected immutable batch unit before
execution; retain only these bounded specifications, not mutable operator trees.
The configured entry limit and stale-file bound remain enforced independently.

Growth is monotonic and atomic at the existing coordinator. Existing leases
remain charged; new capacities become visible only after the additional promise
fits physical headroom. A smaller concurrent request cannot shrink the ticket.
An executing reader must not queue behind resources held by its own downstream
writer: growth either succeeds immediately or returns a memory error before
expanding record or conversion storage. Cancellation, denial and closure publish
no partial increase. This can
reject a genuinely wider row under pressure; it never reads or converts that row
using unreserved storage. Root/last-lease cleanup retires the enlarged promise
once. Active IO jobs keep their original capacity; later jobs use the larger one.

This reuses the existing controller and entry limit, with no new worker or tuning
parameter. Merely lowering the JSON expansion factors would lose the allocation
proof; pre-scanning files would add IO and cannot handle LOCAL streams. The
selected approach adds one integer comparison to existing parser size checks;
only a new record high-water mark invokes reservation, at most logarithmically
many times per parser. Validate short LOCAL/large-file controls, wider records,
denial before growth, concurrent growth/close, and final promise retirement.

Capacity forecasts run before a possible allocation. During one CSV fill or
merge output, derive the data-row capacity frontier from the actual backing and
reuse it while data and variable-length area still fit. Refresh after growth,
buffer replacement or oversized-buffer cleanup; do not retain it across calls.
Bitmap-bearing merge inputs keep exact bitmap growth prediction. Single-row
variable payload is calculated once and reused by extension. These shortcuts
preserve the same byte boundary and singleton exception. Writer window selection
grows its search prefix geometrically before refining the crossing boundary, so
small byte windows do not repeatedly scan the remaining input batch.

Writer bounds include all index projections, old/new vector backing, bitmaps,
delete preparation, dedup state and output metadata. Committed-delete work stays
charged until materialized or destroyed. Metadata uses its fixed-object/frame
contract rather than borrowing an unrelated raw JSON row bound. Partition and
partial-initialization cleanup still destroy every active/retired writer even
where the statement is ineligible for controlled admission.

Preserve LOAD FAIL/IGNORE and UPDATE index semantics: irregular index sources
must follow the actual final row images and duplicate filtering. DOP recompiling
must not mutate caller routing labels. Old disk Insert uses the same admitted
input/output/workspace ownership without changing its legacy write path.

HashBuild acquires capacity before copying and constructing its hash map. Spill
scatter and encoded frames have byte bounds, and key-expression bounds include
simultaneously live intermediate/results, hash resize, selection and iterator
storage. Unknown expression contracts preserve the legacy execution path.

Controlled HashJoin output has a byte unit; ordinary output retains its existing
behavior. Track retained batch capacity at ownership mutations rather than
rescanning historical batches per append. Local broadcast spill seals actual
participants and gives one finalizer the shared build and spooled probes. Deep
skew uses bounded build chunks, probe replay and sidecar match state while
preserving inner/outer/semi/anti/single semantics. No FD crosses a CN boundary. Participant registration includes empty probe
streams, uses stable scope/parallel identities and transfers all probe files
atomically to one finalizer. Claim and abort establish exactly one terminal
owner; file IO and closing run outside the group lock. Readers use independent
logical offsets through `ReadAt`/`SectionReader`, rather than shared `Seek`.

Queued IO jobs hold their input owners but no W slot. Output transfers into
pre-reserved I or an existing D owner before W is released, so a downstream
consumer never needs the workspace retained by its own input. A replacement
allocation reserves the full new capacity while the old capacity is still live.
The common streaming serializer preserves the persistent format; only temporary
spill frames and anonymous local files use the bounded replay protocol.

Idle Arena pools have a shared byte budget across size tiers. Checked-out arena
capacity belongs to the active job, not to the idle-cache budget. Native/Go
recovery only yields headroom after a fresh physical sample confirms it. Native
trimming is best effort on supported Linux/glibc; online memory-limit resizing
and new support for other libc platforms are outside this change. No table-,
row-count- or elapsed-time-specific production parameter is introduced. No
persistent catalog/object format, authentication or tenant boundary changes.

## Pipeline lifetime

The target branch's typed PipelineEdge terminal is authoritative. Reset publishes
End/Error independently of data-channel capacity; Call does not publish terminal
signals. Preserve primary errors, consumer Stop settlement and cancellation of
siblings before joining them. Failed publication discards only unsent ownership.

Register each spool and receiver immediately after construction. A failure in
later Prepare or earlier cleanup must not strand a saved current batch. After all
local scopes, notifications and cleanup have joined, the attempt owner:

1. Releases saved receiver currents on every owned BaseProcess.
2. Drains edges, including directly accepted frames, retaining terminal causes.
3. Performs final spool-slot cleanup and clears the attempt registries.
4. Restores process views and closes the statement working-set root.

Cache cleanup and final slot cleanup remain separate, idempotent operations.
Active borrowers stay valid until release or proven quiescence. Preallocated
abort bookkeeping, receiver/channel capacities, registry arrays and final graph
metadata are included in admission. An unsupported final topology falls back
before admission rather than repairing its bound through later allocation.

## Validation and delivery

### Ordinary execution and local planning costs

The normal `bytes.Buffer` encoding API keeps its direct, allocation-free scalar
writes. Bounded spill files use the streaming writer API. Both must emit the
same persistent format, with independent compatibility tests. Making ordinary
buffer callers use the generic writer adds per-vector escaping scalars and is
not an acceptable cost for enabling bounded spill IO.

Reject a graph without a controlled CSV source before expression or producer
analysis. For a candidate import, build the producer proof once, including its
operator/key eligibility checks. The proof refers to immutable local expression
trees during pre-execution planning; it must not marshal those trees and decode
them again for each candidate byte unit. The bound calculators never mutate the
trees, and execution starts only after this local planning lifetime ends.

Keep only the admission modes exercised by execution: immediate try for explicit
transactions, retry-capable candidates before DOP recompilation, and cancellable
final-pressure waiting. A one-candidate request uses that same final policy; it
does not need a separate historical waiting mode. Compiler headroom exposes only
the limit and available bytes it consumes. Coordinator ownership remains private;
compiler mocks must not invent owner-state dimensions that execution never reads.

Copy-growth and hash-map admission apply to builds with an import-memory view.
An ordinary shuffle build keeps its existing post-copy row/byte threshold and
does not acquire import data credits or evaluate an import hash-map forecast.
Its spill algorithm and result semantics remain available. An uncontrolled
broadcast build keeps its existing in-memory path. No spill threshold changes.

All memory pools remain visible to the process accounting owner. Their grants
and import admission serialize at that owner. Keep the aggregate physical guard
even without an import ticket: independently valid pool quotas can grant more
pending memory than the process limit before RSS reflects any allocation. For
example, 100 units of RSS plus pending grants of 600 and 350 exceed a 1000-unit
limit even though each pool's local 900-unit quota permits its request. Merely
restricting this guard to import tickets would reopen that overcommit. The CN S3
pool also retains native/cache recovery. No stale RSS credit is restored.
Narrow ordinary execution cost by removing import build forecasting and grants
from ordinary shuffle joins, rather than weakening shared memory exclusion.

Alternatives considered are retaining all generic writer/provider paths, which
preserves measured ordinary-path cost and unused branches, and disabling shared
accounting/spill bounds, which removes safety under concurrent grants or large
dedup builds. Preserve the active ownership and byte bounds while narrowing their
consumer boundary. Do not add a new worker, cache, configuration or workload
specific threshold. Validate normal buffer allocation and encoding, no-source
early rejection, unchanged local expressions, both admission wait policies,
ordinary shuffle threshold behavior, controlled pre-copy denial and cross-pool
promise exclusion separately.

Map each remaining test to a distinct invariant, reachable path, varied dimension
and independent oracle. Use the production manifest/admission path in compiler
tests. Keep real public LOAD planning controls, index/DOP assertions, partial
allocation failures, cancellation and joined retirement. Consolidate equivalent
flag combinations and reusable preparation without deleting distinct checks.
Retire tests whose only contract is the removed distributed control extension.
Preserve remote-graph and remote-receiver legacy fallback regression tests.

Validate affected packages normally and under race where ownership changed;
stress only exact affected concurrency tests. Keep native artifacts matched to
the source/toolchain and run incremental vet/lint. Regenerate changed protobufs
with the repository-supported generator, preserving spill codecs.

Bounded processing cannot guarantee success if one irreducible row/minimum
working set exceeds the process limit or unrelated cgroup users exhaust it.
Skew chunk replay trades bounded memory for potentially repeated probe IO; do
not describe it as a free performance improvement.

Final workload acceptance is separate: fixed 22 GiB/no swap, original Amway
inputs/order/index options, five continuous complete rounds without retries,
restarts, forced GC or inter-round pauses, followed by row/index/temporary-table
and resource-retirement checks. Measure TPCH under the selected matching
deployment configuration; a memory improvement does not waive a performance
regression. Historical executions remain attached to their tested source, not
relabelled as executions of this revision.

Keep raw measurements, manifests and review execution records outside tracked
source. The maintained design describes current contracts; the PR body contains
the problem, resulting behavior, key evidence and material limitations.
