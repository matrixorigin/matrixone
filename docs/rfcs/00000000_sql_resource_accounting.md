- Status: draft
- Start Date: 2026-07-15
- Authors: MatrixOne SQL / Observability team
- Implementation PR: TBD
- Related issues:
  - https://github.com/matrixorigin/matrixone/issues/22263
  - https://github.com/matrixorigin/matrixone/issues/18449

# SQL Resource Accounting Refactor

## 1. Summary

MatrixOne currently reconstructs a value called CPU by adding operator and SQL
phase durations, then subtracting statement-global IO and lock waits. The terms
do not have the same concurrency, CN, plan, or retry scope. This produces
negative CPU and unstable derived CU inputs. Compile retry adds a second problem:
some resources are reset, some span attempts, some remain on an old plan, and
some remote work is lost on error.

This RFC replaces that model with one reduction tree:

```text
AttemptSummary =
    AttemptLocal
  + FragmentDelta*
  + JoinedLeaseDelta*
  + MemoryDomainSummary*

ExecutionSummary = sum(AttemptSummary*)

ResourceRoot =
    RootLocal
  + ExecutionSummary*
  + RootMeta
```

The design is intentionally small:

- one accounting owner for every physical resource event;
- one attempt ownership handle in the execution context;
- fragment-local counters in hot paths;
- one immutable fragment delta at terminal state;
- one single-writer reducer for attempt and statement totals;
- no statement-global time subtraction;
- no persistent attempt or fragment ledger;
- no new resource table or column;
- no long-lived old/new compatibility layer.
- no pricing formula in the accounting layer.

There is exactly one production reduction and export path:

```text
producer event
  -> LocalRecorder
  -> owning terminal delta/summary
  -> AttemptSummary or RootLocal
  -> ExecutionSummary
  -> ResourceRoot
  -> StatementResourceSummary
  -> CalculateCU once
  -> StatementExport{Summary, DerivedCU}
  -> optional short-statement group reducer
  -> StatsArray v6 projection
  -> statement_info
```

Every composition uses one typed merge operation. `StatsArray`, `AnalyzeInfo`,
explain JSON, and CU are consumers of this path; none of them is an accounting
source. There is no second merge implementation in frontend, trace export, or
physical plan code.

The new statistic is `exclusive_active_ns`, not CPU. It is the sum of
producer-local exclusive active intervals and may exceed statement wall time
under parallel or multi-CN execution. Waits and wall latency are independent
diagnostics.

MatrixOne validates the new model with deterministic tests and a new-only
canary, then replaces and deletes the old resource-accounting path. No runtime
mode calculates both models.

## 2. What Is Persisted Today

Resource information is already persisted, but there is no attempt/fragment
resource ledger:

- `system.statement_info.stats` stores `StatsArray` as text;
- `system.statement_info.cu` stores the current derived CU;
- `system.statement_info.exec_plan` stores plan diagnostics;
- `system_metrics.sql_statement_cu` stores the aggregated CU metric.

Current `StatsArray` version 5 is a positional JSON array containing time,
memory, S3 request counts, traffic, connection type, packet count, and derived
CU.

This RFC does not add or change a table/column. It keeps the positional
`StatsArray` in the existing `stats` text column and bumps its data version to
6. The first 11 positions retain their locations and v6 appends only named,
measurable resource, quality, and retry quantities:

```text
[version, active_ns, peak_memory_bytes, s3_put, s3_get,
 client_egress_bytes, conn_type, packet_count, cu, s3_list, s3_delete,
 quality_flags, s3_read_bytes, s3_write_bytes, spill_bytes,
 total_wait_ns, attempt_count]
```

Version 6 preserves existing positions while defining these changes:

- index 1 is `exclusive_active_ns`, never reconstructed “CPU”;
- index 2 is the maximum MPool live-byte occupancy observed in the statement:
  exact for isolated execution MPools and an absolute occupancy peak for the
  request-serialized session MPool. New code exposes it as `PeakMemoryBytes`
  while retaining the serialized `MemorySize` position;
- index 5 becomes actual accepted protocol bytes as defined in section 5.2;
- index 10 includes DELETE and DELETE_MULTI;
- index 11 makes completeness and invariant quality visible;
- indexes 12-16 persist exact IO/spill bytes, aggregate wait, and retry count.

Old rows remain self-describing versions 1-5 and are not rewritten. No reader
needs to distinguish an array from an object, and old/new rows can coexist in
the same column.

Consumers use index 0 when interpreting historical data. No new binary executes
old and new accounting simultaneously, and no v6 row is downgraded or translated
into a v5 value. Reusing the text column avoids a table migration; preserving an
incorrect 11-element semantic shape is not a design goal.

Attempt, fragment, retry plan, and remote report details live only in memory.
Only a bounded statement summary is persisted.

## 3. Problems in the Current Model

### 3.1 Invalid active-time algebra

The frontend currently calculates:

```text
operator time
+ parse + plan + compile + prepare
- lock wait - plan IO - filesystem IO - IO merger wait
```

Parallel waits are additive across goroutines and may be much larger than
statement wall time. Subtracting them from a different scope is invalid.

When the result is negative, the frontend logs it but leaves the original
operator time in `StatsArray`. Therefore the same field silently has two
semantics:

```text
normal path   -> reconstructed time
negative path -> operator time only
```

Similar arithmetic exists in frontend, analysis, and physical-plan display
code. Serialization must not own accounting rules.

### 3.2 Retry has no resource boundary

The retry loop creates a new `Compile`, but resource state is mixed:

- `resetStatsInfoPreRun` clears selected fields;
- context-level waits may span all attempts;
- CounterSets may be reset, replaced, or inherited;
- failed operator statistics may be released before collection;
- a definition-change retry may replace the plan topology;
- final-plan node statistics cannot represent old-plan work;
- statement duration includes retries even when counters do not.

### 3.3 Remote error paths lose accounting

Remote physical-plan analysis is primarily returned on successful completion.
An error or abort may consume resources without returning a complete delta.
Retrying on the coordinator can then lose remote work.

### 3.4 Counter ownership is implicit

`perfcounter.Update` discovers several context keys, CounterSet maps, extra
CounterSets, and service counters. A producer cannot determine from the call
site which resource totals it updates. This makes omission and double counting
difficult to prevent.

### 3.5 Memory has mixed semantics

Operator `MemorySize` is used both as cumulative allocation and as peak memory.
The values are summed across nodes and multiplied by the whole statement
duration. The result is neither peak-live memory nor memory byte-time.

## 4. Goals and Non-goals

### Goals

1. One name, unit, scope, owner, and merge rule per field.
2. Include every finalized attempt, including failed and canceled attempts.
3. Merge every terminal local/remote fragment exactly once and mark every
   missing dispatched fragment explicitly.
4. Separate statement resource totals from final-plan diagnostics.
5. Make missing data explicit with `partial`, never silent zero.
6. Keep retry memory bounded and hot-path overhead negligible.
7. Remove obsolete accounting code after cutover.
8. Provide a deterministic harness for algebra, retry, remote, cancellation,
   and concurrency.

### Non-goals

1. Exact per-SQL OS CPU time. Go goroutines migrate between OS threads and many
   SQL statements share a process. Low-overhead exact attribution is not
   available.
2. Persisting every attempt, fragment, or operator resource report.
3. Retaining every historical retry plan or error.
4. Redesigning the current CU coefficients or formula. CU remains a downstream
   derived value and cannot redefine resource semantics.
5. Exact per-SQL attribution of Go heap, native-library, or shared service-cache
   memory. OOM events expose those CN-level quantities as unattributed context.

## 5. Resource Semantics

### 5.1 Time

| Field | Definition | Merge |
| --- | --- | --- |
| `statement_wall_ns` | Client request accepted to response completed | root timeline |
| `attempt_wall_ns` | Attempt begin to sealed summary | sum in retry summary |
| `exclusive_active_ns` | Producer-local wall minus local waits and local child calls | sum |
| `wait_ns[kind]` | Aggregate blocked time by reason | sum |

`exclusive_active_ns` is not called CPU. It may exceed statement wall time when
workers or CNs run in parallel. Aggregate waits may also exceed wall time.
Neither condition is an error.

The initial wait enum is deliberately small:

```text
lock
filesystem
io_merger
remote
output
other
```

Only producer-local subtraction is valid. For an operator Call:

```text
exclusive active = call wall - local wait - local child-call wall
```

All producers use one checked constructor from `pkg/util/resource`. If local
wait plus child-call wall exceeds call wall, the contribution is zero and an
invariant flag is set. Producers cannot cast a negative duration to `uint64` or
implement their own subtraction.

Parse, plan, compile, and pre-run work contributes to active time only when a
producer can measure a non-overlapping local interval. Legacy I/O/lock counters
may aggregate parallel waits and therefore exceed coordinator wall time. In
that case the measured wait is retained, the phase contributes no active
interval, and no invariant is raised: the inputs describe different scopes,
not broken arithmetic. Completeness is preferable to fabricated CPU precision.

Wait totals remain in the typed summary and are projected into the persisted
`exec_plan` resource-diagnostics section and `EXPLAIN ANALYZE`; they are not
discarded after root export. Low-cardinality metrics expose aggregate wait by
kind. StatsArray remains the compact resource/CU projection and does not
duplicate the wait vector.

### 5.2 S3 and traffic

The accounting names actual operations:

```text
s3_requests[head|get|put|list|delete|delete_multi]
s3_read_bytes
s3_write_bytes
client_egress_bytes
```

Fileservice is the only accounting source of truth for S3 operations and bytes.
Plan rows must not estimate an operation that fileservice already observed.
Any temporarily necessary estimate is a separately named diagnostic and is not
merged into observed counts.

When IO merger/coalescing lets one physical S3 request serve several statements,
the merger leader/request creator owns the observed request and bytes. Followers
record merger wait only. A service-owned or detached prefetch owns its physical
request in the service sink. MatrixOne does not allocate a participant list or
split one request fractionally across statements.

The protocol/net writer is the only source of truth for client egress. The value
is the number of application-protocol bytes successfully accepted for client
write, including the MySQL packet header actually written. It excludes estimated
TCP/IP or TLS overhead, bytes prepared but not accepted, and synthetic error
packet estimates. Internal CN traffic remains an RPC diagnostic.

StatsArray index 3 projects PUT. Index 4 projects GET plus HEAD. Index 9 projects
LIST. Index 10 projects DELETE plus DELETE_MULTI. The projection tests enumerate
all operations so a request kind cannot silently disappear from CU.

### 5.3 Memory

Memory records physical facts useful for OOM and leak diagnosis. It does not
estimate a charge. Internally, an attributable allocator domain exposes:

```text
allocated_bytes
freed_bytes
peak_live_bytes
live_bytes_at_seal
cross_pool_free_count
```

`allocated_bytes` and `freed_bytes` are additive. The allocator
updates `current_live_bytes = allocated_bytes - freed_bytes` and its high-water
mark on every allocation/free; it does not read a clock. `peak_live_bytes` is
that exact domain high-water mark. `live_bytes_at_seal` must normally be zero;
a non-zero value is retained as an ownership/leak invariant rather than hidden.
At every snapshot, `allocated_bytes = freed_bytes + current_live_bytes` must
hold. Any cross-pool free makes attribution invalid and sets an invariant.

`spill_bytes` is separately additive and comes from the operator spill boundary,
not from MPool or a peak-derived estimate.

Exact allocation/free/live-at-seal values may only come from an isolated
allocator domain backed by one actual MPool. Independent remote pipeline MPools
are such domains and publish their terminal summaries with the fragment.

The local session MPool is request-serialized but not isolated: prepared plans
and other session state may be retained across statements. At request start its
high-water mark is rebased to current live bytes, and index 2 receives the
maximum absolute occupancy observed through response completion. This value is
useful for OOM correlation and remains valid for `PREPARE`, `EXECUTE`, and
`DEALLOCATE`; it is not an incremental allocation claim. The session pool does
not publish fabricated allocation/free/live-at-seal facts.

The MPool high-water update is a real atomic-max CAS loop. `peak_live_bytes`
advances only after an allocation succeeds; a failed capacity reservation does
not appear as live memory. If reservation pressure is useful, it is separately
named `peak_reserved_bytes` and is diagnostic only. The adapter adds no clock
read or second independent accounting vector per allocation.

Starting with non-zero session occupancy is normal. The rebase is safe at the
serialized request boundary and does not reset allocation/free counters.

Pre-response consumers such as `EXPLAIN PHYPLAN ANALYZE` read only the atomic
peak of the same open epoch. The live preview never calls the terminal snapshot
or exposes allocated, freed, cross-pool-free, or live-at-seal fields. It is
never merged into the root; response completion clears it and publishes the
terminal peak observation exactly once. A peak of zero is valid for an empty
session pool with no allocation after the epoch begins.

Peaks are not additive. Attempt and root summaries expose
`max_domain_peak_live_bytes` and
`sum_domain_peak_live_bytes_upper_bound`; neither is named statement peak. A
configurable high-water threshold emits a bounded fixed-size scalar event with
statement, attempt, CN, domain, plan fingerprint, domain counters, and spill.

High-water events use geometric thresholds from one configured starting value,
so one growing domain emits O(log peak) events rather than one event per
allocation. The allocator path only enqueues immutable scalars. It does not
format strings, synchronously log a full MPool report, read RSS/Go heap/cache, or
hand an MPool pointer to an asynchronous consumer. The consumer may add a
best-effort CN snapshot before export. The queue has entry-count, entry-byte, and
total-byte caps.

The event is best effort and may be dropped or lost with the process. A
coordinator-observed CN disappearance and missing-domain flag are reliable; a
pre-OOM high-water event is additional evidence, not a guarantee.

The initial cutover removes the synchronous full-MPool report from the
allocation CAS path and relies on the terminal statement snapshot plus existing
CN memory metrics/profiles. A per-statement pre-OOM scalar event is a follow-up;
until then, a hard process OOM can prevent the statement memory row from being
written. No boolean or fabricated zero hides that limitation.

The persisted `MemorySize` position stores only
`max_domain_peak_live_bytes`. It does not persist an `oom` boolean: a process
OOM may prevent statement finalization entirely, so absence or `false` would be
misleading. A recoverable allocation error and a CN disappearance remain in
error/log/metric signals and correlate with the last high-water event.

The old operator `MemorySize` field is not allowed to keep two meanings.
Operator diagnostics either report separately named `AllocatedBytes` and
`PeakLiveBytes`, when the operator can measure them, or report neither. These
operator fields help explain a plan but never override the execution-MPool peak
projected into StatsArray index 2.

The old mixed `MemorySize`, `memory_byte_ns`, and sampled integrals are not raw
version-6 resource facts. The current CU formula may use peak multiplied by
duration as an explicitly named pricing proxy, but that derived term never
changes or overloads the memory fields.

### 5.4 What is not in the resource summary

Rows, scan bytes, cache details, spill rows, disk diagnostics, and per-node plan
statistics remain in existing operator/plan diagnostics. They do not need to be
duplicated into every statement/fragment accounting object.

## 6. One Source of Truth Per Field

| Field | Authoritative producer |
| --- | --- |
| Operator active/wait | operator analyzer, using only local Call data |
| Parse/plan/compile active/wait | the owning local phase recorder |
| S3 operation and bytes | fileservice request boundary |
| Client egress | protocol writer |
| Allocation/free/peak | isolated MPool accounting adapter |
| Spill bytes | operator spill boundary |
| Statement wall | frontend request lifecycle |
| Attempt wall/outcome | compile attempt driver |
| Remote fragment total | terminal envelope from that fragment |
| Connection type | frontend session metadata |
| Output packet count | protocol writer |
| Partial/missing quality | attempt/root reducer from terminal slot state |
| Derived CU | one post-seal `CalculateCU` call |

Plan marshaling, trace serialization, explain rendering, and any future pricing
consumer read the immutable summary. They never reconstruct or add physical
resources from a second source.

Global service metrics can still be updated by a producer, but they are a
separate sink and never merge back into SQL statement accounting.

## 7. Core Model

### 7.1 Package boundary

The core lives in a dependency-free leaf package:

```text
pkg/util/resource/
  usage.go       enums, Usage, checked merge
  recorder.go    fragment-local recorder and async lease
  attempt.go     fragment slots, state machine, immutable summaries
  harness_test.go
```

It imports only the standard library. Frontend, compile, fileservice,
perfcounter, MPool, remote execution, and motrace drive or adapt this package.
The core never imports them.

### 7.2 Compact usage

The initial resource vector contains only additive quantities:

```go
type Usage struct {
    ExclusiveActiveNS uint64
    WaitNS            [WaitKindCount]uint64

    S3Requests   [S3OpCount]uint64
    S3ReadBytes  uint64
    S3WriteBytes uint64

    ClientEgressBytes uint64
    SpillBytes        uint64
}
```

All `Usage` fields are additive. Allocator memory is emitted once by its domain,
not copied into every fragment delta:

```go
type MemoryDomainSummary struct {
    AllocatedBytes     uint64
    FreedBytes         uint64
    PeakLiveBytes      uint64
    LiveBytesAtSeal    uint64
    CrossPoolFreeCount uint64
}
```

An execution attempt pre-registers one memory-domain slot per remote isolated
MPool before that pool begins attributable work. The local serialized session
epoch belongs directly to the statement root and is published once at response
completion. Remote MPool owners publish exactly once after local users quiesce;
duplicate/stale reports are rejected like fragment terminals, and a missing
remote terminal marks both fragment and memory data partial.
Allocation/free/spill and live-at-seal values sum at the root. Peak is exposed
internally only as max and sum upper bound.
Wall time, outcome, state, and partialness also live in envelopes because they
have different merge rules.

Checked overflow is performed at terminal merge, not on every hot-path update.

### 7.3 Three runtime objects

```text
LocalRecorder  one single-writer producer scope; ordinary integers
AsyncLease     joined asynchronous IO ownership; publishes before close
Delta          immutable fragment terminal snapshot
```

The whole `Usage` is not atomic. Concurrent operators do not share one recorder;
each operator, coordinator phase, or other producer owns a local recorder. The
fragment harvests those local values after quiescence. A genuinely concurrent
joined IO operation updates its own small lease-local counter and publishes
once.

`LocalRecorder` replaces or embeds the relevant fields in the existing operator
analyzer/phase object. It does not allocate a second recorder per operator and
does not register recorders in a statement-wide list. Fragment terminal harvest
walks the analyzer state the pipeline already owns.

The execution context contains one pointer to the current attempt handle. A
producer resolves it once when entering an operator, phase, or request and then
uses its local recorder or lease. Counter updates do not repeat multi-key
`ctx.Value` lookups.

### 7.4 Ownership tree

`ResourceRoot` represents one client or standalone internal request.

`Execution` represents one logical SQL execution. A compound or internal SQL
may contain multiple executions, each with its own final attempt. The root holds
only the sum.

An Execution publishes one immutable summary to its root owner. Concurrent
children do not mutate the root vector directly. The root merges a completed
child and discards the child summary; it does not retain an execution list, so
compound statement size does not create unbounded accounting memory.

An internal execution explicitly selects one mode:

```text
InlineToCurrentAttempt
ChildExecution
StandaloneRoot
```

Context-key presence never guesses resource ownership.

`InlineToCurrentAttempt` is limited to helper work that cannot create an
independent retry lifecycle. A retryable error is returned to the parent and
retries the parent attempt. Any logical child that needs independent retry uses
`ChildExecution`.

`Attempt` represents one dispatched run generation. Parse, initial plan, and
initial compile are root-owned phase facts. Attempt 0 begins at the execution
handoff. Retry cancellation, rollback, and cleanup close the failed attempt;
the next attempt begins before its generation-specific rebuild. A retry rebuild
failure is therefore a real finalized attempt, while a failure to quiesce the
old generation does not create an empty one.

`Fragment` represents one local or remote execution fragment. Fragment details
are discarded after attempt sealing.

The merge algebra follows the actual ownership tree:

```text
AttemptSummary =
    AttemptLocal
  + sum(FragmentDelta)
  + sum(JoinedLeaseDelta)
  + reduce(MemoryDomainSummary)

ExecutionSummary = sum(AttemptSummary)

StatementResourceSummary =
    RootLocal
  + sum(ExecutionSummary)
  + RootMeta
```

`RootMeta` owns statement wall, connection type, output packets, quality flags,
and missing counts. Additive fields sum, memory peak takes max, quality flags use
bitwise OR, missing counts sum, and connection type preserves the group value.

### 7.5 ResourceRoot lifecycle

The root exists from request acceptance through protocol response completion.
It has a small root-local recorder for parse/auth/frontend work that is not part
of an execution attempt.

```text
root open
  -> merge sealed Execution summaries
  -> write success or error response
  -> protocol writer publishes final client egress
  -> root seal and capture statement wall
  -> enqueue resource trace/metric persistence
```

An Execution sealing does not seal the root. A parse error with no Execution and
response work after the last attempt still have valid ownership.

A request-level defer owns root sealing. It runs for success, parse/build error,
response-write error, client disconnect, and panic. Egress records only bytes
actually accepted by the protocol writer. A standalone internal root without a
protocol writer records zero client egress and uses the same seal path. Seal,
summary publication, and persistence enqueue each execute exactly once.

Before response serialization, after all Executions have sealed, the root
produces one immutable `PreResponseSummary` containing RootLocal work so far plus
all Execution summaries. `EXPLAIN ANALYZE` overview reads that snapshot; it does
not wait for final client egress and does not claim to be the post-response root.
The final root seal remains the only input to statement trace and CU.

## 8. Attempt Lifecycle

### 8.1 State machine

```text
open
  register fragments
  register one memory domain per actual isolated execution MPool
  update local recorders
  acquire joined async leases

closing
  reject new fragments and leases
  existing fragments may complete Reset/abort/remote terminal
  memory domains publish only after their local fragments quiesce
  existing joined leases return or detach at the close deadline

sealed
  all fragments are terminal or marked missing
  all memory domains are terminal or marked missing
  no writer can reach Attempt memory
  immutable AttemptSummary is available
```

The compile coordinator is the single owner that transitions and seals an
attempt. It does not require a shared `sync.Once` or a global atomic reducer.
Fragment terminal publication may be concurrent, but each fragment owns one
pre-registered dense slot and publishes at most once.

Memory domains use a second pre-registered dense slot set with the same
generation and terminal rules. A fragment terminal never copies a shared MPool
that it does not own. Only the designated MPool domain owner publishes that
domain summary once; one CN may publish several domain slots.

The synchronization is fixed: Attempt owns one mutex used only on terminal
publication and a dense array of small slot states. Under that mutex it validates
generation/slot, rejects duplicates, merges the immutable Delta immediately,
and marks the slot terminal. Slots do not retain a full Usage/Delta, and hot-path
counter updates never take this mutex.

Slots distinguish `planned`, `dispatched`, `terminal`, and `missing`. A send
failure before dispatch becomes a terminal zero-usage slot, not missing data.
Only a dispatched fragment that loses its terminal envelope makes accounting
partial.

### 8.2 Required order

```text
Begin attempt 0 at execution handoff, or retry attempt before retry build
  -> retry build / compile when applicable, then prepare / execute
  -> remember primary/root error
  -> enter closing; reject new fragments and leases
  -> stop/cancel pipelines
  -> preserve pipeline-tree Reset/terminal/cleanup ordering
  -> quiesce only independent pipeline trees and remote streams concurrently
  -> wait for already joined async leases
  -> consume remote terminal envelopes through one attempt-level deadline
  -> complete rollback workspace, cache cleanup, and retry transition work
  -> publish immutable non-memory deltas from recorders/analyzers
  -> release/reset attempt-local analyzers, counters, and batches
  -> mark disconnected/missing remote fragments or memory domains partial
  -> seal and create immutable summary
  -> merge summary into Execution only
  -> retry or return
```

Non-memory deltas are copied before `Compile.Reset` or `Release` clears their
producer state. Remote isolated memory arrives in the same terminal envelope.
The statement-owned local MPool epoch is snapshotted later by the root after all
attempts and protocol output complete. No producer state is read after seal.

Remote cancel/wait, `RollbackLastStatement`, and cache cleanup belong to the
failed attempt's closing phase. After that attempt seals, the next attempt owns
its rebuild and compile work, including terminal rebuild failures. No retry
transition or build work is left outside an attempt.

Attempt owns an `AttemptLocal` recorder from its boundary through closing. It
contains retry build/compile, prepare, rollback, and other coordinator work that
does not belong to a fragment. Initial parse/plan/compile remain root phases;
a retry build failure with no fragment still has an attributable attempt.

### 8.3 Panic-safe helper

The retry loop must not reproduce finalize logic in many branches. Its outer
panic defer captures the original panic, publishes the open generation's
immutable analyzer and remote state, and rethrows. Statement-owned memory is
not reset per attempt; it is published once at the response boundary. Cleanup
errors cannot overwrite the original panic.

`Seal` and checked merge are non-panicking total functions: invariant failures
produce a partial summary plus flags. The outer retry loop only decides whether
to start another attempt.

### 8.4 Error precedence

Cancellation is often fanout cleanup rather than the root result. Attempt
outcome uses this precedence:

```text
primary execution/build error
> client deadline or cancellation
> peer/fanout cancellation
> cleanup error
```

The attempt saves the first primary error before cancellation propagates.
Cleanup errors are preserved diagnostically but do not overwrite a primary
retryable error.

A necessary transition failure is different from cleanup diagnostics. Failure
of rollback, statement-ID advancement, required cancel/wait, or state reset
vetoes retry and becomes the returned control-flow error. The original retry
cause remains attached for diagnosis but cannot cause another attempt.

### 8.5 Retry summary

The root stores fixed-size data only:

```text
attempt_count
retry_reason_counts[fixed enum]
retry_wall_ns
last_retry_reason
last_error_code
partial
```

It does not append attempts or plans. Memory is O(1) in retry count and O(current
plan fragments) while an attempt is open.

### 8.6 Termination bounds

Retry is never unbounded. Preflight freezes non-zero `max_attempts` and
`max_retry_wall` defaults from observed workloads; the request deadline remains
the hard outer bound when earlier. Retry-budget exhaustion returns the last
retry cause with the bounded summary.

Remote terminal and joined-lease closing use a cancellation-independent cleanup
context:

```text
close context  = context.WithoutCancel(request context)
close deadline = now + accounting_close_grace
```

An already-expired request deadline never collapses the cleanup window. A live,
independent server-shutdown deadline may cap the close deadline; the request
deadline may not. `accounting_close_grace` is non-zero and bounded. Accounting
adds no new wait to local pipeline cleanup: local Abort/Reset must terminate
through the existing pipeline protocol before Compile resources are released.
Remote slots and async lease groups can be detached or marked partial at the
close deadline; local goroutines can never be declared missing and released
while still accessing operator state.

The close grace bounds only accounting-added remote and joined-lease waits. It
does not make the existing local Abort/Reset protocol hard-bounded. A CN-level
budget caps the number and retained bytes of attempts concurrently waiting in
accounting close. When that budget is exhausted, new remote/lease accounting
waits become partial immediately; running local pipelines still follow their
normal safe termination and are never released early. Cancellation-storm tests
measure the real retained Compile, MPool, slot, lease, and tombstone bytes.

## 9. Async Work

Async ownership has one unambiguous rule:

- statement-owned async work must acquire a joined `AsyncLease` before closing;
- closing rejects new leases and waits for existing leases under the same
  attempt-level deadline used for fragment quiescence;
- a joined lease publishes its small delta before release;
- joined operations must implement cancellation and bounded join;
- work that will not join must detach before launch;
- detached work, including its IO requests and completion, belongs entirely to
  the service sink;
- the statement records only synchronous enqueue active time for detached work.

A detached ticket never writes to a finalized attempt. No recorder is kept alive
across retry attempts to accommodate detached background work.

`AsyncLease` is embedded in the existing request and contains only an IO-local
delta. A lazily allocated `LeaseGroup` tracks closing state and outstanding
references; point selects without joined async work allocate no group. Acquire
and Release change the count, individual counter updates do not.

Only the lease owner reads and publishes its local delta. At the close deadline,
the group atomically detaches from Attempt without reading in-flight counters,
marks the attempt partial, and lets it seal. Outstanding leases retain only a
ref-counted tombstone/service sink, never Attempt memory. Late Release publishes
to the service sink or discards the SQL delta, records an invariant, and drops
the tombstone reference. Every joined operation must install cancellation and a
Release defer; failure to release is an operation leak and fails Q1 tests.

Preflight inventories every `AsyncLease.Acquire` call site. Acquire is permitted
only when all success, error, cancellation, and panic paths reach `Release` and
the underlying operation has a hard completion bound. An operation that cannot
prove both properties is detached before launch and charged to the service sink;
it may not retain an attempt tombstone. The merge gate includes fault injection
at every audited acquire site and a zero outstanding-tombstone assertion after
the completion bound. A configured per-attempt joined-lease limit rejects new
joined work before launch, so one attempt cannot create unbounded tombstones.
If the operation is required for SQL correctness, the caller runs it
synchronously or returns an explicit resource-limit error. It may not relabel
already-required statement work as detached merely to bypass the accounting
limit.

## 10. Operator and Pipeline Contract

Accounting follows the existing lifecycle:

- `Call()` processes one batch and records local data;
- `Call()` does not finalize a fragment or send terminal signals;
- `Reset()` performs cleanup and terminal notification;
- abort cleanup must not wait for a receiver that has exited;
- terminal state must still be recorded when a bounded channel is full;
- fragment delta is published only after relevant operator cleanup is complete.

Pipeline/fragment wrapper wall time is diagnostic only. Billable active time is
the sum of every operator's non-overlapping exclusive interval plus coordinator
exclusive phases that provably do not overlap execution. Parent operator child
Call time is removed before summing; Filter, Join, Project, and other non-leaf
operators still contribute their own exclusive work.

## 11. Remote CN Protocol

### 11.1 Atomic terminal envelope

Success, error, cancellation, and abort use one terminal envelope:

```text
outcome
error
attempt_generation
fragment_slot
usage_delta
optional memory_domain_slot
optional memory_domain_summary
partial_flags
```

Usage must not be sent as a second best-effort message after an error. The
client registers the delta before returning the execution error to the retry
loop.

The designated MPool domain owner attaches its memory summary only after every
user of that pool has quiesced. Other fragment terminals carry no copy of that
domain. A CN may therefore publish multiple independent domain slots. On CN
error/cancellation, each owned slot publishes its last valid MPool state or is
marked missing through the same terminal lifecycle.

Existing stream/session identity supplies statement and CN identity. They are
not repeated in every report.

### 11.2 Dense fragment slots

Coordinator registers all expected remote fragments before sending work and
assigns dense slots. Each slot accepts one terminal publication. Attempt
generation rejects a stale report from a previous retry.

This avoids a statement-wide hash map and makes duplicate/out-of-order handling
O(1). Slot storage is released when the attempt is sealed.

### 11.3 Missing reports

Accounting does not add another per-fragment timeout. It reuses the existing
remote lifecycle and attempt-level deadline. Disconnection or lifecycle expiry
marks the pre-registered slot missing:

```text
known resource subtotal is retained
attempt.partial = true
missing_fragment_count increments
```

A dispatched memory domain that loses its terminal summary similarly increments
`missing_memory_domain_count`. No missing resource is guessed.

### 11.4 Payload budget

The terminal delta uses a sparse protobuf representation:

- typical terminal accounting payload <= 128 bytes;
- maximum terminal accounting payload <= 256 bytes;
- payload size does not depend on rows processed;
- no physical Plan JSON is required for accounting.

Full remote physical-plan analysis remains only for explain/diagnostics and is
removed from statement accounting after the new path is authoritative.

## 12. Prepared, Internal, and Compound SQL

Prepared `Compile` objects may survive multiple COM_STMT_EXECUTE calls;
accounting scopes may not.

- each execution installs a fresh scope on the current Process/query context;
- retry-created temporary Compile objects use that attempt scope;
- execution clears the scope before returning;
- cached Compile does not retain the old root, attempt, or recorder;
- PREPARE owns work actually performed by PREPARE;
- EXECUTE owns execution and any recompile it triggers;
- cached plan residence is session/cache resource, not execution-domain memory.

Compound and internal SQL use explicit ownership modes from section 7.4. A
physical event is included in either the child standalone root or its parent,
never both.

## 13. Statement Resource Output

### 13.1 Persisted summary

Only the existing StatsArray projection of the final `ResourceRoot` is persisted
in `system.statement_info.stats`:

```text
index 0   version = 6
index 1   exclusive_active_ns
index 2   max_domain_peak_live_bytes (serialized MemorySize position)
index 3   S3 input/PUT request count
index 4   S3 output/GET+HEAD request count
index 5   client egress bytes
index 6   connection type
index 7   output packet count
index 8   existing derived CU
index 9   S3 LIST request count
index 10  S3 DELETE + DELETE_MULTI request count
index 11  quality_flags
index 12  S3 read bytes
index 13  S3 write bytes
index 14  spill bytes
index 15  total_wait_ns
index 16  attempt_count
```

`quality_flags` is an exactly representable fixed bitset:

```text
PARTIAL
MISSING_FRAGMENT
MISSING_MEMORY_DOMAIN
INVARIANT_FAILURE
PROJECTION_OVERFLOW
CROSS_POOL_FREE
NON_ZERO_LIVE_AT_SEAL
AGGREGATED
```

Flags merge with bitwise OR. S3 bytes, spill, total wait, and attempt count are
additive. This keeps the persisted vector small while making an incomplete row
self-describing and retaining the highest-value resource quantities.

`total_wait_ns` is the sum of classified producer waits and may exceed statement
wall under concurrency. Missing counts, retry wall, the per-kind wait vector,
domain live-at-seal bytes, peak upper bounds, and allocation/free conservation
remain in persisted exec-plan diagnostics and bounded diagnostic events.

The old `GetTimeConsumed` and `GetMemorySize` accessors remain only at the
serialization/derived-consumer boundary. New production code uses
`GetExclusiveActiveNS` and `GetPeakMemoryBytes`; no producer writes indexes
directly.

CU remains a supported derived output. After the `ResourceRoot` seals, one
`StatementResourceSummary` is created. The existing CU formula is evaluated
once over that typed summary and statement duration before float projection:

```text
cpu term     = exclusive_active_ns * CpuPrice
memory term  = max_domain_peak_live_bytes * statement_duration_ns * MemPrice
IO term      = physical S3 request counts * configured prices
traffic term = client_egress_bytes * connection-type price
CU           = (cpu term + memory term + IO term + traffic term) / CUUnit
```

The result is stored in `StatementExport{Summary, DerivedCU}`. The existing
decimal overflow-safe path, price configuration, index 8, `statement_info.cu`,
and statement-CU metric remain. Producers, fragments, attempts, and remote CNs
never calculate or merge CU. CU cannot influence resource ownership,
field semantics, retry decisions, or plan construction.

For v6, `mo_cu(..., 'total')` returns the authoritative DerivedCU in index 8; it
does not rebuild total CU from projected floats. Component or alternate-price
recalculation is defined only for non-aggregated rows. When `AGGREGATED` is set,
`mo_cu(..., 'mem')`, `mo_cu_v1`, or any function requiring the lost per-row
memory-duration basis returns NULL with a diagnostic counter instead of
inventing `max(peak) * sum(duration)`.

The memory term is the current pricing proxy, not a claim that peak multiplied
by wall duration is measured byte-time. Resource correctness is validated on
the raw v6 quantities, while CU correctness is validated by deterministic
application of the configured formula. A later pricing change may replace only
this downstream function.

Because indexes 1 and 2 acquire new meanings, the derived CU distribution will
change. The cutover does not preserve the old numeric result, shadow-run the old
formula, or add a compatibility adjustment.

### 13.2 Clean cutover

There is one StatsArray writer after cutover. It writes the 17-position version
6 array directly; no array/object dual writer or shadow calculation exists.

Before implementation, inventory production consumers of:

```text
StatsArray versions 1-6
mo_cu
mo_cu_v1
observability.cu_v1
statement_info.stats
```

Cutover stops generating versions 1-5 but retains the existing array parser and
column. Historical rows remain readable by their version and are not rewritten.
There is no new persistence schema and no old-array/new-object compatibility
branch.

The persistence contract is deliberately narrow:

- the `stats` column, JSON number representation, first 11 positions, and
  existing protobuf field numbers remain unchanged;
- v6 appends six resource/quality positions; there is no table migration;
- v1-v5 historical rows remain readable and are interpreted with their recorded
  version;
- a homogeneous new cluster produces only v6 rows;
- the cutover drains old aggregation/export queues, so one aggregate never mixes
  semantic versions;
- dashboards and SQL views compare index 1 or 2 across time only after grouping
  or filtering by version.

This avoids schema migration without retaining old accounting code. Numeric
equality with v5 CPU, MemorySize, or CU is not a requirement.

### 13.3 Pricing separation

Version 6 defines resource quantities independently of the current CU formula.
Failed and canceled attempts retain the physical resources already consumed.
Any future pricing version must be a downstream function of the versioned
quantities; a pricing change cannot alter producer ownership or merge rules.

For a single statement, CU is calculated once from its sealed v6 summary and
statement duration. For a short-statement aggregate, already calculated
per-statement CU values are summed; CU is never recomputed from the aggregate's
maximum memory and summed duration. If a summary is partial, CU is calculated
from the known subtotal and `PARTIAL` is persisted on the same row. Detailed
missing counts remain in exec-plan diagnostics. Missing usage is never estimated.

### 13.4 Statement trace aggregation

All statement composition uses the same typed immutable
`StatementResourceSummary`. Short-statement aggregation consumes
`StatementExport{Summary, DerivedCU}` before JSON encoding. This includes
background SQL, authentication SQL, prepared execution, retry, compound
statements, and trace-window aggregation. It never parses `stats` text and never
rebuilds usage from `exec_plan`.

For an aggregated trace row:

```text
exclusive active, S3 counts/bytes, traffic, packets, CU -> sum
spill, total wait, attempt count                        -> sum
MemorySize / max domain peak                            -> max
connection type                                         -> preserve group value
quality flags                                           -> bitwise OR plus AGGREGATED
duration and aggr_count columns                         -> existing aggregation
```

Detailed missing counts, retry wall, and per-kind wait diagnostics sum in the
aggregated exec-plan resource section rather than expanding StatsArray further.

Memory is no longer converted to a duration-weighted average. The v6 array is
encoded once in `FillRow`. The old lazy `ExecPlan2Stats` resource inference is
deleted; typed root projection and the merge rules above replace it.
An aggregation group never mixes StatsArray versions: a version mismatch flushes
the existing group before starting the new one.

`StatsArray` is a serialization DTO, not an accumulator. Its generic `Add`
method is deleted. `GetExecStatsArray`, composite-statement StatsArray handoff,
and similar internal APIs are replaced by typed summary handoff. Consequently,
peak memory has only one `max` implementation, additive resources have only one
checked `sum` implementation, and connection metadata has only one preserve
rule.

### 13.5 Explain, physical-plan, and view consumers

Statement accounting and plan diagnostics share producer facts where useful but
do not aggregate each other:

- final statement `exclusive_active_ns` and peak memory come only from the sealed
  `ResourceRoot`;
- operator and plan-node diagnostics remain in `AnalyzeInfo` and cannot be
  summed to reconstruct statement memory or active time;
- `EXPLAIN ANALYZE` overview reads the immutable pre-response snapshot after all
  Executions seal; node rows show final-attempt operator diagnostics;
- `mo_explain_phy` and physical-plan display stop contributing to statement
  accounting;
- `system.sql_statement_hotspot.memorysize` retains its SQL shape but is
  documented as v6 peak live bytes; historical comparisons partition by stats
  version.

Ordinary `EXPLAIN`, optimizer costing, SQL results, transaction behavior, and
user-table schemas do not consume the resource summary and are unaffected.

### 13.6 Exact facts, bounds, and declared limits

The accounting layer never replaces an unobservable quantity with a
plausible-looking point estimate. It reports an exact scoped fact where one
exists, otherwise it reports a bound, a known subtotal with a partial signal, or
a separate diagnostic event.

The representations and their limits are:

- Active work is `exclusive_active_ns`: non-negative, additive producer-local
  intervals. It is not OS-thread CPU time and may exceed wall time.
- Statement memory is the maximum of exact isolated-MPool peaks and the local
  serialized session-MPool occupancy peak. The latter includes retained
  session state. It is not CN RSS or a synchronized multi-CN total peak.
- `sum_domain_peak_live_bytes_upper_bound` is retained in memory and diagnostic
  events as an additive bound. It is not persisted as `MemorySize` or presented
  as a peak.
- A coalesced S3 request is owned by its merger leader, so every physical request
  is counted once. Follower benefit is represented by merger wait rather than a
  fractional request estimate.
- A partial execution exports the known resource subtotal, persisted quality
  flags, detailed exec-plan diagnostics, and a bounded cause metric/event.
  Missing work is never guessed.
- A hard OOM is reliably represented by CN disappearance/missing-domain when the
  coordinator survives, with any geometric high-water event as best-effort
  additional evidence. A killed CN may never write the final statement row.
- CU applies the current formula to sealed v6 facts and duration. It is
  deterministic for one configuration and input summary, but remains a pricing
  proxy rather than an independently measured physical resource.

Typed summaries retain `uint64` values and CU is calculated before StatsArray
projection. Every integer projected into a `float64` position must be at most
`2^53`. The short-statement reducer flushes before a merge would cross that
limit. If one statement already exceeds it, projection saturates at `2^53`, sets
`PROJECTION_OVERFLOW`, and preserves the exact typed value only in bounded
diagnostics; it never emits a rounded value while claiming exactness.

Diagnostic events carry statement, attempt, CN, domain, and plan fingerprint so
operators can inspect the limiting scope. Dashboards use names and help text that
state these limits; they do not shorten `max_domain_peak_live_bytes` to an
unqualified global "memory peak" or label active time as CPU.

## 14. Code to Delete

Cutover is incomplete until obsolete code is removed:

1. Frontend negative-CPU reconstruction and operator-time fallback.
2. Duplicate resource arithmetic in analyze/show-physical-plan paths.
3. `resetStatsInfoPreRun` resource resets.
4. Retry-time CounterSet reset/reuse used for statement accounting.
5. Resource-related `StatsInfo` fields and methods after producer migration.
6. Implicit multi-CounterSet accounting fanout and obsolete context mark keys.
7. Row-derived S3 request estimates after fileservice coverage is complete.
8. Mixed `Analyzer.Alloc`/`SetMemUsed` writes to one `MemorySize`; independently
   useful operator diagnostics move to separately named fields.
9. StatsArray version 1-5 producer paths and unused conversions; historical
   parsing remains.
10. Remote Plan-based resource aggregation after terminal usage is authoritative.
11. Lazy `ExecPlan2Stats` and old `mergeStats` resource inference after typed
    root projection is installed. `StatementInfo.statsArray` remains the v6
    persistence container.
12. `AggrMemoryTime`, `calculateAggrMemoryBytes`, and duration-weighted memory
    aggregation; version-6 memory aggregation is `max`.
13. Generic `StatsArray.Add` and internal StatsArray handoff APIs; all live
    aggregation uses `StatementResourceSummary.Merge`.
14. Physical-plan or `AnalyzeInfo` formulas used as inputs to statement
    accounting; explain remains a read-only diagnostic consumer.
15. One-shot MPool high-water CAS and synchronous full-report logging on the
    allocator threshold path.
16. Blocking `MOCollector.Collect` use from statement completion; resource trace
    export uses the bounded `TryCollect` contract.

Deleting old tests is not sufficient. Replace them with new semantic tests
before deleting the implementation they covered.

## 15. Performance Design

### 15.1 Hot-path rules

1. Core `Usage` has fixed arrays and no maps or strings.
2. Operator/pipeline counters are single-writer plain integers.
3. No allocation per Start/Stop or counter update.
4. Context recorder is resolved once per operation scope.
5. No full-vector atomics.
6. Async leases are embedded/reused by existing requests, not heap-allocated per
   update.
7. Fragment merge occurs once at terminal state.
8. Dense slots replace fragment hash maps.
9. Retry summaries do not append.
10. StatsArray serialization happens after root seal in the trace exporter,
    outside the response critical path.
11. ResourceRoot is embedded in `StatementInfo`; Execution/Attempt reuse compile
    storage; dense slots reuse scope/fragment storage where possible.
12. The persisted StatsArray has 17 fixed `float64` positions and a pooled
    exporter buffer.
13. CU is calculated from the typed summary before float projection.
14. The allocator threshold path enqueues only fixed-size immutable scalars.

### 15.2 Mechanical gates

Preflight freezes these initial in-memory limits:

```text
Usage          <= 160 bytes
LocalRecorder  <= 160 bytes
Delta          <= 192 bytes
MemoryDomainSummary <= 40 bytes
dense slot     <= 16 bytes
StatsArray JSON <= 512 bytes
```

CI must check:

- `unsafe.Sizeof` limits for Usage, recorder, delta, memory summary, and slot;
- zero allocation for recorder updates and analyzer Start/Stop;
- protobuf size <= 128 bytes typical and <= 256 bytes worst case;
- StatsArray JSON bytes/statement, allocations/op, and encode throughput;
- a hard `MaxStatsArrayJSONBytes = 512` encoder check; overflow records an invariant
  and follows the new bounded `TryCollect` drop path rather than growing the
  buffer or queue;
- reducer benchmarks at 1, 8, and 64 concurrent fragment publishers;
- atomic-max MPool allocation at 1, 8, and 64 concurrent writers;
- non-zero session baseline peak rebase and isolated MPool snapshots;
- AsyncLease acquire/release and terminal protobuf marshal/unmarshal allocs/op;
- end-to-end `EndStatement + CU + group merge + FillRow`;
- queue-full `TryCollect`, high-water crossing, and cancellation-storm retained
  bytes;
- 10,000 retries produce no retained slice/map growth;
- `AllocsPerRun` does not grow with retry count;
- total point-select accounting allocations and bytes/op do not exceed the old
  path.

Statement completion uses a dedicated single-attempt `TryCollect`; it never
enters the current sleep/retry loop while holding the statement mutex. Queue
saturation increments a bounded drop/backpressure metric, returns ownership of
the item and all buffers to the caller for release, and never blocks the next SQL
request. An export drop never causes resource quantities to be recomputed from
another source.

Preflight freezes the trace queue entry capacity and an accounting retained-byte
budget of at most 16 MiB per CN. The bound uses the measured incremental heap
size of the complete queued `StatementInfo`, embedded summary, backing arrays,
and JSON buffer, not only JSON length or `unsafe.Sizeof` headers. Pooled buffers
above `MaxStatsArrayJSONBytes` are discarded instead of retained. CI tests the
real `EndStatement -> ReportStatement -> TryCollect` full-queue, success, and
encode-error paths for item and buffer release.

SQL performance gates run on fixed hardware with repeated `benchstat`, not as a
single noisy unit-test assertion:

- point-select throughput regression <= 1%;
- representative workload p99 regression <= 2%;
- no material regression in scan, join, insert, load, spill, retry, or multi-CN
  workloads.

## 16. Deterministic Harness

The harness uses the production reducer and state machine with an injected
clock. It does not implement a parallel test-only accounting model and does not
use `time.Sleep`.

```text
ManualClock
AttemptScript
FakeFragment
FakeRemoteTerminal
ExpectedSummary
```

### 16.1 Algebra and lifecycle

Every seal verifies these conservation equations with the same production merge
code used by the harness:

```text
ResourceRoot.Usage = RootLocal.Usage + sum(ExecutionSummary.Usage)
Execution.Usage   = sum(AttemptSummary.Usage)
Attempt.Usage     = AttemptLocal.Usage + sum(FragmentDelta.Usage) + sum(JoinedLease.Usage)
expected slots    = terminal slots + missing slots
expected memory domains = terminal memory domains + missing memory domains
```

Known-answer fixtures provide exact values for every term. Property tests vary
merge order, duplication, cancellation boundary, and retry count while
preserving the same expected total.

- exact Usage merge and overflow handling;
- open -> closing -> sealed transitions;
- closing rejects new fragments but accepts registered terminal writers;
- seal is single-owner and cannot execute twice;
- a conservation mismatch sets an invariant failure and fails tests;
- checked active subtraction reports invariant instead of unsigned underflow;
- aggregate phase wait exceeding coordinator wall retains wait, omits that
  phase's active contribution, and does not report a false invariant;
- AttemptSummary merges into Execution once and is never also merged into root;
- no late writer can affect the next attempt;
- serializer performs no resource inference.
- StatementExport calculates CU once from the typed summary before projection;
- quality flags use OR, detailed missing counts sum in diagnostics, and projected
  integers remain exact.

### 16.2 Parallel wait

N fake reads each wait 10 ms while running concurrently for 10 ms:

```text
statement wall = 10 ms
filesystem wait = N * 10 ms
active does not become negative
```

Cover IO merger, joined async IO, and detached service work.

### 16.3 Retry

```text
attempt 0: partial execution -> retryable error
attempt 1: definition change -> different topology
attempt 2: success
```

Assert:

- root total equals all three immutable summaries;
- final plan contains attempt 2 only;
- failed S3/active/wait/memory/traffic facts are retained once;
- build, compile, prepare, execution, cancellation, deadline, and panic paths
  seal exactly once;
- primary retryable error is not overwritten by peer cancellation;
- rollback/cancel/state-transition failure vetoes retry;
- panic in each cleanup step cannot skip seal, merge, or release;
- max-attempt and max-retry-wall exhaustion terminate deterministically;
- 10,000 retries remain O(1) in retained memory.

### 16.4 Remote

- success/error/cancel/abort terminal envelope;
- client records usage before returning error;
- duplicate, stale, and out-of-order terminal messages;
- pre-registered missing fragment sets partial;
- send failure before dispatch produces terminal zero usage, not partial;
- multiple CNs and retry after partial remote work;
- multiple isolated MPool domains on one CN and one shared local domain;
- duplicate, missing, and late memory-domain terminals;
- terminal payload size budget.

### 16.5 Ownership

- external and internal SQL;
- permission/auth child execution;
- compound statement;
- PREPARE, cached EXECUTE, and recompile;
- standalone versus parent-attributed child;
- physical usage appears in exactly one resource root.

### 16.6 Memory

- exclusive versus shared MPool domain rejection;
- exact allocation/free/current/high-water ground truth;
- concurrent atomic-max at 1, 8, and 64 writers;
- failed allocation does not increase peak live;
- zero and non-zero `live_bytes_at_seal` paths;
- allocation/free/live conservation and cross-pool-free rejection;
- retry scope does not reuse a non-zero or old epoch;
- peaks merge as max and explicit sum upper bound, never as a statement peak;
- allocator-error/high-water producer enqueues only a fixed-size scalar snapshot;
- geometric threshold crossing, diagnostic queue saturation, event loss, and
  coordinator missing-domain recovery;
- allocator/update microbenchmarks.

### 16.7 Root and trace output

- success, parse error, response write error, disconnect, and panic seal root;
- protocol writer contributes only successfully accepted bytes;
- standalone internal root has zero client egress;
- short-statement aggregation merges typed summaries exactly once;
- aggregate DerivedCU equals the sum of row CU values; `mo_cu(total)` returns
  index 8 and aggregate component recalculation returns NULL;
- background, authentication, compound, prepared, and retry summaries use the
  same production merge algebra;
- peak memory uses `max` through every composition layer and is never added by a
  generic array operation;
- StatsArray v6 projects active by sum and MemorySize by max;
- partial/invariant flags survive StatsArray projection and aggregation; detailed
  missing counts survive exec-plan diagnostic aggregation;
- projection flushes before `2^53` and flags a single-row overflow;
- a version change flushes the current aggregation group before accepting the
  next summary;
- Explain overview reads the pre-response snapshot and cannot depend on final
  root seal;
- physical-plan and hotspot consumers cannot mutate or reconstruct resource
  truth;
- the real statement completion path uses `TryCollect`; saturation cannot block
  the next request and releases every item/buffer.

### 16.8 Race and failure injection

- race test fragment terminal publication and closing;
- fuzz duplicate/order/state transitions;
- cancel at every lifecycle boundary;
- channel-full and terminal-signal failure;
- disconnected remote and missing report;
- joined lease timeout detaches to tombstone without reading owner counters;
- late lease release cannot reference Attempt memory;
- cleanup remains bounded and idempotent.
- cancellation storm respects CN-level closing-attempt and retained-byte caps.

### 16.9 Q1-Q3 closure

| Question | Created/waiting/growing object | Required terminal or bound |
| --- | --- | --- |
| Q1 destruction | LocalRecorder/analyzer | existing Reset/Release after seal |
| Q1 destruction | isolated execution MPool epoch | quiescent; non-zero live bytes retained as an invariant before destruction |
| Q1 destruction | serialized session MPool peak epoch | terminal peak captured; retained live bytes are normal session state |
| Q1 destruction | LeaseGroup/tombstone | every owner has Release defer; refcount reaches zero |
| Q1 destruction | fragment slots | released when Attempt seals |
| Q1 destruction | pooled JSON buffer | returned by exporter on success/drop/error |
| Q2 termination | retry loop | request deadline, max attempts, or max retry wall |
| Q2 termination | remote/lease close | independent close grace or earlier live server-shutdown deadline |
| Q2 termination | local pipeline | existing Abort/Reset terminal protocol; never released while running |
| Q2 termination | trace export | bounded non-blocking queue policy |
| Q2 termination | memory diagnostic event | bounded non-blocking queue policy |
| Q3 bound | retry summaries | fixed counters; no attempt slice |
| Q3 bound | fragment slots | current plan fragment count |
| Q3 bound | memory-domain slots | actual isolated MPool count for the current attempt |
| Q3 bound | child executions | immutable summary merged then discarded |
| Q3 bound | tombstones | audited bounded operations and configured per-attempt joined-lease limit |
| Q3 bound | closing attempts | CN-level count and retained-byte caps |
| Q3 bound | export queue | fixed entry/byte capacity, 512-byte JSON ceiling, and 16 MiB retained-byte budget |
| Q3 bound | memory events | geometric thresholds plus entry-count, entry-byte, and total-byte caps |

## 17. Implementation Plan

### Preflight: one-week spikes and consumer inventory

- benchmark compact Usage, dense slots, and sparse terminal protobuf;
- prove isolated-MPool ownership and per-pool domain registration, plus the
  request-serialized session-MPool peak boundary and atomic-max high-water;
- inventory every `StatsArray`, `mo_cu*`, `cu_v1`, and statement-CU metric
  consumer; identify aggregate calls that cannot reconstruct component CU;
- inventory every `StatsArray.Add`, `GetExecStatsArray`, composite-statement
  handoff, physical-plan aggregation, hotspot view, dashboard, and alert, then
  classify it as typed producer, typed reducer, projection, or read-only
  consumer; no unclassified path may remain;
- inventory every joined-lease acquire site and detach any operation without a
  guaranteed bounded completion and Release path;
- measure full queued StatementInfo retained bytes and prove a real non-blocking
  `TryCollect` completion path;
- measure current negative CPU, retry, memory-attribution, and performance
  baselines;
- freeze the field contract, memory-domain lifecycle, structure-size limits,
  `max_attempts`, `max_retry_wall`, and `accounting_close_grace` before PR 1;
- verify how the previous binary handles the 17-position version-6 row and that
  rollback can resume version-5 writes without affecting new-cluster correctness.

### PR 1: leaf model and production harness

- add `pkg/util/resource` Usage, recorder, reducer, states, and summaries;
- add ManualClock harness, algebra, lifecycle, fuzz, race, size, and allocation
  tests;
- no production producer switch and no second runtime reducer.

### Integration PR: direct replacement

- create ResourceRoot before initial build-plan;
- isolate every attempt before its build-plan;
- route all exit/panic paths through `executeOneAttempt`;
- publish non-memory deltas before reset/release, then snapshot memory and seal;
- separate root totals from final plan;
- migrate operator and coordinator active/wait;
- migrate fileservice S3 operations/bytes;
- migrate protocol client egress;
- split session/cache memory from exact execution-MPool domains; publish only
  the session occupancy peak and keep alloc/free/live facts isolated;
- fix MPool high-water to successful-allocation atomic-max, remove synchronous
  allocator-threshold reports, and add session-boundary peak rebasing;
- add attempt generation, dense fragment slots, and the atomic remote terminal
  envelope;
- project the sealed root into StatsArray version 6 in existing
  `statement_info.stats`;
- persist the compact six-field extension, calculate CU once before projection,
  and make v6 `mo_cu(total)` read the authoritative index 8;
- replace blocking statement collector submission with bounded `TryCollect` and
  measured retained-byte limits;
- replace background/internal/compound and trace-window StatsArray accumulation
  with the canonical typed summary reducer;
- update Explain overview to the pre-response snapshot; update physical-plan
  diagnostics, hotspot metadata, dashboards, and alerts to the v6 names without
  making them accounting sources;
- remove destructive retry resource reset/reuse and every obsolete path listed
  in section 14 in the same integration change.

This closes the core of issue 18449.
This closes the local core of issue 22263.

The integration PR may contain reviewable commits for lifecycle, producers,
remote protocol, resource output, and deletion, but it has one final runtime
state and is merged as one direct replacement. It contains no old/new selector,
dual writer, dual reducer, or fallback formula.

## 18. Rollout

1. Capture the old release's dev/prod correctness, resource distribution, OOM
   signals, and performance baseline before deploying new code.
2. Prove exact synthetic scenarios with the deterministic production harness.
3. Pass race, fuzz, failure-injection, structure-size, allocation, wire-size,
   and fixed-hardware SQL performance gates.
4. Drain/flush the old statement aggregation and exporter queues, then deploy
   the replacement binary to dev. Every new statement writes version 6 only.
5. Run the complete SQL harness and verify accounting conservation invariants,
   retry/remote completeness, zero negative active values, partial reporting,
   and performance.
6. Deploy a homogeneous new-only canary cluster on internal
   workloads using blue/green or drain-and-switch; all CNs are upgraded before
   the cluster receives traffic.
7. Move one bounded representative workload cohort to a homogeneous new-only
   canary after owner approval.
8. Review at least seven days or an agreed statement volume.
9. Roll out cluster-by-cluster. Each cluster runs exactly one accounting model.

Rollback deploys the previous binary to the whole affected cluster. It does not
select the old model inside the new binary. Version 6 preserves the first 11
positions and appends six compact resource/quality positions. A rehearsal must determine how
the previous binary treats trailing positions before production rollout;
rollback resumes version-5 writes and never activates an old reducer inside the
new binary.

The new terminal envelope requires a homogeneous cluster version. MatrixOne
does not use rolling mixed-CN upgrades and does not add mixed-version accounting
compatibility to this design.

Required bounded metrics:

```text
resource_invariant_total{kind}
resource_attempt_total{outcome}
resource_retry_total{reason}
resource_partial_total{cause}
resource_remote_duplicate_total
resource_projection_overflow_total
resource_export_drop_total{cause}
resource_memory_event_drop_total{cause}
resource_closing_attempts
resource_closing_retained_bytes
```

Statement ID, SQL text, account, fragment ID, and CN UUID belong in sampled
logs/traces, not metric labels.

## 19. Acceptance Criteria

1. New active time cannot become negative by construction.
2. Wait greater than wall is accepted and clearly named.
3. Attempt 0 starts before first build-plan; every retry starts before retry
   build-plan.
4. Every attempt copies non-memory deltas before reset/release, snapshots memory
   after cleanup, and seals once.
5. Failed, canceled, and remote-error resources are retained once.
6. Different retry plan topologies are never merged by node index.
7. Root totals contain every execution/attempt; final plan contains only the
   final top-level attempt.
8. Remote outcome, error, and usage are one terminal envelope.
9. Duplicate/stale fragment or memory-domain reports do not change totals;
   missing dispatched reports set partial.
10. Detached async work never writes into a statement recorder.
11. Prepared Compile objects do not retain an old accounting scope.
12. Each field has one authoritative producer.
13. Retry retained memory is O(1) in attempt count.
14. Hot-path allocation, size, wire, and SQL performance gates pass.
15. Root, Execution, Attempt, fragment-slot, memory-domain, and joined-lease
    conservation equations hold in known-answer, property, retry, and remote
    tests.
16. The existing `stats` column and first 11 StatsArray positions are reused;
    version 6 appends six compact resource/quality positions without a
    table-schema migration.
17. The old negative CPU formula, retry resets, implicit accounting fanout,
    duplicate formulas, and version 1-5 producer paths are deleted.
18. The replacement binary has no dual resource reducer, runtime old/new
    selector, or fallback resource formula.
19. AttemptSummary merges only into Execution; each sealed Execution publishes
    once to ResourceRoot.
20. Checked local subtraction cannot underflow or cast a negative duration to
    `uint64`.
21. Retry and accounting closing have non-zero hard bounds; transition failure
    vetoes retry.
22. Root seal covers response error/disconnect/panic and records actual egress.
23. Trace aggregation merges typed summaries, projects StatsArray v6 once, and
    never rebuilds resources from plan JSON.
24. Canceled requests receive a fresh bounded accounting-close window; an
    expired request deadline cannot force systematic partial summaries.
25. StatsArray JSON, exporter accounting bytes, joined leases, and tombstones have
    explicit mechanical bounds.
26. Memory allocation/free/live-at-seal values come from one domain per actual
    isolated execution MPool. The serialized session MPool contributes only an
    absolute occupancy high-water; successful-allocation high-water uses
    atomic-max, and isolated `live_bytes_at_seal` is zero or an explicit
    invariant.
27. StatsArray v6 persists no `oom` boolean; reliable process-death evidence is
    the CN/missing-domain signal, with high-water events explicitly best effort.
28. `StatementResourceSummary.Merge` is the only live statement/group merge
    algebra; `StatsArray` and `AnalyzeInfo` are never merged to produce resource
    truth.
29. Background, internal, compound, retry, prepared, and short-statement paths
    obey the same sum/max/preserve rules and cannot double-own an event.
30. Historical v1-v5 and new v6 rows remain in the same text column; v6 quality
    flags make partial/invariant rows self-describing, with counts in exec-plan
    diagnostics.
31. Explain, physical-plan, hotspot, CU, dashboard, and alert consumers cannot
    feed values back into statement accounting.
32. CU is calculated once from the sealed v6 summary and statement duration;
    aggregate rows sum per-statement CU instead of recomputing from aggregate
    peak memory and duration.
33. Every persisted quantity is either an exact scoped fact or a named derived
    value. Bounds, partialness, and unobservable scopes are explicitly named and
    never converted into an estimated resource fact.
34. `mo_cu(total)` reads v6 index 8. Aggregate component/alternate-price
    recalculation returns NULL rather than reconstructing lost memory-duration
    information.
35. Every projected integer is exact up to `2^53`; aggregation flushes before
    overflow and a single-row overflow sets `PROJECTION_OVERFLOW`.
36. Explain overview uses the immutable pre-response snapshot; statement trace
    and CU use only the final post-response sealed root.
37. Statement completion uses a real one-attempt `TryCollect`; queue saturation
    cannot sleep/retry while holding statement state and releases all ownership.
38. Wait detail remains observable in persisted exec-plan diagnostics and
    `EXPLAIN ANALYZE`.
39. Index 5 counts actual accepted application-protocol bytes, and index 10
    includes DELETE plus DELETE_MULTI.
40. Cancellation storms obey CN-level closing-attempt and retained-byte caps;
    local pipeline state is never released before safe termination.

### 19.1 Local acceptance workflow

The implementation must deliver reproducible repository targets rather than a
manual-only checklist:

```text
make test-resource-accounting       deterministic unit and integration tests
make test-resource-accounting-race  focused race tests
make bench-resource-accounting      fixed benchmark suite
make bvt-resource-accounting        single-CN and multi-CN SQL cases
```

These targets are added with PR 1 or the integration PR and become required
pre-merge gates. They must use the production recorder, reducer, terminal, CU,
projection, and exporter code; the harness may inject clocks, terminal order,
failures, and queue capacity, but may not carry a second accounting
implementation.

Before direct Go tests that can reach `usearch`, prepare the local CGO
environment:

```sh
make thirdparties
export CGO_ENABLED=1
export CGO_CFLAGS="-I$(pwd)/thirdparties/install/include"
export CGO_LDFLAGS="-L$(pwd)/thirdparties/install/lib -Wl,-rpath,$(pwd)/thirdparties/install/lib"
export LD_LIBRARY_PATH="$(pwd)/thirdparties/install/lib:${LD_LIBRARY_PATH}"
```

On macOS, use `DYLD_LIBRARY_PATH` instead of `LD_LIBRARY_PATH`. Disabling CGO
is not an accepted workaround.

The fast local correctness loop is:

```sh
go test -count=1 -timeout 5m \
  ./pkg/util/resource \
  ./pkg/common/mpool

go test -count=1 -timeout 10m \
  ./pkg/util/trace/impl/motrace/statistic \
  ./pkg/util/trace/impl/motrace

go test -count=1 -timeout 20m \
  ./pkg/sql/compile \
  ./pkg/sql/plan/explain \
  ./pkg/frontend
```

Package paths may be narrowed as the implementation lands, but the target must
retain coverage of the core algebra, MPool adapter, attempt/root lifecycle,
trace export, CU projection, Explain consumer, and frontend sealing path.

The deterministic suite must cover:

- checked active-time subtraction, sum/max/OR merge rules, overflow, duplicate
  terminal publication, stale generations, and out-of-order terminals;
- success, build failure, execution error, cancellation, panic, retry,
  prepared, internal, and compound execution lifecycles;
- failed allocation, cross-pool free, zero/non-zero live-at-seal, epoch reuse,
  and concurrent atomic high-water updates at 1, 8, and 64 writers;
- local and remote fragment success/error, missing dispatched fragments and
  memory domains, send-before-dispatch failure, and close-deadline expiry;
- joined-lease release on success/error/cancel/panic, queue saturation, late
  release, and zero retained tombstones after the completion bound;
- calculate-CU-once behavior, v6 `mo_cu(total)` index 8 behavior, aggregation
  by summing per-statement CU, and NULL for unrecoverable aggregate components;
- the real `EndStatement -> ReportStatement -> TryCollect` path with a full
  collector queue, proving non-blocking completion and ownership release.

Focused concurrency and malformed-sequence tests run separately so a normal
unit invocation cannot hide them:

```sh
go test -race -count=1 -timeout 20m \
  ./pkg/util/resource \
  ./pkg/common/mpool \
  ./pkg/util/trace/impl/motrace

go test ./pkg/util/resource \
  -run=^$ -fuzz=FuzzReducer -fuzztime=60s

go test ./pkg/util/resource \
  -run=^$ -fuzz=FuzzAttemptLifecycle -fuzztime=60s
```

The named fuzz entry points are implementation deliverables. The race and fuzz
runs must finish with no race, panic, underflow, double merge, goroutine leak,
MPool-domain leak, lease leak, or tombstone leak.

The checked-in SQL gate is `pkg/embed/resource_accounting_bvt_test.go`. It runs
real SQL and `EXPLAIN PHYPLAN ANALYZE` against an embedded single-CN service,
then forces a multi-CN scan and requires resource overviews from at least two
distinct CN addresses. `make bvt-resource-accounting` first runs the focused
package gate, then both embedded SQL success cases.

The embedded service does not initialize the asynchronous statement exporter,
so it must not pretend to validate `system.statement_info` persistence. That
boundary is covered deterministically by the frontend completion test, which
executes terminal response accounting and feeds its sealed summary through the
production StatsArray projection, asserting the exact serialized v6 values
(version, active time, peak memory, egress bytes/packets, attempt count,
quality, and non-negative CU).

For optional end-to-end diagnosis of exporter deployment, build and run a
single-process cluster:

```sh
make build
./mo-service -launch ./etc/launch/launch.toml
```

Run the multi-CN success path separately with:

```sh
./mo-service -launch ./etc/launch-multi-cn/launch.toml
```

After asynchronous statement export has completed, a manually tagged query can
inspect the persisted row:

```sql
select statement, duration, stats, cu, aggr_count, exec_plan
from system.statement_info
where statement like '%resource_acceptance%'
order by response_at desc;
```

This manual smoke check is supplementary, not part of the reproducible local
gate. The repository gate is deliberately compositional rather than claiming a
single synthetic end-to-end fixture: resource algebra tests cover terminal
outcomes, partial/missing data and spill; compile tests cover generation-local
retry attribution and remote terminals; trace tests cover projection,
aggregation and collector saturation; frontend tests cover response completion
and protocol failure; embedded SQL covers real single-CN and multi-CN plan
execution. A deployment with the asynchronous exporter enabled remains the
only end-to-end persistence check. Assertions for that smoke check include:

- `json_length(stats) = 17`, index 0 is 6, and every persisted count, byte, and
  duration is non-negative;
- exact known input produces exact active, aggregate wait, S3 byte/request,
  protocol egress, spill, attempt-count, memory-peak, and CU output;
- the memory field is the maximum isolated-domain peak, never cumulative
  allocation, a sample, or a sum of domain peaks;
- missing data retains known subtotals and sets the expected quality flag;
- the stored CU equals the one calculation from the same sealed root summary;
- ordinary `EXPLAIN`, plan construction, serialization, and cache decisions are
  unchanged; `EXPLAIN ANALYZE` alone exposes the renamed resource semantics.

Remote disconnect, missing terminal, terminal reordering, retry transition
failure, allocation failure, panic, and collector saturation are accepted only
through deterministic fault injection. Manually killing a local process may be
used for diagnosis but is not a reproducible acceptance result.

Performance is compared against the merge-base or another recorded clean
baseline using identical binaries, data, host, and benchmark arguments:

```sh
go test -run=^$ \
  -bench='Benchmark(Resource|MPoolAtomicMax|AsyncLease|Terminal|StatementExport)' \
  -benchmem -count=10 \
  ./pkg/util/resource ./pkg/common/mpool ./pkg/util/trace/impl/motrace
```

`benchstat` output must demonstrate the section 15 mechanical gates: point
select throughput regression at most 1%, representative SQL p99 regression at
most 2%, no material scan/join/insert/load/spill/multi-CN regression, and all
object, wire, JSON, exporter-retention, lease, and tombstone bounds. No shadow
or dual-calculation path is enabled for this comparison.

Local sign-off attaches the deterministic, race, fuzz, embedded SQL BVT, and
benchmark results. A representative v6 `statement_info` row is required only
for deployment smoke testing where the asynchronous exporter is enabled. Local
acceptance passes only when all repository suites pass, unexpected
invariant/partial flags remain zero, ordinary query results and plans are
unchanged, and the old accounting path is absent from the built binary.

## 20. Decisions Required Before Implementation or Rollout

1. Does the prototype register every isolated remote domain, rebase the local
   serialized session peak safely, and keep atomic-max overhead within the
   gates?
2. Do all StatsArray/CU consumers use authoritative v6 total CU and reject
   mathematically unrecoverable aggregate component recalculation?
3. Does the real statement completion path remain non-blocking under collector
   saturation and release all retained objects?
4. Does rollback rehearsal characterize the previous binary's handling of the
   17-position v6 row and resume version-5 writes safely?

These decisions block PR 1 field freeze or production rollout, but not review of
the ownership model, retry state machine, active/wait semantics, fileservice
ownership, remote terminal envelope, or harness.

## 21. Decision Log

1. `exclusive_active_ns` is an active-work estimate, not OS CPU.
2. Wall and aggregate waits are independent and are not subtracted globally.
3. A coalesced physical S3 request belongs to its merger leader; followers own
   wait only. This favors exactly-once physical accounting over fractional cost
   allocation.
4. Known resources from a partial statement are projected into StatsArray;
   missing resources are never estimated, quality flags are persisted on the
   same row, and detailed counts remain in exec-plan diagnostics.
5. Stats output remains the same positional array and text column; version 6
   redefines index 1 as active time and index 2 as peak memory.
6. The replacement binary contains one resource reducer and one writer.
7. Cluster upgrade is blue/green or drain-and-switch, not mixed-CN rolling
   compatibility.
8. Attempt/fragment detail is in-memory and bounded; only root summary persists.
9. Version 6 preserves the first 11 positions and appends six compact,
   independently meaningful quantities; no second persistence table/schema is
   introduced.
10. Memory v6 records exact isolated allocator-domain facts plus the serialized
    session-pool occupancy peak for diagnosis. It contains no byte-time
    estimate, peak-duration proxy, price, or charge.
11. CU remains the current deterministic downstream formula over sealed v6
    quantities and duration. It does not participate in producer accounting or
    merge back into the resource summary.
12. `StatementResourceSummary.Merge` is the sole composition algebra.
    `StatsArray` is write-once serialization output, and `AnalyzeInfo` is
    read-only plan diagnostics.
13. The table schema and first 11 positions remain; v6 adds six compact
    resource/quality positions and the terminal protobuf is deployed only on a
    homogeneous cluster.
14. One actual isolated MPool is one memory domain; a CN is not assumed to own
    only one domain.
15. Explain uses a pre-response snapshot, while statement trace and CU use the
    final post-response sealed root.
16. v6 total CU is authoritative in index 8. Aggregate component CU that cannot
    be reconstructed from persisted facts is unavailable rather than estimated.
17. Client egress means actual accepted application-protocol bytes. S3 DELETE
    projection includes DELETE_MULTI.
18. High-water diagnostic events are bounded and best effort; coordinator
    missing-domain evidence is the reliable process-loss signal.

## 22. Alternatives Rejected

### Clamp negative CPU

It hides invalid algebra and undercounts resource use.

### Keep adjusting subtraction terms

No statement-global subtraction works across concurrent workers, CNs, and
attempts.

### Use statement wall as CPU

Wall charges IO/lock/client waits and undercounts parallel work.

### Use process CPU delta

Concurrent statements share the process and goroutines migrate between OS
threads. Attribution would be sampled and diagnostic, not exact active work.

### Preserve both accounting systems indefinitely

It doubles ownership paths, makes future fixes ambiguous, and guarantees drift.
The replacement binary contains only the new reducer. Validation relies on
known-answer tests, conservation invariants, and new-only canaries.

### Add a second resource persistence schema

The existing statement `stats` text field is sufficient for the bounded
version-6 projection. Existing `cu`, duration, status, and aggregation columns
remain unchanged. Attempt/fragment persistence would add cost without improving
diagnosis.

### Persist every retry plan

It creates unbounded trace/memory growth. Root totals plus final plan and bounded
retry summary are sufficient.

## 23. Review Ownership

Approval is required from:

- frontend/session owners for ResourceRoot and prepared/compound semantics;
- SQL compile/execution owners for attempt and pipeline lifecycle;
- fileservice owners for S3 ownership;
- remote execution owners for terminal envelope and dense slots;
- MPool owners for memory attribution;
- observability owners for StatsArray v6 output and historical-version reads;
- observability/product owners for the downstream CU impact of new field
  semantics.

Field names, ownership, and merge rules are frozen before implementation. A
serializer, dashboard, or downstream consumer cannot independently redefine
them.
