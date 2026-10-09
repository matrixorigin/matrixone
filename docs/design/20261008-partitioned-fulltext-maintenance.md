# Partitioned classic FULLTEXT maintenance contract

Revision: R2 resource clarification, 2026-10-09. Issue #28311 / PR #28477.

Status: **proposal, approval required**. This document describes the local
maintenance candidate based on `23d8f61252f3017dff218b530817a240206fd762`.
It is not an approval record. The earlier ODKU-only contract does not authorize
the UPDATE, REPLACE, partition routing, or wire changes described here.

## Problem, invariants, and decision boundary

The reported #28311 entry is synchronous classic FULLTEXT DML on a partitioned
parent. A posting row has `(doc_id, pos, word, hidden fake primary key)`;
it cannot evaluate a partition expression that refers to the parent columns.
The logical hidden relation also does not identify the physical hidden index
owned by each parent partition. Maintenance must retain that identity before
tokenization changes the row shape. Allocating the hidden key and transporting
the route are separate obligations; neither substitutes for the other.

The candidate must preserve four invariants:

- Every delete addresses the old document and old physical partition; every
  insert addresses the final document and final physical partition.
- Stored posting columns have the existing order/types and a non-null allocated
  hidden key. The execution-only ordinal never becomes a stored column.
- All physical flush records reach the originating transaction before success;
  any failure/cancellation prevents statement success and partial visibility.
- Cleanup finishes before a new execution generation can reuse operator state.

The complete PR crosses planner, allocation, physical-index ownership and remote
protocol boundaries, so the design gate remains applicable despite narrowing
the earlier non-partitioned PK-update expansion. R2 specifies a proposal for
those boundaries; design approval and distributed acceptance remain separate
blocking decisions. It does not introduce another transaction owner.

## Supported and rejected semantics

The candidate routes synchronous classic FULLTEXT maintenance for partitioned
parents through the existing MULTI_UPDATE operator: INSERT/INSERT SELECT,
UPDATE, DELETE, ODKU, and REPLACE. A partitioned document-key or partition-key
change deletes hidden entries using the old row/route and inserts entries
using the final row/new route. Regular hidden indexes must move with that row.
Only the logical parent operation contributes client-visible affected rows;
index-only token writes do not.

Non-partitioned synchronous FULLTEXT primary-key UPDATE remains rejected,
including its existing RETURNING error contract. The partitioned exception
does not relax synchronous vector primary-key UPDATE or RETURNING restrictions.
Existing non-partitioned BVT expectations remain byte-identical to the base.
Standalone CREATE INDEX/ALTER ADD INDEX on partitioned parents, asynchronous
index maintenance, FULLTEXT2, and new transaction semantics are not introduced.

## Ownership and representation

The planner computes the parent partition ordinal before tokenization and
carries it as an execution-only column after the complete stored projection.
PRE_INSERT allocates the hidden fake key while preserving the route; the route
is not stored as a hidden-table column. Old-row deletion and final-row insertion
have separate sources. Late consumers must retain the parent row and old route
through sink remapping and pruning.

`PartitionIndexCtx` carries the parent reference, parent TableDef, and route
column. The target TableDef still describes the logical hidden index. Execution
resolves parent partition metadata in the current transaction and maps that
logical index's parent-owned position to each physical partition's hidden index.
Missing ownership, metadata, or invalid/null route is an error, not a fallback
write to the logical aggregate relation.

Planner deep copies own copied metadata; execution-context clones share the
immutable parent metadata. Grouped batches are local temporary copies released
after each routed target call. The wrapper owns active S3 writers and drained
writers awaiting cleanup. Reset/Free release both sets and invalidate cached
partition relations and writer IDs before reuse. Transaction commit/rollback
and object-storage publication remain owned by existing execution/transaction
components; local source inspection alone does not establish atomicity.

Existing synchronous maintenance and partition-wrapper owners are reused. The
classic FULLTEXT token/document row shape still requires algorithm-specific
planner dispatch; accepting this placement rather than a broader plugin hook
is an explicit design-review question, not an already approved abstraction.

The concrete responsibility split is:

| Boundary | Existing owner | Required inputs and result |
| --- | --- | --- |
| INSERT / INSERT SELECT | `bind_insert.go`, shared FULLTEXT builder in `build_dml_util.go` | Final parent image, document key and route before tokenization |
| UPDATE / ODKU | UPDATE binding and conflict-maintenance planning | Keep old identity separate from sequentially evaluated final values, including generated partition columns |
| REPLACE | `bind_replace.go` conflict-row selection | Delete each matched old identity; insert the replacement's final identity |
| Posting allocation | `PRE_INSERT` | Allocate the stored hidden key and preserve the trailing ordinal |
| Physical resolution | `PartitionMultiUpdate.Prepare` / `resolvePartitionContexts` | Resolve transaction metadata and parent-owned hidden-index position; reject stale/missing ownership |
| Write / flush | Existing raw `MultiUpdate`, writer delegates and `FlushS3Info` | Return all physical table IDs, counts and block metadata to the transaction workspace |
| Publication | Existing originating transaction owner | Commit only after complete remote success; roll back error/cancel paths |

Statement binders own old/conflicting/final parent images because that is where
SQL assignment order and conflict semantics are available. The shared FULLTEXT
builder owns token expansion and hidden-key preparation; the writer owns only
physical resolution and storage. Moving parent-image selection to the writer
would require reconstructing discarded parent columns or duplicating SQL
conflict semantics. The proposal therefore retains necessary binder decisions
and common maintenance construction; a broader plugin API refactor requires a
separate scope decision rather than an implied approval here.

Superseded assumptions are explicit: ordinary synchronous FULLTEXT PK UPDATE is
not newly supported; 102 is not a route capability; additive protobuf fields
are not safe without actual-peer admission; codec reconstruction, `IsRemote`
and local disk-backed S3 writes are not deployed acceptance. No standalone
FULLTEXT DDL, per-index transaction owner, or logical-relation fallback is added.

## Success, failure, cancellation, and reuse

`Prepare -> grouped input -> active physical writers -> flush records -> remote
completion -> originating workspace -> transaction commit` is the success
chain. Producing an object or one flush record is not the commit point.

| Phase | Success transition | Failure/cancel obligation |
| --- | --- | --- |
| Prepare / metadata resolution | Admit valid parent/index mapping | Return error; Free handles partial initialization |
| Group / append | Copy each row once into its selected physical writer | Release temporary groups on every return; caller aborts the statement |
| Sync / flush | Drain every writer and return complete metadata | Return error without publishing a successful partial result; retain drained and active writers for cleanup |
| Remote completion / workspace apply | Existing owner accepts the full result | Stop/cancel producers and reject incomplete work; roll back parent and all index maintenance |
| Reset / Free | Release active and drained writers, maps and cached metadata | Same cleanup after success, partial progress, error or cancel |
| Reuse | Fresh Prepare resolves metadata and creates fresh writer identities | No previous output, buffered rows or canceled context may serve the next generation |

The wrapper does not retry a physical write or make duplicate remote delivery
idempotent. Retry admission remains with the existing statement/transaction
machinery after the failed generation has terminated. A cancellation must reach
the receiving pipeline process context and its filesystem calls; a client
error alone does not prove that chain terminated. Reset/Free release in-process
buffers; they do not promise immediate deletion of already written objects.
Failed-transaction object reclamation stays with the existing storage lifecycle
and must be checked separately from SQL invisibility. Restart/rollback reuse
existing persisted block and catalog formats, not a new posting format.

## Cumulative compatibility

Base main owns MORPC 101-106, including vector-cache control at 102. The new
route capability must not reuse 102. This candidate uses **proposed 107** for
route-preserving PRE_INSERT and partition-index MULTI_UPDATE. It is neither an
exclusive reservation nor an approved protocol allocation; integration must
recheck the target registry and coordinate the final allocation.

New fields are additive: plan UpdateCtx.partition_index_ctx=20,
plan PreInsertCtx.preserve_input=15, pipeline PreInsert.preserve_input=19.
Main's existing fields and protocol meanings are retained. The existing
MultiUpdate wire operator is used; decoding restores its partition wrapper.
Sender conversion and recursive remote-scope validation must reject routed
payloads below the new capability. Actual predecessor 106 and historical
102-105 are negative controls, while ordinary payloads retain their prior
admission. Correct mixed-version refusal is required; silently dropping route
metadata is not a compatibility strategy. The sender checks both route fields
through the complete lowered child tree and probes the actual destination on
each send. Expression floors and this routing floor share one per-send
observation; unknown capabilities and failed probes reject the send. Receiver
checks remain independent because the endpoint can change after admission.

## Resource envelope and alternatives

Let P be parent partitions, I be routed contexts, C be input vectors, B be
input/token rows, T be serialized parent-definition bytes, and H be hidden
indexes per partition. Parent metadata is repeated per context: O(I*T) payload
and deep-copy cost. Per-target grouping visits B rows and copies all C vectors
once across at most min(P,B) batches: O(P) group pointers, O(B*C) element-copy
work plus variable payload bytes. Targets are processed sequentially; grouping is not a second
copy per partition of every row.

Relation caches can retain O(P*H) references per target. Writer identity is
scoped by target, physical relation, and action/phase: `s3WriterAction` derives
delete, insert, or combined-update behavior from the captured contexts, and
`writerID` includes that action in the target's key. A same-partition update
can therefore retain separate delete and insert delegates; physical relation
count alone is not the delegate count. With A distinct actions per physical
relation, the per-target envelope includes O(A*P) delegates and their per-index
resources, not one writer globally. Active and drained delegates remain owned
until Reset/Free; count both when measuring the live-writer high water.

There is existing shared admission, not only independent writer thresholds.
Each delegate obtains the service's `CNMemoryThrottler`. After appending, it
accounts input-vector bytes selected by `checkSizeCols`; reaching its flush
threshold invokes `sortAndSync`. Below that threshold, a failed shared
`Acquire(increment)` also invokes `sortAndSync` rather than retaining the batch
for a later call. Successful grants accumulate in the delegate and are released
by its cleanup path. See [writer identity](../../pkg/sql/colexec/multi_update/multi_update_partition.go)
and [delegate admission/cleanup](../../pkg/sql/colexec/multi_update/s3writer_delegate.go).

This proposal adds no separate statement-wide budget for metadata, grouping,
all delegates, and their other allocations. The existing shared throttler and
flush behavior do not establish a hard bound on that complete footprint;
fan-out, copies, allocations before admission, and concurrent CN consumers
still require measurement. These are source-derived bounds and accounting
paths, **not measured overhead or an OOM proof**.

Re-evaluating a parent partition expression against token rows loses the parent
row layout; writing the logical hidden index loses the physical partition.
Materializing a route fixes those ownership problems but costs payload/copies
and fan-out. Shared parent metadata or a generic plugin-maintenance hook may
reduce duplication, at the cost of changing ownership/API scope. Review must
accept the trade-off or request a revised design before claiming completion.

The alternatives to compare during design review are:

| Alternative | Correctness / compatibility | Cost and operational consequence |
| --- | --- | --- |
| Status quo: logical hidden writer / no parent route | Cannot identify the required physical target after tokenization | No new route payload, but cannot satisfy partitioned maintenance |
| Materialized ordinal plus existing partition writer (proposal) | Parent image determines route; existing storage/transaction owners remain | Metadata duplication, full-column grouping and writer fan-out; peer gate and topology acceptance required |
| Shared parent-metadata reference plus narrower grouped projection | Can retain identity with less repeated payload/copying | New lifetime/reference contract and remapping surface; compare measured costs before expanding this repair |
| Generic plugin maintenance interface carrying old/final parent images | Could consolidate algorithm-specific binder decisions | Wider planner/plugin API and lifecycle change; requires its own approved contract and regression surface |

Protobuf's additive decoding is an interoperability constraint, not evidence
that an older implementation understands new route fields. The cumulative
MORPC gate is the repository's admission contract. Existing SQL transaction
atomicity is the required user-visible contract. No new external protocol or
compatibility guarantee is asserted by this proposal.

Design review must accept the existing ownership, per-writer thresholds and
shared CN admission against a measured deployment envelope, or request a
specifically justified change. Lack of a new statement budget is not absence
of shared admission and does not by itself require a new limiter. Measure P/I/H
and action/phase fan-out, actual serialized T, grouping allocations/copy cost,
total grouped vector bytes, live active/drained writer high-water memory, and
throughput/latency with equivalent controls and variation. Record the shared
throttler configuration and other CN load; asymptotic bounds cannot settle the
trade-off. Token expansion changes B relative to parent-row count, so report
both cardinalities. No measurement or reviewer acceptance is asserted here.

## Acceptance still required

1. Explicit review of this versioned scope, field ownership, cumulative
   capability, and resource trade-offs by the blocking design reviewers.
2. Execute focused planner/operator/codec tests on this candidate and exact
   base, including 106 refusal / proposed-107 acceptance, unchanged ordinary
   admission, partitioned key moves, and non-partitioned/vector rejection.
3. Execute the canonical partition FULLTEXT BVT plus unchanged ordinary
   FULLTEXT PK-update/consistency controls; record SHA, environment and results.
4. In a real multi-CN deployment, prove the producer selects WriteS3 and the
   receiver routes physical hidden IDs through object-storage flush. Cover
   failure and cancellation after partial physical progress, transaction
   rollback/no visible partial parent-or-index changes, cleanup, and immediate
   subsequent-statement reuse. Mocks, codec-only tests, and a local writer
   test cannot satisfy this requirement.
5. Measure parent payload bytes, grouping allocations/copy cost, writer peak
   resources and throughput/latency against an equivalent workload/control,
   with partition/index fan-out, versions, raw results and variation recorded.

### Focused acceptance protocol for an allocated deployment

Reuse the existing multi-CN/object-storage fixture after allocation. Pin the
candidate SHA, actual main/predecessor binaries, capability registry, storage
backend, CN identities, run ownership and cleanup scope. Freeze any experiment
wrapper before execution. Stop if remote WriteS3 selection or physical-writer
progress cannot be observed; increasing SQL volume or setting `IsRemote=true`
does not prove either condition.

Use one inline classic FULLTEXT table with two partitions and a minimal row
whose partition key moves A -> B while its text/doc identity changes. Retain
the canonical BVT's supported SQL forms. Record logical-to-physical hidden IDs
and the production submitted/receiving operator path. Capture each physical
flush record and its row/block/object counts, plus coordinator consumption;
correlate all records to the same statement/transaction.

| Scenario | Required terminal oracle |
| --- | --- |
| Successful remote move and local control | Actual remote WriteS3 with all physical flush records returned; an independent connection observes final parent rows and matching MATCH document IDs, with old postings absent |
| Failure after one completed physical write | Observe the completed write before injecting failure into a later write/completion; no successful partial result; independent parent and MATCH reads retain pre-statement state after rollback |
| Cancellation after the same progress barrier | Cancel the originating query through its production path; prove receiver termination and writer cleanup on every participating CN, then the same rollback oracle |
| Immediate subsequent statement / reuse | Same admitted topology succeeds with fresh data; no stale output, receiver, writer or route mapping survives |
| Actual predecessor / ordinary control | Routed payload rejected by the actual old destination before submission; an ordinary compatible payload retains admission |

Use deterministic existing injection/observation seams. If the fixture cannot
inject after physical progress or report receiver cleanup, that missing seam
is a blocker to acceptance, not permission to infer it from client results.
An explicit transaction must be ended on every path. Preserve exact SQL,
independent-connection results, query/transaction IDs, path evidence, object
records, cancellation outcome and cleanup/reuse checks as private run evidence.

Compare payload bytes, grouping allocations and writer peaks at identical
parent/token cardinality with low and higher P/I fan-out. Compare throughput
and latency only on an allocated stable environment with the same workload,
topology, storage and warmup. Report variation and configured resource budget;
neither a capacity smoke nor one latency sample proves acceptable overhead.
The blocking reviewers must decide the acceptable cost envelope before rollout.

### Component evidence and its limit

`TestPartitionIndexWriteS3CallFailureCancelAndReuse` extends the existing
`partition_s3_flush_test.go` fixture. It drives the production constructor,
Prepare and Call/WriteS3/Sync chain with three pre-tokenized posting rows, two
physical partitions and one index. The stored column order/types follow
`pkg/fulltext/plugin/plan/schema.go`; preallocated fake keys intentionally
start after PRE_INSERT. It reads real object columns back through the returned
flush metadata, checks physical IDs/counts and all stored values, injects error
or process-context cancellation on the second filesystem Write after one
completed write, then checks Reset resource release and a fresh successful
generation. There are no sleeps, external services, cluster processes or
large-data triggers.

Catalog/partition lookup and the child are fixtures; the S3 file service uses
its disk backend. This is real local storage/operator-boundary coverage, not
receiving-CN, frontend cancellation, old/new posting deletion, transaction
visibility, object-GC or multi-CN acceptance. The new test's execution result
belongs in the maintenance evidence receipt, not an unverified PASS here.

## Rollout and unresolved decisions

Before landing, integration owns a fresh cumulative-capability check and final
allocation; no reservation is created by this document. During mixed versions,
reject routed work to an incapable/unknown destination and retain the existing
ordinary admission path. Rollout requires approved design, current-head CI and
the focused topology/failure/cost acceptance above. Downgrade must drain active
routed work; additive fields must not be silently dropped to bypass the gate.
Existing persisted posting schemas need no new migration in this proposal.

Use existing query/transaction diagnostics and test-owned observers; no new
per-row logging or public metric labels are proposed. Access to physical hidden
relations remains under the originating tenant transaction and existing engine
checks; supplied parent/table metadata is not independent authorization.
Invalid or stale metadata fails closed. Large partition/index fan-out remains
a capacity risk until its memory envelope is accepted.

| Open decision | Owner / decision point | Status |
| --- | --- | --- |
| R2 ownership/placement/supported scope | Blocking design reviewers, before implementation approval | Approval required |
| Final cumulative route capability | Integrator and protocol owners, against landing main | Proposed 107 only |
| Multi-CN WriteS3 failure/cancel/visibility/reuse | Allocated deployment validation owner, before acceptance | NOT_RUN |
| Payload/copy/writer budget and comparative cost | Design reviewers and validation owner, before rollout | Unmeasured / unaccepted |

Previously reported local execution at semantic candidate `9b89e663d549d93233cfd48a9c265eb8a844396f`
(2026-10-08, direct Go1.27, Darwin/arm64): fresh owning
native/service build PASS. Six owning packages pass 16,771 tests/subtests.
Sender admission regressions fail with the pre-fix destination implementation
and pass after repair; 61 focused codec/expression tests pass. Canonical BVTs
fulltext_partition (124), fulltext_update_pk (21), fulltext_update_consistency
(71), returning (64), and partition3 (105) each pass twice, with zero failed,
ignored, or abnormal statements. Inputs/results are byte-identical to the
current canonical files; ordinary FULLTEXT controls retain main's semantics.
These are current single-CN results, not distributed/old-binary acceptance.
Focused PRE_INSERT route preservation and partition target/failure/reset/S3
writer/free race validation passes 16 tests/subtests, with no detected race.

Explicit design acceptance, final capability allocation, actual multi-CN/S3
transaction/cleanup acceptance, and measured cost items remain NOT_RUN or
unaccepted. No previous-head execution receipt is promoted to this candidate.
