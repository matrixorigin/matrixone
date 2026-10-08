# Partitioned classic FULLTEXT maintenance contract

Revision: R1, 2026-10-08. Issue #28311 / PR #28477.

Status: **proposal, approval required**. This document describes the local
maintenance candidate based on `bab4b3286a0dd5683a9b291763817722233e586c`.
It is not an approval record. The earlier ODKU-only contract does not authorize
the UPDATE, REPLACE, partition routing, or wire changes described here.

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

Relation caches can retain O(P*H) references per target. Writer identities are
unique per target/physical relation, so the statement envelope is bounded by
the physical relations touched across its targets, not one writer globally.
Existing writer buffering controls still apply per writer. There is no new
aggregate memory cap here; high partition/index counts can multiply buffering.
These are source-derived bounds, **not measured overhead or an OOM proof**.

Re-evaluating a parent partition expression against token rows loses the parent
row layout; writing the logical hidden index loses the physical partition.
Materializing a route fixes those ownership problems but costs payload/copies
and fan-out. Shared parent metadata or a generic plugin-maintenance hook may
reduce duplication, at the cost of changing ownership/API scope. Review must
accept the trade-off or request a revised design before claiming completion.

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

Current local execution (2026-10-08, direct Go1.27, Darwin/arm64): fresh owning
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
