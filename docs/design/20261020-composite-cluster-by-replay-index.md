# Composite CLUSTER BY pruning in CN replay

- Revision: 2 (supersedes revision 1).
- Status: design approved; implementation validation and delivery gates remain.
- Issue: #26996. Implementation: #30617.
- Owners: disttae replay, catalog and reader owners.
- Scope: the complete feature, including schema admission and performance costs.
- Design gate: >500 production lines and a new retained index on replay/read hot
  paths. The BUG label does not exempt this feature from design review.

## Problem and invariant

The reported selective query returns two committed, unflushed rows but visits
100,000 replay rows. Persisted block pruning cannot accelerate that memory path.
The existing rows tree is ordered by physical identity and timestamp; the
fake-primary-key index cannot seek a composite cluster predicate.

`rows` remains the sole MVCC visibility authority. The optional cluster index
supplies candidates, never visibility decisions. Every indexed read must return
the same visible identities as exhaustive iteration plus the residual predicate.
Duplicates, NULL components, deletes, rollback, retained snapshots and persisted
matches must survive. A cluster hit never arms a unique-PK exact-hit shortcut.

Current DDL rejects CLUSTER BY with an explicit primary key. These tables retain
the hidden fake PK; its index and deletion/dedup contracts remain unchanged.
Single-column cluster pruning and persisted-object pruning are outside scope.

Revision 1 inferred declaration from a prefix and VARCHAR type. A legal ordinary
VARCHAR shadow therefore paid for an unused index. It also encoded keys before
checking sticky disablement, enlarged every RowEntry by a slice, and omitted
per-candidate visibility lookups from its cost model. Those are design defects,
not reasons to weaken the performance claims or tests.

## Architecture and authorities

### Schema binding at the serialized consumer boundary

Disttae, not replay, owns catalog lookup. Capture an immutable descriptor at the
serialized logtail dispatcher, after catalog application and before enqueueing
user-table work. It contains account/database/table identity, schema version,
schema effective timestamp, coverage upper bound, exact declared compound
column name and sequence number, and supported encoding/type information.
Replay has no catalog dependency and does not infer declaration from names.

Use the existing name-ordered catalog lookup and verify table ID. Select the
current version applicable to the frame; do not scan every table by ID, create a
second schema cache, or expand the logtail wire protocol. Copy descriptor values;
never enqueue a live mutable TableItem pointer. Capture shares the existing
catalog publication guard with InsertColumns initialization, or occurs entirely
inside its serialized publisher. Missing/incomplete metadata rejects admission.

The ordering proof is the existing protocol, not CatalogCache duration:

1. TN logtailSender serializes incremental publication and subscription phase 2
   against one waterline (`tae/logtail/service/server.go`). A subscription's end
   timestamp is that published waterline; catalog changes precede later user
   snapshots in the same session response order.
2. CN dispatchUpdateResponse synchronously applies mo_database, mo_tables and
   mo_columns before enqueueing user tails. Bind descriptors at this boundary,
   not later in independent table workers, which may lag later catalog frames.
3. Catalog bootstrap/delayed application must be complete. The delayed-apply
   lock/ready condition is authoritative; a duration ending at MaxTs is not an
   applied watermark. No waiting or new background worker is introduced.
4. The descriptor proves one schema interval. Every nonempty batch must have
   commit timestamps at or after its effective timestamp and no later than the
   captured coverage bound. A retained interval crossing a schema change is
   rejected. The matching batch attribute must occur exactly at the declared
   sequence position, have one exact match, and contain a supported vector.

A prefix scan is only a cheap candidate precheck. Plain batches need no catalog
lookup. Once disabled, a state needs neither another schema lookup nor another
cluster-specific scan. A syntactically valid generated-name shadow still fails
without a declared hidden, clustered VARCHAR column in the authoritative schema.
Unrelated prefixed user columns do not themselves invalidate a proven exact
column; ambiguity of the declared column does.

The original freshly-created-table/cold-data-subscription reproduction remains
an acceptance requirement: its catalog creation is applied before data replay.
After restart, a bootstrap catalog snapshot can lack older schema intervals.
Such historical batches fall back exhaustively; automatic pruning recovery in
that case is not promised. Do not fabricate historical coverage or add a naming
exception to make this limitation disappear. Validation must exercise the real
subscription/dispatcher admission boundary, not just a trusted test descriptor.

### One completeness state

| State | Meaning | Transition |
|---|---|---|
| Fresh | No retained insertion escaped admission; tree absent | Compatible nonempty insert -> Active |
| Active | Every retained insert is indexed under one descriptor | Compatible insert maintains it |
| Disabled | Coverage cannot be proved, or state is noData | Drop tree/descriptor; remain Disabled |

Empty batches and object/checkpoint notifications do not disable a Fresh state.
Any nonempty insert that cannot be proved compatible disables the mutable state
before key encoding, allocation or cluster writes. Existing decoding/fake-PK
work remains baseline behavior. Do not retain the old interacting era flags.

The optional tree is allocated only on activation. Copy copies Active tree COW
and preserves descriptor/state. Disabling one mutable copy drops only that
copy's index; previously published snapshots retain their valid tree. Readers
match schema identity/version and column identity before pruning. Unknown,
mismatched, historical or incomplete descriptors retain exhaustive iteration.

### Index and cleanup ownership

Retain a separate nonunique cluster index using existing entry/iterator types.
Reuse common predicate parsing, encoding and interval traversal. Primary-key
unique-hit behavior remains separate. No new RowEntry field or reverse map.

The immutable retained Batch and Offset are the cleanup source. Only states
retaining Batch data build this index; noData states do not. On physical insert
version removal, reconstruct its encoded cluster key using the Active descriptor
and one scratch packer per GC operation. Synchronous B-tree deletion borrows that
buffer; it must not clone cleanup keys or retain the scratch buffer. Unexpected
reconstruction failure disables and discards the current index.

All four rows-removal sites use the common removal owner. Data-object removal
paths remove insertion-index entries. Tombstone-object paths remove delete
versions without trying to recover a cluster column. Deletes never enter the
cluster index. Replay of an existing version clones its RowEntry before changing
Batch/Offset or other fields, preserving published snapshot ownership.

No new state is persisted. Index and descriptor lifetime is bounded by retained
rows and COW snapshots. GC frees index entries with the rows they describe;
disablement drops the current index. Restart/rebuild can safely remain exhaustive.

### Avoid test-only production work

Remove the private advancement/surfaced/validated counters and wrapper calls
added to the shared PK iterator solely for tests. Ordinary PK reads must not
maintain unused telemetry. Preserve range/duplicate correctness tests and public
datasource candidate counts. Use benchmarks for scale/cost claims; do not add a
nil callback check, mutable global flag or parallel counter owner on each step.

## Alternatives and decisions

| Alternative | Decision and reason |
|---|---|
| Exhaustive scan/vector filtering | Correct baseline; cannot give selective retained-row access |
| Batch zone maps | Out-of-order data can span the entire domain in each batch |
| Repurpose fake-PK index | Reject: deletion and PKExistInMemBetween consume its existing domain |
| Independent optional cluster index | Selected; candidates reuse existing rows visibility authority |
| Cluster bytes on every RowEntry | Reject: 112->136 bytes on amd64, even for ordinary/delete versions |
| Reverse cleanup map | Reject: duplicated ownership, COW and maintenance state |
| Batch-derived cleanup | Selected; no global per-version tax, cluster-only GC encoding cost |
| Prefix/regex declaration inference | Reject: user-controlled names cannot establish declaration |
| Lazy mutable read-side build | Reject: another publication/concurrency path and query-time build cost |

No generic index framework, background worker, global cache, wire or disk format
is introduced. Existing residual filtering and workspace/persisted tombstones
remain authoritative. There is no change to authorization or tenant access:
descriptors are bound to account/database/table, never looked up by bare name.

## Cost model and delivery budgets

For N cluster entries, R retained row versions, C surfaced candidates and I
intervals, indexed reads cost O(I log N + C log R), plus traversal/union work.
Each candidate still seeks the rows tree for visibility. A broad predicate can
approach O(N log R), so a candidate-count assertion alone is not a latency proof.

Plain tables pay no new retained bytes or allocation per row version. A one-time
candidate/eligibility check and constant partition state remain. Active cluster
states retain entry bytes, owned encoded-key capacity and B-tree/COW overhead.
Schema lookup/descriptor capture is per eligible frame, never per row. GC adds
cluster-only encoding; key width, version retention and held snapshots matter.

These are acceptance budgets, not measured results:

| Measurement | Acceptance before delivery |
|---|---|
| Ordinary replay | No added retained bytes/allocations per version; <=5% CPU regression |
| Shadows/Disabled | No cluster encoding, retained keys or payload-driven packer growth |
| Active narrow-key replay | <=2x base CPU; wide-key costs reported separately |
| Cluster memory | Account for entries, owned key capacity, tree and retained-snapshot COW |
| Selective read | Exact results; <=25% of exhaustive latency at benchmark scale |
| Broad/duplicate-heavy read | <=25% latency regression; otherwise block and revisit iterator selection |

Compare requested main c7bd2e7c1d9ca53daf80d640b3b307902d54161c with repaired
rebased head, identical fixtures/toolchain/modes. Measure schema admission plus
replay CPU/allocations, retained Go/native memory, GC, selective and broad reads.
Interleave bounded samples and report dispersion; small differences within noise
are not established regressions. Plain-vs-cluster on one implementation is not
a base/head comparison. No zero-cost or measured-speedup claim precedes evidence.

## Verification and rollout

- Reproduce ordinary VARCHAR shadow indexing and sticky-disabled large-key
  encoding through production HandleRowsInsert before correction.
- Extend existing admission fixtures: syntactically valid name shadow, actual
  declaration, wrong identity/version, delayed/newer/absent metadata, sequence
  gaps and malformed vectors. Exercise serialized consumer metadata capture.
- Preserve Copy/idempotence/disable-after-Copy and all four GC removal boundaries,
  snapshot/delete/duplicate/NULL/open-bound differential oracles and PK stopping.
- Keep one real TAE logtail -> consumer -> flush -> mixed persisted/replay test.
  Configure small blocks and use minimum rows to cross them. Keep allocator
  cleanup assertions; original and decoded vectors have distinct owners.
- Reduce functional SQL data to required boundaries. Move 1k/10k/100k seek scale
  and the original 100k workload to benchmarks. Preserve each distinct oracle
  when merging or shrinking test setup; line deletion alone is not improvement.
- Normal/race owning tests, configured incremental SCA and public SQL comparison
  run on final content. SQL comparison reruns on the same instance when changed.
- Validate the complete feature, not only review fixes. Exact-head green CI is
  historical evidence for unchanged contracts, not disputed cost guarantees.

Rollout uses existing PR CI. Unsupported/unproven states fail open, never return
partial results. Removal deletes descriptor admission, optional tree, filter
wiring and its tests/docs together; no migration or persisted compatibility debt.

## Review record

Revision 1: REQUEST_CHANGES (declaration authority, disabled encoding, global
RowEntry growth and incomplete cost model). Revision 2 corrects these decisions
and budgets, and adds serialized metadata capture plus removal of test counters.

Revision 2: GO-design, gpt-6.1-sol / xhigh, session
`01a156e9-7d52-7913-9900-e06502c7c49e`. Exact reviewed content SHA-256:
`fa3ac0adde617b2e1b1801127845f97d52bc4743e49337b454cabdf999e7a503`.
The only post-review edit is this approval/status record. Implementation may
proceed; final test, cost-budget and overall-review gates still block delivery.
