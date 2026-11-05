# Native database Rebase: design v0.2

Status: P0 selected-PK contract recorded; REQUEST_CHANGES for production
implementation until P1 transaction/recovery evidence closes.
This historical v0.2 decision is superseded for minimal R1 by the reviewed
[typed executor revision 1](native_rebase_r1_executor.md). P1 substrate is now
qualified; R2 publication and no-PK replay remain separately gated.
Execution plan: [native_rebase_development_plan.md](native_rebase_development_plan.md).
Frozen dependencies, compatibility findings and P1 probe:
[P0/P1 implementation record](native_rebase_p0_p1.md).
The revised plan governs staging and scope; this document is not design approval.
Owner: this task. Issue/PR: not created. No public SQL is implemented here.
Original local inspected base: dd2b0b38f056ac56671a5d78c9de33b68b06ef07.
Selected implementation base (fresh official main, 2026-10-29):
`b01e952ad1021ef8f188641f3a11a1360c001218`. The original base is not its
ancestor; implementation uses an isolated worktree, not a rebase of local work.
External reference heads (not imported into this worktree):

- https://github.com/zuyu/db-fork/blob/c2737beaf503dd1f3208efc457d369040f0aca40/dblib/matrixone.py
- https://github.com/zuyu/matrixone/blob/7611687f60de63d01aa4ac648a0aefad66f66176/pkg/frontend/data_branch_restore.go

## 1. Scope and evidence

The driver currently clones an upstream generation, reconciles schemas, replays
changes, resolves conflicts, copies history, switches its registry, and drops the
old generation. A native data operation alone does not replace this protocol.
The intended contract is net-state replay, NOT replay of each historical commit.
Do not advertise equivalence to Git commit rewriting.

Verified locally:

- `prepareDataBranchWorker` rejects PICK on fake-primary-key tables.
- `getDataBranchMutationExecutor` and `getLineageOwnerMutationExecutor` own
  transaction/lifecycle admission, including explicit-transaction validation.
- `dataBranchPrimaryKeyColumns` distinguishes explicit and fake primary keys.
- Fake-PK hash diff uses visible columns for matching and hidden keys for some
  ancestor probes. Hidden-key regeneration alone is not proof of a bad diff.
- Database restore from the external fork is absent from this local base.

In the inspected fork, no-PK Reset deletes and reinserts writable visible columns;
the hidden auto-increment key is excluded. The reported data_agent blow-up is a
hypothesis to reproduce, not a proven kernel defect or benchmark result.

## 2. Observable success and non-goals

Success means one server request produces the defined database result, preserves
the source/upstream, and publishes data and ancestry atomically. There must be no
per-row client DELETE loop, no silent fallback, and no partially rebased database.
Subsequent Rebase and Merge must use the new upstream base, not the old ancestor.

V1 targets same-schema, same-tenant base tables with explicit primary keys,
row-level conflicts, and FAIL/KEEP_UPSTREAM/KEEP_BRANCH policies. Initially reject
FK-related tables, schema divergence, external/subscription tables, unsupported
indexes and generated-column cases before mutation unless separately proven.
Initially only ordinary scalar stored columns and literal defaults are selected;
reject non-PK indexes/unique constraints, checks, FKs and generated columns.
Existing constraints cannot be bypassed when applying changes. The exact matrix
in the P0/P1 record narrows this target until runtime qualification.

No-PK support is a separately gated milestone, not an invisible fallback.
Python conflict callbacks and client history rewriting are not V1 server features.
V1 alone therefore does NOT close the reported data_agent case.

## 3. Endpoints and proposed API

Use four immutable endpoints:

- Bf: feature branch's old base.
- Bu: upstream's corresponding old base, with proven logical correspondence to Bf.
- F: feature head to replay.
- U: upstream head to replay onto.

They cannot be collapsed to one timestamp: branches can have different physical
databases and snapshots. Missing ancestry is an error, never an implicit two-way
merge. Resolve names once to tenant/database/table IDs and timestamps; pin all
endpoints against drop/GC for the operation.

Proposed conceptual request (syntax intentionally not registered):

```text
RebaseDatabase(feature_head=F, feature_base=Bf,
               upstream_head=U, upstream_base=Bu,
               destination=new_database, conflict=FAIL)
    -> destination identity, new base identity, summary
```

Use a new destination first. This avoids inventing an unsafe rename/swap of live
databases. The original feature remains intact. A native result must retain the
U base reference before replay so the next delta excludes upstream changes.

The request does not silently update BranchBench's private registry. Until a
transactional driver publication contract is implemented and tested, report it
as a native replay primitive, NOT an end-to-end native adapter Rebase.

## 4. Explicit-primary-key semantics

Each row state is absent or its entire visible typed tuple (NULL included).
For key k let b=Bf[k]=Bu[k], f=F[k], u=U[k].

| Condition | Result |
|---|---|
| f == b | u: feature did not change the row |
| u == b | f: upstream did not change the row |
| f == u | f: identical concurrent change, no conflict |
| otherwise | conflict policy |

Deletion is a state, not a NULL row. Key changes are delete+insert. Different
column edits to the same row conflict in V1; do not claim column-level merging.
FAIL rejects the whole database operation, including earlier tables.
KEEP_UPSTREAM selects u; KEEP_BRANCH selects f, including absence.
Distinct-PK rows violating a unique secondary constraint still fail normally;
conflict preference does not waive database constraints.

The adjacent executable model tests these decisions; Python tuple equality is
NOT the engine's collation/decimal/JSON equality implementation.

## 5. No-PK contract and hidden keys

`__mo_fake_pk_col` is a generated internal key, not physical Rowid. It can help
only when common-origin identity and independent branch allocation are proven.
Never equate same numeric hidden keys across independently inserted rows.
Never treat object IDs/row addresses as durable logical row identity.

Rejected as an adapter-compatible default: the old proposed visible-state
fallback treats tables as multisets. Canonical typed row values
map to multiplicities. A reset changing only hidden IDs has zero visible delta.
Preserve duplicate counts and SQL NULL; hashes require full-value collision checks.

For each visible tuple x, proposed conservative count-level three-way merge uses
the same decision table as PK rows, with nonnegative counts instead of row states.
If both sides change its count differently, FAIL or explicitly prefer a side.
Identical changes coalesce. This is NOT additive event replay: two independent
identical inserts do not necessarily add two rows under this contract.
Frozen-adapter method probes demonstrate this distinction matters: the actual
helper adds independent equal inserts rather than coalescing them, and counts
attempted deletes even when no matching row remains. Those probes do not qualify
SQL DIFF or hidden-key identity. Initial R1 rejects changed no-PK inputs; P3 must
select and validate a compatible contract separately.

Implementation must reconcile multiplicities in server batches, selecting exactly
the required number of physical rows to delete. Do not issue one DELETE per row,
delete all duplicates, or publish an all-table rewrite as an optimized path.
Full scans may be necessary when identity cannot be proved; bound memory and
spill through existing operators. A hash-only equality check is not sufficient.

## 6. Internal execution and ownership

1. Bind and authorize all endpoints and destination; validate ancestry/schema,
   feature restrictions and resource admission before creating public state.
2. Acquire existing lineage-owner lifecycle admission in the operation's owner
   transaction. Lock/validate catalog identity using existing ordering; no new
   process-local lock may substitute for distributed transaction protection.
3. Pin endpoint references and create the destination from fixed U in that transaction.
4. Derive typed delta streams for Bf->F and Bu->U, normalize unchanged tuples,
   resolve conflicts, and apply bounded vector batches to the private result.
5. Validate constraints and persist new base/ancestry in the SAME transaction.
   First test the existing clone edge plus branch-protection snapshot as the
   durable U reference; do not introduce a second base catalog by default.
6. Commit once; only then report success and expose the destination.

The clone, DML, snapshot/base retention and ancestry update must all use the same
transaction operator. Re-entering public SQL handlers that independently commit
is forbidden. Existing PICK is not assumed to provide this four-endpoint contract.

State machine:
`BOUND -> ADMITTED -> PREPARED -> APPLIED -> COMMITTED`, or `ABORTED` from any
precommit state. COMMIT is the linearization point. Explicit BEGIN support stays
disabled for V1 unless destination visibility and rollback are proven end to end.

On error/cancel: cancel producers, drain/join consumers, release batches and
snapshot pins, abort the owner transaction; preserve the primary failure and
surface cleanup failure separately. No successful summary before commit.
Crash before commit must leave no public destination; after commit it must recover
data and base metadata together. Lost acknowledgement is outcome-unknown:
inspect destination identity, never blindly execute or DROP it on retry.

The adapter publishes a generation with a compare-and-swap against its expected
old generation only after native success. CAS failure must not delete a resource
another publisher adopted. Old generation reclamation waits for active readers.
These requirements need an explicit durable publication/ownership design before
calling the complete adapter operation atomic.

## 7. Fast paths and costs

| Path | Required proof | Saved work |
|---|---|---|
| F logically equals Bf | exact delta normalization | no feature replay |
| U equals Bu | endpoint/visible-state proof | no upstream conflict scan |
| table unchanged | logical identity, not file timestamp | skip delta/apply |
| common immutable objects | matching schema and deletion visibility | reuse existing diff pruning |
| no-PK identity preserved | ancestry + stable keys + allocation proof | avoid visible-state scan |

First deliver correct bounded batched execution; add each shortcut only with a
counterexample test. Do not introduce a new global cache, worker or background GC.
Expected explicit-PK work is proportional to collected deltas plus lookup/index
maintenance, but fallback can scan all rows. No unconditional O(delta) claim.
Use the existing query memory budget, cancellation, spill and disk accounting;
reject unsupported bounded-execution paths instead of unbounded maps.

## 8. Compatibility, security and alternatives

V1 must not change existing Merge/PICK semantics. Old servers reject the new
capability; adapters must capability-check before mutation and record native vs
fallback. Never retry through legacy mode after a possibly committed request.
No new catalog/on-disk layout is approved yet: base-retention representation and
restore/backup/mixed-version behavior are design blockers, not deferred details.
Do not store client-specific BranchBench log tables in the generic kernel API.

Require source/snapshot read permissions plus destination create/write and normal
table constraints; preserve tenant/account checks. Source immutability is mandatory.
Do not bypass permissions through the internal BackgroundExec context.

Alternatives:

- Keep driver orchestration: low engine risk, but excessive round trips and
  non-atomic publication; retain as explicitly labeled unsupported-case fallback.
- Wrap existing SQL/PICK calls: saves network overhead but does not establish
  shared transactions, no-PK support, or correct new ancestry; insufficient alone.
- New typed replay coordinator: chosen direction, reusing proven clone/diff/DML
  boundaries; higher initial correctness work, measurable reduction in client work.
- Full Git-style commit replay: different contract, history and conflict cost;
  excluded rather than pretending net-state replay implements it.

## 9. Implementation sequence and validation

1. Freeze target base and close the blockers below. Do not import the whole fork.
2. Add parser/AST/formatter, dispatch, privileges and rejection tests together;
   no externally reachable placeholder that reports success without execution.
3. Add a typed coordinator with one transaction and explicit endpoint owners.
   Prove multi-table rollback, cancellation, concurrency and restart publication.
4. Add PK replay and independent SQL result tests: insert/update/delete,
   equal changes, conflicts, composite keys, NULL, collation, decimal, unique
   constraint failures, empty delta and repeated Rebase then Merge.
5. Add no-PK support only after identity/count semantics approval. Tests include
   duplicate rows, hidden-ID regeneration, null tuples, collision handling,
   simultaneous branch inserts and logically unchanged Reset followed by Rebase.
6. Adapt driver registry/history separately, with crash-before/after-publication,
   stale-generation and concurrent-reader cases. Expose exact supported features.
7. Run tiny matched old/new fixtures first, then W1 Rebase and data_agent.
   Record native SQL count, client round trips, scanned/written rows/bytes,
   operation elapsed, prepare/cleanup separately, and final logical results.
   A faster operation with different data/history semantics is not accepted.

Correctness tests should use tiny data and deterministic fault hooks, not SF100.
Timing thresholds are not unit-test assertions. No speedup target is claimed yet.

## 10. Design review v0.2

Trigger: public SQL/API, transaction ownership, persistent ancestry and recovery.
Decision: REQUEST_CHANGES for production implementation; permit only the
documented P0 contract work and bounded P1 substrate probes at this stage.

Blocking findings:

1. P0 resolved the implementation base and selected zero fork prerequisites for
   its transaction probe. No fork series is approved for production import.
2. Prove clone + base retention + batch replay + ancestry publication can share
   one transaction, including restart and explicit-transaction restrictions.
3. Select a durable base reference representation and its backup/restore,
   deletion/GC, upgrade and mixed-version behavior.
4. Resolve adapter registry/history publication ownership and transaction
   participation; creating a new database is not a complete branch-name swap.
5. Initial R1 explicitly rejects changed no-PK tables. This closes the first
   milestone's choice, not P3 compatibility or hidden-key identity qualification.

The model is not parser, executor, transaction, performance or runtime evidence.
Implementation readiness must not be inferred from its tests passing.
