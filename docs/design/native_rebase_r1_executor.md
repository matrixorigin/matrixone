# Native Rebase R1 executor - revision 1

Status: APPROVED_FOR_MINIMAL_R1_IMPLEMENTATION (2026-10-30). Scope is the native replay primitive, not R2
branch-name/history publication. Base: b01e952ad1021ef8f188641f3a11a1360c001218.
P1 substrate and runtime evidence: `native_rebase_validation.json`, committed
in cb28006f08. This review is separate from implementation acceptance.

## Contract

One request supplies immutable feature base Bf, feature head F, corresponding
upstream base Bu, upstream head U, a NEW destination database and conflict
policy. All belong to the caller's account. Existing destination is an error,
not an overwrite or an implicit retry. The default path owns a private
transaction. An explicit BEGIN may borrow an existing pessimistic RC owner;
implicit autocommit-off transactions and unsupported transaction modes are
rejected. Sources are never mutated. FAIL, KEEP_UPSTREAM and KEEP_BRANCH use
the same row decision; user-defined conflict callbacks are not supported.

For each key, absent differs from a present tuple containing NULL. If feature
did not change, retain U; if only feature changed, take F; equal concurrent
changes coalesce; otherwise FAIL or explicitly select the conflict side.
PK changes are delete+insert. Counts distinguish logical changed keys/updates
from physical delete/insert work. No commit history rewriting is implied.

## Endpoint and retention owner

Use typed endpoint records containing account/database identity, immutable TS,
resolved table definitions and the reference that protects that TS. Named
snapshots are resolved and locked using existing snapshot admission, sorted
by account/name, before revalidation. Bare MO_TS is not a substitute for a pin:
accept it only when an existing protected branch edge proves that exact
parent-table/clone_ts reference; lock and validate its kind=branch pin under the
same owner. Arbitrary unretained timestamps are rejected before writes.

Acquire the existing C/G lifecycle admission, sorted database-name domains for
all four endpoints and destination, sorted branch-component roots K and source
table identities, then sorted snapshot-reference keys. Reuse
admitBranchCloneRC/lockNamedDataBranchCloneSnapshot rather than acquiring each
endpoint with a separately committed handler. Re-resolve inventories, schema,
table identities and snapshot references at the admitted frontier. Never acquire
a new earlier lock domain after clone/apply has begun.
Respect existing K -> branch-quota Q ordering: lock/check destination branch
quota before snapshot E keys and target writes, not after acquiring E. A
kind=branch base pin has its own exact identity validator; the named user
snapshot validator intentionally rejects that kind and cannot be reused for it.

For each base/head pair require either the same table ID with base TS <= head
TS, or a protected ancestor path whose fork boundary includes the selected base.
Reject missing/cyclic ancestry; sharing an unrelated LCA is not enough. Prove
that Bf and Bu have equal typed visible state, not merely equal names, checksums
or object IDs. This enables physical base copies without silently choosing an
unrelated two-way merge. The new result clones U and retains U's exact pre-replay
table IDs/TS in existing mo_branch_metadata and kind=branch pins.

## Authorization and pre-write validation

The public request follows normal frontend privilege dispatch. Inside the
executor check destination CREATE and all resolved source/snapshot read
permissions using existing clone authorization, for every endpoint and every
table, before privileged catalog calls. Require all resolved accounts equal
the session account; subscriptions/cross-account routing are rejected.

Preflight every table inventory/schema/category before creating the destination.
Support ordinary explicit PK (including composite PK), nullable stored scalar
values, engine NULL/collation/decimal comparison and literal defaults. Reject
schema/inventory divergence, views/routines/sequences, external/temporary/
partitioned/clustered tables, generated/on-update columns, FKs/checks and
non-PK indexes/unique constraints until their contracts are separately accepted.
Never disable constraints. Logical column/type/default/PK comparison ignores
physical table/column IDs but not SQL semantics.

A no-PK table can be retained without replay only after exact SQL bag equality
proves no visible change on either side and matching bases. GROUP BY visible
typed columns plus multiplicity handles duplicate/NULL values. Otherwise reject
before writes. Do not treat hidden IDs or hash-only equality as row identity.

## One transaction and resource ownership

| Resource | First owner | Release / transfer |
| --- | --- | --- |
| Private pessimistic RC transaction and BackgroundExec | R1 coordinator via getDataBranchComponentExecutor | finish once; error/panic rollback, success commit |
| Explicit pessimistic RC transaction | Caller | R1 borrows the owner; only the caller commits or rolls back |
| Lifecycle process and distributed locks | Existing admission object | finishDataBranchComponent; no alternate transaction |
| Endpoint definitions, identities, protected refs | Coordinator | Until finish; O(table/column metadata), no row map |
| Query pipeline batches/hash tables/spill | Existing SQL planner/executor for each internal statement | Normal query cancellation/cleanup, before next statement |
| Aggregate result | BackgroundExec, copied as a few integer values | ClearExecResultSet/ClearExecResultBatches after each read |
| Client summary | Coordinator, one aggregate row | After private commit, or provisional until caller-owned commit; discard on statement error |

Sequence: authorize/bind -> admit/revalidate -> validate all tables/base states ->
clone fixed U with handleCloneDatabaseWithSource using the NON-NIL owner ->
write clone receipts/retention through that owner -> process tables sequentially
-> commit once -> publish summary. No public clone/snapshot/merge handler chain.
Internal SQL is parsed/planned DML in the same owner transaction, not another
frontend request. P1 does not prove this new coordinator's cancellation path.
Use bh.Exec on trusted, typed lowering output. backExec.ExecStmt is currently
an unimplemented nil-returning seam and must NOT be used as evidence of work.

In a caller-owned transaction, an error can roll back only the failing
statement, leaving earlier caller statements intact when rollback-on-error is
disabled. The caller must explicitly roll back on every failure; it must not
publish a branch or infer transaction rollback from the statement error. A
successful statement result does not prove its surrounding transaction committed.

## Chosen first implementation: relational delta lowering

Use full outer PK joins of Bf/F/U after exact Bf/Bu correspondence validation.
Each side's first non-nullable PK component is its presence marker. Join the
third side on coalesced Bf/F PK components. Use typed <=> comparisons for full
visible-row equality, EXCEPT that every PK identity predicate, including
base correspondence, third-endpoint joins and destination deletes, reuses
dataBranchSQLKeyEqual with the resolved column type. FLOAT/DOUBLE PK identity
uses serial(left)=serial(right), not numeric equality: +0/-0 and NaN payloads
must never collapse. Composite keys apply this per component. Stored PK
components are non-nullable; a zero numeric key remains a present row. SQL
tests must include both signed zeros and distinct NaN storage identities
before advertising float-key support; unsupported literal construction is not
a reason to silently substitute ordinary equality.
The generated relational plan computes feature changes,
upstream changes, conflicts and applied insert/update/delete counts. It returns
only aggregates to Go; never all rows.

For selected feature changes differing from U, delete the affected existing
destination keys using DELETE JOIN, then INSERT SELECT the selected present F
rows with explicit visible column lists. These are bulk engine statements, not
one query per row or a full-table rewrite. Internal RowIDs may change; explicit
PK net state does not. All supported constraints stay enabled. Source snapshots
are fixed across both statements, so the second read cannot observe a moving F.
FAIL detects conflicts before mutating that table; a later table's conflict still
rolls back all earlier work and clone metadata. Empty deltas produce no writes.

The SQL engine owns memory/CPU/spill under existing request/resource policies;
unsupported bounded operators must fail, not fall back to an unbounded Go map.
There is no new worker/cache/temp catalog. Costs can be O(total endpoint rows)
and repeated scans per table, not claimed O(delta). Physical batch/write counts
must remain separate from logical updates. P5 measures this initial implementation
before selecting native Diff pruning or a faster DML path.

Alternatives: (1) existing PICK/diff public handlers do not bind four endpoints
and may own separate transactions/buffers; composing them is not this contract;
(2) a new delta-stream/map/spill framework duplicates engine ownership and risks
unbounded state; (3) chosen relational lowering reuses typed equality, spill,
DML and cancellation, at the explicit cost of more scans. No fast-path claim.

## Public integration and unknown outcomes

The SQL entry point uses four fixed database endpoints:

`DATA BRANCH REBASE DATABASE dst FROM F{snapshot='f'} AGAINST U{snapshot='u'} BETWEEN Bf{snapshot='bf'} AND Bu{snapshot='bu'} [TABLES (t1, ...)] [WHEN CONFLICT FAIL|SKIP|ACCEPT]`.

The optional table list selects which feature changes to replay; omitted tables
are copied from U. Permissions, inventory and retained-base checks still cover
the whole database. Selected tables must satisfy the normal explicit-PK and
schema restrictions. A changed no-PK table is rejected; an unchanged one is
retained only after exact bag comparisons. Omitted ordinary, matching,
non-unique indexes may be cloned, but are not supported for replay. Return
destination and retained-base identities and logical counts; a caller-owned
result remains provisional until that caller commits.

R1 does not update adapter registry/history. A lost commit acknowledgement means
outcome UNKNOWN; inspect the explicitly named destination and its base identity,
never automatically DROP it or retry through legacy SQL. R2 must atomically bind
expected generation/history to the owner or use a separately reviewed publication
protocol; do not add that framework as a prerequisite of R1.

No new catalog or wire state: use existing clone edges/pins and normal transaction
recovery. Shutdown-only upgrade, backup and retention have exactly those existing
representations; no mixed-version serving or invented per-feature RPC gate.

The creation-commit boundary correction used by subsequent Diff/Merge can compare
visible parent and child states at each affected clone edge. This is a shared
path, not a Rebase-only optimization, and can scan whole tables. Its selected
correctness tests do not establish acceptable large-table cost or all lineage
shapes. Treat performance and broader review as separate acceptance gates.

## Acceptance

Test the lowerer's typed NULL/presence/composite-key predicates and SQL parsing.
Then execute the NEW coordinator on tiny real SQL fixtures: feature-only changes,
upstream-only changes, coalesced/conflicting insert/update/delete, literal NULL,
base mismatch, invalid/unauthorized endpoints and unsupported schema/no-PK delta.
Late-table conflict, injected commit failure and cancellation must remove target,
edges/pins; deterministic competing DROP/snapshot deletion must not invalidate
the admitted four endpoints. Inspect actual executor resource cleanup.

Test KEEP_* and a second Rebase after new upstream/feature changes, then Merge.
Restart/crash evidence must bind the coordinator, not be relabeled P1 probe
results. Only after these pass integrate R2 and run matched W1 correctness/cost.
Do not run SF100, full data_agent, Reset/Revert or generic infrastructure sweeps.

## Design-first review

Independent read-only review (Faraday, 2026-10-30) initially blocked on FLOAT
PK identity. Reusing dataBranchSQLKeyEqual in every PK predicate plus explicit
signed-zero/NaN acceptance closed that finding. Re-review approved this revision
for minimal R1 implementation, including K/T -> quota Q -> snapshot E ordering,
one multi-source admission, non-nil clone owner, branch-pin validation and
post-commit result publication. This is not code/runtime/release approval or
approval of an R2 publication protocol. Implementation must preserve these
invariants and supply its own tests, not relabel P1 substrate tests.
