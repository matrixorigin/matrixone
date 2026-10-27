# Restricted M0 ADD COLUMN: read boundary proposal R3

Status: **DRAFT; design decision and implementation blocked**. This proposal
belongs to [PR #30816](https://github.com/matrixorigin/matrixone/pull/30816)
and parent issue [#30518](https://github.com/matrixorigin/matrixone/issues/30518).
It examines source `a44bc4b37cc221938dd29da63cf965185c75ab5c`, including
main merge `2465dae7a4ada1b277113cca5afff6fe10149b96`. It does not assert
that the current implementation meets this proposal. No runtime tests,
upgrade qualification, performance measurement, or maintainer approval were
produced for R3.

[R2](20261026-m0-restricted-add.md) and its
[review record](20261026-m0-restricted-add-review.md) remain unchanged. Their
model review predates the maintainer's ObjectIO/redesign objection and is not
approval of R3. This proposal revisits R2's default compatibility decision;
until owners decide, it does not supersede R2 as a decided contract.

## Decision requested

Ask aptend and LeftHandCold to decide whether a restricted, default-disabled
append domain should proceed, where schema-evolution interpretation belongs,
and what historical default-reader contract the project supports. Prefer A
below if its entry points can carry the required semantics. B is an alternative,
not an already accepted fallback. Either option is blocked until the origin
and state-classification obligation in section 3 is closed. Rejection of both
options is an acceptable decision; no implementation or production rollout is
authorized by this draft.

## 1. Current code and responsibility map

| Boundary | Current source behavior | What it establishes / does not establish |
| --- | --- | --- |
| SQL and CN admission | `pkg/sql/plan/m0_hook_mo.go` and `pkg/vm/engine/disttae/m0_hook_mo.go` check the constrained schema, transaction, before/after layout and visible persisted objects before the ADD. | Conditional on the observed starting state and existing transaction protection. Shape and visible objects alone do not document their complete prior DDL history. |
| Transaction and catalog | `pkg/vm/engine/disttae/txn_table.go` records undo, increments the normal table version, queues ALTER and recreates catalog rows within the transaction; `txnimpl/table.go` applies a transactional TN schema. | ADD uses the existing commit/rollback owner. A normal table version is not in itself an exclusive M0 domain marker: the same CN routine increments it for other ALTER requests. |
| Object selection | `ForeachVisibleObjects` for ADD and `ForeachSnapshotObjects` for ranges obtain objects through the target relation and snapshot. | These owners establish the selected membership/visibility; an object name or header alone does not establish table, tenant or snapshot membership. |
| Ranges, filters and statistics | `disttae/txn_table.go` and `stats.go` qualify objects; `colexec/evalExpression.go` consumes a proof width for NULL predicates. COUNT rewrite restrictions are separately enabled in M0. | Interpretation is needed before materializing a column. A reader-only change cannot close pruning and aggregate behavior. |
| Block reader and ObjectIO | `readutil/reader.go` reuses a context/current-object proof or proves the location; `objectio/funcs.go` permits a missing ordinary column only with `CanFillMissingColumn`. | Shared ObjectIO rejection affects default builds too. The proof holds object name and widths, not relation ID, schema identity or snapshot; the caller owns their binding. |
| Writer and merge | CN writer derives a version only for a complete dense prefix; TN merge proves its own inputs and derives a candidate layout from complete schema identities. | Their formulas are guarded physical-layout checks; deriving a version from the current layout does not by itself prove the historical sequence of DDL. |
| Clone and recovery | Clone copies the source layout/version and resets relation IDs; the catalog decoder carries existing nonzero version to TN. Existing transaction/WAL/checkpoint machinery owns recovery. | These are carriers of state, not a durable, independently identified M0 admission record. Existing restart tests are not proof of arbitrary historical input or mixed-version behavior. |

Physical rowid is special and excluded from the ordinary prefix width. The
current object validator checks the header version, object/block widths and
the physical type OID of each expected prefix column. It does not compare the
complete historical SQL type descriptor or prove column lineage. The existing
header contains a version; `M0ObjectSchemaIdentity` notes that the ordinary
writer's table ID can be zero, so the header cannot be treated as a general
historical relation/schema lookup.

## 2. Conditional append invariant

If a trusted root has hidden composite-PK seqnum `k`, set `b = k + 1`.
Assuming every admitted transition appends exactly one nullable, no-backfill
column and preserves earlier identities and types, current version `v` has
width `b + v`; an object version `u <= v` must contain a complete prefix of
width `b + u`. Only `[b + u, b + v)` is eligible for NULL synthesis. The
current validator checks these present-state constraints. Admission also
checks the individual before/after ADD transition and the current visible
objects. These checks are necessary, but they do not independently establish
that all earlier transitions satisfied the assumptions.

| Transition | Observed mechanism | R3 obligation |
| --- | --- | --- |
| CREATE / COPY root | R2 assumes a version-zero dense root with the hidden composite key at the end. COPY is the normal reference path. | Identify how the actual root and its objects become trustworthy, including imported/restored data; a version-zero shape is not provenance. |
| M0 ADD | Planner and CN restrict one trailing nullable ADD; CN verifies the before/after layout and current persisted objects. | Establish the trusted pre-state, transaction ownership and exactly-once committed version change. |
| Other DDL | Planner/CN reject some operations on a recognized versioned append-domain table; ordinary ALTER can advance the table version. | Enumerate all reachable SQL/CN/TN transitions and verify protection remains effective after clone/restart. Equality `version = width - b` alone is not a history proof. |
| Clone | The clone hook copies complete definitions/version and assigns new relation identity. | Establish which relation owns each visible shared object, and preserve the same evolution contract without treating the parent identity as the child identity. |
| INSERT / UPDATE / flush / merge | Writers use complete dense schema or existing fallback; merge qualifies data input separately. | Show that every admitted writer and every object produced under the contract preserves the prefix/identity assumptions. An incomplete writer fallback must not silently authorize an M0 object. |
| Restart / WAL / checkpoint / restore | Normal catalog and transaction persistence carry existing fields. | Show how a reader recovers the same admission/contract decision, including a restored or cloned relation; a successful replay alone is not origin evidence. |

The following is a **design counterexample, not a reproduced production bug**.
Start with an M0 relation with one appended column and an old object missing
that suffix. If a table-definition input's version is inconsistent with its
width, `M0PlanLooksLikeAppendDomain` returns false, and `proveM0ReadObject`
can report "not M0" after layout validation fails. Today the shared ObjectIO
missing-column rejection still blocks an unproved physical read. If ordinary
unconditional NULL filling were restored and this false result were treated as
ordinary authorization, that protection could disappear. This establishes a
required negative input for the new design; it does not establish that this
malformed state is reachable through a supported SQL sequence.

We have not proved either (a) two supported, reachable histories with identical
available metadata requiring different interpretations, or (b) that no such
pair exists. In particular, no supported-release inventory or persisted domain
provenance is established here. A design must provide either a verified source
of the missing distinction, or a narrower enforceable deployment/data boundary.
The mere absence of a new on-disk field is not that distinction.

## 3. Classification and fail-closed contract

| State | Required authority | Consumer behavior |
| --- | --- | --- |
| Ordinary | An existing ordinary-read contract and a trustworthy basis that this input belongs to it. | Preserve the accepted historical behavior; do not fail every unrelated table merely because it has no M0 shape. |
| Restricted M0 | Relation/snapshot membership plus a trusted origin and valid current schema/object checks for this lifecycle. | Interpret only proven absent appended columns as NULL. |
| Unknown / inconsistent | Neither of the above is established, or a required identity/version/layout check fails. | Return an error; never turn a failed M0 identification into ordinary NULL filling. |

These are conceptual states, not a proposed catalog field or public API. A
shape predicate, `version == 0`, lack of a proof, build tag or context value
alone does not decide ordinary versus unknown. A build/deployment restriction
is a valid candidate only if an identified owner can enforce it for every
reader and writer, after restart, restore and clone; current code and tests do
not establish such enforcement. If the data cannot be classified under the
agreed scope, retain the blocker rather than adding a heuristic or weakening
default compatibility.

Physical validation and semantic authorization are distinct. The physical
result describes a particular immutable object location/header/meta and the
validated prefix. Semantic authority additionally binds tenant/relation,
snapshot visibility, target schema/column identities, domain and read attempt.
An object name alone does not carry those bindings. Consumers must reject
stale/mismatched bindings, and release per-attempt state on success, error,
cancel, retry or close. No unbounded or cross-statement proof cache is proposed.

## 4. Alternatives and consumer closure

| Alternative | Physical reads / NULL | Main compatibility and cost question |
| --- | --- | --- |
| Existing COPY | Existing ordinary readers consume fully materialized columns. | Baseline; retains established ADD behavior at the cost of rewriting data. |
| A: engine/reader mapping, preferred for review | Engine proves and maps each logical column to a physical read or an allowed NULL; ObjectIO supplies layout and reads present columns. | Must cover pre-read pruning, statistics, actual execution and TN merge without duplicating decisions or materializing whole tables. |
| B: explicit bounded authorization | Engine validates the same relation/schema/snapshot obligations; ObjectIO accepts a narrowly scoped per-read authorization to fill a proved suffix. | Must make caller/consumer binding and invalidation enforceable, preserve ordinary compatibility and avoid exposing M0 policy as a generic permissive token. |

Both A and B first require the classification contract above; moving the
existing predicate between packages does not provide it. ObjectIO must report
and decode physical facts in either design. Whether it also materializes an
explicitly authorized NULL is the ownership choice for maintainers.

For A, the proposed **internal**, non-persistent read contract is as follows;
it is not a claim that current call signatures already implement it:

1. The relation reader binds tenant/relation identity, snapshot, target schema
   identity and trusted classification to one read attempt. A relation or
   schema change, retry or reopen requires a new binding.
2. For a selected visible object, the engine reads the existing physical
   header/meta and checks the complete required prefix. The resulting physical
   facts bind the exact immutable location and validated object version/width;
   an error ends the attempt, with no ordinary-path fallback.
3. The engine constructs a per-object mapping in requested-column order:
   present columns name their physical seqnum and expected type; only a proven
   nullable appended suffix may be marked all-NULL. Unknown, missing-prefix,
   type-inconsistent, wrong-object or stale-schema mappings are errors.
4. Pruning and statistics consume the same semantic mapping before scanning.
   The scan requests present physical columns and constructs typed NULL vectors
   for allowed absent positions with the correct block row count. Existing
   visibility and tombstone processing then retain their normal ordering and
   ownership. Empty batches and errors must not retain partial vectors.
5. The binding and mapping are released with the reader/attempt. An
   implementation may retain a current-object mapping within that lifecycle,
   bounded by requested columns; this draft does not introduce a cross-object
   or cross-statement cache.

The current reader, pruning, metadata and TN scan interfaces must be checked
against these requirements after the domain authority is resolved. If an
existing entry point cannot independently bind relation/snapshot/schema on its
execution side, R3 does not assume a new wire field or an ambient context
value will solve it. Document the unavailable input and stop that route. B
would use the same binding, mapping semantics and lifetime but delegate the
permitted missing-column synthesis to ObjectIO via an explicit narrow input.

| Consumer | Required decision before implementation |
| --- | --- |
| ranges / filters | Resolve semantic absence before pruning; `IS NULL`, `IS NOT NULL` and other predicates must not discard qualifying objects. |
| statistics / COUNT | Define absent-column metadata and NULL counts; `COUNT(column)` must not be equated to `COUNT(*)` when old objects lack the column. |
| actual scan / UPDATE | Preserve requested column order, vector types, physical and visible row counts and tombstone filtering; no full-table pre-materialization. |
| remote execution | Identify the actual scanning relation/schema/snapshot and re-establish authorization at the receiver; a Go context is not assumed to cross RPC. If existing inputs cannot do so, stop and describe the interface gap. |
| writer / independent TN merge | Produce complete versioned output and establish input visibility/authorization locally. TN merge does not borrow a CN proof. |
| retry / clone / recovery | Re-open the relevant relation/schema generation and re-establish the contract; stale attempt or wrong-relation results are not reusable. |

Proof validation today costs O(sum of object blocks times physical prefix
columns), with header/meta reads, temporary allocation and possible repeated
qualification in ranges, reader, statistics and merge. Reader-local current
object reuse already exists; no cross-scan cache is claimed. A and B must state
their per-object/block work, allocation ownership, lifetimes and I/O; neither
may introduce unbounded retention. No new profile or speedup is claimed.
Compared with COPY, both options still pay this qualification and later DML/
merge work. A additionally constructs and consumes a logical mapping outside
ObjectIO; B retains an explicit authorization check inside ObjectIO. Their
relative allocation, I/O and wall-clock costs are not measured, and cannot be
derived from the shared validation complexity alone.

## 5. Default compatibility and operational boundary

The current default build selects COPY but shared `objectio/funcs.go` rejects
unproved missing normal columns. Its narrowly identified
`mo_catalog.mo_tables.rel_logical_id` exception is not a general proof for
historical user tables. R2 declares other historical missing geometries
unsupported; this proposal asks owners whether that policy is acceptable or
whether ordinary behavior must instead be retained under a reliable separate
contract. Historical geometry is not automatically corruption.

| Input / operation | R3 status and required decision |
| --- | --- |
| Ordinary objects with complete requested columns | Preserve normal reads and ordinary ADD/COPY; existing test evidence is reusable where inputs are unchanged. |
| Supported historical user objects with missing columns | Support set and reliable attribution are **unresolved**. Verify with a provenance-bound historical object or faithful persisted fixture through the real reader, not a synthetic proof marker alone. |
| Older canonical system object lacking `rel_logical_id` | Current code has a narrow DB/table/column/type/object-bound exception. Preserve or replace only under an accepted compatibility decision. |
| Restricted M0 object read by matching participants | Conditional on trusted domain classification and complete proof; candidate evidence is not production qualification. |
| M0 data read by default/old binary or mixed participants | **Unsupported / not qualified** by current evidence. The owner must specify how access is prevented before using that as a safety claim; no automatic enforcement is established here. |
| Backup, restore, clone and downgrade | **Not qualified** across the proposed boundary. Current-table rewrite alone cannot prove that snapshots, clones or backups no longer reference old objects. Define actual inventory, owner and migration/rollback procedure before supporting it. |

If maintainers retain a stricter global rule, treat it as an explicit
compatibility change: supported source inventory, deterministic diagnosis and
an agreed migration/rollback path are required. A sentinel internal error by
itself is not such a path. If ordinary behavior is retained, M0 and unknown
inputs must still be reliably separated before any permissive fill is used.

## 6. Gate, verification and next decision

**BLOCKED: trusted domain origin and fail-closed classification are not yet
proved.** This is an evidence/design gap, not a claim that existing metadata
has been proved fundamentally insufficient or that the current fail-closed
implementation has silently corrupted data. Supply a reproducible supported
history counterexample, or a verifiable authority and transition proof that
distinguishes ordinary, M0 and inconsistent state across the stated lifecycle.
If that requires a new persistent marker or public protocol, stop this scope
and present the interface/migration impact for a separate decision.

After the responsible maintainers accept an exact revised design, implement
only that design and verify: wrong object/schema/domain and malformed-prefix
rejection; real historical default reads and COPY; consistent NULL/predicate/
COUNT/statistics/UPDATE behavior; clone/snapshot/merge isolation; rollback,
cancel, interleave, retry/close and race where affected; writer version and
WAL/checkpoint/restart. The customer SQL BVT must be selected and executed.
Reuse matching evidence only for unchanged semantics; build matching native
artifacts and run scoped Linux qualification when affected. No performance
result, mixed-version support or production readiness follows from this draft.

Maintainer decisions requested: (1) accepted ownership A, B or neither;
(2) authoritative origin/classification and its lifecycle; (3) ordinary
historical-read and upgrade/downgrade policy; (4) any explicit experiment-only
deployment restrictions and the owner who enforces them. Record the accepted
revision and decision before implementation review or code delivery.
