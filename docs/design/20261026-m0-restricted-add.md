# Restricted M0 ADD COLUMN: design R2

Revision: R2, 2026-10-26. This frozen proposal's separate decision is recorded
in [the design review record](20261026-m0-restricted-add-review.md); its proposal
text alone is not approval.
Owner: [PR #30816](https://github.com/matrixorigin/matrixone/pull/30816), a
partial follow-up to [#30518](https://github.com/matrixorigin/matrixone/issues/30518).
Implementation reference: `4c9092203a49f5bba5fcd8e1d2f2a8c6bd0c4982`;
kernel content: `b3c86e3f3375f6441d3326d08ac23ac6598d4d1d`.

This document starts a separate design-approval phase in response to
[review 5632814494](https://github.com/matrixorigin/matrixone/pull/30816#pullrequestreview-5632814494).
Implementation already exists; this is not a claim of historical design-first
approval. Implementation approval remains blocked until this design phase
passes. The record must identify the immutable design revision, reviewer and
decision. Earlier `PASS_CODE_REVIEW`, CI and feasibility are not design approval.

The gate applies because the complete feature crosses planner, CN, TN,
ObjectIO and recovery owners, changes shared default behavior and affects
persistent reads and hot paths. [M0_FEASIBILITY.md](../../M0_FEASIBILITY.md)
preserves the experiment; [M0_STATUS.md](../../M0_STATUS.md) records provenance.

## 1. Problem, goals and alternatives

The software_dev workload clones persisted customer data and independently
adds two nullable columns before UPDATE and evaluation. Ordinary ADD uses COPY,
rewriting old payload even though the added values are all SQL NULL. Avoiding
that rewrite is useful only if old/new objects retain unambiguous identities,
NULL semantics, transaction visibility and recovery. Physical absence cannot
distinguish a legitimate added column from a corrupt or unrelated object.

The goal is a default-disabled same-version experiment. Committed eligible ADDs
must preserve old object identity/payload, supported reads/writes/clone/merge/
recovery must agree with COPY, and failure must preserve the old schema.
Performance improvement is a measurement, not a design axiom.

This changes MO's physical implementation, not SQL's logical nullable-column
meaning. Existing MO COPY, transaction, snapshot and object-format contracts
are the interoperability baseline; no new external protocol/standard is added.
Other systems' instant-DDL behavior does not prove MO lineage or NULL safety.

| Alternative | Correctness, compatibility and cost | Decision |
| --- | --- | --- |
| Existing COPY | Rewrites all rows and materializes schema; broad supported DDL | Default and independent reference oracle |
| Per-object historical-schema/lineage lookup | Broader evolution; needs trustworthy relation identity, retained historical schemas and clone lineage; existing writer table IDs may be zero | Reject here; never invent provenance from zero IDs |
| New persisted per-object schema map | Explicit lineage, but new format, upgrade, recovery and GC obligations | Defer as a substantially wider project |
| Restricted append invariant, re-proved by consumers | No new format, narrower schemas and conservative metadata cost; all producers/transitions must preserve identity | Select only under `mo_m0` |

Non-goals: production enablement, arbitrary evolution, mixed-version/multi-CN
qualification, secondary indexes/FKs/checks/partitions, generated/auto-increment
or backfill transitions, P1/P2, proof caching, new merge policies and object-level
UPDATE replacement. #30958's DEFAULT false extension is independent, not admitted.

## 2. Admission and inductive invariant

Let `k` be the stable ordinary seqnum of the hidden composite PK, `b = k + 1`,
and `v` the append version. Physical rowid is special and excluded from ordinary
object width. The root has version zero, dense seqnums `0..b-1`, hidden
composite PK last among the root ordinary columns, one sort key and rowid last
in the logical definition.

Each independent trailing ADD appends one nullable/no-backfill column,
preserves all existing identities/types/flags and increments version and
`NextColSeqnum` once. Thus `NextColSeqnum = b + v`. An object written at
version `u <= v` must contain the complete typed prefix `0..b+u-1` in **every
block**. Only seqnums in `[b+u, b+v)` may become SQL NULL. Future versions,
prefix holes, wrong types, inconsistent widths and malformed metadata are
errors, not optimization fallback.

Responsibilities are layered, not delegated to a boolean marker:

- Planner admits one trailing column in independent pessimistic RC autocommit,
  rechecking nullable/no-backfill semantics after binding. Explicit transactions,
  non-NULL defaults, ENUM/SET, column comments and unsupported attributes do not
  gain M0 admission.
- CN `buildM0CNAddColumn` rechecks mode, layout, constraints, identities, next
  seqnum and exact before/after transition; refuses snapshot/created-in-txn
  relations and uncommitted data objects; proves every current persisted object
  before mutation. Tables are non-temporary and have no secondary indexes,
  FKs/referring children, checks or partition semantics.
- Unsupported planner selection can retain COPY before mutation. An appended
  domain cannot silently take arbitrary in-place changes; executor/CN guards
  reject them. There is no half-published fallback.
- Clone deep-copies the complete layout/version and resets relation identity;
  ColId/seqnum/hidden-key positions are not regenerated from public ordinals.
  The real catalog create decoder carries nonzero `rel_version` as `VersionDef`
  through the existing CN/protobuf/TN path.

## 3. Commit, rollback and concurrency ownership

M0 reuses existing DDL protection and transaction machinery. There is no second
commit protocol or background DDL worker.

| Phase / first owner | Protected state | Terminal action |
| --- | --- | --- |
| Compiler | Existing catalog metadata exclusive lock and origin-table lock; existing RC definition-change retry | Admission/mutation follow protection; lock/definition conflicts return through normal transaction handling |
| CN preparation | Current relation's visible object inventory and private cloned definitions, including rowid descriptor | Proof/preparation error leaves schema unchanged |
| CN mutation | Restore callback registered before private defs/version/extraInfo change; queued ALTER plus deletion/recreation of catalog rows with same relation ID | All writes are one transaction; rollback restores defs/version/extraInfo and rebuilds name/column/PK indices |
| TN apply | Existing table catalog MVCC entry, transaction entries and local schema | Apply is provisional; TN-after-apply error/cancel rolls back through transaction ownership |
| Durable commit | Existing coordinator/TN transaction decision | Complete new catalog/schema becomes visible, never an independently committed half-definition |
| New CN transaction | Reopened committed catalog/logtail relation | Complete old or committed new definition; current connection cache alone is not an oracle |

The linearization point is the existing transaction commit decision, **not**
admission, catalog batching, TN apply or flush `OK`. Schema version is not a
second timestamp. Old snapshots retain MVCC schema/object visibility; ADD does
not rewrite or physically delete old data. Clone/snapshot/GC ownership remains
relation-local under existing rules. SQL privilege/tenant/relation checks stay
with normal callers; object name is not an authorization token.

Two ADDs serialize/conflict via existing protection/definition checks; each
committed logical ADD advances once. DML before ALTER must be included by the
protected/retried path. DML while ALTER holds locks waits or receives existing
retry/conflict, never publishes an old layout under a new definition. Tests
cannot wait for a blocked INSERT before releasing ALTER's barrier.

Error/deadline/disconnect before commit use the same rollback ownership. A
failed second ADD cannot undo the first committed ADD. An ambiguous response
requires resolving the existing transaction outcome and reopening the relation;
blindly resending SQL is not an idempotency protocol. No independent replay or
retry loop is added. The CN restore stack runs in reverse on rollback; normal
transaction cleanup releases captures. Test barriers require timeout and
unconditional release, never weakening production protection.

## 4. Proof, readers, writers and merge

`ValidateM0Object` checks header/version, aggregate width and every block's
complete typed prefix. Metadata must come from the standard object reader for
an object already selected from the target relation. A lookalike layout or
arbitrary file name does not establish relation membership.

The internal proof holds **object name, written width and read schema width**.
It has no table ID, snapshot or generation identity and is not a durable or
general-purpose capability. The producer owns relation/snapshot/schema checks;
the consumer checks exact object name and requested suffix range. Do not carry
it across another relation, schema or transaction. Reader-local reuse is bound
to that reader's schema and current object; retry/reopen creates a new lifecycle.
TN merge proves its own inputs, never borrowing a CN context. Wrong binding,
missing proof and broken prefixes remain errors even for nullable columns.

Ranges, statistics/metadata and actual block readers prove before interpreting
absence. M0 bypasses the unproven fast block-filter route for the append domain;
`IS NULL`/`IS NOT NULL` use proven written width, not a missing descriptor.
Conservative COUNT rules prevent absence being mistaken for NOT NULL.
Unordered eligible M0 readers stripe complete descriptors with partition/
tombstone association and the memory sentinel preserved. Default/ordered
splitting stays unchanged; this is not universal scheduling optimization.

CN output version derivation requires the complete dense write batch and
hidden-key anchor. TN flush/merge writers validate complete schema/seqnums,
not an obsolete local version on wider data. Separately, TN merge reconstructs
a candidate append layout from complete column identities, validates it and
proves each input object. This is not permission to infer missing read data
from a writer's formula. Nonzero clone version propagation closes the carrier
loss; derivation remains checked and domain-limited. Invalid M0-shaped inputs
report errors, not ordinary fallback. Existing merge/transfer/commit/reclamation
semantics remain. Repeated failed merges are not successful rewrites.

## 5. Persistence, resources and performance tradeoffs

Catalog version, ALTER request and object header fields already exist; their
encoding does not change. WAL/checkpoint replay retains normal transaction/
schema entries. Aborted ALTER must not recover a new committed schema;
committed ADD plus later writes must recover version and NULL semantics.
Restart evidence is not a complete power-loss matrix. DUMP/backup keeps normal
visibility rules: TN flush `OK` requires CN logtail visibility before a complete
DUMP; the in-memory-row guard is not relaxed.

| Resource / first owner | Growth, bound and terminal cleanup |
| --- | --- |
| Layout / proof call | O(columns), existing column/seqnum limits; released on return, no historical schema retention |
| Header/meta reader / current object | Existing ObjectIO/fileservice limits; one object's metadata per proof, temporary mpool deleted on all returns, error/cancel propagated |
| Marker / context and reader | Fixed widths plus object name, one current-object reader slot; context/reader lifecycle release; no global cache/queue/goroutine |
| Definitions / CN transaction | O(columns) clone and restore captures per mutation; admitted independent autocommit bounds ADD to one; commit/rollback cleanup releases them |
| Descriptors / existing range-reader | O(selected blocks), existing query budget/backpressure; stripes descriptors, not row payload; normal reader close releases buffers |
| Objects / existing engine | ADD writes metadata, not duplicate payload; later DML/flush/merge use ordinary budgets, cancellation and GC; no M0 retry worker |

These are not hard global memory quotas. Existing query/operator/file-cache
budgets and object/column limits apply. Proof CPU is O(sum of object blocks
times written columns) per qualification, not O(1) from maxseq. Metadata I/O,
cache pressure and CN proof while holding DDL locks can increase latency,
including pristine eligible version-zero composite-PK roots.

Ranges, reader, statistics and merge can re-prove objects. The reader already
reuses context/same-object proofs, and fileservice may cache metadata; there is
no cross-scan proof cache. A global cache adds invalidation/lifetime risk and is
not justified here. The core range/reader proof chain already existed in CF9;
later ordinal/docs/CI repairs do not establish a newly introduced proof cost.

The accepted trade is removing ADD COPY while paying proof, conservative
filtering and M0-wide COUNT rewrite restrictions. UPDATE/read/merge cost and
post-commit debt remain measurable liabilities. No current-head speedup or
zero added capacity cost is promised.

## 6. Shared default behavior and historical compatibility

Default COPY selection does **not** make this whole patch tag-isolated:

| Shared owner/change | Boundary and required regression |
| --- | --- |
| ObjectIO missing columns | Exact narrow proof or error; legitimate caller, wrong object/column, no proof and damaged prefix controls |
| Catalog nullability | Authoritative `attnotnull` rebuilds nullable metadata even with stale default descriptors; no encoding change |
| PRE_INSERT | Resolve actual written column by name/map; retain required-column and constant-NULL rejection, not hidden-offset assumptions |
| Catalog create decoder | Preserve nonzero `rel_version` to TN; zero-version behavior unchanged |
| COUNT planner/compiler | Identity/name binding and expression/column nullability guards, NULL/outer-join/DISTINCT controls; only M0 broadly disables rewrite |
| Ordinals/View templates | Dense public ordinals without changing ColId/seqnum/hidden-key identity; retain early derived-View filtering and ordinary/subscription/tenant checks |

Former unconditional missing-column NULL filling now fails closed. Historical
objects outside explicit proofs may become unreadable although older binaries
returned NULL; they are not automatically classified as corrupt. Arbitrary
pre-COPY metadata-only ADD layouts are **not qualified upgrade inputs** here.
The supported source boundary is based on physical geometry, not an inferred
release age. It is fixed for this candidate, not delegated to a future QA
inventory:

| Source/read geometry | Decision before promotion | Required validation |
| --- | --- | --- |
| Ordinary objects from CREATE/COPY/DML/flush/merge or restore, with every requested ordinary column physically present and correctly typed | Preserve existing reads, regardless of source binary age; ordinary supported ADD stays COPY | Default reader/BlockRead/TopN/publication and ordinary COPY regressions; restored objects obey the same rule |
| Canonical system `mo_catalog.mo_tables` object predating `rel_logical_id`, with that single requested column absent | Preserve the existing logical-ID-from-rel-ID compatibility behavior through one exact proof; other missing columns still error | Require DB ID `MO_CATALOG_ID`, table ID `MO_TABLES_ID`, schema name, logical-ID column name/canonical seqnum/uint64 type, object name and requested seqnum; wrong-DB/table/type/seqnum/object controls must reject |
| M0-created absent nullable suffix | Only matching tagged readers and the complete append proof may read it; not an ordinary-build upgrade/downgrade input | Tagged success/clone/merge/history/recovery plus wrong-prefix/object controls |
| Pre-COPY ADD or any other ordinary object missing a requested column outside that canonical exception | Explicitly unsupported input for this candidate, including backup/restore/import; fail closed, not NULL and not automatically corruption | No-proof/other-column rejection; verify pre-upgrade inventory/migration instead of accepting the object based on version age |

For that last class, do not replace the running binary against its data
directory. Under a known-compatible source binary, export logical contents
(including its historic NULL values), recreate the target schema with ordinary
CREATE/COPY in a separate directory, import and compare complete contents before
switching. Retain the original backup. This is a required migration contract,
not a tested migration tool or automatic fallback supplied by this PR. A source
without a working compatible reader is not admitted. If a supported deployment
requires in-place acceptance of another missing geometry, this candidate cannot
be promoted there without a reviewed proof/migration revision.

The current implementation at the reference commit checks only schema/column
names for the legacy proof. **Required implementation correction before renewed
implementation approval:** bind the persisted-node caller's real DB/table IDs
and the canonical column identity/type before minting that proof. No format or
new caller-supplied identity is necessary; `Scan` already has the catalog-owned
`common.ID`. This requirement is an explicit deviation, not a claim it is fixed
by documentation. Synthetic fixture markers cannot authorize real callers.

QA verifies this decided boundary and inventories migration needs; it does not
choose which missing objects silently become supported. The sentinel plus
object/column detail is diagnostic, not a promised stable public error code.
Historical-source acceptance and migration execution remain release gates.

## 7. Verification, delivery and open gates

The oracle is COPY-equivalent content plus explicit object/path/transaction
checks, not SQL success or an admission log alone.

| Invariant | Deterministic evidence requirement |
| --- | --- |
| Dense domain/complete prefix | Layout/object/writer tests: wrong object, version, type, holes, width, suffix and clone mapping |
| Success/COPY separation | Formal customer ALTER, NULL/COUNT, INSERT, partial/full UPDATE, old fingerprints; default COPY control; customer SQL BVT input/result |
| Atomic failure | Private defs, queued TN ALTER, catalog deletion and actual TN-applied injection; new-connection schema/version/seqnum/object readback and retry |
| Cancellation/ordering | Cancel/deadline, first-commit/second-failure, concurrent ADD and DML-before/ALTER-holds-lock, bounded barrier release and focused race |
| Clone/history/background | Parent ADD/UPDATE, clone, child ADD/UPDATE, committed merge, mixed objects, old snapshot and parent isolation |
| Recovery | WAL and checkpoint-plus-WAL, CN/TN restart readback, historical DUMP with CN visibility barrier |
| Default compatibility | Reader/TopN/BlockRead/publication, catalog/PRE_INSERT/COUNT and ordinal/View tests; narrow legacy scope, not generic upgrade certification |

`M0_STATUS.md` binds local/Q1 Linux/shared D3/C3 evidence, native/tag identity
and failed generations. Reuse is content-scoped; no exact-head Linux run of
all closures is claimed. Ordinary CI is not tagged M0 execution. Historical
CF9's approximately 27% driver observation is package/reference-only, not
current-head performance or complete maintenance-debt acceptance.

Delivery order: approve this immutable design revision; resume implementation
review against it and reconcile deviations; complete maintainer review/QA;
then consider separately approved release. No model approval/CI enables production.

All experiment components/native must match qualified source/tag. Do not mix
ordinary and M0 readers on an M0 data directory. No runtime enable/disable
switch is promised. Disabling the tag or rolling back a binary is **not a data
migration**: old objects can retain absent suffixes. Stop experiment writes,
retain backups/evidence, and return to a known-compatible backup or separately
validate COPY-based export/restore in a new directory before removing M0.
Never rewrite versions or delete shared files to force downgrade compatibility.

Operators diagnose proof errors, transaction outcome and actual merge commits/
failures; `M0_ADMIT_ADD_COLUMN` denotes an attempt, not commit. Repeated merge
failure or content/NULL mismatch stops promotion. Existing logs, transaction/
object metadata and receipts suffice; there is no new telemetry service.

Distinct remaining gates:

- **Design:** decide this restricted invariant, ownership, deliberate fail-closed
  boundary and conservative cost model on an immutable revision.
- **Implementation:** verify the existing complete feature conforms; historical
  code reviews are evidence, not substitutes for the design phase.
- **Maintainer/QA:** verify the decided source/geometry matrix and any required
  migration, deployed tagged/default behavior, recovery/backup and diagnosis.
  Out-of-contract legacy geometry blocks release, rather than gaining a proof.
- **Production/performance:** mixed-version/multi-CN, full crash matrix,
  current-head cost/debt and enablement need separate approval.

No architectural choice within the restricted experiment is intentionally
deferred to implementation. Broader compatibility/schema support requires a
reviewed design revision rather than relaxing checks.
