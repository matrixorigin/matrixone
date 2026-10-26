# Restricted appended-column reads and BOOLEAN false birth values — design R1

Owner: investigation #30518. Implementation series: #30816 (NULL suffix and
shared correctness fixes), #30958 (restricted false suffix and safe COPY exit).
This revision is submitted for a distinct integration design review on
2026-10-26. It documents and challenges an existing prototype; it is not a
retrospective claim that implementation had prior design approval. The review
decision must identify this exact Git revision and document digest before the
updated implementation is delivered. An RFC or human sign-off is not implied.

## Problem, contract and alternatives

COPY ADD rewrites existing rows even when an appended nullable column has a
constant value. software_dev needs NULL suffixes; data_cleaning needs nullable
BOOLEAN DEFAULT false. SQL requires existing rows to retain the value determined
when the column was added, while later INSERT uses the current SQL default.
Stored NULL must remain NULL. A later DEFAULT change cannot reinterpret old rows.
The control is the existing transactional COPY implementation, not paper timing.

The scope is a default-disabled, same-capability isolated experiment. One
independent pessimistic RC autocommit ADD appends one column to a proved stable
layout. FALSE admission additionally requires a nullable BOOLEAN literal false,
one trailing column, the existing hidden composite-PK domain and live deployment
admission. Secondary indexes, references, partition/temporary/cluster layouts,
backfill expressions, NOT NULL and unsupported properties do not qualify.
No SQL syntax, public API, scheduler, merge policy or other default type is added.

The inherited NULL-only experimental domain remains restricted: a later
non-qualifying DDL is rejected before mutation rather than allowed to violate
next=b+v. General COPY exit is the false-history extension described below;
this series does not expand the NULL-only prototype's later-DDL support. Ordinary
builds retain their existing legal COPY behavior. This limitation must remain
visible during experimental deployment and is not a production SQL promise.

Alternatives are: (1) retain COPY, with simple and established semantics but a
full rewrite per ADD; (2) reconstruct arbitrary historical schemas from lineage,
which might support more tables but lacks reliable writer relation identity for
all historical objects and adds history/retention dependencies; (3) encode NULL
in the existing restricted append invariant and persist an immutable birth value
only when NULL is insufficient. Choose (3), subject to complete proofs and a
transactional exit to (1). Do not infer arbitrary lineage from object names, use
the mutable Default as history, or introduce another deferred rewrite executor.
The SQL behavior follows the existing COPY oracle, including errors and NULLs;
there is no new interoperability standard to substitute for that oracle.

## Authoritative state and representations

For the NULL-only domain, let k be the stable hidden PK seqnum and b=k+1 the
ordinary root width. A root has version zero, dense ordinary identities and a
final rowid. Each qualified single ADD increments both version and next seqnum;
at version v, next=b+v. An object with header version u must prove u<=v, width
b+u and the complete typed prefix in every block before a suffix can mean NULL.

For false history, SchemaExtra.append_missing_state (api.proto field 23,
persisted through mo_tables.extra_info) is the authority. TableDef field 43 is a
projection, not another mutable owner. State format 1 contains a 16-byte layout
identity, base width, PK seqnum and ordered birth entries: stable seqnum, append
ordinal, exact type and NULL/BOOLEAN_FALSE kind. Kind zero is invalid. A false
introduction after an old NULL suffix records that suffix as NULL explicitly.
Current INSERT Default remains in the ordinary column definition.

Validate format, type, order, uniqueness, widths, layout identity and immutable
history extension. Unknown or inconsistent state fails before mutation or read;
protobuf unknown-field tolerance is not old-reader compatibility. Handwritten
plan/engine/cache/schema projections and clone/MVCC copies preserve independently
owned state. api.Batch.logtail_write_schema field 3 carries captured write
version/next seqnum; a replay-row reference is an immutable shared projection.
It must never become a second schema authority.

## Operations, ownership and terminal outcomes

| Boundary | Owner and required outcome |
|---|---|
| Planner admission | Bind column/default, transaction and layout eligibility before choosing an algorithm. Ineligible new false ADDs and false-history exits choose COPY for legal default-algorithm operations; unsupported explicit INPLACE rejects. The inherited NULL-only domain rejects unsupported later DDL before mutation. No fallback after mutation. |
| CN workspace | Resolve the actual relation/snapshot, verify persisted/uncommitted state, construct private definitions/default/history and register one rollback owner. Do not alias old Extra or rowid. |
| TN publication | Verify expected source version/next seqnum and immutable extension before mutation. One ADD advances one schema version. Column, current default and birth state commit in the existing DDL transaction. |
| Persistent read | The actual relation and visible schema establish ownership. Full object/block identity, type, header and complete prefix establish legal missing suffix. Present columns, including NULL bitmap, always win. Holes or mismatches return errors. |
| Memory read | Use captured writer schema and complete vectors. Distinguish a genuine old prefix from a truncated new-layout batch. Error propagation must stop callers before commit-TS or deletion postprocessing consumes output. |
| CN writer | Every ordinary identity appears exactly once; allow the real PK-last DML projection without weakening catalog/read ordering. Borrowed input is not overwritten or freed prematurely. |
| TN flush/merge | Capture one write schema, synthesize the typed suffix and publish a complete output under the derived version. Preserve old valid prefix headers where appropriate; never label incomplete new data as old. Tombstone semantics remain unchanged. |
| Logtail | Preserve captured version/next seqnum and split mixed versions using existing entries. Default builds do not emit the new capture. CN replay retains immutable schema context. |
| Recovery/clone | Existing SchemaExtra serialization, MVCC and snapshot visibility carry state through WAL/checkpoint and clone. Readers use the visible schema, not the latest schema globally. |
| Failure/cancel/conflict | Return the original error, release existing owners/waits, restore private and TN-applied derived state; retry only in a new transaction. No partial schema/default publication. |

No new global publication gate, wait order, retry loop or asynchronous completion
wait is introduced. Existing transactions own commit/rollback. Existing batches,
vector pools and reader scopes own allocations; generated suffix vectors close
on success and error, and proof references expire with the existing read/batch.

## Safe exit and subsequent SQL

After false history exists, further qualified false ADDs extend it. Other legal
DDL uses the existing standard COPY in the same user transaction. Read the source
with the old birth interpretation; construct a full ordinary target with dense
identities, version zero and no append state; publish through the existing COPY
replacement path. DEFAULT changes, DROP/re-add, COMMENT, rename and index creation
must all obey this rule. Independent CREATE INDEX and ALTER ADD INDEX share the
same exit owner, not separate index publication logic. A failed UNIQUE build must
return the duplicate error and preserve source identity/version/data. Rename
rebinding also remains in that transaction. Historical snapshots and child
clones continue to interpret their original relation/schema independently.

The fallback is not permission to clear metadata on an existing source, reuse a
new default for old rows, defer materialization until after elapsed, or silently
accept an explicitly unsupported algorithm.

## Shared ordinary-build behavior and trust boundaries

Ordinary builds select COPY and do not enable false admission. The series also
contains shared fixes: name-based PRE_INSERT checks, authoritative attnotnull,
nullable COUNT correctness, version propagation and dense public ordinals for
trusted system-view templates. They require ordinary tests independent of M0.
Parsed per-binding view rewriting must not alter persisted templates, tenant
partitions, authorization predicates or ordinary user View metadata.

Strict missing-column reads replace broad implicit NULL filling. The known
mo_tables.rel_logical_id case has an explicit legacy proof. Other historical
missing-column layouts must not be called compatible without migration/upgrade
evidence. Report an error rather than conceal corrupted or unsupported data;
still treat relevance of older layouts as a QA/rollout prerequisite, not proof
that every error is corruption. Default-build replay-row pointer storage and
shared checks are real costs even when feature selection is off.

Existing SQL permissions and tenant checks remain the authority for DDL/reads;
the feature does not grant access. Relation/schema proof scope prevents reusing
one object's name as cross-branch or cross-account authority. A local run marker
is operational admission, not a tenant permission or security sandbox.

## Deployment, compatibility and removal

False history is permitted only in one isolated static LOG/TN/CN deployment
using the same bound executable, native receipt and effective data/config roots.
The launcher and runtime verify executable/config hashes, actual roots, sidecar,
inode xattrs and a held inherited lifetime lock before opening candidate data.
Missing xattr support or failed identity is rejection, not a bypass. Ordinary D
seed import must preserve candidate markers; old binaries and recovery tools
must never open candidate-marked data under this operational contract.

An arbitrary old executable cannot retroactively understand a marker. The guard
therefore relies on the exclusive owner using the launcher; it does not prove
universal old-binary exclusion. No mixed-version, rolling upgrade, downgrade,
multi-CN rollout, remote object store, arbitrary restore tool or power-loss
qualification is claimed. If isolated admission cannot be enforced, keep the
feature disabled. Removing the feature from a deployment requires materializing
affected tables through supported COPY and independently qualifying the export /
ordinary destination; deleting the descriptor or ignoring the marker is unsafe.

## Capacity, performance and observable costs

Birth history is bounded by existing column/seqnum limits and grows O(columns).
Read proof is O(blocks * physical columns) and may read complete metadata; the
fileservice cache does not make the CPU work free. Memory proof may repeat
validation for multiple missing columns. Captured replay schema adds a pointer
slot per replay row even in ordinary builds. No unbounded proof cache, worker or
queue is introduced; a cache or pruning optimization would be a separate design.
Conservative filtering/count/statistics may increase scans and cannot classify a
missing false suffix as all NULL. COPY exit remains full materialization.

Performance validation compares matched source/toolchain/native/config/seed and
resource contracts. Local chain screening uses original SQL plus separately
labelled artificial continuations and fixed observations; full pilot keeps the
original driver. CPU, LocalFS successful write-call bytes, object payload and
device I/O are distinct. A faster ADD alone or delayed maintenance is not an
overall benefit. One full pair with differing operation graphs is a candidate
result, not statistical significance, production acceptance or paper recovery.
Historical results remain tied to their measured commits; integration changes
invalidate only the evidence for paths they actually affect.

## Verification gates and remaining decisions

Before updated implementation approval: independently approve this versioned
design and review material deviations. Then validate exact-content field/copy
roundtrips, prefix/identity corruption rejection, stored NULL, old/new memory and
object reads, complete writers, logtail, atomic failures, clone/history, merge,
WAL/checkpoint and standard COPY exits. Use existing small fixtures, deterministic
faults/barriers and COPY content oracle, not sleeps or a generic new harness.

Default-build shared changes require their owning tests, actual public BVT,
static checks and coverage. Tagged mode requires its selected tests and necessary
race plus actual same-version Linux SQL; ordinary green CI alone is insufficient.
Tests must prove path entry as well as data. Reviewers can reuse unchanged,
source-bound evidence, but may not relabel old binaries as the integrated head.

The isolated candidate may be proposed for formal review after these bounded
developer gates and explicit dependency handling. Merge remains blocked on
#30816 and the repository's review/CI policy. Production enablement remains
blocked on maintainers accepting the durable-format/old-layout contract and QA
closing relevant compatibility, topology and recovery cases. Owners: PR authors
provide source/evidence; maintainers decide compatibility/deployment acceptance;
QA owns deployed qualification. Long-term maintenance neutrality and proof CPU
cost remain explicit performance follow-ups, not reasons to invent another
mechanism or silently claim zero cost in this series.
