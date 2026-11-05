# Source-ordered ODKU on MatrixOne 4.2

`INSERT ... ON DUPLICATE KEY UPDATE` executes a sequence of row actions. Each
input chooses the first eligible conflicting constraint in table order and
applies assignments in AST order to that row's current image. Later actions
observe generated-column changes and vacated keys. Grouping actions by their
initial snapshot target cannot implement this contract.

The existing `PreInsertUnique` operator owns action ordering, current images
and constraint-key ownership. The engine owns captured prior data; transactions
and `lockop` own conflicts and locks. Existing FK plans, index plugins and
`MultiUpdate` own validation and physical writes. DedupJoin and HashBuild do not
maintain another ODKU replay state.

Direct PK/UNIQUE assignment restrictions retain their 4.2 behavior. Indirect
changes through generated columns support collision rejection, old-key reuse
and hidden-index payload updates. Tables without unique constraints use modern
INSERT; binding still validates the dead RHS, parameters, dependencies and
authorization.

## Plan and expressions

The converted source receives a positive INT64 ordinal before lookup joins.
A global sort restores order before the single action owner. A statically proven
single VALUES row can use ordinal 1 without numbering or sorting; estimates and
LIMIT do not provide that proof. Ordinals increase across batches, with gaps
allowed. Empty batches and EOF do not advance them. INSERT SELECT without ORDER
BY does not gain a SQL ordering guarantee.

Constraint keys use existing physical builders. ENUM uses uint16;
prefix/composite keys use existing serialization; NULL UNIQUE keys claim no
owner. Original physical RowID identifies a retained owner. Its raw original PK
is a separate writer payload: distinct stored floating +0/-0 PKs must not
collapse through SQL equality.

`PreInsertUkCtx.OrderedOdku` and `OdkuActions` carry source/original columns,
table-to-source positions, ordered assignments, checks, FK eligibility,
constraints and ordinal. Evaluator relation 0 means the current table image;
relation 1 means incoming values. Relational rewrites remap source references
without rewriting these namespaces. Liveness, parameter visitors, deep copy
and codecs preserve this distinction.

Output contains the materialized source image and original table image,
including the original physical PK and RowID. Original columns are nullable
because a new row has no original identity. Existing projections and index
writers consume this layout.

## One action owner

Retain one current image per touched row, its constraint keys and immutable
original image when present. Accounted key maps identify current owners; an
original-RowID map recognizes an original after its keys move.

Consult current ownership first, then resolve the original from the captured
prior view or the joined original for an immutable-key attempt. A known original
conflicts only if its current key still matches: a snapshot can return a vacated
key. Register an untouched original once; start an INSERT when no eligible key
matches. Incoming keys unused by an UPDATE never become claims.

Evaluate every RHS once in assignment order, including repeated targets.
Recompute dependent generated values and the composite physical PK from those
results. Check proposed non-NULL keys against current owners and prior rows
before publication. Another live owner yields the typed duplicate error.

Evaluate into stable private scratch before modifying retained vectors. New rows copy the full
image; updates copy assigned table positions and source values needed by later
consumers. Physically unchanged keys retain their existing claims and storage.
Grouping metadata takes the general transition path. This does not skip RHS,
checks, prior-key resolution or lock handoff.

Vectors, hash cells, iterators and scratch use the attempt allocation account.
Writer-fence copies select that account even when an upstream tokenizer has no
selection or uses a different one. Retained and returned Go batch metadata has
its own shared-budget reservation; readers clean the batch before releasing it.
Transient keys share existing hash groups for lock-history deduplication.
Nonspilling state has a finite admission budget and returns the existing resource
error on exhaustion. Output uses 8192-row and soft byte quotas, allowing one-row
byte overshoot. Any partial publication failure makes the attempt terminal until
Reset. Reset/Free also own partial preparation.

## Checks and foreign keys

Explicit input CHECKs retain their prefix order; declarative NOT NULL checks
apply to the actual written image. An invalid explicit input CHECK is rejected even
when an UPDATE could repair it; this deliberately corrects the former FK-dependent
4.2 acceptance of that input. CHECK UNKNOWN passes. Unrelated historical
CHECK violations are not revalidated. Only exact builtin NOT NULL assertions
with literal messages use direct null-bit evaluation.

For each enabled non-self FK, INSERT is eligible and UPDATE is eligible when its
tuple changes under SQL NULL-safe comparison. Last-action eligibility differs
from cumulative final eligibility. Superseded images use existing snapshot
MARK/assert checks; final images reach existing parent-lock/wait/refresh before
missing-parent errors. Physical no-op filtering follows required FK validation.
An unrelated payload update on an unchanged orphan can pass, while
orphan-to-valid-to-orphan still requires final validation.

When intermediate FK output fills the quota, retain one pending target and
resume without repeating its RHS or ordinal. Filter-pushdown barriers preserve
these evaluation and validation boundaries.

## Captured prior data

Compile captures an attempt after table locks and before data-source
initialization. Resolve relation definitions outside the transaction mutex,
then capture base and hidden tables together under it. Retain Entry values,
raw batches, copied deletion selections, object stats/tombstones and committed
snapshot state/TS. Offsets alone cannot survive workspace reordering.

DELETE prepares metadata and epoch checks outside the mutex. Under the mutex,
stage the raw/CN/residual partition, copy only residual TN rows, then publish
all selections and the TN entry atomically. This bounded vector work performs
no metadata lookup, internal SQL or file IO while holding the mutex.

Pins prevent merge, shrink, dump and cleanup from changing captured RowIDs or
freeing captured objects. Last-pin deletion obligations transfer to a quota-backed
engine queue before pin removal. Bounded deletion runs outside transaction and
view locks, releases charges after success, and retains failed chunks for retry.
A canceled view hands off promptly; an uncanceled close may try engine cleanup,
whose failure is diagnostic and cannot replace the statement's typed result.
Engine shutdown joins the bounded drain and reports remaining cleanup errors. Rollback invalidates the view and detaches
pinned resources; it never waits for readers under the transaction mutex. LOAD cleanup
cancels and joins affected readers outside that mutex, then performs existing
bounded synchronous deletion before releasing filename protection.

Lookup returns an independently owned typed row or absence. Sparse workspace
probes can promote captured workspace rows to the existing accounted index.
Committed rows remain in storage. Finite floating probes use a typed point or
closed codec-equivalence interval, followed by exact physical-PK or logical-UK
residual comparison. Zero uses a closed packed interval covering both signed
encodings; NaN and infinity use a streaming scan. Float block filters retain
zone-map pruning and use conservative scans where NaNs invalidate sorted order.
Each remote probe owns its filtered ranges only until its reader closes;
a known matching commit is required before sending any narrowed statement-key
filter, including integer and string filters. References
and selectors reserve existing query HashBuildBudget; selected block-list
capacity is admitted before growth and released after reader close. The local
data source reuses that list. Remote request serialization reserves its own
buffer before allocation and keeps the charge through reader close; the
tombstone-free codec allocates the complete checked frame, including headers,
once. The receiver still builds and serializes its
selected ranges under its ordinary storage ownership; this does not establish
constant-size remote fallback metadata. Physical vectors use the native
allocation account. ODKU does not consume the separate
CTE budget. Overlapping zones or exceptional values can still require a full
scan, but lookup never retains all committed row images.

Remote/sharded lookup uses the existing delegate and transmits committed ranges
only. Captured uncommitted tombstones apply locally to returned RowIDs, so lost
ReadClose acknowledgement cannot retain our pinned files. Readers use the
existing bounded close policy. A primary read/cancellation error preserves its
exact typed identity; secondary close failure is diagnostic. Without a primary
error, close failure is returned.

Compile closes the view and detaches prepared-operator callbacks before sealing
the attempt account. Owned results survive closure. A joined tuple alone does
not attest membership in the capture, so it cannot replace mutable-key lookup
without a provenance check.

## Locks, optimistic commits and writers

Keep incoming/original locks and collect distinct physical old, new and transient
keys. Acquire them through existing `lockop.LockRows` before writes. Snapshot
refresh retries the complete attempt. Validate the captured workspace after
locking so a same-transaction mutation cannot make an old RowID write stale.

LockRows does not enforce optimistic transactions. TN precommit deduplication
must check persisted replacement keys against concurrent commits. Data and
tombstone base tables own separate scan frontiers, advanced only after every
eligible source scan succeeds. Unexpected Bloom/storage errors and cancellation
propagate without advancing a frontier. Per-block buffers release before the
next block is read.

At the common RPC request boundary, optimistic transaction metadata plus default
Incremental policy selects existing FullSkipWorkspace policy. Pessimistic
Incremental behavior and explicit policies remain unchanged. Copy borrowed
requests before normalization. No new wire policy or ODKU-specific TN state is
needed.

Complete old regular-index reads and synchronous irregular-index delete
preparation before mutation. Materialized sources can stream before Finish,
so ODKU writer dependencies wait for every producer's successful completion.
CTE streaming retains its own policy.

Writers receive final images and original deletion identities. A PK move updates
every hidden index's PK payload even when its key is unchanged. Fulltext/IVF
deletion uses the original PK; composite physical PKs are regenerated after
component updates. There is no all-deletes-first compatibility path.

## Protocol and validation

MORPC version 12 is the branch-local threshold for the ordered layout and
filter barrier within a homogeneous 4.2 deployment. The service-local numeric
check does not negotiate peer branch or schema capabilities; main assigns
version numbers independently. Full query plans with the current marker and an
incorrect output width fail before producers run. Instruction-only scopes lack an
output-layout identifier, and older private metadata can decode as unknown fields
without the current marker. Preflight cannot identify every old private plan.

Drain in-flight work and invalidate or recompile cached and prepared ODKU plans
from older builds before the coordinated upgrade, and drain before rollback.
Keyed ODKU can be unavailable at gate 11. Mixed main/4.2 execution is not
supported. Catalog, disk and TN dedup formats do not change.

Validation uses public base/index oracles for ordered updates, physical signed
zero, generated move/reuse/collision/NULL/composite keys, prior raw/flushed writes
and deletes, checks/FKs, prepared reuse, competing commits, spill, remote execution
and cancellation/rollback. Optimistic tests exercise default policy and real
commit outcomes. Resource tests verify ownership after failure and closure.

Performance comparisons use correct workloads, the exact base, matching settings,
at least three warmups and seven measurements, with sufficient A/A resolution.
Singleton median regression must be at most 5%; representative 8192-row batches
at most 10%, including insert/conflict/no-op and narrow/wide data. Report
distributions and peak memory. Ordering, retained images and prior-view indexing
have real costs; profile breaches without weakening semantics or waiving gates.
