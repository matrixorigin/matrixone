# Index planning ownership and execution lifetime

This design addresses issue #32318 in PR #32341.

Mixed `DATETIME`/`TIMESTAMP` comparisons use wall-clock and instant semantics,
respectively. A useful index bound can be a necessary range rather than an
exact SQL comparison. The complete original predicate remains a row-level
residual, including columns that are not projected. Every SQL-TRUE row must
survive each early filter, and skipping rows must not change observable errors,
warnings or effects. Unsupported or unknown optional evidence declines the
optimization or widens the physical range.

The type-specific inverse, precision, sentinel and physical-tail proofs are in
[temporal index ranges](20261028-temporal-index-ranges.md); the eligible query
shapes and cost model are in [temporal CBO](20261030-temporal-cbo.md). This
document defines the shared owners and their boundaries. It does not broaden
AUTO admission to prepared execution, named-zone inversion, joins, aggregates,
LIMIT/OFFSET, non-covering backfill or distributed temporal candidate scans.

## Ownership

| Concern | Owner and contract |
| --- | --- |
| Execution-time expression boundaries | The existing function registry identifies endpoint families whose subtrees must survive folding until the execution context exists. The normal folding and constant-analysis owners consume this fact. |
| Original access and plan lifetime | The root `QueryBuilder` analyzes the bounded original scan once. It keeps positive admission separate from the conservative `ExecutionDependentAccess` reuse fact. |
| Resolution and read identity | Ordinary authorized relation resolution can return optional borrowed evidence. `CatalogReadStamp` compares the caller's actual transaction, workspace, visibility and snapshot identity; it grants no authority or resource ownership. |
| Index choice and construction | The existing regular-index selector, common comparison classification, scorer and covering-scan constructor own both ordinary and necessary-range choices. |
| Necessary storage filter | `Expr.CandidateFilter` records the producer's proof that an independent storage predicate is necessary and safe to evaluate early. The SQL residual remains authoritative. |
| Captured physical execution | `IndexScanInfo.requires_captured_execution` restricts a physical scan to its admitted execution context and placement. Frontend, scheduler and compile/retry owners enforce it. |
| Reuse | Frontend's existing cache-policy owner decides ordinary generation reuse, prepared logical-template rebuild and prepared physical-compile reuse from the facts relevant to each layer. |
| Metadata observation | The relation's bounded, cache-only index-planning capability observes existing metadata synchronously. It creates no relation cache, subscription, worker or second publication route. |
| Physical filtering | Reader and raw sort-key filtering independently check their actual comparison and encoding domains; a candidate proof alone does not authorize either operation. |

These facts serve different existing boundaries. A necessary filter is not
permission to access a hidden relation or place a scan, and `ForceOneCN` alone
does not prove the selected CN is the current coordinator. Statistics can
choose a path but cannot authorize dropping rows.

## Original-plan proof and lifetime

The builder traces a SELECT's bounded unary project/filter path to the original
scan before folding. Its fitting single-comparison proof is required, together
with the current plan and diagnostic-safety checks, before the optional access
can be admitted. Each attempt has fixed expression and metadata budgets; an
exhausted budget does not grant access.

The same owner also determines whether a future execution could select a
different access. A BASE decision needs the same lifetime consideration as an
INDEX decision: a later clock can change the cost. For a complete single proof,
the owner checks mixed types and combines regular-index leading-part exclusion
and any-part membership in one bounded traversal. An incomplete or stopped
proof uses a bounded conservative walk of original filter expressions and
index metadata. Unknown or unvisited evidence cannot prove independence; an
actual witness or unknown yields a dependent lifetime. A completed negative
proof preserves reuse for unrelated ordinary plans. Prepared roots retain their
existing reusable-plan owner and do not acquire temporal candidate input.

The positive proof has a 4,096-unit work budget, at most 64 expressions per
list and depth 64. Dependency fallback is also capped at 4,096 charged units,
depth 64, 64 index definitions, 16 parts per index and 256 bytes per inspected
name or algorithm. An oversized or uncharged algorithm is not normalized: it
invalidates both a negative membership proof and leading-index exclusion. A
regular existing index with no parts, a malformed alias, an unvisited part, an
unproved source or a stopped proof is unknown, never negative evidence. The result is only the existing query
reuse fact; no retained dependency tree or general visitor framework is added.

## Authorized resolution and read identity

Ordinary `Resolve`, its namespace variant and `ResolveIndexTableByRef` accept
an optional caller-owned `TableResolution` output. A call without the output
does not add stamp capture, native lookup, subscription or allocation for this
optimization. A requested output is cleared before work and filled only after
the ordinary resolution and its temporary-table, subscription, snapshot and
borrow checks succeed. Wrapper overrides remain the routes actually called;
wrappers with different authority or visibility can decline the optional
borrow without changing their normal descriptor and error behavior.

User scans pass through existing physical-index authorization. Generated SHOW
and index-maintenance sources use their existing authorized routes and parent
references. Metadata-only catalog validation retains its separate identity
purpose and does not receive borrowed output. The builder revalidates the
original caller and process after reentry and checks the real base/hidden pair,
account, table and schema identities, transaction and workspace revision,
snapshot, subscription and visibility before publication. Borrowed relations
are synchronous evidence owned by the transaction; the observer does not close
or cache them.

`CatalogReadStamp` binds the actual caller's operator, including wrappers, and
provides the semantic and structural timestamp matching modes required by its
existing consumers. Sampling and blocking policy stay with each caller:
optional index observation can decline on a failed try, while mandatory view
metadata retains its own errors. The stamp detects changes; it is not a lock,
permission, resource owner or substitute for the final pair check.

The root builder claims its planning input once, associates it with a scan and
bounded record set, and clears borrowed references and input on success, error,
cancellation and panic through the same lexical owner. Nested statements,
prepared binding and retries cannot claim an earlier root's state. No plan
stores a native relation, transaction operator, callback or statement owner.

## Selection, observation and publication

The comparison owner classifies oriented column/bound pairs as raw, losslessly
normalized, inverse necessary range or unsupported. The type inverse supplies
logical arms to the common direct or serialized physical-leaf lowering.
Ordinary exact serialization and equality-pinning callers retain their stricter
requirements. The selector owns enumeration and one scorer; existing FORCE,
small-table, sorted-scan and spatial policies keep their normal priority.

A necessary-range candidate is considered after ordinary matching owns neither
a winner nor a fallback. Its private mapping, bind tag, full residual and block
hint become visible only after actual hidden-table resolution, pair validation,
reentry checks, budget checks and cancellation checks succeed. Ordinary
resolution and its errors occur at their existing checkpoint, even if optional
costing later keeps BASE. No extra relation lookup is introduced.

An already eligible ordinary integer covering-index winner can also be
compared with complete observed BASE work. This optional observation does not
set the captured-execution requirement. An incomplete observation preserves
the ordinary choice. Before constructing executable predicates for a possible
necessary-range winner, the selector can reject a losing candidate using a
valid lower cost bound; possible winners are still priced by the same scorer
with their real predicate work. Unknown or invalid pricing retains the normal
fallback and error path.

The observation walks cached metadata under the existing attempt budget and
read identity. It tries the existing subscription, engine, transport and cache
locks instead of waiting; unavailable or changed input discards all partial
counts. It closes its iterator and rechecks statistics, read identity,
subscription and transport before use. Cancellation and substantive error
precedence stay with the existing operation; optional ordinary refinement does
not suppress cancellation or change mandatory resolution errors.

S3-FIFO `Get` and `TryGet` share the same locked lookup, admission, retention
and frequency behavior. `TryGet` differs in acquiring the index-shard mutex
with one try; a miss or contended shard does not admit or retain a value.
Callers requiring bounded work must supply bounded, non-reentrant hash and
callbacks. The current metadata payload is immutable Go-managed bytes with
no-op retain/release; observation keeps the slice for its synchronous lifetime
and returns copied column values. Metadata aggregators must clone the first
cached zone map before updating it; `MaxAndMinValues` otherwise mutates the
shared cache while observers copy it. No cache-wide lock or per-read copy is
added. A future pooled or mutable payload requires a new lifetime proof.

## Independent filter and execution facts

`CandidateFilter` means the producer proved an independently derived storage
condition is necessary and safe to evaluate early, including diagnostic and
ancestor-work preservation. It does not imply authorization, placement,
captured execution, ordered seek or raw-key compatibility. The producer marks
the root of an independent block-filter entry. Exact copying, folding,
remapping, statistics recomputation and transport preserve the proof; a hint
whose equivalence or owning scan cannot be preserved is discarded. Marked and
unmarked filters do not deduplicate one another. Ordinary filters remain
unmarked unless their own producer establishes the contract.

`IndexScanInfo.requires_captured_execution`, protobuf tag 7, is set on a
physical index scan only after the necessary access has passed its admission
and publication checks. The compiler rejects malformed flagged nodes and
requires the captured process/clock/zone, fixed offset, non-offloaded pinned
local scheduling and the expected current CN. A folded or removed filter does
not remove that requirement. The compile owner retains a sticky topology fact
across retry and clears it on pool reset; frontend validation and an unscoped
rebuild use the same physical requirement. Drift is rejected before message
board or deferred transaction-offset publication, and the safe rebuild
reauthorizes the original statement without captured planning input.

The ordinary compile constructor owns immediate publication, deferred
preflight publication and definition retry at their existing offset and
message-board checkpoints. Cancellation, substantive errors, retries, panic
cleanup and pool lifecycle retain their existing owners. Regular-index and
index-merge failures return through the planner's normal error path; a failed
child rewrite does not publish a replacement child ID or partial branch.

Frontend plan reuse derives the separate ordinary-generation, prepared-logical
and prepared-physical decisions at the existing policy owner. Existing
foreign-key, view/UDF/subscription, index-coverage, DDL-default, time-zone,
percentile/EWM and snapshot checks retain their layer-specific meaning.
Unrelated ordinary and prepared queries do not acquire a new execution intent.

## Reader and sort-key admission

A marked hint may prune only after the reader proves that each supported leaf
matches the stored comparison domain. Unmarked OR retains its existing
metadata-load and error behavior. For a marked OR, an unknown arm retains data;
malformed OR declines optional compilation. A seek can skip a block only when
its required bound is known: an unknown lower proof starts at zero, and an
unknown upper proof retains the lower seek. Ordinary seeks keep their existing
behavior.

### Variable-length zone-map upper bounds

A stored variable-length maximum with 30 `0xff` bytes cannot by itself prove a
finite upper bound. The current writer marks a longer maximum with the existing
`MaxTruncated` bit; an older V3 object-summary writer could omit that bit while
copying a block maximum. The same bytes can also represent an exact 30-byte
value or the rounded upper bound of a different long value. Without producer
provenance, the zone-map owner treats that maximum as potentially unbounded in
both flagged and unflagged form. The bit itself and stored format do not change.
All other valid rounded maxima below that sentinel remain finite. The minimum
remains a conservative lower bound and can still disprove a match.

The shared zone-map owner applies this envelope to strict scalar comparisons,
zone-map-to-zone-map comparisons, prefix equality/ranges, ordered seek and
stop, and sorted-vector membership. A missing upper proof retains the block or
vector tail; it is not represented as a false match. Prefix checks retain the
existing operand orientation and may use a provable theoretical bound for short
prefixes. The vector and colexec singleton owners must not turn an ambiguous
upper endpoint into an exact value. `ZMToVector` represents an unknown upper
as NULL, and the existing zone-map merge owner preserves truncation provenance;
callers retain their own sum-handling rules.

Sorted raw-byte membership may retain extra candidates: every actual key is at
or above the stored lower bound, and every key inside a finite upper bound is
at or below that upper bound. The existing prefix-based binary searches are
therefore safe only as conservative supersets; their global predicate need not
be monotone outside that interval. When the maximum may extend, the upper end
of a candidate vector is its length. Callers must not treat these bounds as an
exact SQL equality proof, especially for padded CHAR or non-byte collations.
Empty and nullable-vector behavior remains with the current vector owner.

The admitted scalar forms cover the current temporal producer's equality,
BETWEEN and serialized prefix equality/BETWEEN, with compatible integer and
decimal controls. Bound types and fixed payload widths must match; existing
scale-aware zone-map comparison and Unicode collation restrictions apply.
Float, unsupported types, marked IN/vector forms and other unproved leaves
fail open. The common upper-bound correction replaces marker-only strict-byte
and long-prefix guards; the current serialized temporal endpoints are short
and have a type-code prefix.

Raw PK/memtable filtering independently requires its actual encoding and
ordering proof, including bound type and decimal scale. Marked scalar CHAR
declines because SQL padding can equate different byte strings; Unicode and
other unproved domains also decline. In AND, separately proved arms can still
help; in OR, an unsupported arm prevents unsafe pruning. The ordinary unmarked
PK path and temporary-vector cleanup remain owned by the existing reader.

## Compatibility and validation

The feature changes no table/index storage, SQL syntax or configuration. Saved
query-result metadata can contain serialized plans used for privilege checks;
new optional fields must decode safely when absent, and index scans must still
resolve privilege checks to their parent table. The supported upgrade contract
is a stopped upgrade from 4.2; mixed-version execution and downgrade are outside
this change. A synthetic plan encoded with the v4.2.8 schema can check wire and
authorization compatibility, but is not a live stopped-upgrade test.

Validation must exercise the real owners and entry points: comparison
boundaries and full SQL results against BASE controls; sparse/dense clock
transitions; ordinary and prepared reuse; authorization and schema changes;
read-identity drift, contention, cancellation, retries and failed publication;
marker copy/fold/transport; malformed, unknown and unsupported reader/PK
inputs; and saved-plan privilege allow/deny. Test flagged and old-writer-shaped
unflagged maxima, scalar and prefix bounds, sorted/nullable membership,
multi-block seeks, cached-metadata immutability under concurrent aggregation,
and malformed/oversized index metadata with ordinary-reuse controls. Normal and race owner tests,
public SQL/BVT and incremental static checks cover their affected scopes.

Performance comparisons use matched source bases, native libraries, data,
settings and topology, with same-binary A/A controls and interleaved runs.
Report scan rows/blocks, p50/p95, CPU and allocation scope, including ordinary,
prepared and scan-free controls. Optional observation may decline on contention;
its cost and measurable changes to ordinary cache lookups must be reported,
not assumed away. Ambiguous maximums may retain extra blocks or vector
entries; report that rare-domain tradeoff alongside unaffected controls.
