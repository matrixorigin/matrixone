# Immutable JSON read ownership

Owning issue: #28037. Consumer: [PR #28275](https://github.com/matrixorigin/matrixone/pull/28275).
The ownership foundation is [PR #32546](https://github.com/matrixorigin/matrixone/pull/32546),
a separate dependency of #28275. Both PRs remain subject to the design gate for
the complete series; splitting the implementation does not approve its design.

This dependency provides binary JSON admission and opaque, immutable read
owners. SQL syntax, overloads, response policies, persisted expression admission,
and operator delivery belong to consumers and are outside this change.

## Design decision D1: review target and acceptance

Status: **PROPOSED; independent design approval pending** (2026-11-11).
This section is a design-only amendment to the implementation reviewed as
`4df268ea5f21882599fdfd6f415b67f849b99675` ->
`42ea0b3940fe9d9b41f9d37daee767a4de3d2fe6`. The detailed mechanism and revision
history below remain part of this design. The approving review must identify
the exact commit containing D1, this document, both PRs, its decision and any
remaining conditions. An author update, publication permission, or a passing
test is not that approval.

The change crosses Vector, FileService, ObjectIO, storage publication and
execution-resource ownership. It adds immutable versions and modifies cache-pin
admission and execution-generation initialization, so the architectural design
gate applies regardless of SQL syntax changes or the standalone PR's label.
Implementation approval remains blocked during this distinct design-review
phase. Historical paragraphs headed "Review decision" record development
decisions; none is a claim of an external reviewer approval of D1.

### Problem and baseline

Main `4df268e` decodes admitted binary JSON for a legacy lookup without a fresh
complete traversal. Ordinary Vector APIs can subsequently expose writable
payloads and descriptors, so admission at an earlier write is insufficient to
justify unchecked reads of the current mutable input. The defensive #28275
implementation validates the complete current document on every lookup. That
preserves malformed-descendant rejection but makes a small-field query depend
on unrelated document contents, repeatedly for each path and expression.

The required invariant is complete validation of a stable owned version before
any unchecked query, including unselected descendants. Repeated queries may
reuse that conclusion only while descriptors, payload, NULL/constant metadata
and row mapping remain owned and immutable. Unknown or ordinary mutable input
must retain complete validation. A pointer, length, lease or flag alone is not
the ownership proof. The existing binary JSON representation, query engine,
reference-counted leases and query/CN memory accounts are the constraints and
local precedents; this design introduces no new interoperability format.

### Alternatives and selection

| Option | Correctness and cost | Complexity, compatibility and validation |
| --- | --- | --- |
| Keep defensive validation before every lookup | Meets the malformed-input contract; Q lookups pay Q complete traversals, even for a fixed small path. | Smallest implementation and unchanged formats, but retains the measured common-path regression. Easy to test; retained as the ordinary mutable-input fallback, not the normal immutable route. |
| Always copy and fully validate a requested column into a private snapshot | Meets the contract and shares one validated version across paths; every producer pays a full descriptor/payload copy, including a private builder or sealed storage result. | A credible simpler ownership boundary with unchanged formats and straightforward isolation tests. It increases peak live memory and scan-copy cost even where a producer can safely transfer private storage. It is the fallback for external aliases, not the preferred route for every source. |
| Make all Vector writes copy-on-write and encapsulate every writable alias | Could support reusable validation if every descriptor, area, NULL and row-map mutation creates a new version. | Requires changing or restricting existing byte/slice access and all mutation, window, transport and reuse paths. Compatibility is at the API and ownership level despite unchanged disk bytes. Migration and testing extend beyond JSON_VALUE; a single missed writable alias invalidates the guarantee. Not selected for this series. |
| Private producers publish immutable owners; other inputs use snapshots (selected) | Complete admission occurs before publication. Private builders/storage avoid an extra document copy; externally aliased input copies once. Paths share the same version. | More publication/lifetime machinery than snapshot-only, bounded to actual consumers and the two private owner implementations. Reuses existing formats, leases and accounts. Requires producer-to-consumer, alias, mapping, cancellation and accounting tests, plus end-to-end cost measurement. |

Header-only/path-local checking and a trusted flag are rejected because they
cannot preserve complete rejection of malformed unselected descendants or
protect mutable aliases. A path cache would address a different cost and is
outside this decision. No second partial JSON decoder is introduced.

### Architecture, resource bounds and fallback

The publication point follows private construction, complete admission and
successful backing/metadata reservation. Failure publishes no handle. A view or
result retains its owner; the last effective reference releases the backing and
its shared pin charge exactly once. Handle and row-map charges have their own
bounded lifetimes. Selection copies indices, not whole documents per path;
constant repetition stores one row and a logical length. Reset, Free, new batches
and prepared rebinding do not inherit another version's qualification.

For B payload bytes, D descriptor/NULL metadata, N document nodes, Q lookups and
S selected rows: a snapshot uses O(B + D) private backing and O(B + D + N)
construction/admission work. A private transfer avoids an additional full
backing copy. Admission retains the existing depth, byte and traversal-work
bounds; it does not allocate width-dependent child/frame tables. Subsequent
queries pay existing path parsing/lookup and owned output only. A selection
uses O(S) accounted indices; its owner is shared rather than multiplied by Q.

SQL backing, pin capacity, metadata and maps use the existing finite statement
and CN budgets. Physical retained capacity is charged, not just logical bytes;
missing accounting authority takes the accounted snapshot path. Reserve before
retain/publication, and undo reservations on every partial failure. Retained
results can outlive a reader, so closing a reader alone is not a zero-memory
assertion; after the last consumer closes, this feature's live references and
charges must be zero. There is no global cache, cleanup worker or new disk use.

Query-result backing is separate from the source pin. The existing ByteJson
path traversal reserves each growing match table and any merged-array backing
before allocation; temporary match charges end after traversal, and a merged
result charge belongs to its result or cursor until Close or ResetQuery.
Insufficient budget is a hard execution error. The ordinary ByteJson query API
is unchanged; the budget-aware variant neither validates bytes nor grants a
reader capability and is used by the already-admitted private readers.

Text returned from a reader or query result has its own explicit, independently
charged handle. It survives closing the source reader, and its consumer closes
the handle after copying into the caller-owned destination. Handle copies share
one release; none may be used after any copy is closed. Formatting reserves
temporary storage before conversion, then retains a separate output charge.
Allocation/admission errors are terminal, not JSON_VALUE conversion errors.
For a simple path, an eligible string or bounded numeric/literal can instead be
copied synchronously into the existing accounted function result. Source bytes
never leave the reader; each reader's fixed scalar-formatting scratch is part
of its accounted handle. Calls on one reader are sequential; independent
readers can be used concurrently. Other paths and types use the owned selection.
For composite and extended values the existing formatter is admitted with a
conservative temporary bound of `32 * encoded bytes + 2304` (plus the owned
output charge), so a tight statement budget can reject before the actual peak
would consume that amount. This bound favors safe admission over precise
measurement; it is not a claim of minimal memory use for these values.

Memory-only admission obtains memory authority for the existing execution
generation, not spill authority. Promotion initializes spill capacity on that
same generation before any spill-capable handle is returned. It must preserve
existing reservations, locking, limits and closed-generation behavior. Known
FileService allocators may create new private storage; an unknown allocator
uses the default private-allocation path rather than lending qualification to
caller-controlled storage or rehome callbacks.

Unsupported conversions materialize independent mutable data and lose the old
qualification. A malformed candidate publishes no reader; the consumer may
defer to the ordinary fully checked evaluator to preserve NULL/mask/error order.
Allocation, account and cancellation failures remain terminal. Concurrency is
limited to independent retained readers; concurrent source writes or Close
racing operations on the same handle are unsupported. Releasing backing or
returning output never exposes writable aliases to another consumer or tenant.

Disk, wire and catalog formats remain unchanged. The series follows the
repository's shutdown-upgrade contract, not mixed-running-version or old-node
rejoin support; unrelated existing capability gates remain intact. Removing
consumer demand returns to the defensive evaluator and discards read owners,
without an on-disk migration. A rollout still requires supported-release data,
recovery and deployed QA evidence at the applicable consumer boundary.

### Fixed quantitative acceptance criteria

These are merge/acceptance requirements, not a claim that the current code has
passed them. Permission to publish a Draft does not change the numbers.

| Boundary | Required result |
| --- | --- |
| Reuse | After successful admission of one version, additional 1/2/8 path queries add zero complete-document validations and zero full-document snapshot copies. At most one snapshot is shared for the same input version within its consuming evaluation. |
| Unselected growth | With a fixed `$.keep`, increasing unrelated elements from 16 to 4096 adds no query-phase validation or full-document copy. Initial admission and externally aliased snapshot construction remain explicit costs. |
| Resident/64-row performance | For each integer, string, mixed, wide and deep scenario and its 1/2/8-path inputs, five alternating paired candidate/base ratios have median <=1.20. Compare the actual legacy baseline; the seven-argument comparison covers only the shared short-text result domain. |
| Actual SQL | Five alternating paired ratios have median <=1.10 for every fixed scenario. Timing includes planning, cold scan, admission, copying, evaluation and result release. Resident setup exclusion cannot establish this result. |
| Memory and release | Charge all introduced live backing, pins, metadata and row maps before publication. Document storage and pin capacity do not multiply with path count; after all results/readers close, introduced live charges and references are zero. Record B/op, allocs/op, copies and per-domain/process peaks without adding overlapping counters. |
| Unused feature | Queries without JSON_VALUE demand create zero additional snapshots, JSON owner/pin reservations or publication maps through this facility. |
| Evidence identity | Same environment/toolchain/native inputs and harness in both arms; retain all five pairs and result oracles. Noise that prevents a decision is INCONCLUSIVE, not PASS. Observations use tests/probes, not new production high-frequency counters. |

These criteria apply to the composed series, not to a standalone dependency
that has no SQL consumer. Ordinary mutable-input full-validation benchmarks
remain separate and must disclose their cost; they cannot be relabeled as
immutable-reader results. A numeric fast path alone cannot satisfy coverage.
Any proposed numerical exception requires an explicit new decision naming the
policy owner, scope, rationale and exact measurements; it cannot be inferred
from publication permission.

### Measured costs and their decision status

The #32546 publication record reports main `4df268e` versus composed tree
`0b0eaa2464c6268edbb4855dbbe1c33e5c7540d3`, tested as `d64cd49` and also present
at `9b47393`. Ubuntu used matching toolchain/native inputs and five AB/BA pairs.
These are composed-tree observations, not standalone-foundation performance or
independent reviewer reruns.

| Observation | Paired median | Decision status under the fixed criteria |
| --- | ---: | --- |
| Worst actual SQL, deep/16/1 path | 1.0979x | Within 1.10x; limited margin, not proof of production throughput. |
| Worst warm lookup | 1.0084x | Within 1.20x. |
| Legacy 64 rows, wide/16/8 paths | 1.2034x | Above 1.20x; requester accepted this measured cost; independent review OPEN. |
| Contract 64 rows, integer/4096/1 path | 1.2103x | Above 1.20x; requester accepted this measured cost; independent review OPEN. |
| Contract 64 rows, string/16/1 path | 1.2007x | Above 1.20x; requester accepted this measured cost; independent review OPEN. |
| Mutable input, 4096 elements | 9.7723x | Disclosed defensive fallback; not an immutable-query PASS or an accepted blanket workload cost. |

The requester explicitly accepted these measured costs for publication. The
three numerical targets above were still exceeded; the requester's acceptance
does not itself approve the design or establish reviewer/QA acceptance. The PR body
retains per-arm ns/op, B/op, allocs/op and all three above-target ratio series.
Twenty expression windows had no observed major faults or global swap; two
global swap-in pages occurred in one SQL baseline window. These observations
do not prove absence of scheduling noise. Raw evidence remains author-retained;
independent reproduction is not claimed.

### Validation and approval record

| Contract | Minimum proof and current evidence boundary |
| --- | --- |
| Complete admission and mutable aliases | Owning tests for self/overlap/bounds/encoding/depth errors, source/descriptor/inline mutation, repair and legal same-size mutation; no malformed version may publish. |
| Lifetime and coordinates | Independent readers/results, dictionary reorder/repetition/NULL, nested selection/window/repeat, failed allocation, Reset/Free and deterministic scoped race; final charges zero. |
| Private cache and budgets | Malicious/unknown allocator and retained rehome aliases, physical pin accounting, failed/cancelled reservations, memory-only promotion and ordinary full-budget controls. |
| Consumer and SQL | Actual storage, memory, remote reception and constructor routes, filter/projection and prepared reuse; two overloads and response semantics belong to #28275. Standalone owning tests do not substitute for SQL. |
| Performance | Fixed five-pair protocol above. Three composed cases remain above target; historical results on another base are not this candidate's PASS. |
| Recovery and native risk | Attribute existing supported-release reports to their actual code/data. The historical proot native release error is unclassified; native passes do not erase it. A candidate-specific reproduction requires repair, and reviewer/QA disposition is required before merge. |

The approval record is intentionally explicit:

| Field | Current value |
| --- | --- |
| Design revision | D1 plus the detailed mechanism/revisions below, at the exact commit carrying this amendment. |
| PR series | Foundation #32546 -> consumer #28275; owning issue #28037. |
| Phase | Design review; implementation approval not yet granted. |
| Independent reviewer and immutable review URL | PENDING; none supplied or fabricated by this amendment. |
| Design decision | PENDING independent review; publication and local validation are not substitutes. |
| Implementation/performance acceptance | OPEN, including the three fixed-threshold failures above. |

To close the design gate, record an actual review naming the exact D1 commit
and its PASS/REQUEST_CHANGES decision, resolved alternatives and cost decisions,
and both PRs. Only then resume implementation approval. Design approval alone
does not turn the remaining numerical failures, native-risk disposition, CI or
deployed QA into PASS. A material ownership/accounting/compatibility change
requires review of the affected design decision again.

## Invariant and state

A readable version owns every descriptor, payload, NULL/constant flag and row
mapping used to interpret its contents. Its complete documents, including
unselected descendants, passed canonical validation before publication.

The lifecycle is private construction -> complete validation -> read-only
publication -> reference-zero release. Failure publishes no consumable owner.
A lease on ordinary mutable storage does not prove immutability. Reset, Free,
address reuse, a new batch, and another query never confer qualification.

Snapshot construction requires the caller to keep its input stable during the
call. It copies the descriptors, payload and necessary metadata, validates the
private copy, then publishes. Later source mutations do not alter that version;
another snapshot sees updated input or rejects corruption. Concurrent mutation
of the source during construction is unsupported.

The private builder copies each appended payload, validates it before recording
the row, and transfers storage only at Finish. Its writable storage has never
escaped. A failed append or Finish retains cleanup ownership; Close is safe on
all terminal paths. This is not a sealing operation on an arbitrary Vector.

ObjectIO can retain its existing sealed validated cache representation. Only
ObjectIO's private marker grants access to the already validated backing. A
generic cache Data value instead uses checked decoding and a private snapshot.
Cache leases pin that immutable version; ordinary cache access still does not
export its writable bytes.

## Interfaces and release

JSONReader and its result/cursor implementations are sealed within the defining
packages. They expose logical length, retained window/selection views and query
operations, not internal slices, mutable Vectors or externally writable ByteJson
values. Results expose typed scalars and owned strings, or append copied output
to the caller's destination. Existing path parsing, querying and formatting are
reused; no partial decoder, trusted-byte switch or new wire format is added.

A view holds an owner reference. Selection indices are copied into accounted
private storage and composed with existing row coordinates. A retained cursor
survives closure of its parent handle. Closing a handle is idempotent; callers
must not operate on the same handle concurrently with Close. Independent
retained readers may read the frozen owner concurrently.

Materialize is a fallible boundary that returns independent writable data. A
failed allocation leaves the original read owner usable. No ordinary mutable
Vector inherits qualification from materialization or a retained cache lease.

Backing and selection allocations use existing allocation-account selections
and leases. There is no global cache or background cleanup. All references and
charges must return to zero after their last consumer closes. Query output
allocation errors remain errors for the consumer to propagate.

## Cost and validation

Snapshot publication pays one descriptor/payload copy and one complete
validation. Builder publication pays one source-payload copy and admission;
Finish does not repeat admission. Sealed cache reads retain existing validated
storage without another document copy or scan. Additional queries of a version
pay existing path lookup and owned result formatting only, independently of
unselected descendants. Views do not copy complete documents per path.

Canonical admission checks scalar encodings, finite floats, shortest varints,
UTF-8 keys/strings, sorted unique keys, bounds, nonoverlapping ranges, depth and
bounded traversal work. It retains no width-dependent child/frame list.
Within an object each key is charged against the remaining-byte budget before
value callbacks or child traversal. This retains the amplification bound,
including for ordinary validation that permits overlapping key ranges.
The canonical stored validator may process adjacent strings as one run: it
checks each descriptor, shortest length prefix, range and UTF-8 payload while
enforcing running byte and node budgets. It commits those charges before any
other scalar callback or child descent. Generic validators keep their existing
callbacks; the optimization never accepts an unvalidated document.
For canonical object keys, a single UTF-8 scan may cover contiguous ranges
after each key's range, ordering and first-byte boundary are checked. Gaps end
the scan and are excluded. This cannot join an incomplete rune from one key
with a continuation byte from the next; the scan completes before values.
GetBytesAtChecked provides bounds-checked access to mutable descriptors but does
not validate JSON or establish read ownership.

Owning tests cover source/descriptor/inline isolation, failed allocation,
partial construction, independent reader lifetimes, selection/window mapping,
const/NULL domains and scoped concurrent reads. Coverage-count workloads compare
1/2/8 queries and require invariant admission/copy counts. Cache tests cover
sealed and unmarked data, owner retention and zero extra payload allocation.
Consumer SQL, end-to-end performance, supported-release recovery and independent
review remain separate acceptance work; local owning tests cannot establish them.

### Private cache ownership boundary (candidate revision 28)

The pre-existing ObjectIO validation marker is not an immutable-ownership
proof: its validator accepts caller-owned `fscache.Data`, and its public rehome
callback can retain the source or destination writable bytes. Both behaviors
are covered by reproductions. JSON readers therefore must not derive a private
owner from that marker alone.

A requested JSON column may be published from a distinct ObjectIO-owned cache
version only when its constructor created a fresh allocation, retained every
writable view locally through decompression and complete validation, and
published the wrapper without exposing those views. The constructor obtains
fresh storage through a FileService allocator adapter restricted to the known
allocator implementations; it uses non-recycled allocation and falls back to
the FileService default for an unknown allocator. Selection of an allocator, a
lease, or a marker does not by itself upgrade pre-existing data. Only the
constructor performing the entire private creation and validation can publish
the capability. Non-JSON_VALUE reads keep their existing path.

The new private wrapper must not lend its backing to the public cache-rehome
callback. Rehoming passes a copy and drops JSON-reader qualification, because
the callback can retain or replace its output. Ordinary or remotely received
cache data is copied and fully admitted before use as a JSON reader; an old
reader remains stable if the source changes, and a new admission observes the
changed source or rejects it. Query result, descriptor, row map, null metadata
and lifetime rules remain unchanged. A safe cache miss can use the direct
private constructor; a pre-existing unqualified cache hit incurs explicit
readmission, shared by the consuming batch's JSON_VALUE expressions.

Review decision: the previous general cache marker cannot support the claimed
immutability contract and its use as reader authority is rejected. The revised
boundary must pass alias-retention, malicious-allocator, rehome, independent
reader/lease, production scan and end-to-end cost checks before acceptance.
This is a development review decision, not an external reviewer approval.

### Failed admission and evaluation order (candidate revision 29)

An eager batch snapshot can encounter a malformed document that the original
JSON_VALUE evaluator would never read, for example because its path is NULL or
the row is masked. Returning that candidate-admission error before expression
evaluation changes both overloads' short-circuit and error precedence. Tests
reproduce this mismatch.

The snapshot operation will distinguish only a failed document/descriptor
validation from allocation, accounting, cancellation and programming errors.
A failed validation publishes no reader and confers no trust; the caller uses
the existing ordinary evaluator, which fully validates each document it
actually reads and preserves the original path, mask, DEFAULT and error order.
Other failures remain terminal. This exceptional fallback is scoped to the
current evaluation; successful versions retain the existing one-snapshot,
shared-reader path, and no error is routed to ON ERROR merely by deferring it.
Batch publishers may decline a malformed candidate without retaining partial
qualification; a later consumer makes the same safe decision.

Review decision: a failure classification is not evidence of validity and
never permits unchecked bytes into the trusted query path. Complete admission
remains mandatory for every published document version, including unselected
descendants. Tests must cover both overloads, active versus skipped corruption,
error precedence, and allocation/cancellation failures before acceptance.

### Accounted cache pins (candidate revision 30)

A retained cache lease prevents eviction while any reader, window, selection or
result remains live. Reporting its size is not itself an admission charge. The
private cache reader must reserve the cache allocation's physical capacity
against the existing statement account before retaining it; the last shared
reader/result release returns that charge exactly once. An explicit vector
allocation selection uses its existing capacity class and account. Where that
selection is absent, the SQL scan supplies its existing execution-generation
transient-memory admission. An internal caller that supplies neither cannot
borrow the cache owner: it constructs an accounted private snapshot instead.

Reservation failure or cancellation is terminal and must leave the cache
reference, source version and all accounts unchanged. Selection and window
views share the owner's single pin charge; row-mapping allocations retain their
existing separate accounting. Only a requested JSON_VALUE scan installs the
admission callback. The physical capacity, rather than logical payload length,
is charged; no cache flag or lease alone grants immutability.

Review decision: the earlier size-only cache lease does not meet statement
accounting and is rejected. Accounted/unaccounted-call paths, bounded failure,
retained result lifetime, and real SQL performance must be revalidated.

### Constant row views (candidate revision 31)

A one-row immutable owner may serve an arbitrary logical constant batch without
constructing a batch-sized temporary row-index array. A repeat view retains the
same owner and stores only the selected physical row and logical length.
Windows, selections, cursors and materialization compose with that mapping;
the owner's final release remains responsible for its existing accounting.
The operation rejects an invalid row or negative length and publishes no view on
failure. Repetition does not admit new bytes or repeat document validation.

Review decision: an unaccounted O(batch rows) temporary index array is not an
acceptable way to build a constant view. The compact mapping removes that array
and its second, accounted copy. Tests must cover nulls, nested selection/window,
reader closure, independent cursor lifetime and final account release for both
private and cache-backed owners.

### Closed reader consumption (candidate revision 32)

The Go JSONReader interface alone does not seal its implementations: another
package can wrap a real owner and override its query methods. JSON_VALUE's
reader entry must therefore accept only the concrete private Vector owner or
the concrete private ObjectIO cache owner, checking identity before calling any
interface method. Windows, selections and repeat views preserve those concrete
owner types. An unrelated implementation or a wrapper around a valid owner is
rejected; it cannot manufacture read authority by exposing a bool, a lease, a
buffer address or arbitrary callbacks. The identity predicates confer no new
qualification and accept no byte payload.

Review decision: interface documentation by itself does not meet the closed
capability requirement. Add a forged-reader regression and preserve deterministic
cancellation tests without letting production accept arbitrary readers. Earlier
references to sealed interfaces mean this enforced consumption boundary, not a
claim that the Go compiler prevents other packages from implementing an exported
interface.

### Reader and publication metadata accounting (candidate revision 34)

Document backing, descriptors, row maps and retained cache capacity are already
charged, but a live Go reader/window/cursor and the publication maps have their
own heap footprint. A lease size field does not reserve that memory. Each SQL
owner must therefore carry the existing statement memory admission into every
retained handle. An explicit allocation-account selection takes precedence;
otherwise the caller supplies the execution-generation transient-memory
admission. Standalone callers without any statement account retain their
existing standalone API; every SQL producer and receiver must provide one of
the two. No boolean or user-provided bytes become admission authority.

Reserve a conservative bounded metadata charge before creating each private
owner, retained view, cursor, or query result. The owner charge follows the
backing lease to its final release; handle charges end when that handle ceases
to be retained. A cursor copies immutable row coordinates and retains the
backing lease, rather than keeping an otherwise-closed reader object alive.
A selected view also covers its row-map Vector and lease metadata; the actual
row-map backing remains separately charged by the MPool. Publication and
expression-binding maps/slices must have their own scoped reservations and
release them on failure, cancellation, reset and free. Unused JSON_VALUE paths
must not create these reservations or snapshots.

Review decision: Go heap metadata is additional memory even when its payload
is small; omitting it violates the stated accounting contract. Charges may be
conservative rather than exact Go allocator/RSS measurements, but must cover
all live control objects and avoid both double-charging the backing and hiding
failed reservations. Tests must demonstrate account growth for retained
views, cursors and results, independent lifetimes, zero final charge, admission
failure atomicity, real SQL budget usage, and no new work for unrelated queries.
All performance gates must be remeasured with these reservations enabled.

### Synchronous text selection (candidate revision 35)

An already-live immutable reader may perform a synchronous text selection
without constructing an independently retained cursor. The caller keeps the
reader alive throughout the call, including against concurrent Close; separate
reader handles remain safe for concurrent use. This operation returns only an
owned string, value type and scalar status/error classification. It uses the
existing validated row lookup, path parser and QueryWithExists implementation;
it accepts neither arbitrary document bytes nor a trusted flag. The existing
retained cursor API remains available for typed or longer-lived results.

Provide both a path-bytes operation preserving the legacy NULL-before-path
short circuit and a parsed-path operation for consumers that validate the path
before checking document NULL. Both reuse the same selection and unquoting
logic as the retained cursor. Synchronous scratch must remain on the stack or
otherwise be charged; output must be independent of the retained backing.

Review decision: the accounted benchmark measured cursor creation as a fixed
cost on every short lookup. This optimization does not change admission or
reduce validation of unselected descendants. Verify path/mask/error ordering,
owned output, independent concurrent readers and allocation behavior, then
repeat the matched performance gates.

### Memory-only admission before spill setup (candidate revision 36)

A JSON reader needs the existing query/CN memory generation but never obtains
spill-disk authority. Allow it to capture a narrow transient-memory reservation
function for that same generation without sampling the spill filesystem. Do
not expose the underlying generation through that function. A subsequent
ordinary resource-budget request must initialize spill capacity on that exact
generation before returning a spill-capable handle; it samples the filesystem,
applies existing epoch/explicit-cap rules, and sets the per-generation limit
from the resulting live ledger. No spill reservation can precede promotion.

The readiness state is generation-local, reset with the generation, and guarded
by the existing process/budget synchronization. Only initialized generations
may cross into a separately locked borrowed process. Full-budget callers that
open a new generation and explicitly installed test generations keep their
existing behavior. A closed generation remains closed; promotion must neither
replace it nor move existing memory reservations. This adds no global cache or
background task.

Review decision: the small SQL profile attributed significant CPU to spill
filesystem sampling initiated solely for JSON memory accounting. Deferring
that unrelated work is acceptable only with evidence for exact-once promotion,
current disk and explicit limits, no stale or premature spill authority,
concurrent callers, borrowed processes, failure/cancellation and final account
release. Validate common structure sizes and rerun the complete SQL gate.

### Owned text from an admitted JSON string (candidate revision 37)

For `TpCodeString`, the existing unquote operation returns the stored payload
as an owned Go string; legacy float formatting has no effect on that type. A
private reader or cache reader may make the same owned string conversion
directly after selection. This relies on the complete admission check having
already verified the canonical length and UTF-8 payload, including for nested
and wildcard-selected values. It must not expose a slice or a string alias to
the retained backing, and it does not relax checks for any other type.

Review decision: compare both concrete readers with the existing unquote
operation for empty, escaped, multibyte and NUL-containing strings, including
single wildcard matches. Verify that results survive reader and cache-owner
release, and rerun the scoped concurrency and paired performance checks.

### Dictionary inputs (candidate revision 38)

A dictionary Vector is an ordinary source, not evidence of admitted JSON.
Before materializing selected rows, check each non-NULL physical descriptor
without exposing its backing. Copy the mapped payloads and logical row metadata
into one private flat version, then validate every copied document before
publication. No validation qualification crosses the dictionary boundary.
Allocation failures remain terminal and leave the source unchanged; malformed
descriptors or JSON report invalid input so evaluation can preserve its existing
row and NULL short-circuit order. An admitted reader owns its copied version
independently of the dictionary and its source.

Review decision: exercise reordered and repeated rows, NULLs, malformed
unselected descendants and descriptors, allocation failure, and release of the
dictionary before reading the snapshot. The dictionary source's selected rows
are copied once and fully validated once; subsequent path queries reuse the
published version.

### Transient-only physical backing and legacy aggregate text (candidate revision 39)

An input scope can have a statement transient-memory admission without an
allocation-account selection. In that case, snapshots, private builders, row
maps and mutable materializations use a transient allocation policy. It is a
memory policy only: it does not attest JSON validity or make mutable bytes
trusted. If an allocation account is present, it still takes precedence.

Each new physical MPool block reserves its requested capacity plus a bounded
allowance for registry and release metadata before allocation. Its physical
registry entry retains the admission and release until Free or terminal pool
cleanup. Growth and reallocation reserve the complete replacement before
copying and freeing the old block, accounting for the temporary peak. Failure
releases the attempted reservation and preserves the old backing. This also
applies to bitmap/sidecar storage and to vectors materialized from either
reader. Retained windows and selections share their owners; no admission result
is inferred from a pointer, length, or lease. Callbacks used by independent
readers must be safe for concurrent use.

Canonical JSON admission validates UTF-8, while legacy CHAR/VARCHAR/TEXT
values may contain malformed bytes. JSON_ARRAYAGG and JSON_OBJECTAGG retain
the original input for aggregation and DISTINCT, then replace each invalid
byte with U+FFFD when writing the final JSON string or object key. Object keys
are normalized before final ordering and duplicate-key selection; the last
entry still wins. The accounted encoder reserves normalization scratch before
allocating it. Typed binary aggregate values retain their existing rejection;
binary object keys still follow the existing string-key entry path. JSON
inputs do not receive this text conversion.

This amendment addresses review findings on the transient-only budget and
legacy aggregate encoders. Independent approval of D1 and this amendment is
still pending. Required evidence covers finite-budget refusal, growth and
rollback, all three backing sources, retained owners, independent concurrent
readers, aggregate values and keys, normalized-key collisions, DISTINCT,
binary rejection and the production SQL terminal.
