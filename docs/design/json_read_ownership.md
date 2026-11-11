# Immutable JSON read ownership

Owning issue: #28037. Consumer: [PR #28275](https://github.com/matrixorigin/matrixone/pull/28275).
The ownership foundation is organized as a separate dependency of that PR.

This dependency provides binary JSON admission and opaque, immutable read
owners. SQL syntax, overloads, response policies, persisted expression admission,
and operator delivery belong to consumers and are outside this change.

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
