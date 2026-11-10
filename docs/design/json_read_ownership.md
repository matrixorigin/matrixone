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
