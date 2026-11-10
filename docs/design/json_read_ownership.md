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
Within an object the existing remaining-byte budget is checked against a
running total of key lengths and debited once before value callbacks or child
traversal. The running check retains the amplification bound, including for
ordinary validation that permits overlapping key ranges.
GetBytesAtChecked provides bounds-checked access to mutable descriptors but does
not validate JSON or establish read ownership.

Owning tests cover source/descriptor/inline isolation, failed allocation,
partial construction, independent reader lifetimes, selection/window mapping,
const/NULL domains and scoped concurrent reads. Coverage-count workloads compare
1/2/8 queries and require invariant admission/copy counts. Cache tests cover
sealed and unmarked data, owner retention and zero extra payload allocation.
Consumer SQL, end-to-end performance, supported-release recovery and independent
review remain separate acceptance work; local owning tests cannot establish them.
