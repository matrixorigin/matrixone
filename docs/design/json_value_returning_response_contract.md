# JSON_VALUE RETURNING and response contract

Issue: #28037

Compatibility note (2026-11-05): this candidate follows MatrixOne's
[shutdown-upgrade contract](../../.agents/skills/mo-dev/references/upgrade-compatibility.md).
It introduces no JSON_VALUE-specific MORPC generation or peer gate. Existing
independent capability gates remain unchanged. Performance, supported-release
upgrade, recovery and review evidence remain separate acceptance items.

## Contract

`JSON_VALUE` accepts a document and path, with optional `RETURNING`, `ON EMPTY`,
and `ON ERROR` clauses. MatrixOne keeps its existing extension that permits an
expression (including a prepared marker) as the path. In the clause-bearing
form, a NULL path returns SQL NULL, and every non-NULL path is validated before
the SQL NULL document shortcut; an invalid path remains a statement error. Bare
two-argument calls preserve the legacy SQL NULL document shortcut, including
when the supplied path is invalid. The new grammar accepts the same expression
path as the legacy form.

For clause-bearing calls, the default return type is nullable `VARCHAR(512)`
in `utf8mb4_bin`. Bare two-argument overloads 0/1 retain their legacy unbounded
VARCHAR metadata and text behavior. Supported
explicit targets are `CHAR`, `BINARY`, `SIGNED`, `UNSIGNED`, `DECIMAL`, `FLOAT`,
`DOUBLE`, `DATE`, `TIME`, `DATETIME`, `YEAR`, and `JSON`. `DEFAULT` values are
signed literals and are converted and validated while binding/preparing the
statement. A default NULL, expression, column, or parameter is rejected.

The extraction state is separated from response policy. A row can be SQL NULL,
path NULL, empty, a single JSON value, multiple values, a document conversion
error, or a hard error. Empty rows use `ON EMPTY`; multiple matches and value
conversion errors use `ON ERROR`. Malformed source JSON is a statement error and
does not enter the `ON ERROR` response policy. Invalid paths and excessive JSON
depth are hard errors. After the clause-form path check, SQL NULL documents and
JSON null values return SQL NULL directly.

For a single scalar match, explicit character targets return canonical text
(strings are unquoted), `RETURNING JSON` preserves JSON values, and
numeric/temporal targets use strict conversion. The omitted `RETURNING` form
keeps the legacy text behavior for composite values. A composite value selected
by an explicit scalar target, or a conversion that exceeds the target or loses
data, is an `ON ERROR` event rather than a silently truncated value.

## Plan representation

The parser creates a dedicated JSON_VALUE expression carrying the document,
path, target type, and both response policies. Its formatter preserves an omitted
target so reparsing retains `VARCHAR(512)` rather than an explicit `CHAR(512)`,
and preserves explicit policies. The binder keeps an expression with no
`RETURNING`, `ON EMPTY`, or `ON ERROR` clause on the existing two-argument
JSON_VALUE overload. When any new clause is present, it lowers the expression
to an internal seven-argument JSON_VALUE overload. The target type is carried
by the existing plan `Expr_T`; response modes are integer constants and
validated defaults are typed constants. The internal overload is not reachable
through the public grammar, and no protobuf change is required.

The executor performs extraction first and applies the selected policy per row.
It does not use a limiting ordinary cast for a document or prepared value. The
function remains nullable and handles policy arguments itself instead of relying
on generic strict-function NULL short-circuiting.

## Compatibility and rollout

Existing two-argument calls remain on the legacy two-argument plan with their
original type, collation and unbounded text behavior; the new default applies
only when a RETURNING/ON EMPTY/ON ERROR clause selects overload 2. Existing persisted generated
values are not automatically recomputed. Upgrade validation covers views,
generated columns, indexes, values over the 512-character boundary, and the
explicit rebuild procedure.

### Shutdown-upgrade compatibility (candidate revision 14)

A supported upgrade stops all services, replaces their binaries, and restarts
against the existing data. Mixed-running-version operation, old-peer rejoin,
and downgrade are not supported. The seven-argument overload does not change
RPC framing and therefore adds no feature-specific protocol version or peer,
placement, sender, receiver, or persisted-expression admission gate. Existing
function-lookup bounds checking remains in place.

The new binary must read supported released data: legacy persisted expressions,
views, generated values and index metadata keep their original semantics and
are not automatically recomputed. The source for compatibility validation is a
pinned supported release and its actual data, including retained snapshots or
backups; an earlier unpublished main commit is not a release fixture. Existing
historical upgrade/restart reports remain attributed to their actual binary
and data identities, not relabelled as final release validation.

Other features in the same expression or owner retain their own existing
admission floors. For example, functional indexes, signed WEEK/DAYOFYEAR,
collation-aware LEAST/GREATEST, JSON text processing, float conversions,
JSON MIN/MAX and ODKU keep their independent contracts. Adding JSON_VALUE must
neither raise nor lower those floors, including for algorithm-expression-only
owners, folded views and both expression orders. In particular, the existing
v129 opaque-JSON text contract also applies when JSON_VALUE consumes a
JSON-typed input; VARCHAR-only inputs do not acquire that floor merely because
they use the seven-argument overload. SQL ON UPDATE does not accept
JSON_VALUE; testing the internal owner walker does not imply SQL support.
Prepared handles are session state, not durable catalog objects.

Strict temporal conversions do not discard components: DATE rejects date-time
spellings including midnight; TIME rejects date-bearing values while preserving
ordinary TIME/duration spellings. Both executor conversion and DEFAULT binding
use these boundaries. Runtime loss follows ON ERROR; invalid DEFAULT ON EMPTY
or ON ERROR fails bind/prepare even when unused. Existing fractional precision,
zero-date and ordinary CAST semantics remain unchanged.

### Immutable JSON ownership redesign (candidate revision 15)

Complete canonical validation belongs to a published immutable data version.
The owner covers payload, descriptors, NULL/constant metadata and logical row
mapping. A lease proves lifetime only; neither a lease, a Vector address nor an
admitted boolean proves that externally writable aliases are absent.

An arbitrary mutable Vector is snapshotted while the caller keeps it stable.
Descriptors and payload are copied into private, statement-accounted storage;
that copy is validated before publication. No existing mutable Vector is sealed
in place. A private builder can instead copy and admit each appended document,
then transfer its unpublished storage without a second walk. Failed admission
publishes no reader; failed append leaves earlier builder rows available.

The state transitions are private construction -> full validation -> readonly
publication -> last-reference release. Reader windows retain their owner and
narrow only the row domain. Queries return opaque results with independent
references. They export owned text or copy JSON into an explicit destination;
they never export ByteJson.Data, a mutable Vector or a payload/descriptor slice.
Independent references may read concurrently. Closing a handle must not race
with an operation on that same handle. Mutable source writes during snapshot
construction are unsupported; later source writes cannot affect a snapshot.

Malformed unselected descendants must be rejected on admission. Queries of a
published version do not recheck unvisited bytes. Ordinary mutable inputs retain
full validation on every public JSON_VALUE call, including mutation/reuse tests.
Malformed documents remain hard errors for every ON ERROR policy. Header-only
validation and a second unchecked local decoder are rejected alternatives.

The implementation is staged: a private Vector reader/builder foundation,
ObjectIO scan integration, in-memory and remote batch publication, private JSON
expression results, then both JSON_VALUE overload families. A new internal
reader parameter path must preserve existing general expression interfaces and
share one owner across different paths. Unsupported conversions explicitly
materialize and lose their qualification; receiving peers re-admit bytes. No
qualification is serialized and no feature-specific MORPC gate is introduced.

Only JSON_VALUE consumers request new preparation. A normal scan or expression
that does not consume JSON_VALUE must not pay for a new snapshot. Existing
materialization should become private publication where possible; a snapshot
per expression or per query is not an acceptable substitute. Sources with no
exclusive ownership proof copy once at publication. Arbitrary mutable-input
benchmarks retain their defensive path and are reported separately.

For document size N and K lookups, the target is V(N) + optional C(N) + K*Q(P),
not K*(V(N)+Q(P)). Full validation and necessary copying stay inside preparation
or end-to-end timed loops. Validation must not increase with the number of
lookups on one published version. Output work may depend on the selected
subtree. Peak backing storage must not multiply by the number of paths.

Acceptance requires matching five-pair measurements: 4096-element resident
small-path and 64-row expression medians <=1.20 times the matched legacy base;
single-query end-to-end SQL median <=1.10 times that base, including admission,
copying and scan. Test 1/2/8 paths, 16/4096 unrelated elements, integers, strings,
mixed values, wide objects and deep documents. These are targets, not results.

This revision is a development candidate. Design closure requires a separate
review of producer-to-consumer ownership, output isolation, allocation failure,
cancellation, reset/reuse and actual performance. The reader foundation alone
does not establish production integration, independent approval or acceptance.

### Producer publication implementation (candidate revision 16)

The storage admission path now has a request-scoped `JSONReadSet`. A prepared
JSON_VALUE projection requests logical source columns before a synchronous
reader call. The physical reader binds those ordinals to its resolved column
sequence numbers; ObjectIO publishes a retained sealed cache version while the
cache entry is pinned. Ordinary output vectors remain mutable and are not used
as evidence of that version. The scope owns all published handles until the
projection retains them or the read/reset/free/error path closes the scope.
Repeated publication of the same logical column disables the scope, because
concatenation requires an explicit new row domain.

Storage tombstones compose the exact pre-shrink row positions into accounted
selection views. Selected ObjectIO loads publish selected views rather than
copying a second payload. Eager filters compose surviving row positions before
projection. Other transformations must discard qualification until their exact
mapping is implemented and tested. Mapping is never inferred from equal row
counts or a Vector address.

In-memory reader delivery and sharding receive delivery copy writable inputs
into private snapshots. The receiving peer validates its own version; it does
not accept a remote qualification bit. The sharding envelope decoder returns
errors for malformed input instead of panicking in the response callback.
Preparation costs remain real work; producer-side snapshot publication is not
zero-copy publication. Direct JSON_ARRAY/JSON_OBJECT producers write through their shared constructor
row sinks into a private builder and publish without materializing an ordinary
JSON result. Generic remote pipeline receivers, constant/version sharing and
remaining transformations still require integration before acceptance.

Internal ownership review confirms the intended terminal paths: a partially
published scope releases already published columns on failure, selection is
failure-atomic, every retained result pins its backing independently, and scan
Reset/Free releases an unconsumed publication. This is an implementation review,
not independent reviewer approval or measured performance acceptance. Updated
normal/race/SQL and five-pair performance evidence remain mandatory.

### Constructor and mutable-input boundaries (candidate revision 17)

Direct JSON_ARRAY/JSON_OBJECT consumers use the same row conversion functions
as ordinary execution, with a sink exposing only checked append operations.
The sink cannot expose a mutable Vector. Publication occurs only after every
selected output row has been admitted; a later conversion, allocation or cancel
error closes the entire unpublished builder. Mutable JSON operands are checked
before constructor traversal, and the final result retains complete canonical
admission including unselected descendants.

Mutable varlen reads check descriptor row and payload bounds before returning
borrowed bytes. This is a bounded accessor, not immutable qualification or a
second JSON decoder. The JSON_VALUE fallback still validates each complete
document; only a published owner can skip that walk. Restoring a corrupted
mutable descriptor restores normal execution. Cancellation and append failures
remain terminal errors. The private cursor takes a parsed path by value to
avoid forcing the caller's path object onto the heap through an interface;
there is no path cache and the public QueryWithExists signature is unchanged.

### Row domains and execution lifetime (candidate revision 18)

A snapshot may receive its producer's explicit batch row count. Flat columns
must match that count; a constant retains one physical, completely validated
value while its private metadata adopts the logical batch domain. The mutable
source is never resized. Type-size metadata is checked before copying, because
the ordinary duplication primitive uses that size for descriptor slicing.

Cursors retain their own backing lease and refer to immutable row coordinates.
Closing the source handle revokes that handle without rewriting coordinates
still used by cursors. The last lease releases column and mapping backing;
closed cursors cannot revive when another cursor is opened. Scan projection
preparation forwards the caller's allocation selection to private expression
producers and ordinary result storage.

The synchronous read request is hidden from DataSource.Next's internal reads.
Only the final persisted materialization or memory delivery may publish its
version. An exhausted empty merge child can rebind the schema because it has
published no version; rebinding an already published scope discards it.

During binding, cancellation comes from the live top-level request, since the
Process may retain an ended pipeline context. During execution it comes from
the active pipeline. This preserves DEFAULT/DDL folding and still terminates
canceled execution without applying ON ERROR.

### General pipeline delivery (candidate revision 19)

The standalone Projection prepares the same JSON document bindings as fused
projections. After its child delivers an actual batch through the existing VM
call, it may request versions from an optional producer publication interface.
Merge and ValueScan publish requested columns at that delivery boundary. Their
ordinary batches can still have writable aliases, so this path copies and fully
validates each requested column once. It does not infer exclusive ownership
from a pipeline lease or an ordinary result Vector.

Returned handles belong to the consuming projection, which releases every
handle on successful evaluation, partial binding failure, expression failure,
or cancellation. The producer retains no cross-call publication state. Grouping
set continuation uses already computed, owned projection results. Unsupported
operators do not propagate qualification implicitly; their ordinary output is
re-admitted at the next consuming boundary. No signal, ACK, spool, or wire
protocol changes are required. Only prepared JSON_VALUE column consumers request
publication, and all paths for one column bind to the same delivered version.

Local candidate checks cover the actual Merge publication consumed by two
paths, partial-publication rollback, and the production remote message-fragment
receive/decode/forward path into Merge and Projection. The latter replaces RPC
transport with its message channel; it is not two-CN SQL acceptance. Scoped race
checks and four existing SQL integration roots passed for this revision. The
initial local channel fixture used the wrong expected producer count and timed
out; the corrected fixture passed. These checks do not establish performance
acceptance or replace the remaining BVT and persistence gates.

### Parent demand before storage delivery (candidate revision 20)

A standalone Projection declares requested JSON document output ordinals before
calling its direct producer. A TableScan with an identity output (including an empty projection list) or a
direct local-column projection maps those output ordinals to storage columns
and binds them to its storage Read scope. After VM delivery, the consumer retains those exact readers
and the scan ends the scope. Applying an inline filter composes the existing
scope row map; it does not re-admit or copy selected documents.

A transformed scan output must not reuse output ordinals as physical storage
ordinals. Such an output follows explicit mutable admission until its producer
provides an exact mapping. Missing or disabled storage publications likewise
retain full mutable admission. Demand contains column ordinals only, never a
Vector address, and does not retain qualification across Read calls. Partial
publication, row-domain mismatch and cancellation close both the retained
handles and the originating scope.

The contract executor parses each selected row's path once and passes that
result to extraction. Validation still precedes the contract's document-NULL
handling; the legacy overload keeps its historical NULL short circuit. This
reuse is confined to one row evaluation and adds no path cache.

### Capability-only expression transport (candidate revision 21)

The private expression getter returns the closed reader without a synthetic
mutable metadata Vector. The JSON_VALUE invocation reserves document slot zero
for that separate capability and leaves its Vector slot nil. This is only an
in-process executor convention: ordinary expression interfaces, stored plans
and wire batches are unchanged. The reader remains the authority for document
type, NULLs and row mapping; the mutable source's later metadata cannot change
an existing frozen version. Generic consumers still explicitly materialize.

A cursor can load a frozen row, parse a path with the existing parser, and
return owned text with selection flags through one call. An absent input
short-circuits path evaluation for the legacy text operation. Invalid paths are
tagged for the SQL layer to retain its existing error text. The typed contract
keeps its path-before-NULL validation order. Both operations use the existing
query and formatter and expose no ByteJson or backing slices. It introduces neither
path caching nor another JSON decoder. Entry and successful-return cancellation
checks use the same captured binding/runtime context; an in-flight cancellation
is a terminal error and never becomes a NULL/default response.

The repeated SQL benchmark explicitly checks results before timing and can warm
cache admission. It is a resident SQL control, not evidence for the separate
cold first-admission gate. Matched runs fix both arms to ERROR logging so SQL
INFO writes do not dominate measurement; earlier INFO runs remain separate.

### Private constant constructors (candidate revision 22)

A JSON_ARRAY/JSON_OBJECT used as a JSON_VALUE document publishes constant
construction directly into its private reader. It does not first create a
mutable folded JSON Vector. The executor owns this constant version for the
current query; row-dependent paths use retained coordinate views of its one
physical document rather than rebuilding and readmitting it for each batch.

If the consumer's remaining parameters also fold, JSON_VALUE evaluates from
that reader and retains its ordinary folded scalar result. This preserves
constant-result folding without exposing the document backing. Constructor
errors still terminate evaluation. Reset and Free close the executor's reader
reference; the next execution constructs and admits a fresh version. Independent
retained views follow the existing lease lifetime. Generic constructor consumers
continue to use their ordinary expression interface.

### JSON cast producer boundary (candidate revision 23)

A CAST-to-JSON document consumer can use the same private producer path for
string and existing JSON inputs. The ordinary CAST and private output share the
existing row conversions; the private sink copies and fully admits the result
before publication. No ordinary result Vector is sealed or upgraded. Mutable
input descriptors are checked before reading their payload. An identity JSON
cast copies its input into the private sink and admits that copy; it does not
infer qualification from the source Vector.

The initial private CAST domain is text/binary strings and JSON. Other CAST
domains retain their existing conversion dispatch and explicit mutable result
boundary, including low-precision numeric bridges. Constant private casts follow
the same folding, row-view and Reset rules as the private JSON constructors.
SQL cast modes, binary subtype handling, parser errors and ordinary output
behavior remain owned by the existing cast implementation.

### Binary JSON literal publication (candidate revision 24)

The constant folder represents a JSON value as a typed binary-payload literal.
When that literal is a JSON_VALUE document, executor construction copies it
directly into a private builder and completes admission before publishing the
input. It does not create an intermediate mutable constant Vector. The plan's
immutable string is the reconstruction seed, not a read qualification.

Constant-result folding consumes the same opaque reader; row-dependent paths
use retained views of its one physical row. Reset releases the private version
and the next execution copies and fully admits the seed again. An explicit
ordinary Eval materializes independent writable output through the existing
fallible reader boundary. Free releases both the reader and materialized output.
The enclosing plan/literal encoding and wire format do not change.

### Full validation budget accounting (candidate revision 25)

Each object key is charged against the remaining-byte budget before any value
or scalar callback is visited. Canonical validation also requires ordered,
nonoverlapping, UTF-8 keys. The key-table entry count is bounded by the
validated serialized table size. These checks also bound ordinary validation
where overlapping key ranges may otherwise amplify traversal work.

### Canonical string runs (candidate revision 26)

The complete stored-document validator can handle adjacent string scalars in a
single traversal run. Each descriptor, shortest length prefix, range and UTF-8
payload is still checked. Running byte and node charges are checked before
processing each member and committed before any other scalar callback or child
descent. The string rule is the existing canonical string rule, not a trust
assertion about the input. Generic validators retain their scalar callbacks and
their existing behavior for callers with different string semantics. Tests must
compare the stored validator with the existing budgeted walk on valid and
malformed encodings, including limits and later-descriptor mutations.

### Contiguous key UTF-8 runs (candidate revision 27)

Canonical object validation still checks each key's bounds, byte budget,
nonoverlap and ordering. Adjacent key payload ranges may share one UTF-8 scan:
each nonempty key must start at a UTF-8 rune boundary, and a gap ends the run
before the gap bytes. Therefore no rune can be made valid by joining two
individually invalid keys, and unreferenced padding is not validated as a key.
The scan completes before any value callback or child descent. Generic object
validation retains its existing behavior. Tests cover split UTF-8 sequences,
invalid and valid Unicode keys, empty keys, gaps, ordering and overlap.

## Required evidence

- Temporal helper, executor and binder tests plus canonical JSON_VALUE BVT:
  NULL/DEFAULT/ERROR ON ERROR, unused defaults, column inputs, legacy controls,
  generated/indexed values, CREATE/ALTER VIEW and prepared reuse.
- Same-binary tests for absence of a JSON_VALUE floor and for independent floors
  in real SQL DDL, persisted owners, placement, sender, receiver and stream;
  injected protocol numbers do not model a supported mixed-version cluster.
- Actual pinned supported release data followed by shutdown upgrade: read and
  rebind legacy persisted metadata and values, then exercise new expressions.
  Restart and backup/restore checks cover supported data and independent existing
  admission state. A CN-only restart is not full storage recovery.
- Exact-content static checks, related package tests, canonical BVT comparison,
  benchmark measurements and exact-head CI. A skipped upgrade job is NOT_RUN.
- Review and deployment QA remain separate; this document and local tests do not
  establish reviewer acceptance.

Earlier independent reviewers have not withdrawn their design-approval or
mixed-binary requests. Those requests must be reconciled with the candidate
shutdown-only policy by the responsible maintainers. Prior approvals retain
their original scope and do not approve this revision.

### Persistence acceptance for shutdown upgrade

| Boundary | Required terminal result | Evidence boundary |
| --- | --- | --- |
| Supported release data -> new reader | Legacy views, DEFAULTs, stored generated values and indexes retain results, types and overloads 0/1; 512/513-character controls remain unbounded. Fresh writes and indexed reads agree with scans; existing values are not silently recomputed. | Pin release tag, resolved commit, binaries and data identity. Historical receipts remain historical; final supported-release validation is still required. |
| New expressions and independent admission | Create/read DEFAULT, generated, check, index and view owners; combined expressions retain unrelated floors and a failed ALTER leaves existing metadata intact. | Same-binary injected-floor and SQL tests validate local behavior; no new JSON_VALUE floor is asserted. |
| Restart and backup/restore | Restart the deployment against existing data and restore an older supported backup on the new binary; verify old and new owners, index contents and subsequent writes. | A CN-only restart does not prove storage recovery or full deployment restore. Existing independent admission state must remain valid. |
| Prepared plans -> new session | Re-prepare legacy and clause-bearing SQL after upgrade/restart; verify defaults, response policies and metadata. | Same-binary prepared tests do not prove the upgraded-session outcome. |

### Performance acceptance

Use `BenchmarkJSONAdmissionExistingPaths` and
`BenchmarkJSONValueLegacySmallPath` with identical 16/4096-element documents,
`$.keep`, Go/platform/native inputs and serial single-CPU runs on base and head.
Record ns/op, B/op, allocs/op, document bytes and repeat variation. Warm up and
verify bytes/results outside timing; reset/copy/admission/execution work stays
inside its respective timed loop. Benchmark setup owns and releases its vectors;
the admitted-execution measurement intentionally excludes ingestion.

The redesign target base is `22a02c581feaf56cf5ce0c319bcac8635c901369`;
the starting PR head is `7714b82bef18d89fec2f45e6df90e43ce0ea0e82`. Earlier
`bab4b3286a0dd5683a9b291763817722233e586c` receipts remain historical. A comparison may
use a Go source overlay restoring every base-modified Go file and removing every
PR-added Go file, with the same two benchmark files retained in both arms. Record
the overlay and exact inputs; do not label a handwritten DecodeJson substitute
or the seven-argument benchmark as legacy execution. Neither a matched comparison
nor functional success approves the cost. Record a retained-cost or consolidation
decision against measured common-path overhead before final acceptance. Both
existing scalar/structural and new canonical/range/UTF-8 invariants must survive
any later consolidation in the existing validation owner.

The candidate repair consolidates canonical admission into the existing bounded
structural traversal and reuses the scalar validator. It eliminates the second
descendant traversal and width-dependent child/frame slice allocations, while
retaining canonical ranges/key order/UTF-8, scalar encoding/nonfinite checks,
depth and both serialized-byte and node/entry work budgets. The ordinary mutable-input executor
still validates descendants on every call. Immutable-reader queries instead reuse
the complete admission of their private version, as described above; a lease or
mutable Vector is never promoted into that capability. Incremental publication of this repair does not
approve its remaining cost or the complete design/persistence acceptance gate.
Measurements and the independent decision belong in the task/PR evidence rather
than a permanent execution diary here.

### Evidence status

Historical measurements and revision-4 validation diary remain in git/PR history
at immutable source `88c5d3e65be9a887a5094ae584d371d35d4e95d2`; they are not
new-head PASS evidence. In particular, admitted-extraction benchmarks exclude
generic vector ingestion/unmarshal and cannot answer their regression cost.

Earlier local, CI, BVT, benchmark and upgrade receipts remain tied to their
actual revisions, binaries and environments. They do not validate this candidate
or establish the final supported-release upgrade, full recovery, deployment QA,
or acceptance of the legacy lookup cost. A focused base/head ingestion/unmarshal
and legacy two-argument JSON_VALUE comparison must include actual measurements,
equivalent inputs, environment and variation, plus a retained-cost decision.
Structural and canonical JSON safety checks remain required while that evidence
is pending.

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

### Synchronous text output copy (candidate revision 33)

The first complete paired run found that small-document SQL still exceeded the
1.10x target, with per-row string-to-byte allocation in the result-writing
path. The existing result writer synchronously validates and copies input into
its own varlena storage, including its constant-result path. The evaluator may
therefore pass a temporary read-only byte view of an already owned string for
that call only; the view is neither retained nor exposed. Length, UTF-8,
collation and allocation-failure behavior remain unchanged for both overloads.

Review decision: this removes an intermediate output copy, not document
validation or the final caller-owned output copy. It adds no path cache or new
JSON decoder. Keep a regression proving long output survives reader closure
and source mutation, and measure this change separately from admission reuse.

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

### Literal executor lifecycle (candidate revision 35)

An admitted JSON literal also has a dedicated expression executor, distinct
from its reader and private payload. Reserve its fixed-size metadata before
constructing the executor. Reset releases that reservation together with the
current reader; a later evaluation obtains a reservation in the new execution
generation before it rebuilds the reader. Free and a failed initial build
release both reservations. The immutable plan string is a reconstruction seed
and is not copied by the executor itself.

Review decision: charging only the reader misses this additional executor for
literal-only JSON_VALUE expressions. Test initial admission, reset and reuse,
final release, and an allocation-account rejection before publication.

### Demand-only executor and operator state (candidate revision 36)

The matched base/candidate layout probe showed that adding JSON fields directly
to every scan, projection and function executor increases memory even for SQL
with no JSON_VALUE. That violates the no-demand contract. Keep the active
reader as an evaluation-local parameter; attach constructor state only to a
JSON_VALUE constructor input; attach projection bindings to an accounted
wrapper installed only when the projection actually has such bindings. A scan
has at most one lazy JSON state pointer, placed by grouping existing boolean
fields so its total structure size does not grow. Its state and all wrappers
are reserved against the statement before construction and dropped at reset.

Review decision: require a matched base/candidate structure-size probe and
zero-allocation demand discovery, in addition to lifecycle, selection, folding
and real SQL tests. A nil pointer or an uncharged larger common executor is not
evidence that an unrelated query has no additional memory cost.

### Representative warm measurements (candidate revision 37)

The first warm harness built a standalone snapshot without a statement
admission callback. That omitted the metadata reservations made by SQL reader
views and cursors, even though the cold SQL measurement included them. Warm
reader and resident projection candidates must use the same generation-bound
admission as production; only their one-time publication remains outside the
warm timer. Cold SQL continues to time publication and result release. The
matched legacy baseline and input fixtures remain unchanged.

Review decision: report standalone unaccounted timings only as separate
mechanism observations. They cannot establish the warm performance gate.

### Scoped text reads and memory-only admission (candidate revision 38)

Five matched pairs with statement accounting enabled did not meet the warm or
small-document SQL goals. A warm profile attributed substantial CPU to
creating a retained cursor for each lookup; the seven-argument text path also
performs multiple interfaces per row. A cold SQL profile showed that opening
the statement generation for JSON-only memory accounting sampled the spill
filesystem even when the query did not request spill authority.

Use the ownership dependency's synchronous, owned-text selection for the
two-argument reader path and for text-returning seven-argument queries. Preserve
the legacy NULL-before-path order and the contract's path-before-document-NULL
order, all response policies, source/conversion errors and 512-character
checking. Typed and JSON outputs retain the cursor path. A projection-owned
reader can be borrowed for one evaluation while its existing binding remains
live; it need not create another retained window for that call.

Use the dependency's narrow memory-only admission, which keeps the same query
and CN memory limits and original generation. Full resource-budget callers
promote that generation and initialize spill limits before obtaining a
spill-capable handle. There is no new cache, timer, byte trust flag or alternate
JSON decoder. Re-run semantic, lifecycle, race, SQL and all paired performance
gates, retaining the earlier failed measurements.

### Direct text-result success handling (candidate revision 39)

The accounted two-pair diagnostic after scoped reads reduced the warm legacy
cost, while some 64-row text-contract cases remained near or above 1.20x.
For a successfully selected immutable text result, the common execution loop
may perform the existing scalar, UTF-8, width and output-copy operations
directly instead of constructing an intermediate extraction and invoking a
per-row callback. Missing, NULL, multiple, source-error and conversion-error
cases retain the same response and warning helpers. Mutable input, typed
outputs and JSON outputs continue to use their existing paths.

Review decision: this is a SQL-result dispatch optimization, not a new path
cache or JSON decoder. Compare it against ordinary mutable execution for
empty/error/default modes, composites, NULL ordering, warnings, output-memory
failure and 512/513 characters before re-running the fixed performance gates.

### Normalize semantic keys during formatting (candidate revision 40)

Five alternating cold SQL pairs at revision `b153a3505a` still exceeded 1.10x
in five of 30 cases, particularly small documents with eight paths. Temporary
stage probes localized approximately 40 microseconds of the eight-path
difference to plan optimization. The semantic-key helper cloned and reflected
over every expression solely to normalize JSON_VALUE clauses, including bare
two-argument calls for which normalization changes nothing.

The semantic-key formatter should apply the existing JSON_VALUE normalization
to a local copy of the clause while rendering, using an explicitly selected
formatting option. Normal SQL formatting, persisted definitions and the source
AST retain their original spelling. Nested JSON_VALUE expressions, including
those inside subqueries and sample expressions, inherit the same option.
Grouping-set identifier normalization continues to use an independent clone,
because that separate operation mutates identifiers.

Review decision: compare the new keys with the previous clone-and-walk behavior
for legacy calls, explicit and implicit response modes, character sets,
defaults, parameters and nested expressions; prove the AST remains unchanged.
Re-run planning/SQL regressions and the same paired performance gates. This
changes neither JSON admission nor path evaluation and introduces no path
cache or production profiling counters.

### Reuse admitted string validation (candidate revision 41)

The immutable reader has already admitted every stored JSON string with a
canonical length and valid UTF-8. For a selected `TpCodeString`, its existing
unquote operation returns the payload as a newly owned string regardless of
the legacy float-formatting option. The two private reader implementations may
perform that same owned conversion directly; the returned text must never
alias the retained document, including after a reader or view is closed.

For the implicit `VARCHAR(512)` result only, a selected admitted JSON string
whose byte length is at most 512 already satisfies both UTF-8 validity and the
character-width bound. Reuse that admission result instead of scanning the
same text again. Longer strings and all other JSON types continue through the
existing conversion checks; in particular, binary/blob payloads do not acquire
the JSON-string UTF-8 guarantee. Mutable vectors and unknown inputs retain
complete validation. Output allocation failures, response routing, path and
NULL order remain unchanged.

The Ubuntu prototype used separate test-only overlays and retained the
unmodified benchmark binaries. It showed a potential improvement in the
string and mixed cases, but is not an original-baseline acceptance result.
Review must independently cover owned output, escaped and multibyte strings,
512/513 character boundaries, binary payload errors and both reader owners;
then repeat all fixed five-pair performance gates with preparation and copies
included where required. No path cache or alternate decoder is introduced.

### Local-domain scan filtering (candidate revision 42)

An upstream local-domain probe can remove rows after a storage read has
published JSON versions. Apply the probe's exact surviving-row selection to
all published readers before shrinking the batch, as for other scan filters.
The publication remains scoped to that read and is closed on reset, free, or
failure; a selection failure terminates execution. A regression must exercise
the actual probe together with JSON publication, including a removed row and
the retained values' mapping.
