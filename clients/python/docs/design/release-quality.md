# Python SDK release compatibility boundaries

The SDK supports the 4.1.x and 4.2.x release families through common execution
and SQL construction contracts. Version banners identify test targets; they do
not establish that a server supports a feature. Explicit native operations stay
authoritative. Related issues: #30963, #31023, #30067, #30068, #30069.

## SQL construction

Administrative SQL distinguishes identifiers, grammar STRING tokens and SQL
expressions. One identifier decoder accepts complete legacy quoted components,
rejects trailing SQL and re-emits valid identifiers. One grammar token renderer
doubles apostrophes and rejects NUL/backslash where SQL mode would change their
meaning. Expression values retain the existing expression-literal renderer.
Managers and statement builders share these authorities. Unsupported server
catalog serialization remains an error; the SDK never changes SQL mode or
silently removes user input.

## Typed queries and engine ownership

Snapshot Selects are immutable annotations participating in SQLAlchemy structural
cache keys. The MatrixOne compiler handles the root query and all qualified JOIN
sources. Nested snapshot SELECTs and CTEs reject explicitly instead of returning
live rows. Compilation for export uses the same compiler. Bind and result
processors, including exact Decimal values, remain owned by the driver dialect.

Engine adoption preserves the original dialect object, driver, pool and hooks.
A before_execute listener stamps typed statements with a compiler-contract
namespace so a caller cache warmed before adoption cannot reuse MySQL SQL without
snapshot hints. Caller cache entries remain intact. Engine-created and adopted
sync/async engines share the listener; raw driver SQL is unaffected.

Each live immutable statement retains at most one prepared annotation memo,
guarded by exact original identity and compiler-contract version. Generative
copies cannot reuse their parent's prepared SQL. The prepared copy drops any
inherited memo. Reused statements perform constant-time preparation and retain
their generated cache key; no global statement registry or per-execution growth
is introduced. Concurrent initial preparation can duplicate equivalent work but
never publishes partially constructed state.

Buffered results use the driver's returns_rows flag, with an ORM row-result
fallback where that flag is absent. Both clients and Session owners preserve
rows, empty SELECTs and DML affected counts. QueryError retains the original
exception and native errno; existing QueryError instances are not rewrapped.
Cancellation remains cancellation rather than a query failure.

## CLONE completion and no-op semantics

Ordinary clone managers execute plain CLONE followed by SHOW CREATE on the same
executor. A missing target after acknowledgement is an explicit CloneError with
its original source. The SDK does not claim row identity from this existence
check, issue compensating DROP, or take transaction ownership from Session.

Native CLONE IF NOT EXISTS can append data to an existing target in supported
releases. Safe manager if_not_exists therefore observes the target using bounded,
read-only information_schema queries on the same executor. Existing targets
return immediately. Missing targets use plain CLONE and verification. Names
are decoded and normalized by server LOWER to match native catalog behavior.
The operation introduces one query for an existing target, or three for a missing
target. Ordinary CLONE uses two queries.

This observation is not a race-free creation lock. A concurrent creator can win;
any native failure propagates, while an acknowledgement still requires target
existence verification. No duplicate-error recovery or automatic retry occurs.
Raw table/database builders cannot implement this multi-statement contract and
reject their if_not_exists flag at compilation, directing callers to managers.

Snapshot convenience APIs delegate directly to the same execution owner; no
client-level existence lookup acquires another connection from a Session pool.
Async CLONE wraps preflight, execution and verification in
one query_timeout budget. Caller cancellation propagates and cancellation cleanup
finishes under the existing execution owner. An elapsed timeout does not prove
rollback or absence of durable effects. Sync driver timeouts remain per query.
Native clone source visibility is release-dependent; integration tests compare
with an equivalent independent native transaction before asserting rollback.

## Local admission and buffered consumption

Explicit configuration key presence takes precedence before environment lookup
or conversion, including zero, False, empty strings and None. Unoverridden
numeric environment settings still convert or fail visibly. All configuration
outputs share this owner and preserve their existing kwargs/tuple translations.

Both buffered result wrappers validate integer-index batch sizes before moving
their cursor. Negative sizes raise ValueError and preserve remaining rows.
Noninteger sizes retain TypeError, while bool and NumPy integer inputs retain
Python's integer-index semantics. No network or server cursor is involved.

## Qualification

The required runner selects nonempty offline or explicit server contracts,
rejects skips/errors/failures and input drift, and checks exact observed release
identity. Full server contracts include shared export/file access; core contracts
cover queries, transactions, values, snapshots, errors, administration, metadata,
fulltext zero limits and safe clone no-ops without an export mount.

Test 4.1.0/4.1.6/4.2.0/4.2.6 with the full suite and every intermediate stable
patch with core contracts. Independently test SQLAlchemy 1.4, 2.0 and 2.1.
Native unsupported outcomes are accepted only by a narrow native-versus-SDK
negative contract, recorded as feature outcomes rather than hidden skips.
Optional TLS/S3/HNSW, multi-CN topologies and async Arrow are separate qualification
surfaces; passing this matrix does not certify them. Direct registered
`matrixone://` engine URLs still lack a DBAPI entrypoint in the baseline and are
not qualified here; use SDK connect or adopt a configured MySQL driver engine.
