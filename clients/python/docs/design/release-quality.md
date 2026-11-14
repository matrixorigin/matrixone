# Python SDK release compatibility boundaries

This record qualifies the SDK against pinned main and 4.2 candidate builds.
Official 4.2.8 is a compatibility/reproduction baseline with known failures; it
is not certified as fully compatible. Common execution contracts are intended
for the 4.1/4.2 families, but this work does not recertify every historical patch.
Version banners do not establish feature support: native operations and exact
source/image identities stay authoritative. Related issues: #30963, #31023,
#30067, #30068, #30069.

## Maintaining version contracts

Target identity and capability support are separate contracts. Use the following
decisions when diagnosing future failures; the dated evidence below freezes the
specific builds actually tested.

| Boundary | Owner and maintenance rule |
|---|---|
| Published v4.2.8 defects: metadata/replay, branch deletion, quoted/ASYNC FULLTEXT, numeric/dollar syntax, Stage literals and FLOAT32 infinity | Kernel companion #32723 fixes these on the 4.2 line. Keep the original functional inputs and positive assertions. Do not retry changes to an immutable release image or add SDK expected-error/version branches for these defects. Qualify a new server build containing the fixes. |
| Native Arrow LOAD on 4.2 | `matrixone/capabilities.py` owns checkout-scoped format detection. SDK Arrow-input upload remains required through checked Parquet, with the same value, transaction and cleanup oracles. Reevaluate from the native probe if support changes; do not add a guessed minimum-version threshold. |
| Native VECF8/VECF4 on 4.2 | These types are outside the bug-fix backport. `tests/online/test_block_vector_reflection_online.py` requires exact native rejection and no created object on the declared 4.2 profiles. This is an unsupported feature boundary, not a defect to emulate in the SDK. Change the expectation only after a verified 4.2 build implements the types and passes the complete positive tests. |
| Current main and a future fixed 4.2 release | Freeze real revisions/images and run the same required positive scenarios. A previous passing commit does not certify a later commit. Advance the CI release target and its configured expected identity together after a release containing #32723 exists. |

`scripts/run_contracts.py`, the online fixture, the Stage functional test and the
capability/type decisions link here. `MO_SDK_SOURCE_LINE` describes the selected
source line; it neither proves a feature nor suppresses a required failure.
Do not generalize one pinned release's capability observations to every 4.2
patch. Reopen a recorded limitation when a native implementation, a contrary
public-path reproduction or an explicitly expanded product requirement changes
its premises. A red old-release baseline alone does not reopen an already fixed
candidate defect. Never label that baseline as fully compatible.

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
Legacy SQLAlchemy 1.4 string execution passes through the listener unchanged.
Snapshot tokens inserted during typed compilation follow the active compiler's
percent-escaping policy; raw/export SQL retains the original snapshot name.
Compiler-local snapshot bindings avoid caller and expanded parameter names, and
execution parameters cannot replace the immutable snapshot annotation.

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

Typed `filter(a, b)` and `where(a, b)` conjoin predicates. A raw SQL fragment
continues to own its positional values (`filter('id = ?', 1)`). Combine raw
fragments with typed predicates through separate chained calls or explicit
SQLAlchemy text expressions; a bound value cannot become SQL by accident.
Single wildcard projections use server column names, including CTE results,
and retain mapped result processors instead of exposing a field named `*`.

## Autocommit and pool ownership

SDK-created autocommit engines suspend the physical driver's autocommit for
ordinary SQLAlchemy transactions. An explicit SQLAlchemy `AUTOCOMMIT` execution
option retains its semantics. Before a raw checkout returns to the pool,
abandoned work is rolled back before autocommit is enabled; cleanup failures
invalidate the connection. Checkout restores the requested baseline after
SQLAlchemy's isolation finalizers, before another borrower receives the lease.
Explicit commits remain committed, while rollback and abandonment remain
uncommitted. No write is retried during cleanup.

Driver-parameter migration warnings describe the list form of executemany
without rendering any bound values, including collection-valued parameters.
Session error reporting also preserves the database exception when either
query or error logging itself fails. Cancellation remains outside that
secondary `Exception` guard.

## Portable pagination

OFFSET without a user LIMIT uses MySQL's unlimited unsigned row count,
`LIMIT 18446744073709551615 OFFSET n`. This preserves the requested slice on
released grammar which rejects standalone OFFSET. Explicit LIMIT 0 remains
empty; omitting both arguments adds neither clause. ORM/index SQL and both
direct fulltext renderers share the same constant-time compiler helper.

## Export quote boundaries

OUTFILE follows trailing comments on a new line. Terminator normalization shares
the native dollar-tag, identifier and numeric token rules with parameter
rendering. Raw backslash-bearing SQL also depends on the executing connection's
`NO_BACKSLASH_ESCAPES` and `ANSI_QUOTES` settings.

Exports render structured queries once, preserving callable bind identity. For
backslash-bearing rendered SQL, the existing deferred literal executor reads
the actual selected checkout's SQL mode before normalizing the query. The same
checkout executes OUTFILE, including when a Session routing hook wraps the
statement. This introduces one read-only mode query, no additional pool lease,
no SQL mode mutation and no write retry. Existing Client buffering, Session
ownership, async timeout and cancellation cleanup remain authoritative.

## CLONE completion and no-op semantics

Ordinary clone managers execute plain CLONE followed by SHOW CREATE on the same
executor. A missing target after acknowledgement is an explicit CloneError with
its original source. The SDK does not claim row identity from this existence
check, issue compensating DROP, or take transaction ownership from Session.
Live database clones first verify their source with SHOW CREATE on the same
executor: some releases otherwise acknowledge a missing source by creating an
empty target. Snapshot clones do not require that source to remain live.

Native CLONE IF NOT EXISTS can append data to an existing target in supported
releases. Safe manager if_not_exists therefore observes the target using bounded,
read-only information_schema queries on the same executor. Existing targets
return immediately. Missing targets use plain CLONE and verification. Names
are decoded and compared as binary catalog values, preserving exact spelling
when `lower_case_table_names=0` and using server LOWER for the other modes.
The operation introduces one query for an existing target. A missing target
uses three queries for table/snapshot clones, or four for live database clones.
Ordinary clones use two queries, or three for live databases.

In tested 4.1 and 4.2 releases, the server's internal CLONE executor can still
lowercase a new target in mode 0. A conflicting lowercase sibling can therefore
cause a native error; the SDK propagates the error instead of reporting a no-op
for an absent exact target. In mode 2, native CLONE can create a lowercase name
while SHOW CREATE uses the requested spelling, causing verification to fail even
after creation. The SDK reports that failure rather than claiming success.

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

Broader historical certification would require full suites on
4.1.0/4.1.6/4.2.0/4.2.6 and core contracts on intermediate stable patches;
this round does not claim those runs. Independently test SQLAlchemy 1.4, 2.0
and 2.1. Native-error-preservation tests certify propagation only; required
functional tests still demand success. Optional native format/type rejections
are recorded separately from positive feature support, without hidden skips.
Optional TLS/S3/HNSW, multi-CN topologies and async Arrow are separate qualification
surfaces; passing this matrix does not certify them. Direct registered
`matrixone://` engine URLs still lack a DBAPI entrypoint in the baseline and are
not qualified here; use SDK connect or adopt a configured MySQL driver engine.

## 2026-11-13 release and main validation

Each validation round freezes a backend identity. The original release is
**v4.2.8**, revision `10d204b8763f19b2b37ba2147d1d4c1a7828d6d4`, image
`matrixorigin/matrixone@sha256:eec6cee1ab0d1ddaa5c81cbc9d79ef6a59ecf13f2b5835631ef3b03ee6822fc6`.
The 4.2 candidate uses [kernel companion #32723](https://github.com/matrixorigin/matrixone/pull/32723),
based on 4.2-dev `1a3e7fb6960995e9a143e5cc3d5a50f2feb0980f`.
Its real-service binary is `ec0e3ad8af2`; the later uninitialized-process guard
has complete owning-package race/coverage proof and leaves initialized SQL
consumers unchanged. Main is now frozen at
`5540b068209c27b0ed13c65d3a0364d28d2512e0`; it is rebuilt with Go 1.27.1 and
its new binary is SHA-256
`f21f54f1036edbba6c9423c10204346f1e0ae69ba45d1f352c55917860e5812c`.
The full main suites below are fresh exact-source runs, superseding the earlier
`2a42b3e99f`/`14b1ddced58` semantic-reuse record.

### Required behavior and genuine capability boundaries

A required functional test has the same positive oracle on released servers,
4.2 candidates and main. In this follow-up, remove version branches which had
accepted errors for view/BOOL/ENUM reflection and replay, empty database branch
deletion, special FULLTEXT identifiers and numeric/dollar query/export syntax.
Restore the original special identifiers and ASYNC gojieba parser on 4.2;
ordinary names or synchronous execution cannot substitute for those scenarios.
Remove the legacy Stage-support override: catalog values, credentials, status,
file reads, import/export, nested Stage use and cleanup must all succeed.
An original 4.2.8 failure remains a failure, without xfail or a known-bug pass.

The former Stage negative branch accepted any QueryError. Fault injection
showed it accepted unrelated permission/connection errors, an incorrect target,
a mismatched version and partial state. An intermediate attempt to narrow its
errno assumed a syntax code; the real pinned server returned 20101/invalid-input,
and direct catalog cleanup was forbidden. Preserve those failed experiments;
do not enlarge the expected-error set. The final change deletes this negative
branch. A separate nine-case probe proves old override settings cannot convert
any required-feature error into a pass and preserves the primary exception.

Native format/type controls have a different contract. Native Arrow LOAD is
absent on 4.2, while public SDK Arrow-input upload must still pass all value,
transaction, streaming, cancellation and cleanup assertions through checked
Parquet staging. Only the exact 20300 format rejection selects Parquet; unknown
errors and invalidated connections do not. Input is consumed once, into one
bounded file, and the actual write checkout is requalified. All former Arrow
positive skip gates are gone. Four VECF8/VECF4 tests on 4.2 explicitly require
native rejection and no catalog object; these are unsupported-type checks,
**not positive reflection/round-trip support**. Main and unknown profiles keep
the full positive type checks. VECF16/VECF32 positives remain on both lines.

Separate native-error-preservation/differential tests certify propagation of a
native outcome; they cannot certify that the failing feature works. Test pass
counts must be read together with this functional/capability map.

### Terminal evidence

Full server sweeps use Python 3.12.16, SQLAlchemy 2.0.43, PyMySQL 1.2.4 and
aiomysql 0.3.2. The eight offline profiles are a separate dependency matrix,
not a full online cross-product. Executable runtime, offline-test and runner
inputs are unchanged by this follow-up; final maintenance comments preserve
their parsed syntax trees. Their frozen evidence remains valid. The full
server sweeps ran the strengthened oracles above; final comment-only edits
preserve those executable inputs too.

| Selection | Result |
|---|---|
| Main `5540b068209`, new complete online | 1296 passed; zero failures/errors/skips, unchanged inputs |
| Main `5540b068209`, new complete analytics | 338 passed; zero failures/errors/skips, unchanged inputs |
| Patched 4.2, restored complete online | 1296 passed; zero failures/errors/skips, including the four explicitly unavailable-type controls above |
| Patched 4.2, restored complete analytics | 338 passed; zero failures/errors/skips, all mandatory Stage and Arrow-input positives execute |
| Original immutable v4.2.8, same restored online oracles | 1269 passed, 27 failed, no errors/skips; **not qualified** |
| Original immutable v4.2.8, same restored analytics oracles | 329 passed, 9 failed, no errors/skips; **not qualified** |
| Full offline, Python 3.12 / SQLAlchemy 1.4.54, 2.0.43, 2.1.8 | 3363 / 3359 / 3367 passed; no failures/errors/skips |
| Full offline, Python 3.10 / SQLAlchemy 1.4.54, 2.0.57 | 3363 / 3359 passed; no skips |
| Full offline, Python 3.11 / SQLAlchemy 2.1.8 | 3367 passed; no skips |
| Full offline, Python 3.12 / SQLAlchemy 2.0.57 | 3359 passed; no skips |
| Exact PyMySQL 1.2.3 regression tuple | 3359 passed; no skips |
| Core without Arrow, Python 3.8/SA1.4 and Python 3.9/SA2.0 | 139 runtime checks per profile; installed wheel imports/portable compilation pass |
| Async lifecycle/real drivers | 95 focused checks each on SA1.4/2.0/2.1; six actual SA1.4/patched4.2 and six SA2.1/main controls pass |
| CI selection parity | Same 1634 cases: 207 required + 1427 extended, no omissions/duplicates |
| CI/static/package | All 50 CI scope checks pass; scoped Black/fatal flake8 pass; final package verification recorded before push |

The first original-release baseline had 25 additional file-access failures (17 LOAD and eight Stage cases)
because the Docker fixture still shared another SDK worktree's tmpfiles path.
Add only the missing current-worktree bind and rerun the complete baseline;
exclude that environment invocation from native bug counts. Keep its logs.
Original release data and previous physical-upgrade copies remain untouched.

### Ownership, resource envelope and preserved oracles

| Scenario | Retained or strengthened proof |
|---|---|
| Transactions/pool reset | Independent observer rows, rollback before autocommit, failed-reset disposal, explicit AUTOCOMMIT, next-checkout mode |
| Export/query building | Exact CSV/JSONL bytes, real dollar/numeric tokens, both escaping modes, one-slot checkout, one callable evaluation, grouped predicates and executed CTEs |
| Metadata/FULLTEXT | Standard MySQL source/view/clone processors and replay; exact decimals/domain values; quoted identifiers, asynchronous catch-up and parser propagation |
| CLONE/branch/restore | Missing-source non-mutation, no-op targets, empty/populated deletion, preserved source, historical clones, account/privilege differences and DDL conflicts |
| Pending async probe close | Existing two admissions remain charged through close completion on timeout/error/cancel; third input/checkout/DDL is blocked; drain and reuse checked |
| Relation cleanup retry | Real one-slot aiosqlite pool, original deadlines, retained single DROP, unrelated row42, absent table and released pool; memory SQLite removes irrelevant fsync |
| Native BVT/upgrade | Six stateful scripts twice:911total/903success/8existingignores each; saved-SHOW scripts twice:510total/509success/1existingignore each; metadata1435+Boolean67; real v4.2.8 copy upgrade |
| Native races | Full frontend race, four affected roots100times, deterministic DDL/delete lifecycle; datalink/stageutil complete race+coverage, vet and lint |

Each target owns independent two-CN data, deployment identity and
HAKeeper/raft/gossip/status ports. Restore-capable suites hold a whole-cluster
lease including readiness and teardown. The earlier missing S3 Stage was
independently reproduced as overlapping RESTORE CLUSTER rewinding mo_stages;
different databases do not isolate it. Synthetic S3 buckets use unique names.

During earlier qualification, host IO pressure and later native Raft/LocalETLFS
fsync stacks accompanied request/startup timeouts. Fresh NVMe qualification
keeps client deadlines unchanged. Elapsed failure time includes cleanup and is
not a claim about the configured timeout. Original data, failed runs and partial
migration copies are retained; physical upgrade evidence is separate.

Source builds are identified by real revisions and binary hashes. The 4.2 build
uses `MO_VERSION=4.2-dev`; Make's local branch-name default had manufactured the
invalid banner3267142. This is corrected build metadata, not a new release tag
or a relaxed SDK parser. Main's detached build advertises v1.3.0; its commit/hash
identifies the code and capability probes decide operation support.

A new 4.2 release containing #32723 is required for the native fixes. Existing
unmarked empty databases cannot safely be inferred as branches; persisted
ENUM/SET views that lost provenance require recreation. Upgrades are shutdown-
only, and old binaries must not open data written by the new binary.

The existing two online CI jobs still select 4.2.8 and current-source, with an
explicit source-line label. Required functional failures on the old release
are visible when that job executes; the candidate is qualified locally until
its release exists. A skipped GitHub online job is not validation. No runner or
shard is added, and no case-count/build-time claim substitutes for coverage.
