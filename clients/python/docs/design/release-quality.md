# Python SDK released-server compatibility

The SDK targets **published MatrixOne releases**. Main is a separate forward
compatibility target. A future server fix does not replace an SDK contract on
the version users can install today.

## Maintaining version contracts

For each released-server operation, prefer an equivalent implementation that
preserves values, identifiers, types, transactions and cleanup. If that is not
possible, give a specific actionable SDK error and verify its effects. Do not
silently change ASYNC to sync, rename columns, widen types, retry writes or
replace branch deletion with DROP. A generic native error is not compatibility.

The current qualified release profile is official **v4.2.8**, revision
`10d204b8763f19b2b37ba2147d1d4c1a7828d6d4`, immutable image
`matrixorigin/matrixone@sha256:eec6cee1ab0d1ddaa5c81cbc9d79ef6a59ecf13f2b5835631ef3b03ee6822fc6`.
Runtime decisions read the **selected physical connection's handshake**;
`MO_SDK_TARGET`, `MO_SDK_SOURCE_LINE` and caller version overrides are not runtime
feature switches. Unknown/custom/source builds keep their own execution path
and require qualification. A custom patched build should advertise a distinct
build/release identity. Do not guess an unpublished minimum fixed version.

| Surface on official 4.2.8 | SDK behavior and usable alternative |
|---|---|
| Numeric tokens adjacent to dollar aliases | Insert token-separating whitespace on SDK-managed SQL. Preserve values, aliases, bound parameters and export bytes. `execute_driver` keeps its exact SQL; use `execute` or supply whitespace explicitly. |
| BOOL, uppercase view types, DATE display width and unsigned modifiers | Adopt the SDK dialect with `Client.from_engine(engine)` or use an SDK-created engine. Normalize metadata spelling, preserving native types, dimensions, values and processors. Plain MySQL engines remain owned by SQLAlchemy. |
| SET member case in source tables | Restore the original native catalog domain on the same connection, scoped by current account and the server's table-name case rules. Preserve SET options, binding and result processing. Do not lowercase domain values. |
| View ENUM/SET provenance | Preserve actual native VARCHAR metadata when the old view already lost its domain. If metadata cannot construct a usable type, raise an actionable metadata error. Declare types from the known source model explicitly, or recreate the view on a fixed release; the SDK cannot invent lost provenance. |
| Arrow-input upload | Use checked Parquet staging after the exact native Arrow-format rejection. All public upload value/transaction/streaming/cancellation/cleanup assertions still execute. Native Arrow LOAD remains unsupported. |
| TLS negotiation | Original 4.2.8 has a native buffered-handshake defect: an independent SSLRequest/ClientHello coalescing control times out before authentication. A later SDK sweep also recorded intermittent TLS EOF setup errors; their exact relationship to that defect is not established. Use a server containing the native TLS fix in #32723. The SDK does not silently disable TLS, relax verification or turn connection errors into passing functional tests. |
| Special FULLTEXT identifiers and ASYNC gojieba | Reject the known unsafe definition before dispatch with the release, feature and advice. Keep ordinary FULLTEXT and ASYNC indexing supported. Explicitly choose ordinary identifiers or synchronous parsing only if those meet the application's needs; otherwise upgrade to a release containing #32723. An unrelated quoted column is permitted. Only ASYNC and gojieba options of the same index select the parser limitation; a column/index named `async` and options of separate indexes do not. |
| Apostrophe Stage names/values | Reject unsafe catalog serialization before dispatch, including bound values. Percent-encode URI path characters where appropriate; quoted credentials/comments require a fixed server. No input is removed or changed silently. |
| CREATE STAGE with enable=False | Reject before creation: the release ignores the option. Explicit CREATE-enabled followed by ALTER-disabled is usable only when an initially enabled Stage is acceptable; otherwise a fixed release is required. |
| VECF4/VECF8 | Explain that the requested precision is unavailable. Choose supported precision explicitly. These storage types are outside the #32723 bug-fix backport. |
| Empty database branch deletion | Preserve the real native identity rejection, source/target state and cause. Explain the release limitation and upgrade requirement. Never substitute DROP DATABASE. Populated supported branches retain normal deletion. |
| ENUM/SET SHOW CREATE/replay defects | Preserve the native failure with an actionable schema diagnostic, including SQLAlchemy's reflection path that bypasses error listeners. Source data/domain queries remain usable; complex replay needs a fixed release. |
| Parquet FLOAT32 infinity | Preserve the native error and transaction/cleanup evidence. Explicitly cast the input/schema to FLOAT64 if acceptable; all special values are checked. No implicit storage widening. |

`matrixone/_release_compat.py` owns these narrow release adaptations and
messages. Engine-local listeners cover sync/async Client, both Session facades,
adopted engines, export and analytics on the selected checkout. No global MySQL
registry, new worker, capability cache, queue or write retry is added. SET
reflection uses one read-only catalog query only when needed, with bound names,
binary equality, server case normalization and `current_account_id()`.
The release listener issues zero SQL before dispatch. At most four quote-mode
interpretations must agree before it rewrites/refuses SQL; ambiguous raw input
keeps its native path. In particular, compatibility inspection cannot start a
transaction before Stage DDL. It never commits/rolls back the caller's work.

Unsupported preflight errors have `phase='compatibility'`,
`committed=False`, `statement_acknowledged=False` and **no fabricated native
errno or cause**. Metadata-only refusal uses `phase='metadata_reflection'`.
Known native failures retain `orig`, errno and SQLSTATE; effect/rollback status
remains owned by the executing transaction. QueryError exposes
`compatibility_feature`, `compatibility_hint` and `backend_version`; manager
exceptions retain the diagnostic through their message/cause. Diagnostic
messages do not copy SQL, credentials, values or paths. Permissions, transport
failures and unrelated errors are not recast as known release limitations.

Reopen a limitation when a native implementation, contrary public-path evidence
or an expanded product requirement changes its premises. Qualify new releases
and then update the pinned CI target together with its expected identity. Kernel
companion [#32723](https://github.com/matrixorigin/matrixone/pull/32723) adds native
capabilities to the 4.2 line; it is not required for the SDK's supported release
paths and explicit limitation contracts above.

Design and separate root review precede implementation:
[release contract r2](released-server-compatibility.md),
[r1 review](released-server-compatibility-review-r1.md),
[SET amendment review](released-server-compatibility-review-r2.md),
[transaction-neutral amendment](released-server-compatibility-mode-amendment.md)
and [r3 review](released-server-compatibility-mode-review-r3.md).

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

## Qualification and outcome reporting

A required SDK case on a release proves either the equivalent positive result
or the exact actionable limitation, native control, unchanged state and cleanup.
Where usable, execute the explicit alternative as well. Main and the fixed 4.2
candidate keep the full original positive feature scenarios, including special
identifiers, ASYNC parser configuration, exact metadata/replay and Stage file
consumers. Keep unique privilege/account/error/resource-lifecycle checks.

JUnit feature outcomes distinguish `supported`, `equivalent-implementation` and
`unsupported-with-actionable-error`. A green SDK compatibility suite **does not
mean every native feature is supported**. Generic QueryError, unrelated native
failures, xfail, skipped tests and substituted inputs cannot certify a feature.
Native controls use independent driver execution where necessary. The test-only
release selector verifies the actual release banner and pinned revision.

The required runner rejects empty selections, skips, errors, failures, identity
mismatch and source drift. Required/extended CI phases partition the same full
collection without omissions or duplicates. The existing release/current-source
targets and runner count remain unchanged. Online workflow jobs require their
configured disposable server; skipped jobs provide no online proof.

### Current validation round

Release: official v4.2.8 above. Main: frozen at
`71390fb3c16f986b5cbe77e44ea0e5b7f5dbcff6`, fetched and merged in this round,
fresh Go 1.27.1 build SHA-256
`55e3651e0954d03203c0ec59fea66ec0df555a7c7f3ffad4d15f50e88181a0cc`.
The 4.2 candidate uses #32723, based on
`1a3e7fb6960995e9a143e5cc3d5a50f2feb0980f`; real-service build
`ec0e3ad8af2`, binary SHA-256
`3286723ad0a7ba28faa43c5c93c4ff6538eca9066c4c1ae1b0784967fa03c917`.
Later native nil-process/saved-result fixes have their own owning-package and
BVT evidence; this is not a claim of an identical current-head candidate binary.

| Target / selection | Terminal result |
|---|---|
| Original official v4.2.8 | Earlier complete online 1298/1298 and analytics 340/340, zero failures/errors/skips. After the option-scope correction, a full online sweep completes with 1292 passes and six TLS setup errors, zero assertion failures/skips. Those six original cases subsequently pass once on the same TLS configuration; that does not erase the failed sweep or qualify TLS reliability. |
| Main `71390fb3c16` | Full online 1298/1298 and analytics 340/340, zero failures/errors/skips. Final-source SQLAlchemy 2.1 consumer renewal: 12/12. |
| Fixed 4.2 service `ec0e3ad8af2` | Full online 1298/1298 and analytics 340/340, zero failures/errors/skips. Final-source consumer renewal: 5/5. |
| Release with SQLAlchemy 1.4 | Final-source actual-service checks: 102/102, including changed metadata, Stage, FULLTEXT, numeric and upload surfaces. |
| Python 3.12 / SQLAlchemy 1.4.54, 2.0.43, 2.1.8 | Complete offline suites: 3431, 3427, 3435; all pass, zero skips. |
| Python 3.10 / SQLAlchemy 1.4.54, 2.0.57 | Complete offline suites: 3431, 3427; all pass, zero skips. |
| Python 3.11 / SQLAlchemy 2.1.8 | Complete offline suite: 3435; all pass, zero skips. |
| Python 3.12 / SQLAlchemy 2.0.57; exact PyMySQL 1.2.3 floor tuple | Complete offline suites: 3427 each; all pass, zero skips. |
| Python 3.8 / SQLAlchemy 1.4; Python 3.9 / SQLAlchemy 2.0 | Final installed wheel, no Arrow: imports/portable query compilation and 139 runtime checks each pass, zero skips. |
| Packaging and CI selection | Wheel/sdist build and Twine checks pass; all 63 runtime modules byte-match source. All 50 CI scope contracts pass. Collection partitions 1638 scenarios into 207 required and 1431 extended, with no omissions or duplicates. Black on 18 changed Python files, fatal Flake8 and diff checks pass. |

The full main/candidate sweeps precede the transaction-neutral listener
amendment and the focused FULLTEXT option-scope correction. Both change only
the official-4.2.8 branch after its unchanged physical-banner guard, plus
release-specific assertions and offline regressions. Main/candidate do not
select that branch. Their full sweep evidence is reused with the actual
consumer renewals above; this is not a second full sweep of the final source.
The release online suite and all eight offline profiles were rerun
completely, without source drift, with their distinct results above. The failed
full online sweep remains failed; focused renewal is not reported as a second
passing full sweep. Independent native handshake failure remains reproducible.
Analytics 340/340 is retained from the full
transaction-neutral run: the later fix only narrows FULLTEXT option matching;
analytics has no FULLTEXT queries and those paths are unchanged.

The option-scope correction adds ten lexical boundary controls, bringing the
focused suite to 68 passes on each of SQLAlchemy 1.4/2.0/2.1. Independent native
and SDK calls on original 4.2.8 both create/search synchronous gojieba indexes
when a column or index is named `async`; the old SDK listener wrongly refused
all three controls. The persistent online alternative retains its original
Chinese search data and additionally exercises these legal identifiers.

Server sweeps use Python 3.12.16, SQLAlchemy 2.0.43, PyMySQL 1.2.4 and aiomysql
0.3.2. The offline dependency matrix is separate from full online qualification.
Python 3.8/3.9 core imports and packaging remain required. Each backend owns
independent data, deployment/control-plane/status ports and a whole-fixture
lease through readiness and teardown. S3 buckets use unique names. Client
budgets and cleanup assertions are unchanged; fixtures use owned NVMe data.

### Retained history and scope limits

Before these SDK release adaptations, the original immutable server produced
27 online and nine analytics failures under full native-positive assertions.
That is a retained native reproduction baseline, not the current SDK acceptance
contract. The previous kernel-first requirement was incorrect for SDK delivery
and is superseded here. Original failed artifacts and revisions remain retained.
An initial missing Docker tmpfiles bind caused 25 additional file-access
failures; the corrected baseline excludes those environment failures.

Existing Arrow staging and pending-close lifecycle designs remain authoritative:
[upload compatibility](legacy-arrow-upload.md),
[bounded close amendment](legacy-arrow-upload-cleanup-amendment.md).
The two existing async admissions stay charged until actual close/producer
completion; no third producer/checkout/DDL is admitted early. The real SQLite
lifecycle fixture retains its one-slot pool, original deadlines, single DROP,
unrelated row, absence and reuse assertions.

This round does not certify every historical 4.1/4.2 patch, optional driver,
GPU or topology. Registered `matrixone://` DBAPI URLs remain unqualified; use SDK
connect or adopt a configured MySQL driver engine. Main's detached v1.3.0 banner
is not a release identity; exact source revisions and binary hashes identify its
code. The 4.2 candidate uses `MO_VERSION=4.2-dev`, not digits manufactured from a
worktree branch name.

Native upgrades are shutdown-only. Original released data and physical-upgrade
copies are retained; old binaries must not open newly written data. Old unmarked
empty databases cannot safely be inferred as branches, and old view domain
provenance cannot be reconstructed by upgrading alone.
