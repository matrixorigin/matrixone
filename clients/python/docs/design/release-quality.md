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

Test 4.1.0/4.1.6/4.2.0/4.2.6 with the full suite and every intermediate stable
patch with core contracts. Independently test SQLAlchemy 1.4, 2.0 and 2.1.
Native unsupported outcomes are accepted only by a narrow native-versus-SDK
negative contract, recorded as feature outcomes rather than hidden skips.
Optional TLS/S3/HNSW, multi-CN topologies and async Arrow are separate qualification
surfaces; passing this matrix does not certify them. Direct registered
`matrixone://` engine URLs still lack a DBAPI entrypoint in the baseline and are
not qualified here; use SDK connect or adopt a configured MySQL driver engine.

## 2026-11-13 release and main validation

The selected targets are official **v4.2.8**, revision **10d204b**, and main
**1064837ae2648aa20e5e1abb7edd0564493e6b4d**, rechecked against the remote.
The release image is pinned to
`matrixorigin/matrixone@sha256:eec6cee1ab0d1ddaa5c81cbc9d79ef6a59ecf13f2b5835631ef3b03ee6822fc6`.
Main's version banner is v1.3.0; the git revision identifies the source build.
Each disposable target has two CNs, shared server/client export paths and its
own data. This does not certify other historical releases.

The full online/analytics sweeps use Python 3.12.16, SQLAlchemy 2.0.43,
PyMySQL 1.2.4 and aiomysql 0.3.2. Final native pool/export/error checks also
pass 34 cases on SQLAlchemy 1.4.54/release and 2.1.8/main. The offline matrix
does not imply a full online cross-product of all dependency versions.

| Evidence | Result |
|---|---|
| Full offline, Python 3.12 / SQLAlchemy 1.4.54, 2.0.43, 2.1.8 | 3351 / 3347 / 3355 passed; no skips |
| Full offline, Python 3.10 / SQLAlchemy 1.4.54, 2.0.57 | 3351 / 3347 passed; no skips |
| Full offline, Python 3.11 / SQLAlchemy 2.1.8 | 3355 passed; no skips |
| Full offline, Python 3.12 / SQLAlchemy 2.0.57 | 3347 passed; no skips |
| Exact regression tuple, Python 3.12 / SQLAlchemy 2.0.43 / PyMySQL 1.2.3 / aiomysql 0.3.2 | 3347 passed; no skips |
| Installed core without Arrow, Python 3.8 / SQLAlchemy 1.4 and Python 3.9 / SQLAlchemy 2.0 | Import and portable query compilation passed; 119 runtime checks passed per profile |
| Complete main online sweep | 1273 passed, no skips |
| Final changed-owner online selection, main and release | 227 passed on each; release functional checks explicitly use the plain CN |
| Complete analytics, main | 319 passed, no skips |
| Complete analytics, release plain CN | 235 passed, 84 Arrow-dependent positive cases unvalidated |
| Complete release online sweep using a TLS-capable primary | 1254 passed, 7 failures, 8 setup errors, 4 unsupported vector skips; **not green** |
| Distributions and static checks | Wheel/sdist build, `twine check`, fatal flake8 and diff checks passed |

The complete sweeps preceded the last error-logging, pagination and fixture
corrections. The final 227-case selection rechecks their owners and public
consumers; all eight offline profiles use the final Python source. Broad source
manifests record edits during investigation rather than concealing drift.
The final online selection's SDK, scripts and online inputs are unchanged;
only unselected offline SQL-text assertions changed during its main run.
The current collection has 1285 online and 319 analytics cases. Its case-by-case
inventory is accounted for by the complete sweeps plus final owner validation:
1285 main online cases have passing evidence; release has 1281 passing case
oracles and four explicitly unvalidated vector cases. Native-error oracles
below do not certify the unavailable positive feature.

### Coverage decisions

| Earlier scenario | Retained or strengthened oracle |
|---|---|
| Abandoned, rolled-back and committed transactions | Independent observer reads, failed-reset disposal, explicit AUTOCOMMIT and next-checkout mode; sync/async real drivers and three SQLAlchemy series |
| Export literals, comments and callable binds | Dollar/numeric token controls; default and NO_BACKSLASH modes; actual one-slot checkout identity; exact CSV/JSONL bytes; failure/cancel/timeout cleanup and one callable evaluation |
| Multiple predicates, joins and CTEs | Grouping and atomic invalid calls; explicit raw binding; native RIGHT JOIN; executed CTEs and complete row/value oracles replace unused CTEs and permissive fallback assertions |
| Metadata, labels and CLI indexes | Original unique label controls retained; exact decimals, result names/processors, FULLTEXT-only catalog selection and logical index counts |
| CLONE, branch and historical sources | Missing-source non-mutation, existing-target no-op, populated branch deletion and source preservation, dropped-source snapshot clones |
| Analytics lifecycle and cross-CN conflicts | Original Arrow positives remain on main; file variants retain released-server conflict, rollback and session restoration; the same 20000-row/127-row streaming boundary uses Parquet preparation |
| OFFSET None/0/nonzero and LIMIT None/0/positive | All three fulltext facades plus sync/async ORM slices; unlimited SQL spelling changes without imposing a smaller row limit |

### Release limitations and unresolved observations

* **TLS remains blocked in official 4.2.8.** An independent coalesced SSLRequest/
  ClientHello times out; main completes TLS 1.3. Ordinary TLS controls pass both.
  Main includes [the buffered-handshake fix #29996](https://github.com/matrixorigin/matrixone/pull/29996);
  the released binary still hands TLS the raw socket without unread buffer bytes.
  The 4.2-dev backport is tracked in [#32723](https://github.com/matrixorigin/matrixone/pull/32723).
  Plain functional results cannot certify TLS, and no SDK retry or TLS fallback
  was added.
* Native Arrow LOAD returns 20300. The 84 positive write cases are skipped only
  for the verified exact release and a definite unsupported observation. A
  separate native/public negative control verifies the error, no committed
  table, no staging files and pool release. Unknown/current versions run the
  positive cases. Native VECF8/VECF4 absence accounts for four online skips.
* Empty database branch deletion, particular fulltext identifier generation,
  standalone numeric/dollar aliases, standard MySQL view/BOOL reflection and
  ENUM/SET SHOW CREATE replay have native incompatibilities. Named differential
  cases record native error/metadata outcomes and preserve source data. Main
  retains positive coverage; these are not SDK-provided server fixes.
* The earlier missing S3 stage came from overlapping validation runs on one
  disposable cluster. The online manager suite executed `RESTORE CLUSTER`
  while analytics created a stage after the snapshot. Restore rewound
  `mo_catalog.mo_stages`; an independent native control reproduced the same
  20300 error. Suites sharing a cluster that can restore account or cluster
  state must run exclusively, including preflight and teardown. Separate
  databases do not isolate this global catalog state.

The online CI target is updated to 4.2.8/current-source with the same two jobs.
Its required and extended selections are disjoint: 207 and 1397 cases cover the
1604-case collection with no omissions or duplicates. The release-contract
file previously ran in both phases. Configuration must identify the selected
isolated server and shared paths; a skipped/unconfigured workflow is not online
validation. No job wall-time reduction is inferred from checkout/build costs.
