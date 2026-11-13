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

The main source target is `14b1ddced58d411c4b13ffaaf6034b3468076009`.
The patched 4.2 target is [the kernel companion PR #32723](https://github.com/matrixorigin/matrixone/pull/32723),
based on 4.2-dev `1a3e7fb6960995e9a143e5cc3d5a50f2feb0980f`.
Official **v4.2.8**, revision `10d204b8763f19b2b37ba2147d1d4c1a7828d6d4`,
remains the immutable reproduction and physical-upgrade source, pinned to
`matrixorigin/matrixone@sha256:eec6cee1ab0d1ddaa5c81cbc9d79ef6a59ecf13f2b5835631ef3b03ee6822fc6`.
Fixes in the companion need a new 4.2 release; this SDK does not rewrite an
already published server binary.

Each source target has its own two-CN cluster, data, deployment identity and
HAKeeper/raft/gossip/status ports. Restore-capable suites own an exclusive lease
for the whole cluster, including readiness and teardown. Different databases
alone cannot isolate `RESTORE CLUSTER`. The earlier missing S3 Stage was
reproduced as overlapping cluster restore rewinding `mo_catalog.mo_stages`;
serial restore/S3 controls passed with no SDK Stage workaround.

Source binaries are pinned by Git revision and SHA-256, independently of the
version banner. Build the 4.2 candidate with `make build MO_VERSION=4.2-dev`:
Make's default uses the local working branch, and the legacy native formatter
turns `codex/pr32671-42-compat` into the invalid version `3267142`. The corrected
build keeps the real branch and commit metadata and advertises the actual
4.2 development series. A new full 1296-case online sweep passes with no skips.
The SDK parser and version assertions are unchanged.
Main's detached source build advertises v1.3.0; its revision identifies the code.

Full online/analytics validation uses Python 3.12.16, SQLAlchemy 2.0.43,
PyMySQL 1.2.4 and aiomysql 0.3.2. Additional real async upload controls exercise
SQLAlchemy 1.4 on patched 4.2 and 2.1 on main. Offline profiles are a separate
matrix; they do not imply an online cross-product of every dependency version.

| Selection | Result |
|---|---|
| Full offline, Python 3.12 / SQLAlchemy 1.4.54, 2.0.43, 2.1.8 | 3363 / 3359 / 3367 passed; no errors, failures or skips |
| Full offline, Python 3.10 / SQLAlchemy 1.4.54, 2.0.57 | 3363 / 3359 passed; no skips |
| Full offline, Python 3.11 / SQLAlchemy 2.1.8 | 3367 passed; no skips |
| Full offline, Python 3.12 / SQLAlchemy 2.0.57 | 3359 passed; no skips |
| Exact PyMySQL 1.2.3 regression tuple, Python 3.12 / SQLAlchemy 2.0.43 / aiomysql 0.3.2 | 3359 passed; no skips |
| Core without Arrow, Python 3.8 / SQLAlchemy 1.4 and Python 3.9 / SQLAlchemy 2.0 | 139 runtime checks passed per profile; installed wheel import/portable compilation passed |
| Main complete online | 1296 passed at `2a42b3e99f`; semantic reuse for `14b1ddced58` described below |
| Main complete analytics on current target | 338 passed, no errors/failures/skips, unchanged source |
| Patched 4.2 complete online | 1296 passed in the new complete sweep; no failures, errors or skips, unchanged source |
| Patched 4.2 complete analytics, including all former Arrow positives | 338 passed in the final complete sweep; no failures, errors or skips, unchanged source |
| Real async driver facade controls | Six passed on SA1.4/patched4.2 and six on SA2.1/main; no failures, errors or skips |
| Native 4.2 stateful BVT | Six Stage/snapshot/branch scripts passed twice: each run has 911 statements, 903 successes, 8 unchanged issue-marked ignores and no failures/abnormal results |
| Native saved-SHOW BVT and missing-process guard | Three complete scripts pass twice: 510 statements, 509 successes, 1 existing ignore each; whole datalink + stageutil race/coverage, vet and lint pass |
| Native 4.2 upgrade | Actual physical copy of official v4.2.8 data passed value, marker, restore, legacy receipt and rejection checks |
| Native 4.2 concurrency | Whole frontend race suite, four affected roots 100 times each, deterministic embedded DDL/delete lifecycle under race passed |
| CI selection parity | 1634 distinct online/analytics cases: 207 required + 1427 extended; no omissions or duplicates |

### Preserved behavior and evidence reuse

| Scenario | Retained or strengthened oracle |
|---|---|
| Abandoned, rolled-back and committed transactions | Independent observer rows; rollback before autocommit; failed-reset disposal; explicit AUTOCOMMIT and next-checkout state on real sync/async drivers |
| Export literals, comments and callable binds | Dollar/numeric tokens, both escaping modes, actual one-slot checkout, exact CSV/JSONL bytes, failure/cancel/timeout cleanup and one callable evaluation |
| Multiple predicates, joins and CTEs | Grouped typed conditions, atomic invalid calls, raw binding, native RIGHT JOIN, executed CTEs and complete row/value results |
| Metadata and FULLTEXT | Exact decimal values and processors, standard MySQL BOOL/view reflection, native schema replay and FULLTEXT-only catalog/index counts |
| CLONE and branch lifecycle | Missing-source non-mutation, existing-target no-op, populated and empty branch deletion, source preservation, historical clones, account privileges and DDL/restore conflict controls |
| Arrow upload on 4.2 | Same public input, wire schema, values, create/append/rollback, staging bounds, cross-CN conflicts, cancellation and session/pool cleanup through checked Parquet staging |
| Raw native Arrow rejection | Independent 20300 format-unsupported control remains; a successful Parquet upload does not advertise native Arrow support |
| Async pending probe close | Timeout, primary error and cancellation retain the existing two-upload admission; the third operation cannot consume input, check out or create a table; drain and reuse are checked |
| Relation cleanup retry | Real one-slot aiosqlite pool, both initial deadline/cancel paths, one retained DROP, one execution, unrelated row 42, absence and released pool are unchanged; in-memory SQLite removes irrelevant disk fsync |
| OFFSET None/0/nonzero and LIMIT None/0/positive | Three fulltext facades plus sync/async ORM exact slices, preserving explicit zero and unlimited semantics |

Both current-main analytics attempts that found a file-backed SQLite deadline
and two native SQL timeouts are retained. The later in-memory cleanup fixture
keeps its original deadlines; the final full analytics run on a fresh NVMe
instance passes all 338 cases with no SDK timeout increase. During the earlier
run host IO pressure exceeded 50%, and a later native stack showed Raft and
LocalETLFS fsync waits. This supports the resource explanation without claiming
a pre-event causal profile. Original data and partial migration copies remain
untouched; the physical-upgrade evidence is recorded separately.

The final offline matrix freezes all production, selected test and script inputs.
The later in-memory fixture edit changes only two unselected analytics scenarios;
all offline semantic inputs remain equal. The async probe-close fix has full
three-series focused and core-without-Arrow evidence and renewed complete
analytics evidence; online modules do not call its async upload owner.

The 1296-case main online sweep used exactly the final SDK production/online
inputs before the probe-close fix. The only subsequent main kernel delta is
`14b1ddced58`'s INTERVAL user-variable binding guard; those online queries do not
use INTERVAL. Fourteen independent current-main public Session controls cover
calendar operations, inherited numeric behavior, source values, the native
strict-literal error and next-checkout reuse. The final 32-case wave2 selection and complete 338-case analytics suite pass
on current main and exercise their current consumers. This records the scope of reused evidence;
it does not claim one new exact-head full run or treat a pending test as a pass.

Final distributions build and pass `twine check`. All 62 packaged Python modules
byte-match the final source. Installed Python 3.8/3.9 core imports and portable
compilation pass without Arrow. Scoped Black covers all 66 changed Python files;
fatal flake8 and the 50 CI scope contracts pass.

### Release capability boundaries

- Native Arrow LOAD remains unavailable on 4.2. A bounded read-only probe permits
  Parquet only for the exact native 20300 format rejection. Unknown errors do
  not choose a fallback; an invalidated connection fails. The actual write
  checkout is requalified before DDL, and input is consumed once into one
  bounded file. All former Arrow-dependent positive tests execute; their skip
  gate has been deleted. Native Arrow stays the main path.
- 4.2 has no VECF8/VECF4 storage types. Four explicit native 1064 and no-catalog-
  object checks are negative capability contracts, with no skips. They do not
  certify positive storage/round-trip support. Main and unrecognized source
  profiles keep the original positive cases. VECF16/VECF32 positives remain.
- The companion fixes TLS buffering/reset monitoring, schema display/view
  provenance, FULLTEXT identifiers, numeric tokenization, Stage grammar/catalog
  writes, FLOAT32 infinities and branch identity/restore locking. Existing
  persisted ENUM/SET views that lost provenance require recreation; an old
  unmarked empty database cannot safely be inferred to be a branch. The upgrade
  is shutdown-only, and the original released data is not opened by the new
  binary. Do not run the old binary on the upgraded copy.

The online CI matrix still has two jobs (4.2.8 and current-source); no runners or
shards were added. Required/extended file sets are disjoint and complete.
An unconfigured/skipped GitHub online workflow is not online validation, and
local pass evidence is not a claim that every GitHub check is green. No job
wall-time reduction is inferred from checkout/build time or case counts.
