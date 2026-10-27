# Python SDK contract testing

Run from `clients/python`. Core query, driver and lifecycle tests use real
SQLAlchemy dependencies, not a global replacement of its modules.

```sh
python -m pip install -e '.[core-test]'
python scripts/run_contracts.py --output contract-results
```

The runner records Python/dependency versions, source revision, worktree status,
source/test/config hashes, test selection, counts and exit status in
`provenance.json`, with JUnit results beside it. It fails on empty selections,
failures, errors, skips or inputs changing during execution. Server fixtures
record observed version and git revision in JUnit properties. A revision alone
does not identify a dirty checkout; the content manifest identifies its tested
inputs. Portable native reports and their JUnit results are indexed in
[docs/validation/README.md](docs/validation/README.md).

## Dependency and server dimensions

`tox` selects SQLAlchemy 1.4, 2.0 and 2.1 independently. Exact 2.0.43 is also
validated for the aiomysql pre-ping regression. Python 3.8/3.9 core wheel-import
checks exclude Arrow; analytics requires its separate optional dependency and
supported Python version. Installing the core SDK must not require PyArrow.

Server contracts are explicitly configured; an offline green run does not prove
server compatibility. Release qualification selects an exact MatrixOne 4.1.x or 4.2.x patch, or a built
current-source server. Use full contracts on 4.1.0, 4.1.6, 4.2.0 and 4.2.6,
and core contracts on every intermediate stable patch. Set these variables for an isolated disposable target:

- `MO_HOST`, `MO_PORT`, `MO_USER`, `MO_PASSWORD`.
- `MO_SDK_EXPECT_VERSION`: the exact selected release, or the source server banner.
- `MO_SDK_EXPECT_REVISION`: required for `current-source`, matching `git_version()`.
- `MO_SDK_EXPORT_DIR`: directory visible to the server.
- `MO_SDK_EXPORT_LOCAL_DIR`: the same directory visible to the test process.
- `MO_SDK_DISPOSABLE_SERVER=1`: required for full contracts, only when the server or tenant is disposable and
  exclusively owned by this test run. Case-mode tests change a tenant-global setting and must run serially; they
  restore it afterwards, but concurrent sessions could observe the temporary setting.

```sh
python scripts/run_contracts.py --server 4.1.0 --suite full --output release-contracts
python scripts/run_contracts.py --server 4.2.3 --suite core --output core-contracts
python scripts/run_contracts.py --server current-source --output source-contracts
```

Tests create unique objects and clean them up. The configured account needs the
DDL, snapshot, publication, vector/fulltext and checkpoint permissions exercised
by the suite. Assertions compare native SQL values, not just successful calls or
Top-K ordering. Required contracts never silently skip an unavailable feature.
The broader online/analytics suite is reported separately with its skip count.

## Compatibility behavior

Version banners are descriptive, not a capability authority. Client capability
observations are per connection generation, bounded and immutable; unknown
support stays unknown. Explicit feature actions and operations can still be
rejected by the server. The SDK never retries a failed write to probe support.

One async session belongs to one task. Use sequential awaits within a transaction;
use separate sessions for parallel transactions. Client session contexts issue
server `BEGIN` before yielding, then commit or roll back and close. Administrative
commands retain the server's transaction restrictions; create snapshots outside
these contexts. Cancellation does not prove that an already submitted COMMIT
was rolled back.

Connection hooks run once per physical pool connection at checkout, including
connections adopted from an existing engine. Awaitable callbacks finish before
admission. Use `ConnectionAction` for initialization SQL; callbacks cannot reenter
the same client's pool. Ordinary callback errors remain best effort and are
logged; cancellation propagates while the pool owns physical cleanup.

An explicit fulltext algorithm is scoped to the query's connection. Restore the
previous setting before pool reuse; discard the connection if restoration fails.
The primary query error/cancellation retains precedence over cleanup failures.

LOAD option validation happens before execution. Inline LOAD and grammar-only
string tokens reject NUL and backslash because their meaning depends on server
SQL mode. For inline payloads containing these characters, use a caller-owned
file and the file-loading API; file paths and separator/escape tokens themselves
remain subject to the same token restriction. Unsupported option combinations
raise instead of silently discarding arguments.

## Static checks and contribution evidence

Run the repository Black configuration on changed Python files and fatal flake8
checks (`--select=E9,F63,F7,F82`) on their owning modules. Type-check shared query,
snapshot and capability boundaries using the configured mypy settings. Record
baseline findings separately from introduced failures.

Regressions should exercise public methods with precise native-SQL oracles and
assert resource reuse after errors. For async lifecycle changes, use deterministic
cancellation and physical pool ownership assertions across supported SQLAlchemy
versions. Python has no Go-style race detector; a skipped concurrency check is
not equivalent to this evidence.

## Additional data contracts

Pinecone handles require exactly one primary-key column. Upsert validates the
whole batch before mutation and uses one transaction for all rows. Server errors
or cancellation before commit roll back that transaction; an unknown COMMIT
outcome is not automatically retried. An async batch shares one query timeout.
Returned vectors are numeric lists and metadata excludes the ID column.

Vector similarity/range/rank results reject duplicate column labels, including a
stored `distance` colliding with the computed value. Select
`distance AS stored_distance` explicitly to keep both. `l2_sq` uses the server's
squared-distance function; unknown metric names raise.

LOCAL imports require `connect(..., local_infile=True)` on both clients. The
option is false by default and negotiated when the driver opens a connection.
LOCAL calls on disabled or externally adopted engines fail before execution;
use an explicitly configured SDK connection. No per-load pool mutation occurs.

Fulltext STRING grammar supports ordinary quoted text and backslashes without a
dollar sign. NUL or combined backslash/dollar input raises before execution due
to the server lexer restriction. Queries never change sql_mode to escape text.

Metadata `scan(source_column='v', columns=['min', 'max', 'sum'])` reads that
column's native encoded ZoneMap values. With no source_column, explicit statistic
projections (also structured `columns='*'` or `[]`) discover and sequentially
scan the observed columns. `columns=None` and explicit `source_column='*'` keep
the wildcard fast path, where ZoneMaps may be NULL. Object brief/detail statistics
count row/size fields once per object; null counts sum null cells across columns
and may exceed row counts. Column sizes and object sizes have distinct meanings.

CSV relations compare fresh observed source types to the creation-time observed
schema before exposing SQL, querying or importing. Intentional explicit casts
remain supported. Inference inspects a bounded initial block, not an immutable
file snapshot; changes after validation or beyond that block are not ruled out.

Stage CREATE returns observed catalog state even for IF NOT EXISTS. Missing
ALTER IF EXISTS returns None. Catalog lookup errors after acknowledged Stage or
PITR writes retain their error category and state the readback failure.


## Grouped fulltext and mapped source availability

`boolean_match('body').group(a, b)` requires either supported positive branch.
Each child can be one required atom or a flat optional OR of plain word/prefix
atoms. Children must use the same columns and BOOLEAN mode. Conjunctions,
exclusions, phrases, weights and nested/raw boolean syntax inside a child raise
before changing the parent; existing explicit `FulltextGroup` methods retain
their separate behavior.

File relations validate every explicit mapped server file through a bounded
DATALINK read (one byte per source), at construction, before scanning/importing,
and at completed stream EOF or before import commit. Checks reuse the active
stream/write connection; missing sources raise and a failed import rolls back.
Existing empty files remain distinct from missing files. Local schema checks
also remain in place. Server DATALINK read permission is required.

Mappings accept absolute local paths, local `file:///` URIs and `stage://` URIs.
Relative paths, URI query/fragment components and literal glob metacharacters
are rejected. Native filename restrictions still apply: for example 4.2.5
rejects hash/space names, and its Stage resolver cannot represent percent names.
These failures propagate; the SDK never strips a filename or tries another file.

These are availability observations, not immutable file snapshots. Replacing a
file or removing and restoring it between checks is not detected. A stream can
already have yielded partial batches before a final validation error; callers
must observe successful exhaustion before treating its dataset as complete.
Early close does not certify completion. SQL text obtained from `.sql` does not
pin files or validate a later independently executed statement.

All SDK contract tests live under `tests/offline/contracts`, so the existing
trusted SDK workflow's `pytest tests/offline tests/analytics` selects them.
The local runner rejects skips and empty selections for the required offline
and server contracts. SDK workflow enhancements and non-SDK workflow routing
are delivered separately from SDK production changes; their configuration only
controls `pull_request_target` once present in the trusted base.

Decimal parameters retain native driver precision. Where a legacy builder must
render a SQL literal, it uses fixed-point notation to prevent scientific SQL
tokens from triggering floating-point comparisons. Non-finite values and
fixed-point representations exceeding 4096 characters are rejected before
formatting; the expansion budget includes the sign and decimal point. This
budget also conservatively rejects excessive positive-exponent zero inputs.


Release boundaries and deliberate compatibility restrictions are described in
[the release compatibility design](docs/design/release-quality.md). Core server
contracts do not require the export mount variables. Full contracts do. Neither
suite substitutes for optional TLS, S3, HNSW or multi-CN qualification.

CLONE managers verify target existence after acknowledgement. Manager
`if_not_exists=True` uses a read-only catalog observation and never the native
flag, which can duplicate existing data. Raw clone builders reject that flag;
use `client.clone` or `session.clone` for the safe operation. Concurrent creation
can still produce a native error. Async clone steps share one query timeout.
