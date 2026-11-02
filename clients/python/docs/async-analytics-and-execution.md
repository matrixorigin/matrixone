# Async analytics and execution contracts

`AsyncClient.analytics` uses the client's existing aiomysql pool. PyArrow remains
optional: core imports, ordinary queries and sessions do not import it. Install
`matrixone-python-sdk[analytics]` on Python 3.9 or newer for file/Arrow operations.
Core Python 3.8 does not provide Arrow analytics. File paths are shared paths,
not an implicit file transfer, Arrow Flight or a zero-copy transport.

Query builders preserve SQLAlchemy aliases and correlated subqueries. Required
binds need a value; an explicit `None` sends SQL NULL. Reusing one callable bind
across clauses evaluates it once per statement, including through CTEs, and
reevaluates it on the next execution. Conflicting same-name bind definitions
across fragments reject before execution: use distinct names, reuse one bind,
or execute a complete SQLAlchemy statement for its binding precedence. Supply
SQLAlchemy join targets when a subquery needs JOIN correlation; the builder
cannot infer that context from raw JOIN strings.

Shorthand table definitions preserve `datetime(6)`, `timestamp(3)` and `time(6)`.
`database.table` and explicitly quoted atomic names address the same object for
creation and DROP. SQLAlchemy reflection also recognizes existing F16, BF16,
INT8 and UINT8 vectors; Arrow support is qualified separately.

SQLAlchemy 1.4.24 or newer is required: projection inference uses
`Select.get_final_froms()` (1.4.23), and native async streams use the public
`driver_connection` accessor (1.4.24). SQLAlchemy 2.1 itself requires Python
3.11 or newer; Python 3.8/3.9 use a compatible 1.4/2.0 series.

```python
async with client.analytics.query("SELECT id, amount FROM orders", batch_size=4096) as stream:
    async for batch in stream:
        consume(batch)

relation = await client.analytics.read_csv(
    "orders.csv", server_paths=["/shared/orders.csv"], header=True,
)
async with relation:
    sql = await relation.sql()  # checks client/server source availability
    async with relation.query() as stream:
        table = await stream.to_arrow()  # explicitly materializes remaining rows
    await relation.import_into("orders")

await client.analytics.upload_arrow(
    "new_orders", arrow_table, staging_dir="/client/shared",
    server_staging_dir="/server/shared", batch_size=4096,
)
```

File relations own uniquely named ordinary external catalog tables. Closing a
relation drops only those identities. Starting close seals further use; a timed-out
close retains one DROP task, and repeated close calls join it until it completes.
A failed DROP can be retried against only the remaining identities with
`await relation.close()`; it retains the engine on which the relation was made.
A process crash can leave tables. CSV defaults to a header; `header=False`
preserves the first row and uses explicit schema names or generated names.
JSON relations accept JSON Lines objects (`lines=True, orient="records"`).
Discovery stops at `max_files`; duplicate paths are scanned once. The immutable
`relation.sources` tuple exposes the stable expanded client-side paths in scan
order, including when distinct server paths are supplied.

Each async analytics operation uses `client.query_timeout` as its overall budget,
including planning, worker/pool admission, database work and COMMIT. Each stream
entry/fetch gets its own budget; time spent consuming a yielded batch is excluded.
`network_timeout` separately bounds an individual stream network operation and
does not time local file validation or CPU admission. Cleanup remains bounded and
may finish after timeout; inspect pending ownership and uncertain write outcomes.

A stream is a single-consumer async context manager. Close it after partial
consumption. Early exit, cancellation or conversion failure discards its
physical connection rather than draining unread results. EOF restores session
settings. Row limits bound batch row count, not the size of an individual cell.

Two blocking workers and two staging admissions are shared by each client
across analytics calls and reconnects. Streams reserve a worker before acquiring
a DB slot. Uploads hold no DB slot while preparing files and release their
worker before waiting for the pool. Staging files stay charged until cleanup;
a separate, bounded unlink lane prevents slow conversions from blocking cleanup.
The byte budget is checked before writes, including the IPC footer.

Cancellation cannot forcibly interrupt arbitrary user readers or filesystem
calls. A canceled producer retains its worker and file until the producer exits;
its completion callback cleans up even if the original event loop has closed.
`pending_operations()` reports operation IDs and exact owned staging paths.
An unlink failure retains admission; after fixing the filesystem, call
`await client.analytics.retry_cleanup(operation_id)`. Disconnect closes new
admission and cancels tracked DB work without waiting forever for foreign I/O.
A reconnect preserves existing worker accounting and refuses while old database
finalizers are still closing. Python may still wait for a blocked executor thread
when the interpreter exits.

Uploads/imports verify actual server autocommit is off before writes. They keep
UTC, strict conversion and PK checking through COMMIT. Before COMMIT, a failed or
canceled operation never schedules a later COMMIT; uncertain rollback discards
the physical connection. Original autocommit is restored only after a terminal
acknowledgement. A late server LOAD may fail when its canceled operation's file
is removed; this does not imply the server has already finished rollback.

Inspect `committed` on errors: `True` means COMMIT was acknowledged and cleanup
failed, `False` means failure before COMMIT dispatch, and `None` means unknown or
not applicable. A lost COMMIT response must be reconciled before retrying. No
SDK error flag authorizes an automatic uncertain-write retry. Older CPython
Tasks can replace `CancelledError` and lose attached attributes; treat a
canceled write as uncertain unless its original outcome was independently
observed. Pending staging identities and optional query events provide separate
diagnostics.

# Parameters, results and observations

Existing `execute()` retains the SDK's lexical `?` / `:name` conventions.
SQLAlchemy statements retain their dialect's typed bind/result processors.
`execute_driver()` is an explicit alternative using the owning driver's grammar:
PyMySQL/aiomysql use `%s` or `%(name)s`. It does not rewrite placeholders or values.
Follow DBAPI percent escaping when supplying parameters; with `params=None`, SQL
is sent verbatim. No supported legacy call emits a deprecation warning or changes
meaning; migration is opt-in.

Driver-bound execution requires the physical driver to know the session's SQL
mode. The matching server fix reports NO_BACKSLASH_ESCAPES in OK/EOF responses;
managed SDK connections perform session initialization. Some aiomysql versions
ignore authentication OK and result-set EOF status. For an externally configured
engine that skips all OK-producing setup, the caller must arrange an unbound
session-setting initialization on each physical connection before binding,
including reconnects. A SELECT alone does not initialize those driver versions.
Older servers lacking the status fix are not qualified for driver-bound NBE.


```python
result = await client.execute_driver("SELECT %s AS value", ("literal ? :name 100%",))
async with client.session() as session:
    result = await session.execute_driver("SELECT %(value)s", {"value": 7})
    value = result.scalar()
```

Client results are buffered `ResultSet` / `AsyncResultSet`; their fetch position
and scalar ordinal semantics agree. Session results remain SQLAlchemy results
owned by that session; fetch them before leaving it. Arrow streams are separate,
context-managed batch results.

`client.set_query_observer(callback)` enables immutable query events. The
callback is synchronous and should be fast; send events to an application-owned
queue for asynchronous processing. Observer failures cannot change database
outcomes, and observer-triggered queries do not recursively emit events. Disable
with `set_query_observer(None)`; no correlation IDs are allocated when disabled.
Events carry bounded operation/outcome categories, opaque correlation IDs, rows,
and measured pool/execute/fetch/conversion/commit/total durations. They contain no
SQL text, bind values, identifiers or credentials. `None` means a phase was not
measured. Session fetch after `execute()` is not included. Buffered DBAPI execution
may include network fetching inside driver execution; SDK fetch time measures
only its separate fetch call. Stream open, batch fetch and explicit materialization
are separate operations, excluding caller idle time from batch latency.

`get_capabilities(refresh=False)` uses read-only observations, independent of the
advertised version. Results distinguish supported, disabled, unsupported and
unknown; privileges or a successful EXPLAIN do not certify actual Arrow LOAD.
Public discovery checks the CN server ID in its existing metadata query and caches
observations in one bounded slot on the selected physical connection. Same-build
CNs and different pooled sessions do not share observations. Missing server IDs
use fresh, uncached observations. Ordinary SQL performs no capability discovery.
Use `refresh=True` after manual session-setting changes; these are point-in-time
observations, not a guarantee about a later proxy-selected backend. Uploads do not treat an observation
from a different pooled connection as a veto: they probe the actual selected
write checkout before CREATE/LOAD. A bounded owned staging file may therefore
be created and cleaned before an unavailable-server rejection. Explicit
unsupported/disabled overrides reject before staging; a supported assertion does
not bypass an actual server rejection. Unknown observations remain unknown.
MatrixOne 4.2.5 rejects native Arrow LOAD, while ordinary Arrow query/file scanning
can still work. Explicit caller overrides are assertions, not SDK certification.

SQL text logging now defaults to `off`, as do the logger factories. Driver error
logs retain category/code without rendering SQL, parameters or server-echoed
values. The raised exception preserves its original diagnostics. Existing
`auto`, `simple` and `full` modes remain available by explicit configuration;
`auto`/`full` and requested tracebacks can include SQL/data. Metrics callbacks
never contain those fields. This default prevents Stage credentials and literal
ORM values from being printed merely by enabling the SDK.

## Administrative and fulltext boundaries

Fulltext index DDL does not configure ranking. Explicit non-default index
`algorithm="BM25"` requests now fail before checkout; use the search builder's
`.with_algorithm("BM25")` for ranking on the connection executing that search.
The historical TF-IDF default argument remains accepted for index creation and
does not change the query session's ranking policy.

PITR SHOW statements inside an active explicit transaction are rejected by the
qualified MatrixOne source/release servers. Perform PITR inspection outside that
transaction; the SDK does not commit another caller's transaction to bypass this.
CREATE acknowledgement also does not prove a table persisted: a later index
failure can roll back the surrounding CREATE transaction.

## Local file import

Default SDK-created connections support `load_data.from_local_file(path, table)`
without another connection option. The protocol capability is negotiated while
file reads remain disabled; only that statement on its selected connection may
read the exact requested path, once. The same owner and transaction apply to
Session imports. Errors, later SQLAlchemy hooks and cancellation revoke this
authority. A server requesting a different file closes the physical connection.

`connect(local_infile=False)` disables transfer and rejects before SQL.
Explicit `local_infile=True` preserves the original unrestricted driver opt-in,
including raw LOCAL SQL; use that mode only with a trusted server because the
driver accepts server-directed filenames. Adopted engines are not silently
reconfigured or certified for file transfer.

## Stage names and literal values

The included server fix preserves quoted Stage names, URLs, credentials and
comments in catalog reads/writes. Upgrade every CN before using this contract.
For a logical name which is not a URL hostname, use
`stage:///` + `urllib.parse.quote(name, safe="")` + `/file.csv`; Stage convenience
load/export methods build this representation. The first encoded segment is the
name, so `%2F` and `%25` inside it are decoded once. Dot-only names use host form.
Export preserves filename templates independently of URI query encoding.

The existing comma-separated credentials format cannot represent commas in
keys/values, equals signs in keys or duplicate keys after case folding. Those
requests fail before any CREATE/ALTER mutation; equals signs in values work.

See [design revision 2.3](design/systemic-sdk-boundaries.md) for ownership,
compatibility and failure-path decisions.
