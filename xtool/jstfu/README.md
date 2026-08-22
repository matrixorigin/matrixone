# jstfu — Java STream Fast Upload

The reference gRPC server for MatrixOne **datastream external tables**
(`CREATE EXTERNAL TABLE ... ENGINE = DATASTREAM`).  MO opens a streaming
`Read` RPC to this server and consumes CSV-encoded chunks; see the design
spec in `docs/cn/stream_transport_fast_upload.md` and the wire contract in
`proto/datastream/v1/datastream.proto` (single source of truth, compiled for
both Go and Java).

## Build

```
make jstfu            # from the repo root; needs a JDK and Maven
# or: MVN=/path/to/mvn make jstfu
```

Produces the self-contained fat jar `xtool/jstfu/target/jstfu.jar`.  The jar
targets **Java 8 bytecode** so it runs on the JDK 8 of the BVT tester docker
image (`matrixorigin/tester:go1.26.4-jdk8`); building works on any modern JDK
(`--release 8`).

## Run

```
java -jar xtool/jstfu/target/jstfu.jar conf/example.json
```

Configuration is a JSON file:

```json
{
    "port": 4444,
    "chunksize": 1048576,
    "datasource": [
        { "name": "the_table_to_read_from",
          "type": "jdbc",
          "connectionstring": "jdbc:mysql://127.0.0.1:6001/test",
          "user": "dump",
          "password": "111",
          "sql": "select col1, col2 from src where ${FILTER}" },
        { "name": "a_file",
          "type": "file",
          "path": "/path/to/file.csv" }
    ]
}
```

- `name` must be unique; it is what the MO table's `'table'` option refers to.
- **jdbc**: runs the configured SQL per request.  A `${FILTER}` placeholder is
  replaced with the pushed-down predicate text (or `1=1` when the request has
  none).  For the filter to be valid on the source side, the query's column
  names should match the MO external table's column names.  The substitution
  is textual — the SQL runs with the configured credentials, so only serve
  trusted MatrixOne instances.
- **file**: streams an existing CSV file verbatim, re-chunked so that no
  record spans two chunks.  The filter hint is a noop.

## CSV dialect

Fields separated by `,`, enclosed by `"` when needed, backslash escapes,
records terminated by `\n`, NULL encoded as unquoted `\N` — the dialect the
MO reader parses by default.

## MO side

```sql
CREATE EXTERNAL TABLE t (col1 int, col2 datetime, col3 varchar(100), col4 text)
ENGINE = DATASTREAM WITH (
    'server' = '127.0.0.1',
    'port' = '4444',
    'table' = 'the_table_to_read_from',
    'recheck' = 'true'      -- optional, default true
);
SELECT * FROM t WHERE col2 > '2020-11-11 00:00:00';
```

`recheck` is the pushdown authority switch:

- `recheck = 'true'` (default, **safe**): no filter is pushed. The server
  returns the full datasource and MO applies every predicate locally, so the
  result is correct regardless of the server's collation, time-zone, or
  coercion semantics. A pushed predicate is not provably superset-preserving
  across engines — a case-insensitive source evaluating `s <> 'a'` drops both
  `'a'` and `'A'`, but MO wants to keep `'A'`, and local recheck can only
  remove over-returned rows, never restore dropped ones — so the safe default
  never lets the server narrow.
- `recheck = 'false'` (**opt-in**): the WHERE conjuncts that deparse to plain
  SQL text (comparisons, AND/OR/NOT, IN, BETWEEN, IS [NOT] NULL, LIKE over
  columns and literals) are pushed and applied server-side, and MO skips
  local re-evaluation for exactly those conjuncts. Use this only when the
  source's predicate semantics match MO's for the pushed columns (e.g. a
  jdbc bridge to another MySQL-compatible engine with matching collation);
  it trades the correctness guarantee above for reduced data transfer.

## Security

The server binds to **127.0.0.1** by default (`host` in the config): the
`${FILTER}` text is substituted into SQL and requests are unauthenticated, so
an off-box client that reached the port could run arbitrary SQL with the
configured credentials. Co-located MO reaches it on loopback (the compose
sidecar shares the CN network namespace; a launch deployment runs on the same
host). Set `host` to `0.0.0.0` or a specific NIC only behind an authenticating
trust boundary you control.

Binary columns (`BINARY`/`VARBINARY`/`BLOB`) are out of scope for the v1 CSV
bridge — they cannot round-trip byte-for-byte through a UTF-8 CSV stream, so
the jdbc source rejects them rather than corrupting; project them as text
(e.g. `HEX(col)`) in the configured SQL.

## Tests

- Java unit tests: `mvn test` (chunker record-boundary invariant, `${FILTER}`
  substitution, config validation).
- Cross-language e2e (starts this server itself): `go test ./test/datastream/`
  — see `test/datastream/datastream_e2e_local_test.go`; the MO-side test
  runs when a MatrixOne is reachable (`MO_DATASTREAM_E2E_DSN`, default
  `dump:111@tcp(127.0.0.1:6001)/`).
- BVT: `test/distributed/cases/datastream/` — start the server first with
  `optools/jstfu_bvt.sh <resources_dir> <mo_host:mo_port>`.
