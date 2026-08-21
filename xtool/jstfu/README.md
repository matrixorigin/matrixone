# jstfu — Java STream Fast Upload

The reference gRPC server for MatrixOne **datastream external tables**
(`CREATE EXTERNAL TABLE ... ENGINE = DATASTREAM`).  MO opens a streaming
`Read` RPC to this server and consumes CSV-encoded chunks; see the design
spec in `docs/cn/stream_transport_fast_upload.md` and the wire contract in
`proto/datastream/v1/datastream.proto` (single source of truth, compiled for
both Go and Java).

## Build

```
make jstfu            # from the repo root; needs JDK 17+ and Maven
# or: MVN=/path/to/mvn make jstfu
```

Produces the self-contained fat jar `xtool/jstfu/target/jstfu.jar`.

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

The WHERE conjuncts that can be expressed as plain SQL text (comparisons,
AND/OR/NOT, IN, BETWEEN, IS [NOT] NULL, LIKE over columns and literals) are
pushed to the server as a hint.  With `recheck = 'true'` (default) MO
re-applies every filter locally, so a server that ignores the hint is still
correct.  `recheck = 'false'` skips local re-evaluation for exactly the
conjuncts that were pushed.

## Tests

- Java unit tests: `mvn test` (chunker record-boundary invariant, `${FILTER}`
  substitution, config validation).
- Cross-language e2e (starts this server itself): `go test ./test/datastream/`
  — see `test/datastream/datastream_e2e_local_test.go`; the MO-side test
  runs when a MatrixOne is reachable (`MO_DATASTREAM_E2E_DSN`, default
  `dump:111@tcp(127.0.0.1:6001)/`).
- BVT: `test/distributed/cases/datastream/` — start the server first with
  `optools/jstfu_bvt.sh <resources_dir> <mo_host:mo_port>`.
