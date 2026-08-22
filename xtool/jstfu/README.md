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
java -jar xtool/jstfu/target/jstfu.jar /path/to/config.json
```

The one and only argument is the path to a JSON config file (read once at
startup; edit and restart to change it).

## Configuration file

A JSON object with top-level server settings and a `datasource` array:

```json
{
    "host": "127.0.0.1",
    "port": 4444,
    "apikey": "",
    "chunksize": 1048576,
    "datasource": [
        { "name": "the_table_to_read_from",
          "type": "jdbc",
          "connectionstring": "jdbc:mysql://127.0.0.1:6001/test?useSSL=false&allowPublicKeyRetrieval=true",
          "user": "dump",
          "password": "111",
          "sql": "select col1, col2 from src where ${FILTER}" },
        { "name": "a_file",
          "type": "file",
          "path": "/path/to/file.csv" }
    ]
}
```

### Top-level fields

| Field | Type | Default | Meaning |
|---|---|---|---|
| `host` | string | `"127.0.0.1"` | Interface to bind. `127.0.0.1` = loopback only (not reachable off-box); `0.0.0.0` = all IPv4 interfaces; `::` = all interfaces incl. IPv6; or a specific NIC address. **Set an `apikey` before binding a routable interface** (see [Setting the listen address and API key](#setting-the-listen-address-and-api-key)). |
| `port` | int | *(required)* | TCP port to listen on. Must match the MO table's `'port'` option. |
| `apikey` | string | `""` | Shared-secret API key. When non-empty, every request must present a matching key (constant-time compared) or the server replies `ERROR_UNAUTHENTICATED`. Empty disables the check. |
| `chunksize` | int | `1048576` (1 MiB) | Target CSV chunk size in bytes. Chunks are cut only at record boundaries, so an individual chunk may exceed this by up to one record. |
| `datasource` | array | *(required)* | One or more datasource objects (below). |

### Datasource fields

Each datasource has a unique `name` (what the MO table's `'table'` option
refers to) and a `type` of `jdbc` or `file`.

| Field | Applies to | Meaning |
|---|---|---|
| `name` | both | Unique lookup name; the MO table's `'table'` option selects it. |
| `type` | both | `"jdbc"` or `"file"`. |
| `connectionstring` | jdbc | JDBC URL of the source database. |
| `user`, `password` | jdbc | Credentials the SQL runs as. |
| `sql` | jdbc | Query to run per request. A `${FILTER}` placeholder is replaced with the pushed-down predicate text (or `1=1` when the request has none). The query's column names should match the MO external table's column names so the pushed filter is valid on the source side. |
| `path` | file | Filesystem path of an existing CSV file. |

- **jdbc**: runs the configured SQL per request; the substitution is textual,
  and the SQL runs with the configured credentials.
- **file**: streams an existing CSV file verbatim, re-chunked so that no
  record spans two chunks. The `${FILTER}` hint is a noop for a file source.

### Setting the listen address and API key

By default the server listens only on loopback with no authentication, which
suits a co-located MO (the compose sidecar shares the CN network namespace; a
launch deployment runs on the same host). To make it reachable from another
host, bind a routable interface **and** require an API key:

```json
{
    "host": "0.0.0.0",
    "port": 4444,
    "apikey": "a-long-random-shared-secret",
    "datasource": [
        { "name": "orders",
          "type": "jdbc",
          "connectionstring": "jdbc:mysql://db.internal:3306/sales?useSSL=false",
          "user": "reader",
          "password": "…",
          "sql": "select id, amount, ts from orders where ${FILTER}" }
    ]
}
```

Then every MO table that reads this server must supply the matching key:

```sql
CREATE EXTERNAL TABLE orders (id int, amount decimal(10,2), ts datetime)
ENGINE = DATASTREAM WITH (
    'server' = '10.0.1.5',      -- the jstfu host, reachable from the CN
    'port'   = '4444',
    'table'  = 'orders',
    'apikey' = 'a-long-random-shared-secret'
);
```

Notes:
- `host` must not be empty (the config validator rejects it), but the server
  does **not** force an `apikey` — requiring one on an exposed bind is the
  operator's responsibility.
- The gRPC channel is plaintext in v1, so also front an exposed server with a
  network trust boundary / TLS-terminating proxy.
- The config file holds the `apikey` and the JDBC `password` in plaintext —
  restrict its permissions (e.g. `chmod 600`). The key is read only at
  startup; rotate it by editing the file and restarting.
- On the MO side the `apikey` is stored in the catalog and never shown by
  SHOW CREATE, so a table restored from SHOW CREATE must have its `apikey`
  re-supplied.

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
    'recheck' = 'true',     -- optional, default true
    'apikey' = 'shared-secret'  -- optional; required if the server sets apikey
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

The server binds to **127.0.0.1** with **no authentication** by default: the
`${FILTER}` text is substituted into SQL that runs with the configured
credentials, so an off-box client that reached the port could read the
datasources. Before exposing it beyond a co-located MO, set `host` to a
routable interface **and** an `apikey`, and front it with TLS / a network
trust boundary — see [Setting the listen address and API
key](#setting-the-listen-address-and-api-key).

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
