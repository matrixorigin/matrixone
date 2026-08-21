Streaming Transport Fast Upload — Implementation Plan
=====================================================

Spec: [stream_transport_fast_upload.md](stream_transport_fast_upload.md)

Goal: a new external table variant `ENGINE = datastream` whose scan opens a
streaming gRPC connection to an external server, receives CSV-encoded chunks,
and feeds them through the existing CSV external-table machinery; plus a Java
reference server (`xtool/jstfu`) with jdbc and file data sources.

Guiding decision: **maximize reuse of the CSV external table path.** The scan
operator stays `pkg/sql/colexec/external.External`; we only add a new
`ExternalFileReader` implementation whose byte source is a gRPC stream instead
of a fileservice file. The precedent to mirror for syntax/DDL/plan plumbing is
`ENGINE = MONGODB` (`pkg/sql/mongodb`, `plan.ExternType_MONGODB_TB`), which is
the most recent engine-dispatched external table.

Phase 0 — Protobuf / gRPC contract
----------------------------------

New file `proto/datastream/v1/datastream.proto` (proto3, **not** gogoproto —
this proto is shared with Java, so use standard `protoc-gen-go` +
`protoc-gen-go-grpc`, same toolchain as `pkg/udf/udf.proto`, which is the
existing real-gRPC precedent in the repo; the main `proto/*.proto` files are
gogo-generated and Java-hostile).

```proto
syntax = "proto3";
package datastream.v1;
option go_package = "github.com/matrixorigin/matrixone/pkg/datastream/v1";
option java_package = "io.matrixone.datastream.v1";
option java_multiple_files = true;

service DataStream {
  // Server-streaming read. Errors during streaming are delivered either as
  // a gRPC status or as a final Error frame (see ReadResponse).
  rpc Read(ReadRequest) returns (stream ReadResponse) {}
}

message ReadRequest {
  string table = 1;          // datasource name to look up in server config
  string filter = 2;         // pushed-down predicate as SQL text, "" if none.
                             // A HINT: server may ignore it entirely.
  // room to grow: projected columns, chunk size preference, auth token...
}

message ReadResponse {
  oneof payload {
    Chunk chunk = 1;
    Error error = 2;         // terminal; client aborts the scan
  }
}

message Chunk {
  // CSV-encoded complete records: RFC4180-style, fields terminated by ',',
  // enclosed by '"', lines terminated by '\n', NULL encoded as \N (matching
  // MO's csvparser defaults). No record ever spans two chunks.
  bytes data = 1;
}

message Error {
  int32 code = 1;            // enum: TABLE_NOT_FOUND, DATASOURCE_ERROR, ...
  string message = 2;
}
```

Notes:
- Errors travel two ways: gRPC status (connection refused, deadline) and the
  explicit `Error` frame (spec requires protobuf-level error messages — e.g.
  jdbc SQL failure discovered mid-stream, after headers already sent).
- Codegen: a small `pkg/datastream/Makefile` mirroring `pkg/udf/Makefile`
  (`protoc --go_out --go-grpc_out`); Java side generates from the same .proto
  via the protobuf-gradle/maven plugin pointing at `proto/datastream/v1/`.
  Single source of truth, two generators.
- Generated Go code lives in `pkg/datastream/v1/`.

Phase 1 — Parser and DDL
------------------------

Mirror the MongoDB grammar exactly (`mysql_sql.y` lines ~10192 & ~10979):

1. `mysql_sql.y`:
   - `%token DATASTREAM` (next to `ICEBERG`/`MONGODB`, ~line 594) and add to
     `non_reserved_keyword` (~line 15553).
   - `datastream_table_param: ENGINE equal_opt DATASTREAM datastream_option_list_opt`
     plus `datastream_option_list_opt / _list / option / key / value`
     productions — copy of the `mongodb_*` set.
   - Fifth `create_table_stmt` production for
     `CREATE EXTERNAL TABLE ... '(' cols ')' datastream_table_param`
     setting `t.DataStreamParam`.
   - Union member + `%type` declarations (~line 744).
2. `keywords.go:~256`: `"datastream": DATASTREAM`.
3. New `pkg/sql/parsers/tree/datastream.go`: `DataStreamTableParam` +
   `DataStreamOption(s)`, `Format()` emitting `engine = datastream with (...)`
   (copy `tree/mongodb.go`, redact nothing — no secrets in these options for
   now, but keep the redaction hook if we later add auth).
4. `tree.CreateTable` (`tree/create.go:~936`): add `DataStreamParam` field and
   extend `Format()`'s external check.
5. Regenerate parser: `(cd pkg/sql/parsers && make mysql)` — goyacc must stay
   at 0 conflicts. **Assign `$$` in every action** (see goyacc empty-action
   gotcha, issue #25131).

DDL handling in `pkg/sql/plan/build_ddl.go` (`buildCreateTable`, new branch
next to the MongoDB one at ~line 2286):

- New package `pkg/sql/datastream/` (mirroring `pkg/sql/mongodb/`):
  - `ParseTableOptions`: validate option keys — exactly
    `server`, `port`, `table`, `recheck` (recheck optional, default `true`;
    port must be a valid port number; reject unknown/duplicate/empty keys).
  - `BuildCreateSQLEnvelope` / `ParseCreateSQLEnvelope`: store config as a
    `/* MO_DATASTREAM: version=1; server=...; port=...; table=...; recheck=... */`
    envelope in `rel_createsql`, anchored at string start (copy
    `pkg/sql/mongodb/envelope.go` — the anchoring prevents forgery via
    user-controlled strings).
- Set `TableDef.TableType = catalog.SystemExternalRel` and a new feature bit
  `features.DataStreamExternal` in `pkg/sql/features/table_feature.go`
  (typed discriminator, same rationale as `MongoDBExternal`).
- Reject inline indexes / on-update like the other external variants
  (`rejectExternalTableInlineIndexes` already fires on TableType).
- `SHOW CREATE TABLE`: add `" ENGINE = DATASTREAM WITH ("` emitter in
  `build_show_util.go` (~line 1298, next to MONGODB). Required for
  snapshot/PITR restore, which replays SHOW CREATE output — round-trip must
  reparse cleanly.

Phase 2 — Plan
--------------

`proto/plan.proto` (gogo side — internal plan serialization only):

- `enum ExternType`: `DATASTREAM_TB = 5` (line ~1164).
- `message DataStreamScan { string server; int32 port; string table;
  bool recheck; string pushed_filter; }` and `DataStreamScan datastream_scan = 13`
  in `ExternScan` (line ~1172). Regenerate with `make pb`.
  (`deepCopyExternScan` uses `proto.Clone`, so no deepcopy work.)

`pkg/sql/plan/query_builder.go` `buildTable` (~line 10948): detect the
`MO_DATASTREAM` envelope (or the feature bit) → `externType = DATASTREAM_TB`,
populate `externScan.DatastreamScan` from the envelope. Do **not** append the
hidden `__mo_filepath` column (that branch is `EXTERNAL_TB`-only).

Predicate pushdown + recheck (the interesting part):

- Where: after the planner has finalized `node.FilterList` on the
  EXTERNAL_SCAN node — a small pass over the filter list at compile-prep time
  (in `compileExternScanWithPlanNodeID`) or late in plan building, whichever
  binds after filter pushdown into the scan node has settled.
- Deparse `plan.Expr` → MySQL SQL text with a deliberately conservative
  deparser in `pkg/sql/datastream/filter.go`: support column refs, literals
  (int/float/string/date/timestamp/bool), comparison ops, `AND`/`OR`/`NOT`,
  `IN`, `BETWEEN`, `IS [NOT] NULL`, `LIKE`. Anything else (functions, casts,
  subqueries, params) ⇒ that conjunct is not pushed. Column names quoted with
  backticks; string literals escaped with **default sql_mode escaping**
  (internal-executor gotcha: never session-mode-aware formatting for text that
  another parser will read).
- `recheck = true` (default): pushed filter is a pure hint; `node.FilterList`
  is left untouched, so the normal downstream filter operator still runs.
  (Note: the external operator itself never evaluates `FilterParam.FilterExpr`;
  rechecking is the downstream filter op — free, already there.)
- `recheck = false`: remove from `node.FilterList` exactly those conjuncts the
  deparser successfully pushed; non-deparsable conjuncts stay and are still
  evaluated locally. This makes `recheck=false` safe-by-construction: we only
  skip rechecking what we actually told the server.

Phase 3 — Execution
-------------------

Reuse `pkg/sql/colexec/external`:

1. `compile.go` `compileExternScanWithPlanNodeID` (~line 2446): new dispatch
   `case ExternType_DATASTREAM_TB` **before** the file-list logic → build a
   single serial scope (like `compileExternScanSerialReadWrite` but with no
   file list; use the `tree.INLINE`-style virtual-one-file convention,
   `external.go:112-115`). One gRPC stream ⇒ one reader scope; parallelism
   across a query is out of scope (spec's parallel-load story is N concurrent
   SQL statements, each with its own filter).
   Optionally add a write-parallel merge (reader scope + `dispatch` to mcpu
   consumers, `compileExternScanParallelWrite` pattern) if the single pipeline
   proves to be the bottleneck — defer until measured.
2. New `pkg/sql/colexec/external/reader_datastream.go`: `DataStreamReader`
   implementing `ExternalFileReader`:
   - `Open`: `grpc.NewClient(server:port)` with connect timeout,
     `MaxCallRecvMsgSize` headroom, then `Read(ReadRequest{table, filter})`;
     wrap the response stream in an `io.Reader` (buffer each `Chunk.data`,
     surface `Error` frames and gRPC status as moerr with the server's
     message text); feed it to the existing `newCSVParserFromReader`
     (`types.go` — it takes any `io.Reader`, so the whole CSV → batch
     pipeline, `makeBatchRows` + `getOneRowData` + `getColData`, is reused
     verbatim, same as `CsvReader.ReadBatch`).
   - `ReadBatch`: identical shape to `CsvReader.ReadBatch`.
   - `Close`: cancel the stream context, close the conn. Close must be safe
     on early query cancel/limit — the operator's Reset/Free path calls it
     (operator lifecycle contract: no goroutine or conn may outlive Free).
   - CSV dialect fixed to the proto-documented defaults (',', '"', '\n',
     `\N`); the table's `tail` CSV options don't apply to datastream.
3. Reader dispatch in `external.go` `Prepare` (~line 146): add
   `case ExternType_DATASTREAM_TB: newDataStreamReader(...)` — config comes
   from the `DataStreamScan` carried on the instruction (see next point).
4. Remote-run: add the `DataStreamScan` fields to `pipeline.proto`'s
   `message ExternalScan` (~line 468) and thread through `remoterun.go`
   encode/decode (~780/~1316). Cheap because we reuse the External operator's
   existing instruction. If this turns out to drag, fallback v1: pin the
   scope to the local CN and leave a TODO — but the 2-CN dev cluster is the
   only place remote-run paths get exercised, so prefer doing it now.

Error semantics: any `Error` frame or non-EOF stream error aborts the query
with a moerr carrying the server's code/message (spec: table not found,
execution error, connection broken mid-stream).

Phase 4 — Java gRPC server `xtool/jstfu`
----------------------------------------

New top-level `xtool/` directory; `xtool/jstfu` is a Maven project (Maven over
Gradle: simpler fat-jar story via shade plugin, no wrapper binaries to vendor).

Layout:
```
xtool/jstfu/
  pom.xml                  # grpc-java + protobuf plugin (protoSourceDir →
                           # ../../proto/datastream/v1), jackson-databind,
                           # mysql-connector-j, commons-csv, shade plugin
  src/main/java/io/matrixone/jstfu/
    Main.java              # args: config file path; starts grpc Server
    Config.java            # JSON config: {port, datasource:[...]}
    DataStreamService.java # implements DataStream.Read
    source/DataSource.java # interface: stream(filter) -> chunk iterator
    source/JdbcSource.java
    source/FileSource.java
  src/test/java/...        # unit tests: chunker, ${FILTER} substitution
  conf/example.json
```

Behavior:
- Config: JSON per spec. Datasource `name` must be unique — reject duplicate
  names at startup (the spec's example shows two entries with the same name;
  treat that as illustrative, not as a lookup-order feature).
- Lookup miss ⇒ `Error{TABLE_NOT_FOUND}` as first (and only) frame.
- **JdbcSource**: open connection per request, run the configured SQL with
  `${FILTER}` replaced by the request filter (or `1=1` when empty). Encode
  each `ResultSet` row as CSV via commons-csv configured to MO's dialect:
  comma, always-quote strings containing specials, `\n` records, `\N` for
  SQL NULL, dates/timestamps as `yyyy-MM-dd[ HH:mm:ss[.SSSSSS]]`. Accumulate
  rows into a ~1MB buffer, flush a `Chunk` at a record boundary. SQLException
  mid-stream ⇒ `Error{DATASOURCE_ERROR, message}` frame, then close.
  Security note for the README: `${FILTER}` is textual SQL injection by
  design (the filter comes from the MO operator, not end users of the Java
  server, but the config's SQL runs with the configured credentials — deploy
  accordingly).
- **FileSource**: stream the file, cutting chunks at ~1MB but only at a
  newline that is outside a quoted field (minimal quote-state scanner —
  cheaper than a full CSV re-parse and satisfies "no record spans chunks").
  Open failure or malformed-quote state at EOF ⇒ `Error` frame. Filter is a
  documented noop.
- Chunk size, port, etc. constants in Config with sane defaults.

Phase 5 — Build and deployment
------------------------------

- Makefile: `jstfu` target → `cd xtool/jstfu && mvn -q -DskipTests package`,
  producing `xtool/jstfu/target/jstfu.jar` (shaded, self-contained). Guard on
  `mvn`/JDK presence with a clear skip message so `make` on a Java-less box
  still works; wire into the release-artifact target and the test docker
  image (add the jar + a `jstfu` service entry to
  `etc/docker-multi-cn-local-disk` compose for `make dev-up`, so BVT in the
  dev cluster has a server to talk to).
- CI: a workflow step that builds the jar and starts it (file datasource on a
  checked-in resource CSV + jdbc datasource pointing back at the MO under
  test) before the BVT stage.

Phase 6 — Tests
---------------

1. **Go unit tests**: filter deparser (table-driven: pushable vs
   not-pushable exprs); `DataStreamReader` against an in-process
   `grpc.Server` fake (happy path, `Error` frame mid-stream, connection drop,
   cancel-during-read → no goroutine/conn leak); envelope parse/roundtrip;
   SHOW CREATE roundtrip. Registration closures and binder branches need
   direct iteration tests — the Utils CI enforces 75% changed-line coverage.
2. **Java unit tests**: chunker record-boundary invariant (property-style:
   random CSV with embedded newlines-in-quotes, every chunk must parse
   standalone), `${FILTER}` substitution, config validation.
3. **Go e2e harness** `test/datastream/` (pattern: `test/mongodb/`
   `*_e2e_local_test.go`): spins the jar (skip if `java` absent), creates the
   external table, runs queries. This is the primary automated integration
   test because it can manage the server process — BVT cannot.
4. **BVT** `test/distributed/cases/datastream/`:
   - file datasource over `$resources/datastream/…` fixture CSVs (mounted
     into the dev cluster via DEV_MOUNT / compose volume);
   - jdbc datasource whose connectionstring points back at the MO server
     itself (mysql wire compat) — seed a source table first;
   - error cases: nonexistent datasource name, server not running (connection
     refused), jdbc SQL error (configured SQL referencing a missing table) —
     genrs records errors as expected results, eyeball them;
   - recheck on/off with a filter the file source ignores (recheck=true must
     still return correct rows; recheck=false documents hint semantics);
   - ETL: `INSERT INTO dest SELECT * FROM ext_t`, then a parallel-load case
     with several concurrent sessions using disjoint `${FILTER}` ranges
     (`-- @session` blocks).
   These cases assume the jstfu container from Phase 5 is up; they run in the
   dev cluster and the CI job that starts the server.

Order of work / dependencies
----------------------------

Phase 0 → 1 → 2 → 3 are strictly sequential (each consumes the previous
one's types). Phase 4 only needs Phase 0 and can proceed in parallel with
1–3. Phases 5–6 close the loop. Milestone check after Phase 3: `SELECT`
against a hand-run local jstfu (or a 20-line Go fake server) returns correct
rows — before investing in deployment/CI plumbing.

As-built status (2026-08-21, branch feat-datastream-external-table)
-------------------------------------------------------------------

Implemented as planned, with these notes:

- Phases 0–4 landed as designed.  Verified end-to-end against a live MO:
  `test/datastream/datastream_e2e_local_test.go` covers file + jdbc sources,
  pushdown/recheck, SHOW CREATE, error frames, ETL, and parallel load; BVT
  `test/distributed/cases/datastream/` passes 40/40 (server started via
  `optools/jstfu_bvt.sh`).
- The virtual-one-file convention reuses `ScanType = tree.INLINE` wholesale
  (see `external.DatastreamExternParam`), which made the operator changes
  minimal: a reader-dispatch case plus a synthetic-param branch in Prepare.
- Remote-run is wired (`pipeline.ExternalScan.datastream_scan = 24`).
- jdbc-source contract: the configured SQL's column names should match the
  external table's column names or the pushed `${FILTER}` text will not be
  valid on the source side (documented in xtool/jstfu/README.md).
- Deferred: docker-compose/release-artifact packaging of jstfu.jar and the
  CI step that starts it before BVT (the e2e harness self-manages the server
  and is the primary automated integration test until that lands).

Open questions (defaults chosen, flag if you disagree)
------------------------------------------------------

1. **recheck=false scope**: implemented as "skip recheck only for conjuncts
   actually deparsed and pushed" (safe), not "trust server for the whole
   WHERE". The spec reads like the latter; the chosen semantics never return
   wrong rows when the deparser bails on an expression.
2. **CSV dialect is fixed** (',', '"', '\n', `\N`), documented in the proto —
   no FIELDS/LINES options on datastream tables in v1.
3. **No auth/TLS in v1** — plaintext gRPC; proto leaves room for a token
   field. Worth confirming this is acceptable for the intended deployments.
4. **Parallelism**: one stream per query in v1; spec's parallelism is
   N statements. A future `parallel` option could open N streams with
   server-side sharding, but that needs protocol support (shard field in
   ReadRequest).
