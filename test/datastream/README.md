# Datastream external table — local E2E harness

End-to-end tests for `CREATE EXTERNAL TABLE ... ENGINE = DATASTREAM`
(spec: `docs/cn/stream_transport_fast_upload.md`, plan:
`docs/cn/stream_transport_fast_upload_plan.md`, server: `xtool/jstfu`).

Unlike BVT, this harness manages the jstfu Java server process itself, so it
is fully self-contained given a built jar:

```
make jstfu                       # needs JDK 17+ and Maven (or MVN=/path/to/mvn)
go test ./test/datastream/       # TestJstfu* need only java + the jar
```

- `TestJstfuFileSource` / `TestJstfuErrors` — Go gRPC client against the Java
  server directly: file datasource verbatim streaming, filter-hint noop,
  TABLE_NOT_FOUND and DATASOURCE_ERROR frames.
- `TestJstfuMultiChunkStreaming` — a generated ~1MB file served with a 2KB
  chunksize: asserts hundreds of chunks on the wire, every chunk boundary on
  a record boundary (quote-state checked, incl. quoted embedded newlines),
  and lossless reassembly.  The BVT server config
  (`optools/jstfu_bvt.sh`) likewise sets `chunksize: 4096` so the BVT
  fixtures stream as hundreds of chunks instead of one or two 1MB chunks.
- `TestDatastreamThroughMatrixOne` — the full path through a running
  MatrixOne: DDL, file + jdbc scans (the jdbc datasource dials back into the
  same MO over the mysql wire), predicate pushdown with recheck on/off,
  SHOW CREATE round-trip, error propagation, ETL insert, and a parallel load
  with disjoint filters.  Skips when MO is unreachable; point it at a server
  with `MO_DATASTREAM_E2E_DSN` (default `dump:111@tcp(127.0.0.1:6001)/`).

Note the jdbc-source contract exercised here: the configured SQL's column
names should match the external table's column names so the pushed
`${FILTER}` text stays valid on the source side.

BVT cases live in `test/distributed/cases/datastream/`; start the server for
them with `optools/jstfu_bvt.sh <resources_dir> <mo_host:mo_port>`.
