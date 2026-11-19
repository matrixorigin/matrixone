# Binary-prepared OUTFILE audit receipts

Run this public-protocol regression against an isolated MatrixOne instance
with tracing enabled and statement aggregation enabled (the default). The user
must be able to create/drop stages and read `system.statement_info`. The client
and server must share an existing local directory named by `OUTFILE_PARENT`;
the probe creates and removes only its own child directory and unique stage.

Set `DB_URL`, `DB_USER`, `DB_PASSWORD`, `OUTFILE_PARENT`, and `CONNECTOR_JAR`.
For example, `DB_URL=jdbc:mysql://127.0.0.1:6001/?useSSL=false`.

```sh
probe_classes=$(mktemp -d "${TMPDIR:-/tmp}/outfile-jdbc.XXXXXX")
trap 'rm -rf "$probe_classes"' EXIT
javac -Xlint:all -cp "$CONNECTOR_JAR" -d "$probe_classes" \
  test/manual/issue30745/PreparedOutfileReceiptProbe.java
java -cp "$probe_classes:$CONNECTOR_JAR" PreparedOutfileReceiptProbe
```

Both prepared handles must be Connector/J `ServerPreparedStatement` objects;
client emulation and statement caching are disabled. Thus this exercises
COM_STMT_PREPARE/COM_STMT_EXECUTE, which SQL `PREPARE` and the direct-CSV BVT do
not establish. One OUTFILE handle executes twice with values `73` and `7301`.
Each execution is immediately followed by a pre-prepared ordinary `SET` on the
same connection. Its parameter is read back; its individual terminal telemetry
must have no OUTFILE receipt. `SET` naturally avoids SELECT aggregation, so no
sleep query or aggregation override is needed.

A separate observer polls the actual asynchronous telemetry publication, with
a 60-second bound and per-query/network timeouts. Each export must produce a
distinct successful statement UUID with `aggr_count=0`, receipt version 1,
format CSV, `complete=true`, and a target stripped of the query-token canary.
Both receipt JSON and the Select execution diagnostic statement text must omit
the canary. Actual
file content and independently measured byte/row totals identify each execution;
the file is removed between executions of the same handle. The final totals
must be exactly two export rows and two ordinary rows, with four distinct UUIDs.

Statements/result sets, connections, stage and output directory have scoped
cleanup on both success and failure. No database/table or global setting is
created or changed. The expected result is two `protocol=...ServerPreparedStatement`
lines, two `PASS run=...` lines, `PASS cleanup...`, and process exit 0. On a
build without the feature, the exported file alone is insufficient: missing
receipts fail the bounded publication assertion.

The existing owning Go/race/static checks and split-output BVT remain the
validation for exporter lifecycle and sink failure paths. This small manual
probe supplies only the missing public binary-protocol entry-point evidence.

## Privacy boundary

These assertions cover OUTFILE Select execution receipts and execution diagnostic
text. Separate `Prepare` telemetry follows the existing logging policy and can
retain the original target, including its query token. This probe observed that
behavior on the verified production revision; it does not claim target secrecy
across all SQL telemetry. The accepted feature scope is direct top-level SELECT
and binary prepared execution, rather than a redesign of PREPARE logging.

## Retained execution

On 2026-10-25 this exact probe passed against a fresh single-CN Linux amd64
instance built from production revision
`2bad2d317bd1baeab933bc53802bc9fd1f0675b2`. The follow-up adds only this manual
probe and its instructions; production/build inputs remain identical. Existing
owning frontend/motrace normal/race/static checks and split BVT evidence are
retained under that explicit source equivalence.

- OpenJDK/javac 21.0.8; Oracle MySQL Connector/J 8.4.0.
- Probe source SHA256: `143313b6b4f8903de34214a758f22e931be145d290be923e290d18490e7f95f8`.
- Connector JAR SHA256: `d77962877d010777cff997015da90ee689f0f4bb76848340e1488f2b83332af5`.
- Service binary SHA256: `8cbc2257aa8f2ab477b0bc740357800d4dc0d92c084ece53ee7e4d626af3a342`.
- Service: Go 1.27.1, GOAMD64=v3, GOEXPERIMENT=simd, CPU native clang 19.1.7;
  clean build metadata (`vcs.modified=false`), DISK-V2 local storage,
  RC/pessimistic, default statement aggregation enabled.
- SQL 16916, debug 17916; `trace-export-interval=1` only in the owned instance
  configuration. SQL readiness and `ISCP-Task Start` preceded the probe.

The service build command was:

```sh
env -u CGO_CFLAGS -u CGO_CXXFLAGS -u CGO_LDFLAGS \
  GOTOOLCHAIN=go1.27.1 GOMAXPROCS=4 \
  TMPDIR=/mnt/nvme/pr30916-repair-tmp GOTMPDIR=/mnt/nvme/pr30916-repair-tmp \
  make -j4 build
```

The compilation/run commands above used the checked source, that connector JAR,
`DB_URL=jdbc:mysql://127.0.0.1:16916/?useSSL=false`, and
`OUTFILE_PARENT=/mnt/nvme/pr30916-prepared-runtime/out`; user/password were supplied
in the environment. Build, warning-free Java compilation and Java process each
exited 0. The complete Java process stdout/stderr was:

```text
protocol=com.mysql.cj.jdbc.ServerPreparedStatement
protocol=com.mysql.cj.jdbc.ServerPreparedStatement
export statement_id=01a171c5-0bd4-733e-a8e9-3fd719d1a3a8 exec_plan={"outfile":{"version":1,"target":"stage://qa_outfile30745_f02e24f68c9d/prepared.csv","format":"csv","bytes_written":9,"rows_written":1,"complete":true,"target_redacted":true}}
ordinary statement_id=01a171c5-0bd9-73cf-a2bc-6b52cffbf40f exec_plan={}
PASS run=1 value=73 target=stage://qa_outfile30745_f02e24f68c9d/prepared.csv bytes=9 rows=1 version=1 format=csv complete=true target_redacted=true aggr_count=0 export_ids=[01a171c5-0bd4-733e-a8e9-3fd719d1a3a8] ordinary_ids=[01a171c5-0bd9-73cf-a2bc-6b52cffbf40f] ordinary_outfile=absent
export statement_id=01a171c5-0bd4-733e-a8e9-3fd719d1a3a8 exec_plan={"outfile":{"version":1,"target":"stage://qa_outfile30745_f02e24f68c9d/prepared.csv","format":"csv","bytes_written":9,"rows_written":1,"complete":true,"target_redacted":true}}
export statement_id=01a171c5-0e56-7582-b0bb-319dd403df18 exec_plan={"outfile":{"version":1,"target":"stage://qa_outfile30745_f02e24f68c9d/prepared.csv","format":"csv","bytes_written":11,"rows_written":1,"complete":true,"target_redacted":true}}
ordinary statement_id=01a171c5-0bd9-73cf-a2bc-6b52cffbf40f exec_plan={}
ordinary statement_id=01a171c5-0e5b-774f-9890-4a070b5ec4d3 exec_plan={}
PASS run=2 value=7301 target=stage://qa_outfile30745_f02e24f68c9d/prepared.csv bytes=11 rows=1 version=1 format=csv complete=true target_redacted=true aggr_count=0 export_ids=[01a171c5-0bd4-733e-a8e9-3fd719d1a3a8, 01a171c5-0e56-7582-b0bb-319dd403df18] ordinary_ids=[01a171c5-0bd9-73cf-a2bc-6b52cffbf40f, 01a171c5-0e5b-774f-9890-4a070b5ec4d3] ordinary_outfile=absent
PASS cleanup stage=absent output_directory=absent
```

A separate post-run catalog query returned zero stages with this probe's prefix,
and the owned output parent was empty. The service was then shut down gracefully.
