# Experimental materialized-view validation

The implementation is disabled by default. On 2026-10-13, the isolated local
cluster passed 66 main SQL checks, 3 restart checks, 5 native failure/cleanup
checks and 9 two-CN checks. Read `docs/design/materialized_view_results.md` for
results and scope.

Build the harness from the repository with the Go version specified in go.mod:

```bash
go build -mod=readonly -o /workspace/mv-build/mv-validation ./test/materialized_view
/workspace/mv-build/mv-validation -rows 1000000 -keep
```

The DSN defaults to the local development cluster, `dump:111` at
`127.0.0.1:6001`. Override it with `-dsn`. Use an isolated cluster: the harness
owns the `mv_validation` database, two `mv_validation_*` accounts, and test roles.
It removes prior fixture tables before starting. Do not use it on a shared or
production cluster. Every run writes its passed checks, errors and timings to
`-output` (default `/workspace/mv-build/sql-results.json`). A failure exits nonzero.

The harness covers manual/deferred and scheduled refresh, independent exact
numeric/NULL oracles, joins and dimension changes, empty aggregates, repeated
refresh, read-only/dependency guards, unsupported query shapes, SQL mode and
fixed-offset timezone capture, prepared SELECT, source privilege revocation,
owner checks, concurrent readers/publishers, and same-name tenant isolation.

The validation deployment's binary, configs and data are retained locally under
`/workspace/mv-build`. After validation, its task-owned service processes are
stopped. Start the primary deployment in a separate terminal when reproducing:

```bash
env LD_LIBRARY_PATH=/workspace/matrixone/lib:/workspace/matrixone/cgo:/workspace/matrixone/thirdparties/install/lib \
  MO_JIEBA_DICT_DIR=/workspace/matrixone/dict OMP_NUM_THREADS=4 \
  /workspace/mv-build/mo-service -launch /workspace/mv-build/etc/launch.toml
```

For the second CN, use the same environment with
`/workspace/mv-build/mo-service -cfg /workspace/mv-build/etc/cn2.toml`.
These are local validation artifacts, not deployment templates.

After a successful `-keep` run, stop and restart the same service with the same
isolated data directory, then run:

```bash
/workspace/mv-build/mv-validation -restart-check \
  -output /workspace/mv-build/restart-results.json
```

The main benchmark uses deterministic integer data (`generate_series`, no random
seed), 1 million source rows, and 128 groups. It verifies full result equality,
records EXPLAIN output and refresh time, warms both queries, and retains 50
alternating paired samples with the median ratio. It retrieves all rows. This
measures an explicit MV query; automatic rewrite is a later stage. It does not
claim cold-cache, multi-CN, or production latency results.

Unit and failure-injection tests live beside their owning Go packages. They
cover the transaction-bound capability, source identity/schema fencing,
publication failure, snapshot drift, stale epoch/task replay, lost commit
response, optimistic retry, bounded admission, writer budget, captured timezone,
and reconciliation identity. See `docs/design/materialized_view_validation.md`
for executed evidence and remaining stage-specific cases.

Run native failure rollback and dependency cleanup independently:

```bash
/workspace/mv-build/mv-validation -edge-check \
  -output /workspace/mv-build/edge-results.json
```

The companion performance cases include explicit rollup, repeated join and a
high-cardinality control. All observations and EXPLAIN rows are saved in JSON.

The failure mode owns `mv_validation_failure`; it does not delete a benchmark
fixture retained with `-keep`. After restart validation, explicitly drop the
retained `bench_mv`, `joined`, `empty_total`, and `totals` materialized views,
then drop `mv_validation`.

### Automated BVT

`optools/run_bvt_group.sh` runs the native contract suite once, after group 0's
mo-tester scripts finish. `optools/run_materialized_view_bvt.sh` reuses the two
existing Compose CNs, discovers their published SQL ports, and runs functional,
native rollback, and causal cross-CN modes. The functional mode uses the small
SQL fixtures and omits all performance experiments. Scheduled publication polls
the actual result with a 120-second outer deadline; its minimum supported period
is 60 seconds. No extra service is started.

The Go runner is coverage-instrumented and writes into the same `coverage/`
directory as mo-service, so the existing BVT profile and required coverage merge
include both real server execution and client oracles. A failed mode or missing,
empty, or error-bearing JSON report fails the BVT step. To reuse local servers:

```bash
MV_BVT_DSN='dump:111@tcp(127.0.0.1:6001)/?timeout=5s&readTimeout=330s&writeTimeout=330s' \
MV_BVT_SECOND_DSN='dump:111@tcp(127.0.0.1:6002)/?timeout=5s&readTimeout=330s&writeTimeout=330s' \
  bash optools/run_materialized_view_bvt.sh "$PWD" /tmp/mv-bvt-report
```

`test/distributed/cases/view/materialized_view.sql` separately specifies the
deferred/manual SQL contract, unchanged stored rows before refresh, read-only and
dependency rejections, no replacement under IF NOT EXISTS, and schedule metadata.

With two CN SQL endpoints sharing the same TN/LOG and storage, run the small
topology suite independently of the benchmark:

```bash
/workspace/mv-build/mv-validation \
  -second-dsn 'dump:111@tcp(127.0.0.1:6002)/?timeout=5s&readTimeout=330s&writeTimeout=330s' \
  -output /workspace/mv-build/multi-cn-results.json
```

This mode owns and removes `mv_validation_multi_cn`, using four source rows.
It checks publication in both directions, five barrier-started concurrent
refresh rounds, stable relation identity, source snapshot progression and
terminal history. A concurrent w-w conflict is allowed; arbitrary errors are not.

Publication verification on fresh main `3e50e1b32d8da61bbbf70c89989fb08b671809de`
uses protocol 112 and passed 84 SQL checks (67 main + 3 restart + 5 failure/drop +
9 two-CN). The extra case covers a backslash in a source identifier with captured
NO_BACKSLASH_ESCAPES. Prepared/authorized reads consume all rows and assert
terminal errors. The publication binary/config/data are retained under
`/workspace/mv-publish-artifacts`; use `runtime/etc/launch.toml` and the matching
native libraries in `/workspace/matrixone-mv-pr` when reproducing that build.
See the execution report for the distinct prototype/publication evidence.


For cross-CN checks, default MO Push mode permits an independently connected CN
to select an older applied snapshot. The harness uses `mo_ctl('cn','synccommit','')`
at cross-connection commit handoffs, then checks the complete published values
and exact unpopulated error. No timed sleep or repeated read is used. These
checks prove visibility after that causal handoff, not globally fresh reads in
the default asynchronous mode. Current integration reserves protocol 113 for MV
after main allocated 112 to WEEK/DAYOFYEAR; older protocol records above describe
their historical runs.

Account lifecycle regressions are part of the normal `-functional` suite:
suspension/restriction preserve last-good rows/provenance, reopening resumes
refresh, ordinary database deletion restricts surviving MVs, and account deletion
retires populated/periodic MVs. A missing-target fault checks real rollback after
the first native MV drop; the fixture repairs its own catalog name afterward.
`-tenant-check` selects only these account scenarios for focused diagnosis.
The scheduler oracle waits for durable typed history and queue completion or
completed-task GC, so an old bounded retry cannot overwrite the next due-time
fixture. It uses the real reconciler, without adding task execution hooks.

`-catalog-missing` is an opt-in kernel guard check for an isolated initialized
4.0.14 catalog with all four MV catalogs absent and `auto-upgrade=false`. Bootstrap
that catalog using the pre-feature binary. Standard login may reject this version
mismatch before DDL; a test-only `cn.frontend.skipCheckUser=true` session isolates
the compiler guard, with privilege checks still enabled. This scoped fixture is
not mixed-version or login-compatibility evidence. It never edits system schemas
and cleans its own database. Validate both transaction modes by configuring
`cn.txn.mode` to `pessimistic` or `optimistic` on that isolated CN.


`-consumer-check` selects the native consumer regressions for review5562448876.
It reuses the existing authenticated BVT service and owns a two-row indexed
source, manual MVs and one altered periodic policy. Fourteen checks cover prepared
DELETE before MV creation in a different session/CN, unrestricted DELETE with creation disabled, affected
rows including an empty source, unique/secondary maintenance, explicit TRUNCATE
protection and ALTER COPY rejection without preparation residue, DUMP/LOAD kind admission before file access with unchanged publication,
and SHOW CREATE TABLE equality/replay for manual and altered periodic policies.
The fixture drops its MVs before its database on both success and failure. The
normal BVT entrypoint includes the mode and requires a nonempty successful JSON
report, using the same deployment and coverage path.

The consumer mode accepts `-second-dsn` for the MV-creating connection; without
it, it opens a second session on the primary endpoint. The BVT entrypoint uses
the second CN. Creating the MV on the prepared statement's own session would
advance its DDL generation and mask stale truncate eligibility. Truncating
DELETE plans now bypass ordinary and physical prepared caches and rebind on
EXECUTE; filtered/row DELETE and ordinary SELECT retain their existing policy.

The policy export case also prepares SHOW CREATE TABLE at60 seconds, changes
the policy to120 from the other connection/CN, and executes the same handle.
Both prepared and direct export must return the current120-second definition.
Prepared SHOW CREATE TABLE rebinds from its original AST instead of retaining
its lowered literal SELECT; ordinary SHOW already bypasses the SQL plan cache.

To validate refresh crash/replay without repeating migration tests, reuse the
native recovery fixture with a fresh directory:

```bash
python3 optools/materialized_view_recovery.py --refresh-only \
  --service /absolute/path/to/mo-service --probe /absolute/path/to/mv-native-probe \
  --work-dir /absolute/path/to/new-fixture
```

This runs durable commit-response loss, SIGKILL before/after publication,
same-run queue replay after each restart, and catalog/history/task cleanup.
The current production binary supports these SQL-enabled refresh failpoints;
the `matrixone_mv_faults` build tag is required for startup migration injection
in the full upgrade mode. The original full mode still requires `--old-service`.

After integrating coordinator-owned correlated programs (protocol119), MV
admission requires protocol120. The consumer fixture also checks deep correlated
reads of unpopulated/populated MVs, LIMIT0/WHERE FALSE controls and read-only
INSERT/UPDATE/DELETE with dependent expressions. Both compiler paths share the
transaction-bound target capability check. Historical119 MV receipts predate
this main integration.
The consumer report retains the correlated MV query EXPLAIN in `raw_plan`.
