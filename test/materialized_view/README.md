# Materialized-view validation

Read [the design](../../docs/design/materialized_views.md) for the feature
contract, implementation and limitations. This directory contains the native
SQL runner and recovery probe. The feature is experimental and disabled by default.

## Run SQL validation

Use the Go version in `go.mod` and an isolated MatrixOne cluster:

```bash
go build -mod=readonly -o /tmp/mv-validation ./test/materialized_view
export MV_DSN='dump:111@tcp(127.0.0.1:6001)/?timeout=5s&readTimeout=330s&writeTimeout=330s'
export MV_SECOND_DSN='dump:111@tcp(127.0.0.1:6002)/?timeout=5s&readTimeout=330s&writeTimeout=330s'
/tmp/mv-validation -dsn "$MV_DSN" -functional -output /tmp/mv-functional.json
/tmp/mv-validation -dsn "$MV_DSN" -edge-check -output /tmp/mv-failure.json
/tmp/mv-validation -dsn "$MV_DSN" -consumer-check -second-dsn "$MV_SECOND_DSN" \
  -output /tmp/mv-consumers.json
/tmp/mv-validation -dsn "$MV_DSN" -second-dsn "$MV_SECOND_DSN" \
  -output /tmp/mv-multi-cn.json
```

The runner owns and removes its `mv_validation`, `mv_consumer_validation`,
`mv_validation_failure` and `mv_validation_multi_cn` fixtures, plus test accounts
and roles. Do not run it against a shared or production cluster. Each invocation
writes passed cases, timings and any error to `-output`, and exits nonzero on
failure. `-keep` retains the main fixture for restart checks.

| Mode | Coverage |
| --- | --- |
| `-functional` | Small SQL fixtures: manual/scheduled refresh, exact numeric/NULL results, empty aggregates, joins, captured session semantics, read-only/dependency guards, permissions and account lifecycle |
| `-edge-check` | Native failure rollback, preservation of last-good rows/provenance and dependency cleanup |
| `-consumer-check` | 17 branch, prepared DELETE, DUMP/LOAD, index/affected-row and SHOW CREATE contracts |
| `-second-dsn` alone | Causal two-CN visibility, concurrent refresh, relation identity and terminal history |
| `-tenant-check` | Account lifecycle subset for focused diagnosis |
| `-restart-check` | Previously retained fixture after stopping and restarting the same cluster/data directory |
| No mode flag | Main SQL checks and benchmark experiments |

Scheduled checks poll durable publication with a 120-second outer deadline; the
minimum policy period is 60 seconds. Two-CN cases require shared storage/TN/LOG
and use `mo_ctl('cn','synccommit','')` at commit handoffs. They prove visibility
after that causal handoff, consistent with MO's normal asynchronous snapshots.

The prepared DELETE case creates its comparison tables before PREPARE, then
creates the dependent MV through the second session/CN. It executes the same
prepared handle and verifies affected rows, indexes and MV publication. Keeping
DDL off the prepared statement's own session prevents its DDL generation from
masking stale truncate eligibility. Consumer checks also reject both populated
and unpopulated branch endpoints and export the current altered refresh policy.

`-catalog-missing` is a separate kernel-guard fixture for a pre-MV catalog with
all four MV catalogs absent and automatic upgrade disabled. A test-only frontend
login bypass isolates compiler admission. It is not mixed-version/login evidence;
use the native recovery driver below for that contract.

## BVT integration

The existing BVT group-0 entrypoint invokes the wrapper after mo-tester. It reuses
the two Compose CNs, runs functional/failure/consumer/two-CN contracts, validates
nonempty successful JSON reports and merges runner coverage with server coverage.
It starts no additional MatrixOne services. To reuse local endpoints:

```bash
MV_BVT_DSN="$MV_DSN" MV_BVT_SECOND_DSN="$MV_SECOND_DSN" \
  bash optools/run_materialized_view_bvt.sh "$PWD" /tmp/mv-bvt-report
```

The standalone SQL contract also lives in
[`test/distributed/cases/view/materialized_view.sql`](../distributed/cases/view/materialized_view.sql).
Unit and failure-injection tests live beside their owning packages, including
`pkg/mv`, `pkg/catalog`, planner/compiler/frontend and bootstrap versions.

## Native upgrade, crash and replay validation

Build the normal native dependencies first using the repository build workflow.
The full driver needs a fault-tagged current binary and a clean pre-MV
4.0.17/protocol-121 binary from its own checkout:

```bash
make build BUILD_TAGS=matrixone_mv_faults BIN_NAME=mo-service-mv-recovery
go build -mod=readonly -o /tmp/mv-native-probe ./test/materialized_view/native_probe
python3 optools/materialized_view_recovery.py \
  --service "$PWD/mo-service-mv-recovery" \
  --old-service /path/to/clean-main417/mo-service \
  --old-version 4.0.17 --old-protocol 121 \
  --probe /tmp/mv-native-probe --work-dir /tmp/mv-recovery-new-empty-directory
```

The work directory must be new/empty. The driver owns isolated processes and
storage, records their exits, and cleans up its services. The migration startup
fault exists only in the tagged build. `--refresh-only` accepts a normal current
binary and omits the old binary/migration stages.

The full suite checks authenticated old-catalog operation, mixed-old admission,
absence of executor-17 work on old CNs, interrupted catalog installation,
restart/repeated upgrade, retained ordinary data, downgrade refusal with a
populated periodic MV, lost commit acknowledgement, and before/after-commit
crash plus same-run replay. An expected SIGKILL or rejected downgrade is checked
explicitly rather than treated as an unexplained process failure.

## Performance experiments and retained evidence

```bash
/tmp/mv-validation -dsn "$MV_DSN" -rows 1000000 -output /tmp/mv-performance.json
```

The default benchmark uses deterministic integer data: one million facts and
128 groups. It checks full result equality, records EXPLAIN and refresh time,
warms both queries, and retains 50 alternating paired samples with a median
ratio. Companion cases cover explicit rollup, a repeated join and a
high-cardinality control. Results describe explicit MV queries and warm local
reads; they do not measure automatic rewrite or production/cold-cache latency.

[Retained reports and source/binary identities](https://github.com/matrixorigin/matrixone/pull/30085#issuecomment-6141628463)
are kept outside the design document. Product inputs published as `0626df5d`
passed all required CI, 7,984 owning-package test roots, 17 native consumer cases
in each RC/SI deployment, and all 13 native compatibility/crash/replay groups.
The report records that local new binaries were built from the staged merged
product inputs with `vcs.modified=true`; the old comparison binary was clean.
Those results describe the retained pre-merge product inputs; protocol-122
merge validation is recorded separately in the linked report.

Historical performance samples were measured on prototype
`dd38098853ea197699ed3fac3e69dc53975ac061` on 2026-10-13, on one local CN/TN/LOG
with four CPUs and 16 GiB RAM. Grouped aggregation, explicit rollup and join
showed median ratios of 7.43x, 8.89x and 5.36x; the high-cardinality control was
1.04x. Full refresh took 151.270 ms, 40.947 ms and 34.560 ms for the respective
stored summaries (rollup reused the grouped summary). These are historical
feasibility observations, not performance claims for the current revision.
Re-run the command above on the intended revision and deployment before using
those ratios for a capacity or rollout decision.
