# Native MV upgrade and crash validation

This supplements C07/C10/C12 in the P1 validation plan. The existing in-memory
failure tests and graceful restart check do not prove transaction durability or
task replay when a CN process dies. The process-owning driver is
`optools/materialized_view_recovery.py`; its SQL client is
`test/materialized_view/native_probe`. It uses normal authenticated SQL and the
actual bootstrap, transaction, catalog, task-service and scheduler owners.

## Contracts and test boundaries

| Scenario | Observable oracle |
| --- | --- |
| Real 4.0.14 deployment | Normal login, persisted ready version 4.0.14, no MV catalogs, two ordinary rows summing to 30 |
| Mixed old/new nodes | Native protocol inventory blocks the first MV catalog DDL; registration visibly declines before task publication; the normal old session sees zero executor-17 cron and async rows; no unknown-executor log; new-node login waits for upgrade |
| Interrupted migration | A tagged startup fault waits after the second real DDL in the owning transaction; stack confirms the handler's wait; SIGKILL ends only the upgrader |
| Migration recovery/repetition | Restart with injection absent completes the pending route; all four catalogs exist exactly once, one 4.0.15/offset-4 target is ready, source rows survive; another normal restart preserves these assertions |
| Unsafe downgrade | Start the actual old binary while a populated periodic MV exists; its normal startup must exit with the persisted protocol or cluster-version fence; no executor-17 task is assigned to it and the full result/provenance tuple survives |
| Lost commit response | ECHO(1) after native `ExecTxn` commits, before the refresh owner receives acknowledgement; fault hit count is one; the real durable-state read returns success, matching rows/timestamps and SUCCEEDED history, with no FAILED run |
| Crash before publication | Real periodic task waits after staging rows, provenance and SUCCEEDED status but before commit; another session sees the entire last-good tuple and RUNNING history; queue status is Running; SIGKILL only the CN |
| Crash after publication/before acknowledgement | The entire new tuple and SUCCEEDED history are already visible while the same periodic task is still Running; SIGKILL only the CN |
| Restart/replay | Inspect the first post-restart tuple, then await acknowledgement/GC of the same queue identity; the same 64-character run ID and sequence succeeds exactly once; row values and both source timestamps agree; source TS advances; post-commit replay preserves the exact published tuple |
| Teardown | Native DROP removes the MV/database, definitions, dependencies and history; require no active per-view tasks, then observe existing HAKeeper GC remove completed queue rows |

The two refresh faults are disabled by default and add two atomic fault-registry
lookups per full refresh, never per source row. The migration fault has four
lookups in a one-time upgrade. The startup environment initializer is compiled
only with `matrixone_mv_faults`; production builds have no such environment
entry. WAIT honors the existing operation context. The driver observes actual
waiters/hit counts, uses SIGKILL rather than a graceful service stop, and never
edits task/run status, cluster/account version rows or catalog schemas to fake
recovery. Existing completed-task GC is asynchronous; a Completed queue row is
not an active leak and is not required to disappear in the DROP statement.

The upgrade handler must check the common protocol itself: its
`UpgradeTenant=No` metadata means the framework's tenant-upgrade protocol check
does not run. Installing persistent MV catalogs now fails closed before any DDL
while an inventoried node is below protocol 118 or the inventory query fails.
Creation/read/refresh gates and the existing persisted protocol startup fence
retain their separate responsibilities.

## Reproduction

Use the repository-supported Go/CGo toolchain and native libraries. Build a
normal production service as usual, plus a separate instrumented service:

```sh
mv_repo=$PWD
CGO_CFLAGS="-I${mv_repo}/cgo -I${mv_repo}/thirdparties/install/include" \
CGO_LDFLAGS="-L${mv_repo}/thirdparties/install/lib" \
go build -mod=readonly -tags matrixone_mv_faults \
  -ldflags="-extldflags '-L${mv_repo}/cgo -lmo -L${mv_repo}/thirdparties/install/lib -lusearch_c -lroaring -fopenmp -Wl,-rpath,${mv_repo}/cgo -Wl,-rpath,${mv_repo}/thirdparties/install/lib'" \
  -o /tmp/mo-service-mv-recovery ./cmd/mo-service
CGO_ENABLED=0 go build -mod=readonly \
  -o /tmp/mv-native-probe ./test/materialized_view/native_probe
python3 optools/materialized_view_recovery.py \
  --service /tmp/mo-service-mv-recovery \
  --old-service /absolute/path/to/4.0.14-mo-service \
  --old-protocol 117 \
  --probe /tmp/mv-native-probe \
  --work-dir /tmp/a-new-mv-native-fixture
```

The latest tested old binary is a clean build of upstream main
`9f873692e0` (catalog 4.0.14, protocol 117), whose collation capability does not
include MVs. The driver verifies its actual protocol inventory through normal
SQL before testing the 118 fence. Earlier captures pinned pre-feature commit
`fcad62a8ff06000e682824802f1155cbeea02a2f` (catalog 4.0.14, protocol 112);
select `--old-protocol 112` for that binary. Both use normal authentication.
The current result artifact identifies the exact old and new source/binaries.

Run from the repository root with free local ports 32001, 19000–19019,
18000–18019, 20000–20019, 6101–6102, 7301–7304 and 7603. The driver uses
`etc/launch` templates, disables auto-ID caching for this rolling topology, and
refuses an existing work directory. Separate LOG, TN and CN processes are
necessary to leave storage alive when the CN dies. The mixed-version phase
temporarily overlaps the old and new CN. Data cardinality is two rows; process
startup, inventory fencing and actual scheduling/replay dominate runtime.
There is no equivalent existing fixture covering these destructive boundaries;
ordinary BVT/UT CI does not start this isolated topology.

All process handles are retained immediately and closed in the driver's finally
path, including assertion/SQL/deadline failures. Each SQL operation has a context
and a separate client-response guard; phase polling has an outer hang deadline.
Native logs, the interrupted-upgrade stack, generated configs, binary hashes,
SQL observations with attempt counts/state changes, process exit codes and the
terminal result remain in the owned work directory. A failure writes a nonempty
error and exits nonzero. Fixture data are retained for diagnosis; remove only
that stopped fixture's `data` directory when no longer needed.

## Evidence scope

`materialized_view_native_recovery_results.json` records the completed native
run. Instrumented native evidence is identified separately from the normal
production build and remote CI. The service build reports the pre-delivery
baseline plus `vcs.modified=true`; it must not be described as a pristine build
of the later publication commit. Source hashes and semantic input checks bridge
that delivery metadata boundary.

These cases establish the selected old revision and local LOG/TN/CN topology.
They do not establish arbitrary release-pair compatibility, operator-directed
downgrade after removing the feature, every network/WAL fault, cold-cache tail
latency or scheduler-scale performance. Optional compatibility jobs skipped in
general CI remain skipped; the native run is separate evidence. External
implementation re-review and final-head CI are still required.

The earlier 12-case capture passed its selected oracles but omitted mixed-version
task-publication exclusion. Its raw old-CN log exposed a claimed unknown executor
17. The current protocol-118 execution with a real protocol-117 old node passes
13/13 groups in 324.495 seconds,
including zero cron/async task metadata before migration. Three CN
processes exited by the expected SIGKILLs; the old binary exited at the expected
protocol fence; all remaining owned services exited cleanly. The pre-merge
admission/retry mechanism evidence includes owning race passes for MV (1.219 s)
and CN (9.961 s), plus 100 repetitions of each root; these are not represented
as new merge-head race executions.


## Normal CI consumer of the SQL bridge

The existing `optools/run_materialized_view_bvt.sh` also builds the SQL bridge
with covermode=set and runs `test/materialized_view/native_probe/check.py`
against its existing primary CN. Two client lifetimes each verify nine requests:
malformed input/DSN, ordered multi-row SQL NULL decoding, empty results,
Exec/Query, persistent connection-local state and continued use after server
errors. It creates no database objects or service processes. Counters enter the
existing BVT coverage producer with unchanged thresholds/exclusions. The normal
entry still runs the 70 functional, five failure and nine two-CN cases.
The separate `materialized_view_ci_bridge_results.json` records this consumer.
The current recovery ledger supersedes the earlier capture after the task-
publication counterexample, and identifies its current pre-delivery inputs.


## Coordinator publication must precede execution eligibility

A catalog migration barrier alone did not prevent the new CN from publishing
its executor-17 reconcile cron while old CNs were still running. The original
12-case run's old-CN log contains a fetched `materialized-view-reconcile-v1:1/17`
and `executor with code 17 not exists`. This is a real counterexample to task
capability exclusion, even though no MV object could yet be created.

`mv.Service.RegisterCron` now checks all four installed catalogs and the
existing common protocol inventory before `TaskService.CreateCronTask`.
The CN's existing registration retry owner calls this operation; lineage GC
uses the same retry owner with its unchanged task metadata and cron expression.
There is no second scheduler, persistent state, new task code, feature-flag
bypass or unbounded retry owner. While the old topology remains, the native
suite observes an actual registration rejection and then requires zero typed
cron and async rows. The strengthened suite still proves publication and task
replay after upgrade. See `materialized_view_task_admission_results.json` for
the original raw log excerpt, exact before/after source identity and results.
