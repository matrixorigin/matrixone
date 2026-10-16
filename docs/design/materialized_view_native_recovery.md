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
| Mixed old/new nodes | Native protocol inventory blocks the first MV catalog DDL; new-node login waits for upgrade; an old-session catalog control, if still admitted, sees zero MV catalogs |
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
while an inventoried node is below protocol 117 or the inventory query fails.
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
  --probe /tmp/mv-native-probe \
  --work-dir /tmp/a-new-mv-native-fixture
```

The tested old binary is built from pre-feature commit
`fcad62a8ff06000e682824802f1155cbeea02a2f`, with catalog 4.0.14 and protocol
112. Use that exact revision for the frozen old-node assertions. The archived
source's 11,132 tracked blobs were compared with the Git object tree, with zero
mismatches. It has no MV implementation and no authentication bypass.

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

Final native execution: 12/12 groups passed in 288.732 seconds. Three CN
processes exited by the expected SIGKILLs; the old binary exited at the expected
protocol fence; all remaining owned services exited cleanly. Final MV owning
race suite passed in 1.223 seconds, in addition to 100 exact-root repetitions.
