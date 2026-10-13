# Materialized view CI integration corrections

Tracking: [feature #30079](https://github.com/matrixorigin/matrixone/issues/30079),
[draft PR #30085](https://github.com/matrixorigin/matrixone/pull/30085).

## Observed failures

The reviewed run was [38345343672, attempt 1](https://github.com/matrixorigin/matrixone/actions/runs/38345343672),
checking `417811f5949dc04c9bd6d74d624bce2b852506c2`.
The reusable CI workflow was `matrixorigin/CI@b0f959479893a3ab54ff3d9422e135ae2e9c67b2`.
SCA ran on Linux/arm64; UT and the two BVT configurations ran on Linux/x86.
The Go toolchain was 1.27.1. These were concrete source, policy, and test failures,
not a reason to skip checks or retry unchanged code.

| Failure | Root cause and correction |
| --- | --- |
| SCA, Static Code Analysis | 18 new Go files used abbreviated Apache headers. Replace them with the complete repository header. |
| UT and coverage UT, bootstrap | The expected migration chain jumped from 4.0.13 to the new final version, omitting the existing 4.0.14 tenant migration. Include that hop and test both 4.0.14 and current 4.0.15 inputs. |
| UT and coverage UT, CN service | MV cron creation was coupled to lineage GC registration. A successful lineage retry still failed the MV-specific call. Register each task independently through the same bounded, stopper-owned retry mechanism; preserve cancellation and readiness checks. |
| UT and coverage UT, logservice build | The gossip fixture called the old port allocator signature. Resolve the OS-specific range outside ephemeral client ports and pass both bounds to the allocator. |
| Frontend UT and account/cluster restore BVT | The four global MV catalogs had no restore policy. Generic restore could recreate their schemas, and cluster-table cleanup could delete them even after bulk copy was skipped. Register skip policies and apply the common policy in destructive cluster cleanup. Preserve the existing view-metadata clear/rebuild behavior and ordinary user cluster-table deletion. |
| Git4Data, COPY ALTER, database DROP, DROP INDEX, cluster restore UT | A new account-wide MV write happened before native pessimistic lifecycle admission. Its retained locks changed wait policy and serialized unrelated operations. Pessimistic creation now pins all source and target names in the existing admission domain; source DDL checks dependencies after its native locks and snapshot are ready. Optimistic SI retains its commit-time write/write fence. |
| Both BVT groups, catalog inventories | Four new visible cluster catalogs changed exact table listings. Update seven result fixtures, including clone, tenant, and cluster-restore inventories; preserve complete row comparison. |
| Coverage aggregation | The job failed its UT/BVT prerequisites. It has no independent product fix; it must rerun after those checks pass. |

The proxy group reported 18 failed statements. The pessimistic group reported
153 failed statements, mostly cascading from catalog loss during restore.
Counts of failed statements are not counts of independent bugs.

## Admission and restore invariants

Pessimistic MV publication and source DDL share the same database/table-name
locks. RC creation includes every source in `createNameDomain`, checks that a
replanned domain has not expanded, and uses the existing applied-snapshot
frontier. Pessimistic SI pins the sorted source/target name set. These locks are
held through catalog publication; source DDL reads dependency rows after native
admission. Ordinary pessimistic DDL adds no MV catalog writes or account-wide
locks. Optimistic SI cannot borrow pessimistic name locks and continues to use
the durable account revision conflict, as does MV count admission.

Snapshot/PITR cleanup must preserve the current schema and lifetime of
subsystem-owned cluster catalogs. MV objects remain unsupported for logical
restore; preserving empty global catalogs does not enable restoring MV data or
identity-qualified definitions. User-created cluster tables still follow the
normal destructive restore path, and ordinary view metadata is still cleared
for reconstruction.

## Verification

The correction was checked with the repository CGo wrapper and Go 1.27.1.
Focused tests used CI's `-short -tags matrixone_test` and coverage instrumentation
(`-covermode=set`); this is focused reproduction, not the complete CI
`-coverpkg` graph. The five owning suites (bootstrap, CN service, frontend,
compile, MV) passed in normal short-test mode with those same tags.

New typed oracles cover source-domain completeness and replan changes,
pessimistic dependency rejection without account-fence writes, independent MV
cron retry, and cluster cleanup preserving all four MV catalogs while clearing
view metadata and dropping a user cluster table.

Native embedded-cluster reproductions passed for the CI failures in
`TestIssue26121DatabaseOperationsKeepOrdinaryInternalLookingTables`,
`TestIssue26404MixedDataBranchLifecycleDoesNotDeadlock`,
`TestIssue29400CopyAlterRetainedGatePromotionFastFails`,
`TestIssue29400DropDatabaseDoesNotHoldBranchDAGAfterTable`,
`TestDropIndexAdmissionDoesNotHoldNamesWhileWaiting`,
`TestDataBranchDiffAsFile`, and
`TestIssue26640ClusterRestoreRebindsSubscriptionPrivileges`.
The account-restore reproduction was rerun after the cluster-cleanup fix.

The two existing lineage cancellation/retry tests and the new MV cron retry
test each passed 20 race-enabled repetitions: 60 top-level passes, no data races.

License-eye 0.4.0 checked 60 changed files: 54 valid, six ignored, zero invalid.
Configured golangci-lint 2.14.0 reported zero new issues in the 17-package closure;
the configured molint command exited successfully. These local checks run on
Linux/amd64 and do not replace CI's arm64 SCA.

The rebuilt native service and CI's exact mo-tester revision
`309a0ca650e0c55e90292767fd61e4976818d66d` are used for targeted BVT verification.
All 12 originally failing BVT files passed as isolated invocations: 2,741
executed statements, 22 existing ignored statements, zero failures or abnormal
results. Resource paths use the repository's `test/distributed/resources`.
The main MV harness passed 67 native checks (including scheduled refresh) on
10,000 source rows; the second-CN harness passed nine checks. See
[the machine-readable correction results](materialized_view_ci_results.json)
for per-case counts, source fingerprints, and evidence modes.

Fresh full CI results must be checked at the correction commit. A local pass,
skipped job, or queued check is not a full CI pass. This correction makes no new
performance claim and does not relabel the earlier benchmark evidence.


## Follow-up SCA policy gate

[Run 38370142556](https://github.com/matrixorigin/matrixone/actions/runs/38370142556)
checked `7b0d6f04a72a060bd6a1b739e0a9b5dbc9f7f8dd`. Its complete header check
passed (6,097 valid files, zero invalid), then `make err-check` rejected the two
`fmt.Errorf` calls in the MV metadata decoder before lint could run. This policy
is separate from golangci-lint. Missing/unsupported persisted definitions now
return `moerr.NewInternalErrorNoCtx`, matching the other catalog decoders; the
existing invalid-definition test asserts the engine error class. Valid metadata
and its admission, refresh, and restore paths are unchanged.

The follow-up passed `make err-check`, the complete catalog suite, and configured
catalog lint. Earlier native BVT, race, and owning-package results are reused
only for their unchanged successful-metadata and lifecycle behavior. Full CI
must still be assessed at the follow-up head.


## Full-group result inventory follow-up

At `955c3f787a`, shared build, complete arm64 SCA (zero lint issues), and the
complete coverage UT passed. The readable UT producer report records exit 0
and no failed package/test event. PROXY BVT passed all 44,841 executed statements
(633 existing ignored statements). Pessimistic BVT reported only three failures
among 83,520 statements: two `show tables` results in
`snapshot/cluster_level_snapshot_restore_cluster.result` and one in
`tenant/privilege/create_user_default_role.result`, each missing the four new
catalog rows. The earlier runtime/restore/admission failures did not recur.

Both complete inventories now include the four visible cluster catalogs.
A repository-wide search audited all eight complete inventory result files;
all include the new rows. Both newly identified files passed isolated native
mo-tester execution with their complete statement sets. This follow-up changes
only result fixtures and evidence documentation. Code-level SCA, coverage UT,
PROXY BVT, and local lifecycle/race evidence remain valid for unchanged source;
the new full-group CI status must be tracked separately. Ordinary UT was still
queued for an `amd64-mo-shanghai-8c16g` runner when this evidence was captured.


## Main merge conflict resolution (2026-10-13)

Merge inputs are feature head `a321a75afce2ebc4cdd875c345ece7aef32cc038` and
main `35c8dd5854750ef96f52f9dd298a70acd031d5e4`. The generated MySQL parser
conflicted with the new JSON EXPLAIN grammar. It was regenerated from the merged
`mysql_sql.y`, without yacc conflicts, preserving both MV and JSON EXPLAIN syntax.
The gossip fixture uses main's once-per-cohort `testPortRange` admission and its
new listener-range test; the redundant inner range lookup was removed.

The integration also exposed a main-side build failure: JSON EXPLAIN still used
`Node_VECTOR_INDEX_SCAN`, `VectorIndexScan` and removed scan fields. An isolated
worktree at the exact main SHA reproduced the same four production compiler
errors with unchanged tracked source and matching native libraries. The renderer
and its tests now use the current `IndexSearchScan` protocol, existing plugin
explain hooks, named algorithm expressions, and lossless opaque options. Existing
vector payload, literal, settings, range and malformed-expression oracles remain;
new cases cover missing metadata, unpaired/empty expression names, malformed
plugin options, fulltext, and binary options from an unregistered plugin.

Review scope is the merge resolution and interacting source closures. Grammar
and generated output are R2; the test port fixture is R1; the read-only renderer
and its plan/frontend consumers are R2. MV creation dispatch, scan/write guards
and single-CN refresh routing were checked against the merged main side. No
MV persistence, refresh state, lock ordering, task ownership, or native dependency
changed, so previous concurrency and restart evidence remains applicable to those
unchanged contracts. Original performance measurements keep their recorded SHA.

Linux/amd64, Go 1.27.1, the repository CGo wrapper, `-mod=readonly -p=1
-count=1 -short -tags matrixone_test`: the complete parser, plan, JSON EXPLAIN,
frontend, compile and function suites passed, as did all 11 focused gossip fixture
tests. Initial network-restricted runs could not bind local test sockets; the
network-enabled final runs passed without skips or product workarounds. The
Makefile error-constructor check passed. Full GitHub CI at the new merge head must
be reported independently; these local results do not claim full CI completion.

Configured golangci-lint (including govet) passed the seven-package integration
closure with zero new issues; standalone `go vet` passed the same closure.
`go vet -vettool=molint` exited 0 for the renderer and reported two advisory
recover diagnostics in unchanged main-side serialization helpers. Their bodies
match main exactly: vector cleanup is deferred, and malformed serialization is
converted into an error. No diagnostic was suppressed or cleanup removed.
