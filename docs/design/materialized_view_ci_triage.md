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


## Remaining CI correction and integration (2026-10-14)

At `cb53485ffe`, run 38383781049 passed shared build, arm64 SCA, Pessimistic
BVT and Connector/J E2E. PROXY BVT had exactly three failing assertions in
`optimizer/explain_format_json.sql` (83,197 successful statements, three failed,
455 ignored). They still expected the original payload/probe labels after the
first renderer migration. Ordinary UT failed source checkout with a TLS handshake
termination; coverage UT timed out at checkout after five minutes. Neither UT
job reached test execution. Coverage aggregation and CI Required failed their
prerequisites. Product failures and source-fetch failures are separate.

The correction merges main `bcd4cb99e7fa3f8fa750e773d5409a7bb803b0bf`, adopting
#30111's complete index-search renderer, detailed plugin hooks and expanded BVT.
Existing additional metadata/fulltext/opaque-option oracles remain. Main owns
protocol 112 for WEEK/DAYOFYEAR; MV planner authoring, frontend admission, compile
registration/read admission, catalog upgrade and latest protocol now use 113.
Typed tests reject 112 before catalog access, accept private scans at 113, reject
an authoring floor of 112, and preserve ordinary scans at 112.

A clean-source main worktree reproduced four NULLIF rebind failures in the
merged #30095 fixtures. The main binder consumes unsupported assignments and
continues collecting later valid NULLIF rules; the fixtures still expected the
older prefix-only behavior. They now assert the complete maps on all 20 rebinds,
assert every expression was consumed, and exercise JSON round trips for those
previously excluded cases. Production NULLIF behavior is unchanged.

The nested UNION allocation-owner test also encountered the host's automatic
spill-disk margin: less than 16 GiB free storage allowed only the previous
fixture's 128-byte explicit floor. The ownership test now uses the existing
private-ledger testing API with its original 1 MiB query limit and a bounded
4 MiB aggregate. Result and terminal allocation assertions remain; cleanup also
checks memory, disk and FD reservations before closing the generation and ledger.
No production quota or resource policy was changed. This test, the existing
128-byte spill-budget test and the five-case compile protocol test each passed
ten repeats, followed by the complete compile suite.

Topology diagnosis found that default Push mode allows an independently connected
CN to read an older applied snapshot. An immediate cross-connection read could
therefore still observe an unpopulated/previous view. The topology harness now
uses MO's existing bounded `SYNCCOMMIT` handoff after DDL, source commits and MV
publication. Latest complete values, stable identity, monotonic source coverage,
at least one concurrent success and terminal history assertions are preserved.
The unpopulated check now requires the exact error class and `MV_NOT_POPULATED`
marker. No sleep or query retry is used. These checks prove atomic publication
after a causal handoff, not stronger freshness than MO's configured read mode.

Final Linux/amd64 local verification: eight owning suites passed with Go 1.27.1,
repository CGo wrapper, `-mod=readonly -p=1 -count=1 -short -tags matrixone_test`.
Configured lint and vet passed the eight-package integration closure; the final
compile/frontend changes and test harness passed their refreshed checks. The
error-constructor policy passed. The final rebuilt service passed all 137
statements in the complete JSON EXPLAIN, external NULLIF and LOAD-variable
scripts, with zero failed/ignored/abnormal statements. The native MV suite passed
67 checks; 20 independent causal two-CN scenarios passed all 180 checks. Original
one-million-row performance observations keep their historical SHA; the current
10,000-row run is diagnostic evidence. New full CI must be evaluated at the
pushed correction head.

The follow-up main merge at `f02952f7bf9c3294d5cb7876a07888d2380d48a9` is clean.
Both complete compile and plan suites pass again. Its delta affects ordinary
view replacement admission and primary-key ODKU binding/partition locking;
MV-only admission remains aligned and protocol 112 remains main's latest.
The native MV/JSON/NULLIF fixtures contain no ODKU or ordinary-view replacement,
so their relevant execution branches are unchanged and their evidence is reused.
Configured compile/plan lint is refreshed for this integration. Full CI remains
separate and must be read at the final pushed head.
### Required coverage producer integration (2026-10-14)

At head `9ffe2fb06a2e311f937a8c3cd622885438b30eb6`, run `38489522602`
passed the shared build, arm64 SCA, coverage UT, and both full BVT deployments.
Ordinary UT was still running when the coverage verdict arrived. Coverage job
`115630262739` failed its actual changed-code gate: 386/1040 instrumented blocks
(37.12%), below the required strict >75%; the complete profile was valid.
The report exposed two missing consumers: default-off native MV SQL had no
distributed case, and the manually executed Go validation program was never
executed as a coverage producer in CI. This is a test-integration failure, not
an infrastructure failure or a reason to lower/exclude coverage.

The correction adds the independently runnable `view/materialized_view.sql`
case and runs the existing native contract oracles after group 0's mo-tester
work. No account/cluster restore fixture can overlap the native suite. Reuse
the existing two Compose CNs through their directly published ports, rather
than relying on proxy connection placement to prove distinct CN execution.
Build the pure Go client with coverage and share the existing producer's
coverage directory. Existing trusted CI workflows already convert that
directory and enforce the same threshold; their policy remains unchanged.

Validation map: SQL result/error goldens prove manual/deferred/DDL behavior;
the small functional mode proves typed NULL/numeric results, source grants and
revocation, authoring context, tenant isolation and scheduled refresh; the
independent failure mode proves last-good rows and FAILED history after native
overflow; the existing four-row cross-CN mode proves complete publication,
identity/provenance and terminal history after explicit causal handoffs.
Performance samples and actual process restart remain separate manual modes.
Only the minimum 60-second scheduled period uses elapsed time, with observable
result equality and a 120-second deadline. Client compilation reuses the CI Go
toolchain/cache and does not need native dependencies or new server processes.

The coverage report then identified missing component cases for interrupted
attempt recovery and terminal-history retention. Add 21 named task subcases:
9 envelope/policy/definer cases, 5 interrupted recovery cases, and 7 retention
cases. They use existing in-memory catalog fixtures, one metadata row and one
overlapping terminal run; assert cancellation/error propagation, epoch/run
fences, no execution for obsolete/manual policies, sanitized failure history,
conditional deletion excluding RUNNING attempts, deduplication across retention
pages, and no maintenance cursor advancement after failure. No clock or service
process is added to UT.

Local verification: all 54 SQL BVT statements passed with zero ignored/failed;
the final instrumented two-CN contract entry passed 77 checks (63 functional,
5 native rollback, 9 causal topology) in 105 seconds including client build.
Complete `pkg/mv` owning UT passed with 21 new named subcases. Using the unchanged
CI parser and strict >75% gate, the prior complete CI producer profiles for
unchanged source plus fresh local server/client and owning-UT profiles produce
793/1043 changed blocks (76.03%). This is a local coverage reproduction, not a
new GitHub CI result. Old client blocks were removed from the diagnostic merge
because source line positions changed; the fresh full client profile supplies
their replacement. Nothing is excluded from the actual CI coverage policy.

The ordinary UT and Connector/J jobs of run `38489522602` also completed
successfully. Its CI Required gate failed only for the actual coverage verdict.
The first partial local coverage build omitted the entry package and emitted no
profile; that empty attempt is not evidence. The corrected build includes the
service entry and emits nonempty metadata/counters on graceful shutdown. The
automated CI build already instruments its entry through its normal `-cover`
build. Match the client's `set` coverage mode to that existing server build.
