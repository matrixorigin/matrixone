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

### Published CN SQL port (2026-10-14)

Run `38500065915`, head `07bc11c55e`, passed the shared build, arm64 SCA,
coverage UT, Connector/J and full pessimistic BVT. PROXY's existing mo-tester
selection also passed: 83990 statements, 83535 successful, 455 existing ignored,
zero failures, including all 54 new MV SQL statements. Its subsequent native
consumer failed before its first case with `packets.go:58 unexpected EOF` and
`invalid connection`. Coverage merge then correctly rejected its prerequisite;
this run did not produce an independent coverage threshold verdict.

The CN TOMLs and runtime log both show SQL listening on container port 6001.
Compose instead published container port 6002, and the new native runner queried
that incorrect target. CI's docker listing showed `32772->6002` and `32773->6002`
while the native report contained zero passed cases. The normal proxy route used
port 6009 inside its own container, so its success did not validate CN publication.

Publish each CN's actual port 6001 with a dynamically allocated host port and
query that same target in the native runner. Keep the proxy's `6001:6009` mapping.
No database, authentication, fixture assertions, timeout, retry, or coverage policy
changes are needed.

Validation checks both old and corrected Compose JSON against the existing CN
TOMLs: the old targets omit the listener and corrected targets include it. An
isolated real Docker deployment uses two independently published CNs, both with
`proxy-enabled=true` and SQL port 6001, sharing TN/LOG and DISK-V2 storage. The
runner uses its default Docker port discovery, with no DSN override. Both mapped
ports return real MySQL greetings; all 77 contract checks pass (63 functional,
5 rollback, 9 causal cross-CN) in 90 seconds including client build. The local
image matches the Debian 13 native binary ABI; this environment adaptation is
not a CI image or product change. Task-owned containers are stopped/removed after
verification. Evidence is in `materialized_view_ci_port_results.json`. New-head
CI remains independent of this local port-boundary proof.

Latest-main parser conflict resolution (2026-10-14): main
`fcad62a8ff06000e682824802f1155cbeea02a2f` adds parenthesized
`ALTER TABLE ADD COLUMN (...)` (#29963). The grammar merges cleanly; regenerate
`mysql_sql.go` with the repository goyacc instead of choosing either generated
file. Generation reports zero shift/reduce and zero reduce/reduce conflicts.
Retain upstream parser tests. Complete mysql parser and planner UT pass
(0.366 and 56.890 seconds), incremental vet passes, configured lint reports zero
issues. A freshly rebuilt native service passes 58/58 SQL statements with zero
ignored, failed or abnormal cases, plus all 77 native checks (63 functional,
5 rollback, 9 causal two-CN). New SQL controls reject parenthesized ADD on the
MV and its dependent source, then allow ordinary ADD after dropping the MV and
verify original rows plus NULL new columns. Port-boundary Docker proof above is
separate. This resolves the local generated-parser conflict; final-head GitHub
checks remain independent. Evidence is in `materialized_view_ci_port_results.json`.

### Account/catalog review closure (2026-10-14)

Reviewed head `15ef9e056fc4` passed CI, but source review exposed missing catalog
readiness, live tenant status and account-retirement integration. Revision 3
selects ordinary DROP DATABASE RESTRICT and transactional DROP ACCOUNT retirement.
The correction also synchronizes InternalExecutor's session account identity.

All 58 SQL statements and 84 native checks pass (70 functional, 5 failure,
9 two-CN); owning packages, incremental vet and configured lint pass. Isolated
4.0.14 kernel controls reproduce the pre-fix missing dependency table and pass
six DDL operations in each transaction mode afterward. Only that separate fixture
bypasses the earlier tenant-version login fence; main account/ACL tests authenticate
normally. Completed-task GC is an explicit terminal scheduler oracle. Full scope,
accessor-only native evidence reuse and release/re-review limits are recorded in
materialized_view_self_review.md and materialized_view_review_lifecycle_results.json.
New-head CI remains independently required; no local coverage percentage is claimed.


### 2026-10-15: empty account retirement and protocol collision

Exact failed head: `e55a8a6baa9eac0e45e60f38b10ea5aad1660803`, ALL CI run
`38582531165`. Ordinary UT job `115915980970`, coverage UT job
`115916066201`, Proxy BVT job `115920997966`, and pessimistic BVT job
`115920997947` failed. SCA and shared build passed. Coverage aggregation failed
its producer prerequisite; it did not evaluate the changed-line threshold.

The first BVT failures are ordinary `DROP ACCOUNT` statements reporting
`invalid materialized view retirement result`: Proxy at
`ddl/alter_publish_subscribe_table.sql:78`, pessimistic at
`analyze/analyze_tenant_domain.sql:45`. Ordinary UT diagnostics show the same
error in tenant-catalog, account-lifecycle, restore and published-view cleanup.
Coverage diagnostics show it in CDC and branch-account cleanup as well. Later
BVT account-exists, metadata mismatch and unavailable-connection results are
retained as failures; this patch does not claim every downstream result has been
independently reproduced or that the old run passed.

Invariant: an installed MV catalog with zero definitions is a valid account
retirement state. The background SELECT can return either no result set or one
zero-row result set. Both must reach queued-task and metadata cleanup in the
caller-owned transaction. The helper previously rejected the first form.
The fix normalizes only the row count; multiple result sets, the 128-object
bound, exact native-table capabilities, SQL errors and rollback behavior retain
their checks. Existing readiness/failure-injection tests now exercise both empty
forms. No retries, ignored failures or coverage-policy changes are introduced.

Merge main `f49bc6fa53` and preserve its DECIMAL statistical-result protocol
113. Move MV authoring, reading, catalog-upgrade admission and typed tests to
114, including rejecting the immediately preceding protocol/floor 113. Regenerate
the MySQL parser from the merged grammar rather than accepting a textual merge
of generated parse tables.

Change map: retirement helper/test is R2 (public account deletion, same transaction
and cleanup owner); protocol consumers/tests are R3 compatibility (only MV
admission moves; main's DECIMAL gate stays 113); parser output is generated from
the merged grammar; this record is R0. This is a focused regression/conflict fix,
not a new feature-design revision. The existing RFC revision 3 approval remains
pending and is not self-approved by this change.

Local validation of the merged source (Go 1.27.1, linux/amd64, repository CGo
wrapper, `matrixone_test`, `-short`, `-count=1`):

- Full frontend package: PASS, 26.501 s.
- Full compile package: PASS, 9.396 s.
- Materialized-view focused tests in frontend, compile, plan, and v4_0_15:
  PASS in all four packages, including 113/114 admission boundaries.
- Full MySQL dialect parser package after regeneration: PASS, 0.170 s.
- Existing real-client `TestIssue26114CrossAccountBranchQuotaAndOwnership`:
  PASS, 9.009 s, including the previously failing account cleanup.
- `go vet` on frontend, compile, plan, v4_0_15, defines and MySQL parser: PASS.

Evidence logs are retained under `/workspace/mv-ci/empty-retirement-*`.
These local checks do not establish the new GitHub CI result. The full BVT and
changed-line coverage result must come from the new exact-head workflow.

Configured golangci-lint 2.14.0 over the same six-package closure, reporting only
findings since verified upstream main: PASS, `0 issues`. Final regression-scope
self-review found no additional code blockers: both empty representations reach
the existing cleanup; catalog migration, query errors, native-table capability,
transaction rollback and object bounds remain enforced. Existing RFC approval
and the new remote CI run remain separate pending delivery gates.


### 2026-10-15: transaction-mode-independent SHOW CREATE fixture and native DROP

Run `38665103998`, exact head `1ec43a9800906d6f22f6932f35eb4e9e25a371f9`:
ordinary UT, coverage UT, SCA, shared build and Proxy BVT passed. Pessimistic
BVT job `116197952536` failed exactly three SQL assertions (84,357 statements,
83,899 successes, 3 failures, 455 existing ignores, 0 abnormal). All three are
`SHOW CREATE MATERIALIZED VIEW` in `view/materialized_view.sql`: the stored
GROUP BY is `k`, while the expected text has `facts.k`. Coverage aggregation
failed its BVT-producer prerequisite and did not evaluate its threshold.

The formatter persists the bound SELECT; qualification introduced during binding
can differ with the transaction/optimizer path. Qualification equivalence is not
a separate MV result contract. The fixture now explicitly supplies `facts.k` in
both CREATE statements. The three exact SHOW CREATE result assertions, all data
and error assertions, and the 58-statement count remain unchanged; the result
file only updates the two echoed CREATE inputs. Unqualified grouping remains
covered by the native consumer's aggregate/join/tenant cases, and the failing
baseline itself passed all data/publication assertions with unqualified `k`.
No alternate accepted output, ignore marker, retry or threshold change is used.

Cross-mode validation also found an actual DROP ownership bug: with an optimistic
deployment default, `handleDropMaterializedView` opens an optimistic internal
transaction and native DROP TABLE rejects it with `data branch lineage lifecycle
writer requires a pessimistic transaction`. The enclosing frontend transaction
cannot select the mode of this independent executor transaction. The fix selects
pessimistic/RC at the owning `ExecTxn`, matching normal native DROP admission in
`frontend/txn.go`. The nested native DROP still borrows the same transaction and
exact account/table/transaction capability; privileges, definition cleanup,
rollback, wait-for-commit behavior and refresh transaction mode remain unchanged.

Validation uses one local CN with the existing LOG/TN configuration, normal
frontend authentication, three source rows and the actual mo-tester consumer.
The test-owned service is stopped in a finally block after each run. Switching
CN defaults changes only `[cn.txn].mode`; no fixture data or expected output is
changed between the final two modes.

- Unfixed pessimistic baseline: 55/58 PASS, the same 3 SHOW CREATE failures as CI.
- Qualified-input fixture, before DROP fix: pessimistic 58/58 PASS; optimistic
  51/58 PASS, exposing native DROP failure and six downstream failures. This
  failed run is retained as diagnosis, not claimed as validation success.
- Final fixture plus native DROP ownership fix: optimistic **58/58 PASS**,
  pessimistic **58/58 PASS**, both 0 failures/ignores/abnormal.
- Full frontend owning package: PASS, 25.077 s (Go 1.27.1, repository CGo wrapper,
  `matrixone_test`, `-short`, `-count=1`).

Logs: `/workspace/mv-ci/showcreate-{before,after-pessimistic,after-optimistic,
fixed-optimistic,fixed-pessimistic}.log` and `showcreate-frontend-ut.log`.
The local cluster binary uses the unchanged 1ec43a98 kernel plus the explicit
DROP transaction-mode selection. No new cluster fixture or test data is added
to CI; the existing SQL contract is reused for both public-path regressions.

Review map: SQL fixture/result is R1 test determinism; DROP transaction ownership
is R3 catalog lifecycle, using the existing native protocol rather than a new
lock or retry mechanism; this record is R0. The public cross-mode test supplies
the discriminating failure and success oracles; no options-only mock test is
added. The existing TxnExecutor callback retains sole commit/rollback ownership,
and no new asynchronous state, wait dependency or retained resource is added.
This is a focused regression fix; RFC revision 3 remains pending external review.

Final static checks: gofmt/diff whitespace checks PASS; frontend `go vet` PASS;
configured golangci-lint 2.14.0 on frontend reports **0 issues**. Final-scope
self-review found no additional code blockers. New exact-head CI remains required;
passes from 1ec43a98 are reused only for unchanged semantic inputs.


### 2026-10-15: merge upstream POINT/CHAR/index grammar without losing MV syntax

Previous head `c0bab97f8cae9ecd0bb4611b9490e1eb3d94b1b2` completed ALL CI
`38672006638` successfully after the failed-checkout rerun: ordinary UT, SCA,
coverage UT, both BVT groups, coverage and CI Required passed. Its strict
changed-line coverage was 894/1190 (75.13%). That is evidence for the old head,
not a claim about the new merge commit.

Merge verified main `f2093ae4b3cf019802091f6309e31c9524829b70` (six commits
since f49bc6fa53). The only textual conflict is generated `mysql_sql.go`.
The grammar source merges cleanly: preserve upstream POINT function syntax,
CHAR CHARACTER SET casts and index USING suffix/options, together with all MV
productions/tokens. Regenerate the complete parse tables from that source using
the repository Makefile. Generation reports zero conflicts. A second generation
produces the identical blob, proving that the delivered parser is reproducible.

Validation on merged source, Go 1.27.1, linux/amd64, `-count=1`:

- Full parser API package: PASS, 0.103 s.
- Full MySQL dialect package: PASS, 0.225 s; includes MV syntax plus upstream
  POINT, CHAR charset round trips and index suffix/identifier controls.
- Full parser tree package: PASS, 0.054 s.
- Planner `TestMaterializedView|Test.*Charset`: PASS, 0.179 s, using the repository
  CGo wrapper with `matrixone_test` and `-short`; selection is nonempty.
- MySQL dialect vet: PASS. Configured incremental golangci-lint 2.14.0: 0 issues.
- Diff whitespace and unresolved-entry checks: PASS.

Logs: `/workspace/mv-ci/parser-merge-f209-{generate,repeat,owning,plan,vet,lint}.log`.
Self-review closure: grammar composition is R2 (two public syntax consumers),
generated output and this record are R0. No handwritten parse-table selection,
new SQL feature or MV lifecycle/protocol change is introduced by the conflict
resolution. Upstream native cache/error/JSON fixes are merged unchanged. The
owning parser suites and typed planner consumer tests cover the overlap; broader
old-head CI is retained only within its semantic scope. No extra fixtures,
retries, skips or thresholds are added. No new code blockers were found in this
merge scope; external RFC/design review remains pending separately.


### 2026-10-15: FK publication waiter owns cancellation, not coordination timing

Exact failed head: `02ce42dc57df449b4333c0f9470a70677d53f760`, ALL CI
`38713744052`, ordinary UT job `116353860779`. SCA, shared build, coverage UT,
both BVT groups and coverage aggregation passed. The sole failed test event is
`TestIssue29400CopyAlterRetainedGatePromotionFastFails/fk_publication/forward_true`,
with `context deadline exceeded` at the existing successful-CREATE assertion.

The FK CREATE intentionally queues behind a retained lifecycle lock while the
test goroutine checks both catalogs and commits the holder. Its separate
15-second child deadline starts before holder setup/connection/session work.
CI shows WAL append/commit delays of 6.4 seconds and multiple delayed Raft ticks;
the CREATE is canceled at 11:15:33.44 UTC, before the holder commit completes at
11:15:34.60 UTC. Cancellation therefore defeats the very release the positive
publication case is waiting for. This evidence establishes a competing test
budget, not a demonstrated native lock-release failure or a clean-baseline
attribution of a pre-existing CI failure.

Use a cancellable child of the existing two-minute test context for this waiter.
The outer bound is unchanged. Cancellation, holder rollback, connection close
and waiter joining retain their existing lexical owners. The queued barrier,
zero table/FK catalog counts before release, successful CREATE after commit,
current parent generation and invalid-reference rejection assertions all remain.
No sleeps, retries, skips, weaker result acceptance or larger outer timeout are
introduced. Fast-fail/deadlock error-class cases are unchanged.

Also merge verified main `e8b3ed7f21c2b1966d74814adaa2bcd96d8c129f` to resolve
the current protocol-number conflict. Preserve main's native Unicode UNIQUE
protocol 114; move all MV authoring/read/registration and catalog-upgrade gates
to 115. Typed tests reject protocol/floor 114 and accept 115, while ordinary
scans retain their prior admission. The automatic planner merges retain both
upstream Unicode/fulltext/locking-read changes and MV hooks; no generated
parser or protobuf change is required.

Review map: the waiter cancellation fixture is R1 with a concurrency timing
trigger; the MV protocol allocation is R3 compatibility; this evidence is R0.
The parent deadline remains the liveness bound, and exact output/error/catalog
state remain the oracles. No new process fixture, data, hook or production wait
mechanism is added. Fresh exact-root normal/race validation and focused protocol
consumer checks are recorded below; full new-head CI remains separate.


Validation of the merged/fixed source (Go 1.27.1, linux/amd64, repository CGo
wrapper, `matrixone_test`, `-short`, `-count=1`):

- Exact failed root in normal mode: PASS, 14.300 s; all 17 subcases PASS,
  including both FK-publication directions; zero failures/skips.
- Exact failed root under `-race`: PASS, 33.390 s; all 17 subcases PASS,
  zero failures/skips and no race report. Adaptive stress budget B=30 s gives
  N=clamp(floor(30/33.390),1,100)=1; the measured exact-root run supplies that
  one execution. Build time is excluded from this measurement.
- MV-focused frontend/compile/plan/v4_0_15 tests: all four packages PASS,
  covering current 115 and immediately preceding 114 admission boundaries.
- Full owning frontend: PASS, 23.589 s; compile: PASS, 10.006 s;
  plan: PASS, 22.058 s; v4_0_15: PASS, 0.055 s.
- Six-package vet PASS; configured incremental golangci-lint 2.14.0 reports
  0 issues. Formatting, unresolved-entry and diff whitespace checks PASS.

Logs: `/workspace/mv-ci/issue29400-{fixed,race}.jsonl`,
`issue29400-mv-protocol.log`, `issue29400-owning.log`, `issue29400-vet.log`
and `issue29400-lint.log`. The exact failing concurrency group is the owning
test closure; unrelated issue roots are left to full CI. Normal/race timing is
functional evidence, not a performance comparison to the slow CI runner.
No new blockers were found in final-scope self-review. The new remote CI result
and the existing external design/re-review gates remain separate.

### 2026-10-16: preserve calendar protocol and temporary DELETE on main merge

Head `065e4d77dd0cbccfbd686a3bb362721ba82a0f09` completed ALL CI
[38760090521](https://github.com/matrixorigin/matrixone/actions/runs/38760090521)
successfully, including ordinary UT, coverage UT, SCA, shared build, both
multi-CN BVT groups, coverage aggregation and CI Required. Exact-head
Connector/J Pool Reset E2E `38760095747` and Iceberg Connector E2E
`38760095799` also completed successfully. Optional jobs explicitly skipped by
their workflows are not counted as passing tests.

Main then advanced to `a85d90efc996dfa2621211ec592be4721fd3cb61`.
The two conflicted source files cover independent contracts:

- R3 compatibility: preserve upstream calendar DAY/SECOND normalization at
  protocol 115; move MV authoring, scan, registration and 4.0.15 upgrade
  admission together to 116. Update the existing typed boundary cases so 115
  rejects MV operations and 116 admits them, while ordinary scans still admit
  at 115. Calendar-expression gates remain 115. No persisted definition format,
  task executor ID, catalog version or generated parser/protobuf changes.
- R2 DELETE planning: both base-table and index-maintenance truncate eligibility
  require an unrestricted DELETE, a persistent target and a non-MV target.
  Keeping only either side of the conflict would respectively lose temporary
  table rollback safety or MV identity preservation during atomic refresh.
  Keep both predicates. A lightweight planner regression asserts the typed
  origin DELETE node retains the MV relation, with ordinary-table truncate as
  the nearest control; reuse upstream's temporary DELETE regression. No new
  cluster, rows, timers, retries, hooks or stateful mechanism is introduced.

Automatic merges retain upstream metadata SHOW authorization, LAST_INSERT_ID
retry baselines and final ALTER/FK names alongside the existing MV guards.
Upstream's added cross-CN SYNCCOMMIT fences in the previously failing
`TestIssue29400CopyAlterRetainedGatePromotionFastFails` are retained; its FK
publication waiter keeps the explicit child cancellation fix under the existing
outer deadline. Because that fixture changed, its earlier normal/race results
are historical evidence and the exact root is selected again.

This is conflict-resolution maintenance of the existing contracts, with no new
feature/design scope. External RFC revision 3 review and the existing Request
changes remain separate release/merge gates. New-head remote CI is required;
the green 065e4d77 run does not establish success after this merge.

Validation on the merged source (Go 1.27.1, linux/amd64, repository CGo wrapper,
`matrixone_test`, `-short`, `-count=1`):

- MV/calendar/temporary-DELETE focused tests: plan 0.160 s, compile 0.102 s,
  frontend 0.078 s and v4_0_15 0.048 s, all PASS. The new MV origin-DELETE
  identity oracle and ordinary-table control are selected and PASS.
- Full owning suites: frontend 17.079 s, compile 10.092 s, plan 26.073 s and
  v4_0_15 0.073 s, all PASS.
- Exact formerly failing concurrency root: normal PASS 10.460 s, race PASS
  38.540 s. All 17 subcases PASS in each mode; zero failures/skips and no race
  report. B=30 s yields N=clamp(floor(30/38.540),1,100)=1, supplied by the
  measured race run. Build time is excluded. Test-local synchronization needs
  this exact root; unrelated issue roots remain the full-CI responsibility.
- Six-package vet PASS and configured incremental lint 2.14.0 reports 0 issues.
  Formatting, whitespace and unresolved-index checks PASS. Main's Makefile
  changes affect UT scheduling only; CGo/thirdparty sources, build flags and
  native artifacts remain unchanged. No benchmark is needed for the constant
  protocol allocation and constant-cost planner predicate conjunction.

Logs: `/workspace/mv-ci/main-a85-{focused,owning,vet,lint}.log` and
`main-a85-{root,race}.jsonl`. Both test processes terminated successfully;
there is no surviving native fixture. Source/self-review found no new blocker
in this maintenance scope. New-head multi-CN SQL BVT and full CI remain remote
checks, separately from the green parent-head evidence and external design
approval.

### 2026-10-16: follow-up main merge preserves SELECT division protocol

While exact-head ALL CI `38835654106` was running for
`6e5cffc74fd47905b0807a5f99ea63681b724a55`, main advanced from a85d90ef to
`76deafc8062f9a7e33a31a4df858ebcdb9477cb5`. GitHub then reported a new
content conflict. The existing run had passed shared build, SCA, coverage UT,
Connector/J and Iceberg; ordinary UT and both BVT jobs were still running.
This is a new protocol allocation collision, not a failed CI assertion.

Preserve upstream SELECT scalar division diagnostics at protocol 116 and
move all MV authoring/scan/registration/4.0.15 upgrade gates together to 117.
Existing typed tests now reject MV protocol/floor 116 and admit 117, retaining
the ordinary-scan control at 116. The previous temporary-table/MV truncate
predicate conjunction and its identity regression are unchanged. Upstream's
ProcessInfo outer-SELECT classification and generated protobuf are accepted
unchanged, as is its checkpoint object-data sharing fix. There is no new
feature or design scope in this maintenance merge.

Evidence reuse is scoped: the exact FK lifecycle test and its fixture are
unchanged from 6e5cffc7, including its cancellable waiter, original deadline,
cross-CN fences, cleanup owners and all 17 assertion scenarios. The new division
policy only affects division expressions, which that root does not issue;
checkpoint-version replay is not entered by its fresh, non-restarting fixture.
The prior race PASS (38.540 s, 17 subcases, no race report) remains evidence
for that unchanged waiter/holder cancellation closure, not new diagnostic or
checkpoint behavior. Fresh normal-root validation and focused division codec/
protocol consumers validate the new base; full new-head CI remains required.

The 6e5cffc7 native service was built with `vcs.modified=false` and passed
`view/materialized_view.sql` 58/58 under both optimistic and pessimistic modes,
twice on each instance. SQL plus ISCP readiness was observed. After each run,
the database and MV definition/dependency/run/task records were absent; both
owned services exited. Those observations are parent-head evidence; the new
merged source receives separate validation below.

Parent-head ALL CI subsequently completed successfully on 6e5cffc7:
[38835654106](https://github.com/matrixorigin/matrixone/actions/runs/38835654106),
2026-10-16 01:32:17–02:35:57 UTC. Ordinary UT, coverage UT, SCA, shared build,
both multi-CN BVT groups, coverage aggregation and CI Required all PASS.
Connector/J and Iceberg also completed successfully. Explicit optional skips
remain skips. This closes the original CI failure on that revision; the new
main merge still needs separate exact-head CI.

Fresh merged-source validation (Go 1.27.1, linux/amd64):

- Focused MV/temporary DELETE/division diagnostic/SQL-mode/codec tests PASS:
  plan 0.311 s, compile 0.113 s, frontend 0.142 s, v4_0_15 0.039 s and process
  0.052 s. This includes upstream division warning send/receive admission.
- Full owning suites PASS: frontend 15.464 s, compile 10.591 s, plan 28.136 s,
  v4_0_15 0.047 s and process 1.152 s.
- Exact FK concurrency root freshly rerun: normal PASS 9.930 s, race PASS
  40.260 s, all 17 subcases PASS in both modes, no failure/skip/race report.
  B=30 s gives N=clamp(floor(30/40.260),1,100)=1. Race build time is excluded;
  `-vet=off` avoids repeating the separate seven-package successful vet check.
- Seven-package vet PASS; configured incremental lint 2.14.0: 0 issues.
- Production service build PASS. Native SQL normal comparison PASS 58/58 in
  optimistic and pessimistic modes, twice per instance, zero ignored/failed/
  abnormal statements. SQL plus ISCP readiness observed. Every run verifies
  zero test database, MV definitions, dependencies, refresh history and
  executor-17 task records before repetition. Both services exited.

The pre-delivery native binary reports baseline 6e5cffc7 with `vcs.modified=true`,
because it includes this staged main merge and protocol fix. It is not a clean
6e5cffc7 build. All 11,039 tracked non-documentation input blob identities were
recorded before the native run and checked against the final delivery index;
only this documentation/evidence changed afterward. Commit metadata and these
documentation edits do not affect the tested SQL behavior. CGo/thirdparty sources
and native build flags/artifacts are unchanged by this main merge.

Logs: `/workspace/mv-ci/main-76de-{focused,owning,vet,lint}.log`,
`main-76de-{root,race}.jsonl`, `main-76de-native-build{,info}.log`,
`main-76de-bvt.json` and `main-76de-production-inputs.json`. Formatting,
whitespace, unresolved entries and final non-documentation input identity checks
PASS. No new blocker was found in this maintenance scope; external RFC review
and new-head remote CI remain separate gates.


### 2026-10-16: native upgrade and commit/ack crash evidence

The exact previous head `d50c4c77513c08837cf6ba351fae047ff9728bd2` has
completed ALL CI `38843251708` successfully, including ordinary UT, coverage
UT/aggregation, arm64 SCA, shared build, both multi-CN BVT groups and CI Required.
Connector/J `38843254257` and Iceberg `38843254214` also succeeded. Optional
compatibility jobs were skipped. Review `5543067251` subsequently requested
native upgrade/recovery validation rather than reporting a new CI failure.

The 4.0.15 handler has no tenant-upgrade phase; the framework's tenant protocol
check therefore does not consume its RequiredProtocolVersion. The handler now
checks the existing inventory gate before its first catalog DDL, failing closed
on an old node or inventory error. Existing creation/read/refresh gates remain.
Two bounded, disabled refresh fault points surround native publication; a
startup initializer exists only in an explicitly tagged instrumented binary.
No production startup environment control, new scheduler, persistent state,
protocol number, retry loop or catalog schema is introduced by this follow-up.

The final isolated native run passes **12/12 groups in 288.732 seconds**, with
normal authenticated 4.0.14 SQL, mixed-version gate refusal, SIGKILL after two
staged catalog DDLs, automatic recovery/repetition, unsafe old-node refusal with
a populated periodic MV, native lost commit-response resolution, and real
periodic task crashes before commit and after commit/before acknowledgement.
The first post-restart result is last-good before the uncommitted replay, and
already-new after durable publication. Both actual queue identities acknowledge
on restart. Post-commit replay retains the exact result/run/sequence/source-TS/
SUCCEEDED tuple; each task has one durable run. Teardown asserts no active
per-view tasks and observes completed-task GC reaching zero. All owned processes
exit: three expected SIGKILLs, the expected old-node refusal, other services
cleanly. No normal service failure is counted as a pass.

Final normal MV/4.0.15 suites pass (0.057/0.044 s); complete bootstrap passes
(12.835 s). The exact refresh-failure root passes 100 race repetitions, with
zero failures/skips/race reports; measured T=0.010 s and B=30 s yield N=100.
The full MV race package passes (1.223 s). Five-package vet and configured lint
including the startup tag pass, with 0 lint issues. Tagged and ordinary native
service builds pass. The Python driver syntax and its real terminal exit are
checked separately. Baseline CI SQL/multi-CN evidence is not relabeled as a
new-head pass; final-head remote CI must run again.

See `materialized_view_native_recovery.md`, the complete committed SQL/process
ledger `materialized_view_native_recovery_results.json`, and
`materialized_view_recovery_self_review.md`. The current pre-delivery binaries
report d50c4c77 with `vcs.modified=true`, not a pristine publication commit.
All 11,041 frozen tracked/new non-documentation inputs match at delivery; the
old archived source matches 11,132 tracked blobs of fcad62a8. Native service code
was unchanged after its build; final changes outside that build are test-only
cleanup, the process driver and documentation, separately validated. Logs and
full source-input ledgers remain in `/workspace/mv-ci/recovery-*` and the owned
`/tmp/mv-native-recovery-7` directory. No current-head performance result or
arbitrary release-pair compatibility is claimed. External re-review remains.


### 2026-10-16: license-policy correction and covered SQL bridge consumer

Follow-up head `a9bff0c918e835da7aab474896d0f0476ded09c1`, ALL CI
`38862376845`, SCA job `116847424049`, failed its Static Code Analysis step
before the Go analyzers. The complete license-eye scan checked 6,381 files:
6,195 valid, 185 ignored and one invalid:
`cmd/mo-service/materialized_view_faults.go`. Its shortened Apache notice did
not satisfy the configured full header. Add the complete notice; the same
license-eye tool, configured for that changed path with the same license and
ignore rules, reports one valid and zero invalid. No license policy changes.
Both connector workflows and shared build on a9bff0c9 succeeded; other jobs
were still queued/running. The failed run is not presented as green.

Inspection also found that the new native SQL bridge was exercised by the local
crash suite but not by the normal CI consumer. A direct coverage invocation
records 32 unexecuted blocks for that new package, while the prior d50c4c77
aggregate was 894/1190 (75.13%). This is an execution-path gap, not an observed
new-head coverage verdict. Add real protocol/session/error checks to the
existing covered BVT entry rather than changing thresholds or exclusions.

`test/materialized_view/native_probe/check.py` sends nine public requests in
one bridge lifetime: malformed JSON, invalid DSN, multi-row/NULL decoding,
Exec, session reuse, empty results, Query/Exec server errors and a subsequent
successful read. It repeats with a fresh bridge process against the same
native instance. Only session-local state changes. Both client lifetimes close
on EOF; the shell removes stale reports, verifies the new report, and merges
its normal covermode=set counters with the existing producer. The local bridge
counters cover 28/32 blocks (87.5%); this component result is not an aggregate
CI percentage. The CI consumer result ledger records full-entry validation.

The 12-case native recovery capture remains tied to a9bff0c9. Service, upgrade
and bridge Go sources are byte-identical; the startup initializer's bytes from
its unchanged build tag onward are identical, with only the preceding license
comment expanded. The capture ledger records that semantic reuse explicitly.
New consumer checks are separate test-only inputs; the earlier complete input
fingerprint is not relabeled as byte-identical to this follow-up's test scripts.


Final covered-consumer validation passes the exact immutable entry script twice
on the same two-CN native instance: each run has 2 x 9 bridge requests and
70 functional + 5 failure + 9 cross-CN checks. Independent post-run database,
definition, dependency, run and task counts are zero before repetition. All
owned services exit. `materialized_view_ci_bridge_results.json` retains both
runs, exact changed input hashes, license proof and component coverage. The
production Go service/bridge inputs are unchanged from the 12-case capture;
only the startup initializer license comment changes. Final-head remote SCA,
UT, coverage and BVT remain required separately.


### 2026-10-16: mixed-version coordinator publication counterexample

Raw old-CN logs from the earlier 12-case capture show the old fcad62a8 CN
fetching `materialized-view-reconcile-v1:1/17` and reporting `executor with code
17 not exists` at 06:29:34 UTC. Catalog DDL was blocked correctly, but the CN
registered the new reconcile cron independently of catalog/protocol readiness.
The old native suite omitted that negative assertion. Its selected passes do
not establish task exclusion; the current ledger supersedes it with 13 groups.

Move admission to the first publisher: `mv.Service.RegisterCron` checks all
four committed catalogs and common protocol >=117 before creating any cron
metadata. Reuse the existing CN stopper-owned retry loop by passing its create
operation as a callback; lineage GC retains its original metadata/expression.
Missing/partial catalogs, inventory/query errors and old protocols publish no
task metadata. No task-registry/scheduler/protocol/catalog-policy redesign.

Eight admission scenarios pass; the retry fixture proves a readiness rejection,
a subsequent task-store rejection and eventual registration under one retry
owner. Full MV/CN owning suites pass normally (0.057/8.337 s) and under race
(1.219/9.961 s). Both exact concurrency roots pass 100 race repetitions (T is
below measured resolution, N=100). Initial full CN race inherited the author's
3-GiB memory cap and failed the unrelated default-limit assertion: actual
3221225472 versus expected MaxInt64. Same code passes with the normal unset
limit; no assertion/skip/source fix was made. Both logs remain available.
Two-package vet/incremental lint pass, 0 issues; tagged and normal builds pass.

The strengthened native suite passes 13/13 groups in 324.111 s, with normal old
login, observed registration denial, zero executor-17 cron/async rows while old
nodes run, no old unknown-executor log, then successful upgrade, periodic
publication and both real crash/replay boundaries. All 11,042 frozen tracked
non-documentation inputs remain unchanged. Current binaries report f8a9127f
plus vcs.modified=true. Full SQL/process and counterexample evidence are in
`materialized_view_native_recovery_results.json` and
`materialized_view_task_admission_results.json`. All owned processes exit.


Final post-admission production-consumer proof: the exact entry passes twice
on the same two-CN instance, each with 18 bridge requests and 70 functional,
5 failure and 9 cross-CN checks. Independent cleanup counts are zero after
each run and all services exit. Bridge counters remain 28/32 blocks; no
coverage-policy change. All 11,042 frozen inputs match. The new source/result
ledger supersedes the earlier consumer-only capture. Final-head remote CI
remains separate.
