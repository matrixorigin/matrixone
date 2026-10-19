# Materialized View Validation and Performance Plan

- Status: experimental P1; SQL, publication, lifecycle and native upgrade/crash checks have separate evidence; broader workloads remain pending
- Design: [Materialized Views RFC, revision 3](../rfcs/00000000_materialized_views.md)
- Historical performance prototype: `dd38098853ea197699ed3fac3e69dc53975ac061`
- User requirement: implement the feature, independently design cases from
  StarRocks documentation, then verify usability and actual performance benefit

## Native upgrade and crash boundaries (C07/C10/C12)

The [native recovery procedure](materialized_view_native_recovery.md) defines
the isolated old/new binary topology, deterministic commit barriers and
independent SQL oracles. Its [result ledger](materialized_view_native_recovery_results.json)
retains binary/source identity, SQL state changes and process exit codes.
This evidence supplements the older graceful restart and in-memory failure
tests; those tests alone do not establish durable task replay or compatibility.
General CI's skipped compatibility jobs are not relabeled as successful.

## Reference behavior and scope

Use the official [async MV overview](https://docs.starrocks.io/docs/using_starrocks/async_mv/Materialized_view/)
for grouped/join reports and manual/scheduled refresh scenarios;
[REFRESH syntax](https://docs.starrocks.io/docs/sql-reference/sql-statements/materialized_view/REFRESH_MATERIALIZED_VIEW/)
for completion and refresh task observability; and
[query rewrite use cases](https://docs.starrocks.io/docs/using_starrocks/async_mv/use_cases/query_rewrite_with_materialized_views/)
for matching, rollup, predicates and EXPLAIN verification. These documentation
pages currently display Latest-4.1; the implementation reference is separately
pinned to code tag 3.5.7. Record both, not pretend they are the same revision.

These are behavior references. MO uses its proposed syntax, native transaction
semantics, and conservative permissions. Distribution clauses, external catalogs,
partition refresh, nested MVs and transparent rewrite not implemented in P1 are
reported as NOT IMPLEMENTED. Rejection tests can pass without claiming support.

## Minimal SQL fixture and independent oracle

Use one test-owned database per case or suite fixture with deterministic cleanup.
No million-row fixture is needed to prove ordinary correctness.

```sql
CREATE TABLE orders (
    id INT PRIMARY KEY,
    day DATE,
    region VARCHAR(8),
    amount DECIMAL(12,2)
);
INSERT INTO orders VALUES
    (1,'2026-10-01','east',10.00),
    (2,'2026-10-01','east',20.00),
    (3,'2026-10-01','west',NULL),
    (4,'2026-10-02','east',30.00),
    (5,'2026-10-02',NULL,5.00),
    (6,'2026-10-02','east',NULL);

-- Proposed MO syntax; requires implementation.
CREATE MATERIALIZED VIEW daily REFRESH MANUAL AS
SELECT day, region, SUM(amount) AS total,
       COUNT(*) AS n, COUNT(amount) AS nonnull_n,
       MIN(amount) AS lo, MAX(amount) AS hi, AVG(amount) AS mean_amount
FROM orders GROUP BY day, region;
REFRESH MATERIALIZED VIEW daily;
SELECT * FROM daily ORDER BY day, region;
```

Expected groups, compared without relying on result order:

| day | region | total | n | nonnull_n | lo | hi | mean_amount |
|---|---|---|---|---|---|---|---|
| 2026-10-01 | east | 30 | 2 | 2 | 10 | 20 | 15 |
| 2026-10-01 | west | NULL | 1 | 0 | NULL | NULL | NULL |
| 2026-10-02 | east | 30 | 2 | 1 | 30 | 30 | 30 |
| 2026-10-02 | NULL | 5 | 1 | 1 | 5 | 5 | 5 |

Two independent oracles are required: typed expected values for this fixture,
and the original source SELECT at the captured source snapshot S. Compare
multisets, SQL NULLs, DECIMAL scale/precision and result column metadata. A hash
alone is not the sole correctness oracle. Never compare an old MV result with
current source rows after a concurrent write and call the mismatch a bug.

Mutation sequence: INSERT east=7; UPDATE id=1 from 10 to 50; DELETE id=2;
REFRESH again. Verify totals with a separately computed expected result. Read
before refresh still returns last-good materialization. Keep GROUP BY and scalar
aggregate empty-input cases separate: grouped output has zero rows, scalar
COUNT(*) has one row containing 0, scalar SUM has one row containing NULL.

## Functional case inventory

| ID | Case | Required observable assertion | Stage |
|---|---|---|---|
| F01 | Create and inspect | SHOW CREATE/columns preserve definition, types and aliases; unpopulated read errors | P1 |
| F02 | First full refresh | Fixture equals typed oracle and source SELECT at S | P1 |
| F03 | Repeated refresh with no writes | No duplicate output; row counts and coverage are valid | P1 |
| F04 | INSERT/UPDATE/DELETE then refresh | Last-good before refresh; complete new result afterward | P1 |
| F05 | Empty and all-NULL input | COUNT, SUM, AVG and grouping retain SQL semantics | P1 |
| F06 | Join and aggregate | Dimension/fact duplicate matches retain multiplicity; changed dimension reflected after full refresh | P1 |
| F07 | Captured SQL context | Different session timezone/SQL mode cannot silently change persisted definition semantics | P1 |
| F08 | Output naming | Expanded star, aliases and headings fixed; duplicate output names rejected | P1 |
| F09 | Read-only object | DML, LOAD DATA, generic ALTER/TRUNCATE/DROP TABLE and unauthorized internal writes rejected | P1 |
| F10 | Source identity lifecycle | ALTER/RENAME/DROP/TRUNCATE/COPY blocked with dependencies; name reuse never rebinds silently | P1 |
| F11 | Permissions and tenant | Same object names in two accounts isolated; live owner/role revocation suspends refresh | P1 |
| F12 | Manual/scheduled policy | Fake clock proves coalescing, no overlapping queue growth, observable run status | P1 |
| F13 | Drop cleanup | DROP MV cleans owned state; ordinary DROP DB rejects surviving MVs; DROP ACCOUNT transactionally retires populated/periodic MVs and cleans metadata/grants/objects | P1 |
| F14 | Unsupported shapes | Volatile functions, UDF, LIMIT, external/history sources and MV nesting rejected clearly | P1 |
| F15 | Prepared/direct queries | Prepared SELECT still respects object kind, snapshot and privilege changes | P1 |
| G01 | Native timestamp provenance | Source S differs from publication C and is reported accurately | P1 |
| G02 | Branch source | Branch and main results differ correctly; branch operation obeys identity fences | P1 if branch owner admitted |
| G03 | Historical refresh | Pinned snapshot used for all sources; snapshot name recreated with new identity rejected | P1.1 |
| G04 | Snapshot retention | DROP/PITR shrink/reclaim race cannot remove active historical input | P1.1 |
| G05 | Historical output | Prior output snapshot reads corresponding result and historical metadata | later extension |
| R01 | Exact match rewrite | Original SQL reads MV; output equals base query; EXPLAIN proves selection | P2 |
| R02 | Group-key rollup | Coarser grouping merges SUM/COUNT correctly and finalizes AVG from SUM/COUNT | P2 |
| R03 | Predicate/HAVING negatives | Changed filter, missing grouping column, wrong collation or DISTINCT cannot unsafe-match | P2 |
| R04 | Freshness default | DML after S without schema change forces base fallback unless complete no-change proof exists | P2 |
| R05 | Explicit stale permission | Age checked from source S; boundaries use fake time; EXPLAIN exposes permission | P2 |
| R06 | Prepared rewrite reexecution | Changed data/privileges/epoch never reuse an obsolete freshness authorization | P2 |

## Concurrency, failure and recovery cases

Use injected hooks and barriers in the refresh executor. Map a concrete producer
and consumer to each hook; no arbitrary sleep-based polling.

| ID | Injected boundary | Assertion |
|---|---|---|
| C01 | Pause after DELETE, before INSERT | Reader at another snapshot still sees complete old rows |
| C02 | Pause after INSERT, before publication commit | Readers never see a mixed/partial result |
| C03 | Source commits between refresh statements | Every source read still uses one SI snapshot S |
| C04 | Two CNs publish same MV | At most one conflicting publication commits; stale sequence cannot regress state |
| C05 | Replay same task/run | No double publication or contradictory terminal status |
| C06 | Failure in scan/write/metadata publication | Entire replacement aborts; last-good rows and coverage remain |
| C07 | Lost response after commit | Restart checks durable run/txn outcome; no accidental FAILED overwrite |
| C08 | Timeout/cancel/shutdown during wait or execution | Transaction, Result, admission and spill resources terminate/release |
| C09 | DROP MV/DB/account racing task | DROP DB RESTRICT preserves live MVs; MV/account retirement fences publication into dropped/recreated identity and makes queued tasks obsolete |
| C10 | Crash after policy commit before task creation | Reconciliation restores intended schedule once |
| C11 | Backup/PITR restore | Objects restored paused; owner/bindings checked; tasks not resurrected |
| C12 | Mixed-version or downgrade | Incapable CN cannot claim task/execute MV; unsafe downgrade refused |

Run focused race evidence only for the changed concurrency closures. UT owns
failure injection/clock/algorithm proofs; BVT owns public SQL/results/errors;
multi-CN/restart suites own distribution and persistence, not duplicate all UTs.

## Performance experiment

Verify correctness before timing. Use the same binary, account, resource limits,
typed parameters and committed source version for raw and MV queries. Baseline
mode must explicitly disable any later automatic MV rewrite. Record plans.

Deterministically generate 1 million fact rows and 10 thousand dimension rows;
scale to 10 million only if environment/admission permits. Use integer cents
and DECIMAL for exact comparison, 365 days, 20 regions, 1000 products, and skewed
as well as uniform keys. Persist generator version/seed, row counts and hashes.
Correctness fixtures remain separate from this benchmark dataset.

| ID | Raw query / materialization | Question answered |
|---|---|---|
| B01 | Daily-region SUM/COUNT over facts | Benefit of high reduction ratio |
| B02 | Monthly report from daily summary | Explicit rollup benefit; automatic rollup measured separately in P2 |
| B03 | Fact-dimension join grouped by segment | Savings from precomputing repeated joins |
| B04 | Narrow filter on raw indexed key | Control showing an MV need not win every query |
| B05 | High-cardinality summary near fact size | Storage/refresh cost and weak-benefit boundary |
| B06 | 4-client mixed reports | Throughput, latency and contention |
| B07 | Query while full refresh runs | Reader stability and resource competition |
| B08 | Source-write workload with P1 enabled | Refresh/background overhead on business writes |

For single-query warm timings: two warmups, then 50 paired observations; alternate
or seeded-randomize raw/MV order. Retain all samples, median, P95, dispersion and
any errors. P95 from 50 samples is descriptive, not a reliable production-tail
estimate. Separate client connection time from query execution. Retrieve the
entire result, not only time Execute(). Compare outputs each observation.

Cold-cache claims require an actual controlled cache-reset protocol. A first
run or reconnect is insufficient. If cache cannot be controlled, report warm
results only. For concurrent runs use fixed offered work or equal duration and
separate connection setup; record completions, errors, QPS and latency. Respect
the environment budget and avoid running unrelated databases simultaneously.

Measure refresh duration, rows scanned/written, CPU time, peak admitted memory,
spill/temporary disk, physical result size, retained MVCC/compaction cost, and
source-write median/P95. Report initial and repeated full refresh separately.
Use actual engine profile/metrics where available; do not invent absent counters.

Required reported calculations:

```text
query_speedup = median(raw_query_ms) / median(mv_query_ms)
break_even_queries = ceil(refresh_ms / (raw_query_ms - mv_query_ms))
```

Break-even is defined only when the denominator is positive. Include CREATE and
repeated-refresh costs for the chosen refresh window; latency-only break-even is
not a complete CPU/storage economic model. For a window with N queries compare
`N * raw_cost` with `refresh_cost + N * mv_cost`. Report dimension cardinality,
freshness interval and reduction ratio alongside speedup. Do not choose only the
fastest query or best repeat. If no measurable benefit exists, report that result.

Correctness has zero tolerance for wrong rows/partial visibility/privilege leaks.
Query improvement targets are workload-specific hypotheses, not claims before
measurement. A positive grouped-report outcome requires the observed median gain
to exceed repeat dispersion and its refresh cost to amortize within the stated
query frequency. Controls and low-benefit cases remain in the report.

## Delivery evidence and stage labels

After implementation, deliver checked-in SQL cases/expected outputs, focused UTs,
the reproducible generator/benchmark runner, raw measurements, EXPLAIN/profile
evidence, binary/source SHA, configuration and a concise measured report.
Every case records PASS/FAIL/NOT IMPLEMENTED/BLOCKED, stage and exact command.
Pending, skipped, blocked or parser-only evidence is never an MV runtime pass.
P1 explicit-MV acceleration is never labeled transparent-query-rewrite benefit.

Follow the repository native/CGo validation wrapper for owning packages. Reuse
valid existing evidence, but new code and altered semantic inputs invalidate the
relevant result. Any environment/download/port permission blocker is reported
separately from product correctness. This plan itself is documentation-only and
has not produced benchmark results.


## Local execution record — 2026-10-12

Task baseline is `dd38098853ea197699ed3fac3e69dc53975ac061`; task changes are local
tracked/untracked changes relative to that HEAD. The unrelated committed range
against origin/main is excluded from this task delivery. Remote base freshness
was not re-established. Toolchain: installed Go 1.27.1, Linux amd64, CPU CGo,
repository native artifacts reused by the controlled test wrapper.

| Evidence | Result | Scope/limit |
|---|---|---|
| Focused materialized-view tests | PASS: 19 top-level tests and 64 subcases | Parser/binder/catalog/frontend/compile/service/writer boundaries; final plan/parser log supersedes earlier failing attempts |
| Owning package suites | PASS | catalog, mv, bootstrap v4_0_15, mysql parser, insert, multi_update |
| Race, 100 repeats per exact test | PASS | output counter, failure boundaries, admission, timezone, reconciliation identity; package JSON terminal pass events retained |
| Go build | PASS | service binary and standalone SQL harness |
| Incremental go vet | PASS | changed package closure plus direct consumers; final plan/harness check repeats after last edits |
| Formatting and diff whitespace | PASS | task's changed/new Go files and git diff --check |
| Full configured lint | NOT RUN | golangci-lint unavailable; goimports tool build hit a missing cached module and denied network |
| Local service/public SQL | BLOCKED | TCP bind: socket operation not permitted; permission request interrupted |
| Scheduled SQL, native restart, multi-CN and downgrade | NOT EXECUTED | no running local cluster |
| Performance B01 | HARNESS READY, NOT EXECUTED | deterministic million-row/128-group explicit MV; 50 alternating samples, plans, equality and refresh cost |
| Performance B02–B08 | PLANNED | broader benchmark inventory above remains required; no fabricated speedup |

Evidence logs are under `/workspace/mv-build/`: `final-focused.log`,
`plan-parser-final.log`, `owning-tests.log`, `race-*.json`, `vet.log`,
`vet-final.log`, and the failed `server.log`. The current service and SQL harness
are in that same test-owned directory. Read `test/materialized_view/README.md`
for the runnable SQL, security, tenant, concurrency and restart companion.

The unit failures found and fixed single-table JOIN representation, missing
sampling/parameter rejection, source-kind admission after view expansion,
duplicate output names, and fixed-offset timezone reconstruction. A qualified
function rejected by the parser is tested at that parser boundary rather than
counted as an AST-validation case. Actual SQL fixtures and performance remain
unverified; this is not a release PASS.


## Runtime evidence update — 2026-10-13

Network permission was granted after the earlier interrupted request. The local
service then exposed and helped fix the nested-statement, global catalog tenant
initialization, task ID width and missing deadline issues. Final runs passed
66 main SQL checks, 3 restart checks and 5 native failure/cleanup checks.
Four performance scenarios completed with 50 alternating pairs each. The earlier
BLOCKED rows above describe the initial attempt and are superseded by
[the execution report](materialized_view_results.md). Nine additional two-CN
publication/identity/history checks passed (83 total SQL checks). Delivery-focused
tests passed 22 top-level tests and 65 subcases; the six owning package suites
passed again after the runtime fixes. Task identity, reconciliation identity and
refresh failure boundaries each passed a separate exact-name 100-repeat race
run. No mixed-version upgrade/downgrade or B04/B06–B08 execution is claimed.

## Publication update — 2026-10-13

The branch rebased onto fresh main `3e50e1b32d8da61bbbf70c89989fb08b671809de`
uses protocol 112. Revalidation passed 22 focused tests / 66 subcases and six
owning suites. Protocol 111 and authoring floor 111 reject MV authoring.
Configured incremental golangci-lint 2.14.0 passed with zero findings after
strengthening complete result consumption, stream-error checks and row cleanup
in the SQL harness. A new native escaped-source-name / NO_BACKSLASH_ESCAPES case
found and verified a control-plane parser-mode isolation fix. The final native
case count is 84: 67 main + 3 restart + 5 failure/cleanup + 9 two-CN.
The publication evidence is separate from original baseline measurements in
[the execution report](materialized_view_results.md).

### Revision 3 account and migration regression acceptance

- Existing initialized catalog with all four MV tables absent and automatic
  migration disabled: ordinary ALTER, DROP/TRUNCATE, CREATE/DROP INDEX and DROP
  DATABASE remain usable in pessimistic and optimistic modes, with the feature
  disabled. Restore the task-owned schema before any MV feature cases. Installed
  dependent sources continue to reject DDL; partial catalog installation and
  catalog-read errors fail closed rather than silently disabling guards.
- One periodic MV with last-good rows: change its source, suspend/restrict the
  account, dispatch a typed scheduled envelope and await a terminal failure.
  Verify successful_sequence, source snapshot and native rows did not change.
  Reopen and dispatch a fresh envelope; verify successful refresh equals source.
  Missing account/unknown status and downstream status-query errors reject.
- Administrative DROP ACCOUNT with surviving populated and periodic MVs succeeds;
  verify all four MV catalog rows, native relations, tenant grants and queued
  account envelopes are removed. A captured old envelope cannot publish after
  retirement. Ordinary DROP DATABASE still rejects the same surviving MVs.
- Inject failure at retirement and after retirement in the account transaction:
  require rollback (no tenant DROP DATABASE after early failure); account, MV
  policy, dependency closure and last-good rows remain intact for retry.

This is the selected P1 policy, not a claim of external design approval.

Evidence boundaries for the old-catalog controls: a fresh normal login on the
4.0.14 fixture first hits existing tenant-version admission. The isolated kernel
before/after comparison uses supported skipCheckUser sessions only there, with
privilege checks retained, and proves six DDL guards in each transaction mode.
Normal account/ACL cases use authenticated sessions. This does not satisfy the
separate normal-login/mixed-version release gate. Case-level/source hashes and
functional fixture cost are in materialized_view_review_lifecycle_results.json.


## Native consumer regression follow-up (2026-10-18)

Review5562448876 adds three acceptance boundaries: native object loading must
reject MV targets before file/Write admission; unrestricted source DELETE must
use row DML when dependencies pin identity, with matching index maintenance;
and standard SHOW CREATE TABLE must export the same logical MV/current policy
as explicit SHOW CREATE MATERIALIZED VIEW. The executable `-consumer-check`
mode includes prepared-before-creation and feature-disabled DELETE, affected-row
boundaries, unique/secondary controls, unchanged publication, native import
rejection and manual/altered-policy DDL replay. See
[consumer corrections](materialized_view_consumer_fixes.md) and its evidence JSON.
## Additional cross-CN cache boundary

Prepare source DELETE on one CN, create/refresh its MV on another, then EXECUTE
without changing the prepared owner's DDL generation. Require row deletion,
unchanged last-good MV rows/full publication and zero fixture residue in RC and
SI. [Requirement, design, controls and execution receipts](materialized_view_cached_delete.md).

## Prepared metadata and retirement follow-up (2026-10-18)

See [the design and failure analysis](materialized_view_prepared_metadata.md)
and [source-bound results](materialized_view_prepared_metadata_results.json)
for live prepared SHOW metadata, the common lifecycle fence, fixed SI
publication under an outer RC lock, both 113-check native transaction modes,
and commit-loss/SIGKILL/replay validation. The merge regenerates the MySQL
parser to retain main's empty-CTE rejection and MV grammar. Native candidates,
merged package checks, discarded trials and historical evidence are scoped
explicitly in those records.

## Protocol120 and dependent execution integration (2026-10-18)

Main75d16a2b allocated119 to correlated programs. MV admission now requires120
across authoring, reads, registration, migration and cron. See [design/cases](materialized_view_protocol120_integration.md)
and [integration results](materialized_view_protocol120_integration_results.json).
The existing native consumer now has14 cases and the full entrypoint116.
Parent95dd4c0d passed ALL CI39145960351 and both connector workflows; these are
parent results, distinct from the new integration's CI.


## Cross-CN coverage fixture visibility correction

The [CI diagnosis](materialized_view_ci_triage.md#coverage-ut-cross-cn-view-description-after-source-restore)
and [scoped results](materialized_view_cross_cn_visibility_results.json) document
the commit-frontier handoff required before asserting post-RESTORE metadata on
a Push-mode peer. This changes the existing ordinary-View test only; it does not
change MV semantics or add a performance claim. Remote CI is checked separately
at the published correction head.


## Native branch endpoint admission (2026-10-19)

[Requirement, design and regression map](materialized_view_branch_admission.md)
close review5564803316: native branch readers explicitly reject MV endpoints in
P1, before diff output or merge writes. The same delivery preserves main's
foreign-key-check propagation and MV's captured SQL mode in the internal compiler.
[Source-bound results](materialized_view_branch_admission_results.json) distinguish
native negative/positive controls, merged suites and final-head CI.


## Catalog4.0.18 integration (2026-10-19)

Main def799e126 allocated4.0.17 to its MEDIUMINT tenant repair. Preserve that
handler and move MV migration to4.0.18/offset5, still requiring protocol120.
[Integration design/case map](materialized_view_catalog418.md) and
[source-bound unit/native results](materialized_view_catalog418_results.json)
separate the real clean-main417 upgrade control from older captures.
