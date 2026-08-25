# MatrixOne UT And BVT Contract

Use this contract when production behavior changes, when adding or changing a
unit/BVT case, or when optimizing test duration or flakiness. Tests are a
behavioral specification and a delivery boundary, not a line-coverage quota.

- **UT purpose:** prove an internal invariant, ownership boundary, state
  transition, or narrow package contract quickly and deterministically.
- **BVT purpose:** prove the externally visible SQL/protocol contract through a
  real service with the smallest sufficient schema and data.

Neither layer is a load, stability, soak, benchmark, or big-data suite. Route a
claim that fundamentally depends on scale or elapsed time to the appropriate
performance/stability/big-data harness, while keeping a small functional UT/BVT
for the contract that can be proved without scale.

## 1. Start With A Validation Map

Before writing a test, map each changed behavior to the cheapest layer that can
prove it:

| Behavior | Focused UT | BVT | Additional layer |
|---|---|---|---|
| Internal algorithm/helper with no public behavioral change | required | normally no; record why | benchmark/race when applicable |
| SQL-visible result, error, metadata, DDL/DML, privilege, or transaction behavior | required where the owning code has a usable seam | required public-path proof | topology/restart only when part of the contract |
| Protocol or client-visible behavior | required for encoding/state logic | required through the real frontend/client | compatibility test when applicable |
| Multi-CN routing, persistence, restart, upgrade, or distributed lifecycle | focused component tests | required on the relevant topology | restart/upgrade/chaos suite when BVT cannot prove it |

For a production bug, preserve the smallest pre-fix reproduction when practical
and derive nearby controls from the violated invariant. Read
[counterexample-testing.md](counterexample-testing.md) when the bug involves
planner, explain, rewrite, or scenario-overfit risk.

Do not add a BVT case merely to repeat internal permutations already proved by
UT. Conversely, an internal UT is not sufficient proof for a changed public SQL
contract. When BVT is not applicable to externally visible-looking work, record
the concrete boundary that makes it unnecessary.

## 2. Cost And Controllability Gate

Before adding a test, state its fixture cost, data cardinality, expected runtime,
and which condition makes it terminate. Use the cheapest controllable mechanism
that still proves the contract, in this order:

1. pure helper/model with explicit inputs;
2. injected dependency, fake clock, failpoint, callback, channel/barrier, or
   deterministic scheduler hook;
3. scoped dynamic configuration applied at fixture construction or test entry
   and restored with immediate cleanup;
4. real cluster, wall clock, external service, or larger data only when those are
   themselves part of the behavior being proved.

Use minimum boundary data: normally zero, one, two/competing rows, plus the
smallest cardinality that crosses the relevant threshold. Do not create a large
table, long transaction history, many accounts/databases, or repeated workload
to probabilistically reach a state that can be injected or configured directly.

`time.Sleep` is not synchronization and a short timeout is not a phase trigger.
Use observable barriers or injection. Bounded polling of a real asynchronous
public condition is acceptable only when no deterministic hook exists; record
the condition and keep the outer deadline a hang guard rather than the oracle.

Any new embedded cluster/service process, external dependency, or material
increase in owning-package/BVT-group time requires a measured before/after delta
and a written reason why reuse, injection, dynamic configuration, or a smaller
dataset cannot prove the same contract. A performance assertion belongs in a
benchmark/performance suite, not in ordinary UT/BVT wall-clock thresholds.
Prefer existing injection seams. A new production-side test hook must have an
explicit owner, be inert outside the test, add no material hot-path cost, and be
reviewed as production code rather than hidden as testing convenience.

## 3. Unit-Test Requirements

### Coverage and oracles

- Test the invariant, its negation, and the nearest non-equivalent controls.
  Select applicable axes: success, empty/zero, boundary, invalid input,
  downstream error, cancel/timeout, retry, reset/reuse, restart, and concurrent
  competitor.
- Prefer a public or package contract oracle. Use white-box assertions only when
  they prove a distinct ownership, plan-shape, or state-transition claim.
- Assert durable output, error class, committed state, or exact side-effect
  count. Do not assert a transient map entry, goroutine winner, log ordering, or
  incidental implementation detail as the only oracle.
- Never make a case pass with scheduler sleeps, retries, skipped cases, weaker
  assertions, or broad `require.NotPanics` checks unless that behavior is itself
  the product contract.

### Orthogonality and fixture economy

Logical scenarios must remain independently identifiable, but expensive setup
does not have to be repeated:

1. Before adding `TestXxx`, search existing tests by changed symbol, invariant,
   error, and public surface. Extend an existing table/subtest suite when it has
   the same contract and fixture; a different issue number alone is not a new
   semantic cell.
2. Before starting a cluster, engine, account, catalog, file service, or large
   data fixture, search the owning package for an equivalent fixture/helper.
3. Share setup only when topology, configuration, lifecycle generation, and
   initial state are compatible. Preserve scenarios as named subtests or table
   rows so a failure still identifies one contract.
4. Reset every mutable boundary between scenarios: database/catalog rows,
   accounts, sessions, globals, runtime hooks, clocks, failpoints, caches, and
   object providers. Register cleanup immediately so `FailNow` and panic paths
   restore state too.
5. Use a fresh lightweight session/context per scenario when session state can
   persist. Run a permanently mutating scenario last only when restoration is
   impossible and the enclosing fixture owns final destruction; document that
   ordering constraint.
6. Do not use `t.Parallel` on a shared fixture until disjoint state and runtime
   hooks are proven. Serialization is preferable to a faster flaky suite.
7. Keep separate fixtures when memory limits, failpoints, topology, transaction
   settings, restart generation, or destructive global state differ. Merging
   by test name or package alone is not a valid optimization.

When optimizing CI, separate stage wall time, package elapsed, test-body time,
and queue/admission-lock time. Compare before/after in the same mode; a test can
look slow only because it waited for another cluster. Optimize repeated setup or
unnecessary work before increasing parallelism, and retain a coverage map for
every merged or removed scenario.

### UT execution evidence

- Derive owning package patterns from the final diff and prove exact focused
  selection with `go test -list` (or the CGo wrapper). A green command that ran
  zero tests is not evidence.
- Run each changed/directly affected behavioral test with `-count=1`, then each
  owning package once in normal mode. Use bounded, test-appropriate timeouts.
- Apply the adaptive focused race protocol and full owning-package race run from
  `mo-self-review` when shared state, concurrency, lifecycle, or timing can fail.
- Use `.agents/skills/mo-dev/scripts/mo-cgo-test` for CGo-direct or transitive
  packages. Do not substitute a successful build for a linked test binary.
- All evidence must be newer than the last semantic edit or rebase and include
  the real exit status.

## 4. BVT Applicability And Reuse Gate

BVT lives under `test/distributed/cases/` and proves behavior through the real
SQL/frontend path with mo-tester. Before adding a file or statements:

1. Search existing cases by SQL surface, function/operator name, catalog table,
   error code, issue number, and neighboring feature directory. Inspect both
   case and result; filename matches alone do not prove coverage.
2. Write a small coverage table: existing scenario/oracle, new semantic cell,
   and whether the fixture/topology is identical.
3. Prefer extending or consolidating an existing coherent case when it uses the
   same topology, account/role, database/schema, data shape, and lifecycle and
   the new statements add a distinct oracle. Reuse one setup and one teardown.
4. Create a separate case when isolation, topology, credentials, global config,
   restart boundary, resource service, or destructive lifecycle differs. A new
   file must be independently runnable; files must never depend on lexical
   execution order or residue from another file.
5. Do not grow a catch-all script. A consolidated file must still have named
   comments/sections for each invariant and local setup only for genuinely
   shared state. Split it when understanding or failure localization would
   require replaying unrelated behavior.

For existing low-quality, duplicate, or non-orthogonal BVT, consolidate it at
the common fixture/contract boundary. Before deleting statements or files, map
every old positive, negative, boundary, privilege/session, metadata, and error
oracle to the retained case. Similar SQL text or a smaller file count is not
coverage equivalence. Measure the exact cases before and after when performance
is the purpose. When a new or moved case materially changes a BVT group, include
the group wall-time delta and keep the two CI groups balanced; do not compensate
for a slow case by weakening its oracle or hiding it in another directory.

## 5. BVT Case Quality

- Make the file self-contained. Use collision-resistant feature-specific names,
  clean possible residue at entry, and remove created databases, accounts,
  roles, stages, snapshots/PITRs, publications, files, sessions, and global
  settings at exit. Restore pre-existing global values rather than assigning an
  arbitrary default.
- Keep setup proportional. Reuse a database/account and seed data across related
  scenarios in one file; do not recreate a heavy fixture for every lightweight
  assertion. Use the minimum rows that distinguish the public outcomes; move
  volume-, duration-, or throughput-dependent coverage to its owning suite.
- Cover observable success plus the relevant rejection/boundary path. For
  transactions or privileges, assert from the affected second session/account,
  not only from the creator.
- Make output deterministic with explicit predicates and `ORDER BY` when order
  is the contract. Use `@sortkey` only when order is intentionally irrelevant,
  `@ignore` only for truly unstable columns, and `@regex` only for the stable
  part of an error/value. These tags must not hide a behavior the case claims to
  verify.
- Synchronize multi-session cases with `@wait` or an observable database
  condition, never a small sleep. When a public dynamic variable/configuration
  exposes the boundary directly, set it to the smallest useful value and restore
  its prior value. Close or finish every transaction/session on success and
  expected-error paths.
- A new expected behavior must not be placed under `@skip` or `@bvt:issue`.
  Existing skips require a live issue and an explicit reason; removing a skip
  requires running the formerly skipped statements.
- Generate `.result` only as a draft. Review every changed row and column
  metadata, then run normal comparison mode. Never accept `genrs` output merely
  because it was generated by the current binary.
- Follow `test/distributed/cases/README.md` for active mo-tester tags; do not add
  deprecated tags to new cases.

## 6. BVT Execution Evidence

Use a test-owned instance and workspace. Do not kill a process or delete
`mo-data` unless its ownership and exact path are known.

1. Start from a clean instance for a suite that observes global catalog state.
   Wait for SQL readiness **and** `ISCP-Task Start` in `mo-service.log`; `select
   1`/`show databases` can succeed before query-service registration.
2. Run the exact changed case in normal comparison mode, using the same flags
   and resources as the active CI path (normally mo-tester `-n -g -o -p`, plus
   `-s` when the case needs resources). Prove the report names the intended file
   and capture its real exit code.
3. Assert teardown postconditions for created global/catalog resources, then run
   the exact case a second time against the same instance. Entry-time `DROP IF
   EXISTS` improves recovery but can mask a failed prior teardown, so repetition
   alone is not cleanup proof. If the contract intentionally restarts the
   service or mutates unrecoverable global state, use a fresh test-owned instance
   for the second run and record why same-instance repetition is invalid.
4. Run the owning directory/group when shared setup, result conventions,
   mo-tester tags, resources, or suite configuration changed. Run both standalone
   and compose/multi-CN only when the behavior or changed harness crosses that
   topology boundary; state the topology decision.
5. Inspect mo-tester report and `mo-service.log`, not only stdout. A failure that
   occurs only on a dirty/not-ready instance is environment or ordering evidence,
   not yet a product regression; rerun the same case on a clean ready instance
   before classifying it.

For a BVT-only edit, validate the case/result pair and its real execution; an
unrelated Go package run does not fill that gate. For production changes, BVT is
additional to the derived UT/build/vet matrix, not a substitute.

## 7. Required Handoff Record

Report:

- behavior-to-UT/BVT validation map, including explicit no-BVT rationale;
- reused versus new fixture/case decision and the isolation boundary;
- fixture/data/runtime cost and the injection/configuration choice;
- exact selected tests/cases and proof they were selected;
- normal/race/topology/repetition commands with exit status;
- any generated result review, clean-instance preconditions, and relevant logs;
- before/after duration when test performance or consolidation changed.
