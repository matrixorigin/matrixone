# CI test cost and behavior coverage

Implementation: [PR #32645](https://github.com/matrixorigin/matrixone/pull/32645).
Comparison base: `2573b2895bb73af99c95960c2f5595edde7b62b1`.
The existing PR at `7fddd3e9306249d84ea7b3fbfff7275557111712` is the
starting point for the additional repairs and measurements below.

The execution budget stays at one UT runner, eight CPUs and 16 GiB. Race,
recovery, retries, failure propagation and cleanup remain required. The target
is a complete successful race job below 3,600 seconds, including setup and
reporting. Individual case timings, summed concurrent workers and incremental
runs do not establish that target.

## Measured critical path

These are completed full race jobs, not the shorter incremental run
43299578189. The linked jobs retain their logs and diagnostic artifacts.

| Run | Job wall | Unit Testing step | Outside that step |
| --- | ---: | ---: | ---: |
| [43273730294](https://github.com/matrixorigin/matrixone/actions/runs/43273730294) | 4,387 s | 4,263 s | 124 s |
| [43297909922](https://github.com/matrixorigin/matrixone/actions/runs/43297909922) | 4,275 s | 4,157 s | 118 s |
| [43322779430, attempt 2](https://github.com/matrixorigin/matrixone/actions/runs/43322779430/attempts/2) | 4,088 s | 3,955 s | 133 s |
| [43339588330](https://github.com/matrixorigin/matrixone/actions/runs/43339588330) | 4,298 s | 4,162 s | 136 s |

The first three checkpoint traces show light 924–1,083 s, HNSW 34–61 s,
issues 495–561 s, embedded 1,680–1,761 s and engine 89–94 s. Heavy takes
592–593 s and overlaps plan's 527–587 s: those durations must not be added.
The latest starting-head trace has light 963 s, issues 518 s, embedded 1,769 s,
engine 98 s and heavy 630 s overlapping plan 606 s. Compile/link and admission
are included in these stages and are distinguished from test bodies.

The embedded wave includes ordinary owners (337–387 s), embed (432–469 s),
DML (125–150 s), isolated issues (244–261 s) and SQL integration (508–549 s).
In run 43322779430 the four SQL-integration workers took approximately
214, 329, 378 and 500 s. The longest worker, rather than the sum, blocks the
next stage. A grouping change requires complete package execution, inventory
and resource measurements before it can replace the existing grouping.

A local full-package comparison used the identical race binary and the same
four workers/two cores per worker in an eight-CPU, 16-GiB, no-swap cgroup.
Round-robin execution took 755.664 s; the existing contiguous algorithm took
778.470 s. All 113 roots, subtest run/terminal multisets and the single existing
private-spill prerequisite skip matched exactly. All four workers passed in
both arms. Preparation cost (281 s versus 66 s) is excluded from that comparison.
Contiguous grouping did not improve the critical path, so the existing policy
is retained. The local disk and host differ from CI; these are phase comparisons,
not a predicted whole-job time.

## Behavior mapping

### Same-account snapshot matrix

The removed explicit-account matrix and retained
`test/distributed/cases/snapshot/nonsys_create_snapshot_without_account.sql`
are identical after replacing only `FOR ACCOUNT acc01` in snapshot creation
with the implicit-current-account spelling and removing blank lines.
The normalized SQL SHA-256 is
`c03b592bb058464a7c9db49aab08aa01c95ce5fd96a6b95e3daa5594be42d71b`.
This is an exact fixture/statement reuse argument, not equivalence inferred
from the number of cases.

| Previous explicit section (base line) | Retained implicit scenario and oracle |
| --- | --- |
| Initial fixture (1) | Snapshot, mutation, account restore, row and count checks, teardown |
| Single database/table (26) | Nullable/decimal/string values, snapshot-time count and restored rows |
| Multiple tables (66) | Independent table mutations and restored contents |
| Foreign keys (115) | Parent/child data, constraints and catalog metadata |
| Account restore (195) | Primary-key/index/partition objects and account-level restored state |
| Empty/null restore (274) | Empty object set and successful account restoration |
| Restore newer then older (285) | Both snapshot generations and reverse chronological restoration |
| Restore older then newer (325) | The opposite restore order and resulting contents |
| Repeated restore (367) | Multiple transitions, intervening mutations and exact restored state |
| Database restore (467) | Database-level restoration across multiple tables |
| Database restore with dependent objects (518) | Dependent object/catalog and data restoration |
| Missing database (580) | The original rejection and surrounding state checks |
| Missing table (597) | The original table-level error and repeated restore checks |
| Cross restore of missing table (675) | Missing-source/target behavior and remaining data |
| Empty tables (724) | Empty table definitions, restoration and row counts |

`doCheckCreateSnapshotPriv` and `doCreateSnapshot` resolve these two spellings
to the same self-account identity before common persistence and restore work.
The replacement explicit file keeps a **differential** contract: the two
snapshots expose the same rows/counts; explicit table, database and account
restores each recover the original rows; the implicit snapshot remains readable.
The UNION count result has explicit ordering. This argument does not apply to
cross-account restoration or different privileges; those scenarios remain.

### View metadata and lifecycle

`TestViewCheckOptionCanonicalization` retains all 25 input expressions and
all source/current-definition/legacy-definition replay oracles in its first
generation. The retained input families are:

- Plain, backslash and multibyte literals; binary values with implicit/explicit
  aliases; computed and escaped column names; alias lists, CTE, UNION and scalar
  subquery references.
- Integer and string NAME_CONST, binary arguments, nested length, parenthesized
  and explicit-collation forms, derived references and quote/backslash/Unicode
  combinations.
- ASCII widths 65,534/65,535/65,536 and both multibyte boundary values.

Each still checks the expected value, public column name, data and empty-result
wire shape, charset/collation, current and legacy replay values, and catalog
column type. The second generation retains plain string, binary alias, integer
NAME_CONST, the 65,535-byte boundary and the largest multibyte NAME_CONST.
This factors generic same-schema recreation from expression-specific replay;
it does not remove a unique expression, byte/character boundary or metadata
oracle. Selected rows keep their original indices (0, 4, 11, 21 and 24), so both
generations use the same source and replay names with the same expression.
Filtering into a shorter slice before enumeration would accidentally renumber
the selected rows and weaken that lifecycle oracle; the loop filters in place.
Both generations verify catalog teardown before reuse. Check-option,
collation and error/cleanup checks outside this replay matrix are unchanged.

### Planner data and session proofs

`optimizer/joinorder.test` still creates six identical schemas containing
3M, 2M and 1M rows twice. Only the second data-generation pass becomes a copy
from the first three tables, before any intervening mutation. Every value,
row count, distribution, query, expected plan and result remains the same.
Real normal BVT comparison validates the plans/results; fewer input rows or
changed expected plans are not an acceptable optimization.

The float/session planner tests keep all 11 opaque, 2 table and 37 altered
session scenarios, their top-level and child names, all SQL/catalog inputs,
timezones/locales/precisions and expected results. Source immutability,
idempotence, forged metadata and work-budget rejection assertions are unchanged.
Only independent children use the existing parallel cap of three. A sequential
parent owns the two runtime protocol variables with the existing fixture's
`t.Cleanup` until every child and its cleanup finishes. The helper itself is
unchanged; children restore the same parent-owned values. The added
`TestFloatCatalogParentProtocolScope` verifies restoration of distinct previous
values and absent keys, including serial child execution and a completed
sibling before a delayed parallel child. The probe checks the protocol values
before the child helper can mask a missing parent scope. It passes normal,
race and 100 same-process race repetitions; removing just the parent scope
deterministically fails both previous-state variants.

Removing only the added parent fixture calls and `t.Parallel` calls produces
the identical original semantic Go token stream (comments and positions excluded):
`42fd7f935d79ad57a612b93b242002ca2197bc96a1841106107f7299f4f2927f`.
Parent `Elapsed` can exclude paused parallel children; wall comparisons use
start/end events and the complete stage, never that scalar alone.

### Exhaustive cache model and ordered observation

S3FIFO `TestMatchesModel` still executes the same four capacities/seeds,
20,000 operations each, all 400 membership checks after every operation,
operation-return/byte-queue assertions and value-ownership check. Only successful
boolean assertions avoid reflection/helper overhead; the first mismatch fails
with capacity, operation, key, actual and expected values. None of the
32 million membership observations is sampled or removed.

`TestDropAccountDuringAsyncIndexInit` retains its real two-CN initializer,
held REINDEX, competing DROP, required real rollback/retry and three zero-result
catalog checks on CN0. Coverage run 43339588330 observed successful DROP on
CN1 followed immediately by a stale account row on CN0. The test now observes
the dropping CN's nonempty commit frontier on CN0 before the same assertions,
using a bounded child of its original context. It does not poll away leftover
rows or relax account/table/live-ISCP cleanup. Release and executor/cluster
cleanup remain owned on assertion and wait failure.

## BVT selection and timing

The actual selector was executed against the real source tree at base and head,
with an argument-capturing tester, for both groups and both resource-argument
modes. All 1,890 scripts are present exactly once in both unions; neither
selection contains duplicates. The inventory SHA-256 is
`5c5e38e7468b0ea39519a54266ae83c2c644bc1dc8a7d5f276852a545f084b72`.
Exactly 122 git4data scripts move from group 0 to group 1. Each remaining ordered
stream and the moved directory's order are retained. The statement-info
producers and verifier remain together in their necessary lexical order;
unknown suites retain the existing deterministic assignment.

Observed PR jobs before the continuation had group-0 Compose case sum 2,321 s
and job wall 2,476 s, versus group-1 Pessimistic 1,663 s and 1,774 s. At the
starting head with the mode assignment reversed, group-1 Compose measured
2,045.883 s and 2,169 s, versus group-0 Pessimistic 1,491.356 s and 1,609 s.
These are actual observations, not a controlled regrouping speedup: mode,
revision, runner load and fixed overhead differ. No additional static move is
justified by optimizing one observation while ignoring the opposite assignment.

## Validation status and remaining target

The two snapshot scripts passed normal comparison twice on one owned instance:
556 implicit + 28 explicit statements, no ignored or failed assertions. Account,
snapshot and database teardown were separately verified before reuse and after
the second run. Actual CI also passed the unchanged planner case and all BVT
scripts. Focused cross-CN tests passed normal, race and set-coverage modes.
Configured incremental golangci, molint and vet checks passed for the final
changed Go packages. The corrected view case passed normal and race execution;
the selector passed race. The full planner owner passed all 4,951 roots under
race in the bounded heavy/plan diagnostic; its seven added protocol-test nodes
are the only root/subtest inventory differences from the 4,950-root baseline.
All original start/terminal/status multisets and existing skips matched.
The same assertions also remain unchanged by the source-level coverage proofs.
The prepared-cache comparison of the same candidate source used the existing
three-slot budget and a four-slot diagnostic. Resource wall was 331 versus
220 s, plan wall 196 versus 242 s, and the joined critical path 331 versus
242 s. Build/discovery (6 versus 37 s) is included, rather than credited as
case optimization. Both arms have exactly the same 42,412 run/terminal nodes,
8,737 passing roots and nine existing skipped roots. Four slots also completed
a preceding arm at 262 s. The measured phase saving is 89 s (26.9%), with
about 12% more phase CPU time inside the unchanged quota. Scope peaks were
7.27 and 5.56 GiB, with zero max/OOM/kill events; differing page-cache charging
prevents claiming the peak ratio as a memory saving. Engine mode and preparation
differ in the old first arm, so its 526 s is not an isolated policy comparison.
The [runner design](race-ut-runner.md) restricts the implicit four-slot fallback
to the measured Linux/eight-task/two-plan-process/two-shard/overlap profile,
after the explicit engine cap. The unoverridden engine therefore remains six.
Other profiles and explicit overrides keep their previous behavior.

The scheduler dispatch passed all 29 normal scenarios, the complete `optools`
owner passed 90 roots under race, and the changed dispatch root passed eight
same-process race repetitions. Any further optimization must retain complete
named scenario/root selection, normal/race/lifecycle oracles and measured
same-runner resource/critical-path evidence. The one-hour full-job target remains open until
a comparable complete successful job establishes it; pending CI, a model or a
fast subset is not success.
