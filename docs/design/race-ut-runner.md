# Race UT execution and resource ownership

The current execution owners are the [Make entrypoint](../../Makefile),
[runner](../../optools/run_ut.sh), [tool helpers](../../optools/ut_tools.bash)
and [process/report helpers](../../optools/ut_process.bash). They preserve the
selected test inventory while bounding process lifetimes and resource admission.
The [CI caller](../../.github/workflows/entrypoint.yaml) uses one runner with
`ut_parallel: 8` and `ut_sharded: false`. Optional shard selection is a diagnostic
interface, not permission to introduce additional concurrent CI runners.

## Configuration and execution budgets

Direct script invocation, Make defaults and the CI profile are different:

| Configuration without further overrides | Direct script | Make defaults | Current Linux CI profile |
| --- | --- | --- | --- |
| `UT_PARALLEL` | 1 | 1 | 8 from the caller |
| `UT_ISSUES_BATCHES` | 1 | 8 | 8 |
| `UT_ISSUES_BATCH_PARALLEL` | 1 | 2 | 4 |
| `UT_EMBEDDED_BATCHES` | 1 | 4 | 4 |
| `UT_EMBEDDED_PACKAGE_PARALLEL` | 1 | 2 | 4 |
| `UT_PREBUILD_EMBEDDED` | 1 | 0 | 0 |

The Make formulas select four issues/embedded processes only for Linux with at
least eight requested global tasks; smaller budgets and other platforms retain
two in the eight-batch pool. One/four issues batches retain the serial default.
Changing the issues profile can also change the default embedded/prebuild policy.
Explicit overrides remain supported and validated by their existing owners.
`UT_PARALLEL` is not a universal cap on every independently configured pool.

Other controls retain separate responsibilities:

- Light tasks default to six, capped by `UT_PARALLEL`; concurrent light links
  default to three. These are task/link admission limits, not a memory cap.
- `UT_OVERLAP_LIGHT=0` keeps the rejected light/issues overlap opt-in.
  `UT_OVERLAP_PLAN=1` reuses released capacity in the existing heavy/plan wave.
- The engine phase uses up to six shards, capped by global and explicit heavy
  limits, before the subsequent heavy/plan wave. Plan process parallelism defaults
  to one; audited plan subtests have a separate ceiling of three.
- `UT_TIMEOUT` defaults to 15 minutes. `UT_HARD_TIMEOUT` defaults to `120m` for
  the whole runner lifecycle. A larger outer budget does not reset a package's
  execution deadline.

## Package and runnable-root selection

The runner resolves the existing complete Go package scope and exclusions.
Embedded owners are derived from transitive race-test dependencies on `pkg/embed`,
not a directory allowlist. Group precedence and the shard-stage map must form a
complete, disjoint partition, including special HNSW, engine and plan owners.
HNSW finishes its native-worker work before another race wave is admitted.

For batching, the compiled binary supplies the runnable inventory. Each selected
Test/Fuzz/Example root appears once; its subtests, fuzz seeds and internal stress
rounds remain with that root. Serial batches use contiguous partitioning;
parallel batches use round-robin partitioning. Unknown `TestMain` contracts and
unsupported Go flags take the ordinary execution path before roots execute.
SQL integration has an explicit cleanup-only `TestMain` audit; this is not an
inferred permission for arbitrary packages.

The package execution deadline is established after compilation and shared by
inventory and all its batches. Preparation fallback uses the remaining deadline,
not a fresh full timeout. Exhausted discovery or failed drainage cannot trigger
an unbounded fallback. Runtime failures never rerun completed roots.

## Embedded execution paths

With eligible prebuild enabled, compilation can overlap the exclusive issues
phase without starting another cluster. The runner later executes those exact
binaries in their package directories through its bounded `test2json` supervisor.
It validates published binaries, metadata and timeout configuration first.
Unavailable preparation falls back before any prebuilt test executes; runtime
failure does not rerun the complete scope.

The prebuilt package wave gives high-footprint packages exclusive admission;
newly discovered owners are conservatively high footprint until classified.
This package-wave policy is distinct from audited batching within one package.

Without successful prebuild, ordinary Go execution owns unaudited packages.
`UT_EMBEDDED_BATCHES=4` enables the audited package generations for `pkg/embed`,
DML, isolated issues and SQL integration. They reuse the same inventory,
deadline, bounded dispatcher and report protocol as issues batching. At a
four-process limit, `pkg/embed` uses eight generations; the other audited
packages use four. Execution scopes the two-core Go budget and cluster-admission
pool to its children; preparation and the caller retain their own budgets.

## Native preparation and temporary linking

Make owns its CGo prerequisite. Its invocation-local prepared indication is
accepted only after the existing native provenance check; direct invocation or
invalid provenance uses the authoritative preparation path. The CGo wrapper
smoke check remains separate. See [native artifact ownership](native_artifact_generation.md)
for generation identity, publication, accelerator and runtime-library contracts.

The Linux [link adapter](../../optools/ut_link_gate.sh) uses kernel `flock` slots
only for real linker invocations. Non-link tools and `-V=full` queries exec the
original tool unchanged, preserving Go's tool identity. The leased descriptor
survives exec and releases on linker exit, error or kill. The runner removes slot
files only after the owning groups drain. Waiting tools remain within Go's task
budget and the outer cancellation boundary.

Non-Linux hosts, missing `flock` or an existing external tool wrapper use the
conservative three-task fallback; an external wrapper is not silently replaced.
`UT_LINK_PARALLEL=0` explicitly disables the adapter. Invalid configuration or
unexpected locking/filesystem errors fail closed.

Temporary prebuilt race binaries use `-ldflags=-w`, without applying it to
production binaries or exporting it through unrelated `GOFLAGS`. Runtime stack
and race-report metadata remain available; rebuild without the flag for DWARF
variable inspection. This is not a guarantee for every external debugger or
evidence of a Go link-cache hit.

## Cancellation, completion and report transfer

Command, child and watchdog ownership is published before pending TERM is
replayed. Cancellation stops admission, signals each owned process group,
escalates within the existing bounds and joins it before releasing artifacts.
A watchdog expiry remains failure even if the test catches TERM and exits zero.

Prebuilt helpers publish explicit completion only after ordinary return or
drained cancellation. The parent reconciles that acknowledgment with reaped
status and group drainage. Missing/inconsistent completion or failed drainage
returns terminal status `125`, retains diagnostics and forbids later admission.
An uncatchably dead helper is not proof that its independent child groups have
been reclaimed. Live lease files and report ownership cannot be discarded on
that assumption.

Report writers join before consumption. The consumer selects one authoritative
representation: the complete ready report or recoverable shard files. It writes
to a same-directory temporary file and publishes atomically. Interrupted or
failed transfer keeps recoverable sources rather than publishing a prefix or
deleting the only copy. Ordinary test failures remain visible in exit status and
JSON; formatting and diagnostic tools cannot turn them into success.

## Fixture and supporting state owners

Cluster admission spans the complete service lifecycle, including failed-start
rollback and cleanup. Process-local borrowing does not authorize a second
process outside the configured lease pool. Failed release retains ownership;
live lock files are not unlinked. Different topology, restart, configuration and
global-hook contracts remain isolated even when their tests share dependencies.

The canonical SQL integration fixture owns serial scenario entry, bounded SQL
teardown and final `TestMain` destruction. Dirty fixtures are disposed before
reuse. DML's `withCNDraining` requires authoritative transition/restoration:
an ambiguous write invalidates the fixture instead of racing a compensating
write. Restoration uses a fresh context and runs on return, Goexit or panic.
The engine fixture cancels and joins its notifier before dependent cleanup.

HAKeeper lookup remains the authority for scheduling and assertion projections.
Scheduling omits display configuration while retaining store/runtime state and
LOG WAL recovery status; retained maps/items are detached. State-only assertions
and complete public/full getters keep their distinct contracts. There is no
snapshot cache, new Raft command or reduced public state schema.

CDC's [watermark updater](../../pkg/cdc/watermark_updater.go) owns publication;
the queue worker alone owns batch scratch. ACK updates published progress under
the updater mutex without mutating that scratch. A guarded checkpoint can be a
durable no-op after ownership changes, so fresh reads/claims publish the durable
tuple rather than maximize it against optimistic cached writes. Map identity,
reader counts and publication revisions guard outstanding reads. A revision
conflict permits one additional SELECT within the original deadline, without
repeating the claim UPDATE; retirement/identity mismatch does not reread.
Deletion blocks admission/publication. Last release removes only its matching
entry; SQL and fence callbacks execute outside the publication mutex. These are
existing product safeguards, not a second runner-owned progress store.

## Diagnostics and regression ownership

The heartbeat owns its finite diagnostic scan process group and incremental
reader state. Replacement/truncation resets offsets; incomplete final lines are
retried; corruption rebuilds from authoritative raw reports. Stop drains the scan
group before deleting state. Resource snapshots identify their cgroup scope;
missing counters do not change test outcomes. Admission completeness requires the
matching generation's acquire/release pair, not process disappearance alone.

Summed admission waits overlap holder work. Stage wall time, test-body time,
CPU, RSS/PSS, cgroup current/peak and memory occupancy answer different questions.
Memory fields overlap, summed RSS counts shared pages repeatedly, and cumulative
pressure counters need timestamp deltas. A canceled or truncated run is not a
complete performance control; rejected overlap measurements remain historical
evidence, not approval to change current defaults.

| Contract | Existing regression owners |
| --- | --- |
| Make/CI budgets, group precedence and stage scheduling | [Scheduler tests](../../optools/ut_schedule_test.go) |
| Prebuilt execution, high-footprint admission, fallback and deadlines | [Embedded scheduler tests](../../optools/ut_embedded_schedule_test.go) |
| Inventory, batch partition, remaining-budget fallback and helper ownership | [Inventory tests](../../optools/ut_inventory_test.go) |
| Forced cleanup, status 125 and stale process identity | [Cleanup tests](../../optools/ut_schedule_cleanup_test.go) |
| Atomic report transfer, native preparation and diagnostic summaries | [Tool tests](../../optools/ut_tools_test.go) and [native owner](../../optools/ut_native_test.go) |
| Report validity, partial failure and package completion | [Report tests](../../optools/ut_report_test.go) |
| Link leases, incremental diagnostics and heartbeat cancellation | [Kernel lease tests](../../optools/ut_link_gate_linux_test.go) and [reader tests](../../optools/ut_incremental_test.go) |
| Cluster-admission evidence | [Holder evidence tests](../../optools/ut_holder_test.go) |

This navigation does not claim a new test run, current CI success or a measured
speedup from documentation consolidation. [DML fixture costs](DML_TEST_COST_REDUCTION.md),
[startup allocation](UT_STARTUP_ALLOCATION.md) and [owner-local costs](ut-race-owner-costs.md)
retain their distinct contracts.

## Historical designs and measurements

The following immutable records preserve the complete experiments, coverage maps,
failure diagnostics and approval history. They include rejected treatments and
adverse resource results. Their timings and validation belong to their recorded
revisions and environments; private machine-local artifacts are not supplied by
these links.

| Historical record | Contents |
| --- | --- |
| [Execution and fixture optimization](https://github.com/matrixorigin/matrixone/blob/6cbe375b49696021d72778baa292a66287f080bd/docs/design/UT_EXECUTION_AND_FIXTURE_OPTIMIZATION.md) | Fixture compatibility, overlap revisions, diagnostics and rejected default promotion. |
| [Cost attribution and scheduling](https://github.com/matrixorigin/matrixone/blob/6cbe375b49696021d72778baa292a66287f080bd/docs/design/UT_COST_ATTRIBUTION_AND_SCHEDULING.md) | Native/readiness ownership and the superseded compile-only decision. |
| [Light link admission](https://github.com/matrixorigin/matrixone/blob/6cbe375b49696021d72778baa292a66287f080bd/docs/design/UT_LIGHT_LINK_ADMISSION.md) | Kernel leases, incremental reader design and local validation. |
| [Light resource experiments](https://github.com/matrixorigin/matrixone/blob/6cbe375b49696021d72778baa292a66287f080bd/docs/design/UT_LIGHT_RESOURCE_BUDGET.md) | Superseded task defaults, rejected prebuild experiment and fixture mappings. |
| [Temporary binary link cost](https://github.com/matrixorigin/matrixone/blob/6cbe375b49696021d72778baa292a66287f080bd/docs/design/RACE_UT_LINK_COST.md) | DWARF tradeoff, matched link measurements and failure caveat. |
| [Resource attribution](https://github.com/matrixorigin/matrixone/blob/6cbe375b49696021d72778baa292a66287f080bd/docs/design/UT_RESOURCE_ATTRIBUTION.md) | Resource timelines and CN-state contamination/restore boundaries. |
| [Critical-path revisions](https://github.com/matrixorigin/matrixone/blob/6cbe375b49696021d72778baa292a66287f080bd/docs/design/pr29760-race-ut-critical-path.md) | Complete batch-generation/coverage maps, CDC/HAKeeper corrections and resource acceptance evidence. |
