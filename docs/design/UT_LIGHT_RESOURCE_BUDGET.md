# Light race-UT resource budget

Related issue: [#28419](https://github.com/matrixorigin/matrixone/issues/28419)

## Problem

The single-runner race suite uses `UT_PARALLEL=6` in CI. That value is also
passed to the light stage, where the Go command may compile/link and execute
several race-instrumented packages at the same time. Recent successful runs
showed multiple concurrent `link` processes, a cgroup peak close to the 16 GiB
limit, and substantial CPU throttling during the light stage.

The issue is not a missing test or a fixture admission failure: the complete
package partition is still required, and similar aggregate admission-wait
totals occurred in runs with materially different wall times. The resource
owner is the light-stage admission point in `optools/run_ut.sh`.

## Change

`UT_LIGHT_PARALLEL` is an independent ceiling for the light race stage. Its
default is `3`; it is capped by the existing `UT_PARALLEL` value. The ceiling
applies to both the normal foreground path and the existing opt-in
light/issues overlap path. The global package budget, native build budget,
embedded/heavy scheduling, race flags, package partition, test data, and
timeouts are unchanged.

This bounds the number of concurrent light-stage build/link/test programs
without changing which packages or tests execute. A caller can set
`UT_LIGHT_PARALLEL=6` to reproduce the former light-stage admission for a
controlled comparison or rollback.

## Prebuild experiment and measured result

The runner retains an explicit `UT_PREBUILD_EMBEDDED=1` cache-warming path.
During the exclusive issues package, up to two embedded packages are compiled
with `go test -c`; no prebuilt binary is executed. The later authoritative
embedded `go test` command still owns package discovery, `TestMain`, fixture
admission, test events, exit status, timeout handling, and report publication.

The default is `UT_PREBUILD_EMBEDDED=0`. In the same-runner comparison, the
prebuild took about 72 seconds, but the authoritative embedded stage changed
from 15m39s to 15m37s and the complete UT job changed from 63m47s to 64m47s.
The treatment also reached a 16 GiB cgroup peak with only 64 KiB of headroom.
The first observed peak was during the light stage, before prebuild started;
this does not prove that prebuild caused the peak, and reverting the default
does not by itself prove that memory headroom is restored. This is not evidence
of a critical-path gain, so the experiment remains opt-in. Its package and join
checkpoints are retained for a future controlled comparison. The light/issues
overlap remains disabled, and embedded execution remains capped at two package
workers until same-runner measurements prove otherwise.

## Acceptance and limits

The default of three is a resource-pressure treatment, not a performance
promise. Promotion is valid only if same-revision, same-runner baseline and
treatment runs show:

- the exact same package/test identity multiset and terminal results;
- lower light-stage and whole-run peak memory, with no new OOM or memory-event
  failure;
- lower CPU throttling/pressure and no material increase in total CPU work;
- lower end-to-end wall time. If the treatment only lowers resource use but
  increases wall time materially, this default must not be merged unchanged.

Historical run-to-run variance is too large to infer these results from one
fast run. The PR is therefore a controlled treatment until same-revision,
same-runner evidence is attached; it is not a completed performance claim.
Measurements must keep toolchain, cgroup limits, cache state, and workflow
revision visible. If wall time worsens materially while only memory improves,
the default should be reverted or retuned rather than presented as a
successful overall optimization.

## Explicit non-goals

This change does not add runners, shard the required suite, weaken race or
coverage gates, remove test-cache invalidation, change `GOMAXPROCS` or test
internal parallelism, enable the previously rejected light/issues overlap, or
repeat lifecycle/linker optimizations already merged in the related series.

## Follow-up: remove work before changing concurrency

The latest completed run, `35362550784` / job `105657722286`, took 66m33s
overall (64m19s for the UT step). The light-to-heavy critical span was 61m27s:
light 25m38s, HNSW 31s, issues 12m18s, embedded 16m18s, and the remaining
engine/heavy tail 6m42s. Its 13.33 GiB peak and zero cgroup OOM events do not
establish a wall-time improvement. All three recent measurements used light
parallelism three, so they are not a controlled three-versus-six comparison.
The 13m22s sum of admission waits overlaps active fixture work; removing that
sum from elapsed time would double-count the same runner interval.

This follow-up therefore changes two local test fixtures, not the scheduler:

- `TestIssue25408PreparedPaginationParameters` assigns each expression/protocol
  write an unindexed literal `case_id`. All 43 expressions, both protocols,
  read/write modes, parameter values, NULL/value assertions, and prepared
  statement lifetimes stay intact. Reading the case's row removes 86 unrelated
  whole-table reset transactions. At most 86 rows survive until the existing
  deferred database cleanup; an assertion failure still closes statements and
  the pinned connection before dropping the database. IDs come from the table
  index, not execution order, so filtered subtest selection remains valid.
- `TestPartition` builds its six-element BIT vector directly, as its neighboring
  cases already do. It no longer links process/service helpers just for one
  vector constructor. The values, vector type, NULL mutations, and assertions
  are unchanged; the vector is freed by its creating test on every exit.

These are focused test-maintenance changes, not a new fixture framework or
production refactor. No BVT is added because no production behavior changes;
the existing real two-CN SQL test remains the protocol validation boundary.

Local evidence (Go 1.26.4, Darwin/arm64, race, `matrixone_test`, native artifacts
built in this worktree; baseline `2f6dc33c3c`):

| Measurement | Before | After |
|---|---:|---:|
| Integer-domain subtest, two executions in one process | 10.11s / 10.91s | 2.42s / 2.39s |
| Complete pagination test, same two executions | 22.37s / 11.86s | 14.00s / 3.71s |
| Partition race binary, without DWARF | 66 MiB | 26 MiB |
| Partition build maximum RSS, one warm-dependency-cache sample | 954,548,224 bytes | 352,534,528 bytes |

The two pagination runs have identical 252-node test identity multisets per
execution, including all 172 expression/protocol/read-write cells. The second
execution reuses the shared cluster but recreates and cleans the database.
The binary-size reduction is structural; the single build RSS sample is not a
whole-job memory prediction. These local gains do not establish the requested
ten-minute CI improvement. A same-runner completed CI result is still needed
to measure the end-to-end gain, and the 25-minute light stage remains the
largest unresolved target. Broad helper extraction was rejected for now:
most consumers retain the heavy dependencies through other imports.
