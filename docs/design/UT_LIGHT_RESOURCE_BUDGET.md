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
