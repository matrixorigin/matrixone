# DML test cost reduction

Related issue: #28419.

## Evidence and scope

In race UT run 35170421812 (PR #29025, head 65240194),
`TestDataBranchDiffAsFile` took 70.24 seconds. Its longest child,
`update_apply_special_columns`, took 28.15 seconds and started about 32 seconds
after its parent. The parent started one goroutine per scenario and let all of
them race for a two-slot semaphore. Declaration order therefore did not control
admission, and a long scenario could leave a single busy slot at the end.

This follow-up changes test execution work and ordering only. It does not change
the runner, cluster admission, production code, timeouts or cluster concurrency.
The 18,193-row PICK regression, its two 8,192-key chunks and all existing oracles
remain unchanged. The scope is a focused test optimization, not a new scheduler
or a claim that the whole-suite timeout problem is solved.

## Contracts

- Admit the expensive independent diff scenarios first, with at most two active
  scenarios, just as before. Acquire a slot before launching the goroutine so
  pending scenarios cannot overtake each other. Release after `t.Run` returns,
  including a child assertion's `FailNow` path. Join every admitted scenario
  before closing sessions, dropping the database or releasing the shared fixture.
- Preserve every scenario name, SQL operation, assertion, session isolation and
  table namespace. The special-column group retains its nested subtest names.
- Reduce only the arbitrary seed volume in `composite_multi_column_mutations`.
  It is a mixed-type SQL-diff round trip, not a block/chunk-boundary test. Preserve
  updated, deleted and untouched base rows, as well as updated, deleted and
  untouched newly inserted rows. Keep repetition of the composite-key components.
  Add explicit preconditions for these six cells so future fixture reductions
  cannot silently remove one.
- Keep all physical-block, output-limit, CSV and PICK boundary fixtures intact.

## Validation

Compare baseline and candidate with the same native artifacts, Go version and
race mode, without concurrent cluster tests. Record `go test -json` parent and
child terminal elapsed values, not summed overlapping durations or build time.
Compare the complete executed subtest-name set and terminal results. Run the
owning DML package, race the changed fixture and check static analysis. Report
local timings separately from constrained Linux CI; admission wait removed from
another process is not an independently additive suite saving.

## Local validation record

Base: `60985da109` (including #29027); macOS/arm64, Go 1.26.4, 10 logical CPUs.
Both race binaries were built with `mo-cgo-test` from this worktree's verified
native generation. The baseline source was checked identical to the base before
building its binary, and the candidate patch was restored afterward. Timed
binaries run from `pkg/tests/dml` through `go tool test2json`, serially, without
concurrent builds or cluster tests during the repeated comparison.

- Full owning package, normal: PASS, 36.498s.
- Full owning package, race: PASS, 90.345s (validation, not a baseline comparison).
- Incremental golangci-lint including govet: PASS, zero issues.
- Initial focused race comparison: all 27 parent/child test identities passed in
  both versions; peak simultaneously active direct children remained two and
  all children terminated.
- Initial mixed-mutation child: 2.15s -> 1.06s. The parent: 31.08s -> 29.41s.
  Excluding its initial fixture setup, the scenario execution window was
  19.615s -> 16.815s. These measurements have different fixture-startup times;
  they are not a whole-CI speedup claim.

The unchanged PICK chunk-boundary case took 8.35s on this base in a separate
focused race run. Improvements already delivered by #29027 belong to that PR,
not this one.

The subsequent serial `-test.count=3` comparison completed successfully:

| Measurement (seconds) | Baseline | Candidate |
|---|---|---|
| Parent, repetition 1 (includes lazy cluster startup) | 32.73 | 30.46 |
| Parent, repetition 2 | 26.22 | 21.15 |
| Parent, repetition 3 | 26.75 | 21.09 |
| Sum of parent elapsed values | 85.70 | 72.70 |
| Mixed-mutation child, repetitions 1/2/3 | 2.09 / 1.92 / 3.68 | 1.32 / 1.68 / 1.20 |
| Focused test process total | 89.506 | 74.226 |

Both runs produced the same 81 parent/child PASS occurrences, with no failure or
skip. Parent elapsed decreased 15.2% across these three local repetitions. This
is preliminary same-host evidence for this fixture, not a Linux whole-suite
prediction; the unchanged global admission remains the resource boundary.
