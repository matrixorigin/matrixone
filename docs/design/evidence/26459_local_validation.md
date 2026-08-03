# #26459 local validation evidence

This file records evidence for the single production path described in
`../allocation_accounted_memory_admission_impl.md`. Removed implementations
are not retained as validation dimensions.

## Static closure checks

- no production allocation-account enable switch;
- no HashBuild logical-size memory reservation token;
- every join/HashBuild expression-owned MPool vector is constructed with the
  attempt account, while opaque library Go heap remains an explicit boundary;
- SpillEngine construction rejects a missing or closed budget generation;
- runtime scan/load clones are attached to the current attempt before worker
  `Prepare`;
- terminal lifecycle validates zero bytes and zero live metadata;
- memory, spill disk, and spill FD admission errors have distinct components.

## Required local test matrix

All Go tests use the repository CGo wrapper so `usearch` headers, libraries,
link flags, and runtime paths match the MatrixOne build contract.

```text
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=240s \
  ./pkg/common/mpool ./pkg/common/bitmap ./pkg/common/hashmap/... \
  ./pkg/container/vector ./pkg/container/batch ./pkg/vm/message \
  ./pkg/vm/process ./pkg/sql/util ./pkg/sql/colexec \
  ./pkg/sql/colexec/hashbuild \
  ./pkg/sql/colexec/hashjoin ./pkg/sql/colexec/dedupjoin \
  ./pkg/sql/colexec/rightdedupjoin ./pkg/sql/colexec/loopjoin \
  ./pkg/sql/colexec/product ./pkg/sql/colexec/productl2 \
  ./pkg/sql/colexec/spillutil ./pkg/sql/compile
```

Selected race coverage:

```text
.agents/skills/mo-dev/scripts/mo-cgo-test -race -count=1 -timeout=300s \
  ./pkg/common/mpool ./pkg/container/vector ./pkg/container/pSpool \
  ./pkg/sql/util ./pkg/sql/colexec ./pkg/sql/colexec/hashbuild \
  ./pkg/sql/colexec/hashjoin ./pkg/sql/colexec/loopjoin \
  ./pkg/sql/colexec/dedupjoin ./pkg/sql/colexec/rightdedupjoin \
  ./pkg/sql/colexec/productl2 \
  ./pkg/sql/colexec/spillutil ./pkg/sql/colexec/sample ./pkg/sql/compile
```

Static checks:

```text
go vet <modified packages>
go build <modified packages>
```

## Fresh local result

The final semantic edit was followed by a clean local run on 2026-08-01.

- the complete package matrix above passed, with Sample, pSpool, compare,
  hashtable, nulls, shuffle, SQL util, vector-index CPU packages, and disttae
  added to the command;
- `-race -p=2 -count=1` passed for mpool, hashjoin, spillutil, Sample, and
  compile;
- `go vet -mod=readonly` passed for every modified production package;
- `go build -mod=readonly` passed for every modified production package;
- `git diff --check` and `gofmt` were clean;
- allocation and performance results are recorded in
  `26459_allocation_accounting_bench.txt`.

No partial or still-running session is counted as a pass.

After the distributed q7 failure exposed the shared remote-MessageBoard
boundary, the final remote-lifecycle amendment was validated separately:

- the complete `pkg/sql/compile` suite passed in normal and race modes;
- `pkg/pb/pipeline`, `pkg/vm/message`, and `pkg/vm/process` passed;
- protobuf regeneration was clean and reproducible;
- vet passed for all four affected packages;
- an independent lifecycle/concurrency review converged with no blocker or
  major finding after its pending-domain cardinality and mixed-version concerns
  were resolved in code or by existing scheduling evidence.

## Behavioral coverage

The local suite covers:

- exact physical allocation/release and capacity rollback;
- owner/site mismatch and sealed-generation terminal errors;
- prepared and retry generation reuse;
- runtime parallel clone attachment;
- JoinMap and spill payload move-only ownership;
- cancellation-safe bitmap mailbox seal/drain;
- spill disk/FD admission and release;
- recursive spill row/schema/file validation;
- minimum-unit and monotonic pressure termination;
- optional runtime-filter degradation;
- nested/selected expression result accounting, capacity rejection, transfer,
  and terminal release;
- HashJoin, LoopJoin, DedupJoin multi-batch finalize, and RightDedupJoin result
  accounting, batch-local reuse, capacity rejection, and prepared
  `Reset -> ClearAllocationAccount` terminal release;
- grouping-aware copy, hash, equality, and ordering;
- late and alternating Sample grouping domains across row, percent, and merge
  modes;
- Product cleanup and account terminal zero;
- accounted JoinMap release after an unaccounted ProductL2 consumer frees it.
- remote scope-graph fragment counting across nested CN execution addresses;
- ProcessInfo topology-map and execution-ID wire round trips, with distinct
  MessageBoard generations across retries;
- remote statement-group board drain after a producer finishes before a later
  sibling registers, including accounted queued-message destruction, exact
  account terminal zero, and aggregate MPool terminal sampling;
- incomplete remote dispatch expiry releases staged account and MPool domains,
  while an unresolved pending terminal marker makes the coordinator summary
  explicitly partial; this includes the case where another registered fragment
  is still active when the old board generation closes;
- fragment failure aborts an incomplete group immediately, and a legacy remote
  plan containing an accounted owner is rejected instead of running without an
  account;
- counted pending/completed group markers resolve independent of
  terminal-response order; a four-fragment lost-final-response case preserves
  all three suppressed reported domains plus the directly missing domain.

## Performance evidence

Performance validation is local and allocation-focused. Benchmarks record
`ns/op`, `B/op`, and `allocs/op` for account acquire/release, vector growth,
hash-map build/lookup, and spill scatter. The acceptance rule is no
new per-row or per-allocation Go object in steady state and no material
regression outside measurement noise.

The local measurements are complemented by the distributed validation below.

## Distributed workload evidence

The final semantic head before the two review counterexample fixes,
`d13b9103c8`, completed the TPCH 100G and 1T TKE run
[`30758186183`](https://github.com/matrixorigin/mo-auto-test/actions/runs/30758186183).
The workflow built that commit, loaded the native fixtures in 11 seconds and
49 seconds, and compared every Q1-Q22 result with its golden result. No query
failed and the run reported no OOM or budget-admission error.

The measured query-only totals were:

| Workload | Candidate turns | Candidate average | Recent main average | Delta |
| --- | --- | ---: | ---: | ---: |
| TPCH 100G | 97.739 / 95.005 / 94.982 / 97.469 s | 96.298 s | 98.252 s | -1.99% |
| TPCH 1T | 1045.573 / 1039.371 s | 1042.472 s | 1027.439 s | +1.46% |

The cited main result is job
[`91396824792`](https://github.com/matrixorigin/mo-nightly-regression/actions/runs/30708854656/job/91396824792):
100G turns were 103.550 / 95.127 / 97.321 / 97.010 seconds and 1T turns
were 1027.322 / 1027.557 seconds. These are adjacent runs of the same TKE
benchmark shape, not a simultaneous same-base A/B; the deltas establish that
the stabilized candidate is within normal workload variance, not a stronger
causal performance claim. Compared with the earlier regressed candidate run
`30738374292` (135.093 seconds for 100G and 1591.049 seconds for 1T), this head
recovered 28.7% and 34.5% respectively.

The current review fixes after `d13b9103c8` are allocation-boundary and
late-RPC-lifetime corrections. They add no per-row work: existing-buffer grow
now passes the logical requirement to the allocator's single capacity policy,
and aborted remote generations retain only a key and timer for the maximum
possible RPC lifetime. Their focused and package validation is recorded in the
PR review response after the final commit.

## Incident acceptance matrix

This matrix separates durable mechanism regressions from workload executions;
one is not presented as a substitute for the other.

| Incident | Durable regression retained on this branch | Workload evidence | Current-head gap |
| --- | --- | --- | --- |
| #26174 | HashBuild build/hashmap/spill regressions introduced by #26178, plus exact physical batch/vector allocation boundaries in this PR | #26178 TKE BVT: all three 3,840,001-row fulltext inserts succeeded with zero HashBuild rejection | full fulltext workload has not been rerun at the final head |
| #26192 | exact accounted runtime-filter payload, one-byte-short PASS degradation, varlena/null coverage, and spill decode/reuse lifecycle tests | historical LOAD failure shape is covered by #26231/#26318; the current TPCH fixture LOAD path succeeds | the original `ca_comprehensive_dataset` workload has not been rerun at the final head |
| #26413 | segmented `CopyIntoBatches` and accounted hash-map growth/rollback regressions, including large external-batch shapes | #26438 verified the real Parquet self-join with both expected 50,000-row results | the Hive fixture has not been rerun at the final head |
| #26454 | `TestIssue26454ExpressionKeyBuildUsesActualCapacity` exercises the CONCAT/CAST and CASE key shapes under a 16 MiB physical account and validates terminal zero | the exact jinpan SQL/data is not available in this repository | full jinpan workload remains external evidence |
| #25782 | `TestShuffleHashBuildAccountedSpillLifecycle`, `TestHashTableAccountedHighCardinalityResizeReturnsToZero`, broadcast error propagation, recursive spill, and terminal-zero tests | the two-CN 132,096-row harness at `f5cc97efe7` returned the exact count with positive spill and zero OOM; current-head TPCH 1T also completed without OOM/query failure | the private original high-cardinality SQL harness has not been rerun at the final head |

Accordingly, the current TKE TPCH acceptance is complete, while the unavailable
external-data workloads and the original private #25782 harness remain explicit
follow-up evidence rather than being silently marked complete.
