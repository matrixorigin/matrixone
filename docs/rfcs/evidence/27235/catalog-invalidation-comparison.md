# Issue #27235 catalog invalidation measurement

Status: evidence-only; no production candidate selected

Measurement head: `ce1b3cc7b93d92f7ded09e820318b0608216bfdd`
Base: `5735684d69c9b171c4d7a5ebee3ebcc539ffc8cc`
Harness: `pkg/embed`, one Log, one TN, two CNs

## Integrity

`catalog-invalidation-report.json` is schema 3 with `integrity=complete`. The
report SHA is bound to the clean measurement checkout. Both CN fragments have
the same exact SHA and complete cleanup. The validator now requires a
per-scenario counter delta, stable terminal observation, no shadow overflow,
zero precise FP/FN, and self-consistent terminal histograms.

The workload completed eight scenarios:

- 128 binary and text prepared schema rebuild iterations across CN0/CN1 DDL;
- 128 RC table-cache reload iterations across CN0/CN1 DDL;
- 128 same-account unrelated-DDL prepared dependencies;
- no-change, TRUNCATE, RENAME, table drop/recreate, and database
  drop/recreate lifecycle checks against dependencies established before DDL.

Prepared rebuild terminals were recorded only after plan rebuild, result-column
metadata, prepared-state publication, cached-compile recreation, and final
statement setup completed. The aggregate histogram contains 259 terminal
outcomes: 256 successes and 3 errors, with no misses. RC reload has 128
successful terminal outcomes and no errors or misses. Errors are retained as
terminal evidence and are not silently counted as successful rebuilds.

## Differential result

Across both CN reports, all 87,851 decisions were stable and no shadow entry
overflowed:

| Oracle | Stable FN | Stable FP | Overflow |
| --- | ---: | ---: | --- |
| precise shadow | 0 | 0 | false |
| 4096-account bucket | 0 | 147 | false |

The 147 bucket false positives are measured mechanism evidence, not a
production budget. The bucket remains measurement-only until Catalog,
frontend, and RC-cache owners record an accepted rebuild/reload and
tail-latency budget in #27235. The precise result is not production approval:
its identity model, replay/GC behavior, retained-memory bound, and independent
no-profile benchmark still require owner review and a separate production Draft
PR.

## Benchmark evidence

Raw five-run output for history lengths 1/16/256/4096 and warmed-negative or
changed states is stored outside the repository under
`/Users/violet/bench/tpcc-mutex-next/results/27235/`.

The valid idle comparison observed no allocation change (`0 B/op`, `0
allocs/op`) and a directional latency difference. Five samples do not establish
a 95% confidence interval, so the prior `-10.16%` geometric-mean value is not
published as a performance improvement. A separate run during a competing
local build produced `+44.32%` and remains invalid contention evidence. The
precise warmed-negative path reports `0 B/op` and `0 allocs/op`. These are
microbenchmark observations only, not TPS or transaction-tail claims.

## Superseded evidence

The previous report from head `0279990ecb9c779e7b52b5d60666b0338b4445b8` is
preserved as
`catalog-invalidation-report-invalid-0279990.json` and explicitly marked
`integrity=invalidated`. It must not be combined with this report.

## Decision

The existing exact BTree remains the production behavior. No bucket or precise
production implementation is opened by this evidence commit. If owners accept
the precise identity and memory model, open a separate production Draft from
the then-current `main`, without attribution counters, shadow state, report
APIs, or the historical A+C `+0.7026%` combination result.
