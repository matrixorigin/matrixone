# Issue #27235 catalog invalidation measurement

Status: evidence-only; no production candidate selected

Measurement head: `8c147612cb0723e1cd90638aa0ab21503a4ab1a2`
Base: `5735684d69c9b171c4d7a5ebee3ebcc539ffc8cc`
Harness: `pkg/embed`, one Log, one TN, two CNs

## Integrity

`catalog-invalidation-report.json` has `schema_version=2` and
`integrity=complete`. Both CN reports have attribution enabled and the same
measurement SHA. The workload completed all planned scenarios with 128 stable
terminal samples each:

- binary and text prepared statements across CN0/CN1 DDL;
- RC table-cache reload across CN0/CN1 DDL;
- same-account unrelated DDL opportunities;
- no-change, TRUNCATE, RENAME, table drop/recreate, and database
  drop/recreate lifecycle scenarios.

The aggregate latency counters contain 255 successful prepared-plan rebuilds
and 128 successful RC table-cache reloads. There are no terminal errors or
misses in either histogram.

## Differential result

Across both CN reports, stable checks had zero inconclusive observations and:

| Oracle | Stable FN | Stable FP | Overflow |
| --- | ---: | ---: | --- |
| precise shadow | 0 | 0 | false |
| 4096-account bucket | 0 | 6 | false |

The bucket false positives are measured mechanism evidence, not a production
budget. The bucket remains measurement-only until Catalog, frontend, and
RC-cache owners record an accepted rebuild/reload and tail-latency budget in
#27235. The precise result is not a production approval: its identity model,
replay/GC behavior, retained-memory bound, and independent no-profile
benchmark still require owner review and a separate production Draft PR.

## Benchmark evidence

Raw five-run output for history lengths 1/16/256/4096 and warmed-negative or
changed states is stored outside the repository under
`/Users/violet/bench/tpcc-mutex-next/results/27235/`.

The attribution-disabled wrapper versus exact warmed-negative comparison used
five runs per history after the competing local build had finished.
`benchstat` reports a -10.16% geometric-mean latency change, 0.00% bytes/op
change, and 0.00% allocs/op change. A separate run during a competing local
build produced +44.32% and is retained as invalid contention evidence, not
used for the gate. The precise warmed-negative path reports `0 B/op` and
`0 allocs/op`. These are microbenchmark results only and are not TPS or
transaction-tail claims.

## Decision

The existing exact BTree remains the production behavior. No bucket or precise
production implementation is opened by this evidence commit. If owners accept
the precise identity and memory model, open a separate production Draft from
the then-current `main`, without attribution counters, shadow state, report
APIs, or the historical A+C `+0.7026%` combination result.
