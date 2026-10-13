# ClickBench Q19 minute extraction formatting

- Status: implementation, local vector benchmarks, focused tests, and full
  owning-package tests are complete; host-55 query-level validation is pending.
- Motivation: ClickBench Q19, [#27679](https://github.com/matrixorigin/matrixone/issues/27679).
- Parent investigation: [#28647](https://github.com/matrixorigin/matrixone/issues/28647).
- Delivery: design, implementation, tests, and benchmark evidence belong in
  one PR. Do not submit a docs-only proposal PR.

## Evidence and limits

The reduced ClickBench dataset on host `10.222.1.55` contains 10,550,791 rows.
On current main, one Q19 run took 2.186s, produced about 6.2 million groups,
and spilled. The historical issue reports 100.590s for MatrixOne and 1.010s
for DuckDB on different hardware; that ratio is not comparable. No
query-correlated Q19 CPU profile or repeated baseline is available in this
continuation, so neither the fraction of time spent in minute extraction nor a
Q19 end-to-end speedup is established.

Source inspection found a directly exercised per-row path: `ExtractFromDatetime`
and `ExtractFromTimestamp` obtain a constant unit once, but call
`extractFromDatetime("minute", d)` for each non-NULL row. The helper checks the
unit map, switches on the unit, and formats the minute with
`fmt.Sprintf("%02d", int(d.Minute()))`. This is a code-level optimization
hypothesis, not profile attribution.

Host-side Q19 A/B validation is deferred until SQL access works and temporary
space is safe. The latest read-only inventory showed `/tmp` about 93% full and
the prior SQL attempt was access-denied; do not launch another query or compile
on host 55 under those conditions.

## Proposed change

In `pkg/sql/plan/function/func_binary.go`, specialize the row loops in
`ExtractFromDatetime` and `ExtractFromTimestamp` after their existing constant
unit and NULL-unit handling. When the unit is exactly `minute`, append the
two-character minute directly for each non-NULL row through a shared formatter:
for values in `[0, 59]`, slice an immutable constant containing `00` through
`59`; for values outside that range, preserve the exact existing
`fmt.Sprintf("%02d", minute)` behavior. Return after the specialized loop.

Keep all other units, the generic helper, and the VARCHAR extraction path
unchanged. The DATETIME loop must retain its current NULL handling and VARCHAR
result behavior. The TIMESTAMP loop must continue calling `ToDatetime(zone)`
for each non-NULL row before extracting the minute; timestamp offsets depend on
the instant, so do not replace this with raw timestamp arithmetic or a cached
offset. Negative raw DATETIME values can produce negative minutes, so the
lower-bound check and formatting fallback are required. Zero DATETIME
continues to format as `"00"`.

The patch is intentionally limited to minute extraction. It does not change
grouping keys to integers, alter ClickBench SQL, change timestamp semantics,
add query-specific branches, or modify spill, shuffle, hash-map, or Top-N
execution. It makes no claim to resolve Q19's overall performance gap.

## Tests and measurements

`func_extract_minute_test.go` covers every valid minute (`00` through `59`),
both zero DATETIME representations, negative/raw DATETIME fallback values,
constant unit and constant temporal inputs, mixed NULLs, NULL unit, and an empty
batch. It also directly checks formatter fallback values outside `[0, 59]`.
The TIMESTAMP vector test uses independent expected values in a `+05:45` zone
across an hour/day rollover, plus zero timestamp and NULL rows. Both fast loops
continue returning `AppendBytes` errors immediately.

`BenchmarkExtractMinuteVector` benchmarks both actual vector-function entry
points over 8,192 varying rows. Five 500ms samples before and after on the same
local AMD Ryzen 9 7900X with Go 1.26.4 produced these medians:

| Entry point | Baseline | Fast path | Change |
| --- | ---: | ---: | ---: |
| DATETIME | 586,082 ns/op; 16,560 B/op; 8,194 allocs/op | 251,537 ns/op; 176 B/op; 2 allocs/op | 57.1% lower time; 99.98% fewer allocations |
| TIMESTAMP | 622,190 ns/op; 16,560 B/op; 8,194 allocs/op | 281,078 ns/op; 176 B/op; 2 allocs/op | 54.8% lower time; 99.98% fewer allocations |

The benchmark retains and verifies the last output row. These repeated local
vector results support the function-level optimization; they do not establish
a SQL-level benefit. The focused tests and full owning function package pass.
The latest full package run completed in 12.842s with
`mo-cgo-test -count=1 -timeout=600s ./pkg/sql/plan/function`.

When host 55 is safe to use, run at least three warm Q19 baseline and patched
queries against the identical binary configuration and reduced dataset,
checking exact results and service health. Report the measured change without
extrapolating to the historical 100m-row result. If this cannot be run before
PR creation, explicitly leave end-to-end validation pending and make no query
speedup claim; do not represent the microbenchmark as Q19 acceptance evidence.

## PR scope

The single PR contains this design note, the minute fast path, focused tests,
and the local benchmark/results. It references #27679 as motivation but does
not close it or claim to solve the overall gap. Exclude the unrelated
parallel-spill proposal. Require final GPT-6 medium review of the exact code
and document diff before pushing the feature branch or opening the PR.
