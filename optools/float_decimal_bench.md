# DOUBLE to DECIMAL query measurements

`float_decimal_bench.py` measures stored DOUBLE columns through an explicit CAST
scan/aggregate and an implicit INSERT SELECT into DECIMAL. It requires Python 3
and PyMySQL, and a caller-owned, ready single-node test service. It never starts
or stops services. Use isolated service ports, configuration and data; verify
process ownership before stopping the service. Do not run against a shared or
production instance.

The runner creates a dedicated database (default `float_decimal_perf`), refuses
to use an existing database, and drops its database in `finally`. Connections
and cursors are closed on failure. A disconnect or forced termination can leave
the database behind: inspect the owned test instance before removing it. Each
query has a 120-second server/client timeout. A failed sample is saved and ends
the run; it is never replaced. Plans must retain the configured cohorts, SQL and
one of the three bounded row counts. Existing output files are refused. Set
`MYSQL_PWD` for credentials (default test credentials are dump/111).

Build comparison binaries with identical toolchain, compiler/native inputs and
build flags. For the #31133 follow-up, A is current main and C disables only the
conservative fast path added by #31279. C retains shortest-decimal rounding and
the exact integral Decimal128 path; it is not a historical parent binary. Use
C–A–A–C order and run the pilot on C.

For future A/B patches, a Go `-overlay` that replaces only
`pkg/container/types/decimal.go` with its baseline contents allows building A
without changing branches. Record source hashes, overlay, binary hashes, flags,
service configurations and host details outside the repo.

Use the baseline comparison binary alone for the pilot:

```sh
python3 optools/float_decimal_bench.py --port 26433 \
  --pilot "$TMPDIR/decimal-plan.json" --output "$TMPDIR/decimal-pilot.json"
```

For each cohort, the pilot selects the smallest of 1M, 4M and 16M rows whose
scan takes at least 200 ms. If none qualifies, it retains 16M and reports the
pilot timings; do not expand the cap. Freeze that plan before measuring the
other binary. The runner verifies stored DOUBLE bit patterns and the entire
grouped converted multiset against an independent Python Decimal half-up oracle.
SQL width 16 selects Decimal64; width 38 selects Decimal128, including the
scale-19/20/38 cohorts. Inputs include both signs and two magnitudes per cohort,
five fallback classes, ordinary and exact-integer controls, plus scale 19/20/38.
NULL and errors belong to the small correctness BVT rather than timing numeric
conversions.

For an A/B patch, start fresh, isolated instances in A–B–B–A order. Run this
once per instance, with a different output path and the same frozen plan:

```sh
python3 optools/float_decimal_bench.py --port 26433 \
  --plan "$TMPDIR/decimal-plan.json" --output "$TMPDIR/decimal-A1.json"
```

Each instance performs two warmups and five measured samples for each scan and
insert, giving ten measured samples per side. MIN/MAX consume the cast result
without overflowing a high-scale SUM. Inspect the recorded EXPLAIN to confirm
runtime conversion remains in the plan, especially for tiny values whose rounded
result is always zero. INSERT timing includes autocommit; destination TRUNCATE,
source generation, multiset verification and setup are outside timing. Plans,
source bit patterns, target metadata, checksums, preparation cost and all
samples are recorded. No performance threshold belongs in UT/BVT.

Compare medians, min/max and quartiles, inspect both instances for drift and
retain failures. Scalar/vector wins alone do not establish query wins. Scan and
write costs can hide conversion improvements; report unresolved differences and
remaining formatting cost. Current code retains formatting for tiny values,
ties, near ties, large scaled values and fractional scale >19.

For an in-process comparison, run `BenchmarkFloatDecimalConversions` in the
types package and `BenchmarkFloatDecimalVectors` in the function package, using
`.agents/skills/mo-dev/scripts/mo-cgo-test`. Use the same benchmark fixture on
both sources. The vector benchmark reports ns/row and includes result reset;
NULL is a separate control, and every output is rational-oracle checked outside
timing. Keep service measurements separate from Go build/test/benchmark jobs.

`current_source_paths` classifies inputs against the current conservative guard;
it is not runtime instrumentation. C disables that guard, so its nonintegral
values all use formatting. Stored bit checks and recorded EXPLAIN complement
this source-level classification.
