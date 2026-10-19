# MatrixOne versus DuckDB TPC-H comparison

This standalone module implements the native CPU comparison in
[`docs/design/mo-duckdb-tpch-benchmark.md`](../../docs/design/mo-duckdb-tpch-benchmark.md)
and [#30569](https://github.com/matrixorigin/matrixone/issues/30569).
It does not change either engine. Its dependencies are isolated from MO's module.

Build with the Go version in this module and a C/C++ toolchain:

```sh
GOWORK=off go test -mod=readonly -count=1 ./...
GOWORK=off go build -mod=readonly -o tpch-compare .
```

The pinned official driver bundles DuckDB 1.5.6. For a symbolizable custom build,
use its supported `duckdb_use_static_lib` or `duckdb_use_lib` build tag and record
the exact native library identity. Keep the same optimized executable for timing
and profiling.

## Prepare immutable inputs

Supply an independently obtained, pinned TPC-H DBGEN/QGEN directory, including
`dists.dss`, `queries/1.sql` through `22.sql`, and SF1 `answers/q1.out` through
`q22.out`. Source code, generated data and reference answers are not vendored here.
Record the actual generator version separately from the workload specification.
TPC-H specification 3.0.1 and a DBGEN version string are different identities.

```sh
./tpch-compare prepare --work-dir /nvme/tpch/sf1 \
  --tools-dir /path/to/pinned/dbgen --scale 1 \
  --generator-revision COMMIT --generator-version VERSION
```

The destination must be new. Generate each scale once, then load its exact input
files into both engines. The manifest records the generator, distribution,
templates, raw QGEN output, reference files, and data hashes. Failed preparation
leaves an owned incomplete directory, never a valid manifest.

The reviewed syntax translations remove day-interval precision and attach
QGEN's separate row-limit directive. Q15 retains CREATE VIEW / SELECT / DROP VIEW
with private per-execution names. The canonical schema uses PARTSUPP
`(ps_partkey, ps_suppkey)` and LINEITEM `(l_orderkey, l_linenumber)` primary keys.

## Worker and correctness boundary

`worker --request request.json --response output/receipt.json` executes one
bounded operation. Its request specifies version 1, operation (`load`, `verify`,
`queries`), engine, dataset/output directories and a positive query deadline in
nanoseconds. Query requests also specify query numbers and repetitions. Output
directories and receipts must be new. Standalone worker receipts are diagnostic;
they do not by themselves establish isolated benchmark measurements.

Engine settings include `name` (`mo` or `duckdb`), `database`, `threads` and
`memory_bytes`. MO credentials come from a named `dsn_env`, never a result file.
MO loading creates a new `tpch_compare_` database on an explicitly selected owned
loopback deployment. DuckDB uses a persistent database file. Both native CSV
load paths explicitly enable parallel reading. Loading verifies
complete typed input contents, composite-key uniqueness and every input foreign
relationship. Ordered DBGEN key groups are checked and sorted within a bounded
buffer, allowing streaming validation without duplicating an entire table.
Database verification reads contiguous leading-key windows, preserving each
composite-key group. The initial unbounded lower range and final upper-tail
probe also reject rows outside the input domain. This checks every value while
avoiding a whole-table external sort during large-scale preparation.
Both loading and standalone verification check every input file against its
manifest hash before reading database contents.

Every query consumes through EOF, checks cursor errors, closes resources, and
captures typed rows. The timer includes conversion/capture and Q15's DDL. File
hashing is streamed; comparison runs outside the query clock. Capture is bounded
to 4 GiB per query and peer-group comparison to 64 MiB. Resource failure remains
a failure, rather than accepting partial results.

`compare --dataset DIR --left RECEIPT --right RECEIPT` verifies artifact hashes,
row counts, complete duplicate multiplicity, required ordering, and all values.
Only declared CHAR padding is normalized. Exact decimal operations stay exact.
Final AVG/division bounds come from qualified physical decimal scales plus a
`1e-12` relative floating-point allowance; missing scale/type coverage fails.
Qualification captures must respect the declared LIMIT. Unlimited diagnostic
receipts cannot be supplied as qualification results. To resolve a boundary tie,
run workers with `unlimited_diagnostic: true` for the affected limited queries
and repetitions on each unchanged database, then provide their receipts:

```sh
./tpch-compare compare --dataset DIR --left LEFT --right RIGHT \
  --left-unlimited LEFT_FULL --right-unlimited RIGHT_FULL
```

The two proof receipts must match each other in query selection and repetitions,
and match the qualification dataset and each engine's build. Proofs may cover
only the affected limited queries. Both unlimited results must agree before
the comparator checks the required preceding groups and legal boundary members
of each qualification capture. A missing or invalid proof leaves a differing
LIMIT result unresolved.

With `reference: true`, SF1 workers additionally check the pinned independent
answer files using TPC-H 3.0.1 clause 2.1.3.5: counts/singletons/quantity sums are
exact, monetary sums allow $100, and rounded AVG/ratios allow 1%. These reference
rules are distinct from the stricter full-precision cross-engine contract.

Keep generated datasets, databases and raw captures outside Git. Large benchmark
campaigns are explicit operations; ordinary tests use only tiny fixtures.
