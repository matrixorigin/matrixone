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
relationship. Catalog checks reject changed column domains, nullability, column
order, or primary-key order before normalizing values. DuckDB normalizes declared
CHAR(n) and VARCHAR(n) to its native VARCHAR domain. Ordered DBGEN key groups are
checked and sorted within a bounded
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
For VARCHAR fields, hash-verified input records recover the boundary between
data spaces and fixed-width answer-file padding. The published values are still
checked; added or removed data spaces and ambiguous input mappings fail.

Keep generated datasets, databases and raw captures outside Git. Large benchmark
campaigns are explicit operations; ordinary tests use only tiny fixtures.

Campaign mode `warm` runs the primary SF1/SF10/SF100 latency comparison: ten
balanced paired suites per scale, with the same budgets, warmup, and correctness
checks as `full`. It prepares one qualified persistent image per engine/scale;
the three-replicate loading experiment, scaling, cache, and profiling studies
remain separate parts of `full`.

## Controlled measurement

Start from `campaign.example.json`, replace paths and build identities, and
reserve an exclusive host window. It defines the complete matrix from the
methodology. `sf1-smoke` mode requires only SF1 and runs one paired suite; its
report is explicitly separate from the full campaign.

```sh
./tpch-compare preflight --config /nvme/tpch/campaign.json
systemd-run --user --scope --unit=tpch-compare-prepare -p Delegate=yes \
  ./tpch-compare run --config /nvme/tpch/campaign.json --phase prepare
systemd-run --user --scope --unit=tpch-compare-measure -p Delegate=yes \
  ./tpch-compare run --config /nvme/tpch/campaign.json --phase measure --id baseline
./tpch-compare report --config /nvme/tpch/campaign.json \
  --run-dir /nvme/tpch/comparison/runs/baseline --output /nvme/tpch/report
```

The runner manages private MO ports and deployment directories. One delegated
application cgroup contains every service and query worker. Its total memory and
CPU set are enforced before exec, including child threads. Swap is disabled for
that application. Controller work uses the other CCD. Source data and database
cache ownership are controlled without dropping the host's entire cache.

Preparation loads fresh databases, reopens and verifies them, checks independent
NULL/decimal/date/string fixtures, and qualifies all 22 query outputs. Full mode
uses three loads per scale/engine and retains the first qualified database.
Later load replicas are discarded only after their owned processes have joined;
all receipts remain. The pilot freezes symmetric query deadlines, throughput
wave lengths and the balanced schedule before scored runs.

Workers report progress through bounded pipes. Parent deadlines and cgroup
termination can stop a blocked driver independently. CPU/memory/I/O accounting
is gated around the measured queries, after connection setup and warmup. Eight
concurrent clients share one DuckDB instance and the original application budget.
A sampled disk guard checks allocated space and reserve every second; its disk
measurements are identified as observations rather than an exact filesystem
quota or unsampled peak. Runtime/cancellation errors and partial artifacts remain
failures. Each measurement invocation uses a new run identifier and preserves
earlier attempts.

Reports contain Markdown, query CSV and whole-window resource CSV. Bootstrap
intervals resample paired blocks/waves, preserving dependencies among calls in a
concurrent wave. Missing or failed blocks invalidate the cell's score. Suite/wave
medians, sums of query medians and geometric mean speedups have separate labels.
Whole-window CPU or memory is never assigned to an arbitrary query.
