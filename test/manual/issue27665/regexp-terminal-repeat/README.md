# Terminal regexp loop: reproducible performance acceptance

Related: [PR #31079](https://github.com/matrixorigin/matrixone/pull/31079), partial improvement for [#27665](https://github.com/matrixorigin/matrixone/issues/27665).

These are manual experiments, not unit-test fixtures. The 2026-10-25 Linux projection was generated locally; it is not the earlier author's unavailable macOS projection. `evidence.json` retains source/binary/data identities, raw samples, query results, plans, validation commands and exclusions. Do not infer performance from the private benchmarks alone.

## Dataset and tools

Use a fresh local directory with at least 10 GB available. The deterministic generator reads only four columns from dispersed row groups in the official [ClickBench Parquet source](https://datasets.clickhouse.com/hits_compatible/hits.parquet). The manifest records the source ETag/length, exact groups and quotas, schema, 10,000,000 output rows and SHA256. The source has signed int64 UserID. The strings retain the original Unicode content. Pin generator dependencies to reproduce the compressed file identity.

```sh
export BENCH_ROOT=/absolute/path/to/fresh/benchmark
export BENCH_OUTPUT="$BENCH_ROOT/results"
mkdir -p "$BENCH_OUTPUT"
python3 -m venv "$BENCH_ROOT/venv"
"$BENCH_ROOT/venv/bin/pip" install pyarrow==21.0.0 fsspec==2025.9.0 aiohttp==3.12.15 pymysql==1.1.2
"$BENCH_ROOT/venv/bin/python" generate-data.py
```

Build DuckDB from source, not a Python wheel:

```sh
git clone https://github.com/duckdb/duckdb.git "$BENCH_ROOT/duckdb-src"
git -C "$BENCH_ROOT/duckdb-src" checkout cc9648d736fd7e4fb2fc813cba6f6d34932c3386
cmake -S "$BENCH_ROOT/duckdb-src" -B "$BENCH_ROOT/duckdb-src/build/release" \
  -DCMAKE_BUILD_TYPE=Release -DBUILD_UNITTESTS=OFF -DBUILD_SHELL=ON \
  '-DBUILD_EXTENSIONS=parquet;json' -DBUILD_JSON_EXTENSION=ON
cmake --build "$BENCH_ROOT/duckdb-src/build/release" -j4
export DUCKDB_LIBRARY="$BENCH_ROOT/duckdb-src/build/release/src/libduckdb.so"
```

Use MatrixOne Go 1.27.1 and the repository's verified native artifacts. Follow the repository CGo provenance checks before `make build-with-prebuilt-native`. Build candidate production code at `68c236f0fd320ba28e9038abaa8110faa960f707`. Build the baseline from the same checkout with a Go overlay replacing `pkg/sql/plan/function/internal/regexp/{regexp,backtrack}.go` by their exact contents at `83fa4d7d74b014286b60adfd2f9857b4d43d978e`. Those are the only production Go files changed by the PR. This keeps current benchmark sources identical between variants. For the original pre-repair control, replace backtrack.go by `23d9ca89f2341465a673a013624ea17d5363bb84`; regexp.go is unchanged by the repair.

```sh
export GOTOOLCHAIN=go1.27.1 GOMAXPROCS=4
make build-with-prebuilt-native
cp mo-service "$BENCH_ROOT/mo-service-candidate"
# baseline-overlay.json: {"Replace": {"/absolute/repo/path.go": "/absolute/snapshot/path.go", ...}}
GOFLAGS=-overlay=/absolute/path/to/baseline-overlay.json make build-with-prebuilt-native
cp mo-service "$BENCH_ROOT/mo-service-baseline"
export MO_LIBRARY_PATH=/absolute/repo/lib:/absolute/repo/cgo:/absolute/repo/thirdparties/install/lib
```

Both recorded services carry the same checkout commit metadata because the baseline uses an overlay. Treat the recorded overlay/file hashes, binary hashes and build commands as the source identity; the service version alone cannot identify that baseline.

## Public SQL comparison

Copy the three supplied config files to `$BENCH_ROOT/cluster`, create launch.toml with absolute logservices/tnservices/cnservices config paths, and link that directory's `dict` to the repository's staged dict directory. The fixed ports must be unused. The directory must contain no prior mo-data on the first run. Use the same data directory for all four phases; the harness creates and loads the table once. Only the harness's own process group is stopped. It waits for SQL readiness and ISCP startup at each restart.

```sh
mkdir -p "$BENCH_ROOT/cluster"
cp config/*.toml "$BENCH_ROOT/cluster/"
# launch.toml: logservices = ["/absolute/.../log.toml"]
#             tnservices = ["/absolute/.../tn.toml"]
#             cnservices = ["/absolute/.../cn.toml"]
ln -s /absolute/repo/dict "$BENCH_ROOT/cluster/dict"
OMP_NUM_THREADS=4 "$BENCH_ROOT/venv/bin/python" public-benchmark.py duckdb
OMP_NUM_THREADS=4 "$BENCH_ROOT/venv/bin/python" public-benchmark.py mo
```

Wait until build/static-analysis processes finish before timing. MatrixOne order is baseline/candidate/candidate/baseline, with two warmups then five measured executions per query per phase. GOMAXPROCS/max_dop and DuckDB threads are four (the MO optimizer may choose fewer workers per operator). Each MO connection reads back max_dop and 4 GiB agg/join/sort spill thresholds. GOMEMLIMIT is 3 GiB; this soft Go heap target is distinct from operator spill thresholds. DuckDB uses a 4 GiB memory limit. Timings include execution and fetching the small result through persistent client connections, and exclude process/database startup and dataset load. This is a shared Linux host, not a controlled hardware performance lab.

Q5/Q6 are distinct-count controls; Q21 is the original LIKE/grouping control. Its c-only LIMIT can select different valid ties, so the harness separately compares `ORDER BY c DESC, SearchPhrase ASC` results. Q29 uses the same anchored regexp with a terminal greedy dot loop. MatrixOne replacement is `$1`, DuckDB replacement is `\1`. MatrixOne LENGTH counts bytes; DuckDB uses `octet_length(encode(Referer))` to preserve that contract for this Unicode dataset. Compare grouping keys/counts/minimum strings exactly and AVG to the four decimal places actually returned by MatrixOne. Preserve unnormalized raw results as well. Do not compare these measurements with DuckDB's character-length variant.

All four MO queries in all four phases have complete EXPLAIN ANALYZE VERBOSE output. `pkg/sql/plan/explain/explain_node.go` emits SpillRows/SpillSize only when AnalyzeInfo.SpillSize > 0; absent `SpillSize=` in a complete actual plan proves zero reported operator spill. DuckDB's pinned profiler uses `system.peak_temp_dir_size` and `system.total_bytes_spilled`, both zero. These representative profiling executions are separate from timing; the harness does not profile every measured sample. Q5/Q6/Q29 result parity must hold across all five samples and both MO variants as well as the reference; Q21 uses the separate deterministic query because its timed query permits ties; parity is not inferred from elapsed times.

## Private and vector controls

Compile private-package test binaries using the same test source and the overlays above. Set MICRO_BIN_ROOT to a directory containing baseline.test, original.test and candidate.test; adjust CPU 12 in the script if unavailable. `micro-benchmark.py` predeclares four B/O/C/C/O/B cycles, eight samples per variant, 200 ms each, GOMAXPROCS=1 and one pinned CPU. It covers successful/short/dotall loops, prefix rejection in both eligible/ineligible patterns and both string/byte entrypoints, capture/lazy/multiline/nonterminal/mixed-program exclusions, early LF, and failed-start scaling.

Compile `pkg/sql/plan/function` tests with `mo-cgo-test -c` and identical current tests plus baseline/candidate overlays. The vector benchmark `BenchmarkRegexpTerminalPrefixRejection` validates 256 zero results before measuring real REGEXP_INSTR evaluation. Use interleaved variants on the same CPU. A repeatable unrelated-path regression blocks this change even if Q29 improves.

## Recorded Linux result (2026-10-25)

Median seconds, with ten measured samples per MatrixOne variant (two phases × five) and five DuckDB samples:

| Query | Frozen main production | Candidate | Change | Source-built DuckDB |
|---|---:|---:|---:|---:|
| Q5 | 0.371544 | 0.370201 | -0.36% | 0.046255 |
| Q6 | 0.307182 | 0.306432 | -0.24% | 0.044363 |
| Q21 | 0.376070 | 0.374761 | -0.35% | 0.097756 |
| Q29 | 5.505865 | 4.541981 | -17.51% | 1.287073 |

Q29 remains 3.53× the DuckDB time, down from 4.28×; this is a partial improvement, not completion of #27665. Sixteen public SQL cases meet explicit value/error expectations in all four phases, including binary invalid UTF-8/NUL, mixed-charset 3995 rejection, NULL, line modes and offsets. Q5/Q6/Q29 parity holds in every sample; Q21's additional deterministic query matches exactly. All sixteen representative MO plans and four DuckDB profiles report zero spill.

Private controls use eight samples per variant. The original ineligible prefix-miss capture-index median is 38.175 ns versus main 35.985 ns (+6.09%); repair is 28.630 ns (-20.44%). MatchString/byte rejection improves 17–19%. Ordinary captured/lazy/multiline/nonterminal/mixed-program controls show no repeatable regression in the complete predeclared samples. Captured-tail early samples were elevated, but later rounds reversed that trend; complete medians are main2259.5 ns, original2294 ns and candidate2241 ns, with overlapping distributions and outliers in each variant. The raw samples are retained rather than discarding those outliers. Successful eligible tail/dotall controls improve about95%; failed-start inputs256/1024/4096 retain approximately linear scaling with a36% reduction.

The vector REGEXP_INSTR rejection medians and raw eight-sample distributions are in `evidence.json` under `vector_summary`; allocations are unchanged. UT/race results, nonempty selections, named tests, source fingerprints and baseline static-analysis diagnostics are under `local_validation`. The failed/preliminary experiments listed under `excluded_preflights` are not counted as acceptance evidence.
