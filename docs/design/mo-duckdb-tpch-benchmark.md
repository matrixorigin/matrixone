# MatrixOne versus DuckDB: TPC-H comparison

Version: 0.6. Status: implementation approved; MO benchmark aggregation threshold
set to 2 GiB at the user's explicit request, 2026-10-20.
Approval: the user accepted the comparison plan and requested implementation.
Revision 0.4 fixes the workstation, resource accounting, and three-part delivery.
Owners: benchmark runner, MO SQL/storage, and performance analysis.
Tracking issue: [#30569](https://github.com/matrixorigin/matrixone/issues/30569).
Implementation series: workload/correctness; controlled measurement; profiling.
PR numbers are recorded here when published.

## 1. Purpose and decisions

Measure where native MatrixOne and upstream DuckDB differ in query latency,
throughput, CPU use, memory demand, spilling, and the cost of preparing data.
The report must explain the largest differences with execution evidence.

The primary comparison is a single-machine MO deployment with one CN, one TN,
and its log/HAKeeper services, versus one persistent DuckDB database instance.
Both use native CPU execution and their native persistent storage formats on
the same local NVMe device. All MO components count toward the resource budget.
DuckDB uses an independently pinned upstream stable source version. Freeze an
optimized, symbolizable build for both measurement and profiling; the Go driver
must link that exact engine build.

This is an engineering comparison using the TPC-H workload. The report identifies
its departures from the audited benchmark and uses descriptive latency and
throughput metrics. Official QphH and price/performance scores are outside scope.

Approved decisions:

- Generate one immutable dataset per scale and load its exact bytes into both.
- Use the same logical schema, query parameters, and query variants.
- Use one Go `database/sql` worker implementation with two supported drivers.
- Make caller-observed execution through complete result consumption primary.
- Enforce equal total CPU, memory, and disk allowances externally.
- Separate warm runs, first-use runs, host-cache-cold runs, and load preparation.
- Publish every requested cell, including wrong results, timeouts and OOMs.
- Freeze the campaign before measurement. A faster MO result is not an acceptance
  requirement; a correct and reproducible comparison is.

## 2. Questions and experiment matrix

The selected machine is the Ryzen 9 7950X3D workstation with 64 GB RAM and local
NVMe. Use CPUs 0-7 on the 96 MiB L3 CCD; keep SMT siblings 16-23 idle. Reserve
the other CCD for the controller and OS. The primary application allowance is
32 GiB. Set its cgroup v2 memory.swap.max to zero; do not change host-wide swap.
Require an exclusive run window and a disk-capacity preflight. Record the CPU
topology, memory limit, and equal disk allowances in the frozen manifest.
Use GiB/bytes explicitly throughout the manifest.

| Experiment | Scale factors | CPU budget | Memory budget | Clients | Repetitions |
| --- | --- | --- | --- | --- | --- |
| Main warm query comparison | 1, 10, 100 | 8 physical cores | 32 GiB | 1 | 10 paired complete suites |
| Fresh process, warm OS cache | 10 | 8 | 32 GiB | 1 | 5 paired trials per query |
| Fresh process, cold data cache | 10 | 8 | 32 GiB | 1 | 5 paired trials per query |
| CPU scaling | 10 | 1, 2, 4, 8 | 32 GiB | 1 | 5 paired suites; reuse the 8-core baseline |
| Memory pressure and spill | 100 | 8 | 8, 16, 32 GiB | 1 | 5 paired suites; reuse the 32-GiB baseline |
| Concurrent read throughput | 10 | 8 total | 32 GiB total | 1, 2, 4, 8 | 5 complete waves per setting |
| Data preparation | 1, 10, 100 | 8 | 32 GiB | engine-native loading | 3 fresh loads each |
| Native operator profiles | every primary query/scale | matching measured profile | matching measured profile | 1 | 3 independent instrumented replays |
| Common CPU/counter profiles | Q1-Q22 at SF10/100 | matching measured profile | matching measured profile | 1 | bounded sampling windows; separate from timing |
| Deep allocation/wait profiles | largest gaps plus operator-coverage controls | matching measured profile | matching measured profile | 1; selected concurrency replay | separate lens per replay |

These are selected experiments, not a Cartesian product. One wave contains one
complete Q1-Q22 stream per client. The single-client warm baseline supplies the
corresponding throughput control. A short independent pilot fixes additional
wave counts when five waves would not provide a useful observation interval;
both engines then execute that same frozen amount of work.

SF100 remains a required target. If the machine cannot hold the immutable input,
both native databases, and the disk allowance, report the capacity limit and
mark that experiment capacity-blocked until sufficient storage is available.
Do not silently omit it or compare
results from different machines in one speedup table. SF1000 can be added as a
separate capacity campaign. A scale factor alone does not establish whether the
actual compressed working set fits in memory; measure database bytes and I/O.

Further experiments have distinct report sections and campaign identities:
generated substitution-parameter sets, prepared statements, global tuning,
shared Parquet scans, refresh functions, and multi-node MO scaling. They do not
replace the native-storage baseline.

## 3. Dataset, schema, and query contract

Use the pinned TPC-H tools/specification 3.0.1 as the starting point for DBGEN,
QGEN, qualification answers, schema and query definitions [1]. Freeze generator
source/build hashes, scale, partitions, seeds, locale, and exact file hashes.
Generate once; neither engine generates its own benchmark input during loading.
Use the same ordered input files and logical insertion sequence.

Create a new benchmark schema manifest. The existing regression DDL at
`test/distributed/cases/benchmark/tpch/01_DDL/01_create_table.sql` uses a
synthetic LINEITEM key and a different PARTSUPP primary key. It is not the
benchmark schema. The intended keys include `(l_orderkey, l_linenumber)` for
LINEITEM and `(ps_partkey, ps_suppkey)` for PARTSUPP.

Both schemas use the same logical column domains, nullability and primary keys.
Money and quantities retain their specified decimal types. No extra columns,
secondary indexes, manual clustering, or precomputed results enter the baseline.
Validate foreign-key relationships in the input; the baseline does not declare
additional foreign-key constraints in only one engine. Record automatic indexes,
native compression, and storage-layout differences as engine behavior.

Use QGEN's qualification parameter set for the main fixed-query comparison.
Record both rendered SQL hashes. Review the small dialect translation list:
date/interval syntax, quoting, casts required by syntax, and statement framing.
Preserve joins, subqueries, grouping, predicate placement, arithmetic, ordering,
and limits. Semantic rewrites create a separately named workload variant.
Freeze UTC, character encoding/collation, SQL modes and decimal-division settings.
Check that the generated text has the same comparison/order semantics in both
engines; a session-setting mismatch must not become a hidden query rewrite.

Q15 uses the original view creation, SELECT, and view removal as one timed unit,
with unique view names per stream and cleanup on failure. A shared CTE version
can be an explicitly named secondary experiment on both engines. It must not
replace Q15 for just one side.

Use DuckDB's TPC-H extension as a convenience/reference, not an independent
data or SQL source for its measured arm: its documented query parameters are
fixed, and its generated datasets have documented differences [2].

Benchmark scripts load the files, collect statistics, checkpoint, warm up,
measure and report. They do not scan loaded data, rehash the complete inputs,
compare catalogs or relationships, or compare query results. Do not replace the
removed row-by-row pass with checksums, sampling or another verification pass.
Correctness testing is separate and runs only when explicitly requested.

## 4. Separate correctness testing

The following rules describe separate correctness tests and explicit diagnostic
commands. Benchmark preparation, timing, profiling, reporting and publication
never invoke them or wait for a correctness verdict. Neither engine is
automatically the oracle. Benchmark completion means the scheduled executions
completed, and must not be labeled a correctness pass.

- Validate SF1 qualification results against the pinned reference answer set.
- Exercise a small independent suite for decimal boundaries, NULLs, strings,
  duplicate rows, dates, top-N ties, and each dialect substitution.
- Compare complete results only within the separately requested correctness task.
- Retain physical result metadata as well as the logical comparison schema.
  A DECIMAL/DOUBLE or CHAR/VARCHAR distinction must be visible in the report.

The result manifest specifies column position, logical type, expression family,
numeric comparison rule, output ordering, and duplicate policy for every query.
Integer, count, key, date and string values are exact. Decimal-only arithmetic
and sums compare exact normalized decimal coefficients; formatting zeros do not
change a value. String normalization is limited to the explicitly declared CHAR
padding contract. Arbitrary whitespace stripping is prohibited.

DuckDB documents floating-point results for decimal division [3]. AVG and ratio
columns therefore need a declared cross-engine numeric contract. Freeze their
expected types, precision, and rounding/truncation behavior before timing. The
proposed engineering bound is `E_MO + E_DuckDB`, plus
`1e-12 * max(1, abs(a), abs(b))` for floating-point rounding. Each E is the
analytically justified fixed-point error through the expression: half a quantum
for a nearest-rounded final operation, up to one quantum for truncation, and
propagated bounds when later arithmetic amplifies it. A floating result has no
fixed-point quantum. The contract must preserve at least two decimal places,
be checked against independent high-precision reference cases, and record
maximum observed absolute/relative errors. These are proposed engineering limits,
not a claim that the engines have identical arithmetic semantics. A missing or
failing rule fails that correctness test; tolerances are never fitted to
discrepancies.

Preserve duplicate multiplicity and required ordering. Compare unordered output
as a full multiset using external sorting. Ordered peers may vary within equal
sort keys. A LIMIT boundary tie requires proof of legal membership using an
untimed unlimited reference for that boundary group; do not add hidden tie-breaker
columns to one engine or accept a different row count. If the legal boundary
cannot be established, report the separate correctness result as unresolved.
Benchmark scripts do not run unlimited-query correctness replays.

Every scored execution drains all rows, checks terminal errors, and produces a
bounded typed result capture. Captures record execution output without comparing
it against another engine or reference. The standalone comparator is reserved
for explicit diagnostic use, outside benchmark scripts.

## 5. Resource and configuration fairness

Use a dedicated Linux host with cgroup v2 and an exclusive CPU set. One hardware
thread per chosen physical core is the baseline; record sibling/CCD/NUMA topology,
frequency policy, throttling, filesystem, mount options, and storage device.
Run only one engine arm at a time. Freeze the same CPU set for both arms.

The application's parent cgroup includes the query worker and every required MO
service, or the DuckDB worker and all its threads. Local object-storage processes,
if a later profile uses them, also count. The controller and offline comparator
run on reserved resources. Report both total application cost and an engine-only
breakdown where available. A single CN's RSS is not MO's total memory cost.

Apply a hard CPU/memory allowance and bounded owned disk space. Configure
`GOMAXPROCS`, DuckDB `threads`, and documented MO concurrency/cache/query budgets
before the campaign. Eight clients share the original total CPU/memory budget;
they do not each receive a full machine. DuckDB uses one database instance with
multiple connections, rather than independent databases per client [4].

Engine settings are not equivalent memory limits. DuckDB's `memory_limit` covers
its buffer manager, not every allocation [5]. Reserve headroom, freeze each
engine's internal budgets and effective settings, and use the application cgroup
as the authoritative total cap. Include page-cache charges, background workers,
temporary files and client allocations. An OOM is a capacity outcome, not a slow
query to discard.

Pin session `agg_spill_mem = 2147483648` on every MO benchmark connection and
record the effective value. This is a per-aggregation-worker threshold; the
application cgroup retains its original total memory allowance. The explicit
positive value bypasses auto mode's adaptive spill decision, while execution
allocation accounting and hard resource limits remain active. Runs using this
setting have a new worker identity and must be distinguished from the published
auto-setting baseline. Correctness verification remains outside benchmark scripts.

Keep the application cgroup across deliberate process restarts in warm-OS-cache
trials so cached file pages remain charged to its allowance. For cold trials,
evict owned data pages before admission and verify the cache preparation. A
database image warmed by an uncapped setup process cannot supply uncharged
cache memory to the measured arm.

Baseline optimizer settings and automatic maintenance remain enabled. Disable
query-result reuse and result persistence; use dynamic SQL through normal client
calls. Disclose internal plan-cache behavior. A distinct fresh-parameter workload
tests whether fixed-query reuse changes the conclusion. Prepared execution and
hand-tuned configurations have separate identifiers. Do not turn off MO GC,
durability, checkpoints or background services to improve a score.

Freeze release-optimized MO and DuckDB builds and verify the actual DuckDB
engine version reported by its library, as distinct from its driver version.
Primary comparison uses the MO main commit selected at campaign freeze and an
upstream stable DuckDB build. A released-MO control is separately labelled.
Record compiler flags, SIMD/ISA capabilities, native libraries and binary hashes.

## 6. Data loading and persisted database state

Use each engine's documented bulk loader for the same DBGEN files. Measure schema
creation, loading to commit, statistics collection, durable/checkpoint preparation,
and reopen-to-ready separately; also report their total time-to-query-ready.
Keep generator time outside the load timer. Equal loading means the same input
and correctness requirements, not identical loader implementation.

Readiness requires all rows committed, prescribed statistics preparation
complete, persistent state reusable after an orderly restart, and healthy service
admission. Use existing observable completion conditions with deadlines rather
than a fixed sleep. Record pending maintenance and any continuing work. Reopening
does not trigger correctness validation. Preserve a checkpointed database image for
each scale/engine; queries do not run on a freshly inserted mutable-only MO state
while DuckDB runs from a checkpointed file. Select the image generation before
query measurements (default: load replicate 1), rather than choosing the image
that later benchmarks fastest. Record any replacement as a new image.

Measure allocated persistent bytes, WAL/log bytes, peak temporary bytes and
post-cleanup size. Store datasets, database images, scratch, and reports under
different owned paths. State whether database-image copies are reflinks and
verify that they do not accidentally prewarm or mischarge the measured cache.

## 7. Execution and timing

Use a standalone Go benchmark module with one shared `database/sql` drain and
typed-capture path. MO uses the established MySQL driver; DuckDB uses the official
Go driver [6], configured to link the pinned symbolizable DuckDB build. Verify
the actual loaded engine version, library/build IDs and client compatibility.
Both are called by the same worker code. The module is isolated from the MO
kernel's dependency graph.

The primary clock starts immediately before SQL submission and ends after EOF,
`Rows.Err`, cursor closure, and all statements in a query unit. Capture
time-to-first-row, time-to-final-row, and cleanup time separately. For an empty
result, first-row latency is N/A. Do not time only obtaining a result handle or
returning the first batch.

Client decoding and bounded result capture are included in the primary latency.
Both arms use the same consumer. Sorting, full comparison and report formatting
occur after the clock. Record capture bytes and spill so client work can be
interpreted. Keep a separate simple-query/driver control; do not subtract its
latency from complex-query measurements. This measures how an application uses
the products: MO's client/server interface and DuckDB's embedded interface.
Engine-internal timings are explanatory metrics with their definitions disclosed.

For each warm cell, execute ten paired blocks with five MO-first and five
DuckDB-first blocks, shuffled with a fixed seed. Publish that schedule. Each arm
starts from its declared state, executes one complete unmeasured warmup suite,
then one measured suite; all queries use the same frozen order on both sides. Warm means
this stated warmup under the memory budget, not a promise that all data fits in
RAM. Stop/join the inactive arm before the other starts. Control owned cache
state and first-touch cgroup accounting between arms.

Fresh-process trials restart the complete MO deployment, not just its CN, and
reopen DuckDB. Cache-cold trials additionally evict the owned dataset's OS pages
before startup using a verified dedicated-host procedure. A restart alone is
not a cold-storage experiment. If eviction cannot be verified, mark cold-cache
results unavailable. Record storage reads during startup: report startup,
first-query and combined startup-to-result latency so preloading cannot disappear
outside the reported cost. Cold comparisons are query-by-query independent trials.

For concurrent throughput, start the fixed query streams behind one admission
barrier, with one connection per stream. Reuse identical query permutations and
parameter seeds. Measure whole-wave completion, successful queries/second,
completed suites/minute, per-query latency and errors. Throughput uses first
submission through final completion, including scheduling gaps; it is not the
sum of per-query timers. Reuse the single-client baseline only when its measured
window and ordering match this contract. Run offline comparison after the entire
measured suite/wave, keeping comparator work outside its admission path.
Closed-loop results are labelled accordingly; an open-loop arrival/queueing
experiment is separate.
Report p95 only with enough samples (proposed minimum 100 per query), and p99
only with at least 1,000 per query. Smaller cells show sample counts, medians,
IQR and maxima rather than unsupported tail percentiles.

Per-query and per-suite deadlines, the campaign budget and disk quota are frozen
after the independent pilot. Use the same limits for both engines. Retain every
timeout, crash, OOM and failed block. A retry is a new recorded attempt, never an
overwrite. Infrastructure-invalid runs need a concrete predeclared reason and
remain in the ledger; replace the paired block transparently when appropriate.

## 8. Diagnosis and report

Fine profiling must explain differences through three connected observations:
the amount of work selected by the plan, the cost of executing that work, and
the waits/overlap that turn work into caller-visible latency. Separate its
executions from scored timing and preserve raw evidence at every layer.

### 8.0 Cross-runtime comparison contract

Go and C++ both execute native machine code on the benchmark CPU. Linux CPU/PMU
measurements provide a common reference; Go pprof and C++ allocator/native tools
provide explanations within their measured domains. Comparable profile formats
or similar function names do not establish comparable resource quantities.

| Quantity | Shared measurement | Runtime-specific explanation |
| --- | --- | --- |
| Request latency | Common worker submission-to-complete-consumption clock | Native phase and pipeline diagnostics |
| Total CPU cost | Application-cgroup `cpu.stat` user/system/usage deltas, plus perf counters [14] | Hot engine functions, runtime, client and background stacks |
| Execution efficiency | Cycles/instructions per completed query and verified unit of logical work | Hashing, decoding, vector/decimal operations and generated code |
| Actual memory demand | The same application-cgroup current/peak measurements | Go heap, MO custom/native domains, DuckDB buffers/native heap and retained pools |
| Blocking and parallelism | OS thread run/wait events plus native execution evidence | Go goroutine scheduling/GC and C++ task/lock behavior |

For a successful profiling window with N completed calls, report:

```text
application CPU seconds/query = delta(cpu.stat.usage_usec) / (1e6 * N)
cycles/query                 = counted application cycles / N
average occupied cores       = application CPU seconds / window wall seconds
```

These gross window quantities include client, runtime and background work. Keep
them separate from sampled foreground estimates or statement-attributed native
facts. First diagnose selected SF10 queries under one total CPU core, using the
existing CPU-scaling control, then repeat at the primary eight-core budget. The
single-core view simplifies CPU-work comparisons; the parallel view exposes
coordination, overlap and imbalance. Neither replaces the primary benchmark.

Separate roles as well as languages. The DuckDB worker contains a Go client and
a C++ engine. MO includes its Go client, server execution and native helpers.
Keep client consumption/conversion, engine execution, runtime memory management,
transport/kernel, background and unattributed costs identifiable where evidence
permits. Go GC in the DuckDB client must not be described as C++ engine GC; MO
client decoding must not be assigned to its server's join operator.

Use semantic work groups and record physical types/algorithms. Different join
orders, compression, decimal representations and materialization choices can
explain different instruction counts. A database-level difference is not proof
of a programming-language effect. If that claim matters, add a separate matched
kernel experiment with the same inputs, types, NULL patterns, algorithm and
required semantics, while retaining the end-to-end product result.

### 8.1 Collect complementary views

| View | MO collection | DuckDB collection | Comparison it supports |
| --- | --- | --- | --- |
| Planning and physical work | Statement phase diagnostics and native EXPLAIN ANALYZE; relation/scan, operator and pipeline counters | Detailed JSON profiling and physical plan [7,8] | Join order, build/probe sides, cardinality errors, pruning, repeated scans, materialization and compile work |
| Actual CPU and hot functions | Whole-application OS CPU counters and Linux perf; Go pprof as an additional language-specific view | Same OS CPU counters and perf, covering C++ execution and the Go client | CPU seconds, cycles/instructions, function and source-line hot spots |
| Allocation, lifetime and copies | Go heap/alloc profiles, MO allocator profiles, cgroup/process memory, sampled copy/encoding stacks | Buffer-manager statistics, covered native allocator profiling, cgroup/process memory, sampled copy/conversion stacks | Allocation churn, peak live versus reserved memory, retention, representation and transfer costs |
| Waiting and parallelism | Classified operator waits, Go block/mutex/runtime traces and OS scheduling/I/O traces | Native blocked metrics and the same OS scheduling/I/O traces | I/O stalls, runnable delay, synchronization, producer/consumer backpressure and parallel imbalance |

MO's existing sources include `pkg/vm/process/operator_analyzer.go`, statement
phase information in `pkg/util/trace/impl/motrace/statistic/stats_array.go`,
the debug profiler handlers in `cmd/mo-service/debug.go`, and the allocator
profiler in `pkg/common/malloc/profile_allocator.go`. Probe these capabilities
at the pinned revision; their existence does not establish full coverage of
every operator, allocation domain or service.

Collect DuckDB JSON for the actual query using the pinned version's supported
`enable_profiling`, `profiling_mode`, `profiling_output` and metric-selection
settings [8]. Capture estimated/actual rows, operator timings, optimizer/planner
phases, result size, available I/O, buffer-memory and temporary-storage metrics.
Use unique files per query statement; configuration readback must not overwrite
the target profile. For Q15, preserve separate create/query/drop profiles and
their complete query-unit boundary.

Use normal-query MO diagnostics where the selected build supports them. MO
EXPLAIN ANALYZE is a separate replay whose result path differs from returning
the ordinary query results. Keep its operator evidence, but use actual SELECT
replays for CPU, allocation and output-path diagnosis. Check that the relevant
plan shape and workload match before relating the two.

### 8.2 Audit metric semantics before translating them

Store each raw name and value alongside its engine version, unit, scope, meaning,
aggregation rule, and quality/coverage flags. The common report distinguishes:

- Caller elapsed time, measured process/cgroup CPU time, and sampled CPU cycles.
- Exclusive active elapsed work, summed worker/operator timings, and waits.
- Logical rows/bytes, engine filesystem reads, and physical device I/O.
- Allocated bytes over an interval, peak live bytes, allocator-reserved bytes,
  buffer-manager usage, RSS/PSS and total application-cgroup memory.

In the inspected MO analyzer, `TimeConsumed` subtracts measured waits and owned
child-call time from a local elapsed interval. It is not an OS CPU timer. DuckDB
documents `CPU_TIME` as cumulative operator timings [9]. Its native field is not
the cross-engine process-CPU denominator. Use OS accounting for that comparison.
DuckDB operator timings can sum beyond query wall time under parallel execution
[7]; MO's parallel work and waits can also exceed a single elapsed interval.

Do not sum all plan nodes into an elapsed-time breakdown, subtract overlapping
children as if their intervals were serial, or add per-process memory peaks to
claim a simultaneous cluster peak. Missing counters remain N/A, including zero
values from collectors whose implementation/coverage was not verified. A native
quality flag marking partial or invalid accounting remains visible.

### 8.3 Align semantic work, not operator numbers

For each query, retain the two physical graphs and align useful subgraphs by
their input relations, predicates, join keys/kinds, aggregate keys/functions,
ordering and consumer. Operator IDs are local identifiers. A local plus merge
aggregate may correspond to one aggregate in the other engine; a fused scan and
filter may correspond to two nodes. Preserve unmapped or engine-specific stages.

Use these reporting families: planning/setup; scan/prune/decompress/decode;
filter/expression evaluation; join build/probe; aggregate/local-merge; sort/top-N;
materialize/exchange; result encode/transfer/decode; and runtime/background work.
Keep both the semantic family and the mechanism (hashing, decimal arithmetic,
allocation, copying, locking). These are two views of the same work, not additive
categories across views.

For each aligned group, compare actual intermediate rows, decoded/logical bytes,
pruning, scans/reuse counts, algorithm, worker degree, observed work, waits and
memory/spill evidence. Normalize CPU/instructions by a named common denominator
where meaningful, such as a logical input row or a verified join probe row.
Do not compare compressed bytes with decoded bytes or assume a metric called
rows-scanned counts the same boundary in both engines.

### 8.4 CPU, hardware counters and sampling windows

Freeze release optimization, native library identities and symbol files before
baseline measurement. Retain build IDs and separate debug information. The
scored and profiled runs use the same executable/native code. An unoptimized,
race-instrumented, or separately rebuilt engine cannot supply the exact baseline
profile. If the driver's prebuilt DuckDB library lacks useful symbols, select
its documented custom-library linkage before freezing the campaign.

Start common perf accounting/sampling outside the application's cgroup and gate
the recorded region around query execution after warmup. Cover the query client
and every MO service, or the DuckDB worker and native threads. Record process-role
breakdowns and distinguish whole-window cost from genuinely statement-attributed
cost. An idle control helps identify background activity; it does not justify
silently subtracting a presumed constant background cost.

Use small PMU event groups for cycles/instructions, branches/misses and supported
cache events in separate passes. Record event meanings, user/kernel scope and
counter running/enabled time. Capture context switches, migrations, faults and
CPU utilization as well. Report IPC, instructions and cycles per completed query
and named unit of work. On-chip cache misses are not storage reads or a direct
measurement of memory bandwidth.

Compare absolute CPU seconds/cycles per query and per named unit of work, not
just flame-graph percentages. Sampling-derived stage CPU is an estimate: retain
sample weights, the measured CPU denominator and coverage. Raw sample counts
from different effective sampling rates are not comparable resource quantities.

Validate Go/native mixed-stack unwinding and symbolization in a pilot. Keep
unknown frames and lost samples in the coverage report. A proposed qualification
target is at least 90% symbolized CPU weight and 90% PMU running/enabled time;
otherwise reduce the event set or report coarser conclusions. Preserve exclusive
sample costs and inclusive call graphs separately. Use the supported DWARF or
frame-pointer method with matching symbols; do not assume `perf -g` alone can
unwind every optimized C++ or Go/cgo stack [15]. A profiler pilot must recover
known Go and native hot paths before source-level attribution is accepted.

Very short queries need bounded repeated-query profiling windows: target 20
seconds, cap at 60 seconds or 2,000 completed repetitions, and record the actual
count. Warmup and connection setup are outside that window. Normalize accumulated
work by completed calls. Repeating a query creates a steady-repeat diagnostic
profile; it does not replace a cold or first-use measurement. If there are too few
samples for a function-level claim, report that limitation rather than making
the SQL or data artificially more expensive.

### 8.5 Allocation, copies, storage waits and critical paths

Run memory and wait profilers independently of CPU sampling; profiling tools can
perturb one another [10]. Record allocation totals/objects, peak live memory and
retained/reserved capacity separately. Go heap profiles cover the Go heap, not
all MO mmap/custom allocations or DuckDB's C++ memory. Likewise, DuckDB buffer
statistics do not cover every native allocation. Use MO's allocator profiles and
a validated native allocation collector where available; report their coverage.
Total application-cgroup memory remains the common footprint reference.

Freeze the actual Go version, GOGC/GOMEMLIMIT policy and native allocator
configuration. Do not force a collection after every query or disable collection
to imitate C++ lifetimes. Measure naturally occurring GC assists, background
collection/sweeping and allocator/free/pool-management costs in their own buckets
[12]. Use a sufficiently long steady-repeat window to observe the relevant
allocation/collection behavior and record GC cycles plus beginning/end live and
reserved memory. If little Go-heap activity occurs, report that fact together
with custom/native allocations; zero GC cycles do not imply zero allocation.

Keep request latency distinct from amortized resource cost: collection and
deferred cleanup may occur after a response. Record visible trailing activity
separately and avoid charging it entirely to the next or last query. CPU spent
by runtime memory management remains part of application cost. Forced-GC heap
inspections or replacement allocators are explicitly named diagnostic modes,
with their perturbation and coverage recorded, not baseline measurements.

Attribute copy/representation costs first through hot stacks: memory copy/clear,
slice growth, vector conversion, string handling and result encoding. Samples
show CPU cost, not a count of copied bytes. Byte counts require covered boundary
counters or validated call-site tracing; out-of-line memcpy probes miss inlined
copies. Report an incomplete count as such. Add narrowly scoped instrumentation
only when an important gap cannot be resolved with existing evidence.

Collect storage request/byte and cache counters alongside process/device I/O.
Use scheduling and I/O traces to distinguish blocked I/O, runnable-but-not-running
delay, locks/futexes, pipeline backpressure and idle workers. Go runtime/block/
mutex profiles are additional MO explanations, not direct DuckDB equivalents.
Reuse a matching sample of the concurrent wave when contention is the suspected
cause; do not assign a whole-wave sample to an arbitrary query.

Go goroutines can migrate and share OS threads. An OS thread CPU timer around a
Go operator call does not provide reliable goroutine CPU attribution; pinning
operators to threads for measurement would change execution. Use common sampled
CPU evidence and Go execution traces for goroutine scheduling [13]. A parked
goroutine need not leave its OS thread idle, and a goroutine in a cgo/syscall
state may have native code actively executing. Correlate native CPU samples and
OS running state before calling such an interval off-CPU waiting. Do not compare
goroutine blocking totals with C++ thread blocking totals as identical metrics.

Produce a request/phase and thread run/wait timeline using real events. A claimed
pipeline critical path requires timestamped producer/consumer dependency evidence;
aggregate operator JSON alone cannot reconstruct it. Keep unexplained time and
unattributed activity visible. A CPU flame graph alone cannot explain off-CPU
latency, and aggregate CPU plus waits is not a serial wall-time equation.

### 8.6 Execution protocol, causal checks and artifacts

Capture three native-operator replays for every primary query/scale and common
CPU/counter profiles for all Q1-Q22 at SF10/100. Apply deeper allocation, copy,
wait and concurrency profiling to the five largest gaps in each direction plus
representative scan, join, aggregate and materialization cases. Profile both
engines for each selected case. Preserve all captures rather than selecting the
one with the most convenient explanation.

Each profile retains the baseline's build, data, SQL, resource budget and cache
condition. Store an unprofiled/profiled/unprofiled control around each compatible
actual-query profiling mode to report perturbation. EXPLAIN output-path changes
are a mode difference, not a pure profiler-overhead estimate. Never divide a
profiled runtime by an estimated overhead factor to manufacture a benchmark time.

The analysis asks, in order:

1. Did a different plan do more scans, create more intermediate rows, repeat a
   subquery, or choose a larger join/aggregate state?
2. For comparable work, were instructions/cycles higher in decoding, hashing,
   decimal evaluation, allocation, copying or result handling?
3. Did I/O, spilling, scheduling, synchronization or an imbalanced pipeline
   extend the request's critical path?
4. Does a controlled replay support the hypothesis: one versus eight cores,
   warm versus cold reads, a larger memory budget, or a separately named query
   variant? Change one factor and preserve correctness and all raw results.

For example, a scan/aggregate query with identical logical input but more decode
instructions points toward representation/reader cost. A join with much larger
intermediate cardinality first points toward plan/selectivity, rather than a
hash-table implementation conclusion. These are hypotheses to test, not assumed
MO or DuckDB weaknesses.

Publish one query dossier with raw plans/profiles, aligned work tables, CPU flame
graphs, memory/allocation summaries, run/wait timelines where supported, counter
coverage, profiling perturbation, controlled-replay results and confidence limits.
Include statements/pipeline IDs where available, process/thread IDs, capture
intervals, sample/event configuration, units and source/build IDs. Missing
observability becomes an explicit gap; source changes need their own reviewed
instrumentation plan before use. Profiling capabilities and symbol coverage are
preflight requirements for the complete diagnostic report [11].

The controller owns profiler processes, file handles and shutdown. Proposed
collector limits are 256 MiB buffering and 1 GiB output per capture, with an
explicit campaign-wide quota. Use reserved monitoring resources and avoid heavy
trace writes to the measured storage device during I/O diagnosis. Reaching a
limit or failing to flush/join makes that capture incomplete. Finalize and hash
profiles after the query/collector have joined; never publish a partial profile
as successful evidence.

### 8.7 Report

The report contains:

1. Exact hardware, builds, data/schema/query manifests and effective settings.
2. Q1-Q22 latency tables at each scale, correctness status, uncertainty and counts.
3. Median complete-suite time, sum of query medians, and query speedup geometric
   mean, each clearly named. Define speedup as DuckDB time / MO time (>1 favors MO).
4. CPU scalability, memory/spill behavior, concurrency throughput and latency.
5. Data-load/ready time, startup, persistent bytes and peak temporary space.
6. The largest five gaps in each direction, linked to plans and measured work.
7. Every failed/unavailable cell and every workload or semantic limitation.

Compute a suite total from a complete measured suite, then take medians across
blocks. Give paired-block bootstrap intervals for ratios; preserve the block as
the resampling unit. Never add medians and call the result a median full suite.
Do not pool scale factors, cache conditions or resource budgets into one score.
When any query is wrong, missing, timed out or OOM, the full-suite score for that
cell is incomplete. Any common-success subset is a labelled diagnostic with its
coverage shown, not the full 22-query comparison. Do not substitute timeout
limits as measured runtimes or select each engine's best different configuration.

## 9. Harness ownership, artifacts and verification

The alternatives are:

| Approach | Benefit | Cost or limitation |
| --- | --- | --- |
| Independent CLI scripts | Small setup and useful smoke checks | Different text rendering, startup, capture and cancellation behavior obscure short-query comparisons |
| Common Go client and two drivers (selected) | Shared caller, timing, capture and lifecycle code; existing supported clients | Driver costs remain part of the application result and require diagnostic controls |
| Dedicated native C/C++ clients | Useful lower-level driver/engine control | Extra client implementations and normalization paths increase verification cost; reserve for diagnosing a demonstrated client bottleneck |

Keep one small controller, two worker modes, and a versioned manifest/result
format. Use a fresh standalone module under `optools/tpch_compare/`; no benchmark
framework, production plugin interface, or MO execution hooks are proposed.
The controller owns the schedule, cgroups, process groups, data-image generations,
deadlines, disk allowance and final report. Workers own connections and cursors.

State flow is preflight -> load/checkpoint -> deadline pilot -> warm/reset ->
measure/drain -> stop/join -> publish. Arm failure seals that arm, cancels its
query, closes its resources, and joins its processes before the next arm starts.
If a driver cannot interrupt, the parent terminates the owned worker/service
group after the configured grace period (proposed default: 30 seconds). The
parent's control deadline must operate independently of a blocked driver call.
Failed join blocks continued reuse. Q15 view cleanup and an unchanged base-data
identity are checked before another query unit can reuse the database.
Never kill unrelated services or recursively remove an unverified data path.

Bound worker capture to 64 MiB RAM and 4 GiB scratch per query by default;
external comparison has the same independent bounds. Freeze larger allowances
before running if the pilot proves a query requires them. All capture charges
are included in the application allowance. Check free space for both databases,
input, comparison and native spill before admitting a campaign. Freeze the
complete expected run count and worst-case deadline budget in the schedule.

Each attempt records engine/build/profile/data/query IDs, block and query order,
timestamps, cache preparation, actual settings, timings, row counts, result-schema
and capture hashes, execution status, resource observations, errors, exit status
and cleanup outcome. Manifests/configuration/SQL and small summaries are versioned;
generated datasets, databases and raw profiler captures live outside Git with
checksum-bound publication when results are shared.

Verify the harness with cheap deterministic tests for schedule balance, query
rendering, manifest rejection, exact/approximate comparisons, duplicates and tie
boundaries, fake-worker timeout/cancellation, disk exhaustion, corrupt receipts
and incomplete-report rejection. A small real fixture exercises both clients,
empty/multirow results, DDL units and cleanup. Use SF1 for the first all-22 public
smoke test. Large performance campaigns are explicit jobs, not ordinary UT/BVT.

## 10. Delivery and review

1. Approve this methodology and the query/schema/numeric-comparison contract.
2. Deliver dataset/DDL/query manifests and separate correctness tests with the
   minimal two-driver worker and deterministic lifecycle checks.
3. Deliver measurement isolation, finite schedules, resource collection and
   offline report validation; run the SF1 public smoke campaign.
4. Freeze binaries/configuration and execute the baseline and scaling campaigns.
5. Execute section 8's operator/common-CPU profiling matrix, diagnose the largest
   gaps with independent allocation/wait lenses and controlled replays, and
   publish the complete report with metric-coverage limits.
6. Run refresh/shared-file or multi-node extensions only under their own manifests.

For refresh extensions, generate RF1/RF2 input once, restore an owned base image,
and measure transaction commit plus post-commit validation. Qualify reader
snapshot behavior at controlled commit boundaries before adding concurrent
refresh pressure; results from different visible database epochs must not be
compared as if they came from the same snapshot.

The first report covers the single-machine matrix; refresh, GPU and multi-node
experiments remain separate. Resolve exact build hashes, supported settings,
numeric contracts, storage capacity and profiling capabilities in preflight
receipts. A missing prerequisite blocks the affected experiment rather than
producing a scored result. Keep MO and DuckDB execution code unchanged for the
baseline. Observability additions and performance fixes require separate PRs.
No performance conclusion is assumed.

## Sources

1. [TPC-H specification 3.0.1](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v3.0.1.pdf).
2. [DuckDB TPC-H extension and limitations](https://duckdb.org/docs/current/core_extensions/tpch).
3. [DuckDB numeric types and decimal division](https://duckdb.org/docs/current/sql/data_types/numeric).
4. [DuckDB concurrency model](https://duckdb.org/docs/current/connect/concurrency).
5. [DuckDB memory limits](https://duckdb.org/docs/current/operations_manual/limits).
6. [Official DuckDB Go client](https://duckdb.org/docs/current/clients/go/overview).
7. [DuckDB EXPLAIN ANALYZE](https://duckdb.org/docs/current/guides/meta/explain_analyze).
8. [DuckDB detailed profiling](https://duckdb.org/docs/current/dev/profiling).
9. [DuckDB profiling metrics and aggregation](https://duckdb.org/docs/current/dev/metrics).
10. [Go diagnostics and profiler interaction](https://go.dev/doc/diagnostics).
11. [Linux perf capabilities and collection scope](https://www.kernel.org/doc/html/latest/admin-guide/perf-security.html).
12. [Go garbage-collector behavior and resource tradeoffs](https://go.dev/doc/gc-guide).
13. [Go execution trace and goroutine tasks](https://pkg.go.dev/runtime/trace).
14. [Linux cgroup CPU accounting](https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html).
15. [Linux perf stack collection and unwinding](https://man7.org/linux/man-pages/man1/perf-record.1.html).
