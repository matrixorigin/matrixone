# Read-through-MO profiling and verification

Date: 2026-09-07. This records the local continuation of review iteration 7.

Committed dependencies: Sirius `d36467ac71a824dba5f464a94cf2d7f4d6deb248`
and sidecar `cc494aa`, whose Sirius submodule points to that commit. The
MatrixOne implementation and this record are committed together. The commits
record the validated source without changing its executable behavior.

## Result

The ordinary MatrixOne reader remains the source. All 22 SF10 queries pass
through explicit `SIDECAR STREAM` with two Sirius GPU pipeline workers.
The final run totals **34.158730 seconds**; its following direct
`SIDECAR GPU` control totals **15.501565 seconds**.
Both routes have corresponding `GPU_MO_SCAN` / `GPU_TAE_SCAN` execution logs.

These are single-run client wall times, excluding golden-file comparison.
They are not medians. The final total ratio is 2.20x
and Q9 is 4.08x; the design's 2x acceptance gate
has not passed. An earlier pooled run was 34.114813s and its direct control
12.683093s. Preserve both observations instead of treating variation as a
successful 2x result.

The 71.028568s figure from September 4 remains historical evidence, not a
freshly reproduced all-22 baseline. Today's controlled Q1 measurements were
7.981466s before the pool change, 8.136971s with source-stage profiling,
6.294594s after pooling, and 6.256535s in the final all-22 run.

## Method and environment

- Existing SF10 database `tpch_10g`, originally loaded into
  `.e2e-27599/benchmark-5mode/fresh-mo-data`.
- RTX 3070, 8 GiB device memory; Sirius GPU use limit 0.5,
  two pipeline workers, two scan workers, per-stream reservations enabled.
- Sirius host pool: 2 GiB capacity, 1 MiB blocks, 64 blocks per pool.
- MO SHARED memory cache: 512 MiB for the final comparison.
- Release build with existing `SIRIUS_PROFILE=ON`; all Sirius build/run/test
  commands use Pixi. MatrixOne uses the existing host binary.
- All query outputs compared against `mo-tpch/golden/TPCH_10` with the
  existing numeric comparator. No fallback is allowed for explicit stream mode.
- Fresh local mTLS certificates; the resolver URL uses `localhost`, matching
  the resolver certificate's DNS identity.
- Source bases: MatrixOne `608d3a6d993b`, sidecar `80828393e1a5`,
  Sirius `9e9cba60f80e`, plus the iteration-7 changes validated below.

| Query | MO reader → Sirius (s) | Direct TAE → Sirius (s) |
|---|---:|---:|
| Q1 | 6.256535 | 2.305652 |
| Q2 | 0.677578 | 0.505422 |
| Q3 | 1.488251 | 0.777654 |
| Q4 | 1.059063 | 0.918734 |
| Q5 | 2.558871 | 1.048262 |
| Q6 | 0.446020 | 0.647452 |
| Q7 | 1.842562 | 0.857222 |
| Q8 | 2.030912 | 0.668857 |
| Q9 | 3.255576 | 0.798354 |
| Q10 | 1.109153 | 0.814643 |
| Q11 | 0.458787 | 0.306363 |
| Q12 | 1.064423 | 0.603782 |
| Q13 | 0.426359 | 0.698354 |
| Q14 | 0.587412 | 0.427325 |
| Q15 | 0.547279 | 0.306097 |
| Q16 | 0.382195 | 0.240069 |
| Q17 | 2.062140 | 0.458385 |
| Q18 | 3.253837 | 0.586131 |
| Q19 | 0.726298 | 0.780376 |
| Q20 | 0.938990 | 0.467864 |
| Q21 | 2.740083 | 1.138033 |
| Q22 | 0.246406 | 0.146534 |
| **Sum** | **34.158730** | **15.501565** |

## Measurements that changed the diagnosis

MO's ObjectIO memory cache stores decompressed column data. A cache hit can
avoid decompression; the previous description of it as only a compressed-byte
cache was incorrect.

`EXPLAIN ANALYZE SELECT l_returnflag, l_linestatus, l_quantity,
l_extendedprice, l_discount, l_tax FROM lineitem WHERE l_shipdate <=
DATE '1998-12-01' - INTERVAL '112' DAY` consumed the raw scan without an
aggregate or MySQL row output. The client completed in approximately 0.75s;
the plan reported 58,682,142 projected rows / 4.37 GiB. Native aggregate Q1
is not a substitute for this scan control.

A 12-second process CPU profile captured 14.62 CPU-seconds, including other
MO activity. Direct frame serialization accounted for approximately 0.58
CPU-seconds; socket writes, column reads/decompression, and batch copies also
appeared prominently. These samples are not additive stage wall times.

Source-stage timing on Q1 measured 71 source tasks, 3,228.54ms waiting for
input, 1,151.26ms allocating pinned host buffers, and 204.78ms copying data.
It does not include host-buffer destruction or all GPU execution. The
per-task registration cost was therefore demonstrated rather than inferred
from transferred byte counts.

A pageable-buffer control passed Q1/Q9 at 6.274421s / 3.352618s.
The final implementation instead reuses the existing cuCascade pinned host
pool, avoiding a new pool or per-read retained cache. The pre-existing
64 MiB expanded source bound and 128-frame source limit remain in place.

The separate 8 GiB MO-cache experiment reduced cold reads in the raw scan.
Its streamed Q1/Q9 were 6.208749s / 3.337650s, offering no material improvement
over the same consumer with the smaller cache. The configuration was restored
before the final table. This does not establish that caching never helps;
it shows that increasing this cache did not remove the current limiting stage.

Further profiling of input transport and overlap is warranted. The measurements
do not prove a hardware ceiling or require abandoning the MO reader.

## Implementation and ownership

The source builder now obtains blocks from the existing host-memory pool.
Writes may span pool blocks; GPU conversion copies only logical used bytes into
one device mirror. The buffer owns both the allocation and its reservation.
Its destructor returns blocks before releasing the reservation, including
partial construction, move assignment, discarded partial input, and publication
failure. The source claim is released only after successful transfer completion.

No MO producer window, wire codec, capability identity, scan visibility,
filtering, or projection change was required for this optimization.

The real nullable-data check also found a compatible-input rejection. Bitmap
coverage belongs to the bitmap and can differ from the vector length.
Validation now checks its own coverage/count/length, while the logical null
count excludes bits beyond the vector. GPU loads are alignment-safe and stop
at the declared bitmap byte length. NULL varlena descriptors are sanitized
only in the Sirius-owned copy before GPU decoding.

| Closure | Ownership / wait / bound review | Evidence |
|---|---|---|
| Pooled host input (R3) | buffer owns blocks and reservation; blocks returned before arena release; existing source wait and cancellation; pool and source capacities remain fixed | cross-block copy, move, reuse, capacity rejection, H2D and partial-discard tests; SF10 GPU run |
| Nullable input (R2/R3) | bitmap validated independently; trailing absent words are valid; no unaligned or out-of-range GPU loads; source bytes unchanged | short/padded/malformed bitmap tests; stale NULL descriptor test; CUDA memcheck; mixed persisted/tail public query |
| Documentation (R0) | preserve historical times, retract unsupported causal attribution | this record and the iteration-7 design update |

## Validation

- Sirius source/Substrait suite: **314 assertions in 23 cases passed** under
  CUDA `compute-sanitizer --tool memcheck --error-exitcode=99`, **0 errors**.
- Sidecar stream suite: **93 assertions in 10 cases passed**.
- Real MO nullable test: persisted rows, a deletion, and an unflushed insert
  match native SQL exactly. The temporary database was dropped afterward.
- Two all-22 pooled-buffer runs pass. The final one includes the nullable-input
  fixes. The subsequently added stale-descriptor test and comment changes do
  not change that execution path.
- No MatrixOne production Go changes were made in this continuation; existing
  Go validation is not rerun for documentation-only changes.
- All input bounds and two GPU workers remain enabled. The 2x performance gate,
  repeated median campaign, and PR delivery remain outstanding.

Local raw evidence is retained under
`.e2e-27599/readthrough-mo.39FHiT/`: runner/configs, `q1-cpu.pprof`,
raw-scan plans, native/stream nullable outputs, full Sirius/MO logs, and
`final-stream/`, `final-tae/`, `pooled-all/`, `control-tae/` results.
Certificates are local test credentials and must not be published.
