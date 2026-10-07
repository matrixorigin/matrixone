# Low-precision float vector columns: vecf8 (MXFP8), vecf4 (NVFP4)

Status: accepted · Issue: #20567 · Scope: packed low-precision vector column types and a
GPU dot-product search function over them · Depends on: the scalar `float8`/`float4`
types (their e4m3/e2m1 element codecs are reused unchanged).

## Motivation

Quantized embeddings are stored in narrow floats to cut memory and bandwidth, but FP8
(±448) and especially FP4 (±6, ~2.5 octaves) are far too narrow to hold a real embedding
directly — every component would saturate or underflow. NVIDIA's microscaling formats
scale a small contiguous *block* of a vector's own dimensions against one shared scale,
centering each block into the format's representable band:

```sql
CREATE TABLE t (id BIGINT PRIMARY KEY, v vecf4(1024));   -- or vecf8(1024)
```

FP8/FP4 are **tensor-core GEMM** formats. Their GPU acceleration is a batch matrix
multiplication — the dot-product matrix `S = D × Qᵀ` of a dataset against a set of
queries. They are not ANN-index element types: cuVS indexes accept only `float`, `half`
and `int8`/`uint8`. So `vecf8`/`vecf4` target **GPU dot-product search via cuBLASLt
block-scaled matmul**, separate from `cgo/cuvs`. For approximate/indexed low precision,
`vecint8` (cuVS scalar quantizer) and IVF-PQ remain the tools.

## The two types

| SQL type   | Element (4/8-bit)        | Block | Block scale (8-bit)                 | GEMM format |
|------------|--------------------------|:-----:|-------------------------------------|-------------|
| `vecf8(N)` | OCP FP8 e4m3 (`Float8`)  |  32   | E8M0 (`CUDA_R_8F_UE8M0`)            | **MXFP8**   |
| `vecf4(N)` | OCP e2m1 (`Float4`)      |  16   | unsigned E4M3 (`CUDA_R_8F_UE4M3`)   | **NVFP4**   |

`N` is the logical dimension, carried in the column type, independent of the cell byte
length. The element is genuinely 4/8-bit; the 8-bit scale is shared across a block
(NVFP4 ≈ 4.5 bits/element, MXFP8 ≈ 8.25 bits/element).

cuBLASLt's FP4 block-scaled matmul consumes **NVFP4 only** (`CUDA_R_4F_E2M1` elements,
`CUDA_R_8F_UE4M3` scales over 16-element blocks); it does not accept MXFP4 (32-block
E8M0). FP8 uses MXFP8 (32-block E8M0). Each type stores what its GEMM consumes, so no
element is re-quantized at search time.

## Element and scale codecs

The element formats are the scalar #20567 types:

- e4m3 = `types.Float8` — `S.EEEE.MMM`, bias 7, NaN `0x7f`, max ±448, no Inf = `CUDA_R_8F_E4M3`.
- e2m1 = `types.Float4` — `S.EE.M`, codes {0,½,1,1½,2,3,4,6}, max ±6, no Inf/NaN = `CUDA_R_4F_E2M1`.

Scale codecs:

- MXFP8 scale = E8M0 — 8-bit unsigned power-of-2 exponent, value `2^(b−127)`; one small new codec.
- NVFP4 scale = unsigned E4M3 — the `Float8` e4m3 codec with the sign bit clear.

**CUDA parity (measured).** `types.Float8`/`types.Float4` were compared with CUDA's
`cuda_fp8.h`/`cuda_fp4.h` (CUDA 13.3): all 256 e4m3 and 16 e2m1 codes decode identically
(signed zeros and subnormals included), and encoding 4.3M float32 inputs — a strided sweep
of every bit pattern plus every rounding midpoint ±1 ulp, saturation and ±Inf — matches
bit for bit. The only difference is NaN encoding (Go keeps the sign for FP8 and maps FP4
NaN to +0; CUDA returns `0x7f` / `0x7`); non-finite values are never stored.

**Quantizer (ours, not NVIDIA's).** NVFP4 and MXFP8 define how a cell decodes, not how
its scales are chosen; a cell written here decodes identically on the CPU and the tensor
cores, but it is not byte-identical to what NVIDIA's quantizers (TensorRT Model
Optimizer, Transformer Engine) or the OCP MX reference produce for the same input:

- Block scales round up: the smallest unsigned E4M3 (vecf4) or E8M0 (vecf8) scale whose
  product with the element maximum (6 or 448) covers the block's largest magnitude, so no
  element saturates. The references round the scale to nearest (NVFP4) or take
  `2^(floor(log2 amax) − 8)` (OCP MX) and let the block maximum saturate.
- The vecf4 global scale is stored as the decode multiplier `amax/(6*448)`; vecf8 stores 1.
- Each element is the exact quotient `v / (global × scale)` rounded once,
  round-to-nearest-even with the CUDA tie and saturation rules: the float64 quotient
  cannot fall on an E2M1/E4M3 midpoint unless the exact quotient is that midpoint.
- Encoding is deterministic: equal inputs give equal cells.

## Cell format

Each value is one varlena cell:

```
offset  size  field       vecf8 (MXFP8)               vecf4 (NVFP4)
------  ----  ----------  --------------------------  --------------------------------------
 0      1     version     1                           1
 1      1     format      1                           2
 2      2     reserved    0                           0
 4      4     dim N       uint32 LE                   uint32 LE
 8      4     global g    float32 LE, 1.0             float32 LE, amax(v)/(6*448); 0 if v = 0
12      S     scales      S = ceil(N/32), E8M0        S = ceil(N/16), unsigned E4M3
12+S    E     elements    E = N, e4m3                 E = ceil(N/2), e2m1
```

- **header** — 12 bytes of MO metadata. The format fixes the element type, scale type
  and block size (32 / 16); the scale count follows from `N`.
- **block scales** — one byte per block, in block order. Scale block `b` governs its
  `blockSize` consecutive elements.
- **value** — element `i` of block `b` is `g × scale[b] × element[i]`.
- **packed elements** — e4m3 one byte each; e2m1 two per byte, **element `2i` in the low
  nibble, `2i+1` in the high nibble** (CUDA's `__nv_fp4x2_e2m1` order, measured). An odd
  `N` leaves the final high nibble `0x0` (+0).

`Size ≠ N × elemSize`: the logical dimension is authoritative from the column type /
header, never derived from byte length.

The cell is also the exact binary form at the SQL surface: `vecblock_binary(v)` returns it
as a `BLOB`, and a `BLOB` of a valid cell casts back to the same bytes. A cell is valid when
the version is 1, the format names the target type, the reserved bytes are 0, `N` is in
range and matches a declared dimension, the length is exactly `12 + S + E`, the vecf8
global is 1.0 and the vecf4 global finite and non-negative, no scale or element is a NaN
code, a vecf4 scale is non-negative, an odd-`N` vecf4 padding nibble is 0, and every
element decodes finite. A `BLOB` of float32 elements is `4N` bytes, which is never the cell
length of `N` (vecf8 would need `3N = 12 + ceil(N/32)`, vecf4 `4N − ceil(N/2) − ceil(N/16)
= 12`; neither has an integer solution), so the length selects the form for a declared
dimension; without one a `BLOB` is a cell when its header names the target format and its
length is the cell length of its `N`.

GPU transfer: the packed-element bytes of consecutive rows form the cuBLASLt operand
directly (memcpy). The block scales are re-laid out into cuBLASLt's tiled scale tensor
(§GPU engine) — a per-byte gather, because a scale tile spans 128 vectors and cannot be
represented inside one cell. The global `g` stays on the host: the GEMM computes the dot
product of the block-scaled values, and the caller multiplies it by `g_d × g_q`.

## Scale derivation

- **vecf8 (MXFP8), one level.** Per block, `scale = 2^ceil(log2(absmax/448))` (the
  smallest E8M0 power of two with `448 × scale ≥ absmax`); `element = e4m3(v / scale)`.
  E8M0 spans 2^-127..2^127, so every finite float32 fits; `g = 1`.
- **vecf4 (NVFP4), two levels, applied per vector.** NVFP4 scales a tensor by an fp32
  global and each 16-element block by an E4M3 scale. MO applies the global per vector,
  so every vector stays independent:
  - `g = amax(v) / (6 × 448)`;
  - per block, `scale = ue4m3_round_up(absmax(block) / (6 × g))`, capped at 448 — the
    block holding the vector maximum gets exactly 448;
  - `element = e2m1(v / (g × scale))`.

  Any finite float32 range fits, and block scales stay out of the E4M3 subnormal range
  (0% of scales on unit-normalized embeddings, vs 60% with a fixed global of 1.0).
- Scales round **up** to a representable value, so no element overflows its format. An
  all-zero block stores scale 0 and zero elements.
- **Every stored value decodes finite.** NaN/Inf input is rejected at build, per the
  repo-wide finite-persistence rule, and so is a finite input whose quantized value
  decodes outside float32: MXFP8 rounds ±3.4028235e38 to 256 × 2^120 = 2^128, which
  decodes to ±Inf, so the cast fails with "out of range" (the same value is finite in
  NVFP4). Cell parsing applies the same rule to stored and received cells: a cell with an
  element whose dequantized value (global × block scale × element, in float32) is not
  finite is rejected. Only blocks whose largest element code could overflow are scanned.
  Every accepted cell therefore renders as finite text and parses back.
- **One zero.** The encoder stores an element that quantizes to zero of either sign as
  code 0. Equality, hashing (`GROUP BY`, `DISTINCT`, joins) and ordering peers compare the
  decoded values, not the bytes (see Column operations), so cells that encode equal values
  with other scales are equal.

## Storage size, 1024-dim

| Format | Header | Scales | Elements | Total | bits/elem |
|--------|:------:|--------|----------|-------|-----------|
| `vecf8` MXFP8 | 12 B | 32 × 1 B (E8M0) | 1024 B (e4m3) | **1068 B** | 8.34 |
| `vecf4` NVFP4 | 12 B | 64 × 1 B (E4M3) | 512 B (e2m1)  | **588 B**  | 4.59 |

vs `vecf32(1024)` = 4096 B: `vecf4` is 7.0× smaller, `vecf8` 3.8×.

## Accuracy (measured)

`wiki_all_1M` 768-d embeddings, unit-normalized, 50,000 base vectors, 200 queries; both
dataset and queries quantized; recall@10 of the quantized dot product against exact fp32:

| Format (the `pkg/container/types` codec) | recall@10 | mean relative L2 reconstruction error |
|--------|:---------:|:-------------------------------------:|
| `vecf8` MXFP8 | 0.971 | 2.7% |
| `vecf4` NVFP4, per-vector global | 0.922 | 9.9% |
| NVFP4 with a fixed global of 1.0 (not used) | 0.918 | 10.6% |

The GEMM multiplies the stored values; the ranking is approximate relative to the original
fp32 vectors because the storage is quantized, and on the GPU it carries the fp32 GEMM's
rounding (see Decisions). `vecf8` is the quality format,
`vecf4` the memory format.

## GPU engine: cuBLASLt in `cgo/cuvs`

The engine computes the fp32 **dot-product matrix** `S = D × Qᵀ` of a packed dataset tile
`D` and packed queries `Q` with `cublasLtMatmul` (a block-scaled matmul for
`vecf8`/`vecf4`, a plain matmul for `vecf32`, `vecf16`, `vecbf16`, `vecint8` and
`vecuint8`), and turns it into scores of a **metric** on the device: inner product,
cosine distance or squared L2 distance. The distances decompose into the dot product and
squared norms, `1 − x·q / (‖x‖‖q‖)` and `‖x‖² + ‖q‖² − 2 x·q` (clamped at 0), so the
matmul stays on the tensor cores and a norm kernel adds one pass over the tile. The scores
are rank scores, the negated distance (largest is nearest); per tile the engine can also
keep the `k` best rows of each query on the device (`run_topk`, below). It has no filter
logic.

| Column type | Engine format | Element type | Compute | Output |
|-------------|---------------|--------------|---------|--------|
| `vecf8` | MXFP8 | `CUDA_R_8F_E4M3` + `VEC32_UE8M0` scales | `CUBLAS_COMPUTE_32F` | `CUDA_R_32F` |
| `vecf4` | NVFP4 | `CUDA_R_4F_E2M1` + `VEC16_UE4M3` scales | `CUBLAS_COMPUTE_32F` | `CUDA_R_32F` |
| `vecf32` | F32 | `CUDA_R_32F` | `CUBLAS_COMPUTE_32F` (no TF32) | `CUDA_R_32F` |
| `vecf16` | F16 | `CUDA_R_16F` | `CUBLAS_COMPUTE_32F` | `CUDA_R_32F` |
| `vecbf16` | BF16 | `CUDA_R_16BF` | `CUBLAS_COMPUTE_32F` | `CUDA_R_32F` |
| `vecint8` | I8 | `CUDA_R_8I` | `CUBLAS_COMPUTE_32I` | `CUDA_R_32I` |
| `vecuint8` | U8 | `CUDA_R_8I` (x − 128) | `CUBLAS_COMPUTE_32I` | `CUDA_R_32I` |

Plain rows are the column's raw element bytes, copied into the padded tile with no header,
scales or global. `vecuint8` has no cuBLASLt integer path: each element is shifted to int8
when the tile is packed (`x ^ 0x80` = x − 128), and the int32 result is corrected as
`x·q = x′·q′ + 128 (Σx′ + Σq′) + 128² · dim` in int64. Integer dot products are exact.

Device kernels around the matmul, on the engine's stream:

- A row-statistics kernel reads the packed tile after the upload (one block per row) and
  computes each row's squared norm from the values the matmul multiplies (decoded
  elements × block scales × global, in double; exact integers for `vecint8`/`vecuint8`),
  and the `vecuint8` shifted-element sums. It runs once on the query matrix when the
  engine is created, and on each tile when the metric is cosine or squared L2 or the
  format is `vecuint8`. The host only packs bytes and reads cell headers.
- The engine's device buffers are slices of one allocation sized when it is created.
- For cosine and squared L2 on `vecf32`, `vecbf16` and `vecf8`, whose values can make the
  fp32 products overflow or underflow, the row-statistics kernel rescales a row right after
  taking its norm: a row whose squared norm without its global scale is outside
  [2^-60, 2^60] is multiplied by a power of two 2^-k (|x| near 2^k) — `vecf32`/`vecbf16`
  elements scaled, `vecf8` E8M0 exponents shifted, a block shifted below the E8M0 range
  zeroed — and its global scale by 2^k, so the GEMM sees values near 1 and the fix-up kernel
  restores the scale exactly. Rows in range are left as they are, at the cost of one
  comparison. `vecf4` (its global scale is outside the GEMM), `vecf16`, `vecint8` and
  `vecuint8` have bounded GEMM operands.
- The row-statistics kernel takes a row per thread up to 128 elements and a row per warp
  above (shuffle reduction, no block barrier), loops its grid over the rows, accumulates in
  double, and is compiled per format. On 1M `vecf32(768)` rows it takes 0.74 ms per
  64K-row tile; a block per row with a separate rescale kernel took about 0.9 ms plus the
  rescale launch. Measured alternatives: float accumulation and 16-byte vector loads were
  not faster; a thread per row is 2–3× slower above 768 elements.
- A fix-up kernel turns the matmul output into rank scores. The inner product is
  `acc × fp32(g_row × g_query)` in fp32, as cuBLASLt applies `alpha = G_a × G_b` to a GEMM
  with per-tensor global scales; cosine and squared L2 take the dot product with the
  global scales in double, with the squared norms: `1 − x·q / (|x||q|)` and
  `|x|² + |q|² − 2·x·q` (integer formats take the corrected int64 sums, which are exact).
  The rank is rounded once to
  fp32; −Inf for NaN and for the padding rows. A zero vector has cosine distance 1, as
  `cosine_distance` returns.
- With one global scale for all rows and one for all queries, the cells are exactly
  NVIDIA's NVFP4 / MXFP8 operands, and the inner-product scores equal a direct cuBLASLt
  block-scaled GEMM on the same bytes bit for bit at the same GEMM shape (another shape
  can select another cuBLASLt algorithm and fp32 summation order);
  `MatchesNvidiaBlockScaledGemm` in `cgo/cuvs/test/blockscaled_matmul_test.cu` checks it.

Two ways to read a tile's scores, both after the fix-up kernel:

- `run` copies the whole score matrix to the host: `n × nq` fp32 values per tile.
- `run_topk` keeps the result on the device: `cuvs::selection::select_k` keeps the `k`
  best rows per query, and `nq × k` scores and row indices are copied back. A second
  kernel counts, per query, the rows equal to the `k`-th score. When more rows tie there
  than `select_k` kept, that query's whole score column is copied back as well, so the
  host applies the `id` tie-break exactly as on the CPU.

Code layout — the engine lives with the other GPU code in `cgo/cuvs` + `pkg/cuvs`:

- `cgo/cuvs/blockscaled_matmul.hpp` — the engine class (header only, C++): packs cells
  into padded element rows and tiled scales, owns the device buffers, CUDA stream and
  cuBLASLt handle, and runs the matmul.
- `cgo/cuvs/blockscaled_matmul_c.h` / `blockscaled_matmul_c.cpp` — the C API
  (`gpu_blockscaled_matmul_new/_run/_run_topk/_max_rows/_destroy`), compiled into libmo with the
  other `*_c.cpp` objects.
- `cgo/cuvs/test/blockscaled_matmul_test.cu` — the standalone `test_blockscaled_matmul`
  executable; `cgo/cuvs/test/narrow_float_golden_gen.cu` — the Float8/Float4 golden-data
  generator.
- `pkg/cuvs/blockscaled_matmul.go` — the Go binding (`//go:build gpu`).
- `pkg/sql/colexec/aggexec/vector_matmul_gpu.go` (`//go:build gpu`) registers the engine
  with the aggregate; CPU builds have no engine and score on the CPU.

Everything else is CPU code in its usual place: the cell codec in `pkg/container/types`,
the SQL functions under `pkg/sql`, and the CPU dot product.

cuBLASLt call sequence:

| Step | Call | Setting |
|------|------|---------|
| 1 | `cublasLtCreate` | once per engine |
| 2 | `cublasLtMatmulDescCreate` | `CUBLAS_COMPUTE_32F`, scale type `CUDA_R_32F`; integer formats `CUBLAS_COMPUTE_32I`, `CUDA_R_32I` |
| 3 | `cublasLtMatmulDescSetAttribute` | `TRANSA=T`, `TRANSB=N`; block-scaled formats only: `A/B_SCALE_MODE` = `VEC32_UE8M0` (vecf8) or `VEC16_UE4M3` (vecf4), `A/B_SCALE_POINTER` = tiled scale tensors |
| 4 | `cublasLtMatrixLayoutCreate` | A: element type, K × rows(D), ld = K; B: K × rows(Q), ld = K; D: `CUDA_R_32F` (`CUDA_R_32I` for integer formats), rows(D) × rows(Q) |
| 5 | `cublasLtMatmulPreferenceCreate` / `SetAttribute` | workspace limit (32 MiB) |
| 6 | `cublasLtMatmulAlgoGetHeuristic` | at construction, for each tile row bucket (128 × 2^i up to the tile capacity); a bucket without an algorithm fails the engine's creation |
| 7 | `cublasLtMatmul` | alpha = 1, beta = 0, the bucket's cached algorithm; the per-vector `g` of row and query is applied by the fix-up kernel, in fp32 for the inner product and in double for cosine and squared L2 (plain formats have `g` = 1) |

Contract (each point measured on sm_120 with cuBLASLt 13.6):

- **Operand orientation: dataset = A, queries = B.** With queries as A, a single query
  (M = 1) returns silently wrong results and M = 2 has no algorithm; with the dataset as A,
  every query count (1, 2, 3, 5, …) matches the CPU reference, up to 100,000 dataset rows.
- **Row padding.** Dataset rows and queries are padded with zero elements to a multiple
  of 128, so a 1-row tail tile is never an M = 1 operand.
- **Scale tensor layout is tiled, not per row.** Per-row scales produce wrong results
  (relative error ≈ 1). The correct layout pads rows to 128 and blocks to 4, and stores
  128 × 4 tiles of 512 bytes:

  ```
  S      = K / blockSize,  Spad = roundup(S, 4),  rows padded to roundup(R, 128)
  offset(r, s) = ((r / 128) * (Spad / 4) + s / 4) * 512
               + (r % 32) * 16 + ((r % 128) / 32) * 4 + (s % 4)
  ```

  The engine writes each row's scales to these offsets on the host while packing the tile.
- **K padding.** The dimension is padded with zero elements to a multiple of 32 (NVFP4 at
  K = 48 has no algorithm; 32, 96, 512, 768, 1536 run). Storage keeps the true `N`.
- **Precision.** Against a double-precision CPU reference: MXFP8 relative error ≤ 1e-6,
  NVFP4 exact, at fp32 output; `vecint8`/`vecuint8` exact; `vecf32`/`vecf16`/`vecbf16`
  within fp32 summation-order tolerance.

Engine state, per `vector_matmul` executor: a CUDA stream, the cuBLASLt handle and
workspace, the queries on the device, and host and device buffers for one tile. No
dataset is cached; every tile is uploaded.

Memory admission:

- Device memory is claimed through `device_memory_governor` before it is allocated. The
  `select_k` temporary workspace is allocated per call from the default device resource
  and is not claimed.
- The engine's native host memory (`gpu_blockscaled_matmul_host_bytes`: tile staging,
  score copy, per-row and per-query globals, query packing) is reserved against the
  aggregate's allocation account (`ReserveCapacity`, committed to a `CapacityLease`)
  before the engine is created.
- The Go tile buffers (cells, ids, groups, scores and the top-k buffers) are allocated
  from the same account; the id buffer grows in preflight to hold the batch.
- Both count in the aggregate's `Size` and are released by `Free`. When the account has
  no room for either, the query fails: with an eligible device enabled there is no CPU
  fallback (see Decisions).
- The engine is created in preflight, so a fill never allocates tile memory.

Dispatch: the compiler reads the session's `gpu_mode` and stores it in the aggregate's
configuration. An executor uses the engine when `gpu_mode` is on and the process has a
visible device meeting the baseline below; otherwise it scores on the CPU. Rows are buffered in a tile of at most
64 MiB (cells plus scores, up to 65,536 rows), scored when the tile is full, and the tile
is drained before the states are read (final result, merge, intermediate result, spill).
A tile whose rows all belong to one group (always the case without `GROUP BY`) is scored
with `run_topk`; a tile mixing groups is scored with `run`, since a per-query top-k over
several groups would let one group crowd out another. `topk` above the tile's row
capacity also uses `run`.

The configuration (query vectors converted to the column type) is parsed once per query:
executors holding the same configuration bytes for the same column type share one parsed
copy, reference-counted and dropped when the last executor is freed. Without it each
executor decodes the query JSON again, including every executor that receives a partial
state for the merge, one after another.

Build integration:

- `cgo/cuvs/Makefile` lists `blockscaled_matmul_c.cpp` in `C_SRCS` and builds the
  `test_blockscaled_matmul` executable.
- `cgo/gpu-toolchain.mk` adds `-lcublasLt` to `MO_GPU_LDFLAGS`.
- No new runtime dependency: `libcuvs.so` already requires `libcublasLt.so.13`, which the
  Pixi environment and the GPU runtime image ship.

## SQL surface

### Column operations

`vecf8`/`vecf4` follow the scalar `float8`/`float4` rule: no native arithmetic; a value
in an expression is dequantized and computed as `vecf32`, and a result is quantized again
only when it is stored into a `vecf8`/`vecf4` column (assignment cast).

| Operation | Behavior |
|-----------|----------|
| `CAST` | text ↔ `vecf8`/`vecf4`; `vecf32` ↔ `vecf8`/`vecf4` |
| `+ - * /` (vector–vector, vector–scalar) | operands promoted to `vecf32`; result `vecf32` |
| `ANY_VALUE`, `COUNT`, `GROUP_CONCAT` | as for `vecf32`; `GROUP_CONCAT` renders the dequantized text |
| `inner_product`, `l2_distance`, `l2_distance_sq`, `l1_distance`, `cosine_distance`, `cosine_similarity` | over the dequantized values; `a`/`b` each `vecf8`, `vecf4` or `vecf32`, a text literal binds as `vecf32`; results as for `vecf32` |
| `vector_dims`, `normalize_l2` | as for `vecf32`; `normalize_l2` returns the argument's type |
| `summation`, `l1_norm`, `l2_norm`, `abs`, `sqrt` | on the values dequantized to `vecf32`; results as for `vecf32` |
| `if`, `case`, `coalesce`, `ifnull` over one `vecf8(N)`/`vecf4(N)` type (text branches are quantized to it) | the column's type, cells copied as stored |
| `case`/`coalesce` mixing vector types, `greatest`, `least`, `json_object`, `json_array`, `JSON_ARRAYAGG`, `JSON_OBJECTAGG` | on the values dequantized to `vecf32`; results as for `vecf32` |
| `SUM`/`AVG`/`MIN`/`MAX` over vectors | not supported (no vector type has them) |
| `ORDER BY`, window `ORDER BY` | as for `vecf32`: by the dequantized values, element-wise |
| `GROUP BY`, `DISTINCT`, hash joins, set operations, `IN (subquery)`, `COUNT(DISTINCT)`, `approx_count_distinct` | by the decoded values, as `=` compares: the equality key (`keycodec.AppendCanonicalVecBlock`) is the cell's decoded float32 values, so cells that encode equal values with other block or global scales (vecf8 `[447]` and `[449]` both store 448) are one key |
| window `PARTITION BY`, `ORDER BY` | by the decoded values, element-wise |
| `subvector` | not supported (as for the other narrow vector types) |
| comparison operators (`=`, `<>`, `<`, `<=`, `>`, `>=`, `<=>`, `IN`, `BETWEEN`) | as for `vecbf16`: a text literal is quantized to the column's type, as a stored value is, so a row matches the text it was inserted from (`'[3000, -12, 0.001, 1000000]'` matches the cell that displays as `[3072, -16, 0, 983040]`), and text of another dimension is rejected; cells compare by their dequantized values, element-wise |
| `hex`, `to_base64` | not supported, as for the other narrow vector types (`vecf32` only) |
| primary key, partition key, secondary/unique index, vector index | rejected at DDL |
| `LOAD` | CSV text `"[…]"`, JSONL arrays; Parquet `LIST<FLOAT/DOUBLE>` and `STRING`/`JSON` columns, quantized per row. The exact text (CSV text, a JSONL object value, a Parquet `STRING`/`JSON` value) loads without quantization. A Parquet `BYTE_ARRAY`/`FIXED_LEN_BYTE_ARRAY` column without a logical type is binary, read as a `BLOB` casts: a value of cell length is the stored cell, a value of `4N` bytes is float32 elements |
| `INTO OUTFILE`, external-table writes | the exact text, in CSV as a quoted field and in JSONL as an object value, so an export reloads to the same cells |
| binary input | a `BLOB` of little-endian float32 elements, as for `vecf32` (`CAST(UNHEX('0000803F…') AS BLOB)` or a BLOB parameter), quantized per row; a length that is not a multiple of 4 or another dimension is rejected |
| exact binary | `vecblock_binary(v)` returns the stored cell as a `BLOB` (§Cell format); a `BLOB` of a cell casts, inserts or binds back to the same bytes without quantization, and a `BLOB` of cell length that is not a valid cell of the target is an error |
| exact text | `vecblock_json(v)` returns the cell as stored, `{"g": g, "s": [scales], "v": [values]}`, the parts in the cell's order: `g` the global scale (1 for `vecf8`), `s` one scale per block of 32 (`vecf8`) / 16 (`vecf4`) values, the last block shorter, `v` the element values; casting that text (or inserting it, or `LOAD`ing it) builds the same cell bytes without quantization. Element `i` is `v[i] · s[i / blockSize] · g`; `g` is required, `s` must hold `ceil(len(v) / blockSize)` scale code values (E8M0 / UE4M3), `v` element code values (E4M3 / E2M1); anything else is an error, never rounded |

The promotion is implemented in these operations only: function resolution dequantizes a
`vecf8`/`vecf4` argument to `vecf32` for the functions listed above, and every other
function rejects the types.

#### Distance kernels

`pkg/vectorindex/metric/distance_func_vecblock*.go`. Scalar Go, no SIMD. One kernel per
operand pair (`vecf8`×`vecf8`, `vecf4`×`vecf4`, `vecf8`×`vecf32`, `vecf4`×`vecf32`,
`vecf8`×`vecf4`; a swapped pair runs with its arguments exchanged) and per metric (dot,
squared L2, L1, cosine parts), generated by `TestVecBlockKernelsGenerated` (`go generate`; the test fails when the file is stale). Each kernel
walks 16-element units, the common divisor of the two block sizes, fully unrolled: an
element is a table lookup (E4M3 code, or an E2M1 byte as two values) times the unit's
`g × block scale`, the same value `Dequantize` produces. A unit accumulates in float32 and
folds into float64; elements past the last full unit take a per-element path.

768-d, one row (Ryzen AI 9 365):

| Pair | dot | L2² | L1 | cosine |
|------|-----|-----|----|--------|
| `vecf8` × `vecf32` | 215 ns | 214 ns | 305 ns | 329 ns |
| `vecf4` × `vecf32` | 200 ns | 201 ns | 303 ns | 333 ns |
| `vecf8` × `vecf8` | 273 ns | 282 ns | 382 ns | 414 ns |
| `vecf4` × `vecf4` | 291 ns | 311 ns | 437 ns | 421 ns |
| `vecf8` × `vecf4` | 264 ns | 266 ns | 387 ns | 417 ns |

Parsing a cell (header and validation) adds 52 ns for `vecf4` and 186 ns for `vecf8`.

Overflow: a unit accumulates in float32 lanes, so finite products can overflow one lane
to +Inf and another to −Inf, whose sum is NaN. Following the metric package's
non-finite contract, the inner product and cosine distances map NaN to +Inf (the largest
distance); L2 and L1 are sums of non-negative terms and cannot produce NaN. The SQL
functions report any non-finite result as an overflow error.

### Batch nearest-neighbour search

#### `vector_matmul` (aggregate)

```sql
vector_matmul(topk, src_id, src_vec, queries [, options]) → JSON
```

| Argument | Type | Meaning |
|----------|------|---------|
| `topk` | constant integer | the hits kept per query, 1–16384 |
| `src_id` | column | the row key: an integer, `char`/`varchar`/`text` or `uuid` column |
| `src_vec` | column | `vecf8(N)`, `vecf4(N)`, `vecf32(N)`, `vecf16(N)`, `vecbf16(N)`, `vecint8(N)` or `vecuint8(N)`; `vecf64` is rejected |
| `queries` | constant string or JSON, or `BLOB` | array of query vectors `[[…], …]`, each of length `N`; or a `BLOB` of little-endian float32 values, `N` per query back to back (`CAST(? AS BLOB)` for a client's bytes), whose length must be a non-zero multiple of `4·N` and whose values must be finite. Converted once to the column's type (quantized for `vecf8`/`vecf4`); for `vecint8`/`vecuint8` every value is an integer in the type's range, otherwise an error. With `"query_format":"vecblock"` (a `vecf8`/`vecf4` column only) the queries are cells used as given: a `BLOB` of cells back to back (`vecblock_binary`), its length a non-zero multiple of the cell size, or a JSON array of vecblock JSON objects; every cell must be a valid cell of the column's format and dimension |
| `options` | optional constant JSON object | `"metric"`: `inner_product` (the default), `cosine` or `l2sq`; `"query_format"`: `float32` (the default, float values) or `vecblock` (cells); other keys are ignored; text that is not a JSON object, or another value of either key, is an error. Dispatch follows the session's `gpu_mode` |

An aggregate: one result per group (one row without `GROUP BY`), of MO's `JSON` type.
`topk`, `queries` and `options` are constants, prepared parameters or user variables; the
compiler moves them into the aggregate's configuration, so each executor receives only
`src_id` and `src_vec`. A scalar subquery is planned as a join and arrives as a column,
so query vectors stored in a table go through a user variable:
`SET @q = (SELECT json_arrayagg(v) FROM query_vectors)`.
MO's distributed aggregation runs it in two phases. Each scan pipeline on each CN fills
its own state, which keeps the top `topk` scores per query: on the CPU each row is scored
on arrival; with the GPU engine rows are appended to a host tile, which is scored when it
is full or before the states are read. The partial states are then merged into the final
top `topk`; across CNs the states are serialized to the merging CN. Rows with a NULL
`src_id` or `src_vec` are skipped. A `WHERE` on the source table filters rows before they
reach the aggregate.

Per group the state holds, for each query, a heap of at most `topk` hits, plus an arena
with the hits' id text (a row that enters several queries stores its id once). All of it
is charged to the aggregate's allocation account; preflight reserves the arena space for
a batch before the batch is filled, and the arena compacts in place.

Instances on one CN share its GPU: each uses its own CUDA stream and cuBLASLt
handle/workspace, and host and device tile memory scale with the instance count
(`instances` × one tile of at most 64 MiB).

On the CPU (gpu_mode off, a CPU build, or no visible device) each distance is computed by
the kernel of the SQL function of the metric — `VecBlockInnerProduct`,
`VecBlockCosineDistance` and `VecBlockL2DistanceSq` for `vecf8`/`vecf4`, the column type's
kernel (`metric.ResolveDistanceFn`) for the other types, as `inner_product`,
`cosine_distance` and `l2_distance_sq` resolve them — so a CPU result equals the scalar
function on the same row and query, including the float64 cosine recompute and the
squared L2 of differences. The GPU's cosine and squared L2 are those of the fp32 GEMM
expansion (see Decisions): within fp32 summation-order tolerance of the scalar function,
relative to the squared norms. An overflowing score (NaN) ranks last; a non-finite score in
the result is an overflow error, since JSON has no infinity.

#### Result format

```json
[
  [ ["17", -0.93], ["4",  -0.91] ],
  [ ["8",  -0.88], ["17", -0.85] ]
]
```

- Outer array: one entry per query, in input order (position = query id).
- Inner array: that query's hits, nearest first (score ascending), ties by the id text in
  byte order (so `"10"` before `"9"`); at most `topk` entries, `[]` when there is no input
  row.
- Hit: a pair `[id, score]`.
  - Position 0, `id`: the source key as a JSON string (exact for 64-bit integers and
    non-integer keys).
  - Position 1, `score`: the distance of the metric, a JSON number.
  - A field added later takes position 2; positions 0 and 1 keep their meaning.

#### Usage

```sql
SELECT vector_matmul(10, id, v, '[[0.12, …], [0.33, …]]') AS result
FROM t
WHERE category = 'news';
```

Relational form of the final result (chained `CROSS APPLY unnest`, verified on the
current build with three levels and with the pair format, including a
`9223372036854775807` id cast back to `bigint` exactly):

```sql
WITH m AS (<the query above>)
SELECT q.`index` AS q_id, h.`index` AS rnk,
       json_unquote(json_extract(h.value, '$[0]'))  AS src_id,
       cast(json_extract(h.value, '$[1]') AS double) AS score
FROM m CROSS APPLY unnest(m.result, '$') q
       CROSS APPLY unnest(q.value, '$') h;
```

#### Scores

The score is the distance of the `metric` option, the value of the matching SQL function:

| `metric` | Score | SQL function |
|----------|-------|--------------|
| `inner_product` (default) | `−x·q` | `inner_product(v, q)` |
| `cosine` | `1 − x·q / (‖x‖‖q‖)`, the similarity clamped to [−1, 1], 1 with a zero vector | `cosine_distance(v, q)` |
| `l2sq` | `‖x − q‖²`, as `‖x‖² + ‖q‖² − 2 x·q` clamped at 0 | `l2_distance_sq(v, q)` |

On the CPU the hits are the same rows, in the same order, as `ORDER BY <function>(v, q), id
LIMIT topk`. On the GPU, rows whose distances differ by more than the fp32 GEMM's rounding
rank as there, and closer rows can rank in either order; the cosine and squared-L2
expansions lose digits relative to the norms when `x` is close to `q` (as in cuVS and
FAISS), so a row equal to a query has distance 0 up to that rounding, never below 0 (see
Decisions). L1
and L2 (the square root) are not provided.

## Hardware & toolchain

- Supported GPU baseline: compute capability 10.0 or newer (Blackwell: `sm_100` B200,
  `sm_120` GeForce RTX 50). The MXFP8 (`VEC32_UE8M0`) and NVFP4 (`VEC16_UE4M3`) scale
  modes start at 10.0; Hopper and older devices are not used, for any format.
- The visible devices are checked once per process
  (`gpu_blockscaled_matmul_device_count`), not per call. Engines use only devices meeting
  the baseline, round-robin; with none, `vector_matmul` scores on the CPU.
- Toolchain: CUDA ≥ 12.8 and cuBLAS ≥ 12.9 for sm_120. The Pixi GPU profile provides
  CUDA 13.3 and cuBLASLt 13.6, on which everything above was measured (RTX 5070 Laptop,
  sm_120, 8 GB).

## Invariants

- The e4m3 / e2m1 element bit layouts are the scalar `float8`/`float4` types; packing is a
  byte/nibble shuffle with no per-element conversion.
- Packed elements of consecutive rows form the cuBLASLt operand byte for byte; any change
  to the element packing is a storage-format change and bumps the header version.
- Every stored vector is independent (no cross-vector or global state); `INSERT`/`UPDATE`
  needs no column-wide statistics.
- The engine returns rank scores of the metric (the negated distance), or per tile the `k`
  best per query with every row tied at the `k`-th score; filtering and the final top-k
  live in the aggregate, which reports the distance.
- Merging partial states is order-independent: the final result does not depend on how
  rows were split across pipelines and CNs (ties are broken by `id`).

## Phasing

- **P1 — storage (CPU):** `vecf8`/`vecf4` types, header codec, E8M0 scale codec,
  pack/unpack, string cast, display; a golden test pinning the element codecs to the CUDA
  outputs above.
- **P2 — column operations (CPU):** casts, arithmetic, `ANY_VALUE`/`GROUP_CONCAT`,
  distance functions, DDL rejections, CSV/Parquet `LOAD`.
- **P3 — CPU aggregate:** `vector_matmul` in `cpu` mode (CPU distance kernel over the
  quantized cells), partial merge and state serialization; UT + BVT.
- **P4 — GPU engine:** the cuBLASLt engine in `cgo/cuvs` (tiled scale layout, row and K
  padding, dataset-as-A matmul) and `vector_matmul` GPU dispatch by `gpu_mode`, compared
  with P3 within fp32 tolerance.

## Testing

Oracle: the result equals a reference computed over the whole table by
`ORDER BY inner_product(src_vec, query) DESC, id-text LIMIT k` per query, with the query
cast to the column's format — on the CPU the same ids in the same order; on the GPU scores
within the fp32 GEMM's tolerance, rows closer than it in either order.

- **Type-switch families** (`pkg/container/types/typeswitch_test.go`): a `switch` over
  `types.T` in `pkg/` that names any `T_float*`/`T_bf*` type must name all of them, or all
  low-precision ones (bf16/float16/float8/float4) and no other; one that names any
  `T_array_*` type must name all of them, or exactly vecf8/vecf4; otherwise it carries
  `// typeswitch:partial <reason>` on or above the switch. The families are read from the
  constants in `types.go`, so a new type is checked at every such switch. Switches that
  predate the check are listed in `testdata/typeswitch_baseline.txt` by file, function and
  family; the list may only shrink (regenerate with `-args -update-typeswitch`).
- **Unit** (`aggexec/vector_matmul_test.go`): top-k and tie order; NULL ids and vectors;
  empty groups (`[]` per query); several groups; merge of three partial states in both
  orders against a brute-force reference; intermediate-result round trip; accounted fill
  and merge under an allocation account (preflight, in-place arena compaction, no leaked
  bytes); state codec including malformed input; id text of every supported id type;
  configuration errors; executors share one parsed configuration (parsed once under
  concurrent first use, dropped after the last executor is freed, errors not kept).
  Binder and compile tests cover the constant-argument rule and the configuration
  encoding.
- **BVT** (`vector/vector_matmul.sql`): both formats; `WHERE`, `GROUP BY`, empty input,
  string and `uuid` ids, the relational form, prepared parameters, the error cases; and a
  400,000-row table scanned by parallel pipelines (partial states merged by `merge group`),
  where the ids and ranks equal the reference (0 mismatches for `vecf8` and `vecf4`).
  400,000 rows is about 49 blocks, enough for a single CN to scan with several pipelines
  (4 group pipelines on the 8-core development machine), which is the topology the merge
  path needs; the insert takes 1.7 s and each query under 0.4 s.
- **Multi CN** (`etc/launch-multi-cn`): the BVT passes; on an 8,000,000-row table (above
  the 512-block multi-CN threshold) the plan runs a remote scope on each CN, the partial
  states are serialized to the merging CN, and the result equals the reference.
- **Finiteness** (`types/vecblock_test.go`): large magnitudes near the float32 maximum,
  both signs, alone and in mixed blocks, in both formats — the encoder either rejects
  with "out of range" or the cell parses, every element decodes finite and the text
  round-trips; ±MaxFloat32 is rejected in MXFP8 and accepted in NVFP4; crafted cells
  (MXFP8 scale 2^120 with element 256; NVFP4 global × block scale overflowing) are
  rejected, with a finite neighbour accepted.
- **Memory admission** (`aggexec/vector_matmul_test.go`, CPU build, shape-only fake engine
  through the real preflight/fill path under an allocation account): with room, the native
  host bytes and tile buffers are charged, counted in `Size` and released by `Free` (account
  and pool back to zero); without room for the native memory, or for the tile after the
  native memory, or when the engine cannot be created, the fill fails (the created engine is
  closed and its charge released).
- **Unit, plain types** (`aggexec/vector_matmul_test.go`): `vecf32`, `vecf16`, `vecbf16`,
  `vecint8`, `vecuint8` against a brute-force dot-product reference; integer queries out
  of range or fractional are rejected. Function resolution and binder tests accept the
  plain types and reject `vecf64`.
- **Unit, metrics** (`aggexec/vector_matmul_test.go`): the options parse (`metric` values,
  ignored keys, non-object text and unknown metrics rejected); the reported distances of
  each metric over `vecf32`, `vecf8` and `vecint8` against a float64 reference computed
  directly (`−dot`, `1 − cos` with 1 for a zero vector, `Σ(x − q)²`), nearest first, a row
  equal to a query at distance 0. The GPU-against-CPU tests run every metric: block-scaled
  data within fp32 tolerance with equal id sets, small-integer plain types byte for byte.
- **GPU engine** (`cgo/cuvs/test/blockscaled_matmul_test.cu`, the `test_blockscaled_matmul`
  executable): both block-scaled formats against a double-precision dequantized reference,
  dimensions 4–768 (K padding), 1 to 300 rows (row padding, a 1-row tile, tile reuse), 1
  and 3 queries, non-unit vecf4 global scales; the five plain formats against a
  double-precision reference (integer formats exact), dimensions 4, 33, 768, 1 to 300
  rows, 1 and 3 queries; `run_topk` against the full scores of `run` for MXFP8, NVFP4,
  F32, int8 and uint8 (exact kept scores, every row above the `k`-th score kept, ties at
  the `k`-th score flagged with the full column, `k` above the tile size); cosine and
  squared L2 for all seven formats against a double-precision reference computed directly
  (a zero vector, a row equal to a query, partial last tiles), and `run_topk` under both;
  the C API and its errors (an unknown metric included); the device baseline (compute capability 7–9 rejected, 10–12 accepted), the
  eligible-device count against the visible devices, and `host_bytes` for two shapes
  computed by hand.
- **GPU binding** (`pkg/cuvs/blockscaled_matmul_test.go`): engine scores equal the CPU
  kernel (`VecBlockDot`) over the same cells for both formats, dimensions 4–768, 1 and 5
  queries; `RunTopK` against `Run` for MXFP8, NVFP4, int8 and uint8, including tied
  queries; buffer and argument errors.
- **GPU aggregate** (`aggexec/vector_matmul_gpu_test.go`): the executor with `gpu_mode`
  on and off over the same rows — one group (GPU top-k) and three groups (full scores),
  tiles drained mid-batch, a merge from an executor whose rows are still in its tile, an
  intermediate-result round trip — returns the same top-k; for the plain types over small
  integer values (many tied scores) the GPU JSON equals the CPU JSON byte for byte.
- **GPU BVT** (`gpu_cases/vector/vector_matmul_gpu.sql`): the same queries under
  `gpu_mode = 1` and `0` return identical JSON (values exact in both formats); the
  400,000-row vecf8 table, scored in GPU tiles by parallel pipelines, equals the reference;
  `vecf32`, `vecf16`, `vecbf16`, `vecint8` and `vecuint8` return identical JSON in both
  modes (`vecuint8` values up to 255, which exercise the shift correction), and a
  200,000-row `vecf32`/`vecint8`/`vecuint8` table equals the reference.
  The CPU BVTs (`vector/vector_matmul.sql`, `dtype/vecblock.sql`) also pass on a GPU build,
  where they run on the GPU.

Performance — 50,000 × 768 rows, top 10, single CN (8 pipelines), RTX 5070 Laptop;
`gpu_mode = 1` against `0`, the same top-10 ids in every case; measured with `run` (full
scores to the host), before `run_topk`:

| Queries | vecf8 GPU | vecf8 CPU | vecf4 GPU | vecf4 CPU |
|---------|-----------|-----------|-----------|-----------|
| 1       | 62 ms     | 25 ms     | 77 ms     | 23 ms     |
| 16      | 66 ms     | 249 ms    | 58 ms     | 260 ms    |
| 128     | 123 ms    | 1,893 ms  | 139 ms    | 2,033 ms  |
| 512     | 404 ms    | 6,565 ms  | 294 ms    | 7,059 ms  |
| 1,024   | 648 ms    | 14,063 ms | 703 ms    | 15,093 ms |

CPU time grows with the query count (about 14 ms per query over 50,000 rows); the GPU is
3.8–4.5× faster at 16 queries and about 21× at 1,024. A single query is faster on the CPU:
each pipeline pays the engine setup (cuBLASLt handle, tile and workspace allocation,
query upload). At large batches the host-side top-k over the copied scores dominated the
GPU time, which `run_topk` removes (below).

Performance and recall — `wiki_all` 1M × 768, rows unit-normalized (`normalize_l2` over a
`vecf32` table, cast to each type), 1,000 queries in one `vector_matmul`, top 10,
`gpu_mode = 1`, single CN, RTX 5070 Laptop. Recall@10 is against the exact top 10 over the
normalized vectors, and against the dataset's L2 ground truth over the raw vectors:

| Type | Bytes/row | `run`, per-executor parse: runs | `run_topk`, shared parse: runs | Recall@10, normalized exact | Recall@10, raw L2 ground truth |
|------|-----------|---------------------------------|--------------------------------|-----------------------------|--------------------------------|
| `vecf32` | 3,072 | 14.91 / 5.73 / 5.04 s | 2.91 / 3.06 / 2.85 s | 0.9999 | 0.701 |
| `vecbf16` | 1,536 | 4.20 / 3.99 / 4.17 s | 1.87 / 1.79 / 1.75 s | 0.9976 | 0.701 |
| `vecf8` | 804 | 2.98 / 4.19 / 3.92 s | 1.44 / 1.43 / 1.29 s | 0.964 | 0.700 |
| `vecf4` | 444 | 3.75 / 3.65 / 3.72 s | 1.35 / 1.09 / 1.00 s | 0.893 | 0.687 |

Recall is the same in both columns. With `run`, warm runs took about 4 s for every type:
the 10⁹ scores were copied to the host and passed through the per-query heaps (for
`vecf4`, 39 s of heap time summed over the pipelines), and each executor that received a
partial state for the merge parsed the 16 MB query JSON again, one after another
(about 1.1 s). With `run_topk` and the shared configuration, `vecf4` takes about 1.1 s:
about 0.5 s parsing the SQL text of 1,000 inline query vectors (a user variable avoids
most of it), 0.15 s converting the queries, and 0.5 s of scan, matmul and top-k. For
`vecf32` the scan of 3 GB of vectors dominates. The raw-L2 column is about 0.70 for every
type; normalization changes the ranking, independent of the format.

Every metric over the same tables, reloaded from the CSV (`vecf32`, then `normalize_l2` and
a cast to each type), with the GEMM-expansion cosine and squared L2 of the Decisions. The
1,000 queries are passed in a user variable (`set @q = ...`, then `vector_matmul(10, id,
embedding, @q, ...)`), so the times are the statement's execution; five warm runs after one
run that reads the table. The inline column passes the same queries as SQL text:

| Type | Metric | Runs, queries in `@q` | Median | Inline SQL, median | Recall@10, normalized exact | Recall@10, raw L2 ground truth |
|------|--------|-----------------------|--------|--------------------|-----------------------------|--------------------------------|
| `vecf32` | `inner_product` | 2.54 / 2.43 / 2.49 / 2.44 / 2.47 s | 2.47 s | 3.11 s | 0.9999 | 0.701 |
| `vecf32` | `cosine` | 2.53 / 2.50 / 2.50 / 2.46 / 2.75 s | 2.50 s | | 0.9999 | 0.701 |
| `vecf32` | `l2sq` | 2.33 / 2.37 / 2.48 / 3.78 / 2.26 s | 2.37 s | | 0.9999 | 0.701 |
| `vecbf16` | `inner_product` | 1.48 / 1.42 / 1.36 / 1.35 / 1.26 s | 1.36 s | 2.46 s | 0.9976 | 0.701 |
| `vecbf16` | `cosine` | 1.64 / 2.02 / 1.64 / 1.44 / 1.45 s | 1.64 s | | 0.9984 | 0.701 |
| `vecbf16` | `l2sq` | 1.37 / 1.31 / 1.37 / 1.40 / 1.43 s | 1.37 s | | 0.9983 | 0.701 |
| `vecf8` | `inner_product` | 0.85 / 0.83 / 0.86 / 0.86 / 0.84 s | 0.85 s | 1.35 s | 0.964 | 0.700 |
| `vecf8` | `cosine` | 0.94 / 0.93 / 0.94 / 0.90 / 0.95 s | 0.94 s | | 0.974 | 0.701 |
| `vecf8` | `l2sq` | 0.89 / 0.88 / 0.89 / 0.88 / 0.94 s | 0.89 s | | 0.974 | 0.700 |
| `vecf4` | `inner_product` | 0.49 / 0.46 / 0.45 / 0.50 / 0.46 s | 0.46 s | 0.83 s | 0.893 | 0.687 |
| `vecf4` | `cosine` | 0.62 / 0.65 / 0.61 / 0.61 / 0.65 s | 0.62 s | | 0.920 | 0.693 |
| `vecf4` | `l2sq` | 0.51 / 0.51 / 0.49 / 0.53 / 0.50 s | 0.51 s | | 0.915 | 0.692 |

Inner-product recall equals the table above. The inline column adds the handling of the
16 MB statement text, which this server also records into `system.statement_info`; it
varies most for the wider types (`vecbf16` inline runs 1.72 to 9.00 s). Engine creation
allocates the tile staging uninitialized (written before it is read): 3.2 ms per engine
for this shape, where zero-filling it took about 20 ms, eight engines per query. On
`vecf8` and `vecf4` cosine and squared L2 recall more than the inner product: the
quantized rows are no longer of unit norm, so the dot product ranks partly by row norm;
cosine divides the norm out and squared L2 includes it.

## Decisions

- `vecf4` = NVFP4 (e2m1, unsigned E4M3 16-block scale, fp32 global per vector in the
  cell header); `vecf8` = MXFP8 (e4m3, E8M0 32-block scale, header global fixed at 1).
- Cell = 12-byte header (version, format, reserved, `N`, `g`) + scales + elements.
- Element codecs = the scalar `Float8`/`Float4`; scale codecs = new E8M0 + `Float8` e4m3.
- Cells store scales per row in block order; the GPU engine re-lays them into the tiled
  scale tensor.
- The GPU engine is a cuBLASLt matmul with the dataset as operand A, plus device kernels
  for the row statistics and the metric; the host packs bytes and reads cell headers, and
  does no score arithmetic.
- Metrics: `inner_product`, `cosine` and `l2sq`, each reporting the value of the matching
  SQL function (`inner_product` = `−dot`, `cosine_distance`, `l2_distance_sq`), nearest
  first; plain L2 and L1 are not provided.
- `vector_matmul` also takes `vecf32`, `vecf16`, `vecbf16`, `vecint8` and `vecuint8`,
  through the same engine with plain formats; `vecf64` is rejected (the engine has no fp64
  format). `vecuint8` runs on the int8 path with the shift correction.
- The per-tile top-k runs on the GPU (`select_k`) for one-group tiles; mixed-group tiles
  copy the full scores. Rows tied at the `k`-th score beyond the kept ones bring back their
  query's full column, so the GPU top-k equals the selection over the full scores.
- The parsed configuration is shared by the executors of a query and reference-counted;
  the configuration bytes and their encoding are unchanged. Plain queries are held once,
  as the engine's packed cells, with the CPU scorer reading typed views of them. Every
  executor holding the configuration charges its allocation account for the retained
  query storage and its score scratch, so a shared configuration is charged to each
  holder; a denial fails the aggregate's creation, and the charge returns when the
  executor is freed.
- v1 is a function call per tile with no index, residency or dataset cache; the cuVS
  brute-force index is unchanged.
- SQL surface = one aggregate, `vector_matmul`; partials per pipeline and the cross-CN
  merge come from MO's two-phase aggregation. A `CROSS APPLY` table function cannot emit a
  row at end of input, which rules out a table-function + merge-aggregate pair.
- Result = JSON with string ids.
- The CPU computes each distance with the scalar SQL function's kernel; the GPU
  accumulates in fp32 (cuBLASLt `CUBLAS_COMPUTE_32F`).
- GPU dispatch follows the session's `gpu_mode` only. The `options` argument is a JSON
  object of which only `metric` and `query_format` are read; a tile size is an internal choice bounded by the
  allocation account, not a user setting.
- Transport keeps the stored cell: CDC and ISCP replication SQL, `data branch` merge SQL
  and `INTO OUTFILE` / external-table writes (CSV and JSONL) carry `vecf8`/`vecf4` values
  as the exact text, which replays or reloads to the same bytes. The
  decoded values (`'[…]'`) would be quantized again: a `vecf4` global scale follows the
  decoded maximum, so a replayed value can move (an element stored as `6.2606535` became
  `6.8867183`). Query output keeps the decoded values.
- Non-finite values are rejected at build, including finite inputs that would decode to
  ±Inf, and cell parsing rejects any cell that decodes to a non-finite value.
- The GPU engine runs only on compute capability 10.0 or newer, checked once per process.
  Rows are scored on the CPU only when the session has `gpu_mode` off or the box has no
  such device; the CPU path is the reference for verification and benchmarks.
- With an eligible device enabled there is no CPU fallback: every row is scored on the
  GPU or the query fails. The engine's native host memory and the tile buffers are
  admitted by the aggregate's allocation account before allocation, and a denial fails the
  query; so do device memory, CUDA and cuBLASLt errors at creation or while scoring.
- **GPU cosine and squared L2 are the GEMM expansion, with its precision near 0.**
  - *Decision owner:* `cpegeric` (Eric), 2026-10-06. Final; reviewers and agents cite this
    entry instead of re-raising it.
  - *Context.* A GEMM only multiplies and adds: it yields `x·q` and cannot form the
    differences `xᵢ − qᵢ`. Cosine and squared L2 on the GPU are therefore assembled from it:
    `1 − x·q / (|x||q|)` and `|x|² + |q|² − 2·x·q`, with the norms in double. Each term
    carries the fp32 GEMM's rounding, about `dim × 2^-24` relative to `|x||q|`; when the
    vectors are close the terms cancel and that rounding is what remains. The scalar
    functions do not have this: `Σ(xᵢ − qᵢ)²` subtracts first, a difference of close values
    is exact, and the result's error is relative to the distance itself; the CPU's cosine
    takes `x·q` and the norms by the same operations, so equal vectors give exactly 0.
  - *Decision.* The GPU's cosine and squared L2 are the expansion's: within fp32
    summation-order tolerance of the scalar function, relative to the squared norms. The
    GEMM is kept in the fp32 range by exact power-of-two rescaling (rows outside
    [2^-60, 2^60] in squared norm). Consequences: a distance near 0 carries the GEMM's
    rounding (a row equal to its query is at about 1e-7 cosine distance, not 0); rows whose
    distances to a query differ by less than that rounding can rank in either order
    (duplicates, near-duplicates, a small difference beside a large shared coordinate); a
    squared L2 whose rounding leaves the float range is an overflow error. Rows whose
    distances differ by more than the rounding rank as the scalar functions rank them.
    Inner product has no subtraction and is the GEMM's result.
  - *Rejected (POISON — do not reintroduce): exact recomputation of near-zero distances on
    the device.* Built and measured: pairs within the GEMM's error of 0 recomputed from the stored values in
    double, a warp per pair. Near-zero pairs are the common case — the nearest rows of a
    top-k are the smallest distances, and deduplication and self-matching queries are made
    of them — so its cost depends on the data: on 1M `vecf32(768)` rows and 128 queries,
    cosine took 1.37 s end to end with no near pair and 10.1 s with every pair near (7×,
    FP64 at 1/64 of FP32 on a GeForce). Exact values for every pair would also need the
    stored values beside any rescaled operand (a second copy of the tile).
  - *Rejected: re-scoring on the CPU,* which runs the work twice and makes the result
    depend on which path ran (see the GPU dispatch decision above).
  - *Exact results* are the CPU path's (`gpu_mode = 0`), which computes each distance with
    the scalar functions' kernels, and the scalar functions themselves: ordering near-ties
    exactly takes re-ranking the top-k rows with `cosine_distance` / `l2_distance_sq`.
- The engine looks up a cuBLASLt algorithm for every tile shape it can run (rows in
  buckets of 128 × 2^i up to the tile capacity) when it is created, so a shape the device
  has no algorithm for fails before any row is scored.
