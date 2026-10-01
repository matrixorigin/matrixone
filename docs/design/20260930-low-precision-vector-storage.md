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
- **Non-finite is never stored.** NaN/Inf input is rejected at build, per the repo-wide
  finite-persistence rule.

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

The GEMM is exact on the stored values; the ranking is approximate relative to the
original fp32 vectors because the storage is quantized. `vecf8` is the quality format,
`vecf4` the memory format.

## GPU engine: cuBLASLt in `cgo/cuvs`

The engine computes one thing: the fp32 **dot-product matrix** `S = D × Qᵀ` of a packed
dataset tile `D` and packed queries `Q`, with `cublasLtMatmul`: a block-scaled matmul for
`vecf8`/`vecf4`, a plain matmul for `vecf32`, `vecf16`, `vecbf16`, `vecint8` and
`vecuint8`. It has no metric, filter or top-k logic.

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
on the host (`x ^ 0x80` = x − 128), the row and query sums of the shifted values are kept,
and the int32 result is corrected as `x·q = x′·q′ + 128 (Σx′ + Σq′) + 128² · dim` in int64.
Integer dot products are exact; they are rounded once to fp32 when written out.

Code layout — the engine lives with the other GPU code in `cgo/cuvs` + `pkg/cuvs`:

- `cgo/cuvs/blockscaled_matmul.hpp` — the engine class (header only, C++): packs cells
  into padded element rows and tiled scales, owns the device buffers, CUDA stream and
  cuBLASLt handle, and runs the matmul.
- `cgo/cuvs/blockscaled_matmul_c.h` / `blockscaled_matmul_c.cpp` — the C API
  (`gpu_blockscaled_matmul_new/_run/_max_rows/_destroy`), compiled into libmo with the
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
| 6 | `cublasLtMatmulAlgoGetHeuristic` | per tile |
| 7 | `cublasLtMatmul` | alpha = 1, beta = 0; the per-vector `g` of row and query is applied in double when the scores are copied out, then rounded once to fp32 (plain formats have `g` = 1) |

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
workspace, the queries on the device, and host and device buffers for one tile. The
device memory is claimed through `device_memory_governor` before it is allocated. No
dataset is cached; every tile is uploaded.

Dispatch: the compiler reads the session's `gpu_mode` and stores it in the aggregate's
configuration. An executor uses the engine when `gpu_mode` is on and the build has a
visible device; otherwise it scores on the CPU. Rows are buffered in a tile of at most
64 MiB (cells plus scores, up to 65,536 rows), scored when the tile is full, and the tile
is drained before the states are read (final result, merge, intermediate result, spill).

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
| `summation`, `l1_norm`, `l2_norm`, `subvector` | not supported (as for the other narrow vector types) |
| `SUM`/`AVG`/`MIN`/`MAX` over vectors | not supported (no vector type has them) |
| `ORDER BY`, window `ORDER BY` | as for `vecf32`: by the dequantized values, element-wise |
| `GROUP BY`, `DISTINCT`, window `PARTITION BY` | by cell bytes (the encoding of a given input is deterministic) |
| comparison operators (`=`, `<`, …) | not supported (as for every vector type) |
| primary key, partition key, secondary/unique index, vector index | rejected at DDL |
| `LOAD` | CSV text `"[…]"`; Parquet `LIST<FLOAT/DOUBLE>` and text columns, quantized per row |

The promotion is implemented in these operations only; there is no implicit
`vecf8`/`vecf4` → `vecf32` cast, so unsupported functions reject the types.

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

### Batch dot-product search

#### `vector_matmul` (aggregate)

```sql
vector_matmul(topk, src_id, src_vec, queries [, options]) → JSON
```

| Argument | Type | Meaning |
|----------|------|---------|
| `topk` | constant integer | the hits kept per query, 1–16384 |
| `src_id` | column | the row key: an integer, `char`/`varchar`/`text` or `uuid` column |
| `src_vec` | column | `vecf8(N)`, `vecf4(N)`, `vecf32(N)`, `vecf16(N)`, `vecbf16(N)`, `vecint8(N)` or `vecuint8(N)`; `vecf64` is rejected |
| `queries` | constant string or JSON | array of query vectors `[[…], …]`, each of length `N`; converted once to the column's type (quantized for `vecf8`/`vecf4`); for `vecint8`/`vecuint8` every value is an integer in the type's range, otherwise an error |
| `options` | optional constant JSON string | `"mode": "auto" \| "cpu"`, `"tile_bytes": n`; validated, not used for dispatch, which follows the session's `gpu_mode` |

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

On the CPU (gpu_mode off, a CPU build, or no visible device) the dot products come from the
CPU distance kernels: `VecBlockInnerProduct` over the quantized cells for `vecf8`/`vecf4`,
the column type's inner-product kernel (`metric.ResolveDistanceFn`) for the other types;
results match the GPU within fp32 summation-order tolerance. An overflowing dot product (NaN, mapped to the +Inf distance)
ranks last; a non-finite score in the result is an overflow error, since JSON has no
infinity.

#### Result format

```json
[
  [ ["17", 0.93], ["4",  0.91] ],
  [ ["8",  0.88], ["17", 0.85] ]
]
```

- Outer array: one entry per query, in input order (position = query id).
- Inner array: that query's hits, score descending, ties by the id text in byte order
  (so `"10"` before `"9"`); at most `topk` entries, `[]` when there is no input row.
- Hit: a pair `[id, score]`.
  - Position 0, `id`: the source key as a JSON string (exact for 64-bit integers and
    non-integer keys).
  - Position 1, `score`: the dot product, a JSON number.
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

The score is the dot product. Cosine similarity equals the dot product for unit-normalized
vectors, the usual form of embeddings. L2, cosine on non-normalized vectors and L1 are not
provided by these functions.

## Hardware & toolchain

- FP4 needs NVIDIA Blackwell with hardware FP4: `sm_100` (B200) or `sm_120` (GeForce RTX
  50). MXFP8 block-scaled matmul also runs on Hopper.
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
- The engine returns dot products only; metric, filtering and top-k live in the SQL
  functions.
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
cast to the column's format — the same ids in the same order, scores within fp32
summation-order tolerance.

- **Unit** (`aggexec/vector_matmul_test.go`): top-k and tie order; NULL ids and vectors;
  empty groups (`[]` per query); several groups; merge of three partial states in both
  orders against a brute-force reference; intermediate-result round trip; accounted fill
  and merge under an allocation account (preflight, in-place arena compaction, no leaked
  bytes); state codec including malformed input; id text of every supported id type;
  configuration errors. Binder and compile tests cover the constant-argument rule and the
  configuration encoding.
- **BVT** (`vector/vector_matmul.sql`): both formats; `WHERE`, `GROUP BY`, empty input,
  string and `uuid` ids, the relational form, prepared parameters, the error cases; and a
  400,000-row table scanned by parallel pipelines (partial states merged by `merge group`),
  where the ids and ranks equal the reference (0 mismatches for `vecf8` and `vecf4`).
- **Multi CN** (`etc/launch-multi-cn`): the BVT passes; on an 8,000,000-row table (above
  the 512-block multi-CN threshold) the plan runs a remote scope on each CN, the partial
  states are serialized to the merging CN, and the result equals the reference.
- **Unit, plain types** (`aggexec/vector_matmul_test.go`): `vecf32`, `vecf16`, `vecbf16`,
  `vecint8`, `vecuint8` against a brute-force dot-product reference; integer queries out
  of range or fractional are rejected. Function resolution and binder tests accept the
  plain types and reject `vecf64`.
- **GPU engine** (`cgo/cuvs/test/blockscaled_matmul_test.cu`, the `test_blockscaled_matmul`
  executable): both block-scaled formats against a double-precision dequantized reference,
  dimensions 4–768 (K padding), 1 to 300 rows (row padding, a 1-row tile, tile reuse), 1
  and 3 queries, non-unit vecf4 global scales; the five plain formats against a
  double-precision reference (integer formats exact), dimensions 4, 33, 768, 1 to 300
  rows, 1 and 3 queries; the C API and its errors.
- **GPU binding** (`pkg/cuvs/blockscaled_matmul_test.go`): engine scores equal the CPU
  kernel (`VecBlockDot`) over the same cells for both formats, dimensions 4–768, 1 and 5
  queries.
- **GPU aggregate** (`aggexec/vector_matmul_gpu_test.go`): the executor with `gpu_mode`
  on and off over the same rows — several groups, tiles drained mid-batch, a merge from an
  executor whose rows are still in its tile, an intermediate-result round trip — returns
  the same top-k.
- **GPU BVT** (`gpu_cases/vector/vector_matmul_gpu.sql`): the same queries under
  `gpu_mode = 1` and `0` return identical JSON (values exact in both formats); the
  400,000-row vecf8 table, scored in GPU tiles by parallel pipelines, equals the reference;
  `vecf32`, `vecf16`, `vecbf16`, `vecint8` and `vecuint8` return identical JSON in both
  modes (`vecuint8` values up to 255, which exercise the shift correction), and a
  200,000-row `vecf32`/`vecint8`/`vecuint8` table equals the reference.
  The CPU BVTs (`vector/vector_matmul.sql`, `dtype/vecblock.sql`) also pass on a GPU build,
  where they run on the GPU.

Performance — 50,000 × 768 rows, top 10, single CN (8 pipelines), RTX 5070 Laptop;
`gpu_mode = 1` against `0`, the same top-10 ids in every case:

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
query upload). At large batches the host-side top-k over the copied scores dominates the
GPU time.

Performance and recall — `wiki_all` 1M × 768, rows unit-normalized (`normalize_l2` over a
`vecf32` table, cast to each type), 1,000 queries in one `vector_matmul`, top 10,
`gpu_mode = 1`, single CN, RTX 5070 Laptop. Recall@10 is against the exact top 10 over the
normalized vectors, and against the dataset's L2 ground truth over the raw vectors:

| Type | Bytes/row | Runs (3 passes) | Recall@10, normalized exact | Recall@10, raw L2 ground truth |
|------|-----------|-----------------|-----------------------------|--------------------------------|
| `vecf32` | 3,072 | 14.91 / 5.73 / 5.04 s | 0.9999 | 0.701 |
| `vecbf16` | 1,536 | 4.20 / 3.99 / 4.17 s | 0.9976 | 0.701 |
| `vecf8` | 804 | 2.98 / 4.19 / 3.92 s | 0.964 | 0.700 |
| `vecf4` | 444 | 3.75 / 3.65 / 3.72 s | 0.893 | 0.687 |

Warm runs take about 4 s for every type: the 10⁹ scores are copied to the host and pass
through the per-query top-k heaps, which dominate the time. The raw-L2 column is about
0.70 for every type; normalization changes the ranking, independent of the format.

## Decisions

- `vecf4` = NVFP4 (e2m1, unsigned E4M3 16-block scale, fp32 global per vector in the
  cell header); `vecf8` = MXFP8 (e4m3, E8M0 32-block scale, header global fixed at 1).
- Cell = 12-byte header (version, format, reserved, `N`, `g`) + scales + elements.
- Element codecs = the scalar `Float8`/`Float4`; scale codecs = new E8M0 + `Float8` e4m3.
- Cells store scales per row in block order; the GPU engine re-lays them into the tiled
  scale tensor.
- The GPU engine is dot-product matmul only, via cuBLASLt, with the dataset as operand A.
- `vector_matmul` also takes `vecf32`, `vecf16`, `vecbf16`, `vecint8` and `vecuint8`,
  through the same engine with plain formats; `vecf64` is rejected (the engine has no fp64
  format). `vecuint8` runs on the int8 path with the shift correction.
- v1 is a function call per tile with no index, residency or dataset cache; the cuVS
  brute-force index is unchanged.
- SQL surface = one aggregate, `vector_matmul`; partials per pipeline and the cross-CN
  merge come from MO's two-phase aggregation. A `CROSS APPLY` table function cannot emit a
  row at end of input, which rules out a table-function + merge-aggregate pair.
- Result = JSON with string ids.
- CPU dot-product accumulation = fp32 within a 16-element unit, fp64 across units; the
  GPU accumulates in fp32 (cuBLASLt `CUBLAS_COMPUTE_32F`).
- GPU dispatch follows the session's `gpu_mode` only; the `options` argument is validated
  and not used for dispatch.
- Non-finite values are rejected at build.
