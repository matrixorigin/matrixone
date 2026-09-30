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

## GPU engine: `cgo/cublaslt`

The engine computes one thing: the fp32 **dot-product matrix** `S = D × Qᵀ` of a packed
dataset tile `D` and packed queries `Q`, with `cublasLtMatmul` block-scaled matmul. It
has no metric, filter or top-k logic.

Code layout — all GPU code for these types lives in two directories, following
`cgo/cuvs` + `pkg/cuvs`; both are separate from `cgo/cuvs`/`pkg/cuvs` (untouched) and
`cgo/cuda` (the `xcall` kernels):

- `cgo/cublaslt` — all C++/CUDA code: the engine (built only with `MO_CL_CUDA=1`) and
  `test/` (CUDA programs, including the Float8/Float4 golden-data generator).
- `pkg/cublaslt` — all Go GPU code: the cgo bindings and the Go API that `vector_matmul`
  calls. Every file carries `//go:build gpu`; there is no CPU stub. Callers split by build
  tag (`*_gpu.go` imports `pkg/cublaslt`, `*_cpu.go` does not), as the cuVS table
  functions do.

Everything else is CPU code in its usual place: the cell codec in `pkg/container/types`,
the SQL functions under `pkg/sql`, and the CPU dot product.

cuBLASLt call sequence:

| Step | Call | Setting |
|------|------|---------|
| 1 | `cublasLtCreate` | once per process |
| 2 | `cublasLtMatmulDescCreate` | `CUBLAS_COMPUTE_32F`, scale type `CUDA_R_32F` |
| 3 | `cublasLtMatmulDescSetAttribute` | `TRANSA=T`, `TRANSB=N`; `A/B_SCALE_MODE` = `VEC32_UE8M0` (vecf8) or `VEC16_UE4M3` (vecf4); `A/B_SCALE_POINTER` = tiled scale tensors |
| 4 | `cublasLtMatrixLayoutCreate` | A: element type, K × rows(D), ld = K; B: K × rows(Q), ld = K; D: `CUDA_R_32F`, rows(D) × rows(Q) |
| 5 | `cublasLtMatmulPreferenceCreate` / `SetAttribute` | workspace limit |
| 6 | `cublasLtMatmulAlgoGetHeuristic` | cached per shape |
| 7 | `cublasLtMatmul` | alpha = 1.0, beta = 0 (per-vector `g` is applied by the caller) |

Contract (each point measured on sm_120 with cuBLASLt 13.6):

- **Operand orientation: dataset = A, queries = B.** With queries as A, a single query
  (M = 1) returns silently wrong results and M = 2 has no algorithm; with the dataset as A,
  every query count (1, 2, 3, 5, …) matches the CPU reference, up to 100,000 dataset rows.
- **Scale tensor layout is tiled, not per row.** Per-row scales produce wrong results
  (relative error ≈ 1). The correct layout pads rows to 128 and blocks to 4, and stores
  128 × 4 tiles of 512 bytes:

  ```
  S      = K / blockSize,  Spad = roundup(S, 4),  rows padded to roundup(R, 128)
  offset(r, s) = ((r / 128) * (Spad / 4) + s / 4) * 512
               + (r % 32) * 16 + ((r % 128) / 32) * 4 + (s % 4)
  ```

  The engine copies each row's scales to the device and applies this re-layout on the GPU.
- **K padding.** The dimension is padded with zero elements to a multiple of 32 (NVFP4 at
  K = 48 has no algorithm; 32, 96, 512, 768, 1536 run). Storage keeps the true `N`.
- **Precision.** Against a double-precision CPU reference: MXFP8 relative error ≤ 1e-6,
  NVFP4 exact, at fp32 output.

Engine state kept across calls, per caller instance: a CUDA stream, the cuBLASLt handle
and workspace, and the heuristic algorithm per shape. No dataset is cached; every call
uploads its tile.

Build integration:

- `cgo/cublaslt/Makefile` includes `../gpu-toolchain.mk` and builds with the Pixi `nvcc`.
- `cgo/Makefile` links `cublaslt/*.o` into libmo next to `cuvs/*.o`.
- `cgo/gpu-toolchain.mk` adds `-lcublasLt` to `MO_GPU_LDFLAGS` (consumed by the top-level
  Go link flags and `mo-cgo-test`).
- The native provenance/contract tests treat `cublaslt` sources as native inputs.
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
| `inner_product(a, b)` | fp32 dot product over dequantized values; `a`/`b` each `vecf8`, `vecf4` or `vecf32` |
| `l2_distance`, `cosine_distance`, `l1_distance`, … | not supported |
| `SUM`/`AVG`/`MIN`/`MAX` over vectors | not supported (no vector type has them) |
| comparison, `ORDER BY`, `GROUP BY`, `DISTINCT`, join keys | not supported |
| primary key, partition key, secondary/unique index, vector index | rejected at DDL |
| `LOAD` | CSV text `"[…]"`; Parquet `LIST<FLOAT/DOUBLE>` and text columns, quantized per row |

The promotion is implemented in these operations only; there is no implicit
`vecf8`/`vecf4` → `vecf32` cast, so unsupported functions reject the types.

### Batch dot-product search

Two functions, sharing one JSON result format.

#### `vector_matmul` (table function)

```sql
vector_matmul(params, src_id, src_vec, queries) → (result JSON)
```

| Argument | Type | Meaning |
|----------|------|---------|
| `params` | constant JSON string | `{"limit": k}` (required); `"mode": "auto" \| "gpu" \| "cpu"` (default `auto`: GPU when the build and a device are present, else CPU); `"tile_bytes": n` (default 64 MiB) |
| `src_id` | source column | the row key, any type |
| `src_vec` | source column | `vecf8(N)` or `vecf4(N)` |
| `queries` | constant JSON string | array of query vectors `[[…], …]`, each of length `N`; quantized once to the column's format |

It is fed by `CROSS APPLY` from the dataset table and runs without `IsSingle`: one
instance per scan pipeline, so the scan keeps its full parallelism. Each instance appends
incoming rows to its own host tile; when the tile reaches `tile_bytes` or input ends it
calls the engine, multiplies each dot product by `g_d × g_q`, and keeps the top `limit`
scores per query. At end of input it emits one row holding its partial result;
`vector_matmul_merge` combines the partials of all instances on all CNs. NULL vectors
are skipped. A `WHERE` on the source table filters rows before they reach the function.

Instances on one CN share its GPU: each uses its own CUDA stream and cuBLASLt
handle/workspace, and host and device tile memory scale with the instance count
(`instances × tile_bytes`).

In `cpu` mode (and on CPU builds) the same function computes the dot products with the
CPU kernel over the dequantized values in fp32; results match the GPU within fp32
summation-order tolerance.

#### `vector_matmul_merge` (aggregate)

```sql
vector_matmul_merge(result JSON, limit) → JSON
```

Merges any number of partial results of the same queries into the final top `limit` per
query. Its input and output use the same format, so it is independent of how many CNs
or instances produced partials.

#### Result format

```json
[
  [ ["17", 0.93], ["4",  0.91] ],
  [ ["8",  0.88], ["17", 0.85] ]
]
```

- Outer array: one entry per query, in input order (position = query id).
- Inner array: that query's hits, score descending, ties by id ascending; at most
  `limit` entries, `[]` when there is no input row.
- Hit: a pair `[id, score]`.
  - Position 0, `id`: the source key as a JSON string (exact for 64-bit integers and
    non-integer keys).
  - Position 1, `score`: the dot product, a JSON number.
  - A field added later takes position 2; positions 0 and 1 keep their meaning.

#### Usage

```sql
SELECT vector_matmul_merge(p.result, 10) AS result
FROM (SELECT f.result
      FROM t AS src
      CROSS APPLY vector_matmul('{"limit":10}', src.id, src.v, '[[0.12, …], [0.33, …]]') AS f
      WHERE src.category = 'news') p;
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
- `vector_matmul` and `vector_matmul_merge` produce and consume the same result format;
  merging a single partial returns it unchanged.

## Phasing

- **P1 — storage (CPU):** `vecf8`/`vecf4` types, header codec, E8M0 scale codec,
  pack/unpack, string cast, display; a golden test pinning the element codecs to the CUDA
  outputs above.
- **P2 — column operations (CPU):** casts, arithmetic, `ANY_VALUE`/`GROUP_CONCAT`,
  `inner_product`, DDL rejections, CSV/Parquet `LOAD`.
- **P3 — CPU functions:** `vector_matmul` in `cpu` mode (fp32 dot product over dequantized
  blocks) and `vector_matmul_merge`; UT + BVT.
- **P4 — GPU engine:** `cgo/cublaslt` (tiled scale re-layout, K padding, dataset-as-A
  matmul) and `vector_matmul` `gpu`/`auto` mode; acceptance cases from the verification
  harness (both formats, 1-query batches, non-multiple-of-128 rows, 100,000-row tiles),
  compared with P3 within fp32 tolerance.

## Testing

Oracle: the final JSON of `vector_matmul_merge` equals a single-partial reference (CPU mode
over the whole table) — the same ids in the same order per query, ties broken by `id`,
scores within fp32 summation-order tolerance.

- **Unit (`vector_matmul_merge`):** one partial (returned unchanged), several partials with
  overlapping and disjoint ids, partials shorter than `limit`, empty partials (`[]`), equal
  scores across partials (tie order), a query count mismatch between partials (error).
- **Single CN** (`etc/launch`, BVT): `vector_matmul` + `vector_matmul_merge` on `vecf8` and
  `vecf4` in `cpu` mode, and `auto`/`gpu` mode on a GPU build; `WHERE` pre-filter; NULL
  vectors; one and several queries; a table smaller than `limit`.
- **Multi CN** (`etc/launch-multi-cn`, BVT; `etc/docker-multi-cn-local-disk` for separate
  CN processes): the same cases on a table large enough to be scanned by more than one
  CN. The test asserts that the partial count (`SELECT count(*)` over the `vector_matmul`
  output) is greater than 1, so the merge is exercised, and that the merged result equals
  the single-partial reference.

## Decisions

- `vecf4` = NVFP4 (e2m1, unsigned E4M3 16-block scale, fp32 global per vector in the
  cell header); `vecf8` = MXFP8 (e4m3, E8M0 32-block scale, header global fixed at 1).
- Cell = 12-byte header (version, format, reserved, `N`, `g`) + scales + elements.
- Element codecs = the scalar `Float8`/`Float4`; scale codecs = new E8M0 + `Float8` e4m3.
- Cells store scales per row in block order; the GPU engine re-lays them into the tiled
  scale tensor.
- The GPU engine is dot-product matmul only, via cuBLASLt, with the dataset as operand A.
- v1 is a function call per tile with no index, residency or dataset cache; the cuVS
  brute-force index is unchanged.
- SQL surface = `vector_matmul` (one partial per scan pipeline, no `IsSingle`) + `vector_matmul_merge`
  (final), JSON result format with string ids.
- CPU dot-product accumulation = fp32.
- Non-finite values are rejected at build.
