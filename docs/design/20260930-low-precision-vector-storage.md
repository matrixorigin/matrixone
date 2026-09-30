# Low-precision float vector columns: vecf8, vecf4 (block-scaled MXFP)

Status: proposed · Issue: #20567 · Scope: new packed vector column types for quantized
ML embeddings · Depends on: the scalar `float8`/`float4` types (their e4m3/e2m1 element
codecs are reused).

## Motivation

Quantized embeddings are stored in narrow floats to cut memory and bandwidth, but FP8
(±448) and especially FP4 (±6, ~2.5 octaves of normal range) are far too narrow to hold
a real embedding directly — every component would saturate or underflow. NVIDIA's
microscaling formats (MXFP8/MXFP4) solve this by scaling a small contiguous *block* of a
vector's own dimensions against one shared scale, centering each block into the format's
representable band. This adds those as first-class vector column types:

```sql
CREATE TABLE t (v vecf4(1024));   -- or vecf8(1024)
```

The scalar `float8`/`float4` types (issue #20567) supply the element codecs; a scalar
column holds one value, these hold a scaled block-quantized vector.

**Where these pay off (and where they do not).** FP8/FP4 are **tensor-core GEMM**
formats. Their GPU acceleration is a **batch matmul** — the similarity matrix
`S = Q × Dᵀ` (a batch of query vectors against the database), i.e. **brute-force / exact**
similarity on tensor cores. They are **not** ANN-index element types: cuVS indexes
(CAGRA / IVF-Flat / IVF-PQ / brute_force) accept only `float`, `half` (fp16), and
`int8`/`uint8`, with scalar / product / binary quantization — there is no fp8/fp4 index
path and there is not expected to be one. So `vecf8`/`vecf4` target **GPU brute-force
batch similarity via cuBLASLt block-scaled GEMM**, a path distinct from `cgo/cuvs`. For
*approximate/indexed* low precision, keep using `vecint8` (cuVS scalar quantizer) or
IVF-PQ.

## The two types

| SQL type   | Element        | Block | Block scale        | Element bytes   |
|------------|----------------|:-----:|--------------------|-----------------|
| `vecf8(N)` | OCP FP8 e4m3   |  32   | E8M0 (1 byte, 2^k) | `N`             |
| `vecf4(N)` | OCP MXFP4 e2m1 |  32   | E8M0 (1 byte, 2^k) | `ceil(N/2)`     |

Both share one storage format; only the element width and packing differ. `N` is the
logical dimension, carried in the column type, independent of the cell byte length. The
32-block / E8M0 scale is exactly cuBLASLt's `CUDA_R_UE8` block-scaled MXFP layout (§GPU).

## Cell format (the contract)

Each value is one varlena cell:

```
[ header (MO metadata) | E8M0 block scales | packed elements ]
                        └──────── byte-exact OCP MXFP ────────┘
```

- **header** — fixed-size MO metadata: format version, element format (e4m3 / e2m1),
  scale format (E8M0), block size (32), logical dimension `N`, and the block-scale count
  `ceil(N/32)`. It is MO-only bookkeeping and is **not** part of the MXFP payload.
- **payload** — byte-exact OCP MXFP: `ceil(N/32)` E8M0 scales followed by the packed
  elements (e4m3 one byte each; e2m1 two nibbles per byte, `hi<<4|lo`, odd `N` pads the
  final high nibble with `0x0` = +0). Scale block `b` governs elements `[32b, 32b+32)`.

Consequence: unlike every existing vector element type, `Size ≠ N × elemSize`. The logical
dimension is authoritative from the column type / header, never derived from byte length.

## Decisions

- **Block-scaled, not per-vector.** A single per-vector scale is too coarse for FP4;
  per-32-block scaling matches MXFP and keeps each block in range.
- **Scale = E8M0 (power-of-2), block size = 32** — the OCP MXFP standard, and exactly what
  cuBLASLt consumes as `CUDA_R_UE8` scales. The write side derives
  `scale = 2^ceil(log2(absmax(block)/FORMAT_MAX))` (FORMAT_MAX = 448 for e4m3, 6 for e2m1),
  rounding up so no element overflows its block; dequant is an exact `ldexp`. E8M0 is
  coarser than an fp32 scale — accepted as the price of MXFP interop.
- **Payload is byte-exact MXFP; the GPU path strips the header and memcpys the payload into
  the cuBLASLt operand + scale tensors** — no dequant/requant, no per-element arithmetic.
  CPU-only builds dequantize per block for distance and display; the on-disk bytes are
  identical in both builds.
- **fp4 storage = MXFP4 (32-block E8M0)** by default. NVFP4 (16-block, E4M3 scale + a
  per-tensor fp32 global) is the more accurate alternative cuBLASLt also supports; it is
  an *open* storage choice (§open) — if adopted it changes the fp4 payload and header
  (scale dtype, block size, an optional global-scale field) but not the vecf8 format.
- **Non-finite is never stored.** A NaN/Inf input (which would make a block absmax
  non-finite) is rejected at build, consistent with the repo-wide finite-persistence rule.

## Invariants

- The e4m3 / e2m1 element bit layouts are the same as the scalar `float8`/`float4` types;
  packing is a pure byte/nibble shuffle with no per-element conversion.
- The payload sub-region layout and endianness match the target cuBLASLt block-scaled MXFP
  operand/scale contract exactly, so the GPU transfer stays a memcpy. Any change to that
  contract is a storage-format change and must bump the header version.
- The header never participates in the MXFP payload; stripping it must never require
  rewriting payload bytes.

## GPU compute path: cuBLASLt block-scaled GEMM

The GPU consumer is **`cublasLtMatmul` block-scaled matmul** (cuBLASLt), not cuVS. Batch
similarity is a GEMM: inner-product / cosine is `Q × Dᵀ` directly; L2 is
`‖q‖² + ‖d‖² − 2·q·d` (the cross term is the GEMM, norms precomputed). The stored payload
feeds it directly:

- **Formats.** `CUDA_R_8F_E4M3` (vecf8) and `CUDA_R_4F_E2M1` (vecf4), both with
  `CUDA_R_UE8` (E8M0) scales over 32-element blocks — identical to the stored MXFP payload.
  (NVFP4 = `CUDA_R_4F_E2M1` + `CUDA_R_UE4M3` 16-element scales + fp32 global, if adopted.)
- **Transfer = memcpy.** Per row, strip the MO header and copy the scale sub-region and the
  element sub-region into the cuBLASLt scale and operand tensors — no arithmetic. MO's
  `vector.Vector` is per-row, so this is a per-row gather of raw bytes.
- **Tiling is mandatory.** Brute force materializes `Q × Dᵀ`; the database and the result
  tile must fit VRAM. The operator tiles over the dataset (and query batch), streaming
  tiles to the GPU — required on any card, and especially on 12 GB consumer parts.
- **Exact, not approximate.** This is full-scan brute force; it does not build an index.

### Hardware & toolchain

- **Hardware: NVIDIA Blackwell with hardware FP4** — datacenter `sm_100` (B200) or
  **consumer `sm_120` (GeForce RTX 50, incl. RTX 5070/5080/5090)**. FP8 block-scaled GEMM
  also runs on Hopper; FP4 needs Blackwell. A RTX 5070 (sm_120, 12 GB) is sufficient to
  develop and validate this path.
- **Toolchain: CUDA ≥ 12.8, cuBLAS ≥ 12.9.** Earlier toolkits lack sm_120 kernels
  ("no kernel image"). Build must target `sm_120a` (consumer) / `sm_100a` (datacenter).
- **cuBLASLt is the turnkey path.** CUTLASS custom block-scaled kernels (for a fused L2
  epilogue) only gained sm_120 FP4 in CUTLASS ≥ 4.2; prefer cuBLASLt unless a fused kernel
  is needed.
- This is a **new `cgo` integration** (cuBLASLt), separate from `cgo/cuvs`, behind the
  existing GPU build tag.

## Phasing

- **P1 — storage round-trip (CPU)**: types `vecf8`/`vecf4` + header codec + pack/unpack +
  string cast + display. UT + BVT. No GPU.
- **P2 — casts + LOAD (CPU)**: `vecf32 ↔ vecf8/vecf4`, CSV/parquet import.
- **P3 — CPU brute-force distance**: dequant-per-block L2/cosine/IP, so the feature is
  usable and testable without a GPU.
- **P4 — GPU brute-force via cuBLASLt** (Blackwell box): strip-header memcpy into
  `cublasLtMatmul` block-scaled GEMM, dataset tiling; validate results match the CPU path.
  Pin the payload sub-region order against the real cuBLASLt operand/scale contract here.

P1–P3 are buildable/validatable on an ordinary machine; only P4 needs the Blackwell GPU.

## Open items

- SQL element format for fp4: **MXFP4** (32/E8M0, default) vs **NVFP4** (16/E4M3+fp32
  global, more accurate) — decide before P1 if NVFP4, since it changes the fp4 payload.
- Exact cuBLASLt operand/scale layout (memory order, leading dims, whether scales are a
  separate tensor) pinned on the GPU box in P4; storage byte order chosen to match so the
  transfer stays a memcpy (header-version bump if it disagrees).
- CPU distance accumulation precision (fp32 vs fp16).
