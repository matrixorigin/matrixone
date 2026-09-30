# Low-precision float vector columns: vecf8 (MXFP8), vecf4 (NVFP4)

Status: proposed · Issue: #20567 · Scope: new packed vector column types for quantized
ML embeddings · Depends on: the scalar `float8`/`float4` types (their e4m3/e2m1 element
codecs are reused unchanged).

## Motivation

Quantized embeddings are stored in narrow floats to cut memory and bandwidth, but FP8
(±448) and especially FP4 (±6, ~2.5 octaves) are far too narrow to hold a real embedding
directly — every component would saturate or underflow. NVIDIA's microscaling formats
scale a small contiguous *block* of a vector's own dimensions against one shared scale,
centering each block into the format's representable band:

```sql
CREATE TABLE t (v vecf4(1024));   -- or vecf8(1024)
```

**Where these pay off (and where they do not).** FP8/FP4 are **tensor-core GEMM** formats.
Their GPU acceleration is a **batch matmul** — the similarity matrix `S = Q × Dᵀ` (a batch
of query vectors against the database), i.e. **brute-force / exact** similarity on tensor
cores. They are **not** ANN-index element types: cuVS indexes accept only `float`, `half`,
`int8`/`uint8` (with scalar / product / binary quantization); there is no fp8/fp4 index
path. So `vecf8`/`vecf4` target **GPU brute-force batch similarity via cuBLASLt
block-scaled GEMM**, distinct from `cgo/cuvs`. For *approximate/indexed* low precision,
keep using `vecint8` (cuVS scalar quantizer) or IVF-PQ.

## The two types

Each type's stored bytes are exactly the format cuBLASLt's block-scaled GEMM consumes for
that element width (§GPU) — so the GPU transfer is strip-header + memcpy, no conversion.

| SQL type   | Element (4/8-bit)     | Block | Block scale (8-bit)         | GEMM format |
|------------|-----------------------|:-----:|-----------------------------|-------------|
| `vecf8(N)` | OCP FP8 e4m3 (`Float8`)|  32   | E8M0 (`CUDA_R_UE8`)         | **MXFP8**   |
| `vecf4(N)` | OCP e2m1 (`Float4`)   |  16   | E4M3 unsigned (`CUDA_R_UE4M3`) | **NVFP4** |

`N` is the logical dimension, carried in the column type, independent of the cell byte
length. The element is genuinely 4/8-bit; the 8-bit scale is *shared* across a block
(NVFP4 ≈ 4.5 bits/element, MXFP8 ≈ 8.25 bits/element — see §sizes).

### Why NVFP4 (not MXFP4) for the 4-bit type

cuBLASLt's FP4 block-scaled matmul consumes **NVFP4 only** — `CUDA_R_4F_E2M1` elements
with `CUDA_R_UE4M3` (E4M3) scales over **16**-element blocks. It does **not** accept MXFP4
(32-block E8M0) for FP4. Storing MXFP4 would force a lossy 32→16 re-block + E8M0→E4M3
re-quantization at GEMM time — the opposite of the memcpy goal. So we **store what the
GEMM eats**: FP4 → NVFP4, FP8 → MXFP8. (FP8's MXFP8 32-block E8M0 is what cuBLASLt's FP8
path takes directly.) NVFP4 is also the more accurate 4-bit format (finer block, mantissa'd
scale). If a future cuBLAS adds an MXFP4 FP4 path, MXFP4 could become an alternative
`vecf4` sub-format then; today NVFP4 is the correct choice.

## Element and scale codecs (already built)

The number formats are done — the scalar #20567 work IS the element format, bit-identical
to the GPU element types:

- element e4m3 = `types.Float8` — `S.EEEE.MMM`, bias 7, NaN `0x7f`, max ±448, no Inf =
  `CUDA_R_8F_E4M3`.
- element e2m1 = `types.Float4` — `S.EE.M`, codes {0,½,1,1½,2,3,4,6}, max ±6, no Inf/NaN =
  `CUDA_R_4F_E2M1`.

So packing a cell is a pure byte/nibble shuffle of existing values, no element conversion.
Only the **block scales** are new work, and both reuse existing pieces:

- MXFP8 scale = **E8M0** — an 8-bit unsigned power-of-2 exponent; one small new codec.
- NVFP4 scale = **E4M3 unsigned** — reuses the `Float8` e4m3 codec with the sign ignored.

## Cell format (the contract)

Each value is one varlena cell; same *structure* for both types, differing only in scale
dtype / block size (both recorded in the header):

```
[ header (MO metadata) | block scales | packed elements ]
                        └─ byte-exact MXFP8 / NVFP4 payload ─┘
```

- **header** — fixed-size MO metadata: format version, element format (e4m3 / e2m1), scale
  format (E8M0 / E4M3), block size (32 / 16), logical dimension `N`, block-scale count
  (`ceil(N/32)` for vecf8, `ceil(N/16)` for vecf4). MO bookkeeping only; **not** part of
  the payload.
- **payload** — byte-exact GPU format: the block scales followed by the packed elements
  (e4m3 one byte each; e2m1 two nibbles per byte, `hi<<4|lo`, odd `N` pads the final high
  nibble with `0x0` = +0). Scale block `b` governs its `blockSize` consecutive elements.

Consequence: unlike every existing vector element type, `Size ≠ N × elemSize`. The logical
dimension is authoritative from the column type / header, never derived from byte length.

## Scale derivation & the NVFP4 global

- **Per-block symmetric absmax.** For each block, `scale = absmax(block) / FORMAT_MAX`
  (FORMAT_MAX = 448 e4m3, 6 e2m1); each element `code = quantize(v / scale)`, dequant
  `v ≈ dequant(code) × scale`. Rounding the scale up keeps every element in range, so no
  element overflows its block.
  - MXFP8 (E8M0): scale is power-of-2 (`2^ceil(log2(absmax/448))`); dequant is an exact
    `ldexp`.
  - NVFP4 (E4M3): scale has mantissa bits, fitting the block absmax tightly.
- **NVFP4's per-tensor fp32 global is NOT stored.** NVFP4 defines a third level (one fp32
  scalar per operand *tensor*), which conflicts with per-row incremental storage. Instead
  each vector is **self-normalized into its per-block E4M3 scales**, and the GEMM passes a
  global of **1.0**. This keeps every stored vector independent and memcpy-able; the E4M3
  block scale (max 448) is ample for normalized embeddings, which lack the extreme dynamic
  range the global was designed for.
- **Non-finite is never stored.** NaN/Inf input (which would make a block absmax
  non-finite) is rejected at build, per the repo-wide finite-persistence rule.

## Storage size (excl. 8-byte header), 1024-dim example

| Format | Elements | Scales | Total | bits/elem |
|--------|----------|--------|-------|-----------|
| `vecf8` MXFP8 | 1024 B (e4m3) | 32 × 1 B (E8M0) | **1056 B** | 8.25 |
| `vecf4` NVFP4 | 512 B (e2m1)  | 64 × 1 B (E4M3) | **576 B** | ~4.5 |

vs `vecf32(1024)` = 4096 B: `vecf4` is ~7× smaller, `vecf8` ~4×.

## Invariants

- The e4m3 / e2m1 element bit layouts are the scalar `float8`/`float4` types; packing is a
  pure byte/nibble shuffle, no per-element conversion.
- The payload sub-region layout and endianness match the target cuBLASLt block-scaled
  operand/scale contract exactly, so the GPU transfer stays a memcpy. Any change to that
  contract is a storage-format change and must bump the header version.
- The header never participates in the payload; stripping it must never rewrite payload
  bytes.
- Every stored vector is independent (no cross-vector/global state), so incremental
  `INSERT`/`UPDATE` needs no column-wide statistics.

## GPU compute path: cuBLASLt block-scaled GEMM

The GPU consumer is **`cublasLtMatmul` block-scaled matmul**, not cuVS. Batch similarity is
a GEMM: inner-product/cosine is `Q × Dᵀ`; L2 is `‖q‖² + ‖d‖² − 2·q·d` (cross term = GEMM,
norms precomputed). The stored payload feeds it directly:

- **Formats:** `CUDA_R_8F_E4M3` + `CUDA_R_UE8` 32-block (vecf8/MXFP8);
  `CUDA_R_4F_E2M1` + `CUDA_R_UE4M3` 16-block + global=1.0 (vecf4/NVFP4).
- **Transfer = memcpy.** Per row, strip the MO header, copy the scale sub-region and the
  element sub-region into cuBLASLt's scale and operand tensors — no arithmetic. (`vector.
  Vector` is per-row, so this is a per-row gather of raw bytes.)
- **Tiling is mandatory.** Brute force materializes `Q × Dᵀ`; the operator tiles over the
  dataset and query batch, streaming tiles to VRAM — required on any card, especially
  12 GB consumer parts.
- **Exact, not approximate** — full scan, no index build.

### Hardware & toolchain

- **Hardware: NVIDIA Blackwell with hardware FP4** — datacenter `sm_100` (B200) or consumer
  `sm_120` (GeForce RTX 50, incl. **RTX 5070/5080/5090**). FP8 block-scaled GEMM also runs
  on Hopper; FP4 needs Blackwell. A RTX 5070 (sm_120, 12 GB) suffices to develop/validate.
- **Toolchain: CUDA ≥ 12.8, cuBLAS ≥ 12.9.** Earlier toolkits lack sm_120 kernels. Build
  targets `sm_120a` (consumer) / `sm_100a` (datacenter).
- **cuBLASLt is the turnkey path.** CUTLASS custom block-scaled kernels (for a fused L2
  epilogue) only gained sm_120 FP4 in CUTLASS ≥ 4.2; prefer cuBLASLt unless a fused kernel
  is needed.
- **New `cgo` integration** (cuBLASLt), separate from `cgo/cuvs`, behind the GPU build tag.

## Phasing

- **P1 — storage round-trip (CPU)**: types `vecf8`/`vecf4` + header codec + E8M0 scale codec
  + pack/unpack + string cast + display. UT + BVT. No GPU.
- **P2 — casts + LOAD (CPU)**: `vecf32 ↔ vecf8/vecf4`, CSV/parquet import.
- **P3 — CPU brute-force distance**: dequant-per-block L2/cosine/IP, **fp32 accumulate**
  (matches the GEMM's fp32 accumulate, so CPU and GPU agree). Usable/testable without a GPU.
- **P4 — GPU brute-force via cuBLASLt** (Blackwell box): strip-header memcpy into
  `cublasLtMatmul` block-scaled GEMM, dataset tiling; validate results match P3's CPU path.

P1–P3 build/validate on an ordinary machine; only P4 needs the Blackwell GPU.

## Decisions (locked)

- `vecf4` = **NVFP4** (e2m1 elements, E4M3 unsigned 16-block scale, per-vector-normalized,
  GEMM global = 1.0, no stored global) — dictated by cuBLASLt's FP4 path; also most accurate.
- `vecf8` = **MXFP8** (e4m3 elements, E8M0 32-block scale) — matches cuBLASLt's FP8 path.
- Element codecs = the merged scalar `Float8`/`Float4`; scale codecs = new E8M0 + reused
  `Float8` e4m3.
- CPU distance accumulate = **fp32**.
- Non-finite rejected at build; every vector independent.

## Open items (verify on the Blackwell box in P4)

- Exact cuBLASLt operand/scale tensor layout (memory order, leading dims, whether the scale
  is a separate tensor) — pin the P1 payload byte order to match so the memcpy is a no-op
  (header-version bump if it disagrees).
- Confirm cuBLASLt FP4 accepts per-vector-normalized E4M3 16-block scales with global = 1.0.
- Whether a newer cuBLAS (13.x) adds an MXFP4 FP4 path (would enable an MXFP4 `vecf4`
  variant; not required now).
