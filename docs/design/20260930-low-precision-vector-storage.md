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

## The two types

| SQL type   | Element        | Block | Block scale        | Element bytes   |
|------------|----------------|:-----:|--------------------|-----------------|
| `vecf8(N)` | OCP FP8 e4m3   |  32   | E8M0 (1 byte, 2^k) | `N`             |
| `vecf4(N)` | OCP MXFP4 e2m1 |  32   | E8M0 (1 byte, 2^k) | `ceil(N/2)`     |

Both share one storage format; only the element width and packing differ. `N` is the
logical dimension, carried in the column type, independent of the cell byte length.

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
- **Scale = E8M0 (power-of-2), block size = 32** — the OCP MXFP standard. The write side
  derives `scale = 2^ceil(log2(absmax(block)/FORMAT_MAX))` (FORMAT_MAX = 448 for e4m3,
  6 for e2m1), rounding up so no element overflows its block; dequant is an exact `ldexp`.
  E8M0 is coarser than an fp32 scale — accepted as the price of MXFP interop.
- **Payload is byte-exact MXFP; the GPU path strips the header and memcpys the payload to
  device memory** — no dequant/requant, no per-element arithmetic. The stored bytes feed
  cuVS/CUTLASS MXFP8/MXFP4 kernels directly. CPU-only builds dequantize per block for
  distance and display; the on-disk bytes are identical in both builds.
- **Single-level scaling only.** NVFP4's second per-tensor fp32 global scale is a GPU
  compute concern, not stored.
- **Non-finite is never stored.** A NaN/Inf input (which would make a block absmax
  non-finite) is rejected at build, consistent with the repo-wide finite-persistence rule.

## Invariants

- The e4m3 / e2m1 element bit layouts are the same as the scalar `float8`/`float4` types;
  packing is a pure byte/nibble shuffle with no per-element conversion.
- The payload sub-region layout and endianness match the target cuVS/CUTLASS MXFP tensor
  contract exactly, so the GPU transfer stays a memcpy. Any change to that contract is a
  storage-format change and must bump the header version.
- The header never participates in the MXFP payload; stripping it must never require
  rewriting payload bytes.

## GPU / cuVS status (P4 dependency)

MatrixOne's current cuVS integration (`cgo/cuvs`) supports only `float`, `half` (fp16),
`int8_t`, `uint8_t` element types, with an int8/uint8 scalar quantizer and an fp16
quantization mode — **there is no MXFP (fp8/fp4) ingestion path today**, and the cuVS
library headers that would define the MXFP tensor/scale layout are not vendored in the
repo (they live in the GPU box's CUDA/conda install). Therefore:

- The CPU side — storage format, pack/unpack, casts, LOAD, and CPU distance (dequant per
  block) — can be built and validated on an ordinary machine now.
- The GPU side (strip-header + memcpy into a cuVS MXFP kernel) is blocked on (a) a cuVS
  version that actually consumes MXFP8/MXFP4 and (b) GPU-box access to read its exact
  tensor/scale contract. The storage payload is defined as byte-exact OCP MXFP precisely
  so that, once that path exists, the transfer is a memcpy — but the payload sub-region
  order must be pinned against the real cuVS API at that time (a header-version bump if it
  disagrees). Until then, the GPU consumer is a forward-looking target, not a shipped path.

## Open items

- Exact payload sub-region order (scales-then-data vs separate scale/data tensors) pinned
  against the target cuVS MXFP API at the GPU phase; the storage byte order is chosen to
  match it so the memcpy stays a no-op.
- CPU distance accumulation precision (fp32 vs fp16).
