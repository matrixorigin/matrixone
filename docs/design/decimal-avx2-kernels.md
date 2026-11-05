# Decimal expression SIMD

Owner: #30860. The user approved this expanded two-PR plan on2026-11-05.
This PR owns expression kernels; an independent PR owns aggregate reductions.
#32175 is merged. Arithmetic and aggregation are validated against clean main
independently; neither PR depends on the other. Risk R3: exact hot arithmetic.

## Contract and consumers

Keep SQL types, scales, precision, implicit-cast proofs and SUM/AVG accumulators.
Keep original expression allocation/reset/free ownership. New helpers borrow
input views synchronously and retain no references, buffers or state. No new
CGo, big.Int, unsafe import, cache, runtime configuration, wire or disk format.
The existing bounded chain remains four nodes/five leaves with256-row chunks
and at most24KiB scratch. No additional expression layer or register-chain
fusion is introduced.

Ordinary same-scale Decimal128 add/sub uses checked SIMD on non-NULL arrays.
Native Decimal64 arithmetic stays unchanged. Prepared aggregate arguments use
their existing admission/proof boundary before optional SIMD dispatch. They
support D64 add/sub to64/128, D128 add/sub to128/256, and D64xD64 orD128xD64
multiplication to128/256. Unsupported shapes and scale changes remain scalar.

## Exact kernels

Go1.27 goexperiment.simd/amd64 helpers check runtime AVX2; other builds use
scalar stubs. Minimum256 rows; scalar tails. Flat non-NULL operands and
constant broadcasts are supported; masked/grouping forms remain unchanged.
Ordinary checked D128 kernels select addition/subtraction outside the loop,
retain permuted lanes internally, propagate unsigned carry/borrow and signed
overflow, and publish only through the first failing logical row. Preserve
the original first-error index and exact in-place operands, including a
broadcast whose backing row is overwritten. Existing array callers provide
disjoint or exact in-place destinations; partial overlap is not added.

Prepared add/sub uses original per-node precision proofs; wide output retains
the exact extra sign/carry limb. D64-output kernels omit unused high limbs.
Multiplication chooses physical width, result width and broadcast orientation
once. It vectorizes only coefficients proved signed32, with canonical high
limbs for D128 inputs. Constants are checked once. First-block refusal leaves
output untouched; later refusal completes the untouched suffix through the
original exact scalar span, without replaying the prefix. Products retain their
declared carriers. Do not remove necessary Decimal256 results or accumulators.

## Validation and delivery

Independent exact boundary tests cover carry/borrow, every overflow lane,
257/263-row tails, offsets, broadcasts, source immutability and both in-place
orientations. Prepared full-width tests compare128/256 carriers with unchanged
scalar arithmetic. Existing Group fixtures prove real argument activation,
metadata and lifecycle. The existing expression SQL case adds257-row native
arithmetic and nullable fallbacks; normal mo-tester comparison is separate
from timing. No data/result verification is performed in benchmark scripts.

Component controls include dispatch, guards, packing, tiny inputs and wide
fallbacks, with the production64-byte function alignment. Test families
independently before final full Q1-Q22 acceptance. First cold suite is excluded
per independent startup. Report wall/CPU per query and Total, plus separate
instruction profiles and actual SIMD activation. No confirmed query/control/
Total regression; component speedup alone is insufficient. Keep the outstanding
Q4 disk signal visible until equivalent full-suite evidence clears it.

Local SF10 uses all32 CPUs,48GiB application/16GiB cache, RAM and disk modes.
SF100 on a quiet server129 is also mandatory, all64 CPUs/interleaved NUMA with
the established216GiB/192GiB RAM and84GiB/16GiB disk protocols. No spill/swap/
OOM/interference is accepted. Reuse loaded data; no transfer or reload.
Templated draft PR only after local acceptance; readiness also requires SF100.
Fresh main, pre-push gate and self-review are required before publication.
