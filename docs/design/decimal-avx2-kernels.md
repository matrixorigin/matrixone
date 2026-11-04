# AVX2 decimal kernels

Owner #30860. User approved this design with Implement the plan on2026-11-05.
Separate local branch perf/decimal-avx2-kernels atop scalar32175 bb4e0c0311.
Publication/acceptance waits for scalar baseline acceptance; do not modify32175.
Risk R3 hot arithmetic. Keep SQL types, scales, cast/operation proofs and SUM/AVG
accumulators. SIMD only inside existing aggregate typed dispatch, never replace
native arithmetic without eliminated work. No production big.Int/CGo/cache/VM.

## Implementation

AVX2/Go1.27 SIMD-tagged private helpers, scalar stubs elsewhere; check ISA and
shape once per chunk. >=256 rows, groups of four, scalar tails. Flat non-NULL
columns and normalized literal broadcast; both-constant/masked/grouping/scaling
unsupported. D64 add/sub->D64 orD128, carry/borrow-derived high limb. Multiply
D64xD64 orD128xD64 ->D128/D256 via checked signed32 lanes; D128 high limb must
be canonical. First block refusal leaves output untouched. Later refusal
completes entire remainder through existing scalar span, never restarts input.
At most one failed speculative block per call. Load/rearrange/store in registers,
no packing buffers/new allocations or retained references. Original owner handles
allocation/errors/reset/free. Existing fused multiply/subtract remains scalar.

## Proof and delivery

Tests exact endpoints, signed carry/borrow, tails/offsets, upper-limb rejection,
first/later fallback, literal broadcast, source immutability and no-write decline.
Real Group consumer must prove admission and exact output. Inspect emitted AVX2
instructions and actual executed TPC-H profiles before scoring. Complete kernel
benchmarks include guard/packing/store costs, wide/mixed controls, tiny/256/8192
sizes. Keep a family scalar if complete cost regresses. Scalar-v-SIMD independent
Q1-Q22 paired memory/disk benchmarks, cold excluded, all local32/SF100server64CPU,
identical budgets, no spill/swap/interference. Full wall/CPU/instruction tables
and Total; no data/result verification in timing. No confirmed query/control/
Total regression; no mandatory overall gain. Draft until acceptance. Reuse
verified CPU native artifacts; fresh-main merge/fullprepush/selfreview beforepush.
