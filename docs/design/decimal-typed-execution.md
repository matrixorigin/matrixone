# Decimal execution without unnecessary vectors

Owner: #30860. Approved by the user with “Implement the plan” on 2026-11-04.
Risk: R3, hot arithmetic, expression ownership and allocation. This replaces
SIMD-first implementation planning, retaining historical designs and evidence.

## Contract

Keep DECIMAL(15,2), SQL inference, per-operation precision/scale and aggregate
accumulator types unchanged. Implementation branch: `perf/decimal-typed-execution`. One independent PR covers:
typed +,-,* operand execution and bounded complete aggregate-argument chains.
Do not change general SELECT/filter constructors or add a query-specific path.
The existing expression tree exclusively owns executors, literals and output;
specializations borrow leaves and delegate reset/free once.

Skip only provably lossless implicit casts over supported columns/literals.
Normalize bound literals once. Select typed kernels at preparation, not per row. A single binary root
requires an eliminated physical conversion; native arithmetic retains its
existing kernel. The SF10 disk confirmation exposed a Q6 slowdown when that
route was replaced without removing any widening work.
Add/subtract align scales exactly; multiply uses exact 64x64->128 and
128x64->256 primitives. Retain original result checks. Admit only operations
with proven representability and no rounding/scale reduction. Explicit/fallible
casts, parameters, variables, shared/memo nodes and unsupported types stay generic.
No sampled bounds, batch range scan, production big.Int, SIMD, JIT or new VM.

## Bounded chains and ownership

Admit unshared trees of at most four arithmetic nodes. A single-node chain
requires a proven eliminated physical widening/scale-conversion cast. Process 256 rows per
chunk in original tree order, with at most three 256x32-byte scratch buffers
(24 KiB arithmetic scratch payload per argument), charged through the existing
expression allocation owner. At most four normalized literal coefficients add
32 payload bytes; vector metadata and allocator rounding remain accounted.
The root writes its normal output vector. Only flat non-NULL inputs/non-NULL
literals initially use the chain path. Masks, broadcast columns, NULL/grouping
representations and ownership-transferring Eval use the original tree.
Validate runtime representations before mutation. Allocate scratch only after
admission, release partial allocations on failure, retain no input references
between evaluations. Preserve every intermediate boundary; never reassociate
or rely on final cancellation to suppress an intermediate error.
Keep the existing multiply/subtract specialization until replacement proves
semantic and performance equivalence. No public executor/aggregate API, SQL,
configuration, catalog or wire-format change.

## Evidence and delivery

Extend existing function/evaluator fixtures for exact arithmetic, widths,
scales, signs, carry/borrow, NULL/constants, rejected casts and fallback, chain
shapes/tails/admission bounds, intermediate errors, input immutability and
allocation failure/reuse/reset/free. Reuse minimal existing SQL setup for real
aggregate consumption, prepared reuse and metadata. Native exact values suffice
for narrow tests; existing wide test oracles may be reused. No production big.Int.

Measure complete dispatch/conversion/scratch/fallback work at tiny,256,8192-row
sizes before independent current-main Q1-Q22 acceptance. Record CPU/instructions,
allocations/temp bytes and all wall times with Total. Exclude cold runs; separate
profiles from latency scoring. Require work reduction and no confirmed affected
query/control/Total regression; gain is not mandatory. Independently investigate
slower signals. SF10 local; SF100 on server129 only after user resumes timing,
all CPUs, sufficient configured memory/cache, no swap/spill. Timing scripts only
execute/drain; no data/result verification. Leave draft while acceptance pending.
Use PR template; fresh authoritative-main merge/full prepush/self-review required.
After this scalar change, reprofile before designing SIMD. Division/modulo need
separate rounding/quotient/remainder/zero-divisor design. Preserve other PR plans.
