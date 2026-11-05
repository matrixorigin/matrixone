# Exact decimal SIMD reductions

User approved two independent PRs on2026-11-05; #30860.
Risk R3. Package-local helpers reduce a bounded four-group prefix into private
exact128 sums/counts. Decimal128/256 coefficients require runtime signed64
canonical-limb proofs; persistent state precision and format are unchanged.
Every four-row block is transactional. Fifth-group/wide-value refusal leaves
that block unprocessed; existing coalescers resume with the exact private
prefix, preserving final checked publication and error ordering. No input
materialization, allocation, external API, DISTINCT/null/constant replacement.
The four-group capacity is an AVX2 budget, never a query/schema rule; validate
1/2/3/4/5/8/64/255-257 groups, shuffled/skewed/late fallback. Drop candidate
if fallback controls regress or benefit is specific to Q1. Contiguous BulkFill
uses the same exact reducer. Local SF10 and quiet server129 SF100 both gate
readiness; full cold suite excluded, wall/CPU/Total and separate instruction
profiles, no spill/swap or query-result verification in timing scripts.

## Candidate evaluation

The bounded-group candidate failed five alternating pairs of existing controls:
four groups +86.8%, late transition +85.1%, high-cardinality +3.7%, tiny +5.0%.
Its grouped BatchFill hooks and implementation were removed per the approved
discard rule. The remaining implementation accelerates contiguous BulkFill only,
using private exact128 prefix totals and the original scalar suffix/final
checked publication. Grouped coalescing is byte-identical to main. This is not
yet an end-to-end acceptance result; no general small-group win is claimed.

## Retained implementation

Only contiguous BulkFill is accelerated. SIMD-disabled/non-AVX2 targets keep
the original scalar dispatch. The generic executor records its physical Go
input/state carrier codes once at construction, in existing struct padding;
these are not SQL result precision or serialized state. SIMD conversions use
a concrete-carrier helper rather than adding generic dictionary entries.
Nullable/grouped/constant inputs retain original methods. Private narrow
prefixes and ordered scalar suffixes preserve the existing local checked/
unchecked mode and final checked state publication, including error-state
assignment order. Result metadata, counts, window behavior and serialization
remain unchanged. A compact257-row SQL fixture covers SUM/AVG carriers, NULLs
and wide coefficients in normal comparison mode.

Initial test-binary screens used default function alignment; retained-path
controls use production-equivalent64-byte alignment. Earlier NULL/control
signals are retained until comparable evidence clears them. No end-to-end
acceptance or readiness is claimed from component speedups.
