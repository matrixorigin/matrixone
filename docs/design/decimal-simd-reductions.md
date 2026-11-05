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

## BulkFill-only checkpoint

At checkpoint9e200b7277, only contiguous BulkFill is accelerated. SIMD-disabled/non-AVX2 targets keep
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

## Register packing refinement

The2026-11-05 refinement keeps the same signed64 coefficient admission,
four-row transactional prefix and existing scalar suffix. Decimal128 inputs
are separated into low/high vectors with unpack instructions. Decimal256
inputs use a four-by-four limb transpose in registers; every upper limb must
equal the coefficient's sign extension before any lane is accumulated.
The private lanes may stay in row order0,2,1,3 because their bounded exact128
totals are folded once, after the prefix. No per-block stack staging array is
needed. Signed comparisons against zero form sign masks using AVX2; signed64
vector arithmetic shifts are AVX-512-only and are excluded. Refusal tests cover
each row lane and every upper limb with signed64 endpoint coefficients.

Non-DISTINCT one-row batches reuse the existing scalar Fill operation, avoiding
batch accumulation/SIMD setup. The unchanged batch bodies are the parity oracle
for NULL/constant/wide inputs, populated state, counts and overflow publication.

The mixed SIMD/scalar slowdown remains an acceptance blocker until measured
evidence clears it; faster packing alone does not clear that gate.

## Uniform-span candidate

The BulkFill-only checkpoint9e200b7277 fails independent TPC-H RAM acceptance:
six paired process blocks give Total+1.02% [95% CI+0.07%,+1.97%]. Native shared
libraries match main. Preserve those results; they do not accept this revision.
Normal Group aggregation calls BatchFill, so isolated BulkFill gains do not
establish acceleration of that consumer.

Narrow the original approved one-to-four-group family to a single uniform
group per BatchFill span. Before arithmetic or state mutation, prove that all
group IDs match one nonzero ID with AVX2 comparisons. A differing/unmatched
group immediately retains the existing coalescer for the entire span. This
does not introduce another grouping algorithm or change mixed-group handling.
Integer inputs, short spans, DISTINCT, NULLs/constants/grouping masks and
non-AVX2 builds retain their existing paths.

Reuse the contiguous reducer with explicit offset/count, including its exact
private prefix, ordered wide-value suffix and final persistent-state update.
Native SUM still maintains its count state; generic SUM still publishes NULL
metadata in the original order. AVG counts only the requested span. No public
API, allocation, retained state, schema, accumulator or configuration changes.

Prove nonzero offsets, tails, every wide-refusal lane, early/late group mismatch,
unmatched IDs, existing state and error publication against the retained scalar
coalescers. Use the real Group consumer as well as the existing SQL comparison.
First screen uniform and mixed/late-mismatch/nullable/integer controls with
production alignment; discard a confirmed fallback regression before broad
timing. Then independently compare all22 queries against clean main with the
same local/SF100 gates. This new source must earn its own acceptance; it cannot
reuse the failed source's performance verdict.

The first uniform-span control screen retains its80–93% reductions for uniform
256-row batches, but fails late-mismatch/unmatched and tiny Decimal64 controls.
A complete admission scan followed by scalar coalescing duplicates the scan on
late rejection. Check unequal first/last IDs before any SIMD scan. This is only
an early refusal: all interior IDs are still checked before admission. One-row
decimal BatchFill calls reuse the existing Fill operation, with retained scalar
coalescers checking offset, unmatched, NULL/constant and error-state parity.
Integer BatchFill keeps its original route. No TPC-H run accepts the failed
control screen; archive it before measuring this revised source.

## Dense-state integration

Main781ebc03d9 adds compact group lookup and scalar dense-state decimal fills.
The uniform SIMD admission remains ahead of the dense/general scalar dispatch;
rejected spans retain main's exact dense eligibility, including its checked
large-call publication guard. Integer and mixed-group calls use main's scalar
paths. Focused dense-boundary, checked-overflow, uniform-span and compact/global
Group tests pass in the required build modes. Earlier timing against007dd57a
precedes this faster dense baseline and is historical; fresh performance
qualification is required before readiness.

## Approved 2026-11-06 coverage revision

Requalify this independent candidate against main5792e1c372, including its
compact/dense scalar implementation. Measure32/64/128/256 thresholds through
real BulkFill and BatchFill consumers, with adjacent lengths, wide/NULL,
mixed/late/unmatched and integer controls. Select a smaller minimum only if it
wins at T,2T and8192 without a confirmed nearby or fallback slowdown; otherwise
retain256. No new grouping algorithm, run matcher or persistent state is added.
Preserve dense fallback eligibility, checked large-call error order and one-row
Fill. Component results precede independent all22/Total local SF10 and server129
SF100 acceptance. The cold suite is excluded, with no spilling/swap/OOM and
quiet CPUs. Historical results against older scalar main do not qualify this
source. This revision was approved by the requester before implementation.

The first refreshed dense-main screen confirms uniform gains of72–93%, but
also confirms small0.4–1.1% fallback costs. It does not pass. Reject obvious
first/last mismatches before metadata/SIMD setup at the BatchFill owner, while
retaining the complete helper proof for admission. Add an interior penultimate
mismatch control too, so the endpoint shortcut cannot hide a repeated-scan
cost. Keep the existing256 minimum until those controls and all22 qualify it.

The penultimate mismatch confirms the limitation: D64 uniform rejection is
6.31% slower at256 rows [95% CI5.68%,6.94%] and2.69% slower at8192
[1.82%,3.56%]. Other decimal carriers also regress. Endpoint rejection fixes
the obvious late/unmatched cases but cannot remove a complete admission scan
followed by replay through the scalar coalescer. This revision remains blocked.
Do not add more sampled endpoint checks or claim acceptance from uniform gains.
Eliminating the repeated scan generally would require a separately approved
integration into the existing dense coalescer, preserving its private state and
checked publication order; that is outside this revision's no-new-grouping
algorithm scope. No TPC-H result can label this failed component screen passed.
