# Decimal intermediate precision correction

## Problem and contract

Arithmetic currently substitutes the capacity of a physical Decimal128 carrier
(38 digits) for an expression's actual declared precision. That capacity then
feeds later arithmetic and SUM inference. For DECIMAL(15,2) inputs, a product
becomes precision 38 instead of 30, and `price * (1 - discount)` becomes
Decimal256(53,4) although the original declared domains fit Decimal128.

This is a focused type-inference bug fix. It adds no SQL capability, function
identifier, persistent state, protocol or arithmetic rounding policy. Existing
Decimal256 support for genuinely wide values remains required. SUM's public
aggregate headroom and partial-state layout are not changed by this patch.

## Invariants and implementation

- Decimal128/256 addition/subtraction precision is max(p1-s1,p2-s2)+max(s1,s2)+1.
- Decimal64 addition/subtraction retains its established 18-digit metadata and
  physical route; this patch does not introduce a carry-promotion policy.
  That legacy envelope can carry a 19-digit signed coefficient, including
  historical TIME + BIGINT coercions and a carry from Decimal64 addition. Downstream inference
  conservatively treats Decimal64 precision 18 (or unspecified precision) as
  19 digits. Narrower declared domains remain precise. This avoids introducing
  expression-provenance state or changing the legacy producer's public type.
- Multiplication precision is p1+p2; its existing result-scale rule is retained.
- Physical input coercion preserves each original numeric operand's precision
  and scale rather than substituting the target carrier capacity.
- Signed/unsigned integer domains reserve their full 3..20 decimal digits;
  display width is not a value bound. BIT(64) reserves 20 digits.
- Exact declared domains beyond 38 digits continue to use Decimal256. Preserve
  the released checked Decimal128 multiplication route when nominal operand
  precisions fit 38 and only a legacy carrier-capacity interpretation would
  promote it. Its precision-38 result remains the existing carrier envelope;
  it must not be mistaken for an exact 38-digit bound in later multiplication.
- For downstream MUL, Decimal128's legacy precision-38/unspecified envelope
  admits its full signed 128-bit coefficient (39 digits). ADD/SUB already
  reserve a carry, which bounds the sum/difference of two such coefficients;
  they retain their existing metadata rules. Below the carrier ceiling,
  declared domains remain precise.
- Keep the existing Decimal64 multiplication kernel for safe small-integer /
  Decimal64 pairs. Its output is Decimal128. BIGINT, UINT64 and BIT use
  Decimal128 inputs so synthesized casts respect their full 19/20-digit
  domains under the current integer-to-decimal width checks.
- Explicit Decimal256 operands retain that carrier. Unsupported coercion
  domains retain their old physical route. Result precision remains capped by
  the existing physical/public limits.
  In particular, an untyped NULL borrows a peer's cast type without acquiring
  that peer's numeric domain. A typed NULL retains its declared domain.

Persisted-origin verification retains its independently frozen pre-correction
coercion and result formulas. It cannot relax runtime assignment checks or
leak a historical binding policy into normal execution.

The first owner is operator overload resolution in list_operator.go. Comparison
coercion remains unchanged. Existing kernels consume the same supported physical
input/output pairs and the same serialized function IDs; no new execution
layout is introduced.

## Validation and performance acceptance

Resolver regressions prove the small-domain failures on unchanged main and
retain 39-digit addition and 40-digit multiplication controls. Planner tests
must exercise nested revenue expressions, carry boundaries, unsigned/BIT inputs,
NULLs, and the distinction between integer display width and full value domain.
Public SQL regression coverage checks values, NULL and derived expression/CTAS
types. Integer addition/subtraction retains the full storage-domain policy, so
`1 - discount` reports precision 22 and the revenue product reports 37, rather
than replacing the physical Decimal128 capacity with precision 38. No new
integer-literal refinement or prepared TIME policy is introduced.

Use focused tests before owning function/planner suites and full static checks.
Compare each candidate independently with clean main using local SF10 Q1-Q22,
16 GiB CN SHARED cache and 32 GiB whole-application memory, zero measured I/O,
spill and swap. Exclude cold warmups and collect profiles separately. Do not
validate loaded data or query results in TPC-H benchmark scripts. A confirmed
end-to-end query regression fails acceptance; an unresolved query slowdown is
not a pass. TPC-H is the primary performance acceptance indicator. Execution
microbenchmarks diagnose kernel selection, allocations and batch costs; report
their results separately and assess their practical impact.

## Checked-route cost and downstream bounds

A declared Decimal(20,0) times Decimal(18,0) fits the released checked D128
route. Treating D64's legacy envelope as 19 digits must not itself insert two
D256 input conversions and a wide row kernel for that route. Keep coefficient
bounds separate from route eligibility: infer cast domains conservatively,
but retain the old checked carrier when nominal precision fits it. A result
at that carrier ceiling is an envelope. Retain its released implicit cast
domains as well; tighter annotations add no result precision at that boundary.
Later MUL reserves its full
physical bound before selecting a narrower result precision. Checked physical
overflow behavior remains the released policy for this ambiguous case.

This adds no value prescan, new physical input/output pair, function ID,
provenance marker or per-row branch. Existing casts and checked kernels remain
the execution owners. Test both operand orders, scaled/unspecified domains,
a 39-digit D128 coefficient consumed by later wide arithmetic, and genuine
wide declared domains. Benchmark the declared 20-by-18 boundary against the
17/19-digit neighbors at scalar, 256-row and 8192-row sizes, including NULL
and constant controls. An independent complete performance pass remains
required; neither an interrupted run nor a predicted loop cost is acceptance.

## Separate SUM execution work

A wide SUM result domain does not require every row to use wide arithmetic.
A separate execution change can reduce proven-safe blocks in Decimal128 and
convert only at the existing wide-state merge. It must preserve the public
result type, serialized state, wide overflow/cancellation behavior, DISTINCT,
window and merge contracts. It is independently reviewed and benchmarked rather
than mixed into this planner experiment.

## Function placement correction

The arithmetic-only candidate independently exposed a Q4 regression of roughly
2.4–3.4% despite unchanged scan/filter instructions. Its hot selection loop moved
by 32 bytes. Separate long caller profiles located most extra foreground CPU in
that loop. An ABBA hardware-counter diagnostic measured front-end no-dispatch
slots increasing 9.24%, while instructions increased 0.27%, branch misses fell
0.16%, and instruction-cache misses increased 0.07%. Background merges were
present in both whole-application profiles. This supports a function-placement
explanation rather than additional decimal execution; it does not establish
that a particular cache-line crossing alone caused the slowdown.

Use Go 1.27's documented `-funcalign=64` linker option for amd64 executable
builds. This keeps an unchanged function's instructions at the same cache-line
offset when unrelated functions change size. Keep other architectures on their
existing policy. Include the option consistently in the ordinary, prebuilt
native, Darwin and musl link paths, retaining their existing native and version
flags. This changes padding, not arithmetic instructions or native libraries.
The existing native provenance owner must rebuild if editing the Makefile
invalidates its source receipt.

The measured clean-main control adds about 3.6 MB of text, so increased code
footprint and other hot-path placement remain performance risks. Validate this
linker option alone against the ordinary clean-main build before applying it
to the precision candidate. Both sides use independent clean checkouts and
normal native-owner builds, with no decimal patch or source overlays in the
alignment control. Independently resolve positive timing estimates before
delivery. Then compare three isolated variants at the current base: ordinary clean main,
clean main with alignment, and the precision candidate with alignment. Require
all three pairwise TPC-H comparisons to pass so the build option cannot hide a
query regression. Retain the declared-decimal controls to catch accidental wide
kernels, allocation increases and material batch costs. A gain in one query
cannot waive a regression in another query.

## Current performance evidence

The current isolated SF10 comparison has no statistically supported slowdown in
Q1-Q22 across the three build comparisons. Q1 improves 7.65% against ordinary
main and 7.26% against main with fixed alignment. Cold warmups are excluded and
profiles are collected separately, with zero measured physical I/O, spill and
swap under the stated cache and memory limits.

The one-row DECIMAL(20,0) multiplication controls retain small execution costs
against main with fixed alignment: 231.55 to 235.45 ns for the 17-digit peer,
and 232.25 to 235.85 ns for the 18-digit peer. Allocation counts are unchanged,
and the 256-row and 8192-row controls have no slower signals. With TPC-H as the
primary indicator, these roughly 4 ns scalar differences are disclosed minor
tradeoffs rather than automatic blockers for this correction. This decision
does not waive a query regression, incorrect kernel selection, allocation
increase or material batch regression.

## Candidate evidence

The first, broader candidate also changed Decimal64 carry/literal coercion. It
was rejected after independent SF10 confirmation established Q6, Q15 and Q21
slowdowns; Q1 gains did not override those regressions. This revision removes
those policy changes and is independently revalidated. No performance
acceptance is claimed from the rejected candidate.

### Persisted binding witnesses

DUMP/LOAD must select historical precision for decimal-only arithmetic and rounding as well as integer coercions. There are independent released eras: pre-integer-width bindings, strict integer casts with pre-intermediate-precision metadata, and current bindings. Reconstruct complete trees for the first two eras; do not normalize supplied widths or combine partial witnesses. Both historical contexts are confined to origin verification, retain declared CAST/runtime checks, and share the existing cancellation and candidate-node limits. Ordinary integer-only defaults add no precision witness.

Regression fixtures are serialized by an unmodified main planner and self-verified there before testing both DUMP and LOAD here. Include declared-decimal multiplication, nested/negative rounding, strict integer peers, unchanged controls, forged precision rejection and work limits.
