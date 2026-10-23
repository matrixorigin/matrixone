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

- Addition/subtraction precision is max(p1-s1,p2-s2)+max(s1,s2)+1.
- Multiplication precision is p1+p2; its existing result-scale rule is retained.
- Physical input coercion preserves each original numeric operand's precision
  and scale rather than substituting the target carrier capacity.
- Signed/unsigned integer domains reserve their full 3..20 decimal digits;
  display width is not a value bound. BIT(64) reserves 20 digits.
- Decimal64 addition/subtraction uses Decimal128 operands when the carry needs
  a nineteenth digit. Results beyond 38 digits continue to use Decimal256.
- Keep the existing Decimal64 multiplication kernel for safe signed-integer /
  Decimal64 pairs. Its output is Decimal128; an internal BIGINT cast retains
  its 19-digit logical domain. UINT64 and BIT cannot use the signed coefficient.
- Explicit Decimal256 operands retain that carrier. Unsupported coercion
  domains retain their old physical route. Result precision remains capped by
  the existing physical/public limits.

The first owner is operator overload resolution in list_operator.go. Comparison
coercion remains unchanged. Existing kernels consume the same supported physical
input/output pairs and the same serialized function IDs; no new execution
layout is introduced.

## Validation and performance acceptance

Resolver regressions prove the small-domain failures on unchanged main and
retain 39-digit addition and 40-digit multiplication controls. Planner tests
must exercise nested revenue expressions, carry boundaries, unsigned/BIT inputs,
NULLs, and the distinction between integer display width and full value domain.
Public SQL regression coverage checks values and derived expression/CTAS types.

Use focused tests before owning function/planner suites and full static checks.
Compare each candidate independently with clean main using local SF10 Q1-Q22,
16 GiB CN SHARED cache and 32 GiB whole-application memory, zero measured I/O,
spill and swap. Exclude cold warmups and collect profiles separately. Do not
validate loaded data or query results in TPC-H benchmark scripts. A confirmed
regression fails acceptance; an unresolved slowdown is not a pass.

## Separate SUM execution work

A wide SUM result domain does not require every row to use wide arithmetic.
A separate execution change can reduce proven-safe blocks in Decimal128 and
convert only at the existing wide-state merge. It must preserve the public
result type, serialized state, wide overflow/cancellation behavior, DISTINCT,
window and merge contracts. It is independently reviewed and benchmarked rather
than mixed into this planner experiment.
