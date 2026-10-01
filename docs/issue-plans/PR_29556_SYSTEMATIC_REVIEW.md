# PR #29556: systemic QA findings

Reviewed on 2026-10-01. Source head: `6624b1baae427b48016e81d727a65ad61c1c69b6`.
Base: `152a1c4f5f072dab37e5af620b10c7086deeb1f4`.

This records executed review findings before implementation. Both findings remain
open at this revision; existing passing tests do not establish their closure.

## F1: representable wide DECIMAL values rejected (blocking)

```sql
SELECT CAST('12345678901234567890100000000000000000000E-20'
            AS DECIMAL(38,0));
```

Expected: `123456789012345678901`. Observed: invalid input, scientific
mantissa exceeds 38 digits. The compact spelling succeeds. The same scientific
spelling succeeds for DECIMAL(65,0), but adding 80 trailing zeros and a cancelling
exponent fails for both destination widths. Literal, VARCHAR column and prepared
execution reproduce the failure. Independent exact decimal arithmetic confirms
that these spellings represent the same in-range integer.

### Cause and ownership

`decimalCastParseString` in `pkg/sql/plan/function/func_cast.go` recovers long
scientific mantissas with `NormalizeExactIntegerString`. That helper deliberately
limits its output to 20 digits for prepared integer comparisons; DECIMAL has a
wider domain. Reusing that bounded integer witness as a decimal parser imposes
the wrong contract.

The fix belongs to existing decimal parsing, scale and rounding responsibilities.
Preserve the prepared integer witness contract. Canonicalization must respect
actual decimal precision, complete exponent syntax and rounding semantics without
allocating strings proportional to an arbitrary exponent. Reconsider long
fractional scientific spellings through this same owner rather than adding
individual exceptions. Only proven numeric overflow may enter overflow clamping.

### Required evidence

- Equivalent compact/scientific/zero-padded spellings at 20/21 digits and widths
  38/65, including signs, zero, e/E and cancelling exponents.
- Literal, vector and prepared paths agree.
- Fractional rounding, malformed input, underflow and true overflow retain their
  intended behavior; extreme exponents have bounded work.

## F2: derived prepared constants lose integer block pruning

A BIGINT primary-key point comparison with `CAST(? AS DECIMAL(38,0))`, or
`ABS(CAST(? AS DECIMAL(38,0)))`, returns the correct row but casts the entire
integer column to DECIMAL and has no block filter.

Executed fixture:

```sql
CREATE TABLE t(id BIGINT PRIMARY KEY);
INSERT INTO t
SELECT CAST(9007199254740992 AS BIGINT) + CAST(result AS BIGINT)
FROM generate_series(1,30000) g;
-- Substitute the fixture's database name below.
SELECT mo_ctl('dn', 'flush', '<database>.t');
SET @p = '9007199254740993';
PREPARE q FROM 'SELECT COUNT(*) FROM t WHERE id=CAST(? AS DECIMAL(38,0))';
EXECUTE q USING @p;
```

EXPLAIN ANALYZE and execution were checked for all four forms on the flushed
fixture, with a private instance and an 8 MB memory cache:

| Comparison | Result count | Input blocks | Input rows | Block filter |
| --- | ---: | ---: | ---: | --- |
| `id=?` | 1 | 1 | 1 | Present |
| `id=CAST(? AS SIGNED)` | 1 | 1 | 1 | Present |
| `id=CAST(? AS DECIMAL(38,0))` | 1 | 4 | 30000 | Absent |
| `id=ABS(CAST(? AS DECIMAL(38,0)))` | 1 | 4 | 30000 | Absent |

These counters establish excess scanned work. They do not establish a throughput
ratio on this shared machine. The fixture and private runtime were cleaned up.

### Cause and ownership

The derived-expression branch in `prepared_binding.go` attempts a ROUND-specific
proof and then continues. The exact-value proof for direct parameter markers does
not cover these deterministic derived constants.

Reuse existing constant evaluation and comparison conversion owners. Evaluate
actual CAST/function semantics before admitting an exact, in-range integer
comparison. Preserve parameter provenance, value-dependent plan handling,
diagnostics and conservative behavior for expressions that cannot be proved safe.
Do not introduce a second expression evaluator or prepared state machine.

### Required evidence

- Results and scan counters for direct, explicitly cast and derived constants.
- Fractional CAST rounding, signed boundaries, NULL, errors and volatile functions.
- Repeated execution with changed parameter values; column-dependent expressions
  must not be treated as constants.
- Existing fractional, FLOAT and ROUND comparisons keep their semantics.

## Overall review disposition

Request changes for F1; F2 is a measured optimization gap now included in the
requested systemic repair. Existing decimal, prepared binding, LAST_DAY ABI and
EXPLAIN changes were reviewed together. No additional lifecycle or persisted
LAST_DAY compatibility blocker was confirmed. GitHub had no actionable inline
review comments at the reviewed revision.

Earlier local incremental SCA, owning UT, selected race UT and seven BVT cases
passed on the reviewed source. Changed BVT cases also passed twice on one live
instance with cleanup checks. Those results are historical evidence: production
or test changes require validation of the resulting revision. CI was still in
progress; no CI success is claimed.

Implementation, tests and documentation must each justify their additions.
Prefer existing responsibilities, remove replaced branches and redundant work,
and test observable behavior and unhappy paths rather than helper structure.

## Post-review correction and validation

F1 now quantizes the complete validated scientific coefficient directly into the
requested decimal width and scale before physical parsing. Output and arithmetic
are bounded by the destination precision; the integer normalizer remains an exact
integer owner. Removed the replaced overflow and underflow parsing branches.

F2 now proves the actual typed, parameter-dependent peer with the existing
constant evaluator and preserves its executable conversions. The late numeric
rewrite handles scan filters and JOIN conditions. Its shared singleton projection
resolver admits only an ordinary PROJECT over the executor's one-row dummy
VALUE_SCAN, with matching value types and no filtering, limits or expansion.
The existing runtime-filter and outer-join policy remains responsible for pruning.

QA caught and corrected an intermediate JOIN regression: scanning 20004 rows
instead of one. The final BVT retains runtime-filter build/probe and the original
1-block/1-row expectation; only the retained conversion text changed.

Current local evidence: incremental vet/lint (zero issues), full function and
planner UT, prepared public integration, selected race UT, bounded decimal
public probes, JOIN rebinding and LEFT/RIGHT/FULL controls passed. Eight BVT
cases passed twice on one instance, 453/453 each, with fixture cleanup checked.
The 30000-row flushed single-table decimal/ABS probes read one block/one row
versus four blocks/30000 rows previously. This is scanned-work evidence, not a
matched throughput comparison. Final independent review and remote CI remain
separate delivery gates.

Independent overall review: **APPROVE**, actual `gpt-6.1-sol` / `xhigh`,
session `01a0f853-da2a-7d62-a00f-763bffd0b158`. All 31 changed files were reviewed against the
main base with no unresolved material blocker. Remote CI is not certified here.
