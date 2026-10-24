# Numeric and temporal SQL contracts

## DECIMAL conversion

String CAST, assignment and MySQL numeric-prefix conversion share the existing
numeric scanner and destination-scale quantizer. Strict CAST validates the
complete token; prefix conversion retains its established prefix grammar.
Binary/octal/hex tokens retain their existing decoding owner.

Quantization combines mantissa position, exponent and destination scale before
checking precision. Wide mantissas and cancelling exponents are therefore judged
by the represented value, not by spelling length. Magnitudes are rounded once,
half away from zero. Only proven numeric overflow may clamp in explicit CAST;
invalid syntax and physical-construction errors remain errors.

Work is linear in supplied text plus destination precision. Exponents are
saturated only after a cancellation-safe bound and are consumed completely.
Coefficient scratch is caller-owned and bounded to 76 bytes. The existing typed
decimal accumulator consumes these digits directly, without building or
reparsing a canonical string. Successful ordinary base-10 conversion adds no
per-row heap allocation. Persisted decimal representations are unchanged.

## Prepared integer predicates

Complete in-range integer text uses the integer peer's actual domain, preserving
adjacent BIGINT identities beyond DOUBLE's exact range. Approximate FLOAT,
fractional, NULL, volatile and diagnostic-producing expressions retain their
existing guarded conversion paths.

Derived constant comparisons use the existing typed evaluator. Their original
executable conversions and parameter provenance are preserved; value-dependent
proofs retain the existing cache-admission owner. No new evaluator or cache is
introduced.

Singleton JOIN peers are admitted only through guarded PROJECT over the
executor's one-row dummy VALUE_SCAN. Proof follows existing projection
normalization; column references and changed predicate estimates are refreshed.
Existing runtime-filter and outer-join policies remain authoritative.

The prepared regression's operator-scoped regex checks the lookup table scan's
own inputBlocks and inputRows counters. Result cardinality and ordinary EXPLAIN
runtime-filter checks remain independent assertions. Timing, costs and node IDs
are not performance oracles.

## LAST_DAY and EXPLAIN

New LAST_DAY bindings return nullable DATE values and metadata, including
CTAS and views. Native DATE/DATETIME evaluation avoids string formatting.
Invalid, zero and NULL inputs remain NULL. Existing serialized VARCHAR overload
IDs retain their executable ABI; mixed-version rollout is not part of this
contract.

Prepared EXPLAIN, ANALYZE and PHYPLAN classify the underlying SELECT while
retaining the diagnostic wrapper. Explained DML does not enter SELECT-only
numeric proofs.

## Validation boundaries

Focused decimal tests cover precision, scientific spellings, rounding, syntax,
radix and bounded-exponent behavior. Coefficient tests cover physical widths and
reject non-digit or over-width inputs; a short-text allocation guard and
benchmark protect the hot path. Public frontend tests exercise persisted values,
prepared rebinding/DML, DATE metadata and EXPLAIN consumers.

Performance measurements compare matched source/toolchain/native modes.
CAST-stage or scanned-work improvements do not establish whole-query throughput.
Current execution evidence and review decisions belong on the PR, not in this
contract document.
