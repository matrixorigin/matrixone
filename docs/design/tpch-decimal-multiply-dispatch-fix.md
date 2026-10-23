# Decimal256 multiplication dispatch correction

Owner: #30840; umbrella #30838. Implementation is the PR containing this document. Design approved by the user's three-corrective-PR plan and subsequent implementation instruction (2026-10-23); the user explicitly requires retaining bulk/Q15 gains instead of reverting #30885.

## Contract and cause

Preserve coefficients, scale/rounding, NULL/mask/constant semantics, physical overflow and declared result precision. The merged capturing callback adds classification and an extra call on scalar/unknown-domain paths. Confirmed one-row regressions are 2.51%/2.95%, even though proven bulk multiplication improves. No regression is an accepted tradeoff.

## Implementation

Use a private typed Decimal256 multiply driver. Read already typed vector storage directly, including one physical value for constants, without parameter-wrapper or operand-slice allocations. Resolve constant NULLs, input NULL union and selection before reading operands or computing proofs. Reuse existing selection/evaluability helpers. Compute proofs only for multi-value physical operands, once per call; keep unknown/scalar operands dynamic. Call one canonical arithmetic kernel directly with explicit proof arguments, removing the forwarding kernel and capture closure. Keep existing arithmetic/scaling/tier ordering unchanged. Constants broadcast to the requested logical rows; an all-masked call never evaluates overflowing constants.

Share the existing declared-precision validator between the generic Decimal256 wrapper and typed multiplication, with unchanged error translation and metadata. Generic operand preparation and other arithmetic kernels stay unchanged. Proofs and scalar temporaries are stack-local; vectors retain their existing owner and no input metadata/NULL bitmap is mutated. No stored proof, worker, cache, interface, format, configuration, planner rule or new dependency.

## Alternatives and risk review

A rollback is not authorized: it loses bulk/Q15 gains. A scalar guard over the current callback does not remove the forwarding call. Duplicating full arithmetic kernels or caching proofs introduces unnecessary complexity or invalidation state. The typed entry removes dispatch overhead and reuses the existing numeric implementation. Shared precision validation requires sibling ADD/SUB/DIV controls. This is a bounded hot-path refactor requiring design-first review; decision PASS for this approved design, not performance acceptance of an implementation.

## Validation and acceptance

Independent numeric oracle and public dispatcher tests cover scalar/vector/constant/NULL/masks, tails, mixed ranges, extreme scales, overflow, result metadata, broadcasting and result reuse. Reuse the 54-case audit and extend tiny/tail and sibling arithmetic controls. Compare pre-30885 and merged implementations on the same base/native build against the candidate. Preserve bulk/Q15 benefit and eliminate scalar/fallback regressions; confirm suspected regressions independently. Use warm balanced resource-matched SF10/SF100 target/control and full-suite replays, separate profiles, retained samples and no dataset/result verification in scripts. Confirmed regressions FAIL; unresolved signals stay INCONCLUSIVE. Passing functional/static checks or merging is not performance acceptance.
