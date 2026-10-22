# Declared precision proofs for Decimal256 multiplication

Owner: #30840; umbrella #30838. Implementation PR: recorded in the PR that contains this document.

## Reviewed design

This document records the corresponding PR from the user-approved three-PR plan (2026-10-23, “Implement the plan”). Base at design capture: `060cb430e9ff59e60dc7bdade05758d343516acb`. Each branch starts from authoritative main, does not include or depend on #30880, and leaves its cast files untouched.

## Contract and implementation

For valid typed operands, preserve coefficient, scale, rounding, NULL/selection, overflow/error and result metadata while eliminating only range scans implied by declared precision.

Precision 1..9 proves signed int32 fit, 10..18 proves signed int64 fit; unknown/invalid metadata is unproven. Preserve int32-before-int64 tier order and all existing kernels/fallbacks. Proofs are immutable and batch-local. Keep metadata-free d256Mul for direct callers. No cast, planner, wire, representation or aggregate-state change.

## Alternatives and boundaries

The unchanged implementation is the comparison control. Broader cache-policy changes, physical decimal representation changes, optimizer rewrites and aggregate-state redesign are deferred because they have different correctness/resource contracts and would obscure attribution. No new SQL/configuration, persistent/catalog/disk or inter-service wire interface is introduced. Existing success/error/cancel/cleanup owners remain unchanged. Reverting this branch restores the prior implementation without data migration.

## Evidence and acceptance

Precision 9/10 and 18/19, signed boundaries, scalar/vector/NULL/selection/empty shapes, scales and rounding, wide overflow fallback, public metadata, allocation and wide-path controls.

Use the owning-package tests and actual public dispatcher where applicable. Performance uses identical baseline/candidate builds and retained images, excluded cold warmups, separate instrumented profiles, and sequential runs. Benchmark scripts execute/drain queries and collect completion/resource metadata; no input/table/schema/row-count/reference/result verification. Report uncertainty and controls; no unmeasured whole-suite speedup. This focused PR relates to its owning issue and does not automatically close broader unfinished work.

## Design review record

Trigger: material hot-path impact; versioned design before implementation. Invariant and resource ownership reviewed against current main. Decision: PASS for the bounded scope above; approval source is the user's accepted plan and implementation instruction. Mandatory implementation self-review, focused evidence and current-main pre-push checks remain required.
