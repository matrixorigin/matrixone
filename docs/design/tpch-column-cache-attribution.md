# Cache admission and column reconstruction counters

Owner: #30849; umbrella #30838. Implementation PR: recorded in the PR that contains this document.

## Reviewed design

This document records the corresponding PR from the user-approved three-PR plan (2026-10-23, “Implement the plan”). Base at design capture: `060cb430e9ff59e60dc7bdade05758d343516acb`. Each branch starts from authoritative main, does not include or depend on #30880, and leaves its cast files untouched.

## Contract and implementation

Counters explain existing behavior without changing admission, eviction, decoding, buffer ownership, or page-discard policy.

Reader threshold blocks; FileService policy skips, eligible entries, admitted entries and pressure rejections; column construction calls, successful decoded bytes and errors, split by fixed codec classes. Existing CounterSet reporting/reset owns the counters. Updates are aggregated per block, IOVector or column construction, never per row. No labels, histories, timers or workers.

## Alternatives and boundaries

The unchanged implementation is the comparison control. Broader cache-policy changes, physical decimal representation changes, optimizer rewrites and aggregate-state redesign are deferred because they have different correctness/resource contracts and would obscure attribution. No new SQL/configuration, persistent/catalog/disk or inter-service wire interface is introduced. Existing success/error/cancel/cleanup owners remain unchanged. Reverting this branch restores the prior implementation without data migration.

## Evidence and acceptance

Cold/cache-hit, threshold boundary, explicit policy, every admission-rejection stage, decode error, counter reset, concurrent independent reads, and overhead controls.

Use the owning-package tests and actual public dispatcher where applicable. Performance uses identical baseline/candidate builds and retained images, excluded cold warmups, separate instrumented profiles, and sequential runs. Benchmark scripts execute/drain queries and collect completion/resource metadata; no input/table/schema/row-count/reference/result verification. Report uncertainty and controls; no unmeasured whole-suite speedup. This focused PR relates to its owning issue and does not automatically close broader unfinished work.

## Design review record

Trigger: material hot-path impact; versioned design before implementation. Invariant and resource ownership reviewed against current main. Decision: PASS for the bounded scope above; approval source is the user's accepted plan and implementation instruction. Mandatory implementation self-review, focused evidence and current-main pre-push checks remain required.
