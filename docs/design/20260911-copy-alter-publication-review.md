# COPY ALTER publication design review

- Design: [COPY ALTER publication protocol](20260911-copy-alter-publication.md)
- Issue: [#28319](https://github.com/matrixorigin/matrixone/issues/28319)
- Implementation PR: [#28418](https://github.com/matrixorigin/matrixone/pull/28418)
- Revision reviewed: 1 (2026-09-11)
- Review type: independent design handoff review

## Decision

**Conditional pass for implementation handoff.** The design makes the
preparation/publication boundary, time identities, lock order, deferred task
ownership, retry generation, prepared-statement binding and rollback terminal
states explicit. It is sufficiently concrete for a maintainer to review the
implementation against one protocol rather than against an informal PR
summary.

This is a separate design artifact and decision record. It is not a maintainer
approval and does not waive the blocking design review requested on PR #28418.

## Review checklist

| Invariant | Design location | Required implementation evidence |
| --- | --- | --- |
| Source locks versus global gates | Contract; Preparation; Publication | Different-table barrier and lock-wait traces |
| `T_data`, `T_catalog`, commit visibility | Contract; Publication | Snapshot/PITR interleavings and lineage clone timestamp |
| View -> SNAPSHOT order and write barrier | Publication; Concurrency | Gate-order and optimistic-writer tests |
| Temporary relation/task ownership | State and ownership; Preparation | No temporary View/task residue; final-ID task inspection |
| Wait/reuse versus full retry | Contract; Retry | Copy count remains one for ordinary contention; two transaction IDs for retry |
| Prepared AST/database/parameter ownership | Retry | PREPARE -> database change -> EXECUTE and binary retry tests |
| Rollback/cancel/partial failure | Retry and cleanup | Failure injection at each listed phase and follow-up ALTER |
| Compatibility and cost | Scope; Validation | SI/optimistic/explicit/multi-CN matrix and paired workload |

## Open approval and acceptance items

The implementation PR must obtain a distinct maintainer design decision, then
re-request the blocking reviewers. The original nightly ADD/DROP workload,
paired performance comparison and QA evidence remain delivery gates even when
unit, BVT and CI checks are green.
