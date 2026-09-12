# Mmap recovery design decision

Date: 2026-09-11.
Reviewer: Codex, in a distinct **design self-review** phase for this delivery.
This is not an independent human review, GitHub APPROVE, or permission to bypass
branch protection. Maintainer review of both implementation PRs remains required.

Scope: shared controller in #28737 and #28739, incident #28736.
Trigger: process-wide admission/resource controller, deferred cleanup ownership,
retry lifecycle and a background reporting worker, with availability impact.
Reviewed design: [v1 at 1c4858f0636362cfb0ace04d6d386fd066f14dd3](https://github.com/XuPeng-SH/matrixone/blob/1c4858f0636362cfb0ace04d6d386fd066f14dd3/docs/design/mmap-reclamation.md).

## Design phase: PASS for implementation alignment review

The initial ordinary-fix exemption was incorrect. The design was committed as a
separate artifact before this decision; it does not retroactively assert that
the first implementation followed the required order. No further implementation
acceptance was made while the document was missing.

The design was assessed against alternatives, rather than assuming the existing
code was the only answer. Panic, lost ownership, and unrestricted retention each
violate a goal. Arena allocation is credible long-term work but adds unrelated
allocation geometry, fragmentation and performance decisions. The selected gate
is accepted as bounded failure containment, not as a cure for map-count growth.

| Review axis | Decision / accepted limitation |
|---|---|
| Q1 ownership | Exact failed slices have one cleanup owner through detached retry; not reused, dropped or forgotten |
| Q2 waits | No queue/cache lock spans unmap or diagnostics; log sink independent. Kernel/proc latency is not a hard deadline |
| Q3 growth | Admission bounds mapping identities relative to pre-existing/in-flight work; queue is not an absolute memory quota |
| Availability | One failed mapping can gate the process's fresh mmap indefinitely; explicit, observable degradation is accepted over crash/leak |
| Retry | Bounded work/rate and fair rotation, not bounded lifetime or universal recovery time; other releases/operations may be needed |
| Performance | One atomic fresh-mmap check; no new hit/free-path allocation; source-matched microbench evidence, no universal zero-cost claim |
| Diagnostics | Bounded storage/report rate; dropped reports and one blocked writer accepted; metrics retain pending/failure evidence |
| Platform | Exact-slice unix wrapper contract retained; real Linux test covers the specific split-at-limit mechanism |
| Compatibility | Process-local, no disk/wire migration; rollback restarts into the old panic risk |
| Security/rollout | Restricted operational logs; no automatic sysctl changes; canary and persistent-pending intervention required |

Resolved design questions: distinguish retry rate from lifetime; distinguish
admission-relative retention from fixed capacity; state in-flight allowance and
out-of-scope allocations; distinguish cooperative diagnostic work budget from
blocking syscall latency; distinguish report submission limits from delayed sink
output; document process-wide failure scope and non-guaranteed self-healing.

Blocking design questions remaining: none for the documented containment scope.
Deployment-specific alert thresholds and mapping-growth root-cause investigation
remain maintainer/operations follow-up before broad rollout, not claimed solved.

## Subsequent implementation alignment phase

Reviewed 4.2 implementation `2e2a5b31e4e174a9251a0566befa0683180e3fd4`
against base `4199b11c5c7bb24eccdb379cd6f68abcbd89e450`; main implementation
`87fcca569d063240bcff1b00652353ff018291b4`, retained unchanged by the unrelated
main merge `547ebfcb16a3764a21a851ef2a809e4ff8dfae0f`.

Compared queue transitions, admission/reopen ordering, original-slice ownership,
bounded retry/reporting and branch-specific release callers against v1. 4.2 has
no HybridMmapAllocator, and intentionally does not add one. No material deviation
or newly demonstrated runtime defect was found. The review comment itself did
not establish another runtime defect, so no speculative runtime edits were made.

Evidence: reuse the branch-specific malloc/mpool normal/race results, focused
race x100 and actual-kernel 55 recovery x2 recorded in each PR. The delivered
follow-up changes only these two documents; implementation, tests, module inputs
and relevant dependency closure are unchanged. New checks: whitespace/delivery
diff, versioned design reference, and both branches' document identity. No new
UT, benchmark, static Go check or production experiment is claimed for docs.

Decision: design self-review PASS; implementation alignment PASS within the
stated scope. This record supplies a traceable design decision, not an external
approval. Any material change to the accepted policy requires renewed design
review; a maintainer can request a different availability tradeoff before merge.
