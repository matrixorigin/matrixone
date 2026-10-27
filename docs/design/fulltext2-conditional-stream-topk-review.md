# Design-first decision, 2026-10-27

Scope: conditional exact stream Top-K within existing Draft PR #31407.
Trigger: planner/plugin/search ownership boundaries, optional plan JSON, cache
generation provenance, hot-path memory/cancellation behavior.

Reviewed design: [revision 1](fulltext2-conditional-stream-topk.md), SHA-256
`ae0c68d5e5c172d5f94ce8bfe03e2f7517fc7a991b6127449e36959185983cff`.
Source before implementation: `b366f221c3f69d038b66fe8857d4434e5f683354`,
combining existing PR head `42a5e13b249995009cc9cb2c4fddaea7706fd2ad`
and freshly fetched main `baa83f4b96e56ec214f92c68f7b56539b9e0fa45`.

Independent design reviewer: current-model agent fulltext_gap_code_audit.
Decision: PASS, no blocking design findings. Its source audit verified the
cache lock, readonly transaction/snapshot closure, source-bar contract, original
scorer/liveness, and byte-order VARCHAR comparator. Sensitive implementation
seams: derived cancellation context must reach the callback/collector; relation
lookup must use the original transaction without resetting shared handles;
load provenance must be invalidated before retry and published only after
successful unchanged readonly loading. Early-exit SourceCommitTS is acceptable
only when it proves C > B and rejects acceleration.

Root design decision: PASS for implementation under revision 1. The selected
stage preserves the existing exhaustive scorer; it does not enable WAND pruning
or remove the source join. The no-tail restriction deliberately limits first
rollout. Remaining work is implementation and empirical verification, not an
unresolved correctness decision. No production changes existed at approval.

Implementation owner: GPT-6.1-Sol, extra-high reasoning, with root responsible
for analysis, review, performance acceptance and delivery. Any material design
deviation must be reviewed before delivery. This design approval is not a claim
that the implementation passes tests or improves performance.

Risk map: planner eligibility/optional JSON R3; loaded-generation visibility
proof R3; query-local bounded collector and cancellation R3; existing row-key
and score-batch changes retain their previously mapped evidence. Final evidence
must cover every changed closure, including negative public SQL controls,
static/package checks, focused lifecycle/race tests and real same-data A/B.
