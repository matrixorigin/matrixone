# GROUP_CONCAT diagnostics: design review record

## Decision

**PASS — design only**, reviewed independently with GPT-6 Astra, medium
reasoning, on 2026-09-10. This is the distinct design decision requested by
PR #28504 review `5161993522`, not a retrospective claim that the original
implementation had already satisfied the design-first gate.

Reviewed document: [revision 1](GROUP_CONCAT_EXECUTION_DIAGNOSTICS.md).
Original reviewed commit: `5df121ebed123898fdb31c6c5a9fd5184af5de3a`.
Document Git blob: `a2d2e634ab5179aaa88ce1c289cab96d299a6dc8`.
The document is byte-identical after rebase onto main
`fc621e3616d229c7a29d0c80e73ed8eb1997459c`; the blob identifies the approved
content independently of rewritten commit IDs.

## Review conclusions

- Attempt-owned diagnostics isolate failed retries and late callbacks without
  replacing session identity. Direct session publication plus text deduplication
  would lose independent warning instances; session snapshot/restore cannot
  isolate late writers.
- Append/seal has a mutex linearization point; publication happens outside the
  lock. Child success feeds the parent collector, and parent failure discards it.
  Successful `Compile.Run`, not transaction commit, is the publication boundary.
- The prepared floor belongs to the logical statement and survives physical
  recompilation. Migration replays PREPARE and starts a new floor lifetime:
  an original floor of 1024 with a current limit of 5 can become 5 on migration.
  This is an accepted, documented limitation, not a guarantee of preservation.
- Exact diagnostic count is separate from retained samples. Aggregate row
  numbers are contribution order, not global source row positions; distributed
  arrival order can change which samples are retained.
- The design accounts for per-owner retention, process/scope binding maps,
  callback retention, bounded lock work, variable message bytes, and independent
  aggregate evaluation costs. It does not claim a measured performance win.

## Evidence limits

This decision used document and targeted source inspection, not executed tests
or benchmarks, a full implementation approval, live mixed-version/distributed
validation, or CI verification. `TestWarningAttemptNestedAndBounded` proves child
success followed by parent discard; its name must not be treated as evidence for
every nested outcome. Implementation acceptance and rebase validation are
recorded separately in the PR body.

## Internal SQL handoff amendment

The subsequent implementation review found that new top-level internal SQL
processes inherited Session but not the active warning attempt. The corrective
design captures the current explicit `Process.WarningSink` in a typed context
value at internal SQL call boundaries and installs it before child planning.
It does not capture the mutable Process or replace Session. An explicit nil
binding masks an inherited binding; no sink and no inherited value allocates
nothing. Each retry captures its own sink, while old contexts retain the sealed
old sink. The same small carrier serves the internal executor and frontend
SqlHelper bridges. The independent GPT-6 Astra medium follow-up accepted the
common handoff and required both bridges to preserve this ownership rule.
Outcome tests must use the real internal executor boundary, not manually pass
the parent collector into `child.finish`.

The independent source-level delta review of this correction found no production
blocker: both bridges capture/install the sink, preserve Session, and mask stale
bindings correctly. Test limits remain explicit: the child-failure case may
fail during planning, while parent terminal outcomes use collector transitions.
Existing real `Compile.Run` failure/retry tests are complementary evidence.
Execution and SCA results are recorded separately in the PR body.
