# Distinct design-review decision for PR #30916

Decision: **PASS / APPROVE (design only)**, 2026-10-24.

Reviewer agent: `/root/design` (canonical agent/session identifier supplied by orchestration). Designated review model/effort: `gpt-6.1-sol`, `xhigh`, as explicitly selected in the task dispatch; no separate runtime model introspection endpoint is available in this agent. No production/test implementation, expensive package test, git push, or external review submission was performed by this reviewer.

Change scope: the complete per-statement SELECT INTO OUTFILE receipt feature in [PR #30916](https://github.com/matrixorigin/matrixone/pull/30916), the three scoped repairs/optimization, and the requested rebase. Partial acceptance slice of issue #30745; issue #30815 collector/session/timer changes excluded.

Design gate trigger: additive persisted `exec_plan.outfile` JSON; frontend/exporter/tracing ownership transfer; nonaggregation admission; privacy and hot-path diagnostic behavior. The gate applies regardless of production line/package thresholds.

Reviewed artifact: `design.md`, intended to be committed verbatim as `docs/design/statement-outfile-audit-receipts.md`.

Reviewed design SHA256: `6c41942200b3e1788b0cc411a4289e36b78040ffb3dde7f306728c50bffea00e`.

Reviewed source revisions:

- Original feature head: `ad4dff1adb408bb0b23048f08f501c5a72393041`.
- Original base: `2b388dd1690f83e2e807d6809180336dae2a68eb`.
- Frozen requested rebase target: `70e48cbef1235a8ef4b6c887cb37b0947bace714`.
- Clean rebased feature head: `3a3052bdc938e0a01e80655f7c38effaec31bf88`.

Review method: read task/PR/linked-issue records and the complete original diff; inspected the causal exporter, status-statement, statement admission/prepared diagnostic, tracing ownership/aggregation/serialization, fileservice grammar, and persistence-capacity consumer paths; checked relevant rebase deltas. Applied mo-dev feature-design-review, validation-evidence and testing contracts plus mo-self-review. This is source/design evidence, not certification of historical author tests or a package-runtime pass.

Blocking design findings: **none**. The document now closes the previously missing mandatory artifact/decision. Implementation remains blocked from delivery until it matches this design and supplies the defined evidence.

Resolved decisions and required implementation changes:

1. Keep additive version-1 per-statement JSON and existing telemetry availability/retention semantics; explicitly distinguish exact complete output from incomplete sink-confirmed lower bounds. Do not call this a crash-consistent or exactly-once audit log.
2. Finalize panicking OUTFILE execution through abort/cancel/join, publish incomplete confirmed totals, then re-panic the original payload for the established outer recovery. This prevents successful-looking prefix receipts and a worker-publication wait that otherwise precedes cancellation.
3. Preserve original URI path bytes while parsing credential boundaries. Correct the Unicode/literal-percent identity collision. Keep the prior `@`/literal `%` fix closed and retain its existing tests.
4. Defer prepared SQL redaction until environment/separator output budget admits the prepared SQL. Continue full-source redaction before clipping whenever prepared SQL can be retained. Demonstrate the narrow avoided-work benefit; do not claim all retained prepared redaction is O(limit).
5. Preserve exact JSON number tokens, cache/clone/pool ownership, bounded receipt-first plan fallback, aggregation exclusion, existing concrete SQL errors, ordinary statement behavior and tenant access controls.
6. Rebase onto the frozen target and validate the new base's JSONLINE duplicate-column failure as incomplete with its existing SQL error.
7. Keep cloud/backend runtime limits explicit and defer the pre-existing Parquet dynamic-fileservice close issue to separate ownership work; it does not change the chosen `Write` confirmation semantics.

Validation acceptance: focused deterministic regressions for the three changes, required frontend/motrace normal/race ownership coverage, incremental static checks, real direct/binary-prepared/stage public paths and independent actual-file byte/row oracles, final clean delivery review. The main execution agent owns these checks and must record exact final relevant revisions, nonempty selection, mode and terminal outcomes. Historical checks are reusable only under the documented semantic-validity rules.

Implementation deviations at approval time: **none; repairs not yet implemented**. Material changes to completion meaning, privacy/target policy, persistence shape, aggregation ownership, or resource protocol require review of the affected design section before implementation approval. Editorial/path-only movement with identical content does not invalidate this SHA256 approval.
