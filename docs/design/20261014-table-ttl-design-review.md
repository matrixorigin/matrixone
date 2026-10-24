# Table TTL discovery: distinct design self-review phase

Issue: [#24552](https://github.com/matrixorigin/matrixone/issues/24552). Implementation PR: [#30072](https://github.com/matrixorigin/matrixone/pull/30072). Review input: `b65fa0d0b98bbc0b69931e5882a54b75cf547ff4`; comments from aptend and XuPeng-SH.

Phase: design only, completed before the discovery implementation amendment. Reviewer: the implementing Codex assistant, acting in a separate author design self-review phase. This is **not** an approval from aptend, XuPeng-SH, or any maintainer; external re-approval remains required for merge. No GitHub review state is changed by this record.

| Exact reviewed document | Git blob | SHA256 |
|---|---|---|
| [20261013-table-ttl.en.md](20261013-table-ttl.en.md) | `e483ee8613d748b2f816e6c7fce3ee56efdaa0d8` | `44c4d4d7b6d169b59eace2e167af45ee7fa5b5aa3af636cb9df5b1693199e62a` |
| [20261013-table-ttl.md](20261013-table-ttl.md) | `b8ff49e061b51f40b813de46642545c124fa280b` | `1b8cc571d15883b93df6e915995bc6520ee65ef48a69675a5bdcd7f3365735df` |

Change scope / trigger: the complete table-TTL feature persists policies and deletes tenant rows through parser/planner/catalog/task/transaction boundaries; the background worker and persistent/public capability independently trigger design-first review. The original 793 changed non-generated production lines also exceed the size threshold.

Blocking findings resolved in the proposal:

- The first owner's all-table materialization and candidate copying have explicit page, width, count and conservative byte-credit bounds enforced before candidate copies.
- Every acquired Result has immediate cleanup; cancellation checks include result handoff and decode loops and prohibit returning/publishing a partial window.
- Diagnostics have bounded samples/details and retain no unbounded metadata or underlying error tree.
- One task closure owns a constant-size fenced cursor and non-waiting admission token. Publication follows completed table attempts; cancellation cannot skip unattempted candidates, while a finite upper fence makes each stable assignment's scan cycle finite.

Decision log: choose keyset pages over an all-result deadline or a new streaming credit protocol. Accept existing engine admission/cache/scan-resource semantics rather than claiming a new whole-process RSS cap; accept task-local cursor reset on migration/restart rather than adding durable catalog/protocol state. Oversized metadata fails closed with bounded reporting. Document query-scan cost, rollout limits and the deterministic validation map, including the review's canceled-result counterexample.

Q1 ownership: one page result -> Close on success/failure/cancel before the next query; credited candidate window -> invocation return; task-local cursor/token -> registered executor lifetime; admission -> deferred release.
Q2 waits: each query inherits the invocation deadline; decode checks cancellation and finite-width input bounds; duplicate admission does not wait behind data work; existing DELETE/backoff/table budgets remain unchanged.
Q3 growth: page output, candidate count/credit, decode input, query count, diagnostics and cursor state all have finite caps independent of total catalog cardinality. Native engine buffering is an explicitly scoped existing control, not a hidden new TTL guarantee.

Design decision: **PASS for the amended proposal in this author self-review phase**. No blocking architectural question remains for implementing the amendment. Acceptance remains contingent on the mapped deterministic tests, real catalog SQL validation, race evidence and external implementation/design re-review. Implementation approval is not asserted here.

Refinement self-review: clarify transient bounded policy decoding versus retained candidate credit, and apply the same credited-length predicate to locked revalidation. These close the same memory-owner contract, retain identity/byte revalidation semantics, and add no persistent state. The current exact revisions above were re-reviewed before delivery; decision remains PASS in this author phase, with external maintainer approval still outstanding. The original design-first checkpoint is commit `3f029bdbdc`.
