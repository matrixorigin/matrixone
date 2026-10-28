# Distributed ODKU result protocol: independent r2 design decision

Decision: **PASS**. Blocking design findings: **none**. Design approval conditions:
**none**. This is an independent approval of the r2 decisions and their integration
with r1, not inherited v1 approval or implementation/runtime acceptance.

- Issue: #28162; implementation PR: #28444.
- Reviewer: independent Codex agent Popper, dispatch ID
  `01a14406-78af-7420-a83b-aabc68e36324`; not the r2 author.
- Review date: 2026-10-16; final binding check: `2026-10-16T09:28:26Z`.
- Reviewed source: `0c2090984b2aebef9cf10fba304465b14e3f5c77`.
- Integration target: `b3d6b21fe9d260c91e1777529a48a9d93061af33`.
- Reviewed staged candidate tree: `f07e9a39d185e6afa270a8464995cc839f411eef`.
- Reviewed r2: `20261016-odku-distributed-result-protocol-r2-addendum.md`,
  blob `f59e2d8da15c8be6407f939b8eb6854f5f8e1b41`, SHA256
  `3c7824f054e20328e46c7d6e818ea7b8425765124d1faadc734e7cc18f4d6a0a`.
- Retained r1 with its historical-scope preface: blob
  `0f41de5514cdfc38a5ae26e9d65898d24b54d7a6`, SHA256
  `7382036c352faacc0d5fbe711927fe5007d92ae8b9368e2cf49d70e2700f2735`.
- The immutable v1 approval record is unchanged. Its approval still applies only
  to original r1 blob `914d5ddd19635d07924ebbe22cbb5eaa6f192fa2`.

After review, only r2's status metadata was changed to link this decision and to
keep implementation/runtime acceptance explicitly unclaimed. All substantive r2
decisions below the revision header retain their reviewed content. The original
PENDING status is superseded by this record; no old approval or runtime result
has been relabelled.

| Reviewed decision | Result and rationale |
| --- | --- |
| Actual successful plan owns the result | PASS: select legacy scalar versus tracked summary from the final plan, including retry rebuild; empty summary never selects stale legacy state. |
| Multiple visible AUTO_INCREMENT columns | PASS: preserve accepted DML and allocate later columns normally; first visible column consistently owns provenance and fallback references. Later-only generation intentionally does not publish a modern ODKU session ID. This is a documented MatrixOne policy, not portable MySQL behavior. |
| Visible internal-looking SQL name | PASS: visibility/internal catalog ownership distinguishes user IDs from hidden allocator columns without weakening constraints or tenancy. |
| Cumulative protocol requirements | PASS: upstream117, JSON118, typed MINMAX119 and ODKU/baseline120 remain distinct; every admission/send/decode/stream owner composes their maximum. |
| Retry, rollback, ownership and resource bounds | PASS: r1 coordinator publication, quiescence, explicit-zero baseline, generation reset, reducer bounds and performance budgets are retained. No per-column result vectors or new unbounded state are introduced. |
| Alternatives and validation | PASS: AST/empty-summary dispatch, broad statement rejection, cross-column reduction, name-based ownership and old-floor reuse were considered; deterministic tests, real-client results, canonical BVT, binary/fault and performance obligations can falsify the selected design. |

Implementation acceptance still requires executed owner/column/floor regressions,
raw OK/session/row assertions and normal comparisons of `on_duplicate_key_edge.sql`
and `last_insert_id_hidden_pk.sql`. Actual mixed binaries, ordered producer and
fault/retry qualification, r1's full measured capacity/performance matrix, CI and
deployment QA retain their separate owners and gates. None was executed by this
design review, and the complete candidate implementation was not approved.

Root retained the full independent `REVIEW.md` and `REVIEW.json` in task evidence
`heartbeat-20261016-0834.qUtx67/design-review-28444-r2/`, independently checked the
document/candidate binding, and confirmed the reviewer stopped before this record
was added. This repository record makes the design decision reviewable without
requiring access to that private evidence directory. It does not resolve GitHub
threads, request another review, or change `CHANGES_REQUESTED`.
