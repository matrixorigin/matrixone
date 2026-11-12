# Hotfix scalar CTE sharing and safe ZoneMap admission — v3-public.2

Design document for [PR #32550](https://github.com/matrixorigin/matrixone/pull/32550). Requested design reviewer: **XuPeng-SH**. The review covers restricted CTE-sharing admission, evaluation semantics, and the planner/compiler/pipeline ownership boundary. Maintainer design approval is **pending**; publication is not approval.

### Version, scope, and approval provenance

- **Document:** `v3-public.2`, published on 2026-11-11. This is the repository-hosted sanitized public edition of the locally reviewed **v3, section 8**, with implementation constraints and validation outcomes made explicit. It is being published retrospectively for maintainer traceability; it was not publicly reviewed before implementation.
- **Revision history:** [`v3-public.1`](https://github.com/matrixorigin/matrixone/pull/32550#issuecomment-6813247903) was initially published in a PR comment. `v3-public.2` moves the authoritative document into `docs/design/` at the requester's direction, without changing the technical design. Git history and the PR's commit-pinned file link identify the exact repository revision.
- **Base:** [`54feca6423`](https://github.com/matrixorigin/matrixone/commit/54feca6423c2e2e24b57f58e66eaf951891607cc), `4.1.6-hotfix`.
- **Implementation under review:** [`ce71f6e3bf`](https://github.com/matrixorigin/matrixone/commit/ce71f6e3bf3c2aa58772b2087c77be4621c06e0f) (ZoneMap) and [`9f19e6fb7c`](https://github.com/matrixorigin/matrixone/commit/9f19e6fb7c842d942e22ac3c6882f281929cdef6) (restricted scalar CTE sharing).
- **Subsequent CI expectation-only fix:** [`35edb87c48`](https://github.com/matrixorigin/matrixone/commit/35edb87c486efafe0ea66f08af93f64d9e6c20e8). No production Go or SQL changes relative to the reviewed implementation.
- **Approval provenance:** the task requester reviewed local v3 and replied **“go ahead”** before implementation, then **“go on”** to authorize commits/push/Draft PR delivery. These are private-session scope/implementation authorizations, not a GitHub maintainer design approval. There is no public URL for that private exchange; this statement is an attribution, not an independently verifiable approval link. The requester has now explicitly asked for this design review. **XuPeng-SH design approval is pending**; its eventual GitHub permalink should be recorded separately.
- This repository file is the authoritative design document. The earlier comment is historical, not the canonical document location. Semantic design changes must increment the document version, describe the changes, and link the exact Git revision for renewed review.

### 1. Problem and invariants

Two CTAS workloads repeatedly inline the same costly common aggregate through four/eight UNION ALL branches. Block pruning alone cannot remove that repeated computation. Conversely, sharing computation does not substitute for safe storage pruning.

Required invariants:

1. Preserve SQL values, multiplicities, types/precision, nullability, observable errors, authorization, and snapshot identity. Do not promise an order that the original SQL did not specify.
2. Preserve CTAS `HaveDDL=true`, same-transaction target-table visibility, and the existing placement decision. Do not force CTAS MULTICN.
3. Share only when declaration identity, source equivalence, structural cardinality, expression safety, consumer admissibility, and bounded cost can be proved; otherwise retain inline planning.
4. A missing/unreliable ZoneMap must retain data, never justify false-negative pruning.
5. Reuse existing pipeline data/terminal ownership. Do not introduce a general replay/materialization store, spill mechanism, cross-query cache, or new terminal protocol.

### 2. Alternatives and chosen boundary

- **Only apply #32522:** insufficient; hotfix has no nonrecursive CTE-sharing caller/foundation for that proof.
- **Backport the complete main CTE materialization/independent-reader/spill subsystem:** broader dependency and lifecycle changes than this hotfix should carry; rejected.
- **Rewrite UNION into a new CROSS JOIN/CASE optimization:** adds a second relational/type/evaluation proof rather than reusing the branch's SINK infrastructure; rejected for this iteration.
- **Chosen:** two independently reviewable/rollbackable closures: safe ZoneMap admission and a restricted scalar-only sharing path using existing SINK/SINK_SCAN, plus the non-truncating LIMIT proof.

This is a target-branch adaptation of #32012/#32419/#32522, not a pure cherry-pick or a claim of complete main/4.2-dev feature parity. Partition behavior changes, procedures, UDFs, warning compatibility, and unrelated fixes are outside scope.

### 3. Closure A — safe ZoneMap admission

**Owners/consumers:** `pkg/sql/plan/stats.go` → `pkg/vm/engine/readutil/{expr_filter.go,pk_filter_base.go}` → existing ZoneMap APIs.

- Admit supported stable direct-column comparisons/ranges without requiring favorable estimated block selectivity, including missing/error statistics. This branch's existing helpers are not sufficient to rule out volatile expressions, so an independent expression-stability check is required.
- Preserve row-level residual predicates, estimates, explicit `blockFilter=2`, and existing index/composite-key contracts. Partition tables keep their previous admission policy rather than importing #32419's partition work.
- Normalize constant-on-left inequality direction in both reader and PK filtering without modifying shared expressions/function IDs.
- Missing/uninitialized summaries fail open. Retain #32431's legacy NaN correctness. Floating keys cannot justify sorted seek/early-stop or raw-byte PK/Bloom shortcuts when NaN ordering or signed-zero representations could lose rows.

**Risks/oracles:** reversed comparisons, missing summaries, NaNs/signed zero, stable/volatile expression distinction, and residual-result equivalence. A six-row flushed fixture distinguishes genuinely prunable ranges from interleaved ranges without a timing assertion.

### 4. Closure B — candidate identity and bounded admission

**Owners:** `pkg/sql/plan/{query_builder.go,types.go,scalar_cte_reuse.go,opt_misc.go}`.

- Record each independently bound CTE occurrence by the same `CTERef` declaration. Keep existing binding, privilege, and snapshot handling; do not substitute SQL text, business names, or estimated `hasSingleRow` for identity.
- Inspect the complete tree before column pruning/final remapping. Compare actual bound sources, including table/object identity, snapshots, schema/types, expressions, and fresh binding-tag mappings. Shadowed declarations/different sources must not collapse together.
- Admit a nonrecursive, noncorrelated, deterministic aggregate producer whose published output is structurally **at most one fixed-width row**. Internal grouped aggregation/nested CTEs may participate only when the final producer meets that proof.
- Restrict this version to one-step SELECT or ordinary INSERT SELECT/CTAS. Do not renumber recursive/complex DML steps or introduce shared-producer dependency chains.
- Bounds: **one shared producer/query, at most 64 readers, 256 output columns, and 64 KiB of fixed-width output**. Unknown/variable-width outputs stay inline. These are output/admission bounds, not a claim that the entire aggregate's working memory or executor vector capacity is capped at 64 KiB.
- Retain a scan-cost profitability check. Shared result storage/reader copies do not scale with base-table row count; receiver metadata and copies scale with the explicitly bounded fan-out/schema. No wall-clock performance guarantee is used as a correctness proof.

### 5. Evaluation semantics and non-truncating LIMIT

- Determinism alone is insufficient: widening projected demand can expose errors hidden by pruning. This version conservatively requires all producer expressions to be read-only and data-error-free under the target executor contract. Unknown functions, volatile calls, checked integer/high-precision decimal SUM, ABS overflow, and fallible casts remain inline.
- Floating SUM, Decimal64 accumulation into Decimal128, and valid Decimal128 precision ≤28 SUM are admitted according to this branch's actual Fill/Merge/DISTINCT Flush behavior. The precision rule matches its unchecked executor paths; it is **not** a mathematical assertion that arbitrary decimal sums cannot overflow.
- Before the first UNION ALL, consumers may project but may not introduce filters, JOINs, internal LIMIT, branch OFFSET/window, or other row-domain reductions/early-stop shapes. After UNION ALL, only the supported unary wrappers and structurally safe LIMIT are allowed. Finite/unknown session SELECT limits fall back.
- Structural row bounds: scalar aggregate ≤1; supported single-input nodes propagate; UNION ALL adds two known bounds with overflow protection. Unknown/cyclic/unsupported shapes yield **unknown**, not zero rows.
- Only a **positive literal LIMIT covering the structural bound** is non-truncating. Statistics, predicates, OFFSET, and LIMIT itself never tighten the bound. Zero/dynamic/smaller LIMITs remain inline. An unordered WINDOW is not independently assumed to drain input, and a branch-local blocking node does not by itself prove whole-UNION drain.

### 6. Publication schema, placement, and reader isolation

**Owner:** `pkg/sql/compile/compile.go`; existing dispatch/merge/connector/PipelineEdge/pSpool are consumers, not new protocol owners.

Publication sequence:

1. Replace accepted references with separately tagged SINK_SCAN readers; keep one private producer tree and SINK step, marked `ExtraOptions="scalar_cte"`.
2. Compute the union of demanded producer columns. Count-only readers retain an explicit row-carrier column.
3. Compact/project the published schema before broadcast. Remap reader columns to compact publication positions **after** source pruning; never use stale original ordinals. Apply this contract only to the new marked mode, not recursive/DML SINKs.
4. Merge the final scalar producer result onto the coordinator CN, then broadcast locally. Remote source scans/partial aggregates remain eligible for their normal placement; a set of remote scopes is not mistaken for one coordinator-local publisher.
5. Each reader passes through an existing connector/spool private-copy boundary before evaluating its own projection. A projection is **not necessarily vector-read-only**: decimal casts can change vector type metadata. Borrowing the same vector for two projecting readers without isolation is unsafe.

### 7. Ownership, terminal paths, and failure scope

- The producer's existing pSpool owns broadcast batches. Readers release their own borrowed references; private connector spools own their reader-local copies. No reader may mutate another reader's published batch/schema.
- Successful End and failed/cancelled execution use the branch's existing terminal publication, abort, receiver reset, and attempt-owned quiescent cleanup. This PR does not alter dispatch/merge/pSpool terminal protocols or depend on a spare data-queue slot for terminal delivery.
- The wait-for graph must not require a reader to wait for itself: producer → local broadcast edges → reader private copies → UNION consumer. Failure/cancellation must settle pending/borrowed data and partially prepared or published edges before attempt memory is released.
- Relevant paths: success, producer error, consumer expression error, cancellation, partial preparation/registration/publication, repeated prepare/execute, and final cleanup. If these require a new terminal protocol/materialized source, stop and request a separate lifecycle design review rather than expanding this hotfix implicitly.

### 8. Validation and evidence limits

**Risk:** ZoneMap/planner/schema contracts are R2; the newly connected shared pipeline DAG is R3. The feature-design gate applies even though existing runtime components are reused.

Completed validation summaries (raw customer SQL, credentials, local paths, and operational logs are deliberately excluded):

- Go/gofmt 1.26.4, branch-local native libraries/CGo wrapper, vet and incremental lint; changed-line coverage **84.25%**.
- Planner tests: 2/4/8 branches and N−1/N/N+1 LIMIT boundaries; unknown/overflow/cycle controls; declaration/source/snapshot/type identity; pruning/count-only; unsafe-inline cases.
- Compiler/operator tests: local/remote-shaped producers and 2/4/8 readers; 4/8-reader actual connector/merge/spool execution, concurrent vector mutation isolation, End/source error/cancellation, partial preparation/publication, and zero residual mpool usage. Targeted race validation passed.
- New BVT: scalar CTE **41/41**, ZoneMap **60/60**, each verified twice; adjacent ordinary/recursive CTE, recursive INSERT, and legacy NaN cases passed. NULL/empty input, duplicates, OFFSET/dynamic limits, consumer overflow followed by successful prepared reuse, and CTAS metadata were checked.
- Both complete incident SQLs on their original schema with **12 synthetic rows**: base scans **8/4 → 1/1**, actual CTAS **8/4 rows**, business values and table metadata equal to disabled-optimization controls. Unordered row-number assignments are excluded from equality claims.
- Two independent local CN processes: both source SELECTs matched from either coordinator. Actual remote contribution was established by zero local scanned rows on one coordinator while the producer received grouped inputs and returned the correct nonempty results, not merely by the MULTICN label.
- CI at `9f19e6fb7c`: UT/SCA/UT Coverage passed; 33 EXPLAIN expectation mismatches were subsequently fixed in `35edb87c48`. The 10 affected cases passed twice locally: **999 successes, zero failures, eight unchanged existing decimal ignores per round**. Coverage aggregation was blocked by the failed BVT producers, not a calculated low-coverage result. No claim is made about the new CI outcome before it completes.
- Three local Parquet tests failed identically on a verified clean base; those unrelated failures were disclosed in the PR rather than changed here.

**Not established:** production-scale latency, throughput, or memory acceptance. Small-fixture scan reductions are not production speedup guarantees. No production deployment/write/load test was performed during implementation/validation.

### 9. Rollback and requested decision

- Keep the ZoneMap and scalar CTE changes as separate logical commits. `blockFilter=2` and `scalarCTEReuse=2` provide existing-session comparison/disable paths; this design does not authorize production configuration changes.
- Unsupported/uncertain candidates continue inline; storage pruning retains residual evaluation. No persisted catalog/wire format or upgrade migration is introduced.
- Please review: (a) whether the scalar-only restriction is an acceptable hotfix prerequisite; (b) totality/drain and schema proofs; (c) coordinator publication and reader isolation; (d) existing terminal/attempt ownership for this new DAG; and (e) whether the evidence is sufficient for this scope.
- **Decision requested:** approve `v3-public.2` at its linked Git revision or request explicit design changes. Until that response, maintainer design approval remains **pending**; publishing this document does not itself resolve the design-gate blocker. Record the eventual GitHub approval permalink in the PR; do not substitute private-session authorization for maintainer review.
