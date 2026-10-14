# Indexed MATCH under SQL OR and NOT

## Revision and gate

- Owning issue: https://github.com/matrixorigin/matrixone/issues/30166 (read in full, including its recipe and regression expectations; no comments as of 2026-10-14).
- Design revision: v1, 2026-10-14. Status: DRAFT, independent review required.
- Implementation: local work only; no implementation PR exists or is authorized for publication.
- Verified base/head/merge-base before this document: `5a0570ea70083c9365719009a5ed8d7618f9ed91`; isolated worktree `/workspace/shared/matrixone-30166` was clean.
- G-FEATURE-DESIGN applies: new SQL capability and material hot-path cardinality/resource change, independent of diff size. This document is the design-only phase. No production code may precede recorded independent PASS on its exact content revision.
- Approval record belongs in a separate versioned review document identifying this document's commit and content SHA256. Material design changes invalidate approval of affected decisions. No user reconfirmation is required for internal review; publishing remains outside scope.

## 1. Problem, contract, owner

The issue records error 20105 for `MATCH(body) AGAINST('needle') OR tag=1` and `NOT MATCH(body) AGAINST('alpha') AND tag IS NOT NULL`, for both FULLTEXT and FULLTEXT2. Positive conjunctive MATCH and several consumers pass. Issue receipt base `8586fa08715c868cab997d11d2687a76b8cc360f` differs from our verified implementation base; historical receipt is not a new local baseline run.

Current `collectDrivingFullTextMatches` deliberately descends AND only: its stream is INNER joined to the base scan, imposing index membership. Removing that guard alone loses scalar-OR rows and the complement required by NOT. Scalar MATCH has no full-table execution fallback. The first owner is QueryBuilder's indexed expression replacement at the base-table relational input; downstream operators must receive an ordinary scalar score expression and a correctly filtered relation.

**Invariant:** for a supported scan-local Boolean predicate, preserve every base row until the original expression evaluates on that row's indexed MATCH scores; retain the row iff the SQL predicate evaluates TRUE. Each MATCH answers its own table instance, exact index parts, pattern and mode. No rewrite changes base multiplicity, aggregation/window input, explicit ordering, or global pagination.

**Negation/minimum witness:** one nonmatching row with `tag=1` disappears under `MATCH OR tag=1`, or a nonmatching row disappears under `NOT MATCH`. Also, one row matching two OR leaves appears twice.

Goals: arbitrary nested AND/OR/NOT around supported scan-local MATCH leaves, scalar sibling predicates, score comparisons/projections/order for served leaves, classic and FULLTEXT2, prepared reuse, and snapshot provenance. Exact Boolean composition is relative to each index's existing visibility and scoring contract; FULLTEXT2 asynchronous index visibility is unchanged.

Non-goals: change standalone projection/order-only MATCH row inclusion; implement generic full-table/token LIKE fallback; change tokenizers/scoring; make FULLTEXT2 synchronous; optimize OR/NOT with new bitmaps or runtime engines; change SQL syntax/catalog/protobuf; support cross-table MATCH arguments or arbitrary MATCH solely above a relational barrier; default relevance sorting changes owned by issue 30164.

Compatibility precedent matters: maintainers intentionally retain score-zero omission in projection-only MATCH (https://github.com/matrixorigin/matrixone/issues/29299#issuecomment-5809939130 and https://github.com/matrixorigin/matrixone/issues/28327#issuecomment-5572594737); broad LEFT JOIN change PR 28340 was closed unmerged. The new path must require a MATCH beneath OR or NOT in a scan-local WHERE filter. A bare/projection-only positive query keeps the existing route. A SELECT score in a query routed by the new Boolean WHERE uses the preserved-row semantics below.

## 2. Exact scalar semantics and three-valued logic

Define `S_m(r)` from the COMPLETE existing search stream for MATCH m: emitted relevance if that stream contains r's primary key; otherwise typed FLOAT32 zero. An emitted zero or negative relevance is preserved, not replaced by a Boolean membership bit. Bare MATCH uses the binder's existing numeric-to-Boolean conversion: zero is FALSE, nonzero TRUE. Comparisons/casts/rounding retain their original bound expression; no algebraic approximation.

This is an explicit contract for the newly supported composition domain, not a claim that a previously unexecutable scalar form already returned these values. Missing index rows do not become SQL NULL merely because LEFT JOIN represents absence with NULL.

Indexed nullable content: NULL inputs are omitted by the existing tokenizer; a non-NULL sibling can still match. All-NULL indexed content with no emitted result has score zero in this new domain. We deliberately do not apply scalar function STRICT metadata to each original text argument: the function is a nonexecuting placeholder and indexed multi-column NULL behavior already contradicts “any NULL argument suppresses all siblings.” A new explicit BVT must pin all-NULL-body NOT with a non-NULL scalar tag, since issue row 1026's NULL tag masks that decision.

Scalar siblings keep SQL NULLs unchanged. Required truth table (U=UNKNOWN):

| a | b | a OR b | a AND b |
|---|---|---|---|
| T | T/F/U | T | T/F/U respectively |
| F | T | T | F |
| F | F | F | F |
| F | U | U | F |
| U | T | T | U |
| U | F | U | F |
| U | U | U | U |

`NOT T=F`, `NOT F=T`, `NOT U=U`; WHERE retains T only. Examples: missing MATCH OR NULL is U and excluded; missing MATCH OR TRUE included; NOT missing MATCH included; NOT missing MATCH AND NULL excluded; `MATCH=0` under an OR/NOT-routed query includes absent matches; threshold NULL remains U. Preserve original tree and engine truth functions rather than converting UNKNOWN to FALSE inside it.

Pattern/mode errors are not score defaults. Preserve original pattern expression, execution parameter indexes/types, mode validation and engine diagnostics. Classic `AGAINST(NULL)` / empty pattern reports its existing invalid-input error. FULLTEXT2 NULL pattern follows its existing empty-scan hook and therefore gives an empty stream/zero scores in this new domain; other parser errors propagate. Do not attach the existing zero-relevance threshold guard: it guards an INNER-driver precondition that the preserving path no longer needs. Do not silently standardize the two index kinds. No promise of branch short-circuit error suppression: a required MATCH stream is evaluated even when a scalar OR sibling is TRUE, as existing indexed planning does.

Normalize absence BEFORE an enclosing user outer join. A per-scan PROJECT publishes normalized score columns as part of the base relation; an enclosing LEFT/RIGHT/SINGLE join can null-extend those columns normally. Reapplying COALESCE above that user join would turn an absent table instance into a fabricated zero score and is forbidden. Cross-table expressions not placed on one scan stay outside this first implementation.

## 3. Selected relational design

Selected candidate: chain complete unique `(doc_id,score)` indexed streams as LEFT joins to the SAME base scan, publish normalized score columns, then evaluate lifted original predicates in a real Node_FILTER. Existing LEFT JOIN, PROJECT and FILTER operators own execution; no new runtime state or index plugin is required.

1. Detect MATCH below OR/NOT in a TABLE_SCAN FilterList. Validate ALL MATCH leaves of those affected predicates against that binding and exact index coverage before rewriting anything. Collection can traverse the existing scalar expression tree but cannot cross a subquery/aggregate/window ownership boundary. Unsupported leaf leaves the existing refusal path intact (or returns the same not-supported class), never partially executes a changed predicate.
2. Once triggered, collect and deduplicate ALL scan-filter MATCH leaves, including sibling conjunctive/bare drivers, plus downstream references to those served leaves. This prevents a later legacy INNER route or score range pushdown from restricting a preserving stream. A distinct unserved projection-only MATCH keeps its legacy behavior or refuses; it must not silently become a preserving driver. Prefer explicit refusal if mixing those routes cannot preserve their existing contract.
3. Split the scan FilterList only at its already-conjunctive list boundary: leave whole MATCH-free filters on base scan, lift whole filters containing MATCH. Do not extract `tag=1` from inside `MATCH OR tag=1`; it is not a necessary condition on all retained rows. Conservatively keep common scalar filters above if their ownership/volatility prevents safe placement.
4. Resolve/reuse the existing search-node construction, configuration, hidden-index/source identity, PK type and scan snapshot logic. Emit a COMPLETE stream per distinct m: no CandidateLimit, Offset, ScoreRange, relevance TopK, INCLUDE/pk/runtime membership reduction, bloom prefilter, covered-index path, or zero-relevance guard. Fulltext2 zero/unlimited search mode must remain exhaustive; verify in focused reader/typed-plan evidence, not by assuming LIMIT nil alone is sufficient.
5. Guarantee each score stream has at most one row per doc_id before LEFT joining. Current normal classic and FULLTEXT2 searches provide unique documents; assert this provider contract in owning-reader tests. JSONProbeMode is optimizer-internal and excluded from new leaf collection. Do not aggregate duplicates with MAX/SUM without a score contract. An unexpected provider duplicate violates the provider contract and requires fixing/reviewing that owner before admission, not arbitrary relevance arbitration.
6. LEFT join base/result-so-far with each stream on base PK=doc_id. Existing base PK or hidden row identity is the identity; preserve exact binary/composite PK type, width/scale/collation and encoding using the existing construction path. No join connects hit streams to each other with INNER.
7. Add a PROJECT over the preserving join chain with fresh binding tags for normalized `COALESCE(score,typed_zero)` slots; carry required base columns unchanged and remap all consumers. Each original base row produces exactly one output row. Repeated m shares one slot. Preserve NULLability of emitted scores/provider contract and use a presence test if scores can validly be NULL; current emitted score vectors are non-NULL.
8. Rewrite lifted predicate copies using normalized slots; build Node_FILTER above that PROJECT. JOIN.FilterList alone is not an execution filter and is forbidden. Publish served leaf-to-score references for PROJECT, SORT, AGG, WINDOW/PARTITION and join-child consumers via the existing served-match mechanism, with binding-aware equality and explicit replacement expression support.
9. Keep the full relational chain below existing AGG/WINDOW/order barriers when WHERE already belonged on that scan. Do not move HAVING/post-window filters or ON predicates down. Move an existing scan-owned LIMIT/OFFSET above the Boolean FILTER only when it represents pagination originally pushed from this query block; otherwise refuse a rewrite whose barrier provenance cannot be established. Existing explicit outer ORDER BY and LIMIT/OFFSET stay at their owner. New path introduces no implicit relevance sort.
10. Publish new nodes and remapping only after validation and node construction succeed. Builder-local maps live for this query build only. On error return error, no executable half-rewritten graph. Prepare retains expressions, not execution hit sets; reset/repeated EXECUTE uses existing readers and snapshots.

### Reachability and optimizer ordering

Use a new `apply_indices_fulltext_boolean.go` for detector, validation, collection and preserving rewrite. Minimal call-site routing in `apply_indices.go` TABLE_SCAN executes before regular-index rewriting. Include the same detector in `scanHasMatchedFullTextFilter` and `detectFullTextGuard`, because projection removal and early vector/regular-index paths currently depend on those guards. Keep the protected scan available until its preserving rewrite runs; consume it once.

The early ANN path runs before this late rewrite. It MUST recognize a new Boolean fulltext guard and decline candidate-truncating ANN conversion; exact distance sort remains correct over the full Boolean filtered set. A vector index existing on the table must not silently remove the scalar-only branch. Performance-oriented ANN over these predicates is deferred rather than claimed exact. Final projection handling must still visit ordinary downstream rewrites after serving scores; no early return that bypasses nonconflicting optimizations.

Per-scan replacement closes ordinary query, EXPLAIN ANALYZE, count/grouping/window and join-child WHERE variants without expanding each legacy anchor's driver collector. Scope joins only where the optimizer already placed this WHERE predicate on the scan; existing ANTI/MARK and ASOF/DEDUP pushdown eligibility remains unchanged. An arbitrary OR spanning table instances or remaining above a join has no scan-local authority and stays intentionally unsupported in v1. Score references must remain valid under join swapping, repeated aliases, project remaps, and outer null extension. Tests assert typed references and reachable paths.

## 4. Alternatives and decision log

| Alternative | Correctness/compatibility | Cost/complexity | Decision |
|---|---|---|---|
| Status quo / harvest OR into INNER | Status quo refuses capability; harvesting loses rows | Cheapest but incorrect | Reject |
| Whole-query hit-set union and anti-membership | Credible for pure Boolean leaves; general scalar branches, scores, NULL and overlap need careful bag/set handling | Needs base complement plus score lookup and disjoint-branch dedup; can multiply work/branches | Defer optimization, no benefit sufficient for initial complexity |
| Exact scalar full-table evaluation | Could evaluate any bound composition with true leaf semantics | No existing evaluator; tokenizer/TF-IDF corpus/snapshot parity and per-row hot-path cost become a new cross-owner contract | Reject for this scope |
| DNF OR branches using existing INNER plans plus scalar branches | Credible if made disjoint and UNKNOWN-aware | Exponential expansion, duplicated scans, overlap/zero thresholds and global pagination hazards | Reject |
| LEFT score attachment in new Boolean domain | Preserves bag multiplicity and unchanged tree; score/NULL explicitly defined | Base scan plus complete stream per distinct leaf, potentially expensive | Select bounded existing execution machinery, defer pruning |
| Globally LEFT join every MATCH | Broadly changes projection-only zero-row behavior | Known rejected precedent and unnecessary cost | Reject |

Accepted tradeoffs: new OR/NOT query reads base rows even with selective MATCH; no score/limit pushdown; exact vector reranking may use full sort; indexed NULL-body score-zero semantics are intentional in new domain; existing FULLTEXT2 async visibility and leaf-specific pattern errors retained. New SQL composition does not promise MySQL compatibility for every leaf.

## 5. Cost, lifecycle, security, compatibility

Let N be visible base rows after independently valid scalar conjuncts, k distinct served MATCH leaves, H_i documents in each complete stream, and E scalar expression size. Plan memory is O(k+E), conservatively O(k^2*E) equality/collection work using existing small-list dedup; do not add global cache. Search work is existing index search work plus all H_i emissions. LEFT attachment work is O(k*N + sum H_i) expected hash-probe cost; outputs stay N until Boolean filtering, never N*product H_i. Hash build memory is O(sum H_i*(PK_bytes+score+hash overhead)) subject to existing query/operator budget and cancellation; repeated leaves do not add streams. Variable-width/composite keys matter. If existing join budget cannot contain a build, propagate the ordinary memory error; never truncate to make it fit. Existing hash/aggregation ownership/spill policies remain authoritative; do not claim every hash join spills.

No background goroutine, queue, cache, retry, storage or durable resource introduced. Existing reader Close/reset owns search resources and query operators own temporary join/project/filter batches. Error, cancellation, memory failure, empty scan and repeated execution use those existing terminal paths. A lightweight canceled-query consumer test is appropriate if integration exposes it; race/topology/restart/upgrade suites are not required merely for planner node assembly, since no lifecycle protocol or persisted/wire shape changes.

No DDL, index rebuild, migration or catalog/protobuf changes. Rollback restores old refusal for new shapes and old plans for legacy shapes. CPU path applies to both indexes; no GPU-specific machinery. Existing source/index account, subscription, snapshot and access checks are reused, never reconstructed from user strings. Base table remains present, so scores cannot expose an unauthorized hidden-table-only path. No telemetry labels or literal-query logging added. Ordinary EXPLAIN shows LEFT score attachments and final FILTER; EXPLAIN ANALYZE must run those same semantics. Existing errors are diagnostic containment for unsupported leaf/index/provider contracts.

No feature flag added for an exact newly supported capability under already-existing experimental fulltext flags. Delivery remains blocked pending design review and required evidence. Operational fallback is normal not-supported/memory errors or rollback, never silently approximate Boolean membership.

## 6. Change map and shared edits

| Closure/artifacts | Owner/consumers | Risk | Required proof |
|---|---|---|---|
| This versioned design and approval record | Design/implementation reviewers | R0 | Full document, links, range, content hash and gate evidence; no Go build needed |
| New `pkg/sql/plan/apply_indices_fulltext_boolean.go` and focused `_test.go` | QueryBuilder -> scan/project/filter/join/agg/window | R2; material cardinality R3 dimension | Typed preserving shape, exact dedup, no raw MATCH, no unsafe pruning, correct tags, supported/refusal cases |
| `pkg/sql/plan/apply_indices.go` minimal TABLE_SCAN route + detector guards | Regular/vector paths, postorder recursion, project protection | R2 | Reachable public planner fixtures with ordinary secondary/vector indexes; no early ANN truncation |
| `pkg/sql/plan/apply_indices_fulltext.go` shared narrow seams | Existing stream constructors and served-score consumers | R2 | Extract/reuse construction preserving old inputs; binding-aware served replacement expression; legacy positive/projection controls |
| BVT cases/results in `test/distributed/cases/fulltext` and `fulltext2` | SQL frontend/real reader/operators | R2 | Independent controlled token row/score/truth oracles, repeated prepared execution and snapshots |
| User documentation of supported/refusal domain | SQL users | R0 | Matches tested leaf NULL/error/visibility/ordering behavior |

Shared file edit needs are explicit: (a) reusable stream-construction helper extracted without changing existing legacy options or duplicating inline algorithm dispatch; (b) `fulltextServedMatch` replacement-expression support with current nodeID lookup as legacy default; (c) guard detector call. Coordinate these symbols with issue30164 owner; do not alter default sorting code, unrelated projection pagination, or its performance implementation. New per-algorithm SQL/catalog dispatch is prohibited by G-INDEXPLUGIN; reuse the current dispatch rather than adding an algorithm switch in the new file. No plugin registration/kernel changes are planned; if needed, stop and re-review this scope.

## 7. Verification plan and independent oracles

Existing reusable tests: `apply_indices_fulltext_guard_test.go`, `_score_guard_test.go`, `_agg_test.go`, `_json_test.go`, and `apply_indices_test.go`; public fulltext/fulltext2 parser/index query fixtures. Inspect full fixture before extending. Use table/subtests for pure tree permutations, not repeated embedded clusters. BVT requires only 6–8 controlled-token rows and one table per index kind; issue's 1026-row vector-distance witness can be reduced to a scalar-only closest row plus match-only, overlap and null controls. No sleeps or large fixtures. Build after approval through the supported CGo wrapper/toolchain prepared by the environment owner.

| Invariant/witness | Typed UT distinct claim | Public independent oracle |
|---|---|---|
| MATCH OR scalar, NOT MATCH, nested AND/OR/NOT | LEFT attachments and unchanged bound tree in FILTER | Hand-labeled controlled terms: match-only, scalar-only, both, neither; exact IDs |
| UNKNOWN propagation and indexed NULL content | Typed zero default below user outer join; scalar NULL untouched | Match+NULL tag retained by OR; no-match+NULL excluded; all-NULL body with tag1 passes OR and NOT; NULL sibling with matching text still hits |
| Repeated vs distinct leaves | One stream/slot for equal binding+parts+pattern+mode; two for distinct; aliases not conflated | `(M OR M)=M` row equivalence and nonmatching scalar branch still present; two terms overlap produces one base row |
| Scoring/thresholds including zero/negative and NULL | No range/guard/limit on complete stream; normalized FLOAT32 score | Projection score0 on scalar-only rows; compare known emitted hit score to a positive indexed control, same engine; threshold equality/zero/NULL and SQL NOT |
| ORDER/global LIMIT/OFFSET | Limits only above Boolean FILTER/sort, no candidate limit, no new implicit sort | Closest vector row has scalar-only eligibility; distance then PK explicit ties, LIMIT/OFFSET exact IDs; score order includes zero rows |
| Aggregates/windows and user joins | No raw MATCH or invalid tags; preserving normalized columns below enclosing null extension | COUNT/group count and row_number over explicit PK match independently listed input rows; LEFT join unmatched control not lost |
| Prepared reset/provenance | Pattern/threshold parameter positions intact, no mutable hit cache | Execute same prepared shape with changing term/threshold/tag twice; equivalent fresh execution; classic/FT2 NULL-pattern outcomes match their documented leaf behavior |
| Index/column matching and unsupported inputs | All-or-nothing validation; exact parts and same binding; JSON probe excluded | Matching composite parts executes; no fulltext/nonindexed/wrong subset/cross-table leaf refuses honestly, never scalar-only silent success |
| Snapshot and FULLTEXT2 visibility | Existing ScanSnapshot and source/hidden index metadata copied exactly | Existing supported snapshot fixture reused; assert Boolean membership against SAME visible index generation, not an invented synchronous promise |
| Legacy compatibility | Old collector/INNER/covered choices unchanged absent OR/NOT trigger | Existing bare positive, projection-only, aggregate/window and threshold-guard cases keep prior outcomes |
| EXPLAIN reachability | Stable JOIN/FILTER and references only, no whole text snapshot | Execute + EXPLAIN ANALYZE both succeed; results, not plan text, are semantics oracle |

Unfixed evidence: new typed collector/rewrite tests must fail for missing route/preserving shape; public OR/NOT baseline must fail 20105 on the verified clean base. Historical issue receipt alone is insufficient for a new-base claim. Verify exact named test/case selection nonempty, then focused UT, owning planner tests/incremental gofmt+vet+lint, focused BVT group, and affected reader tests proving exhaustive/no-duplicate contract. Do not run repo-wide SCA or rebuild concurrently with setup owner.

Cost evidence: typed H_i/k invariants plus a small deterministic benchmark comparing one repeated leaf to k distinct leaves and increasing N demonstrates dedup and expected growth; it cannot claim production capacity numbers. A benchmark is required only if implementation adds materially new allocation/scan work beyond the relational bound; ordinary BVT has no wall-clock performance assertions. No CI-green or service-pass claim until terminal exact-revision evidence exists.

Acceptance: independent design PASS; no raw executable MATCH in admitted domain; all new UT/BVT pass with baseline failure evidence; legacy controls unchanged; exhaustive reader contract established; no unapproved production deviations; self-review zero blockers. Scope growth, negative/nullable provider-score incompatibility, inability to preserve scan pagination provenance, or nonunique provider results blocks implementation until an updated design is reviewed.

## 8. Decisions/open items

Decided: preserving rewrite is composition-triggered only; absent indexed relevance0 even for all-NULL indexed content; scalar NULLs use SQL 3VL; pattern errors are engine-specific; full streams/no score or candidate pruning; unique provider contract; normalization below outer joins; exact vector-sort fallback; unsupported cross-binding/above-barrier leaves refuse.

Implementation proof obligations, not deferred semantic decisions: confirm complete unlimited FULLTEXT2 search and one-doc-per-score contract for admitted search modes; prove current scan pagination provenance or refuse affected shape; preserve consumer tags and prepared snapshots. If any is false, do not invent a fallback: notify owner and revise/re-review. No unresolved semantic design decision is intentionally delegated to code.

Independent review decision: PENDING. Production implementation remains blocked until exact-revision PASS.
