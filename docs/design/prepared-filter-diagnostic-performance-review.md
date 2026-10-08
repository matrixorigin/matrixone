# PR #29462 deep review record

Updated: 2026-09-29. The original published head reviewed was `f5b8e9f33588f7e0c7945f305c7d2de2f901a1ae`; this record includes the subsequent source-binding rewrite. Merge base: `a543410f766d2f41237519ed7cc1376c31e03d99`. Current design: [prepared-filter-diagnostic-performance.md](prepared-filter-diagnostic-performance.md).

The user authorized staged GPT-6 Astra / xhigh review. That reviewer approved source binding before optimization and a single runtime cache. The full implementation review found two concrete consumer bugs; both were corrected. Repeated two-CN BVT and performance acceptance passed on the previously reviewed head. Subsequent public-regression review found additional failures at `74d6811d3ddae92e1841efab3892bf841588e16f`; the earlier PASS does not cover the repair described below. New-head CI and a future merge result remain separate from local acceptance.

## Public-regression repair after the prior PASS

Unchanged public prepared-statement tests failed at `74d6811` and passed at merge base `a543410`. Failures included string-source numeric arithmetic, NULL/ANY relational materialization, YEAR→BIT stored-procedure assignment, and prepared-result presentation. The repair keeps NULL markers executable while giving physical result columns concrete types; preserves SQL EXECUTE's visible TEXT spelling without changing inner numeric consumers; applies source-specific casts only at the relevant consumer; and adds YEAR→BIT to the common cast path. It does not add a second execution path or cache. An independent GPT-6 Astra/high read-only review then found two further counterexamples: exact unsigned comparison of integral decimal/scientific string spellings and `CHAR` rounding of explicitly DOUBLE sources. Both received public-protocol regression tests and narrow fixes. The former casts the executable marker through exact DECIMAL before unsigned comparison; the latter restricts lexical inference to string sources. Rebinding from an inexact value/error back to an exact value is tested to challenge stale cache/proof state.

## Change map

| Closure | Contract / owner | Validation |
| --- | --- | --- |
| Source binding and keys | Original AST ordinals and actual source domains precede comparison/index lowering; parameters remain references | Typed planner, current-value executor, SQL and binary composite-key controls |
| Diagnostic proof and optimizer | Each builder captures immutable candidates before rewrites; fresh proof on every cache hit; no child or previous execution can authorize another | Safe/unsafe/safe, guarded diagnostics, unrelated literal and row warnings |
| Frontend cache and retry | One cache entry; compile success gates publication; retry rebinds and clears stale proof | Frontend full UT, failure/replacement and retry controls |
| Reader/block/remote | Reusable source immutable; current raw selection renewed and folded at destination | Direct compiler safe/unsafe/safe yields 1/0/1 blocks and matching remote lists |
| String and scalar consumers | Source metadata, numeric domain and width preserved before consumer selection | Typed and real protocol TIME, JSON, DECIMAL, CHAR, REGEXP and binary state controls |
| JSON source protocol | Native JSON serialization and binary JSON_DEPTH inputs require v101 through placement, send, receive and persisted expression floor | Protocol boundary tests plus ordinary JSON column execution |
| EXPLAIN | Common reusable optimizer and original parameter ordinals | `TestIssue26859BinaryPreparedExplain` passes through the current route |
| Cleanup and engineering | Remove dual templates, late QUERY Fill and unused traits/proof owners | Deleted-test oracles mapped to current typed/retry/public execution tests |

## Ownership and unhappy paths

- A session's PrepareStmt owns one bounded runtime cache entry. Existing session serialization remains the synchronization boundary; no new workers, locks or waits.
- Q1: probes release temporary vectors/results. Failed builds restore compiler context and database. Failed compile never publishes; displaced compiles are released after current statement cleanup, preserving shared Process state.
- Q2: SQL diagnostics retain their original owner; cancellation/internal/resource failures propagate. Runtime source selection references immutable plan expressions and resets each execution.
- Q3: proof and block lists are renewed; schema/generation invalidation clears the old cache. Values and folded results are not cached. Value-dependent configuration stays out of the type-only cache.

## Review disposition

[aptend review 5347189476](https://github.com/matrixorigin/matrixone/pull/29462#pullrequestreview-5347189476) is accepted. The published golden's false-positive compound-key rows and internal-key warnings are invalid. Current SQL and binary tests require the scalar comparison result, with `k2 + 0 = ?` as an independent control: invalid text returns no rows/one conversion warning unless an actual zero key matches; NULL returns no rows/no warnings; valid rebinding recovers.

Astra's complete implementation review confirmed the single QUERY normal/retry route, fresh retry proof, original ordinals, context restoration, compile publication and JSON protocol closure. It found:

1. Physical string CASTs discarded runtime domain metadata. Lowering now leaves all MySQL string parameters intact; the direct/derived multibyte test uses the final planning API and repeated values.
2. Opaque state functions rejected TEXT/BLOB states. Their common checker now converts MySQL strings to unbounded VARBINARY, keeping the established aggregate ABI and removing the PREPARE-only cast.

Cleanup findings were adopted: removed `PreparedPlanDiagnosticNeedsTemplate`, context proof fallback and optional post-optimization candidate reconstruction. Historical dual-template design text was replaced. Removed synthetic template tests are covered by the real safe/unsafe/safe retry, typed ordinal and cache publication tests.

## Evidence and remaining gates

- For the post-PASS repair, seven affected public prepared-statement integration tests passed together (15.640 seconds), including the unchanged tests that failed at `74d6811`, the prior composite-key regression, and new exact-integer, NULL, `CHAR`, and SQL UNION controls. Full plan/function/frontend/compile unit packages passed (5.799/15.512/26.191/6.610 seconds). Exact comparison's same-type inexact/error→exact rebinding passed for SQL PREPARE and COM_STMT separately. `git diff --check` passed. These results are local macOS/CGo evidence; the previously recorded two-CN BVT and NVMe throughput measurements have not been rerun on this repair.
- Complete frontend/plan/function/compile UT passed: 21.889/7.131/14.682/4.125 seconds.
- After QUERY retry deletion and common planning accounting extraction, real two-CN `TestIssue29463PreparedCompositeKeyDomains` passed in 13.213 seconds. Covers both protocols, warnings, keys/no keys/secondary indexes, fractions, NULL, invalid text, rebinding, UPDATE, adjacent BIGINT literals, ordinary UINT64/BIT multiplication and JSON CONCAT.
- Subsequent frontend/pb-plan/colexec/parser-tree full UT passed: 29.981/0.043/1.865/0.027 seconds.
- After source NULL ownership and temporal floating conversion fixes, full frontend/plan/function/compile UT passed: 23.411/7.795/18.770/4.244 seconds. Real protocol TIME, specialized domains, JSON_DEPTH, binary EXPLAIN and composite-key tests passed in embed/issues: 26.357/37.285 seconds.
- The final temporal string/NULL extension passed full plan/function UT (7.099/17.709 seconds), the frontend legacy TEXT NULL migration test (0.075 seconds), and the complete TIME protocol test (14.392 seconds). This supersedes two obsolete planner assertions encountered while adding the extension; DATE arithmetic is supported and explicit CAST1 preserves the cast identity without the separate syntax flag.
- Incremental vet and lint across the ten changed/consumer packages passed (lint: zero issues); the final frontend/embed/plan/function affected-package check also passed with zero issues. Focused race passed for cache replacement, retry, AP reuse and remote filter ownership (frontend/compile: 2.433/2.432 seconds). On a fresh two-CN cluster, the issue BVT passed 61/61 and vector prepared-range BVT passed 64/64 twice on each CN (500/500 total). Initial SQL-only readiness was insufficient for the first ordinary IVF query; the rerun `SHOW BACKEND SERVERS` actively refreshed the admission-aware inventory and confirmed both CNs as Working. The original startup failure is tracked separately as [#29476](https://github.com/matrixorigin/matrixone/issues/29476); two direct-parent startup probes did not reproduce it, so historical reproduction is unconfirmed. The startup/discovery owner is unchanged by this PR. Three formal parent-paired runs also passed: parent 5831.75/5611.58/5563.75 versus current 5900.37/5785.63/5761.17 tpmC, six zero-error runs, mean ratio 102.59%. Merged CPU/transaction fell approximately 9.00%; parameter initialization and compile cumulative CPU decreased. See the design document for the complete table, sampling limits and binary identities. Earlier published-head CI/race/performance results do not establish acceptance for the current cutover.
- Performance method: NVMe, fresh version-compatible snapshot per run, sync and own-file cache eviction, fixed benchmark seed, 10 warehouses/10 clients/2 minutes. Temporal string/NULL closure includes a legacy TEXT NULL migration snapshot and typed CHAR/BINARY NULL across multiply, divide and DIV. Current performance reached direct parent `1575e70c3db676d3ed00abc080aadc029b0e1435`. The earlier 100.44% result applies only to the superseded binary.

## Inherited bugs and scope

Clean baseline confirmed [#29463](https://github.com/matrixorigin/matrixone/issues/29463) (compound-key domain), [#29464](https://github.com/matrixorigin/matrixone/issues/29464) (fractional protocol comparison), [#29465](https://github.com/matrixorigin/matrixone/issues/29465) (REGEXP metadata), [#29469](https://github.com/matrixorigin/matrixone/issues/29469) (unsigned multiplication), [#29473](https://github.com/matrixorigin/matrixone/issues/29473) (ordinary temporal/FLOAT arithmetic), and [#29470](https://github.com/matrixorigin/matrixone/issues/29470) (JSON fractional arithmetic). JSON CONCAT already has [#28907](https://github.com/matrixorigin/matrixone/issues/28907), so no duplicate was created. [#29471](https://github.com/matrixorigin/matrixone/issues/29471) tracks the separate existing global JSON CAST/assignment contract; this PR preserves that behavior and does not close it.

[#29476](https://github.com/matrixorigin/matrixone/issues/29476) records the observed first-query IVF placement failure during clean startup. It is separate from parameter binding, remains open, and is not included in this PR's closing references.

Production diff versus the merge base is +1,597/-1,284 (net +313). Compared with the original published PR head, the rewrite is +1,330/-1,661 (net -331). Tests and documentation are counted separately.

Formal performance command: fixed `run-aligned.sh` with sequences 2, 3 and 4; each restores its initial snapshot, starts a new service, waits for all benchmark terminals to start, captures 120 seconds of CPU and runs `runMins=2`. Sequence 1 is excluded as pilot evidence. Raw terminal log: `/tmp/29462-binding-perf-final.log`; workload/profile manifests and summary: `/mnt/nvme/issue-29429-perf/source-binding-check`. These local paths identify retained evidence; the tables above provide the portable result summary.
