# Session-controlled CN validation for ordinary LOAD

Related issue: [#30613](https://github.com/matrixorigin/matrixone/issues/30613). Implementation: [#30697](https://github.com/matrixorigin/matrixone/pull/30697).

Status: APPROVED design; implementation follows this contract. Design review by delegated gpt-6-astra, medium reasoning, task `/root/cn_switch_design`, against clean c38e3715539749aee13f3175d4900593b53f6bb8. It complements the existing sampling fix.

## Contract and evidence

#30421 enabled ordinary LOAD CN primary-key validation independently of CNPrimaryCheck, changing the historical fast-load default. The existing TN switch `mo_pk_check_by_dn` does not control that CN plan. The recorded old-baseline runtime experiment proves that TN ON did not reject duplicate input on the examined baseline; it must not be presented as equivalent to CN validation. The remaining indexed-load cost also includes validation topology and writer parallelism, so removal of hash work is not an equal-contract performance optimization.

Add dynamic SESSION BOOL `mo_pk_check_by_cn`, default OFF, with SET_VAR hints disabled. It controls only the ordinary LOAD primary-key validation introduced by #30421. OFF preserves the historical CN bypass; ON restores the current eligible validation behavior. INSERT semantics, TN settings, foreign-key enforcement, index maintenance, assignment/decoder policies, sampling, and scheduler heuristics remain independently governed by their existing code.

The name is broader than this release's scope: documentation must explicitly say it controls ordinary LOAD, including LOCAL, and does not disable INSERT checks. OFF is not a guarantee that every duplicate succeeds: TN and other constraints can still reject a statement. ON is not a promise to validate every unique index; it preserves the existing primary-key eligibility and exemptions.

Invariant: every newly built ordinary LOAD plan uses its owning session's current switch. OFF produces no LOAD primary-key validation branch; ON preserves eligible input/target duplicate errors and statement rollback. A session SET cannot influence another session. Cached plans must be invalidated after SET; existing prepared handles remain usable. LOAD itself is not currently admitted by PREPARE.

The smallest negations are a default LOAD containing reachable PK dedup, an ON duplicate LOAD succeeding, the next EXECUTE retaining its pre-SET behavior, or another session inheriting the setting.

## Minimal implementation

1. Register the variable adjacent to `mo_pk_check_by_dn` in `pkg/frontend/variables.go`: ScopeSession, Dynamic true, SetVarHintApplies false, BOOL type, Default int8(0). Do not add runtime/global mirrors or callbacks that mutate process-wide configuration.
2. Keep `loadChecksPrimaryKey(stmt, tableDef)` as the existing pure eligibility predicate. Add one small shared context-aware helper that combines eligibility with `CompilerContext.ResolveVariable("mo_pk_check_by_cn", true, false)` and returns `(bool, error)`. Call it from both `bindLoad` and legacy `buildLoad` where `checkLoadPrimaryKey` is assigned. Reset the modern statement-local flag before binding as today. Propagate real resolver errors. Accept canonical int8 and existing internal int64 BOOL representations; nil from an internal resolver means default OFF. Reject unsupported/non-BOOL representations rather than accidentally enabling validation. Ineligible statements can short-circuit without resolving the variable.
3. The legacy fallback must use the same helper. Existing `ifNeedCheckPkDup := !LoadTag || checkLoadPrimaryKey` then naturally follows the session policy without touching INSERT. Preserve modern `canSkipDedup` exemptions (restore, optimizer skip hint, internal index) and existing IGNORE/REPLACE/fake-PK exclusions. Do not broaden exemption policy on the legacy path as part of this change.
4. Add `mo_pk_check_by_cn` to the existing `planDependencyVariable` predicate in `Session.setSessionSysVar` (`pkg/frontend/types.go`). That existing branch compares converted old/new values and calls `invalidateCachedPlans(true)` only on change. It clears COM_QUERY plans and marks prepared statements `needsRebuild`, preserving handles and buffers. `computation_wrapper.go` already consumes `needsRebuild`, rebuilds the plan, and replaces the cached Compile through the existing lifecycle. Reuse this owner and protocol; no new cache key/state or unconditional LOAD cache ban is needed. Invalid SET leaves caches and effective value unchanged; equivalent ON/1 preserves a warm cache.
5. Add the default OFF case to `MockCompilerContext.ResolveVariable`; do not globally set mocks ON. Existing tests whose oracle is specifically ON validation should explicitly override this variable, while delegating other variable names to the normal resolver. Attached-session compiler contexts must resolve through their session; standalone contexts returning nil correctly default OFF.

Cache invalidation is session-local and occurs on infrequent SET, so invalidating all session plans on a switch change is an acceptable existing mechanism. No executor ownership, goroutine, cancellation, wire format, catalog schema, or persistent resource changes are needed. ON retains existing validation/writer topology and its terminal paths. OFF removes the validation resource demand; it makes no throughput ratio guarantee.

## Risk-to-validation map

| Risk / contract | Smallest useful evidence |
|---|---|
| Variable definition and isolation | Frontend UT: default OFF, ON/OFF and 1/0 conversion, invalid value and SET GLOBAL rejection, second session remains OFF. |
| Modern vs legacy divergence | Extend planner LOAD PK suite: default/OFF zero reachable DEDUP; ON expected FAIL DEDUP with input-uniqueness not assumed; direct legacy buildLoad OFF zero FUZZY_FILTER, ON existing filter regardless CNPrimaryCheck. Preserve scalar/composite PK cases. |
| Resolver errors or hidden global dependency | Helper tests for canonical int8/int64, nil OFF, resolver error and invalid representation; compiler context test with attached session proves delegation. Do not swallow arbitrary errors to accommodate stale mocks. |
| Exemption / neighboring semantics | Existing IGNORE/REPLACE/fake-PK/restore/skip-hint/internal-index tests run with ON where they test exemptions; INSERT plan/error controls under OFF and ON remain identical. FK self-reference fallback exercises actual fallback with both values and unchanged FK enforcement. |
| Stale COM_QUERY or prepared Compile | Extend existing session invalidation tests: changed normalized value clears cache and marks prepared needsRebuild; same value does neither; failed SET does neither. Public ordinary LOAD repeats identical SQL across OFF/ON/OFF; a prepared INSERT remains usable and rejects duplicate keys across those SETs. PREPARE LOAD is not admitted by the current grammar. |
| Public errors, rollback, recovery | Reuse issue30411 fixture and pinned connection: default OFF valid LOAD plus typed-plan no-dedup oracle; ON input duplicate and existing-target conflict give 1062, failed statement adds no rows, following valid INSERT/LOAD succeeds. Add second pinned connection for isolation. Use 2–3 rows and fresh target state between duplicate cases. Test OFF public bypass only with explicitly controlled TN behavior; do not advertise it as unconditional duplicate acceptance. |
| Existing tests silently changing meaning | Inventory affected planner, compiler, issue/integration, and distributed BVT tests that expect CN validation or its plan shape; opt them in locally and reset session value at end. Do not weaken their error/rollback assertions or set the suite-wide default ON. Keep existing ON orientation/locality/compiler-promotion tests. |

Prefer extending current fixtures over adding services. Full repository tests, new stress loops, race runs, or another large-file benchmark are unnecessary for the session/planning contract. Run focused owning-package tests and incremental formatting/vet/lint after edits; retain terminal status and nonempty selection. Public integration/BVT remains necessary because the new setting changes SQL semantics. Historical benchmarks can remain context but do not prove new-head throughput.

## Acceptance and delivery

Accept when both planner entry points honor the same session policy, current SET values invalidate cached plans correctly and preserve existing prepared handles, OFF avoids reachable CN validation, ON errors/rollback and exemptions remain, sessions are isolated, and INSERT remains unchanged. Review the whole final diff with the mandated overall review and reconcile sampling-fix evidence without rerunning unchanged checks solely for another SHA.

A separate PR would make the default-policy change independently reviewable and revertible; however, the owner explicitly plans to update existing PR30697 for the same issue's LOAD regression. That is coherent if its title/body clearly present both the sampling correction and the deliberate default-OFF policy, retain the sampling fix, and do not imply that disabling validation solved equal-contract performance. No rebase or duplicate sampling patch is required by this design.

## SQL usage

The setting is session-only and defaults to OFF on a new connection:

```sql
SELECT @@session.mo_pk_check_by_cn;
SET SESSION mo_pk_check_by_cn = ON;
-- Subsequent ordinary LOAD statements validate primary keys on CN.
SET SESSION mo_pk_check_by_cn = OFF;
-- Subsequent ordinary LOAD statements bypass that CN validation.
```

Use the same connection for SET and LOAD. OFF can accept duplicate primary keys
within the incoming data; it does not deduplicate, replace, or ignore rows.
The independent TN setting can still reject conflicts. ON preserves ordinary
LOAD primary-key checks, including duplicates within input and against the
existing table, with error 1062 and statement rollback. INSERT checks are
unaffected. Existing LOAD IGNORE/REPLACE policies and secondary UNIQUE handling
are outside this setting's scope. This is an intentional default-policy change
from #30421, not a claim of faster execution under equivalent validation.

## Validated design adjustment

The current parser and PREPARE statement admission do not support LOAD (the
binary prepare probe returns 1064 before planning). This change does not expand
PREPARE syntax. Public coverage therefore repeats identical ordinary LOAD text
across OFF/ON/OFF and executes a prepared INSERT across those same transitions;
the existing frontend cache test proves query-cache invalidation and prepared
handle rebuild flags. A prepared-LOAD experiment is unreachable on this base. No new cache mechanism
is needed.
