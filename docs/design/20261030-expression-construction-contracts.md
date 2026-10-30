# Expression construction contracts — design r1

Issue: **Refs #29249**, one PR; do not close the ongoing issue.
Reviewer: **gpt-6.1-sol / max**. Reviewed source:
`a54850983d9cddaacc97e38a84515ec724c937b7` in
`/private/tmp/mo-29249-contract-cleanup`. Read-only source review; no builds or
tests run by this reviewer. The primary owns implementation and QA.

**Decision: PASS** for the bounded design below. No blocking questions remain.
Version a faithful concise design identifying r1/base before implementation
delivery and link its exact revision in the PR. This is design approval, not
implementation approval or evidence of a user-visible runtime defect.

## Scope and candidate dispositions

The cohesive change makes existing diagnostic ownership explicit at expression
executor construction and persisted unsigned-DECIMAL assignment replay.

1. **DML target ownership: leave intact.** `plan/dml_table_name.go:33,73,82`
   already owns DELETE source identity and alias/qualifier/CTE checks; both
   DELETE planners and frontend use its mapping. Frontend READ ONLY admission
   must precede `TxnHandler.Create`; unqualified multi-source UPDATE ownership
   deliberately defers until the post-Create catalog transaction exists.
   `dml_context.go:373` and `build_constraint_util.go:330` subsequently own
   resolved metadata/type/FK policies of their planner routes. These are
   different phases, not a second UPDATE binding; #31798 removed that binding.
   A universal policy/resolution object would duplicate binding or reorder
   errors. A name struct alone is churn. Preserve temporary/remap/prepared
   identity, fail-closed missing catalog behavior and source/assignment order.
2. **Executor construction: change.** `colexec/evalExpression.go:175,486-555`
   mixes memo accumulation with lexical diagnostic state. Two depth counters
   are observed only as zero/nonzero; owner and temporal state are mutated and
   restored with deferred closures. Sibling correctness depends on balanced
   restoration although the state is lexical. Replace it with value scope.
3. **DECIMAL identity: change.** `build_constraint_util.go:1239` recognizes a
   supposedly typed persisted boundary by display names, while execution uses
   encoded `Func.Obj`. The existing registry is the canonical execution
   identity. Use it in this matcher, retaining supported replay shapes.
4. **DELETE RF proof: leave intact.** Its bounded-source, one-local-nonshuffle,
   exact-key-domain, unchanged-build-consumption and recorded-cost-rejection
   guards are causal correctness conditions. It already reuses equality-domain
   helpers and ordinary RF generation. A generic property engine for this one
   consumer adds propagation obligations without demonstrated recurring need.
   Conservatism and 231 lines alone do not establish overfit or a bug.
5. **Background catalog action: leave intact.** Both role-grant and role-rule
   locks use `execWithProcessHook(ctx,"select 1",hook)`. The native lifecycle
   attaches/validates the transaction before the hook and starts the workspace
   afterward (`mysql_cmd_executor.go:5942`). It owns tenant/process/session
   setup, tracing/stats, transaction finalization and cleanup. No reusable live
   process action API exists: `backSession.GetProc` returns nil; compiler
   context refers to an ExecCtx cleared at Close. Removing placeholder SQL now
   needs a second lifecycle or a substantially larger extraction. Bounded
   administrative overhead is not a measured performance defect.

No incidental cleanup, new framework, duplicate state/classifier or second
execution path belongs in this PR. In particular, do not change the T_any/NULL
rule: a supported non-NULL ANY witness has not been established.

## Executor scope API and transition contract

Public constructors/options and runtime executor APIs remain unchanged. The
private recursive builder receives the existing shared accumulator pointer and
a new **by-value** lexical scope. Keep the accumulator limited to:

```go
type expressionExecutorBuildContext struct {
    memos          map[int32]*memoExpressionState
    states         []*memoExpressionState
    joinActivation *[]ExpressionExecutor // existing borrowed output
}
type temporalDiagnosticMode uint8
const (
    temporalPerRow temporalDiagnosticMode = iota
    temporalOwnedFilter
    temporalDeferredJoin
)
type expressionDiagnosticScope struct {
    foldOwnedLiteralNumericCast bool
    joinOwner                   *DeferredJoinDiagnostic
    conditionalJoin             bool
    temporalMode                temporalDiagnosticMode
}
```

Ordinary roots use zero scope plus their existing numeric-fold option. Owned
filter roots enable numeric folding and temporalOwnedFilter. Join build/probe
roots enable numeric folding, carry the supplied owner and start temporalPerRow.
Preserve probe's owner-nil fallback exactly, including its folding default.
Probe activation output remains shared across roots. List, parameter transport
cast and AuxId-without-memo recursion inherit the incoming scope unchanged.

At each function:

1. Defer only when incoming joinOwner exists, the subtree is statement-constant
   and it contains no row-scoped conversion, exactly as today.
2. Preserve numeric cast, explicit DECIMAL cast, volatility, realtime and
   overload HasRowScopedDiagnostics decisions. A temporal-text call adds row
   scope unless it is the deferred root, inside temporalDeferredJoin, or an
   eligible statement constant in temporalOwnedFilter. The overload's own
   unconditional HasRowScopedDiagnostics still wins.
3. Copy only the scope into childScope. A deferred root clears its child
   joinOwner and sets temporalDeferredJoin, retaining incoming owner locally
   for the wrapper. This prevents nested duplicate wrappers.
4. Otherwise IFF, CASE, COALESCE and **ELT overload 1** mark descendants
   conditionalJoin. Preserve that runtime-lazy overload distinction.
5. A **row-dependent IFF, CASE, COALESCE or any ELT** changes a child scope from
   temporalOwnedFilter to temporalPerRow. Deferred mode stays deferred. The
   broader ELT set is intentional: current temporal and join guards differ.
6. Build arguments with childScope. A deferred wrapper is added to activation
   only when the **incoming** conditionalJoin is false and collection exists.

Delete old lexical policy fields, both counters and their mutation/defer
restoration. Do not copy memo maps/states slices or retain shadow old state.
Small private predicates/transition methods may express these decisions once;
there is no generalized policy engine.

Invariant: policy depends on constructor and ancestor path; siblings cannot
change one another. Shared AuxId memo identities and activation accumulation
remain shared, with roots alone owning executor Free. Negation: sibling order
changes warnings, guarded constants activate eagerly, or memo/free ownership
changes. The enum genuinely reduces state: a row guard permanently masks filter
ownership for its subtree, while deferred ownership takes precedence.

## Owners, failure, cleanup and cost

Constructors are the canonical admission points. Consumers are
`filter/filter.go:49`, `hashbuild/expression_memory.go:32,52,93`,
`hashbuild/hashmap.go:202`, `hashjoin/join.go:146-202` and compiler wiring
`compile/operator.go:1576,2706`. DeferredJoinDiagnostic remains the sole runtime
owner of captured warnings/errors. Do not change its mutex, retention/budget,
activation, Prepare/Reset or SQL-versus-resource error classification.

Construction stays sequential and query-local. Publish executors only after
argument construction succeeds. Preserve Free on wrapper/child failure and
successful-prefix cleanup of root slices. Borrowed activation references are
returned only on success and never freed separately. Keep AuxId first-use memo
behavior, allocation selection and executor Reset/Free unchanged.

HashBuild may precede a nonempty probe. Semantic diagnostics wait until the join
observes both sides; cancellation, deadlines, internal and resource failures
remain immediate. Empty inputs/unselected branches emit no speculative
diagnostics. Reset discards captured state; new execution resets caches.
HashJoin Reset/Free still resets the owner and drops borrowed references. No
Call/Reset terminal signal, sender/receiver or distributed protocol changes.

Scope is a fixed-size construction stack value: no per-row allocations, work,
locks or goroutines. Keep IsStatementConstantInput/ContainsRowScopedConversion
short-circuited where needed; do not eagerly classify every node and add
repeated subtree walks. Temporal guard analysis is unnecessary once mode is
per-row/deferred because it cannot restore filter ownership. Compare the
primary's external construction benchmark in the same mode to detect material
regression; record allocation deltas, without claiming an unsupported speedup.

## Persisted unsigned-DECIMAL boundary identity

The registry owns stable execution IDs. Existing admission/cast writers persist
both encoded ID and display name; keep writers, function IDs/overloads and
encoding unchanged. In matchUnsignedDecimalAssignmentComposition:

* Outer identity is a registered encoded overload of CAST, CAST_ASSIGN,
  CAST_STRICT or CAST_IGNORE.
* Inner identity is a registered encoded overload of
  UNSIGNED_DECIMAL_ADMISSION, UNSIGNED_DECIMAL_ADMISSION_STRICT or
  UNSIGNED_DECIMAL_ADMISSION_IGNORE.
* Check the **full encoded overload** is registered, not only decoded function
  bits. Missing Func/unknown overload is no match. Small local predicates
  suffice; there is no new exported registry layer or spelling fallback.

Retain existing attributed-type and identical target-carrier shape guards,
source expression and label extraction. Accept both existing two-argument
legacy admission (empty label) and current three-argument shape. Recognized
legacy replay is reconstructed to the current three-argument boundary before
execution; do not send it directly to the three-argument executor, invent a new
overload or delete support. Preserve empty-label fallback, strict authoring,
IGNORE changing both layers, ordinary mode-resolved replay, idempotence,
destination-column labels and diagnostic ordering.

Replay consumers: forceCastExpr2WithProcess, getDefaultExprForAssignment
(`build_util.go:2689`), generated assignments (`build_util.go:2538`), INSERT
defaults (`bind_insert.go:2915`), UPDATE ON UPDATE (`bind_update.go:853`).
Keep isUnsignedDecimalAdmissionName: `base_binder.go:7566` guards SQL before
resolution and legitimately folds user spelling. Preserve its mixed-case
public-call rejection. Do not generalize unrelated cast consumers or NULL
handling. No proto/catalog/plan format, protocol version, authoring floor,
migration or peer-version gate changes. Supported stored shapes remain readable
after shutdown upgrade. Direct serialized replay proves this boundary; a new
restart cluster adds no distinct format proof.

## Alternatives and acceptance

Status quo is safe under current writers but requires balanced lexical
restoration. Wrapping the same mutable flags keeps that obligation; copying the
whole context risks losing slice accumulation/duplicating owners. Chosen value
scope plus one accumulator removes two lexical policy states and restoration
without a new runtime owner. Global diagnostic strategies/caches have no need.

For DECIMAL, name fallback creates two authorities and may rewrite the wrong
executor. Rebinding stored expressions by name adds resolution and may alter
semantics. Registry identity is already the executor contract. The relevant
precedent is lexical environment passing and this repository's typed encoded-ID
matching, including DELETE SERIAL/SERIAL_FULL proof. No external SQL standard
change is introduced; existing MatrixOne/MySQL diagnostic and assignment
behavior remains the compatibility baseline.

Acceptance: no old restoration state remains, memo/vector owners are unchanged,
valid expressions retain results/errors/warning multiplicity, registry identity
controls replay, and legacy two-argument replay reconstructs successfully.
Candidate 1/4/5 production code stays outside the diff. No SQL bug/speedup claim.

## Focused validation map

Construction/consumers are R2 with the existing shared join-owner lifecycle
lens. Replay is R2 with persisted-input compatibility proof. Formats,
synchronization and distribution do not change. Primary derives final changed
packages/consumers from the diff and records exact selection, mode and terminal
status. Historical test passes are not current evidence.

| Contract / risk | Cheapest sufficient oracle |
|---|---|
| Sibling scope and execution reuse | Extend TestTemporalOwnedFilterRespectsRowGuard in its existing lightweight fixture. The primary confirmed the clean-base witness: equality between IF([true,false,true],DATE('bad'),NULL) and unconditional DATE('bad'), both sibling orders, all rows NULL; cumulative warning counts 3 on first Eval, 5 on second, 8 after Reset+Eval. This is characterization, not a baseline bug. Assert values and deltas, not scope fields. |
| Join activation versus conditional selection | Existing evalExpressionSubjectReuse_test.go:651 proves probe activation ownership. Extend deferred_join_diagnostic_test.go only for missing unconditional-versus-conditional sibling/nested/reset behavior; use baseline-characterized counts and actual branch/error observations. Preserve ELT's differing transfer rules; add an overload control only if supported typed construction distinguishes it. |
| Numeric/temporal/DECIMAL diagnostic boundaries | Reuse TestCompatWarningRuntimeFolding, TestTemporalConstantFilterOwner, TestTemporalWarningsFollowSelectedRows, TestTemporalMixedRowsWithSelection, TestPreparedCastLifecycle, TestExplicitDecimalCastRowDiagnostics, TestDecimalCastFoldingBoundaries and existing deferred activation/error/reset tests. Do not duplicate their permutations. |
| Shared memo state and failure/free ownership | TestMemoExpressionLifecycle, TestDependentExpressionMemoAndOccurrence, existing subject-reuse lazy folding/activation and partial-build rollback tests. In particular, copying the accumulator must fail a memo oracle. |
| Encoded identity and old replay | Extend TestUnsignedDecimalBoundaryRewriter: valid IDs with changed/empty/mixed-case display names still match; misleading names with another/unregistered ID preserve input pointer. Serialized two-/three-argument inputs replay to registered current entries, preserve source/label and remain idempotent through IGNORE -> ordinary. Keep strict-authored/unattributed/NULL controls. Expected identities must not be generated by the matcher. |
| Real assignment consumer | Existing DDL strict DEFAULT, ON UPDATE, INSERT admission, NULL assignment and public-call rejection tests; include at least one default/generated/ON UPDATE consumer oracle in focused replay tests. Do not stop at helper shape. |
| Real expression consumers | Existing constructor-driven filter/hashbuild/hashjoin tests and focused compiler temporal storage-ownership / prepared reset/rebinding tests. Public APIs remain unchanged; reuse existing fixtures instead of a new service. |
| Shared diagnostics reset race | Run directly affected deferred reset/concurrency test(s) individually under adaptive focused race protocol, then colexec once under race as the changed constructor feeds that owner. The deterministic planner matcher alone does not require plan-wide race stress. |
| Owning packages and SCA | Focus first, then owning pkg/sql/colexec and pkg/sql/plan normal packages once using supported CGo wrapper/artifacts. Run gofmt plus configured incremental vet/lint on final changed packages/direct consumers. No repository-wide scan by default. |
| Public behavior | No new capability or claimed SQL defect: do not add duplicate BVT files. Reuse existing function/temporal_guarded_prepared_join_28851.test for prepared lazy/empty-join controls and dtype/decimal_unsigned_zerofill.test for assignment controls, or semantically equivalent existing public-path evidence. Check ordinary versus coordinator-filter warning multiplicity through existing func_datetime_date.test or minimum-row public SQL smoke on the same owned instance. Review existing case/results; normal comparison and cleanup/repetition follow testing-contract. Standalone suffices because placement/wire is untouched. |

Normal consumer tests may be focused; do not turn this scope into a full
frontend/compile/hashjoin or distributed regression audit. Avoid adding every
hypothesis from the original multi-commit review. Reuse semantically valid
evidence and only broaden after a failure or changed dependency justifies it.
DML/RF/background production code remains unchanged; characterized DML tests
are useful controls, not a reason to rebind sources or rerun their whole suites.

## Delivery decision

Version the approved design; remove superseded lexical fields/transitions;
inspect every final hunk within the two closures. Final independent sol/max
review must verify short-circuit cost, memo accumulation, lazy activation and
legacy replay. Use Refs #29249, never a closing keyword.

Residual risks are eager repeated subtree analysis, copied accumulator slices,
conflating ELT guards and lost old replay; each has an oracle above. There are
no new accumulating resources, security/tenant decisions, rollout switches or
rollback state. Missing/failed required evidence blocks delivery instead of
widening policy or weakening assertions.

Design gate: ownership and persisted-expression boundary refactor; design r1;
blocking findings none; accepted choices are lexical value scope and registry
identity, with candidates 1/4/5 intact. **PASS.** Implementation deviations: none
yet. No builds/tests claimed by this design review.
