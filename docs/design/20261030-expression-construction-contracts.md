# Expression diagnostic construction scope

Refs #29249. Base: `a54850983d9cddaacc97e38a84515ec724c937b7`.
Design approved before implementation by **gpt-6.1-sol / max**, session
`/root/design_29249_contracts_solmax`. The approved lexical-scope contract is
unchanged between proposals r1 and r2. R1 was versioned in design-only commit
`3234c644d379`; r2 withdrew the separate DECIMAL matcher proposal after its
stored-expression assumption was disproved. Implementation review is a separate gate.

## Problem and invariant

Expression construction mixes shared memo/activation accumulation with lexical
policy for constant folding, guarded branches and deferred join diagnostics.
The old builder mutates policy and balances it with deferred restoration; its
two depth counters are only observed as zero/nonzero.

Policy must depend solely on the constructor and ancestor path. A sibling must
not change another sibling's diagnostics. Memo identities and activation
references must still accumulate across children of the same root.

## Ownership and representation

`expressionExecutorBuildContext` retains the existing shared memo map, memo-state
slice and borrowed probe-activation output. Never copy their accumulation into
separate child contexts. Roots own executor cleanup; activation references are
borrowed and must not be freed separately.

A private, by-value `expressionDiagnosticScope` carries the numeric-fold option,
join diagnostic owner, conditional-join flag and temporal mode:

| Constructor | Numeric constant folding | Join owner | Temporal mode |
| --- | --- | --- | --- |
| Ordinary | Existing option; default false | None | Per row |
| Owned coordinator filter | True | None | Owned filter |
| Join build/probe with owner | True | Supplied owner | Per row |
| Join probe without owner | Ordinary fallback | None | Per row |

The temporal modes are per-row, owned-filter and deferred-join. They replace
independent booleans and the temporal depth counter. The conditional-join flag
replaces the other counter. No old policy fields or restoration path remains.

## Child transitions

- A join owner defers a statement-constant subtree only when it contains no
  row-scoped conversion. Preserve the incoming owner for its wrapper; descendants
  receive no join owner and deferred temporal mode, preventing nested wrappers.
- IFF, CASE, COALESCE and ELT overload 1 mark join descendants conditional,
  except when the current root is already deferred. Only wrappers whose incoming
  scope is unconditional are collected for eager probe activation.
- Row-dependent IFF, CASE, COALESCE and **every ELT overload** convert owned-filter
  temporal mode to per-row for their descendants. Deferred mode takes precedence.
  The differing ELT rules preserve existing behavior.
- Siblings receive the same incoming scope. List elements, parameter transport
  casts and the recursion that removes an AuxId marker inherit it unchanged.
- The overload's own row-scoped-diagnostics flag remains authoritative. Preserve
  numeric/explicit-DECIMAL cast, volatility and realtime decisions. Temporal calls
  can fold in owned-filter mode only when statement-constant and free of row-scoped
  conversion. Ordinary temporal calls remain per-row; deferred ones stay captured.

Only analyze a temporal guard while it can change owned-filter mode. Keep
subtree classification short-circuited rather than adding repeated tree walks.
Scope is construction-stack data; it adds no per-row allocation, lock or worker.

## Lifecycle and compatibility

Keep successful-prefix/partial-child construction cleanup and allocation
selection unchanged. `DeferredJoinDiagnostic` remains the runtime owner of
captured warnings/errors, with its existing budget, synchronization and
Prepare/Activate/Reset behavior. Semantic diagnostics can wait for a nonempty
join; cancellation, deadline, internal and resource errors remain immediate.

Public constructors, executor APIs, SQL results, warning multiplicity, prepared
reuse, persisted formats and pipeline placement/termination are unchanged.
No protocol, catalog migration, configuration or rollout gate is introduced.

## Validation and cost

Reuse existing colexec tests for numeric/temporal/DECIMAL folding, lazy branches,
memo occurrence/cleanup and deferred activation/error/reset. Two added behavioral
oracles cover both sibling orders:

- Owned-filter equality between IF([true,false,true], DATE('bad'), NULL) and an
  unconditional DATE('bad'): all results NULL; cumulative warnings 3, 5 and 8
  across Eval, Eval and Reset+Eval. This passes on the baseline.
- Probe activation reports only the unconditional sibling before the residual;
  an unselected conditional branch stays silent, selection reports its constant
  once, and reset starts a new execution. Check allocation cleanup as well.

Run the owning colexec package normally and with race, relevant filter/hashbuild/
hashjoin and compiler consumers, and configured incremental formatting/vet/lint.
Reuse existing public SQL prepared temporal/empty-join controls; do not add a
second service fixture or duplicate BVT files. Compare repeated constructor
benchmarks for ordinary, owned-filter, build and probe modes under the same
conditions. Report allocations and timing without claiming query-wide speedup.

## Alternatives and scope decisions

Keeping mutable flags preserves restoration obligations; wrapping them in a
struct does not remove those obligations. Copying the whole context can lose
memo accumulation. Value scope plus the existing accumulator provides the
required lexical contract without a new runtime owner or policy framework.

The other reviewed candidates remain outside this change: DML admission and
binding own different phases; DELETE RF guards prove bounded local execution;
background catalog hooks reuse the complete transaction lifecycle. Stored
DECIMAL names are authoritative rebinding keys, and generated-column paths do
not universally normalize IDs before matching. An ID-only matcher therefore
lacks a common valid-input precondition. Preserve these contracts rather than
adding parallel resolution/execution or broadening this refactor.
