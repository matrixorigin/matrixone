# Retire unused SQL execution helpers

Version 1, 2026-10-22. Owner: #29249.
Implementation: `cleanup/29249-runtime-20261022`.
Base: `d7f95aa754bfe39126752729d307298710947c17`.

## Problem and boundary

A source-wide AST inventory, including tests and all checked-in build tags,
finds disconnected SQL utility entrypoints. Reference searches include source,
build/registration directives and documentation. Retire complete unreachable
closures, without changing the paths that execute queries:

- `compile/runtime_filter.go`: `ApplyRuntimeFilters`, its evaluator interface,
  structs and methods. Live `Scope.waitForRuntimeFilters` and
  `Scope.handleRuntimeFilters` own runtime-filter delivery and expression setup.
- `compile.AddFkeyToRelation`: unused incremental wrapper. Keep live DDL foreign
  key reconstruction, constraint updates, rollback and self-reference handling.
- Planner utilities: `GetBindings` with recursive `doGetBindings`, `EvalFilterExpr`,
  `BuildVectorsByData` with recursive `exchangeVectors`, `GenUniqueColJoinExpr`,
  `GenUniqueColCheckExpr`, `MakeCPKEYRuntimeFilter`, `MakeSerialRuntimeFilter`,
  `DeepCopyTableDefList`, and `FindColumnByOriginName`.
  Keep the live unique-column collector, DML rewrite, expression evaluation,
  cloning, column resolution and runtime-filter constructors.
- Colexec: `FillPartitionBatchForDelete`, `FillPartitionBatchForInsert`,
  `IsDeletionOnTxnUnCommitPersisted` and `IsDeletionOnTxnUnCommitInMem`.
  Keep partition-pruning operators and the live workspace-segment predicate.
  Update the workspace design reference to the actual segment-identity check.
- Constant folding: remove unused `IsDivisionByZeroConstant`. Keep the live,
  recursive `ContainsSqlModeDependentDivision` and its shared `isZeroLiteral`.

Consolidate `rule.IsConstant` into its existing owner `function.IsConstant`.
The former is an exact forwarding wrapper. Migrate all source callers and its
one local caller with unchanged arguments; keep the canonical implementation.
No new abstraction, compatibility shim or state is needed.

## Invariants and exclusions

Selected standalone functions have no Go callers, function-value registrations,
linkname uses or test consumers. Only the retired runtime-filter types implement
an interface, and that interface is private to the disconnected closure. These
are internal kernel packages; unknown out-of-tree Go consumers are outside the
source-compatibility proof. No SQL, wire, catalog, persistent format or exported
service interface changes. Shared expression/filter/partition resource owners,
error checks, cancellation, cleanup and reset paths remain authoritative.

Do not retire diagnostic tools, parser constructors, integration entrypoints,
GPU surfaces, or implementations selected through interfaces/build tags merely
because a name count suggests no caller. Prior open cleanup PRs are excluded.
No test fixtures or regression cases need deletion or addition for these unused
entrypoints. Preserve all active correctness and unhappy-path oracles.

## Validation and cost

The total production retirement may exceed 500 lines, triggering design review.
Run focused existing constant-folding, runtime-filter, DML/foreign-key and
partition tests, then complete normal/race suites for the four owning packages
and live partition insert/multi-update consumers. Run incremental configured
vet/lint, gofmt/diff checks and a default CPU main build using verified CGo
artifacts. GPU dispatch is untouched. No public behavior changes require new BVT.

No new hot-path operations, allocations, waits or resources are introduced.
An exact forwarding-wrapper migration has no semantic or expected runtime cost;
do not claim throughput, memory or build-time improvements without measurement.
Keep inventories, model reviews and build artifacts outside tracked source.
