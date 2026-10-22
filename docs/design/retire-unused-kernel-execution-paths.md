# Retire unused kernel execution paths

Version 3, 2026-10-22. Owner: #29249.
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

## Storage retirement extension

V1 remains the approved SQL boundary. Extend the same unreachable-path rule to
these storage closures, preserving active recovery and I/O compatibility:

- Remove `db/scanner.go` and `scannerop.go`: the scanner constructor, private
  implementation, scanner/error interfaces and callback dispatch have no live
  constructor, registration or type consumer. Remove its commented cron-job
  remnants; keep current controller, cron jobs and generic catalog processors.
- Remove `BlockDataReadNoCopy`; keep `readBlockData`, `BlockDataRead`, row-ID,
  tombstone, selection and release owners used by live reads.
- Remove `BlockReader.LoadOneSubColumns` with its sole underlying
  `objectReaderV1.ReadOneSubBlock`, and unused `ReadMultiSubBlocks`.
  Preserve live `LoadSubColumns`/`ReadSubBlock`, `ReadBlockOptions` and formats.
- Remove the disconnected `CreateBlockOpt` builder and all four methods.
  Keep the live object creation options and writers.
- Remove unused `ReplayCheckpointEntries`. Retire the complete old catalog
  replay closure: `OnReplayObjectBatch`, `onReplayCheckpointObject`, and
  `replayCheckpointDeleteObject`. Their only non-closure consumer is the
  existing soft-delete regression test. Migrate that test to the live
  `OnReplayObjectBatch_V2`, keeping all version-link, object-data sharing,
  visibility and idempotence assertions. Update its stale comments/name and
  the live replay comment; do not keep an obsolete implementation solely to
  test itself. Keep all current checkpoint loading, legacy-version decoders,
  active replay callbacks and shared `ObjectListReplayer`.
- Retire the disabled special-table size classification: `GatherSpecialTableSize`,
  private `checkSpecial`, both regexes, `cluster`, `triode` constants, and
  `UsageData.special` including its propagation/zero-initialization writes.
  Its only nonzero writer is the unreachable classification method. Preserve
  B-tree ordering, sentinel equality, counters, snapshot sizes and explicit
  persisted columns. Audit whole-value comparisons, unsafe/serialization use
  and consumers before field removal; memory accounting must keep deriving
  actual struct size rather than a stale literal.
- In `HandleStorageUsage`, remove the disabled call/comment, constant-zero
  `specialSize`, and account traversal that only adds zero. Preserve response
  contents, account ordering semantics, cache update, locking and timing metrics.

No test cases or assertions are deleted; reuse the current catalog fixture.
Mutation of the active first-seen delete-counterpart creation must fail the
migrated test. Keep the mutation outside source through a temporary overlay.
Reuse full normal/race suites for
objectio, objectio/ioutil, blockio, catalog, checkpoint, logtail, db and RPC,
plus relevant existing storage-usage, replay/cancellation and read-release cases.
Validate a real storage debug consumer if the RPC closure requires it. Reconcile
previous SQL validation using semantic dependency changes; rerun invalidated
checks, not every prior check mechanically. Main build and incremental configured
SCA must cover the complete final range.

## Storage compatibility gate

Preserve persisted encoding, schema/version constants, column order, metadata
and WAL/checkpoint codecs. `UsageData.special` is not encoded; remove it only
under the proved constant-zero invariant, including the sentinel comparison.
No legacy decoder or active backup/restore fallback is retired. Shared backup
fault injection remains because the active logtail backup path still uses it.

Reuse the existing compatibility case with only its data-root path relocated
outside source: baseline-written data must reopen and scan on the candidate,
and candidate-written data on the baseline. Also run existing backup/restore
and fresh-WAL cases and current legacy/replay tests. These tests protect this
refactor against its main baseline; do not claim exhaustive compatibility with
all historical releases. Any candidate lacking reachability or format proof
must be retained rather than removed to meet a line-count target.
