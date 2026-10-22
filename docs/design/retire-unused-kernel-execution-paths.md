# Retire unused kernel execution paths

Version 6, 2026-10-22. Owner: #29249.
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
  constructor, registration or type consumer. Keep controller and cron-job files
  unchanged, including reserved integration comments and generic catalog processors.
- Remove `BlockDataReadNoCopy`; keep `readBlockData`, `BlockDataRead`, row-ID,
  tombstone, selection and release owners used by live reads.
- Remove `BlockReader.LoadOneSubColumns` with its sole underlying
  `objectReaderV1.ReadOneSubBlock`, and unused `ReadMultiSubBlocks`.
  Preserve live `LoadSubColumns`/`ReadSubBlock`, `ReadBlockOptions` and formats.
- Remove the disconnected `CreateBlockOpt` builder and all four methods.
  Keep the live object creation options and writers.
- Retire the disabled special-table size classification: `GatherSpecialTableSize`,
  private `checkSpecial`, both regexes, `cluster`, `triode` constants, and
  `UsageData.special` including its propagation/zero-initialization writes.
  Its only nonzero writer is the unreachable classification method. Preserve
  B-tree ordering, sentinel equality, counters, snapshot sizes and explicit
  persisted columns. Audit whole-value comparisons, unsafe/serialization use
  and consumers before field removal; memory accounting must keep deriving
  actual struct size rather than a stale literal.
- Remove the classifier-only `TNUsageMemo.C` catalog back-pointer and unused
  constructor argument, including both initialization writes and the sole
  caller's `nil`. Preserve both cache allocations, authoritative `db.Catalog`,
  catalog-held memo access, controller/rollback order and reserved role wiring.
  The pointer has no reader or serialization/layout contract after classifier
  retirement; memory accounting continues to use actual `unsafe.Sizeof`.
- In `HandleStorageUsage`, remove the disabled call/comment, constant-zero
  `specialSize`, and account traversal that only adds zero. Preserve response
  contents, account ordering semantics, cache update, locking and timing metrics.

Catalog/checkpoint replay retirement is explicitly excluded by the owner. Keep
`OnReplayObjectBatch`, its complete helper closure and tests, and
`ReplayCheckpointEntries`. Preserve read-instance and write-instance paths,
including readonly mode, continuous WAL replay, promotion/demotion, rollback,
and reserved future integration points. Current lack of wiring does not prove
these are obsolete. Do not delete them to reach a line-count target.

No test cases or assertions are deleted or migrated.
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

## CDC/frontend extension

Remove the disconnected `CDCDao.DeleteTaskAndWatermark` and its sole result type
`DeleteCDCArtifactsResult`. Remove unused `cdcSQLBuilder` methods
`UpdateTaskStateByTaskIdSQL`, `InsertWatermarkWithValuesSQL`,
`GetTableWatermarkSQL`, `UpdateWatermarkErrMsgSQL`, `DeleteWatermarkByTableSQL`,
`UpdateWatermarkSQL`, and `OnDuplicateUpdateWatermarkSQL`, plus unused
`Executor.GetMaxAllowedPacket` and `Executor.ExecSimpleSQL`.

All ten methods have definition-only repository references, with no interface,
function-value, test, tagged, registration or documentation consumer. Keep every
SQL template string, numeric index, registry entry and attribute unchanged.
Keep the engine-tested `OnDuplicateUpdateWatermarkErrMsgSQL`, live task and
snapshot-epoch deletion, transaction lifecycle, guarded/monotonic watermark
updates, ownership fencing and error paths. Reserved engine roles remain out
of scope. Unknown external Go importers are outside this compatibility proof.

No tests or fixtures change. Validate full normal/race suites for CDC and
frontend once on the combined storage/CDC candidate, plus existing engine
`TestGetErrorMsg` normal/race as the real error/watermark consumer. Final
reference audit, incremental vet/lint and pinned non-JSON molint baseline
comparison, CPU main build and overall review cover the complete delivery.
Reuse unaffected V1 evidence. No new hot-path work or resource owner is added;
no throughput improvement is claimed.
