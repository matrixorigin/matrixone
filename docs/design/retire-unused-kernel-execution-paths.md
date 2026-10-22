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

## Approval provenance

Owning issue: #29249. Implementation: [PR #30866](https://github.com/matrixorigin/matrixone/pull/30866).
This section publishes the original design and implementation review decisions;
it changes no design scope and does not create a retroactive approval. The
original V6 content above is retained byte-for-byte. Its SHA-256 is
`102e7221858119a2f77de558cf5d96259e2ff87cd2554477814c46c97bec58f6`
at [the pre-implementation V6 revision](https://github.com/matrixorigin/matrixone/blob/ab0b3db39d1f64239e7da5198c3917039c16806e/docs/design/retire-unused-kernel-execution-paths.md).
That hash identifies the historical design, not this appended document.

Reviewer for every original decision below: **gpt-6.1-sol / xhigh**, session
`01a1637c-da51-7972-94c9-1d901f359876`; actual model/effort were checked
against the session metadata. UTC timestamps and excerpts come from its original
responses. Source filenames and SHA-256 receipts identify the retained local
records; the substantive decisions are reproduced here for public review.

| Phase | Original UTC decision | Approved boundary and ordering |
| --- | --- | --- |
| V1 | 2026-10-22T12:09:55.148Z: PASS — V1 is approved for implementation within the scope below. No design blockers remain. | SQL retirement and exact constancy delegate consolidation. [V1](https://github.com/matrixorigin/matrixone/blob/d02bae7cae7541cb17411012b41c3b50d4b11280/docs/design/retire-unused-sql-execution-helpers.md) precedes implementation `f0cd1f8b52`. |
| V2 | 2026-10-22T12:33:11.527Z: PASS — V2’s bounded storage extension is approved before implementation. No design blockers remain. | Storage retirement, constant-zero usage classification and RPC simplification; explicit persistence, sentinel and memory-accounting invariants. Reviewed snapshot SHA-256 `a26b734a412018a50dc6e05d2133a5b292ff2164c682e5809d07e5a96143c9a4`. |
| V3 | 2026-10-22T12:39:02.197Z: PASS — V3’s design delta is approved. No blockers or mandatory design edits remain. | [V3](https://github.com/matrixorigin/matrixone/blob/8c1b36adf921fd258fc0d51f6592149173270e3c/docs/design/retire-unused-kernel-execution-paths.md) precedes storage implementation `bccb754c83`; baseline↔candidate reopen/scan, backup and real object-data replay validation required. Earlier replay-removal scope is superseded by the V4 narrowing below. |
| CDC → V5 | 2026-10-22T12:50:29.179Z: PASS — approve this bounded CDC/frontend addendum to V4. Record the scope below in the canonical design before implementation. | Bounded CDC/frontend method and result-type retirement; preserve SQL templates, indices and live transaction/watermark/error paths. [V5](https://github.com/matrixorigin/matrixone/blob/de7ac5c308dba2b5852c82e7cadb1b2a058c3399/docs/design/retire-unused-kernel-execution-paths.md) records the addendum before implementation `e263be6b8f`. |
| V5 implementation | 2026-10-22T13:02:11.443Z: PASS — final overall review complete; no unresolved material blockers. | Combined SQL/storage/CDC implementation accepted at `e263be6b8f`; design SHA-256 `77344daba426e9ca6670248f195af3b455da8ca45931e648c8dd411fced4a383`. |
| Memo → V6 | 2026-10-22T13:06:36.711Z: PASS — this derivative cleanup is approved against `e263be6b8f`, with no material blockers. Record and commit canonical V6 before implementation. | Remove classifier-only `TNUsageMemo.C`, unused constructor argument and two initialization writes. Preserve both caches, catalog ownership/access and controller/rollback/role wiring. [V6](https://github.com/matrixorigin/matrixone/blob/ab0b3db39d1f64239e7da5198c3917039c16806e/docs/design/retire-unused-kernel-execution-paths.md) precedes implementation `f94980da1c`. |
| V6 implementation | 2026-10-22T13:11:01.235Z: PASS — combined final head has no unresolved material blockers. | Combined final implementation accepted at `f94980da1c`, against the V6 hash above; refreshed memo-owner/consumer checks and unaffected compatibility evidence reconciled. |

V4 is an owner-directed scope reduction, not approval to remove reserved paths:
catalog/checkpoint replay, its tests, controller/cronjobs and read/write-instance
wiring remain intact. It supersedes the replay and integration-comment removals
previously allowed in V2/V3. The combined V5 and final V6 implementation reviews
accepted this narrowed scope. CDC approval reviewed V4 SHA-256
`8ba8076d494bf747a7e767ec1a93b195a3cf73cf5ca0b9563a1ec04c9d9d45c6`
and required recording the exact addendum before implementation. Memo approval
likewise required recording V6 before its implementation.

Original design hashes: V1
`da1547c822198d0ca8ca27b32f36f391521d330897c75c0f65aff41d1b683c64`;
V3 `39188f52670bb92a757ee30c9efb5297b16e07b41bf3ea6e0eeb2de88a433bb8`.
The immutable V1, V3, V5 and V6 links above identify those exact design bytes.

| Original review record | SHA-256 receipt |
| --- | --- |
| `design.md` | `92b434bc8d46367f0ead1c54a556d5dc9f6a6835f5e45821912c07605de5c238` |
| `storage-design.md` | `0ac9381354c121e78dcfc6f5cfcb3d17b096fbd43db84de3f99afc6234452e1d` |
| `storage-design-v3.md` | `70cb35ae35525877507174a1cca89e5cf549e10e0e87be0c36df0e37810ceb06` |
| `cdc-design.md` | `170f57bf3f9b9d3113c00fde5d91a78f78beb1910a01b67a9970ebc51cf53a62` |
| `combined-review.md` | `832789978b70b048f078ae7f7f60ff834b7802ae22c15a4b910c85d0d7f19d58` |
| `memo-design.md` | `e3c2d5b94862c918460bd2c14f6cb77d3dc95232e09e57f265746d5ce7135aea` |
| `final-delta-review.md` | `56851b6e925f9f0d2bd1892d4a2ada6316e0e1d1466e8f07d35fb76254f8888b` |

Original implementation review range: `d7f95aa754b` → `f94980da1c`.
After merging main, `3ad9adbb4c` → `72dd7c4db4` has the same production
changes (31 files, +37/−1,353; no test edits), with identical stable patch ID
`6ad7b221504d3354e4234c68ad3358b029b06e41`. Original local tests/builds
remain attributed to their recorded revisions. The successful build, UT, SCA
and PROXY/PESSIMISTIC multi-CN BVT checks on `72dd7c4db4` are separate CI
evidence, linked from the PR. No new performance or historical-release
compatibility claim is introduced.
