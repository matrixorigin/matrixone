# MySQL MEDIUMINT and INT3 semantics

Design revision 1 · 2026-10-10 · implementation snapshot `669ecb6f59e623b98652e6da3a4989bd845c10b7`

Issue: [#29723](https://github.com/matrixorigin/matrixone/issues/29723), including the maintainer follow-up at [issue comment 6093473730](https://github.com/matrixorigin/matrixone/issues/29723#issuecomment-6093473730). Implementation: [PR #29818](https://github.com/matrixorigin/matrixone/pull/29818). This document records the design for the follow-up that adds MEDIUMINT support; it does not change the existing behavior that `CAST(... AS MEDIUMINT)` is rejected.

## Problem and contract

The original issue found that `MEDIUMINT` and its `INT3` alias could be accepted as ordinary `INT`, so a strict-mode insert outside MySQL's 24-bit domain succeeded. The follow-up asks MatrixOne to accept the type and preserve its signedness, range, DDL identity, and MySQL metadata.

The SQL contract is:

| Declaration | Logical domain | MatrixOne plan/catalog identity | Physical vector |
|---|---:|---|---|
| `MEDIUMINT`, `INT3` | signed `[-8,388,608, 8,388,607]` | `T_int32`, width 24, scale -1 | existing 4-byte `int32` |
| `MEDIUMINT UNSIGNED`, `INT3 UNSIGNED`, `ZEROFILL` | unsigned `[0, 16,777,215]` | `T_uint32`, width 24, scale -1 | existing 4-byte `uint32` |
| `INT`, `INT(24)` | ordinary 32-bit signed domain | `T_int32`, width 32 | existing 4-byte `int32` |
| `INT UNSIGNED`, `INT(24) UNSIGNED` | ordinary 32-bit unsigned domain | `T_uint32`, width 32 | existing 4-byte `uint32` |

MySQL uses three bytes for MEDIUMINT. MatrixOne deliberately retains its established four-byte vectors and storage. The SQL-visible domain remains 24-bit. MySQL documents that MEDIUMINT has the ranges above and that integer display width is unrelated to value range ([integer types](https://dev.mysql.com/doc/refman/8.4/en/integer-types.html), [numeric type attributes](https://dev.mysql.com/doc/refman/8.4/en/numeric-type-attributes.html)).

The parser distinguishes the declarations before they enter the catalog: `MEDIUMINT` and `INT3` parse as `MYSQL_TYPE_INT24`, while `INT(24)` parses as `MYSQL_TYPE_LONG`. The planner maps those to width 24 and width 32 respectively. `TestGetTypeFromAstBuildsMediumIntAliases` protects this distinction. This is unambiguous for SQL-created `INT(24)` tables. The old silent-widening behavior could already have persisted MEDIUMINT columns as `T_int32`/`T_uint32` with width 24; those existing catalog records cannot identify which binary values were outside the intended range. They are therefore interpreted as MEDIUMINT by the new code, without rewriting their stored bytes.

## Ownership and write path

The design reuses existing owners rather than adding a second general integer conversion system:

| Concern | Owner and behavior | Evidence |
|---|---|---|
| Parse and type identity | MySQL parser emits `MYSQL_TYPE_INT24`; `getTypeFromAstWithoutCharset` selects existing signed/unsigned 32-bit OIDs and preserves width 24. Runtime `types.Type.IsMediumInt` and `MediumIntBounds` recognize the OID/width pair and provide inclusive bounds; planner and protobuf expression walkers recognize their corresponding serialized type form. | `pkg/sql/parsers/dialect/mysql/mysql_sql.y`; `pkg/sql/plan/build_util.go`; `pkg/container/types/types.go`; `pkg/sql/plan/build_constraint_util.go`; `pkg/pb/plan/string_literal_form.go`; `TestGetTypeFromAstBuildsMediumIntAliases` |
| SQL assignment | Existing planner assignment-cast construction keeps MEDIUMINT casts even when source and target OIDs match (`needsSameTypeAssignmentCast`). The existing `newCast` path performs the ordinary conversion first, then checks the converted `int32`/`uint32` vector against the target domain. This covers ordinary and prepared INSERT, INSERT SELECT, UPDATE, and generated-column assignments without changing ordinary INT casts. | `pkg/sql/plan/build_constraint_util.go`; `pkg/sql/plan/function/func_cast.go`; `TestMediumIntAssignmentBounds`; `TestIssue29723MediumIntSemantics` |
| Defaults and DDL | Constant defaults use the existing DDL assignment cast and are rejected if their converted value is outside the target domain. CREATE and ALTER authoring use the existing planner protocol admission check. Narrowing ALTER validates source rows before replacement, leaving the source table unchanged on failure. | `pkg/sql/plan/make.go`; `pkg/sql/plan/build_util.go`; `pkg/sql/plan/build_alter_modify_column.go`; SQL integration test |
| AUTO_INCREMENT | The existing increment service selects MEDIUMINT-specific signed and unsigned maxima while retaining its current allocation path. | `pkg/incrservice/column_cache.go`; `TestInsertMediumIntAutoIncrementRange`; SQL integration test |
| CSV LOAD | The existing external CSV field validator and parser use 24-bit bounds for width-24 int32/uint32 targets. The unit test covers validation and append behavior, and the issue SQL test exercises parallel CSV LOAD. A serial SQL LOAD end-to-end case was not run. | `pkg/sql/colexec/external/external.go`; `TestMediumIntCSVLoadBounds`; `TestIssue29723MediumIntSemantics` |
| Parquet LOAD | The existing Parquet mapper validates MEDIUMINT values before appending them to the target vector; invalid values do not append partial output. | `pkg/sql/colexec/external/parquet.go`; `TestParquetMediumIntEnforcesLogicalBounds` |
| Arrow LOAD | The Arrow bridge checks MEDIUMINT source values at the LOAD boundary before borrowing buffers or materializing conversions. Ordinary integer columns keep their existing validation path. | `pkg/container/arrowbridge/budget.go::validateRecordColumns`; `TestMediumIntArrowLoadEnforcesLogicalBounds` in `pkg/container/arrowbridge/bridge_test.go`. A full SQL-to-Arrow LOAD end-to-end case was not run. |
| Typed arrays | MEDIUMINT array element casts use the existing JSON-to-array conversion and assignment validator. Same-type array assignment cannot elide element-bound checks. | `pkg/sql/plan/mysql_special_types.go`; `pkg/sql/plan/function/func_mo.go`; `TestFuncCastForTypedArrayMediumIntKeepsBoundsValidation`; `Test_BuiltIn_MoShowVisibleBinMediumIntDataType` |
| DDL and protocol metadata | SHOW CREATE and information_schema derive the name/precision from the same OID-plus-width identity. MySQL result metadata uses `MYSQL_TYPE_INT24`, with ColumnDefinition41 length 9 for signed and 8 for unsigned values; the engine and protocol continue to carry the existing 32-bit value representation. | `pkg/sql/plan/function/func_builtin.go`; `pkg/frontend/util.go`; `pkg/defines/type.go`; `TestFormatColTypeMediumInt`; `Test_setMysqlColumnTypeMetadataMediumIntLength`; `TestMysqlMediumIntProtocolMetadata`; SQL integration test |
| Persistence and schema copying | No new catalog field or on-disk vector format is introduced; the existing plan type fields preserve OID, width, and scale. The SQL integration test restarts the cluster and checks SHOW CREATE, value reads, and a rejected out-of-range write. No dedicated MEDIUMINT dump/clone end-to-end test was run. | `TestPersistedMediumIntCatalogType`; `TestIssue29723MediumIntSemantics` |

The core range check runs only for a recognized MEDIUMINT target. Ordinary `INT` and `INT UNSIGNED` keep their existing cast path and do not run a MEDIUMINT per-row scan. For non-integer sources, the existing conversion/rounding is completed before the 24-bit bounds are checked; the design does not add a new parser or coercion rule.

## Mode and failure behavior

For DML assignment, the existing assignment-cast mode is retained. With `STRICT_TRANS_TABLES`, `STRICT_ALL_TABLES`, or `TRADITIONAL`, an out-of-range converted value returns an out-of-range error and the statement does not store that value. Under non-strict assignment, the MEDIUMINT bounds pass clips an over-range representable value to the nearest endpoint and appends a warning diagnostic when a warning sink is available. The `INSERT IGNORE`/`UPDATE IGNORE` assignment mode reaches the non-strict range pass; the existing `cast_ignore` path continues to own lexical and conversion failures.

This is not a claim of full MySQL sql_mode compatibility. Dedicated MEDIUMINT regression coverage proves strict rejection and unit coverage proves non-strict range clipping, but a complete `IGNORE` integration matrix and exact warning parity for all source types/import modes were not run. CSV/Parquet readers reject values outside their target range in their existing ingestion paths. Constant defaults outside the range are rejected at DDL time. An ALTER that would narrow an existing out-of-range row fails without replacing the source table.

`CAST(... AS MEDIUMINT)` remains unsupported, matching MySQL's CAST target syntax. A planner test asserts the existing NYI error; column declarations and assignment semantics are a separate feature.

## Distributed execution and rollout

The bound check changes the meaning of assignment expressions sent to remote CNs. `MORPCVersion109` is the minimum protocol version for this behavior:

| Boundary | Existing owner reused | Required behavior |
|---|---|---|
| Authoring CREATE/ALTER and assignment expressions | `requireMediumIntProtocolForAuthoring` and `RequirePersistedProtocolVersionForAuthoring` | In production, require both the local `MOProtocolVersion` and the installed `PersistedExpressionProtocolAuthoringFloor` to be at least v109 before authoring MEDIUMINT semantics. Standalone test runtimes retain the existing missing-floor fallback. |
| Remote expression analysis and placement | `RequiredRemoteExpressionFeatures` recognizes scalar assignment CAST and typed-array JSON-to-array CAST; existing expression destination/send/receive checks enforce the version floor. | Do not send an expression containing the new bounds check to a destination below v109 or with unknown protocol. |
| Persisted expression owners | `RequiredPersistedExpressionProtocolVersion` adds v109 when the expression-feature analyzer finds MEDIUMINT assignment bounds; existing persisted-expression readers reapply that floor. | Preserve the expression compatibility floor across restart/rebind for catalog-bound expressions that contain the feature. This does not create a new catalog column format. |

Tests exercise authoring rejection below the floor, remote placement/send behavior, persisted feature aggregation, and combination with an existing JSON feature floor: `TestMediumIntCatalogAuthoringProtocolFence`, `TestMediumIntAssignmentRequiresClusterProtocolFence`, `TestMediumIntAssignmentProtocolPlacementAndSend`, `TestRequiredRemoteExpressionFeaturesMediumIntAssignmentBounds`, and `TestPersistedMediumIntRequirementCombinesWithJSONInput`.

The fence is specific to authoring and executing the new assignment semantics. A plain scan or MySQL metadata response still uses 32-bit values, so the design does not add a blanket v109 read gate for existing table rows. Upgrades should bring all CNs to v109-capable code before creating MEDIUMINT schemas or running assignments. A cluster restart on the current code preserves the identity and the bounds, as covered by the integration test.

Downgrading or rolling back to a binary below v109 after MEDIUMINT columns have been created is not safe: that binary may treat width-24 int32/uint32 values as ordinary 32-bit integers and accept out-of-range writes. The protocol floor is not an automatic downgrade migration. A rollback plan must keep v109-capable execution or explicitly convert affected columns to ordinary INT and resolve any out-of-range values before pre-v109 CNs resume.

For legacy width-24 columns, reads return the stored 32-bit value unchanged. A later assignment to the same MEDIUMINT target checks the target range even when the source and target OIDs match. Widening the column to ordinary INT preserves its value. No automatic data rewrite or repair is performed at upgrade.

## Alternatives and trade-offs

| Option | Benefit | Cost / decision |
|---|---|---|
| Add dedicated signed/unsigned 24-bit engine OIDs | The logical identity is explicit and cannot be confused with any existing width metadata. | Requires new vector/codec/catalog handling across type switches, serialization, function dispatch, protocol mapping, and upgrade compatibility. Rejected because the engine already has correct 32-bit storage and a single width-based identity that the parser can distinguish from INT(24). |
| Keep `T_int32`/`T_uint32`, use width 24, validate at existing owners | Preserves current four-byte vectors and catalog shape, reuses assignment/import/auto-increment and protocol owners, and leaves INT(24) at width 32. | Historical width-24 catalog records are reinterpreted as MEDIUMINT, and old over-range values are possible. No migration is implicit; raw reads/widening preserve them, while future same-target writes reject them. Selected. |
| Reject the aliases or keep mapping them to INT | Avoids changing storage and distributed semantics. | Rejection does not meet the maintainer's follow-up request for full MEDIUMINT/INT3 support; mapping to INT retains the bug. Rejected. |

## Validation and measured cost

The test names above are the acceptance map. The public SQL integration `TestIssue29723MediumIntSemantics` sets `STRICT_TRANS_TABLES` and covers declarations/aliases, defaults, generated values, CSV load failure atomicity, prepared and ordinary assignments, INSERT SELECT, UPDATE, narrowing ALTER failure/reuse, AUTO_INCREMENT, information_schema, result metadata, and restart. Unit tests cover signed and unsigned endpoints and first out-of-range values; ordinary INT(24), ordinary INT casts, same-type legacy values, and widening are explicit controls.

Validation provenance:

| Source revision | Validation | Result |
|---|---|---|
| `669ecb6f59e623b98652e6da3a4989bd845c10b7`, base `e5dd4724f782067e1c76c38c9d2f55cebceda4a9` | `.agents/skills/mo-dev/scripts/mo-cgo-test -run '^TestIssue(29723MediumIntSemantics|25103InformationSchemaMetadata)$' -count=1 ./pkg/tests/issues` | Pass, 16.278s |
| same revision | Repository-pinned golangci-lint v2.14.0, Makefile CGo include flags, `./pkg/tests/issues` | Pass, 0 issues |
| `323283b238209e7091df71a1620d1d63f6b755ca`, pre-rebase base `62827a1c8bfb1c846c0f3528e71503baacb3b3bc` | CGo planner and compile suites plus both issue integration tests | Pass after the persisted feature-floor correction; exact runtimes are recorded in the task validation ledger. |
| `51f86d5054f9893c00c273dcea1114ff6857f1ec`, pre-rebase base `62827a1c8bfb1c846c0f3528e71503baacb3b3bc` | CGo `./pkg/pb/plan ./pkg/sql/plan ./pkg/sql/compile ./pkg/frontend`, `./pkg/sql/plan/function`, and both issue integration tests | Pass. |
| `48f45bdafd90c1ec7f174884a6cb72b2494475f5`, pre-rebase base `62827a1c8bfb1c846c0f3528e71503baacb3b3bc` | CGo `./pkg/container/arrowbridge ./pkg/container/types ./pkg/incrservice ./pkg/sql/colexec/external ./pkg/sql/colexec/table_clone` | Pass for the initial implementation. Later amendments did not change these packages or their call contracts. |

The upstream base advanced between these pre-rebase runs and `669ecb6`. The final-base issue integrations and SCA lint were rerun on `669ecb6`; the full affected-package suites above were not all rerun after rebase.

A local microbenchmark compared a 1,024-row ordinary `int64`-to-`int32` cast before and after the MEDIUMINT-only guard: baseline 5.17–5.47 µs/op, guarded 5.15–5.48 µs/op; both 96 B/op and one allocation. These ranges overlap and show no measurable overhead in this narrow path. This is not a TPC/import throughput result; no full TPCH/TPC benchmark, sustained import benchmark, full Kafka/Flink CDC pipeline, or full `mo-tester` DATALINK suite was run. The CI run on the rebased head remains the authoritative pending validation for the new PR revision.

## Review concern map

This map responds to the design obligations in [review 5478435322](https://github.com/matrixorigin/matrixone/pull/29818#pullrequestreview-5478435322). There are no inline review threads attached to that review.

| Concern | Design section | Evidence / remaining limit |
|---|---|---|
| Identity, bounds, INT(24), and reuse of current owners | Contract; Ownership and write path | Parser alias tests and the OID-plus-width type predicate; four-byte storage is deliberate. Arbitrary historical raw width-24 values have no provenance bit and are not migrated. |
| DML, metadata, import, auto-increment, arrays, mode behavior | Ownership and write path; Mode and failure behavior | Exact unit and issue integration tests listed above. Arrow has a focused bridge test but no full SQL-to-Arrow LOAD e2e; complete IGNORE and warning parity are not claimed. |
| v109 authoring, execution, persisted expressions, upgrade/restart/downgrade | Distributed execution and rollout | Existing feature analyzer/protocol owners and named tests; plain legacy scans remain readable. Rollback below v109 after feature use is unsafe without conversion. |
| Representation alternatives and performance/acceptance evidence | Alternatives and trade-offs; Validation and measured cost | Focused ordinary-INT microbenchmark is within noise; broad TP/import throughput was not measured. |

## Review provenance

The design pass was requested from the independent Codex task `/root/luna_fix/design_review` using `gpt-6.1-sol` at `xhigh` on 2026-10-10 before implementation; it recommended the existing 32-bit OIDs plus width-24 identity and reuse of current assignment/import/metadata/protocol owners. The independent final code review used the same requested model and effort on implementation snapshot `669ecb6f59e623b98652e6da3a4989bd845c10b7` and reported no blockers. These entries identify internal model-review provenance, not a project maintainer decision.
