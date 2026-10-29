# MySQL MEDIUMINT and INT3 semantics

Issue: [#29723](https://github.com/matrixorigin/matrixone/issues/29723), including the maintainer follow-up at [issue comment 6093473730](https://github.com/matrixorigin/matrixone/issues/29723#issuecomment-6093473730). Implementation: [PR #29818](https://github.com/matrixorigin/matrixone/pull/29818). This maintained contract includes the assignment, common-result, default, Arrow and protocol corrections. `CAST(... AS MEDIUMINT)` remains rejected.

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
| SQL assignment | Existing planner assignment-cast construction keeps MEDIUMINT casts even when source and target OIDs match (`needsSameTypeAssignmentCast`). The existing `newCast` path performs conversion first, then applies target bounds only in strict, ordinary and IGNORE assignment modes. Normal/comparison casts do not impose storage bounds. This covers ordinary and prepared INSERT, INSERT SELECT, UPDATE, and generated-column assignments without changing ordinary INT casts. | `pkg/sql/plan/build_constraint_util.go`; `pkg/sql/plan/function/func_cast.go`; `TestMediumIntAssignmentBounds`; `TestIssue29723MediumIntSemantics` |
| Defaults and DDL | Constant defaults receive a separate strict DDL assignment check independent of session `sql_mode`; range failure maps to `ER_INVALID_DEFAULT` (1067). The stored/runtime expression is retained, and volatile or row-dependent defaults are not evaluated early. CREATE and ALTER authoring use the existing planner protocol admission check. Narrowing ALTER validates source rows before replacement, leaving the source table unchanged on failure. | `pkg/sql/plan/make.go`; `pkg/sql/plan/build_util.go`; `pkg/sql/plan/build_alter_modify_column.go`; SQL integration test |
| AUTO_INCREMENT | The existing increment service selects MEDIUMINT-specific signed and unsigned maxima while retaining its current allocation path. | `pkg/incrservice/column_cache.go`; `TestInsertMediumIntAutoIncrementRange`; SQL integration test |
| CSV LOAD | The existing external CSV field validator and parser use 24-bit bounds for width-24 int32/uint32 targets. The unit test covers validation and append behavior, and the issue SQL test exercises parallel CSV LOAD. A serial SQL LOAD end-to-end case was not run. | `pkg/sql/colexec/external/external.go`; `TestMediumIntCSVLoadBounds`; `TestIssue29723MediumIntSemantics` |
| Parquet LOAD | The existing Parquet mapper validates MEDIUMINT values before appending them to the target vector; invalid values do not append partial output. | `pkg/sql/colexec/external/parquet.go`; `TestParquetMediumIntEnforcesLogicalBounds` |
| Arrow LOAD | The Arrow bridge resolves logical validity before reading payloads, including dictionary indices and referenced values. Null indices, null dictionary values and Arrow NULL dictionaries remain SQL NULL even with poisoned physical payloads. Non-null values are checked before buffers are borrowed or conversions materialized. Ordinary integer columns keep their existing validation path. | `pkg/container/arrowbridge/budget.go::validateRecordColumns`; `TestMediumIntArrowLoadEnforcesLogicalBounds` in `pkg/container/arrowbridge/bridge_test.go`. A full SQL-to-Arrow LOAD end-to-end case was not run. |
| Typed arrays | MEDIUMINT array element casts use the existing JSON-to-array conversion and assignment validator. Same-type array assignment cannot elide element-bound checks. | `pkg/sql/plan/mysql_special_types.go`; `pkg/sql/plan/function/func_mo.go`; `TestFuncCastForTypedArrayMediumIntKeepsBoundsValidation`; `Test_BuiltIn_MoShowVisibleBinIntegerMetadata` |
| DDL and protocol metadata | SHOW CREATE and information_schema derive the name/precision from the same OID-plus-width identity. MySQL result metadata uses `MYSQL_TYPE_INT24`, with ColumnDefinition41 length 9 for signed and 8 for unsigned values; the engine and protocol continue to carry the existing 32-bit value representation. | `pkg/sql/plan/function/func_builtin.go`; `pkg/frontend/util.go`; `pkg/defines/type.go`; `TestFormatColTypeMediumInt`; `Test_setMysqlColumnTypeMetadataMediumIntLength`; `TestMysqlMediumIntProtocolMetadata`; SQL integration test |
| Persistence and schema copying | No new catalog field or on-disk vector format is introduced; the existing plan type fields preserve OID, width, and scale. The SQL integration test restarts the cluster and checks SHOW CREATE, value reads, and a rejected out-of-range write. No dedicated MEDIUMINT dump/clone end-to-end test was run. | `TestPersistedMediumIntCatalogType`; `TestIssue29723MediumIntSemantics` |

`CommonMediumIntType` owns same-signed common-result width for COALESCE, CASE,
IF and UNION alignment. Width 24 is retained only when every value-bearing
32-bit branch is MEDIUMINT; any ordinary INT branch widens the result to 32,
regardless of branch order. Untyped NULL is neutral. UNION can widen outer type
metadata on a shallow copy without a per-row conversion. Other OIDs and
mixed-signedness promotion retain their existing owners; global type equality
is unchanged.

The assignment range check runs only for a recognized MEDIUMINT target. Ordinary `INT` and `INT UNSIGNED` keep their existing cast path and do not run a MEDIUMINT per-row scan. For non-integer sources, the existing conversion/rounding is completed before the 24-bit bounds are checked; the design does not add a new parser or coercion rule.

## Mode and failure behavior

For DML assignment, the existing assignment-cast mode is retained. With `STRICT_TRANS_TABLES`, `STRICT_ALL_TABLES`, or `TRADITIONAL`, an out-of-range converted value returns an out-of-range error and the statement does not store that value. Under non-strict assignment, the MEDIUMINT bounds pass clips an over-range representable value to the nearest endpoint and appends a warning diagnostic when a warning sink is available. The `INSERT IGNORE`/`UPDATE IGNORE` assignment mode reaches the non-strict range pass; the existing `cast_ignore` path continues to own lexical and conversion failures.

This is not a claim of full MySQL sql_mode compatibility. Dedicated MEDIUMINT regression coverage proves strict rejection and unit coverage proves non-strict range clipping, but a complete `IGNORE` integration matrix and exact warning parity for all source types/import modes were not run. CSV/Parquet readers reject values outside their target range in their existing ingestion paths. Constant defaults outside the range are rejected at DDL time even under empty `sql_mode`. An ALTER that would narrow an existing out-of-range row fails without replacing the source table.

`CAST(... AS MEDIUMINT)` remains unsupported, matching MySQL's CAST target syntax. A planner test asserts the existing NYI error; column declarations and assignment semantics are a separate feature.

## Distributed execution and rollout

The bound check changes the meaning of assignment expressions sent to remote CNs. `MORPCVersion110` is the minimum protocol version for this behavior. Version 109 is already used by native Unicode collation identities in persisted `information_schema.COLUMNS` metadata on the updated base:

| Boundary | Existing owner reused | Required behavior |
|---|---|---|
| Authoring CREATE/ALTER and assignment expressions | `requireMediumIntProtocolForAuthoring` and `RequirePersistedProtocolVersionForAuthoring` | In production, require both the local `MOProtocolVersion` and the installed `PersistedExpressionProtocolAuthoringFloor` to be at least v110 before authoring MEDIUMINT semantics. Standalone test runtimes retain the existing missing-floor fallback. |
| Remote expression analysis and placement | `RequiredRemoteExpressionFeatures` recognizes scalar assignment CAST and typed-array JSON-to-array CAST; existing expression destination/send/receive checks enforce the version floor. | Do not send an expression containing the new bounds check to a destination below v110 or with unknown protocol. |
| Persisted expression owners | `RequiredPersistedExpressionProtocolVersion` adds v110 when the expression-feature analyzer finds MEDIUMINT assignment bounds; existing persisted-expression readers reapply that floor. | Preserve the expression compatibility floor across restart/rebind for catalog-bound expressions that contain the feature. This does not create a new catalog column format. |

The shared feature analyzer recognizes the actual internal assignment IDs:
`cast_strict` 541, `cast_assign` 554 and `cast_ignore` 555. It inspects the cast
target argument, because UNION may widen only outer result metadata. Generic
CAST ID 21 does not enforce assignment bounds and does not activate this gate.
Typed-array conversion keeps `CAST_JSON_TO_ARRAY` ID 513. Combining features
uses the existing maximum floor, including JSON input v101; unrelated Unicode
metadata v109 negotiation remains separate.

Tests exercise authoring rejection below the floor, remote placement/send behavior, persisted feature aggregation, and combination with an existing JSON feature floor: `TestMediumIntCatalogAuthoringProtocolFence`, `TestMediumIntAssignmentRequiresClusterProtocolFence`, `TestMediumIntAssignmentProtocolPlacementAndSend`, `TestRequiredRemoteExpressionFeaturesMediumIntAssignmentBounds`, and `TestPersistedMediumIntRequirementCombinesWithJSONInput`.

The fence is specific to authoring and executing the new assignment semantics. A plain scan or MySQL metadata response still uses 32-bit values, so the design does not add a blanket v110 read gate for existing table rows. Upgrades should bring all CNs to v110-capable code before creating MEDIUMINT schemas or running assignments. A cluster restart on the current code preserves the identity and the bounds, as covered by the integration test.

Downgrading or rolling back to a binary below v110 after MEDIUMINT columns have been created is not safe: that binary may treat width-24 int32/uint32 values as ordinary 32-bit integers and accept out-of-range writes. The protocol floor is not an automatic downgrade migration. A rollback plan must keep v110-capable execution or explicitly convert affected columns to ordinary INT and resolve any out-of-range values before pre-v110 CNs resume.

For legacy width-24 columns, reads return the stored 32-bit value unchanged. A later assignment to the same MEDIUMINT target checks the target range even when the source and target OIDs match. Widening the column to ordinary INT preserves its value. No automatic data rewrite or repair is performed at upgrade.

## Alternatives and trade-offs

| Option | Benefit | Cost / decision |
|---|---|---|
| Add dedicated signed/unsigned 24-bit engine OIDs | The logical identity is explicit and cannot be confused with any existing width metadata. | Requires new vector/codec/catalog handling across type switches, serialization, function dispatch, protocol mapping, and upgrade compatibility. Rejected because the engine already has correct 32-bit storage and a single width-based identity that the parser can distinguish from INT(24). |
| Keep `T_int32`/`T_uint32`, use width 24, validate at existing owners | Preserves current four-byte vectors and catalog shape, reuses assignment/import/auto-increment and protocol owners, and leaves INT(24) at width 32. | Historical width-24 catalog records are reinterpreted as MEDIUMINT, and old over-range values are possible. No migration is implicit; raw reads/widening preserve them, while future same-target writes reject them. Selected. |
| Reject the aliases or keep mapping them to INT | Avoids changing storage and distributed semantics. | Rejection does not meet the maintainer's follow-up request for full MEDIUMINT/INT3 support; mapping to INT retains the bug. Rejected. |

## Regression ownership and historical evidence

| Contract | Existing regression owners |
| --- | --- |
| Assignment versus comparison, strict/nonstrict bounds | `TestMediumIntAssignmentBounds`; `TestIssue29723MediumIntAdjacentContracts` |
| Order-independent common width | `TestMediumIntCommonResultTypeIsOrderIndependent`; both UNION/CTAS orders and mixed CASE in the adjacent integration test |
| Strict DDL defaults and unchanged table after rejection | Signed/unsigned default and narrowing ALTER cases in `TestIssue29723MediumIntSemantics` and the adjacent integration test |
| Logical Arrow NULL and non-null overflow | `TestMediumIntDictionaryValidationUsesLogicalNulls`; bridge validation/conversion tests |
| Authoring, real assignment IDs, target metadata and remote floors | The planner, protobuf and compiler protocol tests named above |
| Identity, metadata, imports, auto-increment, arrays and restart | Existing type/parser/import unit tests and [issue integration tests](../../pkg/tests/issues/issue_29723_test.go) |

The [original design and measurements](https://github.com/matrixorigin/matrixone/blob/6cbe375b49696021d72778baa292a66287f080bd/docs/design/20261010-mysql-mediumint-semantics.md)
and [review corrections with validation](https://github.com/matrixorigin/matrixone/blob/6cbe375b49696021d72778baa292a66287f080bd/docs/design/20261012-mysql-mediumint-review-corrections.md)
preserve the complete review findings, test revisions, microbenchmark and
limitations. Correction semantics are incorporated in their owning sections
above rather than retained in a separate review document.

The historical compile-suite failure in `TestTruncateLifecycleRCWaitPolicy`
was reproduced on its clean base as well as the feature branch; the focused
MEDIUMINT protocol tests passed separately. Neither those passes nor the
ordinary-INT microbenchmark establish current-head CI or whole-query throughput.
Full serial SQL LOAD, SQL-to-Arrow LOAD, dump/clone, complete IGNORE/warning
parity, sustained import and Kafka/Flink pipeline coverage were not established
by that campaign. This documentation consolidation supplies no new execution
or performance evidence.
