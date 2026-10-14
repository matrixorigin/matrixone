# MEDIUMINT review corrections

Design revision 2 · 2026-10-12 · follow-up to [revision 1](https://github.com/matrixorigin/matrixone/blob/6eadc465bb5fed9a96a65218ca58d86b8b321ce7/docs/design/20261010-mysql-mediumint-semantics.md)

Issue: [#29723](https://github.com/matrixorigin/matrixone/issues/29723). Implementation: [PR #29818](https://github.com/matrixorigin/matrixone/pull/29818). This revision addresses four concrete behavior gaps reported on the PR and a protocol-feature detection gap found while verifying those fixes. Revision 1's header identifies an earlier implementation snapshot; the v110 floor here is the post-rebase design and must not be attributed to that earlier snapshot. The floor is v110 because upstream main uses v109 for Unicode collation metadata. This revision does not change the 32-bit physical representation, catalog identity, or the existing unsupported `CAST(... AS MEDIUMINT)` syntax.

## Correctness boundaries

The logical MEDIUMINT domains remain signed `[-8,388,608, 8,388,607]` and unsigned `[0, 16,777,215]`. MatrixOne stores them in the existing `T_int32`/`T_uint32` vectors with width 24. Ordinary `INT` uses the same signed/unsigned OIDs with width 32.

The fixes keep four distinct owners separate:

| Behavior | Owner | Rule |
|---|---|---|
| Runtime assignment | `newCast` and the existing assignment cast modes | Apply MEDIUMINT bounds only for strict, ordinary, and IGNORE assignment modes. A normal or comparison cast may carry a physically representable 32-bit operand without treating it as a stored MEDIUMINT value. Ordinary DML writes remain range checked. |
| Common result type | `CommonMediumIntType`, used by COALESCE, CASE, IF, and UNION output/branch alignment | For same-signed 32-bit branches containing MEDIUMINT, retain width 24 only when every value-bearing branch is MEDIUMINT. If any branch is ordinary INT, widen the common result to width 32, regardless of branch order. Untyped NULL is neutral. UNION widens the branch expression's outer type metadata on a shallow copy because the physical vector is already int32/uint32; it adds no row-wise cast. Other OIDs and mixed-signedness promotion continue through their existing rules. Global `types.Type.Eq` is unchanged. |
| Constant DDL default | `buildDefaultExprWithColumns` | Validate constant MEDIUMINT defaults through a separate strict assignment cast, independent of session `sql_mode`; map range failure to MySQL `ER_INVALID_DEFAULT` (1067). Keep the stored/runtime assignment expression unchanged, and do not evaluate volatile or row-dependent defaults at DDL time. |
| Arrow logical NULL | `mediumIntArrowValue` and the existing Arrow validation/conversion callers | Check row, dictionary-index, and referenced dictionary-value validity before reading a numeric payload. Arrow NULL values and logical NULLs are SQL NULL even when their physical payload contains an out-of-range integer. Non-null out-of-range values remain errors. |
| Protocol feature detection | `RequiredRemoteExpressionFeatures`, shared by remote placement/send/receive and persisted-expression admission | Recognize the real internal assignment function IDs: `cast_strict` 541, `cast_assign` 554, and `cast_ignore` 555, and read the logical width from the cast's target argument. UNION may widen only the outer result metadata to width 32 while retaining a width-24 assignment target. Generic CAST ID 21 does not enforce assignment bounds and must not trigger this write-specific v110 gate. Typed-array conversion continues to use `CAST_JSON_TO_ARRAY` ID 513. |

The assignment check runs after the existing conversion, so the established source conversion and rounding behavior remains in place. The change does not add a general MySQL `sql_mode` compatibility layer. Nonstrict DML keeps its existing clipping behavior; strict default declaration validation is intentionally independent of that runtime policy.

## PR review findings and disposition

| Finding | Disposition | Regression evidence |
|---|---|---|
| [Comparison operands were rejected at the MEDIUMINT storage boundary](https://github.com/matrixorigin/matrixone/pull/29818#discussion_r4237647302) | Addressed by restricting the bounds pass to assignment modes. Comparisons on either side of the predicate accept a representable INT32 value; an ordinary INT control is unchanged. An out-of-range INSERT still fails. | `TestIssue29723MediumIntAdjacentContracts/comparison_operands_are_not_assignment_bounds`; existing `TestMediumIntAssignmentBounds`. |
| [Mixed MEDIUMINT and INT results depended on branch order](https://github.com/matrixorigin/matrixone/pull/29818#discussion_r4237647309) | Addressed at the existing common-result owners. UNION result metadata, same-OID branch width metadata, and CASE/IF/COALESCE casts now use the common width-32 domain when an ordinary INT branch is present. No row-wise conversion, global equality, or catalog rule changed. | `TestMediumIntCommonResultTypeIsOrderIndependent`; `TestIssue29723MediumIntAdjacentContracts` checks both default UNION orders, both CTAS orders, and mixed CASE CTAS. |
| [Nonstrict `sql_mode` allowed an invalid constant default](https://github.com/matrixorigin/matrixone/pull/29818#discussion_r4237647313) | Addressed by strict constant-default validation at DDL authoring, without replacing the persisted runtime assignment cast. Both signed and unsigned overflow defaults fail with 1067 and leave no table, under strict and empty `sql_mode`. | `TestIssue29723MediumIntAdjacentContracts/ddl_defaults_reject_overflow_in_every_sql_mode`; `TestMediumIntAssignmentBounds` retains nonstrict clipping coverage. |
| [Arrow dictionary validation read a NULL payload as a value](https://github.com/matrixorigin/matrixone/pull/29818#discussion_r4237647316) | Addressed by resolving logical validity before numeric extraction for null indices, null referenced values, and Arrow NULL dictionaries. Non-null overflow remains rejected. | `TestMediumIntDictionaryValidationUsesLogicalNulls` exercises both `ValidateRecord` and `Convert`, including poisoned null payloads, null indices, Arrow NULL values, and non-null overflow. |
| The feature analyzer test used CAST ID 21 with the name `cast_assign`, unlike planner output | Addressed by analyzing the real IDs 541/554/555 and reading the cast target argument rather than outer result metadata. UNION can shallow-widen that outer result; generic CAST ID 21 remains outside the assignment-bounds feature. Planner-resolved and widened-result regressions use the real IDs. | `TestMediumIntAssignmentRequiresClusterProtocolFence`; `TestRequiredRemoteExpressionFeaturesMediumIntAssignmentBounds`; `TestMediumIntAssignmentProtocolPlacementAndSend`. |

Together, these dispositions address the four linked technical reports and the feature-analysis mismatch found during verification. They do not change GitHub review state or claim a project maintainer has cleared a review.

## Mixed-version floor after rebasing

The current main branch already uses MORPC v109 for native Unicode collation identities in persisted `information_schema.COLUMNS` metadata. That metadata path is negotiated in `pkg/frontend/authenticate.go`, separately from the persisted-expression analyzer. MEDIUMINT assignment semantics therefore require MORPC v110. The authoring gate, remote destination/send/receive checks, persisted-expression feature analyzer, and tests use v110; unrelated v109 metadata negotiation remains unchanged. When a persisted expression combines MEDIUMINT with another expression contract, such as JSON input at v101, the existing maximum-version aggregation returns v110. This prevents a v109 receiver from being mistaken for a MEDIUMINT-capable executor.

## Validation and limits

Before the final rebase, on main `5a5501b3e1048f3d19d96824cc68a62b7f0fcb07`, these broader CGo checks passed:

| Command | Result |
|---|---|
| `.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 ./pkg/sql/plan` | Pass, 30.694s |
| `.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 ./pkg/sql/plan/function` | Pass, 18.241s |
| `.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 ./pkg/container/arrowbridge ./pkg/container/types` | Pass, 2.206s and 4.817s |
| `.agents/skills/mo-dev/scripts/mo-cgo-test -run '^(TestMediumIntAssignmentProtocolPlacementAndSend|TestMediumIntAssignmentRequiresClusterProtocolFence|TestMediumIntCatalogAuthoringProtocolFence|TestPersistedMediumIntRequirementCombinesWithJSONInput)$' -count=1 ./pkg/sql/compile ./pkg/sql/plan` | Pass, 3.117s and 4.481s; MORPC v109 rejects MEDIUMINT assignment features and v110 accepts them |
| `.agents/skills/mo-cgo-test -run '^(TestFormatSQLColumnType|Test_BuiltIn_MoShowVisibleBinIntegerMetadata|Test_BuiltIn_MoShowVisibleBinMediumIntDataType)$' -count=1 ./pkg/container/types ./pkg/sql/plan/function` | Pass, 1.424s and 1.782s |
| Scoped SQL regressions (command below) | Pass, 16.550s |

```sh
.agents/skills/mo-dev/scripts/mo-cgo-test -run '^(TestIssue29723MediumIntSemantics|TestIssue29723MediumIntAdjacentContracts|TestIssue25103InformationSchemaMetadata)$' -count=1 ./pkg/tests/issues
```

`gofmt -d` and `git diff --check` are clean. Repository-pinned golangci-lint v2.14.0 reported `0 issues` across all seven changed Go packages: `pkg/pb/plan`, `pkg/sql/plan`, `pkg/sql/compile`, `pkg/container/types`, `pkg/container/arrowbridge`, `pkg/sql/plan/function`, and `pkg/tests/issues`.

After the final protocol-feature detector correction, these focused checks also passed:

| Command | Result |
|---|---|
| `.agents/skills/mo-dev/scripts/mo-cgo-test -run '^TestRequiredRemoteExpressionFeaturesMediumIntAssignmentBounds$' -count=1 ./pkg/pb/plan` | Pass, 0.661s; real assignment IDs 541/554/555 are recognized, while generic CAST 21 and ordinary INT are not assignment-bound features |
| `.agents/skills/mo-dev/scripts/mo-cgo-test -run '^(TestMediumIntAssignmentRequiresClusterProtocolFence|TestPersistedMediumIntRequirementCombinesWithJSONInput)$' -count=1 ./pkg/sql/plan` | Pass, 2.265s; planner-resolved expressions carry the v110 persisted-expression requirement |
| `.agents/skills/mo-dev/scripts/mo-cgo-test -run '^TestMediumIntAssignmentProtocolPlacementAndSend$' -count=1 ./pkg/sql/compile` | Pass, 2.743s with the actual `CAST_ASSIGN` function ID |
| `.agents/skills/mo-dev/scripts/mo-cgo-test -run '^(TestIssue29723MediumIntSemantics|TestIssue29723MediumIntAdjacentContracts|TestIssue25103InformationSchemaMetadata)$' -count=1 ./pkg/tests/issues` | Pass, 12.793s |
| Repository-pinned golangci-lint v2.14.0 over `pkg/pb/plan`, `pkg/sql/plan`, `pkg/sql/compile`, `pkg/container/types`, `pkg/container/arrowbridge`, `pkg/sql/plan/function`, and `pkg/tests/issues` | Pass, 0 issues |

After the final rebase onto main `7391899db5073f33765341641daacecb95f02ab7` and the target-argument correction, the focused SQL regressions passed in 12.114s, and the actual-ID protocol-feature, persisted-expression, and remote-placement regressions passed in `pkg/pb/plan` (0.822s), `pkg/sql/plan` (1.861s), and `pkg/sql/compile` (3.112s). Full `pkg/pb/plan` and `pkg/sql/plan` passed in 0.624s and 11.108s. Repository-pinned golangci-lint v2.14.0 was rerun on the three packages touched by the target-argument correction (`pkg/pb/plan`, `pkg/sql/plan`, `pkg/sql/compile`) and reported `0 issues`; the earlier seven-package run also reported `0 issues`.

The full compile package suite fails in `TestTruncateLifecycleRCWaitPolicy`, added by main commit `7391899` in `pkg/sql/compile/ddl_test.go`. Its three subtests time out, get an unexpected `Snapshot`, and report missing `HasLockTable(41)` / “TRUNCATE did not request lifecycle admission.” An isolated rerun reproduces the failure both on the PR branch and in a clean detached worktree at exact base `7391899db`; the MEDIUMINT placement test passes separately. The changed Go files passed `gofmt -d` and `git diff --check`.

The issue tests use the embedded MatrixOne cluster and MySQL client protocol. This follow-up did not run a full `mo-tester`/DATALINK suite, a complete Kafka/Flink CDC pipeline, or a new throughput benchmark. Revision 1 records the storage, protocol, rollout, and performance limits for the broader MEDIUMINT implementation. Historical width-24 rows are not rewritten: reads preserve their existing 32-bit values, widening to ordinary INT preserves those values, and future assignments to MEDIUMINT continue to check the logical domain.
