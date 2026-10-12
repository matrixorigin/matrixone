# MEDIUMINT review corrections

Design revision 2 · 2026-10-12 · follow-up to [revision 1](https://github.com/matrixorigin/matrixone/blob/6eadc465bb5fed9a96a65218ca58d86b8b321ce7/docs/design/20261010-mysql-mediumint-semantics.md)

Issue: [#29723](https://github.com/matrixorigin/matrixone/issues/29723). Implementation: [PR #29818](https://github.com/matrixorigin/matrixone/pull/29818). This revision addresses four concrete behavior gaps reported on the PR. It supplements revision 1; it does not change the 32-bit physical representation, catalog identity, protocol floor, or the existing unsupported `CAST(... AS MEDIUMINT)` syntax.

## Correctness boundaries

The logical MEDIUMINT domains remain signed `[-8,388,608, 8,388,607]` and unsigned `[0, 16,777,215]`. MatrixOne stores them in the existing `T_int32`/`T_uint32` vectors with width 24. Ordinary `INT` uses the same signed/unsigned OIDs with width 32.

The fixes keep four distinct owners separate:

| Behavior | Owner | Rule |
|---|---|---|
| Runtime assignment | `newCast` and the existing assignment cast modes | Apply MEDIUMINT bounds only for strict, ordinary, and IGNORE assignment modes. A normal or comparison cast may carry a physically representable 32-bit operand without treating it as a stored MEDIUMINT value. Ordinary DML writes remain range checked. |
| Common result type | `CommonMediumIntType`, used by COALESCE, CASE, IF, and UNION output/branch alignment | For same-signed 32-bit branches containing MEDIUMINT, retain width 24 only when every value-bearing branch is MEDIUMINT. If any branch is ordinary INT, widen the common result to width 32, regardless of branch order. Untyped NULL is neutral. UNION widens the branch expression's outer type metadata on a shallow copy because the physical vector is already int32/uint32; it adds no row-wise cast. Other OIDs and mixed-signedness promotion continue through their existing rules. Global `types.Type.Eq` is unchanged. |
| Constant DDL default | `buildDefaultExprWithColumns` | Validate constant MEDIUMINT defaults through a separate strict assignment cast, independent of session `sql_mode`; map range failure to MySQL `ER_INVALID_DEFAULT` (1067). Keep the stored/runtime assignment expression unchanged, and do not evaluate volatile or row-dependent defaults at DDL time. |
| Arrow logical NULL | `mediumIntArrowValue` and the existing Arrow validation/conversion callers | Check row, dictionary-index, and referenced dictionary-value validity before reading a numeric payload. Arrow NULL values and logical NULLs are SQL NULL even when their physical payload contains an out-of-range integer. Non-null out-of-range values remain errors. |

The assignment check runs after the existing conversion, so the established source conversion and rounding behavior remains in place. The change does not add a general MySQL `sql_mode` compatibility layer. Nonstrict DML keeps its existing clipping behavior; strict default declaration validation is intentionally independent of that runtime policy.

## PR review findings and disposition

| Finding | Disposition | Regression evidence |
|---|---|---|
| [Comparison operands were rejected at the MEDIUMINT storage boundary](https://github.com/matrixorigin/matrixone/pull/29818#discussion_r4237647302) | Addressed by restricting the bounds pass to assignment modes. Comparisons on either side of the predicate accept a representable INT32 value; an ordinary INT control is unchanged. An out-of-range INSERT still fails. | `TestIssue29723MediumIntAdjacentContracts/comparison_operands_are_not_assignment_bounds`; existing `TestMediumIntAssignmentBounds`. |
| [Mixed MEDIUMINT and INT results depended on branch order](https://github.com/matrixorigin/matrixone/pull/29818#discussion_r4237647309) | Addressed at the existing common-result owners. UNION result metadata, same-OID branch width metadata, and CASE/IF/COALESCE casts now use the common width-32 domain when an ordinary INT branch is present. No row-wise conversion, global equality, or catalog rule changed. | `TestMediumIntCommonResultTypeIsOrderIndependent`; `TestIssue29723MediumIntAdjacentContracts` checks both default UNION orders, both CTAS orders, and mixed CASE CTAS. |
| [Nonstrict `sql_mode` allowed an invalid constant default](https://github.com/matrixorigin/matrixone/pull/29818#discussion_r4237647313) | Addressed by strict constant-default validation at DDL authoring, without replacing the persisted runtime assignment cast. Both signed and unsigned overflow defaults fail with 1067 and leave no table, under strict and empty `sql_mode`. | `TestIssue29723MediumIntAdjacentContracts/ddl_defaults_reject_overflow_in_every_sql_mode`; `TestMediumIntAssignmentBounds` retains nonstrict clipping coverage. |
| [Arrow dictionary validation read a NULL payload as a value](https://github.com/matrixorigin/matrixone/pull/29818#discussion_r4237647316) | Addressed by resolving logical validity before numeric extraction for null indices, null referenced values, and Arrow NULL dictionaries. Non-null overflow remains rejected. | `TestMediumIntDictionaryValidationUsesLogicalNulls` exercises both `ValidateRecord` and `Convert`, including poisoned null payloads, null indices, Arrow NULL values, and non-null overflow. |

These dispositions address the four linked technical reports. They do not change GitHub review state or claim a project maintainer has cleared a review.

## Validation and limits

On the local implementation before the required latest-base review, the following CGo checks passed:

| Command | Result |
|---|---|
| `.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 ./pkg/sql/plan/function` | Pass, 14.622s |
| `.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 ./pkg/sql/plan` | Pass on the final pre-rebase source, 25.793s |
| `.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 ./pkg/container/arrowbridge` | Pass, 0.943s |
| Scoped SQL regressions (command below) | Pass on the final pre-rebase source, 15.667s |

```sh
.agents/skills/mo-dev/scripts/mo-cgo-test -run '^(TestIssue29723MediumIntSemantics|TestIssue29723MediumIntAdjacentContracts|TestIssue25103InformationSchemaMetadata)$' -count=1 ./pkg/tests/issues
```

`gofmt -d` and `git diff --check` are clean. Repository-pinned golangci-lint v2.14.0 reported `0 issues` for the changed Go packages; after the final query-builder metadata adjustment, the planner package was checked again and also reported `0 issues`.

The issue tests use the embedded MatrixOne cluster and MySQL client protocol. This follow-up did not run a full `mo-tester`/DATALINK suite, a complete Kafka/Flink CDC pipeline, or a new throughput benchmark. Revision 1 records the storage, protocol, rollout, and performance limits for the broader MEDIUMINT implementation. Historical width-24 rows are not rewritten: reads preserve their existing 32-bit values, widening to ordinary INT preserves those values, and future assignments to MEDIUMINT continue to check the logical domain.
