# P1 evidence: DEFAULT input-type legality (PR #28935)

**Verdict: LEGAL-DEFECT** — the alleged DEFAULT input-type issue is real.
Minimal fix + regression tests are on local branch `codex/p1-default-type-28935`.

- Base SHA (pinned PR head): `ea52b154190e2271915d0bfe00c9694f0c250abd`
- Worktree: `/private/tmp/mo-p1-28935-default-20261013-162340`
- Scope: DEFAULT input-type trigger path only. No hybrid CN / mixed-env suite.

## Trigger path (legal)

A pre-change catalog can store a column-reference DEFAULT bound to CRC32
overload 0 with a JSON-typed ColRef:

```sql
CREATE TABLE t(j JSON, c BIGINT UNSIGNED DEFAULT (CRC32(j)));
-- upgrade to this PR
ALTER TABLE t MODIFY COLUMN j BIGINT;
INSERT INTO t(j) VALUES (1);  -- evaluates the retained DEFAULT
```

Evidence chain before the fix:

1. `pkg/sql/plan/build_util.go` (`buildDefaultExprWithColumns`) short-circuited
   on `preserveCRC32Default(source.Default)` and copied `source.Default`
   wholesale — no referenced-input compatibility check.
2. `pkg/sql/plan/crc32_catalog_compat.go` (`validateLegacyCRC32GeneratedInputs`)
   guarded only `GeneratedCol` owners, not DEFAULT/ON UPDATE.
3. `pkg/sql/plan/build_alter_change_column.go` final loop likewise walked only
   `col.GetGeneratedCol().GetExpr()`.
4. `validateDefaultColumnDependencies` checks dependency legality (cycle,
   generated refs, auto-incr), not ColRef type identity.
5. Default expansion retains the function identity
   (`build_util.go` default expansion / `getDefaultExprForAssignment`);
   `pkg/sql/colexec/evalExpression.go` returns the actual row vector; CRC32
   overload 0 reads it as `types.Varlena` (`function/baseTemplate.go`
   `OptGetBytesParamFromWrapper` / `GenerateFunctionStrParameter`).
   A converted BIGINT input can therefore produce a wrong checksum or fail at
   runtime instead of hashing its SQL string representation.

Related review finding: PR review `5495340612`
`[P1] Legacy CRC32 DEFAULT preservation bypasses input-type compatibility`.

## Fix (minimal)

1. `crc32_catalog_compat.go`: generalized
   `validateLegacyCRC32GeneratedInputs` → `validateLegacyCRC32ExprInputs`
   with a subject string; generated-column callers keep their old subject.
2. `build_util.go` (`buildDefaultExprWithColumns`): retained legacy CRC32
   DEFAULT now runs `validateLegacyCRC32ExprInputs(..., "default expression")`
   against the final `columns` slice before publication.
3. `build_alter_change_column.go`: the post-attribute COPY guard now walks
   generated, DEFAULT and ON UPDATE owners of every column.

Converted inputs are rejected with
`changing a legacy CRC32 JSON default expression requires an explicit table rebuild`.
Unchanged JSON inputs and folded literal defaults keep their existing identity.

## Tests

New file `pkg/sql/plan/crc32_default_input_type_test.go`:

| Test | Claim |
|---|---|
| `TestCRC32DefaultPreserveRejectsConvertedInput` | preserve path rejects BIGINT/VARCHAR/UINT64 conversions of the hashed input |
| `TestCRC32DefaultPreserveKeepsMatchingInput` | matching JSON input keeps overload-0 identity |
| `TestCRC32DefaultAlterRejectsConvertedInput` | MODIFY/CHANGE of a default input is rejected even when the default column is not in the ALTER list |
| `TestCRC32UnchangedDefaultClauseRejectsConvertedSiblingInput` | same-name repeated default clause still rejects a converted sibling input |
| `TestCRC32DefaultCopyRebuildRejectsConvertedInput` | COPY rebuild through build_ddl per-column binding rejects JSON→BIGINT |

Command (Darwin/arm64, CGo wrapper, isolated GOCACHE/tmp):

```text
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=300s \
  -run 'TestCRC32|TestLegacyCRC32' ./pkg/sql/plan/
# PASS — all CRC32 planner roots including the five new ones
.agents/skills/mo-dev/scripts/mo-cgo-test -count=1 -timeout=180s \
  -run 'TestCRC32' ./pkg/sql/plan/function/
# PASS
```

Existing CRC32 DEFAULT/generated/CHECK/AUTO_INCREMENT regressions remain green
(`TestCRC32CopyPreservesFoldedDefaultWithoutSource`,
`TestCRC32UnchangedDefaultClauseRetainsIdentity`,
`TestCRC32FoldedDefaultRetainsCapability`,
`TestCRC32LegacyGeneratedAttributeValidation`,
`TestCRC32CopyGeneratedAttributeValidation`,
`TestLegacyCRC32CopyRejectsAutoIncrementInput`, …).

## Residual risks / open questions

- `buildOnUpdate` preserve path still copies a retained legacy CRC32 ON UPDATE
  without an input-type check when called outside the ALTER final loop
  (`build_ddl.go` CREATE/COPY rebuild). The ALTER guard in
  `buildColumnAndConstraint` covers ON UPDATE owners of existing columns.
- No live-server INSERT using the default was executed here (focused unit +
  planner only, per mission). The COPY-rebuild and preserve-path tests prove
  the bad metadata cannot be published.
- Full BVT / mixed-CN / cross-version upgrade not re-run for this delta.

## BVT decision (publication gate)

**BVT: N/A** for this delta.

- Production path that exposes the defect is a **legacy CRC0 catalog DEFAULT** whose
  JSON input is later type-converted. Fresh DDL binds new CRC32 semantics
  (overload 1); existing `func_crc32.sql` already states "Legacy CRC0 catalog
  identity is covered by the planner regression, not manufactured by this BVT."
- Equivalent regression: `pkg/sql/plan/crc32_default_input_type_test.go`
  (5 roots) — `TestCRC32DefaultPreserveRejectsConvertedInput`,
  `TestCRC32DefaultAlterRejectsConvertedInput`, and siblings; asserted terminal
  is `not supported: changing a legacy CRC32 JSON default expression requires an
  explicit table rebuild` (or the matching subject for ON UPDATE / COPY rebuild),
  with catalog left unchanged on rejection.

## buildOnUpdate preserve gap (follow-up)

`buildOnUpdate` retained a legacy CRC32 ON UPDATE by wholesale copy without
`validateLegacyCRC32ExprInputs`, the same defect class as DEFAULT. Fixed
symmetrically: preserve branch now type-checks against the column scope
(`on-update expression`), and CREATE/COPY (`build_ddl` `allColDefs`), ALTER
(`defaultScope`), ADD COLUMN (`scopeCols`) and table-dump (`def.Cols`) all pass
that scope. Regression: `TestCRC32OnUpdatePreserveRejectsConvertedInput`,
`TestCRC32OnUpdatePreserveKeepsMatchingInput`,
`TestCRC32OnUpdateUnchangedClauseRejectsConvertedSiblingInput`.
