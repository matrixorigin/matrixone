# COPY ALTER replacement — design v1

**Base:** `a7bc2008e9236708fee0328376f2c2c6b6f0d3fc`; its tree equals the parent of reverted PR #32253. Implement this as one focused PR. The design exceeds five production owners, so this is the versioned document for the caller to commit before implementation.

The central invariant is: **every replacement index has one backfill owner, and cloning is allowed only when its stored keys, included values, and row locators retain their identity and encoding.** Deduplication is a separate proof; it never grants permission to leave an index empty.

## Index decision and ownership

In `pkg/sql/plan/build_alter_table.go`, accumulate PK impact across *all* clauses. Recognize every logical member of a composite PK, including changes followed by unrelated clauses. PK addition, removal, or affected member invalidates every secondary locator; preserve fake PKs only when their locators remain valid.

Compute one `SkipIndexesCopy[indexName]` map after all clauses. A true entry means the logical index survives by hidden-table identity and its keys, prefixes, included columns, generated values, and locator are proven unchanged. An unknown or replaced index is not cloneable. Group multiple physical definitions by logical index and use the existing plugin dependency hooks. Do not infer affected columns from index names.

Use that map in all consumers:

- Clone-eligible regular, unique, and master indexes are populated by `cloneUnaffectedIndexes`; affected ones are populated by the COPY INSERT. Preserve nullable-unique filtering.
- For synchronous plugin indexes, both modern `pkg/sql/plan/bind_insert.go` and legacy `pkg/sql/plan/build_dml_util.go` suppress COPY INSERT maintenance. The compiler owns the single post-copy clone or rebuild, including all physical tables of a logical index.
- Honor each plugin’s existing clone behavior, including clearing seeded tables and CDC-owned portions. A missing expected source/target hidden-table match must fail rather than silently produce an empty index.

`cloneUnaffectedIndexes` has one production caller. Remove the redundant `AffectedCols` producer and consumers; reserve its protobuf tag/name and regenerate `plan.pb.go`. Do not retain a second inference path.

Pass every non-nil COPY option, including the all-affected, no-skip case. In the existing statement-option/context boundary, carry an explicit table-COPY purpose plus its database; reuse `AlterCopyOpt.TargetTableName` for the generated target name. Require the qualified target to match before applying COPY behavior. `CREATE UNIQUE INDEX` continues to use `AlterCopyOpt` for its existing dedup optimization, without acquiring table-COPY privileges. Restore statement context after execution.

In legacy INSERT, remove the merge of `SkipUniqueIdxDedup` into the map that suppresses hidden-index population. For the matching COPY target, bypass the omitted-column shortcut and clear `ifInsertFromUniqueColMap`: source uniqueness does not prove uniqueness after conversion or for a member of a composite key. Retain target checks whenever proof is absent.

## Values, schema, and constraints

Restore the original patch’s source-lineage transitions in the ALTER planner: ADD records its default or missing source; DROP removes the old lineage; RENAME and CHANGE move the existing expression; MODIFY preserves it. Guard intermediate nil/fake PK states and preserve typed errors for repeated DROP.

Static PK/unique dedup may be skipped only when complete key definitions and value-relevant types match and the actual COPY projection proves the original values survive. Resolve aliases and include prefix parameters. For the compiler’s source precheck, additionally require unambiguous old column IDs, matching sequence numbers, and `ChangeTblColIdMap` pointing to the final target. ID zero is valid; the map’s target `ColId` is not yet populated. Reject generated or otherwise unproven inputs. Run NULL and duplicate prechecks on the same locked snapshot as COPY; decline precheck for FLOAT/DOUBLE or other types whose grouping equality is not proven equivalent to physical key identity. Only enable pipeline flush after that proof or an independently safe static skip.

Restore generated-value preservation with the original dependency proof: unchanged materialized values are copied; unproven values are recomputed. Recomputed generated-key indexes are affected, and a recomputed generated PK affects every locator. Retain the incoming/self-FK guard and allow supported same-name dependency CHANGE operations. Preserve raw ENUM ordinals and SET bitmaps only when source and target definitions match.

For AUTO_INCREMENT, project NULL for a newly added synthetic auto value, but preserve existing zero values regardless of SQL mode. Restore the typed preinsert flag through construction, operator duplication, and pipeline protobuf encoding/decoding; keep normal NULL allocation and allocator reconciliation.

For index metadata: shared RENAME/CHANGE handling preserves implicit PK aliases, prefix encodings, other parameters, and nested `session_vars`. Emit quoted catalog updates for INPLACE rename even in FK-banned databases, gating FK-only SQL at its producers. Merge visibility actions without restoring stale parameters. For COPY, restore visibility and captured session settings only for surviving indexes identified by their source hidden tables, updating both constraints and `mo_indexes` before backfill. Use `sqlquote` for internal identifiers and catalog literals.

The remaining owners are `pkg/sql/compile/{alter,ddl,operator,remoterun,sql_executor}.go`, `pkg/util/executor`, `pkg/defines`, `pkg/sql/colexec/preinsert`, and the plan/pipeline protobufs. Restore schema-version validation and RC retry-with-definition-change, and retain a parent’s child reference until its final FK is removed, including self-references.


# COPY ALTER replacement — design v2

This replaces v1’s async rejection. The accepted source-lineage, deduplication, metadata, and single-index-owner design remains in force. No ALTER combination is newly rejected solely because an affected index is asynchronous.

## Clone eligibility

In `pkg/sql/plan/build_alter_table.go`, calculate locator impact from the **final schema and actual COPY projection**, after all clauses. A locator is unchanged only if its complete ordered PK maps to the same source columns by valid, unambiguous `ColID`/`Seqnum` and lineage; copied values are preserved; and their physical key types and encodings are equivalent. `Typ.Table` is not physical equality. Apply the corresponding proof to indexed parts, prefixes, included values, generated values, and surviving index identity.

Thus `MODIFY id BIGINT NOT NULL, ADD extra` may clone when `id` and its encoding are unchanged. A real PK conversion, replacement, reordered composite key, or recomputed generated PK invalidates every secondary locator. This preserves the issue’s required rebuilds without rebuilding indexes for a proven no-op.

HNSW’s catalog requires a single BIGINT PK, so the composite-PK trigger cannot directly create a valid HNSW case. The no-op proof removes the obvious new single-PK HNSW rebuild. It does **not** prove all newly affected HNSW cases unreachable: a changed generated PK can require rebuilding one.

Keep `SkipIndexesCopy` as the sole per-logical-index decision. COPY INSERT maintains affected regular, unique, and master indexes; the compiler clones eligible indexes. For plugins, both INSERT builders suppress COPY maintenance and the compiler owns the single clone or rebuild. Scope all options to the exact generated COPY target, including the all-affected case; keep CREATE UNIQUE INDEX’s separate use of `AlterCopyOpt`.

## Snapshot-owner correction

For an affected plugin, retain the existing compiler/plugin creation and CDC scheduling contracts. Correct snapshot replacement at the existing consumer owners; do not force unrelated async indexes to rebuild.

- **HNSW:** In `pkg/iscp/index_consumer.go` and `pkg/vectorindex/hnsw/sync.go`, give an initial complete snapshot an explicit *replacement* mode. Build from empty models, not the persisted graph. In the output transaction, establish the old maximum generation floor, delete old metadata and model rows, save the new models, and publish snapshot completion through `DataRetriever.UpdateWatermark`. The empty-source case still deletes old output and publishes completion. Normal CDC tail updates continue to load existing models.
- **SQL FULLTEXT and IVF:** In the existing snapshot consumer/writer, clear only that index’s derived output in bounded batches before inserting a complete snapshot. IVF retains its model/metadata tables; reset entries. Each retry starts with another reset, so rows absent from a newer snapshot cannot survive a failed earlier attempt.
- **FULLTEXT2 and cuVS:** Their existing single-transaction snapshot persist owners replace the complete old base/tail or model output and publish snapshot progress in that same transaction. Keep algorithm dispatch in the plugin framework.

For every independently committed reset or output batch, validate the exact active job and target hidden-table identities in the same transaction, using the existing durable job/task ownership and expected snapshot progress. The job-row write must conflict with retirement or replacement. Snapshot completion must be published once by the output owner; the outer iteration must not separately mark it successful. Verify that the existing task lease actually excludes overlapping writers through runner migration; if it cannot, close that ownership gap in the existing job owner before delivery.

A failure before commit leaves progress unadvanced; a retry replaces output again. On an ambiguous commit, reread durable status before retrying. Preserve the original typed work or commit error over cleanup failures; report cleanup failures without losing ownership of submitted work. Pass the caller’s deadline and cancellation through all work—do not extend it with an unconstrained context. Cancellation after a confirmed commit cannot turn that committed outcome into a rollback.

## Unchanged async and partition defects

Pending IVF initialization lost during an **unrelated clone-eligible COPY** is confirmed on both the parent and merged trees. Keep that clone path unchanged in this PR and document the limitation. For an *affected* IVF rebuild, use the existing `HandleCreateIndex`/InitSQL path; initialization must determine training versus placeholder from the replacement source when the existing initializer runs, and publish any model-version change with the model transaction. Do not invent a persistent InitSQL format.

Do not change ALTER algorithm selection. The existing partition COPY child-index loss remains separate; the child-ID failure was exposed by the rejected approach of routing renames through COPY. Neither is claimed fixed here.

## Acceptance

Reuse E185/E186 for the baseline and FULLTEXT regression, E87/E88 for HNSW replay, and E116 for partial IVF snapshot retry. They are historical evidence, not a pass for this design. Before delivery:

- Verify both reported PK cases with default/forced/ignored queries, bidirectional raw locator equality, existing prepared statements, and subsequent INSERT/UPDATE/DELETE. Include a same-type no-op clone control and generated-PK invalidation.
- Verify synchronous FULLTEXT postings, doc lengths, and score exactly once through both INSERT builders; retain cluster unique-index, conversion-collision, source-lineage, generated/FK, ENUM/SET, AUTO_INCREMENT zero, prefix/alias, and rollback cases from v1.
- Exercise affected HNSW snapshots with copied rows plus post-ALTER writes, empty input, deleted rows after a failed attempt, failure before output/status commit, ambiguous commit, cancellation, and runner handoff. Test SQL snapshot retries after a committed partial batch and FULLTEXT2/cuVS replacement at their owners. Verify source snapshot boundary behavior using the existing `CollectChanges` contract.
- Compare unaffected async and partition controls against the baseline. Any new corruption or loss blocks delivery. Run the affected owner-package build, vet, incremental lint, race tests, targeted SQL suites, protobuf checks, and final `gpt-6.1-terra`/max review.

This is the versioned design for the caller to commit. No files were edited and no tests or services were run in this review.
## Review record

Owner issue: #32056. Replaces #32253 after #32483. Design reviewed before implementation using `gpt-6.1-terra` with `max` reasoning, session `01a1c58d-92a8-7373-a2ce-2df83fa80fe4`. Implementation and final review will be recorded in the replacement PR.
