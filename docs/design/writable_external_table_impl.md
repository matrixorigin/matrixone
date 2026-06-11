# Writable External Table — Design Spec & Implementation Plan

This document refines the requirements in
[`writable_external_table.md`](./writable_external_table.md) into a concrete
design spec, then lays out a phased implementation plan with the exact code
locations to touch.

---

## 0. Implementation status (as-built)

Implemented and verified end-to-end (BVT `test/distributed/cases/stage/writable_external_table.sql`
passes 37/37; unit tests in `pkg/sql/colexec/externalwrite`). Key decisions that
firmed up or differed from the original plan below:

- **No proto change.** The write config is carried on the Go `insert.InsertCtx`
  struct (`ExternalConfig externalwrite.WriterConfig`) + `Insert.ToExternal`,
  populated at compile time from `TableDef.Createsql`. The plan `InsertCtx`
  (proto) is untouched.
- **Operator: Option A** — extended the existing `insert` operator with a third
  mode (`ToExternal` → `insert_external`), alongside `ToWriteS3`/`insert_table`.
- **Modern vs legacy binder.** INSERT/LOAD now go through the *modern* binder
  (`bindInsert`/`bindLoad`); external tables are diverted to the *legacy*
  `buildInsert`/`buildLoad` by having `DMLContext.ResolveSingleTable` return
  `ErrUnsupportedDML` for any external target (triggers the existing fallback).
  The external-write plan + writer live entirely in the legacy path.
- **`checkTableType(ctx, tableDef, op)`** gained an `op` param: a writable
  external table (has `WRITE_FILE_PATTERN`) is allowed for `op=="insert"`;
  read-only externals and all other ops still error.
- **`initInsertStmt` Pkey guard.** External tables have no primary key (not even
  a fake hidden one), so the `tableDef.Pkey.Names` loop was nil-guarded.
- **Hidden columns.** The resolved external `TableDef` carries a synthetic
  `__mo_filepath` column; it (and any hidden/Row_ID column) is excluded from the
  writer's `Attrs`, and the encoder only emits `len(Attrs)` leading columns.
- **Format/options.** `format`, `write_file_pattern`, etc. live in
  `ExternParam.Option[]` (not the typed fields) in the stored `Createsql`, so the
  compile-time writer config reads them from `Option`. `write_file_pattern` was
  added as an allowed key in `build_ddl.go` *and* as a no-op in the three
  read-side option validators in `utils.go`.
- **Const vectors.** The encoder is const/const-null aware (`cellIsNull`), since
  the insert batch may contain constant vectors.
- **Files & paths.** Output is streamed via `fileservice.NewFileServiceWriter`
  (io.Pipe, `Size=-1`); the stage path is resolved with
  `stageutil.UrlToStageDef(...).ToPath()`. `LocalFS.Write` creates parent dirs.

---

## 1. Goals & Non-Goals

### Goals
- Allow `INSERT INTO ext_tbl SELECT ...` and `LOAD DATA ... INTO ext_tbl` where
  `ext_tbl` is an external table.
- Writes go through the normal planner/optimizer; only the final write step is
  diverted from "write to a MatrixOne relation" to "encode rows and append to a
  file in a stage".
- The write API is **batch-oriented** (one call writes many rows).
- Large `INSERT`/`LOAD` run on multiple CNs in parallel; each parallel pipeline
  writes **exactly one** output file.
- Supported output formats: **CSV** and **JSONLine**, to a **`stage://`**
  destination only.

### Non-Goals (explicitly out of scope for this change)
- `UPDATE` / `DELETE` on external tables (added later).
- Transactional/atomic write semantics across a statement (see §3.6).
- Parquet write output (read-only for now).
- S3 endpoints reached by raw `s3://` options instead of a named stage.

---

## 2. Design Spec

### 2.1 New table option: `WRITE_FILE_PATTERN`

An external table becomes **writable** iff it was created with an extra option:

```sql
CREATE EXTERNAL TABLE t (...)
  INFILE{...}                 -- or the usual read config
  ... FORMAT='csv'
  WRITE_FILE_PATTERN='stage://mystage/dt=%Y-%m-%d/part-%U.csv';
```

Rules:
- `WRITE_FILE_PATTERN` is optional. Without it the table stays **read-only**;
  any write attempt errors out (`NewNotSupported`).
- Its value **must** resolve to a writable `stage://...` path. Non-stage paths
  are rejected at DDL time.
- The pattern is a `strftime(3)` format string evaluated at write time
  (statement start timestamp), with two MatrixOne extensions:
  - `%nN` → replaced by `n` random decimal digits (e.g. `%6N` → `"492013"`).
  - `%U` → replaced by a freshly generated UUID.
- The file extension / format is governed by the table's existing `FORMAT`
  option (`csv` or `jsonline`). The pattern's literal extension is cosmetic.

### 2.2 One file per pipeline

When a write runs in parallel across `K` pipelines (possibly on multiple CNs),
each pipeline instance:
- expands `WRITE_FILE_PATTERN` **independently**, and
- writes a single file.

Uniqueness is the user's responsibility via the pattern. The recommended
pattern includes `%U` or `%nN` so concurrent writers never collide. To make
collisions effectively impossible even without `%U`/`%nN`, the expander also
mixes a per-pipeline writer id into the random/UUID sources (see §4.2). Per the
upstream spec, writers are assumed to not race with each other.

### 2.3 Batch API

Rows are handed to the writer as `*batch.Batch` (the unit already flowing
through the execution pipeline). The writer encodes the whole batch and appends
to its file. No per-row API.

### 2.4 Supported formats

| FORMAT     | Encoder                                  |
|------------|------------------------------------------|
| `csv`      | reuse the row→CSV byte logic from export |
| `jsonline` | reuse the row→JSONLine logic from export |

Field/line terminators, enclosure and escaping come from the table's
`TailParameter` (`FIELDS`/`LINES`), defaulting to the same defaults as
`SELECT ... INTO OUTFILE`.

### 2.5 Empty result → no file

A pipeline that receives zero rows creates **no** file (lazy file open on first
non-empty batch). This avoids littering stages with empty parts.

### 2.6 Consistency / failure semantics (documented limitation)

External writes are **not** transactional. Files are streamed to the stage and
finalized when the pipeline closes. If the statement aborts after some pipelines
have finalized files, those files remain. This matches the spec's "assume the
writer will be able to write without race condition" stance and is acceptable
for v1. A future improvement could write to a temp prefix and rename-on-commit.

---

## 3. Architecture & Data Flow

```
INSERT INTO ext SELECT ...                 LOAD DATA ... INTO ext
        │                                          │
   build_insert.go                            build_load.go
        │  (detect external + writable)            │  (detect external target)
        ▼                                          ▼
   Simplified plan:  <source pipeline> ──► Node_INSERT{ external write ctx }
        │                                          │
   compile.go (compileInsert): parallelize, one writer op per pipeline
        ▼
   colexec/external_write operator
        │  WriteBatch(batch)            (per pipeline → one ExternalWriter)
        ▼
   ExternalWriter (csv | jsonline)
        │  encode batch → bytes → io.Pipe
        ▼
   fileservice.Write(stage-resolved path)   (LocalFS / S3FS, atomic per file)
```

Key idea: **reuse the existing INSERT plan node and the existing parallel-insert
compilation**, but mark the node as an "external write" so the executor
constructs an `ExternalWriter` instead of obtaining an engine `Relation`.
External tables have no indexes, no PK/FK, no auto-increment, so the planner
skips all the constraint/hidden-table machinery and emits a minimal pipeline.

---

## 4. Implementation Plan (phased)

### Phase 1 — DDL: accept & persist `WRITE_FILE_PATTERN`

**Files:**
- `pkg/sql/plan/build_ddl.go:923-929` — add `write_file_pattern` to the
  allowed external-option keys (the `switch` that currently lists
  `endpoint, region, ... hive_partition_columns`).
- Validate at DDL time:
  - value must start with `stage://` (reuse the prefix check used in
    `InitInfileOrStageParam`, `pkg/sql/plan/utils.go:2127`).
  - `FORMAT` must be `csv` or `jsonline` if a write pattern is present
    (Parquet write unsupported).
  - the `strftime` pattern must parse (dry-run the expander from Phase 2 with a
    fixed timestamp; reject unknown `%` directives early).
- Persistence needs **no schema change**: `ExternParam.Option []string`
  (`pkg/sql/parsers/tree/update.go:229`) already stores arbitrary key/value
  pairs (even index = key, odd = value) and the whole struct is JSON-marshaled
  into `catalog.SystemRelAttr_CreateSQL` at `build_ddl.go:938-949`.

**Accessor:** add a helper `GetWriteFilePattern(param *tree.ExternParam) (string, bool)`
near the other option readers in `pkg/sql/plan/utils.go` that scans
`param.Option` for the `write_file_pattern` key.

**Parser:** no grammar change — `WRITE_FILE_PATTERN='...'` is already accepted as
a generic external option key/value. Confirm with a parser test; if the lexer
does not pass arbitrary identifiers through the external-option list, add the
keyword to the option production in `pkg/sql/parsers/dialect/mysql/`.

### Phase 2 — Writer API, format encoders, strftime expander

New package: `pkg/sql/colexec/externalwrite/` (or `pkg/extwriter/`).

**2a. Pattern expander** — `expand.go`
```go
// ExpandFilePattern expands a strftime pattern with MO extensions:
//   %nN -> n random decimal digits ; %U -> a UUID
// `t` is the statement timestamp; `salt` distinguishes parallel writers.
func ExpandFilePattern(pattern string, t time.Time, salt uint64) (string, error)
```
- No strftime lib is vendored (verified), so implement a small directive mapper
  for the common `%Y %m %d %H %M %S %j %p ...` set over Go's `time` package,
  then handle `%nN` (read optional digit count, emit random digits) and `%U`
  (`uuid.NewV7` via `pkg/util` — see `pkg/util/uuid.go:44` `FastUuid`, and
  `pkg/objectio/id.go:43` for the `google/uuid` usage pattern).
- Randomness: do **not** use `Math.random`-style global state in a way that
  breaks determinism of tests; seed from `salt` + a crypto rand source.

**2b. Writer interface** — `writer.go`
```go
type ExternalWriter interface {
    // WriteBatch encodes all rows of bat and appends to the output file.
    // The file is created lazily on the first non-empty batch.
    WriteBatch(ctx context.Context, bat *batch.Batch) error
    // Close flushes and finalizes the file. No-op if no rows were written.
    Close(ctx context.Context) (rowsWritten uint64, err error)
}

type WriterConfig struct {
    Pattern   string            // WRITE_FILE_PATTERN
    Format    string            // "csv" | "jsonline"
    Tail      *tree.TailParameter
    Attrs     []string          // column names (for jsonline keys / csv header)
    Types     []types.Type
    Stmt      time.Time
    WriterID  uint64            // per-pipeline salt
}

func NewExternalWriter(proc *process.Process, cfg WriterConfig) ExternalWriter
```

**2c. CSV / JSONLine encoders** — `csv.go`, `jsonline.go`
- Reuse the proven row→bytes conversion from
  `pkg/frontend/export.go`:
  - CSV: `constructByte()` (export.go:380-589), `formatOutputString()`
    (export.go:249-268), `addEscapeToString()` (export.go:591).
  - JSONLine: `constructJSONLine()` (export.go:1099) and
    `vectorValueToJSON()` (export.go:1169).
  - Refactor the per-type vector→string/JSON logic out of `frontend` into a
    shared helper the new writer can call, OR copy it. **Recommendation:**
    extract the type-switch into a small reusable function
    (`pkg/frontend` already owns it; factor into `pkg/common/exportcodec` so
    both frontend export and external write share one copy and stay in sync).

**2d. File sink** — `writer.go`
- Resolve the expanded path: `stageutil.UrlToStageDefForExport(...)`
  (`pkg/stage/stageutil/stageutil.go:178`) → `StageDef.ToPath()`
  (`pkg/stage/stage.go:73`) → `fileservice.GetForETL()`
  (`pkg/fileservice/get.go:77`). Use the *ForExport* variant so any `%`-derived
  literals in the final path are not re-interpreted.
- Stream with the `io.Pipe` pattern used by export
  (`pkg/frontend/export.go:140-229`): set `IOEntry.ReaderForWrite` and
  `IOEntry.Size = -1` and call `fs.Write(ctx, IOVector{...})` in a background
  goroutine; the encoder writes into the pipe. This streams arbitrarily large
  output without buffering the whole file.
- File created on first non-empty batch (Phase §2.5). Optional CSV header from
  `Attrs` if the table is configured with a header.

### Phase 3 — Planner: detect external write & build minimal plan

**Files:** `pkg/sql/plan/build_insert.go:33`, `pkg/sql/plan/build_dml_util.go`.

- In `buildInsert` (build_insert.go:49, after `ctx.Resolve`), detect
  `t.TableType == catalog.SystemExternalRel`:
  - If the table has **no** `WRITE_FILE_PATTERN` →
    `moerr.NewNotSupported(ctx, "insert into read-only external table %s")`.
  - Else build a **simplified** insert plan: bind the `SELECT`/VALUES source,
    project to the table's columns, and append a single `Node_INSERT` whose
    `InsertCtx` carries an *external write* marker + the resolved write config
    (pattern, format, tail, attrs, types). Skip `appendPreInsertNode`,
    constraint checks, index/hidden-table fan-out
    (`buildInsertPlansWithRelatedHiddenTable`, build_dml_util.go:898) — external
    tables have none of these.
- Plumb the external-write config into the plan. Two options:
  1. Add fields to `plan.InsertCtx` (proto `pkg/pb/plan` → `plan.proto`
     `InsertCtx`): `bool is_external`, `string write_file_pattern`,
     `string format`, plus reuse existing tail/column info. **Preferred.**
  2. Re-derive from `TableDef` properties (`Createsql` JSON) in the executor
     like the read path does (`pkg/sql/compile/compile.go:1556` `getExternParam`).
     Less proto churn but more executor work. Option 1 is cleaner; pick it.
- `LOAD` planning lives in `build_load.go:473` and reuses `buildInsertPlans`;
  see Phase 6.

### Phase 4 — Execution operator

**Option A (recommended): extend the existing `insert` operator** at
`pkg/sql/colexec/insert/` with a third write mode.

- `pkg/sql/colexec/insert/types.go:49` `Insert` struct: add
  `ToExternal bool` and an `extWriter externalwrite.ExternalWriter` to
  `container` (types.go:36).
- `pkg/sql/colexec/insert/types.go:90` `InsertCtx`: add the external write
  config (pattern/format/tail/attrs/types) mirrored from the plan.
- `Prepare` (insert.go:116): when `ToExternal`, construct the
  `ExternalWriter` with a `WriterID` derived from the parallel index (so each
  pipeline instance gets a distinct salt) instead of getting an engine
  `Relation`.
- `Call` (insert.go:180): route `ToExternal` to a new
  `insert_external(proc, analyzer)` (parallel to `insert_s3`/`insert_table`).
- `insert_external`: pull child batch (like `insert_table`, insert.go:417-462),
  call `extWriter.WriteBatch(ctx, bat)`, accumulate affected rows. On the final
  call / `Reset`/`Free` (types.go:99-141), call `extWriter.Close` to finalize
  the file and add its row count.

**Option B:** a brand-new `colexec/externalwrite` operator. Cleaner separation
but duplicates the operator plumbing (reuse pool, analyzer, children-call).
Given the insert operator already multiplexes `ToWriteS3` vs `insert_table`,
Option A is lower-risk and consistent with the codebase. **Go with A.**

### Phase 5 — Compile & parallelism

**File:** `pkg/sql/compile/compile.go` `compileInsert` (≈ compile.go:4105).

- When the insert node is external:
  - Build the source scopes as usual.
  - Create **one external-write insert operator per pipeline** (per CN core),
    each with a unique `WriterID`. Reuse the existing parallel-insert path that
    already duplicates the insert operator across scopes.
  - **Do not** add the S3 mergeBlock/dispatch shuffle used for normal tables —
    each writer is independent and self-contained (writes its own file). A
    trailing merge only needs to sum affected-row counts.
- Multi-CN: the same scope-distribution mechanism that spreads `LOAD`/`INSERT`
  across CNs (see `compile.go:4152` shuffle handling and
  `colexec/dispatch`) carries the external-write operators to remote CNs. The
  remote-run encoding (`pipeline.Insert` in `proto/pipeline.proto`) carries
  `to_external` plus the statement-start timestamp; the receiving CN rebuilds
  the writer config from the serialized `TableDef`'s stored ExternParam
  (`buildExternalInsertArg` in `pkg/sql/compile/operator.go`), so every CN
  expands `WRITE_FILE_PATTERN` time directives against the same instant. Since
  each writer is independent, no further cross-CN coordination is required —
  exactly the spec's assumption.
- `WriterID` must be **globally unique across CNs**: derive it from
  `(CN index/uuid, pipeline index)` so two CNs never expand to the same salt.

### Phase 6 — LOAD path

**File:** `pkg/sql/plan/build_load.go:473` `buildLoad`.

- `LOAD` already builds `EXTERNAL_SCAN(source file) → PROJECT → ... → INSERT`.
  When the **target** table is an external writable table, reuse Phase 3:
  the terminal `Node_INSERT` is marked external-write. The source side
  (reading the LOAD file) is unchanged.
- Keep the existing parallel-LOAD behavior (build_load.go:600-637 sets
  `Shuffle=true`); under external write, the shuffle is unnecessary — drop it
  for external targets so each scan pipeline writes its own file directly
  (one-file-per-pipeline). If shuffle is left on, it still works but adds a
  pointless redistribution; prefer dropping it for external targets.
- The frontend LOAD entry (`pkg/frontend`) needs no change beyond letting an
  external target through (it currently routes LOAD into the planner).

### Phase 7 — Tests

- **Unit tests**
  - `externalwrite/expand_test.go`: `%Y/%m/%d`, `%nN` (length + digit-only),
    `%U` (valid UUID, uniqueness across salts), unknown-directive error.
  - `externalwrite/writer_test.go`: CSV & JSONLine encoding of all common
    types, NULL handling, empty-batch → no file, large multi-batch streaming.
  - Planner test: INSERT into read-only external table errors; INSERT into
    writable external table produces the minimal plan.
- **BVT** (`test/distributed/cases/external/` — follow CLAUDE.md workflow):
  - `CREATE EXTERNAL TABLE ... WRITE_FILE_PATTERN=stage://...` over a `file://`
    stage (local fs, deterministic in CI).
  - `INSERT INTO ext SELECT ...`, then read it back via the same external table
    (read path) and assert row equality.
  - `LOAD DATA ... INTO ext`, read back.
  - jsonline format case.
  - Use a `%nN`/`%U`-free fixed pattern for the read-back assertion, or list the
    stage dir. Generate expected results with `mo-tester -m genrs`.
  - Multi-CN parallel case (large insert) → assert N files created, total rows
    correct.

---

## 5. Touch-point cheat sheet

| Concern | Location |
|---|---|
| Allowed ext options | `pkg/sql/plan/build_ddl.go:923-929` |
| Ext option persistence | `pkg/sql/plan/build_ddl.go:938-958` |
| ExternParam struct | `pkg/sql/parsers/tree/update.go:215-237` |
| Stage URL → fs path | `pkg/stage/stageutil/stageutil.go:178` (`...ForExport`), `pkg/stage/stage.go:73` (`ToPath`) |
| Get ETL fileservice | `pkg/fileservice/get.go:77` (`GetForETL`) |
| FileService Write API | `pkg/fileservice/file_service.go` (`Write`, `IOVector`, `IOEntry`) |
| Streaming write pattern | `pkg/frontend/export.go:140-229` (`io.Pipe` + `ReaderForWrite`, Size=-1) |
| CSV row encode (reuse) | `pkg/frontend/export.go:380-597` |
| JSONLine row encode (reuse) | `pkg/frontend/export.go:1099-1264` |
| UUID generation | `pkg/util/uuid.go:44` (`FastUuid`), `pkg/objectio/id.go:43` |
| INSERT planning entry | `pkg/sql/plan/build_insert.go:33` |
| INSERT plan node / InsertCtx | proto `plan.proto` `InsertCtx`; `pkg/sql/colexec/insert/types.go:90` |
| INSERT operator (extend) | `pkg/sql/colexec/insert/insert.go:116` (Prepare), `:180` (Call), `:417` (insert_table) |
| INSERT compile / parallel | `pkg/sql/compile/compile.go` `compileInsert` (~`:4105`, shuffle `~:4152`) |
| LOAD planning | `pkg/sql/plan/build_load.go:473`, parallel `:600-637` |
| Read-side ext param decode (reference) | `pkg/sql/compile/compile.go:1556` (`getExternParam`) |

---

## 6. Open questions / risks

1. **Code reuse vs duplication of export codec.** The CSV/JSONLine type switch
   lives in `pkg/frontend`. Importing `frontend` from `colexec` is undesirable
   (layering). Plan: extract the codec into a neutral package
   (`pkg/common/exportcodec`) used by both. Confirm no hidden frontend-only
   dependencies in that code first.
2. **strftime coverage.** Decide the exact directive set to support; document
   unsupported directives as errors rather than silently passing them through.
3. **Affected-rows reporting.** Sum across all parallel writers/CNs at the merge
   step; verify the existing affected-rows aggregation path handles the
   external-write operator.
4. **Partial-failure files (§2.6).** Accepted limitation for v1; note it in user
   docs. Revisit with temp-name+rename if atomicity is later required.
5. **Stage writability.** `ToPath`/`GetForETL` must produce a writable
   fileservice; confirm S3 credentials in the stage carry write permission and
   that `LocalFS.Write` (`pkg/fileservice/local_fs.go:206`) — which errors on an
   existing file — interacts correctly with `%U`/`%nN` uniqueness.
6. **Column order / projection.** Ensure the projected batch column order
   matches the external table's declared column order so CSV columns line up
   with the read-side parser.
