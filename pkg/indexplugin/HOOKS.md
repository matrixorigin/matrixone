# Index-Plugin Hook Contract

Reference for anyone (human or agent) implementing or changing an index plugin
(`pkg/vectorindex/<algo>/plugin/`, `pkg/fulltext/plugin/`). The registry replaces
per-algo `switch` statements with an *implicit contract* spread across two hook
interfaces and two clone paths. This file is that contract as one picture — the
seams that aren't obvious from any single hook.

**Golden rule: before implementing or changing any hook, open HNSW's version of
it first** (`pkg/vectorindex/hnsw/plugin/`). HNSW is the reference implementation
for an always-async, model-rebuilt index; IVF-FLAT is the reference for a
sync/seeded index. Almost every design question is already answered in one of them.

---

## 1. The two hook interfaces

| Interface | File | Purpose |
|-----------|------|---------|
| `catalog.Hooks` | `pkg/indexplugin/catalog/hooks.go` | **Metadata / policy** — pure declarations read by the SQL layer. No side effects. Says *what* the index is (hidden tables, PK/vector types, async policy, clone/restore policy). |
| `compile.Hooks` | `pkg/indexplugin/compile/hooks.go` | **DDL actions** — runs SQL in a txn. Says *how* to build/rebuild/drop (`HandleCreateIndex`, `HandleReindex`, `RestoreInitSQL`, `HandleDropIndex`). |

`catalog.Hooks` methods (declarations): `HiddenTableTypes`, `ParamsFromTree`,
`DefaultOptions`, `SupportedOpTypes`, `SupportedVectorTypes`,
`SupportedPrimaryKeyTypes`, `SupportedIncludeColumnTypes`, `ValidQuantization`,
`ExperimentalFlag`, `AlterTableCloneBehavior`, `RestoreBehavior`,
`BuildSessionVars`, `ShouldTruncateHiddenTable`, `SyncDescriptor`, `AlwaysAsync`.

`compile.Hooks` methods (actions): `HandleCreateIndex`, `HandleReindex`,
`RestoreInitSQL`, `ValidateReindexParams`, `HandleDropIndex`, `IdxcronMetadata`.

Both are asserted per plugin with `var _ catalog.Hooks = ...` / `var _ compile.Hooks = ...`.
Never delete those assertions.

---

## 2. THE central invariant — empty-at-create vs seeded

This one dichotomy explains most of the clone/restore hooks. Ask of every hidden table:

> **Is it non-empty right after `CREATE INDEX`, before any rows are populated?**

- **SEEDED (non-empty at create):** IVF-FLAT — CREATE writes a `version=0` metadata
  row, an initial centroid, bootstrapped entries. These *must* be emptied before a
  clone appends, or the seed duplicates.
- **EMPTY-at-create (rebuilt later):** HNSW / CAGRA / IVF-PQ / fulltext — CREATE
  leaves hidden tables empty and the model is (re)built afterward, via CDC from ts=0
  or an inline/sync build. Nothing to empty; instead the *whole index* is skipped on
  the ALTER-clone path and *rebuilt* on the restore path.

**Trap that caused the WAND clone/restore double-base bug (fixed 3edae8b5c):** giving
fulltext retrieval a **synchronous inline build** made `CreateTable` *seed* the tag=0
store — silently moving it from "empty-at-create" to "seeded" for the clone paths,
which then appended the source model on top. Fix was to rebuild post-clone via
`RestoreInitSQL` (see §4), not to special-case clone inside `HandleCreateIndex`.

---

## 3. Two separate async axes — don't conflate them

| Axis | Decided by | Meaning |
|------|-----------|---------|
| **DML async** | `AlwaysAsync(params)` **OR** the per-index `async` param (`indexplugin.IndexIsAsync`) | How DML after CREATE is applied: inline patch vs CDC sinker. |
| **BUILD async** | the per-index `async` param alone (`catalog.IsIndexAsync`, default false) | Whether the *initial* build runs inline in the CREATE txn or is deferred to CDC/InitSQL. |

An index can be **DML-async but BUILD-sync** — that is exactly fulltext retrieval
(always-async DML via CDC, but a synchronous initial build by default) and HNSW's
`!async || forceSync` inline-build branch. Gating a build on `AlwaysAsync` (or
`if async || retrieval`) is the classic bug: it conflates the two axes.

---

## 4. The clone/restore closure — TWO paths

"Clone" reaches an index through two different code paths with **different hooks**.
When you touch anything that affects hidden-table contents, trace **both**.

### Path A — ALTER that copies indexes (`cloneUnaffectedIndexes`, `pkg/sql/compile/alter.go`)
Copies a source index's hidden tables onto a schema-modified temp table.
Governed by **`AlterTableCloneBehavior`**:
- `SkipWholeIndex` (bool): when the index is async, skip the *entire* index — don't
  clone any hidden table; CDC rebuilds all from ts=0. (HNSW/CAGRA/IVF-PQ/fulltext.)
- `DeleteBeforeClone []string`: empty these seeded tables before the per-table clone.
- `SkipWhenAsync []string`: don't clone these when async (CDC rebuilds them; cloning
  would duplicate). (IVF-FLAT's `entries`.)

### Path B — `create table … clone` and snapshot restore (`RestoreTable`, `pkg/sql/compile/ddl.go:3560`)
`RestoreTable` runs, in order:
1. **drop** the CDC tasks `CreateTable` registered;
2. **`RestoreBehavior.DeleteBeforeClone`** — `DELETE … WHERE TRUE` each listed table
   (empty the seed) — *before* the block clone;
3. **`s.Run`** — block-level clone of the main table **and** all index hidden tables
   (an APPEND onto whatever step 2 left);
4. **`RestoreInitSQL(ctx, defs) -> (startFromNow, initSQL)`** — re-register each
   index's CDC with a plugin-provided InitSQL run post-commit.
   - `initSQL == ""` → `RestoreTable` forces `startFromNow=false` (CDC catches the
     cloned tables up from their watermark — replays).
   - non-empty `initSQL` → honored with the returned `startFromNow`.

**Key seam:** `RestoreInitSQL` is *the* place to rebuild a compacted model after a
restore. Clone-awareness belongs here (or in the `*Behavior` declarations), **never**
as an `IsTableClone()` branch inside `HandleCreateIndex`.

---

## 5. Per-algo matrix (the closure-critical hooks)

| Hook | HNSW | CAGRA | IVF-PQ | IVF-FLAT | fulltext |
|------|------|-------|--------|----------|----------|
| **Empty-at-create?** | yes | yes | yes | **no (seeded)** | yes |
| `AlwaysAsync` | `true` | `true` | `true` | `false` | `true` iff parser=`retrieval` |
| `AlterTableCloneBehavior` | `SkipWholeIndex` | `SkipWholeIndex` | `SkipWholeIndex` | `DeleteBeforeClone`=all 3 + `SkipWhenAsync`=`entries` | `SkipWholeIndex` |
| `RestoreBehavior` | `{}` | `{}` | `{}` | `DeleteBeforeClone`=all 3 | `DeleteBeforeClone`=`[postings]` |
| `RestoreInitSQL` | `ALTER…REINDEX…hnsw FORCE_SYNC` | `…cagra FORCE_SYNC` | `…ivfpq FORCE_SYNC` | `…ivfflat FORCE_SYNC` | retrieval→`…FULLTEXT FORCE_SYNC`; postings/ngram→`"SELECT 1"` |
| `HiddenTableTypes` | meta, storage | meta, storage | meta, storage | meta, centroids, entries | postings only (retrieval also has ft_index/ft_meta storage/meta) |

Read this row-wise for one algo when adding features; column-wise when adding a new algo.

Notes:
- **Vector algos + IVF-FLAT rebuild on restore** via `RestoreInitSQL` = REINDEX. The
  empty-at-create ones need no `DeleteBeforeClone` (nothing seeded); IVF-FLAT lists all
  three (seeded) *and* reindexes.
- **Fulltext retrieval** rebuilds on restore (REINDEX) like a vector index — because its
  sync build seeds the tag=0 store. **Fulltext postings/ngram** has no compact model:
  it keeps `"SELECT 1"` (non-empty so `startFromNow` stays true — avoids the CDC
  replaying already-cloned postings).

---

## 6. CDC registration — `startFromNow`

`CreateIndexCdcTask(..., startFromNow, initSQL, ...)`:
- `startFromNow=true` → watermark = now; CDC sees only *future* mutations. Use after an
  immediate/inline build, or after a clone whose tables are already supplied, so the
  already-present rows are **not** replayed.
- `startFromNow=false` → consume the full log from the table's creation TS. Use when the
  index must be (re)built from ts=0 by CDC.

---

## 7. Recipe — adding or changing a plugin

1. **Read HNSW (and IVF-FLAT if your index is seeded) first.** Copy the closest template.
2. Classify your index on the §2 axis (empty-at-create vs seeded) and the §3 axes
   (DML-async, build-async). Every clone/restore hook follows from those answers.
3. Fill in **both** `AlterTableCloneBehavior` (Path A) and `RestoreBehavior` +
   `RestoreInitSQL` (Path B). A change to how a hidden table is populated must be
   re-checked against **both** paths (§4).
4. Never put clone-awareness (`ctx.IsTableClone()`) inside `HandleCreateIndex`; express
   it through the `*Behavior` declarations and `RestoreInitSQL`.
5. Never add a per-algo `switch`/`case …Algo:` in `pkg/sql/{compile,plan}` or
   `pkg/catalog` — route through `indexplugin.Get(algo)`. (mo-dev skill §8 guards this.)
6. Register the plugin's blank import in `all/` (CPU) or `all_gpu/` (`//go:build gpu`),
   keep the `var _ Hooks` assertions, and add CPU unit tests for the plan/schema/runtime
   hooks (BVT is GPU-gated and reads 0% coverage in non-GPU CI).
7. Trace the full index closure end-to-end before calling it done:
   **CREATE (+InitSQL) → sync/CDC → query → REINDEX → clone(Path A) → restore(Path B) → DROP.**

---

## 8. Where the machinery lives

| Thing | Location |
|-------|----------|
| Hook interfaces | `pkg/indexplugin/{catalog,compile}/hooks.go` |
| Registry (`Get`, `All`, `IsPluginAlgo`, `IsVectorIndexAlgo`) | `pkg/indexplugin/` |
| CreateTable → HandleCreateIndex dispatch | `pkg/sql/compile/ddl.go:888` |
| CreateIndex → HandleCreateIndex dispatch | `pkg/sql/compile/ddl.go:2359` |
| ALTER REINDEX → HandleReindex dispatch | `pkg/sql/compile/ddl.go:1106` |
| Path A (ALTER clone) | `pkg/sql/compile/alter.go` `cloneUnaffectedIndexes` |
| Path B (clone/restore) | `pkg/sql/compile/ddl.go:3560` `RestoreTable` |
| DROP → HandleDropIndex dispatch | `pkg/sql/compile/ddl.go:2589` |
| Reference impls | `pkg/vectorindex/hnsw/plugin/` (async, rebuilt), `pkg/vectorindex/ivfflat/plugin/` (sync, seeded) |
