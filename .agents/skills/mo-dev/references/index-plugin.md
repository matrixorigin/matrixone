# Vector / Fulltext Index-Plugin Reference

## Contents

- [1. Why The Framework Exists](#1-why-the-framework-exists)
- [2. Architecture](#2-architecture)
- [3. Registry Contract](#3-registry-contract)
- [4. Plugin Package Layout](#4-plugin-package-layout)
- [5. Adding A New Algorithm](#5-adding-a-new-algorithm)
- [6. CPU vs GPU Registration](#6-cpu-vs-gpu-registration)
- [7. Forbidden Patterns](#7-forbidden-patterns)
- [8. Testing And Completion](#8-testing-and-completion)
- [9. Reviewing An Index-Plugin Change](#9-reviewing-an-index-plugin-change)

## 1. Why The Framework Exists

Adding a vector index used to mean editing many files across `pkg/sql/compile`, `pkg/sql/plan`, `pkg/catalog`, and `pkg/vectorindex`, wired through repeated switch/if chains. Miss one site and the algorithm silently misbehaves.

`pkg/indexplugin` collapses those sites into one registry lookup. An algorithm registers one `AlgoPlugin`, and compile-time interface assertions enforce required hooks.

Guarded failure mode: re-introducing per-algorithm `switch algo` / `if IsXxxIndexAlgo || ...` in SQL/catalog layers bypasses the registry and re-opens the "forgot a dispatch site" bug class.

## 2. Architecture

`pkg/indexplugin` is the framework. It must not import `pkg/sql/{compile,plan}`. Hook methods receive narrow `Context` interfaces implemented by SQL-layer code.

```
pkg/indexplugin/
├── plugin.go          -- AlgoPlugin interface; Register/Get/All; IsVectorIndexAlgo/IsFullTextIndexAlgo/IsPluginAlgo
├── catalog/hooks.go   -- catalog.Hooks   (metadata: params, hidden tables, op/vector/pk types, quantization)
├── compile/hooks.go   -- compile.Hooks   (DDL execution: create/reindex/drop/restore) + CompileContext
├── plan/hooks.go      -- plan.Hooks      (schema defs + tablefunc + thin ANN redirects) + PlanBuilder/CompilerContext
├── idxcron/hooks.go   -- idxcron.Hooks   (scheduled-rebuild gating: Updatable)
└── all/
    ├── all.go         -- blank-imports CPU-safe plugins (fulltext, hnsw, ivfflat)
    └── all_gpu.go     -- //go:build gpu -- blank-imports GPU-only plugins (cagra, ivfpq)

pkg/vectorindex/<algo>/plugin/   -- one dir per algorithm; assembles hooks into an AlgoPlugin
pkg/fulltext/plugin/             -- fulltext is a plugin too (kind = fulltext, not vector)
```

Dispatch flow:

```go
p, ok := indexplugin.Get(algo) // case-insensitive, trims whitespace
if ok { return p.Compile().HandleCreateIndex(cctx, defs) }
```

Use:

- `indexplugin.IsVectorIndexAlgo(algo)` instead of `IsIvfIndexAlgo || IsHnswIndexAlgo || ...`
- `indexplugin.IsFullTextIndexAlgo(algo)` for fulltext
- `indexplugin.IsPluginAlgo(algo)` for registered vector or fulltext algorithms

Live dispatch sites include `pkg/sql/plan/build_ddl.go` and `pkg/sql/plan/apply_indices.go`.

## 3. Registry Contract

```go
type AlgoPlugin interface {
    Algo() string
    Catalog() catalogplugin.Hooks
    Compile() compileplugin.Hooks
    Plan()    planplugin.Hooks
    Idxcron() idxcronplugin.Hooks
}
func Register(p AlgoPlugin)
func Get(algo string) (AlgoPlugin, bool)
```

Each plugin keeps compile-time interface assertions:

```go
var _ plugin.AlgoPlugin = (*Plugin)(nil)
var _ Hooks = Hooks{}
```

If `AlgoPlugin` or a `Hooks` interface gains a method and a plugin is not updated, these assertions stop the build. Never delete or `//nolint` them to make the build green. Implement the missing method.

## 4. Plugin Package Layout

Canonical walkthrough: `pkg/vectorindex/ivfpq/plugin/plugin.go`.

Full plugin layout:

```
pkg/vectorindex/<algo>/plugin/
├── plugin.go            -- assembles 4 Hooks into one Plugin; init() { plugin.Register(New()) }
├── runtime/runtime.go   -- catalog.Hooks impl
├── compile/compile.go   -- compile.Hooks impl
├── plan/
│   ├── plan.go          -- thin ApplyForSort/CanApply redirect into *plan.QueryBuilder
│   ├── schema.go        -- BuildSecondaryIndexDefs body
│   └── tablefunc.go     -- <algo>_create / <algo>_search FUNCTION_SCAN builder registrations
├── idxcron/idxcron.go   -- idxcron.Hooks impl
└── iscp/iscp.go         -- CDC / import sync; may be //go:build gpu
```

Lift code into the sub-package matching the original SQL layer:

- `pkg/sql/compile/*.go` -> `plugin/compile/`
- `pkg/sql/plan/*.go` -> `plugin/plan/`
- Algorithm metadata constants -> `plugin/runtime/`

## 5. Adding A New Algorithm

1. Add catalog constants:
   - `MoIndex<X>Algo` in `pkg/catalog/secondary_index_utils.go`
   - hidden-table-type constants in `pkg/catalog/types.go`
   - parser keyword only if syntax needs it
2. Copy `pkg/vectorindex/ivfpq/plugin/` to `pkg/vectorindex/<x>/plugin/`, then rename packages/imports.
3. Implement four hooks:
   - `catalog.Hooks`: hidden table types, params, defaults, supported types, quantization, clone/restore behavior, session vars, sync descriptor.
   - `compile.Hooks`: create, reindex, validate reindex params, drop, restore init SQL, idxcron metadata.
   - `plan.Hooks`: secondary/fulltext index defs, `CanApply`, `ApplyForSort`.
   - `idxcron.Hooks`: `Updatable`.
4. If ANN rewrite is supported, add `applyIndicesForSortUsing<X>` + `prepare<X>IndexContext` in `pkg/sql/plan/apply_indices_<x>.go`, redirect methods in `pkg/sql/plan/plugin_builder.go`, and dispatch via `p.Plan().CanApply`.
5. Register in `plugin.go` with `plugin.Register(New())`.
6. Add exactly one blank import:
   - CPU-safe algo -> `pkg/indexplugin/all/all.go`
   - GPU-only algo -> `pkg/indexplugin/all/all_gpu.go`
7. Add SQL tests under `test/distributed/cases/vector/` for create, ANN query, reindex, drop index, and drop table.

If the plugin compiles and all hooks are implemented, you did not miss a dispatch site. Do not add manual dispatch "to be safe."

## 6. CPU vs GPU Registration

| File | Build tag | Registers | Rationale |
|------|-----------|-----------|-----------|
| `pkg/indexplugin/all/all.go` | none | fulltext, hnsw, ivfflat | CPU-safe algorithms |
| `pkg/indexplugin/all/all_gpu.go` | `//go:build gpu` | cagra, ivfpq | CUDA-backed table functions exist only under `gpu` |

CAGRA / IVF-PQ table functions exist only under `//go:build gpu`. Registering them on CPU would let `CREATE INDEX ... USING cagra|ivfpq` proceed until BUILD SQL fails mid-flight after hidden table side effects. GPU-tagged registration makes plan-build fail cleanly with `unsupported index type: <algo>`.

Pairing rule: an algo with `build_gpu.go` must have a `//go:build !gpu` CPU counterpart stub, and the plugin blank import belongs in `all_gpu.go`, never `all.go`.

## 7. Forbidden Patterns

1. Never re-introduce per-algorithm dispatch in SQL/catalog layers. A new `switch idx.IndexAlgo`, `case MoIndex<X>Algo:`, or `if catalog.IsIvfIndexAlgo(a) || ...` re-opens the bug class the framework closed. Add a method to a hook interface instead.
2. Never import `pkg/sql/plan` or `pkg/sql/compile` from a plugin package. Plugins receive narrow interfaces and call through them.
3. Never register a GPU-only algo in `all.go`; use `all_gpu.go`.
4. Never weaken `var _ AlgoPlugin` / `var _ Hooks` assertions.
5. Keep runtime kernels out of the plugin. Build/search kernels stay in `pkg/vectorindex/<algo>/{build_gpu,search_gpu}.go` and are invoked from table functions.
6. Never pollute `pkg/indexplugin` with algorithm-specific code. It is interfaces, registry, and algorithm-agnostic metadata only. The only files there that may name concrete algorithms are `all/` and `iscp/` aggregators, and only as blank imports.

## 8. Testing And Completion

Index-plugin changes have a coverage trap: end-to-end BVT cases under `test/distributed/cases/vector/` are GPU-gated, so `plan.go` / `schema.go` can read 0% coverage in non-GPU CI. Add CPU-runnable unit tests like `tablefunc_test` / `runtime_test` style tests, not just GPU-gated BVT.

Completion checklist:

```
□ CPU unit tests exist for plan/schema/runtime hooks  -> run, exit 0
□ GPU-only hooks also run with `-tags gpu`             -> exit 0
□ grep: NO new `switch algo` / `IsXxxIndexAlgo ||` added in pkg/sql/*
□ git diff --stat: the ONLY all.go/all_gpu.go edit is one blank import
```

Known in-progress seams: a few DML-sync sites still call `catalog.IsIvfIndexAlgo` directly (`pkg/sql/plan/build_dml_util.go`, `pkg/sql/plan/bind_insert.go`). These are legacy, not a license to add more. When touching one, prefer moving it behind a hook.

## 9. Reviewing An Index-Plugin Change

Apply this when reviewing a diff that touches index-algorithm dispatch, any `pkg/vectorindex/<algo>/plugin/`, `pkg/fulltext/plugin`, or `pkg/indexplugin`. Request changes if a check fails.

1. No new per-algo dispatch:

```bash
git diff -U0 pkg/sql pkg/catalog | grep -E '^\+' \
  | grep -E 'IsIvfIndexAlgo|IsHnswIndexAlgo|IsCagraIndexAlgo|IsIvfpqIndexAlgo|IndexAlgo *==|case .*Algo\b'
```

2. No import cycle:

```bash
git diff pkg/vectorindex/*/plugin pkg/fulltext/plugin \
  | grep -E '^\+.*matrixorigin/matrixone/pkg/sql/(plan|compile)"'
```

3. `indexplugin` not polluted:

```bash
git diff pkg/indexplugin \
  | grep -E '^\+.*(vectorindex/(ivfflat|ivfpq|cagra|hnsw)/plugin|fulltext/plugin)'
```

4. GPU registration correct: GPU-only imports in `all_gpu.go`, not `all.go`; each `build_gpu.go` has CPU counterpart.
5. Assertions intact:

```bash
git diff | grep -E '^-.*var _ (plugin\.)?(AlgoPlugin|Hooks)'
```

6. Runtime kept out: no build/search kernel logic copied into plugin.
7. New-algo completeness: registered in `all/` or `all_gpu/`, SQL case added under `test/distributed/cases/vector/`, CPU unit tests cover plan/schema/runtime hooks.
