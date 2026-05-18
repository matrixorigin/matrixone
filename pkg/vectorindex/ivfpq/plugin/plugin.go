// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package plugin is the IVF-PQ vector index integration AND the canonical
// reference for adding a new vector index algorithm to MatrixOne.
//
// # How to add a new vector index algorithm
//
//  1. Pick an algo token (e.g. "scann"). Add a constant for it in
//     pkg/catalog/secondary_index_utils.go alongside MoIndexIvfpqAlgo,
//     and a tree.INDEX_TYPE_<X> case in pkg/sql/parsers (only if the
//     algorithm introduces a new CREATE INDEX keyword).
//
//  2. Add hidden-table-type constants in pkg/catalog/types.go alongside
//     Ivfpq_TblType_Metadata / Ivfpq_TblType_Storage — one per hidden
//     table the algorithm needs.
//
//  3. Copy this directory to pkg/vectorindex/<algo>/plugin/. Rename the
//     inner package names and update the imports. You'll end up with:
//
//     pkg/vectorindex/<algo>/plugin/
//     ├── plugin.go            -- this file: registry entry point
//     ├── runtime/runtime.go   -- CatalogHooks (HiddenTableTypes,
//     │                          DefaultOptions, ExperimentalFlag)
//     ├── compile/compile.go   -- compile.Hooks (CREATE/ALTER/DROP/SYNC)
//     └── plan/
//     ├── plan.go          -- plan.Hooks: thin redirect (~20 LoC)
//     │                      whose ApplyForSort / CanApply forward
//     │                      to *QueryBuilder methods in pkg/sql/plan
//     ├── schema.go        -- BuildSecondaryIndexDefs body
//     │                      (hidden-table TableDefs + IndexDefs)
//     └── tablefunc.go     -- <algo>_create / <algo>_search
//     FUNCTION_SCAN builders
//
//     Rule of thumb for which sub-package gets the body: lifted code
//     from pkg/sql/compile/<file>.go → compile/; from pkg/sql/plan/<file>.go
//     → plan/; runtime/ is reserved for algorithm-metadata constants
//     that don't belong to a SQL pipeline layer.
//
//  4. Implement the three Hooks interfaces:
//     - pkg/indexplugin/catalog.Hooks  (4 methods — metadata)
//     - pkg/indexplugin/compile.Hooks  (~12 methods — DDL execution)
//     - pkg/indexplugin/plan.Hooks     (3 methods — schema +
//     two thin ANN redirects)
//     The Go compiler enforces completeness via the `var _ Hooks =
//     Hooks{}` interface checks in each sub-package.
//
//  5. If the algorithm supports ANN `ORDER BY <distfn>(col, v) LIMIT k`,
//     add the body methods to pkg/sql/plan:
//
//     pkg/sql/plan/apply_indices_<algo>.go:
//     func (builder *QueryBuilder) applyIndicesForSortUsing<Algo>(...)
//     func (builder *QueryBuilder) prepare<Algo>IndexContext(...)
//
//     Then wire four redirect methods on *QueryBuilder in
//     pkg/sql/plan/plugin_builder.go (ApplyIndicesForSortUsing<Algo> +
//     CanApply<Algo>) and four matching abstract methods on
//     planplugin.PlanBuilder in pkg/indexplugin/plan/hooks.go.
//     Add the dispatch case at pkg/sql/plan/apply_indices.go.
//
//  6. Register: this file's init() calls plugin.Register(New()). To make
//     production binaries and tests pick it up, add ONE blank import
//     line to pkg/indexplugin/all/all.go. That aggregator is the
//     only place that needs editing — pkg/sql/plan and pkg/sql/compile
//     already blank-import pkg/indexplugin/all.
//
//  7. End-to-end test: CREATE INDEX, populate, ORDER BY <distfn>(col, v)
//     LIMIT k, ALTER REINDEX, DROP INDEX, DROP TABLE all exercise
//     different hook paths. Add a SQL case under
//     test/distributed/cases/vector/.
//
// Helpers the plugin may use without re-implementing them:
//   - pkg/indexplugin/plan  — schema / tablefunc helper function
//     variables (CreateIndexDef, MakeHiddenColDefByName,
//     ValidateIncludeColumns, DeepCopyColDefList) wired by pkg/sql/plan's
//     init(). Use these from schema.go / tablefunc.go.
//   - pkg/sql/util.BuildIndexTableName — generate a hidden table name.
//   - pkg/vectorindex/cache.Cache  — runtime in-memory index cache.
//   - pkg/vectorindex/metric       — distance functions, op_type registry.
//
// Helpers the plugin must NOT touch:
//   - pkg/sql/plan or pkg/sql/compile directly — those packages
//     blank-import the plugin for init() registration, so the cycle
//     would break. Always route through the framework hook interfaces.
//
// # What this specific file (plugin.go) does
//
// It is the single registration point. It assembles the three Hooks
// implementations from the sub-packages into one AlgoPlugin and
// registers it via init(). If you forget any of the three Hooks the
// `var _ AlgoPlugin = (*Plugin)(nil)` interface check below fails to
// compile — that is the safety net the framework provides.
package plugin

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/indexplugin"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"

	ivfpqcompile "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfpq/plugin/compile"
	ivfpqplan "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfpq/plugin/plan"
	ivfpqruntime "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfpq/plugin/runtime"
)

// Plugin is the IVF-PQ AlgoPlugin. One instance is registered at init().
//
// New algorithms: copy this struct and the New / accessor methods below;
// only the imported sub-packages and the Algo() return value should
// differ.
type Plugin struct {
	catalogHooks catalogplugin.Hooks
	compileHooks compileplugin.Hooks
	planHooks    planplugin.Hooks
}

func New() *Plugin {
	return &Plugin{
		catalogHooks: ivfpqruntime.CatalogHooks{},
		compileHooks: ivfpqcompile.Hooks{},
		planHooks:    ivfpqplan.Hooks{},
	}
}

// Algo returns the lower-cased algorithm token used in `INDEX … USING <algo>`
// and stored in mo_catalog.mo_indexes.algo. Must match the constant added
// to pkg/catalog (here: MoIndexIvfpqAlgo == "ivfpq").
func (*Plugin) Algo() string                   { return catalog.MoIndexIvfpqAlgo.ToString() }
func (p *Plugin) Catalog() catalogplugin.Hooks { return p.catalogHooks }
func (p *Plugin) Compile() compileplugin.Hooks { return p.compileHooks }
func (p *Plugin) Plan() planplugin.Hooks       { return p.planHooks }

// Compile-time enforcement that *Plugin satisfies plugin.AlgoPlugin. If a
// new method is added to AlgoPlugin and this plugin hasn't been updated,
// this line stops the build.
var _ plugin.AlgoPlugin = (*Plugin)(nil)

// init registers IVF-PQ with the global plugin registry. The SQL layer's
// dispatch sites (pkg/sql/compile/ddl.go, pkg/sql/plan/apply_indices.go,
// pkg/sql/plan/build_ddl.go) look up plugins by algo string at runtime.
//
// For this init() to fire, something must import this package. Production
// does it transitively via pkg/sql/plan and pkg/sql/compile (see their
// plugin_context.go files). The aggregator pkg/indexplugin/all is
// the canonical "load every algorithm" import.
func init() { plugin.Register(New()) }
