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
// 1. Pick an algo token (e.g. "scann"). Add a constant for it in
//    pkg/catalog/secondary_index_utils.go alongside MoIndexIvfpqAlgo, and
//    a tree.INDEX_TYPE_<X> case in pkg/sql/parsers (if your algorithm
//    introduces a new CREATE INDEX keyword).
//
// 2. Add hidden-table-type constants in pkg/catalog/types.go alongside
//    Ivfpq_TblType_Metadata / Ivfpq_TblType_Storage. One per hidden table
//    your algorithm needs.
//
// 3. Copy this directory to pkg/vectorindex/<algo>/plugin/. Rename the
//    inner package names and update the imports. You'll end up with:
//
//        pkg/vectorindex/<algo>/plugin/
//        ├── plugin.go            -- this file: registry entry point
//        ├── runtime/runtime.go   -- algorithm metadata (params, op-types)
//        ├── compile/compile.go   -- DDL hooks (CREATE/ALTER/DROP INDEX)
//        └── plan/
//            ├── plan.go          -- query rewrite (ANN ORDER BY) + DML sync
//            └── schema.go        -- hidden-table CREATE-INDEX schema builder
//
//    Rule of thumb for which sub-package gets the body: lifted code from
//    pkg/sql/compile/<file>.go → compile/; from pkg/sql/plan/<file>.go →
//    plan/; runtime/ is reserved for algorithm-metadata constants that
//    don't belong to a SQL pipeline layer.
//
// 4. Implement the three Hooks interfaces:
//      - pkg/vectorindex/plugin/catalog.Hooks  (4 methods — metadata)
//      - pkg/vectorindex/plugin/compile.Hooks  (4 methods — DDL execution)
//      - pkg/vectorindex/plugin/plan.Hooks     (5 methods — plan-tree work)
//    The Go compiler enforces completeness: if a method is missing, the
//    `var _ planplugin.Hooks = Hooks{}` interface checks below will fail.
//
// 5. Register at init() (last line of this file). Then blank-import the
//    package from pkg/vectorindex/plugin/all/all.go so production builds
//    pick it up.
//
// 6. End-to-end test: CREATE INDEX, populate, ORDER BY <distfn>(col, v)
//    LIMIT k, ALTER REINDEX, DROP INDEX, DROP TABLE all exercise different
//    hook paths. Add a SQL case under test/distributed/cases/vector/.
//
// Helpers the plugin may use without re-implementing them:
//   - pkg/sql/plan/vectorplan      — PlanBuilder facade, shared plan-tree
//                                    helpers (filter pushdown, dist-fn
//                                    rewriting), IVF-PQ-style table-fn
//                                    metadata. Function variables here are
//                                    populated by pkg/sql/plan's init().
//   - pkg/sql/util.BuildIndexTableName — generate a hidden table name.
//   - pkg/vectorindex/cache.Cache  — runtime in-memory index cache.
//   - pkg/vectorindex/metric       — distance functions, op_type registry.
//
// Helpers the plugin must NOT touch:
//   - pkg/sql/plan or pkg/sql/compile directly — those packages
//     blank-import the plugin for init() registration, so the cycle would
//     break. Always route through the framework hook interfaces and the
//     vectorplan facade.
//
// # What this specific file (plugin.go) does
//
// It is the single registration point. It assembles the three Hooks
// implementations from the sub-packages into one AlgoPlugin and registers
// it via init(). If you forget any of the three Hooks the
// `var _ AlgoPlugin = (*Plugin)(nil)` interface check below fails to
// compile — that is the safety net the framework provides.
package plugin

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/plugin"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/vectorindex/plugin/catalog"
	compileplugin "github.com/matrixorigin/matrixone/pkg/vectorindex/plugin/compile"
	planplugin "github.com/matrixorigin/matrixone/pkg/vectorindex/plugin/plan"

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
// plugin_context.go files). The aggregator pkg/vectorindex/plugin/all is
// the canonical "load every algorithm" import.
func init() { plugin.Register(New()) }
