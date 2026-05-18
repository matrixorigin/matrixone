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

// Package vectorplan is the leaf sub-package that holds shared planner
// helpers vector-index plugins call. After Phase 5d, the plugin's
// plan-layer CONTRACT (PlanBuilder interface, VectorSortContext,
// MultiTableIndexRef, ApplyForSortOpts, CompilerContext, BindContext,
// TableFuncBuilder + its registry, Hooks) lives in
// pkg/vectorindex/plugin/plan — mirroring the layout of plugin/compile.
//
// What's still here:
//
//   - Function variables (populated at pkg/sql/plan init() time) — the
//     plugin calls e.g. vectorplan.DeepCopyExpr(expr); pkg/sql/plan owns
//     the body. Bodies stayed in pkg/sql/plan because they have too many
//     tributaries to move cheaply (deepcopy.go is a 1000+ LoC tight
//     cluster; CreateIndexDef has per-algo defaults; etc.).
//   - Standalone helpers (helpers.go): DeepCopyRankOption,
//     MakeRuntimeFilter, the over-fetch factor calculators,
//     ParseIncludedColumnsFromParams, MakePlan2StringConstExprWithType.
//   - filter_predicate.go: GPU vector predicate-pushdown helpers
//     (BuildFilterPredicateJSON + tributaries) shared by CAGRA & IVF-PQ.
//
// WHERE PER-ALGORITHM PLAN-REWRITE BODIES LIVE (not here):
//
//	HNSW     → pkg/vectorindex/hnsw/plugin/plan/
//	CAGRA    → pkg/vectorindex/cagra/plugin/plan/
//	IVF-PQ   → pkg/vectorindex/ivfpq/plugin/plan/
//	IVF-FLAT → pkg/vectorindex/ivfflat/plugin/plan/
//
// Each plugin directory holds plan.go (CanApply + ApplyForSort),
// schema.go (BuildSecondaryIndexDefs), tablefunc.go (per-algo
// `<algo>_create` / `<algo>_search` builders), plus context.go /
// helpers.go for the larger ones (IVF-FLAT).
//
// Cycle-safety: this package depends only on pkg/pb/plan, parsers/tree,
// catalog, vectorindex/metric. pkg/sql/plan imports this package; the
// plugin imports this package; neither imports the other through it.
package vectorplan

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	planplugin "github.com/matrixorigin/matrixone/pkg/vectorindex/plugin/plan"
)

// Function variables populated by pkg/sql/plan at init() time. These break
// the import cycle: pkg/sql/plan defines the bodies, vectorplan publishes
// references the plugin can call.
//
// Plugin code calls e.g. vectorplan.DeepCopyExpr(expr). At plugin init time
// these may be nil; they're guaranteed non-nil by the time a plan-rewrite
// hook actually runs, because pkg/sql/plan must have initialized to even
// invoke the hook in the first place.
var (
	// Bodies in pkg/sql/plan, published here as function variables
	// because their pkg/sql/plan home has too many tributaries to
	// move cheaply (deepcopy.go is a 1000+ LoC tight cluster; the
	// remaining helpers depend on internal helpers like
	// makeHiddenColTyp / makePlan2StringConstExpr / filterExprToPreds).
	// pkg/sql/plan's init() populates them; they're guaranteed
	// non-nil by the time a plan-rewrite hook actually runs because
	// pkg/sql/plan must have initialized to invoke the hook.
	DeepCopyExpr                   func(*plan.Expr) *plan.Expr
	DeepCopyColDefList             func([]*plan.ColDef) []*plan.ColDef
	ReplaceDistFnExprsWithScoreCol func(exprs []*plan.Expr, scanBindingTag, partPos int32, origFuncName string, vecLit *plan.Expr, tableFuncTag int32, scoreColType plan.Type)

	// Hidden-table-schema build helpers — bodies stay in pkg/sql/plan.
	// CreateIndexDef in particular has a per-algo default-options switch
	// that's most naturally expressed alongside the planner.
	CreateIndexDef         func(idx *tree.Index, indexTableName, indexAlgoTableType string, indexParts []string, isUnique bool) (*plan.IndexDef, error)
	MakeHiddenColDefByName func(name string) *plan.ColDef

	// These two have type adapters bridging the plugin's exported
	// types to the internal unexported ones — they cannot become
	// straight aliases without surfacing more internals.
	ValidateIncludeColumns       func(ctx planplugin.CompilerContext, includeCols []*tree.UnresolvedName, colMap map[string]*plan.ColDef, vecColName, pkeyName string) error
	VectorSearchProviderChildren func(*planplugin.VectorSortContext) []int32
)
