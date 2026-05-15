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

// Package vectorplan is the leaf sub-package that lets vector index plugins
// participate in query planning without importing pkg/sql/plan.
//
// What's here:
//   - PlanBuilder         — facade interface implemented by *plan.QueryBuilder
//   - VectorSortContext   — exported version of plan.vectorSortContext
//   - MultiTableIndexRef  — exported view of plan.MultiTableIndex
//   - Function variables  — populated at pkg/sql/plan init() time; the plugin
//                           calls them instead of taking a direct dependency
//                           on pkg/sql/plan
//
// Cycle-safety: this package depends only on pkg/pb/plan, parsers/tree,
// catalog, vectorindex/metric. pkg/sql/plan imports this package; the plugin
// imports this package; neither imports the other through it.
package vectorplan

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// CompilerContext is re-exported for vector-index plugin schema builders
// that need to consult database / variable state during CREATE INDEX
// planning. It mirrors plan.CompilerContext but lives here so plugins can
// reference it without importing pkg/sql/plan. The pkg/sql/plan side
// type-asserts at the call boundary.
type CompilerContext interface {
	GetContext() context.Context
}

// BindContext is opaque to plugins. It's a *plan.BindContext on the inside
// of pkg/sql/plan; the plugin only ever receives one and passes it back into
// PlanBuilder.AppendNode / AddBinding.
type BindContext = any

// VectorSortContext is the captured ORDER BY context for a vector ANN
// rewrite. Exported counterpart of plan.vectorSortContext.
type VectorSortContext struct {
	ProjNode      *plan.Node
	SortNode      *plan.Node
	ScanNode      *plan.Node
	ChildNode     *plan.Node
	OrderExpr     *plan.Expr
	DistFnExpr    *plan.Function
	SortDirection plan.OrderBySpec_OrderByFlag
	Limit         *plan.Expr
	RankOption    *plan.RankOption

	// ProviderNodeID and VecArgExpr are populated only when the ORDER BY
	// reaches the scan through a JOIN (buildVectorSortContextThroughJoin in
	// pkg/sql/plan). Today only HNSW consumes them — see
	// PlanBuilder.GetArgsFromDistFnForJoin and VectorSearchProviderChildren.
	ProviderNodeID int32
	VecArgExpr     *plan.Expr
}

// MultiTableIndexRef is the plugin-facing view of plan.MultiTableIndex.
// Adapted at the dispatch site in pkg/sql/plan/apply_indices.go.
type MultiTableIndexRef struct {
	IndexAlgo       string
	IndexAlgoParams string
	IndexDefs       map[string]*plan.IndexDef
}

// ApplyForSortOpts carries per-call plan-rewrite state a Hooks.ApplyForSort
// implementation may consult. Today only IVF-FLAT's auto-mode two-scan
// rewrite uses these maps (to detect index-only opportunities); HNSW /
// CAGRA / IVF-PQ ignore them. The struct can grow without breaking
// existing plugins.
type ApplyForSortOpts struct {
	// ColRefCnt is the per-(rel,col) reference count from the
	// optimizer's earlier passes. Empty map is safe.
	ColRefCnt map[[2]int32]int

	// IdxColMap maps (rel,col) → expression for the optimizer's
	// index-only column-rewriting pass. Empty map is safe.
	IdxColMap map[[2]int32]*plan.Expr
}

// DMLInsertContext is the narrow view of the planner's pre-insert state
// a Hooks.BuildPreInsertSyncPlan implementation operates against.
// Implemented by pkg/sql/plan as a thin adapter over its internal types.
type DMLInsertContext interface {
	ObjRef() *plan.ObjectRef
	TableDef() *plan.TableDef
	SourceStep() int32
}

// DMLDeleteContext is the narrow view of plan.dmlPlanCtx a
// Hooks.BuildDeleteSyncPlan implementation operates against.
type DMLDeleteContext interface {
	ObjRef() *plan.ObjectRef
	TableDef() *plan.TableDef
	// IsUpdate reports whether this DELETE is the delete half of an
	// UPDATE (i.e. dmlPlanCtx.updateColLength > 0).
	IsUpdate() bool
}

// PlanBuilder is the QueryBuilder facade plugins use to construct plan
// trees. *plan.QueryBuilder satisfies it via methods defined in
// pkg/sql/plan/plugin_builder.go.
type PlanBuilder interface {
	// Bind-tag / node assembly.
	GenNewBindTag() int32
	AppendNode(node *plan.Node, ctx BindContext) int32
	AddBinding(nodeID int32, alias tree.AliasClause, ctx BindContext) error
	CtxByNode(id int32) BindContext

	// Query / compiler state.
	Query() *plan.Query
	GetContext() context.Context
	ResolveVariable(name string, isSystemVar, isGlobalVar bool) (any, error)

	// Vector-specific QueryBuilder methods.
	ValidateVectorIndexSortRewrite(vc *VectorSortContext) (bool, error)
	GetArgsFromDistFn(distFn *plan.Function, partPos int32) (key, value *plan.Expr, found bool)

	// GetArgsFromDistFnForJoin is the through-JOIN variant — used only by
	// HNSW today, when the captured vecCtx came from
	// buildVectorSortContextThroughJoin.
	GetArgsFromDistFnForJoin(distFn *plan.Function, partPos, scanTag int32) (key, value *plan.Expr, found bool)
	PeelAndRewriteDistFnFilters(filters []*plan.Expr, partPos int32, funcName string,
		vecLit *plan.Expr, tableFuncTag int32, scoreColType plan.Type) (newFilters, peeled []*plan.Expr)

	// Bind a function call by name (e.g. "=") through the plan-package's
	// type checker. Wraps BindFuncExprImplByPlanExpr with the builder's
	// own context.Context.
	BindFuncByName(name string, args []*plan.Expr) (*plan.Expr, error)

	// ReplaceColumnsForNode rewrites every column reference in `node` using
	// `projMap`, in place. Wraps plan.replaceColumnsForNode.
	ReplaceColumnsForNode(node *plan.Node, projMap map[[2]int32]*plan.Expr)
}

// Function variables populated by pkg/sql/plan at init() time. These break
// the import cycle: pkg/sql/plan defines the bodies, vectorplan publishes
// references the plugin can call.
//
// Plugin code calls e.g. vectorplan.DeepCopyExpr(expr). At plugin init time
// these may be nil; they're guaranteed non-nil by the time a plan-rewrite
// hook actually runs, because pkg/sql/plan must have initialized to even
// invoke the hook in the first place.
var (
	DeepCopyExpr                    func(*plan.Expr) *plan.Expr
	DeepCopyColDefList              func([]*plan.ColDef) []*plan.ColDef
	MakePlan2StringConstExprWithType func(string, ...bool) *plan.Expr
	BuildFilterPredicateJSON        func(filters []*plan.Expr, scanNode *plan.Node, includeCols []string, pkColName string) (predsJSON string, peeled, residual []*plan.Expr, err error)
	ParseIncludedColumnsFromParams  func(indexAlgoParams string) ([]string, error)
	ReplaceDistFnExprsWithScoreCol  func(exprs []*plan.Expr, scanBindingTag, partPos int32, origFuncName string, vecLit *plan.Expr, tableFuncTag int32, scoreColType plan.Type)
	CalculatePostFilterOverFetchFactor func(uint64) float64

	// Hidden-table-schema build helpers, populated at pkg/sql/plan init().
	// Used by vector-index plugins' BuildSecondaryIndexDefs implementations.
	CreateIndexDef          func(idx *tree.Index, indexTableName, indexAlgoTableType string, indexParts []string, isUnique bool) (*plan.IndexDef, error)
	MakeHiddenColDefByName  func(name string) *plan.ColDef
	ValidateIncludeColumns  func(ctx CompilerContext, includeCols []*tree.UnresolvedName, colMap map[string]*plan.ColDef, vecColName, pkeyName string) error

	// VectorSearchProviderChildren returns the children IDs for an
	// hnsw_search / ivf_search FUNCTION_SCAN when the captured vecCtx came
	// from a JOIN (the search node needs the JOIN's right input as its
	// child). Returns nil for the non-JOIN path. Populated at pkg/sql/plan
	// init() by VectorSearchProviderChildren in apply_indices_vector.go.
	VectorSearchProviderChildren func(*VectorSortContext) []int32
)
