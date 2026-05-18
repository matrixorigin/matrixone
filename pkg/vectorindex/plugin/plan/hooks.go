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

// Package plan defines the plan-layer contract every vector-index plugin
// implements:
//
//   - Hooks                 — three methods plugins implement
//   - PlanBuilder           — facade for *plan.QueryBuilder calls
//   - VectorSortContext     — captured ORDER BY for the ANN rewrite
//   - MultiTableIndexRef    — plugin-facing MultiTableIndex view
//   - ApplyForSortOpts      — per-call rewrite state (colRefCnt / idxColMap)
//   - CompilerContext       — narrow view of plan.CompilerContext
//   - BindContext           — opaque alias for *plan.BindContext
//   - TableFuncBuilder + registry (tablefunc.go)
//
// Mirrors pkg/vectorindex/plugin/compile/hooks.go, which holds the
// compile-layer Hooks + CompileContext in one file. Both packages are
// leaf interfaces — they import only pkg/pb/plan, parsers/tree, and a
// few low-level utilities. pkg/sql/plan imports this package to satisfy
// PlanBuilder; pkg/sql/plan/vectorplan provides the shared planner
// helpers (function variables, predicate-pushdown utilities) that
// plugin bodies call through.
package plan

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

	// GenNewMsgTag mints a new runtime-filter message tag. Used by IVF-FLAT
	// when wiring BloomFilter / IN-list runtime filters between the table
	// function and the source scan.
	GenNewMsgTag() int32

	// CopyNode deep-copies a plan subtree rooted at nodeID and returns the
	// new root's ID. Used by IVF-FLAT pre-mode to build the inner second
	// scan that feeds the BloomFilter.
	CopyNode(ctx BindContext, nodeID int32) int32

	// RebindScanNode reassigns the scan's binding tag (GenNewBindTag) and
	// updates every dependent ColRef in its FilterList / BlockFilterList.
	// Used after CopyNode so the cloned subtree has distinct bindings.
	RebindScanNode(scanNode *plan.Node)

	// ApplyIndicesForFilters runs the optimizer's regular secondary-index
	// rewrite over `node`'s filter list. Returns the (possibly rewritten)
	// node ID. Used by IVF-FLAT to layer regular-index optimization onto
	// the second scan / outer scan when both indexes apply.
	ApplyIndicesForFilters(nodeID int32, node *plan.Node,
		colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) int32

	// WithSuspendedScanProtection runs `fn` with the scan-protection guard
	// for `scanNodeID` temporarily disabled. The scan protection prevents
	// the regular-index optimizer from rewriting a scan that's actively
	// being consumed by an ANN rewrite; for the outer-join case in IVF-FLAT
	// the rewrite is done and we can run regular-index optimization safely.
	WithSuspendedScanProtection(scanNodeID int32, fn func())

	// GetDistRangeFromFilters extracts `<distFn>(part, vecLit) <op> K` style
	// predicates from the filter list and returns the residual filters plus
	// the bounds packaged as a DistRange for the table-function reader.
	GetDistRangeFromFilters(filters []*plan.Expr, partPos int32, origFuncName string,
		vecLitArg *plan.Expr) (newFilters []*plan.Expr, distRange *plan.DistRange)

	// GetColName returns the column name for a ColRef, consulting the
	// builder's nameByColRef table when col.Name is empty.
	GetColName(col *plan.ColRef) string

	// AddNameByColRef registers column names for a binding tag from a
	// TableDef. Used after RebindScanNode so projections / filters can
	// resolve column names against the new tag.
	AddNameByColRef(tag int32, tableDef *plan.TableDef)
}

// Hooks bundles every plan-layer callback for one algorithm.
type Hooks interface {
	// BuildSecondaryIndexDefs constructs the IndexDef and TableDef list
	// for this algorithm's hidden tables, given a CREATE INDEX statement.
	// Replaces buildXxxSecondaryIndexDef and one switch arm at
	// pkg/sql/plan/build_ddl.go:2081.
	//
	// ctx is *plan.CompilerContext expressed through this package's
	// narrow re-export; the algorithm only needs ctx.GetContext() for
	// error messages and util.BuildIndexTableName.
	BuildSecondaryIndexDefs(ctx CompilerContext, idx *tree.Index,
		colMap map[string]*plan.ColDef, existedIndexes []*plan.IndexDef,
		pkeyName string) ([]*plan.IndexDef, []*plan.TableDef, error)

	// CanApply is a non-destructive probe — does this index look like a
	// candidate for the captured ORDER BY? Used by detectVectorGuard to
	// protect the scan node from other optimizers before ApplyForSort
	// runs. Replaces the inner-body prepare<X>IndexContext probes at
	// apply_indices.go:847-885.
	CanApply(pb PlanBuilder, vctx *VectorSortContext,
		mti *MultiTableIndexRef) (bool, error)

	// ApplyForSort rewrites the query plan to use this index for the
	// captured ORDER BY (distfn(col, v)) LIMIT k pattern. Returns:
	//   newNodeID — the root of the rewritten sub-plan
	//   applied   — true if the rewrite was performed; false if the index
	//               cannot satisfy the query (e.g. op_type mismatch)
	//   err       — non-nil only on hard errors; "cannot apply" is
	//               communicated via applied=false
	//
	// opts carries per-call state the algorithm may need; today only
	// IVF-FLAT's auto-mode rewrite consults ColRefCnt / IdxColMap.
	//
	// Replaces apply_indices.go:611 dispatch +
	// prepare<X>IndexContext + applyIndicesForSortUsing<X>.
	ApplyForSort(pb PlanBuilder, vctx *VectorSortContext,
		mti *MultiTableIndexRef, nodeID int32,
		opts ApplyForSortOpts) (newNodeID int32, applied bool, err error)
}

// NOTE: an earlier draft of this interface included three sync-DML hooks
// (DMLSyncTableTypes, BuildPreInsertSyncPlan, BuildDeleteSyncPlan)
// intended to let plugins own synchronous INSERT / DELETE index sync.
// Removed because only IVF-FLAT uses synchronous DML and its bodies
// live in pkg/sql/plan/build_dml_util.go (HNSW / CAGRA / IVF-PQ all use
// CDC). If a future algorithm needs sync DML the hooks can be added
// back — but no point carrying the speculative interface today.
