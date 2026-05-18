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
// implements: hidden-table schema construction, table-function builders,
// plus thin redirects for the ANN rewrite (which actually lives in
// pkg/sql/plan).
//
// History: an earlier iteration of this package held the entire ANN
// rewrite body for each algorithm — ~4000 LoC across 4 plugin
// directories — with a 23-method PlanBuilder facade. Single-call-site
// abstractions like that fight the existing "plan code lives in
// pkg/sql/plan" mental model. Phase 6 pulled ApplyForSort + CanApply
// bodies back. Plugins now own:
//   - BuildSecondaryIndexDefs        (schema.go)        — hidden-table TableDefs
//   - TableFuncBuilder registrations (tablefunc.go)    — ivf_create / hnsw_search / ...
//   - thin ApplyForSort + CanApply   (plan.go, ~10 LoC) — one-line redirect
//     into the matching method on *plan.QueryBuilder
//
// Mirrors pkg/vectorindex/plugin/compile/hooks.go for the layout
// (Hooks + facade in one file).
package plan

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// CompilerContext is re-exported so plugin schema builders can consult
// database / variable state during CREATE INDEX planning without
// importing pkg/sql/plan. The pkg/sql/plan side type-asserts at the call
// boundary.
type CompilerContext interface {
	GetContext() context.Context
}

// BindContext is opaque to plugins. It's a *plan.BindContext on the
// inside of pkg/sql/plan; the plugin only ever receives one and passes
// it back into PlanBuilder.AppendNode.
type BindContext = any

// VectorSortContext is the captured ORDER BY context for a vector ANN
// rewrite. Exported counterpart of plan.vectorSortContext. The redirect
// methods on PlanBuilder convert this back to the internal type before
// invoking the body in pkg/sql/plan.
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

	// ProviderNodeID and VecArgExpr are populated only when the ORDER
	// BY reaches the scan through a JOIN (today only HNSW consumes them).
	ProviderNodeID int32
	VecArgExpr     *plan.Expr
}

// MultiTableIndexRef is the plugin-facing view of plan.MultiTableIndex.
type MultiTableIndexRef struct {
	IndexAlgo       string
	IndexAlgoParams string
	IndexDefs       map[string]*plan.IndexDef
}

// ApplyForSortOpts carries per-call rewrite state. Today only IVF-FLAT's
// auto-mode two-scan rewrite consults these maps.
type ApplyForSortOpts struct {
	ColRefCnt map[[2]int32]int
	IdxColMap map[[2]int32]*plan.Expr
}

// PlanBuilder is the *plan.QueryBuilder facade plugins use. Two roles:
//
//   1. Provide the minimum primitives the per-algo tablefunc.go needs
//      to construct FUNCTION_SCAN nodes (GenNewBindTag, AppendNode,
//      GetContext).
//
//   2. Provide the per-algo redirect methods plan.go calls. Each is
//      implemented in pkg/sql/plan/plugin_builder.go as a one-liner
//      that converts the exported types to internal types and invokes
//      the real body (e.g. `applyIndicesForSortUsingHnsw`).
type PlanBuilder interface {
	// Primitives for tablefunc.go.
	GenNewBindTag() int32
	AppendNode(node *plan.Node, ctx BindContext) int32
	GetContext() context.Context

	// Per-algo redirects for plan.go (one pair per algorithm).
	ApplyIndicesForSortUsingHnsw(vctx *VectorSortContext, mti *MultiTableIndexRef, nodeID int32, opts ApplyForSortOpts) (int32, bool, error)
	ApplyIndicesForSortUsingCagra(vctx *VectorSortContext, mti *MultiTableIndexRef, nodeID int32, opts ApplyForSortOpts) (int32, bool, error)
	ApplyIndicesForSortUsingIvfpq(vctx *VectorSortContext, mti *MultiTableIndexRef, nodeID int32, opts ApplyForSortOpts) (int32, bool, error)
	ApplyIndicesForSortUsingIvfflat(vctx *VectorSortContext, mti *MultiTableIndexRef, nodeID int32, opts ApplyForSortOpts) (int32, bool, error)

	CanApplyHnsw(vctx *VectorSortContext, mti *MultiTableIndexRef) (bool, error)
	CanApplyCagra(vctx *VectorSortContext, mti *MultiTableIndexRef) (bool, error)
	CanApplyIvfpq(vctx *VectorSortContext, mti *MultiTableIndexRef) (bool, error)
	CanApplyIvfflat(vctx *VectorSortContext, mti *MultiTableIndexRef) (bool, error)
}

// Hooks bundles the plan-layer callbacks each plugin must implement.
type Hooks interface {
	// BuildSecondaryIndexDefs constructs the IndexDef and TableDef list
	// for this algorithm's hidden tables. Body lives in the plugin's
	// schema.go.
	BuildSecondaryIndexDefs(ctx CompilerContext, idx *tree.Index,
		colMap map[string]*plan.ColDef, existedIndexes []*plan.IndexDef,
		pkeyName string) ([]*plan.IndexDef, []*plan.TableDef, error)

	// CanApply / ApplyForSort are thin redirects implemented in the
	// plugin's plan.go. Body lives on *plan.QueryBuilder in
	// pkg/sql/plan/apply_indices_<algo>.go.
	CanApply(pb PlanBuilder, vctx *VectorSortContext, mti *MultiTableIndexRef) (bool, error)
	ApplyForSort(pb PlanBuilder, vctx *VectorSortContext, mti *MultiTableIndexRef, nodeID int32, opts ApplyForSortOpts) (int32, bool, error)
}

// Schema-build / tablefunc helper bodies live in pkg/sql/plan. They're
// published here as function variables (init wired up at pkg/sql/plan
// package load). Plugin schema.go and tablefunc.go call them as
// planplugin.<X>.
var (
	CreateIndexDef         func(idx *tree.Index, indexTableName, indexAlgoTableType string, indexParts []string, isUnique bool) (*plan.IndexDef, error)
	MakeHiddenColDefByName func(name string) *plan.ColDef
	ValidateIncludeColumns func(ctx CompilerContext, includeCols []*tree.UnresolvedName, colMap map[string]*plan.ColDef, vecColName, pkeyName string) error
	DeepCopyColDefList     func([]*plan.ColDef) []*plan.ColDef
)
