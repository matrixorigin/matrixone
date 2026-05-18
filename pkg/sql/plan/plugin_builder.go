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

package plan

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	planplugin "github.com/matrixorigin/matrixone/pkg/vectorindex/plugin/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/vectorplan"
)

// init populates the cross-package function variables in vectorplan so the
// vector-index plugin's plan-rewrite body can use them without taking a
// direct dependency on pkg/sql/plan.
// init populates the cross-package function variables in vectorplan
// whose bodies still live in pkg/sql/plan. Phase 5b moved 6 standalone
// helpers (DeepCopyRankOption, MakeRuntimeFilter, the two over-fetch
// calculators, ParseIncludedColumnsFromParams, MakePlan2StringConstExprWithType)
// into vectorplan as real functions; what's left here is the
// dependency-heavy or type-adapter-needing rump.
func init() {
	vectorplan.DeepCopyExpr = DeepCopyExpr
	vectorplan.DeepCopyColDefList = DeepCopyColDefList
	vectorplan.ReplaceDistFnExprsWithScoreCol = replaceDistFnExprsWithScoreCol

	vectorplan.CreateIndexDef = CreateIndexDef
	vectorplan.MakeHiddenColDefByName = MakeHiddenColDefByName
	vectorplan.ValidateIncludeColumns = validateIncludeColumnsForPlugin
	vectorplan.VectorSearchProviderChildren = vectorSearchProviderChildrenForPlugin
}

// vectorSearchProviderChildrenForPlugin adapts vectorSearchProviderChildren
// (which takes *vectorSortContext) to planplugin.VectorSortContext.
func vectorSearchProviderChildrenForPlugin(vc *planplugin.VectorSortContext) []int32 {
	if vc == nil {
		return nil
	}
	return vectorSearchProviderChildren(&vectorSortContext{
		providerNodeID: vc.ProviderNodeID,
		vecArgExpr:     vc.VecArgExpr,
	})
}

// validateIncludeColumnsForPlugin adapts validateIncludeColumns to the
// narrower planplugin.CompilerContext the plugin uses. Dispatch always
// passes a real *plan.CompilerContext, so the assertion is total at call
// time; the second return is only for the compiler's exhaustiveness.
func validateIncludeColumnsForPlugin(ctx planplugin.CompilerContext,
	includeCols []*tree.UnresolvedName, colMap map[string]*plan.ColDef,
	vecColName, pkeyName string) error {
	return validateIncludeColumns(ctx.(CompilerContext), includeCols, colMap, vecColName, pkeyName)
}

// *QueryBuilder satisfies planplugin.PlanBuilder. The compile-time
// assertion below catches any signature drift between the interface
// and the concrete type — add a new method to planplugin.PlanBuilder
// and forget to implement it (or rename it), and this line breaks the
// build.
var _ planplugin.PlanBuilder = (*QueryBuilder)(nil)

// Most interface methods are already defined on *QueryBuilder under
// the same exported names (after Phase 5 renames: GenNewBindTag,
// GenNewMsgTag, GetArgsFromDistFn, etc.). The remaining definitions
// here are genuine type-adapters that bridge the plugin's `any`
// BindContext to the internal *BindContext, or that funnel a
// standalone helper through a method.

func (builder *QueryBuilder) AppendNode(node *plan.Node, ctx planplugin.BindContext) int32 {
	bc, _ := ctx.(*BindContext)
	return builder.appendNode(node, bc)
}

func (builder *QueryBuilder) AddBinding(nodeID int32, alias tree.AliasClause, ctx planplugin.BindContext) error {
	bc, _ := ctx.(*BindContext)
	return builder.addBinding(nodeID, alias, bc)
}

func (builder *QueryBuilder) CtxByNode(id int32) planplugin.BindContext {
	if int(id) < 0 || int(id) >= len(builder.ctxByNode) {
		return nil
	}
	return builder.ctxByNode[id]
}

func (builder *QueryBuilder) Query() *plan.Query { return builder.qry }

// _ = context.TODO is kept so this file still imports "context".
var _ = context.TODO

func (builder *QueryBuilder) ResolveVariable(name string, isSystemVar, isGlobalVar bool) (any, error) {
	return builder.compCtx.ResolveVariable(name, isSystemVar, isGlobalVar)
}

// ValidateVectorIndexSortRewrite is a thin adapter that takes the
// exported planplugin.VectorSortContext (mirrors the unexported
// vectorSortContext used internally). Only the sortDirection field is
// actually inspected, so the copy is cheap.
func (builder *QueryBuilder) ValidateVectorIndexSortRewrite(vc *planplugin.VectorSortContext) (bool, error) {
	if vc == nil {
		return builder.validateVectorIndexSortRewrite(nil)
	}
	return builder.validateVectorIndexSortRewrite(&vectorSortContext{
		projNode:      vc.ProjNode,
		sortNode:      vc.SortNode,
		scanNode:      vc.ScanNode,
		childNode:     vc.ChildNode,
		orderExpr:     vc.OrderExpr,
		distFnExpr:    vc.DistFnExpr,
		sortDirection: vc.SortDirection,
		limit:         vc.Limit,
		rankOption:    vc.RankOption,
	})
}

func (builder *QueryBuilder) BindFuncByName(name string, args []*plan.Expr) (*plan.Expr, error) {
	return BindFuncExprImplByPlanExpr(builder.GetContext(), name, args)
}

func (builder *QueryBuilder) ReplaceColumnsForNode(node *plan.Node, projMap map[[2]int32]*plan.Expr) {
	replaceColumnsForNode(node, projMap)
}

func (builder *QueryBuilder) CopyNode(ctx planplugin.BindContext, nodeID int32) int32 {
	bc, _ := ctx.(*BindContext)
	return builder.copyNode(bc, nodeID)
}

// export converts the package-private vectorSortContext into the exported
// planplugin.VectorSortContext that crosses the plugin boundary.
func (v *vectorSortContext) export() *planplugin.VectorSortContext {
	if v == nil {
		return nil
	}
	return &planplugin.VectorSortContext{
		ProjNode:       v.projNode,
		SortNode:       v.sortNode,
		ScanNode:       v.scanNode,
		ChildNode:      v.childNode,
		OrderExpr:      v.orderExpr,
		DistFnExpr:     v.distFnExpr,
		SortDirection:  v.sortDirection,
		Limit:          v.limit,
		RankOption:     v.rankOption,
		ProviderNodeID: v.providerNodeID,
		VecArgExpr:     v.vecArgExpr,
	}
}
