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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/vectorplan"
)

// init populates the cross-package function variables in vectorplan so the
// vector-index plugin's plan-rewrite body can use them without taking a
// direct dependency on pkg/sql/plan.
func init() {
	vectorplan.DeepCopyExpr = DeepCopyExpr
	vectorplan.DeepCopyColDefList = DeepCopyColDefList
	vectorplan.MakePlan2StringConstExprWithType = makePlan2StringConstExprWithType
	vectorplan.BuildFilterPredicateJSON = buildFilterPredicateJSON
	vectorplan.ParseIncludedColumnsFromParams = parseIncludedColumnsFromParams
	vectorplan.ReplaceDistFnExprsWithScoreCol = replaceDistFnExprsWithScoreCol
	vectorplan.CalculatePostFilterOverFetchFactor = calculatePostFilterOverFetchFactor

	vectorplan.CreateIndexDef = CreateIndexDef
	vectorplan.MakeHiddenColDefByName = MakeHiddenColDefByName
	vectorplan.ValidateIncludeColumns = validateIncludeColumnsForPlugin
	vectorplan.VectorSearchProviderChildren = vectorSearchProviderChildrenForPlugin
}

// vectorSearchProviderChildrenForPlugin adapts vectorSearchProviderChildren
// (which takes *vectorSortContext) to vectorplan.VectorSortContext.
func vectorSearchProviderChildrenForPlugin(vc *vectorplan.VectorSortContext) []int32 {
	if vc == nil {
		return nil
	}
	return vectorSearchProviderChildren(&vectorSortContext{
		providerNodeID: vc.ProviderNodeID,
		vecArgExpr:     vc.VecArgExpr,
	})
}

// validateIncludeColumnsForPlugin adapts validateIncludeColumns to the
// narrower vectorplan.CompilerContext the plugin uses. Dispatch always
// passes a real *plan.CompilerContext, so the assertion is total at call
// time; the second return is only for the compiler's exhaustiveness.
func validateIncludeColumnsForPlugin(ctx vectorplan.CompilerContext,
	includeCols []*tree.UnresolvedName, colMap map[string]*plan.ColDef,
	vecColName, pkeyName string) error {
	return validateIncludeColumns(ctx.(CompilerContext), includeCols, colMap, vecColName, pkeyName)
}

// QueryBuilder facade methods. These make *QueryBuilder satisfy
// vectorplan.PlanBuilder so the plugin can drive plan construction without
// importing pkg/sql/plan.

func (builder *QueryBuilder) GenNewBindTag() int32 { return builder.genNewBindTag() }

func (builder *QueryBuilder) AppendNode(node *plan.Node, ctx vectorplan.BindContext) int32 {
	bc, _ := ctx.(*BindContext)
	return builder.appendNode(node, bc)
}

func (builder *QueryBuilder) AddBinding(nodeID int32, alias tree.AliasClause, ctx vectorplan.BindContext) error {
	bc, _ := ctx.(*BindContext)
	return builder.addBinding(nodeID, alias, bc)
}

func (builder *QueryBuilder) CtxByNode(id int32) vectorplan.BindContext {
	if int(id) < 0 || int(id) >= len(builder.ctxByNode) {
		return nil
	}
	return builder.ctxByNode[id]
}

func (builder *QueryBuilder) Query() *plan.Query { return builder.qry }

// GetContext is already an exported method elsewhere; the receiver alias
// here is a no-op compile-time interface check anchor.
var _ context.Context = context.TODO()

func (builder *QueryBuilder) ResolveVariable(name string, isSystemVar, isGlobalVar bool) (any, error) {
	return builder.compCtx.ResolveVariable(name, isSystemVar, isGlobalVar)
}

// ValidateVectorIndexSortRewrite is a thin facade that takes the exported
// vectorplan.VectorSortContext (mirrors the unexported vectorSortContext
// used internally). Only the sortDirection field is actually inspected, so
// the copy is cheap.
func (builder *QueryBuilder) ValidateVectorIndexSortRewrite(vc *vectorplan.VectorSortContext) (bool, error) {
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

func (builder *QueryBuilder) GetArgsFromDistFn(distFn *plan.Function, partPos int32) (*plan.Expr, *plan.Expr, bool) {
	return builder.getArgsFromDistFn(distFn, partPos)
}

func (builder *QueryBuilder) GetArgsFromDistFnForJoin(distFn *plan.Function, partPos, scanTag int32) (*plan.Expr, *plan.Expr, bool) {
	return builder.getArgsFromDistFnForJoin(distFn, partPos, scanTag)
}

func (builder *QueryBuilder) PeelAndRewriteDistFnFilters(
	filters []*plan.Expr, partPos int32, origFuncName string,
	vecLitArg *plan.Expr, tableFuncTag int32, scoreColType plan.Type,
) (newFilters, peeled []*plan.Expr) {
	return builder.peelAndRewriteDistFnFilters(filters, partPos, origFuncName, vecLitArg, tableFuncTag, scoreColType)
}

func (builder *QueryBuilder) BindFuncByName(name string, args []*plan.Expr) (*plan.Expr, error) {
	return BindFuncExprImplByPlanExpr(builder.GetContext(), name, args)
}

func (builder *QueryBuilder) ReplaceColumnsForNode(node *plan.Node, projMap map[[2]int32]*plan.Expr) {
	replaceColumnsForNode(node, projMap)
}

// export converts the package-private vectorSortContext into the exported
// vectorplan.VectorSortContext that crosses the plugin boundary.
func (v *vectorSortContext) export() *vectorplan.VectorSortContext {
	if v == nil {
		return nil
	}
	return &vectorplan.VectorSortContext{
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
