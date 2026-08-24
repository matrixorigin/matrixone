// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"bytes"
	"math"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

var (
	constTrue = &plan.Expr{
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_Bval{
					Bval: true,
				},
			},
		},
		Typ: plan.Type{
			Id:          int32(types.T_bool),
			NotNullable: true,
		},
	}

	constFalse = &plan.Expr{
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_Bval{
					Bval: false,
				},
			},
		},
		Typ: plan.Type{
			Id:          int32(types.T_bool),
			NotNullable: true,
		},
	}
)

func (builder *QueryBuilder) flattenSubqueries(nodeID int32, expr *plan.Expr, ctx *BindContext) (int32, *plan.Expr, error) {
	return builder.flattenSubqueriesWithContext(nodeID, expr, ctx, false)
}

func (builder *QueryBuilder) flattenFilterSubqueries(nodeID int32, expr *plan.Expr, ctx *BindContext) (int32, *plan.Expr, error) {
	return builder.flattenSubqueriesWithContext(nodeID, expr, ctx, true)
}

// flattenOuterJoinConditionSubqueries decorrelates each subquery against the
// one JOIN input it references, then leaves the rewritten boolean expression
// in the outer join's OnList.  Decorating an input with SINGLE/LEFT/MARK joins
// is row preserving; applying a FILTER above the outer join is not, because it
// would discard the NULL-extended row when every candidate fails the original
// ON predicate.
//
// A subquery that references both inputs cannot be placed below either input
// without first assigning a stable identity to every candidate pair.  Keep
// rejecting that shape instead of silently changing outer-join semantics.
func (builder *QueryBuilder) flattenOuterJoinConditionSubqueries(
	leftID, rightID int32,
	expr *plan.Expr,
	leftCtx, rightCtx *BindContext,
	leftTags, rightTags map[int32]bool,
	defaultSide int8,
	nullResultRejected bool,
) (int32, int32, *plan.Expr, error) {
	var err error

	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		childNullResultRejected := nullResultRejected && nullPropagatesThroughDeepScalarConsumer(exprImpl.F.Func)
		for i, arg := range exprImpl.F.Args {
			leftID, rightID, exprImpl.F.Args[i], err = builder.flattenOuterJoinConditionSubqueries(
				leftID, rightID, arg, leftCtx, rightCtx, leftTags, rightTags, defaultSide, childNullResultRejected)
			if err != nil {
				return 0, 0, nil, err
			}
		}

	case *plan.Expr_List:
		for i, item := range exprImpl.List.List {
			leftID, rightID, exprImpl.List.List[i], err = builder.flattenOuterJoinConditionSubqueries(
				leftID, rightID, item, leftCtx, rightCtx, leftTags, rightTags, defaultSide, nullResultRejected)
			if err != nil {
				return 0, 0, nil, err
			}
		}

	case *plan.Expr_Sub:
		side := builder.outerJoinSubqueryInputSide(exprImpl.Sub, leftTags, rightTags)
		if side&JoinSideOuter != 0 {
			return 0, 0, nil, moerr.NewNYI(
				builder.GetContext(),
				"deeply correlated subquery in outer JOIN condition",
			)
		}
		switch side & JoinSideBoth {
		case JoinSideNone:
			side = defaultSide
		case JoinSideBoth:
			return 0, 0, nil, moerr.NewNYI(
				builder.GetContext(),
				"subquery in outer JOIN condition referencing both join inputs",
			)
		}

		if side&JoinSideLeft != 0 {
			leftID, expr, err = builder.flattenSubquery(leftID, exprImpl.Sub, leftCtx, nullResultRejected)
		} else {
			rightID, expr, err = builder.flattenSubquery(rightID, exprImpl.Sub, rightCtx, nullResultRejected)
		}
	}

	return leftID, rightID, expr, err
}

// outerJoinSubqueryInputSide reports which immediate JOIN input supplies the
// values consumed while flattenSubquery executes.  Correlated references live
// in the subquery plan; quantified subqueries also keep their left operand in
// SubqueryRef.Child, where the outer references are ordinary ColRefs.
func (builder *QueryBuilder) outerJoinSubqueryInputSide(
	subquery *plan.SubqueryRef,
	leftTags, rightTags map[int32]bool,
) int8 {
	if subquery == nil {
		return JoinSideBoth
	}

	side := outerJoinExprInputSide(subquery.Child, leftTags, rightTags, true)
	visited := make(map[int32]struct{})

	var visitNode func(int32)
	visitNode = func(nodeID int32) {
		if nodeID < 0 || int(nodeID) >= len(builder.qry.Nodes) {
			side |= JoinSideBoth
			return
		}
		if _, ok := visited[nodeID]; ok {
			return
		}
		visited[nodeID] = struct{}{}

		node := builder.qry.Nodes[nodeID]
		for _, childID := range node.Children {
			visitNode(childID)
		}

		visitExpr := func(expr *plan.Expr) {
			side |= outerJoinExprInputSide(expr, leftTags, rightTags, false)
		}
		visitExprList := func(exprs []*plan.Expr) {
			for _, expr := range exprs {
				visitExpr(expr)
			}
		}

		visitExpr(node.Limit)
		visitExpr(node.Offset)
		visitExpr(node.Interval)
		visitExpr(node.Sliding)
		visitExpr(node.Timestamp)
		visitExpr(node.WEnd)
		visitExprList(node.OnList)
		visitExprList(node.FilterList)
		visitExprList(node.ProjectList)
		visitExprList(node.GroupBy)
		visitExprList(node.AggList)
		visitExprList(node.WinSpecList)
		visitExprList(node.TblFuncExprList)
		visitExprList(node.BlockFilterList)
		visitExprList(node.FillVal)
		visitExprList(node.OnUpdateExprs)
		visitExprList(node.TimeWindowPartitionBy)
		for _, orderBy := range node.OrderBy {
			visitExpr(orderBy.Expr)
		}
		if param := node.IndexReaderParam; param != nil {
			visitExpr(param.Limit)
			for _, orderBy := range param.OrderBy {
				visitExpr(orderBy.Expr)
			}
			if param.DistRange != nil {
				visitExpr(param.DistRange.LowerBound)
				visitExpr(param.DistRange.UpperBound)
			}
		}
	}

	visitNode(subquery.NodeId)
	return side
}

func outerJoinExprInputSide(
	expr *plan.Expr,
	leftTags, rightTags map[int32]bool,
	includeCols bool,
) int8 {
	if expr == nil {
		return JoinSideNone
	}

	var side int8
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		if includeCols {
			side |= outerJoinTagInputSide(exprImpl.Col.RelPos, leftTags, rightTags)
		}
	case *plan.Expr_Corr:
		if exprImpl.Corr.Depth == 1 {
			side |= outerJoinTagInputSide(exprImpl.Corr.RelPos, leftTags, rightTags)
		} else {
			// The correlation belongs to an enclosing query block, so neither
			// immediate JOIN input can host the decorrelation safely.
			side |= JoinSideOuter
		}
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			side |= outerJoinExprInputSide(arg, leftTags, rightTags, includeCols)
		}
	case *plan.Expr_Lit:
		side |= outerJoinExprInputSide(exprImpl.Lit.Src, leftTags, rightTags, includeCols)
	case *plan.Expr_List:
		for _, item := range exprImpl.List.List {
			side |= outerJoinExprInputSide(item, leftTags, rightTags, includeCols)
		}
	case *plan.Expr_Sub:
		if exprImpl.Sub != nil {
			side |= outerJoinExprInputSide(exprImpl.Sub.Child, leftTags, rightTags, includeCols)
		}
	case *plan.Expr_W:
		side |= outerJoinExprInputSide(exprImpl.W.WindowFunc, leftTags, rightTags, includeCols)
		for _, item := range exprImpl.W.PartitionBy {
			side |= outerJoinExprInputSide(item, leftTags, rightTags, includeCols)
		}
		for _, orderBy := range exprImpl.W.OrderBy {
			side |= outerJoinExprInputSide(orderBy.Expr, leftTags, rightTags, includeCols)
		}
		if exprImpl.W.Frame != nil {
			if exprImpl.W.Frame.Start != nil {
				side |= outerJoinExprInputSide(exprImpl.W.Frame.Start.Val, leftTags, rightTags, includeCols)
			}
			if exprImpl.W.Frame.End != nil {
				side |= outerJoinExprInputSide(exprImpl.W.Frame.End.Val, leftTags, rightTags, includeCols)
			}
		}
	}
	return side
}

func outerJoinTagInputSide(tag int32, leftTags, rightTags map[int32]bool) int8 {
	var side int8
	if leftTags[tag] {
		side |= JoinSideLeft
	}
	if rightTags[tag] {
		side |= JoinSideRight
	}
	if side == JoinSideNone {
		// An immediate correlation must resolve to one of the two inputs.
		// Treat an unknown tag conservatively as unplaceable.
		side = JoinSideBoth
	}
	return side
}

func (builder *QueryBuilder) flattenSubqueriesWithContext(
	nodeID int32,
	expr *plan.Expr,
	ctx *BindContext,
	nullResultRejected bool,
) (int32, *plan.Expr, error) {
	memoID := expr.AuxId
	if memoID < 0 && ctx != nil && ctx.flattenedVolatileExprs != nil {
		if flattened, ok := ctx.flattenedVolatileExprs[memoID]; ok {
			copy := DeepCopyExpr(flattened)
			copy.AuxId = memoID
			return nodeID, copy, nil
		}
	}
	var err error

	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		childNullResultRejected := nullResultRejected && nullPropagatesThroughDeepScalarConsumer(exprImpl.F.Func)
		for i, arg := range exprImpl.F.Args {
			nodeID, exprImpl.F.Args[i], err = builder.flattenSubqueriesWithContext(nodeID, arg, ctx, childNullResultRejected)
			if err != nil {
				return 0, nil, err
			}
		}

	case *plan.Expr_Sub:
		nodeID, expr, err = builder.flattenSubquery(nodeID, exprImpl.Sub, ctx, nullResultRejected)
	}
	if err == nil && memoID < 0 && ctx != nil {
		expr.AuxId = memoID
		if ctx.flattenedVolatileExprs == nil {
			ctx.flattenedVolatileExprs = make(map[int32]*plan.Expr)
		}
		ctx.flattenedVolatileExprs[memoID] = DeepCopyExpr(expr)
	}

	return nodeID, expr, err
}

func (builder *QueryBuilder) flattenSubquery(
	nodeID int32,
	subquery *plan.SubqueryRef,
	ctx *BindContext,
	nullResultRejected bool,
) (int32, *plan.Expr, error) {
	if subquery.Child != nil && hasSubquery(subquery.Child) {
		return 0, nil, moerr.NewNotSupported(builder.GetContext(), "a quantified subquery's left operand can't contain subquery")
	}

	subID := subquery.NodeId
	subCtx := builder.ctxByNode[subID]
	var scalarMatch *plan.Expr
	var scalarOuterResult *plan.Expr
	var scalarExistential bool

	// Strip unnecessary subqueries which have no FROM clause
	subNode := builder.qry.Nodes[subID]
	if subNode.NodeType == plan.Node_PROJECT &&
		builder.qry.Nodes[subNode.Children[0]].NodeType == plan.Node_VALUE_SCAN &&
		builder.qry.Nodes[subNode.Children[0]].TableDef == nil {
		switch subquery.Typ {
		case plan.SubqueryRef_SCALAR:
			newProj, _ := decreaseDepth(subNode.ProjectList[0])
			return nodeID, newProj, nil

		case plan.SubqueryRef_EXISTS:
			return nodeID, constTrue, nil

		case plan.SubqueryRef_NOT_EXISTS:
			return nodeID, constFalse, nil

		case plan.SubqueryRef_IN:
			newExpr, err := builder.generateRowComparison("=", subquery.Child, subCtx, true)
			if err != nil {
				return 0, nil, err
			}

			return nodeID, newExpr, nil

		case plan.SubqueryRef_NOT_IN:
			newExpr, err := builder.generateRowComparison("<>", subquery.Child, subCtx, true)
			if err != nil {
				return 0, nil, err
			}

			return nodeID, newExpr, nil

		case plan.SubqueryRef_ANY, plan.SubqueryRef_ALL:
			newExpr, err := builder.generateRowComparison(subquery.Op, subquery.Child, subCtx, true)
			if err != nil {
				return 0, nil, err
			}

			return nodeID, newExpr, nil

		default:
			return 0, nil, moerr.NewNotSupportedf(builder.GetContext(), "%s subquery not supported", subquery.Typ.String())
		}
	}

	if subquery.Typ == plan.SubqueryRef_SCALAR {
		subID, scalarMatch, scalarOuterResult, scalarExistential =
			builder.normalizeDirectCorrelatedScalarProjection(subID, subCtx)
	}

	subID, preds, err := builder.pullupCorrelatedPredicates(subID, subCtx, subquery.Typ, true)
	if err != nil {
		return 0, nil, err
	}

	// When a scalar aggregate subquery has non-equality correlated predicates,
	// pullupThroughAgg forces inner expressions into GROUP BY, producing
	// multiple rows per outer row and breaking SINGLE JOIN semantics.
	// Fix: bypass the inner AGG, use LEFT JOIN, and re-aggregate on top.
	if subquery.Typ == plan.SubqueryRef_SCALAR && len(subCtx.aggregates) > 0 && builder.findNonEqPred(preds) {
		return builder.flattenScalarSubqueryWithNonEqAgg(nodeID, subID, subCtx, preds, ctx)
	}

	filterPreds, joinPreds := decreaseDepthAndDispatch(preds)
	if subquery.Typ == plan.SubqueryRef_SCALAR && len(subCtx.aggregates) > 0 {
		builder.pushdownScalarAggregateKeys(subID, joinPreds, ctx)
	}

	if len(filterPreds) > 0 {
		deepScalarAggregate := subquery.Typ == plan.SubqueryRef_SCALAR &&
			subCtx.hasSingleRow && len(subCtx.groups) == 0 && len(subCtx.aggregates) > 0 &&
			scalarAggregateResultReturnsNullOnEmpty(subCtx) && nullResultRejected &&
			builder.scalarAggregatePlanSupportsDeepCorrelation(subID, subCtx.aggregateTag)
		if !deepScalarAggregate && !canPullupDeepCorrelatedPredicates(subquery.Typ) {
			return 0, nil, moerr.NewNYIf(builder.GetContext(), "correlated columns in %s subquery deeper than 1 level will be supported in future version", subquery.Typ.String())
		}
		// MARK JOIN only exposes its marker, so a predicate that still refers
		// to the inner relation cannot be moved above it.  A scalar aggregate
		// is different: pulling the predicate through the aggregate has already
		// turned its inner columns into grouping keys, and LEFT JOIN preserves
		// those keys so the enclosing subquery can pull them up again.
		if !deepScalarAggregate && builder.hasInnerColumnInDeepCorrelatedFilters(subID, filterPreds) {
			return 0, nil, moerr.NewNYIf(builder.GetContext(), "deep correlated predicate containing inner columns cannot be pulled above mark join")
		}
	}

	switch subquery.Typ {
	case plan.SubqueryRef_SCALAR:
		var rewriteCount bool

		// Preserve the legacy COUNT fallback for plan shapes that cannot use the
		// more precise empty-input projection reconstruction below.
		if len(joinPreds) > 0 && builder.findAggrCount(subCtx.aggregates) {
			rewriteCount = true
		}

		if scalarExistential {
			if len(joinPreds) == 0 {
				joinPreds = append(joinPreds, constTrue)
			}
			var retExpr *plan.Expr
			nodeID, retExpr, err = builder.insertMarkJoin(nodeID, subID, joinPreds, nil, false, ctx)
			if err != nil {
				return 0, nil, err
			}
			if len(filterPreds) > 0 {
				nodeID = builder.appendNode(&plan.Node{
					NodeType:   plan.Node_FILTER,
					Children:   []int32{nodeID},
					FilterList: filterPreds,
				}, ctx)
			}
			retExpr, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "case", []*plan.Expr{
				retExpr,
				scalarOuterResult,
				makePlan2NullConstExprWithType(),
			})
			return nodeID, retExpr, err
		}

		joinType := plan.Node_SINGLE
		if subCtx.hasSingleRow {
			joinType = plan.Node_LEFT
		}

		postJoinProjection, finalizeProjection, err :=
			builder.prepareCorrelatedScalarAggregatePostJoinProjection(subID, subCtx, joinPreds)
		if err != nil {
			return nodeID, nil, err
		}

		nodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{nodeID, subID},
			JoinType: joinType,
			OnList:   joinPreds,
			SpillMem: builder.joinSpillMem,
		}, ctx)

		if len(filterPreds) > 0 {
			nodeID = builder.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{nodeID},
				FilterList: filterPreds,
			}, ctx)
		}

		retExpr := scalarMatch
		if finalizeProjection {
			retExpr = postJoinProjection
		} else if retExpr == nil {
			retExpr = &plan.Expr{
				Typ: subCtx.results[0].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: subCtx.topTag(),
						ColPos: 0,
					},
				},
			}
		}
		if scalarOuterResult != nil {
			retExpr, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "case", []*plan.Expr{
				retExpr,
				scalarOuterResult,
				makePlan2NullConstExprWithType(),
			})
			if err != nil {
				return 0, nil, err
			}
		}
		if !finalizeProjection && rewriteCount {
			argsType := make([]types.Type, 1)
			argsType[0] = makeTypeByPlan2Expr(retExpr)
			fGet, err := function.GetFunctionByName(builder.GetContext(), "isnull", argsType)
			if err != nil {
				return nodeID, retExpr, err
			}
			funcID, returnType := fGet.GetEncodedOverloadID(), fGet.GetReturnType()
			isNullExpr := &Expr{
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: getFunctionObjRef(funcID, "isnull"),
						Args: []*Expr{retExpr},
					},
				},
				Typ: makePlan2Type(&returnType),
			}
			zeroExpr := makePlan2Int64ConstExprWithType(0)
			argsType = make([]types.Type, 3)
			argsType[0] = makeTypeByPlan2Expr(isNullExpr)
			argsType[1] = makeTypeByPlan2Expr(zeroExpr)
			argsType[2] = makeTypeByPlan2Expr(retExpr)
			fGet, err = function.GetFunctionByName(builder.GetContext(), "case", argsType)
			if err != nil {
				return nodeID, retExpr, nil
			}
			funcID, returnType = fGet.GetEncodedOverloadID(), fGet.GetReturnType()
			retExpr = &Expr{
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: getFunctionObjRef(funcID, "case"),
						Args: []*Expr{isNullExpr, zeroExpr, DeepCopyExpr(retExpr)},
					},
				},
				Typ: makePlan2Type(&returnType),
			}
		}
		return nodeID, retExpr, nil

	case plan.SubqueryRef_EXISTS:
		// Uncorrelated subquery
		if len(joinPreds) == 0 {
			joinPreds = append(joinPreds, constTrue)
		}

		var markExpr *plan.Expr
		nodeID, markExpr, err = builder.insertMarkJoin(nodeID, subID, joinPreds, nil, false, ctx)
		if err != nil {
			return 0, nil, err
		}
		nodeID = builder.appendDeepCorrelatedFilters(nodeID, filterPreds, ctx)
		return nodeID, markExpr, nil

	case plan.SubqueryRef_NOT_EXISTS:
		// Uncorrelated subquery
		if len(joinPreds) == 0 {
			joinPreds = append(joinPreds, constTrue)
		}

		var markExpr *plan.Expr
		nodeID, markExpr, err = builder.insertMarkJoin(nodeID, subID, joinPreds, nil, true, ctx)
		if err != nil {
			return 0, nil, err
		}
		nodeID = builder.appendDeepCorrelatedFilters(nodeID, filterPreds, ctx)
		return nodeID, markExpr, nil

	case plan.SubqueryRef_IN:
		outerPred, err := builder.generateRowComparison("=", subquery.Child, subCtx, false)
		if err != nil {
			return 0, nil, err
		}

		var markExpr *plan.Expr
		nodeID, markExpr, err = builder.insertMarkJoin(nodeID, subID, joinPreds, outerPred, false, ctx)
		if err != nil {
			return 0, nil, err
		}
		nodeID = builder.appendDeepCorrelatedFilters(nodeID, filterPreds, ctx)
		return nodeID, markExpr, nil

	case plan.SubqueryRef_NOT_IN:
		outerPred, err := builder.generateRowComparison("=", subquery.Child, subCtx, false)
		if err != nil {
			return 0, nil, err
		}

		var markExpr *plan.Expr
		nodeID, markExpr, err = builder.insertMarkJoin(nodeID, subID, joinPreds, outerPred, true, ctx)
		if err != nil {
			return 0, nil, err
		}
		nodeID = builder.appendDeepCorrelatedFilters(nodeID, filterPreds, ctx)
		return nodeID, markExpr, nil

	case plan.SubqueryRef_ANY:
		outerPred, err := builder.generateRowComparison(subquery.Op, subquery.Child, subCtx, false)
		if err != nil {
			return 0, nil, err
		}

		var markExpr *plan.Expr
		nodeID, markExpr, err = builder.insertMarkJoin(nodeID, subID, joinPreds, outerPred, false, ctx)
		if err != nil {
			return 0, nil, err
		}
		nodeID = builder.appendDeepCorrelatedFilters(nodeID, filterPreds, ctx)
		return nodeID, markExpr, nil

	case plan.SubqueryRef_ALL:
		outerPred, err := builder.generateRowComparison(subquery.Op, subquery.Child, subCtx, false)
		if err != nil {
			return 0, nil, err
		}

		outerPred, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "not", []*plan.Expr{outerPred})
		if err != nil {
			return 0, nil, err
		}

		var markExpr *plan.Expr
		nodeID, markExpr, err = builder.insertMarkJoin(nodeID, subID, joinPreds, outerPred, true, ctx)
		if err != nil {
			return 0, nil, err
		}
		nodeID = builder.appendDeepCorrelatedFilters(nodeID, filterPreds, ctx)
		return nodeID, markExpr, nil

	default:
		return 0, nil, moerr.NewNotSupportedf(builder.GetContext(), "%s subquery not supported", subquery.Typ.String())
	}
}

// normalizeDirectCorrelatedScalarProjection handles a scalar subquery whose
// only result is a direct reference to the outer row. Rim projections, sorting,
// DISTINCT, and literal positive LIMITs do not change that value. Removing them
// before pulling up predicates keeps join-key references on the real projection
// instead of leaving correlated expressions in executable wrapper nodes.
func (builder *QueryBuilder) normalizeDirectCorrelatedScalarProjection(
	subID int32,
	ctx *BindContext,
) (int32, *plan.Expr, *plan.Expr, bool) {
	if len(ctx.results) != 1 || len(ctx.projects) == 0 {
		return subID, nil, nil, false
	}

	projectCorr := ctx.projects[0].GetCorr()
	if projectCorr == nil || projectCorr.Depth != 1 {
		return subID, nil, nil, false
	}
	if !builder.casePreservesType(ctx.projects[0]) {
		return subID, nil, nil, false
	}

	nodeID := subID
	existential := false
	for {
		node := builder.qry.Nodes[nodeID]
		if node.Offset != nil || node.RankOption != nil {
			return subID, nil, nil, false
		}
		if node.Limit != nil {
			limit, ok := getLiteralUint64(node.Limit)
			if !ok || limit == 0 {
				return subID, nil, nil, false
			}
			if limit == 1 {
				existential = true
			}
		}

		if node.NodeType == plan.Node_PROJECT && len(node.BindingTags) > 0 && node.BindingTags[0] == ctx.projectTag {
			if len(node.ProjectList) == 0 {
				return subID, nil, nil, false
			}
			corr := node.ProjectList[0].GetCorr()
			if corr == nil || corr.RelPos != projectCorr.RelPos || corr.ColPos != projectCorr.ColPos || corr.Depth != projectCorr.Depth {
				return subID, nil, nil, false
			}

			outerResult, _ := decreaseDepth(DeepCopyExpr(ctx.projects[0]))
			marker := DeepCopyExpr(constTrue)
			node.ProjectList = []*plan.Expr{marker}
			ctx.projects = []*plan.Expr{marker}
			node.Limit = nil
			return nodeID, GetColExpr(marker.Typ, ctx.projectTag, 0), outerResult, existential
		}

		if len(node.Children) != 1 {
			return subID, nil, nil, false
		}
		switch node.NodeType {
		case plan.Node_PROJECT, plan.Node_SORT:
		case plan.Node_DISTINCT:
			existential = true
		default:
			return subID, nil, nil, false
		}
		nodeID = node.Children[0]
	}
}

func (builder *QueryBuilder) casePreservesType(expr *plan.Expr) bool {
	sourceType := makeTypeByPlan2Expr(expr)
	caseFn, err := function.GetFunctionByName(builder.GetContext(), "case", []types.Type{
		types.T_bool.ToType(),
		sourceType,
		types.T_any.ToType(),
	})
	return err == nil && caseFn.GetReturnType().Eq(sourceType)
}

func (builder *QueryBuilder) insertMarkJoin(left, right int32, joinPreds []*plan.Expr, outerPred *plan.Expr, negate bool, ctx *BindContext) (nodeID int32, markExpr *plan.Expr, err error) {
	markTag := builder.genNewBindTag()

	for i, pred := range joinPreds {
		if !pred.Typ.NotNullable {
			joinPreds[i], err = BindFuncExprImplByPlanExpr(builder.GetContext(), "istrue", []*plan.Expr{pred})
			if err != nil {
				return
			}
		}
	}

	notNull := true

	if outerPred != nil {
		joinPreds = append(joinPreds, outerPred)
		notNull = outerPred.Typ.NotNullable
	}

	nodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_JOIN,
		Children:    []int32{left, right},
		BindingTags: []int32{markTag},
		JoinType:    plan.Node_MARK,
		OnList:      joinPreds,
		SpillMem:    builder.joinSpillMem,
	}, ctx)

	markExpr = &plan.Expr{
		Typ: plan.Type{
			Id:          int32(types.T_bool),
			NotNullable: notNull,
		},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: markTag,
				ColPos: 0,
			},
		},
	}

	if negate {
		markExpr, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "not", []*plan.Expr{markExpr})
	}

	return
}

func canPullupDeepCorrelatedPredicates(typ plan.SubqueryRef_Type) bool {
	switch typ {
	case plan.SubqueryRef_EXISTS, plan.SubqueryRef_NOT_EXISTS, plan.SubqueryRef_IN, plan.SubqueryRef_NOT_IN,
		plan.SubqueryRef_ANY, plan.SubqueryRef_ALL:
		return true
	default:
		return false
	}
}

func (builder *QueryBuilder) hasInnerColumnInDeepCorrelatedFilters(subID int32, filterPreds []*plan.Expr) bool {
	if len(filterPreds) == 0 {
		return false
	}

	innerTags := builder.collectBindingTags(builder.qry.Nodes[subID])
	for _, pred := range filterPreds {
		for tag := range innerTags {
			if containsTag(pred, tag) {
				return true
			}
		}
	}
	return false
}

func (builder *QueryBuilder) appendDeepCorrelatedFilters(nodeID int32, filterPreds []*plan.Expr, ctx *BindContext) int32 {
	if len(filterPreds) == 0 {
		return nodeID
	}

	return builder.appendNode(&plan.Node{
		NodeType:   plan.Node_FILTER,
		Children:   []int32{nodeID},
		FilterList: filterPreds,
	}, ctx)
}

func getProjectExpr(idx int, ctx *BindContext, strip bool) *plan.Expr {
	if strip {
		newProj, _ := decreaseDepth(ctx.results[idx])
		return newProj
	} else {
		return &plan.Expr{
			Typ: ctx.results[idx].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: ctx.rootTag(),
					ColPos: int32(idx),
				},
			},
		}
	}
}

func (builder *QueryBuilder) generateRowComparison(op string, child *plan.Expr, ctx *BindContext, strip bool) (*plan.Expr, error) {
	switch childImpl := child.Expr.(type) {
	case *plan.Expr_List:
		childList := childImpl.List.List
		if len(childList) == 0 {
			return nil, moerr.NewInternalError(builder.GetContext(), "row comparison requires at least one column")
		}
		switch op {
		case "=", "<>":
			logicalOp := "and"
			if op == "<>" {
				logicalOp = "or"
			}

			comparisons := make([]*plan.Expr, len(childList))
			for i := range childList {
				comparison, err := BindFuncExprImplByPlanExpr(builder.GetContext(), op, []*plan.Expr{
					childList[i],
					getProjectExpr(i, ctx, strip),
				})
				if err != nil {
					return nil, err
				}
				comparisons[i] = comparison
			}

			return combinePlanExprsBalanced(builder.GetContext(), logicalOp, comparisons)

		case "<", "<=", ">", ">=":
			projList := make([]*plan.Expr, len(childList))
			for i := range projList {
				projList[i] = getProjectExpr(i, ctx, strip)

			}

			nonEqOp := op[:1] // <= -> <, >= -> >
			return unwindTupleComparison(builder.GetContext(), nonEqOp, op, childList, projList, 0)

		default:
			return nil, moerr.NewNotSupported(builder.GetContext(), "row constructor only support comparison operators")
		}

	default:
		return BindFuncExprImplByPlanExpr(builder.GetContext(), op, []*plan.Expr{
			child,
			getProjectExpr(0, ctx, strip),
		})
	}
}

// prepareCorrelatedScalarAggregatePostJoinProjection moves the scalar final
// expression above the LEFT JOIN used to decorrelate an implicit single-group
// aggregate. pullupThroughAgg groups the inner input by the correlation key, so
// a missing key produces no right row. Evaluating COALESCE, arithmetic, or CASE
// below the join would therefore skip the expression for that outer row.
//
// The right projection is rewritten to expose only raw aggregate outputs and
// the correlation keys that pullupThroughProj already appended. Aggregate
// outputs are restored from aggexec's canonical empty-input contract after null
// extension. The saved final expression is then evaluated against those
// post-join values.
//
// This is intentionally limited to a direct AGG or the ordinary PROJECT -> AGG
// shape. Wrappers that can remove or reorder the aggregate row (for example
// HAVING, DISTINCT, SORT, or LIMIT) keep the legacy path.
func (builder *QueryBuilder) prepareCorrelatedScalarAggregatePostJoinProjection(
	subID int32,
	subCtx *BindContext,
	joinPreds []*plan.Expr,
) (*plan.Expr, bool, error) {
	if !subCtx.hasSingleRow || len(subCtx.groups) != 0 || len(subCtx.aggregates) == 0 || len(joinPreds) == 0 {
		return nil, false, nil
	}

	project := builder.qry.Nodes[subID]
	if project.NodeType == plan.Node_AGG {
		if len(project.BindingTags) < 2 || project.BindingTags[1] != subCtx.aggregateTag ||
			len(project.AggList) != 1 || len(subCtx.aggregates) != 1 || len(subCtx.results) != 1 {
			return nil, false, nil
		}
		aggregate := project.AggList[0]
		fn := aggregate.GetF()
		if fn == nil || fn.Func == nil {
			return nil, false, nil
		}
		projected := GetColExpr(aggregate.Typ, subCtx.aggregateTag, 0)
		projected.Typ.NotNullable = false
		postJoinProjection, err := builder.restoreAggregateEmptyResult(projected, aggregate, fn.Func.ObjName)
		return postJoinProjection, err == nil, err
	}
	if project.NodeType != plan.Node_PROJECT || len(project.Children) != 1 || len(project.BindingTags) != 1 ||
		len(project.ProjectList) == 0 || project.Limit != nil || project.Offset != nil || project.RankOption != nil {
		return nil, false, nil
	}

	agg := builder.qry.Nodes[project.Children[0]]
	if agg.NodeType != plan.Node_AGG || len(agg.BindingTags) < 2 || agg.BindingTags[1] != subCtx.aggregateTag ||
		len(agg.AggList) != len(subCtx.aggregates) {
		return nil, false, nil
	}

	projectTag := project.BindingTags[0]
	projectedAggregates := make([]*plan.Expr, len(agg.AggList))
	rawAggregates := make([]*plan.Expr, len(agg.AggList))
	firstAppendedPos := int32(len(project.ProjectList))
	for i, aggregate := range agg.AggList {
		fn := aggregate.GetF()
		if fn == nil || fn.Func == nil {
			return nil, false, nil
		}

		projectPos := int32(0)
		if i > 0 {
			projectPos = firstAppendedPos + int32(i-1)
		}
		rawAggregates[i] = GetColExpr(aggregate.Typ, subCtx.aggregateTag, int32(i))
		projected := GetColExpr(aggregate.Typ, projectTag, projectPos)
		projected.Typ.NotNullable = false

		var err error
		projectedAggregates[i], err = builder.restoreAggregateEmptyResult(projected, aggregate, fn.Func.ObjName)
		if err != nil {
			return nil, false, err
		}
	}

	postJoinProjection, ok := replaceAggregateRefsForPostJoin(
		DeepCopyExpr(project.ProjectList[0]), subCtx.aggregateTag, projectedAggregates)
	if !ok {
		return nil, false, nil
	}
	postJoinProjection, stillCorrelated := decreaseDepth(postJoinProjection)
	if stillCorrelated {
		return nil, false, nil
	}

	newProjectList := make([]*plan.Expr, len(project.ProjectList), len(project.ProjectList)+len(rawAggregates)-1)
	copy(newProjectList, project.ProjectList)
	newProjectList[0] = rawAggregates[0]
	newProjectList = append(newProjectList, rawAggregates[1:]...)
	project.ProjectList = newProjectList
	return postJoinProjection, true, nil
}

func (builder *QueryBuilder) restoreAggregateEmptyResult(
	aggregateExpr *plan.Expr,
	aggregate *plan.Expr,
	aggregateName string,
) (*plan.Expr, error) {
	kind := aggexec.GetEmptyResultKind(aggregate.GetF().Func.Obj)
	if kind == aggexec.EmptyResultNull {
		return aggregateExpr, nil
	}
	if kind == aggexec.EmptyResultUnsupported {
		return nil, moerr.NewNYIf(builder.GetContext(),
			"aggregate %s in a correlated scalar projection will be supported in a future version", aggregateName)
	}

	isNullExpr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "isnull", []*plan.Expr{aggregateExpr})
	if err != nil {
		return nil, err
	}
	emptyExpr, err := makeAggregateEmptyResultExpr(kind, aggregate.Typ)
	if err != nil {
		return nil, err
	}
	return BindFuncExprImplByPlanExpr(builder.GetContext(), "case", []*plan.Expr{
		isNullExpr,
		emptyExpr,
		DeepCopyExpr(aggregateExpr),
	})
}

func makeAggregateEmptyResultExpr(kind aggexec.EmptyResultKind, aggregateType plan.Type) (*plan.Expr, error) {
	var expr *plan.Expr
	switch types.T(aggregateType.Id) {
	case types.T_binary, types.T_varbinary:
		fill := byte(0)
		if kind == aggexec.EmptyResultAllBitsSet {
			fill = 0xff
		}
		expr = makePlan2VarBinaryConstExprWithType(string(bytes.Repeat([]byte{fill}, int(aggregateType.Width))))
	case types.T_uint64:
		value := uint64(0)
		if kind == aggexec.EmptyResultAllBitsSet {
			value = math.MaxUint64
		}
		expr = makePlan2Uint64ConstExprWithType(value)
	default:
		if kind == aggexec.EmptyResultAllBitsSet {
			return nil, moerr.NewInternalErrorNoCtxf("all-bits-set empty aggregate result has unsupported type %s", makeTypeByPlan2Expr(&plan.Expr{Typ: aggregateType}))
		}
		expr = makePlan2Int64ConstExprWithType(0)
	}
	expr.Typ = aggregateType
	expr.Typ.NotNullable = true
	return expr, nil
}

func replaceAggregateRefsForPostJoin(
	expr *plan.Expr,
	aggregateTag int32,
	projectedAggregates []*plan.Expr,
) (*plan.Expr, bool) {
	if expr == nil {
		return nil, false
	}

	switch item := expr.Expr.(type) {
	case *plan.Expr_Col:
		if item.Col.RelPos != aggregateTag {
			return nil, false
		}
		if item.Col.ColPos < 0 || int(item.Col.ColPos) >= len(projectedAggregates) {
			return nil, false
		}
		return DeepCopyExpr(projectedAggregates[item.Col.ColPos]), true
	case *plan.Expr_F:
		for i, arg := range item.F.Args {
			var ok bool
			item.F.Args[i], ok = replaceAggregateRefsForPostJoin(arg, aggregateTag, projectedAggregates)
			if !ok {
				return nil, false
			}
		}
		return expr, true
	case *plan.Expr_List, *plan.Expr_W, *plan.Expr_Sub:
		return nil, false
	default:
		return expr, true
	}
}

func (builder *QueryBuilder) findAggrCount(aggrs []*plan.Expr) bool {
	for _, aggr := range aggrs {
		switch exprImpl := aggr.Expr.(type) {
		case *plan.Expr_F:
			if exprImpl.F.Func.ObjName == "count" || exprImpl.F.Func.ObjName == "starcount" {
				return true
			}
		}
	}
	return false
}

// allAggregatesReturnNullOnEmpty is deliberately conservative. Pulling a deep
// correlated predicate through an aggregate turns its inner expression into a
// GROUP BY key. If that key has no input rows, the grouped plan has no row and
// the LEFT JOIN exposes NULL. Matching the aggregate's SQL result for empty
// input is necessary but not sufficient: the complete consuming expression
// must also reject that NULL. Unknown and newly added aggregates remain on the
// NYI path until their empty-input contract is verified here.
func allAggregatesReturnNullOnEmpty(aggrs []*plan.Expr) bool {
	if len(aggrs) == 0 {
		return false
	}

	for _, aggr := range aggrs {
		f := aggr.GetF()
		if f == nil || f.Func == nil {
			return false
		}

		fid, _ := function.DecodeOverloadID(f.Func.Obj & function.DistinctMask)
		switch fid {
		case function.MIN, function.MAX, function.SUM, function.AVG, function.ANY_VALUE:
		default:
			return false
		}
	}
	return true
}

// scalarAggregateResultReturnsNullOnEmpty verifies the missing-group contract
// through the scalar subquery's own result projection. It is not enough for
// the underlying aggregates to return NULL: a projection such as
// COALESCE(MAX(...), 0) observes that NULL, while the grouped rewrite has no
// row on which to evaluate the projection at all.
func scalarAggregateResultReturnsNullOnEmpty(ctx *BindContext) bool {
	return len(ctx.projects) > 0 &&
		allAggregatesReturnNullOnEmpty(ctx.aggregates) &&
		nullPropagatesFromAggregate(ctx.projects[0], ctx.aggregateTag)
}

// scalarAggregatePlanSupportsDeepCorrelation verifies the complete path from
// the scalar root to its implicit aggregate. pullupThroughAgg adds the deep
// correlation key to GROUP BY, so row limiting that was once evaluated inside
// each scalar invocation would otherwise become global across all keys.
//
// PROJECT is the only wrapper proven to remain per-key after that rewrite.
// LIMIT, OFFSET, rank, and every other operator stay on the NYI path until they
// are explicitly rewritten or proven partition-local.
func (builder *QueryBuilder) scalarAggregatePlanSupportsDeepCorrelation(nodeID, aggregateTag int32) bool {
	for range builder.qry.Nodes {
		if nodeID < 0 || int(nodeID) >= len(builder.qry.Nodes) {
			return false
		}

		node := builder.qry.Nodes[nodeID]
		if node == nil || node.Limit != nil || node.Offset != nil || node.RankOption != nil {
			return false
		}

		switch node.NodeType {
		case plan.Node_PROJECT:
			if len(node.Children) != 1 {
				return false
			}
			nodeID = node.Children[0]

		case plan.Node_AGG:
			return len(node.BindingTags) > 1 && node.BindingTags[1] == aggregateTag

		default:
			return false
		}
	}

	return false
}

func nullPropagatesFromAggregate(expr *plan.Expr, aggregateTag int32) bool {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		return exprImpl.Col.RelPos == aggregateTag

	case *plan.Expr_F:
		if !nullPropagatesThroughDeepScalarConsumer(exprImpl.F.Func) {
			return false
		}
		for _, arg := range exprImpl.F.Args {
			if nullPropagatesFromAggregate(arg, aggregateTag) {
				return true
			}
		}
	}

	return false
}

// nullPropagatesThroughDeepScalarConsumer identifies the deliberately narrow
// set of scalar functions through which a missing deep scalar result can be
// proven to remain NULL. Combined with a FILTER root, that means both the SQL
// expression and the decorrelated plan discard the enclosing input row.
//
// Keep this list conservative. In particular, COALESCE/CASE can observe NULL,
// and logical AND is not NULL-propagating for every input combination.
func nullPropagatesThroughDeepScalarConsumer(fn *plan.ObjectRef) bool {
	if fn == nil {
		return false
	}

	fid, _ := function.DecodeOverloadID(fn.Obj)
	switch fid {
	case function.EQUAL, function.NOT_EQUAL,
		function.GREAT_THAN, function.GREAT_EQUAL,
		function.LESS_THAN, function.LESS_EQUAL,
		function.NOT, function.CAST, function.CAST_STRICT:
		return true
	default:
		return false
	}
}

func (builder *QueryBuilder) findNonEqPred(preds []*plan.Expr) bool {
	for _, pred := range preds {
		if containsNonEqComparison(pred) {
			return true
		}
	}
	return false
}

// pushdownScalarAggregateKeys limits a decorrelated scalar aggregate to keys
// that can actually occur in the outer relation. Decorrelating
//
//	... where inner.key = outer.key
//
// currently adds inner.key to the aggregate GROUP BY, but leaves the
// aggregate input independent of the filtered outer relation. A SEMI join
// provides the missing dependency without duplicating rows (which an ordinary
// INNER JOIN would do when the outer relation has duplicate keys).
//
// This is deliberately conservative: the correlation must be direct equality
// on pulled-up group keys, all outer keys must come from one base-table
// binding, and that table must have a deterministic local WHERE predicate.
// Copying only that predicate domain is safe because it is a superset of the
// outer rows that survive the complete query. Volatile, multi-table, derived,
// and unfiltered shapes retain the existing decorrelation plan.
func (builder *QueryBuilder) pushdownScalarAggregateKeys(subID int32, preds []*plan.Expr, ctx *BindContext) {
	aggNode := builder.findAggNodeBelow(subID)
	if aggNode == nil || len(aggNode.Children) != 1 || len(aggNode.BindingTags) == 0 {
		return
	}

	type keyPair struct {
		outerPos int32
		groupPos int32
	}
	pairs := make([]keyPair, 0, len(preds))
	outerTag := int32(-1)

	for _, pred := range splitPlanConjunctions(preds) {
		f := pred.GetF()
		if f == nil || f.Func == nil {
			return
		}
		if f.Func.ObjName == "istrue" && len(f.Args) == 1 {
			f = f.Args[0].GetF()
			if f == nil || f.Func == nil {
				return
			}
		}
		if len(f.Args) != 2 || (f.Func.ObjName != "=" && !IsEqualFunc(f.Func.GetObj())) {
			return
		}

		left := f.Args[0].GetCol()
		right := f.Args[1].GetCol()
		if left == nil || right == nil {
			return
		}

		leftGroupPos, leftIsGroup := builder.scalarAggregateGroupPos(subID, aggNode, left)
		rightGroupPos, rightIsGroup := builder.scalarAggregateGroupPos(subID, aggNode, right)
		var outer *plan.ColRef
		var groupPos int32
		switch {
		case leftIsGroup && !rightIsGroup:
			groupPos, outer = leftGroupPos, right
		case rightIsGroup && !leftIsGroup:
			groupPos, outer = rightGroupPos, left
		default:
			return
		}
		if groupPos < 0 || int(groupPos) >= len(aggNode.GroupBy) ||
			ctx == nil || ctx.bindingByTag[outer.RelPos] == nil {
			return
		}
		if outerTag >= 0 && outerTag != outer.RelPos {
			return
		}
		outerTag = outer.RelPos
		pairs = append(pairs, keyPair{
			outerPos: outer.ColPos,
			groupPos: groupPos,
		})
	}
	if len(pairs) == 0 {
		return
	}

	outerBinding := ctx.bindingByTag[outerTag]
	if outerBinding == nil || outerBinding.nodeId < 0 || int(outerBinding.nodeId) >= len(builder.qry.Nodes) {
		return
	}
	outerScan := builder.qry.Nodes[outerBinding.nodeId]
	if outerScan.NodeType != plan.Node_TABLE_SCAN || len(outerScan.Children) != 0 ||
		len(outerScan.BindingTags) == 0 || outerScan.BindingTags[0] != outerTag {
		return
	}

	outerTags := map[int32]bool{outerTag: true}
	domainFilters := make([]*plan.Expr, 0, len(ctx.whereFilters))
	for _, filter := range splitPlanConjunctions(ctx.whereFilters) {
		if !hasSubquery(filter) && !containsVolatileFunction(filter) &&
			containsTag(filter, outerTag) && containsOnlyTags(filter, outerTags) {
			domainFilters = append(domainFilters, DeepCopyExpr(filter))
		}
	}
	if len(domainFilters) == 0 {
		return
	}

	for _, pair := range pairs {
		if pair.outerPos < 0 || int(pair.outerPos) >= len(outerBinding.types) {
			return
		}
	}
	domainScan := DeepCopyNode(outerScan)
	domainScan.ScanSnapshot = DeepCopySnapshot(outerScan.ScanSnapshot)
	domainScan.FilterList = append(domainScan.FilterList, domainFilters...)
	builder.rebindScanNode(domainScan)
	domainTag := domainScan.BindingTags[0]
	semiPreds := make([]*plan.Expr, len(pairs))
	for i, pair := range pairs {
		innerKey := DeepCopyExpr(aggNode.GroupBy[pair.groupPos])
		rightKey := &plan.Expr{
			Typ: *outerBinding.types[pair.outerPos],
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				RelPos: domainTag,
				ColPos: pair.outerPos,
			}},
		}
		cond, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{innerKey, rightKey})
		if err != nil {
			return
		}
		semiPreds[i] = cond
	}

	domainID := builder.appendNode(domainScan, ctx)
	semiID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{aggNode.Children[0], domainID},
		JoinType: plan.Node_SEMI,
		OnList:   semiPreds,
		SpillMem: builder.joinSpillMem,
	}, ctx)
	aggNode.Children[0] = semiID
}

// scalarAggregateGroupPos resolves a pulled-up predicate column through the
// transparent unary projections above aggNode. It returns an AGG group-output
// position, never an aggregate-output position.
func (builder *QueryBuilder) scalarAggregateGroupPos(
	nodeID int32,
	aggNode *plan.Node,
	col *plan.ColRef,
) (int32, bool) {
	if col == nil || aggNode == nil || len(aggNode.BindingTags) == 0 {
		return 0, false
	}
	ref := *col
	for nodeID != aggNode.NodeId {
		if nodeID < 0 || int(nodeID) >= len(builder.qry.Nodes) {
			return 0, false
		}
		node := builder.qry.Nodes[nodeID]
		if node.NodeType == plan.Node_PROJECT && len(node.BindingTags) > 0 && ref.RelPos == node.BindingTags[0] {
			if ref.ColPos < 0 || int(ref.ColPos) >= len(node.ProjectList) {
				return 0, false
			}
			projected := node.ProjectList[ref.ColPos].GetCol()
			if projected == nil {
				return 0, false
			}
			ref = *projected
		}
		if len(node.Children) != 1 {
			return 0, false
		}
		nodeID = node.Children[0]
	}
	if ref.RelPos != aggNode.BindingTags[0] || ref.ColPos < 0 || int(ref.ColPos) >= len(aggNode.GroupBy) {
		return 0, false
	}
	return ref.ColPos, true
}

// containsNonEqComparison reports whether expr contains a comparison
// operator other than "=".  Logical operators (and/or/not) are treated
// as containers and recursed into; only the leaf comparison operators
// determine the result.
func containsNonEqComparison(expr *plan.Expr) bool {
	f, ok := expr.Expr.(*plan.Expr_F)
	if !ok {
		return false
	}
	name := f.F.Func.ObjName
	switch name {
	case "and", "or", "not":
		for _, arg := range f.F.Args {
			if containsNonEqComparison(arg) {
				return true
			}
		}
		return false
	case "<", "<=", ">", ">=", "<>", "!=":
		return true
	}
	return false
}

// flattenScalarSubqueryWithNonEqAgg handles scalar subqueries that have
// aggregation with non-equality correlated predicates.
//
// After pullupThroughAgg, inner expressions from non-eq predicates are added
// to GROUP BY, causing the AGG to produce multiple rows per outer row.
// Instead of using SINGLE JOIN (which would fail), we:
//  1. Bypass the inner AGG node
//  2. Use LEFT JOIN with all predicates applied directly
//  3. Add a new AGG on top that groups by outer columns
//
// This way the aggregate function operates on all matching raw rows,
// producing the correct result.
func (builder *QueryBuilder) flattenScalarSubqueryWithNonEqAgg(
	nodeID, subID int32, subCtx *BindContext, preds []*plan.Expr, ctx *BindContext,
) (int32, *plan.Expr, error) {
	// Find the AGG node in the subquery plan
	aggNode := builder.findAggNodeBelow(subID)
	if aggNode == nil {
		return 0, nil, moerr.NewNYIf(builder.GetContext(),
			"aggregation with non equal predicate in scalar subquery will be supported in future version")
	}

	// This rewrite bypasses nodes above AGG by joining directly against the
	// AGG input. Only allow the simple scalar aggregate shape:
	//   AGG(...)
	// or:
	//   PROJECT(agg_col) -> AGG(...)
	// Other shapes (user GROUP BY keys, HAVING/FILTER, and computed PROJECT
	// expressions) need different rewrites to preserve scalar subquery semantics.
	//
	// Note: pullupThroughAgg may have appended inner expressions of the
	// correlated predicates to aggNode.GroupBy.  Those entries do not affect
	// our rewrite because we bypass the inner AGG entirely, so we must not
	// inspect aggNode.GroupBy here.  Instead, use subCtx.groups, which holds
	// only the GROUP BY explicitly written by the user and is not mutated by
	// the pullup.
	if len(aggNode.BindingTags) == 0 || len(aggNode.Children) != 1 || len(subCtx.groups) > 0 {
		return 0, nil, moerr.NewNYIf(builder.GetContext(),
			"aggregation with non equal predicate in scalar subquery will be supported in future version")
	}
	subRoot := builder.qry.Nodes[subID]
	if subRoot != aggNode {
		if subRoot.NodeType != plan.Node_PROJECT ||
			len(subRoot.Children) != 1 ||
			builder.qry.Nodes[subRoot.Children[0]] != aggNode ||
			len(subRoot.BindingTags) == 0 ||
			len(subRoot.ProjectList) == 0 {
			return 0, nil, moerr.NewNYIf(builder.GetContext(),
				"aggregation with non equal predicate in scalar subquery will be supported in future version")
		}

		col, ok := subRoot.ProjectList[0].Expr.(*plan.Expr_Col)
		if !ok || col.Col == nil {
			return 0, nil, moerr.NewNYIf(builder.GetContext(),
				"aggregation with non equal predicate in scalar subquery will be supported in future version")
		}
	}

	groupTag := aggNode.BindingTags[0]
	innerID := aggNode.Children[0]

	// pullupThroughProj may have rewritten predicates to reference the
	// PROJECT tag.  Unwind PROJECT first, then AGG, so that predicates
	// end up referencing columns from the scan below AGG.
	projNode := subRoot
	if projNode.NodeType == plan.Node_PROJECT && len(projNode.BindingTags) > 0 {
		projTag := projNode.BindingTags[0]
		for i, pred := range preds {
			preds[i] = replaceGroupTagRefs(pred, projTag, projNode.ProjectList)
		}
	}

	// Replace groupTag column refs in predicates with the actual GroupBy
	// expressions so they reference columns below the AGG (the scan).
	for i, pred := range preds {
		preds[i] = replaceGroupTagRefs(pred, groupTag, aggNode.GroupBy)
	}

	filterPreds, joinPreds := decreaseDepthAndDispatch(preds)
	if len(filterPreds) > 0 {
		return 0, nil, moerr.NewNYIf(builder.GetContext(),
			"correlated columns in scalar subquery deeper than 1 level will be supported in future version")
	}

	// Collect outer columns for GROUP BY.
	// Reuse the outer binding tag as the AGG's group tag so that existing
	// column references to the outer table remain valid after the AGG.
	//
	// Restrictions (avoid known correctness traps):
	//  1. Exactly one outer binding.  Multiple bindings would force us to
	//     pick a single tag for the AGG, dropping access to the others.
	//  2. The single binding must have at least one hidden column (Row_ID).
	//     Without a unique row identifier in GROUP BY, duplicate outer rows
	//     would be merged by the AGG, producing wrong results.  Base table
	//     scans always carry Row_ID; derived tables (FROM (...) sub) do not.
	if len(ctx.bindings) != 1 {
		return 0, nil, moerr.NewNYIf(builder.GetContext(),
			"aggregation with non equal predicate in scalar subquery referencing multiple outer tables will be supported in future version")
	}
	outerBinding := ctx.bindings[0]
	hasHiddenCol := false
	for _, hidden := range outerBinding.colIsHidden {
		if hidden {
			hasHiddenCol = true
			break
		}
	}
	if !hasHiddenCol {
		return 0, nil, moerr.NewNYIf(builder.GetContext(),
			"aggregation with non equal predicate in scalar subquery on derived tables will be supported in future version")
	}
	outerGroupBy := make([]*plan.Expr, 0, len(outerBinding.cols))
	for i := range outerBinding.cols {
		outerGroupBy = append(outerGroupBy, &plan.Expr{
			Typ:  *outerBinding.types[i],
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: outerBinding.tag, ColPos: int32(i)}},
		})
	}
	// Reuse the outer binding tag as groupTag so outer column refs
	// (RelPos == outerBinding.tag) resolve through the AGG node directly.
	reuseGroupTag := outerBinding.tag

	// Build the aggregate expressions — deep copy so we don't mutate the
	// original AGG node.
	aggExprs := make([]*plan.Expr, len(aggNode.AggList))
	for i, agg := range aggNode.AggList {
		aggExprs[i] = DeepCopyExpr(agg)
	}

	// LEFT JOIN produces a NULL row for non-matching outer rows.
	// starcount/count(*) would count that NULL row as 1 instead of 0.
	//
	// Fix: rewrite starcount → count(inner.Row_ID).  Row_ID is always
	// non-null on real inner rows and becomes NULL when the LEFT JOIN
	// produces a no-match row, so count() naturally returns 0 for
	// outer rows that have no matching inner rows.
	//
	// We require the inner subtree to walk down through single-child
	// nodes to a single TABLE_SCAN that exposes Row_ID; otherwise the
	// rewrite is unsafe and we fall back to NYI.
	hasStarCount := false
	for _, agg := range aggExprs {
		if f, ok := agg.Expr.(*plan.Expr_F); ok && f.F.Func.ObjName == "starcount" {
			hasStarCount = true
			break
		}
	}
	if hasStarCount {
		markerCol := builder.findRowIDColRef(innerID)
		if markerCol == nil {
			return 0, nil, moerr.NewNYIf(builder.GetContext(),
				"count(*) with non equal predicate in scalar subquery on this inner shape will be supported in future version")
		}
		for _, agg := range aggExprs {
			f, ok := agg.Expr.(*plan.Expr_F)
			if !ok || f.F.Func.ObjName != "starcount" {
				continue
			}
			argType := makeTypeByPlan2Expr(markerCol)
			fGet, err := function.GetFunctionByName(builder.GetContext(), "count", []types.Type{argType})
			if err != nil {
				return 0, nil, err
			}
			f.F.Func.ObjName = "count"
			f.F.Func.Obj = fGet.GetEncodedOverloadID()
			f.F.Args = []*plan.Expr{DeepCopyExpr(markerCol)}
			retType := fGet.GetReturnType()
			agg.Typ = makePlan2Type(&retType)
		}
	}

	// LEFT JOIN outer with inner scan, all predicates as join conditions
	nodeID = builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{nodeID, innerID},
		JoinType: plan.Node_LEFT,
		OnList:   joinPreds,
		SpillMem: builder.joinSpillMem,
	}, ctx)

	// New AGG: group by outer columns, compute aggregates on raw inner rows
	newAggTag := builder.genNewBindTag()
	nodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_AGG,
		Children:    []int32{nodeID},
		GroupBy:     outerGroupBy,
		AggList:     aggExprs,
		BindingTags: []int32{reuseGroupTag, newAggTag},
		SpillMem:    builder.aggSpillMem,
	}, ctx)

	retExpr := &plan.Expr{
		Typ:  subCtx.results[0].Typ,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: newAggTag, ColPos: 0}},
	}

	// COUNT rewrite: LEFT JOIN produces NULLs for non-matching rows,
	// COUNT should return 0 instead of NULL.
	if builder.findAggrCount(aggExprs) {
		argsType := []types.Type{makeTypeByPlan2Expr(retExpr)}
		fGet, err := function.GetFunctionByName(builder.GetContext(), "isnull", argsType)
		if err != nil {
			return nodeID, retExpr, err
		}
		funcID, returnType := fGet.GetEncodedOverloadID(), fGet.GetReturnType()
		isNullExpr := &Expr{
			Typ: makePlan2Type(&returnType),
			Expr: &plan.Expr_F{F: &plan.Function{
				Func: getFunctionObjRef(funcID, "isnull"),
				Args: []*Expr{retExpr},
			}},
		}
		zeroExpr := makePlan2Int64ConstExprWithType(0)
		argsType = []types.Type{
			makeTypeByPlan2Expr(isNullExpr),
			makeTypeByPlan2Expr(zeroExpr),
			makeTypeByPlan2Expr(retExpr),
		}
		fGet, err = function.GetFunctionByName(builder.GetContext(), "case", argsType)
		if err != nil {
			return nodeID, retExpr, err
		}
		funcID, returnType = fGet.GetEncodedOverloadID(), fGet.GetReturnType()
		retExpr = &Expr{
			Typ: makePlan2Type(&returnType),
			Expr: &plan.Expr_F{F: &plan.Function{
				Func: getFunctionObjRef(funcID, "case"),
				Args: []*Expr{isNullExpr, zeroExpr, DeepCopyExpr(retExpr)},
			}},
		}
	}

	return nodeID, retExpr, nil
}

// findAggNodeBelow walks down from nodeID through single-child nodes to find
// the first AGG node.
func (builder *QueryBuilder) findAggNodeBelow(nodeID int32) *plan.Node {
	for {
		node := builder.qry.Nodes[nodeID]
		if node.NodeType == plan.Node_AGG {
			return node
		}
		if len(node.Children) != 1 {
			return nil
		}
		nodeID = node.Children[0]
	}
}

// findRowIDColRef walks down from nodeID through single-child nodes to find
// a TABLE_SCAN, and returns a column reference to its Row_ID column.
//
// Row_ID is always present and NotNullable on a base TABLE_SCAN, so it
// makes a safe "match marker": after a LEFT JOIN, Row_ID is non-null on
// matched rows and NULL on non-matched rows, which is exactly what
// count(marker) needs to distinguish "no inner row" from "matched zero
// inner rows".
//
// Returns nil if the walk hits a multi-child node, a non-TABLE_SCAN leaf,
// or a TABLE_SCAN whose TableDef does not expose Row_ID.
func (builder *QueryBuilder) findRowIDColRef(nodeID int32) *plan.Expr {
	for {
		node := builder.qry.Nodes[nodeID]
		if node.NodeType == plan.Node_TABLE_SCAN {
			if node.TableDef == nil || node.TableDef.Name2ColIndex == nil {
				return nil
			}
			idx, ok := node.TableDef.Name2ColIndex[catalog.Row_ID]
			if !ok || int(idx) >= len(node.TableDef.Cols) {
				return nil
			}
			col := node.TableDef.Cols[idx]
			typ := col.Typ
			typ.NotNullable = true
			return &plan.Expr{
				Typ:  typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: node.BindingTags[0], ColPos: idx}},
			}
		}
		if len(node.Children) != 1 {
			return nil
		}
		nodeID = node.Children[0]
	}
}

// replaceGroupTagRefs replaces column references with RelPos == groupTag
// with the corresponding GroupBy expression (deep-copied).
func replaceGroupTagRefs(expr *plan.Expr, groupTag int32, groupBy []*plan.Expr) *plan.Expr {
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		if e.Col.RelPos == groupTag && int(e.Col.ColPos) < len(groupBy) {
			return DeepCopyExpr(groupBy[e.Col.ColPos])
		}
	case *plan.Expr_F:
		for i, arg := range e.F.Args {
			e.F.Args[i] = replaceGroupTagRefs(arg, groupTag, groupBy)
		}
	}
	return expr
}

func (builder *QueryBuilder) pullupCorrelatedPredicates(
	nodeID int32,
	ctx *BindContext,
	subqueryType plan.SubqueryRef_Type,
	isSubqueryRoot bool,
) (int32, []*plan.Expr, error) {
	node := builder.qry.Nodes[nodeID]

	var preds []*plan.Expr
	var err error

	var subPreds []*plan.Expr
	for i, childID := range node.Children {
		childIsSubqueryRoot := isSubqueryRoot && len(node.Children) == 1 &&
			node.Limit == nil && node.Offset == nil &&
			(node.NodeType == plan.Node_PROJECT || node.NodeType == plan.Node_SORT)
		node.Children[i], subPreds, err = builder.pullupCorrelatedPredicates(
			childID, ctx, subqueryType, childIsSubqueryRoot)
		if err != nil {
			return 0, nil, err
		}

		preds = append(preds, subPreds...)
	}

	switch node.NodeType {
	case plan.Node_AGG:
		groupTag := node.BindingTags[0]
		for _, pred := range preds {
			builder.pullupThroughAgg(ctx, node, groupTag, pred)
		}

	case plan.Node_PROJECT:
		projectTag := node.BindingTags[0]
		for _, pred := range preds {
			builder.pullupThroughProj(ctx, node, projectTag, pred)
		}

	case plan.Node_FILTER:
		var newFilterList []*plan.Expr
		for _, cond := range node.FilterList {
			if hasCorrCol(cond) {
				//cond, err = bindFuncExprImplByPlanExpr("is", []*plan.Expr{cond, DeepCopyExpr(constTrue)})
				if err != nil {
					return 0, nil, err
				}
				preds = append(preds, cond)
			} else {
				newFilterList = append(newFilterList, cond)
			}
		}

		if len(newFilterList) == 0 {
			nodeID = node.Children[0]
		} else {
			node.FilterList = newFilterList
		}
	}

	if len(preds) > 0 && (node.Limit != nil || node.Offset != nil) {
		nodeID, err = builder.rewriteCorrelatedPagination(
			nodeID, node, preds, ctx, subqueryType, isSubqueryRoot)
		if err != nil {
			return 0, nil, err
		}
	}

	return nodeID, preds, err
}

// rewriteCorrelatedPagination preserves the evaluation boundary of LIMIT and
// OFFSET while decorrelating an equality-correlated subquery. Before predicate
// pull-up, pagination runs once for every outer correlation key. After pull-up,
// a plain LIMIT on the right input would run once globally and silently remove
// rows belonging to other keys.
//
// Rewrite the pagination as ROW_NUMBER over the inner equality keys, then keep
// the requested row-number interval. Predicates that cannot define stable
// inner partitions are rejected instead of being executed with global limit
// semantics. In particular, a non-equality predicate may select a different
// set for two outer rows that share every equality key.
func (builder *QueryBuilder) rewriteCorrelatedPagination(
	nodeID int32,
	node *plan.Node,
	preds []*plan.Expr,
	ctx *BindContext,
	subqueryType plan.SubqueryRef_Type,
	isSubqueryRoot bool,
) (int32, error) {
	limit, literal := getLiteralUint64(node.Limit)
	if !literal {
		return 0, moerr.NewNYI(builder.GetContext(), "dynamic LIMIT in correlated subquery")
	}
	if limit == 0 {
		// An empty right input is empty for every correlation key, so the global
		// zero-row plan already has the same scalar and existential semantics.
		return nodeID, nil
	}

	offset := uint64(0)
	if node.Offset != nil {
		var ok bool
		offset, ok = getLiteralUint64(node.Offset)
		if !ok {
			return 0, moerr.NewNYI(builder.GetContext(), "dynamic OFFSET in correlated subquery")
		}
	}
	if offset > math.MaxInt64 || limit > uint64(math.MaxInt64)-offset {
		return 0, moerr.NewNYI(builder.GetContext(), "correlated LIMIT or OFFSET larger than INT64_MAX")
	}
	if node.RankOption != nil {
		return 0, moerr.NewNYI(builder.GetContext(), "correlated LIMIT with BY RANK")
	}
	if offset == 0 && isSubqueryRoot {
		switch subqueryType {
		case plan.SubqueryRef_EXISTS, plan.SubqueryRef_NOT_EXISTS:
			// A positive LIMIT and its ordering cannot change whether the
			// complete existential input is empty.  The semantic root can be
			// below transparent PROJECT/SORT wrappers, so remove the complete
			// pagination boundary before validating or copying its ORDER BY.
			node.Limit = nil
			node.Offset = nil
			if node.NodeType == plan.Node_SORT {
				if len(node.Children) != 1 {
					return 0, moerr.NewInternalError(builder.GetContext(), "correlated LIMIT sort must have one child")
				}
				return node.Children[0], nil
			}
			node.OrderBy = nil
			return nodeID, nil
		}
	}
	for _, orderBy := range node.OrderBy {
		if orderBy != nil && builder.hasCorrColThroughProjection(orderBy.Expr, node.Children) {
			return 0, moerr.NewNYI(builder.GetContext(), "correlated columns in ORDER BY with LIMIT")
		}
	}
	for _, pred := range preds {
		if !allCorrColsAtDepthOne(pred) {
			if subqueryType == plan.SubqueryRef_SCALAR {
				// Preserve the established deep-scalar rejection below, including
				// its public diagnostic, instead of partially rewriting its limit.
				return nodeID, nil
			}
			return 0, moerr.NewNYI(builder.GetContext(), "deeply correlated LIMIT")
		}
	}

	partitionBy, ok := correlatedPaginationPartitionKeys(preds)
	if !ok {
		return 0, moerr.NewNYI(builder.GetContext(), "correlated LIMIT with non-equality predicates")
	}
	if len(partitionBy) == 0 {
		// Correlated predicates depending only on the outer row do not change
		// the right-side set, so its pagination is legitimately global.
		return nodeID, nil
	}
	for _, key := range partitionBy {
		if !correlatedPaginationPartitionTypeSupported(types.T(key.Typ.Id)) {
			return 0, moerr.NewNYIf(builder.GetContext(),
				"correlated LIMIT partition key type %s", makeTypeByPlan2Expr(key))
		}
	}

	inputID := nodeID
	if node.NodeType == plan.Node_SORT {
		if len(node.Children) != 1 {
			return 0, moerr.NewInternalError(builder.GetContext(), "correlated LIMIT sort must have one child")
		}
		inputID = node.Children[0]
	}
	orderBy := DeepCopyOrderBySpecList(node.OrderBy)
	node.Limit = nil
	node.Offset = nil

	windowTag := builder.genNewBindTag()
	partitionOrder := make([]*plan.OrderBySpec, len(partitionBy))
	for i, key := range partitionBy {
		partitionOrder[i] = &plan.OrderBySpec{
			Expr: DeepCopyExpr(key),
			Flag: plan.OrderBySpec_INTERNAL,
		}
	}
	partitionID := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PARTITION,
		Children:    []int32{inputID},
		OrderBy:     partitionOrder,
		BindingTags: []int32{windowTag},
	}, ctx)

	rowNumberFunc, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "row_number", nil)
	if err != nil {
		return 0, err
	}
	rowNumberExpr := &plan.Expr{
		Typ: rowNumberFunc.Typ,
		Expr: &plan.Expr_W{W: &plan.WindowSpec{
			WindowFunc:  rowNumberFunc,
			Name:        "row_number",
			PartitionBy: DeepCopyExprList(partitionBy),
			OrderBy:     orderBy,
			Frame: &plan.FrameClause{
				Type: plan.FrameClause_ROWS,
				Start: &plan.FrameBound{
					Type:      plan.FrameBound_PRECEDING,
					UnBounded: true,
				},
				End: &plan.FrameBound{
					Type:      plan.FrameBound_FOLLOWING,
					UnBounded: true,
				},
			},
		}},
	}
	rowNumberCol := GetColExpr(rowNumberFunc.Typ, windowTag, 0)
	upperBound := makePlan2Int64ConstExprWithType(int64(offset + limit))
	rowFilter, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "<=", []*plan.Expr{
		DeepCopyExpr(rowNumberCol),
		upperBound,
	})
	if err != nil {
		return 0, err
	}
	if offset > 0 {
		lowerBound := makePlan2Int64ConstExprWithType(int64(offset))
		afterOffset, bindErr := BindFuncExprImplByPlanExpr(builder.GetContext(), ">", []*plan.Expr{
			DeepCopyExpr(rowNumberCol),
			lowerBound,
		})
		if bindErr != nil {
			return 0, bindErr
		}
		rowFilter, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "and", []*plan.Expr{
			afterOffset,
			rowFilter,
		})
		if err != nil {
			return 0, err
		}
	}

	return builder.appendNode(&plan.Node{
		NodeType:    plan.Node_WINDOW,
		Children:    []int32{partitionID},
		WinSpecList: []*plan.Expr{rowNumberExpr},
		WindowIdx:   0,
		BindingTags: []int32{windowTag},
		FilterList:  []*plan.Expr{rowFilter},
	}, ctx), nil
}

func (builder *QueryBuilder) hasCorrColThroughProjection(expr *plan.Expr, inputIDs []int32) bool {
	return builder.hasCorrColThroughProjectionImpl(expr, inputIDs, make(map[[2]int32]bool))
}

func (builder *QueryBuilder) hasCorrColThroughProjectionImpl(
	expr *plan.Expr,
	inputIDs []int32,
	resolving map[[2]int32]bool,
) bool {
	if expr == nil {
		return false
	}

	switch item := expr.Expr.(type) {
	case *plan.Expr_Corr:
		return true
	case *plan.Expr_Col:
		if item.Col == nil {
			return false
		}
		key := [2]int32{item.Col.RelPos, item.Col.ColPos}
		if resolving[key] {
			return false
		}
		projected, children, ok := builder.findProjectedExpr(inputIDs, item.Col)
		if !ok {
			return false
		}
		resolving[key] = true
		hasCorr := builder.hasCorrColThroughProjectionImpl(projected, children, resolving)
		delete(resolving, key)
		return hasCorr
	case *plan.Expr_F:
		for _, arg := range item.F.Args {
			if builder.hasCorrColThroughProjectionImpl(arg, inputIDs, resolving) {
				return true
			}
		}
	case *plan.Expr_List:
		for _, arg := range item.List.List {
			if builder.hasCorrColThroughProjectionImpl(arg, inputIDs, resolving) {
				return true
			}
		}
	case *plan.Expr_W:
		if item.W == nil {
			return false
		}
		if builder.hasCorrColThroughProjectionImpl(item.W.WindowFunc, inputIDs, resolving) {
			return true
		}
		for _, partitionBy := range item.W.PartitionBy {
			if builder.hasCorrColThroughProjectionImpl(partitionBy, inputIDs, resolving) {
				return true
			}
		}
		for _, orderBy := range item.W.OrderBy {
			if orderBy != nil && builder.hasCorrColThroughProjectionImpl(orderBy.Expr, inputIDs, resolving) {
				return true
			}
		}
	}
	return false
}

func (builder *QueryBuilder) findProjectedExpr(
	inputIDs []int32,
	col *plan.ColRef,
) (*plan.Expr, []int32, bool) {
	visited := make(map[int32]bool)
	var find func(int32) (*plan.Expr, []int32, bool)
	find = func(nodeID int32) (*plan.Expr, []int32, bool) {
		if nodeID < 0 || int(nodeID) >= len(builder.qry.Nodes) || visited[nodeID] {
			return nil, nil, false
		}
		visited[nodeID] = true
		node := builder.qry.Nodes[nodeID]
		if node.NodeType == plan.Node_PROJECT && len(node.BindingTags) > 0 &&
			node.BindingTags[0] == col.RelPos {
			if col.ColPos < 0 || int(col.ColPos) >= len(node.ProjectList) {
				return nil, nil, false
			}
			return node.ProjectList[col.ColPos], node.Children, true
		}
		for _, childID := range node.Children {
			if expr, children, ok := find(childID); ok {
				return expr, children, true
			}
		}
		return nil, nil, false
	}
	for _, inputID := range inputIDs {
		if expr, children, ok := find(inputID); ok {
			return expr, children, true
		}
	}
	return nil, nil, false
}

func correlatedPaginationPartitionKeys(preds []*plan.Expr) ([]*plan.Expr, bool) {
	keys := make([]*plan.Expr, 0, len(preds))
	for _, pred := range preds {
		if !collectCorrelatedPaginationPartitionKeys(pred, &keys) {
			return nil, false
		}
	}
	return keys, true
}

func collectCorrelatedPaginationPartitionKeys(expr *plan.Expr, keys *[]*plan.Expr) bool {
	if expr == nil || !hasCorrCol(expr) {
		return true
	}

	fn := expr.GetF()
	if fn != nil && fn.Func != nil && fn.Func.ObjName == "and" {
		for _, arg := range fn.Args {
			if !collectCorrelatedPaginationPartitionKeys(arg, keys) {
				return false
			}
		}
		return true
	}

	if fn != nil && fn.Func != nil &&
		(fn.Func.ObjName == "=" || fn.Func.ObjName == "<=>") && len(fn.Args) == 2 {
		leftCorr := hasCorrCol(fn.Args[0])
		rightCorr := hasCorrCol(fn.Args[1])
		if leftCorr != rightCorr {
			outerExpr := fn.Args[0]
			innerExpr := fn.Args[1]
			if rightCorr {
				outerExpr, innerExpr = innerExpr, outerExpr
			}
			if exprHasColRef(outerExpr) || !allCorrColsAtDepthOne(outerExpr) {
				return false
			}
			if exprHasColRef(innerExpr) {
				*keys = append(*keys, DeepCopyExpr(innerExpr))
			}
			return true
		}
	}

	// A predicate depending only on outer columns does not change which inner
	// rows belong to an equality partition; the eventual join still applies it.
	return !exprHasColRef(expr) && allCorrColsAtDepthOne(expr)
}

func exprHasColRef(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	switch item := expr.Expr.(type) {
	case *plan.Expr_Col:
		return true
	case *plan.Expr_F:
		for _, arg := range item.F.Args {
			if exprHasColRef(arg) {
				return true
			}
		}
	case *plan.Expr_List:
		for _, arg := range item.List.List {
			if exprHasColRef(arg) {
				return true
			}
		}
	}
	return false
}

func allCorrColsAtDepthOne(expr *plan.Expr) bool {
	if expr == nil {
		return true
	}
	switch item := expr.Expr.(type) {
	case *plan.Expr_Corr:
		return item.Corr != nil && item.Corr.Depth == 1
	case *plan.Expr_F:
		for _, arg := range item.F.Args {
			if !allCorrColsAtDepthOne(arg) {
				return false
			}
		}
	case *plan.Expr_List:
		for _, arg := range item.List.List {
			if !allCorrColsAtDepthOne(arg) {
				return false
			}
		}
	}
	return true
}

// Node_PARTITION builds pkg/compare comparators for these types. Keep this
// list aligned with compare.New so an otherwise valid equality key cannot
// reach the partition operator with a nil comparator.
func correlatedPaginationPartitionTypeSupported(typ types.T) bool {
	switch typ {
	case types.T_bool, types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_date, types.T_datetime, types.T_time, types.T_timestamp,
		types.T_decimal64, types.T_decimal128, types.T_decimal256,
		types.T_TS, types.T_Rowid, types.T_Blockid, types.T_uuid,
		types.T_enum, types.T_year,
		types.T_char, types.T_varchar, types.T_blob, types.T_binary,
		types.T_varbinary, types.T_json, types.T_text, types.T_datalink,
		types.T_geometry,
		types.T_array_float32, types.T_array_float64, types.T_array_bf16,
		types.T_array_float16, types.T_array_int8, types.T_array_uint8:
		return true
	default:
		return false
	}
}

func (builder *QueryBuilder) pullupThroughAgg(ctx *BindContext, node *plan.Node, tag int32, expr *plan.Expr) *plan.Expr {
	if !hasCorrCol(expr) {
		switch expr.Expr.(type) {
		case *plan.Expr_Col, *plan.Expr_F:
			break

		default:
			return expr
		}

		colPos := int32(len(node.GroupBy))
		// The new correlated key was not part of the earlier primary-key FD
		// proof. Keep the logical rewrite correct by dropping the physical hint.
		node.GroupByHashKey = nil
		node.GroupBy = append(node.GroupBy, expr)

		if colRef, ok := expr.Expr.(*plan.Expr_Col); ok {
			oldMapId := [2]int32{colRef.Col.RelPos, colRef.Col.ColPos}
			newMapId := [2]int32{tag, colPos}

			builder.nameByColRef[newMapId] = builder.nameByColRef[oldMapId]
		}

		return &plan.Expr{
			Typ: expr.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: tag,
					ColPos: colPos,
				},
			},
		}
	}

	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for i, arg := range exprImpl.F.Args {
			exprImpl.F.Args[i] = builder.pullupThroughAgg(ctx, node, tag, arg)
		}
	}

	return expr
}

func (builder *QueryBuilder) pullupThroughProj(ctx *BindContext, node *plan.Node, tag int32, expr *plan.Expr) *plan.Expr {
	if !hasCorrCol(expr) {
		switch expr.Expr.(type) {
		case *plan.Expr_Col, *plan.Expr_F:
			break

		default:
			return expr
		}

		colPos := int32(len(node.ProjectList))
		node.ProjectList = append(node.ProjectList, expr)

		if colRef, ok := expr.Expr.(*plan.Expr_Col); ok {
			oldMapId := [2]int32{colRef.Col.RelPos, colRef.Col.ColPos}
			newMapId := [2]int32{tag, colPos}

			builder.nameByColRef[newMapId] = builder.nameByColRef[oldMapId]
		}

		return &plan.Expr{
			Typ: expr.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: tag,
					ColPos: colPos,
				},
			},
		}
	}

	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for i, arg := range exprImpl.F.Args {
			exprImpl.F.Args[i] = builder.pullupThroughProj(ctx, node, tag, arg)
		}
	}

	return expr
}
