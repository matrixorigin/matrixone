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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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
	var err error

	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for i, arg := range exprImpl.F.Args {
			nodeID, exprImpl.F.Args[i], err = builder.flattenSubqueries(nodeID, arg, ctx)
			if err != nil {
				return 0, nil, err
			}
		}

	case *plan.Expr_Sub:
		if builder.isForUpdate {
			return 0, nil, moerr.NewInternalError(builder.GetContext(), "not support subquery for update")
		}
		nodeID, expr, err = builder.flattenSubquery(nodeID, exprImpl.Sub, ctx)
	}

	return nodeID, expr, err
}

func (builder *QueryBuilder) flattenSubquery(nodeID int32, subquery *plan.SubqueryRef, ctx *BindContext) (int32, *plan.Expr, error) {
	if subquery.Child != nil && hasSubquery(subquery.Child) {
		return 0, nil, moerr.NewNotSupported(builder.GetContext(), "a quantified subquery's left operand can't contain subquery")
	}

	subID := subquery.NodeId
	subCtx := builder.ctxByNode[subID]

	// Strip unnecessary subqueries which have no FROM clause
	subNode := builder.qry.Nodes[subID]
	if subNode.NodeType == plan.Node_PROJECT && builder.qry.Nodes[subNode.Children[0]].NodeType == plan.Node_VALUE_SCAN {
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

	subID, preds, err := builder.pullupCorrelatedPredicates(subID, subCtx)
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

	if len(filterPreds) > 0 && subquery.Typ >= plan.SubqueryRef_SCALAR {
		return 0, nil, moerr.NewNYIf(builder.GetContext(), "correlated columns in %s subquery deeper than 1 level will be supported in future version", subquery.Typ.String())
	}

	switch subquery.Typ {
	case plan.SubqueryRef_SCALAR:
		var rewrite bool
		// Uncorrelated subquery
		if len(joinPreds) > 0 && builder.findAggrCount(subCtx.aggregates) {
			rewrite = true
		}

		joinType := plan.Node_SINGLE
		if subCtx.hasSingleRow {
			joinType = plan.Node_LEFT
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

		retExpr := &plan.Expr{
			Typ: subCtx.results[0].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: subCtx.topTag(),
					ColPos: 0,
				},
			},
		}
		if rewrite {
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

		return builder.insertMarkJoin(nodeID, subID, joinPreds, nil, false, ctx)

	case plan.SubqueryRef_NOT_EXISTS:
		// Uncorrelated subquery
		if len(joinPreds) == 0 {
			joinPreds = append(joinPreds, constTrue)
		}

		return builder.insertMarkJoin(nodeID, subID, joinPreds, nil, true, ctx)

	case plan.SubqueryRef_IN:
		outerPred, err := builder.generateRowComparison("=", subquery.Child, subCtx, false)
		if err != nil {
			return 0, nil, err
		}

		return builder.insertMarkJoin(nodeID, subID, joinPreds, outerPred, false, ctx)

	case plan.SubqueryRef_NOT_IN:
		outerPred, err := builder.generateRowComparison("=", subquery.Child, subCtx, false)
		if err != nil {
			return 0, nil, err
		}

		return builder.insertMarkJoin(nodeID, subID, joinPreds, outerPred, true, ctx)

	case plan.SubqueryRef_ANY:
		outerPred, err := builder.generateRowComparison(subquery.Op, subquery.Child, subCtx, false)
		if err != nil {
			return 0, nil, err
		}

		return builder.insertMarkJoin(nodeID, subID, joinPreds, outerPred, false, ctx)

	case plan.SubqueryRef_ALL:
		outerPred, err := builder.generateRowComparison(subquery.Op, subquery.Child, subCtx, false)
		if err != nil {
			return 0, nil, err
		}

		outerPred, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "not", []*plan.Expr{outerPred})
		if err != nil {
			return 0, nil, err
		}

		return builder.insertMarkJoin(nodeID, subID, joinPreds, outerPred, true, ctx)

	default:
		return 0, nil, moerr.NewNotSupportedf(builder.GetContext(), "%s subquery not supported", subquery.Typ.String())
	}
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
		switch op {
		case "=":
			leftExpr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), op, []*plan.Expr{
				childList[0],
				getProjectExpr(0, ctx, strip),
			})
			if err != nil {
				return nil, err
			}

			for i := 1; i < len(childList); i++ {
				rightExpr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), op, []*plan.Expr{
					childList[i],
					getProjectExpr(i, ctx, strip),
				})
				if err != nil {
					return nil, err
				}

				leftExpr, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "and", []*plan.Expr{leftExpr, rightExpr})
				if err != nil {
					return nil, err
				}
			}

			return leftExpr, nil

		case "<>":
			leftExpr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), op, []*plan.Expr{
				childList[0],
				getProjectExpr(0, ctx, strip),
			})
			if err != nil {
				return nil, err
			}

			for i := 1; i < len(childList); i++ {
				rightExpr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), op, []*plan.Expr{
					childList[i],
					getProjectExpr(i, ctx, strip),
				})
				if err != nil {
					return nil, err
				}

				leftExpr, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "or", []*plan.Expr{leftExpr, rightExpr})
				if err != nil {
					return nil, err
				}
			}

			return leftExpr, nil

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

func (builder *QueryBuilder) findNonEqPred(preds []*plan.Expr) bool {
	for _, pred := range preds {
		switch exprImpl := pred.Expr.(type) {
		case *plan.Expr_F:
			if exprImpl.F.Func.ObjName != "=" {
				return true
			}
		}
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

	groupTag := aggNode.BindingTags[0]
	innerID := aggNode.Children[0]

	// pullupThroughProj may have rewritten predicates to reference the
	// PROJECT tag.  Unwind PROJECT first, then AGG, so that predicates
	// end up referencing columns from the scan below AGG.
	projNode := builder.qry.Nodes[subID]
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
	if len(ctx.bindings) == 0 {
		return 0, nil, moerr.NewNYIf(builder.GetContext(),
			"aggregation with non equal predicate in scalar subquery will be supported in future version")
	}
	var outerGroupBy []*plan.Expr
	for _, binding := range ctx.bindings {
		for i := range binding.cols {
			outerGroupBy = append(outerGroupBy, &plan.Expr{
				Typ:  *binding.types[i],
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: binding.tag, ColPos: int32(i)}},
			})
		}
	}
	// Use the first outer binding's tag as groupTag so outer column refs
	// (RelPos == binding.tag) resolve through the AGG node directly.
	reuseGroupTag := ctx.bindings[0].tag

	// Build the aggregate expressions — deep copy so we don't mutate the
	// original AGG node.
	aggExprs := make([]*plan.Expr, len(aggNode.AggList))
	for i, agg := range aggNode.AggList {
		aggExprs[i] = DeepCopyExpr(agg)
	}

	// LEFT JOIN produces a NULL row for non-matching outer rows.
	// starcount/count(*) would count that NULL row as 1 instead of 0.
	// Fix: replace starcount(const) with count(inner_col) so NULLs are
	// skipped.  We pick the first column from the inner scan node.
	for _, agg := range aggExprs {
		if f, ok := agg.Expr.(*plan.Expr_F); ok && f.F.Func.ObjName == "starcount" {
			innerCol := builder.getFirstColRef(innerID)
			if innerCol == nil {
				continue
			}
			argType := makeTypeByPlan2Expr(innerCol)
			fGet, err := function.GetFunctionByName(builder.GetContext(), "count", []types.Type{argType})
			if err != nil {
				return 0, nil, err
			}
			f.F.Func.ObjName = "count"
			f.F.Func.Obj = fGet.GetEncodedOverloadID()
			f.F.Args = []*plan.Expr{innerCol}
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
			return nodeID, retExpr, nil
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

// getFirstColRef returns a column reference to the first column of the
// TABLE_SCAN node found at or below nodeID.
func (builder *QueryBuilder) getFirstColRef(nodeID int32) *plan.Expr {
	for {
		node := builder.qry.Nodes[nodeID]
		if node.NodeType == plan.Node_TABLE_SCAN && node.TableDef != nil && len(node.TableDef.Cols) > 0 {
			return &plan.Expr{
				Typ:  node.TableDef.Cols[0].Typ,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: node.BindingTags[0], ColPos: 0}},
			}
		}
		if len(node.Children) == 0 {
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

func (builder *QueryBuilder) pullupCorrelatedPredicates(nodeID int32, ctx *BindContext) (int32, []*plan.Expr, error) {
	node := builder.qry.Nodes[nodeID]

	var preds []*plan.Expr
	var err error

	var subPreds []*plan.Expr
	for i, childID := range node.Children {
		node.Children[i], subPreds, err = builder.pullupCorrelatedPredicates(childID, ctx)
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

	return nodeID, preds, err
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
