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
			return 0, nil, moerr.NewNotSupported(builder.GetContext(), "%s subquery not supported", subquery.Typ.String())
		}
	}

	subID, preds, err := builder.pullupCorrelatedPredicates(subID, subCtx)
	if err != nil {
		return 0, nil, err
	}

	if subquery.Typ == plan.SubqueryRef_SCALAR && len(subCtx.aggregates) > 0 && builder.findNonEqPred(preds) {
		return 0, nil, moerr.NewNYI(builder.GetContext(), "aggregation with non equal predicate in %s subquery  will be supported in future version", subquery.Typ.String())
	}

	filterPreds, joinPreds := decreaseDepthAndDispatch(preds)

	if len(filterPreds) > 0 && subquery.Typ >= plan.SubqueryRef_SCALAR {
		return 0, nil, moerr.NewNYI(builder.GetContext(), "correlated columns in %s subquery deeper than 1 level will be supported in future version", subquery.Typ.String())
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
				Typ: *makePlan2Type(&returnType),
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
				Typ: *makePlan2Type(&returnType),
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
		return 0, nil, moerr.NewNotSupported(builder.GetContext(), "%s subquery not supported", subquery.Typ.String())
	}
}

func (builder *QueryBuilder) insertMarkJoin(left, right int32, joinPreds []*plan.Expr, outerPred *plan.Expr, negate bool, ctx *BindContext) (nodeID int32, markExpr *plan.Expr, err error) {
	markTag := builder.genNewTag()

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
