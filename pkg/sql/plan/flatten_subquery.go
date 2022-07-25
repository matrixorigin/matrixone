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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
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
		nodeID, expr, err = builder.flattenSubquery(nodeID, exprImpl.Sub, ctx)
	}

	return nodeID, expr, err
}

func (builder *QueryBuilder) flattenSubquery(nodeID int32, subquery *plan.SubqueryRef, ctx *BindContext) (int32, *plan.Expr, error) {
	// TODO: use SINGLE JOIN for scalar subquery and MARK JOIN for quantified subquery

	subID := subquery.NodeId
	subCtx := builder.ctxByNode[subID]

	if subquery.Typ == plan.SubqueryRef_SCALAR && !subCtx.hasSingleRow {
		return 0, nil, errors.New("", "runtime check of scalar subquery will be supported in future version")
	}

	subID, preds, err := builder.pullupCorrelatedPredicates(subID, subCtx)
	if err != nil {
		return 0, nil, err
	}

	filterPreds, joinPreds := decreaseDepthAndDispatch(preds)

	if len(filterPreds) > 0 && subquery.Typ >= plan.SubqueryRef_SCALAR {
		return 0, nil, errors.New("", fmt.Sprintf("correlated columns in %s subquery deeper than 1 level will be supported in future version", subquery.Typ.String()))
	}

	alwaysTrue := &plan.Expr{
		Expr: &plan.Expr_C{
			C: &plan.Const{
				Isnull: false,
				Value: &plan.Const_Bval{
					Bval: true,
				},
			},
		},
		Typ: &plan.Type{
			Id:       plan.Type_BOOL,
			Nullable: false,
			Size:     1,
		},
	}

	switch subquery.Typ {
	case plan.SubqueryRef_SCALAR:
		// Uncorrelated subquery
		if len(joinPreds) == 0 {
			joinPreds = append(joinPreds, alwaysTrue)
		}

		nodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{nodeID, subID},
			JoinType: plan.Node_LEFT,
			OnList:   joinPreds,
		}, ctx)

		if len(filterPreds) > 0 {
			nodeID = builder.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{nodeID},
				FilterList: filterPreds,
			}, ctx)
		}

		return nodeID, &plan.Expr{
			Typ: subCtx.results[0].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: subCtx.rootTag(),
					ColPos: 0,
				},
			},
		}, nil

	case plan.SubqueryRef_EXISTS:
		// Uncorrelated subquery
		if len(joinPreds) == 0 {
			joinPreds = append(joinPreds, alwaysTrue)
		}

		nodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{nodeID, subID},
			JoinType: plan.Node_SEMI,
			OnList:   joinPreds,
		}, ctx)

		return nodeID, nil, nil

	case plan.SubqueryRef_NOT_EXISTS:
		// Uncorrelated subquery
		if len(joinPreds) == 0 {
			joinPreds = append(joinPreds, alwaysTrue)
		}

		nodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{nodeID, subID},
			JoinType: plan.Node_ANTI,
			OnList:   joinPreds,
		}, ctx)

		return nodeID, nil, nil

	case plan.SubqueryRef_IN:
		expr, err := builder.generateComparison("=", subquery.Child, subCtx)
		if err != nil {
			return 0, nil, err
		}

		joinPreds = append(joinPreds, expr)

		nodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{nodeID, subID},
			JoinType: plan.Node_SEMI,
			OnList:   joinPreds,
		}, ctx)

		return nodeID, nil, nil

	case plan.SubqueryRef_NOT_IN:
		expr, err := builder.generateComparison("=", subquery.Child, subCtx)
		if err != nil {
			return 0, nil, err
		}

		joinPreds = append(joinPreds, expr)

		nodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{nodeID, subID},
			JoinType: plan.Node_ANTI,
			OnList:   joinPreds,
		}, ctx)

		return nodeID, nil, nil

	case plan.SubqueryRef_ANY:
		expr, err := builder.generateComparison(subquery.Op, subquery.Child, subCtx)
		if err != nil {
			return 0, nil, err
		}

		joinPreds = append(joinPreds, expr)

		nodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{nodeID, subID},
			JoinType: plan.Node_SEMI,
			OnList:   joinPreds,
		}, ctx)

		return nodeID, nil, nil

	case plan.SubqueryRef_ALL:
		expr, err := builder.generateComparison(subquery.Op, subquery.Child, subCtx)
		if err != nil {
			return 0, nil, err
		}

		expr, err = bindFuncExprImplByPlanExpr("not", []*plan.Expr{expr})
		if err != nil {
			return 0, nil, err
		}

		joinPreds = append(joinPreds, expr)

		nodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{nodeID, subID},
			JoinType: plan.Node_ANTI,
			OnList:   joinPreds,
		}, ctx)

		return nodeID, nil, nil

	default:
		return 0, nil, errors.New("", fmt.Sprintf("%s subquery not supported", subquery.Typ.String()))
	}
}

func (builder *QueryBuilder) generateComparison(op string, child *plan.Expr, ctx *BindContext) (*plan.Expr, error) {
	switch childImpl := child.Expr.(type) {
	case *plan.Expr_List:
		childList := childImpl.List.List
		switch op {
		case "=":
			leftExpr, err := bindFuncExprImplByPlanExpr(op, []*plan.Expr{
				childList[0],
				{
					Typ: ctx.results[0].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: ctx.rootTag(),
							ColPos: 0,
						},
					},
				},
			})
			if err != nil {
				return nil, err
			}

			for i := 1; i < len(childList); i++ {
				rightExpr, err := bindFuncExprImplByPlanExpr(op, []*plan.Expr{
					childList[i],
					{
						Typ: ctx.results[i].Typ,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: ctx.rootTag(),
								ColPos: int32(i),
							},
						},
					},
				})
				if err != nil {
					return nil, err
				}

				leftExpr, err = bindFuncExprImplByPlanExpr("and", []*plan.Expr{leftExpr, rightExpr})
				if err != nil {
					return nil, err
				}
			}

			return leftExpr, nil

		case "<>":
			leftExpr, err := bindFuncExprImplByPlanExpr(op, []*plan.Expr{
				childList[0],
				{
					Typ: ctx.results[0].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: ctx.rootTag(),
							ColPos: 0,
						},
					},
				},
			})
			if err != nil {
				return nil, err
			}

			for i := 1; i < len(childList); i++ {
				rightExpr, err := bindFuncExprImplByPlanExpr(op, []*plan.Expr{
					childList[i],
					{
						Typ: ctx.results[i].Typ,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: ctx.rootTag(),
								ColPos: int32(i),
							},
						},
					},
				})
				if err != nil {
					return nil, err
				}

				leftExpr, err = bindFuncExprImplByPlanExpr("or", []*plan.Expr{leftExpr, rightExpr})
				if err != nil {
					return nil, err
				}
			}

			return leftExpr, nil

		case "<", "<=":
			return builder.generateRecursiveComparison("<", op, childList, ctx, 0)

		case ">", ">=":
			return builder.generateRecursiveComparison(">", op, childList, ctx, 0)

		default:
			return nil, errors.New("", "row constructor only support comparison operators")
		}

	default:
		return bindFuncExprImplByPlanExpr(op, []*plan.Expr{
			child,
			{
				Typ: ctx.results[0].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: ctx.rootTag(),
						ColPos: 0,
					},
				},
			},
		})
	}
}

func (builder *QueryBuilder) generateRecursiveComparison(op, tailOp string, childList []*plan.Expr, ctx *BindContext, idx int) (*plan.Expr, error) {
	if idx == len(childList)-1 {
		return bindFuncExprImplByPlanExpr(tailOp, []*plan.Expr{
			childList[idx],
			{
				Typ: ctx.results[idx].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: ctx.rootTag(),
						ColPos: int32(idx),
					},
				},
			},
		})
	}

	expr, err := bindFuncExprImplByPlanExpr(op, []*plan.Expr{
		childList[idx],
		{
			Typ: ctx.results[idx].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: ctx.rootTag(),
					ColPos: int32(idx),
				},
			},
		},
	})
	if err != nil {
		return nil, err
	}

	eqExpr, err := bindFuncExprImplByPlanExpr("=", []*plan.Expr{
		childList[idx],
		{
			Typ: ctx.results[idx].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: ctx.rootTag(),
					ColPos: int32(idx),
				},
			},
		},
	})
	if err != nil {
		return nil, err
	}

	tailExpr, err := builder.generateRecursiveComparison(op, tailOp, childList, ctx, idx+1)
	if err != nil {
		return nil, err
	}

	tailExpr, err = bindFuncExprImplByPlanExpr("and", []*plan.Expr{eqExpr, tailExpr})
	if err != nil {
		return nil, err
	}

	return bindFuncExprImplByPlanExpr("or", []*plan.Expr{expr, tailExpr})
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
