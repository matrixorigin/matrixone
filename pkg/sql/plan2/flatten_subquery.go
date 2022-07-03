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

package plan2

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
)

func (builder *QueryBuilder) flattenSubqueries(nodeId int32, expr *plan.Expr, ctx *BindContext) (int32, *plan.Expr, error) {
	var err error

	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for i, arg := range exprImpl.F.Args {
			nodeId, exprImpl.F.Args[i], err = builder.flattenSubqueries(nodeId, arg, ctx)
			if err != nil {
				return 0, nil, err
			}
		}

	case *plan.Expr_Sub:
		nodeId, expr, err = builder.flattenSubquery(nodeId, exprImpl.Sub, ctx)
	}

	return nodeId, expr, err
}

func (builder *QueryBuilder) flattenSubquery(nodeId int32, subquery *plan.SubqueryRef, ctx *BindContext) (int32, *plan.Expr, error) {
	// TODO: use SINGLE JOIN for scalar subquery and MARK JOIN for quantified subquery

	subId := subquery.NodeId
	subCtx := builder.ctxByNode[subId]

	if subquery.Typ == plan.SubqueryRef_SCALAR && !subCtx.hasSingleRow {
		return 0, nil, errors.New("", "runtime check of scalar subquery will be supported in future version")
	}

	subId, preds, err := builder.pullupCorrelatedPredicates(subId, subCtx)
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

		nodeId = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{nodeId, subId},
			JoinType: plan.Node_LEFT,
			OnList:   joinPreds,
		}, ctx)

		if len(filterPreds) > 0 {
			nodeId = builder.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{nodeId},
				FilterList: filterPreds,
			}, ctx)
		}

		return nodeId, &plan.Expr{
			Typ: subCtx.projects[0].Typ,
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

		nodeId = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{nodeId, subId},
			JoinType: plan.Node_SEMI,
			OnList:   joinPreds,
		}, ctx)

		return nodeId, nil, nil

	case plan.SubqueryRef_NOT_EXISTS:
		// Uncorrelated subquery
		if len(joinPreds) == 0 {
			joinPreds = append(joinPreds, alwaysTrue)
		}

		nodeId = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{nodeId, subId},
			JoinType: plan.Node_ANTI,
			OnList:   joinPreds,
		}, ctx)

		return nodeId, nil, nil

	case plan.SubqueryRef_IN:
		expr, err := bindFuncExprImplByPlanExpr("=", []*plan.Expr{
			subquery.Child,
			{
				Typ: subCtx.projects[0].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: subCtx.rootTag(),
						ColPos: 0,
					},
				},
			},
		})
		if err != nil {
			return 0, nil, err
		}

		joinPreds = append(joinPreds, expr)

		nodeId = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{nodeId, subId},
			JoinType: plan.Node_SEMI,
			OnList:   joinPreds,
		}, ctx)

		return nodeId, nil, nil

	case plan.SubqueryRef_NOT_IN:
		expr, err := bindFuncExprImplByPlanExpr("=", []*plan.Expr{
			subquery.Child,
			{
				Typ: subCtx.projects[0].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: subCtx.rootTag(),
						ColPos: 0,
					},
				},
			},
		})
		if err != nil {
			return 0, nil, err
		}

		joinPreds = append(joinPreds, expr)

		nodeId = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{nodeId, subId},
			JoinType: plan.Node_ANTI,
			OnList:   joinPreds,
		}, ctx)

		return nodeId, nil, nil

	case plan.SubqueryRef_ANY:
		expr, err := bindFuncExprImplByPlanExpr(subquery.Op, []*plan.Expr{
			subquery.Child,
			{
				Typ: subCtx.projects[0].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: subCtx.rootTag(),
						ColPos: 0,
					},
				},
			},
		})
		if err != nil {
			return 0, nil, err
		}

		joinPreds = append(joinPreds, expr)

		nodeId = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{nodeId, subId},
			JoinType: plan.Node_SEMI,
			OnList:   joinPreds,
		}, ctx)

		return nodeId, nil, nil

	case plan.SubqueryRef_ALL:
		expr, err := bindFuncExprImplByPlanExpr(subquery.Op, []*plan.Expr{
			subquery.Child,
			{
				Typ: subCtx.projects[0].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: subCtx.rootTag(),
						ColPos: 0,
					},
				},
			},
		})
		if err != nil {
			return 0, nil, err
		}

		expr, err = bindFuncExprImplByPlanExpr("not", []*plan.Expr{expr})
		if err != nil {
			return 0, nil, err
		}

		joinPreds = append(joinPreds, expr)

		nodeId = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{nodeId, subId},
			JoinType: plan.Node_ANTI,
			OnList:   joinPreds,
		}, ctx)

		return nodeId, nil, nil

	default:
		return 0, nil, errors.New("", fmt.Sprintf("%s subquery not supported", subquery.Typ.String()))
	}
}

func (builder *QueryBuilder) pullupCorrelatedPredicates(nodeId int32, ctx *BindContext) (int32, []*plan.Expr, error) {
	node := builder.qry.Nodes[nodeId]

	var preds []*plan.Expr
	var err error

	var subPreds []*plan.Expr
	for i, childId := range node.Children {
		node.Children[i], subPreds, err = builder.pullupCorrelatedPredicates(childId, ctx)
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
			nodeId = node.Children[0]
		} else {
			node.FilterList = newFilterList
		}
	}

	return nodeId, preds, err
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
