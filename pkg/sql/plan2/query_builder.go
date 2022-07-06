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
	"sort"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func NewQueryBuilder(queryType plan.Query_StatementType, ctx CompilerContext) *QueryBuilder {
	return &QueryBuilder{
		qry: &Query{
			StmtType: queryType,
		},
		compCtx:   ctx,
		ctxByNode: []*BindContext{},
		nextTag:   0,
	}
}

func (builder *QueryBuilder) remapExpr(expr *Expr, colMap map[[2]int32][2]int32) error {
	switch ne := expr.Expr.(type) {
	case *plan.Expr_Col:
		if ids, ok := colMap[[2]int32{ne.Col.RelPos, ne.Col.ColPos}]; ok {
			ne.Col.RelPos = ids[0]
			ne.Col.ColPos = ids[1]
		} else {
			return errors.New("", fmt.Sprintf("can't find column in context's map %v", colMap))
		}

	case *plan.Expr_F:
		for _, arg := range ne.F.GetArgs() {
			err := builder.remapExpr(arg, colMap)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type ColRefRemapping struct {
	globalToLocal map[[2]int32][2]int32
	localToGlobal [][2]int32
}

func (m *ColRefRemapping) addColRef(colRef [2]int32) {
	m.globalToLocal[colRef] = [2]int32{0, int32(len(m.localToGlobal))}
	m.localToGlobal = append(m.localToGlobal, colRef)
}

func (builder *QueryBuilder) remapAllColRefs(nodeId int32, colRefCnt map[[2]int32]int) (*ColRefRemapping, error) {
	node := builder.qry.Nodes[nodeId]

	remapping := &ColRefRemapping{
		globalToLocal: make(map[[2]int32][2]int32),
	}

	switch node.NodeType {
	case plan.Node_TABLE_SCAN, plan.Node_MATERIAL_SCAN:
		for _, expr := range node.FilterList {
			increaseRefCnt(expr, colRefCnt)
		}

		internalRemapping := &ColRefRemapping{
			globalToLocal: make(map[[2]int32][2]int32),
		}

		tag := node.BindingTags[0]
		newTableDef := &plan.TableDef{
			Name: node.TableDef.Name,
			Defs: node.TableDef.Defs,
		}

		for i, col := range node.TableDef.Cols {
			globalRef := [2]int32{tag, int32(i)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			internalRemapping.addColRef(globalRef)

			newTableDef.Cols = append(newTableDef.Cols, col)
		}

		if len(newTableDef.Cols) == 0 {
			internalRemapping.addColRef([2]int32{tag, 0})
			newTableDef.Cols = append(newTableDef.Cols, node.TableDef.Cols[0])
		}

		node.TableDef = newTableDef

		for _, expr := range node.FilterList {
			decreaseRefCnt(expr, colRefCnt)
			err := builder.remapExpr(expr, internalRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}
		}

		for i, col := range node.TableDef.Cols {
			if colRefCnt[internalRemapping.localToGlobal[i]] == 0 {
				continue
			}

			remapping.addColRef(internalRemapping.localToGlobal[i])

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
					},
				},
			})
		}

		if len(node.ProjectList) == 0 {
			remapping.addColRef([2]int32{tag, 0})

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: node.TableDef.Cols[0].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
					},
				},
			})
		}

	case plan.Node_JOIN:
		for _, expr := range node.OnList {
			increaseRefCnt(expr, colRefCnt)
		}

		internalMap := make(map[[2]int32][2]int32)

		leftId := node.Children[0]
		leftRemapping, err := builder.remapAllColRefs(leftId, colRefCnt)
		if err != nil {
			return nil, err
		}

		for k, v := range leftRemapping.globalToLocal {
			internalMap[k] = v
		}

		rightId := node.Children[1]
		rightRemapping, err := builder.remapAllColRefs(rightId, colRefCnt)
		if err != nil {
			return nil, err
		}

		for k, v := range rightRemapping.globalToLocal {
			internalMap[k] = [2]int32{1, v[1]}
		}

		for _, expr := range node.OnList {
			decreaseRefCnt(expr, colRefCnt)
			err := builder.remapExpr(expr, internalMap)
			if err != nil {
				return nil, err
			}
		}

		childProjList := builder.qry.Nodes[leftId].ProjectList
		for i, globalRef := range leftRemapping.localToGlobal {
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjList[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
					},
				},
			})
		}

		if node.JoinType == plan.Node_MARK {
			globalRef := [2]int32{node.BindingTags[0], 0}
			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: &plan.Type{
					Id:       plan.Type_BOOL,
					Nullable: true,
					Size:     1,
				},
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: -1,
						ColPos: 0,
					},
				},
			})

			break
		}

		if node.JoinType != plan.Node_SEMI && node.JoinType != plan.Node_ANTI {
			childProjList = builder.qry.Nodes[rightId].ProjectList
			for i, globalRef := range rightRemapping.localToGlobal {
				if colRefCnt[globalRef] == 0 {
					continue
				}

				remapping.addColRef(globalRef)

				node.ProjectList = append(node.ProjectList, &plan.Expr{
					Typ: childProjList[i].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: 1,
							ColPos: int32(i),
						},
					},
				})
			}
		}

		if len(node.ProjectList) == 0 && len(leftRemapping.localToGlobal) > 0 {
			remapping.addColRef(leftRemapping.localToGlobal[0])

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: builder.qry.Nodes[leftId].ProjectList[0].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
					},
				},
			})
		}

	case plan.Node_AGG:
		for _, expr := range node.GroupBy {
			increaseRefCnt(expr, colRefCnt)
		}

		for _, expr := range node.AggList {
			increaseRefCnt(expr, colRefCnt)
		}

		childRemapping, err := builder.remapAllColRefs(node.Children[0], colRefCnt)
		if err != nil {
			return nil, err
		}

		groupTag := node.BindingTags[0]
		aggregateTag := node.BindingTags[1]

		for idx, expr := range node.GroupBy {
			decreaseRefCnt(expr, colRefCnt)
			err := builder.remapExpr(expr, childRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}

			globalRef := [2]int32{groupTag, int32(idx)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: -1,
						ColPos: int32(idx),
					},
				},
			})
		}

		for idx, expr := range node.AggList {
			decreaseRefCnt(expr, colRefCnt)
			err := builder.remapExpr(expr, childRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}

			globalRef := [2]int32{aggregateTag, int32(idx)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						RelPos: -2,
						ColPos: int32(idx),
					},
				},
			})
		}

		if len(node.ProjectList) == 0 {
			if len(node.GroupBy) > 0 {
				globalRef := [2]int32{groupTag, 0}
				remapping.addColRef(globalRef)

				node.ProjectList = append(node.ProjectList, &plan.Expr{
					Typ: node.GroupBy[0].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: -1,
							ColPos: 0,
						},
					},
				})
			} else {
				globalRef := [2]int32{aggregateTag, 0}
				remapping.addColRef(globalRef)

				node.ProjectList = append(node.ProjectList, &plan.Expr{
					Typ: node.AggList[0].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: -2,
							ColPos: 0,
						},
					},
				})
			}
		}

	case plan.Node_SORT:
		for _, orderBy := range node.OrderBy {
			increaseRefCnt(orderBy.Expr, colRefCnt)
		}

		childRemapping, err := builder.remapAllColRefs(node.Children[0], colRefCnt)
		if err != nil {
			return nil, err
		}

		for _, orderBy := range node.OrderBy {
			decreaseRefCnt(orderBy.Expr, colRefCnt)
			err := builder.remapExpr(orderBy.Expr, childRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}
		}

		childProjList := builder.qry.Nodes[node.Children[0]].ProjectList
		for i, globalRef := range childRemapping.localToGlobal {
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjList[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
					},
				},
			})
		}

		if len(node.ProjectList) == 0 && len(childRemapping.localToGlobal) > 0 {
			remapping.addColRef(childRemapping.localToGlobal[0])

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjList[0].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
					},
				},
			})
		}

	case plan.Node_FILTER:
		for _, expr := range node.FilterList {
			increaseRefCnt(expr, colRefCnt)
		}

		childRemapping, err := builder.remapAllColRefs(node.Children[0], colRefCnt)
		if err != nil {
			return nil, err
		}

		for _, expr := range node.FilterList {
			decreaseRefCnt(expr, colRefCnt)
			err := builder.remapExpr(expr, childRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}
		}

		childProjList := builder.qry.Nodes[node.Children[0]].ProjectList
		for i, globalRef := range childRemapping.localToGlobal {
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjList[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
					},
				},
			})
		}

		if len(node.ProjectList) == 0 {
			if len(childRemapping.localToGlobal) > 0 {
				remapping.addColRef(childRemapping.localToGlobal[0])
			}

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjList[0].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
					},
				},
			})
		}

	case plan.Node_PROJECT, plan.Node_MATERIAL:
		projectTag := node.BindingTags[0]

		var neededProj []int32

		for i, expr := range node.ProjectList {
			globalRef := [2]int32{projectTag, int32(i)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			neededProj = append(neededProj, int32(i))
			increaseRefCnt(expr, colRefCnt)
		}

		if len(neededProj) == 0 {
			increaseRefCnt(node.ProjectList[0], colRefCnt)
			neededProj = append(neededProj, 0)
		}

		childRemapping, err := builder.remapAllColRefs(node.Children[0], colRefCnt)
		if err != nil {
			return nil, err
		}

		var newProjList []*plan.Expr
		for _, needed := range neededProj {
			expr := node.ProjectList[needed]
			decreaseRefCnt(expr, colRefCnt)
			err := builder.remapExpr(expr, childRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}

			globalRef := [2]int32{projectTag, needed}
			remapping.addColRef(globalRef)

			newProjList = append(newProjList, expr)
		}

		node.ProjectList = newProjList

	case plan.Node_DISTINCT:
		childRemapping, err := builder.remapAllColRefs(node.Children[0], colRefCnt)
		if err != nil {
			return nil, err
		}

		// Rewrite DISTINCT to AGG
		node.NodeType = plan.Node_AGG
		preNode := builder.qry.Nodes[node.Children[0]]
		node.GroupBy = make([]*Expr, len(preNode.ProjectList))
		node.ProjectList = make([]*Expr, len(preNode.ProjectList))

		for i, prjExpr := range preNode.ProjectList {
			node.GroupBy[i] = &plan.Expr{
				Typ: prjExpr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
					},
				},
			}

			node.ProjectList[i] = &plan.Expr{
				Typ: prjExpr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: -1,
						ColPos: int32(i),
					},
				},
			}
		}

		remapping = childRemapping

	case plan.Node_VALUE_SCAN:
		// VALUE_SCAN always have one column now
		node.ProjectList = append(node.ProjectList, &plan.Expr{
			Typ:  &plan.Type{Id: plan.Type_INT64},
			Expr: &plan.Expr_C{C: &plan.Const{Value: &plan.Const_Ival{Ival: 0}}},
		})

	default:
		return nil, errors.New("", "unsupport node type")
	}

	node.BindingTags = nil

	return remapping, nil
}

func (builder *QueryBuilder) createQuery() (*Query, error) {
	for i, rootId := range builder.qry.Steps {
		rootId = builder.pushdownSemiAntiJoins(rootId)
		rootId, _ = builder.pushdownFilters(rootId, nil)
		rootId = builder.resolveJoinOrder(rootId)
		builder.qry.Steps[i] = rootId

		colRefCnt := make(map[[2]int32]int)
		rootNode := builder.qry.Nodes[rootId]
		resultTag := rootNode.BindingTags[0]
		for i := range rootNode.ProjectList {
			colRefCnt[[2]int32{resultTag, int32(i)}] = 1
		}

		_, err := builder.remapAllColRefs(rootId, colRefCnt)
		if err != nil {
			return nil, err
		}
	}
	return builder.qry, nil
}

func (builder *QueryBuilder) buildSelect(stmt *tree.Select, ctx *BindContext, isRoot bool) (int32, error) {
	// preprocess CTEs
	if stmt.With != nil {
		ctx.cteByName = make(map[string]*CTERef)
		maskedNames := make([]string, len(stmt.With.CTEs))

		for i := range stmt.With.CTEs {
			idx := len(stmt.With.CTEs) - i - 1
			cte := stmt.With.CTEs[idx]

			name := string(cte.Name.Alias)
			if _, ok := ctx.cteByName[name]; ok {
				return 0, errors.New("", fmt.Sprintf("WITH query name %q specified more than once", name))
			}

			var maskedCTEs map[string]any
			if len(maskedNames) > 0 {
				maskedCTEs = make(map[string]any)
				for _, mask := range maskedNames {
					maskedCTEs[mask] = nil
				}
			}

			maskedNames = append(maskedNames, name)

			ctx.cteByName[name] = &CTERef{
				ast:        cte,
				maskedCTEs: maskedCTEs,
			}
		}
	}

	var clause *tree.SelectClause

	switch selectClause := stmt.Select.(type) {
	case *tree.SelectClause:
		clause = selectClause
	case *tree.ParenSelect:
		return builder.buildSelect(selectClause.Select, ctx, isRoot)
	case *tree.UnionClause:
		return 0, errors.New("", fmt.Sprintf("'%s' will be supported in future version.", selectClause.Type.String()))
	case *tree.ValuesClause:
		return 0, errors.New("", "'SELECT FROM VALUES' will be supported in future version.")
	default:
		return 0, errors.New("", fmt.Sprintf("Statement '%s' will be supported in future version.", tree.String(stmt, dialect.MYSQL)))
	}

	// build FROM clause
	nodeId, err := builder.buildFrom(clause.From.Tables, ctx)
	if err != nil {
		return 0, err
	}

	ctx.binder = NewWhereBinder(builder, ctx)

	// unfold stars and generate headings
	var selectList tree.SelectExprs
	for _, selectExpr := range clause.Exprs {
		switch expr := selectExpr.Expr.(type) {
		case tree.UnqualifiedStar:
			cols, names, err := ctx.unfoldStar("")
			if err != nil {
				return 0, err
			}
			selectList = append(selectList, cols...)
			ctx.headings = append(ctx.headings, names...)

		case *tree.UnresolvedName:
			if expr.Star {
				cols, names, err := ctx.unfoldStar(expr.Parts[0])
				if err != nil {
					return 0, err
				}
				selectList = append(selectList, cols...)
				ctx.headings = append(ctx.headings, names...)
			} else {
				if len(selectExpr.As) > 0 {
					ctx.headings = append(ctx.headings, string(selectExpr.As))
				} else {
					ctx.headings = append(ctx.headings, expr.Parts[0])
				}

				newExpr, err := ctx.qualifyColumnNames(expr, nil, false)
				if err != nil {
					return 0, err
				}

				selectList = append(selectList, tree.SelectExpr{
					Expr: newExpr,
					As:   selectExpr.As,
				})
			}

		default:
			if len(selectExpr.As) > 0 {
				ctx.headings = append(ctx.headings, string(selectExpr.As))
			} else {
				for {
					if parenExpr, ok := expr.(*tree.ParenExpr); ok {
						expr = parenExpr.Expr
					} else {
						break
					}
				}
				ctx.headings = append(ctx.headings, tree.String(expr, dialect.MYSQL))
			}

			newExpr, err := ctx.qualifyColumnNames(expr, nil, false)
			if err != nil {
				return 0, err
			}

			selectList = append(selectList, tree.SelectExpr{
				Expr: newExpr,
				As:   selectExpr.As,
			})
		}
	}

	if len(selectList) == 0 {
		return 0, errors.New("", "No tables used")
	}

	// rewrite right join to left join
	builder.rewriteRightJoinToLeftJoin(nodeId)

	if clause.Where != nil {
		whereList, err := splitAndBindCondition(clause.Where.Expr, ctx)
		if err != nil {
			return 0, err
		}

		var newFilterList []*plan.Expr
		var expr *plan.Expr

		for _, cond := range whereList {
			nodeId, expr, err = builder.flattenSubqueries(nodeId, cond, ctx)
			if err != nil {
				return 0, err
			}

			if expr != nil {
				newFilterList = append(newFilterList, expr)
			}
		}

		nodeId = builder.appendNode(&plan.Node{
			NodeType:   plan.Node_FILTER,
			Children:   []int32{nodeId},
			FilterList: newFilterList,
		}, ctx)
	}

	ctx.groupTag = builder.genNewTag()
	ctx.aggregateTag = builder.genNewTag()
	ctx.projectTag = builder.genNewTag()

	// bind GROUP BY clause
	if clause.GroupBy != nil {
		groupBinder := NewGroupBinder(builder, ctx)
		for _, group := range clause.GroupBy {
			group, err = ctx.qualifyColumnNames(group, nil, false)
			if err != nil {
				return 0, err
			}

			_, err = groupBinder.BindExpr(group, 0, true)
			if err != nil {
				return 0, err
			}
		}
	}

	// bind HAVING clause
	var havingList []*plan.Expr
	havingBinder := NewHavingBinder(builder, ctx)
	if clause.Having != nil {
		ctx.binder = havingBinder
		havingList, err = splitAndBindCondition(clause.Having.Expr, ctx)
		if err != nil {
			return 0, err
		}
	}

	// bind SELECT clause (Projection List)
	projectionBinder := NewProjectionBinder(builder, ctx, havingBinder)
	ctx.binder = projectionBinder
	for _, selectExpr := range selectList {
		expr, err := projectionBinder.BindExpr(selectExpr.Expr, 0, true)
		if err != nil {
			return 0, err
		}

		alias := string(selectExpr.As)
		if len(alias) > 0 {
			ctx.aliasMap[alias] = int32(len(ctx.projects))
		}
		ctx.projects = append(ctx.projects, expr)
	}

	resultLen := len(ctx.projects)
	for i, proj := range ctx.projects {
		exprStr := proj.String()
		if _, ok := ctx.projectByExpr[exprStr]; !ok {
			ctx.projectByExpr[exprStr] = int32(i)
		}
	}

	ctx.isDistinct = clause.Distinct

	// bind ORDER BY clause
	var orderBys []*plan.OrderBySpec
	if stmt.OrderBy != nil {
		orderBinder := NewOrderBinder(projectionBinder, selectList)
		orderBys = make([]*plan.OrderBySpec, 0, len(stmt.OrderBy))

		for _, order := range stmt.OrderBy {
			expr, err := orderBinder.BindExpr(order.Expr)
			if err != nil {
				return 0, err
			}

			orderBy := &plan.OrderBySpec{
				Expr: expr,
			}

			switch order.Direction {
			case tree.DefaultDirection:
				orderBy.Flag = plan.OrderBySpec_INTERNAL
			case tree.Ascending:
				orderBy.Flag = plan.OrderBySpec_ASC
			case tree.Descending:
				orderBy.Flag = plan.OrderBySpec_DESC
			}

			orderBys = append(orderBys, orderBy)
		}
	}

	// bind limit/offset clause
	var limitExpr *Expr
	var offsetExpr *Expr
	if stmt.Limit != nil {
		limitBinder := NewLimitBinder()
		if stmt.Limit.Offset != nil {
			offsetExpr, err = limitBinder.BindExpr(stmt.Limit.Offset, 0, true)
			if err != nil {
				return 0, err
			}
		}
		if stmt.Limit.Count != nil {
			limitExpr, err = limitBinder.BindExpr(stmt.Limit.Count, 0, true)
			if err != nil {
				return 0, err
			}

			if cExpr, ok := limitExpr.Expr.(*plan.Expr_C); ok {
				if c, ok := cExpr.C.Value.(*plan.Const_Ival); ok {
					ctx.hasSingleRow = c.Ival == 1
				}
			}
		}
	}

	if (len(ctx.groups) > 0 || len(ctx.aggregates) > 0) && len(projectionBinder.boundCols) > 0 {
		return 0, errors.New("", fmt.Sprintf("column %q must appear in the GROUP BY clause or be used in an aggregate function", projectionBinder.boundCols[0]))
	}

	// FIXME: delete this when SINGLE join is ready
	if len(ctx.groups) == 0 && len(ctx.aggregates) > 0 {
		ctx.hasSingleRow = true
	}

	// append AGG node
	if len(ctx.groups) > 0 || len(ctx.aggregates) > 0 {
		nodeId = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_AGG,
			Children:    []int32{nodeId},
			GroupBy:     ctx.groups,
			AggList:     ctx.aggregates,
			BindingTags: []int32{ctx.groupTag, ctx.aggregateTag},
		}, ctx)

		if len(havingList) > 0 {
			var newFilterList []*plan.Expr
			var expr *plan.Expr

			for _, cond := range havingList {
				nodeId, expr, err = builder.flattenSubqueries(nodeId, cond, ctx)
				if err != nil {
					return 0, err
				}

				if expr != nil {
					newFilterList = append(newFilterList, expr)
				}
			}

			nodeId = builder.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{nodeId},
				FilterList: newFilterList,
			}, ctx)
		}
	}

	// append PROJECT node
	for i, proj := range ctx.projects {
		nodeId, proj, err = builder.flattenSubqueries(nodeId, proj, ctx)
		if err != nil {
			return 0, err
		}

		if proj == nil {
			// TODO: implement MARK join to better support non-scalar subqueries
			return 0, errors.New("", "non-scalar subquery in SELECT clause will be supported in future version.")
		}

		ctx.projects[i] = proj
	}

	nodeId = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		ProjectList: ctx.projects,
		Children:    []int32{nodeId},
		BindingTags: []int32{ctx.projectTag},
	}, ctx)

	// append DISTINCT node
	if clause.Distinct {
		nodeId = builder.appendNode(&plan.Node{
			NodeType: plan.Node_DISTINCT,
			Children: []int32{nodeId},
		}, ctx)
	}

	// append SORT node (include limit, offset)
	if len(orderBys) > 0 {
		nodeId = builder.appendNode(&plan.Node{
			NodeType: plan.Node_SORT,
			Children: []int32{nodeId},
			OrderBy:  orderBys,
		}, ctx)
	}

	if limitExpr != nil || offsetExpr != nil {
		node := builder.qry.Nodes[nodeId]

		node.Limit = limitExpr
		node.Offset = offsetExpr
	}

	// append result PROJECT node
	if builder.qry.Nodes[nodeId].NodeType != plan.Node_PROJECT {
		for i := 0; i < resultLen; i++ {
			ctx.results = append(ctx.results, &plan.Expr{
				Typ: ctx.projects[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: ctx.projectTag,
						ColPos: int32(i),
					},
				},
			})
		}

		ctx.resultTag = builder.genNewTag()

		nodeId = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_PROJECT,
			ProjectList: ctx.results,
			Children:    []int32{nodeId},
			BindingTags: []int32{ctx.resultTag},
		}, ctx)
	}

	if isRoot {
		builder.qry.Headings = append(builder.qry.Headings, ctx.headings...)
	}

	return nodeId, nil
}

func (builder *QueryBuilder) appendNode(node *plan.Node, ctx *BindContext) int32 {
	nodeId := int32(len(builder.qry.Nodes))
	node.NodeId = nodeId
	builder.qry.Nodes = append(builder.qry.Nodes, node)
	builder.ctxByNode = append(builder.ctxByNode, ctx)
	return nodeId
}

func (builder *QueryBuilder) rewriteRightJoinToLeftJoin(nodeId int32) {
	node := builder.qry.Nodes[nodeId]
	if node.NodeType == plan.Node_JOIN {
		builder.rewriteRightJoinToLeftJoin(node.Children[0])
		builder.rewriteRightJoinToLeftJoin(node.Children[1])

		if node.JoinType == plan.Node_RIGHT {
			node.JoinType = plan.Node_LEFT
			node.Children = []int32{node.Children[1], node.Children[0]}
		}
	} else if len(node.Children) > 0 {
		builder.rewriteRightJoinToLeftJoin(node.Children[0])
	}
}

func (builder *QueryBuilder) buildFrom(stmt tree.TableExprs, ctx *BindContext) (int32, error) {
	if len(stmt) == 1 {
		return builder.buildTable(stmt[0], ctx)
	}

	var rightChildId int32
	leftCtx := NewBindContext(builder, ctx)
	rightCtx := NewBindContext(builder, ctx)

	nodeId, err := builder.buildTable(stmt[0], leftCtx)
	if err != nil {
		return 0, err
	}

	rightChildId, err = builder.buildTable(stmt[1], rightCtx)
	if err != nil {
		return 0, err
	}

	nodeId = builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{nodeId, rightChildId},
		JoinType: plan.Node_INNER,
	}, nil)

	// build the rest table with preNode as join step by step
	for i := 2; i < len(stmt); i++ {
		newCtx := NewBindContext(builder, ctx)

		builder.ctxByNode[nodeId] = newCtx
		err = newCtx.mergeContexts(leftCtx, rightCtx)
		if err != nil {
			return 0, err
		}

		rightCtx = NewBindContext(builder, ctx)
		rightChildId, err = builder.buildTable(stmt[i], rightCtx)
		if err != nil {
			return 0, err
		}

		nodeId = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{nodeId, rightChildId},
			JoinType: plan.Node_INNER,
		}, nil)

		leftCtx = newCtx
	}

	builder.ctxByNode[nodeId] = ctx
	err = ctx.mergeContexts(leftCtx, rightCtx)

	return nodeId, err
}

func (builder *QueryBuilder) buildTable(stmt tree.TableExpr, ctx *BindContext) (nodeId int32, err error) {
	switch tbl := stmt.(type) {
	case *tree.Select:
		subCtx := NewBindContext(builder, ctx)
		nodeId, err = builder.buildSelect(tbl, subCtx, false)
		if subCtx.isCorrelated {
			return 0, errors.New("", "correlated subquery in FROM clause is will be supported in future version")
		}

		if subCtx.hasSingleRow {
			ctx.hasSingleRow = true
		}

	case *tree.TableName:
		schema := string(tbl.SchemaName)
		table := string(tbl.ObjectName)
		if len(table) == 0 || table == "dual" { //special table name
			nodeId = builder.appendNode(&plan.Node{
				NodeType: plan.Node_VALUE_SCAN,
			}, ctx)

			ctx.hasSingleRow = true

			break
		}

		if len(schema) == 0 {
			cteRef := ctx.findCTE(table)
			if cteRef != nil {
				subCtx := NewBindContext(builder, ctx)
				subCtx.maskedCTEs = cteRef.maskedCTEs
				subCtx.cteName = table

				switch stmt := cteRef.ast.Stmt.(type) {
				case *tree.Select:
					nodeId, err = builder.buildSelect(stmt, subCtx, false)

				case *tree.ParenSelect:
					nodeId, err = builder.buildSelect(stmt.Select, subCtx, false)

				default:
					err = errors.New("", fmt.Sprintf("unexpected statement: '%v'", tree.String(stmt, dialect.MYSQL)))
				}

				if err != nil {
					return
				}

				if subCtx.isCorrelated {
					return 0, errors.New("", "correlated column in CTE is will be supported in future version")
				}

				if subCtx.hasSingleRow {
					ctx.hasSingleRow = true
				}

				cols := cteRef.ast.Name.Cols

				if len(cols) > len(subCtx.headings) {
					return 0, errors.New("", fmt.Sprintf("table %q has %d columns available but %d columns specified", table, len(subCtx.headings), len(cols)))
				}

				for i, col := range cols {
					subCtx.headings[i] = string(col)
				}

				break
			}
		}

		obj, tableDef := builder.compCtx.Resolve(schema, table)
		if tableDef == nil {
			return 0, errors.New("", fmt.Sprintf("table %q does not exist", table))
		}

		nodeId = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_TABLE_SCAN,
			ObjRef:      obj,
			TableDef:    tableDef,
			BindingTags: []int32{builder.genNewTag()},
		}, ctx)

	case *tree.JoinTableExpr:
		return builder.buildJoinTable(tbl, ctx)

	case *tree.ParenTableExpr:
		return builder.buildTable(tbl.Expr, ctx)

	case *tree.AliasedTableExpr: //allways AliasedTableExpr first
		if _, ok := tbl.Expr.(*tree.Select); ok {
			if tbl.As.Alias == "" {
				return 0, errors.New("", fmt.Sprintf("subquery in FROM must have an alias: %T", stmt))
			}
		}

		nodeId, err = builder.buildTable(tbl.Expr, ctx)
		if err != nil {
			return
		}

		err = builder.addBinding(nodeId, tbl.As, ctx)

		return

	case *tree.StatementSource:
		// log.Printf("StatementSource")
		return 0, errors.New("", fmt.Sprintf("unsupport table expr: %T", stmt))

	default:
		// Values table not support
		return 0, errors.New("", fmt.Sprintf("unsupport table expr: %T", stmt))
	}

	return
}

func (builder *QueryBuilder) genNewTag() int32 {
	builder.nextTag++
	return builder.nextTag
}

func (builder *QueryBuilder) addBinding(nodeId int32, alias tree.AliasClause, ctx *BindContext) error {
	node := builder.qry.Nodes[nodeId]

	if node.NodeType == plan.Node_VALUE_SCAN {
		return nil
	}

	var cols []string
	var types []*plan.Type
	var binding *Binding

	if node.NodeType == plan.Node_TABLE_SCAN || node.NodeType == plan.Node_MATERIAL_SCAN {
		if len(alias.Cols) > len(node.TableDef.Cols) {
			return errors.New("", fmt.Sprintf("table %q has %d columns available but %d columns specified", alias.Alias, len(node.TableDef.Cols), len(alias.Cols)))
		}

		var table string
		if alias.Alias != "" {
			table = string(alias.Alias)
		} else {
			table = node.TableDef.Name
		}

		if _, ok := ctx.bindingByTable[table]; ok {
			return errors.New("", fmt.Sprintf("table name %q specified more than once", table))
		}

		cols = make([]string, len(node.TableDef.Cols))
		types = make([]*plan.Type, len(node.TableDef.Cols))

		for i, col := range node.TableDef.Cols {
			if i < len(alias.Cols) {
				cols[i] = string(alias.Cols[i])
			} else {
				cols[i] = col.Name
			}
			types[i] = col.Typ
		}

		binding = NewBinding(node.BindingTags[0], nodeId, table, cols, types)
	} else {
		// Subquery
		subCtx := builder.ctxByNode[nodeId]
		headings := subCtx.headings
		projects := subCtx.projects

		if len(alias.Cols) > len(headings) {
			return errors.New("", fmt.Sprintf("table %q has %d columns available but %d columns specified", alias.Alias, len(headings), len(alias.Cols)))
		}

		table := subCtx.cteName
		if len(alias.Alias) > 0 {
			table = string(alias.Alias)
		}
		if _, ok := ctx.bindingByTable[table]; ok {
			return errors.New("", fmt.Sprintf("table name %q specified more than once", table))
		}

		cols = make([]string, len(headings))
		types = make([]*plan.Type, len(headings))

		for i, col := range headings {
			if i < len(alias.Cols) {
				cols[i] = string(alias.Cols[i])
			} else {
				cols[i] = col
			}
			types[i] = projects[i].Typ
		}

		binding = NewBinding(builder.ctxByNode[nodeId].rootTag(), nodeId, table, cols, types)
	}

	ctx.bindings = append(ctx.bindings, binding)
	ctx.bindingByTag[binding.tag] = binding
	ctx.bindingByTable[binding.table] = binding

	for _, col := range cols {
		if _, ok := ctx.bindingByCol[col]; ok {
			ctx.bindingByCol[col] = nil
		} else {
			ctx.bindingByCol[col] = binding
		}
	}

	ctx.bindingTree = &BindingTreeNode{
		binding: binding,
	}

	return nil
}

func (builder *QueryBuilder) buildJoinTable(tbl *tree.JoinTableExpr, ctx *BindContext) (int32, error) {
	var joinType plan.Node_JoinFlag

	switch tbl.JoinType {
	case tree.JOIN_TYPE_CROSS, tree.JOIN_TYPE_INNER, tree.JOIN_TYPE_NATURAL:
		joinType = plan.Node_INNER
	case tree.JOIN_TYPE_LEFT, tree.JOIN_TYPE_NATURAL_LEFT:
		joinType = plan.Node_LEFT
	case tree.JOIN_TYPE_RIGHT, tree.JOIN_TYPE_NATURAL_RIGHT:
		joinType = plan.Node_RIGHT
	case tree.JOIN_TYPE_FULL:
		joinType = plan.Node_OUTER
	}

	leftCtx := NewBindContext(builder, ctx)
	rightCtx := NewBindContext(builder, ctx)

	leftChildId, err := builder.buildTable(tbl.Left, leftCtx)
	if err != nil {
		return 0, err
	}

	rightChildId, err := builder.buildTable(tbl.Right, rightCtx)
	if err != nil {
		return 0, err
	}

	err = ctx.mergeContexts(leftCtx, rightCtx)
	if err != nil {
		return 0, err
	}

	nodeId := builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{leftChildId, rightChildId},
		JoinType: joinType,
	}, ctx)
	node := builder.qry.Nodes[nodeId]

	ctx.binder = NewTableBinder(builder, ctx)

	switch cond := tbl.Cond.(type) {
	case *tree.OnJoinCond:
		joinConds, err := splitAndBindCondition(cond.Expr, ctx)
		if err != nil {
			return 0, err
		}

		node.OnList = joinConds

	case *tree.UsingJoinCond:
		for _, col := range cond.Cols {
			expr, err := ctx.addUsingCol(string(col), joinType, leftCtx, rightCtx)
			if err != nil {
				return 0, err
			}

			node.OnList = append(node.OnList, expr)
		}

	default:
		if tbl.JoinType == tree.JOIN_TYPE_NATURAL || tbl.JoinType == tree.JOIN_TYPE_NATURAL_LEFT || tbl.JoinType == tree.JOIN_TYPE_NATURAL_RIGHT {
			leftCols := make(map[string]any)
			for _, binding := range leftCtx.bindings {
				for _, col := range binding.cols {
					leftCols[col] = nil
				}
			}

			var usingCols []string
			for _, binding := range rightCtx.bindings {
				for _, col := range binding.cols {
					if _, ok := leftCols[col]; ok {
						usingCols = append(usingCols, col)
					}
				}
			}

			for _, col := range usingCols {
				expr, err := ctx.addUsingCol(col, joinType, leftCtx, rightCtx)
				if err != nil {
					return 0, err
				}

				node.OnList = append(node.OnList, expr)
			}
		}
	}

	return nodeId, nil
}

func (builder *QueryBuilder) pushdownFilters(nodeId int32, filters []*plan.Expr) (int32, []*plan.Expr) {
	node := builder.qry.Nodes[nodeId]

	var canPushdown, cantPushdown []*plan.Expr

	switch node.NodeType {
	case plan.Node_AGG:
		groupTag := node.BindingTags[0]
		aggregateTag := node.BindingTags[1]

		for _, filter := range filters {
			if !containsTag(filter, aggregateTag) {
				canPushdown = append(canPushdown, replaceColRefs(filter, groupTag, node.GroupBy))
			} else {
				cantPushdown = append(cantPushdown, filter)
			}
		}

		childId, cantPushdownChild := builder.pushdownFilters(node.Children[0], canPushdown)

		if len(cantPushdownChild) > 0 {
			childId = builder.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{node.Children[0]},
				FilterList: cantPushdownChild,
			}, nil)
		}

		node.Children[0] = childId

	case plan.Node_FILTER:
		canPushdown = filters
		for _, filter := range node.FilterList {
			canPushdown = append(canPushdown, splitPlanConjunction(applyDistributivity(filter))...)
		}

		childId, cantPushdownChild := builder.pushdownFilters(node.Children[0], canPushdown)

		if len(cantPushdownChild) > 0 {
			node.Children[0] = childId
			node.FilterList = cantPushdownChild
		} else {
			nodeId = childId
		}

	case plan.Node_JOIN:
		leftTags := make(map[int32]*Binding)
		for _, tag := range builder.enumerateTags(node.Children[0]) {
			leftTags[tag] = nil
		}

		rightTags := make(map[int32]*Binding)
		for _, tag := range builder.enumerateTags(node.Children[1]) {
			rightTags[tag] = nil
		}

		if node.JoinType == plan.Node_INNER {
			for _, cond := range node.OnList {
				filters = append(filters, splitPlanConjunction(applyDistributivity(cond))...)
			}

			node.OnList = nil
		}

		var leftPushdown, rightPushdown []*plan.Expr
		var turnInner bool

		joinSides := make([]int8, len(filters))

		for i, filter := range filters {
			canTurnInner := true

			joinSides[i] = getJoinSide(filter, leftTags, rightTags)
			if f, ok := filter.Expr.(*plan.Expr_F); ok {
				for _, arg := range f.F.Args {
					if getJoinSide(arg, leftTags, rightTags) == JoinSideBoth {
						canTurnInner = false
						break
					}
				}
			}

			if joinSides[i]&JoinSideRight != 0 && canTurnInner && node.JoinType == plan.Node_LEFT && rejectsNull(filter) {
				turnInner = true
				for _, cond := range node.OnList {
					filters = append(filters, splitPlanConjunction(applyDistributivity(cond))...)
				}

				node.JoinType = plan.Node_INNER
				node.OnList = nil
				turnInner = true

				break
			}

			// TODO: FULL OUTER join should be handled here. However we don't have FULL OUTER join now.
		}

		if turnInner {
			joinSides = make([]int8, len(filters))

			for i, filter := range filters {
				joinSides[i] = getJoinSide(filter, leftTags, rightTags)
			}
		} else if node.JoinType == plan.Node_LEFT {
			var newOnList []*plan.Expr
			for _, cond := range node.OnList {
				conj := splitPlanConjunction(applyDistributivity(cond))
				for _, conjElem := range conj {
					side := getJoinSide(conjElem, leftTags, rightTags)
					if side&JoinSideLeft == 0 {
						rightPushdown = append(rightPushdown, conjElem)
					} else {
						newOnList = append(newOnList, conjElem)
					}
				}
			}

			node.OnList = newOnList
		}

		for i, filter := range filters {
			switch joinSides[i] {
			case JoinSideNone:
				if c, ok := filter.Expr.(*plan.Expr_C); ok {
					if c, ok := c.C.Value.(*plan.Const_Bval); ok {
						if c.Bval {
							break
						}
					}
				}

				switch node.JoinType {
				case plan.Node_INNER:
					leftPushdown = append(leftPushdown, DeepCopyExpr(filter))
					rightPushdown = append(rightPushdown, filter)

				case plan.Node_LEFT, plan.Node_SEMI, plan.Node_ANTI:
					leftPushdown = append(leftPushdown, filter)

				default:
					cantPushdown = append(cantPushdown, filter)
				}

			case JoinSideLeft:
				if node.JoinType != plan.Node_OUTER {
					leftPushdown = append(leftPushdown, filter)
				} else {
					cantPushdown = append(cantPushdown, filter)
				}

			case JoinSideRight:
				if node.JoinType == plan.Node_INNER {
					rightPushdown = append(rightPushdown, filter)
				} else {
					cantPushdown = append(cantPushdown, filter)
				}

			case JoinSideBoth:
				if node.JoinType == plan.Node_INNER {
					if f, ok := filter.Expr.(*plan.Expr_F); ok {
						if f.F.Func.ObjName == "=" {
							if getJoinSide(f.F.Args[0], leftTags, rightTags) != JoinSideBoth {
								if getJoinSide(f.F.Args[1], leftTags, rightTags) != JoinSideBoth {
									node.OnList = append(node.OnList, filter)
									break
								}
							}
						}
					}
				}

				cantPushdown = append(cantPushdown, filter)
			}
		}

		childId, cantPushdownChild := builder.pushdownFilters(node.Children[0], leftPushdown)

		if len(cantPushdownChild) > 0 {
			childId = builder.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{node.Children[0]},
				FilterList: cantPushdownChild,
			}, nil)
		}

		node.Children[0] = childId

		childId, cantPushdownChild = builder.pushdownFilters(node.Children[1], rightPushdown)

		if len(cantPushdownChild) > 0 {
			childId = builder.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{node.Children[1]},
				FilterList: cantPushdownChild,
			}, nil)
		}

		node.Children[1] = childId

	case plan.Node_PROJECT:
		child := builder.qry.Nodes[node.Children[0]]
		if child.NodeType == plan.Node_VALUE_SCAN && child.RowsetData == nil {
			cantPushdown = filters
			break
		}

		projectTag := node.BindingTags[0]

		for _, filter := range filters {
			canPushdown = append(canPushdown, replaceColRefs(filter, projectTag, node.ProjectList))
		}

		childId, cantPushdownChild := builder.pushdownFilters(node.Children[0], canPushdown)

		if len(cantPushdownChild) > 0 {
			childId = builder.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{node.Children[0]},
				FilterList: cantPushdownChild,
			}, nil)
		}

		node.Children[0] = childId

	case plan.Node_TABLE_SCAN:
		node.FilterList = append(node.FilterList, filters...)

	default:
		if len(node.Children) > 0 {
			childId, cantPushdownChild := builder.pushdownFilters(node.Children[0], filters)

			if len(cantPushdownChild) > 0 {
				childId = builder.appendNode(&plan.Node{
					NodeType:   plan.Node_FILTER,
					Children:   []int32{node.Children[0]},
					FilterList: cantPushdownChild,
				}, nil)
			}

			node.Children[0] = childId
		} else {
			cantPushdown = filters
		}
	}

	return nodeId, cantPushdown
}

func (builder *QueryBuilder) pushdownSemiAntiJoins(nodeId int32) int32 {
	node := builder.qry.Nodes[nodeId]

	for i, childId := range node.Children {
		node.Children[i] = builder.pushdownSemiAntiJoins(childId)
	}

	if node.NodeType != plan.Node_JOIN {
		return nodeId
	}

	if node.JoinType != plan.Node_SEMI && node.JoinType != plan.Node_ANTI {
		return nodeId
	}

	for _, filter := range node.OnList {
		if f, ok := filter.Expr.(*plan.Expr_F); ok {
			if f.F.Func.ObjName != "=" {
				return nodeId
			}
		}
	}

	var targetNode *plan.Node
	var targetSide int32

	joinNode := builder.qry.Nodes[node.Children[0]]

	for {
		if joinNode.NodeType != plan.Node_JOIN {
			break
		}

		if joinNode.JoinType != plan.Node_INNER && joinNode.JoinType != plan.Node_LEFT {
			break
		}

		leftTags := make(map[int32]*Binding)
		for _, tag := range builder.enumerateTags(joinNode.Children[0]) {
			leftTags[tag] = nil
		}

		rightTags := make(map[int32]*Binding)
		for _, tag := range builder.enumerateTags(joinNode.Children[1]) {
			rightTags[tag] = nil
		}

		var joinSide int8
		for _, cond := range node.OnList {
			joinSide |= getJoinSide(cond, leftTags, rightTags)
		}

		if joinSide == JoinSideLeft {
			targetNode = joinNode
			targetSide = 0
			joinNode = builder.qry.Nodes[joinNode.Children[0]]
		} else if joinNode.JoinType == plan.Node_INNER && joinSide == JoinSideRight {
			targetNode = joinNode
			targetSide = 1
			joinNode = builder.qry.Nodes[joinNode.Children[1]]
		} else {
			break
		}
	}

	if targetNode != nil {
		nodeId = node.Children[0]
		node.Children[0] = targetNode.Children[targetSide]
		targetNode.Children[targetSide] = node.NodeId
	}

	return nodeId
}

func (builder *QueryBuilder) resolveJoinOrder(nodeId int32) int32 {
	node := builder.qry.Nodes[nodeId]

	if node.NodeType != plan.Node_JOIN || node.JoinType != plan.Node_INNER {
		for i, child := range node.Children {
			node.Children[i] = builder.resolveJoinOrder(child)
		}

		return nodeId
	}

	leaves, conds := builder.gatherJoinLeavesAndConds(node, nil, nil)

	sort.Slice(leaves, func(i, j int) bool {
		if leaves[j].Cost == nil {
			return false
		}

		if leaves[i].Cost == nil {
			return true
		}

		return leaves[i].Cost.Card < leaves[j].Cost.Card
	})

	leafByTag := make(map[int32]int32)

	for i, leaf := range leaves {
		tags := builder.enumerateTags(leaf.NodeId)

		for _, tag := range tags {
			leafByTag[tag] = int32(i)
		}
	}

	nLeaf := int32(len(leaves))

	adjMat := make([]bool, nLeaf*nLeaf)
	firstConnected := nLeaf
	visited := make([]bool, nLeaf)

	for _, cond := range conds {
		hyperEdge := make(map[int32]any)
		getHyperEdgeFromExpr(cond, leafByTag, hyperEdge)

		for i := range hyperEdge {
			if i < firstConnected {
				firstConnected = i
			}
			for j := range hyperEdge {
				adjMat[int32(nLeaf)*i+j] = true
			}
		}
	}

	if firstConnected < nLeaf {
		nodeId = leaves[firstConnected].NodeId
		visited[firstConnected] = true

		eligible := adjMat[firstConnected*nLeaf : (firstConnected+1)*nLeaf]

		for {
			nextSibling := nLeaf
			for i := range eligible {
				if !visited[i] && eligible[i] {
					nextSibling = int32(i)
					break
				}
			}

			if nextSibling == nLeaf {
				break
			}

			visited[nextSibling] = true

			nodeId = builder.appendNode(&plan.Node{
				NodeType: plan.Node_JOIN,
				Children: []int32{nodeId, leaves[nextSibling].NodeId},
				JoinType: plan.Node_INNER,
			}, nil)

			for i, adj := range adjMat[nextSibling*nLeaf : (nextSibling+1)*nLeaf] {
				eligible[i] = eligible[i] || adj
			}
		}

		for i := range visited {
			if !visited[i] {
				nodeId = builder.appendNode(&plan.Node{
					NodeType: plan.Node_JOIN,
					Children: []int32{nodeId, leaves[i].NodeId},
					JoinType: plan.Node_INNER,
				}, nil)
			}
		}
	} else {
		nodeId = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{leaves[0].NodeId, leaves[1].NodeId},
			JoinType: plan.Node_INNER,
		}, nil)

		for i := 2; i < len(leaves); i++ {
			nodeId = builder.appendNode(&plan.Node{
				NodeType: plan.Node_JOIN,
				Children: []int32{nodeId, leaves[i].NodeId},
				JoinType: plan.Node_INNER,
			}, nil)
		}
	}

	nodeId, _ = builder.pushdownFilters(nodeId, conds)

	return nodeId
}

func (builder *QueryBuilder) gatherJoinLeavesAndConds(joinNode *plan.Node, leaves []*plan.Node, conds []*plan.Expr) ([]*plan.Node, []*plan.Expr) {
	if joinNode.NodeType != plan.Node_JOIN || joinNode.JoinType != plan.Node_INNER {
		nodeId := builder.resolveJoinOrder(joinNode.NodeId)
		leaves = append(leaves, builder.qry.Nodes[nodeId])
		return leaves, conds
	}

	for _, childId := range joinNode.Children {
		leaves, conds = builder.gatherJoinLeavesAndConds(builder.qry.Nodes[childId], leaves, conds)
	}

	conds = append(conds, joinNode.OnList...)

	return leaves, conds
}

func (builder *QueryBuilder) enumerateTags(nodeId int32) []int32 {
	node := builder.qry.Nodes[nodeId]
	if len(node.BindingTags) > 0 {
		return node.BindingTags
	}

	var tags []int32

	for _, childId := range builder.qry.Nodes[nodeId].Children {
		tags = append(tags, builder.enumerateTags(childId)...)
	}

	return tags
}
