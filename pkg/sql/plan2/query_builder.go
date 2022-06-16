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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/errno"
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
		compCtx:    ctx,
		ctxByNode:  []*BindContext{},
		tagsByNode: [][]int32{},
		nextTag:    0,
	}
}

func getColMapKey(relPos int32, colPos int32) int64 {
	return (int64(relPos) << 32) + int64(colPos)
}

func (builder *QueryBuilder) remapExpr(expr *Expr, colMap map[int64][2]int32) error {
	switch ne := expr.Expr.(type) {
	case *plan.Expr_Col:
		if ids, ok := colMap[getColMapKey(ne.Col.RelPos, ne.Col.ColPos)]; ok {
			ne.Col.RelPos = ids[0]
			ne.Col.ColPos = ids[1]
		} else {
			return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("can't find column in context's map %v", colMap))
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

func (builder *QueryBuilder) remapAllColRefs(nodeId int32) (map[int64][2]int32, error) {
	node := builder.qry.Nodes[nodeId]
	returnMap := make(map[int64][2]int32)

	switch node.NodeType {
	case plan.Node_TABLE_SCAN, plan.Node_MATERIAL_SCAN:
		node.ProjectList = make([]*Expr, len(node.TableDef.Cols))
		tag := builder.tagsByNode[nodeId][0]

		for idx, col := range node.TableDef.Cols {
			node.ProjectList[idx] = &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(idx),
					},
				},
			}

			returnMap[getColMapKey(tag, int32(idx))] = [2]int32{0, int32(idx)}
		}
		for _, expr := range node.FilterList {
			err := builder.remapExpr(expr, returnMap)
			if err != nil {
				return nil, err
			}
		}

	case plan.Node_JOIN:
		joinCondMap := make(map[int64][2]int32)

		childId := node.Children[0]
		childMap, err := builder.remapAllColRefs(childId)
		if err != nil {
			return nil, err
		}

		for k, v := range childMap {
			returnMap[k] = v
			joinCondMap[k] = v
		}

		for prjIdx, prj := range builder.qry.Nodes[childId].ProjectList {
			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: prj.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(prjIdx),
					},
				},
			})
		}

		colIdx := int32(len(returnMap))
		childId = node.Children[1]

		if node.JoinType == plan.Node_MARK {
			returnMap[getColMapKey(builder.tagsByNode[nodeId][0], 0)] = [2]int32{0, colIdx}
			colIdx++
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
		}

		childMap, err = builder.remapAllColRefs(childId)
		if err != nil {
			return nil, err
		}

		for k, v := range childMap {
			returnMap[k] = [2]int32{0, colIdx + v[1]}
			joinCondMap[k] = [2]int32{1, v[1]}
		}

		if node.JoinType != plan.Node_SEMI && node.JoinType != plan.Node_ANTI && node.JoinType != plan.Node_MARK {
			for prjIdx, prj := range builder.qry.Nodes[childId].ProjectList {
				node.ProjectList = append(node.ProjectList, &plan.Expr{
					Typ: prj.Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: 1,
							ColPos: int32(prjIdx),
						},
					},
				})
			}
		}

		for _, expr := range node.OnList {
			err := builder.remapExpr(expr, joinCondMap)
			if err != nil {
				return nil, err
			}
		}

	case plan.Node_AGG:
		childMap, err := builder.remapAllColRefs(node.Children[0])
		if err != nil {
			return nil, err
		}

		node.ProjectList = make([]*Expr, len(node.GroupBy)+len(node.AggList))
		colIdx := 0
		groupTag := builder.tagsByNode[nodeId][0]
		aggregateTag := builder.tagsByNode[nodeId][1]

		for idx, expr := range node.GroupBy {
			err := builder.remapExpr(expr, childMap)
			if err != nil {
				return nil, err
			}

			node.ProjectList[colIdx] = &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: -1,
						ColPos: int32(idx),
					},
				},
			}

			returnMap[getColMapKey(groupTag, int32(idx))] = [2]int32{0, int32(colIdx)}
			colIdx++
		}

		for idx, expr := range node.AggList {
			err := builder.remapExpr(expr, childMap)
			if err != nil {
				return nil, err
			}

			node.ProjectList[colIdx] = &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: -2,
						ColPos: int32(idx),
					},
				},
			}

			returnMap[getColMapKey(aggregateTag, int32(idx))] = [2]int32{0, int32(colIdx)}
			colIdx++
		}

	case plan.Node_SORT:
		childMap, err := builder.remapAllColRefs(node.Children[0])
		if err != nil {
			return nil, err
		}

		for _, orderBy := range node.OrderBy {
			err := builder.remapExpr(orderBy.Expr, childMap)
			if err != nil {
				return nil, err
			}
		}

		preNode := builder.qry.Nodes[node.Children[0]]
		node.ProjectList = make([]*Expr, len(preNode.ProjectList))
		for prjIdx, prjExpr := range preNode.ProjectList {
			node.ProjectList[prjIdx] = &plan.Expr{
				Typ: prjExpr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(prjIdx),
					},
				},
			}
		}

		returnMap = childMap

	case plan.Node_FILTER:
		childMap, err := builder.remapAllColRefs(node.Children[0])
		if err != nil {
			return nil, err
		}

		for _, expr := range node.FilterList {
			err := builder.remapExpr(expr, childMap)
			if err != nil {
				return nil, err
			}
		}

		preNode := builder.qry.Nodes[node.Children[0]]
		node.ProjectList = make([]*Expr, len(preNode.ProjectList))
		for prjIdx, prjExpr := range preNode.ProjectList {
			node.ProjectList[prjIdx] = &Expr{
				Typ: prjExpr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(prjIdx),
					},
				},
			}
		}

		returnMap = childMap

	case plan.Node_PROJECT, plan.Node_MATERIAL:
		childMap, err := builder.remapAllColRefs(node.Children[0])
		if err != nil {
			return nil, err
		}

		if len(node.ProjectList) > 0 {
			projectTag := builder.tagsByNode[nodeId][0]
			for idx, expr := range node.ProjectList {
				err := builder.remapExpr(expr, childMap)
				if err != nil {
					return nil, err
				}
				returnMap[getColMapKey(projectTag, int32(idx))] = [2]int32{0, int32(idx)}
			}
		}

	case plan.Node_VALUE_SCAN:
		//do nothing,  optimize can merge valueScan and project

	default:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, "unsupport node type to rebiuld query")
	}

	return returnMap, nil
}

func (builder *QueryBuilder) createQuery() (*Query, error) {
	for _, rootId := range builder.qry.Steps {
		_, err := builder.remapAllColRefs(rootId)
		if err != nil {
			return nil, err
		}
	}
	return builder.qry, nil
}

func (builder *QueryBuilder) buildSelect(stmt *tree.Select, ctx *BindContext, isRoot bool) (int32, error) {
	// build CTEs
	err := builder.buildCTE(stmt.With, ctx)
	if err != nil {
		return 0, err
	}

	var clause *tree.SelectClause

	switch selectClause := stmt.Select.(type) {
	case *tree.SelectClause:
		clause = selectClause
	case *tree.ParenSelect:
		return builder.buildSelect(selectClause.Select, ctx, isRoot)
	default:
		return 0, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unknown select statement: %T", stmt))
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

				err = ctx.qualifyColumnNames(expr)
				if err != nil {
					return 0, err
				}

				selectList = append(selectList, tree.SelectExpr{
					Expr: expr,
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

			err = ctx.qualifyColumnNames(expr)
			if err != nil {
				return 0, err
			}

			selectList = append(selectList, tree.SelectExpr{
				Expr: expr,
				As:   selectExpr.As,
			})
		}
	}

	if len(selectList) == 0 {
		return 0, errors.New(errno.SyntaxErrororAccessRuleViolation, "No tables used")
	}

	// bind WHERE clause && append node to query
	if clause.Distinct {
		// rewrite distinct to group by
		if clause.GroupBy != nil {
			return 0, errors.New(errno.SyntaxErrororAccessRuleViolation, "distinct with group by is unsupported")
		}
		for _, selectExpr := range selectList {
			clause.GroupBy = append(clause.GroupBy, selectExpr.Expr)
		}
	}

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
			_, err := groupBinder.BindExpr(group, 0, true)
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
		err = ctx.qualifyColumnNames(selectExpr.Expr)
		if err != nil {
			return 0, err
		}

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
		}
	}

	if (len(ctx.groups) > 0 || len(ctx.aggregates) > 0) && len(projectionBinder.boundCols) > 0 {
		return 0, errors.New(errno.GroupingError, fmt.Sprintf("column %q must appear in the GROUP BY clause or be used in an aggregate function", projectionBinder.boundCols[0]))
	}

	// append AGG node
	if len(ctx.groups) > 0 || len(ctx.aggregates) > 0 {
		nodeId = builder.appendNode(&plan.Node{
			NodeType: plan.Node_AGG,
			Children: []int32{nodeId},
			GroupBy:  ctx.groups,
			AggList:  ctx.aggregates,
		}, ctx, ctx.groupTag, ctx.aggregateTag)

		if len(havingList) > 0 {
			for i, cond := range havingList {
				nodeId, havingList[i], err = builder.flattenSubqueries(nodeId, cond, ctx)
				if err != nil {
					return 0, err
				}
			}

			nodeId = builder.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{nodeId},
				FilterList: havingList,
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
			return 0, errors.New(errno.InternalError, "non-scalar subquery in SELECT clause not yet supported")
		}

		ctx.projects[i] = proj
	}

	nodeId = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		ProjectList: ctx.projects,
		Children:    []int32{nodeId},
	}, ctx, ctx.projectTag)

	// append SORT node (include limit, offset)
	if len(orderBys) > 0 || limitExpr != nil || offsetExpr != nil {
		nodeId = builder.appendNode(&plan.Node{
			NodeType: plan.Node_SORT,
			Children: []int32{nodeId},
			OrderBy:  orderBys,
			Limit:    limitExpr,
			Offset:   offsetExpr,
		}, ctx)
	}

	// append result PROJECT node
	if len(ctx.projects) > resultLen {
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
		}, ctx, ctx.resultTag)
	}

	if isRoot {
		builder.qry.Headings = append(builder.qry.Headings, ctx.headings...)
	}

	return nodeId, nil
}

func (builder *QueryBuilder) appendNode(node *plan.Node, ctx *BindContext, tags ...int32) int32 {
	nodeId := int32(len(builder.qry.Nodes))
	node.NodeId = nodeId
	builder.qry.Nodes = append(builder.qry.Nodes, node)
	builder.ctxByNode = append(builder.ctxByNode, ctx)
	builder.tagsByNode = append(builder.tagsByNode, tags)
	return nodeId
}

func (builder *QueryBuilder) buildCTE(withExpr *tree.With, ctx *BindContext) error {
	if withExpr == nil {
		return nil
	}

	var err error
	for _, cte := range withExpr.CTEs {
		var nodeId int32
		subCtx := NewBindContext(builder, ctx)

		switch stmt := cte.Stmt.(type) {
		case *tree.Select:
			nodeId, err = builder.buildSelect(stmt, subCtx, false)
		case *tree.ParenSelect:
			nodeId, err = builder.buildSelect(stmt.Select, subCtx, false)
		default:
			err = errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unexpected statement: '%v'", tree.String(stmt, dialect.MYSQL)))
		}
		if err != nil {
			return err
		}

		// add a projection node
		alias := string(cte.Name.Alias)

		tableDef := &TableDef{
			Name: alias,
			Cols: make([]*ColDef, len(subCtx.headings)),
		}

		if len(subCtx.headings) < len(cte.Name.Cols) {
			return errors.New(errno.InvalidColumnReference, "CTE table column length not match")
		}

		var col string
		for i, heading := range subCtx.headings {
			if i < len(cte.Name.Cols) {
				col = string(cte.Name.Cols[i])
			} else {
				col = heading
			}

			tableDef.Cols[i] = &ColDef{
				Name: col,
				Typ:  subCtx.projects[i].Typ,
			}
		}

		// set cte table to binderCtx
		ctx.cteTables[alias] = tableDef
		// append node
		cteNodeId := builder.appendNode(&plan.Node{
			NodeType: plan.Node_MATERIAL,
			Children: []int32{nodeId},
		}, subCtx)

		// set cte table node_id to step
		builder.qry.Steps = append(builder.qry.Steps, cteNodeId)
	}

	return nil
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

func (builder *QueryBuilder) bindTableRef(schema string, table string, compCtx CompilerContext, ctx *BindContext) (*ObjectRef, *TableDef, bool) {
	// FIXME: do CTEs have database/schema name?
	tableDef, ok := ctx.cteTables[table]
	for !ok && ctx.parent != nil {
		ctx = ctx.parent
		tableDef, ok = ctx.cteTables[table]
	}

	if ok {
		return &plan.ObjectRef{
			SchemaName: schema,
			ObjName:    table,
		}, tableDef, true
	}

	objRef, tableDef := compCtx.Resolve(schema, table)
	return objRef, tableDef, false
}

func (builder *QueryBuilder) buildTable(stmt tree.TableExpr, ctx *BindContext) (nodeId int32, err error) {
	switch tbl := stmt.(type) {
	case *tree.Select:
		newCtx := NewBindContext(builder, ctx)
		nodeId, err = builder.buildSelect(tbl, newCtx, false)
		if len(newCtx.corrCols) > 0 {
			return 0, errors.New(errno.InvalidColumnReference, "correlated subquery in FROM clause is not yet supported")
		}
		return

	case *tree.TableName:
		schema := string(tbl.SchemaName)
		table := string(tbl.ObjectName)
		if strings.ToLower(table) == "dual" { //special table name
			nodeId = builder.appendNode(&plan.Node{
				NodeType: plan.Node_VALUE_SCAN,
			}, ctx)
		} else {
			// FIXME
			obj, tableDef, isCte := builder.bindTableRef(schema, table, builder.compCtx, ctx)
			if tableDef == nil {
				return 0, errors.New(errno.InvalidTableDefinition, fmt.Sprintf("table %q does not exist", table))
			}

			var nodeType plan.Node_NodeType
			if isCte {
				nodeType = plan.Node_MATERIAL_SCAN
			} else {
				nodeType = plan.Node_TABLE_SCAN
			}

			nodeId = builder.appendNode(&plan.Node{
				NodeType: nodeType,
				ObjRef:   obj,
				TableDef: tableDef,
			}, ctx, builder.genNewTag())
		}
		return

	case *tree.JoinTableExpr:
		return builder.buildJoinTable(tbl, ctx)

	case *tree.ParenTableExpr:
		return builder.buildTable(tbl.Expr, ctx)

	case *tree.AliasedTableExpr: //allways AliasedTableExpr first
		if _, ok := tbl.Expr.(*tree.Select); ok {
			if tbl.As.Alias == "" {
				return 0, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("subquery in FROM must have an alias: %T", stmt))
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
		return 0, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport table expr: %T", stmt))
	}
	// Values table not support
	return 0, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport table expr: %T", stmt))
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
			return errors.New(errno.UndefinedColumn, fmt.Sprintf("table %q has %d columns available but %d columns specified", alias.Alias, len(node.TableDef.Cols), len(alias.Cols)))
		}

		var table string
		if alias.Alias != "" {
			table = string(alias.Alias)
		} else {
			table = node.TableDef.Name
		}

		if _, ok := ctx.bindingByTable[table]; ok {
			return errors.New(errno.DuplicateTable, fmt.Sprintf("table name %q specified more than once", table))
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

		binding = NewBinding(builder.tagsByNode[nodeId][0], nodeId, table, cols, types)
	} else {
		// Subquery
		if len(alias.Cols) > len(node.ProjectList) {
			return errors.New(errno.UndefinedColumn, fmt.Sprintf("table %q has %d columns available but %d columns specified", alias.Alias, len(node.ProjectList), len(alias.Cols)))
		}

		table := string(alias.Alias)
		if _, ok := ctx.bindingByTable[table]; ok {
			return errors.New(errno.DuplicateTable, fmt.Sprintf("table name %q specified more than once", table))
		}

		headings := builder.ctxByNode[nodeId].headings
		projects := builder.ctxByNode[nodeId].projects

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
		conds, err := splitAndBindCondition(cond.Expr, ctx)
		if err != nil {
			return 0, err
		}

		// FIXME: put all conditions in FILTER node and later use optimizer to push down

		var joinConds, filterConds, leftConds, rightConds []*plan.Expr
		for _, cond := range conds {
			side := getJoinSide(cond, leftCtx, rightCtx)
			var isEqui bool
			if f, ok := cond.Expr.(*plan.Expr_F); ok {
				if f.F.Func.ObjName == "=" {
					isEqui = true
				}
			}

			switch side {
			case 0b01:
				if joinType == plan.Node_INNER || joinType == plan.Node_RIGHT {
					leftConds = append(leftConds, cond)
				} else {
					joinConds = append(joinConds, cond)
				}

			case 0b10:
				if joinType == plan.Node_INNER || joinType == plan.Node_LEFT {
					rightConds = append(rightConds, cond)
				} else {
					joinConds = append(joinConds, cond)
				}

			case 0b11:
				if joinType != plan.Node_INNER || isEqui {
					joinConds = append(joinConds, cond)
				} else {
					filterConds = append(filterConds, cond)
				}

			default:
				// has correlated columns
				if joinType == plan.Node_INNER {
					filterConds = append(filterConds, cond)
				} else {
					return 0, errors.New(errno.InternalError, "correlated columns in join condition not yet supported")
				}
			}
		}

		node.OnList = joinConds

		if len(leftConds) > 0 {
			leftChild := builder.qry.Nodes[leftChildId]
			if leftChild.NodeType == plan.Node_FILTER {
				leftChild.FilterList = append(leftChild.FilterList, leftConds...)
			} else {
				node.Children[0] = builder.appendNode(&plan.Node{
					NodeType:   plan.Node_FILTER,
					Children:   []int32{leftChildId},
					FilterList: leftConds,
				}, leftCtx)
			}
		}

		if len(rightConds) > 0 {
			rightChild := builder.qry.Nodes[rightChildId]
			if rightChild.NodeType == plan.Node_FILTER {
				rightChild.FilterList = append(rightChild.FilterList, leftConds...)
			} else {
				node.Children[1] = builder.appendNode(&plan.Node{
					NodeType:   plan.Node_FILTER,
					Children:   []int32{rightChildId},
					FilterList: rightConds,
				}, rightCtx)
			}
		}

		if len(filterConds) > 0 {
			nodeId = builder.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{nodeId},
				FilterList: filterConds,
			}, ctx)
		}

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

func getJoinSide(expr *plan.Expr, leftCtx, rightCtx *BindContext) (side int8) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			side |= getJoinSide(arg, leftCtx, rightCtx)
		}

	case *plan.Expr_Col:
		if _, ok := leftCtx.bindingByTag[exprImpl.Col.RelPos]; ok {
			side = 0b01
		} else {
			side = 0b10
		}

	case *plan.Expr_Corr:
		side = 0b100
	}

	return
}
