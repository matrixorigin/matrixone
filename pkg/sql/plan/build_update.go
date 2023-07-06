// Copyright 2021 - 2022 Matrix Origin
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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildTableUpdate(stmt *tree.Update, ctx CompilerContext, isPrepareStmt bool) (p *Plan, err error) {
	tblInfo, err := getUpdateTableInfo(ctx, stmt)
	if err != nil {
		return nil, err
	}
	// new logic
	builder := NewQueryBuilder(plan.Query_SELECT, ctx, isPrepareStmt)
	queryBindCtx := NewBindContext(builder, nil)
	lastNodeId, updatePlanCtxs, err := selectUpdateTables(builder, queryBindCtx, stmt, tblInfo)
	if err != nil {
		return nil, err
	}
	err = rewriteUpdateQueryLastNode(builder, updatePlanCtxs, lastNodeId)
	if err != nil {
		return nil, err
	}

	sourceStep := builder.appendStep(lastNodeId)
	query, err := builder.createQuery()
	if err != nil {
		return nil, err
	}
	if !updatePlanCtxs[0].checkInsertPkDup {
		pkFilterExpr := getPkFilterExpr(builder, tblInfo.tableDefs[0])
		updatePlanCtxs[0].pkFilterExpr = pkFilterExpr
	}
	builder.qry.Steps = append(builder.qry.Steps[:sourceStep], builder.qry.Steps[sourceStep+1:]...)

	// append sink node
	if tblInfo.isMulti {
		lastNodeId = appendSinkNode(builder, queryBindCtx, lastNodeId)
		sourceStep = builder.appendStep(lastNodeId)
	} else {
		sourceStep = -1
	}

	beginIdx := 0
	for i, tableDef := range tblInfo.tableDefs {
		upPlanCtx := updatePlanCtxs[i]
		upPlanCtx.beginIdx = beginIdx
		upPlanCtx.sourceStep = sourceStep

		updateBindCtx := NewBindContext(builder, nil)
		beginIdx = beginIdx + upPlanCtx.updateColLength + len(tableDef.Cols)
		err = buildUpdatePlans(ctx, builder, updateBindCtx, upPlanCtx, lastNodeId)
		if err != nil {
			return nil, err
		}
		putDmlPlanCtx(upPlanCtx)
	}
	if err != nil {
		return nil, err
	}

	query.StmtType = plan.Query_UPDATE
	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, err
}

func isDefaultValExpr(e *Expr) bool {
	if ce, ok := e.Expr.(*plan.Expr_C); ok {
		_, isDefVal := ce.C.Value.(*plan.Const_Defaultval)
		return isDefVal
	}
	return false
}

func rewriteUpdateQueryLastNode(builder *QueryBuilder, planCtxs []*dmlPlanCtx, lastNodeId int32) error {
	var err error

	lastNode := builder.qry.Nodes[lastNodeId]

	idx := 0
	for _, planCtx := range planCtxs {
		tableDef := planCtx.tableDef

		for colIdx, col := range tableDef.Cols {
			if offset, ok := planCtx.updateColPosMap[col.Name]; ok {
				pos := idx + offset
				posExpr := lastNode.ProjectList[pos]
				if isDefaultValExpr(posExpr) { // set col = default
					lastNode.ProjectList[pos], err = getDefaultExpr(builder.GetContext(), col)
					if err != nil {
						return err
					}
					posExpr = lastNode.ProjectList[pos]
				}
				err = checkNotNull(builder.GetContext(), posExpr, tableDef, col)
				if err != nil {
					return err
				}
				lastNode.ProjectList[pos], err = forceCastExpr(builder.GetContext(), posExpr, col.Typ)
				if err != nil {
					return err
				}

			} else {
				pos := idx + colIdx
				if col.OnUpdate != nil && col.OnUpdate.Expr != nil {
					lastNode.ProjectList[pos] = col.OnUpdate.Expr
				}

				lastNode.ProjectList[pos], err = forceCastExpr(builder.GetContext(), lastNode.ProjectList[pos], col.Typ)
				if err != nil {
					return err
				}
			}
		}
		idx = planCtx.updateColLength + len(tableDef.Cols)
	}
	return nil
}

func selectUpdateTables(builder *QueryBuilder, bindCtx *BindContext, stmt *tree.Update, tableInfo *dmlTableInfo) (int32, []*dmlPlanCtx, error) {
	fromTables := &tree.From{
		Tables: stmt.Tables,
	}
	var selectList []tree.SelectExpr

	var aliasList = make([]string, len(tableInfo.alias))
	for alias, i := range tableInfo.alias {
		aliasList[i] = alias
	}

	updatePlanCtxs := make([]*dmlPlanCtx, len(aliasList))
	checkInsertPkDup := true
	if len(aliasList) == 1 && stmt.Where != nil {
		tableDef := tableInfo.tableDefs[0]
		if comp, ok := stmt.Where.Expr.(*tree.ComparisonExpr); ok {
			if comp.Op == tree.EQUAL {
				if name, ok := comp.Left.(*tree.UnresolvedName); ok {
					if name.NumParts == 1 && name.Parts[0] == tableDef.Pkey.PkeyColName {
						checkInsertPkDup = false
					}
				} else if name, ok := comp.Right.(*tree.UnresolvedName); ok {
					if name.NumParts == 1 && name.Parts[0] == tableDef.Pkey.PkeyColName {
						checkInsertPkDup = false
					}
				}
			}
		}
	}

	for i, alias := range aliasList {
		tableDef := tableInfo.tableDefs[i]
		updateKeys := tableInfo.updateKeys[i]

		// append  table.* to project list
		rowIdPos := -1
		for idx, col := range tableDef.Cols {
			e, _ := tree.NewUnresolvedName(builder.GetContext(), alias, col.Name)
			selectList = append(selectList, tree.SelectExpr{
				Expr: e,
			})
			if col.Name == catalog.Row_ID {
				rowIdPos = idx
			}
		}

		// add update expr to project list
		updateColPosMap := make(map[string]int)
		offset := len(tableDef.Cols)
		updatePkCol := false
		var pkNameMap = make(map[string]struct{})
		for _, pkName := range tableDef.Pkey.Names {
			pkNameMap[pkName] = struct{}{}
		}
		for colName, updateKey := range updateKeys {
			selectList = append(selectList, tree.SelectExpr{
				Expr: updateKey,
			})
			updateColPosMap[colName] = offset
			if _, ok := pkNameMap[colName]; ok {
				updatePkCol = true
			}
			offset++
		}

		// append  table.* to project list
		upPlanCtx := getDmlPlanCtx()
		upPlanCtx.objRef = tableInfo.objRef[i]
		upPlanCtx.tableDef = tableDef
		upPlanCtx.updateColLength = len(updateKeys)
		upPlanCtx.isMulti = tableInfo.isMulti
		upPlanCtx.needAggFilter = tableInfo.needAggFilter
		upPlanCtx.rowIdPos = rowIdPos
		upPlanCtx.updateColPosMap = updateColPosMap
		upPlanCtx.allDelTableIDs = map[uint64]struct{}{}
		upPlanCtx.checkInsertPkDup = checkInsertPkDup
		upPlanCtx.updatePkCol = updatePkCol

		for idx, col := range tableDef.Cols {
			// row_id、compPrimaryKey、clusterByKey will not inserted from old data
			if col.Hidden && col.Name != catalog.FakePrimaryKeyColName {
				continue
			}
			if offset, ok := updateColPosMap[col.Name]; ok {
				upPlanCtx.insertColPos = append(upPlanCtx.insertColPos, offset)
			} else {
				upPlanCtx.insertColPos = append(upPlanCtx.insertColPos, idx)
			}
		}

		updatePlanCtxs[i] = upPlanCtx
	}

	selectAst := &tree.Select{
		Select: &tree.SelectClause{
			Distinct: false,
			Exprs:    selectList,
			From:     fromTables,
			Where:    stmt.Where,
		},
		OrderBy: stmt.OrderBy,
		Limit:   stmt.Limit,
		With:    stmt.With,
	}

	//ftCtx := tree.NewFmtCtx(dialect.MYSQL)
	//selectAst.Format(ftCtx)
	//sql := ftCtx.String()
	//fmt.Print(sql)
	lastNodeId, err := builder.buildSelect(selectAst, bindCtx, false)
	if err != nil {
		return -1, nil, err
	}
	return lastNodeId, updatePlanCtxs, nil
}

func getPkFilterExpr(builder *QueryBuilder, tableDef *TableDef) *Expr {

	for _, node := range builder.qry.Nodes {
		if node.NodeType != plan.Node_FILTER || len(node.Children) != 1 {
			continue
		}

		preNode := builder.qry.Nodes[node.Children[0]]
		if preNode.NodeType != plan.Node_TABLE_SCAN || preNode.TableDef.TblId != tableDef.TblId {
			continue
		}

		basePkName := tableDef.Pkey.PkeyColName
		tblAndPkName := fmt.Sprintf("%s.%s", tableDef.Name, tableDef.Pkey.PkeyColName)
		if e, ok := node.FilterList[0].Expr.(*plan.Expr_F); ok && e.F.Func.ObjName == "=" {
			if pkExpr, ok := e.F.Args[0].Expr.(*plan.Expr_Col); ok && (pkExpr.Col.Name == tblAndPkName || pkExpr.Col.Name == basePkName) {
				return DeepCopyExpr(e.F.Args[1])
			}

			if pkExpr, ok := e.F.Args[1].Expr.(*plan.Expr_Col); ok && (pkExpr.Col.Name == tblAndPkName || pkExpr.Col.Name == basePkName) {
				return DeepCopyExpr(e.F.Args[0])
			}

		}
	}
	return nil
}
