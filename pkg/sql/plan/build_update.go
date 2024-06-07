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
	"go/constant"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

func buildTableUpdate(stmt *tree.Update, ctx CompilerContext, isPrepareStmt bool) (p *Plan, err error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementBuildUpdateHistogram.Observe(time.Since(start).Seconds())
	}()
	tblInfo, err := getUpdateTableInfo(ctx, stmt)
	if err != nil {
		return nil, err
	}
	// new logic
	builder := NewQueryBuilder(plan.Query_SELECT, ctx, isPrepareStmt, false)
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

	// if len(updatePlanCtxs) == 1 && updatePlanCtxs[0].updatePkCol {
	// 	pkFilterExpr := getPkFilterExpr(builder, lastNodeId, updatePlanCtxs[0], tblInfo.tableDefs[0])
	// 	updatePlanCtxs[0].pkFilterExprs = pkFilterExpr
	// }

	builder.qry.Steps = append(builder.qry.Steps[:sourceStep], builder.qry.Steps[sourceStep+1:]...)

	// append sink node
	lastNodeId = appendSinkNode(builder, queryBindCtx, lastNodeId)
	sourceStep = builder.appendStep(lastNodeId)

	beginIdx := 0
	var detectSqls []string
	for i, tableDef := range tblInfo.tableDefs {
		upPlanCtx := updatePlanCtxs[i]
		upPlanCtx.beginIdx = beginIdx
		upPlanCtx.sourceStep = sourceStep

		updateBindCtx := NewBindContext(builder, nil)
		beginIdx = beginIdx + upPlanCtx.updateColLength + len(tableDef.Cols)
		err = buildUpdatePlans(ctx, builder, updateBindCtx, upPlanCtx, false)
		if err != nil {
			return nil, err
		}
		putDmlPlanCtx(upPlanCtx)
		sqls, err := genSqlsForCheckFKSelfRefer(ctx.GetContext(),
			tblInfo.objRef[i].SchemaName, tableDef.Name, tableDef.Cols, tableDef.Fkeys)
		if err != nil {
			return nil, err
		}
		detectSqls = append(detectSqls, sqls...)
	}
	if err != nil {
		return nil, err
	}
	query.DetectSqls = detectSqls
	reduceSinkSinkScanNodes(query)
	ReCalcQueryStats(builder, query)
	reCheckifNeedLockWholeTable(builder)
	query.StmtType = plan.Query_UPDATE
	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, err
}

func isDefaultValExpr(e *Expr) bool {
	if ce, ok := e.Expr.(*plan.Expr_Lit); ok {
		_, isDefVal := ce.Lit.Value.(*plan.Literal_Defaultval)
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
				if col != nil && col.Typ.Id == int32(types.T_enum) {
					lastNode.ProjectList[pos], err = funcCastForEnumType(builder.GetContext(), posExpr, col.Typ)
					if err != nil {
						return err
					}
				} else {
					lastNode.ProjectList[pos], err = forceCastExpr(builder.GetContext(), posExpr, col.Typ)
					if err != nil {
						return err
					}
				}

			} else {
				pos := idx + colIdx
				if col.OnUpdate != nil && col.OnUpdate.Expr != nil {
					lastNode.ProjectList[pos] = col.OnUpdate.Expr
				}

				if col != nil && col.Typ.Id == int32(types.T_enum) {
					lastNode.ProjectList[pos], err = funcCastForEnumType(builder.GetContext(), lastNode.ProjectList[pos], col.Typ)
					if err != nil {
						return err
					}
				} else {
					lastNode.ProjectList[pos], err = forceCastExpr(builder.GetContext(), lastNode.ProjectList[pos], col.Typ)
					if err != nil {
						return err
					}
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
		updatePkColCount := 0
		var pkNameMap = make(map[string]struct{})
		if tableDef.Pkey != nil {
			for _, pkName := range tableDef.Pkey.Names {
				pkNameMap[pkName] = struct{}{}
			}
		}

		for colName, updateKey := range updateKeys {
			for _, coldef := range tableDef.Cols {
				if coldef.Name == colName && coldef.Typ.Id == int32(types.T_enum) {
					binder := NewDefaultBinder(builder.GetContext(), nil, nil, coldef.Typ, nil)
					updateKeyExpr, err := binder.BindExpr(updateKey, 0, false)
					if err != nil {
						return 0, nil, err
					}
					exprs := []tree.Expr{
						tree.NewNumValWithType(constant.MakeString(coldef.Typ.Enumvalues), coldef.Typ.Enumvalues, false, tree.P_char),
						updateKey,
					}
					if updateKeyExpr.Typ.Id >= 20 && updateKeyExpr.Typ.Id <= 29 {
						updateKey = &tree.FuncExpr{
							Func:  tree.FuncName2ResolvableFunctionReference(tree.SetUnresolvedName(moEnumCastIndexValueToIndexFun)),
							Type:  tree.FUNC_TYPE_DEFAULT,
							Exprs: exprs,
						}
					} else {
						updateKey = &tree.FuncExpr{
							Func:  tree.FuncName2ResolvableFunctionReference(tree.SetUnresolvedName(moEnumCastValueToIndexFun)),
							Type:  tree.FUNC_TYPE_DEFAULT,
							Exprs: exprs,
						}
					}
				}
			}
			selectList = append(selectList, tree.SelectExpr{
				Expr: updateKey,
			})
			updateColPosMap[colName] = offset
			if _, ok := pkNameMap[colName]; ok {
				updatePkColCount++
			}
			offset++
		}

		// we don't known if update pk if tableDef.Pkey is nil. just set true and let check pk dup work
		updatePkCol = updatePkColCount > 0

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
		upPlanCtx.allDelTables = map[FkReferKey]struct{}{}
		upPlanCtx.checkInsertPkDup = true
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

// todo:  seems this filter do not make any sense for check pk dup in update statement
//        need more research
// func getPkFilterExpr(builder *QueryBuilder, lastNodeId int32, upCtx *dmlPlanCtx, tableDef *TableDef) []*Expr {
// 	node := builder.qry.Nodes[lastNodeId]
// 	var pkNameMap = make(map[string]int)
// 	for idx, pkName := range tableDef.Pkey.Names {
// 		pkNameMap[pkName] = idx
// 	}

// 	filterExpr := make([]*Expr, len(tableDef.Pkey.Names))
// 	for colName, colIdx := range upCtx.updateColPosMap {
// 		if pkIdx, ok := pkNameMap[colName]; ok {
// 			switch e := node.ProjectList[colIdx].Expr.(type) {
// 			case *plan.Expr_C:
// 			case *plan.Expr_F:
// 				if e.F.Func.ObjName != "cast" {
// 					return nil
// 				}
// 				if _, isConst := e.F.Args[0].Expr.(*plan.Expr_C); !isConst {
// 					return nil
// 				}
// 			default:
// 				return nil
// 			}

// 			expr, err := bindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{{
// 				Typ: node.ProjectList[colIdx].Typ,
// 				Expr: &plan.Expr_Col{
// 					Col: &ColRef{
// 						ColPos: int32(pkIdx),
// 						Name:   tableDef.Pkey.PkeyColName,
// 					},
// 				},
// 			}, node.ProjectList[colIdx]})
// 			if err != nil {
// 				return nil
// 			}
// 			filterExpr[pkIdx] = expr
// 		}
// 	}
// 	return filterExpr
// }
