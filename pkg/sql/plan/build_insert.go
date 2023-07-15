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
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

func buildInsert(stmt *tree.Insert, ctx CompilerContext, isReplace bool, isPrepareStmt bool) (p *Plan, err error) {
	if isReplace {
		return nil, moerr.NewNotSupported(ctx.GetContext(), "Not support replace statement")
	}

	tblInfo, err := getDmlTableInfo(ctx, tree.TableExprs{stmt.Table}, nil, nil, "insert")
	if err != nil {
		return nil, err
	}
	rewriteInfo := &dmlSelectInfo{
		typ:     "insert",
		rootId:  -1,
		tblInfo: tblInfo,
	}
	tableDef := tblInfo.tableDefs[0]
	clusterTable, err := getAccountInfoOfClusterTable(ctx, stmt.Accounts, tableDef, tblInfo.isClusterTable[0])
	if err != nil {
		return nil, err
	}

	if len(stmt.OnDuplicateUpdate) > 0 && clusterTable.IsClusterTable {
		return nil, moerr.NewNotSupported(ctx.GetContext(), "INSERT ... ON DUPLICATE KEY UPDATE ... for cluster table")
	}

	builder := NewQueryBuilder(plan.Query_SELECT, ctx, isPrepareStmt)
	builder.haveOnDuplicateKey = len(stmt.OnDuplicateUpdate) > 0

	bindCtx := NewBindContext(builder, nil)
	checkInsertPkDup, pkPosInValues, err := initInsertStmt(builder, bindCtx, stmt, rewriteInfo)
	if err != nil {
		return nil, err
	}
	lastNodeId := rewriteInfo.rootId
	sourceStep := builder.appendStep(lastNodeId)
	query, err := builder.createQuery()
	if err != nil {
		return nil, err
	}
	var pkFilterExprs []*Expr
	if !checkInsertPkDup {
		pkFilterExprs = getPkValueExpr(builder, ctx, tableDef, pkPosInValues)
	}
	builder.qry.Steps = append(builder.qry.Steps[:sourceStep], builder.qry.Steps[sourceStep+1:]...)

	objRef := tblInfo.objRef[0]
	if len(rewriteInfo.onDuplicateIdx) > 0 {
		// append on duplicate key node
		tableDef = DeepCopyTableDef(tableDef)
		if tableDef.Pkey != nil && tableDef.Pkey.PkeyColName == catalog.CPrimaryKeyColName {
			//tableDef.Cols = append(tableDef.Cols, MakeHiddenColDefByName(catalog.CPrimaryKeyColName))
			tableDef.Cols = append(tableDef.Cols, tableDef.Pkey.CompPkeyCol)
		}
		if tableDef.ClusterBy != nil && util.JudgeIsCompositeClusterByColumn(tableDef.ClusterBy.Name) {
			//tableDef.Cols = append(tableDef.Cols, MakeHiddenColDefByName(tableDef.ClusterBy.Name))
			tableDef.Cols = append(tableDef.Cols, tableDef.ClusterBy.CompCbkeyCol)
		}
		dupProjection := getProjectionByLastNode(builder, lastNodeId)
		onDuplicateKeyNode := &Node{
			NodeType:    plan.Node_ON_DUPLICATE_KEY,
			Children:    []int32{lastNodeId},
			ProjectList: dupProjection,
			OnDuplicateKey: &plan.OnDuplicateKeyCtx{
				TableDef:        tableDef,
				OnDuplicateIdx:  rewriteInfo.onDuplicateIdx,
				OnDuplicateExpr: rewriteInfo.onDuplicateExpr,
			},
		}
		lastNodeId = builder.appendNode(onDuplicateKeyNode, bindCtx)

		// append project node to make batch like update logic, not insert
		updateColLength := 0
		updateColPosMap := make(map[string]int)
		var insertColPos []int
		var projectProjection []*Expr
		tableDef = DeepCopyTableDef(tableDef)
		tableDef.Cols = append(tableDef.Cols, MakeRowIdColDef())
		colLength := len(tableDef.Cols)
		rowIdPos := colLength - 1
		for _, col := range tableDef.Cols {
			if col.Hidden && col.Name != catalog.FakePrimaryKeyColName {
				continue
			}
			updateColLength++
		}
		for i, col := range tableDef.Cols {
			projectProjection = append(projectProjection, &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						ColPos: int32(i + updateColLength),
						Name:   col.Name,
					},
				},
			})
		}
		for i := 0; i < updateColLength; i++ {
			col := tableDef.Cols[i]
			projectProjection = append(projectProjection, &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						ColPos: int32(i),
						Name:   col.Name,
					},
				},
			})
			updateColPosMap[col.Name] = colLength + i
			insertColPos = append(insertColPos, colLength+i)
		}
		projectNode := &Node{
			NodeType:    plan.Node_PROJECT,
			Children:    []int32{lastNodeId},
			ProjectList: projectProjection,
		}
		lastNodeId = builder.appendNode(projectNode, bindCtx)

		// append sink node
		lastNodeId = appendSinkNode(builder, bindCtx, lastNodeId)
		sourceStep = builder.appendStep(lastNodeId)

		// append plans like update
		updateBindCtx := NewBindContext(builder, nil)
		upPlanCtx := getDmlPlanCtx()
		upPlanCtx.objRef = objRef
		upPlanCtx.tableDef = tableDef
		upPlanCtx.beginIdx = 0
		upPlanCtx.sourceStep = sourceStep
		upPlanCtx.isMulti = false
		upPlanCtx.updateColLength = updateColLength
		upPlanCtx.rowIdPos = rowIdPos
		upPlanCtx.insertColPos = insertColPos
		upPlanCtx.updateColPosMap = updateColPosMap
		upPlanCtx.checkInsertPkDup = checkInsertPkDup

		err = buildUpdatePlans(ctx, builder, updateBindCtx, upPlanCtx)
		if err != nil {
			return nil, err
		}
		putDmlPlanCtx(upPlanCtx)

		query.StmtType = plan.Query_UPDATE
	} else {
		err = buildInsertPlans(ctx, builder, bindCtx, objRef, tableDef, rewriteInfo.rootId, checkInsertPkDup, pkFilterExprs)
		if err != nil {
			return nil, err
		}
		query.StmtType = plan.Query_INSERT
	}
	reduceSinkSinkScanNodes(query)
	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, err
}

func getPkValueExpr(builder *QueryBuilder, ctx CompilerContext, tableDef *TableDef, pkPosInValues map[int]int) []*Expr {
	if builder.qry.Nodes[0].NodeType != plan.Node_VALUE_SCAN {
		return nil
	}

	pkPos, pkTyp := getPkPos(tableDef, true)
	if pkPos == -1 {
		if tableDef.Pkey.PkeyColName != catalog.CPrimaryKeyColName {
			return nil
		}
	} else if pkTyp.AutoIncr {
		return nil
	}

	node := builder.qry.Nodes[0]

	pkValueExprs := make([]*Expr, len(pkPosInValues))
	for idx, cols := range node.RowsetData.Cols {
		pkColIdx, ok := pkPosInValues[idx]
		if !ok {
			continue
		}
		if len(cols.Data) == 1 {
			rowExpr := DeepCopyExpr(cols.Data[0].Expr)
			e, err := forceCastExpr(builder.GetContext(), rowExpr, tableDef.Cols[idx].Typ)
			if err != nil {
				return nil
			}
			expr, err := bindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{{
				Typ: tableDef.Cols[idx].Typ,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						ColPos: int32(pkColIdx),
					},
				},
			}, e})
			if err != nil {
				return nil
			}
			pkValueExprs[pkColIdx] = expr
		}
	}
	proc := ctx.GetProcess()
	var bat *batch.Batch
	if builder.isPrepareStatement {
		bat = proc.GetPrepareBatch()
	} else {
		bat = proc.GetValueScanBatch(uuid.UUID(node.Uuid))
	}
	if bat != nil {
		for insertRowIdx, pkColIdx := range pkPosInValues {
			if pkValueExprs[pkColIdx] == nil {
				constExpr := rule.GetConstantValue(bat.Vecs[insertRowIdx], true)
				typ := makePlan2Type(bat.Vecs[insertRowIdx].GetType())

				expr, err := bindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{{
					Typ: typ,
					Expr: &plan.Expr_Col{
						Col: &ColRef{
							ColPos: int32(pkColIdx),
						},
					},
				}, &plan.Expr{
					Typ: typ,
					Expr: &plan.Expr_C{
						C: constExpr,
					},
				}})
				if err != nil {
					return nil
				}

				pkValueExprs[pkColIdx] = expr
			}
		}
	}
	return pkValueExprs

	// if len(pkValueExprs) == 1 {
	// 	return pkValueExprs[0]
	// } else {
	// 	pkValueExpr, err := bindFuncExprImplByPlanExpr(builder.GetContext(), "serial", pkValueExprs)
	// 	if err != nil {
	// 		return nil
	// 	}
	// 	return pkValueExpr
	// }
}
