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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

func buildInsert(stmt *tree.Insert, ctx CompilerContext, isReplace bool) (p *Plan, err error) {
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

	builder := NewQueryBuilder(plan.Query_SELECT, ctx)
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
	var pkFilterExpr *Expr
	if !checkInsertPkDup {
		pkFilterExpr = getPkValueExpr(builder, tableDef, pkPosInValues)
	}
	builder.qry.Steps = append(builder.qry.Steps[:sourceStep], builder.qry.Steps[sourceStep+1:]...)

	objRef := tblInfo.objRef[0]
	if len(rewriteInfo.onDuplicateIdx) > 0 {
		// append on duplicate key node
		tableDef = DeepCopyTableDef(tableDef)
		if tableDef.Pkey != nil && tableDef.Pkey.PkeyColName == catalog.CPrimaryKeyColName {
			tableDef.Cols = append(tableDef.Cols, MakeHiddenColDefByName(catalog.CPrimaryKeyColName))
		}
		if tableDef.ClusterBy != nil && util.JudgeIsCompositeClusterByColumn(tableDef.ClusterBy.Name) {
			tableDef.Cols = append(tableDef.Cols, MakeHiddenColDefByName(tableDef.ClusterBy.Name))
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
		err = buildInsertPlans(ctx, builder, bindCtx, objRef, tableDef, rewriteInfo.rootId, checkInsertPkDup, pkFilterExpr)
		if err != nil {
			return nil, err
		}
		query.StmtType = plan.Query_INSERT
	}
	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, err
}

func getPkValueExpr(builder *QueryBuilder, tableDef *TableDef, pkPosInValues int) *Expr {
	pkPos, pkTyp := getPkPos(tableDef, true)
	if pkPos == -1 || pkTyp.AutoIncr {
		return nil
	}
	colTyp := makeTypeByPlan2Type(pkTyp)
	targetTyp := &plan.Expr{
		Typ: pkTyp,
		Expr: &plan.Expr_T{
			T: &plan.TargetType{
				Typ: pkTyp,
			},
		},
	}

	for _, node := range builder.qry.Nodes {
		if node.NodeType != plan.Node_VALUE_SCAN {
			continue
		}
		if len(node.RowsetData.Cols[0].Data) != 1 {
			continue
		}

		oldExpr := node.RowsetData.Cols[pkPosInValues].Data[0]
		if !rule.IsConstant(oldExpr) {
			return nil
		}
		pkValueExpr, err := forceCastExpr2(builder.GetContext(), DeepCopyExpr(oldExpr), colTyp, targetTyp)
		if err != nil {
			return nil
		}
		return pkValueExpr
	}
	return nil
}
