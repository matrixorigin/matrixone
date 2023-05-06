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

	bindCtx := NewBindContext(builder, nil)
	err = initInsertStmt(builder, bindCtx, stmt, rewriteInfo)
	if err != nil {
		return nil, err
	}
	lastNodeId := rewriteInfo.rootId
	sourceStep := builder.appendStep(lastNodeId)
	query, err := builder.createQuery()
	if err != nil {
		return nil, err
	}
	builder.qry.Steps = append(builder.qry.Steps[:sourceStep], builder.qry.Steps[sourceStep+1:]...)

	objRef := tblInfo.objRef[0]
	if len(rewriteInfo.onDuplicateIdx) > 0 {
		// append on duplicate key node
		var dupProjection []*Expr
		updateColLength := 0
		rowIdPos := -1
		for _, col := range tableDef.Cols {
			dupProjection = append(dupProjection, &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: -1,
						ColPos: int32(updateColLength),
						Name:   col.Name,
					},
				},
			})
			if col.Name == catalog.Row_ID {
				rowIdPos = updateColLength
			}
			updateColLength++
		}
		var insertColPos []int
		for _, col := range tableDef.Cols {
			if col.Name == catalog.Row_ID {
				continue
			}
			idx := len(dupProjection)
			insertColPos = append(insertColPos, idx)
			dupProjection = append(dupProjection, &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: -1,
						ColPos: int32(idx),
						Name:   col.Name,
					},
				},
			})
		}
		projectNode := &Node{
			NodeType:    plan.Node_ON_DUPLICATE_KEY,
			Children:    []int32{lastNodeId},
			ProjectList: dupProjection,
			OnDuplicateKey: &plan.OnDuplicateKeyCtx{
				TableDef:        tableDef,
				OnDuplicateIdx:  rewriteInfo.onDuplicateIdx,
				OnDuplicateExpr: rewriteInfo.onDuplicateExpr,
			},
		}
		lastNodeId = builder.appendNode(projectNode, bindCtx)

		// append sink node
		lastNodeId = appendSinkNode(builder, bindCtx, lastNodeId)
		sourceStep = builder.appendStep(lastNodeId)

		// append plans like update
		updateBindCtx := NewBindContext(builder, nil)
		upPlanCtx := &dmlPlanCtx{
			objRef:          objRef,
			tableDef:        tableDef,
			beginIdx:        0,
			sourceStep:      sourceStep,
			isMulti:         false,
			updateColLength: updateColLength,
			rowIdPos:        rowIdPos,
			insertColPos:    insertColPos,
		}
		err = buildUpdatePlans(ctx, builder, updateBindCtx, upPlanCtx)
		if err != nil {
			return nil, err
		}

	} else {
		err = buildInsertPlans(ctx, builder, bindCtx, objRef, tableDef, rewriteInfo.rootId)
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
