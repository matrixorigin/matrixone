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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildTableUpdate(stmt *tree.Update, ctx CompilerContext) (p *Plan, err error) {
	tblInfo, err := getUpdateTableInfo(ctx, stmt)
	if err != nil {
		return nil, err
	}
	// new logic
	builder := NewQueryBuilder(plan.Query_SELECT, ctx)
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
	builder.qry.Steps = append(builder.qry.Steps[:sourceStep], builder.qry.Steps[sourceStep+1:]...)

	// append sink node
	lastNodeId = appendSinkNode(builder, queryBindCtx, lastNodeId)
	sourceStep = builder.appendStep(lastNodeId)

	beginIdx := 0
	for i, tableDef := range tblInfo.tableDefs {
		upPlanCtx := updatePlanCtxs[i]
		upPlanCtx.beginIdx = beginIdx
		upPlanCtx.sourceStep = sourceStep

		updateBindCtx := NewBindContext(builder, nil)
		beginIdx = beginIdx + upPlanCtx.updateColLength + len(tableDef.Cols)
		err = buildUpdatePlans(ctx, builder, updateBindCtx, upPlanCtx)
		if err != nil {
			return nil, err
		}
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
				if posExpr.Typ == nil { // set col = default
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
		for colName, updateKey := range updateKeys {
			selectList = append(selectList, tree.SelectExpr{
				Expr: updateKey,
			})
			updateColPosMap[colName] = offset
			offset++
		}
		// append  table.* to project list
		upPlanCtx := &dmlPlanCtx{
			objRef:          tableInfo.objRef[i],
			tableDef:        tableDef,
			updateColLength: len(updateKeys),
			isMulti:         tableInfo.isMulti,
			rowIdPos:        rowIdPos,
			updateColPosMap: updateColPosMap,
		}
		for idx, col := range tableDef.Cols {
			// row_id、compPrimaryKey、clusterByKey will not inserted from old data
			if col.Hidden {
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
