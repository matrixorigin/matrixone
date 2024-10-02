// Copyright 2021 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func (builder *QueryBuilder) bindUpdate(stmt *tree.Update, bindCtx *BindContext) (int32, error) {
	dmlCtx := NewDMLContext()
	err := dmlCtx.ResolveUpdateTables(builder.compCtx, stmt)
	if err != nil {
		return 0, err
	}

	var selectList []tree.SelectExpr
	updateColPosMap := make(map[string]int)
	tableOffsetList := make([]int, len(dmlCtx.tableDefs))

	for i, alias := range dmlCtx.aliases {
		tableOffsetList[i] = len(selectList)
		if len(dmlCtx.updateCol2Expr[i]) == 0 {
			continue
		}

		tableDef := dmlCtx.tableDefs[i]

		// append  table.* to project list
		for _, col := range tableDef.Cols {
			e := tree.NewUnresolvedName(tree.NewCStr(alias, bindCtx.lower), tree.NewCStr(col.Name, 1))
			selectList = append(selectList, tree.SelectExpr{
				Expr: e,
			})
		}

		// TODO: support update primary key or unique key or secondary key
		var pkAndIndexCols = make(map[string]bool)
		if tableDef.Pkey != nil {
			for _, colName := range tableDef.Pkey.Names {
				pkAndIndexCols[colName] = true
			}
		}
		for _, idxDef := range tableDef.Indexes {
			for _, colName := range idxDef.Parts {
				pkAndIndexCols[colName] = true
			}
		}

		for colName, updateExpr := range dmlCtx.updateCol2Expr[i] {
			if pkAndIndexCols[colName] {
				return 0, moerr.NewUnsupportedDML(builder.compCtx.GetContext(), "update primary key or unique key")
			}

			for _, colDef := range tableDef.Cols {
				if colDef.Name == colName && colDef.Typ.Id == int32(types.T_enum) {
					binder := NewDefaultBinder(builder.GetContext(), nil, nil, colDef.Typ, nil)
					updateKeyExpr, err := binder.BindExpr(updateExpr, 0, false)
					if err != nil {
						return 0, err
					}
					exprs := []tree.Expr{
						tree.NewNumVal(colDef.Typ.Enumvalues, colDef.Typ.Enumvalues, false, tree.P_char),
						updateExpr,
					}
					if updateKeyExpr.Typ.Id >= 20 && updateKeyExpr.Typ.Id <= 29 {
						updateExpr = &tree.FuncExpr{
							Func:  tree.FuncName2ResolvableFunctionReference(tree.NewUnresolvedColName(moEnumCastIndexValueToIndexFun)),
							Type:  tree.FUNC_TYPE_DEFAULT,
							Exprs: exprs,
						}
					} else {
						updateExpr = &tree.FuncExpr{
							Func:  tree.FuncName2ResolvableFunctionReference(tree.NewUnresolvedColName(moEnumCastValueToIndexFun)),
							Type:  tree.FUNC_TYPE_DEFAULT,
							Exprs: exprs,
						}
					}
				}
			}

			updateColPosMap[alias+"."+colName] = len(selectList)
			selectList = append(selectList, tree.SelectExpr{
				Expr: updateExpr,
			})
		}
	}

	selectAst := &tree.Select{
		Select: &tree.SelectClause{
			Exprs: selectList,
			From: &tree.From{
				Tables: stmt.Tables,
			},
			Where: stmt.Where,
		},
		OrderBy: stmt.OrderBy,
		Limit:   stmt.Limit,
		With:    stmt.With,
	}

	lastNodeID, err := builder.bindSelect(selectAst, bindCtx, false)
	if err != nil {
		return 0, err
	}

	selectNode := builder.qry.Nodes[lastNodeID]

	for i, alias := range dmlCtx.aliases {
		if len(dmlCtx.updateCol2Expr[i]) == 0 {
			continue
		}

		tableDef := dmlCtx.tableDefs[i]
		tableIdx := tableOffsetList[i]

		for colIdx, col := range tableDef.Cols {
			if offset, ok := updateColPosMap[alias+"."+col.Name]; ok {
				updateExpr := selectNode.ProjectList[offset]
				if isDefaultValExpr(updateExpr) { // set col = default
					updateExpr, err = getDefaultExpr(builder.GetContext(), col)
					if err != nil {
						return 0, err
					}
				}
				err = checkNotNull(builder.GetContext(), updateExpr, tableDef, col)
				if err != nil {
					return 0, err
				}
				if col != nil && col.Typ.Id == int32(types.T_enum) {
					selectNode.ProjectList[offset], err = funcCastForEnumType(builder.GetContext(), updateExpr, col.Typ)
					if err != nil {
						return 0, err
					}
				} else {
					selectNode.ProjectList[offset], err = forceCastExpr(builder.GetContext(), updateExpr, col.Typ)
					if err != nil {
						return 0, err
					}
				}
			} else {
				pos := tableIdx + colIdx
				if col.OnUpdate != nil && col.OnUpdate.Expr != nil {
					selectNode.ProjectList[pos] = col.OnUpdate.Expr

					if col.Typ.Id == int32(types.T_enum) {
						selectNode.ProjectList[pos], err = funcCastForEnumType(builder.GetContext(), selectNode.ProjectList[pos], col.Typ)
						if err != nil {
							return 0, err
						}
					} else {
						selectNode.ProjectList[pos], err = forceCastExpr(builder.GetContext(), selectNode.ProjectList[pos], col.Typ)
						if err != nil {
							return 0, err
						}
					}
				}
			}
		}
	}

	var (
		lockTargets   []*plan.LockTarget
		updateCtxList []*plan.UpdateCtx
	)

	selectNodeTag := selectNode.BindingTags[0]

	for i, tableDef := range dmlCtx.tableDefs {
		if len(dmlCtx.updateCol2Expr[i]) == 0 {
			continue
		}

		insertCols := make([]plan.ColRef, len(tableDef.Cols)-1)
		tableOffset := tableOffsetList[i]

		for k, col := range tableDef.Cols {
			if col.Name == catalog.Row_ID {
				continue
			}
			if col.Name == tableDef.Pkey.PkeyColName && tableDef.Pkey.PkeyColName != catalog.FakePrimaryKeyColName {
				lockTarget := &plan.LockTarget{
					TableId:            tableDef.TblId,
					PrimaryColIdxInBat: int32(tableOffsetList[i] + k),
					PrimaryColTyp:      col.Typ,
				}
				lockTargets = append(lockTargets, lockTarget)
			}

			insertCols[k].RelPos = selectNodeTag

			if offset, ok := updateColPosMap[dmlCtx.aliases[i]+"."+col.Name]; ok {
				insertCols[k].ColPos = int32(offset)
			} else {
				insertCols[k].ColPos = int32(tableOffset + k)
			}
		}

		updateCtxList = append(updateCtxList, &plan.UpdateCtx{
			ObjRef:     dmlCtx.objRefs[i],
			TableDef:   tableDef,
			InsertCols: insertCols,
			DeleteCols: []plan.ColRef{
				{
					RelPos: selectNodeTag,
					ColPos: int32(tableOffset) + tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName],
				},
				{
					RelPos: selectNodeTag,
					ColPos: int32(tableOffset) + tableDef.Name2ColIndex[catalog.Row_ID],
				},
			},
		})

	}

	if len(lockTargets) > 0 {
		lastNodeID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_LOCK_OP,
			Children:    []int32{lastNodeID},
			TableDef:    dmlCtx.tableDefs[0],
			BindingTags: []int32{builder.genNewTag(), selectNodeTag},
			LockTargets: lockTargets,
		}, bindCtx)

		reCheckifNeedLockWholeTable(builder)
	}

	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:      plan.Node_MULTI_UPDATE,
		Children:      []int32{lastNodeID},
		BindingTags:   []int32{builder.genNewTag()},
		UpdateCtxList: updateCtxList,
	}, bindCtx)

	return lastNodeID, err
}
