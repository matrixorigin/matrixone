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
	"fmt"

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
	colName2Idx := make(map[string]int32)
	updateColName2Idx := make(map[string]int32)

	for i, alias := range dmlCtx.aliases {
		if len(dmlCtx.updateCol2Expr[i]) == 0 {
			continue
		}

		tableDef := dmlCtx.tableDefs[i]
		useColInPartExpr := make(map[string]bool)
		if tableDef.Partition != nil {
			getPartColsFromExpr(tableDef.Partition.PartitionExpression, useColInPartExpr)
		}

		// append  table.* to project list
		for _, col := range tableDef.Cols {
			colName2Idx[alias+"."+col.Name] = int32(len(selectList))
			e := tree.NewUnresolvedName(tree.NewCStr(alias, bindCtx.lower), tree.NewCStr(col.Name, 1))
			selectList = append(selectList, tree.SelectExpr{
				Expr: e,
			})
		}

		// TODO: support update primary key or unique key or secondary key or master index or ivfflat index
		var pkAndUkCols = make(map[string]bool)
		if tableDef.Pkey != nil {
			for _, colName := range tableDef.Pkey.Names {
				pkAndUkCols[colName] = true
			}
		}
		for _, idxDef := range tableDef.Indexes {
			if !idxDef.TableExist || !idxDef.Unique {
				if catalog.IsRegularIndexAlgo(idxDef.IndexAlgo) {
					continue
				}
			}

			for _, colName := range idxDef.Parts {
				realColName := catalog.ResolveAlias(colName)
				pkAndUkCols[realColName] = true
			}
		}

		for colName, updateExpr := range dmlCtx.updateCol2Expr[i] {
			if pkAndUkCols[colName] {
				return 0, moerr.NewUnsupportedDML(builder.compCtx.GetContext(), "update primary key or unique key or master index or ivfflat index")
			}

			if !dmlCtx.updatePartCol[i] {
				if _, ok := useColInPartExpr[colName]; ok {
					dmlCtx.updatePartCol[i] = true
				}
			}

			for _, colDef := range tableDef.Cols {
				if colDef.Name == colName {
					if colDef.Typ.Id == int32(types.T_enum) {
						if colDef.Typ.AutoIncr {
							return 0, moerr.NewUnsupportedDML(builder.compCtx.GetContext(), "auto_increment default value")
						}

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

					if colDef.Typ.AutoIncr {
						if constExpr, ok := updateExpr.(*tree.NumVal); ok {
							if constExpr.ValType == tree.P_null {
								return 0, moerr.NewConstraintViolation(builder.compCtx.GetContext(), fmt.Sprintf("Column '%s' cannot be null", colName))
							}
						}
					}
				}
			}

			updateColName2Idx[alias+"."+colName] = int32(len(selectList))
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

		for originPos, col := range tableDef.Cols {
			if colPos, ok := updateColName2Idx[alias+"."+col.Name]; ok {
				updateExpr := selectNode.ProjectList[colPos]
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
					selectNode.ProjectList[colPos], err = funcCastForEnumType(builder.GetContext(), updateExpr, col.Typ)
					if err != nil {
						return 0, err
					}
				} else {
					selectNode.ProjectList[colPos], err = forceCastExpr(builder.GetContext(), updateExpr, col.Typ)
					if err != nil {
						return 0, err
					}
				}
			} else {
				if col.OnUpdate != nil && col.OnUpdate.Expr != nil {
					//pos := colName2Idx[alias+"."+col.Name]
					//selectNode.ProjectList[pos] = col.OnUpdate.Expr
					//
					//if col.Typ.Id == int32(types.T_enum) {
					//	selectNode.ProjectList[pos], err = funcCastForEnumType(builder.GetContext(), selectNode.ProjectList[pos], col.Typ)
					//	if err != nil {
					//		return 0, err
					//	}
					//} else {
					//	selectNode.ProjectList[pos], err = forceCastExpr(builder.GetContext(), selectNode.ProjectList[pos], col.Typ)
					//	if err != nil {
					//		return 0, err
					//	}
					//}
					return 0, moerr.NewUnsupportedDML(builder.compCtx.GetContext(), "update column with on update")
				}
				if col.Typ.Id == int32(types.T_enum) {
					selectNode.ProjectList[originPos], err = funcCastForEnumType(builder.GetContext(), selectNode.ProjectList[originPos], col.Typ)
					if err != nil {
						return 0, err
					}
				}
			}
		}
	}

	selectNodeTag := selectNode.BindingTags[0]
	idxScanNodes := make([][]*plan.Node, len(dmlCtx.tableDefs))
	idxNeedUpdate := make([][]bool, len(dmlCtx.tableDefs))

	for i, tableDef := range dmlCtx.tableDefs {
		if len(dmlCtx.updateCol2Expr[i]) == 0 {
			continue
		}

		alias := dmlCtx.aliases[i]
		idxScanNodes[i] = make([]*plan.Node, len(tableDef.Indexes))
		idxNeedUpdate[i] = make([]bool, len(tableDef.Indexes))

		for j, idxDef := range tableDef.Indexes {
			if !idxDef.TableExist || idxDef.Unique {
				continue
			}

			for _, colName := range idxDef.Parts {
				realColName := catalog.ResolveAlias(colName)
				if _, ok := updateColName2Idx[alias+"."+realColName]; ok {
					idxNeedUpdate[i][j] = true
					break
				}
			}
			if !idxNeedUpdate[i][j] {
				continue
			}

			idxObjRef, idxTableDef := builder.compCtx.Resolve(dmlCtx.objRefs[i].SchemaName, idxDef.IndexTableName, bindCtx.snapshot)
			idxTag := builder.genNewTag()
			builder.addNameByColRef(idxTag, idxTableDef)

			idxScanNodes[i][j] = &plan.Node{
				NodeType:     plan.Node_TABLE_SCAN,
				TableDef:     idxTableDef,
				ObjRef:       idxObjRef,
				BindingTags:  []int32{idxTag},
				ScanSnapshot: bindCtx.snapshot,
			}
			idxTableNodeID := builder.appendNode(idxScanNodes[i][j], bindCtx)

			rightPkPos := idxTableDef.Name2ColIndex[catalog.IndexTableIndexColName]
			pkTyp := idxTableDef.Cols[rightPkPos].Typ

			rightExpr := &plan.Expr{
				Typ: pkTyp,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: idxTag,
						ColPos: rightPkPos,
					},
				},
			}

			args := make([]*plan.Expr, len(idxDef.Parts))

			var colPos int32
			var ok bool
			for k, colName := range idxDef.Parts {
				if colPos, ok = colName2Idx[alias+"."+catalog.ResolveAlias(colName)]; !ok {
					errMsg := fmt.Sprintf("bind update err, can not find colName = %s", colName)
					return 0, moerr.NewInternalError(builder.GetContext(), errMsg)
				}
				args[k] = &plan.Expr{
					Typ: selectNode.ProjectList[colPos].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: selectNodeTag,
							ColPos: colPos,
						},
					},
				}
			}

			leftExpr, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial_full", args)

			joinCond, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
				leftExpr,
				rightExpr,
			})

			lastNodeID = builder.appendNode(&plan.Node{
				NodeType: plan.Node_JOIN,
				Children: []int32{lastNodeID, idxTableNodeID},
				JoinType: plan.Node_INNER,
				OnList:   []*plan.Expr{joinCond},
			}, bindCtx)
		}
	}

	lockTargets := make([]*plan.LockTarget, 0)
	updateCtxList := make([]*plan.UpdateCtx, 0)

	finalProjTag := builder.genNewTag()
	finalColName2Idx := make(map[string]int32)
	var finalProjList []*plan.Expr

	for i, tableDef := range dmlCtx.tableDefs {
		if len(dmlCtx.updateCol2Expr[i]) == 0 {
			continue
		}

		alias := dmlCtx.aliases[i]
		insertCols := make([]plan.ColRef, len(tableDef.Cols)-1)

		for j, col := range tableDef.Cols {
			finalColIdx := len(finalProjList)
			if col.Name == tableDef.Pkey.PkeyColName {
				lockTarget := &plan.LockTarget{
					TableId:            tableDef.TblId,
					PrimaryColIdxInBat: int32(finalColIdx),
					PrimaryColRelPos:   finalProjTag,
					PrimaryColTyp:      col.Typ,
				}
				lockTargets = append(lockTargets, lockTarget)

				if dmlCtx.updatePartCol[i] {
					// if update col which partition expr used,
					// need lock oldPk by old partition idx, lock new pk by new partition idx
					lockTarget := &plan.LockTarget{
						TableId:            tableDef.TblId,
						PrimaryColIdxInBat: int32(finalColIdx),
						PrimaryColRelPos:   finalProjTag,
						PrimaryColTyp:      col.Typ,
					}
					lockTargets = append(lockTargets, lockTarget)
				}
			}

			if col.Name != catalog.Row_ID {
				insertCols[j].RelPos = finalProjTag
				insertCols[j].ColPos = int32(finalColIdx)
			}

			colIdx := colName2Idx[alias+"."+col.Name]
			if updateIdx, ok := updateColName2Idx[alias+"."+col.Name]; ok {
				colIdx = updateIdx
			}

			finalColName2Idx[alias+"."+col.Name] = int32(finalColIdx)
			finalProjList = append(finalProjList, &plan.Expr{
				Typ: selectNode.ProjectList[colIdx].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: selectNodeTag,
						ColPos: int32(colIdx),
					},
				},
			})
		}

		updateCtxList = append(updateCtxList, &plan.UpdateCtx{
			ObjRef:          dmlCtx.objRefs[i],
			TableDef:        tableDef,
			InsertCols:      insertCols,
			OldPartitionIdx: -1,
			NewPartitionIdx: -1,
			DeleteCols: []plan.ColRef{
				{
					RelPos: finalProjTag,
					ColPos: finalColName2Idx[alias+"."+catalog.Row_ID],
				},
				{
					RelPos: finalProjTag,
					ColPos: finalColName2Idx[alias+"."+tableDef.Pkey.PkeyColName],
				},
			},
		})

		for j, idxNode := range idxScanNodes[i] {
			if !idxNeedUpdate[i][j] {
				continue
			}

			insertCols := make([]plan.ColRef, 2)
			deleteCols := make([]plan.ColRef, 2)

			idxNodeTag := idxNode.BindingTags[0]

			oldIdx := len(finalProjList)
			rowIDIdx := idxNode.TableDef.Name2ColIndex[catalog.Row_ID]
			finalProjList = append(finalProjList, &plan.Expr{
				Typ: idxNode.TableDef.Cols[rowIDIdx].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: idxNodeTag,
						ColPos: rowIDIdx,
					},
				},
			})
			deleteCols[0].RelPos = finalProjTag
			deleteCols[0].ColPos = int32(oldIdx)

			oldIdx = len(finalProjList)
			idxColIdx := idxNode.TableDef.Name2ColIndex[catalog.IndexTableIndexColName]
			finalProjList = append(finalProjList, &plan.Expr{
				Typ: idxNode.TableDef.Cols[idxColIdx].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: idxNodeTag,
						ColPos: idxColIdx,
					},
				},
			})
			deleteCols[1].RelPos = finalProjTag
			deleteCols[1].ColPos = int32(oldIdx)

			oldIdx = len(finalProjList)
			idxDef := tableDef.Indexes[j]
			args := make([]*plan.Expr, len(idxDef.Parts))

			for k, colName := range idxDef.Parts {
				realColName := catalog.ResolveAlias(colName)
				colPos := int32(colName2Idx[alias+"."+realColName])
				if updateIdx, ok := updateColName2Idx[alias+"."+realColName]; ok {
					colPos = int32(updateIdx)
				}
				args[k] = &plan.Expr{
					Typ: selectNode.ProjectList[colPos].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: selectNodeTag,
							ColPos: colPos,
						},
					},
				}
			}

			newIdxExpr, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial_full", args)
			finalProjList = append(finalProjList, newIdxExpr)
			insertCols[0].RelPos = finalProjTag
			insertCols[0].ColPos = int32(oldIdx)

			insertCols[1].RelPos = finalProjTag
			insertCols[1].ColPos = finalColName2Idx[alias+"."+tableDef.Pkey.PkeyColName]

			updateCtxList = append(updateCtxList, &plan.UpdateCtx{
				ObjRef:          idxNode.ObjRef,
				TableDef:        idxNode.TableDef,
				InsertCols:      insertCols,
				DeleteCols:      deleteCols,
				OldPartitionIdx: -1,
				NewPartitionIdx: -1,
			})
		}
	}

	dmlNode := &plan.Node{
		NodeType:      plan.Node_MULTI_UPDATE,
		BindingTags:   []int32{builder.genNewTag()},
		UpdateCtxList: updateCtxList,
	}

	if dmlCtx.tableDefs[0].Partition != nil {
		partitionTableIDs, partitionTableNames := getPartitionInfos(builder.compCtx, dmlCtx.objRefs[0], dmlCtx.tableDefs[0])
		updateCtxList[0].PartitionTableIds = partitionTableIDs
		updateCtxList[0].PartitionTableNames = partitionTableNames
		var oldPartExpr *Expr
		oldPartExpr, err = getRemapParitionExpr(dmlCtx.tableDefs[0], selectNodeTag, colName2Idx, true)
		if err != nil {
			return -1, err
		}
		updateCtxList[0].OldPartitionIdx = int32(len(finalProjList))
		finalProjList = append(finalProjList, oldPartExpr)
		lockTargets[0].IsPartitionTable = true
		lockTargets[0].PartitionTableIds = partitionTableIDs
		lockTargets[0].FilterColIdxInBat = updateCtxList[0].OldPartitionIdx
		lockTargets[0].FilterColRelPos = finalProjTag

		if dmlCtx.updatePartCol[0] {
			// if update col which partition expr used,
			// need lock oldPk by old partition idx, lock new pk by new partition idx
			var newPartExpr *Expr
			partName2Idx := make(map[string]int32)
			for k, v := range colName2Idx {
				partName2Idx[k] = v
			}
			for k, v := range updateColName2Idx {
				partName2Idx[k] = v
			}
			newPartExpr, err = getRemapParitionExpr(dmlCtx.tableDefs[0], selectNodeTag, partName2Idx, true)
			if err != nil {
				return -1, err
			}
			updateCtxList[0].NewPartitionIdx = int32(len(finalProjList))
			finalProjList = append(finalProjList, newPartExpr)

			lockTargets[1].IsPartitionTable = true
			lockTargets[1].PartitionTableIds = partitionTableIDs
			lockTargets[1].FilterColIdxInBat = updateCtxList[0].NewPartitionIdx
			lockTargets[1].FilterColRelPos = finalProjTag
		} else {
			// if do not update col which partition expr used,
			// just use old partition idx.
			// @todo if update pk, we need another lockTarget too(not support now)
			updateCtxList[0].NewPartitionIdx = updateCtxList[0].OldPartitionIdx
		}

		dmlNode.BindingTags = append(dmlNode.BindingTags, finalProjTag)

	}

	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		ProjectList: finalProjList,
		Children:    []int32{lastNodeID},
		BindingTags: []int32{finalProjTag},
	}, bindCtx)

	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_LOCK_OP,
		Children:    []int32{lastNodeID},
		TableDef:    dmlCtx.tableDefs[0],
		BindingTags: []int32{builder.genNewTag()},
		LockTargets: lockTargets,
	}, bindCtx)
	reCheckifNeedLockWholeTable(builder)

	dmlNode.Children = append(dmlNode.Children, lastNodeID)
	lastNodeID = builder.appendNode(dmlNode, bindCtx)

	return lastNodeID, err
}
