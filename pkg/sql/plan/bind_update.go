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
	"strings"

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
	oldColName2Idx := make(map[string]int32)
	newColName2Idx := make(map[string]int32)
	updateAutoIncrCols := make([]bool, len(dmlCtx.aliases))
	colOffsets := make([]int32, len(dmlCtx.aliases))

	for i, alias := range dmlCtx.aliases {
		if len(dmlCtx.updateCol2Expr[i]) == 0 {
			continue
		}

		tableDef := dmlCtx.tableDefs[i]
		colOffsets[i] = int32(len(selectList))
		useColInPartExpr := make(map[string]bool)

		// append  table.* to project list
		for _, col := range tableDef.Cols {
			oldColName2Idx[alias+"."+col.Name] = int32(len(selectList))
			e := tree.NewUnresolvedName(tree.NewCStr(alias, bindCtx.lower), tree.NewCStr(col.Name, 1))
			selectList = append(selectList, tree.SelectExpr{
				Expr: e,
			})
		}

		validIndexes, hasIrregularIndex := getValidIndexes(tableDef)
		if hasIrregularIndex {
			return 0, moerr.NewUnsupportedDML(builder.GetContext(), "update vector/full-text index")
		}
		tableDef.Indexes = validIndexes

		var pkAndUkCols = make(map[string]bool)

		if tableDef.Name == catalog.MO_PUBS || tableDef.Name == catalog.MO_SUBS {
			for _, colName := range tableDef.Pkey.Names {
				pkAndUkCols[colName] = true
			}
		}

		for _, idxDef := range tableDef.Indexes {
			if !idxDef.Unique {
				continue
			}

			if tableDef.Name == catalog.MO_PUBS || tableDef.Name == catalog.MO_SUBS {
				for _, colName := range idxDef.Parts {
					pkAndUkCols[catalog.ResolveAlias(colName)] = true
				}
			}
		}

		for colName, updateExpr := range dmlCtx.updateCol2Expr[i] {
			if pkAndUkCols[colName] {
				return 0, moerr.NewUnsupportedDML(builder.compCtx.GetContext(), "update pk/uk on pub/sub table")
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

						updateAutoIncrCols[i] = true
					}
				}
			}

			oldPos := oldColName2Idx[alias+"."+colName]
			newColName2Idx[alias+"."+colName] = oldPos
			oldColName2Idx[alias+"."+colName] = int32(len(selectList))
			selectList = append(selectList, selectList[oldPos])
			selectList[oldPos] = tree.SelectExpr{Expr: updateExpr}
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
	selectNodeTag := selectNode.BindingTags[0]

	for i, alias := range dmlCtx.aliases {
		if len(dmlCtx.updateCol2Expr[i]) == 0 {
			continue
		}

		tableDef := dmlCtx.tableDefs[i]

		for originPos, col := range tableDef.Cols {
			if colPos, ok := newColName2Idx[alias+"."+col.Name]; ok {
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
					newDefExpr := DeepCopyExpr(col.OnUpdate.Expr)
					err = replaceFuncId(builder.GetContext(), newDefExpr)

					oldPos := oldColName2Idx[alias+"."+col.Name]
					newColName2Idx[alias+"."+col.Name] = oldPos
					oldColName2Idx[alias+"."+col.Name] = int32(len(selectList))
					selectNode.ProjectList = append(selectNode.ProjectList, selectNode.ProjectList[oldPos])
					selectNode.ProjectList[oldPos] = newDefExpr
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

	for i, tableDef := range dmlCtx.tableDefs {
		if updateAutoIncrCols[i] {
			lastNodeID = builder.appendNode(&plan.Node{
				NodeType: plan.Node_PRE_INSERT,
				Children: []int32{lastNodeID},
				PreInsertCtx: &plan.PreInsertCtx{
					Ref:         dmlCtx.objRefs[i],
					TableDef:    tableDef,
					HasAutoCol:  true,
					ColOffset:   colOffsets[i],
					IsNewUpdate: true,
				},
			}, bindCtx)
		}
	}

	idxScanNodes := make([][]*plan.Node, len(dmlCtx.tableDefs))
	pkNeedUpdate := make([]bool, len(dmlCtx.tableDefs))
	idxNeedUpdate := make([][]bool, len(dmlCtx.tableDefs))
	updatePkOrUk := false

	for i, tableDef := range dmlCtx.tableDefs {
		if len(dmlCtx.updateCol2Expr[i]) == 0 {
			continue
		}

		alias := dmlCtx.aliases[i]

		for _, colName := range tableDef.Pkey.Names {
			if _, ok := newColName2Idx[alias+"."+colName]; ok {
				pkNeedUpdate[i] = true
				updatePkOrUk = true
				break
			}
		}

		idxNeedUpdate[i] = make([]bool, len(tableDef.Indexes))

		for j, idxDef := range tableDef.Indexes {
			for _, colName := range idxDef.Parts {
				if _, ok := newColName2Idx[alias+"."+catalog.ResolveAlias(colName)]; ok {
					idxNeedUpdate[i][j] = true
					updatePkOrUk = true
					break
				}
			}
		}
	}

	if updatePkOrUk {
		newProjTag := builder.genNewBindTag()
		newProjList := make([]*plan.Expr, len(selectNode.ProjectList))
		for i := range selectNode.ProjectList {
			newProjList[i] = &plan.Expr{
				Typ: selectNode.ProjectList[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: selectNodeTag,
						ColPos: int32(i),
					},
				},
			}
		}

		newProjNode := &plan.Node{
			NodeType:    plan.Node_PROJECT,
			ProjectList: newProjList,
			Children:    []int32{lastNodeID},
			BindingTags: []int32{newProjTag},
		}
		lastNodeID = builder.appendNode(newProjNode, bindCtx)

		for i, tableDef := range dmlCtx.tableDefs {
			if len(dmlCtx.updateCol2Expr[i]) == 0 {
				continue
			}

			alias := dmlCtx.aliases[i]

			if pkNeedUpdate[i] {
				if len(tableDef.Pkey.Names) > 1 {
					newColName2Idx[alias+"."+catalog.CPrimaryKeyColName] = int32(len(newProjNode.ProjectList))
					args := make([]*plan.Expr, len(tableDef.Pkey.Names))

					for j, colName := range tableDef.Pkey.Names {
						colPos := int32(oldColName2Idx[alias+"."+colName])
						if updateIdx, ok := newColName2Idx[alias+"."+colName]; ok {
							colPos = int32(updateIdx)
						}

						args[j] = &plan.Expr{
							Typ: selectNode.ProjectList[colPos].Typ,
							Expr: &plan.Expr_Col{
								Col: &plan.ColRef{
									RelPos: selectNodeTag,
									ColPos: colPos,
								},
							},
						}
					}

					newPkExpr, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", args)
					newProjNode.ProjectList = append(newProjNode.ProjectList, newPkExpr)
				}

				scanTag := builder.genNewBindTag()
				scanNodeID := builder.appendNode(&plan.Node{
					NodeType:     plan.Node_TABLE_SCAN,
					TableDef:     tableDef,
					ObjRef:       dmlCtx.objRefs[i],
					BindingTags:  []int32{scanTag},
					ScanSnapshot: bindCtx.snapshot,
				}, bindCtx)

				pkPos := tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]
				pkTyp := tableDef.Cols[pkPos].Typ
				leftExpr := &plan.Expr{
					Typ: pkTyp,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: scanTag,
							ColPos: pkPos,
						},
					},
				}

				rightExpr := &plan.Expr{
					Typ: pkTyp,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: newProjTag,
							ColPos: newColName2Idx[alias+"."+tableDef.Pkey.PkeyColName],
						},
					},
				}

				joinCond, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
					leftExpr,
					rightExpr,
				})

				var dedupColName string
				dedupColTypes := make([]plan.Type, len(tableDef.Pkey.Names))

				if len(tableDef.Pkey.Names) == 1 {
					dedupColName = tableDef.Pkey.Names[0]
				} else {
					dedupColName = "(" + strings.Join(tableDef.Pkey.Names, ",") + ")"
				}

				for j, part := range tableDef.Pkey.Names {
					dedupColTypes[j] = tableDef.Cols[tableDef.Name2ColIndex[part]].Typ
				}

				dedupJoinNode := &plan.Node{
					NodeType:          plan.Node_JOIN,
					Children:          []int32{scanNodeID, lastNodeID},
					JoinType:          plan.Node_DEDUP,
					OnList:            []*plan.Expr{joinCond},
					OnDuplicateAction: plan.Node_FAIL,
					DedupColName:      dedupColName,
					DedupColTypes:     dedupColTypes,
					DedupJoinCtx: &plan.DedupJoinCtx{
						OldColList: []plan.ColRef{
							{
								RelPos: newProjTag,
								ColPos: oldColName2Idx[alias+"."+tableDef.Pkey.PkeyColName],
							},
						},
					},
				}

				lastNodeID = builder.appendNode(dedupJoinNode, bindCtx)
			}

			idxScanNodes[i] = make([]*plan.Node, len(tableDef.Indexes))

			for j, idxDef := range tableDef.Indexes {
				if !idxDef.Unique || !idxNeedUpdate[i][j] {
					continue
				}

				idxObjRef, idxTableDef, err := builder.compCtx.ResolveIndexTableByRef(dmlCtx.objRefs[i], idxDef.IndexTableName, bindCtx.snapshot)
				if err != nil {
					return 0, err
				}
				idxTag := builder.genNewBindTag()
				builder.addNameByColRef(idxTag, idxTableDef)

				idxScanNode := &plan.Node{
					NodeType:     plan.Node_TABLE_SCAN,
					TableDef:     idxTableDef,
					ObjRef:       idxObjRef,
					BindingTags:  []int32{idxTag},
					ScanSnapshot: bindCtx.snapshot,
				}
				idxTableNodeID := builder.appendNode(idxScanNode, bindCtx)

				if len(idxDef.Parts) > 1 {
					oldColName2Idx[idxTableDef.Name+"."+catalog.IndexTableIndexColName] = int32(len(newProjNode.ProjectList))
					oldArgs := make([]*plan.Expr, len(idxDef.Parts))

					for j, colName := range idxDef.Parts {
						colPos := int32(oldColName2Idx[alias+"."+colName])
						oldArgs[j] = &plan.Expr{
							Typ: selectNode.ProjectList[colPos].Typ,
							Expr: &plan.Expr_Col{
								Col: &plan.ColRef{
									RelPos: selectNodeTag,
									ColPos: colPos,
								},
							},
						}
					}

					oldUkExpr, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", oldArgs)
					newProjNode.ProjectList = append(newProjNode.ProjectList, oldUkExpr)

					newColName2Idx[idxTableDef.Name+"."+catalog.IndexTableIndexColName] = int32(len(newProjNode.ProjectList))
					newArgs := make([]*plan.Expr, len(idxDef.Parts))

					for j, colName := range idxDef.Parts {
						colPos := oldColName2Idx[alias+"."+colName]
						if updateIdx, ok := newColName2Idx[alias+"."+colName]; ok {
							colPos = updateIdx
						}

						newArgs[j] = &plan.Expr{
							Typ: selectNode.ProjectList[colPos].Typ,
							Expr: &plan.Expr_Col{
								Col: &plan.ColRef{
									RelPos: selectNodeTag,
									ColPos: colPos,
								},
							},
						}
					}

					newUkExpr, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", newArgs)
					newProjNode.ProjectList = append(newProjNode.ProjectList, newUkExpr)
				} else {
					oldColName2Idx[idxTableDef.Name+"."+catalog.IndexTableIndexColName] = oldColName2Idx[alias+"."+idxDef.Parts[0]]
					newColName2Idx[idxTableDef.Name+"."+catalog.IndexTableIndexColName] = newColName2Idx[alias+"."+idxDef.Parts[0]]
				}

				rightPkPos := idxTableDef.Name2ColIndex[catalog.IndexTableIndexColName]
				pkTyp := idxTableDef.Cols[rightPkPos].Typ

				leftExpr := &plan.Expr{
					Typ: pkTyp,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: idxTag,
							ColPos: rightPkPos,
						},
					},
				}

				rightExpr := &plan.Expr{
					Typ: pkTyp,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: newProjTag,
							ColPos: newColName2Idx[idxTableDef.Name+"."+catalog.IndexTableIndexColName],
						},
					},
				}

				joinCond, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
					leftExpr,
					rightExpr,
				})

				var dedupColName string
				dedupColTypes := make([]plan.Type, len(idxDef.Parts))

				if len(idxDef.Parts) == 1 {
					dedupColName = catalog.ResolveAlias(idxDef.Parts[0])
				} else {
					dedupColName = "(" + strings.Join(idxDef.Parts, ",") + ")"
				}

				for j, part := range idxDef.Parts {
					dedupColTypes[j] = tableDef.Cols[tableDef.Name2ColIndex[catalog.ResolveAlias(part)]].Typ
				}

				dedupJoinNode := &plan.Node{
					NodeType:          plan.Node_JOIN,
					Children:          []int32{idxTableNodeID, lastNodeID},
					JoinType:          plan.Node_DEDUP,
					OnList:            []*plan.Expr{joinCond},
					OnDuplicateAction: plan.Node_FAIL,
					DedupColName:      dedupColName,
					DedupColTypes:     dedupColTypes,
					DedupJoinCtx: &plan.DedupJoinCtx{
						OldColList: []plan.ColRef{
							{
								RelPos: newProjTag,
								ColPos: oldColName2Idx[idxTableDef.Name+"."+catalog.IndexTableIndexColName],
							},
						},
					},
				}

				lastNodeID = builder.appendNode(dedupJoinNode, bindCtx)
			}
		}

		selectNodeTag = newProjTag
		selectNode = newProjNode
	}

	// join index tables to get old RowID
	for i, tableDef := range dmlCtx.tableDefs {
		if len(dmlCtx.updateCol2Expr[i]) == 0 {
			continue
		}

		alias := dmlCtx.aliases[i]

		for j, idxDef := range tableDef.Indexes {
			if !pkNeedUpdate[i] && !idxNeedUpdate[i][j] {
				continue
			}

			idxObjRef, idxTableDef, err := builder.compCtx.ResolveIndexTableByRef(dmlCtx.objRefs[i], idxDef.IndexTableName, bindCtx.snapshot)
			if err != nil {
				return 0, err
			}
			idxTag := builder.genNewBindTag()
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
				if colPos, ok = oldColName2Idx[alias+"."+catalog.ResolveAlias(colName)]; !ok {
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

			leftExpr := args[0]
			if len(idxDef.Parts) > 1 {
				funcName := "serial"
				if !idxDef.Unique {
					funcName = "serial_full"
				}
				leftExpr, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), funcName, args)
			}

			joinCond, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
				leftExpr,
				rightExpr,
			})

			joinType := plan.Node_LEFT
			if !idxDef.Unique {
				joinType = plan.Node_INNER
			}
			lastNodeID = builder.appendNode(&plan.Node{
				NodeType: plan.Node_JOIN,
				Children: []int32{lastNodeID, idxTableNodeID},
				JoinType: joinType,
				OnList:   []*plan.Expr{joinCond},
			}, bindCtx)
		}
	}

	lockTargets := make([]*plan.LockTarget, 0)
	updateCtxList := make([]*plan.UpdateCtx, 0)

	finalProjTag := builder.genNewBindTag()
	finalColName2Idx := make(map[string]int32)
	var finalProjList []*plan.Expr

	finalProjNode := &plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{lastNodeID},
		BindingTags: []int32{finalProjTag},
	}
	lastNodeID = builder.appendNode(finalProjNode, bindCtx)

	for i, tableDef := range dmlCtx.tableDefs {
		if len(dmlCtx.updateCol2Expr[i]) == 0 {
			continue
		}

		alias := dmlCtx.aliases[i]
		insertCols := make([]plan.ColRef, len(tableDef.Cols)-1)

		for j, col := range tableDef.Cols {
			finalColIdx := len(finalProjList)

			if col.Name != catalog.Row_ID {
				insertCols[j].RelPos = finalProjTag
				insertCols[j].ColPos = int32(finalColIdx)
			}

			colIdx := oldColName2Idx[alias+"."+col.Name]
			if updateIdx, ok := newColName2Idx[alias+"."+col.Name]; ok {
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

		oldPkPos := finalColName2Idx[alias+"."+tableDef.Pkey.PkeyColName]
		newPkPos := oldPkPos
		if updateIdx, ok := newColName2Idx[alias+"."+tableDef.Pkey.PkeyColName]; ok {
			oldPkPos = int32(len(finalProjList))
			finalProjList = append(finalProjList, &plan.Expr{
				Typ: selectNode.ProjectList[updateIdx].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: selectNodeTag,
						ColPos: oldColName2Idx[alias+"."+tableDef.Pkey.PkeyColName],
					},
				},
			})
		}

		updateCtxList = append(updateCtxList, &plan.UpdateCtx{
			ObjRef:     dmlCtx.objRefs[i],
			TableDef:   tableDef,
			InsertCols: insertCols,
			DeleteCols: []plan.ColRef{
				{
					RelPos: finalProjTag,
					ColPos: finalColName2Idx[alias+"."+catalog.Row_ID],
				},
				{
					RelPos: finalProjTag,
					ColPos: oldPkPos,
				},
			},
		})

		lockTargets = append(lockTargets, &plan.LockTarget{
			TableId:            tableDef.TblId,
			ObjRef:             dmlCtx.objRefs[i],
			PrimaryColIdxInBat: int32(newPkPos),
			PrimaryColRelPos:   finalProjTag,
			PrimaryColTyp:      finalProjList[newPkPos].Typ,
		})
		if newPkPos != oldPkPos {
			lockTargets = append(lockTargets, &plan.LockTarget{
				TableId:            tableDef.TblId,
				ObjRef:             dmlCtx.objRefs[i],
				PrimaryColIdxInBat: int32(oldPkPos),
				PrimaryColRelPos:   finalProjTag,
				PrimaryColTyp:      finalProjList[oldPkPos].Typ,
			})
		}

		for j, idxNode := range idxScanNodes[i] {
			if !pkNeedUpdate[i] && !idxNeedUpdate[i][j] {
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

			newIdx := oldIdx

			idxDef := tableDef.Indexes[j]
			if idxDef.Unique {
				if idxNeedUpdate[i][j] {
					newPos := newColName2Idx[idxNode.TableDef.Name+"."+catalog.IndexTableIndexColName]
					newIdxExpr := &plan.Expr{
						Typ: selectNode.ProjectList[newPos].Typ,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: selectNodeTag,
								ColPos: newPos,
							},
						},
					}

					newIdx = len(finalProjList)
					finalProjList = append(finalProjList, newIdxExpr)
				}
			} else {
				args := make([]*plan.Expr, len(idxDef.Parts))

				for k, colName := range idxDef.Parts {
					realColName := catalog.ResolveAlias(colName)
					colPos := int32(oldColName2Idx[alias+"."+realColName])
					if updateIdx, ok := newColName2Idx[alias+"."+realColName]; ok {
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

				var newIdxExpr *plan.Expr
				if len(idxDef.Parts) == 0 {
					newIdxExpr = args[0]
				} else {
					funcName := "serial"
					if !idxDef.Unique {
						funcName = "serial_full"
					}
					newIdxExpr, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), funcName, args)
				}

				newIdx = len(finalProjList)
				finalProjList = append(finalProjList, newIdxExpr)
			}

			insertCols[0].RelPos = finalProjTag
			insertCols[0].ColPos = int32(newIdx)

			insertCols[1].RelPos = finalProjTag
			insertCols[1].ColPos = finalColName2Idx[alias+"."+tableDef.Pkey.PkeyColName]

			updateCtxList = append(updateCtxList, &plan.UpdateCtx{
				ObjRef:     idxNode.ObjRef,
				TableDef:   idxNode.TableDef,
				InsertCols: insertCols,
				DeleteCols: deleteCols,
			})

			if idxDef.Unique {
				lockTargets = append(lockTargets, &plan.LockTarget{
					TableId:            idxNode.TableDef.TblId,
					ObjRef:             idxNode.ObjRef,
					PrimaryColIdxInBat: int32(oldIdx),
					PrimaryColRelPos:   finalProjTag,
					PrimaryColTyp:      finalProjList[oldIdx].Typ,
				})
				if idxNeedUpdate[i][j] {
					lockTargets = append(lockTargets, &plan.LockTarget{
						TableId:            idxNode.TableDef.TblId,
						ObjRef:             idxNode.ObjRef,
						PrimaryColIdxInBat: int32(newIdx),
						PrimaryColRelPos:   finalProjTag,
						PrimaryColTyp:      finalProjList[newIdx].Typ,
					})
				}
			}
		}
	}

	finalProjNode.ProjectList = finalProjList

	dmlNode := &plan.Node{
		NodeType:      plan.Node_MULTI_UPDATE,
		BindingTags:   []int32{builder.genNewBindTag()},
		UpdateCtxList: updateCtxList,
	}

	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_LOCK_OP,
		Children:    []int32{lastNodeID},
		TableDef:    dmlCtx.tableDefs[0],
		BindingTags: []int32{builder.genNewBindTag()},
		LockTargets: lockTargets,
	}, bindCtx)
	reCheckifNeedLockWholeTable(builder)

	dmlNode.Children = append(dmlNode.Children, lastNodeID)
	lastNodeID = builder.appendNode(dmlNode, bindCtx)

	return lastNodeID, err
}
