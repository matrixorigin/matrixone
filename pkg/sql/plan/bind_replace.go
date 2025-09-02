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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

func (builder *QueryBuilder) bindReplace(stmt *tree.Replace, bindCtx *BindContext) (int32, error) {
	dmlCtx := NewDMLContext()
	err := dmlCtx.ResolveTables(builder.compCtx, tree.TableExprs{stmt.Table}, nil, nil, true)
	if err != nil {
		return 0, err
	}

	lastNodeID, colName2Idx, skipUniqueIdx, err := builder.initInsertReplaceStmt(bindCtx, stmt.Rows, stmt.Columns, dmlCtx.objRefs[0], dmlCtx.tableDefs[0], true)
	if err != nil {
		return 0, err
	}

	return builder.appendDedupAndMultiUpdateNodesForBindReplace(bindCtx, dmlCtx, lastNodeID, colName2Idx, skipUniqueIdx)
}

func (builder *QueryBuilder) appendDedupAndMultiUpdateNodesForBindReplace(
	bindCtx *BindContext,
	dmlCtx *DMLContext,
	lastNodeID int32,
	colName2Idx map[string]int32,
	skipUniqueIdx []bool,
) (int32, error) {
	objRef := dmlCtx.objRefs[0]
	tableDef := dmlCtx.tableDefs[0]
	pkName := tableDef.Pkey.PkeyColName

	if pkName == catalog.FakePrimaryKeyColName {
		return 0, moerr.NewUnsupportedDML(builder.GetContext(), "fake primary key")
		//return builder.appendDedupAndMultiUpdateNodesForBindInsert(bindCtx, dmlCtx, lastNodeID, colName2Idx, skipUniqueIdx, nil)
	}

	selectNode := builder.qry.Nodes[lastNodeID]
	selectTag := selectNode.BindingTags[0]

	fullProjTag := builder.genNewTag()
	fullProjList := make([]*plan.Expr, 0, len(selectNode.ProjectList)+len(tableDef.Cols))
	for i, expr := range selectNode.ProjectList {
		fullProjList = append(fullProjList, &plan.Expr{
			Typ: expr.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: selectTag,
					ColPos: int32(i),
				},
			},
		})
	}

	idxObjRefs := make([]*plan.ObjectRef, len(tableDef.Indexes))
	idxTableDefs := make([]*plan.TableDef, len(tableDef.Indexes))

	oldColName2Idx := make(map[string][2]int32)

	// get old columns from existing main table
	{
		oldScanTag := builder.genNewTag()

		builder.addNameByColRef(oldScanTag, tableDef)

		oldScanNodeID := builder.appendNode(&plan.Node{
			NodeType:     plan.Node_TABLE_SCAN,
			TableDef:     tableDef,
			ObjRef:       objRef,
			BindingTags:  []int32{oldScanTag},
			ScanSnapshot: bindCtx.snapshot,
		}, bindCtx)

		for i, col := range tableDef.Cols {
			oldColName2Idx[tableDef.Name+"."+col.Name] = [2]int32{fullProjTag, int32(len(fullProjList))}
			fullProjList = append(fullProjList, &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: oldScanTag,
						ColPos: int32(i),
					},
				},
			})
		}

		var err error
		for i, idxDef := range tableDef.Indexes {
			idxObjRefs[i], idxTableDefs[i], err = builder.compCtx.ResolveIndexTableByRef(objRef, idxDef.IndexTableName, bindCtx.snapshot)
			if err != nil {
				return 0, err
			}

			if len(idxDef.Parts) == 1 {
				oldColName2Idx[idxDef.IndexTableName+"."+catalog.IndexTableIndexColName] = oldColName2Idx[tableDef.Name+"."+idxDef.Parts[0]]
			} else {
				args := make([]*plan.Expr, len(idxDef.Parts))
				for j, part := range idxDef.Parts {
					colIdx := tableDef.Name2ColIndex[catalog.ResolveAlias(part)]
					args[j] = &plan.Expr{
						Typ: tableDef.Cols[colIdx].Typ,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: oldScanTag,
								ColPos: colIdx,
							},
						},
					}
				}

				idxExpr := args[0]
				if len(idxDef.Parts) > 1 {
					funcName := "serial"
					if !idxDef.Unique {
						funcName = "serial_full"
					}
					idxExpr, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), funcName, args)
				}

				oldColName2Idx[idxDef.IndexTableName+"."+catalog.IndexTableIndexColName] = [2]int32{fullProjTag, int32(len(fullProjList))}
				fullProjList = append(fullProjList, idxExpr)
			}
		}

		pkPos := tableDef.Name2ColIndex[pkName]
		pkTyp := tableDef.Cols[pkPos].Typ
		leftExpr := &plan.Expr{
			Typ: pkTyp,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: selectTag,
					ColPos: colName2Idx[tableDef.Name+"."+pkName],
				},
			},
		}
		rightExpr := &plan.Expr{
			Typ: pkTyp,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: oldScanTag,
					ColPos: pkPos,
				},
			},
		}

		joinCond, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
			leftExpr,
			rightExpr,
		})

		lastNodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{lastNodeID, oldScanNodeID},
			JoinType: plan.Node_LEFT,
			OnList:   []*plan.Expr{joinCond},
		}, bindCtx)

		lastNodeID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_PROJECT,
			ProjectList: fullProjList,
			Children:    []int32{lastNodeID},
			BindingTags: []int32{fullProjTag},
		}, bindCtx)
	}

	// detect primary key confliction
	{
		scanTag := builder.genNewTag()

		// handle primary/unique key confliction
		builder.addNameByColRef(scanTag, tableDef)

		scanNodeID := builder.appendNode(&plan.Node{
			NodeType:     plan.Node_TABLE_SCAN,
			TableDef:     tableDef,
			ObjRef:       objRef,
			BindingTags:  []int32{scanTag},
			ScanSnapshot: bindCtx.snapshot,
		}, bindCtx)

		pkPos := tableDef.Name2ColIndex[pkName]
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
					RelPos: fullProjTag,
					ColPos: colName2Idx[tableDef.Name+"."+pkName],
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

		for i, part := range tableDef.Pkey.Names {
			dedupColTypes[i] = tableDef.Cols[tableDef.Name2ColIndex[part]].Typ
		}

		oldPkPos := oldColName2Idx[tableDef.Name+"."+pkName]

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
						RelPos: oldPkPos[0],
						ColPos: oldPkPos[1],
					},
				},
			},
		}

		lastNodeID = builder.appendNode(dedupJoinNode, bindCtx)
	}

	// detect unique key confliction
	for i, idxDef := range tableDef.Indexes {
		if !idxDef.Unique {
			continue
		}

		idxTag := builder.genNewTag()
		builder.addNameByColRef(idxTag, idxTableDefs[i])

		idxScanNode := &plan.Node{
			NodeType:     plan.Node_TABLE_SCAN,
			TableDef:     idxTableDefs[i],
			ObjRef:       idxObjRefs[i],
			BindingTags:  []int32{idxTag},
			ScanSnapshot: bindCtx.snapshot,
		}
		idxTableNodeID := builder.appendNode(idxScanNode, bindCtx)

		idxPkPos := idxTableDefs[i].Name2ColIndex[catalog.IndexTableIndexColName]
		pkTyp := idxTableDefs[i].Cols[idxPkPos].Typ

		leftExpr := &plan.Expr{
			Typ: pkTyp,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTag,
					ColPos: idxPkPos,
				},
			},
		}

		rightExpr := &plan.Expr{
			Typ: pkTyp,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: fullProjTag,
					ColPos: colName2Idx[idxTableDefs[i].Name+"."+catalog.IndexTableIndexColName],
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
			dedupColName = idxDef.Parts[0]
		} else {
			dedupColName = "("
			for j, part := range idxDef.Parts {
				if j == 0 {
					dedupColName += catalog.ResolveAlias(part)
				} else {
					dedupColName += "," + catalog.ResolveAlias(part)
				}
			}
			dedupColName += ")"
		}

		for j, part := range idxDef.Parts {
			dedupColTypes[j] = tableDef.Cols[tableDef.Name2ColIndex[catalog.ResolveAlias(part)]].Typ
		}

		oldPkPos := oldColName2Idx[idxTableDefs[i].Name+"."+catalog.IndexTableIndexColName]

		lastNodeID = builder.appendNode(&plan.Node{
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
						RelPos: oldPkPos[0],
						ColPos: oldPkPos[1],
					},
				},
			},
		}, bindCtx)
	}

	// get old RowID for index tables
	for i := range tableDef.Indexes {
		idxTag := builder.genNewTag()
		builder.addNameByColRef(idxTag, idxTableDefs[i])

		idxScanNode := &plan.Node{
			NodeType:     plan.Node_TABLE_SCAN,
			TableDef:     idxTableDefs[i],
			ObjRef:       idxObjRefs[i],
			BindingTags:  []int32{idxTag},
			ScanSnapshot: bindCtx.snapshot,
		}
		idxTableNodeID := builder.appendNode(idxScanNode, bindCtx)

		oldColName2Idx[idxTableDefs[i].Name+"."+catalog.Row_ID] = [2]int32{idxTag, idxTableDefs[i].Name2ColIndex[catalog.Row_ID]}

		idxPkPos := idxTableDefs[i].Name2ColIndex[catalog.IndexTableIndexColName]
		pkTyp := idxTableDefs[i].Cols[idxPkPos].Typ

		leftExpr := &plan.Expr{
			Typ: pkTyp,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTag,
					ColPos: idxPkPos,
				},
			},
		}

		oldPkPos := oldColName2Idx[idxTableDefs[i].Name+"."+catalog.IndexTableIndexColName]
		oldColName2Idx[idxTableDefs[i].Name+"."+catalog.IndexTableIndexColName] = [2]int32{idxTag, idxTableDefs[i].Name2ColIndex[catalog.IndexTableIndexColName]}

		rightExpr := &plan.Expr{
			Typ: pkTyp,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: oldPkPos[0],
					ColPos: oldPkPos[1],
				},
			},
		}

		joinCond, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
			leftExpr,
			rightExpr,
		})

		lastNodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{lastNodeID, idxTableNodeID},
			JoinType: plan.Node_LEFT,
			OnList:   []*plan.Expr{joinCond},
		}, bindCtx)
	}

	lockTargets := make([]*plan.LockTarget, 0)
	updateCtxList := make([]*plan.UpdateCtx, 0)

	finalProjTag := builder.genNewTag()
	finalProjList := make([]*plan.Expr, 0, len(tableDef.Cols)+len(tableDef.Indexes)*2)
	var newPkIdx int32

	{
		insertCols := make([]plan.ColRef, len(tableDef.Cols)-1)
		deleteCols := make([]plan.ColRef, 2)

		for i, col := range tableDef.Cols {
			finalColIdx := len(finalProjList)

			if col.Name != catalog.Row_ID {
				insertCols[i].RelPos = finalProjTag
				insertCols[i].ColPos = int32(finalColIdx)
			}

			colIdx := colName2Idx[tableDef.Name+"."+col.Name]
			finalProjList = append(finalProjList, &plan.Expr{
				Typ: fullProjList[colIdx].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: fullProjTag,
						ColPos: int32(colIdx),
					},
				},
			})

			if col.Name == tableDef.Pkey.PkeyColName {
				newPkIdx = int32(finalColIdx)
			}
		}

		lockTargets = append(lockTargets, &plan.LockTarget{
			TableId:            tableDef.TblId,
			ObjRef:             objRef,
			PrimaryColIdxInBat: newPkIdx,
			PrimaryColRelPos:   finalProjTag,
			PrimaryColTyp:      finalProjList[newPkIdx].Typ,
		})

		oldRowIdPos := oldColName2Idx[tableDef.Name+"."+catalog.Row_ID]
		deleteCols[0].RelPos = finalProjTag
		deleteCols[0].ColPos = int32(len(finalProjList))
		finalProjList = append(finalProjList, &plan.Expr{
			Typ: fullProjList[oldRowIdPos[1]].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: fullProjTag,
					ColPos: oldRowIdPos[1],
				},
			},
		})

		oldPkPos := oldColName2Idx[tableDef.Name+"."+tableDef.Pkey.PkeyColName]
		deleteCols[1].RelPos = finalProjTag
		deleteCols[1].ColPos = int32(len(finalProjList))
		lockTargets = append(lockTargets, &plan.LockTarget{
			TableId:            tableDef.TblId,
			ObjRef:             objRef,
			PrimaryColIdxInBat: int32(len(finalProjList)),
			PrimaryColRelPos:   finalProjTag,
			PrimaryColTyp:      finalProjList[newPkIdx].Typ,
		})
		finalProjList = append(finalProjList, &plan.Expr{
			Typ: fullProjList[oldPkPos[1]].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: fullProjTag,
					ColPos: oldPkPos[1],
				},
			},
		})

		updateCtxList = append(updateCtxList, &plan.UpdateCtx{
			ObjRef:     objRef,
			TableDef:   tableDef,
			InsertCols: insertCols,
			DeleteCols: deleteCols,
		})
	}

	for i, idxDef := range tableDef.Indexes {
		insertCols := make([]plan.ColRef, 2)
		deleteCols := make([]plan.ColRef, 2)

		newIdxPos := colName2Idx[idxDef.IndexTableName+"."+catalog.IndexTableIndexColName]
		if len(idxDef.Parts) > 1 {
			idxExpr := &plan.Expr{
				Typ: fullProjList[newIdxPos].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: fullProjTag,
						ColPos: newIdxPos,
					},
				},
			}
			newIdxPos = int32(len(finalProjList))
			finalProjList = append(finalProjList, idxExpr)
		}

		oldRowIdPos := int32(len(finalProjList))
		oldColRef := oldColName2Idx[idxDef.IndexTableName+"."+catalog.Row_ID]
		rowIdExpr := &plan.Expr{
			Typ: idxTableDefs[i].Cols[idxTableDefs[i].Name2ColIndex[catalog.Row_ID]].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: oldColRef[0],
					ColPos: oldColRef[1],
				},
			},
		}
		finalProjList = append(finalProjList, rowIdExpr)

		oldIdxPos := int32(len(finalProjList))
		oldColRef = oldColName2Idx[idxDef.IndexTableName+"."+catalog.IndexTableIndexColName]
		idxExpr := &plan.Expr{
			Typ: finalProjList[newIdxPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: oldColRef[0],
					ColPos: oldColRef[1],
				},
			},
		}
		finalProjList = append(finalProjList, idxExpr)

		insertCols[0].RelPos = finalProjTag
		insertCols[0].ColPos = int32(newIdxPos)
		insertCols[1].RelPos = finalProjTag
		insertCols[1].ColPos = newPkIdx

		deleteCols[0].RelPos = finalProjTag
		deleteCols[0].ColPos = oldRowIdPos
		deleteCols[1].RelPos = finalProjTag
		deleteCols[1].ColPos = int32(oldIdxPos)

		updateCtxList = append(updateCtxList, &plan.UpdateCtx{
			ObjRef:     idxObjRefs[i],
			TableDef:   idxTableDefs[i],
			InsertCols: insertCols,
			DeleteCols: deleteCols,
		})

		if idxDef.Unique {
			lockTargets = append(lockTargets, &plan.LockTarget{
				TableId:            idxTableDefs[i].TblId,
				ObjRef:             idxObjRefs[i],
				PrimaryColIdxInBat: int32(newIdxPos),
				PrimaryColRelPos:   finalProjTag,
				PrimaryColTyp:      finalProjList[newIdxPos].Typ,
			}, &plan.LockTarget{
				TableId:            idxTableDefs[i].TblId,
				ObjRef:             idxObjRefs[i],
				PrimaryColIdxInBat: int32(oldIdxPos),
				PrimaryColRelPos:   finalProjTag,
				PrimaryColTyp:      finalProjList[oldIdxPos].Typ,
			})
		}
	}

	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{lastNodeID},
		ProjectList: finalProjList,
		BindingTags: []int32{finalProjTag},
	}, bindCtx)

	if len(lockTargets) > 0 {
		lastNodeID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_LOCK_OP,
			Children:    []int32{lastNodeID},
			TableDef:    tableDef,
			BindingTags: []int32{builder.genNewTag()},
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

	return lastNodeID, nil
}

func (builder *QueryBuilder) appendNodesForReplaceStmt(
	bindCtx *BindContext,
	lastNodeID int32,
	tableDef *TableDef,
	objRef *ObjectRef,
	insertColToExpr map[string]*Expr,
) (int32, map[string]int32, []bool, error) {
	colName2Idx := make(map[string]int32)
	hasAutoCol := false
	for _, col := range tableDef.Cols {
		if col.Typ.AutoIncr {
			hasAutoCol = true
			break
		}
	}

	projList1 := make([]*plan.Expr, 0, len(tableDef.Cols)-1)
	projList2 := make([]*plan.Expr, 0, len(tableDef.Cols)-1)
	projTag1 := builder.genNewTag()
	preInsertTag := builder.genNewTag()

	var (
		compPkeyExpr  *plan.Expr
		clusterByExpr *plan.Expr
	)

	columnIsNull := make(map[string]bool)
	hasCompClusterBy := tableDef.ClusterBy != nil && util.JudgeIsCompositeClusterByColumn(tableDef.ClusterBy.Name)

	for i, col := range tableDef.Cols {
		if oldExpr, exists := insertColToExpr[col.Name]; exists {
			projList2 = append(projList2, &plan.Expr{
				Typ: oldExpr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: projTag1,
						ColPos: int32(len(projList1)),
					},
				},
			})
			projList1 = append(projList1, oldExpr)
		} else if col.Name == catalog.Row_ID {
			continue
		} else if col.Name == catalog.CPrimaryKeyColName {
			//args := make([]*plan.Expr, len(tableDef.Pkey.Names))
			//
			//for k, part := range tableDef.Pkey.Names {
			//	args[k] = DeepCopyExpr(insertColToExpr[part])
			//}
			//
			//compPkeyExpr, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", args)
			compPkeyExpr = makeCompPkeyExpr(tableDef, tableDef.Name2ColIndex)
			projList2 = append(projList2, &plan.Expr{
				Typ: compPkeyExpr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: preInsertTag,
						ColPos: 0,
					},
				},
			})
		} else if hasCompClusterBy && col.Name == tableDef.ClusterBy.Name {
			//names := util.SplitCompositeClusterByColumnName(tableDef.ClusterBy.Name)
			//args := make([]*plan.Expr, len(names))
			//
			//for k, part := range names {
			//	args[k] = DeepCopyExpr(insertColToExpr[part])
			//}
			//
			//clusterByExpr, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial_full", args)
			clusterByExpr = makeClusterByExpr(tableDef, tableDef.Name2ColIndex)
			projList2 = append(projList2, &plan.Expr{
				Typ: clusterByExpr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: preInsertTag,
						ColPos: 0,
					},
				},
			})
		} else {
			defExpr, err := getDefaultExpr(builder.GetContext(), col)
			if err != nil {
				return 0, nil, nil, err
			}

			if !col.Typ.AutoIncr {
				if lit := defExpr.GetLit(); lit != nil {
					if lit.Isnull {
						columnIsNull[col.Name] = true
					}
				}
			}

			projList2 = append(projList2, &plan.Expr{
				Typ: defExpr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: projTag1,
						ColPos: int32(len(projList1)),
					},
				},
			})
			projList1 = append(projList1, defExpr)
		}

		colName2Idx[tableDef.Name+"."+col.Name] = int32(i)
	}

	validIndexes, hasIrregularIndex := getValidIndexes(tableDef)
	if hasIrregularIndex {
		return 0, nil, nil, moerr.NewUnsupportedDML(builder.GetContext(), "have vector index table")
	}
	tableDef.Indexes = validIndexes

	skipUniqueIdx := make([]bool, len(tableDef.Indexes))
	pkName := tableDef.Pkey.PkeyColName
	pkPos := tableDef.Name2ColIndex[pkName]
	for i, idxDef := range tableDef.Indexes {
		skipUniqueIdx[i] = true
		for _, part := range idxDef.Parts {
			if !columnIsNull[catalog.ResolveAlias(part)] {
				skipUniqueIdx[i] = false
				break
			}
		}

		idxTableName := idxDef.IndexTableName
		colName2Idx[idxTableName+"."+catalog.IndexTablePrimaryColName] = pkPos
		argsLen := len(idxDef.Parts)
		if argsLen == 1 {
			colName2Idx[idxTableName+"."+catalog.IndexTableIndexColName] = colName2Idx[tableDef.Name+"."+idxDef.Parts[0]]
		} else {
			args := make([]*plan.Expr, argsLen)

			var colPos int32
			var ok bool
			for k := 0; k < argsLen; k++ {
				if colPos, ok = colName2Idx[tableDef.Name+"."+catalog.ResolveAlias(idxDef.Parts[k])]; !ok {
					errMsg := fmt.Sprintf("bind insert err, can not find colName = %s", idxDef.Parts[k])
					return 0, nil, nil, moerr.NewInternalError(builder.GetContext(), errMsg)
				}
				args[k] = DeepCopyExpr(projList2[colPos])
			}

			funcName := "serial"
			if !idxDef.Unique {
				funcName = "serial_full"
			}
			idxExpr, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), funcName, args)
			colName2Idx[idxTableName+"."+catalog.IndexTableIndexColName] = int32(len(projList2))
			projList2 = append(projList2, idxExpr)
		}
	}

	tmpCtx := NewBindContext(builder, bindCtx)
	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		ProjectList: projList1,
		Children:    []int32{lastNodeID},
		BindingTags: []int32{projTag1},
	}, tmpCtx)

	if hasAutoCol || compPkeyExpr != nil || clusterByExpr != nil {
		lastNodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_PRE_INSERT,
			Children: []int32{lastNodeID},
			PreInsertCtx: &plan.PreInsertCtx{
				Ref:           objRef,
				TableDef:      tableDef,
				HasAutoCol:    hasAutoCol,
				CompPkeyExpr:  compPkeyExpr,
				ClusterByExpr: clusterByExpr,
			},
			BindingTags: []int32{preInsertTag},
		}, tmpCtx)
	}

	lastNodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		ProjectList: projList2,
		Children:    []int32{lastNodeID},
		BindingTags: []int32{builder.genNewTag()},
	}, tmpCtx)

	return lastNodeID, colName2Idx, skipUniqueIdx, nil
}
