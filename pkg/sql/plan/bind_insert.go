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

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func (builder *QueryBuilder) bindInsert(stmt *tree.Insert, bindCtx *BindContext) (int32, error) {
	dmlCtx := NewDMLContext()
	err := dmlCtx.ResolveTables(builder.compCtx, tree.TableExprs{stmt.Table}, nil, nil, true)
	if err != nil {
		return 0, err
	}

	// clusterTable, err := getAccountInfoOfClusterTable(ctx, stmt.Accounts, tableDef, tblInfo.isClusterTable[0])
	// if err != nil {
	// 	return 0, err
	// }
	// if len(stmt.OnDuplicateUpdate) > 0 && clusterTable.IsClusterTable {
	// 	return 0, moerr.NewNotSupported(builder.compCtx.GetContext(), "INSERT ... ON DUPLICATE KEY UPDATE ... for cluster table")
	// }

	if stmt.IsRestore {
		builder.isRestore = true
		if stmt.IsRestoreByTs {
			builder.isRestoreByTs = true
		}
		oldSnapshot := builder.compCtx.GetSnapshot()
		builder.compCtx.SetSnapshot(&Snapshot{
			Tenant: &plan.SnapshotTenant{
				TenantName: "xxx",
				TenantID:   stmt.FromDataTenantID,
			},
		})
		defer func() {
			builder.compCtx.SetSnapshot(oldSnapshot)
		}()
	}

	lastNodeID, colName2Idx, skipUniqueIdx, err := builder.initInsertStmt(bindCtx, stmt, dmlCtx.objRefs[0], dmlCtx.tableDefs[0])
	if err != nil {
		return 0, err
	}

	return builder.appendDedupAndMultiUpdateNodesForBindInsert(bindCtx, dmlCtx, lastNodeID, colName2Idx, skipUniqueIdx, stmt.OnDuplicateUpdate)
}

func (builder *QueryBuilder) canSkipDedup(tableDef *plan.TableDef) bool {
	if builder.optimizerHints != nil && builder.optimizerHints.skipDedup == 1 {
		return true
	}

	if builder.qry.LoadTag || builder.isRestore {
		return true
	}

	if strings.HasPrefix(tableDef.Name, catalog.SecondaryIndexTableNamePrefix) {
		return true
	}

	return false
}

func (builder *QueryBuilder) appendDedupAndMultiUpdateNodesForBindInsert(
	bindCtx *BindContext,
	dmlCtx *DMLContext,
	lastNodeID int32,
	colName2Idx map[string]int32,
	skipUniqueIdx []bool,
	astUpdateExprs tree.UpdateExprs,
) (int32, error) {
	var err error

	selectNode := builder.qry.Nodes[lastNodeID]
	selectTag := selectNode.BindingTags[0]

	tableDef := dmlCtx.tableDefs[0]
	pkName := tableDef.Pkey.PkeyColName

	if tableDef.TableType != catalog.SystemOrdinaryRel &&
		tableDef.TableType != catalog.SystemIndexRel {
		return 0, moerr.NewUnsupportedDML(builder.GetContext(), "insert into vector/text index table")
	}

	for _, idxDef := range tableDef.Indexes {
		if !catalog.IsRegularIndexAlgo(idxDef.IndexAlgo) {
			return 0, moerr.NewUnsupportedDML(builder.GetContext(), "have vector index table")
		}
	}

	var onDupAction plan.Node_OnDuplicateAction
	scanTag := builder.genNewTag()
	updateExprs := make(map[string]*plan.Expr)

	if len(astUpdateExprs) == 0 {
		onDupAction = plan.Node_FAIL
	} else if len(astUpdateExprs) == 1 && astUpdateExprs[0] == nil {
		onDupAction = plan.Node_IGNORE
	} else {
		if pkName == catalog.FakePrimaryKeyColName {
			return 0, moerr.NewUnsupportedDML(builder.compCtx.GetContext(), "update on duplicate without primary key")
		}

		onDupAction = plan.Node_UPDATE

		binder := NewOndupUpdateBinder(builder.GetContext(), builder, bindCtx, scanTag, selectTag, tableDef)
		var updateExpr *plan.Expr
		for _, astUpdateExpr := range astUpdateExprs {
			colName := astUpdateExpr.Names[0].ColName()
			colDef := tableDef.Cols[tableDef.Name2ColIndex[colName]]
			astExpr := astUpdateExpr.Expr

			if _, ok := astExpr.(*tree.DefaultVal); ok {
				if colDef.Typ.AutoIncr {
					return 0, moerr.NewUnsupportedDML(builder.compCtx.GetContext(), "auto_increment default value")
				}

				updateExpr, err = getDefaultExpr(builder.GetContext(), colDef)
				if err != nil {
					return 0, err
				}
			} else {
				updateExpr, err = binder.BindExpr(astExpr, 0, true)
				if err != nil {
					return 0, err
				}
			}

			updateExpr, err = forceCastExpr(builder.GetContext(), updateExpr, colDef.Typ)
			if err != nil {
				return 0, err
			}
			updateExprs[colDef.Name] = updateExpr
		}
	}

	for _, part := range tableDef.Pkey.Names {
		if _, ok := updateExprs[part]; ok {
			return 0, moerr.NewUnsupportedDML(builder.GetContext(), "update primary key on duplicate")
		}
	}

	idxNeedUpdate := make([]bool, len(tableDef.Indexes))
	for i, idxDef := range tableDef.Indexes {
		if !idxDef.TableExist {
			continue
		}

		for _, part := range idxDef.Parts {
			if _, ok := updateExprs[catalog.ResolveAlias(part)]; ok {
				if idxDef.Unique {
					return 0, moerr.NewUnsupportedDML(builder.GetContext(), "update unique key on duplicate")
				} else {
					idxNeedUpdate[i] = true
					break
				}
			}
		}
	}

	objRef := dmlCtx.objRefs[0]
	idxObjRefs := make([]*plan.ObjectRef, len(tableDef.Indexes))
	idxTableDefs := make([]*plan.TableDef, len(tableDef.Indexes))

	//lock main table
	lockTargets := make([]*plan.LockTarget, 0, len(tableDef.Indexes)+1)
	for _, col := range tableDef.Cols {
		if col.Name == pkName && pkName != catalog.FakePrimaryKeyColName {
			lockTarget := &plan.LockTarget{
				TableId:            tableDef.TblId,
				ObjRef:             DeepCopyObjectRef(objRef),
				PrimaryColIdxInBat: int32(colName2Idx[tableDef.Name+"."+col.Name]),
				PrimaryColRelPos:   selectTag,
				PrimaryColTyp:      col.Typ,
			}
			lockTargets = append(lockTargets, lockTarget)
			break
		}
	}
	// lock unique key table
	for j, idxDef := range tableDef.Indexes {
		if !idxDef.TableExist || skipUniqueIdx[j] || !idxDef.Unique {
			continue
		}
		idxObjRef, idxTableDef := builder.compCtx.ResolveIndexTableByRef(dmlCtx.objRefs[0], idxDef.IndexTableName, bindCtx.snapshot)
		var pkIdxInBat int32

		if len(idxDef.Parts) == 1 {
			pkIdxInBat = colName2Idx[tableDef.Name+"."+idxDef.Parts[0]]
		} else {
			pkIdxInBat = colName2Idx[idxTableDef.Name+"."+catalog.IndexTableIndexColName]
		}
		lockTarget := &plan.LockTarget{
			TableId:            idxTableDef.TblId,
			ObjRef:             idxObjRef,
			PrimaryColIdxInBat: pkIdxInBat,
			PrimaryColRelPos:   selectTag,
			PrimaryColTyp:      selectNode.ProjectList[int(pkIdxInBat)].Typ,
		}
		lockTargets = append(lockTargets, lockTarget)
	}
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

	// handle primary/unique key confliction
	if builder.canSkipDedup(dmlCtx.tableDefs[0]) {
		// load do not handle primary/unique key confliction
		for i, idxDef := range tableDef.Indexes {
			if !idxDef.TableExist || skipUniqueIdx[i] {
				continue
			}

			idxObjRefs[i], idxTableDefs[i] = builder.compCtx.ResolveIndexTableByRef(objRef, idxDef.IndexTableName, bindCtx.snapshot)
		}
	} else {
		if pkName != catalog.FakePrimaryKeyColName {
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
						RelPos: selectTag,
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

			for j, part := range tableDef.Pkey.Names {
				dedupColTypes[j] = tableDef.Cols[tableDef.Name2ColIndex[part]].Typ
			}

			dedupJoinNode := &plan.Node{
				NodeType:          plan.Node_JOIN,
				Children:          []int32{scanNodeID, lastNodeID},
				JoinType:          plan.Node_DEDUP,
				OnList:            []*plan.Expr{joinCond},
				OnDuplicateAction: onDupAction,
				DedupColName:      dedupColName,
				DedupColTypes:     dedupColTypes,
			}

			if onDupAction == plan.Node_UPDATE {
				oldColList := make([]plan.ColRef, len(tableDef.Cols))
				for i := range tableDef.Cols {
					oldColList[i].RelPos = scanTag
					oldColList[i].ColPos = int32(i)
				}

				updateColIdxList := make([]int32, 0, len(astUpdateExprs))
				updateColExprList := make([]*plan.Expr, 0, len(astUpdateExprs))
				for colName, updateExpr := range updateExprs {
					updateColIdxList = append(updateColIdxList, tableDef.Name2ColIndex[colName])
					updateColExprList = append(updateColExprList, updateExpr)
				}

				dedupJoinNode.DedupJoinCtx = &plan.DedupJoinCtx{
					OldColList:        oldColList,
					UpdateColIdxList:  updateColIdxList,
					UpdateColExprList: updateColExprList,
				}
			}

			lastNodeID = builder.appendNode(dedupJoinNode, bindCtx)
		}

		for i, idxDef := range tableDef.Indexes {
			if !idxDef.TableExist || skipUniqueIdx[i] {
				continue
			}

			idxObjRefs[i], idxTableDefs[i] = builder.compCtx.ResolveIndexTableByRef(objRef, idxDef.IndexTableName, bindCtx.snapshot)

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
						RelPos: selectTag,
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

			idxOnDupAction := onDupAction
			if onDupAction == plan.Node_UPDATE {
				idxOnDupAction = plan.Node_FAIL
			}

			lastNodeID = builder.appendNode(&plan.Node{
				NodeType:          plan.Node_JOIN,
				Children:          []int32{idxTableNodeID, lastNodeID},
				JoinType:          plan.Node_DEDUP,
				OnList:            []*plan.Expr{joinCond},
				OnDuplicateAction: idxOnDupAction,
				DedupColName:      dedupColName,
				DedupColTypes:     dedupColTypes,
			}, bindCtx)
		}
	}

	newProjLen := len(selectNode.ProjectList)
	for _, idxDef := range tableDef.Indexes {
		if idxDef.TableExist && !idxDef.Unique {
			newProjLen++
		}
	}

	if onDupAction == plan.Node_UPDATE {
		newProjLen++
	}

	delColName2Idx := make(map[string][2]int32)

	if newProjLen > len(selectNode.ProjectList) {
		newProjList := make([]*plan.Expr, 0, newProjLen)
		finalProjTag := builder.genNewTag()
		pkPos := colName2Idx[tableDef.Name+"."+tableDef.Pkey.PkeyColName]

		for i, expr := range selectNode.ProjectList {
			newProjList = append(newProjList, &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: selectTag,
						ColPos: int32(i),
					},
				},
			})
		}

		if onDupAction == plan.Node_UPDATE {
			delColName2Idx[tableDef.Name+"."+catalog.Row_ID] = [2]int32{finalProjTag, int32(len(newProjList))}
			rowIDIdx := tableDef.Name2ColIndex[catalog.Row_ID]
			rowIDCol := tableDef.Cols[rowIDIdx]

			newProjList = append(newProjList, &plan.Expr{
				Typ: rowIDCol.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: scanTag,
						ColPos: rowIDIdx,
					},
				},
			})
		}

		for i, idxDef := range tableDef.Indexes {
			if !idxDef.TableExist || idxDef.Unique {
				continue
			}

			idxTableName := idxDef.IndexTableName
			colName2Idx[idxTableName+"."+catalog.IndexTablePrimaryColName] = pkPos
			argsLen := len(idxDef.Parts) // argsLen is alwarys greater than 1 for secondary index
			args := make([]*plan.Expr, argsLen)

			var colPos int32
			var ok bool
			for k := 0; k < argsLen; k++ {
				if colPos, ok = colName2Idx[tableDef.Name+"."+catalog.ResolveAlias(idxDef.Parts[k])]; !ok {
					errMsg := fmt.Sprintf("bind insert err, can not find colName = %s", idxDef.Parts[k])
					return 0, moerr.NewInternalError(builder.GetContext(), errMsg)
				}

				args[k] = &plan.Expr{
					Typ: selectNode.ProjectList[colPos].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: selectTag,
							ColPos: colPos,
						},
					},
				}
			}

			idxExpr, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial_full", args)
			colName2Idx[idxTableName+"."+catalog.IndexTableIndexColName] = int32(len(newProjList))
			newProjList = append(newProjList, idxExpr)

			if idxNeedUpdate[i] {
				delArgs := make([]*plan.Expr, argsLen)

				var colPos int32
				var ok bool
				for k := 0; k < argsLen; k++ {
					if colPos, ok = tableDef.Name2ColIndex[catalog.ResolveAlias(idxDef.Parts[k])]; !ok {
						errMsg := fmt.Sprintf("bind insert err, can not find colName = %s", idxDef.Parts[k])
						return 0, moerr.NewInternalError(builder.GetContext(), errMsg)
					}

					delArgs[k] = &plan.Expr{
						Typ: selectNode.ProjectList[colPos].Typ,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: scanTag,
								ColPos: colPos,
							},
						},
					}
				}

				delIdxExpr, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial_full", delArgs)
				delColName2Idx[idxTableName+"."+catalog.IndexTableIndexColName] = [2]int32{finalProjTag, int32(len(newProjList))}
				newProjList = append(newProjList, delIdxExpr)
			}
		}

		selectTag = finalProjTag
		lastNodeID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_PROJECT,
			ProjectList: newProjList,
			Children:    []int32{lastNodeID},
			BindingTags: []int32{selectTag},
		}, bindCtx)
	}

	if onDupAction == plan.Node_UPDATE {
		for i, idxDef := range tableDef.Indexes {
			if !idxNeedUpdate[i] {
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

			idxTableName := idxDef.IndexTableName
			delColName2Idx[idxTableName+"."+catalog.Row_ID] = [2]int32{idxTag, idxTableDefs[i].Name2ColIndex[catalog.Row_ID]}
			delPkIdx := delColName2Idx[idxTableName+"."+catalog.IndexTableIndexColName]

			rightExpr := &plan.Expr{
				Typ: pkTyp,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: delPkIdx[0],
						ColPos: delPkIdx[1],
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
	}

	dmlNode := &plan.Node{
		NodeType:    plan.Node_MULTI_UPDATE,
		BindingTags: []int32{builder.genNewTag()},
	}

	insertCols := make([]plan.ColRef, len(tableDef.Cols)-1)
	updateCtx := &plan.UpdateCtx{
		ObjRef:     objRef,
		TableDef:   tableDef,
		InsertCols: insertCols,
	}

	for i, col := range tableDef.Cols {
		if col.Name == catalog.Row_ID {
			continue
		}

		insertCols[i].RelPos = selectTag
		insertCols[i].ColPos = colName2Idx[tableDef.Name+"."+col.Name]
	}

	if onDupAction == plan.Node_UPDATE {
		deleteCols := make([]plan.ColRef, 2)
		updateCtx.DeleteCols = deleteCols

		delRowIDIdx := delColName2Idx[tableDef.Name+"."+catalog.Row_ID]
		deleteCols[0].RelPos = delRowIDIdx[0]
		deleteCols[0].ColPos = delRowIDIdx[1]

		deleteCols[1].RelPos = selectTag
		deleteCols[1].ColPos = colName2Idx[tableDef.Name+"."+tableDef.Pkey.PkeyColName]
	}

	dmlNode.UpdateCtxList = append(dmlNode.UpdateCtxList, updateCtx)

	for i, idxTableDef := range idxTableDefs {
		if idxTableDef == nil {
			continue
		}

		idxInsertCols := make([]plan.ColRef, len(idxTableDef.Cols)-1)
		updateCtx := &plan.UpdateCtx{
			ObjRef:     idxObjRefs[i],
			TableDef:   idxTableDef,
			InsertCols: idxInsertCols,
		}

		for j, col := range idxTableDef.Cols {
			if col.Name == catalog.Row_ID {
				continue
			}

			idxInsertCols[j].RelPos = selectTag
			idxInsertCols[j].ColPos = int32(colName2Idx[idxTableDef.Name+"."+col.Name])
		}

		if idxNeedUpdate[i] {
			deleteCols := make([]plan.ColRef, 2)
			updateCtx.DeleteCols = deleteCols

			delRowIDIdx := delColName2Idx[idxTableDef.Name+"."+catalog.Row_ID]
			deleteCols[0].RelPos = delRowIDIdx[0]
			deleteCols[0].ColPos = delRowIDIdx[1]

			delPkIdx := delColName2Idx[idxTableDef.Name+"."+catalog.IndexTableIndexColName]
			deleteCols[1].RelPos = delPkIdx[0]
			deleteCols[1].ColPos = delPkIdx[1]
		}

		dmlNode.UpdateCtxList = append(dmlNode.UpdateCtxList, updateCtx)
	}

	dmlNode.Children = append(dmlNode.Children, lastNodeID)
	lastNodeID = builder.appendNode(dmlNode, bindCtx)

	return lastNodeID, nil
}

// getInsertColsFromStmt retrieves the list of column names to be inserted into a table
// based on the given INSERT statement and table definition.
// If the INSERT statement does not specify the columns, all columns except the fake primary key column
// will be included in the list.
// If the INSERT statement specifies the columns, it validates the column names against the table definition
// and returns an error if any of the column names are invalid.
// The function returns the list of insert columns and an error, if any.
func (builder *QueryBuilder) getInsertColsFromStmt(stmt *tree.Insert, tableDef *TableDef) ([]string, error) {
	var insertColNames []string
	colToIdx := make(map[string]int)
	for i, col := range tableDef.Cols {
		colToIdx[strings.ToLower(col.Name)] = i
	}
	if stmt.Columns == nil {
		for _, col := range tableDef.Cols {
			if !col.Hidden {
				insertColNames = append(insertColNames, col.Name)
			}
		}
	} else {
		for _, column := range stmt.Columns {
			colName := strings.ToLower(string(column))
			idx, ok := colToIdx[colName]
			if !ok {
				return nil, moerr.NewBadFieldError(builder.GetContext(), colName, tableDef.Name)
			}
			insertColNames = append(insertColNames, tableDef.Cols[idx].Name)
		}
	}
	return insertColNames, nil
}

func (builder *QueryBuilder) initInsertStmt(bindCtx *BindContext, stmt *tree.Insert, objRef *plan.ObjectRef, tableDef *plan.TableDef) (int32, map[string]int32, []bool, error) {
	var (
		lastNodeID int32
		err        error
	)

	// var uniqueCheckOnAutoIncr string
	var insertColumns []string

	//var ifInsertFromUniqueColMap map[string]bool
	if insertColumns, err = builder.getInsertColsFromStmt(stmt, tableDef); err != nil {
		return 0, nil, nil, err
	}

	var astSelect *tree.Select
	switch selectImpl := stmt.Rows.Select.(type) {
	// rewrite 'insert into tbl values (1,1)' to 'insert into tbl select * from (values row(1,1))'
	case *tree.ValuesClause:
		isAllDefault := false
		if selectImpl.Rows[0] == nil {
			isAllDefault = true
		}
		if isAllDefault {
			for j, row := range selectImpl.Rows {
				if row != nil {
					return 0, nil, nil, moerr.NewWrongValueCountOnRow(builder.GetContext(), j+1)
				}
			}
		} else {
			colCount := len(insertColumns)
			for j, row := range selectImpl.Rows {
				if len(row) != colCount {
					return 0, nil, nil, moerr.NewWrongValueCountOnRow(builder.GetContext(), j+1)
				}
			}
		}

		// example1:insert into a values ();
		// but it does not work at the case:
		// insert into a(a) values (); insert into a values (0),();
		if isAllDefault && stmt.Columns != nil {
			return 0, nil, nil, moerr.NewInvalidInput(builder.GetContext(), "insert values does not match the number of columns")
		}
		lastNodeID, err = builder.buildValueScan(isAllDefault, bindCtx, tableDef, selectImpl, insertColumns)
		if err != nil {
			return 0, nil, nil, err
		}

	case *tree.SelectClause:
		astSelect = stmt.Rows

		subCtx := NewBindContext(builder, bindCtx)
		lastNodeID, err = builder.bindSelect(astSelect, subCtx, false)
		if err != nil {
			return 0, nil, nil, err
		}
		//ifInsertFromUniqueColMap = make(map[string]bool)

	case *tree.ParenSelect:
		astSelect = selectImpl.Select

		subCtx := NewBindContext(builder, bindCtx)
		lastNodeID, err = builder.bindSelect(astSelect, subCtx, false)
		if err != nil {
			return 0, nil, nil, err
		}
		// ifInsertFromUniqueColMap = make(map[string]bool)

	default:
		return 0, nil, nil, moerr.NewInvalidInput(builder.GetContext(), "insert has unknown select statement")
	}

	if err = builder.addBinding(lastNodeID, tree.AliasClause{Alias: derivedTableName}, bindCtx); err != nil {
		return 0, nil, nil, err
	}

	lastNode := builder.qry.Nodes[lastNodeID]
	if len(insertColumns) != len(lastNode.ProjectList) {
		return 0, nil, nil, moerr.NewInvalidInput(builder.GetContext(), "insert values does not match the number of columns")
	}

	selectTag := lastNode.BindingTags[0]

	insertColToExpr := make(map[string]*plan.Expr)
	for i, column := range insertColumns {
		colIdx := tableDef.Name2ColIndex[column]
		projExpr := &plan.Expr{
			Typ: lastNode.ProjectList[i].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: selectTag,
					ColPos: int32(i),
				},
			},
		}
		if tableDef.Cols[colIdx].Typ.Id == int32(types.T_enum) {
			projExpr, err = funcCastForEnumType(builder.GetContext(), projExpr, tableDef.Cols[colIdx].Typ)
			if err != nil {
				return 0, nil, nil, err
			}
		} else {
			projExpr, err = forceCastExpr(builder.GetContext(), projExpr, tableDef.Cols[colIdx].Typ)
			if err != nil {
				return 0, nil, nil, err
			}
		}
		insertColToExpr[column] = projExpr
	}

	return builder.appendNodesForInsertStmt(bindCtx, lastNodeID, tableDef, objRef, insertColToExpr)
}

func (builder *QueryBuilder) appendNodesForInsertStmt(
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
		} else if tableDef.ClusterBy != nil && col.Name == tableDef.ClusterBy.Name {
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

	skipUniqueIdx := make([]bool, len(tableDef.Indexes))
	pkName := tableDef.Pkey.PkeyColName
	pkPos := tableDef.Name2ColIndex[pkName]
	for i, idxDef := range tableDef.Indexes {
		if !idxDef.TableExist || !idxDef.Unique {
			continue
		}

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

			idxExpr, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", args)
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

func (builder *QueryBuilder) buildValueScan(
	isAllDefault bool,
	bindCtx *BindContext,
	tableDef *TableDef,
	stmt *tree.ValuesClause,
	colNames []string,
) (int32, error) {
	var err error

	proc := builder.compCtx.GetProcess()
	lastTag := builder.genNewTag()
	colCount := len(colNames)
	rowsetData := &plan.RowsetData{
		Cols: make([]*plan.ColData, colCount),
	}
	for i := 0; i < colCount; i++ {
		rowsetData.Cols[i] = new(plan.ColData)
	}
	valueScanTableDef := &plan.TableDef{
		TblId: 0,
		Name:  "",
		Cols:  make([]*plan.ColDef, colCount),
	}
	projectList := make([]*plan.Expr, colCount)

	for i, colName := range colNames {
		col := tableDef.Cols[tableDef.Name2ColIndex[colName]]
		colTyp := makeTypeByPlan2Type(col.Typ)
		targetTyp := &plan.Expr{
			Typ: col.Typ,
			Expr: &plan.Expr_T{
				T: &plan.TargetType{},
			},
		}
		var defExpr *plan.Expr
		if isAllDefault {
			defExpr, err := getDefaultExpr(builder.GetContext(), col)
			if err != nil {
				return 0, err
			}
			defExpr, err = forceCastExpr2(builder.GetContext(), defExpr, colTyp, targetTyp)
			if err != nil {
				return 0, err
			}
			rowsetData.Cols[i].Data = make([]*plan.RowsetExpr, len(stmt.Rows))
			for j := range stmt.Rows {
				rowsetData.Cols[i].Data[j] = &plan.RowsetExpr{
					Expr: defExpr,
				}
			}
		} else {
			binder := NewDefaultBinder(builder.GetContext(), nil, nil, col.Typ, nil)
			binder.builder = builder
			for _, r := range stmt.Rows {
				if nv, ok := r[i].(*tree.NumVal); ok {
					expr, err := MakeInsertValueConstExpr(proc, nv, &colTyp)
					if err != nil {
						return 0, err
					}
					if expr != nil {
						rowsetData.Cols[i].Data = append(rowsetData.Cols[i].Data, &plan.RowsetExpr{
							Expr: expr,
						})
						continue
					}
				}

				if _, ok := r[i].(*tree.DefaultVal); ok {
					defExpr, err = getDefaultExpr(builder.GetContext(), col)
					if err != nil {
						return 0, err
					}
				} else {
					defExpr, err = binder.BindExpr(r[i], 0, true)
					if err != nil {
						return 0, err
					}
					if col.Typ.Id == int32(types.T_enum) {
						defExpr, err = funcCastForEnumType(builder.GetContext(), defExpr, col.Typ)
						if err != nil {
							return 0, err
						}
					}
				}
				defExpr, err = forceCastExpr2(builder.GetContext(), defExpr, colTyp, targetTyp)
				if err != nil {
					return 0, err
				}
				rowsetData.Cols[i].Data = append(rowsetData.Cols[i].Data, &plan.RowsetExpr{
					Expr: defExpr,
				})
			}
		}
		colName := fmt.Sprintf("column_%d", i) // like MySQL
		valueScanTableDef.Cols[i] = &plan.ColDef{
			ColId: 0,
			Name:  colName,
			Typ:   col.Typ,
		}
		expr := &plan.Expr{
			Typ: col.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: lastTag,
					ColPos: int32(i),
				},
			},
		}
		projectList[i] = expr
	}

	rowsetData.RowCount = int32(len(stmt.Rows))
	nodeId, _ := uuid.NewV7()
	scanNode := &plan.Node{
		NodeType:    plan.Node_VALUE_SCAN,
		RowsetData:  rowsetData,
		TableDef:    valueScanTableDef,
		BindingTags: []int32{lastTag},
		Uuid:        nodeId[:],
	}
	nodeID := builder.appendNode(scanNode, bindCtx)
	if err = builder.addBinding(nodeID, tree.AliasClause{Alias: "_valuescan"}, bindCtx); err != nil {
		return 0, err
	}

	lastTag = builder.genNewTag()
	nodeID = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		ProjectList: projectList,
		Children:    []int32{nodeID},
		BindingTags: []int32{lastTag},
	}, bindCtx)

	return nodeID, nil
}
