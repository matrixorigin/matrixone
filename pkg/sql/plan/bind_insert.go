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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

func (builder *QueryBuilder) bindInsert(stmt *tree.Insert, ctx *BindContext) (int32, error) {
	var onDupAction plan.Node_OnDuplicateAction
	if len(stmt.OnDuplicateUpdate) == 0 {
		onDupAction = plan.Node_ERROR
	} else if len(stmt.OnDuplicateUpdate) == 1 && stmt.OnDuplicateUpdate[0] == nil {
		onDupAction = plan.Node_IGNORE
	} else {
		//onDupAction = plan.Node_UPDATE
		return 0, moerr.NewUnsupportedDML(builder.GetContext(), "on duplicate key update")
	}

	tables := tree.TableExprs{stmt.Table}
	objRefs := make([]*plan.ObjectRef, len(tables))
	tableDefs := make([]*plan.TableDef, len(tables))
	for i, tbl := range tables {
		tn, ok := tbl.(*tree.TableName)
		if !ok {
			return 0, moerr.NewUnsupportedDML(builder.GetContext(), "insert not into single table")
		}
		dbName := string(tn.SchemaName)
		tblName := string(tn.ObjectName)
		if len(dbName) == 0 {
			dbName = builder.compCtx.DefaultDatabase()
		}

		objRefs[i], tableDefs[i] = builder.compCtx.Resolve(dbName, tblName, nil)
		if tableDefs[i] == nil {
			return 0, moerr.NewNoSuchTable(builder.compCtx.GetContext(), dbName, tblName)
		}
		if tableDefs[i].TableType == catalog.SystemSourceRel {
			return 0, moerr.NewInvalidInput(builder.compCtx.GetContext(), "cannot insert/update/delete from source")
		} else if tableDefs[i].TableType == catalog.SystemExternalRel {
			return 0, moerr.NewInvalidInput(builder.compCtx.GetContext(), "cannot insert/update/delete from external table")
		} else if tableDefs[i].TableType == catalog.SystemViewRel {
			return 0, moerr.NewInvalidInput(builder.compCtx.GetContext(), "cannot insert/update/delete from view")
		} else if tableDefs[i].TableType == catalog.SystemSequenceRel && builder.compCtx.GetContext().Value(defines.BgKey{}) == nil {
			return 0, moerr.NewInvalidInput(builder.compCtx.GetContext(), "Cannot insert/update/delete from sequence")
		}
	}

	tblInfo, err := getDmlTableInfo(builder.compCtx, tree.TableExprs{stmt.Table}, nil, nil, "insert")
	if err != nil {
		return 0, err
	}
	rewriteInfo := &dmlSelectInfo{
		typ:     "insert",
		rootId:  -1,
		tblInfo: tblInfo,
	}
	modTableDef := tblInfo.tableDefs[0]
	// clusterTable, err := getAccountInfoOfClusterTable(ctx, stmt.Accounts, tableDef, tblInfo.isClusterTable[0])
	// if err != nil {
	// 	return 0, err
	// }
	// if len(stmt.OnDuplicateUpdate) > 0 && clusterTable.IsClusterTable {
	// 	return 0, moerr.NewNotSupported(builder.compCtx.GetContext(), "INSERT ... ON DUPLICATE KEY UPDATE ... for cluster table")
	// }

	if stmt.IsRestore {
		builder.isRestore = true
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

	colName2Idx, err := builder.initInsertStmt(ctx, stmt, rewriteInfo, tableDefs[0])
	if err != nil {
		return 0, err
	}
	replaceStmt := getRewriteToReplaceStmt(modTableDef, stmt, rewriteInfo, builder.isPrepareStatement)
	if replaceStmt != nil {
		return 0, moerr.NewUnsupportedDML(builder.GetContext(), "rewrite to replace")
	}
	lastNodeID := rewriteInfo.rootId

	//colName2Idx := make(map[string]int)

	selectNode := builder.qry.Nodes[lastNodeID]
	idxObjRefs := make([][]*plan.ObjectRef, len(tableDefs))
	idxTableDefs := make([][]*plan.TableDef, len(tableDefs))

	for _, tableDef := range tableDefs {
		for _, idxDef := range tableDef.Indexes {
			if !catalog.IsRegularIndexAlgo(idxDef.IndexAlgo) {
				return 0, moerr.NewUnsupportedDML(builder.GetContext(), "have vector index table")
			}

		}
	}

	for i, tableDef := range tableDefs {
		scanTag := builder.genNewTag()
		builder.addNameByColRef(scanTag, tableDef)

		scanNodeID := builder.appendNode(&plan.Node{
			NodeType:     plan.Node_TABLE_SCAN,
			TableDef:     DeepCopyTableDef(tableDef, true),
			ObjRef:       objRefs[i],
			BindingTags:  []int32{scanTag},
			ScanSnapshot: ctx.snapshot,
		}, ctx)

		pkName := tableDef.Pkey.PkeyColName
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
					RelPos: selectNode.BindingTags[0],
					ColPos: colName2Idx[tableDef.Name+"."+pkName],
				},
			},
		}

		joinCond, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
			leftExpr,
			rightExpr,
		})

		lastNodeID = builder.appendNode(&plan.Node{
			NodeType:          plan.Node_JOIN,
			Children:          []int32{scanNodeID, lastNodeID},
			JoinType:          plan.Node_DEDUP,
			OnList:            []*plan.Expr{joinCond},
			OnDuplicateAction: onDupAction,
		}, ctx)

		idxObjRefs[i] = make([]*plan.ObjectRef, len(tableDef.Indexes))
		idxTableDefs[i] = make([]*plan.TableDef, len(tableDef.Indexes))

		for j, idxDef := range tableDef.Indexes {
			if !idxDef.TableExist {
				continue
			}

			idxObjRefs[i][j], idxTableDefs[i][j] = builder.compCtx.Resolve(objRefs[i].SchemaName, idxDef.IndexTableName, nil)
			colName2Idx[idxTableDefs[i][j].Name+"."+catalog.IndexTablePrimaryColName] = pkPos

			argsLen := len(idxDef.Parts)

			if argsLen == 1 {
				colName2Idx[idxTableDefs[i][j].Name+"."+catalog.IndexTableIndexColName] = colName2Idx[tableDef.Name+"."+idxDef.Parts[0]]
			} else {
				args := make([]*plan.Expr, argsLen)

				if !idxDef.Unique {
					argsLen--
				}

				for k := 0; k < argsLen; k++ {
					colPos := colName2Idx[tableDef.Name+"."+idxDef.Parts[k]]
					args[k] = DeepCopyExpr(selectNode.ProjectList[colPos])
				}

				if !idxDef.Unique {
					args[len(idxDef.Parts)-1] = DeepCopyExpr(selectNode.ProjectList[pkPos])
				}

				fnName := "serial"
				if !idxDef.Unique {
					fnName = "serial_full"
				}
				idxExpr, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), fnName, args)
				colName2Idx[idxTableDefs[i][j].Name+"."+catalog.IndexTableIndexColName] = int32(len(selectNode.ProjectList))
				selectNode.ProjectList = append(selectNode.ProjectList, idxExpr)
			}

			if !idxDef.Unique {
				continue
			}

			idxTag := builder.genNewTag()
			builder.addNameByColRef(idxTag, idxTableDefs[i][j])

			idxScanNode := &plan.Node{
				NodeType:     plan.Node_TABLE_SCAN,
				TableDef:     idxTableDefs[i][j],
				ObjRef:       idxObjRefs[i][j],
				BindingTags:  []int32{idxTag},
				ScanSnapshot: ctx.snapshot,
			}
			idxTableNodeID := builder.appendNode(idxScanNode, ctx)

			idxPkPos := idxTableDefs[i][j].Name2ColIndex[catalog.IndexTableIndexColName]
			pkTyp := idxTableDefs[i][j].Cols[idxPkPos].Typ

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
						RelPos: selectNode.BindingTags[0],
						ColPos: colName2Idx[idxTableDefs[i][j].Name+"."+catalog.IndexTableIndexColName],
					},
				},
			}

			joinCond, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
				leftExpr,
				rightExpr,
			})

			lastNodeID = builder.appendNode(&plan.Node{
				NodeType:          plan.Node_JOIN,
				Children:          []int32{idxTableNodeID, lastNodeID},
				JoinType:          plan.Node_DEDUP,
				OnList:            []*plan.Expr{joinCond},
				OnDuplicateAction: onDupAction,
			}, ctx)
		}
	}

	for i, tableDef := range tableDefs {
		if tableDef.Pkey.PkeyColName == catalog.FakePrimaryKeyColName {
			continue
		}

		pkPos, pkTyp := getPkPos(tableDef, false)

		lockTarget := &plan.LockTarget{
			TableId:            tableDef.TblId,
			PrimaryColIdxInBat: int32(pkPos),
			PrimaryColTyp:      pkTyp,
			RefreshTsIdxInBat:  -1, //unsupported now
		}

		//if tableDef.Partition != nil {
		//	lockTarget.IsPartitionTable = true
		//	lockTarget.FilterColIdxInBat = int32(partitionIdx)
		//	lockTarget.PartitionTableIds = partTableIDs
		//}

		lastNodeID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_LOCK_OP,
			Children:    []int32{lastNodeID},
			LockTargets: []*plan.LockTarget{lockTarget},
		}, ctx)

		for _, idxTableDef := range idxTableDefs[i] {
			pkPos, pkTyp := getPkPos(idxTableDef, false)

			lockTarget := &plan.LockTarget{
				TableId:            idxTableDef.TblId,
				PrimaryColIdxInBat: int32(pkPos),
				PrimaryColTyp:      pkTyp,
				RefreshTsIdxInBat:  -1, //unsupported now
			}

			//if tableDef.Partition != nil {
			//	lockTarget.IsPartitionTable = true
			//	lockTarget.FilterColIdxInBat = int32(partitionIdx)
			//	lockTarget.PartitionTableIds = partTableIDs
			//}

			lastNodeID = builder.appendNode(&plan.Node{
				NodeType:    plan.Node_LOCK_OP,
				Children:    []int32{lastNodeID},
				LockTargets: []*plan.LockTarget{lockTarget},
			}, ctx)
		}
	}

	for i, tableDef := range tableDefs {
		insertCols := make([]*plan.Expr, 0, len(tableDef.Cols))
		for _, col := range tableDef.Cols {
			if col.Name == catalog.Row_ID {
				continue
			}

			insertCols = append(insertCols, &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: selectNode.BindingTags[0],
						ColPos: int32(colName2Idx[tableDef.Name+"."+col.Name]),
					},
				},
			})
		}

		lastNodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_INSERT,
			Children: []int32{lastNodeID},
			ObjRef:   objRefs[i],
			TableDef: tableDef,
			InsertCtx: &plan.InsertCtx{
				Ref:            objRefs[i],
				IsClusterTable: tableDef.TableType == catalog.SystemClusterRel,
				TableDef:       tableDef,
				//PartitionTableIds:   paritionTableIds,
				//PartitionTableNames: paritionTableNames,
				//PartitionIdx:        int32(partitionIdx),
			},
			InsertDeleteCols: insertCols,
		}, ctx)

		for j, idxTableDef := range idxTableDefs[i] {
			idxInsertCols := make([]*plan.Expr, 0, len(idxTableDef.Cols))
			for _, col := range idxTableDef.Cols {
				if col.Name == catalog.Row_ID {
					continue
				}

				idxInsertCols = append(idxInsertCols, &plan.Expr{
					Typ: col.Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: selectNode.BindingTags[0],
							ColPos: int32(colName2Idx[idxTableDef.Name+"."+col.Name]),
						},
					},
				})
			}

			idxObjRef := DeepCopyObjectRef(idxObjRefs[i][j])
			idxTblDef := DeepCopyTableDef(idxTableDef, true)
			lastNodeID = builder.appendNode(&plan.Node{
				NodeType: plan.Node_INSERT,
				Children: []int32{lastNodeID},
				ObjRef:   idxObjRef,
				TableDef: idxTblDef,
				InsertCtx: &plan.InsertCtx{
					Ref:            idxObjRef,
					IsClusterTable: idxTblDef.TableType == catalog.SystemClusterRel,
					TableDef:       idxTblDef,
					//PartitionTableIds:   paritionTableIds,
					//PartitionTableNames: paritionTableNames,
					//PartitionIdx:        int32(partitionIdx),
				},
				InsertDeleteCols: idxInsertCols,
			}, ctx)
		}
	}

	reCheckifNeedLockWholeTable(builder)

	return lastNodeID, err
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

func (builder *QueryBuilder) initInsertStmt(bindCtx *BindContext, stmt *tree.Insert, info *dmlSelectInfo, tableDef *plan.TableDef) (map[string]int32, error) {
	var err error
	colName2Idx := make(map[string]int32)
	// var uniqueCheckOnAutoIncr string
	var insertColumns []string
	colToIdx := make(map[string]int)
	for i, col := range tableDef.Cols {
		colToIdx[col.Name] = i
	}

	//var ifInsertFromUniqueColMap map[string]bool
	if insertColumns, err = builder.getInsertColsFromStmt(stmt, tableDef); err != nil {
		return nil, err
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
					return nil, moerr.NewWrongValueCountOnRow(builder.GetContext(), j+1)
				}
			}
		} else {
			colCount := len(insertColumns)
			for j, row := range selectImpl.Rows {
				if len(row) != colCount {
					return nil, moerr.NewWrongValueCountOnRow(builder.GetContext(), j+1)
				}
			}
		}

		// example1:insert into a values ();
		// but it does not work at the case:
		// insert into a(a) values (); insert into a values (0),();
		if isAllDefault && stmt.Columns != nil {
			return nil, moerr.NewInvalidInput(builder.GetContext(), "insert values does not match the number of columns")
		}
		info.rootId, err = builder.buildValueScan(isAllDefault, bindCtx, tableDef, selectImpl, insertColumns)
		if err != nil {
			return nil, err
		}

	case *tree.SelectClause:
		astSelect = stmt.Rows

		subCtx := NewBindContext(builder, bindCtx)
		info.rootId, err = builder.bindSelect(astSelect, subCtx, false)
		if err != nil {
			return nil, err
		}
		//ifInsertFromUniqueColMap = make(map[string]bool)

	case *tree.ParenSelect:
		astSelect = selectImpl.Select

		subCtx := NewBindContext(builder, bindCtx)
		info.rootId, err = builder.bindSelect(astSelect, subCtx, false)
		if err != nil {
			return nil, err
		}
		// ifInsertFromUniqueColMap = make(map[string]bool)

	default:
		return nil, moerr.NewInvalidInput(builder.GetContext(), "insert has unknown select statement")
	}

	if err = builder.addBinding(info.rootId, tree.AliasClause{Alias: derivedTableName}, bindCtx); err != nil {
		return nil, err
	}

	lastNode := builder.qry.Nodes[info.rootId]
	if len(insertColumns) != len(lastNode.ProjectList) {
		return nil, moerr.NewInvalidInput(builder.GetContext(), "insert values does not match the number of columns")
	}

	var findTableDefFromSource func(node *plan.Node) *plan.TableDef
	findTableDefFromSource = func(node *plan.Node) *plan.TableDef {
		if node == nil {
			return nil
		}
		if node.NodeType == plan.Node_TABLE_SCAN {
			return node.TableDef
		} else {
			if len(node.Children) == 0 {
				return nil
			}
			return findTableDefFromSource(builder.qry.Nodes[node.Children[0]])
		}
	}

	//getUniqueColMap := func(tableDef *plan.TableDef) map[string]struct{} {
	//	result := make(map[string]struct{})
	//	if tableDef != nil {
	//		if tableDef.Pkey != nil {
	//			for _, name := range tableDef.Pkey.Names {
	//				if name != catalog.FakePrimaryKeyColName {
	//					result[name] = struct{}{}
	//				}
	//			}
	//		}
	//		for _, index := range tableDef.Indexes {
	//			if index.Unique {
	//				for _, name := range index.Parts {
	//					result[name] = struct{}{}
	//				}
	//			}
	//		}
	//	}
	//	return result
	//}
	//var fromUniqueCols map[string]struct{}
	//if ifInsertFromUniqueColMap != nil {
	//	tableDef := findTableDefFromSource(lastNode)
	//	fromUniqueCols = getUniqueColMap(tableDef)
	//}

	tag := builder.qry.Nodes[info.rootId].BindingTags[0]
	info.derivedTableId = info.rootId
	oldProject := append([]*Expr{}, lastNode.ProjectList...)

	insertColToExpr := make(map[string]*Expr)
	for i, column := range insertColumns {
		colIdx := tableDef.Name2ColIndex[column]
		projExpr := &plan.Expr{
			Typ: oldProject[i].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: tag,
					ColPos: int32(i),
				},
			},
		}
		if tableDef.Cols[colIdx].Typ.Id == int32(types.T_enum) {
			projExpr, err = funcCastForEnumType(builder.GetContext(), projExpr, tableDef.Cols[colIdx].Typ)
			if err != nil {
				return nil, err
			}
		} else {
			projExpr, err = forceCastExpr(builder.GetContext(), projExpr, tableDef.Cols[colIdx].Typ)
			if err != nil {
				return nil, err
			}
		}
		insertColToExpr[column] = projExpr
		//if ifInsertFromUniqueColMap != nil {
		//	col := lastNode.ProjectList[i].GetCol()
		//	if col != nil {
		//		if _, ok := fromUniqueCols[col.Name]; ok {
		//			ifInsertFromUniqueColMap[column] = true
		//		}
		//	}
		//}
	}

	// have tables : t1(a default 0, b int, pk(a,b)) ,  t2(j int,k int)
	// rewrite 'insert into t1 select * from t2' to
	// select 'select _t.j, _t.k from (select * from t2) _t(j,k)
	// --------
	// rewrite 'insert into t1(b) values (1)' to
	// select 'select 0, _t.column_0 from (select * from values (1)) _t(column_0)
	projectList := make([]*Expr, 0, len(tableDef.Cols))
	for i, col := range tableDef.Cols {
		if oldExpr, exists := insertColToExpr[col.Name]; exists {
			colName2Idx[tableDef.Name+"."+col.Name] = int32(len(projectList))
			projectList = append(projectList, oldExpr)
			// if col.Typ.AutoIncr {
			// if _, ok := pkCols[col.Name]; ok {
			// 	uniqueCheckOnAutoIncr, err = builder.compCtx.GetDbLevelConfig(dbName, "unique_check_on_autoincr")
			// 	if err != nil {
			// 		return false, nil, err
			// 	}
			// 	if uniqueCheckOnAutoIncr == "Error" {
			// 		return false, nil, moerr.NewInvalidInput(builder.GetContext(), "When unique_check_on_autoincr is set to error, insertion of the specified value into auto-incr pk column is not allowed.")
			// 	}
			// }
			// }
		} else if col.Name == catalog.Row_ID {
			continue
		} else if col.Name == catalog.CPrimaryKeyColName {
			args := make([]*plan.Expr, len(tableDef.Pkey.Names))

			for k, part := range tableDef.Pkey.Names {
				args[k] = DeepCopyExpr(insertColToExpr[part])
			}

			colName2Idx[tableDef.Name+"."+col.Name] = int32(len(projectList))
			pkExpr, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", args)
			projectList = append(projectList, pkExpr)
		} else if tableDef.ClusterBy != nil && col.Name == tableDef.ClusterBy.Name {
			names := util.SplitCompositeClusterByColumnName(tableDef.ClusterBy.Name)
			args := make([]*plan.Expr, len(names))

			for k, part := range names {
				args[k] = DeepCopyExpr(insertColToExpr[part])
			}

			colName2Idx[tableDef.Name+"."+col.Name] = int32(len(projectList))
			pkExpr, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial_full", args)
			projectList = append(projectList, pkExpr)
		} else {
			defExpr, err := getDefaultExpr(builder.GetContext(), col)
			if err != nil {
				return nil, err
			}

			projectList = append(projectList, defExpr)
		}

		colName2Idx[tableDef.Name+"."+col.Name] = int32(i)
	}

	// append ProjectNode
	projectCtx := NewBindContext(builder, bindCtx)
	lastTag := builder.genNewTag()
	info.rootId = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		ProjectList: projectList,
		Children:    []int32{info.rootId},
		BindingTags: []int32{lastTag},
	}, projectCtx)

	info.projectList = make([]*Expr, 0, len(projectList))
	info.derivedTableId = info.rootId
	for i, e := range projectList {
		info.projectList = append(info.projectList, &plan.Expr{
			Typ: e.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: lastTag,
					ColPos: int32(i),
				},
			},
		})
	}
	info.idx = int32(len(info.projectList))

	return colName2Idx, nil
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
	projectList := make([]*Expr, colCount)
	bat := batch.NewWithSize(len(colNames))

	for i, colName := range colNames {
		col := tableDef.Cols[tableDef.Name2ColIndex[colName]]
		colTyp := makeTypeByPlan2Type(col.Typ)
		vec := vector.NewVec(colTyp)
		if err := vector.AppendMultiBytes(vec, nil, true, len(stmt.Rows), proc.Mp()); err != nil {
			bat.Clean(proc.Mp())
			return 0, err
		}
		bat.Vecs[i] = vec
		targetTyp := &plan.Expr{
			Typ: col.Typ,
			Expr: &plan.Expr_T{
				T: &plan.TargetType{},
			},
		}
		var defExpr *Expr
		if isAllDefault {
			defExpr, err := getDefaultExpr(builder.GetContext(), col)
			if err != nil {
				return 0, err
			}
			defExpr, err = forceCastExpr2(builder.GetContext(), defExpr, colTyp, targetTyp)
			if err != nil {
				return 0, err
			}
			for j := range stmt.Rows {
				rowsetData.Cols[i].Data = append(rowsetData.Cols[i].Data, &plan.RowsetExpr{
					Pos:    -1,
					RowPos: int32(j),
					Expr:   defExpr,
				})
			}
		} else {
			binder := NewDefaultBinder(builder.GetContext(), nil, nil, col.Typ, nil)
			binder.builder = builder
			for j, r := range stmt.Rows {
				if nv, ok := r[i].(*tree.NumVal); ok {
					canInsert, err := util.SetInsertValue(proc, nv, vec)
					if err != nil {
						bat.Clean(proc.Mp())
						return 0, err
					}
					if canInsert {
						continue
					}
				}

				if _, ok := r[i].(*tree.DefaultVal); ok {
					defExpr, err = getDefaultExpr(builder.GetContext(), col)
					if err != nil {
						bat.Clean(proc.Mp())
						return 0, err
					}
				} else if nv, ok := r[i].(*tree.ParamExpr); ok {
					if !builder.isPrepareStatement {
						bat.Clean(proc.Mp())
						return 0, moerr.NewInvalidInput(builder.GetContext(), "only prepare statement can use ? expr")
					}
					rowsetData.Cols[i].Data = append(rowsetData.Cols[i].Data, &plan.RowsetExpr{
						RowPos: int32(j),
						Pos:    int32(nv.Offset),
						Expr: &plan.Expr{
							Typ: constTextType,
							Expr: &plan.Expr_P{
								P: &plan.ParamRef{
									Pos: int32(nv.Offset),
								},
							},
						},
					})
					continue
				} else {
					defExpr, err = binder.BindExpr(r[i], 0, true)
					if err != nil {
						bat.Clean(proc.Mp())
						return 0, err
					}
					if col.Typ.Id == int32(types.T_enum) {
						defExpr, err = funcCastForEnumType(builder.GetContext(), defExpr, col.Typ)
						if err != nil {
							bat.Clean(proc.Mp())
							return 0, err
						}
					}
				}
				defExpr, err = forceCastExpr2(builder.GetContext(), defExpr, colTyp, targetTyp)
				if err != nil {
					return 0, err
				}
				if nv, ok := r[i].(*tree.ParamExpr); ok {
					if !builder.isPrepareStatement {
						bat.Clean(proc.Mp())
						return 0, moerr.NewInvalidInput(builder.GetContext(), "only prepare statement can use ? expr")
					}
					rowsetData.Cols[i].Data = append(rowsetData.Cols[i].Data, &plan.RowsetExpr{
						RowPos: int32(j),
						Pos:    int32(nv.Offset),
						Expr:   defExpr,
					})
					continue
				}
				rowsetData.Cols[i].Data = append(rowsetData.Cols[i].Data, &plan.RowsetExpr{
					Pos:    -1,
					RowPos: int32(j),
					Expr:   defExpr,
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

	bat.SetRowCount(len(stmt.Rows))
	rowsetData.RowCount = int32(len(stmt.Rows))
	nodeId, _ := uuid.NewV7()
	scanNode := &plan.Node{
		NodeType:    plan.Node_VALUE_SCAN,
		RowsetData:  rowsetData,
		TableDef:    valueScanTableDef,
		BindingTags: []int32{lastTag},
		Uuid:        nodeId[:],
	}
	if builder.isPrepareStatement {
		proc.SetPrepareBatch(bat)
	} else {
		proc.SetValueScanBatch(nodeId, bat)
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
