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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func (builder *QueryBuilder) bindDelete(stmt *tree.Delete, ctx *BindContext) (int32, error) {
	if len(stmt.Tables) != 1 {
		return 0, moerr.NewUnsupportedDML(builder.GetContext(), "delete from multiple tables")
	}

	//FIXME: optimize truncate table?
	//if stmt.Where == nil && stmt.Limit == nil {
	//	return 0, moerr.NewUnsupportedDML(builder.GetContext(), "rewrite to truncate table")
	//}

	aliasMap := make(map[string][2]string)
	for _, tbl := range stmt.TableRefs {
		getAliasToName(builder.compCtx, tbl, "", aliasMap)
	}

	tblInfo, err := getDmlTableInfo(builder.compCtx, stmt.Tables, stmt.With, aliasMap, "delete")
	if err != nil {
		return 0, err
	}

	var selectList []tree.SelectExpr
	colName2Idx := make([]map[string]int, len(stmt.Tables))

	getResolveExpr := func(alias string) {
		defIdx := tblInfo.alias[alias]
		colName2Idx[defIdx] = make(map[string]int)
		for _, col := range tblInfo.tableDefs[defIdx].Cols {
			colName2Idx[defIdx][col.Name] = len(selectList)
			selectExpr := tree.NewUnresolvedName(tree.NewCStr(alias, ctx.lower), tree.NewCStr(col.Name, 1))
			selectList = append(selectList, tree.SelectExpr{
				Expr: selectExpr,
			})
		}
	}

	for _, tbl := range stmt.Tables {
		if aliasTbl, ok := tbl.(*tree.AliasedTableExpr); ok {
			alias := string(aliasTbl.As.Alias)
			if alias != "" {
				getResolveExpr(alias)
			} else {
				astTbl := aliasTbl.Expr.(*tree.TableName)
				getResolveExpr(string(astTbl.ObjectName))
			}
		} else if astTbl, ok := tbl.(*tree.TableName); ok {
			getResolveExpr(string(astTbl.ObjectName))
		}
	}

	fromTables := &tree.From{}
	if stmt.TableRefs != nil {
		fromTables.Tables = stmt.TableRefs
	} else {
		fromTables.Tables = stmt.Tables
	}

	astSelect := &tree.Select{
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

	selectCtx := NewBindContext(builder, ctx)
	lastNodeID, err := builder.bindSelect(astSelect, selectCtx, false)
	if err != nil {
		return 0, nil
	}

	selectNode := builder.qry.Nodes[lastNodeID]
	if selectNode.NodeType != plan.Node_PROJECT {
		return 0, moerr.NewUnsupportedDML(builder.GetContext(), "malformed select node")
	}

	idxScanNodes := make([][]*plan.Node, len(tblInfo.tableDefs))

	for i, tableDef := range tblInfo.tableDefs {
		scanNode := builder.qry.Nodes[builder.name2ScanNode[tableDef.Name]]
		idxDefs := tableDef.Indexes
		idxScanNodes[i] = make([]*plan.Node, len(idxDefs))

		for j, idxDef := range idxDefs {
			if !idxDef.TableExist {
				continue
			}

			if !catalog.IsRegularIndexAlgo(idxDef.IndexAlgo) {
				return 0, moerr.NewUnsupportedDML(builder.GetContext(), "have vector index table")
			}

			idxObjRef, idxTableDef := builder.compCtx.Resolve(tblInfo.objRef[0].SchemaName, idxDef.IndexTableName, nil)
			idxTag := builder.genNewTag()
			builder.addNameByColRef(idxTag, idxTableDef)

			idxScanNodes[i][j] = &plan.Node{
				NodeType:     plan.Node_TABLE_SCAN,
				TableDef:     idxTableDef,
				ObjRef:       idxObjRef,
				BindingTags:  []int32{idxTag},
				ScanSnapshot: scanNode.ScanSnapshot,
			}
			idxTableNodeID := builder.appendNode(idxScanNodes[i][j], ctx)

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

			var leftExpr *plan.Expr
			if len(idxDef.Parts) == 1 && idxDef.Unique {
				leftExpr = &plan.Expr{
					Typ: pkTyp,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: selectNode.BindingTags[0],
							ColPos: int32(colName2Idx[i][idxDef.Parts[0]]),
						},
					},
				}
			} else {
				argsLen := len(idxDef.Parts)
				if !idxDef.Unique {
					argsLen++
				}

				args := make([]*plan.Expr, argsLen)

				for k, part := range idxDef.Parts {
					colPos := int32(colName2Idx[i][part])
					args[k] = &plan.Expr{
						Typ: selectNode.ProjectList[colPos].Typ,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: selectNode.BindingTags[0],
								ColPos: colPos,
							},
						},
					}
				}

				if !idxDef.Unique {
					colPos := int32(colName2Idx[i][tableDef.Pkey.PkeyColName])
					args[len(idxDef.Parts)] = &plan.Expr{
						Typ: selectNode.ProjectList[colPos].Typ,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: selectNode.BindingTags[0],
								ColPos: colPos,
							},
						},
					}
				}

				leftExpr, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial_full", args)
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
			}, ctx)
		}
	}

	for i, tableDef := range tblInfo.tableDefs {
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

		for _, idxNode := range idxScanNodes[i] {
			if idxNode == nil {
				continue
			}

			pkPos, pkTyp := getPkPos(idxNode.TableDef, false)

			lockTarget := &plan.LockTarget{
				TableId:            idxNode.TableDef.TblId,
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

	for i, tableDef := range tblInfo.tableDefs {
		pkPos := int32(colName2Idx[i][tableDef.Pkey.PkeyColName])
		rowIDPos := int32(colName2Idx[i][catalog.Row_ID])
		lastNodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_DELETE,
			Children: []int32{lastNodeID},
			// ProjectList: getProjectionByLastNode(builder, lastNodeId),
			DeleteCtx: &plan.DeleteCtx{
				TableDef: DeepCopyTableDef(tableDef, true),
				//RowIdIdx:            int32(delNodeInfo.deleteIndex),
				Ref: DeepCopyObjectRef(tblInfo.objRef[i]),
				//AddAffectedRows:     delNodeInfo.addAffectedRows,
				//IsClusterTable:      delNodeInfo.IsClusterTable,
				//PartitionTableIds:   delNodeInfo.partTableIDs,
				//PartitionTableNames: delNodeInfo.partTableNames,
				//PartitionIdx:        int32(delNodeInfo.partitionIdx),
			},
			InsertDeleteCols: []*plan.Expr{
				{
					Typ: selectNode.ProjectList[pkPos].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: selectNode.BindingTags[0],
							ColPos: pkPos,
						},
					},
				},
				{
					Typ: selectNode.ProjectList[rowIDPos].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: selectNode.BindingTags[0],
							ColPos: rowIDPos,
						},
					},
				},
			},
		}, ctx)

		for _, idxNode := range idxScanNodes[i] {
			if idxNode == nil {
				continue
			}

			pkPos := int32(idxNode.TableDef.Name2ColIndex[idxNode.TableDef.Pkey.PkeyColName])
			rowIDPos := int32(idxNode.TableDef.Name2ColIndex[catalog.Row_ID])
			lastNodeID = builder.appendNode(&plan.Node{
				NodeType: plan.Node_DELETE,
				Children: []int32{lastNodeID},
				DeleteCtx: &plan.DeleteCtx{
					TableDef: DeepCopyTableDef(idxNode.TableDef, true),
					Ref:      DeepCopyObjectRef(idxNode.ObjRef),
					//PrimaryKeyIdx:       int32(delNodeInfo.pkPos),
					//CanTruncate:         canTruncate,
					//AddAffectedRows:     delNodeInfo.addAffectedRows,
					//IsClusterTable:      delNodeInfo.IsClusterTable,
					//PartitionTableIds:   delNodeInfo.partTableIDs,
					//PartitionTableNames: delNodeInfo.partTableNames,
					//PartitionIdx:        int32(delNodeInfo.partitionIdx),
				},
				InsertDeleteCols: []*plan.Expr{
					{
						Typ: idxNode.TableDef.Cols[pkPos].Typ,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: idxNode.BindingTags[0],
								ColPos: pkPos,
							},
						},
					},
					{
						Typ: idxNode.TableDef.Cols[rowIDPos].Typ,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: idxNode.BindingTags[0],
								ColPos: rowIDPos,
							},
						},
					},
				},
			}, ctx)
		}
	}

	reCheckifNeedLockWholeTable(builder)

	return lastNodeID, err
}

func (builder *QueryBuilder) updateLocksOnDemand(nodeID int32) {
	lockService := builder.compCtx.GetProcess().Base.LockService
	if lockService == nil {
		// MockCompilerContext
		return
	}
	lockconfig := lockService.GetConfig()

	node := builder.qry.Nodes[nodeID]
	if node.NodeType != plan.Node_LOCK_OP {
		for _, childID := range node.Children {
			builder.updateLocksOnDemand(childID)
		}
	} else if !node.LockTargets[0].LockTable && node.Stats.Outcnt > float64(lockconfig.MaxLockRowCount) {
		node.LockTargets[0].LockTable = true
		logutil.Infof("Row lock upgraded to table lock for SQL : %s", builder.compCtx.GetRootSql())
		logutil.Infof("the outcnt stats is %f", node.Stats.Outcnt)
	}
}

/*
// makeOneDeletePlan
// lock -> delete
func makeOneDeletePlan0(
	builder *QueryBuilder,
	bindCtx *BindContext,
	lastNodeId int32,
	delNodeInfo *deleteNodeInfo,
	canTruncate bool,
) (int32, error) {
	var truncateTable *plan.TruncateTable
	if canTruncate {
		tableDef := delNodeInfo.tableDef
		truncateTable = &plan.TruncateTable{
			Table:               tableDef.Name,
			TableId:             tableDef.TblId,
			Database:            delNodeInfo.objRef.SchemaName,
			IndexTableNames:     delNodeInfo.indexTableNames,
			PartitionTableNames: delNodeInfo.partTableNames,
			ForeignTbl:          delNodeInfo.foreignTbl,
			ClusterTable: &plan.ClusterTable{
				IsClusterTable: util.TableIsClusterTable(tableDef.GetTableType()),
			},
			IsDelete: true,
		}
		truncateTable.Table = tableDef.Name
		truncateTable.TableId = tableDef.TblId
		truncateTable.Database = delNodeInfo.objRef.SchemaName
		truncateTable.IndexTableNames = delNodeInfo.indexTableNames
		truncateTable.PartitionTableNames = delNodeInfo.partTableNames
		truncateTable.ForeignTbl = delNodeInfo.foreignTbl
		truncateTable.ClusterTable = &plan.ClusterTable{
			IsClusterTable: util.TableIsClusterTable(tableDef.GetTableType()),
		}
		truncateTable.IsDelete = true
	}
	// append delete node
	deleteNode := &Node{
		NodeType: plan.Node_DELETE,
		Children: []int32{lastNodeId},
		// ProjectList: getProjectionByLastNode(builder, lastNodeId),
		DeleteCtx: &plan.DeleteCtx{
			TableDef:            delNodeInfo.tableDef,
			RowIdIdx:            int32(delNodeInfo.deleteIndex),
			Ref:                 delNodeInfo.objRef,
			CanTruncate:         canTruncate,
			AddAffectedRows:     delNodeInfo.addAffectedRows,
			IsClusterTable:      delNodeInfo.IsClusterTable,
			PartitionTableIds:   delNodeInfo.partTableIDs,
			PartitionTableNames: delNodeInfo.partTableNames,
			PartitionIdx:        int32(delNodeInfo.partitionIdx),
			PrimaryKeyIdx:       int32(delNodeInfo.pkPos),
			TruncateTable:       truncateTable,
		},
	}
	lastNodeId = builder.appendNode(deleteNode, bindCtx)

	return lastNodeId, nil
}

func (builder *QueryBuilder) appendDeletePlan(scanNode *plan.Node, delCtx *dmlPlanCtx, bindCtx *BindContext, canTruncate bool) error {
	builder.deleteNode[delCtx.tableDef.TblId] = builder.qry.Steps[delCtx.sourceStep]
	//isUpdate := delCtx.updateColLength > 0

	// delete unique/secondary index table
	// Refer to this PR:https://github.com/matrixorigin/matrixone/pull/12093
	// we have build SK using UK code path. So we might see UK in function signature even thought it could be for
	// both UK and SK. To handle SK case, we will have flags to indicate if it's UK or SK.
	hasUniqueKey := haveUniqueKey(scanNode.TableDef)
	hasSecondaryKey := haveSecondaryKey(scanNode.TableDef)

	accountId, err := builder.compCtx.GetAccountId()
	if err != nil {
		return err
	}

	enabled, err := IsForeignKeyChecksEnabled(builder.compCtx)
	if err != nil {
		return err
	}

	if enabled && len(delCtx.tableDef.RefChildTbls) > 0 ||
		delCtx.tableDef.ViewSql != nil ||
		(util.TableIsClusterTable(delCtx.tableDef.GetTableType()) && accountId != catalog.System_Account) ||
		delCtx.objRef.PubInfo != nil {
		canTruncate = false
	}

	if (hasUniqueKey || hasSecondaryKey) && !canTruncate {
		typMap := make(map[string]plan.Type)
		posMap := make(map[string]int)
		colMap := make(map[string]*ColDef)
		for idx, col := range delCtx.tableDef.Cols {
			posMap[col.Name] = idx
			typMap[col.Name] = col.Typ
			colMap[col.Name] = col
		}
		multiTableIndexes := make(map[string]*MultiTableIndex)
		for idx, indexdef := range delCtx.tableDef.Indexes {
			if isUpdate {
				pkeyName := delCtx.tableDef.Pkey.PkeyColName

				// Check if primary key is being updated.
				isPrimaryKeyUpdated := func() bool {
					if pkeyName == catalog.CPrimaryKeyColName {
						// Handle compound primary key.
						for _, pkPartColName := range delCtx.tableDef.Pkey.Names {
							if _, exists := delCtx.updateColPosMap[pkPartColName]; exists || colMap[pkPartColName].OnUpdate != nil {
								return true
							}
						}
					} else if pkeyName == catalog.FakePrimaryKeyColName {
						// Handle programmatically generated primary key.
						if _, exists := delCtx.updateColPosMap[pkeyName]; exists || colMap[pkeyName].OnUpdate != nil {
							return true
						}
					} else {
						// Handle single primary key.
						if _, exists := delCtx.updateColPosMap[pkeyName]; exists || colMap[pkeyName].OnUpdate != nil {
							return true
						}
					}
					return false
				}

				// Check if secondary key is being updated.
				isSecondaryKeyUpdated := func() bool {
					for _, colName := range indexdef.Parts {
						resolvedColName := catalog.ResolveAlias(colName)
						if colIdx, ok := posMap[resolvedColName]; ok {
							col := delCtx.tableDef.Cols[colIdx]
							if _, exists := delCtx.updateColPosMap[resolvedColName]; exists || col.OnUpdate != nil {
								return true
							}
						}
					}
					return false
				}

				if !isPrimaryKeyUpdated() && !isSecondaryKeyUpdated() {
					continue
				}
			}
			if indexdef.TableExist && catalog.IsRegularIndexAlgo(indexdef.IndexAlgo) {
				var isUk = indexdef.Unique
				var isSK = !isUk && catalog.IsRegularIndexAlgo(indexdef.IndexAlgo)

				uniqueObjRef, uniqueTableDef := builder.compCtx.Resolve(delCtx.objRef.SchemaName, indexdef.IndexTableName, nil)
				if uniqueTableDef == nil {
					return moerr.NewNoSuchTable(builder.GetContext(), delCtx.objRef.SchemaName, indexdef.IndexTableName)
				}
				var lastNodeId int32
				var err error
				var uniqueDeleteIdx int
				var uniqueTblPkPos int
				var uniqueTblPkTyp Type

				if delCtx.isDeleteWithoutFilters {
					lastNodeId, err = appendDeleteIndexTablePlanWithoutFilters(builder, bindCtx, uniqueObjRef, uniqueTableDef)
					uniqueDeleteIdx = getRowIdPos(uniqueTableDef)
					uniqueTblPkPos, uniqueTblPkTyp = getPkPos(uniqueTableDef, false)
				} else {
					lastNodeId, err = appendDeleteIndexTablePlan(builder, bindCtx, uniqueObjRef, uniqueTableDef, indexdef, typMap, posMap, lastNodeId, isUk)
					uniqueDeleteIdx = len(delCtx.tableDef.Cols) + delCtx.updateColLength
					uniqueTblPkPos = uniqueDeleteIdx + 1
					uniqueTblPkTyp = uniqueTableDef.Cols[0].Typ
				}
				if err != nil {
					return err
				}
				if false {
					// do it like simple update
					newSourceStep := builder.appendStep(lastNodeId)
					// delete uk plan
					{
						delNodeInfo := makeDeleteNodeInfo(builder.compCtx, uniqueObjRef, uniqueTableDef, uniqueDeleteIdx, -1, false, uniqueTblPkPos, uniqueTblPkTyp, delCtx.lockTable, delCtx.partitionInfos)
						lastNodeId, err = makeOneDeletePlan(builder, bindCtx, lastNodeId, delNodeInfo, isUk, isSK, false)
						putDeleteNodeInfo(delNodeInfo)
						if err != nil {
							return err
						}
						builder.appendStep(lastNodeId)
					}
					// insert uk plan
					{
						lastProject := builder.qry.Nodes[lastNodeId].ProjectList
						projectProjection := make([]*Expr, len(delCtx.tableDef.Cols))
						for j, uCols := range delCtx.tableDef.Cols {
							if nIdx, ok := delCtx.updateColPosMap[uCols.Name]; ok {
								projectProjection[j] = lastProject[nIdx]
							} else {
								if uCols.Name == catalog.Row_ID {
									// replace the origin table's row_id with unique table's row_id
									projectProjection[j] = lastProject[len(lastProject)-2]
								} else {
									projectProjection[j] = lastProject[j]
								}
							}
						}
						projectNode := &Node{
							NodeType:    plan.Node_PROJECT,
							Children:    []int32{lastNodeId},
							ProjectList: projectProjection,
						}
						lastNodeId = builder.appendNode(projectNode, bindCtx)
						preUKStep, err := appendPreInsertPlan(builder, bindCtx, delCtx.tableDef, lastNodeId, idx, true, uniqueTableDef, isUk)
						if err != nil {
							return err
						}

						insertUniqueTableDef := DeepCopyTableDef(uniqueTableDef, false)
						for _, col := range uniqueTableDef.Cols {
							if col.Name != catalog.Row_ID {
								insertUniqueTableDef.Cols = append(insertUniqueTableDef.Cols, DeepCopyColDef(col))
							}
						}
						_checkPKDupForHiddenIndexTable := indexdef.Unique // only check PK uniqueness for UK. SK will not check PK uniqueness.
						updateColLength := 1
						addAffectedRows := false
						isFkRecursionCall := false
						updatePkCol := true
						ifExistAutoPkCol := false
						ifInsertFromUnique := false
						var pkFilterExprs []*Expr
						var partitionExpr *Expr
						var indexSourceColTypes []*Type
						var fuzzymessage *OriginTableMessageForFuzzy
						err = makeOneInsertPlan(builder.compCtx, builder, bindCtx, uniqueObjRef, insertUniqueTableDef,
							updateColLength, preUKStep, addAffectedRows, isFkRecursionCall, updatePkCol,
							pkFilterExprs, partitionExpr, ifExistAutoPkCol, _checkPKDupForHiddenIndexTable, ifInsertFromUnique,
							indexSourceColTypes, fuzzymessage)
						if err != nil {
							return err
						}
					}
				} else {
					// it's more simple for delete hidden unique table .so we append nodes after the plan. not recursive call buildDeletePlans
					delNodeInfo := makeDeleteNodeInfo(builder.compCtx, uniqueObjRef, uniqueTableDef, uniqueDeleteIdx, -1, false, uniqueTblPkPos, uniqueTblPkTyp, delCtx.lockTable, delCtx.partitionInfos)
					lastNodeId, err = makeOneDeletePlan(builder, bindCtx, lastNodeId, delNodeInfo, isUk, isSK, false)
					putDeleteNodeInfo(delNodeInfo)
					if err != nil {
						return err
					}
					builder.appendStep(lastNodeId)
				}
			} else if indexdef.TableExist && catalog.IsIvfIndexAlgo(indexdef.IndexAlgo) {
				// IVF indexDefs are aggregated and handled later
				if _, ok := multiTableIndexes[indexdef.IndexName]; !ok {
					multiTableIndexes[indexdef.IndexName] = &MultiTableIndex{
						IndexAlgo: catalog.ToLower(indexdef.IndexAlgo),
						IndexDefs: make(map[string]*IndexDef),
					}
				}
				multiTableIndexes[indexdef.IndexName].IndexDefs[catalog.ToLower(indexdef.IndexAlgoTableType)] = indexdef
			} else if indexdef.TableExist && catalog.IsMasterIndexAlgo(indexdef.IndexAlgo) {
				// Used by pre-insert vector index.
				masterObjRef, masterTableDef := builder.compCtx.Resolve(delCtx.objRef.SchemaName, indexdef.IndexTableName, nil)
				if masterTableDef == nil {
					return moerr.NewNoSuchTable(builder.GetContext(), delCtx.objRef.SchemaName, indexdef.IndexName)
				}

				var lastNodeId int32
				var err error
				var masterDeleteIdx int
				var masterTblPkPos int
				var masterTblPkTyp Type

				if delCtx.isDeleteWithoutFilters {
					lastNodeId, err = appendDeleteIndexTablePlanWithoutFilters(builder, bindCtx, masterObjRef, masterTableDef)
					masterDeleteIdx = getRowIdPos(masterTableDef)
					masterTblPkPos, masterTblPkTyp = getPkPos(masterTableDef, false)
				} else {
					lastNodeId, err = appendDeleteMasterTablePlan(builder, bindCtx, masterObjRef, masterTableDef, lastNodeId, delCtx.tableDef, indexdef, typMap, posMap)
					masterDeleteIdx = len(delCtx.tableDef.Cols) + delCtx.updateColLength
					masterTblPkPos = masterDeleteIdx + 1
					masterTblPkTyp = masterTableDef.Cols[0].Typ
				}

				if err != nil {
					return err
				}

				if false {
					// do it like simple update
					newSourceStep := builder.appendStep(lastNodeId)
					// delete uk plan
					{
						delNodeInfo := makeDeleteNodeInfo(builder.compCtx, masterObjRef, masterTableDef, masterDeleteIdx, -1, false, masterTblPkPos, masterTblPkTyp, delCtx.lockTable, delCtx.partitionInfos)
						lastNodeId, err = makeOneDeletePlan(builder, bindCtx, lastNodeId, delNodeInfo, false, true, false)
						putDeleteNodeInfo(delNodeInfo)
						if err != nil {
							return err
						}
						builder.appendStep(lastNodeId)
					}
					// insert master sk plan
					{
						genLastNodeIdFn := func() int32 {
							//TODO: verify if this will cause memory leak.
							lastProject := builder.qry.Nodes[lastNodeId].ProjectList
							projectProjection := make([]*Expr, len(delCtx.tableDef.Cols))
							for j, uCols := range delCtx.tableDef.Cols {
								if nIdx, ok := delCtx.updateColPosMap[uCols.Name]; ok {
									projectProjection[j] = lastProject[nIdx]
								} else {
									if uCols.Name == catalog.Row_ID {
										// NOTE:
										// 1. In the case of secondary index, we are reusing the row_id values
										// that were deleted.
										// 2. But in the master index case, we are using the row_id values from
										// the original table. Row_id associated with say 2 rows (one row for each column)
										// in the index table will be same value (ie that from the original table).
										// So, when we do UNION it automatically removes the duplicate values.
										// ie
										//  <"a_arjun_1",1, 1> -->  (select serial_full("0", a, c),__mo_pk_key, __mo_row_id)
										//  <"a_arjun_1",1, 1>
										//  <"b_sunil_1",1, 1> -->  (select serial_full("2", b,c),__mo_pk_key, __mo_row_id)
										//  <"b_sunil_1",1, 1>
										//  when we use UNION, we remove the duplicate values
										// 3. RowID is added here: https://github.com/arjunsk/matrixone/blob/d7db178e1c7298e2a3e4f99e7292425a7ef0ef06/pkg/vm/engine/disttae/txn.go#L95
										// TODO: verify this with Feng, Ouyuanning and Qingx (not reusing the row_id)
										projectProjection[j] = lastProject[j]
									} else {
										projectProjection[j] = lastProject[j]
									}
								}
							}
							projectNode := &Node{
								NodeType:    plan.Node_PROJECT,
								Children:    []int32{newLastNodeId},
								ProjectList: projectProjection,
							}
							return builder.appendNode(projectNode, bindCtx)
						}

						preUKStep, err := appendPreInsertSkMasterPlan(builder, bindCtx, delCtx.tableDef, idx, true, masterTableDef, genLastNodeIdFn)
						if err != nil {
							return err
						}

						insertEntriesTableDef := DeepCopyTableDef(masterTableDef, false)
						for _, col := range masterTableDef.Cols {
							if col.Name != catalog.Row_ID {
								insertEntriesTableDef.Cols = append(insertEntriesTableDef.Cols, DeepCopyColDef(col))
							}
						}
						updateColLength := 1
						addAffectedRows := false
						isFkRecursionCall := false
						updatePkCol := true
						ifExistAutoPkCol := false
						ifCheckPkDup := false
						ifInsertFromUnique := false
						var pkFilterExprs []*Expr
						var partitionExpr *Expr
						var indexSourceColTypes []*Type
						var fuzzymessage *OriginTableMessageForFuzzy
						err = makeOneInsertPlan(builder.compCtx, builder, bindCtx, masterObjRef, insertEntriesTableDef,
							updateColLength, preUKStep, addAffectedRows, isFkRecursionCall, updatePkCol,
							pkFilterExprs, partitionExpr, ifExistAutoPkCol, ifCheckPkDup, ifInsertFromUnique,
							indexSourceColTypes, fuzzymessage)

						if err != nil {
							return err
						}
					}

				} else {
					// it's more simple for delete hidden unique table .so we append nodes after the plan. not recursive call buildDeletePlans
					delNodeInfo := makeDeleteNodeInfo(builder.compCtx, masterObjRef, masterTableDef, masterDeleteIdx, -1, false, masterTblPkPos, masterTblPkTyp, delCtx.lockTable, delCtx.partitionInfos)
					lastNodeId, err = makeOneDeletePlan(builder, bindCtx, lastNodeId, delNodeInfo, false, true, false)
					putDeleteNodeInfo(delNodeInfo)
					if err != nil {
						return err
					}
					builder.appendStep(lastNodeId)
				}

			}
		}

		for _, multiTableIndex := range multiTableIndexes {
			switch multiTableIndex.IndexAlgo {
			case catalog.MoIndexIvfFlatAlgo.ToString():

				// Used by pre-insert vector index.
				var idxRefs = make([]*ObjectRef, 3)
				var idxTableDefs = make([]*TableDef, 3)
				// TODO: plan node should hold snapshot and account info
				//idxRefs[0], idxTableDefs[0] = ctx.Resolve(delCtx.objRef.SchemaName, multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata].IndexTableName, timestamp.Timestamp{})
				//idxRefs[1], idxTableDefs[1] = ctx.Resolve(delCtx.objRef.SchemaName, multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Centroids].IndexTableName, timestamp.Timestamp{})
				//idxRefs[2], idxTableDefs[2] = ctx.Resolve(delCtx.objRef.SchemaName, multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Entries].IndexTableName, timestamp.Timestamp{})

				idxRefs[0], idxTableDefs[0] = builder.compCtx.Resolve(delCtx.objRef.SchemaName, multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata].IndexTableName, nil)
				idxRefs[1], idxTableDefs[1] = builder.compCtx.Resolve(delCtx.objRef.SchemaName, multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Centroids].IndexTableName, nil)
				idxRefs[2], idxTableDefs[2] = builder.compCtx.Resolve(delCtx.objRef.SchemaName, multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Entries].IndexTableName, nil)

				entriesObjRef, entriesTableDef := idxRefs[2], idxTableDefs[2]
				if entriesTableDef == nil {
					return moerr.NewNoSuchTable(builder.GetContext(), delCtx.objRef.SchemaName, multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Entries].IndexName)
				}

				var lastNodeId int32
				var err error
				var entriesDeleteIdx int
				var entriesTblPkPos int
				var entriesTblPkTyp Type

				if delCtx.isDeleteWithoutFilters {
					lastNodeId, err = appendDeleteIndexTablePlanWithoutFilters(builder, bindCtx, entriesObjRef, entriesTableDef)
					entriesDeleteIdx = getRowIdPos(entriesTableDef)
					entriesTblPkPos, entriesTblPkTyp = getPkPos(entriesTableDef, false)
				} else {
					lastNodeId, err = appendDeleteIvfTablePlan(builder, bindCtx, entriesObjRef, entriesTableDef, lastNodeId, delCtx.tableDef)
					entriesDeleteIdx = len(delCtx.tableDef.Cols) + delCtx.updateColLength // eg:- <id, embedding, row_id, <... update_col> > + 0/1
					entriesTblPkPos = entriesDeleteIdx + 1                                // this is the compound primary key of the entries table
					entriesTblPkTyp = entriesTableDef.Cols[4].Typ                         // 4'th column is the compound primary key <version,id, org_pk,org_embedding, cp_pk, row_id>
				}

				if err != nil {
					return err
				}

				if false {
					// do it like simple update
					newSourceStep := builder.appendStep(lastNodeId)
					// delete uk plan
					{
						delNodeInfo := makeDeleteNodeInfo(builder.compCtx, entriesObjRef, entriesTableDef, entriesDeleteIdx, -1, false, entriesTblPkPos, entriesTblPkTyp, delCtx.lockTable, delCtx.partitionInfos)
						lastNodeId, err = makeOneDeletePlan(builder, bindCtx, lastNodeId, delNodeInfo, false, true, false)
						putDeleteNodeInfo(delNodeInfo)
						if err != nil {
							return err
						}
						builder.appendStep(lastNodeId)
					}
					// insert ivf_sk plan
					{
						lastProject := builder.qry.Nodes[lastNodeId].ProjectList
						lastProjectForTblJoinCentroids := builder.qry.Nodes[lastNodeId].ProjectList

						projectProjection := make([]*Expr, len(delCtx.tableDef.Cols))
						projectProjectionForTblJoinCentroids := make([]*Expr, len(delCtx.tableDef.Cols))
						for j, uCols := range delCtx.tableDef.Cols {
							if nIdx, ok := delCtx.updateColPosMap[uCols.Name]; ok {
								projectProjection[j] = lastProject[nIdx]
								projectProjectionForTblJoinCentroids[j] = lastProjectForTblJoinCentroids[nIdx]
							} else {
								if uCols.Name == catalog.Row_ID {
									// replace the origin table's row_id with entry table's row_id
									// it is the 2nd last column in the entry table join
									projectProjection[j] = lastProject[len(lastProject)-2]
									projectProjectionForTblJoinCentroids[j] = lastProjectForTblJoinCentroids[len(lastProjectForTblJoinCentroids)-2]
								} else {
									projectProjection[j] = lastProject[j]
									projectProjectionForTblJoinCentroids[j] = lastProjectForTblJoinCentroids[j]
								}
							}
						}
						projectNode := &Node{
							NodeType:    plan.Node_PROJECT,
							Children:    []int32{lastNodeId},
							ProjectList: projectProjection,
						}
						lastNodeId = builder.appendNode(projectNode, bindCtx)

						preUKStep, err := appendPreInsertSkVectorPlan(builder, bindCtx, delCtx.tableDef, lastNodeId, multiTableIndex, true, idxRefs, idxTableDefs)
						if err != nil {
							return err
						}

						insertEntriesTableDef := DeepCopyTableDef(entriesTableDef, false)
						for _, col := range entriesTableDef.Cols {
							if col.Name != catalog.Row_ID {
								insertEntriesTableDef.Cols = append(insertEntriesTableDef.Cols, DeepCopyColDef(col))
							}
						}
						updateColLength := 1
						addAffectedRows := false
						isFkRecursionCall := false
						updatePkCol := true
						ifExistAutoPkCol := false
						ifCheckPkDup := false
						ifInsertFromUnique := false
						var pkFilterExprs []*Expr
						var partitionExpr *Expr
						var indexSourceColTypes []*Type
						var fuzzymessage *OriginTableMessageForFuzzy
						err = makeOneInsertPlan(builder.compCtx, builder, bindCtx, entriesObjRef, insertEntriesTableDef,
							updateColLength, preUKStep, addAffectedRows, isFkRecursionCall, updatePkCol,
							pkFilterExprs, partitionExpr, ifExistAutoPkCol, ifCheckPkDup, ifInsertFromUnique,
							indexSourceColTypes, fuzzymessage)

						if err != nil {
							return err
						}
					}

				} else {
					// it's more simple for delete hidden unique table .so we append nodes after the plan. not recursive call buildDeletePlans
					delNodeInfo := makeDeleteNodeInfo(builder.compCtx, entriesObjRef, entriesTableDef, entriesDeleteIdx, -1, false, entriesTblPkPos, entriesTblPkTyp, delCtx.lockTable, delCtx.partitionInfos)
					lastNodeId, err = makeOneDeletePlan(builder, bindCtx, lastNodeId, delNodeInfo, false, true, false)
					putDeleteNodeInfo(delNodeInfo)
					if err != nil {
						return err
					}
					builder.appendStep(lastNodeId)
				}
			default:
				return moerr.NewNYINoCtx("unsupported index algorithm " + multiTableIndex.IndexAlgo)
			}
		}
	}

	// delete origin table
	lastNodeId := len(builder.qry.Nodes) - 1
	partExprIdx := -1
	if delCtx.tableDef.Partition != nil {
		partExprIdx = len(delCtx.tableDef.Cols) + delCtx.updateColLength
		lastNodeId = appendPreDeleteNode(builder, bindCtx, delCtx.objRef, delCtx.tableDef, lastNodeId)
	}
	pkPos, pkTyp := getPkPos(delCtx.tableDef, false)
	delNodeInfo := makeDeleteNodeInfo(builder.compCtx, delCtx.objRef, delCtx.tableDef, delCtx.rowIdPos, partExprIdx, true, pkPos, pkTyp, delCtx.lockTable, delCtx.partitionInfos)
	lastNodeId, err = makeOneDeletePlan(builder, bindCtx, lastNodeId, delNodeInfo, false, false, canTruncate)
	putDeleteNodeInfo(delNodeInfo)
	if err != nil {
		return err
	}
	builder.appendStep(lastNodeId)

	return nil
}
*/
