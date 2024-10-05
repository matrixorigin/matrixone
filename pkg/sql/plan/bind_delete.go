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

func (builder *QueryBuilder) bindDelete(stmt *tree.Delete, bindCtx *BindContext) (int32, error) {
	if len(stmt.Tables) != 1 {
		return 0, moerr.NewUnsupportedDML(builder.GetContext(), "delete from multiple tables")
	}

	//FIXME: optimize truncate table?
	if stmt.Where == nil && stmt.Limit == nil {
		return 0, moerr.NewUnsupportedDML(builder.GetContext(), "rewrite to truncate table")
	}

	aliasMap := make(map[string][2]string)
	for _, tbl := range stmt.TableRefs {
		getAliasToName(builder.compCtx, tbl, "", aliasMap)
	}

	dmlCtx := NewDMLContext()
	err := dmlCtx.ResolveTables(builder.compCtx, stmt.Tables, stmt.With, aliasMap)
	if err != nil {
		return 0, err
	}

	var selectList []tree.SelectExpr
	colName2Idx := make([]map[string]int, len(stmt.Tables))

	getResolveExpr := func(alias string) {
		defIdx := dmlCtx.aliasMap[alias]
		colName2Idx[defIdx] = make(map[string]int)
		for _, col := range dmlCtx.tableDefs[defIdx].Cols {
			colName2Idx[defIdx][col.Name] = len(selectList)
			selectExpr := tree.NewUnresolvedName(tree.NewCStr(alias, bindCtx.lower), tree.NewCStr(col.Name, 1))
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

	selectCtx := NewBindContext(builder, bindCtx)
	lastNodeID, err := builder.bindSelect(astSelect, selectCtx, false)
	if err != nil {
		return 0, err
	}

	selectNode := builder.qry.Nodes[lastNodeID]
	if selectNode.NodeType != plan.Node_PROJECT {
		return 0, moerr.NewUnsupportedDML(builder.GetContext(), "malformed select node")
	}

	idxScanNodes := make([][]*plan.Node, len(dmlCtx.tableDefs))

	for i, tableDef := range dmlCtx.tableDefs {
		idxDefs := tableDef.Indexes
		idxScanNodes[i] = make([]*plan.Node, len(idxDefs))

		for j, idxDef := range idxDefs {
			if !idxDef.TableExist {
				continue
			}

			if !catalog.IsRegularIndexAlgo(idxDef.IndexAlgo) {
				return 0, moerr.NewUnsupportedDML(builder.GetContext(), "have vector index table")
			}

			idxObjRef, idxTableDef := builder.compCtx.Resolve(dmlCtx.objRefs[0].SchemaName, idxDef.IndexTableName, bindCtx.snapshot)
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

			var leftExpr *plan.Expr

			argsLen := len(idxDef.Parts)
			if argsLen == 1 {
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
				args := make([]*plan.Expr, argsLen)

				for k, colName := range idxDef.Parts {
					colPos := int32(colName2Idx[i][colName])
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

				fnName := "serial"
				if !idxDef.Unique {
					fnName = "serial_full"
				}
				leftExpr, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), fnName, args)
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
		NodeType: plan.Node_MULTI_UPDATE,
		Children: []int32{lastNodeID},
	}

	for i, tableDef := range dmlCtx.tableDefs {
		pkPos := int32(colName2Idx[i][tableDef.Pkey.PkeyColName])
		rowIDPos := int32(colName2Idx[i][catalog.Row_ID])

		dmlNode.UpdateCtxList = append(dmlNode.UpdateCtxList, &plan.UpdateCtx{
			TableDef: DeepCopyTableDef(tableDef, true),
			ObjRef:   DeepCopyObjectRef(dmlCtx.objRefs[i]),
			DeleteCols: []plan.ColRef{
				{
					RelPos: selectNode.BindingTags[0],
					ColPos: rowIDPos,
				},
				{
					RelPos: selectNode.BindingTags[0],
					ColPos: pkPos,
				},
			},
		})

		//lastNodeID = builder.appendNode(&plan.Node{
		//	NodeType: plan.Node_DELETE,
		//	Children: []int32{lastNodeID},
		//	DeleteCtx: &plan.DeleteCtx{
		//		TableDef: DeepCopyTableDef(tableDef, true),
		//		Ref: DeepCopyObjectRef(tblInfo.objRef[i]),
		//		AddAffectedRows:     delNodeInfo.addAffectedRows,
		//		IsClusterTable:      delNodeInfo.IsClusterTable,
		//		PartitionTableIds:   delNodeInfo.partTableIDs,
		//		PartitionTableNames: delNodeInfo.partTableNames,
		//		PartitionIdx:        int32(delNodeInfo.partitionIdx),
		//	},
		//	InsertDeleteCols: []*plan.Expr{
		//		{
		//			Typ: selectNode.ProjectList[pkPos].Typ,
		//			Expr: &plan.Expr_Col{
		//				Col: &plan.ColRef{
		//					RelPos: selectNode.BindingTags[0],
		//					ColPos: pkPos,
		//				},
		//			},
		//		},
		//		{
		//			Typ: selectNode.ProjectList[rowIDPos].Typ,
		//			Expr: &plan.Expr_Col{
		//				Col: &plan.ColRef{
		//					RelPos: selectNode.BindingTags[0],
		//					ColPos: rowIDPos,
		//				},
		//			},
		//		},
		//	},
		//}, ctx)

		for _, idxNode := range idxScanNodes[i] {
			if idxNode == nil {
				continue
			}

			pkPos := int32(idxNode.TableDef.Name2ColIndex[idxNode.TableDef.Pkey.PkeyColName])
			rowIDPos := int32(idxNode.TableDef.Name2ColIndex[catalog.Row_ID])

			dmlNode.UpdateCtxList = append(dmlNode.UpdateCtxList, &plan.UpdateCtx{
				TableDef: DeepCopyTableDef(idxNode.TableDef, true),
				ObjRef:   DeepCopyObjectRef(idxNode.ObjRef),
				DeleteCols: []plan.ColRef{
					{
						RelPos: idxNode.BindingTags[0],
						ColPos: rowIDPos,
					},
					{
						RelPos: idxNode.BindingTags[0],
						ColPos: pkPos,
					},
				},
			})

			//lastNodeID = builder.appendNode(&plan.Node{
			//	NodeType: plan.Node_DELETE,
			//	Children: []int32{lastNodeID},
			//	DeleteCtx: &plan.DeleteCtx{
			//		TableDef: DeepCopyTableDef(idxNode.TableDef, true),
			//		Ref:      DeepCopyObjectRef(idxNode.ObjRef),
			//		PrimaryKeyIdx:       int32(delNodeInfo.pkPos),
			//		CanTruncate:         canTruncate,
			//		AddAffectedRows:     delNodeInfo.addAffectedRows,
			//		IsClusterTable:      delNodeInfo.IsClusterTable,
			//		PartitionTableIds:   delNodeInfo.partTableIDs,
			//		PartitionTableNames: delNodeInfo.partTableNames,
			//		PartitionIdx:        int32(delNodeInfo.partitionIdx),
			//	},
			//	InsertDeleteCols: []*plan.Expr{
			//		{
			//			Typ: idxNode.TableDef.Cols[pkPos].Typ,
			//			Expr: &plan.Expr_Col{
			//				Col: &plan.ColRef{
			//					RelPos: idxNode.BindingTags[0],
			//					ColPos: pkPos,
			//				},
			//			},
			//		},
			//		{
			//			Typ: idxNode.TableDef.Cols[rowIDPos].Typ,
			//			Expr: &plan.Expr_Col{
			//				Col: &plan.ColRef{
			//					RelPos: idxNode.BindingTags[0],
			//					ColPos: rowIDPos,
			//				},
			//			},
			//		},
			//	},
			//}, ctx)
		}
	}

	lastNodeID = builder.appendNode(dmlNode, bindCtx)

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
