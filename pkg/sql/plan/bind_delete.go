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
	err := dmlCtx.ResolveTables(builder.compCtx, stmt.Tables, stmt.With, aliasMap, false)
	if err != nil {
		return 0, err
	}

	var selectList []tree.SelectExpr
	colName2Idx := make([]map[string]int32, len(stmt.Tables))

	getResolveExpr := func(alias string) {
		defIdx := dmlCtx.aliasMap[alias]
		colName2Idx[defIdx] = make(map[string]int32)
		for _, col := range dmlCtx.tableDefs[defIdx].Cols {
			colName2Idx[defIdx][col.Name] = int32(len(selectList))
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

			idxObjRef, idxTableDef := builder.compCtx.ResolveIndexTableByRef(dmlCtx.objRefs[0], idxDef.IndexTableName, bindCtx.snapshot)
			if len(idxTableDef.Name2ColIndex) == 0 {
				idxTableDef.Name2ColIndex = make(map[string]int32)
				for colIdx, col := range idxTableDef.Cols {
					idxTableDef.Name2ColIndex[col.Name] = int32(colIdx)
				}
			}
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
				var colPos int32
				var ok bool
				for k, colName := range idxDef.Parts {
					if colPos, ok = colName2Idx[i][catalog.ResolveAlias(colName)]; !ok {
						errMsg := fmt.Sprintf("bind delete err, can not find colName = %s", colName)
						return 0, moerr.NewInternalError(builder.GetContext(), errMsg)
					}
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

			joinType := plan.Node_INNER
			if idxDef.Unique {
				joinType = plan.Node_LEFT
			}

			lastNodeID = builder.appendNode(&plan.Node{
				NodeType: plan.Node_JOIN,
				Children: []int32{lastNodeID, idxTableNodeID},
				JoinType: joinType,
				OnList:   []*plan.Expr{joinCond},
			}, bindCtx)
		}
	}

	dmlNode := &plan.Node{
		NodeType:    plan.Node_MULTI_UPDATE,
		BindingTags: []int32{builder.genNewTag()},
	}
	selectNodeTag := selectNode.BindingTags[0]
	var lockTargets []*plan.LockTarget

	for i, tableDef := range dmlCtx.tableDefs {
		pkPos := colName2Idx[i][tableDef.Pkey.PkeyColName]
		rowIDPos := colName2Idx[i][catalog.Row_ID]
		updateCtx := &plan.UpdateCtx{
			TableDef: DeepCopyTableDef(tableDef, true),
			ObjRef:   DeepCopyObjectRef(dmlCtx.objRefs[i]),
		}

		for _, col := range tableDef.Cols {
			if col.Name == tableDef.Pkey.PkeyColName {
				lockTarget := &plan.LockTarget{
					TableId:            tableDef.TblId,
					ObjRef:             DeepCopyObjectRef(dmlCtx.objRefs[i]),
					PrimaryColIdxInBat: int32(pkPos),
					PrimaryColRelPos:   selectNodeTag,
					PrimaryColTyp:      col.Typ,
				}
				lockTargets = append(lockTargets, lockTarget)
				break
			}
		}

		updateCtx.DeleteCols = []plan.ColRef{
			{
				RelPos: selectNodeTag,
				ColPos: rowIDPos,
			},
			{
				RelPos: selectNodeTag,
				ColPos: pkPos,
			},
		}

		dmlNode.UpdateCtxList = append(dmlNode.UpdateCtxList, updateCtx)

		for j, idxNode := range idxScanNodes[i] {
			if idxNode == nil {
				continue
			}

			pkPos := idxNode.TableDef.Name2ColIndex[idxNode.TableDef.Pkey.PkeyColName]
			rowIDPos := idxNode.TableDef.Name2ColIndex[catalog.Row_ID]

			if tableDef.Indexes[j].Unique {
				for _, col := range idxNode.TableDef.Cols {
					if col.Name == idxNode.TableDef.Pkey.PkeyColName {
						lockTargets = append(lockTargets, &plan.LockTarget{
							TableId:            idxNode.TableDef.TblId,
							ObjRef:             DeepCopyObjectRef(idxNode.ObjRef),
							PrimaryColIdxInBat: int32(pkPos),
							PrimaryColRelPos:   idxNode.BindingTags[0],
							PrimaryColTyp:      col.Typ,
						})
						break
					}
				}
			}

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
		}
	}

	if len(lockTargets) > 0 {
		lastNodeID = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_LOCK_OP,
			Children:    []int32{lastNodeID},
			TableDef:    dmlCtx.tableDefs[0],
			BindingTags: []int32{builder.genNewTag()},
			LockTargets: lockTargets,
		}, bindCtx)

		reCheckifNeedLockWholeTable(builder)
	}

	dmlNode.Children = append(dmlNode.Children, lastNodeID)
	lastNodeID = builder.appendNode(dmlNode, bindCtx)

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
		logutil.Infof("Row lock upgraded to table lock for SQL : %s", builder.compCtx.GetRootSql())
		logutil.Infof("the outcnt stats is %f", node.Stats.Outcnt)
		for _, target := range node.LockTargets {
			target.LockTable = true
		}
	}
}
