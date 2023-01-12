// Copyright 2022 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

const derivedTableName = "_t"

type deleteSelectInfo struct {
	projectList         []*Expr
	tblInfo             *dmlTableInfo
	idx                 int32
	rootId              int32
	derivedTableId      int32
	onDeleteIdx         []int32
	onDeleteIdxTbl      []*ObjectRef
	onDeleteRestrict    []int32
	onDeleteRestrictTbl []*ObjectRef
	onDeleteSet         [][]int64
	onDeleteSetAttr     [][]string
	onDeleteSetTbl      []*ObjectRef
	onDeleteCascade     []int32
	onDeleteCascadeTbl  []*ObjectRef
}

type dmlTableInfo struct {
	objRef    []*ObjectRef
	tableDefs []*TableDef
	nameToIdx map[string]int
	idToName  map[uint64]string
}

func getAliasToName(ctx CompilerContext, expr tree.TableExpr, alias string, aliasMap map[string][2]string) {
	switch t := expr.(type) {
	case *tree.TableName:
		dbName := string(t.SchemaName)
		if dbName == "" {
			dbName = ctx.DefaultDatabase()
		}
		tblName := string(t.ObjectName)
		if alias != "" {
			aliasMap[alias] = [2]string{dbName, tblName}
		}
	case *tree.AliasedTableExpr:
		alias := string(t.As.Alias)
		getAliasToName(ctx, t.Expr, alias, aliasMap)
	case *tree.JoinTableExpr:
		getAliasToName(ctx, t.Left, alias, aliasMap)
		getAliasToName(ctx, t.Right, alias, aliasMap)
	}
}

func getDmlTableInfo(ctx CompilerContext, tableExprs tree.TableExprs, aliasMap map[string][2]string) (*dmlTableInfo, error) {
	tblLen := len(tableExprs)
	tblInfo := &dmlTableInfo{
		objRef:    make([]*ObjectRef, tblLen),
		tableDefs: make([]*plan.TableDef, tblLen),
		nameToIdx: make(map[string]int),
		idToName:  make(map[uint64]string),
	}

	for idx, tbl := range tableExprs {
		var tblName, dbName string

		if aliasTbl, ok := tbl.(*tree.AliasedTableExpr); ok {
			if baseTbl, ok := aliasTbl.Expr.(*tree.TableName); ok {
				dbName = string(baseTbl.SchemaName)
				if dbName == "" {
					dbName = ctx.DefaultDatabase()
				}
				tblName = string(baseTbl.ObjectName)
			} else {
				return nil, moerr.NewInternalError(ctx.GetContext(), "%v is not a normal table", tree.String(tbl, dialect.MYSQL))
			}
		} else if baseTbl, ok := tbl.(*tree.TableName); ok {
			tblName = string(baseTbl.ObjectName)
			dbName = string(baseTbl.SchemaName)
			if dbName == "" {
				dbName = ctx.DefaultDatabase()
			}
		}
		if aliasNames, exist := aliasMap[tblName]; exist {
			dbName = aliasNames[0]
			tblName = aliasNames[1]
		}

		_, tableDef := ctx.Resolve(dbName, tblName)
		if tableDef == nil {
			return nil, moerr.NewNoSuchTable(ctx.GetContext(), dbName, tblName)
		}
		if tableDef.TableType == catalog.SystemExternalRel {
			return nil, moerr.NewInvalidInput(ctx.GetContext(), "cannot update/delete from external table")
		} else if tableDef.TableType == catalog.SystemViewRel {
			return nil, moerr.NewInvalidInput(ctx.GetContext(), "cannot update/delete from view")
		}
		if util.TableIsClusterTable(tableDef.GetTableType()) && ctx.GetAccountId() != catalog.System_Account {
			return nil, moerr.NewInternalError(ctx.GetContext(), "only the sys account can delete the cluster table %s", tableDef.GetName())
		}

		tblInfo.objRef[idx] = &ObjectRef{
			Obj:        int64(tableDef.TblId),
			SchemaName: dbName,
			ObjName:    tblName,
		}
		tblInfo.tableDefs[idx] = tableDef
		key := dbName + "." + tblName
		tblInfo.nameToIdx[key] = idx
		tblInfo.idToName[tableDef.TblId] = key
	}

	return tblInfo, nil
}

func updateToSelect(builder *QueryBuilder, bindCtx *BindContext, node *tree.Update, haveConstraint bool) (int32, error) {
	return 0, nil
}

func insertToSelect(builder *QueryBuilder, bindCtx *BindContext, node *tree.Insert, haveConstraint bool) (int32, error) {
	return 0, nil
}

func deleteToSelect(builder *QueryBuilder, bindCtx *BindContext, node *tree.Delete, haveConstraint bool) (int32, error) {
	var selectList []tree.SelectExpr
	fromTables := &tree.From{}

	getResolveExpr := func(tblName string) tree.SelectExpr {
		var ret *tree.UnresolvedName
		if haveConstraint {
			ret, _ = tree.NewUnresolvedNameWithStar(builder.GetContext(), tblName)
		} else {
			ret, _ = tree.NewUnresolvedName(builder.GetContext(), tblName, catalog.Row_ID)
		}
		return tree.SelectExpr{
			Expr: ret,
		}
	}

	for _, tbl := range node.Tables {
		if aliasTbl, ok := tbl.(*tree.AliasedTableExpr); ok {
			alias := string(aliasTbl.As.Alias)
			if alias != "" {
				selectList = append(selectList, getResolveExpr(alias))
			} else {
				astTbl := aliasTbl.Expr.(*tree.TableName)
				selectList = append(selectList, getResolveExpr(string(astTbl.ObjectName)))
			}
		} else if astTbl, ok := tbl.(*tree.TableName); ok {
			selectList = append(selectList, getResolveExpr(string(astTbl.ObjectName)))
		}
	}

	if node.TableRefs != nil {
		fromTables.Tables = node.TableRefs
	} else {
		fromTables.Tables = node.Tables
	}

	astSelect := &tree.Select{
		Select: &tree.SelectClause{
			Distinct: false,
			Exprs:    selectList,
			From:     fromTables,
			Where:    node.Where,
		},
		OrderBy: node.OrderBy,
		Limit:   node.Limit,
		With:    node.With,
	}
	// ftCtx := tree.NewFmtCtx(dialectType)
	// astSelect.Format(ftCtx)
	// sql := ftCtx.String()
	// fmt.Print(sql)

	return builder.buildSelect(astSelect, bindCtx, false)
}

func checkIfStmtHaveRewriteConstraint(tblInfo *dmlTableInfo) bool {
	for _, tableDef := range tblInfo.tableDefs {
		for _, def := range tableDef.Defs {
			if _, ok := def.Def.(*plan.TableDef_DefType_UIdx); ok {
				return true
			}
		}
		if len(tableDef.RefChildTbls) > 0 {
			return true
		}
	}
	return false
}

func initDeleteStmt(builder *QueryBuilder, bindCtx *BindContext, info *deleteSelectInfo, stmt *tree.Delete) error {
	var err error
	subCtx := NewBindContext(builder, bindCtx)
	info.rootId, err = deleteToSelect(builder, subCtx, stmt, true)
	if err != nil {
		return err
	}

	err = builder.addBinding(info.rootId, tree.AliasClause{
		Alias: derivedTableName,
	}, bindCtx)
	if err != nil {
		return err
	}

	info.idx = int32(len(info.tblInfo.objRef))
	tag := builder.qry.Nodes[info.rootId].BindingTags[0]
	info.derivedTableId = info.rootId
	for idx, expr := range builder.qry.Nodes[info.rootId].ProjectList {
		if expr.Typ.Id == int32(types.T_Rowid) {
			info.projectList = append(info.projectList, &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: tag,
						ColPos: int32(idx),
					},
				},
			})
		}
	}
	return nil
}

func rewriteDeleteSelectInfo(builder *QueryBuilder, bindCtx *BindContext, info *deleteSelectInfo, tableDef *TableDef, baseNodeId int32) error {
	posMap := make(map[string]int32)
	typMap := make(map[string]*plan.Type)
	id2name := make(map[uint64]string)
	beginPos := 0
	//use origin query as left, we need add prefix pos
	if baseNodeId == info.derivedTableId {
		for _, d := range info.tblInfo.tableDefs {
			if d.Name == tableDef.Name {
				break
			}
			beginPos = beginPos + len(d.Cols)
		}
	}
	for idx, col := range tableDef.Cols {
		posMap[col.Name] = int32(beginPos + idx)
		typMap[col.Name] = col.Typ
		id2name[col.ColId] = col.Name
	}

	// rewrite index
	for _, def := range tableDef.Defs {
		if idxDef, ok := def.Def.(*plan.TableDef_DefType_UIdx); ok {
			for idx, tblName := range idxDef.UIdx.TableNames {
				idxRef := &plan.ObjectRef{
					SchemaName: builder.compCtx.DefaultDatabase(),
					ObjName:    tblName,
				}

				// append table_scan node
				rightCtx := NewBindContext(builder, bindCtx)
				astTblName := tree.NewTableName(tree.Identifier(tblName), tree.ObjectNamePrefix{})
				rightId, err := builder.buildTable(astTblName, rightCtx)
				if err != nil {
					return err
				}
				rightTag := builder.qry.Nodes[rightId].BindingTags[0]
				baseTag := builder.qry.Nodes[baseNodeId].BindingTags[0]
				rightTableDef := builder.qry.Nodes[rightId].TableDef
				rightRowIdPos := int32(len(rightTableDef.Cols)) - 1
				rightIdxPos := int32(0)

				// append projection
				info.projectList = append(info.projectList, &plan.Expr{
					Typ: rightTableDef.Cols[rightRowIdPos].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: rightTag,
							ColPos: rightRowIdPos,
						},
					},
				})

				rightExpr := &plan.Expr{
					Typ: rightTableDef.Cols[rightIdxPos].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: rightTag,
							ColPos: rightIdxPos,
						},
					},
				}

				// append join node
				var joinConds []*Expr
				var leftExpr *Expr
				partsLength := len(idxDef.UIdx.Fields[idx].Parts)
				if partsLength == 1 {
					orginIndexColumnName := idxDef.UIdx.Fields[idx].Parts[0]
					typ := typMap[orginIndexColumnName]
					leftExpr = &Expr{
						Typ: typ,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: baseTag,
								ColPos: int32(posMap[orginIndexColumnName]),
							},
						},
					}
				} else {
					args := make([]*Expr, partsLength)
					for i, column := range idxDef.UIdx.Fields[idx].Parts {
						typ := typMap[column]
						args[i] = &plan.Expr{
							Typ: typ,
							Expr: &plan.Expr_Col{
								Col: &plan.ColRef{
									RelPos: baseTag,
									ColPos: int32(posMap[column]),
								},
							},
						}
					}
					leftExpr, err = bindFuncExprImplByPlanExpr(builder.GetContext(), "serial", args)
					if err != nil {
						return err
					}
				}

				condExpr, err := bindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{leftExpr, rightExpr})
				if err != nil {
					return err
				}
				joinConds = []*Expr{condExpr}

				leftCtx := builder.ctxByNode[info.rootId]
				joinCtx := NewBindContext(builder, bindCtx)
				err = joinCtx.mergeContexts(leftCtx, rightCtx)
				if err != nil {
					return err
				}
				newRootId := builder.appendNode(&plan.Node{
					NodeType: plan.Node_JOIN,
					Children: []int32{info.rootId, rightId},
					JoinType: plan.Node_LEFT,
					OnList:   joinConds,
				}, joinCtx)
				bindCtx.binder = NewTableBinder(builder, bindCtx)
				info.rootId = newRootId

				info.onDeleteIdxTbl = append(info.onDeleteIdxTbl, idxRef)
				info.onDeleteIdx = append(info.onDeleteIdx, info.idx)
				info.idx = info.idx + 1
			}
		}
	}

	// rewrite foreign key
	for _, tableId := range tableDef.RefChildTbls {
		if _, existInDelTable := info.tblInfo.idToName[tableId]; existInDelTable {
			// delete parent_tbl, child_tbl from parent_tbl join child_tbl xxxxxx
			// we will skip child_tbl here.
			continue
		}

		_, childTableDef := builder.compCtx.ResolveById(tableId) //opt: actionRef是否也记录到RefChildTbls里？

		childPosMap := make(map[string]int32)
		childTypMap := make(map[string]*plan.Type)
		childId2name := make(map[uint64]string)
		var childAttrs []string
		for idx, col := range childTableDef.Cols {
			childPosMap[col.Name] = int32(idx)
			childTypMap[col.Name] = col.Typ
			childId2name[col.ColId] = col.Name
			if col.Name != catalog.Row_ID {
				childAttrs = append(childAttrs, col.Name)
			}
		}

		objRef := &plan.ObjectRef{
			Obj:        int64(childTableDef.TblId),
			SchemaName: builder.compCtx.DefaultDatabase(),
			ObjName:    childTableDef.Name,
		}

		for _, fk := range childTableDef.Fkeys {
			if fk.ForeignTbl == tableDef.TblId {
				// append table scan node
				rightCtx := NewBindContext(builder, bindCtx)
				astTblName := tree.NewTableName(tree.Identifier(childTableDef.Name), tree.ObjectNamePrefix{})
				rightId, err := builder.buildTable(astTblName, rightCtx)
				if err != nil {
					return err
				}
				rightTag := builder.qry.Nodes[rightId].BindingTags[0]
				baseNodeTag := builder.qry.Nodes[baseNodeId].BindingTags[0]
				needRecursionCall := false

				// build join conds
				joinConds := make([]*Expr, len(fk.Cols))
				for i, colId := range fk.Cols {
					for _, col := range childTableDef.Cols {
						if col.ColId == colId {
							childColumnName := col.Name
							originColumnName := id2name[fk.ForeignCols[i]]

							leftExpr := &Expr{
								Typ: typMap[originColumnName],
								Expr: &plan.Expr_Col{
									Col: &plan.ColRef{
										RelPos: baseNodeTag,
										ColPos: posMap[originColumnName],
									},
								},
							}
							rightExpr := &plan.Expr{
								Typ: childTypMap[childColumnName],
								Expr: &plan.Expr_Col{
									Col: &plan.ColRef{
										RelPos: rightTag,
										ColPos: childPosMap[childColumnName],
									},
								},
							}
							condExpr, err := bindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{leftExpr, rightExpr})
							if err != nil {
								return err
							}
							joinConds[i] = condExpr
							break
						}
					}
				}

				// append project
				switch fk.OnDelete {
				case plan.ForeignKeyDef_NO_ACTION, plan.ForeignKeyDef_RESTRICT, plan.ForeignKeyDef_SET_DEFAULT:
					info.projectList = append(info.projectList, &plan.Expr{
						Typ: childTypMap[catalog.Row_ID],
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: rightTag,
								ColPos: childPosMap[catalog.Row_ID],
							},
						},
					})
					info.onDeleteRestrict = append(info.onDeleteRestrict, info.idx)
					info.idx = info.idx + 1
					info.onDeleteRestrictTbl = append(info.onDeleteRestrictTbl, objRef)

				case plan.ForeignKeyDef_CASCADE:
					info.projectList = append(info.projectList, &plan.Expr{
						Typ: childTypMap[catalog.Row_ID],
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: rightTag,
								ColPos: childPosMap[catalog.Row_ID],
							},
						},
					})
					info.onDeleteCascade = append(info.onDeleteCascade, info.idx)
					info.idx = info.idx + 1
					info.onDeleteCascadeTbl = append(info.onDeleteCascadeTbl, objRef)

					needRecursionCall = true

				case plan.ForeignKeyDef_SET_NULL:
					fkIdMap := make(map[uint64]struct{})
					for _, colId := range fk.Cols {
						fkIdMap[colId] = struct{}{}
					}
					var setIdxs []int64
					for j, col := range childTableDef.Cols {
						if _, ok := fkIdMap[col.ColId]; ok {
							info.projectList = append(info.projectList, makePlan2NullConstExprWithType())
						} else {
							info.projectList = append(info.projectList, &plan.Expr{
								Typ: col.Typ,
								Expr: &plan.Expr_Col{
									Col: &plan.ColRef{
										RelPos: rightTag,
										ColPos: int32(j),
									},
								},
							})
						}
						setIdxs = append(setIdxs, int64(info.idx))
						info.idx = info.idx + 1
					}
					info.onDeleteSet = append(info.onDeleteSet, setIdxs)
					info.onDeleteSetTbl = append(info.onDeleteSetTbl, objRef)
					info.onDeleteSetAttr = append(info.onDeleteSetAttr, childAttrs)
					needRecursionCall = true
				}

				// append join node
				leftCtx := builder.ctxByNode[info.rootId]
				joinCtx := NewBindContext(builder, bindCtx)
				err = joinCtx.mergeContexts(leftCtx, rightCtx)
				if err != nil {
					return err
				}
				newRootId := builder.appendNode(&plan.Node{
					NodeType: plan.Node_JOIN,
					Children: []int32{info.rootId, rightId},
					JoinType: plan.Node_LEFT,
					OnList:   joinConds,
				}, joinCtx)
				bindCtx.binder = NewTableBinder(builder, bindCtx)
				info.rootId = newRootId

				if needRecursionCall {
					err := rewriteDeleteSelectInfo(builder, bindCtx, info, childTableDef, info.rootId)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}
