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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

const derivedTableName = "_t"

type deleteSelectInfo struct {
	projectList           []*Expr
	tblInfo               *dmlTableInfo
	idx                   int
	rootId                int32
	stmt                  *tree.Delete
	onDeleteIdx           []int
	onDeleteIdxTblName    [][2]string
	onDeleteRestrict      []int
	onDeleteRestrictTblId []uint64
	onDeleteSet           []int
	onDeleteSetTblId      []uint64
	onDeleteCascade       []int
	onDeleteCascadeTblId  []uint64
}

type dmlTableInfo struct {
	dbNames    []string
	tableNames []string
	tableDefs  []*TableDef
	nameMap    map[string]int
}

func extractExprToName(ctx CompilerContext, expr tree.TableExpr, alias string, aliasMap map[string][2]string) {
	switch t := expr.(type) {
	case *tree.TableName:
		dbName := string(t.SchemaName)
		if dbName == "" {
			dbName = ctx.DefaultDatabase()
		}
		tblName := string(t.ObjectName)
		aliasMap[alias] = [2]string{dbName, tblName}
	case *tree.AliasedTableExpr:
		alias := string(t.As.Alias)
		extractExprToName(ctx, t.Expr, alias, aliasMap)
	case *tree.JoinTableExpr:
		extractExprToName(ctx, t.Left, alias, aliasMap)
		extractExprToName(ctx, t.Right, alias, aliasMap)
	}
}

func getAliasToName(ctx CompilerContext, tableExprs tree.TableExprs) map[string][2]string {
	aliasMap := make(map[string][2]string)
	for _, tbl := range tableExprs {
		extractExprToName(ctx, tbl, "", aliasMap)
	}
	return aliasMap
}

func getDmlTableInfo(ctx CompilerContext, tableExprs tree.TableExprs, aliasMap map[string][2]string) (*dmlTableInfo, error) {
	tblLen := len(tableExprs)
	tblInfo := &dmlTableInfo{
		dbNames:    make([]string, tblLen),
		tableNames: make([]string, tblLen),
		tableDefs:  make([]*plan.TableDef, tblLen),
		nameMap:    make(map[string]int),
	}

	for idx, tbl := range tableExprs {
		var tblName, dbName string

		if aliasTbl, ok := tbl.(*tree.AliasedTableExpr); ok {
			if baseTbl, ok := aliasTbl.Expr.(*tree.TableName); ok {
				tblName = string(baseTbl.ObjectName)
			} else {
				return nil, moerr.NewInternalError(ctx.GetContext(), "%v is not a normal table", tree.String(tbl, dialect.MYSQL))
			}
		} else if baseTbl, ok := tbl.(*tree.TableName); ok {
			tblName = string(baseTbl.ObjectName)
		}
		if aliasNames, exist := aliasMap[tblName]; exist {
			dbName = aliasNames[0]
			tblName = aliasNames[1]
		} else {
			dbName = ctx.DefaultDatabase()
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

		tblInfo.dbNames[idx] = dbName
		tblInfo.tableNames[idx] = tblName
		tblInfo.tableDefs[idx] = tableDef
		tblInfo.nameMap[tblName] = idx
	}

	return tblInfo, nil
}

// delete from a1, a2 using t1 as a1 inner join t2 as a2 where a1.id = a2.id
// select a1.row_id, a2.row_id from t1 as a1 inner join t2 as a2 where a1.id = a2.id
// select _t.* from (select a1.row_id, a2.row_id from t1 as a1 inner join t2 as a2 where a1.id = a2.id) _t
func deleteToSelect(node *tree.Delete, dialectType dialect.DialectType, returnAll bool) string {
	ctx := tree.NewFmtCtx(dialectType)
	if node.With != nil {
		node.With.Format(ctx)
		ctx.WriteByte(' ')
	}
	ctx.WriteString("select ")
	prefix := ""
	for _, tbl := range node.Tables {
		ctx.WriteString(prefix)
		tbl.Format(ctx)
		if returnAll {
			ctx.WriteString(".*")
		} else {
			ctx.WriteByte('.')
			ctx.WriteString(catalog.Row_ID)
		}
		prefix = ", "
	}

	// if node.PartitionNames != nil {
	// 	ctx.WriteString(" partition(")
	// 	node.PartitionNames.Format(ctx)
	// 	ctx.WriteByte(')')
	// }

	if node.TableRefs != nil {
		ctx.WriteString(" from ")
		node.TableRefs.Format(ctx)
	} else {
		ctx.WriteString(" from ")
		prefix := ""
		for _, tbl := range node.Tables {
			ctx.WriteString(prefix)
			tbl.Format(ctx)
			prefix = ", "
		}
	}

	if node.Where != nil {
		ctx.WriteByte(' ')
		node.Where.Format(ctx)
	}
	if len(node.OrderBy) > 0 {
		ctx.WriteByte(' ')
		node.OrderBy.Format(ctx)
	}
	if node.Limit != nil {
		ctx.WriteByte(' ')
		node.Limit.Format(ctx)
	}
	return ctx.String()
}

// columns in index_table: idx_col = 0; pri_col = 1; rowid =2
const INDEX_TABLE_IDX_POS = 0
const INDEX_TABLE_ROWID_POS = 2

func initDeleteStmt(builder *QueryBuilder, bindCtx *BindContext, info *deleteSelectInfo) error {
	if info.rootId == -1 {
		sql := deleteToSelect(info.stmt, dialect.MYSQL, true)
		stmts, err := mysql.Parse(builder.GetContext(), sql)
		if err != nil {
			return err
		}
		subCtx := NewBindContext(builder, bindCtx)
		info.rootId, err = builder.buildSelect(stmts[0].(*tree.Select), subCtx, false)
		if err != nil {
			return err
		}

		err = builder.addBinding(info.rootId, tree.AliasClause{
			Alias: derivedTableName,
		}, bindCtx)
		if err != nil {
			return err
		}

		info.idx = len(info.tblInfo.tableNames)
		tag := builder.qry.Nodes[info.rootId].BindingTags[0]
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
				break
			}
		}
	}
	return nil
}

func rewriteDeleteSelectInfo(builder *QueryBuilder, bindCtx *BindContext, tableDef *TableDef, info *deleteSelectInfo) error {
	idxMap := make(map[string]int)
	typMap := make(map[string]*plan.Type)
	id2name := make(map[uint64]string)
	for idx, col := range tableDef.Cols {
		idxMap[col.Name] = idx
		typMap[col.Name] = col.Typ
		id2name[col.ColId] = col.Name
	}
	// originTblName := tableDef.Name

	// rewrite index
	for _, def := range tableDef.Defs {
		if idxDef, ok := def.Def.(*plan.TableDef_DefType_UIdx); ok {
			err := initDeleteStmt(builder, bindCtx, info)
			if err != nil {
				return err
			}
			for idx, tblName := range idxDef.UIdx.TableNames {
				// append table_scan node
				leftId := info.rootId
				rightCtx := NewBindContext(builder, bindCtx)
				astTblName := tree.NewTableName(tree.Identifier(tblName), tree.ObjectNamePrefix{})
				// here we get columns: idx_col = 0; pri_col = 1; rowid =2
				rightId, err := builder.buildTable(astTblName, rightCtx)
				if err != nil {
					return err
				}
				rightTag := builder.qry.Nodes[rightId].BindingTags[0]
				leftTag := builder.qry.Nodes[leftId].BindingTags[0]
				rightTableDef := builder.qry.Nodes[rightId].TableDef

				// append projection
				info.projectList = append(info.projectList, &plan.Expr{
					Typ: rightTableDef.Cols[INDEX_TABLE_ROWID_POS].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: rightTag,
							ColPos: INDEX_TABLE_ROWID_POS,
						},
					},
				})

				rightExpr := &plan.Expr{
					Typ: rightTableDef.Cols[INDEX_TABLE_IDX_POS].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: rightTag,
							ColPos: INDEX_TABLE_IDX_POS,
						},
					},
				}

				// append join node
				var joinConds *Expr
				var leftExpr *Expr
				var funcID int64
				partsLength := len(idxDef.UIdx.Fields[idx].Parts)
				if partsLength == 1 {
					orginIndexColumnName := idxDef.UIdx.Fields[idx].Parts[0]
					typ := typMap[orginIndexColumnName]
					leftExpr = &Expr{
						Typ: typ,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: leftTag,
								ColPos: int32(idxMap[orginIndexColumnName]),
							},
						},
					}
				} else {
					args := make([]*Expr, partsLength)
					astTypes := make([]types.Type, partsLength)
					for i, column := range idxDef.UIdx.Fields[idx].Parts {
						typ := typMap[column]
						astTypes[i] = makeTypeByPlan2Type(typ)
						args[i] = &plan.Expr{
							Typ: typ,
							Expr: &plan.Expr_Col{
								Col: &plan.ColRef{
									RelPos: leftTag,
									ColPos: int32(idxMap[column]),
								},
							},
						}
					}
					funcID, _, _, err = function.GetFunctionByName(builder.GetContext(), "serial", astTypes)
					if err != nil {
						return err
					}
					leftExpr = &Expr{
						Typ: &plan.Type{
							Id: int32(types.T_varchar),
						},
						Expr: &plan.Expr_F{
							F: &plan.Function{
								Func: getFunctionObjRef(funcID, "serial"),
								Args: args,
							},
						},
					}
				}

				funcID, _, _, err = function.GetFunctionByName(builder.GetContext(), "=", []types.Type{makeTypeByPlan2Expr(leftExpr), makeTypeByPlan2Expr(rightExpr)})
				if err != nil {
					return err
				}
				joinConds = &Expr{
					Typ: &plan.Type{
						Id: int32(types.T_bool),
					},
					Expr: &plan.Expr_F{
						F: &plan.Function{
							Func: getFunctionObjRef(funcID, "="),
							Args: []*plan.Expr{leftExpr, rightExpr},
						},
					},
				}

				leftCtx := builder.ctxByNode[leftId]
				err = bindCtx.mergeContexts(leftCtx, rightCtx)
				if err != nil {
					return err
				}
				newRootId := builder.appendNode(&plan.Node{
					NodeType: plan.Node_JOIN,
					Children: []int32{leftId, rightId},
					JoinType: plan.Node_LEFT,
				}, bindCtx)
				node := builder.qry.Nodes[newRootId]
				bindCtx.binder = NewTableBinder(builder, bindCtx)
				node.OnList = []*Expr{joinConds}

				info.rootId = newRootId
				info.onDeleteIdxTblName = append(info.onDeleteIdxTblName, [2]string{builder.compCtx.DefaultDatabase(), tblName})
				info.onDeleteIdx = append(info.onDeleteIdx, info.idx)
				info.idx = info.idx + 1
			}
		}
	}

	// rewrite foreign key
	for _, tableId := range tableDef.RefChildTbls {
		err := initDeleteStmt(builder, bindCtx, info)
		if err != nil {
			return err
		}
		_, childTableDef := builder.compCtx.ResolveById(tableId) //opt: actionRef是否也记录到RefChildTbls里？
		for _, fk := range childTableDef.Fkeys {
			if fk.ForeignTbl == tableDef.TblId {
				// append table scan node
				// rightCtx := NewBindContext(builder, bindCtx)
				// astTblName := tree.NewTableName(tree.Identifier(childTableDef.Name), tree.ObjectNamePrefix{})
				// here we get columns: idx_col = 0; pri_col = 1; rowid =2
				// rightId, err := builder.buildTable(astTblName, rightCtx)
				// if err != nil {
				// 	return err
				// }

				// build join conds
				// joinConds := make([]*Expr, len(fk.Cols))
				// var leftExpr *Expr
				// var rightExr *Expr
				// for i, colId := range fk.Cols {
				// 	for _, col := range childTableDef.Cols {
				// 		if col.ColId == colId {
				// 			childColumnName := col.Name
				// 			originColumnName := id2name[fk.ForeignCols[i]]

				// 			// joinConds[i] =
				// 			break
				// 		}
				// 	}
				// }

				// append project
				switch fk.OnDelete {
				case plan.ForeignKeyDef_NO_ACTION, plan.ForeignKeyDef_RESTRICT, plan.ForeignKeyDef_SET_DEFAULT:
					// info.projects += ", " + childTableDef.Name + "." + catalog.Row_ID
					// info.leftJoins += leftJoinStr
					info.onDeleteRestrict = append(info.onDeleteIdx, info.idx)
					info.idx = info.idx + 1
					info.onDeleteRestrictTblId = append(info.onDeleteRestrictTblId, childTableDef.TblId)

				case plan.ForeignKeyDef_CASCADE:
					// info.projects += ", " + childTableDef.Name + "." + catalog.Row_ID
					// info.leftJoins += leftJoinStr
					info.onDeleteCascade = append(info.onDeleteIdx, info.idx)
					info.idx = info.idx + 1
					info.onDeleteCascadeTblId = append(info.onDeleteCascadeTblId, childTableDef.TblId)

					err := rewriteDeleteSelectInfo(builder, bindCtx, childTableDef, info)
					if err != nil {
						return err
					}

				case plan.ForeignKeyDef_SET_NULL:
					idMap := make(map[uint64]struct{})
					for _, colId := range fk.Cols {
						idMap[colId] = struct{}{}
					}
					// info.projects += ", " + childTableDef.Name + "." + catalog.Row_ID
					info.onDeleteSet = append(info.onDeleteIdx, info.idx)
					info.idx = info.idx + 1
					for _, col := range childTableDef.Cols {
						if col.Name == catalog.Row_ID {
							continue
						}
						// if _, ok := idMap[col.ColId]; ok {
						// 	if fk.OnDelete == plan.ForeignKeyDef_SET_NULL {
						// 		info.projects += ", null"
						// 	} else {
						// 		info.projects += ", " + col.Default.OriginString
						// 	}
						// } else {
						// 	info.projects += ", " + childTableDef.Name + "." + col.Name
						// }
						info.onDeleteSet = append(info.onDeleteIdx, info.idx)
						info.idx = info.idx + 1
					}
					// info.leftJoins += leftJoinStr
					info.onDeleteSetTblId = append(info.onDeleteSetTblId, childTableDef.TblId)

					err := rewriteDeleteSelectInfo(builder, bindCtx, childTableDef, info)
					if err != nil {
						return err
					}
				}

				// append join node
			}
		}
	}

	return nil
}

// func rewriteDeleteSelect(ctx CompilerContext, tableDef *TableDef, info *rewriteSelectInfo) error {
// 	// rewrite index
// 	for _, def := range tableDef.Defs {
// 		if idxDef, ok := def.Def.(*plan.TableDef_DefType_UIdx); ok {
// 			for idx, tblName := range idxDef.UIdx.TableNames {
// 				if len(idxDef.UIdx.Fields[idx].Parts) == 1 {
// 					orginIndexColumnName := idxDef.UIdx.Fields[idx].Parts[0]
// 					info.projects += ", `" + tblName + "`." + catalog.Row_ID
// 					leftJoinStr := fmt.Sprintf(" left join `%s` on `%s`.%s = `%s`.%s", tblName, tblName, catalog.IndexTableIndexColName, derivedTableName, orginIndexColumnName)
// 					info.leftJoins += leftJoinStr
// 				} else {
// 					orginIndexColumnNames := ""
// 					prefix := ""
// 					for _, column := range idxDef.UIdx.Fields[idx].Parts {
// 						orginIndexColumnNames += fmt.Sprintf(" `%s`.%s%s", derivedTableName, column, prefix)
// 						prefix = ","
// 					}
// 					info.projects += ", `" + tblName + "`." + catalog.Row_ID
// 					leftJoinStr := fmt.Sprintf(" left join `%s` on `%s`.%s = serial(%s)", tblName, tblName, catalog.IndexTableIndexColName, orginIndexColumnNames)
// 					info.leftJoins += leftJoinStr
// 				}

// 				info.onDeleteIdxTblName = append(info.onDeleteIdxTblName, [2]string{ctx.DefaultDatabase(), tblName})
// 				info.onDeleteIdx = append(info.onDeleteIdx, info.idx)
// 				info.idx = info.idx + 1
// 			}
// 		}
// 	}

// 	// rewrite refChild
// 	id2name := make(map[uint64]string)
// 	for _, col := range tableDef.Cols {
// 		id2name[col.ColId] = col.Name
// 	}

// 	for _, tableId := range tableDef.RefChildTbls {
// 		_, childTableDef := ctx.ResolveById(tableId) //opt: actionRef是否也记录到RefChildTbls里？
// 		for _, fk := range childTableDef.Fkeys {
// 			if fk.ForeignTbl == tableDef.TblId {
// 				leftJoinStr := ""
// 				prefix := fmt.Sprintf(" left join %s on", childTableDef.Name)
// 				for i, colId := range fk.Cols {
// 					childColumnName := ""
// 					originColumnName := ""
// 					for _, col := range childTableDef.Cols {
// 						if col.ColId == colId {
// 							childColumnName = col.Name
// 							originColumnName = id2name[fk.ForeignCols[i]]
// 							break
// 						}
// 					}
// 					leftJoinStr = fmt.Sprintf("%s %s.%s = %s.%s", prefix, childTableDef.Name, childColumnName, derivedTableName, originColumnName)
// 					prefix = ", and"
// 				}

// 				switch fk.OnDelete {
// 				case plan.ForeignKeyDef_CASCADE:
// 					info.projects += ", " + childTableDef.Name + "." + catalog.Row_ID
// 					info.leftJoins += leftJoinStr
// 					info.onDeleteCascade = append(info.onDeleteIdx, info.idx)
// 					info.idx = info.idx + 1
// 					info.onDeleteCascadeTblId = append(info.onDeleteCascadeTblId, childTableDef.TblId)

// 					err := rewriteDeleteSelect(ctx, childTableDef, info)
// 					if err != nil {
// 						return err
// 					}

// 				case plan.ForeignKeyDef_NO_ACTION, plan.ForeignKeyDef_RESTRICT:
// 					info.projects += ", " + childTableDef.Name + "." + catalog.Row_ID
// 					info.leftJoins += leftJoinStr
// 					info.onDeleteRestrict = append(info.onDeleteIdx, info.idx)
// 					info.idx = info.idx + 1
// 					info.onDeleteRestrictTblId = append(info.onDeleteRestrictTblId, childTableDef.TblId)

// 				case plan.ForeignKeyDef_SET_NULL, plan.ForeignKeyDef_SET_DEFAULT:
// 					idMap := make(map[uint64]struct{})
// 					for _, colId := range fk.Cols {
// 						idMap[colId] = struct{}{}
// 					}
// 					info.projects += ", " + childTableDef.Name + "." + catalog.Row_ID
// 					info.onDeleteSet = append(info.onDeleteIdx, info.idx)
// 					info.idx = info.idx + 1
// 					for _, col := range childTableDef.Cols {
// 						if col.Name == catalog.Row_ID {
// 							continue
// 						}
// 						if _, ok := idMap[col.ColId]; ok {
// 							if fk.OnDelete == plan.ForeignKeyDef_SET_NULL {
// 								info.projects += ", null"
// 							} else {
// 								info.projects += ", " + col.Default.OriginString
// 							}
// 						} else {
// 							info.projects += ", " + childTableDef.Name + "." + col.Name
// 						}
// 						info.onDeleteSet = append(info.onDeleteIdx, info.idx)
// 						info.idx = info.idx + 1
// 					}
// 					info.leftJoins += leftJoinStr
// 					info.onDeleteSetTblId = append(info.onDeleteSetTblId, childTableDef.TblId)

// 					err := rewriteDeleteSelect(ctx, childTableDef, info)
// 					if err != nil {
// 						return err
// 					}

// 				}
// 			}
// 		}
// 	}
// 	return nil
// }
