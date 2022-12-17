// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildTableUpdate(stmt *tree.Update, ctx CompilerContext) (*Plan, error) {
	// build map between base table and alias table
	tbinfo := newTableInfo()
	extractWithTable(stmt.With, tbinfo, ctx)
	for _, expr := range stmt.Tables {
		if err := extractExprTable(expr, tbinfo, ctx); err != nil {
			return nil, err
		}
	}

	// Reverse the mapping of aliases to table names
	tbinfo.reverseAliasMap()

	// build reference
	objRefs := make([]*ObjectRef, 0, len(tbinfo.dbNames))
	tblRefs := make([]*TableDef, 0, len(tbinfo.tableNames))
	for i := range tbinfo.dbNames {
		objRef, tblRef := ctx.Resolve(tbinfo.dbNames[i], tbinfo.tableNames[i])
		if tblRef == nil {
			return nil, moerr.NewInternalError(ctx.GetContext(), "cannot find update table: %s.%s", tbinfo.dbNames[i], tbinfo.tableNames[i])
		}
		if tblRef.TableType == catalog.SystemExternalRel {
			return nil, moerr.NewInternalError(ctx.GetContext(), "the external table is not support update operation")
		} else if tblRef.TableType == catalog.SystemViewRel {
			return nil, moerr.NewInternalError(ctx.GetContext(), "view is not support update operation")
		}
		objRefs = append(objRefs, objRef)
		tblRefs = append(tblRefs, tblRef)
	}

	updateTableList, err := buildUpdateTableList(stmt.Exprs, objRefs, tblRefs, tbinfo.alias2BaseNameMap, ctx)
	if err != nil {
		return nil, err
	}

	// build update ctx and projection
	updateCtxs, useProjectExprs, err := buildUpdateProject(updateTableList, ctx)
	if err != nil {
		return nil, err
	}

	//var leftJoinTableExpr *tree.JoinTableExpr

	leftJoinTableExpr := stmt.Tables[0]
	for _, updateTableinfo := range updateTableList.updateTables {
		//objRef := updateTableinfo.objRef
		tableDef := updateTableinfo.tblDef

		// 2.get origin table Name
		originTableName := tableDef.Name

		// 3.get the definition of the unique index of the original table
		unqiueIndexDef, _ := buildIndexDefs(tableDef.Defs)

		if unqiueIndexDef != nil {
			for j := 0; j < len(unqiueIndexDef.TableNames); j++ {
				// build left join with origin table and index table
				indexTableExpr := buildIndexTableExpr(unqiueIndexDef.TableNames[j])
				joinCond := buildJoinOnCond(tbinfo, originTableName, unqiueIndexDef.TableNames[j], unqiueIndexDef.Fields[j])
				leftJoinTableExpr = &tree.JoinTableExpr{
					JoinType: tree.JOIN_TYPE_LEFT,
					Left:     leftJoinTableExpr,
					Right:    indexTableExpr,
					Cond:     joinCond,
				}
			}
		} else {
			continue
		}
	}

	// 4.build FromClause
	fromClause := &tree.From{Tables: tree.TableExprs{leftJoinTableExpr}}

	// build the stmt of select and append select node
	if len(stmt.OrderBy) > 0 && (stmt.Where == nil && stmt.Limit == nil) {
		stmt.OrderBy = nil
	}
	selectStmt := &tree.Select{
		Select: &tree.SelectClause{
			Exprs: useProjectExprs,
			//From:  &tree.From{Tables: stmt.Tables},
			From:  fromClause,
			Where: stmt.Where,
		},
		OrderBy: stmt.OrderBy,
		Limit:   stmt.Limit,
		With:    stmt.With,
	}

	// This line of code is used for debugging and later deletion
	sql := tree.String(selectStmt, dialect.MYSQL)
	fmt.Printf("%s \n", sql)

	usePlan, err := runBuildSelectByBinder(plan.Query_SELECT, ctx, selectStmt)
	if err != nil {
		return nil, err
	}
	usePlan.Plan.(*plan.Plan_Query).Query.StmtType = plan.Query_UPDATE
	qry := usePlan.Plan.(*plan.Plan_Query).Query

	// rebuild projection for update cols to get right type and default value
	lastNode := qry.Nodes[qry.Steps[len(qry.Steps)-1]]
	/*
		for _, ct := range updateCtxs {
			var idx int
			if ct.PriKeyIdx != -1 {
				idx = int(ct.PriKeyIdx) + 1
			} else {
				idx = int(ct.HideKeyIdx) + 1
			}

			for _, col := range ct.UpdateCols {
				if c := lastNode.ProjectList[idx].GetC(); c != nil {
					if c.GetDefaultval() {
						if lastNode.ProjectList[idx], err = getDefaultExpr(col); err != nil {
							return nil, err
						}
						idx++
						continue
					}
					if c.GetUpdateVal() {
						lastNode.ProjectList[idx] = col.OnUpdate.Expr
						idx++
						continue
					}
				}
				lastNode.ProjectList[idx], err = makePlan2CastExpr(lastNode.ProjectList[idx], col.Typ)
				if err != nil {
					return nil, err
				}
				idx++
			}
		}
	*/
	err = alignProjectExprType(lastNode, updateCtxs)
	if err != nil {
		return nil, err
	}
	// build update node
	node := &Node{
		NodeType:    plan.Node_UPDATE,
		ObjRef:      nil,
		TableDef:    nil,
		TableDefVec: tblRefs,
		Children:    []int32{qry.Steps[len(qry.Steps)-1]},
		NodeId:      int32(len(qry.Nodes)),
		UpdateCtxs:  updateCtxs,
	}
	qry.Nodes = append(qry.Nodes, node)
	qry.Steps[len(qry.Steps)-1] = node.NodeId

	return usePlan, nil
}

func alignProjectExprType(node *Node, updateCtxs []*plan.UpdateCtx) error {
	projectList := node.ProjectList
	for _, updateCtx := range updateCtxs {
		//if !updateCtx.isIndexTableUpdate {
		//}
		startPosition := updateCtx.HideKeyIdx // one table rowid index posistion
		for i, updateCol := range updateCtx.UpdateCols {
			offset := startPosition + int32(i) + 1
			if c := projectList[offset].GetC(); c != nil {
				if c.GetDefaultval() {
					expr, err := getDefaultExpr(updateCol)
					if err != nil {
						return err
					}
					projectList[offset] = expr
					continue
				}
				if c.GetUpdateVal() {
					projectList[offset] = updateCol.OnUpdate.Expr
					continue
				}
			}

			expr, err := makePlan2CastExpr(projectList[offset], updateCol.Typ)
			if err != nil {
				return err
			}
			projectList[offset] = expr
		}
	}
	return nil
}

func buildUpdateProject(updateTableList *UpdateTableList, ctx CompilerContext) ([]*plan.UpdateCtx, tree.SelectExprs, error) {
	var useProjectExprs tree.SelectExprs
	var offset int32 = 0

	updateCtxs := make([]*plan.UpdateCtx, 0)

	for _, updateTableinfo := range updateTableList.updateTables {
		updateCols := updateTableinfo.updateCols

		expr, err := buildRowIdAstExpr(ctx, nil, updateTableinfo.objRef.SchemaName, updateTableinfo.tblDef.Name)
		if err != nil {
			return nil, nil, err
		}
		useProjectExprs = append(useProjectExprs, expr)
		hideKeyIdx := offset

		// construct projection for list of update expr
		for _, updateExpr := range updateTableinfo.updateExprs {
			useProjectExprs = append(useProjectExprs, tree.SelectExpr{Expr: updateExpr})
		}

		// construct other cols and table offset
		var otherAttrs []string = nil

		// figure out other cols that will not be updated
		var onUpdateCols []updateCol

		//orderAttrs := getOrderedColNames(tblDefs[k])
		orderAttrs := make([]string, 0, len(updateTableinfo.tblDef.Cols))

		for _, col := range updateTableinfo.tblDef.Cols {
			if col.Name == catalog.Row_ID {
				continue
			}
			orderAttrs = append(orderAttrs, col.Name)

			isUpdateCol := false
			for _, updateCol := range updateCols {
				if updateCol.colDef.Name == col.Name {
					isUpdateCol = true
				}
			}
			if !isUpdateCol {
				if col.OnUpdate != nil && col.OnUpdate.Expr != nil {
					onUpdateCols = append(onUpdateCols, updateCol{colDef: col})
				} else {
					otherAttrs = append(otherAttrs, col.Name)
				}
			}
		}

		//-------------------
		//offset += int32(len(orderAttrs)) + 1
		//
		//uDef, _ := buildIndexDefs(updateTableinfo.tblDef.Defs)
		//if uDef != nil {
		//	updateCtx, exprs, err := buildIndexTableUpdateCtx(updateTableinfo.objRef, updateTableinfo.tblDef, uDef, offset, ctx)
		//	if err != nil {
		//		return nil, nil, err
		//	}
		//	updateCtxs = append(updateCtxs, updateCtx...)
		//	useProjectExprs = append(useProjectExprs, exprs...)
		//}
		//----------------------
		ct := &plan.UpdateCtx{
			DbName:     updateCols[0].dbName,
			TblName:    updateCols[0].tblName,
			HideKey:    catalog.Row_ID,
			HideKeyIdx: hideKeyIdx,
			OtherAttrs: otherAttrs,
			OrderAttrs: orderAttrs,
			//isIndexTableUpdate: false
		}
		for _, u := range updateCols {
			ct.UpdateCols = append(ct.UpdateCols, u.colDef)
		}
		for _, u := range onUpdateCols {
			ct.UpdateCols = append(ct.UpdateCols, u.colDef)
			useProjectExprs = append(useProjectExprs, tree.SelectExpr{Expr: &tree.UpdateVal{}})
		}
		for _, o := range otherAttrs {
			e, _ := tree.NewUnresolvedName(ctx.GetContext(), updateCols[0].aliasTblName, o)
			useProjectExprs = append(useProjectExprs, tree.SelectExpr{Expr: e})
		}
		updateCtxs = append(updateCtxs, ct)

		offset += int32(len(orderAttrs)) + 1
		uDef, _ := buildIndexDefs(updateTableinfo.tblDef.Defs)
		if uDef != nil {
			updateCtx, exprs, err := buildIndexTableUpdateCtx(updateTableinfo.objRef, updateTableinfo.tblDef, uDef, offset, ctx)
			if err != nil {
				return nil, nil, err
			}
			updateCtxs = append(updateCtxs, updateCtx...)
			useProjectExprs = append(useProjectExprs, exprs...)
		}
	}
	return updateCtxs, useProjectExprs, nil
}

// build the update context and projected columns of the index table
func buildIndexTableUpdateCtx(objRef *ObjectRef, tableDef *TableDef, uDef *UniqueIndexDef, offset int32, ctx CompilerContext) ([]*plan.UpdateCtx, tree.SelectExprs, error) {
	var selectList tree.SelectExprs
	indexUpCtxs := make([]*plan.UpdateCtx, 0)
	schemaName := objRef.SchemaName

	for i, _ := range uDef.IndexNames {
		if uDef.TableExists[i] {
			indexTableName := uDef.TableNames[i]
			//indexField := uDef.Fields[i]

			// Get the definition information of the index table
			idxObjRef, idxTblDef := ctx.Resolve(schemaName, indexTableName)
			rowidExpr, err := buildRowIdAstExpr(ctx, nil, idxObjRef.SchemaName, indexTableName)
			if err != nil {
				return nil, nil, err
			}
			selectList = append(selectList, rowidExpr)

			// get the ordered string array of the table explicit column
			orderAttrs := getOrderedColNames(idxTblDef)

			updatectx := &plan.UpdateCtx{
				DbName:     schemaName,
				TblName:    indexTableName,
				HideKey:    catalog.Row_ID,
				HideKeyIdx: offset,
				UpdateCols: idxTblDef.Cols,
				OtherAttrs: nil,
				OrderAttrs: orderAttrs,
				//isIndexTableUpdate: true
			}
			indexUpCtxs = append(indexUpCtxs, updatectx)
		} else {
			continue
		}
	}
	return indexUpCtxs, selectList, nil
}

// Get the sorting string array of the table explicit column according to the table definition
func getOrderedColNames(tabledef *TableDef) []string {
	orderAttrs := make([]string, 0, len(tabledef.Cols))
	for _, col := range tabledef.Cols {
		// This code may be redundant, and rowid may not be included in Cols,you should check
		if col.Name == catalog.Row_ID {
			continue
		}
		orderAttrs = append(orderAttrs, col.Name)
	}
	return orderAttrs
}

// Check whether the index table needs to be updated
func checkIndexTableNeedUpdate() bool {
	return true
}

func buildUpdateTableList(exprs tree.UpdateExprs, objRefs []*ObjectRef, tblDefs []*TableDef, baseNameMap map[string]string, ctx CompilerContext) (*UpdateTableList, error) {
	tableUpdateInfoMap := make(map[string]*updateTableInfo)

	// The map of columns to be updated of all different tables explicitly specified in the Update statement.
	// The key is the column name with the table name prefix,such as: 't1.col'
	colCountMap := make(map[string]int)
	for _, expr := range exprs {
		dbName, tableName, columnName := expr.Names[0].GetNames()
		// check dbName
		if dbName != "" {
			hasFindDbName := false
			for i := range objRefs {
				if objRefs[i].SchemaName == dbName {
					hasFindDbName = true
					if tableName != tblDefs[i].Name {
						return nil, moerr.NewInternalError(ctx.GetContext(), "cannot find update table of %s in database of %s", tableName, dbName)
					}
				}
			}
			if !hasFindDbName {
				return nil, moerr.NewInternalError(ctx.GetContext(), "cannot find update database of %s", dbName)
			}
			//upCol.dbName = dbName
		}

		var upCol updateCol
		// check tableName
		if tableName != "" {
			realTableName, ok := baseNameMap[tableName]
			if !ok {
				return nil, moerr.NewInternalError(ctx.GetContext(), "the target table %s of the UPDATE is not updatable", tableName)
			}
			hasFindTableName := false
			var tblDef *TableDef
			var objRef *ObjectRef
			for i := range tblDefs {
				// table reference
				if tblDefs[i].Name == realTableName {
					hasFindTableName = true
					tblDef = tblDefs[i]
					objRef = objRefs[i]

					hasFindColumnName := false
					for _, col := range tblDefs[i].Cols {
						if col.Name == columnName {
							hasFindColumnName = true

							colName := fmt.Sprintf("%s.%s", realTableName, columnName)
							if cnt, ok := colCountMap[colName]; ok && cnt > 0 {
								return nil, moerr.NewInternalError(ctx.GetContext(), "the target column %s.%s of the UPDATE is duplicate", tableName, columnName)
							} else {
								colCountMap[colName]++
							}

							upCol.colDef = col
							break
						}
					}
					if !hasFindColumnName {
						return nil, moerr.NewInternalError(ctx.GetContext(), "the target column %s.%s of the UPDATE is ambiguous", tableName, columnName)
					}
					upCol.dbName = objRefs[i].SchemaName
					upCol.tblName = realTableName
					upCol.aliasTblName = tableName

					break
				}
			}

			if !hasFindTableName {
				return nil, moerr.NewInternalError(ctx.GetContext(), "the target table %s of the UPDATE is not updatable", tableName)
			}

			updateInfo, ok := tableUpdateInfoMap[tblDef.Name]
			if ok {
				updateInfo.updateCols = append(updateInfo.updateCols, upCol)
				updateInfo.updateExprs = append(updateInfo.updateExprs, expr.Expr)
			} else {
				updateCols := make([]updateCol, 0)
				updateCols = append(updateCols, upCol)

				updateExprs := make(tree.Exprs, 0)
				updateExprs = append(updateExprs, expr.Expr)

				info := &updateTableInfo{
					objRef:             objRef,
					tblDef:             tblDef,
					useKey:             nil, // To be supplemented,TODO
					colIndex:           -1,  // To be supplemented,TODO
					updateCols:         updateCols,
					updateExprs:        updateExprs,
					isIndexTableUpdate: false,
				}
				tableUpdateInfoMap[tblDef.Name] = info
			}
		} else {
			for i := range tblDefs {
				for _, col := range tblDefs[i].Cols {
					if col.Name == columnName {
						colName := fmt.Sprintf("%s.%s", tblDefs[i].Name, columnName)
						if cnt, ok := colCountMap[colName]; ok && cnt > 0 {
							return nil, moerr.NewInternalError(ctx.GetContext(), "the target column %s of the UPDATE is ambiguous", columnName)
						} else {
							colCountMap[colName]++
						}

						upCol.dbName = objRefs[i].SchemaName
						upCol.tblName = tblDefs[i].Name
						upCol.aliasTblName = tblDefs[i].Name
						upCol.colDef = col

						updateInfo, ok := tableUpdateInfoMap[tblDefs[i].Name]
						if ok {
							updateInfo.updateCols = append(updateInfo.updateCols, upCol)
							updateInfo.updateExprs = append(updateInfo.updateExprs, expr.Expr)
						} else {
							updateCols := make([]updateCol, 0)
							updateCols = append(updateCols, upCol)

							updateExprs := make(tree.Exprs, 0)
							updateExprs = append(updateExprs, expr.Expr)

							info := &updateTableInfo{
								objRef:             objRefs[i],
								tblDef:             tblDefs[i],
								useKey:             nil, // To be supplemented,TODO
								colIndex:           -1,  // To be supplemented,TODO
								updateCols:         updateCols,
								updateExprs:        updateExprs,
								isIndexTableUpdate: false,
							}
							tableUpdateInfoMap[tblDefs[i].Name] = info
						}
						break
					}
				}
			}
			if upCol.tblName == "" {
				return nil, moerr.NewInternalError(ctx.GetContext(), "the target column %s is not exists", columnName)
			}
		}
	}

	updateTableList := NewUpdateTableList()
	for _, info := range tableUpdateInfoMap {
		updateTableList.updateTables = append(updateTableList.updateTables, info)
	}
	return updateTableList, nil
}

// UpdateTableList: information list of tables to be updated
type UpdateTableList struct {
	updateTables []*updateTableInfo // table information list to be updated
	selectList   tree.SelectExprs
	nextIndex    int
}

func NewUpdateTableList() *UpdateTableList {
	return &UpdateTableList{
		updateTables: make([]*updateTableInfo, 0),
		nextIndex:    0,
	}
}

// table information to be updated (original table and index table)
type updateTableInfo struct {
	objRef             *ObjectRef
	tblDef             *TableDef
	useKey             *ColDef // The column used when deletion(dml), currently, it is based on '__row_id' column
	colIndex           int32
	updateCols         []updateCol // The columns of the table that will be updated in the table
	updateExprs        tree.Exprs  // The exec expression of the column which will be updated in the table
	isIndexTableUpdate bool        // Identify whether the current table is an index table
}

//-----------------

// Add the information element[updateTableInfo] of the table which will be update
func (list *UpdateTableList) AddElement(objRef *ObjectRef, tableDef *TableDef, expr tree.SelectExpr, isIndexTableUpdate bool) {
	updateInfo := &updateTableInfo{
		objRef:             objRef,
		tblDef:             tableDef,
		useKey:             nil,
		colIndex:           int32(list.nextIndex),
		isIndexTableUpdate: isIndexTableUpdate,
	}
	list.updateTables = append(list.updateTables, updateInfo)
	list.selectList = append(list.selectList, expr)
	list.nextIndex++
}

func buildUpdateCtxAndProjectionNew(updateColsArray [][]updateCol, updateExprsArray []tree.Exprs, objRefs []*ObjectRef, tblDefs []*TableDef, ctx CompilerContext) ([]*plan.UpdateCtx, tree.SelectExprs, error) {
	var useProjectExprs tree.SelectExprs
	updateCtxs := make([]*plan.UpdateCtx, 0)
	var offset int32 = 0

	for i, updateCols := range updateColsArray {
		expr, err := buildRowIdAstExpr(ctx, nil, updateCols[0].dbName, updateCols[0].tblName)
		if err != nil {
			return nil, nil, err
		}
		useProjectExprs = append(useProjectExprs, expr)
		hideKeyIdx := offset

		// construct projection for list of update expr
		for _, updateExpr := range updateExprsArray[i] {
			useProjectExprs = append(useProjectExprs, tree.SelectExpr{Expr: updateExpr})
		}

		// construct other cols and table offset
		var otherAttrs []string = nil
		var k int

		// get table reference index
		for k = 0; k < len(tblDefs); k++ {
			if updateCols[0].tblName == tblDefs[k].Name {
				break
			}
		}

		// figure out other cols that will not be updated
		var onUpdateCols []updateCol

		//orderAttrs := getOrderedColNames(tblDefs[k])
		orderAttrs := make([]string, 0, len(tblDefs[k].Cols))

		for _, col := range tblDefs[k].Cols {
			orderAttrs = append(orderAttrs, col.Name)

			isUpdateCol := false
			for _, updateCol := range updateCols {
				if updateCol.colDef.Name == col.Name {
					isUpdateCol = true
				}
			}
			if !isUpdateCol {
				if col.OnUpdate != nil && col.OnUpdate.Expr != nil {
					onUpdateCols = append(onUpdateCols, updateCol{colDef: col})
				} else {
					otherAttrs = append(otherAttrs, col.Name)
				}
			}
		}

		offset += int32(len(orderAttrs)) + 1
		uDef, _ := buildIndexDefs(tblDefs[k].Defs)
		if uDef != nil {
			updateCtx, exprs, err := buildIndexTableUpdateCtx(objRefs[k], tblDefs[k], uDef, offset, ctx)
			if err != nil {
				return nil, nil, err
			}
			updateCtxs = append(updateCtxs, updateCtx...)
			useProjectExprs = append(useProjectExprs, exprs...)
		}

		ct := &plan.UpdateCtx{
			DbName:     updateCols[0].dbName,
			TblName:    updateCols[0].tblName,
			HideKey:    catalog.Row_ID,
			HideKeyIdx: hideKeyIdx,
			OtherAttrs: otherAttrs,
			OrderAttrs: orderAttrs,
			//isIndexTableUpdate: false
		}
		for _, u := range updateCols {
			ct.UpdateCols = append(ct.UpdateCols, u.colDef)
		}
		for _, u := range onUpdateCols {
			ct.UpdateCols = append(ct.UpdateCols, u.colDef)
			useProjectExprs = append(useProjectExprs, tree.SelectExpr{Expr: &tree.UpdateVal{}})
		}
		for _, o := range otherAttrs {
			e, _ := tree.NewUnresolvedName(ctx.GetContext(), updateCols[0].aliasTblName, o)
			useProjectExprs = append(useProjectExprs, tree.SelectExpr{Expr: e})
		}
		updateCtxs = append(updateCtxs, ct)
	}
	return updateCtxs, useProjectExprs, nil
}
