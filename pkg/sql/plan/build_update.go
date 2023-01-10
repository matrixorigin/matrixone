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
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"strings"
)

type tableInfo struct {
	dbNames           []string
	tableNames        []string
	alias2BaseNameMap map[string]string // key: alias name, value: base name
	baseName2AliasMap map[string]string //  key: base name, value: alias name, reverse asName2BaseNameMap
}

func newTableInfo() *tableInfo {
	return &tableInfo{
		alias2BaseNameMap: make(map[string]string),
		baseName2AliasMap: make(map[string]string),
	}
}

// reverse the map of alias <-> tableName into baseName2AliasMap
func (tblInfo *tableInfo) reverseAliasMap() {
	for k, v := range tblInfo.alias2BaseNameMap {
		tblInfo.baseName2AliasMap[v] = k
	}
}

type updateCol struct {
	dbName       string
	tblName      string
	aliasTblName string
	colDef       *ColDef
}

// UpdateTableList: information list of tables to be updated
type UpdateTableList struct {
	updateTables []*updateTableInfo // table information list to be updated
	//selectList   tree.SelectExprs
}

func NewUpdateTableList() *UpdateTableList {
	return &UpdateTableList{
		updateTables: make([]*updateTableInfo, 0),
	}
}

// table information to be updated (original table and index table)
type updateTableInfo struct {
	objRef             *ObjectRef
	tblDef             *TableDef
	updateCols         []updateCol // The columns of the table that will be updated in the table
	updateExprs        tree.Exprs  // The exec expression of the column which will be updated in the table
	isIndexTableUpdate bool        // Identify whether the current table is an index table
}

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
		if util.TableIsClusterTable(tblRef.GetTableType()) && ctx.GetAccountId() != catalog.System_Account {
			return nil, moerr.NewInternalError(ctx.GetContext(), "only the sys account can update the cluster table %s", tblRef.GetName())
		}
	}
	//1. get the updated list information of the original table
	updateTableList, err := buildUpdateTableList(stmt.Exprs, objRefs, tblRefs, tbinfo, ctx)
	if err != nil {
		return nil, err
	}

	//2. build update ctx and projection
	updateCtxs, tableDefs, useProjectExprs, err := buildUpdateProject(updateTableList, tbinfo, ctx)
	if err != nil {
		return nil, err
	}

	leftJoinTableExpr := stmt.Tables[0]
	for _, updateTableinfo := range updateTableList.updateTables {
		tableDef := updateTableinfo.tblDef
		// 3.get origin table Name
		originTableName := tableDef.Name

		// 4.get the definition of the unique index of the original table
		unqiueIndexDef, _ := buildIndexDefs(tableDef.Defs)

		if unqiueIndexDef != nil {
			for j := 0; j < len(unqiueIndexDef.TableNames); j++ {
				// check whether the the index table needs to be updated and whether the index exists
				if checkIndexTableNeedUpdate(unqiueIndexDef.Fields[j], updateTableinfo) && unqiueIndexDef.TableExists[j] {
					// build left join with origin table and index table
					indexTableExpr := buildIndexTableExpr(unqiueIndexDef.TableNames[j])
					joinCond := buildJoinOnCond(tbinfo, originTableName, unqiueIndexDef.TableNames[j], unqiueIndexDef.Fields[j])
					leftJoinTableExpr = &tree.JoinTableExpr{
						JoinType: tree.JOIN_TYPE_LEFT,
						Left:     leftJoinTableExpr,
						Right:    indexTableExpr,
						Cond:     joinCond,
					}
				} else {
					continue
				}
			}
		} else {
			continue
		}
	}

	// 5.build FromClause
	fromClause := &tree.From{Tables: tree.TableExprs{leftJoinTableExpr}}

	//6. build the stmt of select and append select node
	if len(stmt.OrderBy) > 0 && (stmt.Where == nil && stmt.Limit == nil) {
		stmt.OrderBy = nil
	}
	selectStmt := &tree.Select{
		Select: &tree.SelectClause{
			Exprs: useProjectExprs,
			From:  fromClause,
			Where: stmt.Where,
		},
		OrderBy: stmt.OrderBy,
		Limit:   stmt.Limit,
		With:    stmt.With,
	}

	usePlan, err := runBuildSelectByBinder(plan.Query_SELECT, ctx, selectStmt)
	if err != nil {
		return nil, err
	}
	usePlan.Plan.(*plan.Plan_Query).Query.StmtType = plan.Query_UPDATE
	qry := usePlan.Plan.(*plan.Plan_Query).Query

	// rebuild projection for update cols to get right type and default value
	lastNode := qry.Nodes[qry.Steps[len(qry.Steps)-1]]
	err = alignProjectExprType(lastNode, updateCtxs, ctx)
	if err != nil {
		return nil, err
	}
	// build update node
	node := &Node{
		NodeType:    plan.Node_UPDATE,
		ObjRef:      nil,
		TableDef:    nil,
		TableDefVec: tableDefs,
		Children:    []int32{qry.Steps[len(qry.Steps)-1]},
		NodeId:      int32(len(qry.Nodes)),
		UpdateCtxs:  updateCtxs,
	}
	qry.Nodes = append(qry.Nodes, node)
	qry.Steps[len(qry.Steps)-1] = node.NodeId

	return usePlan, nil
}

// Align the projection column expression to the target column type
func alignProjectExprType(node *Node, updateCtxs []*plan.UpdateCtx, ctx CompilerContext) error {
	projectList := node.ProjectList
	for _, updateCtx := range updateCtxs {
		if updateCtx.IsIndexTableUpdate {
			continue
		} else {
			startPosition := updateCtx.HideKeyIdx // table rowid index posistion
			for i, updateCol := range updateCtx.UpdateCols {
				offset := startPosition + int32(i) + 1
				if c := projectList[offset].GetC(); c != nil {
					if c.GetDefaultval() {
						expr, err := getDefaultExpr(ctx.GetContext(), updateCol)
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

				expr, err := makePlan2CastExpr(ctx.GetContext(), projectList[offset], updateCol.Typ)
				if err != nil {
					return err
				}
				projectList[offset] = expr
			}
		}
	}
	return nil
}

// Build projection list for table updates
func buildUpdateProject(updateTableList *UpdateTableList, tbinfo *tableInfo, ctx CompilerContext) ([]*plan.UpdateCtx, []*TableDef, tree.SelectExprs, error) {
	var useProjectExprs tree.SelectExprs
	var offset int32 = 0

	updateCtxs := make([]*plan.UpdateCtx, 0)
	tableDefs := make([]*TableDef, 0)

	for _, updateTableinfo := range updateTableList.updateTables {
		updateCols := updateTableinfo.updateCols

		expr, err := buildRowIdAstExpr(ctx, tbinfo, updateTableinfo.objRef.SchemaName, updateTableinfo.tblDef.Name)
		if err != nil {
			return nil, nil, nil, err
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

		ct := &plan.UpdateCtx{
			DbName:             updateCols[0].dbName,
			TblName:            updateCols[0].tblName,
			HideKey:            catalog.Row_ID,
			HideKeyIdx:         hideKeyIdx,
			OtherAttrs:         otherAttrs,
			OrderAttrs:         orderAttrs,
			IsIndexTableUpdate: false,
			CompositePkey:      updateTableinfo.tblDef.CompositePkey,
			ClusterByDef:       updateTableinfo.tblDef.ClusterBy,
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
		tableDefs = append(tableDefs, updateTableinfo.tblDef)

		offset += int32(len(orderAttrs)) + 1
		uDef, _ := buildIndexDefs(updateTableinfo.tblDef.Defs)
		if uDef != nil {
			idxUpdateCtxs, idxTableDefs, exprs, err := buildIndexTableUpdateCtx(updateTableinfo, uDef, &offset, ctx)
			if err != nil {
				return nil, nil, nil, err
			}
			for i := range idxUpdateCtxs {
				pos := len(updateCtxs)
				ct.UniqueIndexPos = append(ct.UniqueIndexPos, int32(pos))
				updateCtxs = append(updateCtxs, idxUpdateCtxs[i])
				tableDefs = append(tableDefs, idxTableDefs[i])
				useProjectExprs = append(useProjectExprs, exprs[i])
			}
		}
	}
	return updateCtxs, tableDefs, useProjectExprs, nil
}

// build the update context and projected columns of the index table
func buildIndexTableUpdateCtx(updateTabInfo *updateTableInfo, uDef *UniqueIndexDef, offset *int32, ctx CompilerContext) ([]*plan.UpdateCtx, []*TableDef, tree.SelectExprs, error) {
	var selectList tree.SelectExprs
	indexUpCtxs := make([]*plan.UpdateCtx, 0)
	indexTableDefs := make([]*TableDef, 0)

	for i := range uDef.IndexNames {
		// check whether the the index table needs to be updated and whether the index exists
		if checkIndexTableNeedUpdate(uDef.Fields[i], updateTabInfo) && uDef.TableExists[i] {
			indexTableName := uDef.TableNames[i]

			// Get the definition information of the index table
			idxObjRef, idxTblDef := ctx.Resolve(updateTabInfo.objRef.SchemaName, indexTableName)
			rowidExpr, err := buildRowIdAstExpr(ctx, nil, idxObjRef.SchemaName, indexTableName)
			if err != nil {
				return nil, nil, nil, err
			}
			selectList = append(selectList, rowidExpr)

			// get the ordered string array of the table explicit column
			orderAttrs := getOrderedColNames(idxTblDef)

			updatectx := &plan.UpdateCtx{
				DbName:             updateTabInfo.objRef.SchemaName,
				TblName:            indexTableName,
				HideKey:            catalog.Row_ID,
				HideKeyIdx:         *offset,
				UpdateCols:         idxTblDef.Cols,
				OtherAttrs:         nil,
				OrderAttrs:         orderAttrs,
				IsIndexTableUpdate: true,
				IndexParts:         uDef.Fields[i].Parts,
			}
			indexUpCtxs = append(indexUpCtxs, updatectx)
			indexTableDefs = append(indexTableDefs, idxTblDef)
			(*offset)++
		} else {
			continue
		}
	}
	return indexUpCtxs, indexTableDefs, selectList, nil
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
func checkIndexTableNeedUpdate(field *plan.Field, updateTabInfo *updateTableInfo) bool {
	var names []string
	if updateTabInfo.tblDef.CompositePkey != nil {
		names = util.SplitCompositePrimaryKeyColumnName(updateTabInfo.tblDef.CompositePkey.Name)
	}

	for _, updateCol := range updateTabInfo.updateCols {
		for _, part := range field.Parts {
			if strings.EqualFold(updateCol.colDef.Name, part) || updateCol.colDef.Primary {
				return true
			}
			if updateCol.colDef.Primary {
				return true
			}
		}
		if updateTabInfo.tblDef.CompositePkey != nil {
			for _, name := range names {
				if strings.EqualFold(updateCol.colDef.Name, name) {
					return true
				}
			}
		}
	}
	return false
}

func buildUpdateTableList(exprs tree.UpdateExprs, objRefs []*ObjectRef, tblDefs []*TableDef, tbinfo *tableInfo, ctx CompilerContext) (*UpdateTableList, error) {
	// The map of columns to be updated of all different tables explicitly specified in the Update statement.
	// The key is the column name with the table name prefix,such as: 't1.col'
	colCountMap := make(map[string]int)

	tableUpdateInfoMap := make(map[string]*updateTableInfo)
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
		}

		var upCol updateCol
		// check tableName
		if tableName != "" {
			realTableName, ok := tbinfo.alias2BaseNameMap[tableName]
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
						upCol.aliasTblName = tbinfo.baseName2AliasMap[tblDefs[i].Name]
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

func extractSelectTable(stmt tree.TableExpr, tblName, dbName *string, ctx CompilerContext) {
	switch s := stmt.(type) {
	case *tree.Select:
		if clause, ok := s.Select.(*tree.SelectClause); ok {
			if len(clause.From.Tables) == 1 {
				extractSelectTable(clause.From.Tables[0], tblName, dbName, ctx)
			}
		}
	case *tree.ParenSelect:
		if clause, ok := s.Select.Select.(*tree.SelectClause); ok {
			if len(clause.From.Tables) == 1 {
				extractSelectTable(clause.From.Tables[0], tblName, dbName, ctx)
			}
		}
	case *tree.ParenTableExpr:
		extractSelectTable(s.Expr, tblName, dbName, ctx)
	case *tree.AliasedTableExpr:
		if s.As.Cols != nil {
			return
		}
		extractSelectTable(s.Expr, tblName, dbName, ctx)
	case *tree.TableName:
		*dbName = string(s.SchemaName)
		*tblName = string(s.ObjectName)
		if *dbName == "" {
			*dbName = ctx.DefaultDatabase()
		}
	case *tree.JoinTableExpr:
		if s.Right == nil {
			extractSelectTable(s.Left, tblName, dbName, ctx)
		}
	}
}

func extractWithTable(with *tree.With, tf *tableInfo, ctx CompilerContext) {
	if with == nil {
		return
	}
	for _, cte := range with.CTEs {
		if cte.Name.Cols != nil {
			continue
		}
		tblName := new(string)
		dbName := new(string)
		switch s := cte.Stmt.(type) {
		case *tree.Select:
			if clause, ok := s.Select.(*tree.SelectClause); ok {
				if len(clause.From.Tables) == 1 {
					extractSelectTable(clause.From.Tables[0], tblName, dbName, ctx)
				}
			}
		case *tree.ParenSelect:
			if clause, ok := s.Select.Select.(*tree.SelectClause); ok {
				if len(clause.From.Tables) == 1 {
					extractSelectTable(clause.From.Tables[0], tblName, dbName, ctx)
				}
			}
		}
		if *tblName != "" {
			tf.tableNames = append(tf.tableNames, *tblName)
			tf.dbNames = append(tf.dbNames, *dbName)
			tf.alias2BaseNameMap[string(cte.Name.Alias)] = *tblName
		}
	}
}

func extractExprTable(expr tree.TableExpr, tf *tableInfo, ctx CompilerContext) error {
	switch t := expr.(type) {
	case *tree.TableName:
		tblName := string(t.ObjectName)
		if _, ok := tf.alias2BaseNameMap[tblName]; ok {
			return nil
		}
		dbName := string(t.SchemaName)
		if dbName == "" {
			dbName = ctx.DefaultDatabase()
		}
		tf.tableNames = append(tf.tableNames, tblName)
		tf.dbNames = append(tf.dbNames, dbName)
		tf.alias2BaseNameMap[tblName] = tblName
		return nil
	case *tree.AliasedTableExpr:
		tn, ok := t.Expr.(*tree.TableName)
		if !ok {
			return nil
		}
		if t.As.Cols != nil {
			return moerr.NewInternalError(ctx.GetContext(), "syntax error at %s", tree.String(t, dialect.MYSQL))
		}
		tblName := string(tn.ObjectName)
		if _, ok := tf.alias2BaseNameMap[tblName]; ok {
			return nil
		}
		dbName := string(tn.SchemaName)
		if dbName == "" {
			dbName = ctx.DefaultDatabase()
		}
		tf.tableNames = append(tf.tableNames, tblName)
		tf.dbNames = append(tf.dbNames, dbName)
		asName := string(t.As.Alias)
		if asName != "" {
			tf.alias2BaseNameMap[asName] = tblName
		} else {
			tf.alias2BaseNameMap[tblName] = tblName
		}
		return nil
	case *tree.JoinTableExpr:
		if err := extractExprTable(t.Left, tf, ctx); err != nil {
			return err
		}
		if t.Right != nil {
			return extractExprTable(t.Right, tf, ctx)
		}
	}
	return nil
}

func isSameColumnType(t1 *Type, t2 *Type) bool {
	if t1.Id != t2.Id {
		return false
	}
	if t1.Width == t2.Width && t1.Precision == t2.Precision && t1.Size == t2.Size && t1.Scale == t2.Scale {
		return true
	}
	return true
}
