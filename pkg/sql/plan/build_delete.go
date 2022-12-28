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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"strings"
)

func buildDelete(stmt *tree.Delete, ctx CompilerContext) (*Plan, error) {
	if len(stmt.Tables) == 1 && stmt.TableRefs == nil {
		return buildSingleTableDelete(ctx, stmt)
	}
	return buildMultTableDelete(ctx, stmt)
}

// When the original table contains an index table of multi table deletion statement, build a multi table join query
// execution plan to query the rowid of the original table and the index table respectively
// link ref: https://dev.mysql.com/doc/refman/8.0/en/delete.html
func buildMultTableDelete(ctx CompilerContext, stmt *tree.Delete) (*Plan, error) {
	// build map between base table and alias table
	tbinfo := &tableInfo{
		alias2BaseNameMap: make(map[string]string),
		baseName2AliasMap: make(map[string]string),
	}

	for _, expr := range stmt.TableRefs {
		if err := extractExprTable(expr, tbinfo, ctx); err != nil {
			return nil, err
		}
	}

	// check database's name and table's name
	tbs := getTableNames(stmt.Tables)
	// get deleted origin table count
	tableCount := len(tbs)
	objRefs := make([]*ObjectRef, tableCount)
	tableDefs := make([]*TableDef, tableCount)
	for i, t := range tbs {
		dbName := string(t.SchemaName)
		if dbName == "" {
			dbName = ctx.DefaultDatabase()
		}
		tblName := string(t.ObjectName)
		if _, ok := tbinfo.alias2BaseNameMap[tblName]; ok {
			tblName = tbinfo.alias2BaseNameMap[tblName]
		}
		objRefs[i], tableDefs[i] = ctx.Resolve(dbName, tblName)
		if tableDefs[i] == nil {
			return nil, moerr.NewInvalidInput(ctx.GetContext(), "delete has no table ref")
		}
		if tableDefs[i].TableType == catalog.SystemExternalRel {
			return nil, moerr.NewInvalidInput(ctx.GetContext(), "cannot delete from external table")
		} else if tableDefs[i].TableType == catalog.SystemViewRel {
			return nil, moerr.NewInvalidInput(ctx.GetContext(), "cannot delete from view")
		}
		if util.TableIsClusterTable(tableDefs[i].GetTableType()) && ctx.GetAccountId() != catalog.System_Account {
			return nil, moerr.NewInternalError(ctx.GetContext(), "only the sys account can delete the cluster table %s", tableDefs[i].GetName())
		}
	}

	// Reverse the mapping of aliases to table names
	tbinfo.reverseAliasMap()
	for _, t := range tbs {
		tblName := string(t.ObjectName)
		if _, ok := tbinfo.baseName2AliasMap[tblName]; !ok {
			if _, ok := tbinfo.alias2BaseNameMap[tblName]; !ok {
				return nil, moerr.NewInvalidInput(ctx.GetContext(), "Unknown table '%v' in MULTI DELETE", tblName)
			}
		}
	}

	leftJoinTableExpr := stmt.TableRefs[0]

	deleteTableList := NewDeleteTableList()

	for i := 0; i < tableCount; i++ {
		// 1.build select list exprs
		objRef := objRefs[i]
		tableDef := tableDefs[i]
		err := buildDeleteProjection(objRef, tableDef, tbinfo, deleteTableList, ctx)
		if err != nil {
			return nil, err
		}

		// 2.get origin table Name
		originTableName := tbinfo.alias2BaseNameMap[string(tbs[i].ObjectName)]

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

	// 5.build complete query clause abstract syntax tree for delete
	selectStmt := &tree.Select{
		Select: &tree.SelectClause{
			Exprs: deleteTableList.selectList, // select list
			From:  fromClause,                 // table and left join
			Where: stmt.Where,                 //append where clause
		},
		OrderBy: stmt.OrderBy,
		Limit:   stmt.Limit,
		With:    stmt.With,
	}

	// 6.build query sub plan corresponding to delete statement
	subplan, err := runBuildSelectByBinder(plan.Query_SELECT, ctx, selectStmt)
	if err != nil {
		return nil, err
	}
	subplan.Plan.(*plan.Plan_Query).Query.StmtType = plan.Query_DELETE
	qry := subplan.Plan.(*plan.Plan_Query).Query

	// build multi table delete Context
	delCtxs := make([]*plan.DeleteTableCtx, len(deleteTableList.delTables))
	for i := 0; i < len(deleteTableList.delTables); i++ {
		delCtxs[i] = &plan.DeleteTableCtx{
			DbName:             deleteTableList.delTables[i].objRefs.SchemaName,
			TblName:            deleteTableList.delTables[i].tblDefs.Name,
			UseDeleteKey:       catalog.Row_ID, // Confirm whether this field is useful
			CanTruncate:        false,
			ColIndex:           deleteTableList.delTables[i].colIndex,
			IsIndexTableDelete: deleteTableList.delTables[i].isIndexTableDelete,
		}
	}

	// build delete node
	node := &Node{
		NodeType:        plan.Node_DELETE,
		ObjRef:          nil,
		TableDef:        nil,
		Children:        []int32{qry.Steps[len(qry.Steps)-1]},
		NodeId:          int32(len(qry.Nodes)),
		DeleteTablesCtx: delCtxs,
	}
	qry.Nodes = append(qry.Nodes, node)
	qry.Steps[len(qry.Steps)-1] = node.NodeId
	return subplan, nil
}

// When the original table contains an index table of single table deletion statement, build a multi table join query
// execution plan to query the rowid of the original table and the index table respectively
// link ref: https://dev.mysql.com/doc/refman/8.0/en/delete.html
func buildSingleTableDelete(ctx CompilerContext, stmt *tree.Delete) (*Plan, error) {
	tbinfo := &tableInfo{
		alias2BaseNameMap: make(map[string]string),
		baseName2AliasMap: make(map[string]string),
		dbNames:           make([]string, 1),
		tableNames:        make([]string, 1),
	}
	extractAliasTable(stmt.Tables[0].(*tree.AliasedTableExpr), tbinfo, ctx)
	tbinfo.reverseAliasMap()

	objRef, tableDef := ctx.Resolve(tbinfo.dbNames[0], tbinfo.tableNames[0])
	if tableDef == nil {
		return nil, moerr.NewInvalidInput(ctx.GetContext(), "delete has no table def")
	}
	if tableDef.TableType == catalog.SystemExternalRel {
		return nil, moerr.NewInvalidInput(ctx.GetContext(), "cannot delete from external table")
	} else if tableDef.TableType == catalog.SystemViewRel {
		return nil, moerr.NewInvalidInput(ctx.GetContext(), "cannot delete from view")
	}
	if util.TableIsClusterTable(tableDef.GetTableType()) && ctx.GetAccountId() != catalog.System_Account {
		return nil, moerr.NewInternalError(ctx.GetContext(), "only the sys account can delete the cluster table %s", tableDef.GetName())
	}

	// optimize to truncate,
	if stmt.Where == nil && stmt.Limit == nil {
		return buildDelete2Truncate(ctx, objRef, tableDef)
	}

	// 1.build select list exprs
	deleteTableList := NewDeleteTableList()
	err := buildDeleteProjection(objRef, tableDef, tbinfo, deleteTableList, ctx)
	if err != nil {
		return nil, err
	}

	// 2.get origin table Expr, only one table
	originTableExpr := stmt.Tables[0]
	aliasTable := originTableExpr.(*tree.AliasedTableExpr)
	originTableName := string(aliasTable.Expr.(*tree.TableName).ObjectName)

	// 3.build FromClause
	var fromClause *tree.From

	// 4.get the definition of the unique index of the original table
	unqiueIndexDef, _ := buildIndexDefs(tableDef.Defs)

	if unqiueIndexDef != nil {
		var leftJoinTableExpr tree.TableExpr
		for i := 0; i < len(unqiueIndexDef.TableNames); i++ {
			// build left join with origin table and index table
			indexTableExpr := buildIndexTableExpr(unqiueIndexDef.TableNames[i])
			joinCond := buildJoinOnCond(tbinfo, originTableName, unqiueIndexDef.TableNames[i], unqiueIndexDef.Fields[i])
			leftJoinTableExpr = &tree.JoinTableExpr{
				JoinType: tree.JOIN_TYPE_LEFT,
				Left:     originTableExpr,
				Right:    indexTableExpr,
				Cond:     joinCond,
			}
			originTableExpr = leftJoinTableExpr
		}
		// 4.build FromClause
		fromClause = &tree.From{Tables: tree.TableExprs{leftJoinTableExpr}}
	} else {
		fromClause = &tree.From{Tables: stmt.Tables}
	}

	// 5.build complete query clause abstract syntax tree for delete
	selectStmt := &tree.Select{
		Select: &tree.SelectClause{
			Exprs: deleteTableList.selectList, // select list
			From:  fromClause,                 // table and left join
			Where: stmt.Where,                 //append where clause
		},
		OrderBy: stmt.OrderBy,
		Limit:   stmt.Limit,
		With:    stmt.With,
	}

	// 6.build query sub plan corresponding to delete statement
	subplan, err := runBuildSelectByBinder(plan.Query_SELECT, ctx, selectStmt)
	if err != nil {
		return nil, err
	}
	subplan.Plan.(*plan.Plan_Query).Query.StmtType = plan.Query_DELETE
	qry := subplan.Plan.(*plan.Plan_Query).Query

	// build multi table delete Context
	delCtxs := make([]*plan.DeleteTableCtx, len(deleteTableList.delTables))
	for i := 0; i < len(deleteTableList.delTables); i++ {
		delCtxs[i] = &plan.DeleteTableCtx{
			DbName:             deleteTableList.delTables[i].objRefs.SchemaName,
			TblName:            deleteTableList.delTables[i].tblDefs.Name,
			UseDeleteKey:       catalog.Row_ID,
			CanTruncate:        false,
			ColIndex:           deleteTableList.delTables[i].colIndex,
			IsIndexTableDelete: deleteTableList.delTables[i].isIndexTableDelete,
		}
	}

	// build delete node
	node := &Node{
		NodeType:        plan.Node_DELETE,
		ObjRef:          nil,
		TableDef:        nil,
		Children:        []int32{qry.Steps[len(qry.Steps)-1]},
		NodeId:          int32(len(qry.Nodes)),
		DeleteTablesCtx: delCtxs,
	}
	qry.Nodes = append(qry.Nodes, node)
	qry.Steps[len(qry.Steps)-1] = node.NodeId
	return subplan, nil
}

// Build delete projection of delete node
func buildDeleteProjection(objRef *ObjectRef, tableDef *TableDef, tbinfo *tableInfo, delTablelist *DeleteTableList, ctx CompilerContext) error {
	expr, err := buildRowIdAstExpr(ctx, tbinfo, objRef.SchemaName, tableDef.Name)
	if err != nil {
		return err
	}
	delTablelist.AddElement(objRef, tableDef, expr, false)

	// make true we can get all the index col data before update, so we can delete index info.
	uDef, _ := buildIndexDefs(tableDef.Defs)
	if uDef != nil {
		for i, indexTblName := range uDef.TableNames {
			if uDef.TableExists[i] {
				idxObjRef, idxTblDef := ctx.Resolve(objRef.SchemaName, indexTblName)
				rowidExpr, err := buildRowIdAstExpr(ctx, nil, idxObjRef.SchemaName, indexTblName)
				if err != nil {
					return err
				}
				delTablelist.AddElement(idxObjRef, idxTblDef, rowidExpr, true)
			} else {
				continue
			}
		}
	}
	return nil
}

// build rowid column abstract syntax tree expression of the table to be deleted
func buildRowIdAstExpr(ctx CompilerContext, tbinfo *tableInfo, schemaName string, tableName string) (tree.SelectExpr, error) {
	hideKey := ctx.GetHideKeyDef(schemaName, tableName)
	if hideKey == nil {
		return tree.SelectExpr{}, moerr.NewInvalidState(ctx.GetContext(), "cannot find hide key")
	}
	tblAliasName := tableName
	if tbinfo != nil {
		tblAliasName = tbinfo.baseName2AliasMap[tableName]
	}
	expr := tree.SetUnresolvedName(tblAliasName, hideKey.Name)
	return tree.SelectExpr{Expr: expr}, nil
}

// build Index table ast expr
func buildIndexTableExpr(indexTableName string) tree.TableExpr {
	prefix := tree.ObjectNamePrefix{
		CatalogName:     "",
		SchemaName:      "",
		ExplicitCatalog: false,
		ExplicitSchema:  false,
	}

	tableExpr := tree.NewTableName(tree.Identifier(indexTableName), prefix)

	aliasClause := tree.AliasClause{
		Alias: "",
	}
	return tree.NewAliasedTableExpr(tableExpr, aliasClause)
}

// construct equivalent connection conditions between original table and index table
func buildJoinOnCond(tbinfo *tableInfo, originTableName string, indexTableName string, indexField *plan.Field) *tree.OnJoinCond {
	originTableAlias := tbinfo.baseName2AliasMap[originTableName]
	// If it is a single column index
	if len(indexField.Parts) == 1 {
		uniqueColName := indexField.Parts[0]
		leftExpr := tree.SetUnresolvedName(originTableAlias, uniqueColName)
		rightExpr := tree.SetUnresolvedName(indexTableName, strings.ToLower(catalog.IndexTableIndexColName))

		onCondExpr := tree.NewComparisonExprWithSubop(tree.EQUAL, tree.EQUAL, leftExpr, rightExpr)
		return tree.NewOnJoinCond(onCondExpr)
	} else { // If it is a composite index
		funcName := tree.SetUnresolvedName(strings.ToLower("serial"))
		// build function parameters
		exprs := make(tree.Exprs, len(indexField.Parts))
		for i, part := range indexField.Parts {
			exprs[i] = tree.SetUnresolvedName(originTableAlias, part)
		}

		// build composite index serialize function expression
		leftExpr := &tree.FuncExpr{
			Func:  tree.FuncName2ResolvableFunctionReference(funcName),
			Exprs: exprs,
		}

		rightExpr := tree.SetUnresolvedName(indexTableName, strings.ToLower(catalog.IndexTableIndexColName))
		onCondExpr := tree.NewComparisonExprWithSubop(tree.EQUAL, tree.EQUAL, leftExpr, rightExpr)
		return tree.NewOnJoinCond(onCondExpr)
	}
}

// Perform the truncation operation of the table, When performing a delete operation,
// if the original table can be truncated, its index table also needs to be truncated
func buildDelete2Truncate(ctx CompilerContext, objRef *ObjectRef, tableDef *TableDef) (*Plan, error) {
	deleteTablesCtx := make([]*plan.DeleteTableCtx, 0)

	// Build the context for deleting the original table
	d := &plan.DeleteTableCtx{
		DbName:             objRef.SchemaName,
		TblName:            tableDef.Name,
		CanTruncate:        true,
		IsIndexTableDelete: false,
	}
	deleteTablesCtx = append(deleteTablesCtx, d)

	// Build the context for deleting the index table
	uDef, _ := buildIndexDefs(tableDef.Defs)
	if uDef != nil {
		for i, indexTblName := range uDef.TableNames {
			if uDef.TableExists[i] {
				idxObjRef, idxTblDef := ctx.Resolve(objRef.SchemaName, indexTblName)
				delIndex := &plan.DeleteTableCtx{
					DbName:             idxObjRef.SchemaName,
					TblName:            idxTblDef.Name,
					CanTruncate:        true,
					IsIndexTableDelete: true,
				}
				deleteTablesCtx = append(deleteTablesCtx, delIndex)
			} else {
				continue
			}
		}
	}

	// build delete node
	node := &Node{
		NodeType:        plan.Node_DELETE,
		ObjRef:          objRef,
		TableDef:        tableDef,
		DeleteTablesCtx: deleteTablesCtx,
	}
	return &Plan{
		Plan: &plan.Plan_Query{
			Query: &Query{
				StmtType: plan.Query_DELETE,
				Steps:    []int32{0},
				Nodes:    []*Node{node},
			},
		},
	}, nil
}

func extractAliasTable(aliasTable *tree.AliasedTableExpr, tf *tableInfo, ctx CompilerContext) {
	dbName := string(aliasTable.Expr.(*tree.TableName).SchemaName)
	if dbName == "" {
		dbName = ctx.DefaultDatabase()
	}
	tf.dbNames[0] = dbName
	tf.tableNames[0] = string(aliasTable.Expr.(*tree.TableName).ObjectName)
	if string(aliasTable.As.Alias) != "" {
		tf.alias2BaseNameMap[string(aliasTable.As.Alias)] = tf.tableNames[0]
	} else {
		tf.alias2BaseNameMap[tf.tableNames[0]] = tf.tableNames[0]
	}
}

func getTableNames(tableExprs tree.TableExprs) []*tree.TableName {
	tbs := make([]*tree.TableName, 0, len(tableExprs))
	for _, tableExpr := range tableExprs {
		tbs = append(tbs, tableExpr.(*tree.TableName))
	}
	return tbs
}

// table information to be deleted (original table and index table)
type deleteTableInfo struct {
	objRefs            *ObjectRef
	tblDefs            *TableDef
	useKeys            *ColDef // The column used when deletion(dml), currently, it is based on '__row_id' column
	colIndex           int32
	attrsArr           []string // Confirm whether this field is useful
	isIndexTableDelete bool     // Identify whether the current table is an index table
}

// DeleteTableList: information list of tables to be deleted
type DeleteTableList struct {
	delTables  []deleteTableInfo // table information list to be deleted
	selectList tree.SelectExprs  // The rowid projection expression that needs to be deleted
	nextIndex  int
}

func NewDeleteTableList() *DeleteTableList {
	return &DeleteTableList{
		delTables: make([]deleteTableInfo, 0),
		nextIndex: 0,
	}
}

// Add the information element[deleteTableInfo] of the table to be deleted
func (list *DeleteTableList) AddElement(objRef *ObjectRef, tableDef *TableDef, expr tree.SelectExpr, isIndexTableDelete bool) {
	delInfo := deleteTableInfo{
		objRefs:            objRef,
		tblDefs:            tableDef,
		useKeys:            nil,
		colIndex:           int32(list.nextIndex),
		attrsArr:           nil,
		isIndexTableDelete: isIndexTableDelete,
	}
	list.delTables = append(list.delTables, delInfo)
	list.selectList = append(list.selectList, expr)
	list.nextIndex++
}
