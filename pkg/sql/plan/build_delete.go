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
)

func buildDelete(stmt *tree.Delete, ctx CompilerContext) (*Plan, error) {
	if len(stmt.Tables) == 1 && stmt.TableRefs == nil {
		return buildDeleteSingleTable(stmt, ctx)
	}
	return buildDeleteMultipleTable(stmt, ctx)
}

func extractAliasTable(aliasTable *tree.AliasedTableExpr, tf *tableInfo, ctx CompilerContext) {
	dbName := string(aliasTable.Expr.(*tree.TableName).SchemaName)
	if dbName == "" {
		dbName = ctx.DefaultDatabase()
	}
	tf.dbNames[0] = dbName
	tf.tableNames[0] = string(aliasTable.Expr.(*tree.TableName).ObjectName)
	if string(aliasTable.As.Alias) != "" {
		tf.baseNameMap[string(aliasTable.As.Alias)] = tf.tableNames[0]
	} else {
		tf.baseNameMap[tf.tableNames[0]] = tf.tableNames[0]
	}
}

func reverseMap(m map[string]string) map[string]string {
	res := make(map[string]string)
	for k, v := range m {
		res[v] = k
	}
	return res
}

// buildDeleteSingleTable, delete single table can optimize to truncate and different from syntax of multiple-tables,
// so it is build singly.
func buildDeleteSingleTable(stmt *tree.Delete, ctx CompilerContext) (*Plan, error) {
	// check database's name and table's name
	tf := &tableInfo{
		baseNameMap: make(map[string]string),
		dbNames:     make([]string, 1),
		tableNames:  make([]string, 1),
	}
	extractAliasTable(stmt.Tables[0].(*tree.AliasedTableExpr), tf, ctx)
	tf.baseNameMap = reverseMap(tf.baseNameMap)
	objRef, tableDef := ctx.Resolve(tf.dbNames[0], tf.tableNames[0])
	if tableDef == nil {
		return nil, moerr.NewInvalidInput(ctx.GetContext(), "delete has no table def")
	}
	if tableDef.TableType == catalog.SystemExternalRel {
		return nil, moerr.NewInvalidInput(ctx.GetContext(), "cannot delete from external table")
	} else if tableDef.TableType == catalog.SystemViewRel {
		return nil, moerr.NewInvalidInput(ctx.GetContext(), "cannot delete from view")
	}

	// optimize to truncate,
	if stmt.Where == nil && stmt.Limit == nil {
		return buildDelete2Truncate(objRef, tableDef)
	}

	// find out use keys to delete
	var useProjectExprs tree.SelectExprs = nil

	useProjectExprs, useKey, attrs, err := buildUseProjection(stmt, useProjectExprs, objRef, tableDef, tf, ctx)
	if err != nil {
		return nil, err
	}

	// build the stmt of select and append select node
	selectStmt := &tree.Select{
		Select: &tree.SelectClause{
			Exprs: useProjectExprs,
			From:  &tree.From{Tables: stmt.Tables},
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
	usePlan.Plan.(*plan.Plan_Query).Query.StmtType = plan.Query_DELETE
	qry := usePlan.Plan.(*plan.Plan_Query).Query

	uDef, sDef := buildIndexDefs(tableDef.Defs)

	// build delete node
	d := &plan.DeleteTableCtx{
		DbName:            objRef.SchemaName,
		TblName:           tableDef.Name,
		UseDeleteKey:      useKey.Name,
		CanTruncate:       false,
		UniqueIndexDef:    uDef,
		SecondaryIndexDef: sDef,
		IndexAttrs:        attrs,
	}
	node := &Node{
		NodeType:        plan.Node_DELETE,
		ObjRef:          nil,
		TableDef:        nil,
		Children:        []int32{qry.Steps[len(qry.Steps)-1]},
		NodeId:          int32(len(qry.Nodes)),
		DeleteTablesCtx: []*plan.DeleteTableCtx{d},
	}
	qry.Nodes = append(qry.Nodes, node)
	qry.Steps[len(qry.Steps)-1] = node.NodeId

	return usePlan, nil
}

func buildDelete2Truncate(objRef *ObjectRef, tblDef *TableDef) (*Plan, error) {
	// build delete node
	uDef, sDef := buildIndexDefs(tblDef.Defs)
	d := &plan.DeleteTableCtx{
		DbName:            objRef.SchemaName,
		TblName:           tblDef.Name,
		CanTruncate:       true,
		UniqueIndexDef:    uDef,
		SecondaryIndexDef: sDef,
	}
	node := &Node{
		NodeType:        plan.Node_DELETE,
		ObjRef:          objRef,
		TableDef:        tblDef,
		DeleteTablesCtx: []*plan.DeleteTableCtx{d},
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

func buildDeleteMultipleTable(stmt *tree.Delete, ctx CompilerContext) (*Plan, error) {
	// build map between base table and alias table
	tf := &tableInfo{baseNameMap: make(map[string]string)}
	for _, expr := range stmt.TableRefs {
		if err := extractExprTable(expr, tf, ctx); err != nil {
			return nil, err
		}
	}

	// check database's name and table's name
	tbs := getTableNames(stmt.Tables)
	tableCount := len(tbs)
	objRefs := make([]*ObjectRef, tableCount)
	tblDefs := make([]*TableDef, tableCount)
	for i, t := range tbs {
		dbName := string(t.SchemaName)
		if dbName == "" {
			dbName = ctx.DefaultDatabase()
		}
		tblName := string(t.ObjectName)
		if _, ok := tf.baseNameMap[tblName]; ok {
			tblName = tf.baseNameMap[tblName]
		}
		objRefs[i], tblDefs[i] = ctx.Resolve(dbName, tblName)
		if tblDefs[i] == nil {
			return nil, moerr.NewInvalidInput(ctx.GetContext(), "delete has no table ref")
		}
		if tblDefs[i].TableType == catalog.SystemExternalRel {
			return nil, moerr.NewInvalidInput(ctx.GetContext(), "cannot delete from external table")
		} else if tblDefs[i].TableType == catalog.SystemViewRel {
			return nil, moerr.NewInvalidInput(ctx.GetContext(), "cannot delete from view")
		}
	}
	originMap := tf.baseNameMap
	tf.baseNameMap = reverseMap(tf.baseNameMap)
	for _, t := range tbs {
		tblName := string(t.ObjectName)
		if _, ok := tf.baseNameMap[tblName]; !ok {
			if _, ok := originMap[tblName]; !ok {
				return nil, moerr.NewInvalidInput(ctx.GetContext(), "Unknown table '%v' in MULTI DELETE", tblName)
			}
		}
	}

	// find out use keys to delete
	var err error
	useKeys := make([]*ColDef, tableCount)
	colIndex := make([]int32, tableCount)
	attrsArr := make([][]string, tableCount)
	var useProjectExprs tree.SelectExprs = nil
	for i := 0; i < tableCount; i++ {
		colIndex[i] = int32(len(useProjectExprs))
		useProjectExprs, useKeys[i], attrsArr[i], err = buildUseProjection(stmt, useProjectExprs, objRefs[i], tblDefs[i], tf, ctx)
		if err != nil {
			return nil, err
		}
	}

	// build the stmt of select and append select node
	selectStmt := &tree.Select{
		Select: &tree.SelectClause{
			Exprs: useProjectExprs,
			From:  &tree.From{Tables: stmt.TableRefs},
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
	usePlan.Plan.(*plan.Plan_Query).Query.StmtType = plan.Query_DELETE
	qry := usePlan.Plan.(*plan.Plan_Query).Query

	ds := make([]*plan.DeleteTableCtx, tableCount)
	for i := 0; i < tableCount; i++ {
		ds[i] = &plan.DeleteTableCtx{
			DbName:       objRefs[i].SchemaName,
			TblName:      tblDefs[i].Name,
			UseDeleteKey: useKeys[i].Name,
			CanTruncate:  false,
			ColIndex:     colIndex[i],
			IndexAttrs:   attrsArr[i],
		}
	}
	node := &Node{
		NodeType:        plan.Node_DELETE,
		ObjRef:          nil,
		TableDef:        nil,
		Children:        []int32{qry.Steps[len(qry.Steps)-1]},
		NodeId:          int32(len(qry.Nodes)),
		DeleteTablesCtx: ds,
	}
	qry.Nodes = append(qry.Nodes, node)
	qry.Steps[len(qry.Steps)-1] = node.NodeId

	return usePlan, nil
}

func getTableNames(tableExprs tree.TableExprs) []*tree.TableName {
	tbs := make([]*tree.TableName, 0, len(tableExprs))
	for _, tableExpr := range tableExprs {
		tbs = append(tbs, tableExpr.(*tree.TableName))
	}
	return tbs
}

func buildUseProjection(stmt *tree.Delete, ps tree.SelectExprs, objRef *ObjectRef, tableDef *TableDef, tf *tableInfo, ctx CompilerContext) (tree.SelectExprs, *ColDef, []string, error) {
	var useKey *ColDef
	tblName := tf.baseNameMap[tableDef.Name]

	// we will allways return hideKey now
	hideKey := ctx.GetHideKeyDef(objRef.SchemaName, tableDef.Name)
	if hideKey == nil {
		return nil, nil, nil, moerr.NewInvalidState(ctx.GetContext(), "cannot find hide key")
	}
	e := tree.SetUnresolvedName(tblName, hideKey.Name)
	ps = append(ps, tree.SelectExpr{Expr: e})
	useKey = hideKey

	// make true we can get all the index col data before update, so we can delete index info.
	indexColNameMap := make(map[string]bool)
	uDef, sDef := buildIndexDefs(tableDef.Defs)
	if uDef != nil {
		for _, def := range uDef.Fields {
			// Judge whether it is a composite index key
			isCompkey := util.JudgeIsCompositePrimaryKeyColumn(def.Cols[0].Name)
			if isCompkey {
				colNames := util.SplitCompositePrimaryKeyColumnName(def.Cols[0].Name)
				for _, colName := range colNames {
					indexColNameMap[colName] = true
				}
			} else {
				indexColNameMap[def.Cols[0].Name] = true
			}
		}
	}
	if sDef != nil {
		for _, def := range sDef.Fields {
			isCPkey := util.JudgeIsCompositePrimaryKeyColumn(def.Cols[0].Name)
			if isCPkey {
				colNames := util.SplitCompositePrimaryKeyColumnName(def.Cols[0].Name)
				for _, colName := range colNames {
					indexColNameMap[colName] = true
				}
			} else {
				indexColNameMap[def.Cols[0].Name] = true
			}
		}
	}

	indexAttrs := make([]string, 0)
	for indexColName := range indexColNameMap {
		indexAttrs = append(indexAttrs, indexColName)
		e, _ := tree.NewUnresolvedName(ctx.GetContext(), tf.baseNameMap[tableDef.Name], indexColName)
		ps = append(ps, tree.SelectExpr{Expr: e})
	}
	return ps, useKey, indexAttrs, nil

}
