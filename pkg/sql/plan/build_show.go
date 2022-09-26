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
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

const MO_CATALOG_DB_NAME = "mo_catalog"

func buildShowCreateDatabase(stmt *tree.ShowCreateDatabase, ctx CompilerContext) (*Plan, error) {
	if !ctx.DatabaseExists(stmt.Name) {
		return nil, moerr.NewBadDB(stmt.Name)
	}

	// get data from schema
	//sql := fmt.Sprintf("SELECT md.datname as `Database` FROM %s.mo_database md WHERE md.datname = '%s'", MO_CATALOG_DB_NAME, stmt.Name)
	// sql := fmt.Sprintf("SELECT md.datname as `Database`,dat_createsql as `Create Database` FROM %s.mo_database md WHERE md.datname = '%s'", MO_CATALOG_DB_NAME, stmt.Name)
	// return returnByRewriteSQL(ctx, sql, plan.DataDefinition_SHOW_CREATEDATABASE)

	sqlStr := "select \"%s\" as `Database`, \"%s\" as `Create Database`"
	createSql := fmt.Sprintf("CREATE DATABASE `%s`", stmt.Name)
	sqlStr = fmt.Sprintf(sqlStr, stmt.Name, createSql)
	// log.Println(sqlStr)

	return returnByRewriteSQL(ctx, sqlStr, plan.DataDefinition_SHOW_CREATEDATABASE)
}

func buildShowCreateTable(stmt *tree.ShowCreateTable, ctx CompilerContext) (*Plan, error) {
	tblName := stmt.Name.Parts[0]
	dbName := ctx.DefaultDatabase()
	if stmt.Name.NumParts == 2 {
		dbName = stmt.Name.Parts[1]
	}

	_, tableDef := ctx.Resolve(dbName, tblName)
	if tableDef == nil {
		return nil, moerr.NewBadDB(tblName)
	}
	if tableDef.TableType == catalog.SystemViewRel {
		newStmt := tree.NewShowCreateView(tree.SetUnresolvedObjectName(1, [3]string{tblName, "", ""}))
		return buildShowCreateView(newStmt, ctx)
	}

	// sql := `
	// 	SELECT *
	// 		FROM %s.mo_tables mt JOIN %s.mo_columns mc
	// 			ON mt.relname = mc.att_relname and mt.reldatabase=mc.att_database
	// 	WHERE mt.reldatabase = '%s' AND mt.relname = '%s'
	// `
	// sql = fmt.Sprintf(sql, MO_CATALOG_DB_NAME, MO_CATALOG_DB_NAME, dbName, tblName)
	// log.Println(sql)

	createStr := fmt.Sprintf("CREATE TABLE `%s` (", tblName)
	rowCount := 0
	var pkDefs []string

	for _, col := range tableDef.Cols {

		colName := col.Name
		if colName == "PADDR" {
			continue
		}
		nullOrNot := "NOT NULL"
		if col.Default != nil {
			if col.Default.Expr != nil {
				nullOrNot = "DEFAULT " + col.Default.OriginString
			} else if col.Default.NullAbility {
				nullOrNot = "DEFAULT NULL"
			}
		}
		if col.AutoIncrement {
			nullOrNot = "NOT NULL AUTO_INCREMENT"
		}

		var hasAttrComment string
		if col.Comment != "" {
			hasAttrComment = " COMMENT '" + col.Comment + "'"
		}

		if rowCount == 0 {
			createStr += "\n"
		} else {
			createStr += ",\n"
		}
		typ := types.Type{Oid: types.T(col.Typ.Id)}
		typeStr := typ.String()
		if typ.Oid == types.T_varchar || typ.Oid == types.T_char {
			typeStr += fmt.Sprintf("(%d)", col.Typ.Width)
		}
		createStr += fmt.Sprintf("`%s` %s %s%s", colName, typeStr, nullOrNot, hasAttrComment)
		rowCount++
		if col.Primary {
			pkDefs = append(pkDefs, colName)
		}
	}
	if len(pkDefs) != 0 {
		pkStr := "PRIMARY KEY ("
		for _, def := range pkDefs {
			pkStr += fmt.Sprintf("`%s`", def)
		}
		pkStr += ")"
		if rowCount != 0 {
			createStr += ",\n"
		}
		createStr += pkStr
	}

	if rowCount != 0 {
		createStr += "\n"
	}
	createStr += ")"

	var comment string
	var partition string
	for _, def := range tableDef.Defs {
		if proDef, ok := def.Def.(*plan.TableDef_DefType_Properties); ok {
			for _, kv := range proDef.Properties.Properties {
				if kv.Key == catalog.SystemRelAttr_Comment {
					comment = " COMMENT='" + kv.Value + "'"
				}
			}
		}

		if partDef, ok := def.Def.(*plan.TableDef_DefType_Partition); ok {
			if len(partDef.Partition.PartitionMsg) != 0 {
				partition = ` ` + partDef.Partition.PartitionMsg
			}
		}
	}
	createStr += comment
	createStr += partition

	sql := "select \"%s\" as `Table`, \"%s\" as `Create Table`"
	var buf bytes.Buffer
	for _, ch := range createStr {
		if ch == '"' {
			buf.WriteRune('"')
		}
		buf.WriteRune(ch)
	}
	sql = fmt.Sprintf(sql, tblName, buf.String())

	return returnByRewriteSQL(ctx, sql, plan.DataDefinition_SHOW_CREATETABLE)
}

// buildShowCreateView
func buildShowCreateView(stmt *tree.ShowCreateView, ctx CompilerContext) (*Plan, error) {
	tblName := stmt.Name.Parts[0]
	dbName := ctx.DefaultDatabase()
	if stmt.Name.NumParts == 2 {
		dbName = stmt.Name.Parts[1]
	}

	_, tableDef := ctx.Resolve(dbName, tblName)
	if tableDef == nil || tableDef.TableType != catalog.SystemViewRel {
		return nil, moerr.NewInvalidInput("show view '%s' is not a valid view", tblName)
	}
	sqlStr := "select \"%s\" as `View`, \"%s\" as `Create View`"
	var viewStr string
	if tableDef.TableType == catalog.SystemViewRel {
		for _, def := range tableDef.Defs {
			if viewDef, ok := def.Def.(*plan.TableDef_DefType_View); ok {
				viewStr = viewDef.View.View
				break
			}
		}
	}

	var viewData ViewData
	err := json.Unmarshal([]byte(viewStr), &viewData)
	if err != nil {
		return nil, err
	}

	// FixMe  We need a better escape function
	stmtStr := strings.ReplaceAll(viewData.Stmt, "\"", "\\\"")
	sqlStr = fmt.Sprintf(sqlStr, tblName, fmt.Sprint(stmtStr))

	// log.Println(sqlStr)

	return returnByRewriteSQL(ctx, sqlStr, plan.DataDefinition_SHOW_CREATETABLE)
}

func buildShowDatabases(stmt *tree.ShowDatabases, ctx CompilerContext) (*Plan, error) {
	if stmt.Like != nil && stmt.Where != nil {
		return nil, moerr.NewSyntaxError("like clause and where clause cannot exist at the same time")
	}
	ddlType := plan.DataDefinition_SHOW_DATABASES
	sql := fmt.Sprintf("SELECT datname `Database` FROM %s.mo_database", MO_CATALOG_DB_NAME)

	if stmt.Where != nil {
		return returnByWhereAndBaseSQL(ctx, sql, stmt.Where, ddlType)
	}

	if stmt.Like != nil {
		// append filter [AND datname like stmt.Like] to WHERE clause
		likeExpr := stmt.Like
		likeExpr.Left = tree.SetUnresolvedName("datname")
		return returnByLikeAndSQL(ctx, sql, likeExpr, ddlType)
	}

	return returnByRewriteSQL(ctx, sql, ddlType)
}

func buildShowTables(stmt *tree.ShowTables, ctx CompilerContext) (*Plan, error) {
	if stmt.Like != nil && stmt.Where != nil {
		return nil, moerr.NewSyntaxError("like clause and where clause cannot exist at the same time")
	}

	if stmt.Full || stmt.Open {
		return nil, moerr.NewNYI("statement: '%v'", tree.String(stmt, dialect.MYSQL))
	}

	dbName := stmt.DBName
	if stmt.DBName == "" {
		dbName = ctx.DefaultDatabase()
	} else if !ctx.DatabaseExists(dbName) {
		return nil, moerr.NewBadDB(dbName)
	}

	if dbName == "" {
		return nil, moerr.NewNoDB()
	}
	ddlType := plan.DataDefinition_SHOW_TABLES
	sql := fmt.Sprintf("SELECT relname as Tables_in_%s FROM %s.mo_tables WHERE reldatabase = '%s' and relname != '%s'", dbName, MO_CATALOG_DB_NAME, dbName, "%!%mo_increment_columns")

	if stmt.Where != nil {
		return returnByWhereAndBaseSQL(ctx, sql, stmt.Where, ddlType)
	}

	if stmt.Like != nil {
		// append filter [AND relname like stmt.Like] to WHERE clause
		likeExpr := stmt.Like
		likeExpr.Left = tree.SetUnresolvedName("relname")
		return returnByLikeAndSQL(ctx, sql, likeExpr, ddlType)
	}

	return returnByRewriteSQL(ctx, sql, ddlType)
}

func buildShowColumns(stmt *tree.ShowColumns, ctx CompilerContext) (*Plan, error) {
	if stmt.Like != nil && stmt.Where != nil {
		return nil, moerr.NewSyntaxError("like clause and where clause cannot exist at the same time")
	}

	if stmt.Full {
		return nil, moerr.NewNotSupported("statement '%v'", tree.String(stmt, dialect.MYSQL))
	}

	dbName := stmt.Table.GetDBName()
	if dbName == "" {
		dbName = ctx.DefaultDatabase()
	} else if !ctx.DatabaseExists(dbName) {
		return nil, moerr.NewBadDB(dbName)
	}

	tblName := string(stmt.Table.ToTableName().ObjectName)
	_, tableDef := ctx.Resolve(dbName, tblName)
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(dbName, tblName)
	}

	ddlType := plan.DataDefinition_SHOW_COLUMNS
	sql := "SELECT attname `Field`,atttyp `Type`, attnotnull `Null`, iff(att_constraint_type = 'p','PRI','') `Key`, att_default `Default`, att_comment `Comment` FROM %s.mo_columns WHERE att_database = '%s' AND att_relname = '%s'"

	sql = fmt.Sprintf(sql, MO_CATALOG_DB_NAME, dbName, tblName)

	if stmt.Where != nil {
		return returnByWhereAndBaseSQL(ctx, sql, stmt.Where, ddlType)
	}

	if stmt.Like != nil {
		// append filter [AND ma.attname like stmt.Like] to WHERE clause
		likeExpr := stmt.Like
		likeExpr.Left = tree.SetUnresolvedName("attname")
		return returnByLikeAndSQL(ctx, sql, likeExpr, ddlType)
	}

	return returnByRewriteSQL(ctx, sql, ddlType)
}

func buildShowIndex(stmt *tree.ShowIndex, ctx CompilerContext) (*Plan, error) {
	return nil, moerr.NewNotSupported("statement: '%v'", tree.String(stmt, dialect.MYSQL))
}

func buildShowVariables(stmt *tree.ShowVariables, ctx CompilerContext) (*Plan, error) {
	if stmt.Like != nil && stmt.Where != nil {
		return nil, moerr.NewSyntaxError("like clause and where clause cannot exist at the same time")
	}

	builder := NewQueryBuilder(plan.Query_SELECT, ctx)
	binder := NewWhereBinder(builder, &BindContext{})

	showVariables := &plan.ShowVariables{
		Global: stmt.Global,
	}
	if stmt.Like != nil {
		expr, err := binder.bindComparisonExpr(stmt.Like, 0, false)
		if err != nil {
			return nil, err
		}
		showVariables.Where = append(showVariables.Where, expr)
	}
	if stmt.Where != nil {
		exprs, err := splitAndBindCondition(stmt.Where.Expr, &BindContext{})
		if err != nil {
			return nil, err
		}
		showVariables.Where = append(showVariables.Where, exprs...)
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_SHOW_VARIABLES,
				Definition: &plan.DataDefinition_ShowVariables{
					ShowVariables: showVariables,
				},
			},
		},
	}, nil
}

func buildShowWarnings(stmt *tree.ShowWarnings, ctx CompilerContext) (*Plan, error) {
	return nil, moerr.NewNotSupported("statement: '%v'", tree.String(stmt, dialect.MYSQL))
}

func buildShowErrors(stmt *tree.ShowErrors, ctx CompilerContext) (*Plan, error) {
	return nil, moerr.NewNotSupported("statement: '%v'", tree.String(stmt, dialect.MYSQL))
}

func buildShowStatus(stmt *tree.ShowStatus, ctx CompilerContext) (*Plan, error) {
	return nil, moerr.NewNotSupported("statement: '%v'", tree.String(stmt, dialect.MYSQL))
}

func buildShowProcessList(stmt *tree.ShowProcessList, ctx CompilerContext) (*Plan, error) {
	return nil, moerr.NewNotSupported("statement: '%v'", tree.String(stmt, dialect.MYSQL))
}

func returnByRewriteSQL(ctx CompilerContext, sql string, ddlType plan.DataDefinition_DdlType) (*Plan, error) {
	stmt, err := getRewriteSQLStmt(sql)
	if err != nil {
		return nil, err
	}
	return getReturnDdlBySelectStmt(ctx, stmt, ddlType)
}

func returnByWhereAndBaseSQL(ctx CompilerContext, baseSQL string, where *tree.Where, ddlType plan.DataDefinition_DdlType) (*Plan, error) {
	sql := fmt.Sprintf("SELECT * FROM (%s) tbl", baseSQL)
	// log.Println(sql)
	newStmt, err := getRewriteSQLStmt(sql)
	if err != nil {
		return nil, err
	}
	// set show statement's where clause to new statement
	newStmt.(*tree.Select).Select.(*tree.SelectClause).Where = where
	return getReturnDdlBySelectStmt(ctx, newStmt, ddlType)
}

func returnByLikeAndSQL(ctx CompilerContext, sql string, like *tree.ComparisonExpr, ddlType plan.DataDefinition_DdlType) (*Plan, error) {
	newStmt, err := getRewriteSQLStmt(sql)
	if err != nil {
		return nil, err
	}
	var whereExpr *tree.Where

	if newStmt.(*tree.Select).Select.(*tree.SelectClause).Where == nil {
		whereExpr = &tree.Where{
			Type: "where",
			Expr: like,
		}
	} else {
		whereExpr = &tree.Where{
			Type: "where",
			Expr: &tree.AndExpr{
				Left:  newStmt.(*tree.Select).Select.(*tree.SelectClause).Where.Expr,
				Right: like,
			},
		}
	}
	// set show statement's like clause to new statement
	newStmt.(*tree.Select).Select.(*tree.SelectClause).Where = whereExpr
	// log.Println(tree.String(newStmt, dialect.MYSQL))
	return getReturnDdlBySelectStmt(ctx, newStmt, ddlType)
}

func getRewriteSQLStmt(sql string) (tree.Statement, error) {
	newStmts, err := parsers.Parse(dialect.MYSQL, sql)
	if err != nil {
		return nil, err
	}
	if len(newStmts) != 1 {
		return nil, moerr.NewInvalidInput("rewrite can only contain one statement, %d provided", len(newStmts))
	}
	return newStmts[0], nil
}

func getReturnDdlBySelectStmt(ctx CompilerContext, stmt tree.Statement, ddlType plan.DataDefinition_DdlType) (*Plan, error) {
	queryPlan, err := BuildPlan(ctx, stmt)
	if err != nil {
		return nil, err
	}
	return queryPlan, nil
	// return &Plan{
	// 	Plan: &plan.Plan_Ddl{
	// 		Ddl: &plan.DataDefinition{
	// 			DdlType: ddlType,
	// 			Query:   queryPlan.GetQuery(),
	// 		},
	// 	},
	// }, nil
}
