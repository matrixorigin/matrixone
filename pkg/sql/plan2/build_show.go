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

package plan2

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const MO_CATALOG_DB_NAME = "mo_catalog"

func buildShowCreateDatabase(stmt *tree.ShowCreateDatabase, ctx CompilerContext) (*Plan, error) {
	if !ctx.DatabaseExists(stmt.Name) {
		return nil, errors.New(errno.InvalidDatabaseDefinition, fmt.Sprintf("database '%v' is not exist", stmt.Name))
	}

	//sql := fmt.Sprintf("SELECT md.datname as `Database` FROM %s.mo_database md WHERE md.datname = '%s'", MO_CATALOG_DB_NAME, stmt.Name)
	sql := fmt.Sprintf("SELECT md.datname as `Database`,dat_createsql as `Create Database` FROM %s.mo_database md WHERE md.datname = '%s'", MO_CATALOG_DB_NAME, stmt.Name)
	return returnByRewriteSql(ctx, sql, plan.DataDefinition_SHOW_CREATEDATABASE)
}

func buildShowCreateTable(stmt *tree.ShowCreateTable, ctx CompilerContext) (*Plan, error) {
	sql := `
		SELECT mc.* 
			FROM %s.mo_tables mt JOIN %s.mo_columns mc 
				ON mt.relname = mc.att_relname 
		WHERE mt.reldatabase = '%s' AND mt.relname = '%s'
	`
	tblName := stmt.Name.Parts[0]
	dbName := ctx.DefaultDatabase()
	if stmt.Name.NumParts == 2 {
		dbName = stmt.Name.Parts[1]
	}

	_, tableDef := ctx.Resolve(dbName, tblName)
	if tableDef == nil {
		return nil, errors.New(errno.UndefinedTable, fmt.Sprintf("table '%v' doesn't exist", tblName))
	}

	sql = fmt.Sprintf(sql, MO_CATALOG_DB_NAME, MO_CATALOG_DB_NAME, dbName, tblName)
	// log.Println(sql)

	return returnByRewriteSql(ctx, sql, plan.DataDefinition_SHOW_CREATETABLE)
}

func buildShowDatabases(stmt *tree.ShowDatabases, ctx CompilerContext) (*Plan, error) {
	if stmt.Like != nil && stmt.Where != nil {
		return nil, errors.New(errno.SyntaxError, "like clause and where clause cannot exist at the same time")
	}
	ddlType := plan.DataDefinition_SHOW_DATABASES
	sql := fmt.Sprintf("SELECT datname `Database` FROM %s.mo_database", MO_CATALOG_DB_NAME)

	if stmt.Where != nil {
		return returnByWhereAndBaseSql(ctx, sql, stmt.Where, ddlType)
	}

	if stmt.Like != nil {
		// append filter [AND datname like stmt.Like] to WHERE clause
		likeExpr := stmt.Like
		likeExpr.Left = tree.SetUnresolvedName("datname")
		return returnByLikeAndSql(ctx, sql, likeExpr, ddlType)
	}

	return returnByRewriteSql(ctx, sql, ddlType)
}

func buildShowTables(stmt *tree.ShowTables, ctx CompilerContext) (*Plan, error) {
	if stmt.Like != nil && stmt.Where != nil {
		return nil, errors.New(errno.SyntaxError, "like clause and where clause cannot exist at the same time")
	}

	if stmt.Full || stmt.Open {
		return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport statement: '%v'", tree.String(stmt, dialect.MYSQL)))
	}

	dbName := stmt.DBName
	if stmt.DBName == "" {
		dbName = ctx.DefaultDatabase()
	} else if !ctx.DatabaseExists(dbName) {
		return nil, errors.New(errno.InvalidDatabaseDefinition, fmt.Sprintf("database '%v' is not exist", dbName))
	}

	ddlType := plan.DataDefinition_SHOW_TABLES
	sql := fmt.Sprintf("SELECT relname as Tables_in_%s FROM %s.mo_tables WHERE reldatabase = '%s'", dbName, MO_CATALOG_DB_NAME, dbName)

	if stmt.Where != nil {
		return returnByWhereAndBaseSql(ctx, sql, stmt.Where, ddlType)
	}

	if stmt.Like != nil {
		// append filter [AND relname like stmt.Like] to WHERE clause
		likeExpr := stmt.Like
		likeExpr.Left = tree.SetUnresolvedName("relname")
		return returnByLikeAndSql(ctx, sql, likeExpr, ddlType)
	}

	return returnByRewriteSql(ctx, sql, ddlType)
}

func buildShowColumns(stmt *tree.ShowColumns, ctx CompilerContext) (*Plan, error) {
	if stmt.Like != nil && stmt.Where != nil {
		return nil, errors.New(errno.SyntaxError, "like clause and where clause cannot exist at the same time")
	}

	if stmt.Full {
		return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport statement: '%v'", tree.String(stmt, dialect.MYSQL)))
	}

	dbName := stmt.DBName
	if stmt.DBName == "" {
		dbName = ctx.DefaultDatabase()
	} else if !ctx.DatabaseExists(dbName) {
		return nil, errors.New(errno.InvalidDatabaseDefinition, fmt.Sprintf("database '%v' is not exist", dbName))
	}

	tblName := string(stmt.Table.ToTableName().ObjectName)
	_, tableDef := ctx.Resolve(dbName, tblName)
	if tableDef == nil {
		return nil, errors.New(errno.UndefinedTable, fmt.Sprintf("table '%v' doesn't exist", tblName))
	}

	ddlType := plan.DataDefinition_SHOW_COLUMNS
	sql := "SELECT attname `Field`,atttyp `Type`, attnotnull `Null`, iff(att_constraint_type = 'p','PRI','') `Key`, att_default `Default`, att_comment `Comment` FROM %s.mo_columns WHERE att_database = '%s' AND att_relname = '%s'"

	sql = fmt.Sprintf(sql, MO_CATALOG_DB_NAME, dbName, tblName)

	if stmt.Where != nil {
		return returnByWhereAndBaseSql(ctx, sql, stmt.Where, ddlType)
	}

	if stmt.Like != nil {
		// append filter [AND ma.attname like stmt.Like] to WHERE clause
		likeExpr := stmt.Like
		likeExpr.Left = tree.SetUnresolvedName("attname")
		return returnByLikeAndSql(ctx, sql, likeExpr, ddlType)
	}

	return returnByRewriteSql(ctx, sql, ddlType)
}

func buildShowIndex(stmt *tree.ShowIndex, ctx CompilerContext) (*Plan, error) {
	return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport statement: '%v'", tree.String(stmt, dialect.MYSQL)))
}

func buildShowVariables(stmt *tree.ShowVariables, ctx CompilerContext) (*Plan, error) {
	if stmt.Like != nil && stmt.Where != nil {
		return nil, errors.New(errno.SyntaxError, "like clause and where clause cannot exist at the same time")
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
	return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport statement: '%v'", tree.String(stmt, dialect.MYSQL)))
}

func buildShowErrors(stmt *tree.ShowErrors, ctx CompilerContext) (*Plan, error) {
	return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport statement: '%v'", tree.String(stmt, dialect.MYSQL)))
}

func buildShowStatus(stmt *tree.ShowStatus, ctx CompilerContext) (*Plan, error) {
	return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport statement: '%v'", tree.String(stmt, dialect.MYSQL)))
}

func buildShowProcessList(stmt *tree.ShowProcessList, ctx CompilerContext) (*Plan, error) {
	return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport statement: '%v'", tree.String(stmt, dialect.MYSQL)))
}

func returnByRewriteSql(ctx CompilerContext, sql string, ddlType plan.DataDefinition_DdlType) (*Plan, error) {
	stmt, err := getRewriteSqlStmt(sql)
	if err != nil {
		return nil, err
	}
	return getReturnDdlBySelectStmt(ctx, stmt, ddlType)
}

func returnByWhereAndBaseSql(ctx CompilerContext, baseSql string, where *tree.Where, ddlType plan.DataDefinition_DdlType) (*Plan, error) {
	sql := fmt.Sprintf("SELECT * FROM (%s) tbl", baseSql)
	// log.Println(sql)
	newStmt, err := getRewriteSqlStmt(sql)
	if err != nil {
		return nil, err
	}
	// set show statement's where clause to new statement
	newStmt.(*tree.Select).Select.(*tree.SelectClause).Where = where
	return getReturnDdlBySelectStmt(ctx, newStmt, ddlType)
}

func returnByLikeAndSql(ctx CompilerContext, sql string, like *tree.ComparisonExpr, ddlType plan.DataDefinition_DdlType) (*Plan, error) {
	newStmt, err := getRewriteSqlStmt(sql)
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

func getRewriteSqlStmt(sql string) (tree.Statement, error) {
	newStmts, err := parsers.Parse(dialect.MYSQL, sql)
	if err != nil {
		return nil, err
	}
	if len(newStmts) != 1 {
		return nil, errors.New(errno.InvalidTableDefinition, "rewrite sql is not one statement")
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
