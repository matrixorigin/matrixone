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

	"github.com/matrixorigin/matrixone/pkg/sql/util"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const MO_CATALOG_DB_NAME = "mo_catalog"
const MO_DEFUALT_HOSTNAME = "localhost"
const INFORMATION_SCHEMA = "information_schema"

func buildShowCreateDatabase(stmt *tree.ShowCreateDatabase,
	ctx CompilerContext) (*Plan, error) {
	if !ctx.DatabaseExists(stmt.Name) {
		return nil, moerr.NewBadDB(ctx.GetContext(), stmt.Name)
	}

	// get data from schema
	//sql := fmt.Sprintf("SELECT md.datname as `Database` FROM %s.mo_database md WHERE md.datname = '%s'", MO_CATALOG_DB_NAME, stmt.Name)
	// sql := fmt.Sprintf("SELECT md.datname as `Database`,dat_createsql as `Create Database` FROM %s.mo_database md WHERE md.datname = '%s'", MO_CATALOG_DB_NAME, stmt.Name)
	// return returnByRewriteSQL(ctx, sql, plan.DataDefinition_SHOW_CREATEDATABASE)

	sqlStr := "select \"%s\" as `Database`, \"%s\" as `Create Database`"
	createSql := fmt.Sprintf("CREATE DATABASE `%s`", stmt.Name)
	sqlStr = fmt.Sprintf(sqlStr, stmt.Name, createSql)
	// logutil.Info(sqlStr)

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
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), dbName, tblName)
	}
	if tableDef.TableType == catalog.SystemViewRel {
		var newStmt *tree.ShowCreateView
		if stmt.Name.NumParts == 1 {
			newStmt = tree.NewShowCreateView(tree.SetUnresolvedObjectName(1, [3]string{tblName, "", ""}))
		} else if stmt.Name.NumParts == 2 {
			newStmt = tree.NewShowCreateView(tree.SetUnresolvedObjectName(2, [3]string{tblName, dbName, ""}))
		}

		return buildShowCreateView(newStmt, ctx)
	}

	// sql := `
	// 	SELECT *
	// 		FROM %s.mo_tables mt JOIN %s.mo_columns mc
	// 			ON mt.relname = mc.att_relname and mt.reldatabase=mc.att_database
	// 	WHERE mt.reldatabase = '%s' AND mt.relname = '%s'
	// `
	// sql = fmt.Sprintf(sql, MO_CATALOG_DB_NAME, MO_CATALOG_DB_NAME, dbName, tblName)
	// logutil.Info(sql)

	var createStr string
	if tableDef.TableType == catalog.SystemOrdinaryRel {
		createStr = fmt.Sprintf("CREATE TABLE `%s` (", tblName)
	} else if tableDef.TableType == catalog.SystemExternalRel {
		createStr = fmt.Sprintf("CREATE EXTERNAL TABLE `%s` (", tblName)
	} else if tableDef.TableType == catalog.SystemClusterRel {
		createStr = fmt.Sprintf("CREATE CLUSTER TABLE `%s` (", tblName)
	} else if tblName == catalog.MO_DATABASE || tblName == catalog.MO_TABLES || tblName == catalog.MO_COLUMNS {
		createStr = fmt.Sprintf("CREATE TABLE `%s` (", tblName)
	}

	rowCount := 0
	var pkDefs []string
	isClusterTable := util.TableIsClusterTable(tableDef.TableType)

	colIdToName := make(map[uint64]string)
	for _, col := range tableDef.Cols {
		colName := col.Name
		colIdToName[col.ColId] = col.Name
		if colName == catalog.Row_ID {
			continue
		}
		//the non-sys account skips the column account_id of the cluster table
		if util.IsClusterTableAttribute(colName) &&
			isClusterTable &&
			ctx.GetAccountId() != catalog.System_Account {
			continue
		}
		nullOrNot := "NOT NULL"
		// col.Default must be not nil
		if len(col.Default.OriginString) > 0 {
			nullOrNot = "DEFAULT " + col.Default.OriginString
		} else if col.Default.NullAbility {
			nullOrNot = "DEFAULT NULL"
		}

		if col.Typ.AutoIncr {
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
		typ := types.T(col.Typ.Id).ToType()
		typeStr := typ.String()
		if types.IsDecimal(typ.Oid) { //after decimal fix,remove this
			typeStr = fmt.Sprintf("DECIMAL(%d,%d)", col.Typ.Width, col.Typ.Scale)
		}
		if typ.Oid == types.T_varchar || typ.Oid == types.T_char ||
			typ.Oid == types.T_binary || typ.Oid == types.T_varbinary {
			typeStr += fmt.Sprintf("(%d)", col.Typ.Width)
		}
		if types.IsFloat(typ.Oid) && col.Typ.Scale != -1 {
			typeStr += fmt.Sprintf("(%d,%d)", col.Typ.Width, col.Typ.Scale)
		}

		updateOpt := ""
		if col.OnUpdate != nil && col.OnUpdate.Expr != nil {
			updateOpt = " ON UPDATE " + col.OnUpdate.OriginString
		}
		createStr += fmt.Sprintf("`%s` %s %s%s%s", colName, typeStr, nullOrNot, updateOpt, hasAttrComment)
		rowCount++
		if col.Primary {
			pkDefs = append(pkDefs, colName)
		}
	}

	// If it is a composite primary key, get the component columns of the composite primary key
	if tableDef.Pkey != nil && len(tableDef.Pkey.Names) > 1 {
		pkDefs = append(pkDefs, tableDef.Pkey.Names...)
	}

	if len(pkDefs) != 0 {
		pkStr := "PRIMARY KEY ("
		for i, def := range pkDefs {
			if i == len(pkDefs)-1 {
				pkStr += fmt.Sprintf("`%s`", def)
			} else {
				pkStr += fmt.Sprintf("`%s`,", def)
			}
		}
		pkStr += ")"
		if rowCount != 0 {
			createStr += ",\n"
		}
		createStr += pkStr
	}

	if tableDef.Indexes != nil {
		for _, indexdef := range tableDef.Indexes {
			var indexStr string
			if indexdef.Unique {
				indexStr = "UNIQUE KEY "
			} else {
				indexStr = "KEY "
			}
			indexStr += fmt.Sprintf("`%s` (", indexdef.IndexName)
			for num, part := range indexdef.Parts {
				if num == len(indexdef.Parts)-1 {
					indexStr += fmt.Sprintf("`%s`", part)
				} else {
					indexStr += fmt.Sprintf("`%s`,", part)
				}
			}
			indexStr += ")"
			if indexdef.Comment != "" {
				indexdef.Comment = strings.Replace(indexdef.Comment, "'", "\\'", -1)
				indexStr += fmt.Sprintf(" COMMENT '%s'", indexdef.Comment)
			}
			if rowCount != 0 {
				createStr += ",\n"
			}
			createStr += indexStr
		}
	}

	for _, fk := range tableDef.Fkeys {
		colNames := make([]string, len(fk.Cols))
		for i, colId := range fk.Cols {
			colNames[i] = colIdToName[colId]
		}
		_, fkTableDef := ctx.ResolveById(fk.ForeignTbl)
		fkColIdToName := make(map[uint64]string)
		for _, col := range fkTableDef.Cols {
			fkColIdToName[col.ColId] = col.Name
		}
		fkColNames := make([]string, len(fk.ForeignCols))
		for i, colId := range fk.ForeignCols {
			fkColNames[i] = fkColIdToName[colId]
		}

		if rowCount != 0 {
			createStr += ",\n"
		}
		createStr += fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY (`%s`) REFERENCES `%s` (`%s`) ON DELETE %s ON UPDATE %s",
			fk.Name, strings.Join(colNames, "`,`"), fkTableDef.Name, strings.Join(fkColNames, "`,`"), fk.OnDelete.String(), fk.OnUpdate.String())
	}

	if rowCount != 0 {
		createStr += "\n"
	}
	createStr += ")"

	if tableDef.ClusterBy != nil {
		clusterby := " CLUSTER BY ("
		if util.JudgeIsCompositeClusterByColumn(tableDef.ClusterBy.Name) {
			//multi column clusterby
			cbNames := util.SplitCompositeClusterByColumnName(tableDef.ClusterBy.Name)
			for i, cbName := range cbNames {
				if i != 0 {
					clusterby += fmt.Sprintf(", `%s`", cbName)
				} else {
					clusterby += fmt.Sprintf("`%s`", cbName)
				}
			}
		} else {
			//single column cluster by
			clusterby += fmt.Sprintf("`%s`", tableDef.ClusterBy.Name)
		}
		clusterby += ")"
		createStr += clusterby
	}

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
	}

	if tableDef.Partition != nil {
		partition = ` ` + tableDef.Partition.PartitionMsg
	}

	createStr += comment
	createStr += partition

	if tableDef.TableType == catalog.SystemExternalRel {
		param := tree.ExternParam{}
		err := json.Unmarshal([]byte(tableDef.Createsql), &param)
		if err != nil {
			return nil, err
		}
		createStr += fmt.Sprintf(" INFILE{'FILEPATH'='%s','COMPRESSION'='%s','FORMAT'='%s','JSONDATA'='%s'}", param.Filepath, param.CompressType, param.Format, param.JsonData)

		escapedby := ""
		if param.Tail.Fields.EscapedBy != byte(0) {
			escapedby = fmt.Sprintf(" ESCAPED BY '%c'", param.Tail.Fields.EscapedBy)
		}

		line := ""
		if param.Tail.Lines.StartingBy != "" {
			line = fmt.Sprintf(" LINE STARTING BY '%s'", param.Tail.Lines.StartingBy)
		}
		lineEnd := ""
		if param.Tail.Lines.TerminatedBy == "\n" || param.Tail.Lines.TerminatedBy == "\r\n" {
			lineEnd = " TERMINATED BY '\\\\n'"
		} else {
			lineEnd = fmt.Sprintf(" TERMINATED BY '%s'", param.Tail.Lines.TerminatedBy)
		}
		if len(line) > 0 {
			line += lineEnd
		} else {
			line = " LINES" + lineEnd
		}

		createStr += fmt.Sprintf(" FIELDS TERMINATED BY '%s' ENCLOSED BY '%c'%s", param.Tail.Fields.Terminated, rune(param.Tail.Fields.EnclosedBy), escapedby)
		createStr += line
		if param.Tail.IgnoredLines > 0 {
			createStr += fmt.Sprintf(" IGNORE %d LINES", param.Tail.IgnoredLines)
		}
	}

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
		return nil, moerr.NewInvalidInput(ctx.GetContext(), "show view '%s' is not a valid view", tblName)
	}
	sqlStr := "select \"%s\" as `View`, \"%s\" as `Create View`"
	var viewStr string
	if tableDef.TableType == catalog.SystemViewRel {
		viewStr = tableDef.ViewSql.View
	}

	var viewData ViewData
	err := json.Unmarshal([]byte(viewStr), &viewData)
	if err != nil {
		return nil, err
	}

	// FixMe  We need a better escape function
	stmtStr := strings.ReplaceAll(viewData.Stmt, "\"", "\\\"")
	sqlStr = fmt.Sprintf(sqlStr, tblName, fmt.Sprint(stmtStr))

	// logutil.Info(sqlStr)

	return returnByRewriteSQL(ctx, sqlStr, plan.DataDefinition_SHOW_CREATETABLE)
}

func buildShowDatabases(stmt *tree.ShowDatabases, ctx CompilerContext) (*Plan, error) {
	if stmt.Like != nil && stmt.Where != nil {
		return nil, moerr.NewSyntaxError(ctx.GetContext(), "like clause and where clause cannot exist at the same time")
	}

	accountId := ctx.GetAccountId()
	ddlType := plan.DataDefinition_SHOW_DATABASES

	var sql string
	// Any account should shows database MO_CATALOG_DB_NAME
	if accountId == catalog.System_Account {
		accountClause := fmt.Sprintf("account_id = %v or (account_id = 0 and datname = '%s')", accountId, MO_CATALOG_DB_NAME)
		sql = fmt.Sprintf("SELECT datname `Database` FROM %s.mo_database where (%s)", MO_CATALOG_DB_NAME, accountClause)
	} else {
		sql = fmt.Sprintf("SELECT datname `Database` FROM %s.mo_database", MO_CATALOG_DB_NAME)
	}

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

func buildShowSequences(stmt *tree.ShowSequences, ctx CompilerContext) (*Plan, error) {
	dbName := stmt.DBName

	if stmt.DBName == "" {
		dbName = ctx.DefaultDatabase()
	} else if !ctx.DatabaseExists(dbName) {
		return nil, moerr.NewBadDB(ctx.GetContext(), dbName)
	}

	if dbName == "" {
		return nil, moerr.NewNoDB(ctx.GetContext())
	}

	ddlType := plan.DataDefinition_SHOW_SEQUENCES

	sql := fmt.Sprintf("select %s.mo_tables.relname as `Names`, mo_show_visible_bin(%s.mo_columns.atttyp, 2) as 'Data Type' from %s.mo_tables left join %s.mo_columns on %s.mo_tables.rel_id = %s.mo_columns.att_relname_id where %s.mo_tables.relkind = '%s' and %s.mo_tables.reldatabase = '%s' and %s.mo_columns.attname = '%s'", MO_CATALOG_DB_NAME,
		MO_CATALOG_DB_NAME, MO_CATALOG_DB_NAME, MO_CATALOG_DB_NAME, MO_CATALOG_DB_NAME, MO_CATALOG_DB_NAME, MO_CATALOG_DB_NAME, catalog.SystemSequenceRel, MO_CATALOG_DB_NAME, dbName, MO_CATALOG_DB_NAME, Sequence_cols_name[0])

	if stmt.Where != nil {
		return returnByWhereAndBaseSQL(ctx, sql, stmt.Where, ddlType)
	}

	return returnByRewriteSQL(ctx, sql, ddlType)
}

func buildShowTables(stmt *tree.ShowTables, ctx CompilerContext) (*Plan, error) {
	if stmt.Like != nil && stmt.Where != nil {
		return nil, moerr.NewSyntaxError(ctx.GetContext(), "like clause and where clause cannot exist at the same time")
	}

	if stmt.Open {
		return nil, moerr.NewNYI(ctx.GetContext(), "statement: '%v'", tree.String(stmt, dialect.MYSQL))
	}

	accountId := ctx.GetAccountId()
	dbName := stmt.DBName
	if stmt.DBName == "" {
		dbName = ctx.DefaultDatabase()
	} else if !ctx.DatabaseExists(dbName) {
		return nil, moerr.NewBadDB(ctx.GetContext(), dbName)
	}

	if dbName == "" {
		return nil, moerr.NewNoDB(ctx.GetContext())
	}
	ddlType := plan.DataDefinition_SHOW_TABLES
	var tableType string
	if stmt.Full {
		tableType = fmt.Sprintf(", case relkind when 'v' then 'VIEW' when '%s' then 'CLUSTER TABLE' else 'BASE TABLE' end as Table_type", catalog.SystemClusterRel)
	}

	var sql string
	if accountId == catalog.System_Account {
		mustShowTable := "relname = 'mo_database' or relname = 'mo_tables' or relname = 'mo_columns'"
		clusterTable := fmt.Sprintf(" or relkind = '%s'", catalog.SystemClusterRel)
		accountClause := fmt.Sprintf("account_id = %v or (account_id = 0 and (%s))", accountId, mustShowTable+clusterTable)
		sql = fmt.Sprintf("SELECT relname as `Tables_in_%s` %s FROM %s.mo_tables WHERE reldatabase = '%s' and relname != '%s' and relname not like '%s' and (%s)",
			dbName, tableType, MO_CATALOG_DB_NAME, dbName, catalog.AutoIncrTableName, catalog.IndexTableNamePrefix+"%", accountClause)
	} else {
		sql = fmt.Sprintf("SELECT relname as `Tables_in_%s` %s FROM %s.mo_tables WHERE reldatabase = '%s' and relname != '%s' and relname not like '%s'",
			dbName, tableType, MO_CATALOG_DB_NAME, dbName, catalog.AutoIncrTableName, catalog.IndexTableNamePrefix+"%")
	}

	// Do not show sequences.
	sql += fmt.Sprintf(" and relkind != '%s'", catalog.SystemSequenceRel)

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

func buildShowTableNumber(stmt *tree.ShowTableNumber, ctx CompilerContext) (*Plan, error) {
	accountId := ctx.GetAccountId()
	dbName := stmt.DbName
	if stmt.DbName == "" {
		dbName = ctx.DefaultDatabase()
	} else if !ctx.DatabaseExists(dbName) {
		return nil, moerr.NewBadDB(ctx.GetContext(), dbName)
	}

	if dbName == "" {
		return nil, moerr.NewNoDB(ctx.GetContext())
	}

	ddlType := plan.DataDefinition_SHOW_TABLES
	var sql string

	if accountId == catalog.System_Account {
		mustShowTable := "relname = 'mo_database' or relname = 'mo_tables' or relname = 'mo_columns'"
		clusterTable := fmt.Sprintf(" or relkind = '%s'", catalog.SystemClusterRel)
		accountClause := fmt.Sprintf("account_id = %v or (account_id = 0 and (%s))", accountId, mustShowTable+clusterTable)
		sql = fmt.Sprintf("SELECT count(relname) `Number of tables in %s`  FROM %s.mo_tables WHERE reldatabase = '%s' and relname != '%s' and relname not like '%s' and (%s)",
			dbName, MO_CATALOG_DB_NAME, dbName, catalog.AutoIncrTableName, catalog.IndexTableNamePrefix+"%", accountClause)
	} else {
		sql = "SELECT count(relname) `Number of tables in %s` FROM %s.mo_tables WHERE reldatabase = '%s' and relname != '%s' and relname not like '%s'"
		sql = fmt.Sprintf(sql, dbName, MO_CATALOG_DB_NAME, dbName, catalog.AutoIncrTableName, catalog.IndexTableNamePrefix+"%")
	}

	return returnByRewriteSQL(ctx, sql, ddlType)
}

func buildShowColumnNumber(stmt *tree.ShowColumnNumber, ctx CompilerContext) (*Plan, error) {
	accountId := ctx.GetAccountId()
	dbName := stmt.Table.GetDBName()
	if dbName == "" {
		dbName = ctx.DefaultDatabase()
	} else if !ctx.DatabaseExists(dbName) {
		return nil, moerr.NewBadDB(ctx.GetContext(), dbName)
	}

	tblName := string(stmt.Table.ToTableName().ObjectName)
	_, tableDef := ctx.Resolve(dbName, tblName)
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), dbName, tblName)
	}

	ddlType := plan.DataDefinition_SHOW_COLUMNS
	var sql string

	if accountId == catalog.System_Account {
		mustShowTable := "att_relname = 'mo_database' or att_relname = 'mo_tables' or att_relname = 'mo_columns'"
		clusterTable := ""
		if util.TableIsClusterTable(tableDef.GetTableType()) {
			clusterTable = fmt.Sprintf(" or att_relname = '%s'", tblName)
		}
		accountClause := fmt.Sprintf("account_id = %v or (account_id = 0 and (%s))", accountId, mustShowTable+clusterTable)
		sql = "SELECT count(attname) `Number of columns in %s` FROM %s.mo_columns WHERE att_database = '%s' AND att_relname = '%s' AND (%s)"
		sql = fmt.Sprintf(sql, tblName, MO_CATALOG_DB_NAME, dbName, tblName, accountClause)
	} else {
		sql = "SELECT count(attname) `Number of columns in %s` FROM %s.mo_columns WHERE att_database = '%s' AND att_relname = '%s'"
		sql = fmt.Sprintf(sql, tblName, MO_CATALOG_DB_NAME, dbName, tblName)
	}

	return returnByRewriteSQL(ctx, sql, ddlType)
}

func buildShowTableValues(stmt *tree.ShowTableValues, ctx CompilerContext) (*Plan, error) {
	dbName := stmt.Table.GetDBName()
	if dbName == "" {
		dbName = ctx.DefaultDatabase()
	} else if !ctx.DatabaseExists(dbName) {
		return nil, moerr.NewBadDB(ctx.GetContext(), dbName)
	}

	tblName := string(stmt.Table.ToTableName().ObjectName)
	_, tableDef := ctx.Resolve(dbName, tblName)
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), dbName, tblName)
	}

	ddlType := plan.DataDefinition_SHOW_TARGET

	sql := "SELECT"
	tableCols := tableDef.Cols
	isAllNull := true
	for i := range tableCols {
		colName := tableCols[i].Name
		if types.T(tableCols[i].GetTyp().Id) == types.T_json {
			sql += " null as `max(%s)`, null as `min(%s)`,"
			sql = fmt.Sprintf(sql, colName, colName)
		} else {
			sql += " max(%s), min(%s),"
			sql = fmt.Sprintf(sql, colName, colName)
			isAllNull = false
		}
	}
	sql = sql[:len(sql)-1]
	sql += " FROM %s"

	if isAllNull {
		sql += " LIMIT 1"
	}
	sql = fmt.Sprintf(sql, tblName)

	return returnByRewriteSQL(ctx, sql, ddlType)
}

func buildShowColumns(stmt *tree.ShowColumns, ctx CompilerContext) (*Plan, error) {
	if stmt.Like != nil && stmt.Where != nil {
		return nil, moerr.NewSyntaxError(ctx.GetContext(), "like clause and where clause cannot exist at the same time")
	}

	accountId := ctx.GetAccountId()
	dbName := stmt.Table.GetDBName()
	if dbName == "" {
		dbName = ctx.DefaultDatabase()
	} else if !ctx.DatabaseExists(dbName) {
		return nil, moerr.NewBadDB(ctx.GetContext(), dbName)
	}

	tblName := string(stmt.Table.ToTableName().ObjectName)
	_, tableDef := ctx.Resolve(dbName, tblName)
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), dbName, tblName)
	}

	var keyStr string
	if dbName == catalog.MO_CATALOG && tblName == catalog.MO_DATABASE {
		keyStr = "case when attname = '" + catalog.SystemDBAttr_ID + "' then 'PRI' else '' END as `Key`"
	} else if dbName == catalog.MO_CATALOG && tblName == catalog.MO_TABLES {
		keyStr = "case when attname = '" + catalog.SystemRelAttr_ID + "' then 'PRI' else '' END as `Key`"
	} else if dbName == catalog.MO_CATALOG && tblName == catalog.MO_COLUMNS {
		keyStr = "case when attname = '" + catalog.SystemColAttr_UniqName + "' then 'PRI' else '' END as `Key`"
	} else {
		if tableDef.Pkey != nil {
			keyStr += "case"
			for _, name := range tableDef.Pkey.Names {
				keyStr += " when attname = "
				keyStr += "'" + name + "'"
				keyStr += " then 'PRI'"
			}
			keyStr += " else '' END as `Key`"
		} else {
			keyStr = "'' as `Key`"
		}
	}

	ddlType := plan.DataDefinition_SHOW_COLUMNS

	var sql string
	if accountId == catalog.System_Account {
		mustShowTable := "att_relname = 'mo_database' or att_relname = 'mo_tables' or att_relname = 'mo_columns'"
		clusterTable := ""
		if util.TableIsClusterTable(tableDef.GetTableType()) {
			clusterTable = fmt.Sprintf(" or att_relname = '%s'", tblName)
		}
		accountClause := fmt.Sprintf("account_id = %v or (account_id = 0 and (%s))", accountId, mustShowTable+clusterTable)
		sql = "SELECT attname `Field`, mo_show_visible_bin(atttyp,3) `Type`, iff(attnotnull = 0, 'YES', 'NO') `Null`, %s, mo_show_visible_bin(att_default, 1) `Default`, '' `Extra`,  att_comment `Comment` FROM %s.mo_columns WHERE att_database = '%s' AND att_relname = '%s' AND (%s) AND attname != '__mo_rowid' AND attname not like '__mo_cpkey_%%' AND attname not like '__mo_cbkey_%%' ORDER BY attnum"
		if stmt.Full {
			sql = "SELECT attname `Field`, mo_show_visible_bin(atttyp,3) `Type`, null `Collation`, iff(attnotnull = 0, 'YES', 'NO') `Null`, %s, mo_show_visible_bin(att_default, 1) `Default`,  '' `Extra`,'select,insert,update,references' `Privileges`, att_comment `Comment` FROM %s.mo_columns WHERE att_database = '%s' AND att_relname = '%s' AND (%s) AND attname != '__mo_rowid' AND attname not like '__mo_cpkey_%%' AND attname not like '__mo_cbkey_%%' ORDER BY attnum"
		}
		sql = fmt.Sprintf(sql, keyStr, MO_CATALOG_DB_NAME, dbName, tblName, accountClause)
	} else {
		sql = "SELECT attname `Field`, mo_show_visible_bin(atttyp,3) `Type`, iff(attnotnull = 0, 'YES', 'NO') `Null`, %s, mo_show_visible_bin(att_default, 1) `Default`, '' `Extra`,  att_comment `Comment` FROM %s.mo_columns WHERE att_database = '%s' AND att_relname = '%s' AND attname != '__mo_rowid' AND attname not like '__mo_cpkey_%%' AND attname not like '__mo_cbkey_%%' ORDER BY attnum"
		if stmt.Full {
			sql = "SELECT attname `Field`, mo_show_visible_bin(atttyp,3) `Type`, null `Collation`, iff(attnotnull = 0, 'YES', 'NO') `Null`, %s, mo_show_visible_bin(att_default, 1) `Default`,  '' `Extra`,'select,insert,update,references' `Privileges`, att_comment `Comment` FROM %s.mo_columns WHERE att_database = '%s' AND att_relname = '%s' AND attname != '__mo_rowid' AND attname not like '__mo_cpkey_%%' AND attname not like '__mo_cbkey_%%' ORDER BY attnum"
		}
		sql = fmt.Sprintf(sql, keyStr, MO_CATALOG_DB_NAME, dbName, tblName)
	}

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

func buildShowTableStatus(stmt *tree.ShowTableStatus, ctx CompilerContext) (*Plan, error) {
	if stmt.Like != nil && stmt.Where != nil {
		return nil, moerr.NewSyntaxError(ctx.GetContext(), "like clause and where clause cannot exist at the same time")
	}

	dbName := stmt.DbName
	if stmt.DbName == "" {
		dbName = ctx.DefaultDatabase()
		stmt.DbName = dbName
		if dbName == "" {
			return nil, moerr.NewNoDB(ctx.GetContext())
		}
	} else if !ctx.DatabaseExists(dbName) {
		return nil, moerr.NewBadDB(ctx.GetContext(), dbName)
	}

	ddlType := plan.DataDefinition_SHOW_TABLE_STATUS
	accountId := ctx.GetAccountId()
	mustShowTable := "relname = 'mo_database' or relname = 'mo_tables' or relname = 'mo_columns'"
	accountClause := fmt.Sprintf("account_id = %v or (account_id = 0 and (%s))", accountId, mustShowTable)
	sql := "select relname as `Name`, 'Tae' as `Engine`, 'Dynamic' as `Row_format`, 0 as `Rows`, 0 as `Avg_row_length`, 0 as `Data_length`, 0 as `Max_data_length`, 0 as `Index_length`, 'NULL' as `Data_free`, 0 as `Auto_increment`, created_time as `Create_time`, 'NULL' as `Update_time`, 'NULL' as `Check_time`, 'utf-8' as `Collation`, 'NULL' as `Checksum`, '' as `Create_options`, rel_comment as `Comment` from %s.mo_tables where reldatabase = '%s' and relname != '%s' and relname not like '%s' and (%s)"

	sql = fmt.Sprintf(sql, MO_CATALOG_DB_NAME, dbName, catalog.AutoIncrTableName, catalog.IndexTableNamePrefix+"%", accountClause)

	if stmt.Where != nil {
		return returnByWhereAndBaseSQL(ctx, sql, stmt.Where, ddlType)
	}

	if stmt.Like != nil {
		// append filter [AND ma.relname like stmt.Like] to WHERE clause
		likeExpr := stmt.Like
		likeExpr.Left = tree.SetUnresolvedName("relname")
		return returnByLikeAndSQL(ctx, sql, likeExpr, ddlType)
	}

	return returnByRewriteSQL(ctx, sql, ddlType)
}

// TODO: Implement show target
func buildShowTarget(stmt *tree.ShowTarget, ctx CompilerContext) (*Plan, error) {
	ddlType := plan.DataDefinition_SHOW_TARGET
	sql := ""
	switch stmt.Type {
	case tree.ShowCharset:
		sql = "select '' as `Charset`, '' as `Description`, '' as `Default collation`, '' as `Maxlen` where 0"
	case tree.ShowTriggers:
		return buildShowTriggers(stmt, ctx)
	default:
		sql = "select 1 where 0"
	}
	return returnByRewriteSQL(ctx, sql, ddlType)
}

func buildShowLocks(stmt *tree.ShowLocks, ctx CompilerContext) (*Plan, error) {
	ddlType := plan.DataDefinition_SHOW_TARGET
	sql := "select 1 where 0"
	return returnByRewriteSQL(ctx, sql, ddlType)
}

func buildShowNodeList(stmt *tree.ShowNodeList, ctx CompilerContext) (*Plan, error) {
	ddlType := plan.DataDefinition_SHOW_TARGET
	sql := "select 1 where 0"
	return returnByRewriteSQL(ctx, sql, ddlType)
}

func buildShowFunctionStatus(stmt *tree.ShowFunctionStatus, ctx CompilerContext) (*Plan, error) {
	if stmt.Like != nil && stmt.Where != nil {
		return nil, moerr.NewSyntaxError(ctx.GetContext(), "like clause and where clause cannot exist at the same time")
	}
	ddlType := plan.DataDefinition_SHOW_TARGET
	if stmt.Like != nil && stmt.Where != nil {
		return nil, moerr.NewSyntaxError(ctx.GetContext(), "like clause and where clause cannot exist at the same time")
	}

	sql := fmt.Sprintf("SELECT db as `Db`, name as `Name`, type as `Type`, definer as `Definer`, modified_time as `Modified`, created_time as `Created`, security_type as `Security_type`, comment as `Comment`, character_set_client, collation_connection, database_collation as `Database Collation` FROM %s.mo_user_defined_function", MO_CATALOG_DB_NAME)

	if stmt.Where != nil {
		return returnByWhereAndBaseSQL(ctx, sql, stmt.Where, ddlType)
	}

	if stmt.Like != nil {
		// append filter [AND ma.attname like stmt.Like] to WHERE clause
		likeExpr := stmt.Like
		likeExpr.Left = tree.SetUnresolvedName("name")
		return returnByLikeAndSQL(ctx, sql, likeExpr, ddlType)
	}

	return returnByRewriteSQL(ctx, sql, ddlType)
}

func buildShowTriggers(stmt *tree.ShowTarget, ctx CompilerContext) (*Plan, error) {
	if stmt.Like != nil && stmt.Where != nil {
		return nil, moerr.NewSyntaxError(ctx.GetContext(), "like clause and where clause cannot exist at the same time")
	}

	dbName := stmt.DbName
	if stmt.DbName == "" {
		dbName = ctx.DefaultDatabase()
		stmt.DbName = dbName
		if dbName == "" {
			return nil, moerr.NewNoDB(ctx.GetContext())
		}
	} else if !ctx.DatabaseExists(dbName) {
		return nil, moerr.NewBadDB(ctx.GetContext(), dbName)
	}

	ddlType := plan.DataDefinition_SHOW_TARGET
	sql := fmt.Sprintf("SELECT trigger_name as `Trigger`, event_manipulation as `Event`, event_object_table as `Table`, action_statement as `Statement`, action_timing as `Timing`, created as `Created`, sql_mode, definer as `Definer`, character_set_client, collation_connection, database_collation as `Database Collation` FROM %s.TRIGGERS ", INFORMATION_SCHEMA)

	if stmt.Where != nil {
		return returnByWhereAndBaseSQL(ctx, sql, stmt.Where, ddlType)
	}

	if stmt.Like != nil {
		// append filter [AND ma.attname like stmt.Like] to WHERE clause
		likeExpr := stmt.Like
		likeExpr.Left = tree.SetUnresolvedName("event_object_table")
		return returnByLikeAndSQL(ctx, sql, likeExpr, ddlType)
	}

	return returnByRewriteSQL(ctx, sql, ddlType)
}

func buildShowIndex(stmt *tree.ShowIndex, ctx CompilerContext) (*Plan, error) {
	dbName := string(stmt.TableName.Schema())
	if dbName == "" {
		dbName = ctx.DefaultDatabase()
		if dbName == "" {
			return nil, moerr.NewNoDB(ctx.GetContext())
		}
	} else if !ctx.DatabaseExists(dbName) {
		return nil, moerr.NewBadDB(ctx.GetContext(), dbName)
	}

	tblName := string(stmt.TableName.Name())
	_, tableDef := ctx.Resolve(dbName, tblName)
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), dbName, tblName)
	}

	accountId := ctx.GetAccountId()
	ddlType := plan.DataDefinition_SHOW_INDEX
	mustShowTable := "att_relname = 'mo_database' or att_relname = 'mo_tables' or att_relname = 'mo_columns'"
	accountClause := fmt.Sprintf("account_id = %v or (account_id = 0 and (%s))", accountId, mustShowTable)
	sql := "select att_relname as `Table`,  iff(att_constraint_type = 'p', 1, 0) as `Non_unique`,  iff(att_constraint_type = 'p', 'PRIMARY', attname) as `Key_name`,  1 as `Seq_in_index`, attname as `Column_name`, 'A' as `Collation`, 0 as `Cardinality`, 'NULL' as `Sub_part`, 'NULL' as `Packed`, iff(attnotnull = 0, 'YES', 'NO') as `Null`, '' as 'Index_type', att_comment as `Comment`,  iff(att_is_hidden = 0, 'YES', 'NO') as `Visible`, 'NULL' as `Expression` FROM %s.mo_columns WHERE att_database = '%s' AND att_relname = '%s' AND (%s)"

	sql = fmt.Sprintf(sql, MO_CATALOG_DB_NAME, dbName, tblName, accountClause)

	if stmt.Where != nil {
		return returnByWhereAndBaseSQL(ctx, sql, stmt.Where, ddlType)
	}

	return returnByRewriteSQL(ctx, sql, ddlType)
}

// TODO: Improve SQL. Currently, Lack of the mata of grants
func buildShowGrants(stmt *tree.ShowGrants, ctx CompilerContext) (*Plan, error) {

	ddlType := plan.DataDefinition_SHOW_TARGET
	if stmt.ShowGrantType == tree.GrantForRole {
		role_name := stmt.Roles[0].UserName
		sql := "select concat(\"GRANT \", p.privilege_name, ' ON ', p.obj_type, ' ', case p.obj_type when 'account' then '' else p.privilege_level end,   \" `%s`\")  as `Grants for %s` from  %s.mo_role_privs as p where p.role_name = '%s';"
		sql = fmt.Sprintf(sql, role_name, role_name, MO_CATALOG_DB_NAME, role_name)
		return returnByRewriteSQL(ctx, sql, ddlType)
	} else {
		if stmt.Hostname == "" {
			stmt.Hostname = MO_DEFUALT_HOSTNAME
		}
		if stmt.Username == "" {
			stmt.Username = ctx.GetUserName()
		}
		sql := "select concat(\"GRANT \", p.privilege_name, ' ON ', p.obj_type, ' ', case p.obj_type when 'account' then '' else p.privilege_level end,   \" `%s`\", \"@\", \"`%s`\")  as `Grants for %s@localhost` from mo_catalog.mo_user as u, mo_catalog.mo_role_privs as p, mo_catalog.mo_user_grant as g where g.role_id = p.role_id and g.user_id = u.user_id and u.user_name = '%s' and u.user_host = '%s';"
		sql = fmt.Sprintf(sql, stmt.Username, stmt.Hostname, stmt.Username, stmt.Username, stmt.Hostname)
		return returnByRewriteSQL(ctx, sql, ddlType)
	}
}

func buildShowVariables(stmt *tree.ShowVariables, ctx CompilerContext) (*Plan, error) {
	showVariables := &plan.ShowVariables{
		Global: stmt.Global,
	}

	// we deal with 'show vriables' statement in frontend now.
	// so just return an empty plan in building plan for prepare statment is ok.

	// if stmt.Like != nil && stmt.Where != nil {
	// 	return nil, moerr.NewSyntaxError(ctx.GetContext(), "like clause and where clause cannot exist at the same time")
	// }

	// builder := NewQueryBuilder(plan.Query_SELECT, ctx)
	// binder := NewWhereBinder(builder, &BindContext{})

	// if stmt.Like != nil {
	//  // here will error because stmt.Like.Left is nil, you need add left expr like : stmt.Like.Left = tree.SetUnresolvedName("column_name")
	//  // but we have no column name, because Variables is save in a hashmap in frontend, not a table.
	// 	expr, err := binder.bindComparisonExpr(stmt.Like, 0, false)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	showVariables.Where = append(showVariables.Where, expr)
	// }
	// if stmt.Where != nil {
	// 	exprs, err := splitAndBindCondition(stmt.Where.Expr, &BindContext{})
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	showVariables.Where = append(showVariables.Where, exprs...)
	// }

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

func buildShowStatus(stmt *tree.ShowStatus, ctx CompilerContext) (*Plan, error) {
	ddlType := plan.DataDefinition_SHOW_STATUS
	sql := "select '' as `Variable_name`, '' as `Value` where 0"
	return returnByRewriteSQL(ctx, sql, ddlType)
}

func buildShowCollation(stmt *tree.ShowCollation, ctx CompilerContext) (*Plan, error) {
	ddlType := plan.DataDefinition_SHOW_COLLATION
	sql := "select 'utf8mb4_bin' as `Collation`, 'utf8mb4' as `Charset`, 46 as `Id`, 'Yes' as `Compiled`, 1 as `Sortlen`"
	return returnByRewriteSQL(ctx, sql, ddlType)
}

func buildShowProcessList(stmt *tree.ShowProcessList, ctx CompilerContext) (*Plan, error) {
	ddlType := plan.DataDefinition_SHOW_PROCESSLIST
	sql := "select '' as `Id`, '' as `User`, '' as `Host`, '' as `db` , '' as `Command`, '' as `Time` , '' as `State`, '' as `Info` where 0"
	return returnByRewriteSQL(ctx, sql, ddlType)
}

func returnByRewriteSQL(ctx CompilerContext, sql string,
	ddlType plan.DataDefinition_DdlType) (*Plan, error) {
	stmt, err := getRewriteSQLStmt(ctx, sql)
	if err != nil {
		return nil, err
	}
	return getReturnDdlBySelectStmt(ctx, stmt, ddlType)
}

func returnByWhereAndBaseSQL(ctx CompilerContext, baseSQL string,
	where *tree.Where, ddlType plan.DataDefinition_DdlType) (*Plan, error) {
	sql := fmt.Sprintf("SELECT * FROM (%s) tbl", baseSQL)
	// logutil.Info(sql)
	newStmt, err := getRewriteSQLStmt(ctx, sql)
	if err != nil {
		return nil, err
	}
	// set show statement's where clause to new statement
	newStmt.(*tree.Select).Select.(*tree.SelectClause).Where = where
	return getReturnDdlBySelectStmt(ctx, newStmt, ddlType)
}

func returnByLikeAndSQL(ctx CompilerContext, sql string, like *tree.ComparisonExpr,
	ddlType plan.DataDefinition_DdlType) (*Plan, error) {
	newStmt, err := getRewriteSQLStmt(ctx, sql)
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
	// logutil.Info(tree.String(newStmt, dialect.MYSQL))
	return getReturnDdlBySelectStmt(ctx, newStmt, ddlType)
}

func getRewriteSQLStmt(ctx CompilerContext, sql string) (tree.Statement, error) {
	newStmts, err := parsers.Parse(ctx.GetContext(), dialect.MYSQL, sql, 1)
	if err != nil {
		return nil, err
	}
	if len(newStmts) != 1 {
		return nil, moerr.NewInvalidInput(ctx.GetContext(), "rewrite can only contain one statement, %d provided", len(newStmts))
	}
	return newStmts[0], nil
}

func getReturnDdlBySelectStmt(ctx CompilerContext, stmt tree.Statement,
	ddlType plan.DataDefinition_DdlType) (*Plan, error) {
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
