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
	"go/constant"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

const MO_CATALOG_DB_NAME = "mo_catalog"
const MO_DEFUALT_HOSTNAME = "localhost"
const INFORMATION_SCHEMA = "information_schema"

func buildShowCreateDatabase(stmt *tree.ShowCreateDatabase,
	ctx CompilerContext) (*Plan, error) {
	var err error
	var name string
	// snapshot to fix
	name, err = databaseIsValid(getSuitableDBName("", stmt.Name), ctx, Snapshot{TS: &timestamp.Timestamp{}})
	if err != nil {
		return nil, err
	}

	if sub, err := ctx.GetSubscriptionMeta(name, Snapshot{TS: &timestamp.Timestamp{}}); err != nil {
		return nil, err
	} else if sub != nil {
		accountId, err := ctx.GetAccountId()
		if err != nil {
			return nil, err
		}
		// get data from schema
		//sql := fmt.Sprintf("SELECT md.datname as `Database` FROM %s.mo_database md WHERE md.datname = '%s'", MO_CATALOG_DB_NAME, stmt.Name)
		sql := fmt.Sprintf("SELECT md.datname as `Database`,dat_createsql as `Create Database` FROM %s.mo_database md WHERE md.datname = '%s' and account_id=%d", MO_CATALOG_DB_NAME, stmt.Name, accountId)
		return returnByRewriteSQL(ctx, sql, plan.DataDefinition_SHOW_CREATEDATABASE)
	}

	sqlStr := "select \"%s\" as `Database`, \"%s\" as `Create Database`"
	createSql := fmt.Sprintf("CREATE DATABASE `%s`", name)
	sqlStr = fmt.Sprintf(sqlStr, name, createSql)

	return returnByRewriteSQL(ctx, sqlStr, plan.DataDefinition_SHOW_CREATEDATABASE)
}

func formatStr(str string) string {
	tmp := strings.Replace(str, "`", "``", -1)
	strLen := len(tmp)
	if strLen < 2 {
		return tmp
	}
	if tmp[0] == '\'' && tmp[strLen-1] == '\'' {
		return "'" + strings.Replace(tmp[1:strLen-1], "'", "''", -1) + "'"
	}
	return strings.Replace(tmp, "'", "''", -1)
}

func buildShowCreateTable(stmt *tree.ShowCreateTable, ctx CompilerContext) (*Plan, error) {
	var err error
	tblName := stmt.Name.GetTableName()
	dbName := stmt.Name.GetDBName()

	snapshot := &Snapshot{TS: &timestamp.Timestamp{}}
	if len(stmt.SnapshotName) > 0 {
		if snapshot, err = ctx.ResolveSnapshotWithSnapshotName(stmt.SnapshotName); err != nil {
			return nil, err
		}
	}

	dbName, err = databaseIsValid(getSuitableDBName(dbName, ""), ctx, *snapshot)
	if err != nil {
		return nil, err
	}

	// check if the database is a subscription
	sub, err := ctx.GetSubscriptionMeta(dbName, *snapshot)
	if err != nil {
		return nil, err
	}

	if sub != nil {
		ctx.SetQueryingSubscription(sub)
		defer func() {
			ctx.SetQueryingSubscription(nil)
		}()
	}

	_, tableDef := ctx.Resolve(dbName, tblName, *snapshot)
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
		if len(stmt.SnapshotName) > 0 {
			newStmt.SnapshotName = stmt.SnapshotName
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
		createStr = fmt.Sprintf("CREATE TABLE `%s` (", formatStr(tblName))
	} else if tableDef.TableType == catalog.SystemExternalRel {
		createStr = fmt.Sprintf("CREATE EXTERNAL TABLE `%s` (", formatStr(tblName))
	} else if tableDef.TableType == catalog.SystemClusterRel {
		createStr = fmt.Sprintf("CREATE CLUSTER TABLE `%s` (", formatStr(tblName))
	} else if tblName == catalog.MO_DATABASE || tblName == catalog.MO_TABLES || tblName == catalog.MO_COLUMNS {
		createStr = fmt.Sprintf("CREATE TABLE `%s` (", formatStr(tblName))
	}

	rowCount := 0
	var pkDefs []string
	isClusterTable := util.TableIsClusterTable(tableDef.TableType)

	colIdToName := make(map[uint64]string)
	for _, col := range tableDef.Cols {
		if col.Hidden {
			continue
		}
		colName := col.Name
		colIdToName[col.ColId] = col.Name
		if colName == catalog.Row_ID {
			continue
		}
		//the non-sys account skips the column account_id of the cluster table
		accountId, err := ctx.GetAccountId()
		if err != nil {
			return nil, err
		}
		if util.IsClusterTableAttribute(colName) &&
			isClusterTable &&
			accountId != catalog.System_Account {
			continue
		}
		nullOrNot := "NOT NULL"
		// col.Default must be not nil
		if len(col.Default.OriginString) > 0 {
			if !col.Primary {
				nullOrNot = "DEFAULT " + formatStr(col.Default.OriginString)
			}
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
		if typ.Oid.IsDecimal() { //after decimal fix,remove this
			typeStr = fmt.Sprintf("DECIMAL(%d,%d)", col.Typ.Width, col.Typ.Scale)
		}
		if typ.Oid == types.T_varchar || typ.Oid == types.T_char ||
			typ.Oid == types.T_binary || typ.Oid == types.T_varbinary ||
			typ.Oid.IsArrayRelate() || typ.Oid == types.T_bit {
			typeStr += fmt.Sprintf("(%d)", col.Typ.Width)
		}
		if typ.Oid.IsFloat() && col.Typ.Scale != -1 {
			typeStr += fmt.Sprintf("(%d,%d)", col.Typ.Width, col.Typ.Scale)
		}

		if typ.Oid.IsEnum() {
			enums := strings.Split(col.Typ.GetEnumvalues(), ",")
			typeStr += "("
			for i, enum := range enums {
				typeStr += fmt.Sprintf("'%s'", enum)
				if i < len(enums)-1 {
					typeStr += ","
				}
			}
			typeStr += ")"
		}

		updateOpt := ""
		if col.OnUpdate != nil && col.OnUpdate.Expr != nil {
			updateOpt = " ON UPDATE " + col.OnUpdate.OriginString
		}
		createStr += fmt.Sprintf("`%s` %s %s%s%s", formatStr(colName), typeStr, nullOrNot, updateOpt, hasAttrComment)
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
				pkStr += fmt.Sprintf("`%s`", formatStr(def))
			} else {
				pkStr += fmt.Sprintf("`%s`,", formatStr(def))
			}
		}
		pkStr += ")"
		if rowCount != 0 {
			createStr += ",\n"
		}
		createStr += pkStr
	}

	if tableDef.Indexes != nil {

		// We only print distinct index names. This is used to avoid printing the same index multiple times for IVFFLAT or
		// other multi-table indexes.
		indexNames := make(map[string]bool)

		for _, indexdef := range tableDef.Indexes {
			if _, ok := indexNames[indexdef.IndexName]; ok {
				continue
			} else {
				indexNames[indexdef.IndexName] = true
			}

			var indexStr string
			if indexdef.Unique {
				indexStr = "UNIQUE KEY "
			} else {
				indexStr = "KEY "
			}
			indexStr += fmt.Sprintf("`%s` ", formatStr(indexdef.IndexName))
			if !catalog.IsNullIndexAlgo(indexdef.IndexAlgo) {
				indexStr += fmt.Sprintf("USING %s ", indexdef.IndexAlgo)
			}
			indexStr += "("
			i := 0
			for _, part := range indexdef.Parts {
				if catalog.IsAlias(part) {
					continue
				}
				if i > 0 {
					indexStr += ","
				}

				indexStr += fmt.Sprintf("`%s`", formatStr(part))
				i++
			}

			indexStr += ")"
			if indexdef.IndexAlgoParams != "" {
				var paramList string
				paramList, err = catalog.IndexParamsToStringList(indexdef.IndexAlgoParams)
				if err != nil {
					return nil, err
				}
				indexStr += paramList
			}
			if indexdef.Comment != "" {
				indexdef.Comment = strings.Replace(indexdef.Comment, "'", "\\'", -1)
				indexStr += fmt.Sprintf(" COMMENT '%s'", formatStr(indexdef.Comment))
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

		var fkTableDef *TableDef

		//fk self reference
		if fk.ForeignTbl == 0 {
			fkTableDef = tableDef
		} else {
			if ctx.GetQueryingSubscription() != nil {
				_, fkTableDef = ctx.ResolveSubscriptionTableById(fk.ForeignTbl, ctx.GetQueryingSubscription())
			} else {
				_, fkTableDef = ctx.ResolveById(fk.ForeignTbl, *snapshot)
			}
		}

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
			formatStr(fk.Name), strings.Join(colNames, "`,`"), formatStr(fkTableDef.Name), strings.Join(fkColNames, "`,`"), fk.OnDelete.String(), fk.OnUpdate.String())
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
	}

	if tableDef.Partition != nil {
		partition = ` ` + tableDef.Partition.PartitionMsg
	}

	createStr += comment
	createStr += partition

	/**
	Fix issue: https://github.com/matrixorigin/MO-Cloud/issues/1028#issuecomment-1667642384
	Based on the grammar of the 'create table' in the file pkg/sql/parsers/dialect/mysql/mysql_sql.y
		https://github.com/matrixorigin/matrixone/blob/68db7260e411e5a4541eaccf78ca9bb57e810f24/pkg/sql/parsers/dialect/mysql/mysql_sql.y#L6076C7-L6076C7
		https://github.com/matrixorigin/matrixone/blob/68db7260e411e5a4541eaccf78ca9bb57e810f24/pkg/sql/parsers/dialect/mysql/mysql_sql.y#L6097
	The 'cluster by' is after the 'partition by' and the 'table options', so we need to add the 'cluster by' string after the 'partition by' and the 'table options'.
	*/
	if tableDef.ClusterBy != nil {
		clusterby := " CLUSTER BY ("
		if util.JudgeIsCompositeClusterByColumn(tableDef.ClusterBy.Name) {
			//multi column clusterby
			cbNames := util.SplitCompositeClusterByColumnName(tableDef.ClusterBy.Name)
			for i, cbName := range cbNames {
				if i != 0 {
					clusterby += fmt.Sprintf(", `%s`", formatStr(cbName))
				} else {
					clusterby += fmt.Sprintf("`%s`", formatStr(cbName))
				}
			}
		} else {
			//single column cluster by
			clusterby += fmt.Sprintf("`%s`", formatStr(tableDef.ClusterBy.Name))
		}
		clusterby += ")"
		createStr += clusterby
	}

	if tableDef.TableType == catalog.SystemExternalRel {
		param := tree.ExternParam{}
		err := json.Unmarshal([]byte(tableDef.Createsql), &param)
		if err != nil {
			return nil, err
		}
		createStr += fmt.Sprintf(" INFILE{'FILEPATH'='%s','COMPRESSION'='%s','FORMAT'='%s','JSONDATA'='%s'}", param.Filepath, param.CompressType, param.Format, param.JsonData)

		fields := ""
		if param.Tail.Fields.Terminated != nil {
			if param.Tail.Fields.Terminated.Value == "" {
				fields += " TERMINATED BY \"\""
			} else {
				fields += fmt.Sprintf(" TERMINATED BY '%s'", param.Tail.Fields.Terminated.Value)
			}
		}
		if param.Tail.Fields.EnclosedBy != nil {
			if param.Tail.Fields.EnclosedBy.Value == byte(0) {
				fields += " ENCLOSED BY ''"
			} else if param.Tail.Fields.EnclosedBy.Value == byte('\\') {
				fields += " ENCLOSED BY '\\\\'"
			} else {
				fields += fmt.Sprintf(" ENCLOSED BY '%c'", param.Tail.Fields.EnclosedBy.Value)
			}
		}
		if param.Tail.Fields.EscapedBy != nil {
			if param.Tail.Fields.EscapedBy.Value == byte(0) {
				fields += " ESCAPED BY ''"
			} else if param.Tail.Fields.EscapedBy.Value == byte('\\') {
				fields += " ESCAPED BY '\\\\'"
			} else {
				fields += fmt.Sprintf(" ESCAPED BY '%c'", param.Tail.Fields.EscapedBy.Value)
			}
		}

		line := ""
		if param.Tail.Lines.StartingBy != "" {
			line += fmt.Sprintf(" STARTING BY '%s'", param.Tail.Lines.StartingBy)
		}
		if param.Tail.Lines.TerminatedBy != nil {
			if param.Tail.Lines.TerminatedBy.Value == "\n" || param.Tail.Lines.TerminatedBy.Value == "\r\n" {
				line += " TERMINATED BY '\\\\n'"
			} else {
				line += fmt.Sprintf(" TERMINATED BY '%s'", param.Tail.Lines.TerminatedBy)
			}
		}

		if len(fields) > 0 {
			fields = " FIELDS" + fields
			createStr += fields
		}
		if len(line) > 0 {
			line = " LINES" + line
			createStr += line
		}

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
	var err error
	tblName := stmt.Name.GetTableName()
	dbName := stmt.Name.GetDBName()

	snapshot := &Snapshot{TS: &timestamp.Timestamp{}}
	if len(stmt.SnapshotName) > 0 {
		if snapshot, err = ctx.ResolveSnapshotWithSnapshotName(stmt.SnapshotName); err != nil {
			return nil, err
		}
	}

	dbName, err = databaseIsValid(getSuitableDBName(dbName, ""), ctx, *snapshot)
	if err != nil {
		return nil, err
	}

	_, tableDef := ctx.Resolve(dbName, tblName, *snapshot)
	if tableDef == nil || tableDef.TableType != catalog.SystemViewRel {
		return nil, moerr.NewInvalidInput(ctx.GetContext(), "show view '%s' is not a valid view", tblName)
	}
	sqlStr := "select \"%s\" as `View`, \"%s\" as `Create View`, 'utf8mb4' as `character_set_client`, 'utf8mb4_general_ci' as `collation_connection`"
	var viewStr string
	if tableDef.TableType == catalog.SystemViewRel {
		viewStr = tableDef.ViewSql.View
	}

	var viewData ViewData
	err = json.Unmarshal([]byte(viewStr), &viewData)
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

	accountId, err := ctx.GetAccountId()
	if err != nil {
		return nil, err
	}
	ddlType := plan.DataDefinition_SHOW_DATABASES

	var sql string
	snapshotSpec := ""
	if len(stmt.SnapshotName) > 0 {
		snapshot, err := ctx.ResolveSnapshotWithSnapshotName(stmt.SnapshotName)
		if err != nil {
			return nil, err
		}
		accountId = snapshot.Tenant.TenantID
		snapshotSpec = fmt.Sprintf("{snapshot = '%s'}", stmt.SnapshotName)
	}
	// Any account should show database MO_CATALOG_DB_NAME
	accountClause := fmt.Sprintf("account_id = %v or (account_id = 0 and datname = '%s')", accountId, MO_CATALOG_DB_NAME)
	sql = fmt.Sprintf("SELECT datname `Database` FROM %s.mo_database %s where (%s) ORDER BY %s", MO_CATALOG_DB_NAME, snapshotSpec, accountClause, catalog.SystemDBAttr_Name)

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
	// snapshot to fix
	dbName, err := databaseIsValid(stmt.DBName, ctx, Snapshot{TS: &timestamp.Timestamp{}})
	if err != nil {
		return nil, err
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

	accountId, err := ctx.GetAccountId()
	if err != nil {
		return nil, err
	}

	snapshot := &Snapshot{TS: &timestamp.Timestamp{}}
	snapshotSpec := ""
	if len(stmt.SnapshotName) > 0 {
		if snapshot, err = ctx.ResolveSnapshotWithSnapshotName(stmt.SnapshotName); err != nil {
			return nil, err
		}
		accountId = snapshot.Tenant.TenantID
		snapshotSpec = fmt.Sprintf("{snapshot = '%s'}", stmt.SnapshotName)
	}

	dbName, err := databaseIsValid(stmt.DBName, ctx, *snapshot)
	if err != nil {
		return nil, err
	}

	ddlType := plan.DataDefinition_SHOW_TABLES
	var tableType string
	if stmt.Full {
		tableType = fmt.Sprintf(", case relkind when 'v' then 'VIEW' when '%s' then 'CLUSTER TABLE' else 'BASE TABLE' end as Table_type", catalog.SystemClusterRel)
	}

	sub, err := ctx.GetSubscriptionMeta(dbName, *snapshot)
	if err != nil {
		return nil, err
	}
	subName := dbName
	if sub != nil {
		accountId = uint32(sub.AccountId)
		dbName = sub.DbName
		ctx.SetQueryingSubscription(sub)
		defer func() {
			ctx.SetQueryingSubscription(nil)
		}()
	}

	var sql string
	mustShowTable := "relname = 'mo_database' or relname = 'mo_tables' or relname = 'mo_columns'"
	clusterTable := fmt.Sprintf(" or relkind = '%s'", catalog.SystemClusterRel)
	accountClause := fmt.Sprintf("account_id = %v or (account_id = 0 and (%s))", accountId, mustShowTable+clusterTable)
	sql = fmt.Sprintf("SELECT relname as `Tables_in_%s` %s FROM %s.mo_tables %s WHERE reldatabase = '%s' and relname != '%s' and relname not like '%s' and relkind != '%s' and (%s)",
		subName, tableType, MO_CATALOG_DB_NAME, snapshotSpec, dbName, catalog.MOAutoIncrTable, catalog.IndexTableNamePrefix+"%", catalog.SystemPartitionRel, accountClause)

	// Do not show views in sub-db
	if sub != nil {
		sql += fmt.Sprintf(" and relkind != '%s'", catalog.SystemViewRel)
	}

	// Do not show sequences.
	sql += fmt.Sprintf(" and relkind != '%s'", catalog.SystemSequenceRel)

	// Order by relname
	sql += fmt.Sprintf(" ORDER BY %s", catalog.SystemRelAttr_Name)

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
	accountId, err := ctx.GetAccountId()
	if err != nil {
		return nil, err
	}
	// snapshot to fix
	dbName, err := databaseIsValid(stmt.DbName, ctx, Snapshot{TS: &timestamp.Timestamp{}})
	if err != nil {
		return nil, err
	}

	sub, err := ctx.GetSubscriptionMeta(dbName, Snapshot{TS: &timestamp.Timestamp{}})
	if err != nil {
		return nil, err
	}

	ddlType := plan.DataDefinition_SHOW_TABLES
	subName := dbName
	var sql string
	if sub != nil {
		accountId = uint32(sub.AccountId)
		dbName = sub.DbName
		ctx.SetQueryingSubscription(sub)
		defer func() {
			ctx.SetQueryingSubscription(nil)
		}()

		if accountId == catalog.System_Account {
			mustShowTable := "relname = 'mo_database' or relname = 'mo_tables' or relname = 'mo_columns'"
			clusterTable := fmt.Sprintf(" or relkind = '%s'", catalog.SystemClusterRel)
			accountClause := fmt.Sprintf("account_id = %v or (account_id = 0 and (%s))", accountId, mustShowTable+clusterTable)
			sql = fmt.Sprintf("SELECT count(relname) `Number of tables in %s`  FROM %s.mo_tables WHERE reldatabase = '%s' and relname != '%s' and relname not like '%s' and (%s) and relkind != '%s'",
				subName, MO_CATALOG_DB_NAME, dbName, catalog.MOAutoIncrTable, catalog.IndexTableNamePrefix+"%", accountClause, catalog.SystemViewRel)
		} else {
			sql = "SELECT count(relname) `Number of tables in %s` FROM %s.mo_tables WHERE reldatabase = '%s' and relname != '%s' and relname not like '%s'and relkind != '%s'"
			sql = fmt.Sprintf(sql, subName, MO_CATALOG_DB_NAME, dbName, catalog.MOAutoIncrTable, catalog.IndexTableNamePrefix+"%", catalog.SystemViewRel)
		}
	} else {
		if accountId == catalog.System_Account {
			mustShowTable := "relname = 'mo_database' or relname = 'mo_tables' or relname = 'mo_columns'"
			clusterTable := fmt.Sprintf(" or relkind = '%s'", catalog.SystemClusterRel)
			accountClause := fmt.Sprintf("account_id = %v or (account_id = 0 and (%s))", accountId, mustShowTable+clusterTable)
			sql = fmt.Sprintf("SELECT count(relname) `Number of tables in %s`  FROM %s.mo_tables WHERE reldatabase = '%s' and relname != '%s' and relname not like '%s' and (%s)",
				subName, MO_CATALOG_DB_NAME, dbName, catalog.MOAutoIncrTable, catalog.IndexTableNamePrefix+"%", accountClause)
		} else {
			sql = "SELECT count(relname) `Number of tables in %s` FROM %s.mo_tables WHERE reldatabase = '%s' and relname != '%s' and relname not like '%s'"
			sql = fmt.Sprintf(sql, subName, MO_CATALOG_DB_NAME, dbName, catalog.MOAutoIncrTable, catalog.IndexTableNamePrefix+"%")
		}

	}

	return returnByRewriteSQL(ctx, sql, ddlType)
}

func buildShowColumnNumber(stmt *tree.ShowColumnNumber, ctx CompilerContext) (*Plan, error) {
	accountId, err := ctx.GetAccountId()
	if err != nil {
		return nil, err
	}
	// snapshot to fix
	dbName, err := databaseIsValid(getSuitableDBName(stmt.Table.GetDBName(), stmt.DbName), ctx, Snapshot{TS: &timestamp.Timestamp{}})
	if err != nil {
		return nil, err
	}

	tblName := string(stmt.Table.ToTableName().ObjectName)
	obj, tableDef := ctx.Resolve(dbName, tblName, Snapshot{TS: &timestamp.Timestamp{}})
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), dbName, tblName)
	}

	ddlType := plan.DataDefinition_SHOW_COLUMNS
	var sql string

	var sub *SubscriptionMeta
	if obj.PubInfo != nil {
		accountId = uint32(obj.PubInfo.GetTenantId())
		dbName = obj.SchemaName
		sub = &SubscriptionMeta{
			AccountId: obj.PubInfo.GetTenantId(),
		}
		ctx.SetQueryingSubscription(sub)
		defer func() {
			ctx.SetQueryingSubscription(nil)
		}()
	}

	if accountId == catalog.System_Account {
		mustShowTable := "att_relname = 'mo_database' or att_relname = 'mo_tables' or att_relname = 'mo_columns'"
		clusterTable := ""
		if util.TableIsClusterTable(tableDef.GetTableType()) {
			clusterTable = fmt.Sprintf(" or att_relname = '%s'", tblName)
		}
		accountClause := fmt.Sprintf("account_id = %v or (account_id = 0 and (%s))", accountId, mustShowTable+clusterTable)
		sql = "SELECT count(attname) `Number of columns in %s` FROM %s.mo_columns WHERE att_database = '%s' AND att_relname = '%s' AND (%s) AND att_is_hidden = 0"
		sql = fmt.Sprintf(sql, tblName, MO_CATALOG_DB_NAME, dbName, tblName, accountClause)
	} else {
		sql = "SELECT count(attname) `Number of columns in %s` FROM %s.mo_columns WHERE att_database = '%s' AND att_relname = '%s'AND att_is_hidden = 0"
		sql = fmt.Sprintf(sql, tblName, MO_CATALOG_DB_NAME, dbName, tblName)
	}

	return returnByRewriteSQL(ctx, sql, ddlType)
}

func buildShowTableValues(stmt *tree.ShowTableValues, ctx CompilerContext) (*Plan, error) {
	dbName, err := databaseIsValid(getSuitableDBName(stmt.Table.GetDBName(), stmt.DbName), ctx, Snapshot{TS: &timestamp.Timestamp{}})
	if err != nil {
		return nil, err
	}

	tblName := string(stmt.Table.ToTableName().ObjectName)
	obj, tableDef := ctx.Resolve(dbName, tblName, Snapshot{TS: &timestamp.Timestamp{}})
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), dbName, tblName)
	}

	if obj.PubInfo != nil {
		sub := &SubscriptionMeta{
			AccountId: obj.PubInfo.GetTenantId(),
		}
		ctx.SetQueryingSubscription(sub)
		defer func() {
			ctx.SetQueryingSubscription(nil)
		}()
	}

	ddlType := plan.DataDefinition_SHOW_TARGET

	sql := "SELECT"
	isAllNull := true
	for _, col := range tableDef.Cols {
		if col.Hidden {
			continue
		}
		colName := col.Name
		if types.T(col.GetTyp().Id) == types.T_json {
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

	accountId, err := ctx.GetAccountId()
	if err != nil {
		return nil, err
	}
	dbName, err := databaseIsValid(getSuitableDBName(stmt.Table.GetDBName(), stmt.DBName), ctx, Snapshot{TS: &timestamp.Timestamp{}})
	if err != nil {
		return nil, err
	}

	tblName := string(stmt.Table.ToTableName().ObjectName)
	obj, tableDef := ctx.Resolve(dbName, tblName, Snapshot{TS: &timestamp.Timestamp{}})
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), dbName, tblName)
	}
	var sub *SubscriptionMeta
	if obj.PubInfo != nil {
		dbName = obj.SchemaName
		accountId = uint32(obj.PubInfo.GetTenantId())
		sub = &SubscriptionMeta{
			AccountId: obj.PubInfo.GetTenantId(),
		}
		ctx.SetQueryingSubscription(sub)
		defer func() {
			ctx.SetQueryingSubscription(nil)
		}()
	}
	var keyStr string
	if dbName == catalog.MO_CATALOG && tblName == catalog.MO_DATABASE {
		keyStr = "case when attname = '" + catalog.SystemDBAttr_ID + "' then 'PRI' else '' END as `Key`"
	} else if dbName == catalog.MO_CATALOG && tblName == catalog.MO_TABLES {
		keyStr = "case when attname = '" + catalog.SystemRelAttr_ID + "' then 'PRI' else '' END as `Key`"
	} else if dbName == catalog.MO_CATALOG && tblName == catalog.MO_COLUMNS {
		keyStr = "case when attname = '" + catalog.SystemColAttr_UniqName + "' then 'PRI' else '' END as `Key`"
	} else {
		if tableDef.Pkey != nil || len(tableDef.Fkeys) != 0 || len(tableDef.Indexes) != 0 {
			keyStr += "case"
			if tableDef.Pkey != nil {
				for _, name := range tableDef.Pkey.Names {
					keyStr += " when attname = "
					keyStr += "'" + name + "'"
					keyStr += " then 'PRI'"
				}
			}
			if len(tableDef.Fkeys) != 0 {
				colIdToName := make(map[uint64]string)
				for _, col := range tableDef.Cols {
					if col.Hidden {
						continue
					}
					colIdToName[col.ColId] = col.Name
				}
				for _, fk := range tableDef.Fkeys {
					for _, colId := range fk.Cols {
						keyStr += " when attname = "
						keyStr += "'" + colIdToName[colId] + "'"
						keyStr += " then 'MUL'"
					}
				}
			}
			if tableDef.Indexes != nil {
				for _, indexdef := range tableDef.Indexes {
					name := indexdef.Parts[0]
					if indexdef.Unique {
						if isPrimaryKey(tableDef, indexdef.Parts) {
							for _, name := range indexdef.Parts {
								keyStr += " when attname = "
								keyStr += "'" + name + "'"
								keyStr += " then 'PRI'"
							}
						} else if isMultiplePriKey(indexdef) {
							keyStr += " when attname = "
							keyStr += "'" + name + "'"
							keyStr += " then 'MUL'"
						} else {
							keyStr += " when attname = "
							keyStr += "'" + name + "'"
							keyStr += " then 'UNI'"
						}
					} else {
						keyStr += " when attname = "
						keyStr += "'" + name + "'"
						keyStr += " then 'MUL'"
					}
				}
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
		sql = "SELECT attname `Field`, CASE WHEN LENGTH(attr_enum) > 0 THEN mo_show_visible_bin_enum(atttyp, attr_enum) ELSE mo_show_visible_bin(atttyp, 3) END AS `Type`, iff(attnotnull = 0, 'YES', 'NO') `Null`, %s, mo_show_visible_bin(att_default, 1) `Default`, '' `Extra`,  att_comment `Comment` FROM %s.mo_columns WHERE att_database = '%s' AND att_relname = '%s' AND (%s) AND att_is_hidden = 0 ORDER BY attnum"
		if stmt.Full {
			sql = "SELECT attname `Field`, CASE WHEN LENGTH(attr_enum) > 0 THEN mo_show_visible_bin_enum(atttyp, attr_enum) ELSE mo_show_visible_bin(atttyp, 3) END AS `Type`, null `Collation`, iff(attnotnull = 0, 'YES', 'NO') `Null`, %s, mo_show_visible_bin(att_default, 1) `Default`,  '' `Extra`,'select,insert,update,references' `Privileges`, att_comment `Comment` FROM %s.mo_columns WHERE att_database = '%s' AND att_relname = '%s' AND (%s) AND att_is_hidden = 0 ORDER BY attnum"
		}
		sql = fmt.Sprintf(sql, keyStr, MO_CATALOG_DB_NAME, dbName, tblName, accountClause)
	} else {
		sql = "SELECT attname `Field`, CASE WHEN LENGTH(attr_enum) > 0 THEN mo_show_visible_bin_enum(atttyp, attr_enum) ELSE mo_show_visible_bin(atttyp, 3) END AS `Type`, iff(attnotnull = 0, 'YES', 'NO') `Null`, %s, mo_show_visible_bin(att_default, 1) `Default`, '' `Extra`,  att_comment `Comment` FROM %s.mo_columns WHERE att_database = '%s' AND att_relname = '%s' AND att_is_hidden = 0 ORDER BY attnum"
		if stmt.Full {
			sql = "SELECT attname `Field`, CASE WHEN LENGTH(attr_enum) > 0 THEN mo_show_visible_bin_enum(atttyp, attr_enum) ELSE mo_show_visible_bin(atttyp, 3) END AS `Type`, null `Collation`, iff(attnotnull = 0, 'YES', 'NO') `Null`, %s, mo_show_visible_bin(att_default, 1) `Default`,  '' `Extra`,'select,insert,update,references' `Privileges`, att_comment `Comment` FROM %s.mo_columns WHERE att_database = '%s' AND att_relname = '%s' AND att_is_hidden = 0 ORDER BY attnum"
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

	dbName, err := databaseIsValid(stmt.DbName, ctx, Snapshot{TS: &timestamp.Timestamp{}})
	if err != nil {
		return nil, err
	}

	stmt.DbName = dbName

	ddlType := plan.DataDefinition_SHOW_TABLE_STATUS
	accountId, err := ctx.GetAccountId()
	if err != nil {
		return nil, err
	}

	sub, err := ctx.GetSubscriptionMeta(dbName, Snapshot{TS: &timestamp.Timestamp{}})
	if err != nil {
		return nil, err
	}
	if sub != nil {
		accountId = uint32(sub.AccountId)
		dbName = sub.DbName
		ctx.SetQueryingSubscription(sub)
		defer func() {
			ctx.SetQueryingSubscription(nil)
		}()
	}

	mustShowTable := "relname = 'mo_database' or relname = 'mo_tables' or relname = 'mo_columns'"
	accountClause := fmt.Sprintf("account_id = %v or (account_id = 0 and (%s))", accountId, mustShowTable)
	sql := `select
				relname as 'Name',
				'Tae' as 'Engine',
				'Dynamic' as 'Row_format',
				0 as 'Rows',
				0 as 'Avg_row_length',
				0 as 'Data_length',
				0 as 'Max_data_length',
				0 as 'Index_length',
				'NULL' as 'Data_free',
				0 as 'Auto_increment',
				created_time as 'Create_time',
				'NULL' as 'Update_time',
				'NULL' as 'Check_time',
				'utf-8' as 'Collation',
				'NULL' as 'Checksum',
				'' as 'Create_options',
				rel_comment as 'Comment',
				owner as 'Role_id',
				'-' as 'Role_name'
			from
				%s.mo_tables
			where
				reldatabase = '%s'
				and relkind != '%s'
				and relname != '%s'
				and relname not like '%s'
				and (%s)`
	sql = fmt.Sprintf(sql, MO_CATALOG_DB_NAME, dbName, catalog.SystemPartitionRel, catalog.MOAutoIncrTable, catalog.IndexTableNamePrefix+"%", accountClause)

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

func buildShowFunctionOrProcedureStatus(stmt *tree.ShowFunctionOrProcedureStatus, ctx CompilerContext) (*Plan, error) {
	var sql string

	ddlType := plan.DataDefinition_SHOW_TARGET
	if stmt.Like != nil && stmt.Where != nil {
		return nil, moerr.NewSyntaxError(ctx.GetContext(), "like clause and where clause cannot exist at the same time")
	}

	if stmt.IsFunction {
		sql = fmt.Sprintf("SELECT db as `Db`, name as `Name`, type as `Type`, definer as `Definer`, modified_time as `Modified`, created_time as `Created`, security_type as `Security_type`, comment as `Comment`, character_set_client, collation_connection, database_collation as `Database Collation` FROM %s.mo_user_defined_function", MO_CATALOG_DB_NAME)
	} else {
		sql = fmt.Sprintf("SELECT db as `Db`, name as `Name`, type as `Type`, definer as `Definer`, modified_time as `Modified`, created_time as `Created`, security_type as `Security_type`, comment as `Comment`, character_set_client, collation_connection, database_collation as `Database Collation` FROM %s.mo_stored_procedure", MO_CATALOG_DB_NAME)
	}

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

	dbName, err := databaseIsValid(stmt.DbName, ctx, Snapshot{TS: &timestamp.Timestamp{}})
	if err != nil {
		return nil, err
	}
	stmt.DbName = dbName

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
	dbName, err := databaseIsValid(getSuitableDBName(stmt.TableName.GetDBName(), stmt.DbName), ctx, Snapshot{TS: &timestamp.Timestamp{}})
	if err != nil {
		return nil, err
	}
	tblName := stmt.TableName.GetTableName()
	obj, tableDef := ctx.Resolve(dbName, tblName, Snapshot{TS: &timestamp.Timestamp{}})
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), dbName, tblName)
	}

	ddlType := plan.DataDefinition_SHOW_INDEX

	if obj.PubInfo != nil {
		sub := &SubscriptionMeta{
			AccountId: obj.PubInfo.GetTenantId(),
		}
		dbName = obj.SchemaName
		ctx.SetQueryingSubscription(sub)
		defer func() {
			ctx.SetQueryingSubscription(nil)
		}()
	}

	sql := "select " +
		"`tcl`.`att_relname` as `Table`, " +
		"if(`idx`.`type` = 'MULTIPLE', 1, 0) as `Non_unique`, " +
		"`idx`.`name` as `Key_name`, " +
		"`idx`.`ordinal_position` as `Seq_in_index`, " +
		"`idx`.`column_name` as `Column_name`, " +
		"'A' as `Collation`, 0 as `Cardinality`, " +
		"'NULL' as `Sub_part`, " +
		"'NULL' as `Packed`, " +
		"if(`tcl`.`attnotnull` = 0, 'YES', '') as `Null`, " +
		"`idx`.`algo` as 'Index_type', " +
		"'' as `Comment`, " +
		"`idx`.`comment` as `Index_comment`, " +
		"`idx`.`algo_params` as `Index_params`, " +
		"if(`idx`.`is_visible` = 1, 'YES', 'NO') as `Visible`, " +
		"'NULL' as `Expression` " +
		"from `%s`.`mo_indexes` `idx` left join `%s`.`mo_columns` `tcl` " +
		"on (`idx`.`table_id` = `tcl`.`att_relname_id` and `idx`.`column_name` = `tcl`.`attname`) " +
		"where `tcl`.`att_database` = '%s' AND " +
		"`tcl`.`att_relname` = '%s' AND " +
		"`idx`.`column_name` NOT LIKE '%s' " +
		// Below `GROUP BY` is used instead of DISTINCT(`idx`.`name`) to handle IVF-FLAT or multi table indexes scenarios.
		// NOTE: We need to add all the table column names to the GROUP BY clause
		//
		// Without `GROUP BY`, we will printing the same index multiple times for IVFFLAT index.
		// (there are multiple entries in mo_indexes for the same index, with differing algo_table_type and index_table_name).
		// mysql> show index from tbl;
		//+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+-----------------------------------------+---------+------------+
		//| Table | Non_unique | Key_name | Seq_in_index | Column_name | Collation | Cardinality | Sub_part | Packed | Null | Index_type | Comment | Index_comment | Index_params                            | Visible | Expression |
		//+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+-----------------------------------------+---------+------------+
		//| tbl   |          1 | idx1     |            1 | embedding   | A         |           0 | NULL     | NULL   | YES  | ivfflat    |         |               | {"lists":"2","op_type":"vector_l2_ops"} | YES     | NULL       |
		//| tbl   |          1 | idx1     |            1 | embedding   | A         |           0 | NULL     | NULL   | YES  | ivfflat    |         |               | {"lists":"2","op_type":"vector_l2_ops"} | YES     | NULL       |
		//| tbl   |          1 | idx1     |            1 | embedding   | A         |           0 | NULL     | NULL   | YES  | ivfflat    |         |               | {"lists":"2","op_type":"vector_l2_ops"} | YES     | NULL       |
		//| tbl   |          0 | PRIMARY  |            1 | id          | A         |           0 | NULL     | NULL   |      |            |         |               |                                         | YES     | NULL       |
		//+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+-----------------------------------------+---------+------------+
		//
		// With `GROUP BY`, we print
		// mysql> show index from tbl;
		//+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+-----------------------------------------+---------+------------+
		//| Table | Non_unique | Key_name | Seq_in_index | Column_name | Collation | Cardinality | Sub_part | Packed | Null | Index_type | Comment | Index_comment | Index_params                            | Visible | Expression |
		//+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+-----------------------------------------+---------+------------+
		//| tbl   |          0 | PRIMARY  |            1 | id          | A         |           0 | NULL     | NULL   |      |            |         |               |                                         | YES     | NULL       |
		//| tbl   |          1 | idx1     |            1 | embedding   | A         |           0 | NULL     | NULL   | YES  | ivfflat    |         |               | {"lists":"2","op_type":"vector_l2_ops"} | YES     | NULL       |
		//+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+-----------------------------------------+---------+------------+
		"GROUP BY `tcl`.`att_relname`, `idx`.`type`, `idx`.`name`, `idx`.`ordinal_position`, " +
		"`idx`.`column_name`, `tcl`.`attnotnull`, `idx`.`algo`, `idx`.`comment`, " +
		"`idx`.`algo_params`, `idx`.`is_visible`" +
		";"
	showIndexSql := fmt.Sprintf(sql, MO_CATALOG_DB_NAME, MO_CATALOG_DB_NAME, dbName, tblName, catalog.AliasPrefix+"%")

	if stmt.Where != nil {
		return returnByWhereAndBaseSQL(ctx, showIndexSql, stmt.Where, ddlType)
	}
	return returnByRewriteSQL(ctx, showIndexSql, ddlType)
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

func buildShowRoles(stmt *tree.ShowRolesStmt, ctx CompilerContext) (*Plan, error) {
	ddlType := plan.DataDefinition_SHOW_TARGET
	sql := fmt.Sprintf("SELECT role_name as `ROLE_NAME`, creator as `CREATOR`, created_time as `CREATED_TIME`, comments as `COMMENTS` FROM %s.mo_role;", MO_CATALOG_DB_NAME)

	if stmt.Like != nil {
		// append filter [AND mo_role.role_name like stmt.Like] to WHERE clause
		likeExpr := stmt.Like
		likeExpr.Left = tree.SetUnresolvedName("role_name")
		return returnByLikeAndSQL(ctx, sql, likeExpr, ddlType)
	}

	return returnByRewriteSQL(ctx, sql, ddlType)
}

func buildShowStages(stmt *tree.ShowStages, ctx CompilerContext) (*Plan, error) {
	ddlType := plan.DataDefinition_SHOW_TARGET
	sql := fmt.Sprintf("SELECT stage_name as `STAGE_NAME`, url as `URL`, case stage_status when 'enabled' then 'ENABLED' else 'DISABLED' end as `STATUS`,  comment as `COMMENT` FROM %s.mo_stages;", MO_CATALOG_DB_NAME)

	if stmt.Like != nil {
		// append filter [AND mo_stages.stage_name like stmt.Like] to WHERE clause
		likeExpr := stmt.Like
		likeExpr.Left = tree.SetUnresolvedName("stage_name")
		return returnByLikeAndSQL(ctx, sql, likeExpr, ddlType)
	}

	return returnByRewriteSQL(ctx, sql, ddlType)
}

func buildShowSnapShots(stmt *tree.ShowSnapShots, ctx CompilerContext) (*Plan, error) {
	ddlType := plan.DataDefinition_SHOW_TARGET
	sql := fmt.Sprintf("SELECT sname as `SNAPSHOT_NAME`, CAST_NANO_TO_TIMESTAMP(ts) as `TIMESTAMP`,  level as `SNAPSHOT_LEVEL`, account_name as `ACCOUNT_NAME`, database_name as `DATABASE_NAME`, table_name as `TABLE_NAME` FROM %s.mo_snapshots ORDER BY ts DESC", MO_CATALOG_DB_NAME)

	if stmt.Where != nil {
		return returnByWhereAndBaseSQL(ctx, sql, stmt.Where, ddlType)
	}

	return returnByRewriteSQL(ctx, sql, ddlType)
}

func buildShowAccountUpgrade(stmt *tree.ShowAccountUpgrade, ctx CompilerContext) (*Plan, error) {
	ddlType := plan.DataDefinition_SHOW_UPGRADE
	sql := fmt.Sprintf("select account_name as `account_name`, create_version as `current_version` from %s.mo_account order by account_id;", MO_CATALOG_DB_NAME)
	return returnByRewriteSQL(ctx, sql, ddlType)
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

func buildShowProcessList(ctx CompilerContext) (*Plan, error) {
	ddlType := plan.DataDefinition_SHOW_PROCESSLIST
	// "show processlist" is implemented by table function processlist().
	sql := "select * from processlist() a"
	return returnByRewriteSQL(ctx, sql, ddlType)
}

func buildShowPublication(stmt *tree.ShowPublications, ctx CompilerContext) (*Plan, error) {
	ddlType := plan.DataDefinition_SHOW_TARGET
	sql := "select" +
		" pub_name as `publication`," +
		" database_name as `database`," +
		" created_time as `create_time`," +
		" update_time as `update_time`," +
		" case account_list " +
		" 	when 'all' then cast('*' as text)" +
		" 	else account_list" +
		" end as `sub_account`," +
		" comment as `comments`" +
		" from mo_catalog.mo_pubs"
	like := stmt.Like
	if like != nil {
		right, ok := like.Right.(*tree.NumVal)
		if !ok || right.Value.Kind() != constant.String {
			return nil, moerr.NewInternalError(ctx.GetContext(), "like clause must be a string")
		}
		sql += fmt.Sprintf(" where pub_name like '%s' order by pub_name;", constant.StringVal(right.Value))
	} else {
		sql += " order by update_time desc, created_time desc;"
	}
	return returnByRewriteSQL(ctx, sql, ddlType)
}

func buildShowCreatePublications(stmt *tree.ShowCreatePublications, ctx CompilerContext) (*Plan, error) {
	ddlType := plan.DataDefinition_SHOW_TARGET
	sql := fmt.Sprintf("select pub_name as Publication, 'CREATE PUBLICATION ' || pub_name || ' DATABASE ' || database_name || ' ACCOUNT ' || account_list as 'Create Publication' from mo_catalog.mo_pubs where pub_name='%s';", stmt.Name)
	return returnByRewriteSQL(ctx, sql, ddlType)
}

func returnByRewriteSQL(ctx CompilerContext, sql string,
	ddlType plan.DataDefinition_DdlType) (*Plan, error) {
	newStmt, err := getRewriteSQLStmt(ctx, sql)
	defer newStmt.Free()
	if err != nil {
		return nil, err
	}
	return getReturnDdlBySelectStmt(ctx, newStmt, ddlType)
}

func returnByWhereAndBaseSQL(ctx CompilerContext, baseSQL string,
	where *tree.Where, ddlType plan.DataDefinition_DdlType) (*Plan, error) {
	sql := fmt.Sprintf("SELECT * FROM (%s) tbl", baseSQL)
	// logutil.Info(sql)
	newStmt, err := getRewriteSQLStmt(ctx, sql)
	defer newStmt.Free()
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
	defer newStmt.Free()
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
	newStmts, err := parsers.Parse(ctx.GetContext(), dialect.MYSQL, sql, 1, 0)
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
	queryPlan, err := BuildPlan(ctx, stmt, false)
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
