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
	"context"
	"encoding/json"
	"fmt"
	"go/constant"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
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
	var snapshot *Snapshot
	if stmt.AtTsExpr != nil {
		if snapshot, err = getTimeStampByTsHint(ctx, stmt.AtTsExpr); err != nil {
			return nil, err
		}
	}

	name, err := databaseIsValid(getSuitableDBName("", stmt.Name), ctx, snapshot)
	if err != nil {
		return nil, err
	}

	if sub, err := ctx.GetSubscriptionMeta(name, snapshot); err != nil {
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

func buildShowCreateTable(stmt *tree.ShowCreateTable, ctx CompilerContext) (*Plan, error) {
	var err error
	tblName := stmt.Name.GetTableName()
	dbName := stmt.Name.GetDBName()

	var snapshot *Snapshot
	if stmt.AtTsExpr != nil {
		if snapshot, err = getTimeStampByTsHint(ctx, stmt.AtTsExpr); err != nil {
			return nil, err
		}
	}

	dbName, err = databaseIsValid(getSuitableDBName(dbName, ""), ctx, snapshot)
	if err != nil {
		return nil, err
	}

	// check if the database is a subscription
	if sub, err := ctx.GetSubscriptionMeta(dbName, snapshot); err != nil {
		return nil, err
	} else if sub != nil {
		if !pubsub.InSubMetaTables(sub, tblName) {
			return nil, moerr.NewInternalErrorNoCtx("table %s not found in publication %s", tblName, sub.Name)
		}

		ctx.SetQueryingSubscription(sub)
		defer func() {
			ctx.SetQueryingSubscription(nil)
		}()
	}

	_, tableDef := ctx.Resolve(dbName, tblName, snapshot)
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), dbName, tblName)
	}
	if tableDef.TableType == catalog.SystemViewRel {
		var newStmt *tree.ShowCreateView
		if stmt.Name.NumParts == 1 {
			newStmt = tree.NewShowCreateView(tree.NewUnresolvedObjectName(tblName))
		} else if stmt.Name.NumParts == 2 {
			newStmt = tree.NewShowCreateView(tree.NewUnresolvedObjectName(dbName, tblName))
		}
		if stmt.AtTsExpr != nil {
			newStmt.AtTsExpr = stmt.AtTsExpr
		}

		return buildShowCreateView(newStmt, ctx)
	}

	ddlStr, _, err := ConstructCreateTableSQL(ctx, tableDef, snapshot, false)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	for i, ch := range ddlStr {
		// escape double quote, for the sql pattern below
		if ch == '"' {
			if i == 0 || ddlStr[i-1] != '\\' {
				buf.WriteRune('"')
			}
		}
		buf.WriteRune(ch)
	}
	sql := "select \"%s\" as `Table`, \"%s\" as `Create Table`"
	sql = fmt.Sprintf(sql, tblName, buf.String())

	return returnByRewriteSQL(ctx, sql, plan.DataDefinition_SHOW_CREATETABLE)
}

// buildShowCreateView
func buildShowCreateView(stmt *tree.ShowCreateView, ctx CompilerContext) (*Plan, error) {
	var err error
	tblName := stmt.Name.GetTableName()
	dbName := stmt.Name.GetDBName()

	snapshot := &Snapshot{TS: &timestamp.Timestamp{}}
	if stmt.AtTsExpr != nil {
		if snapshot, err = getTimeStampByTsHint(ctx, stmt.AtTsExpr); err != nil {
			return nil, err
		}
	}

	dbName, err = databaseIsValid(getSuitableDBName(dbName, ""), ctx, snapshot)
	if err != nil {
		return nil, err
	}

	_, tableDef := ctx.Resolve(dbName, tblName, snapshot)
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

	snapshot := &Snapshot{TS: &timestamp.Timestamp{}}
	if stmt.AtTsExpr != nil {
		if snapshot, err = getTimeStampByTsHint(ctx, stmt.AtTsExpr); err != nil {
			return nil, err
		}

		if stmt.AtTsExpr.Type == tree.ATTIMESTAMPSNAPSHOT {
			accountId = snapshot.Tenant.TenantID
			snapshotSpec = fmt.Sprintf("{snapshot = '%s'}", stmt.AtTsExpr.SnapshotName)
		} else {
			snapshotSpec = fmt.Sprintf("{MO_TS = %d}", snapshot.TS.PhysicalTime)
		}

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
		likeExpr.Left = tree.NewUnresolvedColName("datname")
		return returnByLikeAndSQL(ctx, sql, likeExpr, ddlType)
	}

	return returnByRewriteSQL(ctx, sql, ddlType)
}

func buildShowSequences(stmt *tree.ShowSequences, ctx CompilerContext) (*Plan, error) {
	// snapshot to fix
	dbName, err := databaseIsValid(stmt.DBName, ctx, nil)
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
	if stmt.AtTsExpr != nil {
		if snapshot, err = getTimeStampByTsHint(ctx, stmt.AtTsExpr); err != nil {
			return nil, err
		}

		if stmt.AtTsExpr.Type == tree.ATTIMESTAMPSNAPSHOT {
			accountId = snapshot.Tenant.TenantID
			snapshotSpec = fmt.Sprintf("{snapshot = '%s'}", stmt.AtTsExpr.SnapshotName)
		} else {
			snapshotSpec = fmt.Sprintf("{MO_TS = %d}", snapshot.TS.PhysicalTime)
		}

	}

	dbName, err := databaseIsValid(stmt.DBName, ctx, snapshot)
	if err != nil {
		return nil, err
	}

	var tableType string
	if stmt.Full {
		tableType = fmt.Sprintf(", case relkind when 'v' then 'VIEW' when '%s' then 'CLUSTER TABLE' else 'BASE TABLE' end as Table_type", catalog.SystemClusterRel)
	}

	subName := dbName
	sub, err := ctx.GetSubscriptionMeta(dbName, snapshot)
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

	var sql string
	mustShowTable := "relname = 'mo_database' or relname = 'mo_tables' or relname = 'mo_columns'"
	clusterTable := fmt.Sprintf(" or relkind = '%s'", catalog.SystemClusterRel)
	accountClause := fmt.Sprintf("account_id = %v or (account_id = 0 and (%s))", accountId, mustShowTable+clusterTable)
	sql = fmt.Sprintf("SELECT relname as `Tables_in_%s` %s FROM %s.mo_tables %s WHERE reldatabase = '%s' and relname != '%s' and relname not like '%s' and relkind != '%s' and (%s)",
		subName, tableType, MO_CATALOG_DB_NAME, snapshotSpec, dbName, catalog.MOAutoIncrTable, catalog.IndexTableNamePrefix+"%", catalog.SystemPartitionRel, accountClause)

	// Do not show views in sub-db
	if sub != nil {
		sql += fmt.Sprintf(" and relkind != '%s'", catalog.SystemViewRel)
		if sub.Tables != pubsub.TableAll {
			sql += fmt.Sprintf(" and relname in (%s)", pubsub.AddSingleQuotesJoin(strings.Split(sub.Tables, pubsub.Sep)))
		}
	}

	// Do not show sequences.
	sql += fmt.Sprintf(" and relkind != '%s'", catalog.SystemSequenceRel)

	// Order by relname
	sql += fmt.Sprintf(" ORDER BY %s", catalog.SystemRelAttr_Name)

	ddlType := plan.DataDefinition_SHOW_TABLES

	if stmt.Where != nil {
		return returnByWhereAndBaseSQL(ctx, sql, stmt.Where, ddlType)
	}

	if stmt.Like != nil {
		// append filter [AND relname like stmt.Like] to WHERE clause
		likeExpr := stmt.Like
		likeExpr.Left = tree.NewUnresolvedColName("relname")
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
	snapshot := &Snapshot{TS: &timestamp.Timestamp{}}
	dbName, err := databaseIsValid(stmt.DbName, ctx, snapshot)
	if err != nil {
		return nil, err
	}

	subName := dbName
	sub, err := ctx.GetSubscriptionMeta(dbName, snapshot)
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
	clusterTable := fmt.Sprintf(" or relkind = '%s'", catalog.SystemClusterRel)
	accountClause := fmt.Sprintf("account_id = %v or (account_id = 0 and (%s))", accountId, mustShowTable+clusterTable)
	sql := fmt.Sprintf("SELECT count(relname) `Number of tables in %s` FROM %s.mo_tables WHERE reldatabase = '%s' and relname != '%s' and relname not like '%s' and (%s)",
		subName, MO_CATALOG_DB_NAME, dbName, catalog.MOAutoIncrTable, catalog.IndexTableNamePrefix+"%", accountClause)

	// Do not show views in sub-db
	if sub != nil {
		sql += fmt.Sprintf(" and relkind != '%s'", catalog.SystemViewRel)
		if sub.Tables != pubsub.TableAll {
			sql += fmt.Sprintf(" and relname in (%s)", pubsub.AddSingleQuotesJoin(strings.Split(sub.Tables, pubsub.Sep)))
		}
	}

	ddlType := plan.DataDefinition_SHOW_TABLES
	return returnByRewriteSQL(ctx, sql, ddlType)
}

func buildShowColumnNumber(stmt *tree.ShowColumnNumber, ctx CompilerContext) (*Plan, error) {
	accountId, err := ctx.GetAccountId()
	if err != nil {
		return nil, err
	}
	// snapshot to fix
	dbName, err := databaseIsValid(getSuitableDBName(stmt.Table.GetDBName(), stmt.DbName), ctx, nil)
	if err != nil {
		return nil, err
	}

	tblName := string(stmt.Table.ToTableName().ObjectName)
	obj, tableDef := ctx.Resolve(dbName, tblName, nil)
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
	dbName, err := databaseIsValid(getSuitableDBName(stmt.Table.GetDBName(), stmt.DbName), ctx, nil)
	if err != nil {
		return nil, err
	}

	tblName := string(stmt.Table.ToTableName().ObjectName)
	obj, tableDef := ctx.Resolve(dbName, tblName, nil)
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

	dbName, err := databaseIsValid(getSuitableDBName(stmt.Table.GetDBName(), stmt.DBName), ctx, nil)
	if err != nil {
		return nil, err
	}

	tblName := string(stmt.Table.ToTableName().ObjectName)
	obj, tableDef := ctx.Resolve(dbName, tblName, nil)
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), dbName, tblName)
	}

	colIdToOriginName := make(map[uint64]string)
	colNameToOriginName := make(map[string]string)
	for _, col := range tableDef.Cols {
		if col.Hidden {
			continue
		}
		colNameOrigin := col.GetOriginCaseName()
		colIdToOriginName[col.ColId] = colNameOrigin
		colNameToOriginName[col.Name] = colNameOrigin
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
	if tableDef.Pkey != nil || len(tableDef.Fkeys) != 0 || len(tableDef.Indexes) != 0 {
		keyStr += "case"
		if tableDef.Pkey != nil {
			for _, name := range tableDef.Pkey.Names {
				name = colNameToOriginName[name]
				keyStr += " when col.attname = "
				keyStr += "'" + name + "'"
				keyStr += " then 'PRI'"
			}
		}
		if len(tableDef.Fkeys) != 0 {

			for _, fk := range tableDef.Fkeys {
				for _, colId := range fk.Cols {
					keyStr += " when col.attname = "
					keyStr += "'" + colIdToOriginName[colId] + "'"
					keyStr += " then 'MUL'"
				}
			}
		}
		if tableDef.Indexes != nil {
			for _, indexDef := range tableDef.Indexes {
				name := colNameToOriginName[indexDef.Parts[0]]
				if indexDef.Unique {
					if isPrimaryKey(tableDef, indexDef.Parts) {
						for _, name = range indexDef.Parts {
							name = colNameToOriginName[name]
							keyStr += " when col.attname = "
							keyStr += "'" + name + "'"
							keyStr += " then 'PRI'"
						}
					} else if isMultiplePriKey(indexDef) {
						keyStr += " when col.attname = "
						keyStr += "'" + name + "'"
						keyStr += " then 'MUL'"
					} else {
						keyStr += " when col.attname = "
						keyStr += "'" + name + "'"
						keyStr += " then 'UNI'"
					}
				} else {
					keyStr += " when col.attname = "
					keyStr += "'" + name + "'"
					keyStr += " then 'MUL'"
				}
			}
		}
		keyStr += " else '' END as `Key`"
	} else {
		keyStr = "'' as `Key`"
	}

	ddlType := plan.DataDefinition_SHOW_COLUMNS

	var sql string
	if accountId == catalog.System_Account {
		mustShowTable := "col.att_relname = 'mo_database' or col.att_relname = 'mo_tables' or col.att_relname = 'mo_columns'"
		clusterTable := ""
		if util.TableIsClusterTable(tableDef.GetTableType()) {
			clusterTable = fmt.Sprintf(" or col.att_relname = '%s'", tblName)
		}
		accountClause := fmt.Sprintf("col.account_id = %v or (col.account_id = 0 and (%s))", accountId, mustShowTable+clusterTable)
		sql = "SELECT col.attname `Field`, CASE WHEN LENGTH(col.attr_enum) > 0 THEN mo_show_visible_bin_enum(col.atttyp, col.attr_enum) ELSE mo_show_visible_bin(col.atttyp, 3) END AS `Type`, iff(col.attnotnull = 0, 'YES', 'NO') `Null`, %s, mo_show_visible_bin(col.att_default, 1) `Default`, CASE WHEN tbl.relkind = 'r' and col.att_is_auto_increment = 1 THEN 'auto_increment' ELSE '' END AS `Extra`,  col.att_comment `Comment` FROM %s.mo_columns col left join %s.mo_tables tbl ON col.att_relname_id = tbl.rel_id WHERE col.att_database = '%s' AND col.att_relname = '%s' AND (%s) AND col.att_is_hidden = 0 ORDER BY col.attnum"
		if stmt.Full {
			sql = "SELECT col.attname `Field`, CASE WHEN LENGTH(col.attr_enum) > 0 THEN mo_show_visible_bin_enum(col.atttyp, col.attr_enum) ELSE mo_show_visible_bin(col.atttyp, 3) END AS `Type`, null `Collation`, iff(col.attnotnull = 0, 'YES', 'NO') `Null`, %s, mo_show_visible_bin(col.att_default, 1) `Default`, CASE WHEN tbl.relkind = 'r' and col.att_is_auto_increment = 1 THEN 'auto_increment' ELSE '' END AS `Extra`,'select,insert,update,references' `Privileges`, col.att_comment `Comment` FROM %s.mo_columns col left join %s.mo_tables tbl ON col.att_relname_id = tbl.rel_id WHERE col.att_database = '%s' AND col.att_relname = '%s' AND (%s) AND col.att_is_hidden = 0 ORDER BY col.attnum"
		}
		sql = fmt.Sprintf(sql, keyStr, MO_CATALOG_DB_NAME, MO_CATALOG_DB_NAME, dbName, tblName, accountClause)
	} else {
		sql = "SELECT col.attname `Field`, CASE WHEN LENGTH(col.attr_enum) > 0 THEN mo_show_visible_bin_enum(col.atttyp, col.attr_enum) ELSE mo_show_visible_bin(col.atttyp, 3) END AS `Type`, iff(col.attnotnull = 0, 'YES', 'NO') `Null`, %s, mo_show_visible_bin(col.att_default, 1) `Default`, CASE WHEN col.att_is_auto_increment = 1 THEN 'auto_increment' ELSE '' END AS `Extra`,  col.att_comment `Comment` FROM %s.mo_columns col left join %s.mo_tables tbl ON col.att_relname_id = tbl.rel_id WHERE col.att_database = '%s' AND col.att_relname = '%s' AND col.att_is_hidden = 0 ORDER BY col.attnum"
		if stmt.Full {
			sql = "SELECT col.attname `Field`, CASE WHEN LENGTH(col.attr_enum) > 0 THEN mo_show_visible_bin_enum(col.atttyp, col.attr_enum) ELSE mo_show_visible_bin(col.atttyp, 3) END AS `Type`, null `Collation`, iff(col.attnotnull = 0, 'YES', 'NO') `Null`, %s, mo_show_visible_bin(col.att_default, 1) `Default`, CASE WHEN col.att_is_auto_increment = 1 THEN 'auto_increment' ELSE '' END AS `Extra`,'select,insert,update,references' `Privileges`, col.att_comment `Comment` FROM %s.mo_columns col left join %s.mo_tables tbl ON col.att_relname_id = tbl.rel_id WHERE col.att_database = '%s' AND col.att_relname = '%s' AND col.att_is_hidden = 0 ORDER BY col.attnum"
		}
		sql = fmt.Sprintf(sql, keyStr, MO_CATALOG_DB_NAME, MO_CATALOG_DB_NAME, dbName, tblName)
	}

	if stmt.Where != nil {
		return returnByWhereAndBaseSQL(ctx, sql, stmt.Where, ddlType)
	}

	if stmt.Like != nil {
		// append filter [AND ma.attname like stmt.Like] to WHERE clause
		likeExpr := stmt.Like
		likeExpr.Left = tree.NewUnresolvedColName("attname")
		return returnByLikeAndSQL(ctx, sql, likeExpr, ddlType)
	}

	return returnByRewriteSQL(ctx, sql, ddlType)
}

func buildShowTableStatus(stmt *tree.ShowTableStatus, ctx CompilerContext) (*Plan, error) {
	if stmt.Like != nil && stmt.Where != nil {
		return nil, moerr.NewSyntaxError(ctx.GetContext(), "like clause and where clause cannot exist at the same time")
	}

	dbName, err := databaseIsValid(stmt.DbName, ctx, nil)
	if err != nil {
		return nil, err
	}

	stmt.DbName = dbName

	ddlType := plan.DataDefinition_SHOW_TABLE_STATUS
	accountId, err := ctx.GetAccountId()
	if err != nil {
		return nil, err
	}

	sub, err := ctx.GetSubscriptionMeta(dbName, nil)
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

	// Do not show views in sub-db
	if sub != nil {
		sql += fmt.Sprintf(" and relkind != '%s'", catalog.SystemViewRel)
		if sub.Tables != pubsub.TableAll {
			sql += fmt.Sprintf(" and relname in (%s)", pubsub.AddSingleQuotesJoin(strings.Split(sub.Tables, pubsub.Sep)))
		}
	}

	if stmt.Where != nil {
		return returnByWhereAndBaseSQL(ctx, sql, stmt.Where, ddlType)
	}

	if stmt.Like != nil {
		// append filter [AND ma.relname like stmt.Like] to WHERE clause
		likeExpr := stmt.Like
		likeExpr.Left = tree.NewUnresolvedColName("relname")
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
		likeExpr.Left = tree.NewUnresolvedColName("name")
		return returnByLikeAndSQL(ctx, sql, likeExpr, ddlType)
	}

	return returnByRewriteSQL(ctx, sql, ddlType)
}

func buildShowTriggers(stmt *tree.ShowTarget, ctx CompilerContext) (*Plan, error) {
	if stmt.Like != nil && stmt.Where != nil {
		return nil, moerr.NewSyntaxError(ctx.GetContext(), "like clause and where clause cannot exist at the same time")
	}

	dbName, err := databaseIsValid(stmt.DbName, ctx, nil)
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
		likeExpr.Left = tree.NewUnresolvedColName("event_object_table")
		return returnByLikeAndSQL(ctx, sql, likeExpr, ddlType)
	}

	return returnByRewriteSQL(ctx, sql, ddlType)
}

func buildShowIndex(stmt *tree.ShowIndex, ctx CompilerContext) (*Plan, error) {
	var snapshot *Snapshot
	dbName, err := databaseIsValid(getSuitableDBName(stmt.TableName.GetDBName(), stmt.DbName), ctx, snapshot)
	if err != nil {
		return nil, err
	}

	tblName := stmt.TableName.GetTableName()
	obj, tableDef := ctx.Resolve(dbName, tblName, snapshot)
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
		likeExpr.Left = tree.NewUnresolvedColName("role_name")
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
		likeExpr.Left = tree.NewUnresolvedColName("stage_name")
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

func buildShowPitr(stmt *tree.ShowPitr, ctx CompilerContext) (*Plan, error) {
	ddlType := plan.DataDefinition_SHOW_TARGET
	curAccountId, err := ctx.GetAccountId()
	if err != nil {
		return nil, err
	}

	sql := fmt.Sprintf("SELECT pitr_name as `PITR_NAME`, create_time  as `CREATED_TIME`, modified_time as MODIFIED_TIME, level as `PITR_LEVEL`, IF(account_name = '', '*', account_name)  as `ACCOUNT_NAME`, IF(database_name = '', '*', database_name) as `DATABASE_NAME`, IF(table_name = '', '*', table_name) as `TABLE_NAME`, pitr_length as `PITR_LENGTH`, pitr_unit  as `PITR_UNIT` FROM %s.mo_pitr where create_account = %d ORDER BY create_time DESC", MO_CATALOG_DB_NAME, curAccountId)

	newCtx := ctx.GetContext()
	if curAccountId != catalog.System_Account {
		newCtx = context.WithValue(newCtx, defines.TenantIDKey{}, catalog.System_Account)
	}
	ctx.SetContext(newCtx)

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
	sql := fmt.Sprintf("select pub_name as Publication, 'CREATE PUBLICATION ' || pub_name || ' DATABASE ' || database_name || case table_list when '*' then '' else ' TABLE ' || table_list end || ' ACCOUNT ' || account_list as 'Create Publication' from mo_catalog.mo_pubs where pub_name='%s';", stmt.Name)
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
