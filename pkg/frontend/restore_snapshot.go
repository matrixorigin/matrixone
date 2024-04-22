// Copyright 2024 Matrix Origin
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

package frontend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

func handleRestoreCluster(ctx context.Context, ses *Session, st *tree.RestoreSnapShot) error {
	return nil
}

func handleRestoreAccount(ctx context.Context, ses *Session, st *tree.RestoreSnapShot) error {
	return nil
}

func handleRestoreDatabase(ctx context.Context, ses *Session, stmt *tree.RestoreSnapShot) error {
	snapShot := string(stmt.SnapShotName)

	srcAccountName := string(stmt.AccountName)
	descAccountName := string(stmt.ToAccountName)

	schemaName := string(stmt.DatabaseName)

	crossTenant := false
	if descAccountName != "" {
		crossTenant = srcAccountName != descAccountName
	}

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	err := bh.Exec(ctx, "begin;")
	if err != nil {
		return err
	}

	defer func() {
		err = finishTxn(ctx, bh, err)
	}()

	// 1. check if snapShot exists
	snapShotExist, err := checkSnapShotExistOrNot(ctx, bh, snapShot)
	if err != nil {
		return err
	}

	if !snapShotExist {
		return moerr.NewInternalError(ctx, "snapshot %s does not exist", string(snapShot))
	}

	restoreDatabase := func(execCxt context.Context) (rtnErr error) {
		rtnErr = bh.Exec(execCxt, "drop database if exists "+schemaName)
		if rtnErr != nil {
			return rtnErr
		}

		rtnErr = bh.Exec(execCxt, "create database if not exists "+schemaName)
		if rtnErr != nil {
			return rtnErr
		}
		return nil
	}

	restoreTable := func(execCxt context.Context, tableName string, createTableSql string) (rtnErr error) {
		rtnErr = bh.Exec(execCxt, createTableSql)
		if rtnErr != nil {
			return rtnErr
		}

		restoreTableSql := fmt.Sprintf(restoreTableDataFmt, schemaName, tableName, schemaName, tableName, snapShot)
		rtnErr = bh.Exec(execCxt, restoreTableSql)
		if rtnErr != nil {
			return rtnErr
		}
		return rtnErr
	}

	if crossTenant {
		exists, err := checkTenantExistsOrNot(ctx, bh, descAccountName)
		if err != nil {
			return err
		}

		if !exists {
			return moerr.NewInternalError(ctx, "the tenant %s exists", descAccountName)
		}

		tenantInfo, err := getTenantInfoByName(ctx, bh, descAccountName)
		if err != nil {
			return err
		}

		//with new tenant
		destTenantCtx := defines.AttachAccount(ctx, uint32(tenantInfo.TenantID), uint32(tenantInfo.UserID), uint32(accountAdminRoleID))

		tabledefInDbMap, err := getAllTableDefInDBFromSnapshot(ctx, bh, snapShot, schemaName)
		if err != nil {
			return err
		}

		err = restoreDatabase(destTenantCtx)
		if err != nil {
			return err
		}

		err = bh.Exec(destTenantCtx, "use "+schemaName)
		if err != nil {
			return err
		}

		for tableName, tableDef := range tabledefInDbMap {
			err = restoreTable(destTenantCtx, tableName, tableDef)
			if err != nil {
				return err
			}
		}
	} else {
		tabledefInDbMap, err := getAllTableDefInDBFromSnapshot(ctx, bh, snapShot, schemaName)
		if err != nil {
			return err
		}

		err = restoreDatabase(ctx)
		if err != nil {
			return err
		}

		err = bh.Exec(ctx, "use "+schemaName)
		if err != nil {
			return err
		}

		for tableName, tableDef := range tabledefInDbMap {
			err = restoreTable(ctx, tableName, tableDef)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func handleRestoreTable(ctx context.Context, ses *Session, stmt *tree.RestoreSnapShot) error {
	snapShot := string(stmt.SnapShotName)

	srcAccountName := string(stmt.AccountName)
	descAccountName := string(stmt.ToAccountName)

	schemaName := string(stmt.DatabaseName)
	tableName := string(stmt.TableName)

	crossTenant := false
	if descAccountName != "" {
		crossTenant = srcAccountName != descAccountName
	}

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	err := bh.Exec(ctx, "begin;")
	if err != nil {
		return err
	}

	defer func() {
		err = finishTxn(ctx, bh, err)
	}()

	// 1. check if snapShot exists
	snapShotExist, err := checkSnapShotExistOrNot(ctx, bh, snapShot)
	if err != nil {
		return err
	}

	if !snapShotExist {
		return moerr.NewInternalError(ctx, "snapshot %s does not exist", string(snapShot))
	}

	restoreTable := func(execCxt context.Context) (rtnErr error) {
		rtnErr = bh.Exec(execCxt, "create database if not exists "+schemaName)
		if rtnErr != nil {
			return rtnErr
		}

		rtnErr = bh.Exec(execCxt, "drop table if exists "+schemaName+"."+tableName)
		if rtnErr != nil {
			return rtnErr
		}

		createTableSql, rtnErr := getTableDefFromSnapshot(ctx, bh, snapShot, schemaName, tableName)
		if rtnErr != nil {
			return rtnErr
		}

		rtnErr = bh.Exec(execCxt, "use "+schemaName)
		if rtnErr != nil {
			return rtnErr
		}

		rtnErr = bh.Exec(execCxt, createTableSql)
		if rtnErr != nil {
			return rtnErr
		}

		restoreTableSql := fmt.Sprintf(restoreTableDataFmt, schemaName, tableName, schemaName, tableName, snapShot)
		rtnErr = bh.Exec(execCxt, restoreTableSql)
		if rtnErr != nil {
			return rtnErr
		}
		return rtnErr
	}

	if crossTenant {
		exists, err := checkTenantExistsOrNot(ctx, bh, descAccountName)
		if err != nil {
			return err
		}

		if !exists {
			return moerr.NewInternalError(ctx, "the tenant %s exists", descAccountName)
		}

		tenantInfo, err := getTenantInfoByName(ctx, bh, descAccountName)
		if err != nil {
			return err
		}

		//with new tenant
		destTenantCtx := defines.AttachAccount(ctx, uint32(tenantInfo.TenantID), uint32(tenantInfo.UserID), uint32(accountAdminRoleID))

		err = restoreTable(destTenantCtx)
		if err != nil {
			return err
		}
	} else {
		err = restoreTable(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func getTableDefFromSnapshot(ctx context.Context, bh BackgroundExec, snapshotName string, schemaName string, tableName string) (string, error) {
	sql := fmt.Sprintf("show create table `%s`.`%s` {snapshot = '%s'}", schemaName, tableName, snapshotName)

	bh.ClearExecResultSet()
	err := bh.Exec(ctx, sql)
	if err != nil {
		return "", err
	}

	resultSet, err := getResultSet(ctx, bh)
	if err != nil {
		return "", err
	}

	if len(resultSet) == 0 || resultSet[0].GetRowCount() == 0 {
		return "", moerr.NewNoSuchTable(ctx, schemaName, tableName)
	}

	createTableSQL, err := resultSet[0].GetString(ctx, 0, 1)
	if err != nil {
		return "", err
	}
	return createTableSQL, nil
}

func getTenantInfoByName(ctx context.Context, bh BackgroundExec, accountName string) (TenantInfo, error) {
	tenant := TenantInfo{
		Tenant:        accountName,
		User:          "internal",
		UserID:        GetAdminUserId(),
		DefaultRoleID: GetAccountAdminRoleId(),
		DefaultRole:   GetAccountAdminRole(),
	}

	// Query information_schema.columns to get column info
	query := fmt.Sprintf("SELECT ACCOUNT_ID FROM `mo_catalog`.`mo_account` where ACCOUNT_NAME = '%s'", accountName)
	// Execute the query
	bh.ClearExecResultSet()
	err := bh.Exec(ctx, query)
	if err != nil {
		return tenant, err
	}

	resultSet, err := getResultSet(ctx, bh)
	if err != nil {
		return tenant, err
	}

	if len(resultSet) == 0 || resultSet[0].GetRowCount() == 0 {
		return tenant, moerr.NewInternalError(ctx, "the tenant %s does not exist", accountName)
	}

	accountId, err := resultSet[0].GetUint64(ctx, 0, 0)
	if err != nil {
		return tenant, err
	}
	tenant.TenantID = uint32(accountId)

	return tenant, nil
}

func getAllTableDefInDBFromSnapshot(ctx context.Context, bh BackgroundExec, snapshotName string, schemaName string) (map[string]string, error) {
	sql := fmt.Sprintf("show tables from %s {snapshot = '%s'}", schemaName, snapshotName)

	bh.ClearExecResultSet()
	err := bh.Exec(ctx, sql)
	if err != nil {
		return nil, err
	}

	resultSet, err := getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}

	if len(resultSet) == 0 || resultSet[0].GetRowCount() == 0 {
		return nil, moerr.NewBadDB(ctx, schemaName)
	}

	tablesInDB := make([]string, 0)
	for i := uint64(0); i < resultSet[0].GetRowCount(); i++ {
		tblName, err := resultSet[0].GetString(ctx, i, 0)
		if err != nil {
			return nil, err
		}
		tablesInDB = append(tablesInDB, tblName)
	}

	tabledefMap := make(map[string]string)
	for _, tableName := range tablesInDB {
		tableDef, err := getTableDefFromSnapshot(ctx, bh, snapshotName, schemaName, tableName)
		if err != nil {
			return nil, err
		}
		tabledefMap[tableName] = tableDef
	}
	return tabledefMap, nil
}

const (
	restoreTableDataFmt = "insert into `%s`.`%s` SELECT * FROM `%s`.`%s`{snapshot = '%s'}"
)

func handShowCreateTables(ses *Session, schemaName string, tableNames []string, ts timestamp.Timestamp) (map[string]string, error) {
	showCreateTableMap := make(map[string]string)
	for _, tableName := range tableNames {
		createTable, err := handShowCreateTable(ses, schemaName, tableName, ts)
		if err != nil {
			return nil, err
		}

		showCreateTableMap[tableName] = createTable
	}
	return showCreateTableMap, nil
}

func handShowCreateTable(ses *Session, schemaName string, tblName string, ts timestamp.Timestamp) (string, error) {
	var err error
	ctx := ses.GetTxnCompileCtx()
	_, tableDef := ctx.Resolve(tblName, schemaName, ts)
	if tableDef == nil {
		return "", moerr.NewNoSuchTable(ctx.GetContext(), schemaName, tblName)
	}

	if tableDef.TableType == catalog.SystemViewRel {
		return "", moerr.NewInternalError(ctx.GetContext(), "cannot show create view")
	}

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
			return "", err
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
					return "", err
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

		var fkTableDef *plan.TableDef

		//fk self reference
		if fk.ForeignTbl == 0 {
			fkTableDef = tableDef
		} else {
			_, fkTableDef = ctx.ResolveById(fk.ForeignTbl, timestamp.Timestamp{})
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
			return "", err
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

	var buf bytes.Buffer
	for _, ch := range createStr {
		if ch == '"' {
			buf.WriteRune('"')
		}
		buf.WriteRune(ch)
	}

	return buf.String(), nil
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
