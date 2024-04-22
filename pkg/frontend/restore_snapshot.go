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
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
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
