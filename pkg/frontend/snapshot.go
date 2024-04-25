// Copyright 2021 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/defines"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

var (
	insertIntoMoSnapshots = `insert into mo_catalog.mo_snapshots(
		snapshot_id,
		sname,
		ts,
		level,
		account_name,
		database_name,
		table_name,
		obj_id ) values ('%s', '%s', %d, '%s', '%s', '%s', '%s', %d);`

	dropSnapshotFormat = `delete from mo_catalog.mo_snapshots where sname = '%s' order by snapshot_id;`

	checkSnapshotFormat = `select snapshot_id from mo_catalog.mo_snapshots where sname = "%s" order by snapshot_id;`

	getSnapshotTsWithSnapshotNameFormat = `select ts from mo_catalog.mo_snapshots where sname = "%s" order by snapshot_id;`

	checkSnapshotTsFormat = `select snapshot_id from mo_catalog.mo_snapshots where ts = %d order by snapshot_id;`

	restoreTableDataFmt = "insert into `%s`.`%s` SELECT * FROM `%s`.`%s` {snapshot = '%s'}"
)

func doCreateSnapshot(ctx context.Context, ses *Session, stmt *tree.CreateSnapShot) error {
	var err error
	var snapshotLevel tree.SnapshotLevel
	var snapshotForAccount string
	var snapshotName string
	var snapshotExist bool
	var snapshotId string
	var databaseName string
	var tableName string
	var sql string
	var objId uint64

	// check create stage priv
	err = doCheckRole(ctx, ses)
	if err != nil {
		return err
	}

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()
	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}

	// check create snapshot priv

	// 1.only admin can create tenant level snapshot
	err = doCheckRole(ctx, ses)
	if err != nil {
		return err
	}
	// 2.only sys can create cluster level snapshot
	tenantInfo := ses.GetTenantInfo()
	currentAccount := tenantInfo.GetTenant()
	snapshotLevel = stmt.Object.SLevel.Level
	if snapshotLevel == tree.SNAPSHOTLEVELCLUSTER && currentAccount != sysAccountName {
		return moerr.NewInternalError(ctx, "only sys tenant can create cluster level snapshot")
	}

	// 3.only sys can create tenant level snapshot for other tenant
	if snapshotLevel == tree.SNAPSHOTLEVELACCOUNT {
		snapshotForAccount = string(stmt.Object.ObjName)
		if currentAccount != sysAccountName && currentAccount != snapshotForAccount {
			return moerr.NewInternalError(ctx, "only sys tenant can create tenant level snapshot for other tenant")
		}

		// check account exists or not and get accountId
		getAccountIdFunc := func(accountName string) (accountId uint64, rtnErr error) {
			var erArray []ExecResult
			sql, rtnErr = getSqlForCheckTenant(ctx, accountName)
			if rtnErr != nil {
				return 0, rtnErr
			}
			bh.ClearExecResultSet()
			rtnErr = bh.Exec(ctx, sql)
			if rtnErr != nil {
				return 0, rtnErr
			}

			erArray, rtnErr = getResultSet(ctx, bh)
			if rtnErr != nil {
				return 0, rtnErr
			}

			if execResultArrayHasData(erArray) {
				for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
					accountId, rtnErr = erArray[0].GetUint64(ctx, i, 0)
					if rtnErr != nil {
						return 0, rtnErr
					}
				}
			} else {
				return 0, moerr.NewInternalError(ctx, "account %s does not exist", accountName)
			}
			return accountId, rtnErr
		}

		// if sys tenant create snapshots for other tenant, get the account id
		// otherwise, get the account id from tenantInfo
		if currentAccount == sysAccountName && currentAccount != snapshotForAccount {
			objId, err = getAccountIdFunc(snapshotForAccount)
			if err != nil {
				return err
			}
		} else {
			objId = uint64(tenantInfo.GetTenantID())
		}
	}

	// check snapshot exists or not
	snapshotName = string(stmt.Name)
	snapshotExist, err = checkSnapShotExistOrNot(ctx, bh, snapshotName)
	if err != nil {
		return err
	}
	if snapshotExist {
		if !stmt.IfNotExists {
			return moerr.NewInternalError(ctx, "snapshot %s already exists", snapshotName)
		} else {
			return nil
		}
	} else {
		// insert record to the system table

		// 1. get snapshot id
		newUUid, err := uuid.NewV7()
		if err != nil {
			return err
		}
		snapshotId = newUUid.String()

		// 2. get snapshot ts
		// ts := ses.proc.TxnOperator.SnapshotTS()
		// snapshotTs = ts.String()

		sql, err = getSqlForCreateSnapshot(ctx, snapshotId, snapshotName, time.Now().UTC().UnixNano(), snapshotLevel.String(), string(stmt.Object.ObjName), databaseName, tableName, objId)
		if err != nil {
			return err
		}

		err = bh.Exec(ctx, sql)
		if err != nil {
			return err
		}
	}

	// insert record to the system table

	return err
}

func doDropSnapshot(ctx context.Context, ses *Session, stmt *tree.DropSnapShot) (err error) {
	var sql string
	var stageExist bool
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	// check create stage priv
	// only admin can drop snapshot for himself
	err = doCheckRole(ctx, ses)
	if err != nil {
		return err
	}

	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}

	// check stage
	stageExist, err = checkSnapShotExistOrNot(ctx, bh, string(stmt.Name))
	if err != nil {
		return err
	}

	if !stageExist {
		if !stmt.IfExists {
			return moerr.NewInternalError(ctx, "snapshot %s does not exist", string(stmt.Name))
		} else {
			// do nothing
			return err
		}
	} else {
		sql = getSqlForDropSnapshot(string(stmt.Name))
		err = bh.Exec(ctx, sql)
		if err != nil {
			return err
		}
	}
	return err
}

func doRestoreSnapshot(ctx context.Context, ses *Session, stmt *tree.RestoreSnapShot) (err error) {
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	srcAccountName := string(stmt.AccountName)
	dbName := string(stmt.DatabaseName)
	tblName := string(stmt.TableName)
	snapshotName := string(stmt.SnapShotName)
	toAccountName := string(stmt.ToAccountName)

	accountId := getAccountId(ctx, bh, srcAccountName)
	if accountId == -1 {
		return moerr.NewInternalError(ctx, fmt.Sprintf("account does not exist, account name: %s", srcAccountName))
	}

	// default restore to self
	toAccountId := ses.GetAccountId()

	// can't restore to another account if cur account is not sys
	if len(toAccountName) > 0 {
		// get current tenant
		curAccount := ses.GetTenantInfo()
		if !curAccount.IsSysTenant() {
			err = moerr.NewInternalError(ctx, "non-sys tenant can't restore to another account")
			return
		}

		newAccountName := string(stmt.ToAccountName)
		tenantId := getAccountId(ctx, bh, newAccountName)
		if tenantId == -1 {
			return moerr.NewInternalError(ctx, fmt.Sprintf("account does not exist, account name: %s", newAccountName))
		}
		toAccountId = uint32(tenantId)
	}

	// check if snapShot exists
	if snapShotExist, err := checkSnapShotExistOrNot(ctx, bh, snapshotName); err != nil {
		return err
	} else if !snapShotExist {
		return moerr.NewInternalError(ctx, "snapshot %s does not exist", snapshotName)
	}

	// TODO stop toAccount
	// TODO defer open toAccount

	switch stmt.Level {
	case tree.RESTORELEVELCLUSTER:
		// TODO
	case tree.RESTORELEVELACCOUNT:
		return restoreToAccount(ctx, ses, nil, snapshotName, uint32(accountId), uint32(toAccountId))
	case tree.RESTORELEVELDATABASE:
		return restoreToDatabase(ctx, ses, nil, snapshotName, uint32(accountId), dbName, uint32(toAccountId))
	case tree.RESTORELEVELTABLE:
		return restoreToTable(ctx, ses, nil, snapshotName, uint32(accountId), dbName, tblName, uint32(toAccountId))
	}
	return
}

func restoreToAccount(ctx context.Context, ses *Session, bh BackgroundExec, snapshotName string, accountId uint32, toAccountId uint32) (err error) {
	if bh == nil {
		bh = ses.GetBackgroundExec(ctx)
		defer bh.Close()

		if err = bh.Exec(ctx, "begin;"); err != nil {
			return err
		}
		defer func() {
			err = finishTxn(ctx, bh, err)
		}()
	}

	dbNames, err := showDatabases(ctx, bh, snapshotName)
	if err != nil {
		return
	}

	for _, dbName := range dbNames {
		if err = restoreToDatabase(ctx, ses, bh, snapshotName, accountId, dbName, toAccountId); err != nil {
			return
		}
	}
	return
}

func restoreToDatabase(ctx context.Context, ses *Session, bh BackgroundExec, snapshotName string, accountId uint32, dbName string, toAccountId uint32) (err error) {
	if bh == nil {
		bh = ses.GetBackgroundExec(ctx)
		defer bh.Close()

		if err = bh.Exec(ctx, "begin;"); err != nil {
			return err
		}
		defer func() {
			err = finishTxn(ctx, bh, err)
		}()
	}

	if err = bh.Exec(ctx, "drop database if exists "+dbName); err != nil {
		return
	}

	if err = bh.Exec(ctx, "create database if not exists "+dbName); err != nil {
		return
	}

	createTableSqls, err := getCreateTableSqls(ctx, bh, snapshotName, dbName)
	if err != nil {
		return err
	}

	for tblName, createTableSql := range createTableSqls {
		// create table
		if err = bh.Exec(ctx, createTableSql); err != nil {
			return
		}

		// insert data
		insertIntoSql := fmt.Sprintf(restoreTableDataFmt, dbName, tblName, dbName, tblName, snapshotName)
		if err = bh.Exec(ctx, insertIntoSql); err != nil {
			return
		}
	}

	return
}

func restoreToTable(ctx context.Context, ses *Session, bh BackgroundExec, snapShot string, accountId uint32, schemaName string, tableName string, toAccountId uint32) (err error) {
	if bh == nil {
		bh = ses.GetBackgroundExec(ctx)
		defer bh.Close()

		if err = bh.Exec(ctx, "begin;"); err != nil {
			return err
		}
		defer func() {
			err = finishTxn(ctx, bh, err)
		}()
	}

	currentAccountId := ses.GetAccountId()

	// create table
	createTableSql, rtnErr := getTableDefFromSnapshot(ctx, bh, snapShot, schemaName, tableName)
	if rtnErr != nil {
		return rtnErr
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

		rtnErr = bh.Exec(execCxt, "use "+schemaName)
		if rtnErr != nil {
			return rtnErr
		}

		rtnErr = bh.Exec(execCxt, createTableSql)
		if rtnErr != nil {
			return rtnErr
		}

		restoreTableSql := fmt.Sprintf(restoreTableDataFmt, schemaName, tableName, schemaName, tableName, snapShot)
		if currentAccountId == toAccountId {
			return bh.Exec(execCxt, restoreTableSql)
		} else {
			return bh.ExecRestore(execCxt, restoreTableSql, accountId, toAccountId)
		}
	}

	if currentAccountId == toAccountId {
		return restoreTable(ctx)
	} else {
		//with new tenant
		toTenantCtx := defines.AttachAccount(ctx, uint32(toAccountId), uint32(GetAdminUserId()), uint32(accountAdminRoleID))
		return restoreTable(toTenantCtx)
	}
}

func checkSnapShotExistOrNot(ctx context.Context, bh BackgroundExec, snapshotName string) (bool, error) {
	var sql string
	var erArray []ExecResult
	var err error
	sql, err = getSqlForCheckSnapshot(ctx, snapshotName)
	if err != nil {
		return false, err
	}
	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return false, err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return false, err
	}

	if execResultArrayHasData(erArray) {
		return true, nil
	}
	return false, nil
}

func doResolveSnapshotTsWithSnapShotName(ctx context.Context, ses FeSession, spName string) (snapshotTs int64, err error) {
	var sql string
	var erArray []ExecResult
	err = inputNameIsInvalid(ctx, spName)
	if err != nil {
		return 0, err
	}

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	sql, err = getSqlForGetSnapshotTsWithSnapshotName(ctx, spName)
	if err != nil {
		return 0, err
	}
	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return 0, err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return 0, err
	}

	if execResultArrayHasData(erArray) {
		snapshotTs, err := erArray[0].GetInt64(ctx, 0, 0)
		if err != nil {
			return 0, err
		}

		return snapshotTs, nil
	} else {
		return 0, moerr.NewInternalError(ctx, "snapshot %s does not exist", spName)
	}
}

func getSqlForCheckSnapshot(ctx context.Context, snapshot string) (string, error) {
	err := inputNameIsInvalid(ctx, snapshot)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(checkSnapshotFormat, snapshot), nil
}

func getSqlForGetSnapshotTsWithSnapshotName(ctx context.Context, snapshot string) (string, error) {
	err := inputNameIsInvalid(ctx, snapshot)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(getSnapshotTsWithSnapshotNameFormat, snapshot), nil
}

func getSqlForCheckSnapshotTs(snapshotTs int64) string {
	return fmt.Sprintf(checkSnapshotTsFormat, snapshotTs)
}

func getSqlForCreateSnapshot(ctx context.Context, snapshotId, snapshotName string, ts int64, level, accountName, databaseName, tableName string, objectId uint64) (string, error) {
	err := inputNameIsInvalid(ctx, snapshotName)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(insertIntoMoSnapshots, snapshotId, snapshotName, ts, level, accountName, databaseName, tableName, objectId), nil
}

func getSqlForDropSnapshot(snapshotName string) string {
	return fmt.Sprintf(dropSnapshotFormat, snapshotName)
}

func getStringList(ctx context.Context, bh BackgroundExec, sql string) (ans []string, err error) {
	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	resultSet, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	for _, rs := range resultSet {
		for row := uint64(0); row < rs.GetRowCount(); row++ {
			val, err := rs.GetString(ctx, row, 1)
			if err != nil {
				return nil, err
			}

			ans = append(ans, val)
		}
	}
	return
}

func showDatabases(ctx context.Context, bh BackgroundExec, snapshotName string) ([]string, error) {
	sql := fmt.Sprintf("show databases {snapshot = '%s'}", snapshotName)
	return getStringList(ctx, bh, sql)
}

func showTables(ctx context.Context, bh BackgroundExec, snapshotName string, dbName string) ([]string, error) {
	sql := fmt.Sprintf("show tables from `%s` {snapshot = '%s'}", dbName, snapshotName)
	return getStringList(ctx, bh, sql)
}

func getCreateTableSqls(ctx context.Context, bh BackgroundExec, snapshotName string, dbName string) (map[string]string, error) {
	tables, err := showTables(ctx, bh, snapshotName, dbName)
	if err != nil {
		return nil, err
	}

	createSqlMap := make(map[string]string)
	for _, tblName := range tables {
		tableDef, err := getCreateTableSql(ctx, bh, snapshotName, dbName, tblName)
		if err != nil {
			return nil, err
		}
		createSqlMap[tblName] = tableDef
	}
	return createSqlMap, nil
}

func getCreateTableSql(ctx context.Context, bh BackgroundExec, snapshotName string, dbName string, tblName string) (string, error) {
	sql := fmt.Sprintf("show create table `%s`.`%s` {snapshot = '%s'}", dbName, tblName, snapshotName)

	bh.ClearExecResultSet()
	if err := bh.Exec(ctx, sql); err != nil {
		return "", err
	}

	resultSet, err := getResultSet(ctx, bh)
	if err != nil {
		return "", err
	}

	if len(resultSet) == 0 || resultSet[0].GetRowCount() == 0 {
		return "", moerr.NewNoSuchTable(ctx, dbName, tblName)
	}

	return resultSet[0].GetString(ctx, 0, 1)
}

func getAccountId(ctx context.Context, bh BackgroundExec, accountName string) int32 {
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))
	sql := getAccountIdNamesSql
	if len(accountName) > 0 {
		sql += fmt.Sprintf(" and account_name = '%s'", accountName)
	}

	bh.ClearExecResultSet()
	if err := bh.Exec(ctx, sql); err != nil {
		return -1
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return -1
	}

	if execResultArrayHasData(erArray) {
		if accountId, err := erArray[0].GetInt64(ctx, 0, 0); err != nil {
			return -1
		} else {
			return int32(accountId)
		}
	}

	return -1
}
