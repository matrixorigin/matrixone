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
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
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

	getSnapshotFormat = `select * from mo_catalog.mo_snapshots`

	checkSnapshotTsFormat = `select snapshot_id from mo_catalog.mo_snapshots where ts = %d order by snapshot_id;`

	restoreTableDataFmt = "insert into `%s`.`%s` SELECT * FROM `%s`.`%s` {snapshot = '%s'}"
)

type snapshotRecord struct {
	snapshotId   string
	snapshotName string
	ts           int64
	level        string
	accountName  string
	databaseName string
	tableName    string
	objId        uint64
}

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

	// check snapshot
	snapshot, err := getSnapshotByName(ctx, bh, snapshotName)
	if err != nil {
		return err
	}
	if snapshot == nil {
		return moerr.NewInternalError(ctx, "snapshot %s does not exist", snapshotName)
	}
	if snapshot.accountName != srcAccountName {
		return moerr.NewInternalError(ctx, "accountName(%v) does not match snapshot.accountName(%v)", srcAccountName, snapshot.accountName)
	}

	curAccount := ses.GetTenantInfo()
	srcAccountId, err := getAccountId(ctx, bh, srcAccountName)
	if err != nil {
		return err
	}

	// default restore to self
	toAccountId := curAccount.TenantID

	if len(toAccountName) > 0 {
		// can't restore to another account if cur account is not sys
		if !curAccount.IsSysTenant() {
			err = moerr.NewInternalError(ctx, "non-sys tenant can't restore snapshot to another account")
			return
		}

		newAccountName := string(stmt.ToAccountName)
		if toAccountId, err = getAccountId(ctx, bh, newAccountName); err != nil {
			return err
		}
	}

	// TODO stop toAccount
	// TODO defer open toAccount

	// restore as a txn
	if err = bh.Exec(ctx, "begin;"); err != nil {
		return err
	}
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()

	switch stmt.Level {
	case tree.RESTORELEVELCLUSTER:
		// TODO
	case tree.RESTORELEVELACCOUNT:
		return restoreToAccount(ctx, bh, snapshotName, srcAccountId, toAccountId)
	case tree.RESTORELEVELDATABASE:
		return restoreToDatabase(ctx, bh, snapshotName, srcAccountId, dbName, toAccountId)
	case tree.RESTORELEVELTABLE:
		return restoreToTable(ctx, bh, snapshotName, srcAccountId, dbName, tblName, toAccountId)
	}
	return
}

func restoreToAccount(ctx context.Context, bh BackgroundExec, snapshotName string, accountId uint32, toAccountId uint32) (err error) {
	dbNames, err := showDatabases(ctx, bh, snapshotName)
	if err != nil {
		return
	}

	for _, dbName := range dbNames {
		if err = restoreToDatabase(ctx, bh, snapshotName, accountId, dbName, toAccountId); err != nil {
			return
		}
	}
	return
}

func restoreToDatabase(ctx context.Context, bh BackgroundExec, snapshotName string, srcAccountId uint32, dbName string, toAccountId uint32) (err error) {
	if err = bh.Exec(ctx, "drop database if exists "+dbName); err != nil {
		return
	}

	if err = bh.Exec(ctx, "create database if not exists "+dbName); err != nil {
		return
	}

	if err = bh.Exec(ctx, "use "+dbName); err != nil {
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

func restoreToTable(ctx context.Context, bh BackgroundExec, snapshotName string, srcAccountId uint32, dbName string, tblName string, toAccountId uint32) (err error) {
	toCtx := defines.AttachAccountId(ctx, toAccountId)

	if err = bh.Exec(toCtx, "create database if not exists "+dbName); err != nil {
		return
	}

	if err = bh.Exec(toCtx, "use "+dbName); err != nil {
		return
	}

	if err = bh.Exec(toCtx, "drop table if exists "+dbName+"."+tblName); err != nil {
		return
	}

	createTableSql, err := getCreateTableSql(ctx, bh, snapshotName, dbName, tblName)
	if err != nil {
		return err
	}

	// create table
	if err = bh.Exec(toCtx, createTableSql); err != nil {
		return
	}

	// insert data
	insertIntoSql := fmt.Sprintf(restoreTableDataFmt, dbName, tblName, dbName, tblName, snapshotName)

	curAccountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	if curAccountId == toAccountId {
		if err = bh.Exec(ctx, insertIntoSql); err != nil {
			return
		}
	} else {
		if err = bh.ExecRestore(toCtx, insertIntoSql, curAccountId, toAccountId); err != nil {
			return
		}
	}
	return
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

func getSnapshotRecords(ctx context.Context, bh BackgroundExec, sql string) ([]*snapshotRecord, error) {
	var erArray []ExecResult
	var err error

	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return nil, err
	}

	if erArray, err = getResultSet(ctx, bh); err != nil {
		return nil, err
	}

	var records []*snapshotRecord
	if execResultArrayHasData(erArray) {
		for _, er := range erArray {
			var record snapshotRecord
			for row := uint64(0); row < er.GetRowCount(); row++ {
				if record.snapshotId, err = er.GetString(ctx, row, 0); err != nil {
					return nil, err
				}
				if record.snapshotName, err = er.GetString(ctx, row, 1); err != nil {
					return nil, err
				}
				if record.ts, err = er.GetInt64(ctx, row, 2); err != nil {
					return nil, err
				}
				if record.level, err = er.GetString(ctx, row, 3); err != nil {
					return nil, err
				}
				if record.accountName, err = er.GetString(ctx, row, 4); err != nil {
					return nil, err
				}
				if record.databaseName, err = er.GetString(ctx, row, 5); err != nil {
					return nil, err
				}
				if record.tableName, err = er.GetString(ctx, row, 6); err != nil {
					return nil, err
				}
				if record.objId, err = er.GetUint64(ctx, row, 7); err != nil {
					return nil, err
				}
			}
			records = append(records, &record)
		}
		return records, nil
	}
	return nil, err
}

func getSnapshotByName(ctx context.Context, bh BackgroundExec, snapshotName string) (*snapshotRecord, error) {
	if err := inputNameIsInvalid(ctx, snapshotName); err != nil {
		return nil, err
	}

	sql := fmt.Sprintf("%s where sname = '%s'", getSnapshotFormat, snapshotName)
	if records, err := getSnapshotRecords(ctx, bh, sql); err != nil {
		return nil, err
	} else if len(records) != 1 {
		return nil, moerr.NewInternalError(ctx, "find %v snapshotRecords by name, expect only 1", len(records))
	} else {
		return records[0], nil
	}
}

func doResolveSnapshotTsWithSnapShotName(ctx context.Context, ses FeSession, snapshotName string) (snapshot plan.Snapshot, err error) {
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	var record *snapshotRecord
	if record, err = getSnapshotByName(ctx, bh, snapshotName); err != nil {
		return plan.Snapshot{TS: &timestamp.Timestamp{}}, err
	}

	if record == nil {
		return plan.Snapshot{TS: &timestamp.Timestamp{}}, moerr.NewInternalError(ctx, "snapshot %s does not exist", snapshotName)
	}

	var accountId uint32
	if accountId, err = getAccountId(ctx, bh, record.accountName); err != nil {
		return plan.Snapshot{TS: &timestamp.Timestamp{}}, err
	}

	return plan.Snapshot{
		TS: &timestamp.Timestamp{PhysicalTime: record.ts},
		CreatedByTenant: &plan.SnapshotTenant{
			TenantName: record.accountName,
			TenantID:   accountId,
		},
	}, nil
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
			val, err := rs.GetString(ctx, row, 0)
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

func getAccountId(ctx context.Context, bh BackgroundExec, accountName string) (uint32, error) {
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))
	sql := getAccountIdNamesSql
	if len(accountName) > 0 {
		sql += fmt.Sprintf(" and account_name = '%s'", accountName)
	}

	bh.ClearExecResultSet()
	if err := bh.Exec(ctx, sql); err != nil {
		return 0, err
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return 0, err
	}

	if execResultArrayHasData(erArray) {
		var accountId int64
		if accountId, err = erArray[0].GetInt64(ctx, 0, 0); err != nil {
			return 0, err
		}
		return uint32(accountId), nil
	}

	return 0, moerr.NewInternalError(ctx, "new such account, account name: %v", accountName)
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
