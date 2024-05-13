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
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type tableType string

const view tableType = "VIEW"

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

	restoreSkipDbs = []string{"mysql", "system", "system_metrics", "mo_task", "mo_debug"}
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

type tableInfo struct {
	name      string
	typ       tableType
	createSql string
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

	if toAccountId == sysAccountID && snapshot.accountName != sysAccountName {
		err = moerr.NewInternalError(ctx, "non-sys account's snapshot can't restore to sys account")
		return
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
		return restoreToAccount(ctx, bh, snapshotName, toAccountId)
	case tree.RESTORELEVELDATABASE:
		return restoreToDatabase(ctx, bh, snapshotName, dbName, toAccountId)
	case tree.RESTORELEVELTABLE:
		return restoreToTable(ctx, bh, snapshotName, dbName, tblName, toAccountId)
	}
	return
}

func restoreToAccount(ctx context.Context, bh BackgroundExec, snapshotName string, toAccountId uint32) (err error) {
	dbNames, err := showDatabases(ctx, bh, snapshotName)
	if err != nil {
		return
	}

	for _, dbName := range dbNames {
		if err = restoreToDatabase(ctx, bh, snapshotName, dbName, toAccountId); err != nil {
			return
		}
	}
	return
}

func restoreToDatabase(ctx context.Context, bh BackgroundExec, snapshotName string, dbName string, toAccountId uint32) (err error) {
	return restoreToDatabaseOrTable(ctx, bh, snapshotName, dbName, "", toAccountId)
}

func restoreToTable(ctx context.Context, bh BackgroundExec, snapshotName string, dbName string, tblName string, toAccountId uint32) (err error) {
	return restoreToDatabaseOrTable(ctx, bh, snapshotName, dbName, tblName, toAccountId)
}

func needSkip(dbName string, tblName string) bool {
	if slices.Contains(restoreSkipDbs, dbName) {
		return true
	}

	if dbName == "information_schema" {
		return true
	}

	if dbName == "mo_catalog" {
		return true
	}

	return false
}

func restoreToDatabaseOrTable(ctx context.Context, bh BackgroundExec, snapshotName string, dbName string, tblName string, toAccountId uint32) (err error) {
	if needSkip(dbName, tblName) {
		return
	}

	toCtx := defines.AttachAccountId(ctx, toAccountId)

	// if restore to db, delete the same name db first
	if tblName == "" {
		if err = bh.Exec(toCtx, "drop database if exists "+dbName); err != nil {
			return
		}
	}

	if err = bh.Exec(toCtx, "create database if not exists "+dbName); err != nil {
		return
	}

	if err = bh.Exec(toCtx, "use "+dbName); err != nil {
		return
	}

	// if restore to table, delete the same name table first
	if tblName != "" {
		if err = bh.Exec(toCtx, "drop table if exists "+dbName+"."+tblName); err != nil {
			return
		}
	}

	var tableInfos []*tableInfo
	if tblName == "" {
		tableInfos, err = getTableInfos(ctx, bh, snapshotName, dbName)
		if err != nil {
			return
		}
	} else {
		tableInfos, err = getTableInfo(ctx, bh, snapshotName, dbName, tblName)
		if err != nil {
			return
		}
	}

	// if restore to table, expect only one table here
	if tblName != "" && len(tableInfos) != 1 {
		return moerr.NewInternalError(ctx, "find %v tableInfos by name, expect only 1", len(tableInfos))
	}

	curAccountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	for _, tblInfo := range tableInfos {
		// TODO handle view restoration
		if tblInfo.typ == view {
			continue
		}

		// create table
		if err = bh.Exec(toCtx, tblInfo.createSql); err != nil {
			return
		}

		// insert data
		insertIntoSql := fmt.Sprintf(restoreTableDataFmt, dbName, tblInfo.name, dbName, tblInfo.name, snapshotName)

		if curAccountId == toAccountId {
			if err = bh.Exec(ctx, insertIntoSql); err != nil {
				return
			}
		} else {
			if err = bh.ExecRestore(toCtx, insertIntoSql, curAccountId, toAccountId); err != nil {
				return
			}
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

func getStringColsList(ctx context.Context, bh BackgroundExec, sql string, colIndices ...uint64) (ans [][]string, err error) {
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
			ansRow := make([]string, len(colIndices))
			for i := 0; i < len(colIndices); i++ {
				if ansRow[i], err = rs.GetString(ctx, row, colIndices[i]); err != nil {
					return nil, err
				}
			}
			ans = append(ans, ansRow)
		}
	}
	return
}

func showDatabases(ctx context.Context, bh BackgroundExec, snapshotName string) ([]string, error) {
	sql := fmt.Sprintf("show databases {snapshot = '%s'}", snapshotName)
	// cols: dbname
	colsList, err := getStringColsList(ctx, bh, sql, 0)
	if err != nil {
		return nil, err
	}

	dbNames := make([]string, len(colsList))
	for i, cols := range colsList {
		dbNames[i] = cols[0]
	}
	return dbNames, nil
}

func showFullTables(ctx context.Context, bh BackgroundExec, snapshotName string, dbName string) ([]*tableInfo, error) {
	sql := fmt.Sprintf("show full tables from `%s` {snapshot = '%s'}", dbName, snapshotName)
	// cols: table name, table type
	colsList, err := getStringColsList(ctx, bh, sql, 0, 1)
	if err != nil {
		return nil, err
	}

	ans := make([]*tableInfo, len(colsList))
	for i, cols := range colsList {
		ans[i] = &tableInfo{
			name: cols[0],
			typ:  tableType(cols[1]),
		}
	}
	return ans, nil
}

func getTableInfos(ctx context.Context, bh BackgroundExec, snapshotName string, dbName string) ([]*tableInfo, error) {
	tableInfos, err := showFullTables(ctx, bh, snapshotName, dbName)
	if err != nil {
		return nil, err
	}

	for _, tblInfo := range tableInfos {
		if tblInfo.createSql, err = getCreateTableSql(ctx, bh, snapshotName, dbName, tblInfo.name); err != nil {
			return nil, err
		}
	}
	return tableInfos, nil
}

func getTableInfo(ctx context.Context, bh BackgroundExec, snapshotName string, dbName string, tblName string) ([]*tableInfo, error) {
	tableInfos, err := showFullTables(ctx, bh, snapshotName, dbName)
	if err != nil {
		return nil, err
	}

	// filter by tblName
	var ans []*tableInfo
	for _, tblInfo := range tableInfos {
		if tblInfo.name == tblName {
			ans = append(ans, tblInfo)
		}
	}

	for _, tblInfo := range ans {
		if tblInfo.createSql, err = getCreateTableSql(ctx, bh, snapshotName, dbName, tblInfo.name); err != nil {
			return nil, err
		}
	}
	return ans, nil
}

func getCreateTableSql(ctx context.Context, bh BackgroundExec, snapshotName string, dbName string, tblName string) (string, error) {
	sql := fmt.Sprintf("show create table `%s`.`%s` {snapshot = '%s'}", dbName, tblName, snapshotName)
	// cols: table_name, create_sql
	colsList, err := getStringColsList(ctx, bh, sql, 1)
	if err != nil {
		return "", nil
	}
	if len(colsList) == 0 || len(colsList[0]) == 0 {
		return "", moerr.NewNoSuchTable(ctx, dbName, tblName)
	}
	return colsList[0][0], nil
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
