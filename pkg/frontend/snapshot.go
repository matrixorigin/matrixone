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
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
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

	skipDbs = []string{"mysql", "system", "system_metrics", "mo_task", "mo_debug", "information_schema", "mo_catalog"}
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
	dbName    string
	tblName   string
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

	// default restore to src account
	toAccountId, err := getAccountId(ctx, bh, snapshot.accountName)
	if err != nil {
		return err
	}

	if len(toAccountName) > 0 {
		// can't restore to another account if cur account is not sys
		if !ses.GetTenantInfo().IsSysTenant() {
			err = moerr.NewInternalError(ctx, "non-sys account can't restore snapshot to another account")
			return
		}

		if toAccountName == sysAccountName && snapshot.accountName != sysAccountName {
			err = moerr.NewInternalError(ctx, "non-sys account's snapshot can't restore to sys account")
			return
		}

		if toAccountId, err = getAccountId(ctx, bh, string(stmt.ToAccountName)); err != nil {
			return err
		}
	}

	// restore as a txn
	if err = bh.Exec(ctx, "begin;"); err != nil {
		return err
	}
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()

	// collect views and tables with foreign keys during table restoration
	var views []*tableInfo
	var fkTables []*tableInfo
	fkDeps, err := getFkDeps(ctx, bh, snapshotName, dbName, tblName)
	if err != nil {
		return
	}

	switch stmt.Level {
	case tree.RESTORELEVELCLUSTER:
		// TODO
	case tree.RESTORELEVELACCOUNT:
		if err = restoreToAccount(ctx, bh, snapshotName, toAccountId, fkDeps, &fkTables, &views); err != nil {
			return err
		}
	case tree.RESTORELEVELDATABASE:
		if err = restoreToDatabase(ctx, bh, snapshotName, dbName, toAccountId, fkDeps, &fkTables, &views); err != nil {
			return err
		}
	case tree.RESTORELEVELTABLE:
		if err = restoreToTable(ctx, bh, snapshotName, dbName, tblName, toAccountId, fkDeps, &fkTables, &views); err != nil {
			return err
		}
	}

	if len(fkTables) > 0 {
		if err = restoreTablesWithFk(ctx, bh, snapshotName, fkDeps, fkTables, toAccountId); err != nil {
			return
		}
	}

	if len(views) > 0 {
		if err = restoreViews(ctx, ses, bh, snapshotName, views, toAccountId); err != nil {
			return
		}
	}
	return
}

func restoreToAccount(
	ctx context.Context,
	bh BackgroundExec,
	snapshotName string,
	toAccountId uint32,
	fkDeps map[string][]string,
	fkTables *[]*tableInfo,
	views *[]*tableInfo) (err error) {
	getLogger().Info(fmt.Sprintf("[%s] start to restore account: %v", snapshotName, toAccountId))

	var dbNames []string
	toCtx := defines.AttachAccountId(ctx, toAccountId)

	// delete current dbs
	if dbNames, err = showDatabases(toCtx, bh, ""); err != nil {
		return
	}

	for _, dbName := range dbNames {
		if needSkipDb(dbName) {
			getLogger().Info(fmt.Sprintf("skip drop db: %v", dbName))
			continue
		}

		if err = bh.Exec(toCtx, fmt.Sprintf("drop database %s", dbName)); err != nil {
			return
		}
	}

	// restore dbs
	if dbNames, err = showDatabases(ctx, bh, snapshotName); err != nil {
		return
	}

	for _, dbName := range dbNames {
		if err = restoreToDatabase(ctx, bh, snapshotName, dbName, toAccountId, fkDeps, fkTables, views); err != nil {
			return
		}
	}
	return
}

func restoreToDatabase(
	ctx context.Context,
	bh BackgroundExec,
	snapshotName string,
	dbName string,
	toAccountId uint32,
	fkDeps map[string][]string,
	fkTables *[]*tableInfo,
	views *[]*tableInfo) (err error) {
	getLogger().Info(fmt.Sprintf("[%s] start to restore db: %v", snapshotName, dbName))
	return restoreToDatabaseOrTable(ctx, bh, snapshotName, dbName, "", toAccountId, fkDeps, fkTables, views)
}

func restoreToTable(
	ctx context.Context,
	bh BackgroundExec,
	snapshotName string,
	dbName string,
	tblName string,
	toAccountId uint32,
	fkDeps map[string][]string,
	fkTables *[]*tableInfo,
	views *[]*tableInfo) (err error) {
	getLogger().Info(fmt.Sprintf("[%s] start to restore table: %v", snapshotName, tblName))
	return restoreToDatabaseOrTable(ctx, bh, snapshotName, dbName, tblName, toAccountId, fkDeps, fkTables, views)
}

func restoreToDatabaseOrTable(
	ctx context.Context,
	bh BackgroundExec,
	snapshotName string,
	dbName string,
	tblName string,
	toAccountId uint32,
	fkDeps map[string][]string,
	fkTables *[]*tableInfo,
	views *[]*tableInfo) (err error) {
	if needSkipDb(dbName) {
		getLogger().Info(fmt.Sprintf("skip restore db: %v", dbName))
		return
	}

	toCtx := defines.AttachAccountId(ctx, toAccountId)
	restoreToTbl := tblName != ""

	// if restore to db, delete the same name db first
	if !restoreToTbl {
		if err = bh.Exec(toCtx, "drop database if exists "+dbName); err != nil {
			return
		}
	}

	if err = bh.Exec(toCtx, "create database if not exists "+dbName); err != nil {
		return
	}

	tableInfos, err := getTableInfos(ctx, bh, snapshotName, dbName, tblName)
	if err != nil {
		return
	}

	// if restore to table, expect only one table here
	if restoreToTbl && len(tableInfos) != 1 {
		return moerr.NewInternalError(ctx, "find %v tableInfos by name, expect 1", len(tableInfos))
	}

	for _, tblInfo := range tableInfos {
		if needSkipTable(dbName, tblInfo.tblName) {
			// TODO skip tables which should not to be restored
			getLogger().Info(fmt.Sprintf("skip table: %v.%v", dbName, tblInfo.tblName))
			continue
		}

		// skip table which has foreign keys
		if _, ok := fkDeps[genKey(dbName, tblInfo.tblName)]; ok {
			*fkTables = append(*fkTables, tblInfo)
			continue
		}

		// skip view
		if tblInfo.typ == view {
			*views = append(*views, tblInfo)
			continue
		}

		if err = recreateTable(ctx, bh, snapshotName, tblInfo, toAccountId); err != nil {
			return
		}
	}
	return
}

func restoreTablesWithFk(
	ctx context.Context,
	bh BackgroundExec,
	snapshotName string,
	fkDeps map[string][]string,
	fkTables []*tableInfo,
	toAccountId uint32) (err error) {

	keyTableInfoMap := make(map[string]*tableInfo)
	g := topsort{next: make(map[string][]string)}
	for _, tblInfo := range fkTables {
		key := genKey(tblInfo.dbName, tblInfo.tblName)
		keyTableInfoMap[key] = tblInfo

		g.addVertex(key)
		for _, depTbl := range fkDeps[key] {
			// exclude self constrains
			if key != depTbl {
				g.addEdge(depTbl, key)
			}
		}
	}

	// topsort
	sortedTbls, ok := g.sort()
	if !ok {
		return moerr.NewInternalError(ctx, "There is a cycle in dependency graph")
	}

	// create views
	for _, key := range sortedTbls {
		// if not ok, means that table is not in this restoration task, ignore
		if tblInfo, ok := keyTableInfoMap[key]; ok {
			getLogger().Info(fmt.Sprintf("[%s] start to restore table with fk: %v", snapshotName, tblInfo.tblName))

			if err = recreateTable(ctx, bh, snapshotName, tblInfo, toAccountId); err != nil {
				return
			}
		}
	}
	return
}

func restoreViews(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	snapshotName string,
	views []*tableInfo,
	toAccountId uint32) error {
	snapshot, err := doResolveSnapshotWithSnapshotName(ctx, ses, snapshotName)
	if err != nil {
		return err
	}

	compCtx := ses.GetTxnCompileCtx()
	oldSnapshot := compCtx.GetSnapshot()
	compCtx.SetSnapshot(snapshot)
	defer func() {
		compCtx.SetSnapshot(oldSnapshot)
	}()

	keyTableInfoMap := make(map[string]*tableInfo)
	g := topsort{next: make(map[string][]string)}
	for _, view := range views {
		key := genKey(view.dbName, view.tblName)
		keyTableInfoMap[key] = view

		stmts, err := parsers.Parse(ctx, dialect.MYSQL, view.createSql, 1, 0)
		if err != nil {
			return err
		}

		// build create sql to find dependent views
		if _, err = plan.BuildPlan(compCtx, stmts[0], false); err != nil {
			return err
		}

		g.addVertex(key)
		for _, depView := range compCtx.GetViews() {
			g.addEdge(depView, key)
		}
	}

	// topsort
	sortedViews, ok := g.sort()
	if !ok {
		return moerr.NewInternalError(ctx, "There is a cycle in dependency graph")
	}

	// create views
	toCtx := defines.AttachAccountId(ctx, toAccountId)
	for _, key := range sortedViews {
		// if not ok, means that view is not in this restoration task, ignore
		if tblInfo, ok := keyTableInfoMap[key]; ok {
			getLogger().Info(fmt.Sprintf("[%s] start to restore view: %v", snapshotName, tblInfo.tblName))

			if err = bh.Exec(toCtx, "use "+tblInfo.dbName); err != nil {
				return err
			}

			if err = bh.Exec(toCtx, "drop view if exists "+tblInfo.tblName); err != nil {
				return err
			}

			if err = bh.Exec(toCtx, tblInfo.createSql); err != nil {
				return err
			}
		}
	}
	return nil
}

func recreateTable(
	ctx context.Context,
	bh BackgroundExec,
	snapshotName string,
	tblInfo *tableInfo,
	toAccountId uint32) (err error) {
	curAccountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return
	}

	ctx = defines.AttachAccountId(ctx, toAccountId)

	if err = bh.Exec(ctx, fmt.Sprintf("use %s", tblInfo.dbName)); err != nil {
		return
	}

	if err = bh.Exec(ctx, fmt.Sprintf("drop table if exists %s", tblInfo.tblName)); err != nil {
		return
	}

	// create table
	if err = bh.Exec(ctx, tblInfo.createSql); err != nil {
		return
	}

	// insert data
	insertIntoSql := fmt.Sprintf(restoreTableDataFmt, tblInfo.dbName, tblInfo.tblName, tblInfo.dbName, tblInfo.tblName, snapshotName)

	if curAccountId == toAccountId {
		if err = bh.Exec(ctx, insertIntoSql); err != nil {
			return
		}
	} else {
		if err = bh.ExecRestore(ctx, insertIntoSql, curAccountId, toAccountId); err != nil {
			return
		}
	}
	return
}

func needSkipDb(dbName string) bool {
	return slices.Contains(skipDbs, dbName)
}

func needSkipTable(dbName string, tblName string) bool {
	// TODO determine which tables should be skipped

	if dbName == "information_schema" {
		return true
	}

	if dbName == "mo_catalog" {
		return true
	}

	return false
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
		return nil, moerr.NewInternalError(ctx, "find %v snapshot records by name(%v), expect only 1", len(records), snapshotName)
	} else {
		return records[0], nil
	}
}

func doResolveSnapshotWithSnapshotName(ctx context.Context, ses FeSession, snapshotName string) (snapshot *pbplan.Snapshot, err error) {
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	var record *snapshotRecord
	if record, err = getSnapshotByName(ctx, bh, snapshotName); err != nil {
		return
	}

	if record == nil {
		err = moerr.NewInternalError(ctx, "snapshot %s does not exist", snapshotName)
		return
	}

	var accountId uint32
	if accountId, err = getAccountId(ctx, bh, record.accountName); err != nil {
		return
	}

	return &pbplan.Snapshot{
		TS: &timestamp.Timestamp{PhysicalTime: record.ts},
		Tenant: &pbplan.SnapshotTenant{
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
	sql := "show databases"
	if len(snapshotName) > 0 {
		sql += fmt.Sprintf(" {snapshot = '%s'}", snapshotName)
	}

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

func showFullTables(ctx context.Context, bh BackgroundExec, snapshotName string, dbName string, tblName string) ([]*tableInfo, error) {
	sql := fmt.Sprintf("show full tables from `%s` {snapshot = '%s'}", dbName, snapshotName)
	if len(tblName) > 0 {
		sql = fmt.Sprintf("show full tables from `%s` like '%s' {snapshot = '%s'}", dbName, tblName, snapshotName)
	}

	// cols: table name, table type
	colsList, err := getStringColsList(ctx, bh, sql, 0, 1)
	if err != nil {
		return nil, err
	}

	ans := make([]*tableInfo, len(colsList))
	for i, cols := range colsList {
		ans[i] = &tableInfo{
			dbName:  dbName,
			tblName: cols[0],
			typ:     tableType(cols[1]),
		}
	}
	return ans, nil
}

func getTableInfos(ctx context.Context, bh BackgroundExec, snapshotName string, dbName string, tblName string) ([]*tableInfo, error) {
	tableInfos, err := showFullTables(ctx, bh, snapshotName, dbName, tblName)
	if err != nil {
		return nil, err
	}

	for _, tblInfo := range tableInfos {
		if tblInfo.createSql, err = getCreateTableSql(ctx, bh, snapshotName, dbName, tblInfo.tblName); err != nil {
			return nil, err
		}
	}
	return tableInfos, nil
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

func getFkDeps(ctx context.Context, bh BackgroundExec, snapshotName string, dbName string, tblName string) (ans map[string][]string, err error) {
	sql := fmt.Sprintf("select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys {snapshot = '%s'}", snapshotName)
	if len(dbName) > 0 {
		sql = fmt.Sprintf("%s where db_name = '%s'", sql, dbName)
		if len(tblName) > 0 {
			sql = fmt.Sprintf("%s and table_name = '%s'", sql, tblName)
		}
	}

	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	resultSet, err := getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}

	ans = make(map[string][]string)
	var referDbName, referTblName string

	for _, rs := range resultSet {
		for row := uint64(0); row < rs.GetRowCount(); row++ {
			if dbName, err = rs.GetString(ctx, row, 0); err != nil {
				return
			}
			if tblName, err = rs.GetString(ctx, row, 1); err != nil {
				return
			}
			if referDbName, err = rs.GetString(ctx, row, 2); err != nil {
				return
			}
			if referTblName, err = rs.GetString(ctx, row, 3); err != nil {
				return
			}

			u := genKey(dbName, tblName)
			v := genKey(referDbName, referTblName)
			ans[u] = append(ans[u], v)
		}
	}
	return
}
