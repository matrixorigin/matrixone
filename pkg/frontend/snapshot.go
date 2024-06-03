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

	"github.com/matrixorigin/matrixone/pkg/catalog"
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

const (
	PubDbName = "mo_pubs"
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

	getDbPubCountWithSnapshotFormat = `select count(1) from mo_catalog.mo_pubs {snapshot = '%s'} where database_name = '%s';`

	restorePubDbDataFmt = "insert into `%s`.`%s` SELECT * FROM `%s`.`%s` {snapshot = '%s'} WHERE  DATABASE_NAME = '%s'"

	skipDbs = []string{"mysql", "system", "system_metrics", "mo_task", "mo_debug", "information_schema", "mo_catalog"}

	needSkipTablesInMocatalog = map[string]int8{
		"mo_database":         1,
		"mo_tables":           1,
		"mo_columns":          1,
		"mo_table_partitions": 1,
		"mo_foreign_keys":     1,
		"mo_indexes":          1,
		"mo_account":          1,

		catalog.MOVersionTable:       1,
		catalog.MOUpgradeTable:       1,
		catalog.MOUpgradeTenantTable: 1,
		catalog.MOAutoIncrTable:      1,

		"mo_user":                     0,
		"mo_role":                     0,
		"mo_user_grant":               0,
		"mo_role_grant":               0,
		"mo_role_privs":               0,
		"mo_user_defined_function":    0,
		"mo_stored_procedure":         0,
		"mo_mysql_compatibility_mode": 0,
		"mo_stages":                   0,
		"mo_pubs":                     1,

		"mo_sessions":       1,
		"mo_configurations": 1,
		"mo_locks":          1,
		"mo_variables":      1,
		"mo_transactions":   1,
		"mo_cache":          1,

		"mo_snapshots": 1,
	}
)

func getSqlForGetDbPubCountWithSnapshot(ctx context.Context, snapshot string, dbName string) (string, error) {
	err := inputNameIsInvalid(ctx, dbName)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(getDbPubCountWithSnapshotFormat, snapshot, dbName), nil
}

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

	if needSkipDb(dbName) {
		return moerr.NewInternalError(ctx, "can't restore db: %v", dbName)
	}

	// restore as a txn
	if err = bh.Exec(ctx, "begin;"); err != nil {
		return err
	}
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()

	// drop foreign key related tables first
	if err = deleteCurFkTables(ctx, bh, dbName, tblName, toAccountId); err != nil {
		return
	}

	// get topo sorted tables with foreign key
	sortedFkTbls, err := fkTablesTopoSort(ctx, bh, snapshotName, dbName, tblName)
	if err != nil {
		return
	}
	// get foreign key table infos
	fkTableMap, err := getTableInfoMap(ctx, bh, snapshotName, dbName, tblName, sortedFkTbls)
	if err != nil {
		return
	}

	// collect views and tables during table restoration
	viewMap := make(map[string]*tableInfo)

	switch stmt.Level {
	case tree.RESTORELEVELCLUSTER:
		// TODO
	case tree.RESTORELEVELACCOUNT:
		if err = restoreToAccount(ctx, bh, snapshotName, toAccountId, fkTableMap, viewMap); err != nil {
			return err
		}
	case tree.RESTORELEVELDATABASE:
		if err = restoreToDatabase(ctx, bh, snapshotName, dbName, toAccountId, fkTableMap, viewMap); err != nil {
			return err
		}
	case tree.RESTORELEVELTABLE:
		if err = restoreToTable(ctx, bh, snapshotName, dbName, tblName, toAccountId, fkTableMap, viewMap); err != nil {
			return err
		}
	}

	if len(fkTableMap) > 0 {
		if err = restoreTablesWithFk(ctx, bh, snapshotName, sortedFkTbls, fkTableMap, toAccountId); err != nil {
			return
		}
	}

	if len(viewMap) > 0 {
		if err = restoreViews(ctx, ses, bh, snapshotName, viewMap, toAccountId); err != nil {
			return
		}
	}
	return
}

func deleteCurFkTables(ctx context.Context, bh BackgroundExec, dbName string, tblName string, toAccountId uint32) (err error) {
	getLogger().Info("start to drop cur fk tables")

	ctx = defines.AttachAccountId(ctx, toAccountId)

	// get topo sorted tables with foreign key
	sortedFkTbls, err := fkTablesTopoSort(ctx, bh, "", dbName, tblName)
	if err != nil {
		return
	}
	// collect table infos which need to be dropped in current state; snapshotName must set to empty
	curFkTableMap, err := getTableInfoMap(ctx, bh, "", dbName, tblName, sortedFkTbls)
	if err != nil {
		return
	}

	// drop tables as anti-topo order
	for i := len(sortedFkTbls) - 1; i >= 0; i-- {
		key := sortedFkTbls[i]
		if tblInfo := curFkTableMap[key]; tblInfo != nil {
			getLogger().Info(fmt.Sprintf("start to drop table: %v", tblInfo.tblName))
			if err = bh.Exec(ctx, fmt.Sprintf("drop table if exists %s.%s", tblInfo.dbName, tblInfo.tblName)); err != nil {
				return
			}
		}
	}
	return
}

func restoreToAccount(
	ctx context.Context,
	bh BackgroundExec,
	snapshotName string,
	toAccountId uint32,
	fkTableMap map[string]*tableInfo,
	viewMap map[string]*tableInfo) (err error) {
	getLogger().Info(fmt.Sprintf("[%s] start to restore account: %v", snapshotName, toAccountId))

	var dbNames []string
	toCtx := defines.AttachAccountId(ctx, toAccountId)

	// delete current dbs
	if dbNames, err = showDatabases(toCtx, bh, ""); err != nil {
		return
	}

	for _, dbName := range dbNames {
		if needSkipDb(dbName) {
			getLogger().Info(fmt.Sprintf("[%s]skip drop db: %v", snapshotName, dbName))
			continue
		}

		// do some op to pub database
		if err := checkPubAndDropPubRecord(ctx, bh, snapshotName, dbName); err != nil {
			return err
		}

		getLogger().Info(fmt.Sprintf("[%s]drop current exists db: %v", snapshotName, dbName))
		if err = bh.Exec(toCtx, fmt.Sprintf("drop database if exists %s", dbName)); err != nil {
			return
		}
	}

	// restore dbs
	if dbNames, err = showDatabases(ctx, bh, snapshotName); err != nil {
		return
	}

	for _, dbName := range dbNames {
		if err = restoreToDatabase(ctx, bh, snapshotName, dbName, toAccountId, fkTableMap, viewMap); err != nil {
			return
		}
	}

	// restore system db
	if err = restoreSystemDatabase(ctx, bh, snapshotName, toAccountId); err != nil {
		return
	}
	return
}

func restoreToDatabase(
	ctx context.Context,
	bh BackgroundExec,
	snapshotName string,
	dbName string,
	toAccountId uint32,
	fkTableMap map[string]*tableInfo,
	viewMap map[string]*tableInfo) (err error) {
	getLogger().Info(fmt.Sprintf("[%s] start to restore db: %v", snapshotName, dbName))
	return restoreToDatabaseOrTable(ctx, bh, snapshotName, dbName, "", toAccountId, fkTableMap, viewMap)
}

func restoreToTable(
	ctx context.Context,
	bh BackgroundExec,
	snapshotName string,
	dbName string,
	tblName string,
	toAccountId uint32,
	fkTableMap map[string]*tableInfo,
	viewMap map[string]*tableInfo) (err error) {
	getLogger().Info(fmt.Sprintf("[%s] start to restore table: %v", snapshotName, tblName))
	return restoreToDatabaseOrTable(ctx, bh, snapshotName, dbName, tblName, toAccountId, fkTableMap, viewMap)
}

func restoreToDatabaseOrTable(
	ctx context.Context,
	bh BackgroundExec,
	snapshotName string,
	dbName string,
	tblName string,
	toAccountId uint32,
	fkTableMap map[string]*tableInfo,
	viewMap map[string]*tableInfo) (err error) {
	if needSkipDb(dbName) {
		getLogger().Info(fmt.Sprintf("[%s] skip restore db: %v", snapshotName, dbName))
		return
	}

	toCtx := defines.AttachAccountId(ctx, toAccountId)
	restoreToTbl := tblName != ""

	// if restore to db, delete the same name db first
	if !restoreToTbl {
		getLogger().Info(fmt.Sprintf("[%s] start to drop database: %v", snapshotName, dbName))
		if err = bh.Exec(toCtx, "drop database if exists "+dbName); err != nil {
			return
		}
	}

	getLogger().Info(fmt.Sprintf("[%s] start to create database: %v", snapshotName, dbName))
	if err = bh.Exec(toCtx, "create database if not exists "+dbName); err != nil {
		return
	}

	if !restoreToTbl {
		if err = checkAndRestorePublicationRecord(ctx, bh, snapshotName, dbName, toAccountId); err != nil {
			return
		}
	}

	tableInfos, err := getTableInfos(ctx, bh, snapshotName, dbName, tblName)
	if err != nil {
		return
	}

	// if restore to table, expect only one table here
	if restoreToTbl {
		if len(tableInfos) == 0 {
			return moerr.NewInternalError(ctx, "table %s not exists at snapshot %s", tblName, snapshotName)
		} else if len(tableInfos) != 1 {
			return moerr.NewInternalError(ctx, "find %v tableInfos by name %s at snapshot %s, expect only 1", len(tableInfos), tblName, snapshotName)
		}
	}

	for _, tblInfo := range tableInfos {
		key := genKey(dbName, tblInfo.tblName)

		// skip table which is foreign key related
		if _, ok := fkTableMap[key]; ok {
			continue
		}

		// skip view
		if tblInfo.typ == view {
			viewMap[key] = tblInfo
			continue
		}

		if err = recreateTable(ctx, bh, snapshotName, tblInfo, toAccountId); err != nil {
			return
		}
	}
	return
}

func restoreSystemDatabase(
	ctx context.Context,
	bh BackgroundExec,
	snapshotName string,
	toAccountId uint32) (err error) {
	getLogger().Info(fmt.Sprintf("[%s] start to restore system database: %s", snapshotName, moCatalog))
	tableInfos, err := getTableInfos(ctx, bh, snapshotName, moCatalog, "")
	if err != nil {
		return
	}

	curAccountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return
	}

	for _, tblInfo := range tableInfos {
		if needSkipTable(curAccountId, moCatalog, tblInfo.tblName) {
			// TODO skip tables which should not to be restored
			getLogger().Info(fmt.Sprintf("[%s] skip restore system table: %v.%v", snapshotName, moCatalog, tblInfo.tblName))
			continue
		}

		getLogger().Info(fmt.Sprintf("[%s] start to restore system table: %v.%v", snapshotName, moCatalog, tblInfo.tblName))
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
	sortedFkTbls []string,
	fkTableMap map[string]*tableInfo,
	toAccountId uint32) (err error) {
	getLogger().Info(fmt.Sprintf("[%s] start to drop fk related tables", snapshotName))

	// recreate tables as topo order
	for _, key := range sortedFkTbls {
		// if tblInfo is nil, means that table is not in this restoration task, ignore
		// e.g. t1.pk <- t2.fk, we only want to restore t2, fkTableMap[t1.key] is nil, ignore t1
		if tblInfo := fkTableMap[key]; tblInfo != nil {
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
	viewMap map[string]*tableInfo,
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

	g := topsort{next: make(map[string][]string)}
	for key, view := range viewMap {
		stmts, err := parsers.Parse(ctx, dialect.MYSQL, view.createSql, 1, 0)
		if err != nil {
			return err
		}

		compCtx.SetDatabase(view.dbName)
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
	sortedViews, err := g.sort()
	if err != nil {
		return err
	}

	// create views
	toCtx := defines.AttachAccountId(ctx, toAccountId)
	for _, key := range sortedViews {
		// if not ok, means that view is not in this restoration task, ignore
		if tblInfo, ok := viewMap[key]; ok {
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
	getLogger().Info(fmt.Sprintf("[%s] start to restore table: %v", snapshotName, tblInfo.tblName))
	curAccountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return
	}

	ctx = defines.AttachAccountId(ctx, toAccountId)

	if err = bh.Exec(ctx, fmt.Sprintf("use %s", tblInfo.dbName)); err != nil {
		return
	}

	getLogger().Info(fmt.Sprintf("[%s] start to drop table: %v", snapshotName, tblInfo.tblName))
	if err = bh.Exec(ctx, fmt.Sprintf("drop table if exists %s", tblInfo.tblName)); err != nil {
		return
	}

	// create table
	getLogger().Info(fmt.Sprintf("[%s] start to create table: %v, create table sql: %s", snapshotName, tblInfo.tblName, tblInfo.createSql))
	if err = bh.Exec(ctx, tblInfo.createSql); err != nil {
		return
	}

	// insert data
	insertIntoSql := fmt.Sprintf(restoreTableDataFmt, tblInfo.dbName, tblInfo.tblName, tblInfo.dbName, tblInfo.tblName, snapshotName)
	getLogger().Info(fmt.Sprintf("[%s] start to insert select table: %v, insert sql: %s", snapshotName, tblInfo.tblName, insertIntoSql))

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

func needSkipTable(accountId uint32, dbName string, tblName string) bool {
	// TODO determine which tables should be skipped

	if accountId == sysAccountID {
		return dbName == moCatalog && needSkipTablesInMocatalog[tblName] == 1
	} else {
		if dbName == moCatalog {
			if needSkip, ok := needSkipTablesInMocatalog[tblName]; ok {
				return needSkip == 1
			} else {
				return true
			}
		}
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
	getLogger().Info(fmt.Sprintf("[%s] start to get all database ", snapshotName))
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
	sql := fmt.Sprintf("show full tables from `%s`", dbName)
	if len(tblName) > 0 {
		sql += fmt.Sprintf(" like '%s'", tblName)
	}
	if len(snapshotName) > 0 {
		sql += fmt.Sprintf(" {snapshot = '%s'}", snapshotName)
	}
	getLogger().Info(fmt.Sprintf("[%s] get table %v info sql: %s", snapshotName, tblName, sql))
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
	getLogger().Info(fmt.Sprintf("[%s] start to get table info: %v", snapshotName, tblName))
	tableInfos, err := showFullTables(ctx, bh, snapshotName, dbName, tblName)
	if err != nil {
		return nil, err
	}

	// only recreate snapshoted table need create sql
	if snapshotName != "" {
		for _, tblInfo := range tableInfos {
			if tblInfo.createSql, err = getCreateTableSql(ctx, bh, snapshotName, dbName, tblInfo.tblName); err != nil {
				return nil, err
			}
		}
	}
	return tableInfos, nil
}

func getTableInfo(ctx context.Context, bh BackgroundExec, snapshotName string, dbName, tblName string) (*tableInfo, error) {
	tableInfos, err := getTableInfos(ctx, bh, snapshotName, dbName, tblName)
	if err != nil {
		return nil, err
	}

	// if table doesn't exist, return nil
	if len(tableInfos) == 0 {
		return nil, nil
	}
	return tableInfos[0], nil
}

func getCreateTableSql(ctx context.Context, bh BackgroundExec, snapshotName string, dbName string, tblName string) (string, error) {
	sql := fmt.Sprintf("show create table `%s`.`%s`", dbName, tblName)
	if len(snapshotName) > 0 {
		sql += fmt.Sprintf(" {snapshot = '%s'}", snapshotName)
	}

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
	sql := "select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys"
	if len(snapshotName) > 0 {
		sql += fmt.Sprintf(" {snapshot = '%s'}", snapshotName)
	}
	if len(dbName) > 0 {
		sql += fmt.Sprintf(" where db_name = '%s'", dbName)
		if len(tblName) > 0 {
			sql += fmt.Sprintf(" and table_name = '%s'", tblName)
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

func getTableInfoMap(
	ctx context.Context,
	bh BackgroundExec,
	snapshotName string,
	dbName string,
	tblName string,
	tblKeys []string) (tblInfoMap map[string]*tableInfo, err error) {
	tblInfoMap = make(map[string]*tableInfo)
	curAccountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return
	}
	for _, key := range tblKeys {
		if _, ok := tblInfoMap[key]; ok {
			return
		}

		// filter by dbName and tblName
		d, t := splitKey(key)
		if needSkipDb(d) || needSkipTable(curAccountId, d, t) {
			return
		}
		if dbName != "" && dbName != d {
			return
		}
		if tblName != "" && tblName != t {
			return
		}

		if tblInfoMap[key], err = getTableInfo(ctx, bh, snapshotName, d, t); err != nil {
			return
		}
	}
	return
}

func fkTablesTopoSort(ctx context.Context, bh BackgroundExec, snapshotName string, dbName string, tblName string) (sortedTbls []string, err error) {
	// get foreign key deps from mo_catalog.mo_foreign_keys
	fkDeps, err := getFkDeps(ctx, bh, snapshotName, dbName, tblName)
	if err != nil {
		return
	}

	g := topsort{next: make(map[string][]string)}
	for key, deps := range fkDeps {
		g.addVertex(key)
		for _, depTbl := range deps {
			// exclude self dep
			if key != depTbl {
				g.addEdge(depTbl, key)
			}
		}
	}
	sortedTbls, err = g.sort()
	return
}

// checkPubAndDropPubRecord checks if the database is publicated, if so, delete the publication
func checkPubAndDropPubRecord(
	ctx context.Context,
	bh BackgroundExec,
	snapshotName string,
	dbName string) (err error) {
	// check if the database is publicated
	sql, err := getSqlForDbPubCount(ctx, dbName)
	if err != nil {
		return
	}
	getLogger().Info(fmt.Sprintf("[%s] start to check if db '%v' is publicated, check sql: %s", snapshotName, dbName, sql))

	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	if execResultArrayHasData(erArray) {
		var pubCount int64
		if pubCount, err = erArray[0].GetInt64(ctx, 0, 0); err != nil {
			return
		}
		if pubCount > 0 {
			// drop the publication
			sql, err = getSqlForDeletePubFromDatabase(ctx, dbName)
			if err != nil {
				return
			}
			getLogger().Info(fmt.Sprintf("[%s] start to drop publication for db '%v', drop sql: %s", snapshotName, dbName, sql))
			if err = bh.Exec(ctx, sql); err != nil {
				return
			}
		}
	}
	return
}

// checkAndRestorePublicationRecord checks if the database is publicated, if so, restore the publication record
func checkAndRestorePublicationRecord(
	ctx context.Context,
	bh BackgroundExec,
	snapshotName string,
	dbName string,
	toAccountId uint32) (err error) {

	// check if the database is publicated
	sql, err := getSqlForGetDbPubCountWithSnapshot(ctx, snapshotName, dbName)
	if err != nil {
		return
	}

	getLogger().Info(fmt.Sprintf("[%s] start to check if db '%v' is publicated, check sql: %s", snapshotName, dbName, sql))

	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	if execResultArrayHasData(erArray) {
		var pubCount int64
		if pubCount, err = erArray[0].GetInt64(ctx, 0, 0); err != nil {
			return
		}
		if pubCount > 0 {
			// restore the publication record
			var curAccountId uint32
			curAccountId, err = defines.GetAccountId(ctx)
			if err != nil {
				return
			}

			ctx = defines.AttachAccountId(ctx, toAccountId)

			// insert data
			insertIntoSql := fmt.Sprintf(restorePubDbDataFmt, moCatalog, PubDbName, moCatalog, PubDbName, snapshotName, dbName)
			getLogger().Info(fmt.Sprintf("[%s] start to restore db '%s' pub record, insert sql: %s", snapshotName, PubDbName, insertIntoSql))

			if curAccountId == toAccountId {
				if err = bh.Exec(ctx, insertIntoSql); err != nil {
					return
				}
			} else {
				if err = bh.ExecRestore(ctx, insertIntoSql, curAccountId, toAccountId); err != nil {
					return
				}
			}
		}
	}
	return
}
