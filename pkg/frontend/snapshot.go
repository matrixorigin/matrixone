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
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
)

type tableType string

const view tableType = "VIEW"

const clusterTable tableType = "CLUSTER TABLE"

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

	restoreTableDataByTsFmt = "insert into `%s`.`%s` SELECT * FROM `%s`.`%s` {MO_TS = %d }"

	restoreTableDataByNameFmt = "insert into `%s`.`%s` SELECT * FROM `%s`.`%s` {SNAPSHOT = '%s'}"

	getPubInfoWithSnapshotFormat = `select pub_name, database_name, database_id, table_list, account_list, created_time, update_time, owner, creator, comment from mo_catalog.mo_pubs {snapshot = '%s'} where database_name = '%s';`

	getRestoreAccountsFmt = "select account_name, account_id from mo_catalog.mo_account where account_name in (select account_name from mo_catalog.mo_account {MO_TS = %d }) ORDER BY account_id ASC;"

	getSubsSqlFmt = "select sub_account_id, sub_name, sub_time, pub_account_name, pub_name, pub_database, pub_tables, pub_time, pub_comment, status from mo_catalog.mo_subs %s where 1=1"

	//getDropAccountFmt = "select account_name from mo_catalog.mo_account where account_name not in (select account_name from mo_catalog.mo_account {MO_TS = %d });"

	//restoreDropAccountFmt = "insert into mo_catalog.mo_account select * from mo_catalog.mo_account {MO_TS = %d } where account_name not in (select account_name from mo_catalog.mo_account);"

	//deleteAccountFmt = "delete from mo_catalog.mo_account where account_id != '0';"

	//restoreAccountFmt = "insert into mo_catalog.mo_account select * from mo_catalog.mo_account {MO_TS = %d } where account_id != '0';"

	//dropAccountFmt = "drop account if exists %s;"

	skipDbs = []string{"mysql", "system", "system_metrics", "mo_task", "mo_debug", "information_schema", moCatalog}

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
		catalog.MO_PUBS:               1,
		catalog.MO_SUBS:               1,

		"mo_sessions":       1,
		"mo_configurations": 1,
		"mo_locks":          1,
		"mo_variables":      1,
		"mo_transactions":   1,
		"mo_cache":          1,

		catalog.MO_SNAPSHOTS: 1,
		catalog.MO_PITR:      1,
	}
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

type accountRecord struct {
	accountName string
	accountId   uint64
}

type subDbRestoreRecord struct {
	dbName     string
	Account    uint32
	createSql  string
	snapshotTs int64
}

func NewSubDbRestoreRecord(dbName string, account uint32, createSql string, spTs int64) *subDbRestoreRecord {
	return &subDbRestoreRecord{
		dbName:     dbName,
		Account:    account,
		createSql:  createSql,
		snapshotTs: spTs,
	}
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

	// check create snapshot priv
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
	var snapshotExist bool
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

	// check snapshot exists or not
	snapshotExist, err = checkSnapShotExistOrNot(ctx, bh, string(stmt.Name))
	if err != nil {
		return err
	}

	if !snapshotExist {
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

	var restoreAccount uint32
	var toAccountId uint32
	// restore as a txn
	if err = bh.Exec(ctx, "begin;"); err != nil {
		return err
	}
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()

	// check snapshot
	snapshot, err := getSnapshotByName(ctx, bh, snapshotName)
	if err != nil {
		return err
	}

	if snapshot == nil {
		return moerr.NewInternalError(ctx, "snapshot %s does not exist", snapshotName)
	}
	if snapshot.accountName != srcAccountName && snapshot.level != tree.RESTORELEVELCLUSTER.String() {
		return moerr.NewInternalError(ctx, "accountName(%v) does not match snapshot.accountName(%v)", srcAccountName, snapshot.accountName)
	}

	// default restore to src account
	restoreAccount, err = getAccountId(ctx, bh, srcAccountName)
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
	} else {
		toAccountId = restoreAccount
	}

	// check restore cluster
	if stmt.Level == tree.RESTORELEVELCLUSTER {
		if !ses.GetTenantInfo().IsSysTenant() {
			err = moerr.NewInternalError(ctx, "non-sys account can't restore cluster level snapshot")
			return
		}

		if snapshot.level != tree.SNAPSHOTLEVELCLUSTER.String() {
			err = moerr.NewInternalError(ctx, "snapshot %s is not cluster level snapshot", snapshotName)
			return
		}
	}

	if len(dbName) > 0 && needSkipDb(dbName) {
		return moerr.NewInternalError(ctx, "can't restore db: %v", dbName)
	}

	if snapshot.level == tree.RESTORELEVELCLUSTER.String() && len(srcAccountName) != 0 {
		var srcAccountId uint32
		srcAccountId, err = getAccountId(ctx, bh, srcAccountName)
		if err != nil {
			return err
		}

		var sp string
		sp, err = insertSnapshotRecord(ctx, ses.GetService(), bh, snapshot.snapshotName, snapshot.ts, uint64(srcAccountId), srcAccountName)
		if err != nil {
			return err
		}
		snapshotName = sp
		defer func() {
			if err != nil {
				deleteSnapshotRecord(ctx, ses.GetService(), bh, snapshot.snapshotName, sp)
			}
		}()
	}

	// restore cluster
	if stmt.Level == tree.RESTORELEVELCLUSTER {
		// restore cluster
		subDbToRestore := make(map[string]*subDbRestoreRecord)
		if err = restoreToCluster(ctx, ses, bh, snapshotName, snapshot.ts, subDbToRestore); err != nil {
			return err
		}

		if len(subDbToRestore) > 0 {
			for _, subDb := range subDbToRestore {
				if err = restoreToSubDb(ctx, ses.GetService(), bh, snapshotName, subDb); err != nil {
					return err
				}
			}
		}
		return
	}

	// drop foreign key related tables first
	if err = deleteCurFkTables(ctx, ses.GetService(), bh, dbName, tblName, toAccountId); err != nil {
		return
	}

	// get topo sorted tables with foreign key
	sortedFkTbls, err := fkTablesTopoSort(ctx, bh, snapshotName, dbName, tblName)
	if err != nil {
		return
	}

	// get foreign key table infos
	fkTableMap, err := getTableInfoMap(ctx, ses.GetService(), bh, snapshotName, dbName, tblName, sortedFkTbls)
	if err != nil {
		return
	}

	// collect views and tables during table restoration
	viewMap := make(map[string]*tableInfo)

	switch stmt.Level {
	case tree.RESTORELEVELACCOUNT:
		if err = restoreToAccount(ctx,
			ses.GetService(),
			bh, snapshotName,
			toAccountId,
			fkTableMap,
			viewMap,
			snapshot.ts,
			restoreAccount,
			false,
			nil); err != nil {
			return err
		}
	case tree.RESTORELEVELDATABASE:
		if err = restoreToDatabase(ctx,
			ses.GetService(),
			bh,
			snapshotName,
			dbName,
			toAccountId,
			fkTableMap,
			viewMap,
			snapshot.ts,
			restoreAccount,
			false,
			nil); err != nil {
			return err
		}
	case tree.RESTORELEVELTABLE:
		if err = restoreToTable(ctx,
			ses.GetService(),
			bh,
			snapshotName,
			dbName,
			tblName,
			toAccountId,
			fkTableMap,
			viewMap,
			snapshot.ts,
			restoreAccount); err != nil {
			return err
		}
	}

	if len(fkTableMap) > 0 {
		if err = restoreTablesWithFk(ctx, ses.GetService(), bh, snapshotName, sortedFkTbls, fkTableMap, toAccountId, snapshot.ts); err != nil {
			return
		}
	}

	if len(viewMap) > 0 {
		if err = restoreViews(ctx, ses, bh, snapshotName, viewMap, toAccountId); err != nil {
			return
		}
	}

	if snapshot.level == tree.RESTORELEVELCLUSTER.String() && len(srcAccountName) != 0 {
		err = deleteSnapshotRecord(ctx, ses.GetService(), bh, snapshot.snapshotName, snapshotName)
		if err != nil {
			return err
		}
	}

	// checks if the given context has been canceled.
	if err = CancelCheck(ctx); err != nil {
		return
	}

	return
}

func deleteCurFkTables(ctx context.Context, sid string, bh BackgroundExec, dbName string, tblName string, toAccountId uint32) (err error) {
	getLogger(sid).Info("start to drop cur fk tables")

	ctx = defines.AttachAccountId(ctx, toAccountId)

	// get topo sorted tables with foreign key
	sortedFkTbls, err := fkTablesTopoSort(ctx, bh, "", dbName, tblName)
	if err != nil {
		return
	}
	// collect table infos which need to be dropped in current state; snapshotName must set to empty
	curFkTableMap, err := getTableInfoMap(ctx, sid, bh, "", dbName, tblName, sortedFkTbls)
	if err != nil {
		return
	}

	// drop tables as anti-topo order
	for i := len(sortedFkTbls) - 1; i >= 0; i-- {
		key := sortedFkTbls[i]
		if tblInfo := curFkTableMap[key]; tblInfo != nil {
			getLogger(sid).Info(fmt.Sprintf("start to drop table: %v", tblInfo.tblName))
			if err = bh.Exec(ctx, fmt.Sprintf("drop table if exists %s.%s", tblInfo.dbName, tblInfo.tblName)); err != nil {
				return
			}
		}
	}
	return
}

func restoreToAccount(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	snapshotName string,
	toAccountId uint32,
	fkTableMap map[string]*tableInfo,
	viewMap map[string]*tableInfo,
	snapshotTs int64,
	restoreAccount uint32,
	isRestoreCluster bool,
	subDbToRestore map[string]*subDbRestoreRecord,
) (err error) {
	getLogger(sid).Info(fmt.Sprintf("[%s] start to restore account: %v, restore timestamp : %d", snapshotName, toAccountId, snapshotTs))

	var dbNames []string
	toCtx := defines.AttachAccountId(ctx, toAccountId)

	// delete current dbs
	if dbNames, err = showDatabases(toCtx, sid, bh, ""); err != nil {
		return
	}

	for _, dbName := range dbNames {
		if needSkipDb(dbName) {
			if toAccountId == 0 && dbName == moCatalog {
				// drop existing cluster tables
				if err = dropClusterTable(toCtx, sid, bh, "", toAccountId); err != nil {
					return
				}
			}
			getLogger(sid).Info(fmt.Sprintf("[%s]skip drop db: %v", snapshotName, dbName))
			continue
		}

		getLogger(sid).Info(fmt.Sprintf("[%s]drop current exists db: %v", snapshotName, dbName))
		if err = dropDb(toCtx, bh, dbName); err != nil {
			return
		}
	}

	// restore dbs
	if dbNames, err = showDatabases(ctx, sid, bh, snapshotName); err != nil {
		return
	}

	for _, dbName := range dbNames {
		if err = restoreToDatabase(ctx,
			sid,
			bh,
			snapshotName,
			dbName,
			toAccountId,
			fkTableMap,
			viewMap,
			snapshotTs,
			restoreAccount,
			isRestoreCluster,
			subDbToRestore); err != nil {
			return
		}
	}

	// restore system db
	if err = restoreSystemDatabase(ctx,
		sid,
		bh,
		snapshotName,
		toAccountId,
		snapshotTs); err != nil {
		return
	}
	return
}

func restoreToDatabase(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	snapshotName string,
	dbName string,
	toAccountId uint32,
	fkTableMap map[string]*tableInfo,
	viewMap map[string]*tableInfo,
	snapshotTs int64,
	restoreAccount uint32,
	isRestoreCluster bool,
	subDbToRestore map[string]*subDbRestoreRecord,
) (err error) {
	getLogger(sid).Info(fmt.Sprintf("[%s] start to restore db: %v, restore timestamp: %d", snapshotName, dbName, snapshotTs))
	return restoreToDatabaseOrTable(ctx,
		sid,
		bh,
		snapshotName,
		dbName,
		"",
		toAccountId,
		fkTableMap,
		viewMap,
		snapshotTs,
		restoreAccount,
		isRestoreCluster,
		subDbToRestore)
}

func restoreToTable(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	snapshotName string,
	dbName string,
	tblName string,
	toAccountId uint32,
	fkTableMap map[string]*tableInfo,
	viewMap map[string]*tableInfo,
	snapshotTs int64,
	restoreAccount uint32) (err error) {
	getLogger(sid).Info(fmt.Sprintf("[%s] start to restore table: %v, restore timestamp: %d", snapshotName, tblName, snapshotTs))
	return restoreToDatabaseOrTable(ctx,
		sid,
		bh,
		snapshotName,
		dbName,
		tblName,
		toAccountId,
		fkTableMap,
		viewMap,
		snapshotTs,
		restoreAccount,
		false,
		nil)
}

func restoreToDatabaseOrTable(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	snapshotName string,
	dbName string,
	tblName string,
	toAccountId uint32,
	fkTableMap map[string]*tableInfo,
	viewMap map[string]*tableInfo,
	snapshotTs int64,
	restoreAccount uint32,
	isRestoreCluster bool,
	subDbToRestore map[string]*subDbRestoreRecord,
) (err error) {
	if needSkipDb(dbName) {
		getLogger(sid).Info(fmt.Sprintf("[%s] skip restore db: %v", snapshotName, dbName))
		return
	}

	var createDbSql string
	createDbSql, err = getCreateDatabaseSql(ctx, sid, bh, snapshotName, dbName, restoreAccount)
	if err != nil {
		return
	}

	toCtx := defines.AttachAccountId(ctx, toAccountId)
	restoreToTbl := tblName != ""

	// if restore to table, check if the db is sub db
	isSubDb := strings.Contains(createDbSql, "from") && strings.Contains(createDbSql, "publication")
	if isSubDb && restoreToTbl {
		return moerr.NewInternalError(ctx, "can't restore to table for sub db")
	}

	if isRestoreCluster && isSubDb {
		// if restore to cluster, and the db is sub, append the sub db to restore list
		getLogger(sid).Info(fmt.Sprintf("[%s] append sub db to restore list: %v, at restore cluster account %d", snapshotName, dbName, toAccountId))
		key := genKey(fmt.Sprint(restoreAccount), dbName)
		subDbToRestore[key] = NewSubDbRestoreRecord(dbName, restoreAccount, createDbSql, snapshotTs)
		return

	}

	// if current account is not to account id, and the db is sub db, skip restore
	if restoreAccount != toAccountId && isSubDb {
		getLogger(sid).Info(fmt.Sprintf("[%s] skip restore subscription db: %v, current account is not to account", snapshotName, dbName))
		return
	}

	// if restore to db, delete the same name db first
	if !restoreToTbl {
		getLogger(sid).Info(fmt.Sprintf("[%s] start to drop database: %v", snapshotName, dbName))
		if err = dropDb(toCtx, bh, dbName); err != nil {
			return
		}
	}

	getLogger(sid).Info(fmt.Sprintf("[%s] start to create database: %v", snapshotName, dbName))

	if isSubDb {

		// check if the publication exists
		// if the publication exists, create the db with the publication
		// else skip restore the db

		var isPubExist bool
		isPubExist, err = checkPubExistOrNot(toCtx, sid, bh, snapshotName, dbName, snapshotTs)
		if err != nil {
			return
		}

		if !isPubExist {
			getLogger(sid).Info(fmt.Sprintf("[%s] skip restore db: %v, no publication", snapshotName, dbName))
			return
		}

		// create db with publication
		getLogger(sid).Info(fmt.Sprintf("[%s] start to create db with pub: %v, create db sql: %s", snapshotName, dbName, createDbSql))
		if err = bh.Exec(toCtx, createDbSql); err != nil {
			return
		}

		return
	} else {
		createDbSql = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName)
		// create db
		getLogger(sid).Info(fmt.Sprintf("[%s] start to create db: %v, create db sql: %s", snapshotName, dbName, createDbSql))
		if err = bh.Exec(toCtx, createDbSql); err != nil {
			return
		}
	}

	if !restoreToTbl {
		getLogger(sid).Info(fmt.Sprintf("[%s] start to create pub: %v", snapshotName, dbName))
		if err = createPub(ctx, sid, bh, snapshotName, dbName, toAccountId); err != nil {
			return
		}
	}

	tableInfos, err := getTableInfos(ctx, sid, bh, snapshotName, dbName, tblName)
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

		// checks if the given context has been canceled.
		if err = CancelCheck(ctx); err != nil {
			return
		}

		if err = recreateTable(ctx, sid, bh, snapshotName, tblInfo, toAccountId, snapshotTs); err != nil {
			return
		}
	}
	return
}

func restoreSystemDatabase(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	snapshotName string,
	toAccountId uint32,
	snapshotTs int64,
) (err error) {
	getLogger(sid).Info(fmt.Sprintf("[%s] start to restore system database: %s", snapshotName, moCatalog))
	tableInfos, err := getTableInfos(ctx, sid, bh, snapshotName, moCatalog, "")
	if err != nil {
		return
	}

	for _, tblInfo := range tableInfos {
		if needSkipSystemTable(toAccountId, tblInfo) {
			// TODO skip tables which should not to be restored
			getLogger(sid).Info(fmt.Sprintf("[%s] skip restore system table: %v.%v, table type: %v", snapshotName, moCatalog, tblInfo.tblName, tblInfo.typ))
			continue
		}

		getLogger(sid).Info(fmt.Sprintf("[%s] start to restore system table: %v.%v", snapshotName, moCatalog, tblInfo.tblName))

		// checks if the given context has been canceled.
		if err = CancelCheck(ctx); err != nil {
			return
		}

		if err = recreateTable(ctx, sid, bh, snapshotName, tblInfo, toAccountId, snapshotTs); err != nil {
			return
		}
	}
	return
}

func restoreToSubDb(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	snapshotName string,
	subDb *subDbRestoreRecord) (err error) {
	getLogger(sid).Info(fmt.Sprintf("[%s] start to restore sub db: %v", snapshotName, subDb.dbName))

	toCtx := defines.AttachAccountId(ctx, subDb.Account)

	var isPubExist bool
	isPubExist, err = checkPubExistOrNot(toCtx, sid, bh, snapshotName, subDb.dbName, subDb.snapshotTs)
	if err != nil {
		return
	}

	if !isPubExist {
		getLogger(sid).Info(fmt.Sprintf("[%s] skip restore db: %v, no publication", snapshotName, subDb.dbName))
		return
	}

	getLogger(sid).Info(fmt.Sprintf("[%s] account %d start to create sub db: %v, create db sql: %s", snapshotName, subDb.Account, subDb.dbName, subDb.createSql))
	if err = bh.Exec(toCtx, subDb.createSql); err != nil {
		return
	}

	return
}

func dropClusterTable(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	snapshotName string,
	toAccountId uint32,
) (err error) {
	getLogger(sid).Info("start to drop exists cluster table")

	// get all tables in mo_catalog
	tableInfos, err := getTableInfos(ctx, sid, bh, snapshotName, moCatalog, "")
	if err != nil {
		return
	}

	for _, tblInfo := range tableInfos {
		if toAccountId == 0 && tblInfo.typ == clusterTable {
			getLogger(sid).Info(fmt.Sprintf("[%s] start to drop system table: %v.%v", snapshotName, moCatalog, tblInfo.tblName))
			if err = bh.Exec(ctx, fmt.Sprintf("drop table if exists %s.%s", moCatalog, tblInfo.tblName)); err != nil {
				return
			}
		}
	}
	return
}

func restoreTablesWithFk(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	snapshotName string,
	sortedFkTbls []string,
	fkTableMap map[string]*tableInfo,
	toAccountId uint32,
	snapshotTs int64) (err error) {
	getLogger(sid).Info(fmt.Sprintf("[%s] start to drop fk related tables", snapshotName))

	// recreate tables as topo order
	for _, key := range sortedFkTbls {
		// if tblInfo is nil, means that table is not in this restoration task, ignore
		// e.g. t1.pk <- t2.fk, we only want to restore t2, fkTableMap[t1.key] is nil, ignore t1
		if tblInfo := fkTableMap[key]; tblInfo != nil {
			getLogger(sid).Info(fmt.Sprintf("[%s] start to restore table with fk: %v, restore timestamp: %d", snapshotName, tblInfo.tblName, snapshotTs))
			if err = recreateTable(ctx, sid, bh, snapshotName, tblInfo, toAccountId, snapshotTs); err != nil {
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
	snapshot, err := getSnapshotPlanWithSharedBh(ctx, bh, snapshotName)
	if err != nil {
		return err
	}

	compCtx := ses.GetTxnCompileCtx()
	oldSnapshot := compCtx.GetSnapshot()
	compCtx.SetSnapshot(snapshot)
	defer func() {
		compCtx.SetSnapshot(oldSnapshot)
	}()

	g := toposort{next: make(map[string][]string)}
	for key, view := range viewMap {
		stmts, err := parsers.Parse(ctx, dialect.MYSQL, view.createSql, 1)
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

	// toposort
	sortedViews, err := g.sort()
	if err != nil {
		return err
	}

	// create views
	toCtx := defines.AttachAccountId(ctx, toAccountId)
	for _, key := range sortedViews {
		// if not ok, means that view is not in this restoration task, ignore
		if tblInfo, ok := viewMap[key]; ok {
			getLogger(ses.GetService()).Info(fmt.Sprintf("[%s] start to restore view: %v, restore timestamp: %d", snapshotName, tblInfo.tblName, snapshot.TS.PhysicalTime))

			if err = bh.Exec(toCtx, "use `"+tblInfo.dbName+"`"); err != nil {
				return err
			}

			if err = bh.Exec(toCtx, "drop view if exists "+tblInfo.tblName); err != nil {
				return err
			}

			getLogger(ses.GetService()).Info(fmt.Sprintf("[%s] start to create view: %v, create view sql: %s", snapshotName, tblInfo.tblName, tblInfo.createSql))
			if err = bh.Exec(toCtx, tblInfo.createSql); err != nil {
				return err
			}
			getLogger(ses.GetService()).Info(fmt.Sprintf("[%s] restore view: %v success", snapshotName, tblInfo.tblName))
		}
	}
	return nil
}

func recreateTable(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	snapshotName string,
	tblInfo *tableInfo,
	toAccountId uint32,
	snapshotTs int64) (err error) {
	getLogger(sid).Info(fmt.Sprintf("[%s] start to restore table: %v, restore timestamp: %d", snapshotName, tblInfo.tblName, snapshotTs))
	curAccountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return
	}

	ctx = defines.AttachAccountId(ctx, toAccountId)

	if err = bh.Exec(ctx, fmt.Sprintf("use `%s`", tblInfo.dbName)); err != nil {
		return
	}

	getLogger(sid).Info(fmt.Sprintf("[%s] start to drop table: %v,", snapshotName, tblInfo.tblName))
	if err = bh.Exec(ctx, fmt.Sprintf("drop table if exists %s", tblInfo.tblName)); err != nil {
		return
	}

	// create table
	getLogger(sid).Info(fmt.Sprintf("[%s] start to create table: %v, create table sql: %s", snapshotName, tblInfo.tblName, tblInfo.createSql))
	if err = bh.Exec(ctx, tblInfo.createSql); err != nil {
		return
	}

	if curAccountId == toAccountId {
		// insert data
		insertIntoSql := fmt.Sprintf(restoreTableDataByTsFmt, tblInfo.dbName, tblInfo.tblName, tblInfo.dbName, tblInfo.tblName, snapshotTs)
		beginTime := time.Now()
		getLogger(sid).Info(fmt.Sprintf("[%s] start to insert select table: %v, insert sql: %s", snapshotName, tblInfo.tblName, insertIntoSql))
		if err = bh.Exec(ctx, insertIntoSql); err != nil {
			return
		}
		getLogger(sid).Info(fmt.Sprintf("[%s] insert select table: %v, cost: %v", snapshotName, tblInfo.tblName, time.Since(beginTime)))
	} else {
		insertIntoSql := fmt.Sprintf(restoreTableDataByNameFmt, tblInfo.dbName, tblInfo.tblName, tblInfo.dbName, tblInfo.tblName, snapshotName)
		beginTime := time.Now()
		getLogger(sid).Info(fmt.Sprintf("[%s] start to insert select table: %v, insert sql: %s", snapshotName, tblInfo.tblName, insertIntoSql))
		if err = bh.ExecRestore(ctx, insertIntoSql, curAccountId, toAccountId); err != nil {
			return
		}
		getLogger(sid).Info(fmt.Sprintf("[%s] insert select table: %v, cost: %v", snapshotName, tblInfo.tblName, time.Since(beginTime)))
	}
	return
}

func needSkipDb(dbName string) bool {
	return slices.Contains(skipDbs, dbName)
}

func needSkipTable(accountId uint32, dbName string, tblName string) bool {
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

func needSkipSystemTable(accountId uint32, tblinfo *tableInfo) bool {
	if accountId == sysAccountID {
		return tblinfo.dbName == moCatalog && needSkipTablesInMocatalog[tblinfo.tblName] == 1
	} else {
		return tblinfo.dbName == moCatalog && (tblinfo.typ == clusterTable || needSkipTablesInMocatalog[tblinfo.tblName] == 1)
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
	bh := ses.GetShareTxnBackgroundExec(ctx, false)
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

func showDatabases(ctx context.Context, sid string, bh BackgroundExec, snapshotName string) ([]string, error) {
	getLogger(sid).Info(fmt.Sprintf("[%s] start to get all database ", snapshotName))
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

func showFullTables(ctx context.Context, sid string, bh BackgroundExec, snapshotName string, dbName string, tblName string) ([]*tableInfo, error) {
	sql := fmt.Sprintf("show full tables from `%s`", dbName)
	if len(tblName) > 0 {
		sql += fmt.Sprintf(" like '%s'", tblName)
	}
	if len(snapshotName) > 0 {
		sql += fmt.Sprintf(" {snapshot = '%s'}", snapshotName)
	}
	getLogger(sid).Info(fmt.Sprintf("[%s] show full table `%s.%s` sql: %s", snapshotName, dbName, tblName, sql))
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
	getLogger(sid).Info(fmt.Sprintf("[%s] show full table `%s.%s`, get table number `%d`", snapshotName, dbName, tblName, len(ans)))
	return ans, nil
}

func getTableInfos(ctx context.Context, sid string, bh BackgroundExec, snapshotName string, dbName string, tblName string) ([]*tableInfo, error) {
	getLogger(sid).Info(fmt.Sprintf("[%s] start to get table info: datatabse `%s`, table `%s`", snapshotName, dbName, tblName))
	tableInfos, err := showFullTables(ctx, sid, bh, snapshotName, dbName, tblName)
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

func getCreateDatabaseSql(ctx context.Context,
	sid string,
	bh BackgroundExec,
	snapshotName string,
	dbName string,
	accountId uint32) (string, error) {

	sql := "select datname, dat_createsql from mo_catalog.mo_database"
	if len(snapshotName) > 0 {
		sql += fmt.Sprintf(" {snapshot = '%s'}", snapshotName)
	}
	sql += fmt.Sprintf(" where datname = '%s' and account_id = %d", dbName, accountId)
	getLogger(sid).Info(fmt.Sprintf("[%s] get create database `%s` sql: %s", snapshotName, dbName, sql))

	// cols: database_name, create_sql
	colsList, err := getStringColsList(ctx, bh, sql, 0, 1)
	if err != nil {
		return "", err
	}
	if len(colsList) == 0 || len(colsList[0]) == 0 {
		return "", moerr.NewBadDB(ctx, dbName)
	}
	return colsList[0][1], nil
}

func getTableInfo(ctx context.Context, sid string, bh BackgroundExec, snapshotName string, dbName, tblName string) (*tableInfo, error) {
	tableInfos, err := getTableInfos(ctx, sid, bh, snapshotName, dbName, tblName)
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
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)
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
	sid string,
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
			continue
		}
		if dbName != "" && dbName != d {
			continue
		}
		if tblName != "" && tblName != t {
			continue
		}

		if tblInfoMap[key], err = getTableInfo(ctx, sid, bh, snapshotName, d, t); err != nil {
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

	g := toposort{next: make(map[string][]string)}
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

func restoreToCluster(ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	snapshotName string,
	snapshotTs int64,
	subDbToRestore map[string]*subDbRestoreRecord,
) (err error) {
	getLogger(ses.GetService()).Info(fmt.Sprintf("[%s] start to restore cluster, restore timestamp: %d", snapshotName, snapshotTs))

	// get restore accounts
	var accounts []accountRecord
	accounts, err = getRestoreAccounts(ctx, ses.GetService(), bh, snapshotName, snapshotTs)
	if err != nil {
		return err
	}

	// restore to each account
	for _, account := range accounts {
		getLogger(ses.GetService()).Info(fmt.Sprintf("[%s] cluster restore start to restore account: %v, account id: %d", snapshotName, account.accountName, account.accountId))

		if err = restoreAccountUsingClusterSnapshot(ctx, ses, bh, snapshotName, snapshotTs, account, subDbToRestore); err != nil {
			return err
		}

		getLogger(ses.GetService()).Info(fmt.Sprintf("[%s] restore account: %v, account id: %d success", snapshotName, account.accountName, account.accountId))
	}

	return err

}

func restoreAccountUsingClusterSnapshot(ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	snapshotName string,
	snapshotTs int64,
	account accountRecord,
	subDbToRestore map[string]*subDbRestoreRecord,
) (err error) {
	toAccountId := account.accountId

	newSnapshot, err := insertSnapshotRecord(ctx, ses.GetService(), bh, snapshotName, snapshotTs, toAccountId, account.accountName)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			deleteSnapshotRecord(ctx, ses.GetService(), bh, snapshotName, newSnapshot)
		}
	}()

	// pre restore account
	// drop foreign key related tables first
	if err = deleteCurFkTables(ctx, ses.GetService(), bh, "", "", uint32(toAccountId)); err != nil {
		return err
	}
	// get topo sorted tables with foreign key
	var sortedFkTbls []string
	var fkTableMap map[string]*tableInfo
	sortedFkTbls, err = fkTablesTopoSort(ctx, bh, newSnapshot, "", "")
	if err != nil {
		return err
	}
	// get foreign key table infos
	fkTableMap, err = getTableInfoMap(ctx, ses.GetService(), bh, newSnapshot, "", "", sortedFkTbls)
	if err != nil {
		return err
	}

	// collect views and tables during table restoration
	viewMap := make(map[string]*tableInfo)

	// restore to account
	if err = restoreToAccount(ctx,
		ses.GetService(),
		bh,
		newSnapshot,
		uint32(toAccountId),
		fkTableMap,
		viewMap,
		snapshotTs,
		uint32(toAccountId),
		true,
		subDbToRestore); err != nil {
		return err
	}

	if len(fkTableMap) > 0 {
		if err = restoreTablesWithFk(ctx, ses.GetService(), bh, newSnapshot, sortedFkTbls, fkTableMap, uint32(toAccountId), snapshotTs); err != nil {
			return err
		}
	}

	if len(viewMap) > 0 {
		if err = restoreViews(ctx, ses, bh, newSnapshot, viewMap, uint32(toAccountId)); err != nil {
			return err
		}
	}

	deleteSnapshotRecord(ctx, ses.GetService(), bh, snapshotName, newSnapshot)

	// checks if the given context has been canceled.
	if err = CancelCheck(ctx); err != nil {
		return err
	}
	return
}

func getRestoreAccounts(ctx context.Context,
	sid string,
	bh BackgroundExec,
	snapshotName string,
	snapshotTs int64,
) (accounts []accountRecord, err error) {
	getLogger(sid).Info(fmt.Sprintf("[%s] start to get restore accounts", snapshotName))
	var erArray []ExecResult

	sql := fmt.Sprintf(getRestoreAccountsFmt, snapshotTs)
	getLogger(sid).Info(fmt.Sprintf("[%s] get restore accounts sql: %s", snapshotName, sql))
	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return nil, err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}

	if execResultArrayHasData(erArray) {
		for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
			var account accountRecord
			if account.accountName, err = erArray[0].GetString(ctx, i, 0); err != nil {
				return nil, err
			}
			if account.accountId, err = erArray[0].GetUint64(ctx, i, 1); err != nil {
				return nil, err
			}
			accounts = append(accounts, account)
			getLogger(sid).Info(fmt.Sprintf("[%s] get account: %v, account id: %d", snapshotName, account.accountName, account.accountId))

		}
	}
	return
}

func insertSnapshotRecord(ctx context.Context, sid string, bh BackgroundExec, spName string, spTs int64, toAccountId uint64, accountName string) (snapshotName string, err error) {
	// mock snapshot id and snapshot name
	snapshotUId, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	snapshotId := snapshotUId.String()

	snapshotName = snapshotId + "_" + spName + "_mock"
	var sql string
	sql, err = getSqlForCreateSnapshot(ctx,
		snapshotId,
		snapshotName,
		spTs,
		tree.SNAPSHOTLEVELACCOUNT.String(),
		accountName,
		"",
		"",
		toAccountId)
	if err != nil {
		return "", err
	}
	getLogger(sid).Info(fmt.Sprintf("[%s] mock insert snapshot record sql: %s", spName, sql))
	if err = bh.Exec(ctx, sql); err != nil {
		return "", err
	}
	return
}

func deleteSnapshotRecord(ctx context.Context, sid string, bh BackgroundExec, spName string, snapshotName string) (err error) {
	sql := getSqlForDropSnapshot(snapshotName)
	getLogger(sid).Info(fmt.Sprintf("[%s] mock delete snapshot record sql: %s", spName, sql))
	if err = bh.Exec(ctx, sql); err != nil {
		return err
	}
	return
}

func getSnapshotPlanWithSharedBh(ctx context.Context, bh BackgroundExec, snapshotName string) (snapshot *pbplan.Snapshot, err error) {
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

// dropDb delete related pubs before drops the database
func dropDb(ctx context.Context, bh BackgroundExec, dbName string) (err error) {
	// drop pub first
	pubInfos, err := getPubInfosByDbname(ctx, bh, dbName)
	if err != nil {
		return err
	}
	for _, pubInfo := range pubInfos {
		if err = dropPublication(ctx, bh, true, pubInfo.PubName); err != nil {
			return
		}
	}

	// drop db
	return bh.Exec(ctx, fmt.Sprintf("drop database if exists %s", dbName))
}

// createPub create pub after the database is created
func createPub(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	snapshotName,
	dbName string,
	toAccountId uint32) (err error) {
	// read pub info from mo_pubs
	sql := fmt.Sprintf(getPubInfoWithSnapshotFormat, snapshotName, dbName)
	bh.ClearExecResultSet()
	getLogger(sid).Info(fmt.Sprintf("[%s] create pub: get pub info sql: %s", snapshotName, sql))

	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	pubInfos, err := extractPubInfosFromExecResult(ctx, erArray)
	if err != nil {
		return err
	}

	// restore pub to toAccount
	var ast []tree.Statement
	defer func() {
		for _, s := range ast {
			s.Free()
		}
	}()

	for _, pubInfo := range pubInfos {
		toCtx := defines.AttachAccount(ctx, toAccountId, pubInfo.Owner, pubInfo.Creator)
		if ast, err = mysql.Parse(toCtx, pubInfo.GetCreateSql(), 1); err != nil {
			return
		}
		getLogger(sid).Info(fmt.Sprintf("[%s] create pub: create pub sql: %s", snapshotName, pubInfo.GetCreateSql()))

		if err = createPublication(toCtx, bh, ast[0].(*tree.CreatePublication)); err != nil {
			return
		}
	}
	return
}

func checkPubExistOrNot(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	snapshotName string,
	subName string,
	timsStampTs int64,
) (bool, error) {
	getLogger(sid).Info(fmt.Sprintf("[%s] start to check pub exist or not", snapshotName))
	subInfos, err := getSubInfosFromSubWithSnapshot(
		ctx,
		sid,
		bh,
		snapshotName,
		subName,
		timsStampTs)
	if err != nil {
		return false, err
	} else if len(subInfos) == 0 {
		return false, moerr.NewInternalError(ctx, "there is no subscription for database %s", subName)
	}

	subInfo := subInfos[0]
	var isPubValid bool
	isPubValid, err = checkSubscriptionExist(
		ctx,
		sid,
		bh,
		subInfo.PubAccountName,
		subInfo.PubName)
	if err != nil {
		return false, err
	} else if !isPubValid {
		return false, moerr.NewInternalError(ctx, "there is no publication %s", subInfo.PubName)
	}
	return true, nil
}

func getSubInfosFromSubWithSnapshot(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	snapshotName string,
	subName string,
	timestampTs int64,
) (subInfo []*pubsub.SubInfo, err error) {
	getLogger(sid).Info(fmt.Sprintf("[%s] start to get sub info from sub with snapshot", snapshotName))
	subAccountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return nil, err
	}
	ctx = defines.AttachAccountId(ctx, catalog.System_Account)

	var getSubSqlString string
	if timestampTs > 0 {
		getSubSqlString = fmt.Sprintf(fmt.Sprintf(getSubsSqlFmt, "{MO_TS = %d}"), timestampTs)
	} else {
		getSubSqlString = fmt.Sprintf(getSubsSqlFmt, "")
	}

	sql := getSubSqlString + fmt.Sprintf(" and sub_account_id = %d", subAccountId)
	if len(subName) > 0 {
		sql += fmt.Sprintf(" and sub_name = '%s'", subName)
	}

	getLogger(sid).Info(fmt.Sprintf("[%s] get sub info from sub with snapshot sql: %s", snapshotName, sql))
	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	return extractSubInfosFromExecResult(ctx, erArray)
}

func checkSubscriptionExist(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	pubAccountName,
	pubName string) (isPubValid bool, err error) {

	var (
		sql     string
		erArray []ExecResult
		accId   int64
	)

	newCtx := defines.AttachAccountId(ctx, catalog.System_Account)
	//get pubAccountId from publication info
	if sql, err = getSqlForAccountIdAndStatus(newCtx, pubAccountName, true); err != nil {
		return
	}

	getLogger(sid).Info(fmt.Sprintf("[%s] check subscription exist or not: get account id sql: %s", pubName, sql))
	bh.ClearExecResultSet()
	if err = bh.Exec(newCtx, sql); err != nil {
		return
	}

	if erArray, err = getResultSet(newCtx, bh); err != nil {
		return
	}

	if !execResultArrayHasData(erArray) {
		err = moerr.NewInternalError(newCtx, "there is no publication account %s", pubAccountName)
		return
	}
	if accId, err = erArray[0].GetInt64(newCtx, 0, 0); err != nil {
		return
	}

	//check the publication is already exist or not
	newCtx = defines.AttachAccountId(ctx, uint32(accId))
	pubInfo, err := getPubInfo(newCtx, bh, pubName)
	if err != nil {
		return
	}
	if pubInfo == nil {
		err = moerr.NewInternalError(newCtx, "there is no publication %s", pubName)
		return
	}

	return true, nil
}
