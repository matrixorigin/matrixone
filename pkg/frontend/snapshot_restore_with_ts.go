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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
)

func fkTablesTopoSortWithDropped(ctx context.Context, bh BackgroundExec, dbName string, tblName string, ts int64, from, to uint32) (sortedTbls []string, err error) {
	newCtx := defines.AttachAccountId(ctx, from)
	getLogger("").Info(fmt.Sprintf("[%d:%d] start to get fk tables topo sort from account %d", from, ts, from))
	// get foreign key deps from mo_catalog.mo_foreign_keys
	fkDeps, err := getFkDepsWithDropped(newCtx, bh, dbName, tblName, ts, from, to)
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

func getFkDepsWithDropped(ctx context.Context, bh BackgroundExec, db string, tbl string, ts int64, from, to uint32) (ans map[string][]string, err error) {
	sql := "select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys"
	if ts > 0 {
		sql += fmt.Sprintf(" {MO_TS = %d}", ts)
	}

	if len(db) > 0 {
		sql += fmt.Sprintf(" where db_name = '%s'", db)
		if len(tbl) > 0 {
			sql += fmt.Sprintf(" and table_name = '%s'", tbl)
		}
	}

	getLogger("").Info(fmt.Sprintf("[%d:%d] get fk deps sql: %s", from, ts, sql))

	bh.ClearExecResultSet()
	if err = bh.ExecRestore(ctx, sql, from, to); err != nil {
		return
	}
	resultSet, err := getResultSet(ctx, bh)
	if err != nil {
		return
	}

	ans = make(map[string][]string)
	var dbName, tblName string
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

func getTableInfoMapFromDropped(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	dbName string,
	tblName string,
	tblKeys []string,
	ts int64,
	from,
	to uint32) (tblInfoMap map[string]*tableInfo, err error) {
	getLogger(sid).Info(fmt.Sprintf("[%d:%d] start to get table info map from dropped account %d", from, ts, from))
	newCtx := defines.AttachAccountId(ctx, from)

	tblInfoMap = make(map[string]*tableInfo)

	for _, key := range tblKeys {
		if _, ok := tblInfoMap[key]; ok {
			return
		}

		// filter by dbName and tblName
		d, t := splitKey(key)
		if needSkipDb(d) || needSkipTable(from, d, t) {
			continue
		}
		if dbName != "" && dbName != d {
			continue
		}
		if tblName != "" && tblName != t {
			continue
		}

		if tblInfoMap[key], err = getTableInfoFromDropped(newCtx, sid, bh, d, t, ts, from, to); err != nil {
			return
		}
	}
	return
}

func getTableInfoFromDropped(ctx context.Context,
	sid string,
	bh BackgroundExec,
	dbName,
	tblName string,
	ts int64,
	from,
	to uint32) (*tableInfo, error) {
	getLogger(sid).Info(fmt.Sprintf("[%d:%d]start to get table info: datatabse `%s`, table `%s`", from, ts, dbName, tblName))
	tableInfos, err := getTableInfosFromDropped(ctx, sid, bh, dbName, tblName, ts, from, to)
	if err != nil {
		return nil, err
	}

	// if table doesn't exist, return nil
	if len(tableInfos) == 0 {
		return nil, nil
	}
	return tableInfos[0], nil
}

func getTableInfosFromDropped(ctx context.Context,
	sid string,
	bh BackgroundExec,
	dbName string,
	tblName string,
	ts int64,
	from,
	to uint32) ([]*tableInfo, error) {
	newCtx := defines.AttachAccountId(ctx, from)
	getLogger(sid).Info(fmt.Sprintf("[%d:%d] start to get table info: datatabse `%s`, table `%s`", from, ts, dbName, tblName))
	tableInfos, err := showFullTablesFromDropped(newCtx, sid, bh, dbName, tblName, ts, from, to)
	if err != nil {
		return nil, err
	}

	// only recreate snapshoted table need create sql
	if ts > 0 {
		for _, tblInfo := range tableInfos {
			if tblInfo.createSql, err = getCreateTableSqlFromDropped(newCtx, bh, dbName, tblInfo.tblName, ts, from, to); err != nil {
				return nil, err
			}
		}
	}
	return tableInfos, nil
}

func showFullTablesFromDropped(ctx context.Context,
	sid string,
	bh BackgroundExec,
	dbName string,
	tblName string,
	ts int64,
	from,
	to uint32) ([]*tableInfo, error) {
	getLogger(sid).Info(fmt.Sprintf("[%d:%d] start to show full table `%s.%s`", from, ts, dbName, tblName))
	newCtx := defines.AttachAccountId(ctx, from)
	sql := fmt.Sprintf("show full tables from `%s`", dbName)

	if len(tblName) > 0 {
		sql += fmt.Sprintf(" like '%s'", tblName)
	}

	if ts > 0 {
		sql += fmt.Sprintf(" {MO_TS = %d}", ts)
	}

	getLogger(sid).Info(fmt.Sprintf("[%d:%d] show full table `%s.%s` sql: %s", from, ts, dbName, tblName, sql))
	// cols: table name, table type
	colsList, err := getStringColsListFromDropped(newCtx, bh, sql, from, to, 0, 1)
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
	getLogger(sid).Info(fmt.Sprintf("[%d:%d] show full table `%s.%s`, get table number `%d`", from, ts, dbName, tblName, len(ans)))
	return ans, nil
}

func getStringColsListFromDropped(ctx context.Context, bh BackgroundExec, sql string, from, to uint32, colIndices ...uint64) (ans [][]string, err error) {
	bh.ClearExecResultSet()
	if err = bh.ExecRestore(ctx, sql, from, to); err != nil {
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

func getCreateTableSqlFromDropped(ctx context.Context, bh BackgroundExec, dbName string, tblName string, ts int64, from, to uint32) (string, error) {
	getLogger("").Info(fmt.Sprintf("[%d:%d] start to get create table sql: datatabse `%s`, table `%s`", from, ts, dbName, tblName))
	newCtx := defines.AttachAccountId(ctx, from)
	sql := fmt.Sprintf("show create table `%s`.`%s`", dbName, tblName)
	if ts > 0 {
		sql += fmt.Sprintf(" {MO_TS = %d}", ts)
	}

	// cols: table_name, create_sql
	colsList, err := getStringColsListFromDropped(newCtx, bh, sql, from, to, 1)
	if err != nil {
		return "", err
	}
	if len(colsList) == 0 || len(colsList[0]) == 0 {
		return "", moerr.NewNoSuchTable(ctx, dbName, tblName)
	}
	return colsList[0][0], nil
}

func restoreToAccountFromDropped(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	snapshotTs int64,
	restoreAccount uint32,
	toAccountId uint32,
	fkTableMap map[string]*tableInfo,
	viewMap map[string]*tableInfo,
	isRestoreCluster bool,
	subDbToRestore map[string]*subDbRestoreRecord,
) (err error) {
	getLogger(sid).Info(fmt.Sprintf("[%d:%d] start to restore account: %v, restore timestamp : %d", restoreAccount, snapshotTs, restoreAccount, snapshotTs))

	var dbNames []string
	toCtx := defines.AttachAccountId(ctx, toAccountId)

	// delete current dbs from to account
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
			continue
		}

		getLogger(sid).Info(fmt.Sprintf("[%d:%d]drop current exists db: %v", restoreAccount, snapshotTs, dbName))
		if err = dropDb(toCtx, bh, dbName); err != nil {
			return
		}
	}

	// restore dbs
	if dbNames, err = showDatabasesFromDropped(ctx, sid, bh, snapshotTs, restoreAccount, toAccountId); err != nil {
		return
	}

	for _, dbName := range dbNames {
		if needSkipDb(dbName) {
			getLogger(sid).Info(fmt.Sprintf("[%d:%d] skip restore db: %v", restoreAccount, snapshotTs, dbName))
			continue
		}
		if err = restoreDatabaseFromDropped(ctx,
			sid,
			bh,
			dbName,
			snapshotTs,
			restoreAccount,
			toAccountId,
			fkTableMap,
			viewMap,
			isRestoreCluster,
			subDbToRestore); err != nil {
			return
		}
	}

	// restore system db
	if err = restoreSystemDatabaseFromDropped(ctx,
		sid,
		bh,
		snapshotTs,
		restoreAccount,
		toAccountId,
	); err != nil {
		return
	}
	return
}

func showDatabasesFromDropped(ctx context.Context, sid string, bh BackgroundExec, ts int64, from, to uint32) ([]string, error) {
	getLogger(sid).Info(fmt.Sprintf("[%d:%d] start to get all database ", from, ts))
	newCtx := defines.AttachAccountId(ctx, from)
	sql := "show databases"
	if ts > 0 {
		sql += fmt.Sprintf(" {MO_TS = %d}", ts)
	}

	// cols: dbname
	colsList, err := getStringColsListFromDropped(newCtx, bh, sql, from, to, 0)
	if err != nil {
		return nil, err
	}

	dbNames := make([]string, len(colsList))
	for i, cols := range colsList {
		dbNames[i] = cols[0]
	}
	return dbNames, nil
}

func restoreDatabaseFromDropped(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	dbName string,
	snapshotTs int64,
	restoreAccount uint32,
	toAccountId uint32,
	fkTableMap map[string]*tableInfo,
	viewMap map[string]*tableInfo,
	isRestoreCluster bool,
	subDbToRestore map[string]*subDbRestoreRecord,
) (err error) {

	var createDbSql string
	var isSubDb bool
	createDbSql, err = getCreateDatabaseSqlFromDropped(ctx, sid, bh, dbName, snapshotTs, restoreAccount, toAccountId)
	if err != nil {
		return
	}

	toCtx := defines.AttachAccountId(ctx, toAccountId)
	// if restore to table, check if the db is sub db
	isSubDb, err = checkDbWhetherSub(ctx, createDbSql)
	if err != nil {
		return
	}

	if isRestoreCluster && isSubDb {
		// if restore to cluster, and the db is sub, append the sub db to restore list
		getLogger(sid).Info(fmt.Sprintf("[%d:%d] append sub db to restore list: %v, at restore cluster account %d", restoreAccount, snapshotTs, dbName, toAccountId))
		key := genKey(fmt.Sprint(restoreAccount), dbName)
		subDbToRestore[key] = NewSubDbRestoreRecord(dbName, restoreAccount, createDbSql, snapshotTs)
		return
	}

	// if current account is not to account id, and the db is sub db, skip restore
	if restoreAccount != toAccountId && isSubDb {
		getLogger(sid).Info(fmt.Sprintf("[%d:%d] skip restore subscription db: %v, current account is not to account", restoreAccount, snapshotTs, dbName))
		return
	}

	var isMasterDb bool
	isMasterDb, err = checkDatabaseIsMaster(toCtx, sid, bh, fmt.Sprintf("%d:%d", restoreAccount, snapshotTs), dbName)
	if err != nil {
		return err
	}
	if isMasterDb {
		getLogger(sid).Info(fmt.Sprintf("[%d:%d] skip restore master db: %v, which has been referenced by foreign keys", restoreAccount, snapshotTs, dbName))
		return
	}

	// drop db
	getLogger(sid).Info(fmt.Sprintf("[%d:%d] start to drop database: %v", restoreAccount, snapshotTs, dbName))
	if err = dropDb(toCtx, bh, dbName); err != nil {
		return
	}

	if isSubDb {
		var isPubExist bool
		isPubExist, _ = checkPubExistOrNot(toCtx, sid, bh, "", dbName, snapshotTs)
		if !isPubExist {
			getLogger(sid).Info(fmt.Sprintf("[%d:%d] skip restore db: %v, no publication", restoreAccount, snapshotTs, dbName))
			return
		}

		// create db with publication
		getLogger(sid).Info(fmt.Sprintf("[%d:%d] start to create db with pub: %v, create db sql: %s", restoreAccount, snapshotTs, dbName, createDbSql))
		if err = bh.Exec(toCtx, createDbSql); err != nil {
			return
		}

		return
	} else {
		createDbSql = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName)
		// create db
		getLogger(sid).Info(fmt.Sprintf("[%d:%d] start to create db: %v, create db sql: %s", restoreAccount, snapshotTs, dbName, createDbSql))
		if err = bh.Exec(toCtx, createDbSql); err != nil {
			return
		}
	}

	tableInfos, err := getTableInfosFromDropped(ctx, sid, bh, dbName, "", snapshotTs, restoreAccount, toAccountId)
	if err != nil {
		return
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

		if err = recreateTableFromDropped(ctx,
			sid,
			bh,
			tblInfo,
			snapshotTs,
			restoreAccount,
			toAccountId); err != nil {
			return
		}
	}
	return
}

func getCreateDatabaseSqlFromDropped(ctx context.Context,
	sid string,
	bh BackgroundExec,
	dbName string,
	ts int64,
	from,
	to uint32) (string, error) {

	newCtx := defines.AttachAccountId(ctx, from)
	sql := "select datname, dat_createsql from mo_catalog.mo_database"
	if ts > 0 {
		sql += fmt.Sprintf(" {MO_TS = %d }", ts)
	}
	sql += fmt.Sprintf(" where datname = '%s' and account_id = %d", dbName, from)
	getLogger(sid).Info(fmt.Sprintf("[%d:%d] get create database `%s` sql: %s", from, ts, dbName, sql))

	// cols: database_name, create_sql
	colsList, err := getStringColsListFromDropped(newCtx, bh, sql, from, to, 0, 1)
	if err != nil {
		return "", err
	}
	if len(colsList) == 0 || len(colsList[0]) == 0 {
		return "", moerr.NewBadDB(newCtx, dbName)
	}
	return colsList[0][1], nil
}

func recreateTableFromDropped(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	tblInfo *tableInfo,
	snapshotTs int64,
	restoreAccount uint32,
	toAccountId uint32,
) (err error) {
	getLogger(sid).Info(fmt.Sprintf("[%d:%d] start to restore table: %v, restore timestamp: %d", restoreAccount, snapshotTs, tblInfo.tblName, snapshotTs))

	ctx = defines.AttachAccountId(ctx, toAccountId)
	if err = bh.Exec(ctx, fmt.Sprintf("use `%s`", tblInfo.dbName)); err != nil {
		return
	}

	getLogger(sid).Info(fmt.Sprintf("[%d:%d] start to drop table: %v,", restoreAccount, snapshotTs, tblInfo.tblName))
	if err = bh.Exec(ctx, fmt.Sprintf("drop table if exists `%s`", tblInfo.tblName)); err != nil {
		return
	}

	// create table
	getLogger(sid).Info(fmt.Sprintf("[%d:%d] start to create table: %v, create table sql: %s", restoreAccount, snapshotTs, tblInfo.tblName, tblInfo.createSql))
	if err = bh.Exec(ctx, tblInfo.createSql); err != nil {
		return
	}

	insertIntoSql := fmt.Sprintf(restoreTableDataByTsFmt, tblInfo.dbName, tblInfo.tblName, tblInfo.dbName, tblInfo.tblName, snapshotTs)
	beginTime := time.Now()
	getLogger(sid).Info(fmt.Sprintf("[%d:%d] start to insert select table: %v, insert sql: %s", restoreAccount, snapshotTs, tblInfo.tblName, insertIntoSql))
	if err = bh.ExecRestore(ctx, insertIntoSql, restoreAccount, toAccountId); err != nil {
		return
	}
	getLogger(sid).Info(fmt.Sprintf("[%d:%d] insert select table: %v, cost: %v", restoreAccount, snapshotTs, tblInfo.tblName, time.Since(beginTime)))
	return
}

func restoreSystemDatabaseFromDropped(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	snapshotTs int64,
	restoreAccount uint32,
	toAccountId uint32,

) (err error) {
	getLogger(sid).Info(fmt.Sprintf("[%d:%d] start to restore system database: %s", restoreAccount, snapshotTs, moCatalog))
	var (
		dbName     = moCatalog
		tableInfos []*tableInfo
	)

	tableInfos, err = showFullTablesFromDropped(ctx, sid, bh, dbName, "", snapshotTs, restoreAccount, toAccountId)
	if err != nil {
		return err
	}

	for _, tblInfo := range tableInfos {
		if needSkipSystemTable(toAccountId, tblInfo) {
			// TODO skip tables which should not to be restored
			getLogger(sid).Info(fmt.Sprintf("[%d:%d] skip restore system table: %v.%v, table type: %v", restoreAccount, snapshotTs, moCatalog, tblInfo.tblName, tblInfo.typ))
			continue
		}

		getLogger(sid).Info(fmt.Sprintf("[%d:%d] start to restore system table: %v.%v", restoreAccount, snapshotTs, moCatalog, tblInfo.tblName))
		tblInfo.createSql, err = getCreateTableSqlFromDropped(ctx, bh, dbName, tblInfo.tblName, snapshotTs, restoreAccount, toAccountId)
		if err != nil {
			return err
		}

		// checks if the given context has been canceled.
		if err = CancelCheck(ctx); err != nil {
			return
		}

		if err = recreateTableFromDropped(ctx, sid, bh, tblInfo, snapshotTs, restoreAccount, toAccountId); err != nil {
			return
		}
	}
	return
}

func restoreTablesWithFkFromDropped(
	ctx context.Context,
	sid string,
	bh BackgroundExec,
	snapshotTs int64,
	restoreAccount uint32,
	toAccountId uint32,
	sortedFkTbls []string,
	fkTableMap map[string]*tableInfo,
) (err error) {
	getLogger(sid).Info(fmt.Sprintf("[%d:%d] start to drop fk related tables", restoreAccount, snapshotTs))

	// recreate tables as topo order
	for _, key := range sortedFkTbls {
		// if tblInfo is nil, means that table is not in this restoration task, ignore
		// e.g. t1.pk <- t2.fk, we only want to restore t2, fkTableMap[t1.key] is nil, ignore t1
		if tblInfo := fkTableMap[key]; tblInfo != nil {
			getLogger(sid).Info(fmt.Sprintf("[%d:%d] start to restore table with fk: %v, restore timestamp: %d", restoreAccount, snapshotTs, tblInfo.tblName, snapshotTs))
			err = recreateTableFromDropped(ctx, sid, bh, tblInfo, snapshotTs, restoreAccount, toAccountId)
			if err != nil {
				return
			}
		}
	}
	return
}

func restoreViewsFromDropped(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	snapshotTs int64,
	restoreAccount uint32,
	toAccountId uint32,
	viewMap map[string]*tableInfo,
	restoreAccountName string) error {
	getLogger(ses.GetService()).Info("start to restore views")
	var (
		err         error
		snapshot    *plan.Snapshot
		stmts       []tree.Statement
		sortedViews []string
		oldSnapshot *plan.Snapshot
	)
	snapshot = &pbplan.Snapshot{
		TS: &timestamp.Timestamp{PhysicalTime: snapshotTs},
		Tenant: &pbplan.SnapshotTenant{
			TenantName: restoreAccountName,
			TenantID:   restoreAccount,
		},
	}

	compCtx := ses.GetTxnCompileCtx()
	oldSnapshot = compCtx.GetSnapshot()
	compCtx.SetSnapshot(snapshot)
	defer func() {
		compCtx.SetSnapshot(oldSnapshot)
	}()

	g := toposort{next: make(map[string][]string)}
	for key, viewEntry := range viewMap {
		getLogger(ses.GetService()).Info(fmt.Sprintf("[%d:%d] start to restore view: %v", restoreAccount, snapshotTs, viewEntry.tblName))
		stmts, err = parsers.Parse(ctx, dialect.MYSQL, viewEntry.createSql, 1)
		if err != nil {
			return err
		}

		compCtx.SetDatabase(viewEntry.dbName)
		// build create sql to find dependent views
		_, err = plan.BuildPlan(compCtx, stmts[0], false)
		if err != nil {
			getLogger(ses.GetService()).Info(fmt.Sprintf("try to build view %v failed, try to build it again", viewEntry.tblName))
			stmts, _ = parsers.Parse(ctx, dialect.MYSQL, viewEntry.createSql, 0)
			_, err = plan.BuildPlan(compCtx, stmts[0], false)
			if err != nil {
				return err
			}
		}

		g.addVertex(key)
		for _, depView := range compCtx.GetViews() {
			g.addEdge(depView, key)
		}
	}

	// toposort
	sortedViews, err = g.sort()
	if err != nil {
		return err
	}

	// create views
	toCtx := defines.AttachAccountId(ctx, toAccountId)
	for _, key := range sortedViews {
		// if not ok, means that view is not in this restoration task, ignore
		if tblInfo, ok := viewMap[key]; ok {
			getLogger(ses.GetService()).Info(fmt.Sprintf("[%d:%d] start to restore view: %v, restore timestamp: %d", restoreAccount, snapshotTs, tblInfo.tblName, snapshot.TS.PhysicalTime))

			if err = bh.Exec(toCtx, "use `"+tblInfo.dbName+"`"); err != nil {
				return err
			}

			if err = bh.Exec(toCtx, "drop view if exists "+tblInfo.tblName); err != nil {
				return err
			}

			getLogger(ses.GetService()).Info(fmt.Sprintf("[%d:%d] start to create view: %v, create view sql: %s", restoreAccount, snapshotTs, tblInfo.tblName, tblInfo.createSql))
			if err = bh.Exec(toCtx, tblInfo.createSql); err != nil {
				return err
			}
			getLogger(ses.GetService()).Info(fmt.Sprintf("[%d:%d] restore view: %v success", restoreAccount, snapshotTs, tblInfo.tblName))
		}
	}
	return nil
}
