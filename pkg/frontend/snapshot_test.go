// Copyright 2021 - 2024 Matrix Origin
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
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	sqlmongodb "github.com/matrixorigin/matrixone/pkg/sql/mongodb"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func TestGetFkDepsFromTableInfos(t *testing.T) {
	tableInfos := []*tableInfo{
		{
			dbName:    "d",
			tblName:   "parent",
			typ:       "BASE TABLE",
			createSql: "create table `d`.`parent` (`id` int primary key)",
		},
		{
			dbName:  "d",
			tblName: "child",
			typ:     "BASE TABLE",
			createSql: "create table `d`.`child` (" +
				"`id` int primary key, `parent_id` int, " +
				"constraint `fk_child` foreign key (`parent_id`) " +
				"references `d`.`parent` (`id`))",
		},
		{
			dbName:  "d",
			tblName: "self_ref",
			typ:     "BASE TABLE",
			createSql: "create table `d`.`self_ref` (" +
				"`id` int primary key, `parent_id` int, " +
				"constraint `fk_self` foreign key (`parent_id`) " +
				"references `self_ref` (`id`))",
		},
		{
			dbName:    "d",
			tblName:   "v",
			typ:       view,
			createSql: "create view `d`.`v` as select 1",
		},
	}

	deps, err := getFkDepsFromTableInfos(context.Background(), tableInfos)
	require.NoError(t, err)
	require.Equal(t, []string{genKey("d", "parent")}, deps[genKey("d", "child")])
	require.Equal(t, []string{genKey("d", "self_ref")}, deps[genKey("d", "self_ref")])
	require.NotContains(t, deps, genKey("d", "v"))
}

func TestMongoDBMappingsFollowExternalTableRestoreSkipPolicy(t *testing.T) {
	info := &tableInfo{dbName: moCatalog, tblName: sqlmongodb.TableMappings, typ: "BASE TABLE"}
	for _, accountID := range []uint32{sysAccountID, 7} {
		require.True(t, needSkipTable(accountID, moCatalog, sqlmongodb.TableMappings))
		require.True(t, needSkipSystemTable(accountID, info))
	}
	require.Equal(t, systemCatalogRestoreSkip, systemCatalogRestorePolicies[sqlmongodb.TableMappings])
}

func TestMergeFkDepsDeduplicatesSources(t *testing.T) {
	child := genKey("d", "child")
	parent := genKey("d", "parent")
	otherParent := genKey("d", "other_parent")
	dst := map[string][]string{child: {parent}}
	src := map[string][]string{child: {parent, otherParent}}

	mergeFkDeps(dst, src)

	require.Equal(t, []string{parent, otherParent}, dst[child])
}

func TestFkTablesTopoSortUsesSchemaWhenCatalogRowsAreMissing(t *testing.T) {
	const dbName = "legacy"
	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result["select db_name, table_name, refer_db_name, refer_table_name "+
		"from mo_catalog.mo_foreign_keys where db_name = 'legacy'"] = newMrsForPitrRecord(nil)

	tableInfos := []*tableInfo{
		{
			dbName:    dbName,
			tblName:   "parent",
			typ:       "BASE TABLE",
			createSql: "create table `legacy`.`parent` (`id` int primary key)",
		},
		{
			dbName:  dbName,
			tblName: "child",
			typ:     "BASE TABLE",
			createSql: "create table `legacy`.`child` (" +
				"`id` int primary key, `parent_id` int, " +
				"constraint `fk_legacy` foreign key (`parent_id`) " +
				"references `legacy`.`parent` (`id`))",
		},
	}

	sorted, err := fkTablesTopoSort(
		context.Background(),
		bh,
		nil,
		dbName,
		"",
		tableInfos,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		[]string{genKey(dbName, "parent"), genKey(dbName, "child")},
		sorted,
	)
}

func TestHistoricalRestoreTopoSortUsesSchemaWhenCatalogRowsAreMissing(t *testing.T) {
	const (
		dbName = "legacy"
		ts     = int64(100)
	)
	tableInfos := []*tableInfo{
		{
			dbName:    dbName,
			tblName:   "parent",
			typ:       "BASE TABLE",
			createSql: "create table `legacy`.`parent` (`id` int primary key)",
		},
		{
			dbName:  dbName,
			tblName: "child",
			typ:     "BASE TABLE",
			createSql: "create table `legacy`.`child` (" +
				"`id` int primary key, `parent_id` int, " +
				"constraint `fk_legacy` foreign key (`parent_id`) " +
				"references `legacy`.`parent` (`id`))",
		},
	}
	want := []string{genKey(dbName, "parent"), genKey(dbName, "child")}
	catalogSQL := fmt.Sprintf(
		"select db_name, table_name, refer_db_name, refer_table_name "+
			"from mo_catalog.mo_foreign_keys {MO_TS = %d} where db_name = '%s'",
		ts,
		dbName,
	)

	t.Run("pitr", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[catalogSQL] = newMrsForPitrRecord(nil)

		sorted, err := fkTablesTopoSortInPitrRestore(
			context.Background(),
			bh,
			ts,
			dbName,
			"",
			tableInfos,
		)
		require.NoError(t, err)
		require.Equal(t, want, sorted)
	})

	t.Run("dropped account timestamp restore", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[catalogSQL] = newMrsForPitrRecord(nil)

		sorted, err := fkTablesTopoSortWithTS(
			context.Background(),
			bh,
			dbName,
			"",
			ts,
			0,
			0,
			tableInfos,
		)
		require.NoError(t, err)
		require.Equal(t, want, sorted)
	})
}

func TestCheckRestorePrivEnforcesDatabaseSnapshotScope(t *testing.T) {
	ctx := context.Background()
	ses := &Session{feSessionImpl: feSessionImpl{
		tenant: &TenantInfo{Tenant: "tenant"},
	}}
	snapshot := &snapshotRecord{
		level:        tree.SNAPSHOTLEVELDATABASE.String(),
		accountName:  "tenant",
		databaseName: "source_db",
	}
	stmt := &tree.RestoreSnapShot{
		Level:        tree.RESTORELEVELTABLE,
		AccountName:  "tenant",
		DatabaseName: "source_db",
		TableName:    "table",
	}

	require.NoError(t, checkRestorePriv(ctx, ses, snapshot, stmt))

	stmt.DatabaseName = "other_db"
	err := checkRestorePriv(ctx, ses, snapshot, stmt)
	require.EqualError(t, err, "internal error: databaseName(other_db) does not match snapshot.databaseName(source_db)")
}

func TestCollectRestoreSourceTableInfos(t *testing.T) {
	t.Run("database restore reads only the selected table", func(t *testing.T) {
		var listed bool
		var requestedDB, requestedTable string
		tableInfos, err := collectRestoreSourceTableInfos(
			"db1",
			"child",
			func() ([]string, error) {
				listed = true
				return nil, nil
			},
			func(dbName string, tblName string) ([]*tableInfo, error) {
				requestedDB, requestedTable = dbName, tblName
				return []*tableInfo{{dbName: dbName, tblName: tblName}}, nil
			},
		)
		require.NoError(t, err)
		require.False(t, listed)
		require.Equal(t, "db1", requestedDB)
		require.Equal(t, "child", requestedTable)
		require.Len(t, tableInfos, 1)
	})

	t.Run("account restore reads every user database", func(t *testing.T) {
		var requested []string
		tableInfos, err := collectRestoreSourceTableInfos(
			"",
			"",
			func() ([]string, error) {
				return []string{moCatalog, "db1", "db2"}, nil
			},
			func(dbName string, tblName string) ([]*tableInfo, error) {
				require.Empty(t, tblName)
				requested = append(requested, dbName)
				return []*tableInfo{{dbName: dbName, tblName: "t"}}, nil
			},
		)
		require.NoError(t, err)
		require.Equal(t, []string{"db1", "db2"}, requested)
		require.Len(t, tableInfos, 2)
	})
}

func TestShowDatabasesAtTSDoesNotResolveSnapshotMetadata(t *testing.T) {
	const (
		snapshotTS = int64(100)
		accountID  = uint32(42)
	)
	sql := fmt.Sprintf("show databases {MO_TS = %d}", snapshotTS)
	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[sql] = newMrsForSqlForShowDatabases([][]interface{}{
		{"db1"},
		{"db2"},
	})

	dbNames, err := showDatabasesAtTS(context.Background(), "", bh, snapshotTS, accountID)

	require.NoError(t, err)
	require.Equal(t, []string{"db1", "db2"}, dbNames)
	require.Equal(t, []string{sql}, bh.executedSQLs)
}

func TestRestoreExternalTableSnapshotAndFromTS(t *testing.T) {
	convey.Convey("snapshot bulk restore skips external table", t, func() {
		ctx := context.WithValue(context.TODO(), defines.TenantIDKey{}, uint32(sysAccountID))
		bh := &backgroundExecTest{}
		bh.init()
		const (
			snapshotName = "sp_ext"
			dbName       = "db1"
			snapshotTs   = int64(100)
		)

		bh.sql2result[fmt.Sprintf("select datname, dat_createsql from mo_catalog.mo_database {snapshot = '%s'} where datname = '%s' and account_id = 0", snapshotName, dbName)] =
			newMrsForRestoreStringRows([]string{"datname", "dat_createsql"}, [][]interface{}{{dbName, "create database db1"}})
		bh.sql2result[fmt.Sprintf(checkDatabaseIsMasterFormat, quoteSQLStringLiteral(dbName), quoteSQLStringLiteral(dbName))] = newMrsForRestoreStringRows([]string{"db_name"}, nil)
		bh.sql2result[fmt.Sprintf(getPubInfoSql, uint32(sysAccountID))+" and database_name = 'db1'"] = newMrsForRestoreStringRows([]string{"account_id"}, nil)
		bh.sql2result[buildTableInfoListSQL(dbName, "", snapshotTs, uint32(sysAccountID))] =
			newMrsForRestoreStringRows([]string{"relname", "table_type", "relkind", "viewdef"}, [][]interface{}{
				{"base_t", "BASE TABLE", "r"},
				{"hive_ext", "BASE TABLE", catalog.SystemExternalRel},
			})
		bh.sql2result[fmt.Sprintf("show create table `%s`.`base_t` {MO_TS = %d}", dbName, snapshotTs)] =
			newMrsForRestoreStringRows([]string{"Table", "Create Table"}, [][]interface{}{{"base_t", "create table base_t (id int)"}})
		bh.sql2result[fmt.Sprintf("show create table `%s`.`hive_ext` {MO_TS = %d}", dbName, snapshotTs)] =
			newMrsForRestoreStringRows([]string{"Table", "Create Table"}, [][]interface{}{{"hive_ext", "create external table hive_ext (id int)"}})
		bh.sql2result[fmt.Sprintf(checkTableIsMasterFormat, quoteSQLStringLiteral(dbName), quoteSQLStringLiteral("base_t"))] = newMrsForRestoreStringRows([]string{"db_name"}, nil)

		err := restoreToDatabaseOrTable(ctx, "", bh, snapshotName, dbName, "", uint32(sysAccountID), map[string]*tableInfo{}, map[string]*tableInfo{}, snapshotTs, uint32(sysAccountID), false, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(restoreTestExecutedSQLContains(bh, restoreTableDataByTsSQL(dbName, "base_t", snapshotTs)), convey.ShouldBeTrue)
		convey.So(restoreTestExecutedSQLContains(bh, "hive_ext` clone"), convey.ShouldBeFalse)
	})

	convey.Convey("snapshot table restore rejects explicit external table", t, func() {
		ctx := context.WithValue(context.TODO(), defines.TenantIDKey{}, uint32(sysAccountID))
		bh := &backgroundExecTest{}
		bh.init()
		const (
			snapshotName = "sp_ext"
			dbName       = "db1"
			tblName      = "hive_ext"
			snapshotTs   = int64(100)
		)

		bh.sql2result[fmt.Sprintf("select datname, dat_createsql from mo_catalog.mo_database {snapshot = '%s'} where datname = '%s' and account_id = 0", snapshotName, dbName)] =
			newMrsForRestoreStringRows([]string{"datname", "dat_createsql"}, [][]interface{}{{dbName, "create database db1"}})
		bh.sql2result[buildTableInfoListSQL(dbName, tblName, snapshotTs, uint32(sysAccountID))] =
			newMrsForRestoreStringRows([]string{"relname", "table_type", "relkind", "viewdef"}, [][]interface{}{{tblName, "BASE TABLE", catalog.SystemExternalRel}})
		bh.sql2result[fmt.Sprintf("show create table `%s`.`%s` {MO_TS = %d}", dbName, tblName, snapshotTs)] =
			newMrsForRestoreStringRows([]string{"Table", "Create Table"}, [][]interface{}{{tblName, "create external table hive_ext (id int)"}})

		err := restoreToDatabaseOrTable(ctx, "", bh, snapshotName, dbName, tblName, uint32(sysAccountID), map[string]*tableInfo{}, map[string]*tableInfo{}, snapshotTs, uint32(sysAccountID), false, nil)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "external table db1.hive_ext cannot be restored from snapshot")
		convey.So(restoreTestExecutedSQLContains(bh, "hive_ext` clone"), convey.ShouldBeFalse)
	})

	convey.Convey("restore from TS bulk path skips external table", t, func() {
		ctx := context.WithValue(context.TODO(), defines.TenantIDKey{}, uint32(sysAccountID))
		bh := &backgroundExecTest{}
		bh.init()
		const (
			dbName      = "db1"
			snapshotTs  = int64(100)
			fromAccount = uint32(10)
			toAccount   = uint32(20)
		)

		bh.sql2result[fmt.Sprintf("select datname, dat_createsql from mo_catalog.mo_database {MO_TS = %d } where datname = '%s' and account_id = %d", snapshotTs, dbName, fromAccount)] =
			newMrsForRestoreStringRows([]string{"datname", "dat_createsql"}, [][]interface{}{{dbName, "create database db1"}})
		bh.sql2result[fmt.Sprintf(checkDatabaseIsMasterFormat, quoteSQLStringLiteral(dbName), quoteSQLStringLiteral(dbName))] = newMrsForRestoreStringRows([]string{"db_name"}, nil)
		bh.sql2result[fmt.Sprintf(getPubInfoSql, toAccount)+" and database_name = 'db1'"] = newMrsForRestoreStringRows([]string{"account_id"}, nil)
		bh.sql2result[buildTableInfoListSQL(dbName, "", snapshotTs, fromAccount)] =
			newMrsForRestoreStringRows([]string{"relname", "table_type", "relkind", "viewdef"}, [][]interface{}{
				{"base_t", "BASE TABLE", "r"},
				{"hive_ext", "BASE TABLE", catalog.SystemExternalRel},
			})
		bh.sql2result[fmt.Sprintf("show create table `%s`.`base_t` {MO_TS = %d}", dbName, snapshotTs)] =
			newMrsForRestoreStringRows([]string{"Table", "Create Table"}, [][]interface{}{{"base_t", "create table base_t (id int)"}})
		bh.sql2result[fmt.Sprintf("show create table `%s`.`hive_ext` {MO_TS = %d}", dbName, snapshotTs)] =
			newMrsForRestoreStringRows([]string{"Table", "Create Table"}, [][]interface{}{{"hive_ext", "create external table hive_ext (id int)"}})

		err := restoreDatabaseFromTS(ctx, "", bh, dbName, snapshotTs, fromAccount, toAccount, map[string]*tableInfo{}, map[string]*tableInfo{}, false, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(restoreTestExecutedSQLContains(bh, restoreTableDataByTsSQL(dbName, "base_t", snapshotTs)), convey.ShouldBeTrue)
		convey.So(restoreTestExecutedSQLContains(bh, "hive_ext` clone"), convey.ShouldBeFalse)
	})
}

func TestRestorePitrExternalTable(t *testing.T) {
	convey.Convey("PITR bulk restore skips external table", t, func() {
		ctx := context.WithValue(context.TODO(), defines.TenantIDKey{}, uint32(sysAccountID))
		bh := &backgroundExecTest{}
		bh.init()
		const (
			pitrName = "pitr_ext"
			dbName   = "db1"
			ts       = int64(100)
		)

		bh.sql2result[fmt.Sprintf("select datname, dat_createsql from mo_catalog.mo_database {MO_TS = %d} where datname = '%s' and account_id = 0", ts, dbName)] =
			newMrsForRestoreStringRows([]string{"datname", "dat_createsql"}, [][]interface{}{{dbName, "create database db1"}})
		bh.sql2result[fmt.Sprintf(checkDatabaseIsMasterFormat, quoteSQLStringLiteral(dbName), quoteSQLStringLiteral(dbName))] = newMrsForRestoreStringRows([]string{"db_name"}, nil)
		bh.sql2result[fmt.Sprintf(getPubInfoSql, uint32(sysAccountID))+" and database_name = 'db1'"] = newMrsForRestoreStringRows([]string{"account_id"}, nil)
		bh.sql2result[buildTableInfoListSQL(dbName, "", ts, uint32(sysAccountID))] =
			newMrsForRestoreStringRows([]string{"relname", "table_type", "relkind", "viewdef"}, [][]interface{}{
				{"base_t", "BASE TABLE", "r"},
				{"hive_ext", "BASE TABLE", catalog.SystemExternalRel},
			})
		bh.sql2result[fmt.Sprintf("show create table `%s`.`base_t` {MO_TS = %d}", dbName, ts)] =
			newMrsForRestoreStringRows([]string{"Table", "Create Table"}, [][]interface{}{{"base_t", "create table base_t (id int)"}})
		bh.sql2result[fmt.Sprintf("show create table `%s`.`hive_ext` {MO_TS = %d}", dbName, ts)] =
			newMrsForRestoreStringRows([]string{"Table", "Create Table"}, [][]interface{}{{"hive_ext", "create external table hive_ext (id int)"}})
		bh.sql2result[fmt.Sprintf(checkTableIsMasterFormat, quoteSQLStringLiteral(dbName), quoteSQLStringLiteral("base_t"))] = newMrsForRestoreStringRows([]string{"db_name"}, nil)
		bh.sql2result[getPubInfoWithPitr(ts, uint32(sysAccountID), dbName)] = newMrsForRestoreStringRows([]string{"account_id"}, nil)

		err := restoreToDatabaseOrTableWithPitr(ctx, "", bh, pitrName, ts, dbName, "", map[string]*tableInfo{}, map[string]*tableInfo{}, uint32(sysAccountID))
		convey.So(err, convey.ShouldBeNil)
		convey.So(restoreTestExecutedSQLContains(bh, restoreTableDataByTsSQL(dbName, "base_t", ts)), convey.ShouldBeTrue)
		convey.So(restoreTestExecutedSQLContains(bh, "hive_ext` clone"), convey.ShouldBeFalse)
	})

	convey.Convey("PITR table restore rejects explicit external table", t, func() {
		ctx := context.WithValue(context.TODO(), defines.TenantIDKey{}, uint32(sysAccountID))
		bh := &backgroundExecTest{}
		bh.init()
		const (
			pitrName = "pitr_ext"
			dbName   = "db1"
			tblName  = "hive_ext"
			ts       = int64(100)
		)

		bh.sql2result[fmt.Sprintf("select datname, dat_createsql from mo_catalog.mo_database {MO_TS = %d} where datname = '%s' and account_id = 0", ts, dbName)] =
			newMrsForRestoreStringRows([]string{"datname", "dat_createsql"}, [][]interface{}{{dbName, "create database db1"}})
		bh.sql2result[buildTableInfoListSQL(dbName, tblName, ts, uint32(sysAccountID))] =
			newMrsForRestoreStringRows([]string{"relname", "table_type", "relkind", "viewdef"}, [][]interface{}{{tblName, "BASE TABLE", catalog.SystemExternalRel}})
		bh.sql2result[fmt.Sprintf("show create table `%s`.`%s` {MO_TS = %d}", dbName, tblName, ts)] =
			newMrsForRestoreStringRows([]string{"Table", "Create Table"}, [][]interface{}{{tblName, "create external table hive_ext (id int)"}})

		err := restoreToDatabaseOrTableWithPitr(ctx, "", bh, pitrName, ts, dbName, tblName, map[string]*tableInfo{}, map[string]*tableInfo{}, uint32(sysAccountID))
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "external table db1.hive_ext cannot be restored from pitr")
		convey.So(restoreTestExecutedSQLContains(bh, "hive_ext` clone"), convey.ShouldBeFalse)
	})
}

func TestRestoreExternalTableDefensiveCloneGuards(t *testing.T) {
	convey.Convey("recreate helpers reject external table before executing SQL", t, func() {
		ctx := context.WithValue(context.TODO(), defines.TenantIDKey{}, uint32(sysAccountID))
		tblInfo := &tableInfo{dbName: "db1", tblName: "hive_ext", relKind: catalog.SystemExternalRel}

		bh := &backgroundExecTest{}
		bh.init()
		err := recreateTable(ctx, "", bh, "sp_ext", tblInfo, uint32(sysAccountID), 100)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "external table db1.hive_ext cannot be restored from snapshot")
		convey.So(len(bh.executedSQLs), convey.ShouldEqual, 0)

		bh = &backgroundExecTest{}
		bh.init()
		err = reCreateTableWithPitr(ctx, "", bh, "pitr_ext", 100, tblInfo)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "external table db1.hive_ext cannot be restored from pitr")
		convey.So(len(bh.executedSQLs), convey.ShouldEqual, 0)

		bh = &backgroundExecTest{}
		bh.init()
		err = recreateTableFromTS(ctx, "", bh, tblInfo, 100, 10, 20)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "external table db1.hive_ext cannot be restored from snapshot")
		convey.So(len(bh.executedSQLs), convey.ShouldEqual, 0)
	})
}

func TestBuildTableInfoListSQLEscapesLiterals(t *testing.T) {
	for _, tableName := range []string{"tbl'name", "a_b", "a%b", `child\fk`} {
		t.Run(tableName, func(t *testing.T) {
			sql := buildTableInfoListSQL("db'name", tableName, 0, uint32(sysAccountID))
			if !strings.Contains(sql, "reldatabase = 'db''name'") {
				t.Fatalf("database name was not escaped in SQL: %s", sql)
			}
			if !strings.Contains(sql, "relname = "+quoteSQLStringLiteral(tableName)) {
				t.Fatalf("table name was not matched exactly in SQL: %s", sql)
			}
			if strings.Contains(sql, "relname like "+quoteSQLStringLiteral(tableName)) {
				t.Fatalf("table name was treated as a LIKE pattern in SQL: %s", sql)
			}
			if !strings.Contains(sql, "relkind = 'temporary_table'") {
				t.Fatalf("temporary tables were not filtered by catalog marker: %s", sql)
			}
			if !strings.Contains(sql, "mo_is_legacy_temporary_table(coalesce(relkind, ''), coalesce(relname, ''), coalesce(reldatabase, ''), coalesce(rel_createsql, ''), coalesce(extra_info, ''))") {
				t.Fatalf("legacy temporary base tables were not filtered by CREATE SQL: %s", sql)
			}
			if !strings.Contains(sql, "coalesce(relkind, '') not in ('r', 'v', 'e', 'm', 's', 'cluster', 'partition', 'S') and regexp_like(relname, '^__mo_tmp_[0-9a-f]{32}_')") {
				t.Fatalf("legacy temporary derived objects were not filtered by exact physical name: %s", sql)
			}
			if strings.Contains(sql, "relname not like '__mo_tmp_%'") {
				t.Fatalf("temporary tables were filtered by the broad legal name prefix: %s", sql)
			}
		})
	}
}

func TestGetTableInfosFromTSSkipsStaleTableMetadata(t *testing.T) {
	ctx := context.WithValue(context.TODO(), defines.TenantIDKey{}, uint32(sysAccountID))
	bh := &backgroundExecTest{}
	bh.init()

	const (
		dbName      = "acc_test02"
		snapshotTs  = int64(100)
		fromAccount = uint32(10)
		toAccount   = uint32(20)
	)

	bh.sql2result[buildTableInfoListSQL(dbName, "", snapshotTs, fromAccount)] =
		newMrsForRestoreStringRows([]string{"relname", "table_type", "relkind", "viewdef"}, [][]interface{}{
			{"base_t", "BASE TABLE", "r"},
			{"aff01", "BASE TABLE", "r"},
		})
	bh.sql2result[fmt.Sprintf("show create table `%s`.`base_t` {MO_TS = %d}", dbName, snapshotTs)] =
		newMrsForRestoreStringRows([]string{"Table", "Create Table"}, [][]interface{}{{"base_t", "create table base_t (id int)"}})
	bh.sql2result[fmt.Sprintf("show create table `%s`.`aff01` {MO_TS = %d}", dbName, snapshotTs)] =
		newMrsForRestoreStringRows([]string{"Table", "Create Table"}, nil)

	tableInfos, err := getTableInfosFromTS(ctx, "", bh, dbName, "", snapshotTs, fromAccount, toAccount)
	if err != nil {
		t.Fatal(err)
	}
	if len(tableInfos) != 1 {
		t.Fatalf("expected one restorable table, got %d", len(tableInfos))
	}
	if tableInfos[0].tblName != "base_t" {
		t.Fatalf("expected base_t, got %s", tableInfos[0].tblName)
	}
	if tableInfos[0].createSql != "create table base_t (id int)" {
		t.Fatalf("unexpected create sql: %s", tableInfos[0].createSql)
	}
}

func TestGetTableInfosFromTSReturnsCreateTableErrors(t *testing.T) {
	ctx := context.WithValue(context.TODO(), defines.TenantIDKey{}, uint32(sysAccountID))
	bh := &backgroundExecTest{}
	bh.init()

	const (
		dbName      = "acc_test02"
		snapshotTs  = int64(100)
		fromAccount = uint32(10)
		toAccount   = uint32(20)
	)

	createTableSQL := fmt.Sprintf("show create table `%s`.`aff01` {MO_TS = %d}", dbName, snapshotTs)
	createTableErr := moerr.NewInternalError(ctx, "failed to read create table")
	bh.sql2result[buildTableInfoListSQL(dbName, "", snapshotTs, fromAccount)] =
		newMrsForRestoreStringRows([]string{"relname", "table_type", "relkind", "viewdef"}, [][]interface{}{
			{"aff01", "BASE TABLE", "r"},
		})
	bh.sql2err[createTableSQL] = createTableErr

	_, err := getTableInfosFromTS(ctx, "", bh, dbName, "", snapshotTs, fromAccount, toAccount)
	if err != createTableErr {
		t.Fatalf("expected create table error, got %v", err)
	}
}

func newMrsForRestoreStringRows(colNames []string, rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}
	for _, colName := range colNames {
		col := &MysqlColumn{}
		col.SetName(colName)
		col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		mrs.AddColumn(col)
	}
	for _, row := range rows {
		values := append([]interface{}(nil), row...)
		for len(values) < len(colNames) {
			values = append(values, "")
		}
		mrs.AddRow(values)
	}
	return mrs
}

func TestViewRestoreUsesPersistedParserSQLModeWithoutRewritingSQL(t *testing.T) {
	pipesAsConcat := "PIPES_AS_CONCAT"
	ansiQuotes := "ANSI_QUOTES"
	noBackslashEscapes := "NO_BACKSLASH_ESCAPES"

	tests := []struct {
		name     string
		sqlMode  *string
		create   string
		wantMode string
		want     string
		value    string
	}{
		{
			name:     "pipes as concat",
			sqlMode:  &pipesAsConcat,
			create:   "create view v_pipe as select 'a'||'b' as c",
			wantMode: pipesAsConcat,
			want:     "concat(",
		},
		{
			name:     "ansi quotes",
			sqlMode:  &ansiQuotes,
			create:   `create view v_ansi as select "a" as c from "t"`,
			wantMode: ansiQuotes,
			want:     "`a`",
		},
		{
			name:     "legacy view defaults to pipes as concat",
			create:   "create view v_legacy as select 'a'||'b' as c",
			wantMode: legacyViewParserSQLModeForRestore,
			want:     "concat(",
		},
		{
			name:     "no backslash escapes",
			sqlMode:  &noBackslashEscapes,
			create:   `create view v_backslash as select 'a\b' as c`,
			wantMode: noBackslashEscapes,
			want:     `a\\b`,
			value:    `a\b`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			viewDef, err := json.Marshal(plan.ViewData{Stmt: test.create, SQLMode: test.sqlMode})
			require.NoError(t, err)

			tblInfo := &tableInfo{viewDef: string(viewDef), createSql: test.create}
			parserSQLMode, err := viewParserSQLModeForRestore(tblInfo.viewDef)
			require.NoError(t, err)
			require.Equal(t, test.wantMode, parserSQLMode)

			stmts, err := parseViewCreateSQLForRestore(context.Background(), tblInfo, 1)
			require.NoError(t, err)
			canonical := tree.StringWithOpts(stmts[0], dialect.MYSQL, tree.WithSingleQuoteString(), tree.WithQuoteIdentifier())
			require.Contains(t, strings.ToLower(canonical), test.want)
			if test.value != "" {
				viewStmt, ok := stmts[0].(*tree.CreateView)
				require.True(t, ok)
				selectClause, ok := viewStmt.AsSource.Select.(*tree.SelectClause)
				require.True(t, ok)
				value, ok := selectClause.Exprs[0].Expr.(*tree.NumVal)
				require.True(t, ok)
				require.Equal(t, test.value, value.String())
			}
			freeStatements(stmts)

			bh := &backgroundExecTest{}
			bh.init()
			require.NoError(t, executeViewCreateSQLForRestore(context.Background(), bh, tblInfo))
			require.Equal(t, test.create, bh.currentSql)
			require.Equal(t, test.wantMode, bh.parserSQLMode)
		})
	}
}

func restoreTestExecutedSQLContains(bh *backgroundExecTest, needle string) bool {
	for _, sql := range bh.executedSQLs {
		if strings.Contains(sql, needle) {
			return true
		}
	}
	return false
}

func TestRestoreSQLQuotesEmbeddedBackticks(t *testing.T) {
	const (
		dbName       = "db`name"
		tableName    = "table`name"
		viewName     = "view`name"
		snapshotName = "snapshot'name"
	)
	qualifiedName := "`db``name`.`table``name`"

	require.Equal(t, "show create table "+qualifiedName, showCreateTableSQL(dbName, tableName))
	require.Equal(t, "use `db``name`", useDatabaseSQL(dbName))
	require.Equal(t, "CREATE DATABASE IF NOT EXISTS `db``name`", createDatabaseIfNotExistsSQL(dbName))
	require.Equal(t, "drop database if exists `db``name`", dropDatabaseIfExistsSQL(dbName))
	require.Equal(t, "drop table if exists "+qualifiedName, dropTableIfExistsSQL(dbName, tableName))
	require.Equal(t, "drop view if exists `view``name`", dropViewIfExistsSQL(viewName))
	require.Equal(t,
		"create table "+qualifiedName+" clone "+qualifiedName+" {MO_TS = 123 }",
		restoreTableDataByTsSQL(dbName, tableName, 123))
	require.Equal(t,
		"create table "+qualifiedName+" clone "+qualifiedName+" {SNAPSHOT = 'snapshot\\'name'}",
		restoreTableDataByNameSQL(dbName, tableName, snapshotName))
}

func Test_fkTablesTopoSortWithTS(t *testing.T) {
	convey.Convey("fkTablesTopoSortWithTS ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newTestSession(t, ctrl)
		defer ses.Close()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		setPu("", pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx, "")
		ses.rm = rm

		tenant := &TenantInfo{
			Tenant:        sysAccountName,
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      sysAccountID,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}
		ses.SetTenantInfo(tenant)

		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))

		_, err := fkTablesTopoSortWithTS(ctx, bh, "", "", 0, 0, 0, nil)
		convey.So(err, convey.ShouldNotBeNil)

		sql := "select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys"
		mrs := newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err = fkTablesTopoSortWithTS(ctx, bh, "", "", 0, 0, 0, nil)
		convey.So(err, convey.ShouldBeNil)

		sql = "select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys"
		mrs = newMrsForPitrRecord([][]interface{}{{"db1", "table1", "db2", "table2"}})
		bh.sql2result[sql] = mrs
		_, err = fkTablesTopoSortWithTS(ctx, bh, "", "", 0, 0, 0, nil)
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_getFkDepsWithTS(t *testing.T) {
	convey.Convey("getFkDepsWithTS ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newTestSession(t, ctrl)
		defer ses.Close()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		setPu("", pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx, "")
		ses.rm = rm

		tenant := &TenantInfo{
			Tenant:        sysAccountName,
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      sysAccountID,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}
		ses.SetTenantInfo(tenant)

		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))

		_, err := getFkDepsWithTS(ctx, bh, "", "", 0, 0, 0)
		convey.So(err, convey.ShouldNotBeNil)

		sql := "select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys"
		mrs := newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err = getFkDepsWithTS(ctx, bh, "", "", 0, 0, 0)
		convey.So(err, convey.ShouldBeNil)

		sql = "select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys"
		mrs = newMrsForPitrRecord([][]interface{}{{"db1", "table1", "db2", "table2"}})
		bh.sql2result[sql] = mrs

		_, err = getFkDepsWithTS(ctx, bh, "", "", 0, 0, 0)
		convey.So(err, convey.ShouldBeNil)

		sql = "select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys"
		mrs = newMrsForPitrRecord([][]interface{}{{types.Day_Hour, "table1", "db2", "table2"}})
		bh.sql2result[sql] = mrs

		_, err = getFkDepsWithTS(ctx, bh, "", "", 0, 0, 0)
		convey.So(err, convey.ShouldNotBeNil)

		sql = "select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys"
		mrs = newMrsForPitrRecord([][]interface{}{{"db1", types.Day_Hour, "db2", "table2"}})
		bh.sql2result[sql] = mrs

		_, err = getFkDepsWithTS(ctx, bh, "", "", 0, 0, 0)
		convey.So(err, convey.ShouldNotBeNil)

		sql = "select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys"
		mrs = newMrsForPitrRecord([][]interface{}{{"db1", "table1", types.Day_Hour, "table2"}})
		bh.sql2result[sql] = mrs

		_, err = getFkDepsWithTS(ctx, bh, "", "", 0, 0, 0)
		convey.So(err, convey.ShouldNotBeNil)

		sql = "select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys"
		mrs = newMrsForPitrRecord([][]interface{}{{"db1", "table1", "db2", types.Day_Hour}})
		bh.sql2result[sql] = mrs

		_, err = getFkDepsWithTS(ctx, bh, "", "", 0, 0, 0)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func Test_restoreAccountUsingClusterSnapshotToNew(t *testing.T) {
	convey.Convey("restoreAccountUsingClusterSnapshotToNew ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newTestSession(t, ctrl)
		defer ses.Close()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		setPu("", pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx, "")
		ses.rm = rm

		tenant := &TenantInfo{
			Tenant:        sysAccountName,
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      sysAccountID,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}
		ses.SetTenantInfo(tenant)

		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))

		err := restoreAccountUsingClusterSnapshotToNew(ctx, ses, bh, "sp01", 0, accountRecord{accountName: "sys", accountId: 0}, 0, nil, false, false)
		convey.So(err, convey.ShouldNotBeNil)

		sql := "select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys"
		mrs := newMrsForPitrRecord([][]interface{}{{"db1", "table1", "db2", "table2"}})
		bh.sql2result[sql] = mrs

		err = restoreAccountUsingClusterSnapshotToNew(ctx, ses, bh, "sp01", 0, accountRecord{accountName: "sys", accountId: 0}, 0, nil, false, false)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func Test_dropExistsAccount_InRestoreTransaction(t *testing.T) {
	convey.Convey("dropExistsAccount should not create new transaction during restore", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newTestSession(t, ctrl)
		defer ses.Close()

		bh := &backgroundExecTestWithHistory{}
		bh.init()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		setPu("", pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx, "")
		ses.rm = rm

		tenant := &TenantInfo{
			Tenant:        sysAccountName,
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      sysAccountID,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}
		ses.SetTenantInfo(tenant)

		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))

		// Setup SQL results for dropExistsAccount
		// Note: No "begin;" should be executed since we're in restore transaction
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		// Setup SQL results for doDropAccount (called by dropExistsAccount)
		sql, _ := getSqlForCheckTenant(ctx, "test_acc")
		mrs := newMrsForGetAllAccounts([][]interface{}{
			{uint64(1), "test_acc", "open", uint64(1), nil},
		})
		bh.sql2result[sql] = mrs

		sql, _ = getSqlForDeleteAccountFromMoAccount(context.TODO(), "test_acc")
		bh.sql2result[sql] = nil

		for _, sql = range getSqlForDropAccount() {
			bh.sql2result[sql] = nil
		}

		bh.sql2result["show databases;"] = newMrsForSqlForShowDatabases([][]interface{}{})

		bh.sql2result["show tables from mo_catalog;"] = newMrsForShowTables([][]interface{}{})

		sql = fmt.Sprintf(getPubInfoSql, 1) + " order by update_time desc, created_time desc"
		bh.sql2result[sql] = newMrsForSqlForGetPubs([][]interface{}{})

		sql = "select 1 from mo_catalog.mo_columns where att_database = 'mo_catalog' and att_relname = 'mo_subs' and attname = 'sub_account_name'"
		bh.sql2result[sql] = newMrsForSqlForGetSubs([][]interface{}{{1}})

		sql = getSubsSql + " and sub_account_id = 1"
		bh.sql2result[sql] = newMrsForSqlForGetSubs([][]interface{}{})

		// Call dropExistsAccount (used in restoreToCluster)
		account := accountRecord{
			accountName: "test_acc",
			accountId:   1,
		}
		err := dropExistsAccount(ctx, ses, bh, "test_snapshot", account)

		convey.So(err, convey.ShouldBeNil)
		// Verify that "begin;" was NOT executed (restore scenario)
		// dropExistsAccount should not create new transaction during restore
		convey.So(bh.hasExecuted("begin;"), convey.ShouldBeFalse)
	})
}

// newMrsForSnapshotRecord creates a MysqlResultSet for full snapshot record query (select * from mo_snapshots)
// columns: snapshot_id, sname, ts, level, account_name, database_name, table_name, obj_id
func newMrsForSnapshotRecord(snapshotId, snapshotName string, ts int64, level, accountName, databaseName, tableName string, objId uint64) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("snapshot_id")
	col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mrs.AddColumn(col1)

	col2 := &MysqlColumn{}
	col2.SetName("sname")
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mrs.AddColumn(col2)

	col3 := &MysqlColumn{}
	col3.SetName("ts")
	col3.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	mrs.AddColumn(col3)

	col4 := &MysqlColumn{}
	col4.SetName("level")
	col4.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mrs.AddColumn(col4)

	col5 := &MysqlColumn{}
	col5.SetName("account_name")
	col5.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mrs.AddColumn(col5)

	col6 := &MysqlColumn{}
	col6.SetName("database_name")
	col6.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mrs.AddColumn(col6)

	col7 := &MysqlColumn{}
	col7.SetName("table_name")
	col7.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mrs.AddColumn(col7)

	col8 := &MysqlColumn{}
	col8.SetName("obj_id")
	col8.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	mrs.AddColumn(col8)

	mrs.AddRow([]interface{}{snapshotId, snapshotName, ts, level, accountName, databaseName, tableName, objId})

	return mrs
}

// newMrsForDatabaseNames creates a MysqlResultSet for database names query
func newMrsForDatabaseNames(dbNames []string) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("datname")
	col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mrs.AddColumn(col1)

	for _, dbName := range dbNames {
		mrs.AddRow([]interface{}{dbName})
	}

	return mrs
}

// newMrsForPublicationInfo creates a MysqlResultSet for publication info query
func newMrsForPublicationInfo(accountID uint64, accountName, pubName, dbName string, dbID uint64, tableList, accountList string) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("account_id")
	col1.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	mrs.AddColumn(col1)

	col2 := &MysqlColumn{}
	col2.SetName("account_name")
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mrs.AddColumn(col2)

	col3 := &MysqlColumn{}
	col3.SetName("pub_name")
	col3.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mrs.AddColumn(col3)

	col4 := &MysqlColumn{}
	col4.SetName("database_name")
	col4.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mrs.AddColumn(col4)

	col5 := &MysqlColumn{}
	col5.SetName("database_id")
	col5.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	mrs.AddColumn(col5)

	col6 := &MysqlColumn{}
	col6.SetName("table_list")
	col6.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mrs.AddColumn(col6)

	col7 := &MysqlColumn{}
	col7.SetName("account_list")
	col7.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mrs.AddColumn(col7)

	mrs.AddRow([]interface{}{accountID, accountName, pubName, dbName, dbID, tableList, accountList})

	return mrs
}

// newMrsEmpty creates an empty MysqlResultSet
func newMrsEmpty() *MysqlResultSet {
	return &MysqlResultSet{}
}

func Test_handleGetSnapshotTs(t *testing.T) {
	convey.Convey("handleGetSnapshotTs success case", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newTestSession(t, ctrl)
		defer ses.Close()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		setPu("", pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx, "")
		ses.rm = rm

		tenant := &TenantInfo{
			Tenant:        "test_tenant",
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      1,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}
		ses.SetTenantInfo(tenant)
		ses.mrs = &MysqlResultSet{}

		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(1))

		// Setup mock result for publication info query (permission check)
		// account_list contains "test_tenant" so permission check should pass
		pubQuerySQL := fmt.Sprintf(`SELECT account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			FROM mo_catalog.mo_pubs 
			WHERE account_name = '%s' AND pub_name = '%s'`, "pub_account", "test_pub")
		bh.sql2result[pubQuerySQL] = newMrsForPublicationInfo(
			uint64(100), "pub_account", "test_pub", "test_db", uint64(1), "*", "test_tenant,all",
		)

		// Setup mock result for snapshot record query (select * from mo_snapshots)
		snapshotRecordSQL := fmt.Sprintf("select * from mo_catalog.mo_snapshots where sname = '%s'", "test_snapshot")
		bh.sql2result[snapshotRecordSQL] = newMrsForSnapshotRecord(
			"snap-001", "test_snapshot", int64(1234567890), "account", "", "", "", uint64(1),
		)

		ic := &InternalCmdGetSnapshotTs{
			snapshotName:    "test_snapshot",
			accountName:     "pub_account",
			publicationName: "test_pub",
		}

		execCtx := &ExecCtx{
			reqCtx: ctx,
			ses:    ses,
		}

		err := handleGetSnapshotTs(ses, execCtx, ic)
		convey.So(err, convey.ShouldBeNil)

		// Verify result set contains the snapshot ts
		mrs := ses.GetMysqlResultSet()
		convey.So(mrs.GetColumnCount(), convey.ShouldEqual, 1)
		convey.So(mrs.GetRowCount(), convey.ShouldEqual, 1)
	})

	convey.Convey("handleGetSnapshotTs snapshot not found", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newTestSession(t, ctrl)
		defer ses.Close()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		setPu("", pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx, "")
		ses.rm = rm

		tenant := &TenantInfo{
			Tenant:        "test_tenant",
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      1,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}
		ses.SetTenantInfo(tenant)
		ses.mrs = &MysqlResultSet{}

		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(1))

		// Setup mock result for publication info query
		pubQuerySQL := fmt.Sprintf(`SELECT account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			FROM mo_catalog.mo_pubs 
			WHERE account_name = '%s' AND pub_name = '%s'`, "pub_account", "test_pub")
		bh.sql2result[pubQuerySQL] = newMrsForPublicationInfo(
			uint64(100), "pub_account", "test_pub", "test_db", uint64(1), "*", "test_tenant,all",
		)

		// Setup mock result for snapshot record query - empty result (snapshot not found)
		snapshotRecordSQL := fmt.Sprintf("select * from mo_catalog.mo_snapshots where sname = '%s'", "nonexistent_snapshot")
		bh.sql2result[snapshotRecordSQL] = newMrsEmpty()

		ic := &InternalCmdGetSnapshotTs{
			snapshotName:    "nonexistent_snapshot",
			accountName:     "pub_account",
			publicationName: "test_pub",
		}

		execCtx := &ExecCtx{
			reqCtx: ctx,
			ses:    ses,
		}

		err := handleGetSnapshotTs(ses, execCtx, ic)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "find 0 snapshot records")
	})

	convey.Convey("handleGetSnapshotTs publication permission denied", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newTestSession(t, ctrl)
		defer ses.Close()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		setPu("", pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx, "")
		ses.rm = rm

		tenant := &TenantInfo{
			Tenant:        "unauthorized_tenant",
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      1,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}
		ses.SetTenantInfo(tenant)
		ses.mrs = &MysqlResultSet{}

		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(1))

		// Setup mock result for publication info query
		// account_list does NOT contain "unauthorized_tenant" so permission check should fail
		pubQuerySQL := fmt.Sprintf(`SELECT account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			FROM mo_catalog.mo_pubs 
			WHERE account_name = '%s' AND pub_name = '%s'`, "pub_account", "test_pub")
		bh.sql2result[pubQuerySQL] = newMrsForPublicationInfo(
			uint64(100), "pub_account", "test_pub", "test_db", uint64(1), "*", "other_tenant",
		)

		ic := &InternalCmdGetSnapshotTs{
			snapshotName:    "test_snapshot",
			accountName:     "pub_account",
			publicationName: "test_pub",
		}

		execCtx := &ExecCtx{
			reqCtx: ctx,
			ses:    ses,
		}

		err := handleGetSnapshotTs(ses, execCtx, ic)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "does not have permission")
	})

	convey.Convey("handleGetSnapshotTs publication not found", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newTestSession(t, ctrl)
		defer ses.Close()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		setPu("", pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx, "")
		ses.rm = rm

		tenant := &TenantInfo{
			Tenant:        "test_tenant",
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      1,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}
		ses.SetTenantInfo(tenant)
		ses.mrs = &MysqlResultSet{}

		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(1))

		// Setup mock result for publication info query - empty result (publication not found)
		pubQuerySQL := fmt.Sprintf(`SELECT account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			FROM mo_catalog.mo_pubs 
			WHERE account_name = '%s' AND pub_name = '%s'`, "pub_account", "nonexistent_pub")
		bh.sql2result[pubQuerySQL] = newMrsEmpty()

		ic := &InternalCmdGetSnapshotTs{
			snapshotName:    "test_snapshot",
			accountName:     "pub_account",
			publicationName: "nonexistent_pub",
		}

		execCtx := &ExecCtx{
			reqCtx: ctx,
			ses:    ses,
		}

		err := handleGetSnapshotTs(ses, execCtx, ic)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "does not exist")
	})
}

func Test_handleGetDatabases(t *testing.T) {
	convey.Convey("handleGetDatabases success case", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newTestSession(t, ctrl)
		defer ses.Close()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		setPu("", pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx, "")
		ses.rm = rm

		tenant := &TenantInfo{
			Tenant:        "test_tenant",
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      1,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}
		ses.SetTenantInfo(tenant)
		ses.mrs = &MysqlResultSet{}

		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(1))

		// Setup mock result for publication info query (permission check)
		pubQuerySQL := fmt.Sprintf(`SELECT account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			FROM mo_catalog.mo_pubs 
			WHERE account_name = '%s' AND pub_name = '%s'`, "pub_account", "test_pub")
		bh.sql2result[pubQuerySQL] = newMrsForPublicationInfo(
			uint64(100), "pub_account", "test_pub", "test_db", uint64(1), "*", "test_tenant,all",
		)

		// Setup mock result for snapshot record query
		snapshotRecordSQL := fmt.Sprintf("select * from mo_catalog.mo_snapshots where sname = '%s'", "test_snapshot")
		bh.sql2result[snapshotRecordSQL] = newMrsForSnapshotRecord(
			"snap-001", "test_snapshot", int64(1234567890), "account", "", "", "", uint64(1),
		)

		// Setup mock result for database names query
		dbSQL := fmt.Sprintf("SELECT datname FROM mo_catalog.mo_database{MO_TS = %d} WHERE account_id = %d", int64(1234567890), 100)
		bh.sql2result[dbSQL] = newMrsForDatabaseNames([]string{"db1", "db2", "db3"})

		ic := &InternalCmdGetDatabases{
			snapshotName:    "test_snapshot",
			accountName:     "pub_account",
			publicationName: "test_pub",
		}

		execCtx := &ExecCtx{
			reqCtx: ctx,
			ses:    ses,
		}

		err := handleGetDatabases(ses, execCtx, ic)
		convey.So(err, convey.ShouldBeNil)

		// Verify result set contains the database names
		mrs := ses.GetMysqlResultSet()
		convey.So(mrs.GetColumnCount(), convey.ShouldEqual, 1)
		convey.So(mrs.GetRowCount(), convey.ShouldEqual, 3)
	})

	convey.Convey("handleGetDatabases snapshot not found", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newTestSession(t, ctrl)
		defer ses.Close()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		setPu("", pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx, "")
		ses.rm = rm

		tenant := &TenantInfo{
			Tenant:        "test_tenant",
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      1,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}
		ses.SetTenantInfo(tenant)
		ses.mrs = &MysqlResultSet{}

		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(1))

		// Setup mock result for publication info query
		pubQuerySQL := fmt.Sprintf(`SELECT account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			FROM mo_catalog.mo_pubs 
			WHERE account_name = '%s' AND pub_name = '%s'`, "pub_account", "test_pub")
		bh.sql2result[pubQuerySQL] = newMrsForPublicationInfo(
			uint64(100), "pub_account", "test_pub", "test_db", uint64(1), "*", "test_tenant,all",
		)

		// Setup mock result for snapshot record query - empty result (snapshot not found)
		snapshotRecordSQL := fmt.Sprintf("select * from mo_catalog.mo_snapshots where sname = '%s'", "nonexistent_snapshot")
		bh.sql2result[snapshotRecordSQL] = newMrsEmpty()

		ic := &InternalCmdGetDatabases{
			snapshotName:    "nonexistent_snapshot",
			accountName:     "pub_account",
			publicationName: "test_pub",
		}

		execCtx := &ExecCtx{
			reqCtx: ctx,
			ses:    ses,
		}

		err := handleGetDatabases(ses, execCtx, ic)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "find 0 snapshot records")
	})

	convey.Convey("handleGetDatabases permission denied", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newTestSession(t, ctrl)
		defer ses.Close()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		setPu("", pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx, "")
		ses.rm = rm

		tenant := &TenantInfo{
			Tenant:        "unauthorized_tenant",
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      1,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}
		ses.SetTenantInfo(tenant)
		ses.mrs = &MysqlResultSet{}

		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(1))

		// Setup mock result for publication info query
		// account_list does NOT contain "unauthorized_tenant"
		pubQuerySQL := fmt.Sprintf(`SELECT account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			FROM mo_catalog.mo_pubs 
			WHERE account_name = '%s' AND pub_name = '%s'`, "pub_account", "test_pub")
		bh.sql2result[pubQuerySQL] = newMrsForPublicationInfo(
			uint64(100), "pub_account", "test_pub", "test_db", uint64(1), "*", "other_tenant",
		)

		ic := &InternalCmdGetDatabases{
			snapshotName:    "test_snapshot",
			accountName:     "pub_account",
			publicationName: "test_pub",
		}

		execCtx := &ExecCtx{
			reqCtx: ctx,
			ses:    ses,
		}

		err := handleGetDatabases(ses, execCtx, ic)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "does not have permission")
	})

	convey.Convey("handleGetDatabases empty database list", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newTestSession(t, ctrl)
		defer ses.Close()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		setPu("", pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx, "")
		ses.rm = rm

		tenant := &TenantInfo{
			Tenant:        "test_tenant",
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      1,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}
		ses.SetTenantInfo(tenant)
		ses.mrs = &MysqlResultSet{}

		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(1))

		// Setup mock result for publication info query
		pubQuerySQL := fmt.Sprintf(`SELECT account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			FROM mo_catalog.mo_pubs 
			WHERE account_name = '%s' AND pub_name = '%s'`, "pub_account", "test_pub")
		bh.sql2result[pubQuerySQL] = newMrsForPublicationInfo(
			uint64(100), "pub_account", "test_pub", "test_db", uint64(1), "*", "test_tenant,all",
		)

		// Setup mock result for snapshot record query
		snapshotRecordSQL := fmt.Sprintf("select * from mo_catalog.mo_snapshots where sname = '%s'", "test_snapshot")
		bh.sql2result[snapshotRecordSQL] = newMrsForSnapshotRecord(
			"snap-001", "test_snapshot", int64(1234567890), "account", "", "", "", uint64(1),
		)

		// Setup mock result for database names query - empty result
		dbSQL := fmt.Sprintf("SELECT datname FROM mo_catalog.mo_database{MO_TS = %d} WHERE account_id = %d", int64(1234567890), 100)
		bh.sql2result[dbSQL] = newMrsForDatabaseNames([]string{})

		ic := &InternalCmdGetDatabases{
			snapshotName:    "test_snapshot",
			accountName:     "pub_account",
			publicationName: "test_pub",
		}

		execCtx := &ExecCtx{
			reqCtx: ctx,
			ses:    ses,
		}

		err := handleGetDatabases(ses, execCtx, ic)
		convey.So(err, convey.ShouldBeNil)

		// Verify result set is empty but has correct column
		mrs := ses.GetMysqlResultSet()
		convey.So(mrs.GetColumnCount(), convey.ShouldEqual, 1)
		convey.So(mrs.GetRowCount(), convey.ShouldEqual, 0)
	})

	convey.Convey("handleGetDatabases publication not found", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newTestSession(t, ctrl)
		defer ses.Close()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		setPu("", pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx, "")
		ses.rm = rm

		tenant := &TenantInfo{
			Tenant:        "test_tenant",
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      1,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}
		ses.SetTenantInfo(tenant)
		ses.mrs = &MysqlResultSet{}

		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(1))

		// Setup mock result for publication info query - empty result
		pubQuerySQL := fmt.Sprintf(`SELECT account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			FROM mo_catalog.mo_pubs 
			WHERE account_name = '%s' AND pub_name = '%s'`, "pub_account", "nonexistent_pub")
		bh.sql2result[pubQuerySQL] = newMrsEmpty()

		ic := &InternalCmdGetDatabases{
			snapshotName:    "test_snapshot",
			accountName:     "pub_account",
			publicationName: "nonexistent_pub",
		}

		execCtx := &ExecCtx{
			reqCtx: ctx,
			ses:    ses,
		}

		err := handleGetDatabases(ses, execCtx, ic)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "does not exist")
	})
}

func Test_getAccountFromPublication(t *testing.T) {
	convey.Convey("getAccountFromPublication success case", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		setPu("", pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(1))

		// Setup mock result for publication info query
		pubQuerySQL := fmt.Sprintf(`SELECT account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			FROM mo_catalog.mo_pubs 
			WHERE account_name = '%s' AND pub_name = '%s'`, "pub_account", "test_pub")
		bh.sql2result[pubQuerySQL] = newMrsForPublicationInfo(
			uint64(100), "pub_account", "test_pub", "test_db", uint64(1), "*", "test_tenant,all",
		)

		accountID, accountName, err := getAccountFromPublication(ctx, bh, "pub_account", "test_pub", "test_tenant")
		convey.So(err, convey.ShouldBeNil)
		convey.So(accountID, convey.ShouldEqual, uint64(100))
		convey.So(accountName, convey.ShouldEqual, "pub_account")
	})

	convey.Convey("getAccountFromPublication publication not found", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		setPu("", pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(1))

		// Setup mock result for publication info query - empty result
		pubQuerySQL := fmt.Sprintf(`SELECT account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			FROM mo_catalog.mo_pubs 
			WHERE account_name = '%s' AND pub_name = '%s'`, "pub_account", "nonexistent_pub")
		bh.sql2result[pubQuerySQL] = newMrsEmpty()

		_, _, err := getAccountFromPublication(ctx, bh, "pub_account", "nonexistent_pub", "test_tenant")
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "does not exist")
	})

	convey.Convey("getAccountFromPublication permission denied", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		setPu("", pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(1))

		// Setup mock result for publication info query
		// account_list does NOT contain "unauthorized_tenant"
		pubQuerySQL := fmt.Sprintf(`SELECT account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			FROM mo_catalog.mo_pubs 
			WHERE account_name = '%s' AND pub_name = '%s'`, "pub_account", "test_pub")
		bh.sql2result[pubQuerySQL] = newMrsForPublicationInfo(
			uint64(100), "pub_account", "test_pub", "test_db", uint64(1), "*", "other_tenant",
		)

		_, _, err := getAccountFromPublication(ctx, bh, "pub_account", "test_pub", "unauthorized_tenant")
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "does not have permission")
	})

	convey.Convey("getAccountFromPublication with 'all' in account_list", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		setPu("", pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(1))

		// Setup mock result for publication info query with "all" in account_list
		pubQuerySQL := fmt.Sprintf(`SELECT account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			FROM mo_catalog.mo_pubs 
			WHERE account_name = '%s' AND pub_name = '%s'`, "pub_account", "test_pub")
		bh.sql2result[pubQuerySQL] = newMrsForPublicationInfo(
			uint64(100), "pub_account", "test_pub", "test_db", uint64(1), "*", "all",
		)

		// Any tenant should be able to access when account_list contains "all"
		accountID, accountName, err := getAccountFromPublication(ctx, bh, "pub_account", "test_pub", "any_tenant")
		convey.So(err, convey.ShouldBeNil)
		convey.So(accountID, convey.ShouldEqual, uint64(100))
		convey.So(accountName, convey.ShouldEqual, "pub_account")
	})
}

// newMrsForMoIndexes creates a MysqlResultSet for mo_indexes query
// columns: table_id, name, algo_table_type, index_table_name
func newMrsForMoIndexes(records [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("table_id")
	col1.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	mrs.AddColumn(col1)

	col2 := &MysqlColumn{}
	col2.SetName("name")
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mrs.AddColumn(col2)

	col3 := &MysqlColumn{}
	col3.SetName("algo_table_type")
	col3.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mrs.AddColumn(col3)

	col4 := &MysqlColumn{}
	col4.SetName("index_table_name")
	col4.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mrs.AddColumn(col4)

	for _, record := range records {
		mrs.AddRow(record)
	}

	return mrs
}

// Test_handleGetMoIndexes_GoodPath tests the good path of handleGetMoIndexes
func Test_handleGetMoIndexes_GoodPath(t *testing.T) {
	convey.Convey("handleGetMoIndexes good path - with snapshot", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newTestSession(t, ctrl)
		defer ses.Close()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		setPu("", pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx, "")
		ses.rm = rm

		tenant := &TenantInfo{
			Tenant:        "test_tenant",
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      1,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}
		ses.SetTenantInfo(tenant)
		ses.mrs = &MysqlResultSet{}

		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(1))

		// Setup mock result for publication info query
		pubQuerySQL := fmt.Sprintf(`SELECT account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			FROM mo_catalog.mo_pubs 
			WHERE account_name = '%s' AND pub_name = '%s'`, "pub_account", "test_pub")
		bh.sql2result[pubQuerySQL] = newMrsForPublicationInfo(
			uint64(100), "pub_account", "test_pub", "test_db", uint64(1), "*", "test_tenant,all",
		)

		// Setup mock result for snapshot record query
		snapshotRecordSQL := fmt.Sprintf("select * from mo_catalog.mo_snapshots where sname = '%s'", "test_snapshot")
		bh.sql2result[snapshotRecordSQL] = newMrsForSnapshotRecord(
			"snap-001", "test_snapshot", int64(1234567890), "account", "", "", "", uint64(1),
		)

		// Setup mock result for mo_indexes query with snapshot timestamp
		indexSQL := fmt.Sprintf("SELECT table_id, name, algo_table_type, index_table_name FROM mo_catalog.mo_indexes{MO_TS = %d} WHERE table_id = %d", int64(1234567890), 12345)
		bh.sql2result[indexSQL] = newMrsForMoIndexes([][]interface{}{
			{uint64(12345), "idx_primary", "", ""},
			{uint64(12345), "idx_name", "ivfflat", "__mo_index_idx_name"},
		})

		ic := &InternalCmdGetMoIndexes{
			tableId:                 12345,
			subscriptionAccountName: "pub_account",
			publicationName:         "test_pub",
			snapshotName:            "test_snapshot",
		}

		execCtx := &ExecCtx{
			reqCtx: ctx,
			ses:    ses,
		}

		err := handleGetMoIndexes(ses, execCtx, ic)
		convey.So(err, convey.ShouldBeNil)

		// Verify result set
		mrs := ses.GetMysqlResultSet()
		convey.So(mrs.GetColumnCount(), convey.ShouldEqual, uint64(4))
		convey.So(mrs.GetRowCount(), convey.ShouldEqual, uint64(2))
	})

	convey.Convey("handleGetMoIndexes good path - without snapshot (use current timestamp)", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newTestSession(t, ctrl)
		defer ses.Close()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		setPu("", pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx, "")
		ses.rm = rm

		tenant := &TenantInfo{
			Tenant:        "test_tenant",
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      1,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}
		ses.SetTenantInfo(tenant)
		ses.mrs = &MysqlResultSet{}

		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(1))

		// Setup mock result for publication info query
		pubQuerySQL := fmt.Sprintf(`SELECT account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			FROM mo_catalog.mo_pubs 
			WHERE account_name = '%s' AND pub_name = '%s'`, "pub_account", "test_pub")
		bh.sql2result[pubQuerySQL] = newMrsForPublicationInfo(
			uint64(100), "pub_account", "test_pub", "test_db", uint64(1), "*", "test_tenant,all",
		)

		// Setup mock result for mo_indexes query without snapshot (current timestamp)
		indexSQL := fmt.Sprintf("SELECT table_id, name, algo_table_type, index_table_name FROM mo_catalog.mo_indexes WHERE table_id = %d", 12345)
		bh.sql2result[indexSQL] = newMrsForMoIndexes([][]interface{}{
			{uint64(12345), "idx_primary", "", ""},
		})

		ic := &InternalCmdGetMoIndexes{
			tableId:                 12345,
			subscriptionAccountName: "pub_account",
			publicationName:         "test_pub",
			snapshotName:            "-", // Use "-" to indicate no snapshot
		}

		execCtx := &ExecCtx{
			reqCtx: ctx,
			ses:    ses,
		}

		err := handleGetMoIndexes(ses, execCtx, ic)
		convey.So(err, convey.ShouldBeNil)

		// Verify result set
		mrs := ses.GetMysqlResultSet()
		convey.So(mrs.GetColumnCount(), convey.ShouldEqual, uint64(4))
		convey.So(mrs.GetRowCount(), convey.ShouldEqual, uint64(1))
	})
}

// Test_handleInternalGetDdl_GoodPath tests the good path of handleInternalGetDdl
func Test_handleInternalGetDdl_GoodPath(t *testing.T) {
	convey.Convey("handleInternalGetDdl good path", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newTestSession(t, ctrl)
		defer ses.Close()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		setPu("", pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx, "")
		ses.rm = rm

		tenant := &TenantInfo{
			Tenant:        "test_tenant",
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      1,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}
		ses.SetTenantInfo(tenant)
		ses.mrs = &MysqlResultSet{}

		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(1))

		// Setup mock engine and txn handler
		mockEng := mock_frontend.NewMockEngine(ctrl)
		mockEng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		mockTxnOp := mock_frontend.NewMockTxnOperator(ctrl)
		mockTxnOp.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		mockTxnOp.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
		mockTxnOp.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		mockTxnOp.EXPECT().TryEnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()
		mockTxnOp.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()
		mockTxnOp.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).Return().AnyTimes()
		mockTxnOp.EXPECT().GetWorkspace().Return(newTestWorkspace()).AnyTimes()
		mockTxnOp.EXPECT().NextSequence().Return(uint64(0)).AnyTimes()

		// Setup TxnHandler with mock engine and txn
		txnHandler := InitTxnHandler("", mockEng, ctx, mockTxnOp)
		ses.txnHandler = txnHandler

		// Setup mock result for publication info query
		pubQuerySQL := fmt.Sprintf(`SELECT account_id, account_name, pub_name, database_name, database_id, table_list, account_list 
			FROM mo_catalog.mo_pubs 
			WHERE account_name = '%s' AND pub_name = '%s'`, "pub_account", "test_pub")
		bh.sql2result[pubQuerySQL] = newMrsForPublicationInfo(
			uint64(100), "pub_account", "test_pub", "test_db", uint64(1), "*", "test_tenant,all",
		)

		// Setup mock result for snapshot record query
		snapshotRecordSQL := fmt.Sprintf("select * from mo_catalog.mo_snapshots where sname = '%s'", "test_snapshot")
		bh.sql2result[snapshotRecordSQL] = newMrsForSnapshotRecord(
			"snap-001", "test_snapshot", int64(1234567890), "table", "sys", "test_db", "test_table", uint64(1),
		)

		// Stub ComputeDdlBatchWithSnapshotFunc to return a mock batch
		mp := ses.GetMemPool()
		mockBatch := newDdlBatchForTest(mp, [][]interface{}{
			{"test_db", "test_table", int64(100), "CREATE TABLE test_table (id INT)"},
		})

		ddlStub := gostub.Stub(&ComputeDdlBatchWithSnapshotFunc, func(
			ctx context.Context,
			databaseName string,
			tableName string,
			eng engine.Engine,
			mp *mpool.MPool,
			txnOp TxnOperator,
			snapshotTs int64,
		) (*batch.Batch, error) {
			return mockBatch, nil
		})
		defer ddlStub.Reset()

		ic := &InternalCmdGetDdl{
			snapshotName:            "test_snapshot",
			subscriptionAccountName: "pub_account",
			publicationName:         "test_pub",
			level:                   "table",
			dbName:                  "test_db",
			tableName:               "test_table",
		}

		execCtx := &ExecCtx{
			reqCtx: ctx,
			ses:    ses,
		}

		err := handleInternalGetDdl(ses, execCtx, ic)
		convey.So(err, convey.ShouldBeNil)

		// Verify result set
		mrs := ses.GetMysqlResultSet()
		convey.So(mrs.GetColumnCount(), convey.ShouldEqual, uint64(4))
		convey.So(mrs.GetRowCount(), convey.ShouldEqual, uint64(1))
	})
}

// newDdlBatchForTest creates a batch for DDL test
// columns: dbname, tablename, tableid, tablesql
func newDdlBatchForTest(mp *mpool.MPool, records [][]interface{}) *batch.Batch {
	bat := batch.New([]string{"dbname", "tablename", "tableid", "tablesql"})
	bat.Vecs = []*vector.Vector{
		vector.NewVec(types.T_varchar.ToType()),
		vector.NewVec(types.T_varchar.ToType()),
		vector.NewVec(types.T_int64.ToType()),
		vector.NewVec(types.T_varchar.ToType()),
	}

	for _, record := range records {
		_ = vector.AppendBytes(bat.Vecs[0], []byte(record[0].(string)), false, mp)
		_ = vector.AppendBytes(bat.Vecs[1], []byte(record[1].(string)), false, mp)
		_ = vector.AppendFixed[int64](bat.Vecs[2], record[2].(int64), false, mp)
		_ = vector.AppendBytes(bat.Vecs[3], []byte(record[3].(string)), false, mp)
	}
	bat.SetRowCount(len(records))

	return bat
}

// TestDataBranchAuditFkDepsEscapesQuotedNames verifies every FK dependency
// lookup used by CLONE, snapshot restore, and PITR restore. Legal quoted
// identifiers must survive the SQL literal boundary and still produce the
// dependency order consumed by the restore path (issue #26144).
func TestDataBranchAuditFkDepsEscapesQuotedNames(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	defer ses.Close()

	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	pu.SV.SetDefaultValues()
	setPu("", pu)
	ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
	rm, _ := NewRoutineManager(ctx, "")
	ses.rm = rm

	tenant := &TenantInfo{
		Tenant:        sysAccountName,
		User:          rootName,
		DefaultRole:   moAdminRoleName,
		TenantID:      sysAccountID,
		UserID:        rootID,
		DefaultRoleID: moAdminRoleID,
	}
	ses.SetTenantInfo(tenant)
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))

	const (
		dbName  = `db'name\part`
		tblName = `child'name\part`
		refDB   = `parent'db`
		refTbl  = `parent'table`
		baseSQL = "select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys"
		filters = ` where db_name = 'db''name\\part' and table_name = 'child''name\\part'`
	)
	wantOrder := []string{genKey(refDB, refTbl), genKey(dbName, tblName)}
	result := newMrsForPitrRecord([][]interface{}{{dbName, tblName, refDB, refTbl}})

	tests := []struct {
		name string
		sql  string
		run  func(*backgroundExecTest) ([]string, error)
	}{{
		name: "clone and snapshot",
		sql:  baseSQL + filters,
		run: func(bh *backgroundExecTest) ([]string, error) {
			return fkTablesTopoSort(ctx, bh, nil, dbName, tblName, nil)
		},
	}, {
		name: "pitr restore",
		sql:  baseSQL + " {MO_TS = 42}" + filters,
		run: func(bh *backgroundExecTest) ([]string, error) {
			return fkTablesTopoSortInPitrRestore(ctx, bh, 42, dbName, tblName, nil)
		},
	}, {
		name: "cross-account snapshot restore",
		sql:  baseSQL + " {MO_TS = 42}" + filters,
		run: func(bh *backgroundExecTest) ([]string, error) {
			return fkTablesTopoSortWithTS(ctx, bh, dbName, tblName, 42, 7, 8, nil)
		},
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bh := &backgroundExecTest{}
			bh.init()
			bh.sql2result[tc.sql] = result

			got, err := tc.run(bh)
			require.NoError(t, err)
			require.Equal(t, wantOrder, got)
			require.Equal(t, []string{tc.sql}, bh.executedSQLs)
		})
	}
}
