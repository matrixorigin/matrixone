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
	"fmt"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
)

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
		bh.sql2result[fmt.Sprintf(checkDatabaseIsMasterFormat, dbName)] = newMrsForRestoreStringRows([]string{"db_name"}, nil)
		bh.sql2result[fmt.Sprintf(getPubInfoSql, uint32(sysAccountID))+" and database_name = 'db1'"] = newMrsForRestoreStringRows([]string{"account_id"}, nil)
		bh.sql2result[buildTableInfoListSQL(dbName, "", snapshotTs, uint32(sysAccountID))] =
			newMrsForRestoreStringRows([]string{"relname", "table_type", "relkind"}, [][]interface{}{
				{"base_t", "BASE TABLE", "r"},
				{"hive_ext", "BASE TABLE", catalog.SystemExternalRel},
			})
		bh.sql2result[fmt.Sprintf("show create table `%s`.`base_t` {MO_TS = %d}", dbName, snapshotTs)] =
			newMrsForRestoreStringRows([]string{"Table", "Create Table"}, [][]interface{}{{"base_t", "create table base_t (id int)"}})
		bh.sql2result[fmt.Sprintf("show create table `%s`.`hive_ext` {MO_TS = %d}", dbName, snapshotTs)] =
			newMrsForRestoreStringRows([]string{"Table", "Create Table"}, [][]interface{}{{"hive_ext", "create external table hive_ext (id int)"}})
		bh.sql2result[fmt.Sprintf(checkTableIsMasterFormat, dbName, "base_t")] = newMrsForRestoreStringRows([]string{"db_name"}, nil)

		err := restoreToDatabaseOrTable(ctx, "", bh, snapshotName, dbName, "", uint32(sysAccountID), map[string]*tableInfo{}, map[string]*tableInfo{}, snapshotTs, uint32(sysAccountID), false, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(restoreTestExecutedSQLContains(bh, fmt.Sprintf(restoreTableDataByTsFmt, dbName, "base_t", dbName, "base_t", snapshotTs)), convey.ShouldBeTrue)
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
			newMrsForRestoreStringRows([]string{"relname", "table_type", "relkind"}, [][]interface{}{{tblName, "BASE TABLE", catalog.SystemExternalRel}})
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
		bh.sql2result[fmt.Sprintf(checkDatabaseIsMasterFormat, dbName)] = newMrsForRestoreStringRows([]string{"db_name"}, nil)
		bh.sql2result[fmt.Sprintf(getPubInfoSql, toAccount)+" and database_name = 'db1'"] = newMrsForRestoreStringRows([]string{"account_id"}, nil)
		bh.sql2result[buildTableInfoListSQL(dbName, "", snapshotTs, fromAccount)] =
			newMrsForRestoreStringRows([]string{"relname", "table_type", "relkind"}, [][]interface{}{
				{"base_t", "BASE TABLE", "r"},
				{"hive_ext", "BASE TABLE", catalog.SystemExternalRel},
			})
		bh.sql2result[fmt.Sprintf("show create table `%s`.`base_t` {MO_TS = %d}", dbName, snapshotTs)] =
			newMrsForRestoreStringRows([]string{"Table", "Create Table"}, [][]interface{}{{"base_t", "create table base_t (id int)"}})
		bh.sql2result[fmt.Sprintf("show create table `%s`.`hive_ext` {MO_TS = %d}", dbName, snapshotTs)] =
			newMrsForRestoreStringRows([]string{"Table", "Create Table"}, [][]interface{}{{"hive_ext", "create external table hive_ext (id int)"}})

		err := restoreDatabaseFromTS(ctx, "", bh, dbName, snapshotTs, fromAccount, toAccount, map[string]*tableInfo{}, map[string]*tableInfo{}, false, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(restoreTestExecutedSQLContains(bh, fmt.Sprintf(restoreTableDataByTsFmt, dbName, "base_t", dbName, "base_t", snapshotTs)), convey.ShouldBeTrue)
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
		bh.sql2result[fmt.Sprintf(checkDatabaseIsMasterFormat, dbName)] = newMrsForRestoreStringRows([]string{"db_name"}, nil)
		bh.sql2result[fmt.Sprintf(getPubInfoSql, uint32(sysAccountID))+" and database_name = 'db1'"] = newMrsForRestoreStringRows([]string{"account_id"}, nil)
		bh.sql2result[buildTableInfoListSQL(dbName, "", ts, uint32(sysAccountID))] =
			newMrsForRestoreStringRows([]string{"relname", "table_type", "relkind"}, [][]interface{}{
				{"base_t", "BASE TABLE", "r"},
				{"hive_ext", "BASE TABLE", catalog.SystemExternalRel},
			})
		bh.sql2result[fmt.Sprintf("show create table `%s`.`base_t` {MO_TS = %d}", dbName, ts)] =
			newMrsForRestoreStringRows([]string{"Table", "Create Table"}, [][]interface{}{{"base_t", "create table base_t (id int)"}})
		bh.sql2result[fmt.Sprintf("show create table `%s`.`hive_ext` {MO_TS = %d}", dbName, ts)] =
			newMrsForRestoreStringRows([]string{"Table", "Create Table"}, [][]interface{}{{"hive_ext", "create external table hive_ext (id int)"}})
		bh.sql2result[fmt.Sprintf(checkTableIsMasterFormat, dbName, "base_t")] = newMrsForRestoreStringRows([]string{"db_name"}, nil)
		bh.sql2result[getPubInfoWithPitr(ts, uint32(sysAccountID), dbName)] = newMrsForRestoreStringRows([]string{"account_id"}, nil)

		err := restoreToDatabaseOrTableWithPitr(ctx, "", bh, pitrName, ts, dbName, "", map[string]*tableInfo{}, map[string]*tableInfo{}, uint32(sysAccountID))
		convey.So(err, convey.ShouldBeNil)
		convey.So(restoreTestExecutedSQLContains(bh, fmt.Sprintf(restoreTableDataByTsFmt, dbName, "base_t", dbName, "base_t", ts)), convey.ShouldBeTrue)
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
			newMrsForRestoreStringRows([]string{"relname", "table_type", "relkind"}, [][]interface{}{{tblName, "BASE TABLE", catalog.SystemExternalRel}})
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
	sql := buildTableInfoListSQL("db'name", "tbl'name", 0, uint32(sysAccountID))
	if !strings.Contains(sql, "reldatabase = 'db''name'") {
		t.Fatalf("database name was not escaped in SQL: %s", sql)
	}
	if !strings.Contains(sql, "relname like 'tbl''name'") {
		t.Fatalf("table name was not escaped in SQL: %s", sql)
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
		newMrsForRestoreStringRows([]string{"relname", "table_type", "relkind"}, [][]interface{}{
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
		newMrsForRestoreStringRows([]string{"relname", "table_type", "relkind"}, [][]interface{}{
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
		mrs.AddRow(row)
	}
	return mrs
}

func restoreTestExecutedSQLContains(bh *backgroundExecTest, needle string) bool {
	for _, sql := range bh.executedSQLs {
		if strings.Contains(sql, needle) {
			return true
		}
	}
	return false
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

		_, err := fkTablesTopoSortWithTS(ctx, bh, "", "", 0, 0, 0)
		convey.So(err, convey.ShouldNotBeNil)

		sql := "select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys"
		mrs := newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err = fkTablesTopoSortWithTS(ctx, bh, "", "", 0, 0, 0)
		convey.So(err, convey.ShouldBeNil)

		sql = "select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys"
		mrs = newMrsForPitrRecord([][]interface{}{{"db1", "table1", "db2", "table2"}})
		bh.sql2result[sql] = mrs
		_, err = fkTablesTopoSortWithTS(ctx, bh, "", "", 0, 0, 0)
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
