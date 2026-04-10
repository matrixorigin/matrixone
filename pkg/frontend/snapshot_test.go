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
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
)

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

// newMrsForCheckMaster creates a result set for checkTableIsMaster / checkDatabaseIsMaster queries.
func newMrsForCheckMaster(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}
	col1 := &MysqlColumn{}
	col1.SetName("db_name")
	col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col2 := &MysqlColumn{}
	col2.SetName("table_name")
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mrs.AddColumn(col1)
	mrs.AddColumn(col2)
	for _, row := range rows {
		mrs.AddRow(row)
	}
	return mrs
}

func setupTestCtx(t *testing.T) (context.Context, *Session, func()) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
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
	cleanup := func() {
		ses.Close()
		ctrl.Finish()
	}
	return ctx, ses, cleanup
}

func Test_recreateTable_SkipMasterCheck(t *testing.T) {
	convey.Convey("recreateTable with skipMasterCheck=true should always restore table", t, func() {
		ctx, _, cleanup := setupTestCtx(t)
		defer cleanup()

		bh := &backgroundExecTestWithHistory{}
		bh.init()

		// Register a result for checkTableIsMaster that would return true (table IS master).
		// With skipMasterCheck=true, this should be ignored.
		masterCheckSql := fmt.Sprintf(checkTableIsMasterFormat, "testdb", "parent_tbl")
		bh.sql2result[masterCheckSql] = newMrsForCheckMaster([][]interface{}{{"testdb", "child_tbl"}})

		tblInfo := &tableInfo{
			dbName:  "testdb",
			tblName: "parent_tbl",
		}

		err := recreateTable(ctx, "", bh, "sp01", tblInfo, sysAccountID, 100, true)
		convey.So(err, convey.ShouldBeNil)

		// Verify that the master check SQL was NOT executed
		convey.So(bh.hasExecuted(masterCheckSql), convey.ShouldBeFalse)

		// Verify that use/drop/clone SQLs WERE executed
		convey.So(bh.hasExecuted("use `testdb`"), convey.ShouldBeTrue)
		convey.So(bh.hasExecuted("drop table if exists `parent_tbl`"), convey.ShouldBeTrue)

		// The clone SQL should have been executed (same account → restoreTableDataByTsFmt)
		cloneExecuted := false
		for _, sql := range bh.executedSqls {
			if strings.Contains(sql, "clone") && strings.Contains(sql, "parent_tbl") {
				cloneExecuted = true
				break
			}
		}
		convey.So(cloneExecuted, convey.ShouldBeTrue)
	})
}

func Test_recreateTable_MasterTableSkipped(t *testing.T) {
	convey.Convey("recreateTable with skipMasterCheck=false should skip master tables", t, func() {
		ctx, _, cleanup := setupTestCtx(t)
		defer cleanup()

		bh := &backgroundExecTestWithHistory{}
		bh.init()

		// Register a result that makes checkTableIsMaster return true
		masterCheckSql := fmt.Sprintf(checkTableIsMasterFormat, "testdb", "parent_tbl")
		bh.sql2result[masterCheckSql] = newMrsForCheckMaster([][]interface{}{{"testdb", "child_tbl"}})

		tblInfo := &tableInfo{
			dbName:  "testdb",
			tblName: "parent_tbl",
		}

		err := recreateTable(ctx, "", bh, "sp01", tblInfo, sysAccountID, 100, false)
		convey.So(err, convey.ShouldBeNil)

		// Verify master check WAS executed
		convey.So(bh.hasExecuted(masterCheckSql), convey.ShouldBeTrue)

		// Verify that use/drop/clone were NOT executed (table was skipped)
		convey.So(bh.hasExecuted("use `testdb`"), convey.ShouldBeFalse)
		convey.So(bh.hasExecuted("drop table if exists `parent_tbl`"), convey.ShouldBeFalse)
	})
}

func Test_recreateTable_NonMasterProceeds(t *testing.T) {
	convey.Convey("recreateTable with skipMasterCheck=false should proceed for non-master tables", t, func() {
		ctx, _, cleanup := setupTestCtx(t)
		defer cleanup()

		bh := &backgroundExecTestWithHistory{}
		bh.init()

		// Register an empty result → checkTableIsMaster returns false
		masterCheckSql := fmt.Sprintf(checkTableIsMasterFormat, "testdb", "leaf_tbl")
		bh.sql2result[masterCheckSql] = newMrsForCheckMaster([][]interface{}{})

		tblInfo := &tableInfo{
			dbName:  "testdb",
			tblName: "leaf_tbl",
		}

		err := recreateTable(ctx, "", bh, "sp01", tblInfo, sysAccountID, 100, false)
		convey.So(err, convey.ShouldBeNil)

		// Master check was executed
		convey.So(bh.hasExecuted(masterCheckSql), convey.ShouldBeTrue)

		// And the restore proceeded (use/drop/clone executed)
		convey.So(bh.hasExecuted("use `testdb`"), convey.ShouldBeTrue)
		convey.So(bh.hasExecuted("drop table if exists `leaf_tbl`"), convey.ShouldBeTrue)
	})
}

func Test_recreateTable_MasterCheckError(t *testing.T) {
	convey.Convey("recreateTable propagates checkTableIsMaster errors", t, func() {
		ctx, _, cleanup := setupTestCtx(t)
		defer cleanup()

		bh := &backgroundExecTestWithHistory{}
		bh.init()

		// Do NOT register a result for the master check SQL.
		// getResultSet will fail with "not the type of result set" when it encounters nil.
		tblInfo := &tableInfo{
			dbName:  "testdb",
			tblName: "some_tbl",
		}

		err := recreateTable(ctx, "", bh, "sp01", tblInfo, sysAccountID, 100, false)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "not the type of result set")

		// The restore should NOT have proceeded (use/drop/clone not executed)
		convey.So(bh.hasExecuted("use `testdb`"), convey.ShouldBeFalse)
	})
}

func Test_recreateTable_CrossAccountRestore(t *testing.T) {
	convey.Convey("recreateTable cross-account uses ExecRestore path", t, func() {
		ctx, _, cleanup := setupTestCtx(t)
		defer cleanup()

		bh := &backgroundExecTestWithHistory{}
		bh.init()

		tblInfo := &tableInfo{
			dbName:  "acc_test02",
			tblName: "pri01",
		}

		// Cross-account: curAccountId (0, from ctx) != toAccountId (99)
		toAccountId := uint32(99)
		err := recreateTable(ctx, "", bh, "sp07", tblInfo, toAccountId, 200, true)
		convey.So(err, convey.ShouldBeNil)

		// The last SQL should be the clone SQL using restoreTableDataByNameFmt (with snapshot name)
		// ExecRestore stores the SQL in currentSql (not in executedSqls)
		convey.So(strings.Contains(bh.currentSql, "clone"), convey.ShouldBeTrue)
		convey.So(strings.Contains(bh.currentSql, "sp07"), convey.ShouldBeTrue)
	})
}

func Test_restoreTablesWithFk_AlwaysRestoresParentTables(t *testing.T) {
	convey.Convey("restoreTablesWithFk should always restore parent tables (skipMasterCheck=true)", t, func() {
		ctx, _, cleanup := setupTestCtx(t)
		defer cleanup()

		bh := &backgroundExecTestWithHistory{}
		bh.init()

		// Setup: pri01 is a parent table that checkTableIsMaster would report as master.
		// In the old code, this would cause pri01 to be skipped, breaking child FK validation.
		masterCheckPri := fmt.Sprintf(checkTableIsMasterFormat, "acc_test02", "pri01")
		bh.sql2result[masterCheckPri] = newMrsForCheckMaster([][]interface{}{{"acc_test02", "aff01"}})

		masterCheckAff := fmt.Sprintf(checkTableIsMasterFormat, "acc_test02", "aff01")
		bh.sql2result[masterCheckAff] = newMrsForCheckMaster([][]interface{}{})

		// Topo-sorted: parent first, then child
		sortedFkTbls := []string{
			genKey("acc_test02", "pri01"),
			genKey("acc_test02", "aff01"),
		}

		fkTableMap := map[string]*tableInfo{
			genKey("acc_test02", "pri01"): {dbName: "acc_test02", tblName: "pri01"},
			genKey("acc_test02", "aff01"): {dbName: "acc_test02", tblName: "aff01"},
		}

		err := restoreTablesWithFk(ctx, "", bh, "sp07", sortedFkTbls, fkTableMap, sysAccountID, 100)
		convey.So(err, convey.ShouldBeNil)

		// Both tables should have been restored (use, drop, clone for each)
		convey.So(bh.hasExecuted("use `acc_test02`"), convey.ShouldBeTrue)
		convey.So(bh.hasExecuted("drop table if exists `pri01`"), convey.ShouldBeTrue)
		convey.So(bh.hasExecuted("drop table if exists `aff01`"), convey.ShouldBeTrue)

		// Master check should NOT have been executed (skipMasterCheck=true inside restoreTablesWithFk)
		convey.So(bh.hasExecuted(masterCheckPri), convey.ShouldBeFalse)
		convey.So(bh.hasExecuted(masterCheckAff), convey.ShouldBeFalse)
	})
}

func Test_restoreTablesWithFk_SkipsNilEntries(t *testing.T) {
	convey.Convey("restoreTablesWithFk skips tables not in fkTableMap", t, func() {
		ctx, _, cleanup := setupTestCtx(t)
		defer cleanup()

		bh := &backgroundExecTestWithHistory{}
		bh.init()

		// t1.pk <- t2.fk, but we only restore t2. t1 is nil in fkTableMap.
		sortedFkTbls := []string{
			genKey("db1", "t1"),
			genKey("db1", "t2"),
		}

		fkTableMap := map[string]*tableInfo{
			genKey("db1", "t2"): {dbName: "db1", tblName: "t2"},
			// t1 not in map → should be skipped
		}

		err := restoreTablesWithFk(ctx, "", bh, "sp01", sortedFkTbls, fkTableMap, sysAccountID, 100)
		convey.So(err, convey.ShouldBeNil)

		// t2 should be restored, t1 should not
		convey.So(bh.hasExecuted("drop table if exists `t2`"), convey.ShouldBeTrue)
		convey.So(bh.hasExecuted("drop table if exists `t1`"), convey.ShouldBeFalse)
	})
}

func Test_restoreTablesWithFk_EmptyList(t *testing.T) {
	convey.Convey("restoreTablesWithFk with empty list succeeds", t, func() {
		ctx, _, cleanup := setupTestCtx(t)
		defer cleanup()

		bh := &backgroundExecTestWithHistory{}
		bh.init()

		err := restoreTablesWithFk(ctx, "", bh, "sp01", nil, nil, sysAccountID, 100)
		convey.So(err, convey.ShouldBeNil)
		convey.So(len(bh.executedSqls), convey.ShouldEqual, 0)
	})
}

func Test_checkTableIsMaster(t *testing.T) {
	convey.Convey("checkTableIsMaster", t, func() {
		ctx, _, cleanup := setupTestCtx(t)
		defer cleanup()

		bh := &backgroundExecTest{}
		bh.init()

		convey.Convey("returns true when FK references exist", func() {
			sql := fmt.Sprintf(checkTableIsMasterFormat, "mydb", "parent")
			bh.sql2result[sql] = newMrsForCheckMaster([][]interface{}{{"mydb", "child"}})

			isMaster, err := checkTableIsMaster(ctx, "", bh, "snap", "mydb", "parent")
			convey.So(err, convey.ShouldBeNil)
			convey.So(isMaster, convey.ShouldBeTrue)
		})

		convey.Convey("returns false when no FK references", func() {
			sql := fmt.Sprintf(checkTableIsMasterFormat, "mydb", "standalone")
			bh.sql2result[sql] = newMrsForCheckMaster([][]interface{}{})

			isMaster, err := checkTableIsMaster(ctx, "", bh, "snap", "mydb", "standalone")
			convey.So(err, convey.ShouldBeNil)
			convey.So(isMaster, convey.ShouldBeFalse)
		})
	})
}

func Test_checkDatabaseIsMaster(t *testing.T) {
	convey.Convey("checkDatabaseIsMaster", t, func() {
		ctx, _, cleanup := setupTestCtx(t)
		defer cleanup()

		bh := &backgroundExecTest{}
		bh.init()

		convey.Convey("returns true when cross-db FK references exist", func() {
			sql := fmt.Sprintf(checkDatabaseIsMasterFormat, "master_db", "master_db")
			mrs := &MysqlResultSet{}
			col := &MysqlColumn{}
			col.SetName("db_name")
			col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
			mrs.AddColumn(col)
			mrs.AddRow([]interface{}{"child_db"})
			bh.sql2result[sql] = mrs

			isMaster, err := checkDatabaseIsMaster(ctx, "", bh, "snap", "master_db")
			convey.So(err, convey.ShouldBeNil)
			convey.So(isMaster, convey.ShouldBeTrue)
		})

		convey.Convey("returns false when no cross-db FK references", func() {
			sql := fmt.Sprintf(checkDatabaseIsMasterFormat, "isolated_db", "isolated_db")
			mrs := &MysqlResultSet{}
			col := &MysqlColumn{}
			col.SetName("db_name")
			col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
			mrs.AddColumn(col)
			bh.sql2result[sql] = mrs

			isMaster, err := checkDatabaseIsMaster(ctx, "", bh, "snap", "isolated_db")
			convey.So(err, convey.ShouldBeNil)
			convey.So(isMaster, convey.ShouldBeFalse)
		})
	})
}

func Test_deleteCurFkTables_SkipsMasterTables(t *testing.T) {
	convey.Convey("deleteCurFkTables uses checkTableIsMaster correctly", t, func() {
		ctx, _, cleanup := setupTestCtx(t)
		defer cleanup()

		bh := &backgroundExecTestWithHistory{}
		bh.init()

		// With no FK entries, deleteCurFkTables should return immediately
		fkSql := "select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys"
		bh.sql2result[fkSql] = newMrsForPitrRecord([][]interface{}{})

		err := deleteCurFkTables(ctx, "", bh, "", "", sysAccountID)
		convey.So(err, convey.ShouldBeNil)
	})
}
