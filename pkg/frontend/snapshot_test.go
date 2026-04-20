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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
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
		mockTxnOp.EXPECT().EnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(0)).AnyTimes()
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
