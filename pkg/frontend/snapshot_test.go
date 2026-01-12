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
