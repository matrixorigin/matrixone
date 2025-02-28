// Copyright 2025 Matrix Origin
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
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
)

func Test_getAccountPitrRecords(t *testing.T) {
	convey.Convey("getAccountPitrRecords", t, func() {
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

		_, err := getAccountPitrRecords(ctx, ses, bh, "sys")
		convey.So(err, convey.ShouldNotBeNil)

		sql := "select pitr_name, modified_time,pitr_length, pitr_unit, pitr_status, pitr_status_changed_time from MO_CATALOG.MO_PITR where create_account = 0 and account_name = 'sys' and pitr_name != 'sys_mo_catalog_pitr' and level = 'account'"
		mrs := newMrsForPitrRecord([][]interface{}{{"pitr_01", "2025-01-13 00:00:00", uint64(1), "h", uint64(1), "2025-01-13 00:00:00"}})
		bh.sql2result[sql] = mrs

		_, err = getAccountPitrRecords(ctx, ses, bh, "sys")
		convey.So(err, convey.ShouldBeNil)

		sql = "select pitr_name, modified_time,pitr_length, pitr_unit, pitr_status, pitr_status_changed_time from MO_CATALOG.MO_PITR where create_account = 0 and account_name = 'sys' and pitr_name != 'sys_mo_catalog_pitr' and level = 'account'"
		mrs = newMrsForPitrRecord([][]interface{}{{types.Day_Hour, "2025-01-13 00:00:00", uint64(1), "h", uint64(1), "2025-01-13 00:00:00"}})
		bh.sql2result[sql] = mrs

		_, err = getAccountPitrRecords(ctx, ses, bh, "sys")
		convey.So(err, convey.ShouldNotBeNil)

		sql = "select pitr_name, modified_time,pitr_length, pitr_unit, pitr_status, pitr_status_changed_time from MO_CATALOG.MO_PITR where create_account = 0 and account_name = 'sys' and pitr_name != 'sys_mo_catalog_pitr' and level = 'account'"
		mrs = newMrsForPitrRecord([][]interface{}{{"pitr_01", types.Day_Hour, uint64(1), "h", uint64(1), "2025-01-13 00:00:00"}})
		bh.sql2result[sql] = mrs

		_, err = getAccountPitrRecords(ctx, ses, bh, "sys")
		convey.So(err, convey.ShouldNotBeNil)

		sql = "select pitr_name, modified_time,pitr_length, pitr_unit, pitr_status, pitr_status_changed_time from MO_CATALOG.MO_PITR where create_account = 0 and account_name = 'sys' and pitr_name != 'sys_mo_catalog_pitr' and level = 'account'"
		mrs = newMrsForPitrRecord([][]interface{}{{"pitr_01", "2025-01-13 00:00:00", types.Day_Hour, "h", uint64(1), "2025-01-13 00:00:00"}})
		bh.sql2result[sql] = mrs

		_, err = getAccountPitrRecords(ctx, ses, bh, "sys")
		convey.So(err, convey.ShouldNotBeNil)

		sql = "select pitr_name, modified_time,pitr_length, pitr_unit, pitr_status, pitr_status_changed_time from MO_CATALOG.MO_PITR where create_account = 0 and account_name = 'sys' and pitr_name != 'sys_mo_catalog_pitr' and level = 'account'"
		mrs = newMrsForPitrRecord([][]interface{}{{"pitr_01", "2025-01-13 00:00:00", uint64(1), types.Day_Hour, uint64(1), "2025-01-13 00:00:00"}})
		bh.sql2result[sql] = mrs

		_, err = getAccountPitrRecords(ctx, ses, bh, "sys")
		convey.So(err, convey.ShouldNotBeNil)

		sql = "select pitr_name, modified_time,pitr_length, pitr_unit, pitr_status, pitr_status_changed_time from MO_CATALOG.MO_PITR where create_account = 0 and account_name = 'sys' and pitr_name != 'sys_mo_catalog_pitr' and level = 'account'"
		mrs = newMrsForPitrRecord([][]interface{}{{"pitr_01", "2025-01-13 00:00:00", uint64(1), "h", types.Day_Hour, "2025-01-13 00:00:00"}})
		bh.sql2result[sql] = mrs

		_, err = getAccountPitrRecords(ctx, ses, bh, "sys")
		convey.So(err, convey.ShouldNotBeNil)

		sql = "select pitr_name, modified_time,pitr_length, pitr_unit, pitr_status, pitr_status_changed_time from MO_CATALOG.MO_PITR where create_account = 0 and account_name = 'sys' and pitr_name != 'sys_mo_catalog_pitr' and level = 'account'"
		mrs = newMrsForPitrRecord([][]interface{}{{"pitr_01", "2025-01-13 00:00:00", uint64(1), "h", uint64(1), types.Day_Hour}})
		bh.sql2result[sql] = mrs

		_, err = getAccountPitrRecords(ctx, ses, bh, "sys")
		convey.So(err, convey.ShouldNotBeNil)

	})
}

func TestMarshalRcoveryWindowToJsonForSnapshot(t *testing.T) {
	// 设置测试数据
	ts := time.Now().Unix()
	records := tableRecoveryWindowForSnapshot{
		ts:           ts,
		snapshotName: "test_snapshot",
	}

	// 调用函数
	result := marshalRcoveryWindowToJsonForSnapshot(records)

	// 预期的时间格式
	expectedTime := time.Unix(0, ts).Local().Format("2006-01-02 15:04:05")
	expectedResult := `{"timestamp": "` + expectedTime + `", "source": "snapshot", "source_name": "test_snapshot"}`

	// 验证结果
	if result != expectedResult {
		t.Errorf("Expected %s, but got %s", expectedResult, result)
	}
}

func Test_GetTableRecoveryWindowRows(t *testing.T) {
	convey.Convey("getAccountPitrRecords", t, func() {
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

		_, err := getTableRecoveryWindowRows(ctx, ses, bh, "sys", "db1", "table1")
		convey.So(err, convey.ShouldNotBeNil)

		sql := fmt.Sprintf(getTablePitrRecordsFormat, 0, "sys", "db1", "table1", SYSMOCATALOGPITR)
		mrs := newMrsForPitrRecord([][]interface{}{{"pitr_01", "2025-01-13 00:00:00", uint64(1), "h", uint64(1), "2025-01-13 00:00:00"}})
		bh.sql2result[sql] = mrs

		_, err = getTableRecoveryWindowRows(ctx, ses, bh, "sys", "db1", "table1")
		convey.So(err, convey.ShouldNotBeNil)

		sql = fmt.Sprintf(getTablePitrRecordsFormat, 0, "sys", "db1", "table1", SYSMOCATALOGPITR)
		mrs = newMrsForPitrRecord([][]interface{}{{types.Date(1), "2025-01-13 00:00:00", uint64(1), "h", uint64(1), "2025-01-13 00:00:00"}})
		bh.sql2result[sql] = mrs
		_, err = getTableRecoveryWindowRows(ctx, ses, bh, "sys", "db1", "table1")
		convey.So(err, convey.ShouldNotBeNil)

		sql = fmt.Sprintf(getTablePitrRecordsFormat, 0, "sys", "db1", "table1", SYSMOCATALOGPITR)
		mrs = newMrsForPitrRecord([][]interface{}{{"pitr_01", types.Date(1), uint64(1), "h", uint64(1), "2025-01-13 00:00:00"}})
		bh.sql2result[sql] = mrs
		_, err = getTableRecoveryWindowRows(ctx, ses, bh, "sys", "db1", "table1")
		convey.So(err, convey.ShouldNotBeNil)

		sql = fmt.Sprintf(getTablePitrRecordsFormat, 0, "sys", "db1", "table1", SYSMOCATALOGPITR)
		mrs = newMrsForPitrRecord([][]interface{}{{"pitr_01", "2025-01-13 00:00:00", "uint64(1)", "h", uint64(1), "2025-01-13 00:00:00"}})
		bh.sql2result[sql] = mrs
		_, err = getTableRecoveryWindowRows(ctx, ses, bh, "sys", "db1", "table1")
		convey.So(err, convey.ShouldNotBeNil)

		sql = fmt.Sprintf(getTablePitrRecordsFormat, 0, "sys", "db1", "table1", SYSMOCATALOGPITR)
		mrs = newMrsForPitrRecord([][]interface{}{{"pitr_01", "2025-01-13 00:00:00", uint64(1), types.Date(1), uint64(1), "2025-01-13 00:00:00"}})
		bh.sql2result[sql] = mrs
		_, err = getTableRecoveryWindowRows(ctx, ses, bh, "sys", "db1", "table1")
		convey.So(err, convey.ShouldNotBeNil)

		sql = fmt.Sprintf(getTablePitrRecordsFormat, 0, "sys", "db1", "table1", SYSMOCATALOGPITR)
		mrs = newMrsForPitrRecord([][]interface{}{{"pitr_01", "2025-01-13 00:00:00", uint64(1), "h", "uint64(1)", "2025-01-13 00:00:00"}})
		bh.sql2result[sql] = mrs
		_, err = getTableRecoveryWindowRows(ctx, ses, bh, "sys", "db1", "table1")
		convey.So(err, convey.ShouldNotBeNil)

		sql = fmt.Sprintf(getTablePitrRecordsFormat, 0, "sys", "db1", "table1", SYSMOCATALOGPITR)
		mrs = newMrsForPitrRecord([][]interface{}{{"pitr_01", "2025-01-13 00:00:00", uint64(1), "h", uint64(1), types.Day_Hour}})
		bh.sql2result[sql] = mrs
		_, err = getTableRecoveryWindowRows(ctx, ses, bh, "sys", "db1", "table1")
		convey.So(err, convey.ShouldNotBeNil)

		sql = fmt.Sprintf(getTableSnapshotRecordsFormat, "sys", "db1", "table1")
		mrs = newMrsForPitrRecord([][]interface{}{{types.Date(1), int64(10000000)}})
		bh.sql2result[sql] = mrs
		_, err = getTableRecoveryWindowRows(ctx, ses, bh, "sys", "db1", "table1")
		convey.So(err, convey.ShouldNotBeNil)

		sql = fmt.Sprintf(getTableSnapshotRecordsFormat, "sys", "db1", "table1")
		mrs = newMrsForPitrRecord([][]interface{}{{"sys", "123"}})
		bh.sql2result[sql] = mrs
		_, err = getTableRecoveryWindowRows(ctx, ses, bh, "sys", "db1", "table1")
		convey.So(err, convey.ShouldNotBeNil)

		sql = fmt.Sprintf(getTablePitrRecordsFormat, 0, "sys", "db1", "table1", SYSMOCATALOGPITR)
		mrs = newMrsForPitrRecord([][]interface{}{{"pitr_01", "2025-01-13 00:00:00", uint64(1), "h", uint64(1), "2025-01-13 00:00:00"}})
		bh.sql2result[sql] = mrs
		sql = fmt.Sprintf(getTableSnapshotRecordsFormat, "sys", "db1", "table1")
		mrs = newMrsForPitrRecord([][]interface{}{{"snapshot_01", int64(10000000)}})
		bh.sql2result[sql] = mrs
		_, err = getTableRecoveryWindowRows(ctx, ses, bh, "sys", "db1", "table1")
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_GetDbRecoveryWindowRows(t *testing.T) {
	convey.Convey("GetDbRecoveryWindowRows", t, func(convey.C) {
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

		_, err := getDbRecoveryWindowRows(ctx, ses, bh, "sys", "db1")
		convey.So(err, convey.ShouldNotBeNil)

		sql := fmt.Sprintf(getDbPitrRecordsFormat, 0, "sys", "db1", SYSMOCATALOGPITR)
		mrs := newMrsForPitrRecord([][]interface{}{{"pitr_01", "2025-01-13 00:00:00", uint64(1), "h", uint64(1), "2025-01-13 00:00:00"}})
		bh.sql2result[sql] = mrs
		_, err = getDbRecoveryWindowRows(ctx, ses, bh, "sys", "db1")
		convey.So(err, convey.ShouldNotBeNil)

		sql = fmt.Sprintf(getDbPitrRecordsFormat, 0, "sys", "db1", SYSMOCATALOGPITR)
		mrs = newMrsForPitrRecord([][]interface{}{{types.Date(1), "2025-01-13 00:00:00", uint64(1), "h", uint64(1), "2025-01-13 00:00:00"}})
		bh.sql2result[sql] = mrs
		_, err = getDbRecoveryWindowRows(ctx, ses, bh, "sys", "db1")
		convey.So(err, convey.ShouldNotBeNil)

		sql = fmt.Sprintf(getDbPitrRecordsFormat, 0, "sys", "db1", SYSMOCATALOGPITR)
		mrs = newMrsForPitrRecord([][]interface{}{{"pitr_01", types.Date(1), uint64(1), "h", uint64(1), "2025-01-13 00:00:00"}})
		bh.sql2result[sql] = mrs
		_, err = getDbRecoveryWindowRows(ctx, ses, bh, "sys", "db1")
		convey.So(err, convey.ShouldNotBeNil)

		sql = fmt.Sprintf(getDbPitrRecordsFormat, 0, "sys", "db1", SYSMOCATALOGPITR)
		mrs = newMrsForPitrRecord([][]interface{}{{"pitr_01", "2025-01-13 00:00:00", uint64(1), "h", uint64(1), "2025-01-13 00:00:00"}})
		bh.sql2result[sql] = mrs
		_, err = getDbRecoveryWindowRows(ctx, ses, bh, "sys", "db1")
		convey.So(err, convey.ShouldNotBeNil)

		sql = fmt.Sprintf(getDbPitrRecordsFormat, 0, "sys", "db1", SYSMOCATALOGPITR)
		mrs = newMrsForPitrRecord([][]interface{}{{"pitr_01", "2025-01-13 00:00:00", "uint64(1)", "h", uint64(1), "2025-01-13 00:00:00"}})
		bh.sql2result[sql] = mrs
		_, err = getDbRecoveryWindowRows(ctx, ses, bh, "sys", "db1")
		convey.So(err, convey.ShouldNotBeNil)

		sql = fmt.Sprintf(getDbPitrRecordsFormat, 0, "sys", "db1", SYSMOCATALOGPITR)
		mrs = newMrsForPitrRecord([][]interface{}{{"pitr_01", "2025-01-13 00:00:00", uint64(1), types.Date(1), uint64(1), "2025-01-13 00:00:00"}})
		bh.sql2result[sql] = mrs
		_, err = getDbRecoveryWindowRows(ctx, ses, bh, "sys", "db1")
		convey.So(err, convey.ShouldNotBeNil)

		sql = fmt.Sprintf(getDbPitrRecordsFormat, 0, "sys", "db1", SYSMOCATALOGPITR)
		mrs = newMrsForPitrRecord([][]interface{}{{"pitr_01", "2025-01-13 00:00:00", uint64(1), "h", "uint64(1)", "2025-01-13 00:00:00"}})
		bh.sql2result[sql] = mrs
		_, err = getDbRecoveryWindowRows(ctx, ses, bh, "sys", "db1")
		convey.So(err, convey.ShouldNotBeNil)

		sql = fmt.Sprintf(getDbPitrRecordsFormat, 0, "sys", "db1", SYSMOCATALOGPITR)
		mrs = newMrsForPitrRecord([][]interface{}{{"pitr_01", "2025-01-13 00:00:00", uint64(1), "h", uint64(1), types.Date(1)}})
		bh.sql2result[sql] = mrs
		_, err = getDbRecoveryWindowRows(ctx, ses, bh, "sys", "db1")
		convey.So(err, convey.ShouldNotBeNil)

		sql = fmt.Sprint(getDbSnapshotRecordsFormat, "sys", "db1")
		mrs = newMrsForPitrRecord([][]interface{}{{types.Date(1), int64(10000000)}})
		bh.sql2result[sql] = mrs
		_, err = getDbRecoveryWindowRows(ctx, ses, bh, "sys", "db1")
		convey.So(err, convey.ShouldNotBeNil)

		sql = fmt.Sprint(getDbSnapshotRecordsFormat, "sys", "db1")
		mrs = newMrsForPitrRecord([][]interface{}{{"snapshot_01", types.Date(1)}})
		bh.sql2result[sql] = mrs
		_, err = getDbRecoveryWindowRows(ctx, ses, bh, "sys", "db1")
		convey.So(err, convey.ShouldNotBeNil)

		sql = fmt.Sprintf(getDbPitrRecordsFormat, 0, "sys", "db1", SYSMOCATALOGPITR)
		mrs = newMrsForPitrRecord([][]interface{}{{"pitr_01", "2025-01-13 00:00:00", uint64(1), "h", uint64(1), "2025-01-13 00:00:00"}})
		bh.sql2result[sql] = mrs
		sql = fmt.Sprint(getDbSnapshotRecordsFormat, "sys", "db1")
		mrs = newMrsForPitrRecord([][]interface{}{{"snapshot_01", int64(10000000)}})
		bh.sql2result[sql] = mrs
		_, err = getDbRecoveryWindowRows(ctx, ses, bh, "sys", "db1")
		convey.So(err, convey.ShouldNotBeNil)

		sql = "show tables from " + "db1"
		mrs = newMrsForPitrRecord([][]interface{}{{"table1"}})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf(getTablePitrRecordsFormat, 0, "sys", "db1", "table1", SYSMOCATALOGPITR)
		mrs = newMrsForPitrRecord([][]interface{}{{"pitr_01", "2025-01-13 00:00:00", uint64(1), "h", uint64(1), "2025-01-13 00:00:00"}})
		bh.sql2result[sql] = mrs
		sql = fmt.Sprintf(getTableSnapshotRecordsFormat, "sys", "db1", "table1")
		mrs = newMrsForPitrRecord([][]interface{}{{"snapshot_01", int64(10000000)}})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf(getDbPitrRecordsFormat, 0, "sys", "db1", SYSMOCATALOGPITR)
		mrs = newMrsForPitrRecord([][]interface{}{{types.Date(1), "2025-01-13 00:00:00", uint64(1), "h", uint64(1), "2025-01-13 00:00:00"}})
		bh.sql2result[sql] = mrs
		_, err = getDbRecoveryWindowRows(ctx, ses, bh, "sys", "db1")
		convey.So(err, convey.ShouldNotBeNil)

		sql = fmt.Sprintf(getDbPitrRecordsFormat, 0, "sys", "db1", SYSMOCATALOGPITR)
		mrs = newMrsForPitrRecord([][]interface{}{{"pitr_01", types.Date(1), uint64(1), "h", uint64(1), "2025-01-13 00:00:00"}})
		bh.sql2result[sql] = mrs
		_, err = getDbRecoveryWindowRows(ctx, ses, bh, "sys", "db1")
		convey.So(err, convey.ShouldNotBeNil)

		sql = fmt.Sprintf(getDbPitrRecordsFormat, 0, "sys", "db1", SYSMOCATALOGPITR)
		mrs = newMrsForPitrRecord([][]interface{}{{"pitr_01", "2025-01-13 00:00:00", uint64(1), "h", uint64(1), "2025-01-13 00:00:00"}})
		bh.sql2result[sql] = mrs
		_, err = getDbRecoveryWindowRows(ctx, ses, bh, "sys", "db1")
		convey.So(err, convey.ShouldNotBeNil)

		sql = fmt.Sprintf(getDbPitrRecordsFormat, 0, "sys", "db1", SYSMOCATALOGPITR)
		mrs = newMrsForPitrRecord([][]interface{}{{"pitr_01", "2025-01-13 00:00:00", "uint64(1)", "h", uint64(1), "2025-01-13 00:00:00"}})
		bh.sql2result[sql] = mrs
		_, err = getDbRecoveryWindowRows(ctx, ses, bh, "sys", "db1")
		convey.So(err, convey.ShouldNotBeNil)

		sql = fmt.Sprintf(getDbPitrRecordsFormat, 0, "sys", "db1", SYSMOCATALOGPITR)
		mrs = newMrsForPitrRecord([][]interface{}{{"pitr_01", "2025-01-13 00:00:00", uint64(1), types.Date(1), uint64(1), "2025-01-13 00:00:00"}})
		bh.sql2result[sql] = mrs
		_, err = getDbRecoveryWindowRows(ctx, ses, bh, "sys", "db1")
		convey.So(err, convey.ShouldNotBeNil)

		sql = fmt.Sprintf(getDbPitrRecordsFormat, 0, "sys", "db1", SYSMOCATALOGPITR)
		mrs = newMrsForPitrRecord([][]interface{}{{"pitr_01", "2025-01-13 00:00:00", uint64(1), "h", "uint64(1)", "2025-01-13 00:00:00"}})
		bh.sql2result[sql] = mrs
		_, err = getDbRecoveryWindowRows(ctx, ses, bh, "sys", "db1")
		convey.So(err, convey.ShouldNotBeNil)

		sql = fmt.Sprintf(getDbPitrRecordsFormat, 0, "sys", "db1", SYSMOCATALOGPITR)
		mrs = newMrsForPitrRecord([][]interface{}{{"pitr_01", "2025-01-13 00:00:00", uint64(1), "h", uint64(1), types.Date(1)}})
		bh.sql2result[sql] = mrs
		_, err = getDbRecoveryWindowRows(ctx, ses, bh, "sys", "db1")
		convey.So(err, convey.ShouldNotBeNil)

		sql = fmt.Sprintf(getDbSnapshotRecordsFormat, "sys", "db1")
		mrs = newMrsForPitrRecord([][]interface{}{{types.Date(1), int64(10000000)}})
		bh.sql2result[sql] = mrs
		_, err = getDbRecoveryWindowRows(ctx, ses, bh, "sys", "db1")
		convey.So(err, convey.ShouldNotBeNil)

		sql = fmt.Sprintf(getDbSnapshotRecordsFormat, "sys", "db1")
		mrs = newMrsForPitrRecord([][]interface{}{{"snapshot_01", types.Date(1)}})
		bh.sql2result[sql] = mrs
		_, err = getDbRecoveryWindowRows(ctx, ses, bh, "sys", "db1")
		convey.So(err, convey.ShouldNotBeNil)

		sql = fmt.Sprintf(getDbPitrRecordsFormat, 0, "sys", "db1", SYSMOCATALOGPITR)
		mrs = newMrsForPitrRecord([][]interface{}{{"pitr_01", "2025-01-13 00:00:00", uint64(1), "h", uint64(1), "2025-01-13 00:00:00"}})
		bh.sql2result[sql] = mrs
		sql = fmt.Sprintf(getDbSnapshotRecordsFormat, "sys", "db1")
		mrs = newMrsForPitrRecord([][]interface{}{{"snapshot_01", int64(10000000)}})
		bh.sql2result[sql] = mrs
		_, err = getDbRecoveryWindowRows(ctx, ses, bh, "sys", "db1")
		convey.So(err, convey.ShouldBeNil)
	})
}
