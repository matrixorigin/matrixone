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
	"testing"

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
