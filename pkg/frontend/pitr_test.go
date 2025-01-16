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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func Test_checkPitrInValidDurtion(t *testing.T) {
	t.Run("check pitr unit is h", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue:    1,
			pitrUnit:     "h",
			createTime:   "2024-05-01 00:00:00",
			modifiedTime: "2024-05-01 00:00:00",
		}
		err := checkPitrInValidDurtion(time.Now().UnixNano(), pitr)
		assert.NoError(t, err)
	})

	t.Run("check pitr unit is d", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue:    1,
			pitrUnit:     "d",
			createTime:   "2024-05-01 00:00:00",
			modifiedTime: "2024-05-01 00:00:00",
		}
		err := checkPitrInValidDurtion(time.Now().UnixNano(), pitr)
		assert.NoError(t, err)
	})

	t.Run("check pitr unit is m", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue:    1,
			pitrUnit:     "mo",
			createTime:   "2024-05-01 00:00:00",
			modifiedTime: "2024-05-01 00:00:00",
		}
		err := checkPitrInValidDurtion(time.Now().UnixNano(), pitr)
		assert.NoError(t, err)
	})

	t.Run("check pitr unit is y", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue:    1,
			pitrUnit:     "y",
			createTime:   "2024-05-01 00:00:00",
			modifiedTime: "2024-05-01 00:00:00",
		}
		err := checkPitrInValidDurtion(time.Now().UnixNano(), pitr)
		assert.NoError(t, err)
	})

	t.Run("check pitr unit is h", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue:    1,
			pitrUnit:     "h",
			createTime:   "2024-05-01 00:00:00",
			modifiedTime: "2024-05-01 00:00:00",
		}
		err := checkPitrInValidDurtion(time.Now().Add(time.Duration(-2)*time.Hour).UnixNano(), pitr)
		assert.Error(t, err)
	})

	t.Run("check pitr beyond range", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue:    1,
			pitrUnit:     "h",
			createTime:   "2024-05-01 00:00:00",
			modifiedTime: "2024-05-01 00:00:00",
		}
		err := checkPitrInValidDurtion(time.Now().Add(time.Duration(2)*time.Hour).UnixNano(), pitr)
		assert.Error(t, err)
	})

	t.Run("check pitr beyond range 2", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue:    1,
			pitrUnit:     "d",
			createTime:   "2024-05-01 00:00:00",
			modifiedTime: "2024-05-01 00:00:00",
		}
		err := checkPitrInValidDurtion(time.Now().Add(time.Duration(25)*time.Hour).UnixNano(), pitr)
		assert.Error(t, err)
	})
}

func Test_createPubByPitr(t *testing.T) {
	convey.Convey("createPubByPitr success", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		ts := time.Now().Add(time.Duration(-2) * time.Hour).UnixNano()
		sql := getPubInfoWithPitr(ts, 0, "test")
		mrs := newMrsForSqlForGetPubs([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := createPubByPitr(ctx, "", bh, "pitr01", "test", 0, ts)
		assert.NoError(t, err)
	})

	convey.Convey("createPubByPitr fail", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		ts := time.Now().Add(time.Duration(-2) * time.Hour).UnixNano()
		sql := getPubInfoWithPitr(ts, 0, "test")
		mrs := newMrsForSqlForGetPubs([][]interface{}{
			{"pub01", "test", uint64(0), "test1", "acc01", "", "", uint64(0), uint64(0), ""},
		})
		bh.sql2result[sql] = mrs

		err := createPubByPitr(ctx, "", bh, "pitr01", "test", 0, ts)
		assert.Error(t, err)
	})

	convey.Convey("createPubByPitr fail", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		ts := time.Now().Add(time.Duration(-2) * time.Hour).UnixNano()
		sql := getPubInfoWithPitr(ts, 0, "test")
		mrs := newMrsForSqlForGetPubs([][]interface{}{
			{"pub01", "test", "uint64(0)", "test1", "acc01", "", "", uint64(0), uint64(0), ""},
		})
		bh.sql2result[sql] = mrs

		err := createPubByPitr(ctx, "", bh, "pitr01", "test", 0, ts)
		assert.Error(t, err)
	})
}

func Test_doRestorePitr(t *testing.T) {
	convey.Convey("doRestorePitr fail", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		ts := time.Now().Add(time.Duration(-2) * time.Hour).UnixNano()
		stmt := &tree.RestorePitr{
			Level: tree.RESTORELEVELACCOUNT,
			Name:  "pitr01",

			AccountName: "",
			TimeStamp:   nanoTimeFormat(ts),
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForCheckPitr(ctx, "pitr01", sysAccountID)
		assert.NoError(t, err)
		mrs := newMrsForPitrRecord([][]interface{}{{"018ee4cd-5991-7caa-b75d-f9290144bd9f"}})
		bh.sql2result[sql] = mrs

		sql = "select * from mo_catalog.mo_pitr where pitr_name = 'pitr01' and create_account = 0"
		mrs = newMrsForPitrRecord([][]interface{}{{
			"018ee4cd-5991-7caa-b75d-f9290144bd9f",
			"pitr01",
			uint64(0),
			"2024-05-01 00:00:00",
			"2024-05-01 00:00:00",
			"ACCOUNT",
			uint64(0),
			"sys",
			"",
			"",
			uint64(0),
			uint8(1),
			"d",
		}})
		bh.sql2result[sql] = mrs

		sql, err = getSqlForCheckAccountWithPitr(ctx, ts, ses.GetTenantName())
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{{}})
		bh.sql2result[sql] = mrs

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)
	})

	// sys account
	convey.Convey("doRestorePitr fail", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		ts := time.Now().Add(time.Duration(-2) * time.Hour).UnixNano()
		stmt := &tree.RestorePitr{
			Level: tree.RESTORELEVELACCOUNT,
			Name:  "pitr01",

			AccountName: "",
			TimeStamp:   nanoTimeFormat(ts),
		}

		ses.SetTenantInfo(tenant)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForCheckPitr(ctx, "pitr01", sysAccountID)
		assert.NoError(t, err)
		mrs := newMrsForPitrRecord([][]interface{}{{"018ee4cd-5991-7caa-b75d-f9290144bd9f"}})
		bh.sql2result[sql] = mrs

		sql = "select * from mo_catalog.mo_pitr where pitr_name = 'pitr01' and create_account = 0"
		mrs = newMrsForPitrRecord([][]interface{}{{
			"018ee4cd-5991-7caa-b75d-f9290144bd9f",
			"pitr01",
			uint64(0),
			"2024-05-01 00:00:00",
			"2024-05-01 00:00:00",
			"ACCOUNT",
			uint64(0),
			"sys",
			"",
			"",
			uint64(0),
			uint8(1),
			"d",
		}})
		bh.sql2result[sql] = mrs

		resovleTs, err := doResolveTimeStamp(stmt.TimeStamp)
		assert.NoError(t, err)
		sql, err = getSqlForCheckAccountWithPitr(ctx, resovleTs, ses.GetTenantName())
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)
	})

	// normal account
	convey.Convey("doRestorePitr fail", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		ts := time.Now().Add(time.Duration(-2) * time.Hour).UnixNano()
		stmt := &tree.RestorePitr{
			Level: tree.RESTORELEVELACCOUNT,
			Name:  "pitr01",

			AccountName: "acc01",
			TimeStamp:   nanoTimeFormat(ts),
		}

		ses.SetTenantInfo(tenant)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForCheckPitr(ctx, "pitr01", sysAccountID)
		assert.NoError(t, err)
		mrs := newMrsForPitrRecord([][]interface{}{{"018ee4cd-5991-7caa-b75d-f9290144bd9f"}})
		bh.sql2result[sql] = mrs

		sql = "select * from mo_catalog.mo_pitr where pitr_name = 'pitr01' and create_account = 0"
		mrs = newMrsForPitrRecord([][]interface{}{{
			"018ee4cd-5991-7caa-b75d-f9290144bd9f",
			"pitr01",
			uint64(0),
			"2024-05-01 00:00:00",
			"2024-05-01 00:00:00",
			"ACCOUNT",
			uint64(1),
			"acc01",
			"",
			"",
			uint64(1),
			uint8(1),
			"d",
		}})
		bh.sql2result[sql] = mrs

		resovleTs, err := doResolveTimeStamp(stmt.TimeStamp)
		assert.NoError(t, err)
		sql, err = getSqlForCheckAccountWithPitr(ctx, resovleTs, "acc01")
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)
	})

	// normal account
	// pitrRecord account name is not restore account name
	convey.Convey("doRestorePitr fail", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		ts := time.Now().Add(time.Duration(-2) * time.Hour).UnixNano()
		stmt := &tree.RestorePitr{
			Level: tree.RESTORELEVELACCOUNT,
			Name:  "pitr01",

			AccountName: "acc01",
			TimeStamp:   nanoTimeFormat(ts),
		}

		ses.SetTenantInfo(tenant)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForCheckPitr(ctx, "pitr01", sysAccountID)
		assert.NoError(t, err)
		mrs := newMrsForPitrRecord([][]interface{}{{"018ee4cd-5991-7caa-b75d-f9290144bd9f"}})
		bh.sql2result[sql] = mrs

		sql = "select * from mo_catalog.mo_pitr where pitr_name = 'pitr01' and create_account = 0"
		mrs = newMrsForPitrRecord([][]interface{}{{
			"018ee4cd-5991-7caa-b75d-f9290144bd9f",
			"pitr01",
			uint64(0),
			"2024-05-01 00:00:00",
			"2024-05-01 00:00:00",
			"ACCOUNT",
			uint64(0),
			"sys",
			"",
			"",
			uint64(1),
			uint8(1),
			"d",
		}})
		bh.sql2result[sql] = mrs

		resovleTs, err := doResolveTimeStamp(stmt.TimeStamp)
		assert.NoError(t, err)
		sql, err = getSqlForCheckAccountWithPitr(ctx, resovleTs, "acc01")
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)
	})

	// db
	convey.Convey("doRestorePitr fail", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		ts := time.Now().Add(time.Duration(-2) * time.Hour).UnixNano()
		stmt := &tree.RestorePitr{
			Level: tree.RESTORELEVELDATABASE,
			Name:  "pitr01",

			AccountName:  "",
			DatabaseName: "db01",
			TimeStamp:    nanoTimeFormat(ts),
		}

		ses.SetTenantInfo(tenant)
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForCheckPitr(ctx, "pitr01", sysAccountID)
		assert.NoError(t, err)
		mrs := newMrsForPitrRecord([][]interface{}{{"018ee4cd-5991-7caa-b75d-f9290144bd9f"}})
		bh.sql2result[sql] = mrs

		sql = "select * from mo_catalog.mo_pitr where pitr_name = 'pitr01' and create_account = 0"
		mrs = newMrsForPitrRecord([][]interface{}{{
			"018ee4cd-5991-7caa-b75d-f9290144bd9f",
			"pitr01",
			uint64(0),
			"2024-05-01 00:00:00",
			"2024-05-01 00:00:00",
			"ACCOUNT",
			uint64(0),
			"sys",
			"db01",
			"",
			uint64(0),
			uint8(1),
			"d",
		}})
		bh.sql2result[sql] = mrs

		resovleTs, err := doResolveTimeStamp(stmt.TimeStamp)
		assert.NoError(t, err)
		sql, err = getSqlForCheckAccountWithPitr(ctx, resovleTs, "sys")
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{{"0"}})
		bh.sql2result[sql] = mrs

		sql = "select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys where db_name = 'db01'"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys {MO_TS = %d} where db_name = 'db01'", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql, err = getSqlForCheckDatabaseWithPitr(ctx, resovleTs, "db01")
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)
	})

	// table
	convey.Convey("doRestorePitr fail", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		ts := time.Now().Add(time.Duration(-2) * time.Hour).UnixNano()
		stmt := &tree.RestorePitr{
			Level: tree.RESTORELEVELTABLE,
			Name:  "pitr01",

			AccountName:  "",
			DatabaseName: "db01",
			TableName:    "tbl01",
			TimeStamp:    nanoTimeFormat(ts),
		}

		ses.SetTenantInfo(tenant)
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForCheckPitr(ctx, "pitr01", sysAccountID)
		assert.NoError(t, err)
		mrs := newMrsForPitrRecord([][]interface{}{{"018ee4cd-5991-7caa-b75d-f9290144bd9f"}})
		bh.sql2result[sql] = mrs

		sql = "select * from mo_catalog.mo_pitr where pitr_name = 'pitr01' and create_account = 0"
		mrs = newMrsForPitrRecord([][]interface{}{{
			"018ee4cd-5991-7caa-b75d-f9290144bd9f",
			"pitr01",
			uint64(0),
			"2024-05-01 00:00:00",
			"2024-05-01 00:00:00",
			"ACCOUNT",
			uint64(0),
			"sys",
			"db01",
			"tbl01",
			uint64(222222),
			uint8(1),
			"d",
		}})
		bh.sql2result[sql] = mrs

		resovleTs, err := doResolveTimeStamp(stmt.TimeStamp)
		assert.NoError(t, err)
		sql, err = getSqlForCheckAccountWithPitr(ctx, resovleTs, "sys")
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{{"0"}})
		bh.sql2result[sql] = mrs

		sql = "select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys where db_name = 'db01' and table_name = 'tbl01'"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys {MO_TS = %d} where db_name = 'db01' and table_name = 'tbl01'", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql, err = getSqlForCheckTableWithPitr(ctx, resovleTs, "db01", "tbl01")
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)
	})

	// cluster pitr restore db
	convey.Convey("doRestorePitr fail", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		ts := time.Now().Add(time.Duration(-2) * time.Hour).UnixNano()
		stmt := &tree.RestorePitr{
			Level: tree.RESTORELEVELDATABASE,
			Name:  "pitr01",

			AccountName:  "",
			DatabaseName: "db01",
			TimeStamp:    nanoTimeFormat(ts),
		}

		ses.SetTenantInfo(tenant)
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForCheckPitr(ctx, "pitr01", sysAccountID)
		assert.NoError(t, err)
		mrs := newMrsForPitrRecord([][]interface{}{{"018ee4cd-5991-7caa-b75d-f9290144bd9f"}})
		bh.sql2result[sql] = mrs

		sql = "select * from mo_catalog.mo_pitr where pitr_name = 'pitr01' and create_account = 0"
		mrs = newMrsForPitrRecord([][]interface{}{{
			"018ee4cd-5991-7caa-b75d-f9290144bd9f",
			"pitr01",
			uint64(0),
			"2024-05-01 00:00:00",
			"2024-05-01 00:00:00",
			"CLUSTER",
			uint64(0),
			"",
			"",
			"",
			0,
			uint8(1),
			"d",
		}})
		bh.sql2result[sql] = mrs

		resovleTs, err := doResolveTimeStamp(stmt.TimeStamp)
		assert.NoError(t, err)
		sql, err = getSqlForCheckAccountWithPitr(ctx, resovleTs, "sys")
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{{"0"}})
		bh.sql2result[sql] = mrs

		sql = "select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys where db_name = 'db01'"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys {MO_TS = %d} where db_name = 'db01'", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql, err = getSqlForCheckDatabaseWithPitr(ctx, resovleTs, "db01")
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)
	})

	// cluster pitr restore table
	convey.Convey("doRestorePitr fail", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		ts := time.Now().Add(time.Duration(-2) * time.Hour).UnixNano()
		stmt := &tree.RestorePitr{
			Level: tree.RESTORELEVELTABLE,
			Name:  "pitr01",

			AccountName:  "",
			DatabaseName: "db01",
			TableName:    "tbl01",
			TimeStamp:    nanoTimeFormat(ts),
		}

		ses.SetTenantInfo(tenant)
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForCheckPitr(ctx, "pitr01", sysAccountID)
		assert.NoError(t, err)
		mrs := newMrsForPitrRecord([][]interface{}{{"018ee4cd-5991-7caa-b75d-f9290144bd9f"}})
		bh.sql2result[sql] = mrs

		sql = "select * from mo_catalog.mo_pitr where pitr_name = 'pitr01' and create_account = 0"
		mrs = newMrsForPitrRecord([][]interface{}{{
			"018ee4cd-5991-7caa-b75d-f9290144bd9f",
			"pitr01",
			uint64(0),
			"2024-05-01 00:00:00",
			"2024-05-01 00:00:00",
			"CLUSTER",
			uint64(0),
			"",
			"",
			"",
			0,
			uint8(1),
			"d",
		}})
		bh.sql2result[sql] = mrs

		resovleTs, err := doResolveTimeStamp(stmt.TimeStamp)
		assert.NoError(t, err)
		sql, err = getSqlForCheckAccountWithPitr(ctx, resovleTs, "sys")
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{{"0"}})
		bh.sql2result[sql] = mrs

		sql = "select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys where db_name = 'db01' and table_name = 'tbl01'"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys {MO_TS = %d} where db_name = 'db01' and table_name = 'tbl01'", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql, err = getSqlForCheckTableWithPitr(ctx, resovleTs, "db01", "tbl01")
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)
	})

	// normal account
	convey.Convey("doRestorePitr fail", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		ts := time.Now().Add(time.Duration(-2) * time.Hour).UnixNano()
		stmt := &tree.RestorePitr{
			Level: tree.RESTORELEVELACCOUNT,
			Name:  "pitr01",

			AccountName:    "acc01",
			TimeStamp:      nanoTimeFormat(ts),
			SrcAccountName: "sys",
		}

		ses.SetTenantInfo(tenant)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForCheckPitr(ctx, "pitr01", sysAccountID)
		assert.NoError(t, err)
		mrs := newMrsForPitrRecord([][]interface{}{{"018ee4cd-5991-7caa-b75d-f9290144bd9f"}})
		bh.sql2result[sql] = mrs

		sql = "select * from mo_catalog.mo_pitr where pitr_name = 'pitr01' and create_account = 0"
		mrs = newMrsForPitrRecord([][]interface{}{{
			"018ee4cd-5991-7caa-b75d-f9290144bd9f",
			"pitr01",
			uint64(0),
			"2024-05-01 00:00:00",
			"2024-05-01 00:00:00",
			"ACCOUNT",
			uint64(1),
			"acc01",
			"",
			"",
			uint64(1),
			uint8(1),
			"d",
		}})
		bh.sql2result[sql] = mrs

		resovleTs, err := doResolveTimeStamp(stmt.TimeStamp)
		assert.NoError(t, err)
		sql, err = getSqlForCheckAccountWithPitr(ctx, resovleTs, "acc01")
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)
	})
	convey.Convey("doRestorePitr fail", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		ts := time.Now().Add(time.Duration(-2) * time.Hour).UnixNano()
		stmt := &tree.RestorePitr{
			Level: tree.RESTORELEVELACCOUNT,
			Name:  "pitr01",

			AccountName:    "acc01",
			TimeStamp:      nanoTimeFormat(ts),
			SrcAccountName: "sys",
		}

		ses.SetTenantInfo(tenant)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForCheckPitr(ctx, "pitr01", sysAccountID)
		assert.NoError(t, err)
		mrs := newMrsForPitrRecord([][]interface{}{{"018ee4cd-5991-7caa-b75d-f9290144bd9f"}})
		bh.sql2result[sql] = mrs

		sql = "select * from mo_catalog.mo_pitr where pitr_name = 'pitr01' and create_account = 0"
		mrs = newMrsForPitrRecord([][]interface{}{{
			"018ee4cd-5991-7caa-b75d-f9290144bd9f",
			"pitr01",
			uint64(0),
			"2024-05-01 00:00:00",
			"2024-05-01 00:00:00",
			"CLUSTER",
			uint64(1),
			"acc01",
			"",
			"",
			uint64(1),
			uint8(1),
			"d",
		}})
		bh.sql2result[sql] = mrs

		resovleTs, err := doResolveTimeStamp(stmt.TimeStamp)
		assert.NoError(t, err)
		sql, err = getSqlForCheckAccountWithPitr(ctx, resovleTs, "acc01")
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)
	})
}

func Test_doRestorePitrValid(t *testing.T) {
	// sys account
	convey.Convey("doRestorePitr fail", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		ts := time.Now().Add(time.Duration(-2) * time.Hour).UnixNano()
		stmt := &tree.RestorePitr{
			Level: tree.RESTORELEVELACCOUNT,
			Name:  "pitr01",

			AccountName: "",
			TimeStamp:   nanoTimeFormat(ts),
		}

		ses.SetTenantInfo(tenant)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForCheckPitr(ctx, "pitr01", sysAccountID)
		assert.NoError(t, err)
		mrs := newMrsForPitrRecord([][]interface{}{{"018ee4cd-5991-7caa-b75d-f9290144bd9f"}})
		bh.sql2result[sql] = mrs

		sql = "select * from mo_catalog.mo_pitr where pitr_name = 'pitr01' and create_account = 0"
		mrs = newMrsForPitrRecord([][]interface{}{{
			"018ee4cd-5991-7caa-b75d-f9290144bd9f",
			"pitr01",
			uint64(0),
			"2024-05-34 00:00:00",
			"2024-05-34 00:00:00",
			"ACCOUNT",
			uint64(0),
			"sys",
			"",
			"",
			uint64(0),
			uint8(1),
			"d",
		}})
		bh.sql2result[sql] = mrs

		resovleTs, err := doResolveTimeStamp(stmt.TimeStamp)
		assert.NoError(t, err)
		sql, err = getSqlForCheckAccountWithPitr(ctx, resovleTs, ses.GetTenantName())
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)
	})

	convey.Convey("doRestorePitr fail", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		ts := time.Now().Add(time.Duration(-2) * time.Hour).UnixNano()
		stmt := &tree.RestorePitr{
			Level: tree.RESTORELEVELACCOUNT,
			Name:  "pitr01",

			AccountName: "",
			TimeStamp:   nanoTimeFormat(ts),
		}

		ses.SetTenantInfo(tenant)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForCheckPitr(ctx, "pitr01", sysAccountID)
		assert.NoError(t, err)
		mrs := newMrsForPitrRecord([][]interface{}{{"018ee4cd-5991-7caa-b75d-f9290144bd9f"}})
		bh.sql2result[sql] = mrs

		sql = "select * from mo_catalog.mo_pitr where pitr_name = 'pitr01' and create_account = 0"
		mrs = newMrsForPitrRecord([][]interface{}{{
			"018ee4cd-5991-7caa-b75d-f9290144bd9f",
			"pitr01",
			uint64(0),
			types.CurrentTimestamp().String2(time.UTC, 0),
			types.CurrentTimestamp().String2(time.UTC, 0),
			"ACCOUNT",
			uint64(0),
			"sys",
			"",
			"",
			uint64(0),
			uint8(1),
			"d",
		}})
		bh.sql2result[sql] = mrs

		resovleTs, err := doResolveTimeStamp(stmt.TimeStamp)
		assert.NoError(t, err)
		sql, err = getSqlForCheckAccountWithPitr(ctx, resovleTs, ses.GetTenantName())
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)
	})
}

func TestGetSqlForCheckPitrDup(t *testing.T) {
	tests := []struct {
		createAccount   string
		createAccountId uint64
		stmt            *tree.CreatePitr
		expected        string
	}{
		{
			createAccount:   "sys",
			createAccountId: 0,
			stmt: &tree.CreatePitr{
				Level: tree.PITRLEVELCLUSTER,
			},
			expected: "select pitr_id from mo_catalog.mo_pitr where create_account = 0 and obj_id = 18446744073709551615;",
		},
		{
			createAccount:   "sys",
			createAccountId: 0,
			stmt: &tree.CreatePitr{
				Level: tree.PITRLEVELACCOUNT,
			},
			expected: "select pitr_id from mo_catalog.mo_pitr where create_account = 0 and account_name = 'sys' and level = 'account' and pitr_status = 1;",
		},
		{
			createAccount:   "testAccount",
			createAccountId: 1,
			stmt: &tree.CreatePitr{
				Level: tree.PITRLEVELACCOUNT,
			},
			expected: "select pitr_id from mo_catalog.mo_pitr where create_account = 1 and account_name = 'testAccount' and level = 'account' and pitr_status = 1;",
		},
		{
			createAccount:   "sys",
			createAccountId: 0,
			stmt: &tree.CreatePitr{
				Level:       tree.PITRLEVELACCOUNT,
				AccountName: "testAccountName",
			},
			expected: "select pitr_id from mo_catalog.mo_pitr where create_account = 0 and account_name = 'testAccountName' and level = 'account' and pitr_status = 1;",
		},
		{
			createAccount:   "sys",
			createAccountId: 0,
			stmt: &tree.CreatePitr{
				Level:        tree.PITRLEVELDATABASE,
				DatabaseName: "testDb",
			},
			expected: "select pitr_id from mo_catalog.mo_pitr where create_account = 0 and database_name = 'testDb' and level = 'database' and pitr_status = 1;",
		},
		{
			createAccount:   "testAccount",
			createAccountId: 1,
			stmt: &tree.CreatePitr{
				Level:        tree.PITRLEVELDATABASE,
				DatabaseName: "testDb",
			},
			expected: "select pitr_id from mo_catalog.mo_pitr where create_account = 1 and database_name = 'testDb' and level = 'database' and pitr_status = 1;",
		},
		{
			createAccount:   "sys",
			createAccountId: 0,
			stmt: &tree.CreatePitr{
				Level:        tree.PITRLEVELTABLE,
				DatabaseName: "testDb",
				TableName:    "testTable",
			},
			expected: "select pitr_id from mo_catalog.mo_pitr where create_account = 0 and database_name = 'testDb' and table_name = 'testTable' and level = 'table' and pitr_status = 1;",
		},
		{
			createAccount:   "testAccount",
			createAccountId: 1,
			stmt: &tree.CreatePitr{
				Level:        tree.PITRLEVELTABLE,
				DatabaseName: "testDb",
				TableName:    "testTable",
			},
			expected: "select pitr_id from mo_catalog.mo_pitr where create_account = 1 and database_name = 'testDb' and table_name = 'testTable' and level = 'table' and pitr_status = 1;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := getSqlForCheckPitrDup(tt.createAccount, tt.createAccountId, tt.stmt)
			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func Test_doRestorePitr_Account(t *testing.T) {
	convey.Convey("doRestorePitr fail", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		ts := time.Now().Add(time.Duration(-2) * time.Hour).UnixNano()
		stmt := &tree.RestorePitr{
			Level: tree.RESTORELEVELACCOUNT,
			Name:  "pitr01",

			AccountName: "",
			TimeStamp:   nanoTimeFormat(ts),
		}

		ses.SetTenantInfo(tenant)
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForCheckPitr(ctx, "pitr01", sysAccountID)
		assert.NoError(t, err)
		mrs := newMrsForPitrRecord([][]interface{}{{"018ee4cd-5991-7caa-b75d-f9290144bd9f"}})
		bh.sql2result[sql] = mrs

		sql = "select * from mo_catalog.mo_pitr where pitr_name = 'pitr01' and create_account = 0"
		mrs = newMrsForPitrRecord([][]interface{}{{
			"018ee4cd-5991-7caa-b75d-f9290144bd9f",
			"pitr01",
			uint64(0),
			"2024-05-01 00:00:00",
			"2024-05-01 00:00:00",
			"ACCOUNT",
			uint64(0),
			"sys",
			"",
			"",
			uint64(0),
			uint8(1),
			"d",
		}})
		bh.sql2result[sql] = mrs

		resovleTs, err := doResolveTimeStamp(stmt.TimeStamp)
		assert.NoError(t, err)
		sql, err = getSqlForCheckAccountWithPitr(ctx, resovleTs, ses.GetTenantName())
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{{"0"}})
		bh.sql2result[sql] = mrs

		sql = "select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys {MO_TS = %d}", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = "show databases"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("show databases {MO_TS = %d}", resovleTs)
		mrs = newMrsForSqlForShowDatabases([][]interface{}{
			{"db1"},
		})
		bh.sql2result[sql] = mrs

		sql, err = getSqlForCheckDatabaseWithPitr(ctx, resovleTs, "db1")
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{{"0"}})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("select datname, dat_createsql from mo_catalog.mo_database {MO_TS = %d} where datname = 'db1' and account_id = 0", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{{"db1", "create database db1;"}})
		bh.sql2result[sql] = mrs

		sql = "select pub_name, database_name, database_id, table_list, account_list, created_time, update_time, owner, creator, comment from mo_catalog.mo_pubs where 1=1 and database_name = 'db1'"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("show full tables from `db1` {MO_TS = %d}", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)

		sql = fmt.Sprintf(checkDatabaseIsMasterFormat, "db1")
		mrs = newMrsForPitrRecord([][]interface{}{{"db2"}})
		bh.sql2result[sql] = mrs

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)
	})
}

func Test_doRestorePitr_Account_Sys_Restore_Normal(t *testing.T) {
	convey.Convey("doRestorePitr fail", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		ts := time.Now().Add(time.Duration(-2) * time.Hour).UnixNano()
		stmt := &tree.RestorePitr{
			Level: tree.RESTORELEVELACCOUNT,
			Name:  "pitr01",

			AccountName: "acc01",
			TimeStamp:   nanoTimeFormat(ts),
		}

		ses.SetTenantInfo(tenant)
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForCheckPitr(ctx, "pitr01", sysAccountID)
		assert.NoError(t, err)
		mrs := newMrsForPitrRecord([][]interface{}{{"018ee4cd-5991-7caa-b75d-f9290144bd9f"}})
		bh.sql2result[sql] = mrs

		sql = "select * from mo_catalog.mo_pitr where pitr_name = 'pitr01' and create_account = 0"
		mrs = newMrsForPitrRecord([][]interface{}{{
			"018ee4cd-5991-7caa-b75d-f9290144bd9f",
			"pitr01",
			uint64(0),
			"2024-05-01 00:00:00",
			"2024-05-01 00:00:00",
			"ACCOUNT",
			uint64(1),
			"acc01",
			"",
			"",
			uint64(1),
			uint8(1),
			"d",
		}})
		bh.sql2result[sql] = mrs

		resovleTs, err := doResolveTimeStamp(stmt.TimeStamp)
		assert.NoError(t, err)
		sql, err = getSqlForCheckAccountWithPitr(ctx, resovleTs, "acc01")
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{{"1"}})
		bh.sql2result[sql] = mrs

		sql = "select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys {MO_TS = %d}", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = "show databases"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("show databases {MO_TS = %d}", resovleTs)
		mrs = newMrsForSqlForShowDatabases([][]interface{}{
			{"db1"},
		})
		bh.sql2result[sql] = mrs

		sql, err = getSqlForCheckDatabaseWithPitr(ctx, resovleTs, "db1")
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{{"0"}})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("select datname, dat_createsql from mo_catalog.mo_database {MO_TS = %d} where datname = 'db1' and account_id = 0", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{{"db1", "create database db1;"}})
		bh.sql2result[sql] = mrs

		sql = "select pub_name, database_name, database_id, table_list, account_list, created_time, update_time, owner, creator, comment from mo_catalog.mo_pubs where 1=1 and database_name = 'db1'"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("show full tables from `db1` {MO_TS = %d}", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)
	})
}

func Test_doRestorePitr_Account_Sys_Restore_Normal_To_new(t *testing.T) {
	convey.Convey("doRestorePitr fail", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		ts := time.Now().Add(time.Duration(-2) * time.Hour).UnixNano()
		stmt := &tree.RestorePitr{
			Level: tree.RESTORELEVELACCOUNT,
			Name:  "pitr01",

			AccountName: "acc01",
			TimeStamp:   nanoTimeFormat(ts),
		}

		ses.SetTenantInfo(tenant)
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForCheckPitr(ctx, "pitr01", sysAccountID)
		assert.NoError(t, err)
		mrs := newMrsForPitrRecord([][]interface{}{{"018ee4cd-5991-7caa-b75d-f9290144bd9f"}})
		bh.sql2result[sql] = mrs

		sql = "select * from mo_catalog.mo_pitr where pitr_name = 'pitr01' and create_account = 0"
		mrs = newMrsForPitrRecord([][]interface{}{{
			"018ee4cd-5991-7caa-b75d-f9290144bd9f",
			"pitr01",
			uint64(0),
			"2024-05-01 00:00:00",
			"2024-05-01 00:00:00",
			"ACCOUNT",
			uint64(2),
			"acc02",
			"",
			"",
			uint64(2),
			uint8(1),
			"d",
		}})
		bh.sql2result[sql] = mrs

		resovleTs, err := doResolveTimeStamp(stmt.TimeStamp)
		assert.NoError(t, err)
		sql, err = getSqlForCheckAccountWithPitr(ctx, resovleTs, "acc02")
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{{"1"}})
		bh.sql2result[sql] = mrs

		sql = "select account_id, account_name, status, version, suspended_time from mo_catalog.mo_account where 1=1 and account_name = 'acc01'"
		mrs = newMrsForPitrRecord([][]interface{}{{uint64(1), "acc01", "open", uint64(1), nil}})
		bh.sql2result[sql] = mrs

		sql = "select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys {MO_TS = %d}", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = "show databases"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("show databases {MO_TS = %d}", resovleTs)
		mrs = newMrsForSqlForShowDatabases([][]interface{}{
			{"db1"},
		})
		bh.sql2result[sql] = mrs

		sql, err = getSqlForCheckDatabaseWithPitr(ctx, resovleTs, "db1")
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{{"0"}})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("select datname, dat_createsql from mo_catalog.mo_database {MO_TS = %d} where datname = 'db1' and account_id = 0", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{{"db1", "create database db1;"}})
		bh.sql2result[sql] = mrs

		sql = "select pub_name, database_name, database_id, table_list, account_list, created_time, update_time, owner, creator, comment from mo_catalog.mo_pubs where 1=1 and database_name = 'db1'"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("show full tables from `db1` {MO_TS = %d}", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)
	})

	convey.Convey("doRestorePitr fail", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		ts := time.Now().Add(time.Duration(-2) * time.Hour).UnixNano()
		stmt := &tree.RestorePitr{
			Level: tree.RESTORELEVELACCOUNT,
			Name:  "pitr01",

			AccountName: "acc01",
			TimeStamp:   nanoTimeFormat(ts),
		}

		ses.SetTenantInfo(tenant)
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForCheckPitr(ctx, "pitr01", sysAccountID)
		assert.NoError(t, err)
		mrs := newMrsForPitrRecord([][]interface{}{{"018ee4cd-5991-7caa-b75d-f9290144bd9f"}})
		bh.sql2result[sql] = mrs

		sql = "select * from mo_catalog.mo_pitr where pitr_name = 'pitr01' and create_account = 0"
		mrs = newMrsForPitrRecord([][]interface{}{{
			"018ee4cd-5991-7caa-b75d-f9290144bd9f",
			"pitr01",
			uint64(0),
			"2024-05-01 00:00:00",
			"2024-05-01 00:00:00",
			"ACCOUNT",
			uint64(2),
			"acc02",
			"",
			"",
			uint64(2),
			uint8(1),
			"d",
		}})
		bh.sql2result[sql] = mrs

		resovleTs, err := doResolveTimeStamp(stmt.TimeStamp)
		assert.NoError(t, err)
		sql, err = getSqlForCheckAccountWithPitr(ctx, resovleTs, "acc02")
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{{"1"}})
		bh.sql2result[sql] = mrs

		sql = "select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys {MO_TS = %d}", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = "show databases"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("show databases {MO_TS = %d}", resovleTs)
		mrs = newMrsForSqlForShowDatabases([][]interface{}{
			{"db1"},
		})
		bh.sql2result[sql] = mrs

		sql, err = getSqlForCheckDatabaseWithPitr(ctx, resovleTs, "db1")
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{{"0"}})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("select datname, dat_createsql from mo_catalog.mo_database {MO_TS = %d} where datname = 'db1' and account_id = 0", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{{"db1", "create database db1;"}})
		bh.sql2result[sql] = mrs

		sql = "select pub_name, database_name, database_id, table_list, account_list, created_time, update_time, owner, creator, comment from mo_catalog.mo_pubs where 1=1 and database_name = 'db1'"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("show full tables from `db1` {MO_TS = %d}", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)
	})
}

func Test_doRestorePitr_Account_Sys_Restore_Normal_Using_cluster(t *testing.T) {
	convey.Convey("doRestorePitr fail", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		ts := time.Now().Add(time.Duration(-2) * time.Hour).UnixNano()
		stmt := &tree.RestorePitr{
			Level: tree.RESTORELEVELACCOUNT,
			Name:  "pitr01",

			AccountName:    "acc01",
			SrcAccountName: "acc01",
			TimeStamp:      nanoTimeFormat(ts),
		}

		ses.SetTenantInfo(tenant)
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForCheckPitr(ctx, "pitr01", sysAccountID)
		assert.NoError(t, err)
		mrs := newMrsForPitrRecord([][]interface{}{{"018ee4cd-5991-7caa-b75d-f9290144bd9f"}})
		bh.sql2result[sql] = mrs

		sql = "select * from mo_catalog.mo_pitr where pitr_name = 'pitr01' and create_account = 0"
		mrs = newMrsForPitrRecord([][]interface{}{{
			"018ee4cd-5991-7caa-b75d-f9290144bd9f",
			"pitr01",
			uint64(0),
			"2024-05-01 00:00:00",
			"2024-05-01 00:00:00",
			"CLUSTER",
			uint64(1),
			"",
			"",
			"",
			uint64(1),
			uint8(1),
			"d",
		}})
		bh.sql2result[sql] = mrs

		resovleTs, err := doResolveTimeStamp(stmt.TimeStamp)
		assert.NoError(t, err)
		sql, err = getSqlForCheckAccountWithPitr(ctx, resovleTs, "acc01")
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{{"1"}})
		bh.sql2result[sql] = mrs

		sql = "select account_id, account_name, status, version, suspended_time from mo_catalog.mo_account where 1=1 and account_name = 'acc01'"
		mrs = newMrsForPitrRecord([][]interface{}{{uint64(1), "acc01", "open", uint64(1), nil}})
		bh.sql2result[sql] = mrs

		sql = "select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys {MO_TS = %d}", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = "show databases"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("show databases {MO_TS = %d}", resovleTs)
		mrs = newMrsForSqlForShowDatabases([][]interface{}{
			{"db1"},
		})
		bh.sql2result[sql] = mrs

		sql, err = getSqlForCheckDatabaseWithPitr(ctx, resovleTs, "db1")
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{{"0"}})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("select datname, dat_createsql from mo_catalog.mo_database {MO_TS = %d} where datname = 'db1' and account_id = 0", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{{"db1", "create database db1;"}})
		bh.sql2result[sql] = mrs

		sql = "select pub_name, database_name, database_id, table_list, account_list, created_time, update_time, owner, creator, comment from mo_catalog.mo_pubs where 1=1 and database_name = 'db1'"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("show full tables from `db1` {MO_TS = %d}", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)
	})

	convey.Convey("doRestorePitr fail", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		ts := time.Now().Add(time.Duration(-2) * time.Hour).UnixNano()
		stmt := &tree.RestorePitr{
			Level: tree.RESTORELEVELACCOUNT,
			Name:  "pitr01",

			AccountName:    "acc01",
			SrcAccountName: "acc01",
			TimeStamp:      nanoTimeFormat(ts),
		}

		ses.SetTenantInfo(tenant)
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForCheckPitr(ctx, "pitr01", sysAccountID)
		assert.NoError(t, err)
		mrs := newMrsForPitrRecord([][]interface{}{{"018ee4cd-5991-7caa-b75d-f9290144bd9f"}})
		bh.sql2result[sql] = mrs

		sql = "select * from mo_catalog.mo_pitr where pitr_name = 'pitr01' and create_account = 0"
		mrs = newMrsForPitrRecord([][]interface{}{{
			"018ee4cd-5991-7caa-b75d-f9290144bd9f",
			"pitr01",
			uint64(0),
			"2024-05-01 00:00:00",
			"2024-05-01 00:00:00",
			"CLUSTER",
			uint64(1),
			"",
			"",
			"",
			uint64(1),
			uint8(1),
			"d",
		}})
		bh.sql2result[sql] = mrs

		resovleTs, err := doResolveTimeStamp(stmt.TimeStamp)
		assert.NoError(t, err)
		sql, err = getSqlForCheckAccountWithPitr(ctx, resovleTs, "acc01")
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{{"1"}})
		bh.sql2result[sql] = mrs

		sql = "select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys {MO_TS = %d}", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = "show databases"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("show databases {MO_TS = %d}", resovleTs)
		mrs = newMrsForSqlForShowDatabases([][]interface{}{
			{"db1"},
		})
		bh.sql2result[sql] = mrs

		sql, err = getSqlForCheckDatabaseWithPitr(ctx, resovleTs, "db1")
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{{"0"}})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("select datname, dat_createsql from mo_catalog.mo_database {MO_TS = %d} where datname = 'db1' and account_id = 0", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{{"db1", "create database db1;"}})
		bh.sql2result[sql] = mrs

		sql = "select pub_name, database_name, database_id, table_list, account_list, created_time, update_time, owner, creator, comment from mo_catalog.mo_pubs where 1=1 and database_name = 'db1'"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("show full tables from `db1` {MO_TS = %d}", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)
	})
}

func Test_doRestorePitr_Account_Sys_Restore_Normal_To_new_Using_cluster(t *testing.T) {
	convey.Convey("doRestorePitr fail", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		ts := time.Now().Add(time.Duration(-2) * time.Hour).UnixNano()
		stmt := &tree.RestorePitr{
			Level: tree.RESTORELEVELACCOUNT,
			Name:  "pitr01",

			AccountName:    "acc01",
			SrcAccountName: "acc02",
			TimeStamp:      nanoTimeFormat(ts),
		}

		ses.SetTenantInfo(tenant)
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForCheckPitr(ctx, "pitr01", sysAccountID)
		assert.NoError(t, err)
		mrs := newMrsForPitrRecord([][]interface{}{{"018ee4cd-5991-7caa-b75d-f9290144bd9f"}})
		bh.sql2result[sql] = mrs

		sql = "select * from mo_catalog.mo_pitr where pitr_name = 'pitr01' and create_account = 0"
		mrs = newMrsForPitrRecord([][]interface{}{{
			"018ee4cd-5991-7caa-b75d-f9290144bd9f",
			"pitr01",
			uint64(0),
			"2024-05-01 00:00:00",
			"2024-05-01 00:00:00",
			"CLUSTER",
			uint64(1),
			"",
			"",
			"",
			uint64(1),
			uint8(1),
			"d",
		}})
		bh.sql2result[sql] = mrs

		resovleTs, err := doResolveTimeStamp(stmt.TimeStamp)
		assert.NoError(t, err)
		sql, err = getSqlForCheckAccountWithPitr(ctx, resovleTs, "acc02")
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{{"1"}})
		bh.sql2result[sql] = mrs

		sql = "select account_id, account_name, status, version, suspended_time from mo_catalog.mo_account where 1=1 and account_name = 'acc01'"
		mrs = newMrsForPitrRecord([][]interface{}{{uint64(1), "acc01", "open", uint64(1), nil}})
		bh.sql2result[sql] = mrs

		sql = "select account_id, account_name, status, version, suspended_time from mo_catalog.mo_account where 1=1 and account_name = 'acc02'"
		mrs = newMrsForPitrRecord([][]interface{}{{uint64(2), "acc01", "open", uint64(1), nil}})
		bh.sql2result[sql] = mrs

		sql = "select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys {MO_TS = %d}", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = "show databases"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("show databases {MO_TS = %d}", resovleTs)
		mrs = newMrsForSqlForShowDatabases([][]interface{}{
			{"db1"},
		})
		bh.sql2result[sql] = mrs

		sql, err = getSqlForCheckDatabaseWithPitr(ctx, resovleTs, "db1")
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{{"0"}})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("select datname, dat_createsql from mo_catalog.mo_database {MO_TS = %d} where datname = 'db1' and account_id = 0", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{{"db1", "create database db1;"}})
		bh.sql2result[sql] = mrs

		sql = "select pub_name, database_name, database_id, table_list, account_list, created_time, update_time, owner, creator, comment from mo_catalog.mo_pubs where 1=1 and database_name = 'db1'"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("show full tables from `db1` {MO_TS = %d}", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)
	})

	convey.Convey("doRestorePitr fail", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		ts := time.Now().Add(time.Duration(-2) * time.Hour).UnixNano()
		stmt := &tree.RestorePitr{
			Level: tree.RESTORELEVELACCOUNT,
			Name:  "pitr01",

			AccountName:    "acc01",
			SrcAccountName: "acc02",
			TimeStamp:      nanoTimeFormat(ts),
		}

		ses.SetTenantInfo(tenant)
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForCheckPitr(ctx, "pitr01", sysAccountID)
		assert.NoError(t, err)
		mrs := newMrsForPitrRecord([][]interface{}{{"018ee4cd-5991-7caa-b75d-f9290144bd9f"}})
		bh.sql2result[sql] = mrs

		sql = "select * from mo_catalog.mo_pitr where pitr_name = 'pitr01' and create_account = 0"
		mrs = newMrsForPitrRecord([][]interface{}{{
			"018ee4cd-5991-7caa-b75d-f9290144bd9f",
			"pitr01",
			uint64(0),
			"2024-05-01 00:00:00",
			"2024-05-01 00:00:00",
			"CLUSTER",
			uint64(1),
			"",
			"",
			"",
			uint64(1),
			uint8(1),
			"d",
		}})
		bh.sql2result[sql] = mrs

		resovleTs, err := doResolveTimeStamp(stmt.TimeStamp)
		assert.NoError(t, err)
		sql, err = getSqlForCheckAccountWithPitr(ctx, resovleTs, "acc02")
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{{"1"}})
		bh.sql2result[sql] = mrs

		sql = "select account_id, account_name, status, version, suspended_time from mo_catalog.mo_account where 1=1 and account_name = 'acc02'"
		mrs = newMrsForPitrRecord([][]interface{}{{uint64(2), "acc01", "open", uint64(1), nil}})
		bh.sql2result[sql] = mrs

		sql = "select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys {MO_TS = %d}", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = "show databases"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("show databases {MO_TS = %d}", resovleTs)
		mrs = newMrsForSqlForShowDatabases([][]interface{}{
			{"db1"},
		})
		bh.sql2result[sql] = mrs

		sql, err = getSqlForCheckDatabaseWithPitr(ctx, resovleTs, "db1")
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{{"0"}})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("select datname, dat_createsql from mo_catalog.mo_database {MO_TS = %d} where datname = 'db1' and account_id = 0", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{{"db1", "create database db1;"}})
		bh.sql2result[sql] = mrs

		sql = "select pub_name, database_name, database_id, table_list, account_list, created_time, update_time, owner, creator, comment from mo_catalog.mo_pubs where 1=1 and database_name = 'db1'"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("show full tables from `db1` {MO_TS = %d}", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)
	})

	convey.Convey("doRestorePitr fail", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		ts := time.Now().Add(time.Duration(-2) * time.Hour).UnixNano()
		stmt := &tree.RestorePitr{
			Level: tree.RESTORELEVELACCOUNT,
			Name:  "pitr01",

			AccountName:    "acc01",
			SrcAccountName: "acc02",
			TimeStamp:      nanoTimeFormat(ts),
		}

		ses.SetTenantInfo(tenant)
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForCheckPitr(ctx, "pitr01", sysAccountID)
		assert.NoError(t, err)
		mrs := newMrsForPitrRecord([][]interface{}{{"018ee4cd-5991-7caa-b75d-f9290144bd9f"}})
		bh.sql2result[sql] = mrs

		sql = "select * from mo_catalog.mo_pitr where pitr_name = 'pitr01' and create_account = 0"
		mrs = newMrsForPitrRecord([][]interface{}{{
			"018ee4cd-5991-7caa-b75d-f9290144bd9f",
			"pitr01",
			uint64(0),
			"2024-05-01 00:00:00",
			"2024-05-01 00:00:00",
			"CLUSTER",
			uint64(1),
			"",
			"",
			"",
			uint64(1),
			uint8(1),
			"d",
		}})
		bh.sql2result[sql] = mrs

		resovleTs, err := doResolveTimeStamp(stmt.TimeStamp)
		assert.NoError(t, err)
		sql, err = getSqlForCheckAccountWithPitr(ctx, resovleTs, "acc02")
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{{"1"}})
		bh.sql2result[sql] = mrs

		sql = "select account_id, account_name, status, version, suspended_time from mo_catalog.mo_account where 1=1 and account_name = 'acc01'"
		mrs = newMrsForPitrRecord([][]interface{}{{uint64(1), "acc01", "open", uint64(1), nil}})
		bh.sql2result[sql] = mrs

		sql = "select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("select db_name, table_name, refer_db_name, refer_table_name from mo_catalog.mo_foreign_keys {MO_TS = %d}", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = "show databases"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("show databases {MO_TS = %d}", resovleTs)
		mrs = newMrsForSqlForShowDatabases([][]interface{}{
			{"db1"},
		})
		bh.sql2result[sql] = mrs

		sql, err = getSqlForCheckDatabaseWithPitr(ctx, resovleTs, "db1")
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{{"0"}})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("select datname, dat_createsql from mo_catalog.mo_database {MO_TS = %d} where datname = 'db1' and account_id = 0", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{{"db1", "create database db1;"}})
		bh.sql2result[sql] = mrs

		sql = "select pub_name, database_name, database_id, table_list, account_list, created_time, update_time, owner, creator, comment from mo_catalog.mo_pubs where 1=1 and database_name = 'db1'"
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("show full tables from `db1` {MO_TS = %d}", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)

		sql = fmt.Sprintf("show full tables from `mo_catalog` {MO_TS = %d}", resovleTs)
		mrs = newMrsForPitrRecord([][]interface{}{
			{"mo_user", "BASE TABLE"},
		})
		bh.sql2result[sql] = mrs

		err = restoreSystemDatabaseWithPitr(ctx, "", bh, "pitr01", resovleTs, 0)
		assert.Error(t, err)

		sql = fmt.Sprintf("show full tables from `mo_catalog` {snapshot = '%s'}", "pitr01")
		mrs = newMrsForPitrRecord([][]interface{}{
			{"mo_user", "BASE TABLE"},
		})
		bh.sql2result[sql] = mrs

		err = restoreSystemDatabase(ctx, "", bh, "pitr01", 0, resovleTs)
		assert.Error(t, err)
	})
}

func Test_doCreatePitr(t *testing.T) {
	convey.Convey("doRestorePitr fail", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		stmt := &tree.CreatePitr{
			Name: "pitr01",

			Level:     tree.PITRLEVELACCOUNT,
			PitrValue: 10,
			PitrUnit:  "d",
		}

		ses.SetTenantInfo(tenant)
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForCheckPitr(ctx, "pitr01", sysAccountID)
		assert.NoError(t, err)
		mrs := newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = fmt.Sprintf("select pitr_id from mo_catalog.mo_pitr where create_account = %d", sysAccountID) + fmt.Sprintf(" and account_name = '%s' and level = 'account' and pitr_status = 1;", sysAccountName)
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		err = doCreatePitr(ctx, ses, stmt)
		assert.Error(t, err)

		sql = fmt.Sprintf(getPitrFormat+" where pitr_name = '%s';", SYSMOCATALOGPITR)
		mrs = newMrsForPitrRecord([][]interface{}{
			{
				"018ee4cd-5991-7caa-b75d-f9290144bd9f",
				"pitr01",
				uint64(0),
				"2024-05-01 00:00:00",
				"2024-05-01 00:00:00",
				"database",
				uint64(0),
				"sys",
				"mo_catalog",
				"",
				uint64(1),
				"d",
				"d",
			},
		})
		bh.sql2result[sql] = mrs

		err = doCreatePitr(ctx, ses, stmt)
		assert.Error(t, err)

		sql = fmt.Sprintf(getPitrFormat+" where pitr_name = '%s';", SYSMOCATALOGPITR)
		mrs = newMrsForPitrRecord([][]interface{}{
			{
				"018ee4cd-5991-7caa-b75d-f9290144bd9f",
				"pitr01",
				uint64(0),
				"2024-05-01 00:00:00",
				"2024-05-01 00:00:00",
				"database",
				uint64(0),
				"sys",
				"mo_catalog",
				"",
				uint64(1),
				uint8(1),
				uint8(1),
			},
		})
		bh.sql2result[sql] = mrs

		err = doCreatePitr(ctx, ses, stmt)
		assert.Error(t, err)

		sql = fmt.Sprintf(getPitrFormat+" where pitr_name = '%s';", SYSMOCATALOGPITR)
		mrs = newMrsForPitrRecord([][]interface{}{
			{
				"018ee4cd-5991-7caa-b75d-f9290144bd9f",
				"pitr01",
				uint64(0),
				"2024-05-01 00:00:00",
				"2024-05-01 00:00:00",
				"database",
				uint64(0),
				"sys",
				"mo_catalog",
				"",
				uint64(1),
				uint8(1),
				"d",
			},
		})
		bh.sql2result[sql] = mrs

		err = doCreatePitr(ctx, ses, stmt)
		assert.NoError(t, err)
	})

}

func Test_RestorePitrBadTimeStamp(t *testing.T) {
	convey.Convey("doRestorePitr fail", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		stmt := &tree.RestorePitr{
			Level:     tree.RESTORELEVELACCOUNT,
			Name:      "pitr01",
			TimeStamp: "2024-05-32 00:00:00",
		}

		ses.SetTenantInfo(tenant)
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))

		_, err := doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)
	})
}

func Test_RestorePitrFaultTolerance(t *testing.T) {
	convey.Convey("doRestorePitr BackgroundExec.Exec('begin')", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		stmt := &tree.RestorePitr{
			Level:     tree.RESTORELEVELACCOUNT,
			Name:      "pitr01",
			TimeStamp: "2024-05-21 00:00:00",
		}

		_, err := doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)
	})

	convey.Convey("doRestorePitr check Pitr", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		stmt := &tree.RestorePitr{
			Level:     tree.RESTORELEVELACCOUNT,
			Name:      "pitr01",
			TimeStamp: "2024-05-21 00:00:00",
		}

		_, err := doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)
	})

	convey.Convey("doRestorePitr check Pitr database name", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		stmt := &tree.RestorePitr{
			Level:     tree.RESTORELEVELACCOUNT,
			Name:      "pitr01",
			TimeStamp: "2024-05-21 00:00:00",
		}

		_, err := doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)
	})

	convey.Convey("doRestorePitr check Pitr is legal", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		stmt := &tree.RestorePitr{
			Level:     tree.RESTORELEVELACCOUNT,
			Name:      "pitr01",
			TimeStamp: "2024-05-21 00:00:00",
		}

		_, err := doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)
	})
}

func TestCheckDbIsSubDb(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		createDbsql string
		want        bool
		wantErr     bool
	}{
		{
			name:        "SubscriptionOption exists",
			createDbsql: "create database sub01 from acc01 publication pub01;",
			want:        true,
			wantErr:     false,
		},
		{
			name:        "SubscriptionOption does not exist",
			createDbsql: "CREATE DATABASE test",
			want:        false,
			wantErr:     false,
		},
		{
			name:        "Invalid SQL",
			createDbsql: "INVALID SQL",
			want:        false,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := checkDbWhetherSub(ctx, tt.createDbsql)
			if (err != nil) != tt.wantErr {
				t.Errorf("checkDbIsSubDb() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("checkDbIsSubDb() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_restoreViews(t *testing.T) {
	convey.Convey("restoreViews", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		viewMap := map[string]*tableInfo{}
		err := restoreViews(ctx, ses, bh, "sp01", viewMap, 0, 0)
		assert.Error(t, err)

		sql := "select * from mo_catalog.mo_snapshots where sname = 'sp01'"
		// string/ string/ int64/ string/ string/ string/ string/ uint64
		mrs := newMrsForPitrRecord([][]interface{}{{"1", "sp01", int64(0), "ACCOUNT", "sys", "", "", uint64(1)}})
		bh.sql2result[sql] = mrs

		sql = "select account_id, account_name, status, version, suspended_time from mo_catalog.mo_account where 1=1 and account_name = 'sys'"
		mrs = newMrsForPitrRecord([][]interface{}{{uint64(0), "sys", "open", uint64(1), ""}})
		bh.sql2result[sql] = mrs

		err = restoreViews(ctx, ses, bh, "sp01", viewMap, 0, 0)
		assert.NoError(t, err)

		viewMap = map[string]*tableInfo{
			"view01": {
				dbName:    "db01",
				tblName:   "tbl01",
				typ:       "VIEW",
				createSql: "create view view01",
			},
		}
		err = restoreViews(ctx, ses, bh, "sp01", viewMap, 0, 0)
		assert.Error(t, err)
	})
}

func Test_restoreViewsWithPitr(t *testing.T) {
	convey.Convey("restoreViewsWithPitr", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		viewMap := map[string]*tableInfo{}
		err := restoreViewsWithPitr(ctx, ses, bh, "sp01", 0, viewMap, "sys", 0)
		assert.NoError(t, err)

		viewMap = map[string]*tableInfo{
			"view01": {
				dbName:    "db01",
				tblName:   "tbl01",
				typ:       "VIEW",
				createSql: "create view view01",
			},
		}
		err = restoreViewsWithPitr(ctx, ses, bh, "sp01", 0, viewMap, "sys", 0)
		assert.Error(t, err)

		viewMap = map[string]*tableInfo{
			"view01": {
				dbName:    "db01",
				tblName:   "tbl01",
				typ:       "VIEW",
				createSql: "create database db02",
			},
		}
		err = restoreViewsWithPitr(ctx, ses, bh, "sp01", 0, viewMap, "sys", 0)
		assert.NoError(t, err)
	})
}

func Test_RestoreOtherAccount(t *testing.T) {
	convey.Convey("doRestorePitr fail", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		ts := time.Now().Add(time.Duration(-2) * time.Hour).UnixNano()
		stmt := &tree.RestorePitr{
			Level: tree.RESTORELEVELACCOUNT,
			Name:  "pitr01",

			AccountName: "acc01",
			TimeStamp:   nanoTimeFormat(ts),
		}

		ses.SetTenantInfo(tenant)
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForCheckPitr(ctx, "pitr01", sysAccountID)
		assert.NoError(t, err)
		mrs := newMrsForPitrRecord([][]interface{}{{"018ee4cd-5991-7caa-b75d-f9290144bd9f"}})
		bh.sql2result[sql] = mrs

		sql = "select * from mo_catalog.mo_pitr where pitr_name = 'pitr01' and create_account = 0"
		mrs = newMrsForPitrRecord([][]interface{}{{
			"018ee4cd-5991-7caa-b75d-f9290144bd9f",
			"pitr01",
			uint64(0),
			"2024-05-01 00:00:00",
			"2024-05-01 00:00:00",
			"ACCOUNT",
			uint64(1),
			"acc01",
			"",
			"",
			uint64(1),
			uint8(1),
			"d",
		}})
		bh.sql2result[sql] = mrs

		resovleTs, err := doResolveTimeStamp(stmt.TimeStamp)
		assert.NoError(t, err)

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)

		sql = fmt.Sprintf("select account_id, account_name, admin_name, comments from mo_catalog.mo_account {MO_TS = %d } where account_name = '%s';", resovleTs, "acc01")
		mrs = newMrsForPitrRecord([][]interface{}{{uint64(1), "acc01", "root", ""}})
		bh.sql2result[sql] = mrs

		sql = "select account_id, account_name, status, version, suspended_time from mo_catalog.mo_account where 1=1 and account_name = 'acc01'"
		mrs = newMrsForPitrRecord([][]interface{}{{uint64(1), "acc01", "open", uint64(1), nil}})
		bh.sql2result[sql] = mrs

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)
	})

	convey.Convey("doRestorePitr fail", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		ts := time.Now().Add(time.Duration(-2) * time.Hour).UnixNano()
		stmt := &tree.RestorePitr{
			Level: tree.RESTORELEVELACCOUNT,
			Name:  "pitr01",

			AccountName: "acc02",
			TimeStamp:   nanoTimeFormat(ts),
		}

		ses.SetTenantInfo(tenant)
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForCheckPitr(ctx, "pitr01", sysAccountID)
		assert.NoError(t, err)
		mrs := newMrsForPitrRecord([][]interface{}{{"018ee4cd-5991-7caa-b75d-f9290144bd9f"}})
		bh.sql2result[sql] = mrs

		sql = "select * from mo_catalog.mo_pitr where pitr_name = 'pitr01' and create_account = 0"
		mrs = newMrsForPitrRecord([][]interface{}{{
			"018ee4cd-5991-7caa-b75d-f9290144bd9f",
			"pitr01",
			uint64(0),
			"2024-05-01 00:00:00",
			"2024-05-01 00:00:00",
			"ACCOUNT",
			uint64(1),
			"acc01",
			"",
			"",
			uint64(1),
			uint8(1),
			"d",
		}})
		bh.sql2result[sql] = mrs

		resovleTs, err := doResolveTimeStamp(stmt.TimeStamp)
		assert.NoError(t, err)

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)

		sql = fmt.Sprintf("select account_id, account_name, admin_name, comments from mo_catalog.mo_account {MO_TS = %d } where account_name = '%s';", resovleTs, "acc01")
		mrs = newMrsForPitrRecord([][]interface{}{{uint64(1), "acc01", "root", ""}})
		bh.sql2result[sql] = mrs

		sql = "select account_id, account_name, status, version, suspended_time from mo_catalog.mo_account where 1=1 and account_name = 'acc01'"
		mrs = newMrsForPitrRecord([][]interface{}{{uint64(1), "acc01", "open", uint64(1), nil}})
		bh.sql2result[sql] = mrs

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)
	})

	convey.Convey("doRestorePitr fail", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		ts := time.Now().Add(time.Duration(-2) * time.Hour).UnixNano()
		stmt := &tree.RestorePitr{
			Level: tree.RESTORELEVELACCOUNT,
			Name:  "pitr01",

			AccountName:    "acc01",
			SrcAccountName: "acc01",
			TimeStamp:      nanoTimeFormat(ts),
		}

		ses.SetTenantInfo(tenant)
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForCheckPitr(ctx, "pitr01", sysAccountID)
		assert.NoError(t, err)
		mrs := newMrsForPitrRecord([][]interface{}{{"018ee4cd-5991-7caa-b75d-f9290144bd9f"}})
		bh.sql2result[sql] = mrs

		sql = "select * from mo_catalog.mo_pitr where pitr_name = 'pitr01' and create_account = 0"
		mrs = newMrsForPitrRecord([][]interface{}{{
			"018ee4cd-5991-7caa-b75d-f9290144bd9f",
			"pitr01",
			uint64(0),
			"2024-05-01 00:00:00",
			"2024-05-01 00:00:00",
			"CLUSTER",
			uint64(1),
			"acc01",
			"",
			"",
			uint64(1),
			uint8(1),
			"d",
		}})
		bh.sql2result[sql] = mrs

		resovleTs, err := doResolveTimeStamp(stmt.TimeStamp)
		assert.NoError(t, err)

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)

		sql = fmt.Sprintf("select account_id, account_name, admin_name, comments from mo_catalog.mo_account {MO_TS = %d } where account_name = '%s';", resovleTs, "acc01")
		mrs = newMrsForPitrRecord([][]interface{}{{uint64(1), "acc01", "root", ""}})
		bh.sql2result[sql] = mrs

		sql = "select account_id, account_name, status, version, suspended_time from mo_catalog.mo_account where 1=1 and account_name = 'acc01'"
		mrs = newMrsForPitrRecord([][]interface{}{{uint64(1), "acc01", "open", uint64(1), nil}})
		bh.sql2result[sql] = mrs

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)
	})

	convey.Convey("doRestorePitr fail", t, func() {
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
		pu.SV.KillRountinesInterval = 0
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

		ts := time.Now().Add(time.Duration(-2) * time.Hour).UnixNano()
		stmt := &tree.RestorePitr{
			Level: tree.RESTORELEVELACCOUNT,
			Name:  "pitr01",

			AccountName:    "acc02",
			SrcAccountName: "acc01",
			TimeStamp:      nanoTimeFormat(ts),
		}

		ses.SetTenantInfo(tenant)
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForCheckPitr(ctx, "pitr01", sysAccountID)
		assert.NoError(t, err)
		mrs := newMrsForPitrRecord([][]interface{}{{"018ee4cd-5991-7caa-b75d-f9290144bd9f"}})
		bh.sql2result[sql] = mrs

		sql = "select * from mo_catalog.mo_pitr where pitr_name = 'pitr01' and create_account = 0"
		mrs = newMrsForPitrRecord([][]interface{}{{
			"018ee4cd-5991-7caa-b75d-f9290144bd9f",
			"pitr01",
			uint64(0),
			"2024-05-01 00:00:00",
			"2024-05-01 00:00:00",
			"CLUSTER",
			uint64(1),
			"acc01",
			"",
			"",
			uint64(1),
			uint8(1),
			"d",
		}})
		bh.sql2result[sql] = mrs

		resovleTs, err := doResolveTimeStamp(stmt.TimeStamp)
		assert.NoError(t, err)

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)

		sql = fmt.Sprintf("select account_id, account_name, admin_name, comments from mo_catalog.mo_account {MO_TS = %d } where account_name = '%s';", resovleTs, "acc01")
		mrs = newMrsForPitrRecord([][]interface{}{{uint64(1), "acc01", "root", ""}})
		bh.sql2result[sql] = mrs

		sql = "select account_id, account_name, status, version, suspended_time from mo_catalog.mo_account where 1=1 and account_name = 'acc01'"
		mrs = newMrsForPitrRecord([][]interface{}{{uint64(1), "acc01", "open", uint64(1), nil}})
		bh.sql2result[sql] = mrs

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)
	})
}

func Test_getPitrLengthAndUnit(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), sysAccountID)

	bh := &backgroundExecTest{}
	bh.init()

	bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
	defer bhStub.Reset()

	sql := getSqlForGetLengthAndUnitFmt(0, "account", "acc1", "", "")
	bh.sql2result[sql] = newMrsForPitrRecord([][]interface{}{
		{1, "h"},
	})
	length, unit, ok, err := getPitrLengthAndUnit(ctx, bh, "account", "acc1", "", "")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), length)
	assert.Equal(t, "h", unit)
	assert.True(t, ok)

	sql = getSqlForGetLengthAndUnitFmt(0, "database", "", "db", "")
	bh.sql2result[sql] = newMrsForPitrRecord([][]interface{}{})
	_, _, ok, err = getPitrLengthAndUnit(ctx, bh, "database", "", "db", "")
	assert.NoError(t, err)
	assert.False(t, ok)

	_, _, _, err = getPitrLengthAndUnit(ctx, bh, "table", "", "", "tbl")
	assert.Error(t, err)
}
