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
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func Test_checkPitrInValidDurtion(t *testing.T) {
	t.Run("check pitr unit is h", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue:    1,
			pitrUnit:     "h",
			modifiedTime: "2024-05-01 00:00:00",
		}
		err := checkPitrInValidDurtion(time.Now().UnixNano(), pitr)
		assert.NoError(t, err)
	})

	t.Run("check pitr unit is d", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue:    1,
			pitrUnit:     "d",
			modifiedTime: "2024-05-01 00:00:00",
		}
		err := checkPitrInValidDurtion(time.Now().UnixNano(), pitr)
		assert.NoError(t, err)
	})

	t.Run("check pitr unit is m", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue:    1,
			pitrUnit:     "mo",
			modifiedTime: "2024-05-01 00:00:00",
		}
		err := checkPitrInValidDurtion(time.Now().UnixNano(), pitr)
		assert.NoError(t, err)
	})

	t.Run("check pitr unit is y", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue:    1,
			pitrUnit:     "y",
			modifiedTime: "2024-05-01 00:00:00",
		}
		err := checkPitrInValidDurtion(time.Now().UnixNano(), pitr)
		assert.NoError(t, err)
	})

	t.Run("check pitr unit is h", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue:    1,
			pitrUnit:     "h",
			modifiedTime: "2024-05-01 00:00:00",
		}
		err := checkPitrInValidDurtion(time.Now().Add(time.Duration(-2)*time.Hour).UnixNano(), pitr)
		assert.Error(t, err)
	})

	t.Run("check pitr beyond range", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue:    1,
			pitrUnit:     "h",
			modifiedTime: "2024-05-01 00:00:00",
		}
		err := checkPitrInValidDurtion(time.Now().Add(time.Duration(2)*time.Hour).UnixNano(), pitr)
		assert.Error(t, err)
	})

	t.Run("check pitr beyond range 2", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue:    1,
			pitrUnit:     "d",
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
		setGlobalPu(pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx)
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
		sql := getPubInfoWithPitr(ts, "test")
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
		setGlobalPu(pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx)
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
		sql := getPubInfoWithPitr(ts, "test")
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
		setGlobalPu(pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx)
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
		sql := getPubInfoWithPitr(ts, "test")
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
		setGlobalPu(pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx)
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

		sql, err = getSqlForCheckAccountWithPitr(ctx, ts, ses.GetTenantName())
		assert.NoError(t, err)
		mrs = newMrsForPitrRecord([][]interface{}{{}})
		bh.sql2result[sql] = mrs

		err = doRestorePitr(ctx, ses, stmt)
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
		setGlobalPu(pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx)
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

		err = doRestorePitr(ctx, ses, stmt)
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
		setGlobalPu(pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx)
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

		err = doRestorePitr(ctx, ses, stmt)
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
		setGlobalPu(pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx)
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

		err = doRestorePitr(ctx, ses, stmt)
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
		setGlobalPu(pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx)
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

		err = doRestorePitr(ctx, ses, stmt)
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
		setGlobalPu(pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx)
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

		err = doRestorePitr(ctx, ses, stmt)
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
		setGlobalPu(pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx)
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

		err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)
	})
}
