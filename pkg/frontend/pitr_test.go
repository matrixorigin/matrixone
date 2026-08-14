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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
			createTime:   time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
			modifiedTime: time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
		}
		err := checkPitrInValidDurtion(time.Now().UnixNano(), pitr)
		assert.NoError(t, err)
	})

	t.Run("check pitr unit is d", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue:    1,
			pitrUnit:     "d",
			createTime:   time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
			modifiedTime: time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
		}
		err := checkPitrInValidDurtion(time.Now().UnixNano(), pitr)
		assert.NoError(t, err)
	})

	t.Run("check pitr unit is m", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue:    1,
			pitrUnit:     "mo",
			createTime:   time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
			modifiedTime: time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
		}
		err := checkPitrInValidDurtion(time.Now().UnixNano(), pitr)
		assert.NoError(t, err)
	})

	t.Run("check pitr unit is y", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue:    1,
			pitrUnit:     "y",
			createTime:   time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
			modifiedTime: time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
		}
		err := checkPitrInValidDurtion(time.Now().UnixNano(), pitr)
		assert.NoError(t, err)
	})

	t.Run("check pitr unit is h", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue:    1,
			pitrUnit:     "h",
			createTime:   time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
			modifiedTime: time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
		}
		err := checkPitrInValidDurtion(time.Now().Add(time.Duration(-2)*time.Hour).UnixNano(), pitr)
		assert.Error(t, err)
	})

	t.Run("check pitr beyond range", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue:    1,
			pitrUnit:     "h",
			createTime:   time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
			modifiedTime: time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
		}
		err := checkPitrInValidDurtion(time.Now().Add(time.Duration(2)*time.Hour).UnixNano(), pitr)
		assert.Error(t, err)
	})

	t.Run("check pitr beyond range 2", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue:    1,
			pitrUnit:     "d",
			createTime:   time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
			modifiedTime: time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
		}
		err := checkPitrInValidDurtion(time.Now().Add(time.Duration(25)*time.Hour).UnixNano(), pitr)
		assert.Error(t, err)
	})
}

func Test_checkPitrValidOrNot_AllowsExplicitCurrentAccountForScopedRestore(t *testing.T) {
	tenant := &TenantInfo{
		Tenant:   "acc01",
		TenantID: 101,
	}
	pitr := &pitrRecord{
		pitrName:     "pitr01",
		level:        tree.PITRLEVELACCOUNT.String(),
		accountId:    101,
		accountName:  "acc01",
		databaseName: "db01",
		tableName:    "t01",
	}

	err := checkPitrValidOrNot(pitr, &tree.RestorePitr{
		Level:        tree.RESTORELEVELDATABASE,
		AccountName:  "acc01",
		DatabaseName: "db01",
	}, tenant)
	require.NoError(t, err)

	err = checkPitrValidOrNot(pitr, &tree.RestorePitr{
		Level:        tree.RESTORELEVELTABLE,
		AccountName:  "acc01",
		DatabaseName: "db01",
		TableName:    "t01",
	}, tenant)
	require.NoError(t, err)
}

func Test_checkPitrValidOrNot_RejectsOtherAccountForScopedRestore(t *testing.T) {
	tenant := &TenantInfo{
		Tenant:   "acc01",
		TenantID: 101,
	}
	pitr := &pitrRecord{
		pitrName:     "pitr01",
		level:        tree.PITRLEVELACCOUNT.String(),
		accountId:    101,
		accountName:  "acc01",
		databaseName: "db01",
		tableName:    "t01",
	}

	err := checkPitrValidOrNot(pitr, &tree.RestorePitr{
		Level:        tree.RESTORELEVELDATABASE,
		AccountName:  "acc02",
		DatabaseName: "db01",
	}, tenant)
	require.Error(t, err)

	err = checkPitrValidOrNot(pitr, &tree.RestorePitr{
		Level:        tree.RESTORELEVELTABLE,
		AccountName:  "acc02",
		DatabaseName: "db01",
		TableName:    "t01",
	}, tenant)
	require.Error(t, err)
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
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
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
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
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
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
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
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
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
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
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
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
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
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
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
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
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
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
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
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
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
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
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

		sql = buildTableInfoListSQL("db1", "", resovleTs, uint32(sysAccountID))
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)

		sql = fmt.Sprintf(checkDatabaseIsMasterFormat, quoteSQLStringLiteral("db1"), quoteSQLStringLiteral("db1"))
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
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
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

		sql = buildTableInfoListSQL("db1", "", resovleTs, uint32(sysAccountID))
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
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
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

		sql = buildTableInfoListSQL("db1", "", resovleTs, uint32(sysAccountID))
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
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
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

		sql = buildTableInfoListSQL("db1", "", resovleTs, uint32(sysAccountID))
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
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
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

		sql = buildTableInfoListSQL("db1", "", resovleTs, uint32(sysAccountID))
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
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
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

		sql = buildTableInfoListSQL("db1", "", resovleTs, uint32(sysAccountID))
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
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
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

		sql = buildTableInfoListSQL("db1", "", resovleTs, uint32(sysAccountID))
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
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
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

		sql = buildTableInfoListSQL("db1", "", resovleTs, uint32(sysAccountID))
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
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
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

		sql = buildTableInfoListSQL("db1", "", resovleTs, uint32(sysAccountID))
		mrs = newMrsForPitrRecord([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err = doRestorePitr(ctx, ses, stmt)
		assert.Error(t, err)

		sql = buildTableInfoListSQL(moCatalog, "", resovleTs, uint32(sysAccountID))
		mrs = newMrsForRestoreStringRows([]string{"relname", "table_type", "relkind", "viewdef"}, [][]interface{}{
			{"mo_user", "BASE TABLE", "r"},
		})
		bh.sql2result[sql] = mrs

		err = restoreSystemDatabaseWithPitr(ctx, "", bh, "pitr01", resovleTs, 0)
		assert.Error(t, err)

		sql = buildTableInfoListSQL(moCatalog, "", resovleTs, uint32(sysAccountID))
		mrs = newMrsForRestoreStringRows([]string{"relname", "table_type", "relkind", "viewdef"}, [][]interface{}{
			{"mo_user", "BASE TABLE", "r"},
		})
		bh.sql2result[sql] = mrs

		err = restoreSystemDatabase(ctx, "", bh, "pitr01", 0, resovleTs, 0)
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
				time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
				time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
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
				time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
				time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
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
				time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
				time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
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

		commitErr := errors.New("pitr commit conflict")
		bh.sql2err["commit;"] = commitErr
		err = doCreatePitr(ctx, ses, stmt)
		assert.ErrorIs(t, err, commitErr)
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

		var (
			err         error
			viewMap     = map[string]*tableInfo{}
			sortedViews []string
		)

		_, err = sortedViewInfos(
			ctx, ses, bh, "sp01", nil, viewMap, 0, 0)
		assert.Error(t, err)

		//err = restoreViews(ctx, ses, bh, "sp01", viewMap, 0, sortedViews)
		//assert.Error(t, err)

		sql := "select * from mo_catalog.mo_snapshots where sname = 'sp01'"
		// string/ string/ int64/ string/ string/ string/ string/ uint64
		mrs := newMrsForSnapshotRecord("1", "sp01", int64(0), "ACCOUNT", "sys", "", "", uint64(1))
		bh.sql2result[sql] = mrs

		sql = "select account_id, account_name, status, version, suspended_time from mo_catalog.mo_account where 1=1 and account_name = 'sys'"
		mrs = newMrsForPitrRecord([][]interface{}{{uint64(0), "sys", "open", uint64(1), ""}})
		bh.sql2result[sql] = mrs

		sortedViews, err = sortedViewInfos(
			ctx, ses, bh, "sp01", nil, viewMap, 0, 0)
		require.NoError(t, err)

		err = restoreViews(ctx, ses, bh, "sp01", viewMap, 0, sortedViews, false)
		assert.NoError(t, err)

		viewMap = map[string]*tableInfo{
			genKey("quote`db", "quote view"): {
				dbName:    "quote`db",
				tblName:   "quote view",
				typ:       "VIEW",
				createSql: "create view `quote``db`.`quote view` as select 1",
			},
		}
		sortedViews = []string{genKey("quote`db", "quote view")}
		bh.executedSQLs = nil

		err = restoreViews(ctx, ses, bh, "sp01", viewMap, 0, sortedViews, false)
		require.NoError(t, err)
		require.Equal(t, []string{
			"use `quote``db`",
			"drop view if exists `quote view`",
			"create view `quote``db`.`quote view` as select 1",
		}, bh.executedSQLs)

		viewMap = map[string]*tableInfo{
			"view01": {
				dbName:    "db01",
				tblName:   "tbl01",
				typ:       "VIEW",
				createSql: "create view view01",
			},
		}

		_, err = sortedViewInfos(
			ctx, ses, bh, "sp01", nil, viewMap, 0, 0)
		assert.Error(t, err)
		//
		//err = restoreViews(ctx, ses, bh, "sp01", viewMap, 0, sortedViews)
		//assert.Error(t, err)
	})
}

func Test_restoreViewsSkipMissingDependency(t *testing.T) {
	convey.Convey("restoreViews skips missing dependency for clone", t, func() {
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

		missingSQL := "create view skip_v as select * from missing_t"
		okSQL := "create view ok_v as select 1"

		viewMap := map[string]*tableInfo{
			genKey("db01", "skip_v"): {
				dbName:    "db01",
				tblName:   "skip_v",
				typ:       "VIEW",
				createSql: missingSQL,
			},
			genKey("db01", "ok_v"): {
				dbName:    "db01",
				tblName:   "ok_v",
				typ:       "VIEW",
				createSql: okSQL,
			},
		}
		sortedViews := []string{genKey("db01", "skip_v"), genKey("db01", "ok_v")}

		testCases := []struct {
			name       string
			missingErr error
		}{
			{
				name:       "no such table",
				missingErr: moerr.NewNoSuchTable(ctx, "db01", "missing_t"),
			},
			{
				name:       "parse missing table",
				missingErr: moerr.NewParseErrorf(ctx, "table %q does not exist", "missing_t"),
			},
		}

		for _, tc := range testCases {
			bh.sql2err = map[string]error{missingSQL: tc.missingErr}
			bh.executedSQLs = nil

			err := restoreViews(ctx, ses, bh, "sp01", viewMap, 0, sortedViews, true)
			require.NoError(t, err, tc.name)
			require.Contains(t, bh.executedSQLs, okSQL, tc.name)

			bh.executedSQLs = nil
			err = restoreViews(ctx, ses, bh, "sp01", viewMap, 0, sortedViews, false)
			require.Error(t, err, tc.name)
			require.NotContains(t, bh.executedSQLs, okSQL, tc.name)
		}
	})
}

func Test_canSkipRestoreViewError(t *testing.T) {
	ctx := context.Background()
	testCases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil",
			err:  nil,
			want: false,
		},
		{
			name: "no such table",
			err:  moerr.NewNoSuchTable(ctx, "db01", "missing_t"),
			want: true,
		},
		{
			name: "bad database",
			err:  moerr.NewBadDB(ctx, "missing_db"),
			want: true,
		},
		{
			name: "parse missing table",
			err:  moerr.NewParseErrorf(ctx, "table %q does not exist", "missing_t"),
			want: true,
		},
		{
			name: "parse missing column",
			err:  moerr.NewParseErrorf(ctx, "column %q does not exist", "missing_c"),
			want: false,
		},
		{
			name: "other error",
			err:  moerr.NewInternalError(ctx, "boom"),
			want: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, canSkipRestoreViewError(tc.err))
		})
	}
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
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
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
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
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
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
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
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
			time.Now().UnixNano() - 60*24*time.Hour.Nanoseconds(),
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

func newPitrLifecycleTestSession(
	t *testing.T,
) (*Session, *backgroundExecTest, context.Context) {
	t.Helper()
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	bh := &backgroundExecTest{}
	bh.init()
	registerEmptyHistoricalLineageResults(bh)
	bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
	t.Cleanup(func() {
		bhStub.Reset()
		ses.Close()
		ctrl.Finish()
	})

	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	pu.SV.SetDefaultValues()
	pu.SV.KillRountinesInterval = 0
	setPu("", pu)
	ctx := context.WithValue(context.Background(), config.ParameterUnitKey, pu)
	rm, _ := NewRoutineManager(ctx, "")
	ses.rm = rm
	ses.SetTenantInfo(&TenantInfo{
		Tenant:        sysAccountName,
		User:          rootName,
		DefaultRole:   moAdminRoleName,
		TenantID:      sysAccountID,
		UserID:        rootID,
		DefaultRoleID: moAdminRoleID,
	})

	bh.sql2result["begin;"] = nil
	bh.sql2result["commit;"] = nil
	bh.sql2result["rollback;"] = nil
	return ses, bh, ctx
}

func TestDoDropPitrCompactsHistoricalAlterLineage(t *testing.T) {
	ses, bh, ctx := newPitrLifecycleTestSession(t)
	stmt := &tree.DropPitr{Name: "pitr01"}

	checkSQL, err := getSqlForCheckPitr(ctx, "pitr01", sysAccountID)
	require.NoError(t, err)
	bh.sql2result[checkSQL] = newMrsForPitrRecord([][]interface{}{{"pitr-id"}})
	bh.sql2result[getSqlForDropPitr("pitr01", sysAccountID)] = nil
	otherSQL := fmt.Sprintf(getPitrFormat+" where pitr_name != '%s';", SYSMOCATALOGPITR)
	bh.sql2result[otherSQL] = newMrsForPitrRecord(nil)
	bh.sql2result[getSqlForDropPitr(SYSMOCATALOGPITR, sysAccountID)] = nil

	require.NoError(t, doDropPitr(ctx, ses, stmt))
	require.Contains(t, bh.executedSQLs, historicalAlterLineageMetadataSQL())
}

func TestDoAlterPitrCompactsHistoricalAlterLineage(t *testing.T) {
	ses, bh, ctx := newPitrLifecycleTestSession(t)
	stmt := &tree.AlterPitr{Name: "pitr01", PitrValue: 1, PitrUnit: "h"}

	checkSQL, err := getSqlForCheckPitr(ctx, "pitr01", sysAccountID)
	require.NoError(t, err)
	bh.sql2result[checkSQL] = newMrsForPitrRecord([][]interface{}{{"pitr-id"}})

	require.NoError(t, doAlterPitr(ctx, ses, stmt))
	require.Contains(t, bh.executedSQLs, historicalAlterLineageMetadataSQL())
}

// Test_unservableViewErrorIsIdentifiable pins the contract the restore paths rely on.
//
// restoreViews (snapshot.go) and restoreViewsWithPitr (pitr.go) DROP a view before
// re-creating it from the snapshot. Since #27027, a defining SELECT whose MATCH() no
// FULLTEXT index can serve is refused at CREATE -- and such views could be created before
// that guard existed, so a snapshot may legitimately hold one. If the refusal escaped, a
// single unrunnable legacy view would abort the entire account restore with the view
// already dropped, which is strictly worse than the bug being fixed. Both paths therefore
// skip on this specific error, identified BY CODE so the wording can change freely.
//
// canSkipRestoreViewError must NOT claim it: that predicate means "a dependency is
// missing", it is gated on skipIfDependencyMissing, and RESTORE ACCOUNT passes false --
// routing this error through it would leave the abort in place for the case that matters.
func Test_unservableViewErrorIsIdentifiable(t *testing.T) {
	ctx := context.Background()

	refusal := moerr.NewFtMatchingKeyNotFound(ctx)
	require.True(t, moerr.IsMoErrCode(refusal, moerr.ErrFtMatchingKeyNotFound))
	require.False(t, canSkipRestoreViewError(refusal),
		"the refusal is skipped on its own terms, not as a missing dependency")

	// Unrelated invalid-input errors must stay fatal during restore -- swallowing them
	// would hide real corruption.
	require.False(t, moerr.IsMoErrCode(moerr.NewInvalidInput(ctx, "something else"),
		moerr.ErrFtMatchingKeyNotFound))
	require.False(t, moerr.IsMoErrCode(moerr.NewInternalError(ctx, "boom"),
		moerr.ErrFtMatchingKeyNotFound))

	// It carries MySQL's ER_FT_MATCHING_KEY_NOT_FOUND (1191) wording and code, which is
	// what MySQL returns for the same rejected CREATE / ALTER / REPLACE VIEW.
	require.Contains(t, refusal.Error(), "Can't find FULLTEXT index matching the column list")
	require.Equal(t, moerr.ER_FT_MATCHING_KEY_NOT_FOUND, refusal.MySQLCode(),
		"clients must see MySQL's 1191 for this rejection, as MySQL does")
}

// Test_restoreViewsSkipsUnservableView is the regression for the worst failure this guard
// could cause.
//
// Since #27027 a view whose MATCH() no FULLTEXT index can serve is refused at CREATE. Such
// views could be created before that guard existed, so a snapshot may hold one. restoreViews
// DROPS each view before re-creating it from the snapshot, so if the refusal escaped, one
// unrunnable legacy view would abort the entire account restore WITH THE VIEW ALREADY GONE
// -- strictly worse than the bug being fixed, and unrecoverable, since the definition only
// exists inside the snapshot.
//
// The skipIfDependencyMissing=false case is the one that matters: RESTORE ACCOUNT passes
// false (snapshot.go), so a fix routed through canSkipRestoreViewError would not have helped
// it. Both flag values must continue past the refusal and restore the remaining views.
//
// restoreViewsWithPitr (pitr.go) carries the identical tolerance -- it had no skip at all
// before, just `return err` after the drop -- but is not covered by an executable test here:
// it calls GetSubscriptionMeta before reaching the view loop, which this mock session cannot
// satisfy (nil dereference at pitr.go:1787). Its contract is pinned instead by
// Test_unservableViewErrorIsIdentifiable, which is what both paths key on.
func Test_restoreViewsSkipsUnservableView(t *testing.T) {
	convey.Convey("restoreViews skips a view that can never run", t, func() {
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

		badSQL := "create view ft_v as select id from docs where match(body) against('x')"
		okSQL := "create view ok_v as select 1"

		viewMap := map[string]*tableInfo{
			genKey("db01", "ft_v"): {
				dbName: "db01", tblName: "ft_v", typ: "VIEW", createSql: badSQL,
			},
			genKey("db01", "ok_v"): {
				dbName: "db01", tblName: "ok_v", typ: "VIEW", createSql: okSQL,
			},
		}
		sortedViews := []string{genKey("db01", "ft_v"), genKey("db01", "ok_v")}

		for _, skipIfDependencyMissing := range []bool{false, true} {
			bh.sql2err = map[string]error{badSQL: moerr.NewFtMatchingKeyNotFound(ctx)}
			bh.executedSQLs = nil

			err := restoreViews(ctx, ses, bh, "sp01", viewMap, 0, sortedViews, skipIfDependencyMissing)
			require.NoError(t, err,
				"an unrunnable legacy view must not abort the restore (skip=%v)", skipIfDependencyMissing)
			require.Contains(t, bh.executedSQLs, okSQL,
				"the remaining views must still be restored (skip=%v)", skipIfDependencyMissing)
		}

		// An unrelated failure must still abort: only the specific refusal is tolerated,
		// so genuine corruption is never silently swallowed.
		bh.sql2err = map[string]error{badSQL: moerr.NewInternalError(ctx, "boom")}
		bh.executedSQLs = nil
		err := restoreViews(ctx, ses, bh, "sp01", viewMap, 0, sortedViews, false)
		require.Error(t, err, "an unrelated error must remain fatal")

		// The background executor reconstructs errors, so by the time restore sees one the
		// moerr code is gone and only the text remains -- an end-to-end PITR restore aborted
		// with ERROR 1191 while a code-only guard sat right there. Injecting the error
		// directly (as the cases above do) cannot reproduce that, so pin the degraded form
		// explicitly.
		bh.sql2err = map[string]error{
			badSQL: moerr.NewInternalError(ctx, moerr.FtMatchingKeyNotFoundMsg),
		}
		bh.executedSQLs = nil
		err = restoreViews(ctx, ses, bh, "sp01", viewMap, 0, sortedViews, false)
		require.NoError(t, err, "the refusal must be recognised even once its code is lost")
		require.Contains(t, bh.executedSQLs, okSQL)

		// A view the dependency sort already marked unservable must be left ENTIRELY alone:
		// not created, and NOT DROPPED either. The snapshot may hold an unrunnable definition
		// while the target holds a WORKING view of that name (its FULLTEXT index was recreated
		// after the snapshot was taken), so dropping what we cannot re-create would delete a
		// working object and put nothing back. A stale object is an inconsistency; deleting a
		// working one is data loss.
		marked := map[string]*tableInfo{
			genKey("db01", "ft_v"): {
				dbName: "db01", tblName: "ft_v", typ: "VIEW",
				createSql: badSQL, unservable: true,
			},
			genKey("db01", "ok_v"): {
				dbName: "db01", tblName: "ok_v", typ: "VIEW", createSql: okSQL,
			},
		}
		bh.sql2err = nil
		bh.executedSQLs = nil
		err = restoreViews(ctx, ses, bh, "sp01", marked, 0, sortedViews, false)
		require.NoError(t, err)
		require.Contains(t, bh.executedSQLs, okSQL, "the other views still restore")
		require.NotContains(t, bh.executedSQLs, badSQL, "the marked view is not re-created")
		require.NotContains(t, bh.executedSQLs, dropViewIfExistsSQL("ft_v"),
			"and it is not dropped: we must not destroy an object we cannot replace")
		require.Contains(t, bh.executedSQLs, dropViewIfExistsSQL("ok_v"),
			"an ordinary view is still dropped and re-created")
	})
}

// Test_markUnservableViewInSort covers the #27027 tolerance that all three view-restore
// dependency sorts share (snapshot, PITR, and the cluster-snapshot path). It was three
// near-verbatim copies, and the third never got the tolerance at all — a cluster restore
// still aborted on a view the other two skipped.
func Test_markUnservableViewInSort(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	ctx := context.Background()

	newSort := func() toposort { return toposort{next: make(map[string][]string)} }

	t.Run("the refusal marks the view and KEEPS its vertex", func(t *testing.T) {
		g := newSort()
		v := &tableInfo{dbName: "db01", tblName: "ft_v"}
		handled := markUnservableViewInSort(ses, "sp01", v, &g, genKey("db01", "ft_v"),
			moerr.NewFtMatchingKeyNotFound(ctx))
		require.True(t, handled)
		require.True(t, v.unservable)
		_, ok := g.next[genKey("db01", "ft_v")]
		require.True(t, ok,
			"the vertex must stay: dropped from the graph the restore never visits it, and "+
				"dependents lose their ordering against it")
		sorted, err := g.sort()
		require.NoError(t, err)
		require.Contains(t, sorted, genKey("db01", "ft_v"))
	})

	t.Run("recognised once the executor has stripped the moerr code", func(t *testing.T) {
		g := newSort()
		v := &tableInfo{dbName: "db01", tblName: "ft_v"}
		require.True(t, markUnservableViewInSort(ses, "sp01", v, &g, genKey("db01", "ft_v"),
			moerr.NewInternalError(ctx, moerr.FtMatchingKeyNotFoundMsg)))
		require.True(t, v.unservable)
	})

	t.Run("any other error is left to abort the restore", func(t *testing.T) {
		g := newSort()
		v := &tableInfo{dbName: "db01", tblName: "ft_v"}
		require.False(t, markUnservableViewInSort(ses, "sp01", v, &g, genKey("db01", "ft_v"),
			moerr.NewInternalError(ctx, "boom")))
		require.False(t, v.unservable)
		require.Empty(t, g.next, "a genuine failure must not be silently absorbed into the graph")
	})
}

// Test_skipUnservableViewInRestore: a marked view is left ENTIRELY alone by the create loop.
func Test_skipUnservableViewInRestore(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	defer ses.Close()

	require.True(t, skipUnservableViewInRestore(ses, "sp01",
		&tableInfo{dbName: "db01", tblName: "ft_v", unservable: true}))
	require.False(t, skipUnservableViewInRestore(ses, "sp01",
		&tableInfo{dbName: "db01", tblName: "ok_v"}))
}
