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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func Test_checkPitrInValidDurtion(t *testing.T) {
	t.Run("check pitr unit is h", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue: 1,
			pitrUnit:  "h",
		}
		err := checkPitrInValidDurtion(time.Now().UnixNano(), pitr)
		assert.NoError(t, err)
	})

	t.Run("check pitr unit is d", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue: 1,
			pitrUnit:  "d",
		}
		err := checkPitrInValidDurtion(time.Now().UnixNano(), pitr)
		assert.NoError(t, err)
	})

	t.Run("check pitr unit is m", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue: 1,
			pitrUnit:  "mo",
		}
		err := checkPitrInValidDurtion(time.Now().UnixNano(), pitr)
		assert.NoError(t, err)
	})

	t.Run("check pitr unit is y", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue: 1,
			pitrUnit:  "y",
		}
		err := checkPitrInValidDurtion(time.Now().UnixNano(), pitr)
		assert.NoError(t, err)
	})

	t.Run("check pitr unit is h", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue: 1,
			pitrUnit:  "h",
		}
		err := checkPitrInValidDurtion(time.Now().Add(time.Duration(-2)*time.Hour).UnixNano(), pitr)
		assert.Error(t, err)
	})

	t.Run("check pitr beyond range", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue: 1,
			pitrUnit:  "h",
		}
		err := checkPitrInValidDurtion(time.Now().Add(time.Duration(2)*time.Hour).UnixNano(), pitr)
		assert.Error(t, err)
	})

	t.Run("check pitr beyond range 2", func(t *testing.T) {
		pitr := &pitrRecord{
			pitrValue: 1,
			pitrUnit:  "d",
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
