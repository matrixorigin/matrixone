// Copyright 2021 Matrix Origin
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
	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/config"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/mempool"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestGetTenantInfo(t *testing.T) {
	convey.Convey("tenant", t, func() {
		type input struct {
			input   string
			output  string
			wantErr bool
		}
		args := []input{
			{"u1", "{tenantInfo sys:u1:public -- 0:0:0}", false},
			{"tenant1:u1", "{tenantInfo tenant1:u1:public -- 0:0:0}", false},
			{"tenant1:u1:r1", "{tenantInfo tenant1:u1:r1 -- 0:0:0}", false},
			{":u1:r1", "{tenantInfo tenant1:u1:r1 -- 0:0:0}", true},
			{"tenant1:u1:", "{tenantInfo tenant1:u1:r1 -- 0:0:0}", true},
			{"tenant1::r1", "{tenantInfo tenant1::r1 -- 0:0:0}", true},
			{"tenant1:    :r1", "{tenantInfo tenant1::r1 -- 0:0:0}", true},
			{"     : :r1", "{tenantInfo tenant1::r1 -- 0:0:0}", true},
			{"   tenant1   :   u1   :   r1    ", "{tenantInfo tenant1:u1:r1 -- 0:0:0}", false},
		}

		for _, arg := range args {
			ti, err := GetTenantInfo(arg.input)
			if arg.wantErr {
				convey.So(err, convey.ShouldNotBeNil)
			} else {
				convey.So(err, convey.ShouldBeNil)
				tis := ti.String()
				convey.So(tis, convey.ShouldEqual, arg.output)
			}
		}
	})

	convey.Convey("tenant op", t, func() {
		ti := &TenantInfo{}
		convey.So(ti.GetTenant(), convey.ShouldBeEmpty)
		convey.So(ti.GetTenantID(), convey.ShouldBeZeroValue)
		convey.So(ti.GetUser(), convey.ShouldBeEmpty)
		convey.So(ti.GetUserID(), convey.ShouldBeZeroValue)
		convey.So(ti.GetDefaultRole(), convey.ShouldBeEmpty)
		convey.So(ti.GetDefaultRoleID(), convey.ShouldBeZeroValue)

		ti.SetTenantID(10)
		convey.So(ti.GetTenantID(), convey.ShouldEqual, 10)
		ti.SetUserID(10)
		convey.So(ti.GetUserID(), convey.ShouldEqual, 10)
		ti.SetDefaultRoleID(10)
		convey.So(ti.GetDefaultRoleID(), convey.ShouldEqual, 10)

		convey.So(ti.IsSysTenant(), convey.ShouldBeFalse)
		convey.So(ti.IsDefaultRole(), convey.ShouldBeFalse)
		convey.So(ti.IsMoAdminRole(), convey.ShouldBeFalse)

		convey.So(GetDefaultTenant(), convey.ShouldEqual, sysAccountName)
		convey.So(GetDefaultRole(), convey.ShouldEqual, publicRoleName)
	})
}

func TestPrivilegeType_Scope(t *testing.T) {
	convey.Convey("scope", t, func() {
		pss := []struct {
			ps PrivilegeScope
			s  string
		}{
			{PrivilegeScopeSys, "sys"},
			{PrivilegeScopeAccount, "account"},
			{PrivilegeScopeUser, "user"},
			{PrivilegeScopeRole, "role"},
			{PrivilegeScopeDatabase, "database"},
			{PrivilegeScopeTable, "table"},
			{PrivilegeScopeRoutine, "routine"},
			{PrivilegeScopeSys | PrivilegeScopeRole | PrivilegeScopeRoutine, "sys,role,routine"},
			{PrivilegeScopeSys | PrivilegeScopeDatabase | PrivilegeScopeTable, "sys,database,table"},
		}
		for _, scope := range pss {
			convey.So(scope.ps.String(), convey.ShouldEqual, scope.s)
		}
	})
}

func TestPrivilegeType(t *testing.T) {
	convey.Convey("privilege type", t, func() {
		type arg struct {
			pt PrivilegeType
			s  string
			sc PrivilegeScope
		}
		args := []arg{}
		for i := PrivilegeTypeCreateAccount; i <= PrivilegeTypeExecute; i++ {
			args = append(args, arg{pt: i, s: i.String(), sc: i.Scope()})
		}
		for _, a := range args {
			convey.So(a.pt.String(), convey.ShouldEqual, a.s)
			convey.So(a.pt.Scope(), convey.ShouldEqual, a.sc)
		}
	})
}

func TestFormSql(t *testing.T) {
	convey.Convey("form sql", t, func() {
		convey.So(getSqlForCheckTenant("a"), convey.ShouldEqual, fmt.Sprintf(checkTenantFormat, "a"))
		convey.So(getSqlForPasswordOfUser("u"), convey.ShouldEqual, fmt.Sprintf(getPasswordOfUserFormat, "u"))
		convey.So(getSqlForCheckRoleExists(0, "r"), convey.ShouldEqual, fmt.Sprintf(checkRoleExistsFormat, 0, "r"))
		convey.So(getSqlForRoleIdOfRole("r"), convey.ShouldEqual, fmt.Sprintf(roleIdOfRoleFormat, "r"))
		convey.So(getSqlForRoleOfUser(0, "r"), convey.ShouldEqual, fmt.Sprintf(getRoleOfUserFormat, 0, "r"))
	})
}

func Test_checkSysExistsOrNot(t *testing.T) {
	convey.Convey("check sys tenant exists or not", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil, nil, nil)
		pu.SV.SetDefaultValues()

		pu.HostMmu = host.New(pu.SV.HostMmuLimitation)
		pu.Mempool = mempool.New()

		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New().DoAndReturn(
			func(ootions ...client.TxnOption) (client.TxnOperator, error) {
				return txnOperator, nil
			}).AnyTimes()
		eng := mock_frontend.NewMockEngine(ctrl)

		pu.StorageEngine = eng
		pu.TxnClient = txnClient

		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		exists, err := checkSysExistsOrNot(ctx, pu)
		convey.So(exists, convey.ShouldBeFalse)
		convey.So(err, convey.ShouldBeNil)
	})
}
