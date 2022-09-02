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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/mempool"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/prashantv/gostub"
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
			{"u1", "{tenantInfo sys:u1:moadmin -- 0:0:0}", false},
			{"tenant1:u1", "{tenantInfo tenant1:u1:moadmin -- 0:0:0}", false},
			{"tenant1:u1:r1", "{tenantInfo tenant1:u1:r1 -- 0:0:0}", false},
			{":u1:r1", "{tenantInfo tenant1:u1:r1 -- 0:0:0}", true},
			{"tenant1:u1:", "{tenantInfo tenant1:u1:moadmin -- 0:0:0}", true},
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
		convey.So(GetDefaultRole(), convey.ShouldEqual, moAdminRoleName)
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
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		bh.EXPECT().ClearExecResultSet().Return().AnyTimes()

		mrs1 := mock_frontend.NewMockExecResult(ctrl)
		dbs := make([]string, 0)
		for k := range sysWantedDatabases {
			dbs = append(dbs, k)
		}
		mrs1.EXPECT().GetRowCount().Return(uint64(len(sysWantedDatabases))).AnyTimes()
		mrs1.EXPECT().GetString(gomock.Any(), gomock.Any()).DoAndReturn(func(r uint64, c uint64) (string, error) {
			return dbs[r], nil
		}).AnyTimes()

		mrs2 := mock_frontend.NewMockExecResult(ctrl)
		tables := make([]string, 0)
		for k := range sysWantedTables {
			tables = append(tables, k)
		}

		mrs2.EXPECT().GetRowCount().Return(uint64(len(sysWantedTables))).AnyTimes()
		mrs2.EXPECT().GetString(gomock.Any(), gomock.Any()).DoAndReturn(func(r uint64, c uint64) (string, error) {
			return tables[r], nil
		}).AnyTimes()

		rs := []ExecResult{
			mrs1,
			mrs2,
		}

		cnt := 0
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			old := cnt
			cnt++
			if cnt >= len(rs) {
				cnt = 0
			}
			return []interface{}{rs[old]}
		}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		exists, err := checkSysExistsOrNot(ctx, pu)
		convey.So(exists, convey.ShouldBeTrue)
		convey.So(err, convey.ShouldBeNil)

		err = InitSysTenant(ctx)
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_createTablesInMoCatalog(t *testing.T) {
	convey.Convey("createTablesInMoCatalog", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil, nil, nil)
		pu.SV.SetDefaultValues()

		pu.HostMmu = host.New(pu.SV.HostMmuLimitation)
		pu.Mempool = mempool.New()
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		tenant := &TenantInfo{
			Tenant:        sysAccountName,
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      sysAccountID,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}

		err := createTablesInMoCatalog(ctx, tenant, pu)
		convey.So(err, convey.ShouldBeNil)

		err = createTablesInInformationSchema(ctx, tenant, pu)
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_checkTenantExistsOrNot(t *testing.T) {
	convey.Convey("check tenant exists or not", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil, nil, nil)
		pu.SV.SetDefaultValues()

		pu.HostMmu = host.New(pu.SV.HostMmuLimitation)
		pu.Mempool = mempool.New()
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		mrs1 := mock_frontend.NewMockExecResult(ctrl)
		mrs1.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()

		bh.EXPECT().GetExecResultSet().Return([]interface{}{mrs1}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		exists, err := checkTenantExistsOrNot(ctx, pu, "test")
		convey.So(exists, convey.ShouldBeTrue)
		convey.So(err, convey.ShouldBeNil)

		tenant := &TenantInfo{
			Tenant:        sysAccountName,
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      sysAccountID,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}

		err = InitGeneralTenant(ctx, tenant, &tree.CreateAccount{Name: "test", IfNotExists: true})
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_createTablesInMoCatalogOfGeneralTenant(t *testing.T) {
	convey.Convey("createTablesInMoCatalog", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil, nil, nil)
		pu.SV.SetDefaultValues()

		pu.HostMmu = host.New(pu.SV.HostMmuLimitation)
		pu.Mempool = mempool.New()
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		tenant := &TenantInfo{
			Tenant:        sysAccountName,
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      sysAccountID,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}

		ca := &tree.CreateAccount{
			Name:        "test",
			IfNotExists: true,
			AuthOption: tree.AccountAuthOption{
				AdminName:      "test_root",
				IdentifiedType: tree.AccountIdentified{Typ: tree.AccountIdentifiedByPassword, Str: "123"}},
			Comment: tree.AccountComment{Exist: true, Comment: "test acccount"},
		}

		newTi, err := createTablesInMoCatalogOfGeneralTenant(ctx, tenant, pu, ca)
		convey.So(err, convey.ShouldBeNil)

		err = createTablesInInformationSchemaOfGeneralTenant(ctx, tenant, pu, newTi)
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_checkUserExistsOrNot(t *testing.T) {
	convey.Convey("checkUserExistsOrNot", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil, nil, nil)
		pu.SV.SetDefaultValues()

		pu.HostMmu = host.New(pu.SV.HostMmuLimitation)
		pu.Mempool = mempool.New()
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		mrs1 := mock_frontend.NewMockExecResult(ctrl)
		mrs1.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()

		bh.EXPECT().GetExecResultSet().Return([]interface{}{mrs1}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		exists, err := checkUserExistsOrNot(ctx, pu, "test")
		convey.So(exists, convey.ShouldBeTrue)
		convey.So(err, convey.ShouldBeNil)

		tenant := &TenantInfo{
			Tenant:        sysAccountName,
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      sysAccountID,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}

		err = InitUser(ctx, tenant, &tree.CreateUser{IfNotExists: true, Users: []*tree.User{{Username: "test"}}})
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_initUser(t *testing.T) {
	convey.Convey("init user", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil, nil, nil)
		pu.SV.SetDefaultValues()

		pu.HostMmu = host.New(pu.SV.HostMmuLimitation)
		pu.Mempool = mempool.New()
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		rs := mock_frontend.NewMockExecResult(ctrl)
		//first time, return 1,
		//second time, return 0,
		cnt := 0
		rs.EXPECT().GetRowCount().DoAndReturn(func() uint64 {
			cnt++
			if cnt == 1 {
				return 1
			} else {
				return 0
			}
		}).AnyTimes()
		rs.EXPECT().GetInt64(gomock.Any(), gomock.Any()).Return(int64(10), nil).AnyTimes()
		bh.EXPECT().GetExecResultSet().Return([]interface{}{rs}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		tenant := &TenantInfo{
			Tenant:        sysAccountName,
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      sysAccountID,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}

		cu := &tree.CreateUser{
			IfNotExists: true,
			Users: []*tree.User{
				{
					Username:   "u1",
					AuthOption: &tree.AccountIdentified{Typ: tree.AccountIdentifiedByPassword, Str: "123"},
				},
				{
					Username:   "u2",
					AuthOption: &tree.AccountIdentified{Typ: tree.AccountIdentifiedByPassword, Str: "123"},
				},
			},
			Role:    &tree.Role{UserName: "test_role"},
			MiscOpt: &tree.UserMiscOptionAccountUnlock{},
		}

		err := InitUser(ctx, tenant, cu)
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_initRole(t *testing.T) {
	convey.Convey("init role", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil, nil, nil)
		pu.SV.SetDefaultValues()

		pu.HostMmu = host.New(pu.SV.HostMmuLimitation)
		pu.Mempool = mempool.New()
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		rs := mock_frontend.NewMockExecResult(ctrl)
		rs.EXPECT().GetRowCount().Return(uint64(0)).AnyTimes()
		bh.EXPECT().GetExecResultSet().Return([]interface{}{rs}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		tenant := &TenantInfo{
			Tenant:        sysAccountName,
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      sysAccountID,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}

		cr := &tree.CreateRole{
			IfNotExists: true,
			Roles: []*tree.Role{
				{UserName: "r1"},
				{UserName: "r2"},
			},
		}

		err := InitRole(ctx, tenant, cr)
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_determinePrivilegeSetOfStatement(t *testing.T) {
	convey.Convey("privilege of statement", t, func() {
		type arg struct {
			stmt tree.Statement
			priv privilege
		}

		args := []arg{
			{stmt: &tree.CreateAccount{}},
			{stmt: &tree.DropAccount{}},
			{stmt: &tree.AlterAccount{}},
			{stmt: &tree.CreateUser{}},
			{stmt: &tree.DropUser{}},
			{stmt: &tree.AlterUser{}},
			{stmt: &tree.CreateRole{}},
			{stmt: &tree.DropRole{}},
			{stmt: &tree.GrantRole{}},
			{stmt: &tree.RevokeRole{}},
			{stmt: &tree.GrantPrivilege{}},
			{stmt: &tree.RevokePrivilege{}},
			{stmt: &tree.CreateDatabase{}},
			{stmt: &tree.DropDatabase{}},
			{stmt: &tree.ShowDatabases{}},
			{stmt: &tree.ShowCreateDatabase{}},
			{stmt: &tree.Use{}},
			{stmt: &tree.ShowTables{}},
			{stmt: &tree.ShowCreateTable{}},
			{stmt: &tree.ShowColumns{}},
			{stmt: &tree.ShowCreateView{}},
			{stmt: &tree.CreateTable{}},
			{stmt: &tree.CreateView{}},
			{stmt: &tree.DropTable{}},
			{stmt: &tree.DropView{}},
			{stmt: &tree.Select{}},
			{stmt: &tree.Insert{}},
			{stmt: &tree.Load{}},
			{stmt: &tree.Update{}},
			{stmt: &tree.Delete{}},
			{stmt: &tree.CreateIndex{}},
			{stmt: &tree.DropIndex{}},
			{stmt: &tree.ShowIndex{}},
			{stmt: &tree.ShowProcessList{}},
			{stmt: &tree.ShowErrors{}},
			{stmt: &tree.ShowWarnings{}},
			{stmt: &tree.ShowVariables{}},
			{stmt: &tree.ShowStatus{}},
			{stmt: &tree.ExplainFor{}},
			{stmt: &tree.ExplainAnalyze{}},
			{stmt: &tree.ExplainStmt{}},
			{stmt: &tree.BeginTransaction{}},
			{stmt: &tree.CommitTransaction{}},
			{stmt: &tree.RollbackTransaction{}},
			{stmt: &tree.SetVar{}},
			{stmt: &tree.SetDefaultRole{}},
			{stmt: &tree.SetRole{}},
			{stmt: &tree.SetPassword{}},
			{stmt: &tree.PrepareStmt{}},
			{stmt: &tree.PrepareString{}},
			{stmt: &tree.Deallocate{}},
		}

		var err error
		for i := 0; i < len(args); i++ {
			args[i].priv, err = determinePrivilegeSetOfStatement(args[i].stmt)
			convey.So(err, convey.ShouldBeNil)
		}
		for i := 0; i < len(args); i++ {
			var priv privilege
			priv, err = determinePrivilegeSetOfStatement(args[i].stmt)
			convey.So(err, convey.ShouldBeNil)
			convey.So(priv, convey.ShouldResemble, args[i].priv)
		}
	})
}
