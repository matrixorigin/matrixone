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
	"github.com/matrixorigin/matrixone/pkg/defines"
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

func Test_determinePrivilege(t *testing.T) {
	type arg struct {
		stmt tree.Statement
		priv *privilege
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

	for i := 0; i < len(args); i++ {
		args[i].priv = determinePrivilegeSetOfStatement(args[i].stmt)
	}

	convey.Convey("privilege of statement", t, func() {
		for i := 0; i < len(args); i++ {
			var priv *privilege
			priv = determinePrivilegeSetOfStatement(args[i].stmt)
			convey.So(priv, convey.ShouldResemble, args[i].priv)
		}
	})
}

func Test_determineCreateAccount(t *testing.T) {
	convey.Convey("create/drop/alter account succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.CreateAccount{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0}
		rowsOfMoRolePrivs := [][]interface{}{
			{0, true},
		}

		sql2result := makeSql2ExecResult(0, rowsOfMoUserGrant,
			roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs,
			nil, nil)

		var currentSql string

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string) error {
			currentSql = sql
			return nil
		}).AnyTimes()
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			return []interface{}{sql2result[currentSql]}
		}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		ok, err := authenticatePrivilegeOfStatementWithObjectTypeAccount(ses.GetRequestContext(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})
	convey.Convey("create/drop/alter account fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.CreateAccount{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0, 1}
		rowsOfMoRolePrivs := [][]interface{}{}

		//actually no role dependency loop
		roleIdsInMoRoleGrant := []int{0, 1}
		rowsOfMoRoleGrant := [][]interface{}{
			{1, true},
		}

		sql2result := makeSql2ExecResult(0, rowsOfMoUserGrant,
			roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs,
			roleIdsInMoRoleGrant, rowsOfMoRoleGrant)

		var currentSql string

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string) error {
			currentSql = sql
			return nil
		}).AnyTimes()
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			return []interface{}{sql2result[currentSql]}
		}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		ok, err := authenticatePrivilegeOfStatementWithObjectTypeAccount(ses.GetRequestContext(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})
}

func Test_determineCreateUser(t *testing.T) {
	convey.Convey("create user succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.CreateUser{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//without privilege create user, all
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{
			{0, true},
		}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant,
			roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs,
			nil, nil)

		var currentSql string

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string) error {
			currentSql = sql
			return nil
		}).AnyTimes()
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			return []interface{}{sql2result[currentSql]}
		}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		ok, err := authenticatePrivilegeOfStatementWithObjectTypeAccount(ses.GetRequestContext(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})
	convey.Convey("create user succ 2", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.CreateUser{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0, 1}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//role 0 without privilege create user, all, ownership
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		//role 1 with privilege create user
		rowsOfMoRolePrivs[1][0] = [][]interface{}{
			{1, true},
		}
		rowsOfMoRolePrivs[1][1] = [][]interface{}{}

		//grant role 1 to role 0
		roleIdsInMoRoleGrant := []int{0}
		rowsOfMoRoleGrant := make([][][]interface{}, len(roleIdsInMoRoleGrant))
		rowsOfMoRoleGrant[0] = [][]interface{}{
			{1, true},
		}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant,
			roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs,
			roleIdsInMoRoleGrant, rowsOfMoRoleGrant)

		var currentSql string

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string) error {
			currentSql = sql
			return nil
		}).AnyTimes()
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			return []interface{}{sql2result[currentSql]}
		}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		ok, err := authenticatePrivilegeOfStatementWithObjectTypeAccount(ses.GetRequestContext(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})
	convey.Convey("create user fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.CreateUser{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0, 1, 2}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//role 0 without privilege create user, all, ownership
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		//role 1 without privilege create user, all, ownership
		rowsOfMoRolePrivs[1][0] = [][]interface{}{}
		rowsOfMoRolePrivs[1][1] = [][]interface{}{}

		//role 2 without privilege create user, all, ownership
		rowsOfMoRolePrivs[2][0] = [][]interface{}{}
		rowsOfMoRolePrivs[2][1] = [][]interface{}{}

		roleIdsInMoRoleGrant := []int{0, 1, 2}
		rowsOfMoRoleGrant := make([][][]interface{}, len(roleIdsInMoRoleGrant))
		//grant role 1 to role 0
		rowsOfMoRoleGrant[0] = [][]interface{}{
			{1, true},
		}
		//grant role 2 to role 1
		rowsOfMoRoleGrant[1] = [][]interface{}{
			{2, true},
		}
		rowsOfMoRoleGrant[2] = [][]interface{}{}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant,
			roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs,
			roleIdsInMoRoleGrant, rowsOfMoRoleGrant)

		var currentSql string

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string) error {
			currentSql = sql
			return nil
		}).AnyTimes()
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			return []interface{}{sql2result[currentSql]}
		}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		ok, err := authenticatePrivilegeOfStatementWithObjectTypeAccount(ses.GetRequestContext(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})
}

func Test_determineDropUser(t *testing.T) {
	convey.Convey("drop/alter user succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.DropUser{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//with privilege drop user
		rowsOfMoRolePrivs[0][0] = [][]interface{}{
			{0, true},
		}
		//without privilege all
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant,
			roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs,
			nil, nil)

		var currentSql string

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string) error {
			currentSql = sql
			return nil
		}).AnyTimes()
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			return []interface{}{sql2result[currentSql]}
		}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		ok, err := authenticatePrivilegeOfStatementWithObjectTypeAccount(ses.GetRequestContext(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})
	convey.Convey("drop/alter user succ 2", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.DropUser{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0, 1}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//role 0 without privilege drop user, all, account/user ownership
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		//role 1 with privilege drop user
		rowsOfMoRolePrivs[1][0] = [][]interface{}{
			{1, true},
		}
		rowsOfMoRolePrivs[1][1] = [][]interface{}{}

		//grant role 1 to role 0
		roleIdsInMoRoleGrant := []int{0}
		rowsOfMoRoleGrant := make([][][]interface{}, len(roleIdsInMoRoleGrant))
		rowsOfMoRoleGrant[0] = [][]interface{}{
			{1, true},
		}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant,
			roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs,
			roleIdsInMoRoleGrant, rowsOfMoRoleGrant)

		var currentSql string

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string) error {
			currentSql = sql
			return nil
		}).AnyTimes()
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			return []interface{}{sql2result[currentSql]}
		}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		ok, err := authenticatePrivilegeOfStatementWithObjectTypeAccount(ses.GetRequestContext(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})
	convey.Convey("drop/alter user fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.DropUser{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0, 1, 2}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//role 0 without privilege drop user, all, ownership
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		//role 1 without privilege drop user, all, ownership
		rowsOfMoRolePrivs[1][0] = [][]interface{}{}
		rowsOfMoRolePrivs[1][1] = [][]interface{}{}

		//role 2 without privilege drop user, all, ownership
		rowsOfMoRolePrivs[2][0] = [][]interface{}{}
		rowsOfMoRolePrivs[2][1] = [][]interface{}{}

		roleIdsInMoRoleGrant := []int{0, 1, 2}
		rowsOfMoRoleGrant := make([][][]interface{}, len(roleIdsInMoRoleGrant))
		//grant role 1 to role 0
		rowsOfMoRoleGrant[0] = [][]interface{}{
			{1, true},
		}
		//grant role 2 to role 1
		rowsOfMoRoleGrant[1] = [][]interface{}{
			{2, true},
		}
		rowsOfMoRoleGrant[2] = [][]interface{}{}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant,
			roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs,
			roleIdsInMoRoleGrant, rowsOfMoRoleGrant)

		var currentSql string

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string) error {
			currentSql = sql
			return nil
		}).AnyTimes()
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			return []interface{}{sql2result[currentSql]}
		}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		ok, err := authenticatePrivilegeOfStatementWithObjectTypeAccount(ses.GetRequestContext(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})
}

func Test_determineCreateRole(t *testing.T) {
	convey.Convey("create role succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.CreateRole{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//with privilege create role
		rowsOfMoRolePrivs[0][0] = [][]interface{}{
			{0, true},
		}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant,
			roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs,
			nil, nil)

		var currentSql string

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string) error {
			currentSql = sql
			return nil
		}).AnyTimes()
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			return []interface{}{sql2result[currentSql]}
		}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		ok, err := authenticatePrivilegeOfStatementWithObjectTypeAccount(ses.GetRequestContext(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})
	convey.Convey("create role succ 2", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.CreateRole{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0, 1}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//role 0 without privilege create role, all, ownership
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		//role 1 with privilege create role
		rowsOfMoRolePrivs[1][0] = [][]interface{}{
			{1, true},
		}
		rowsOfMoRolePrivs[1][1] = [][]interface{}{}

		//grant role 1 to role 0
		roleIdsInMoRoleGrant := []int{0}
		rowsOfMoRoleGrant := make([][][]interface{}, len(roleIdsInMoRoleGrant))
		rowsOfMoRoleGrant[0] = [][]interface{}{
			{1, true},
		}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant,
			roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs,
			roleIdsInMoRoleGrant, rowsOfMoRoleGrant)

		var currentSql string

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string) error {
			currentSql = sql
			return nil
		}).AnyTimes()
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			return []interface{}{sql2result[currentSql]}
		}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		ok, err := authenticatePrivilegeOfStatementWithObjectTypeAccount(ses.GetRequestContext(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})
	convey.Convey("create role fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.CreateRole{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0, 1, 2}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//role 0 without privilege create role, all, ownership
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		//role 1 without privilege create role, all, ownership
		rowsOfMoRolePrivs[1][0] = [][]interface{}{}
		rowsOfMoRolePrivs[1][1] = [][]interface{}{}

		//role 2 without privilege create role, all, ownership
		rowsOfMoRolePrivs[2][0] = [][]interface{}{}
		rowsOfMoRolePrivs[2][1] = [][]interface{}{}

		roleIdsInMoRoleGrant := []int{0, 1, 2}
		rowsOfMoRoleGrant := make([][][]interface{}, len(roleIdsInMoRoleGrant))
		//grant role 1 to role 0
		rowsOfMoRoleGrant[0] = [][]interface{}{
			{1, true},
		}
		//grant role 2 to role 1
		rowsOfMoRoleGrant[1] = [][]interface{}{
			{2, true},
		}
		rowsOfMoRoleGrant[2] = [][]interface{}{}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant,
			roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs,
			roleIdsInMoRoleGrant, rowsOfMoRoleGrant)

		var currentSql string

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string) error {
			currentSql = sql
			return nil
		}).AnyTimes()
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			return []interface{}{sql2result[currentSql]}
		}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		ok, err := authenticatePrivilegeOfStatementWithObjectTypeAccount(ses.GetRequestContext(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})
}

func Test_determineDropRole(t *testing.T) {
	convey.Convey("drop/alter role succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.DropRole{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//with privilege drop role
		rowsOfMoRolePrivs[0][0] = [][]interface{}{
			{0, true},
		}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant,
			roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs,
			nil, nil)

		var currentSql string

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string) error {
			currentSql = sql
			return nil
		}).AnyTimes()
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			return []interface{}{sql2result[currentSql]}
		}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		ok, err := authenticatePrivilegeOfStatementWithObjectTypeAccount(ses.GetRequestContext(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})
	convey.Convey("drop/alter role succ 2", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.DropRole{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0, 1}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//role 0 without privilege drop role, all, ownership
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		//role 1 with privilege drop role
		rowsOfMoRolePrivs[1][0] = [][]interface{}{
			{1, true},
		}
		rowsOfMoRolePrivs[1][1] = [][]interface{}{}

		//grant role 1 to role 0
		roleIdsInMoRoleGrant := []int{0}
		rowsOfMoRoleGrant := make([][][]interface{}, len(roleIdsInMoRoleGrant))
		rowsOfMoRoleGrant[0] = [][]interface{}{
			{1, true},
		}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant,
			roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs,
			roleIdsInMoRoleGrant, rowsOfMoRoleGrant)

		var currentSql string

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string) error {
			currentSql = sql
			return nil
		}).AnyTimes()
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			return []interface{}{sql2result[currentSql]}
		}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		ok, err := authenticatePrivilegeOfStatementWithObjectTypeAccount(ses.GetRequestContext(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})
	convey.Convey("drop/alter role fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.DropRole{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0, 1, 2}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//role 0 without privilege drop role, all, ownership
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		//role 1 without privilege drop role, all, ownership
		rowsOfMoRolePrivs[1][0] = [][]interface{}{}
		rowsOfMoRolePrivs[1][1] = [][]interface{}{}

		//role 2 without privilege drop role, all, ownership
		rowsOfMoRolePrivs[2][0] = [][]interface{}{}
		rowsOfMoRolePrivs[2][1] = [][]interface{}{}

		roleIdsInMoRoleGrant := []int{0, 1, 2}
		rowsOfMoRoleGrant := make([][][]interface{}, len(roleIdsInMoRoleGrant))
		//grant role 1 to role 0
		rowsOfMoRoleGrant[0] = [][]interface{}{
			{1, true},
		}
		//grant role 2 to role 1
		rowsOfMoRoleGrant[1] = [][]interface{}{
			{2, true},
		}
		rowsOfMoRoleGrant[2] = [][]interface{}{}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant,
			roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs,
			roleIdsInMoRoleGrant, rowsOfMoRoleGrant)

		var currentSql string

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string) error {
			currentSql = sql
			return nil
		}).AnyTimes()
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			return []interface{}{sql2result[currentSql]}
		}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		ok, err := authenticatePrivilegeOfStatementWithObjectTypeAccount(ses.GetRequestContext(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})
}

func Test_determineGrantRole(t *testing.T) {
	convey.Convey("grant role succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.GrantRole{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//with privilege manage grants
		rowsOfMoRolePrivs[0][0] = [][]interface{}{
			{0, true},
		}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant,
			roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs,
			nil, nil)

		var currentSql string

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string) error {
			currentSql = sql
			return nil
		}).AnyTimes()
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			return []interface{}{sql2result[currentSql]}
		}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		ok, err := authenticatePrivilegeOfStatementWithObjectTypeAccount(ses.GetRequestContext(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("grant role succ 2", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.GrantRole{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0, 1}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//role 0 without privilege manage grants, all, ownership
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		//role 1 with privilege manage grants
		rowsOfMoRolePrivs[1][0] = [][]interface{}{
			{1, true},
		}
		rowsOfMoRolePrivs[1][1] = [][]interface{}{}

		//grant role 1 to role 0
		roleIdsInMoRoleGrant := []int{0}
		rowsOfMoRoleGrant := make([][][]interface{}, len(roleIdsInMoRoleGrant))
		rowsOfMoRoleGrant[0] = [][]interface{}{
			{1, true},
		}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant,
			roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs,
			roleIdsInMoRoleGrant, rowsOfMoRoleGrant)

		var currentSql string

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string) error {
			currentSql = sql
			return nil
		}).AnyTimes()
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			return []interface{}{sql2result[currentSql]}
		}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		ok, err := authenticatePrivilegeOfStatementWithObjectTypeAccount(ses.GetRequestContext(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("grant role succ 3 (mo_role_grant + with_grant_option)", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		roleNames := []string{
			"r1",
			"r2",
			"r3",
		}

		gr := &tree.GrantRole{}
		for _, name := range roleNames {
			gr.Roles = append(gr.Roles, &tree.Role{UserName: name})
		}
		priv := determinePrivilegeSetOfStatement(gr)
		ses := newSes(priv)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0, 1, 5, 6, 7}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//role 0 without privilege manage grants, all, ownership
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		//role 1 without privilege manage grants
		rowsOfMoRolePrivs[1][0] = [][]interface{}{}
		rowsOfMoRolePrivs[1][1] = [][]interface{}{}

		rowsOfMoRolePrivs[2][0] = [][]interface{}{}
		rowsOfMoRolePrivs[2][1] = [][]interface{}{}

		rowsOfMoRolePrivs[3][0] = [][]interface{}{}
		rowsOfMoRolePrivs[3][1] = [][]interface{}{}

		rowsOfMoRolePrivs[4][0] = [][]interface{}{}
		rowsOfMoRolePrivs[4][1] = [][]interface{}{}

		//grant role 1,5,6,7 to role 0
		roleIdsInMoRoleGrant := []int{0, 1, 5, 6, 7}
		rowsOfMoRoleGrant := make([][][]interface{}, len(roleIdsInMoRoleGrant))
		rowsOfMoRoleGrant[0] = [][]interface{}{
			{1, true},
			{5, true},
			{6, true},
			{7, true},
		}
		rowsOfMoRoleGrant[1] = [][]interface{}{}
		rowsOfMoRoleGrant[2] = [][]interface{}{}
		rowsOfMoRoleGrant[3] = [][]interface{}{}
		rowsOfMoRoleGrant[4] = [][]interface{}{}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant,
			roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs,
			roleIdsInMoRoleGrant, rowsOfMoRoleGrant)

		//fill mo_role
		rowsOfMoRole := make([][][]interface{}, len(roleNames))
		rowsOfMoRole[0] = [][]interface{}{
			{5},
		}
		rowsOfMoRole[1] = [][]interface{}{
			{6},
		}
		rowsOfMoRole[2] = [][]interface{}{
			{7},
		}
		makeRowsOfMoRole(sql2result, roleNames, rowsOfMoRole)

		var currentSql string

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string) error {
			currentSql = sql
			return nil
		}).AnyTimes()
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			return []interface{}{sql2result[currentSql]}
		}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		ok, err := authenticatePrivilegeOfStatementWithObjectTypeAccount(ses.GetRequestContext(), ses, gr)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("grant role succ 4 (mo_user_grant + with_grant_option)", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		roleNames := []string{
			"r1",
			"r2",
			"r3",
		}

		gr := &tree.GrantRole{}
		for _, name := range roleNames {
			gr.Roles = append(gr.Roles, &tree.Role{UserName: name})
		}
		priv := determinePrivilegeSetOfStatement(gr)
		ses := newSes(priv)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
			{5, true},
			{6, true},
			{7, true},
		}
		roleIdsInMoRolePrivs := []int{0, 1, 5, 6, 7}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//role 0 without privilege manage grants, all, ownership
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		//role 1 without privilege manage grants
		rowsOfMoRolePrivs[1][0] = [][]interface{}{}
		rowsOfMoRolePrivs[1][1] = [][]interface{}{}

		rowsOfMoRolePrivs[2][0] = [][]interface{}{}
		rowsOfMoRolePrivs[2][1] = [][]interface{}{}

		rowsOfMoRolePrivs[3][0] = [][]interface{}{}
		rowsOfMoRolePrivs[3][1] = [][]interface{}{}

		rowsOfMoRolePrivs[4][0] = [][]interface{}{}
		rowsOfMoRolePrivs[4][1] = [][]interface{}{}

		//grant role 1,5,6,7 to role 0
		roleIdsInMoRoleGrant := []int{0, 1, 5, 6, 7}
		rowsOfMoRoleGrant := make([][][]interface{}, len(roleIdsInMoRoleGrant))
		rowsOfMoRoleGrant[0] = [][]interface{}{
			{1, true},
		}
		rowsOfMoRoleGrant[1] = [][]interface{}{}
		rowsOfMoRoleGrant[2] = [][]interface{}{}
		rowsOfMoRoleGrant[3] = [][]interface{}{}
		rowsOfMoRoleGrant[4] = [][]interface{}{}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant,
			roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs,
			roleIdsInMoRoleGrant, rowsOfMoRoleGrant)

		//fill mo_role
		rowsOfMoRole := make([][][]interface{}, len(roleNames))
		rowsOfMoRole[0] = [][]interface{}{
			{5},
		}
		rowsOfMoRole[1] = [][]interface{}{
			{6},
		}
		rowsOfMoRole[2] = [][]interface{}{
			{7},
		}
		makeRowsOfMoRole(sql2result, roleNames, rowsOfMoRole)

		var currentSql string

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string) error {
			currentSql = sql
			return nil
		}).AnyTimes()
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			return []interface{}{sql2result[currentSql]}
		}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		ok, err := authenticatePrivilegeOfStatementWithObjectTypeAccount(ses.GetRequestContext(), ses, gr)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("grant role fail 1 (mo_user_grant + with_grant_option)", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		roleNames := []string{
			"r1",
			"r2",
			"r3",
		}

		gr := &tree.GrantRole{}
		for _, name := range roleNames {
			gr.Roles = append(gr.Roles, &tree.Role{UserName: name})
		}
		priv := determinePrivilegeSetOfStatement(gr)
		ses := newSes(priv)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
			{5, true},
			{6, false},
			{7, true},
		}
		roleIdsInMoRolePrivs := []int{0, 1, 5, 6, 7}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//role 0 without privilege manage grants, all, ownership
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		//role 1 without privilege manage grants
		rowsOfMoRolePrivs[1][0] = [][]interface{}{}
		rowsOfMoRolePrivs[1][1] = [][]interface{}{}

		rowsOfMoRolePrivs[2][0] = [][]interface{}{}
		rowsOfMoRolePrivs[2][1] = [][]interface{}{}

		rowsOfMoRolePrivs[3][0] = [][]interface{}{}
		rowsOfMoRolePrivs[3][1] = [][]interface{}{}

		rowsOfMoRolePrivs[4][0] = [][]interface{}{}
		rowsOfMoRolePrivs[4][1] = [][]interface{}{}

		//grant role 1,5,6,7 to role 0
		roleIdsInMoRoleGrant := []int{0, 1, 5, 6, 7}
		rowsOfMoRoleGrant := make([][][]interface{}, len(roleIdsInMoRoleGrant))
		rowsOfMoRoleGrant[0] = [][]interface{}{
			{1, true},
		}
		rowsOfMoRoleGrant[1] = [][]interface{}{}
		rowsOfMoRoleGrant[2] = [][]interface{}{}
		rowsOfMoRoleGrant[3] = [][]interface{}{}
		rowsOfMoRoleGrant[4] = [][]interface{}{}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant,
			roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs,
			roleIdsInMoRoleGrant, rowsOfMoRoleGrant)

		//fill mo_role
		rowsOfMoRole := make([][][]interface{}, len(roleNames))
		rowsOfMoRole[0] = [][]interface{}{
			{5},
		}
		rowsOfMoRole[1] = [][]interface{}{
			{6},
		}
		rowsOfMoRole[2] = [][]interface{}{
			{7},
		}
		makeRowsOfMoRole(sql2result, roleNames, rowsOfMoRole)

		var currentSql string

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string) error {
			currentSql = sql
			return nil
		}).AnyTimes()
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			return []interface{}{sql2result[currentSql]}
		}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		ok, err := authenticatePrivilegeOfStatementWithObjectTypeAccount(ses.GetRequestContext(), ses, gr)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})

	convey.Convey("grant role fail 2 (mo_role_grant + with_grant_option)", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		roleNames := []string{
			"r1",
			"r2",
			"r3",
		}

		gr := &tree.GrantRole{}
		for _, name := range roleNames {
			gr.Roles = append(gr.Roles, &tree.Role{UserName: name})
		}
		priv := determinePrivilegeSetOfStatement(gr)
		ses := newSes(priv)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0, 1, 5, 6, 7}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//role 0 without privilege manage grants, all, ownership
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		//role 1 without privilege manage grants
		rowsOfMoRolePrivs[1][0] = [][]interface{}{}
		rowsOfMoRolePrivs[1][1] = [][]interface{}{}

		rowsOfMoRolePrivs[2][0] = [][]interface{}{}
		rowsOfMoRolePrivs[2][1] = [][]interface{}{}

		rowsOfMoRolePrivs[3][0] = [][]interface{}{}
		rowsOfMoRolePrivs[3][1] = [][]interface{}{}

		rowsOfMoRolePrivs[4][0] = [][]interface{}{}
		rowsOfMoRolePrivs[4][1] = [][]interface{}{}

		//grant role 1,5,6,7 to role 0
		roleIdsInMoRoleGrant := []int{0, 1, 5, 6, 7}
		rowsOfMoRoleGrant := make([][][]interface{}, len(roleIdsInMoRoleGrant))
		rowsOfMoRoleGrant[0] = [][]interface{}{
			{1, true},
			{5, true},
			{6, false},
			{7, true},
		}
		rowsOfMoRoleGrant[1] = [][]interface{}{}
		rowsOfMoRoleGrant[2] = [][]interface{}{}
		rowsOfMoRoleGrant[3] = [][]interface{}{}
		rowsOfMoRoleGrant[4] = [][]interface{}{}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant,
			roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs,
			roleIdsInMoRoleGrant, rowsOfMoRoleGrant)

		//fill mo_role
		rowsOfMoRole := make([][][]interface{}, len(roleNames))
		rowsOfMoRole[0] = [][]interface{}{
			{5},
		}
		rowsOfMoRole[1] = [][]interface{}{
			{6},
		}
		rowsOfMoRole[2] = [][]interface{}{
			{7},
		}
		makeRowsOfMoRole(sql2result, roleNames, rowsOfMoRole)

		var currentSql string

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string) error {
			currentSql = sql
			return nil
		}).AnyTimes()
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			return []interface{}{sql2result[currentSql]}
		}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		ok, err := authenticatePrivilegeOfStatementWithObjectTypeAccount(ses.GetRequestContext(), ses, gr)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})
}

func Test_determineRevokeRole(t *testing.T) {
	convey.Convey("revoke role succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.RevokeRole{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//with privilege manage grants
		rowsOfMoRolePrivs[0][0] = [][]interface{}{
			{0, true},
		}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant,
			roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs,
			nil, nil)

		var currentSql string

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string) error {
			currentSql = sql
			return nil
		}).AnyTimes()
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			return []interface{}{sql2result[currentSql]}
		}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		ok, err := authenticatePrivilegeOfStatementWithObjectTypeAccount(ses.GetRequestContext(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("revoke role succ 2", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.RevokeRole{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0, 1}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//role 0 without privilege manage grants, all, ownership
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		//role 1 with privilege manage grants
		rowsOfMoRolePrivs[1][0] = [][]interface{}{
			{1, true},
		}
		rowsOfMoRolePrivs[1][1] = [][]interface{}{}

		//grant role 1 to role 0
		roleIdsInMoRoleGrant := []int{0}
		rowsOfMoRoleGrant := make([][][]interface{}, len(roleIdsInMoRoleGrant))
		rowsOfMoRoleGrant[0] = [][]interface{}{
			{1, true},
		}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant,
			roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs,
			roleIdsInMoRoleGrant, rowsOfMoRoleGrant)

		var currentSql string

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string) error {
			currentSql = sql
			return nil
		}).AnyTimes()
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			return []interface{}{sql2result[currentSql]}
		}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		ok, err := authenticatePrivilegeOfStatementWithObjectTypeAccount(ses.GetRequestContext(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("revoke role fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.RevokeRole{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0, 1, 2}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//role 0 without privilege drop role, all, ownership
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		//role 1 without privilege drop role, all, ownership
		rowsOfMoRolePrivs[1][0] = [][]interface{}{}
		rowsOfMoRolePrivs[1][1] = [][]interface{}{}

		//role 2 without privilege drop role, all, ownership
		rowsOfMoRolePrivs[2][0] = [][]interface{}{}
		rowsOfMoRolePrivs[2][1] = [][]interface{}{}

		roleIdsInMoRoleGrant := []int{0, 1, 2}
		rowsOfMoRoleGrant := make([][][]interface{}, len(roleIdsInMoRoleGrant))
		//grant role 1 to role 0
		rowsOfMoRoleGrant[0] = [][]interface{}{
			{1, true},
		}
		//grant role 2 to role 1
		rowsOfMoRoleGrant[1] = [][]interface{}{
			{2, true},
		}
		rowsOfMoRoleGrant[2] = [][]interface{}{}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant,
			roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs,
			roleIdsInMoRoleGrant, rowsOfMoRoleGrant)

		var currentSql string

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string) error {
			currentSql = sql
			return nil
		}).AnyTimes()
		bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
			return []interface{}{sql2result[currentSql]}
		}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		ok, err := authenticatePrivilegeOfStatementWithObjectTypeAccount(ses.GetRequestContext(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})
}

func newSes(priv *privilege) *Session {
	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil, nil, nil)
	pu.SV.SetDefaultValues()

	pu.HostMmu = host.New(pu.SV.HostMmuLimitation)
	pu.Mempool = mempool.New()
	ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

	proto := NewMysqlClientProtocol(0, nil, 1024, pu.SV)

	ses := NewSession(proto, nil, nil, pu, gSysVariables)
	tenant := &TenantInfo{
		Tenant:        sysAccountName,
		User:          rootName,
		DefaultRole:   moAdminRoleName,
		TenantID:      sysAccountID,
		UserID:        rootID,
		DefaultRoleID: moAdminRoleID,
	}
	ses.SetTenantInfo(tenant)
	ses.priv = priv
	ses.SetRequestContext(ctx)
	return ses
}

func newMrsForRoleIdOfRole(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("role_id")
	col1.SetColumnType(defines.MYSQL_TYPE_LONGLONG)

	mrs.AddColumn(col1)

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return mrs
}

func newMrsForRoleIdOfUserId(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("role_id")
	col1.SetColumnType(defines.MYSQL_TYPE_LONGLONG)

	col2 := &MysqlColumn{}
	col2.SetName("with_grant_option")
	col2.SetColumnType(defines.MYSQL_TYPE_BOOL)

	mrs.AddColumn(col1)
	mrs.AddColumn(col2)

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return mrs
}

func newMrsForCheckRoleHasPrivilege(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("role_id")
	col1.SetColumnType(defines.MYSQL_TYPE_LONGLONG)

	col2 := &MysqlColumn{}
	col2.SetName("with_grant_option")
	col2.SetColumnType(defines.MYSQL_TYPE_BOOL)

	mrs.AddColumn(col1)
	mrs.AddColumn(col2)

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return mrs
}

func newMrsForInheritedRoleIdOfRoleId(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("granted_id")
	col1.SetColumnType(defines.MYSQL_TYPE_LONGLONG)

	col2 := &MysqlColumn{}
	col2.SetName("with_grant_option")
	col2.SetColumnType(defines.MYSQL_TYPE_BOOL)

	mrs.AddColumn(col1)
	mrs.AddColumn(col2)

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return mrs
}

func makeRowsOfMoRole(sql2result map[string]ExecResult, roleNames []string, rows [][][]interface{}) {
	for i, name := range roleNames {
		sql2result[getSqlForRoleIdOfRole(name)] = newMrsForRoleIdOfRole(rows[i])
	}
}

func makeRowsOfMoUserGrant(sql2result map[string]ExecResult, userId int, rows [][]interface{}) {
	sql2result[getSqlForRoleIdOfUserId(userId)] = newMrsForRoleIdOfUserId(rows)
}

func makeRowsOfMoRolePrivs(sql2result map[string]ExecResult, roleIds []int, entries []privilegeEntry, rowsOfMoRolePrivs [][]interface{}) {
	for _, roleId := range roleIds {
		for _, entry := range entries {
			sql := getSqlForCheckRoleHasPrivilege(int64(roleId), entry.objType, int64(entry.objId), int64(entry.privilegeId))
			sql2result[sql] = newMrsForCheckRoleHasPrivilege(rowsOfMoRolePrivs)
		}
	}
}

func makeRowsOfMoRoleGrant(sql2result map[string]ExecResult, roleIds []int, rowsOfMoRoleGrant [][]interface{}) {
	for _, roleId := range roleIds {
		sql := getSqlForInheritedRoleIdOfRoleId(int64(roleId))
		sql2result[sql] = newMrsForInheritedRoleIdOfRoleId(rowsOfMoRoleGrant)
	}
}

func makeSql2ExecResult(userId int,
	rowsOfMoUserGrant [][]interface{},
	roleIdsInMoRolePrivs []int, entries []privilegeEntry, rowsOfMoRolePrivs [][]interface{},
	roleIdsInMoRoleGrant []int, rowsOfMoRoleGrant [][]interface{}) map[string]ExecResult {
	sql2result := make(map[string]ExecResult)
	makeRowsOfMoUserGrant(sql2result, userId, rowsOfMoUserGrant)
	makeRowsOfMoRolePrivs(sql2result, roleIdsInMoRolePrivs, entries, rowsOfMoRolePrivs)
	makeRowsOfMoRoleGrant(sql2result, roleIdsInMoRoleGrant, rowsOfMoRoleGrant)
	return sql2result
}

func makeSql2ExecResult2(userId int,
	rowsOfMoUserGrant [][]interface{},
	roleIdsInMoRolePrivs []int, entries []privilegeEntry, rowsOfMoRolePrivs [][][][]interface{},
	roleIdsInMoRoleGrant []int, rowsOfMoRoleGrant [][][]interface{}) map[string]ExecResult {
	sql2result := make(map[string]ExecResult)
	makeRowsOfMoUserGrant(sql2result, userId, rowsOfMoUserGrant)
	for i, roleId := range roleIdsInMoRolePrivs {
		for j, entry := range entries {
			sql := getSqlForCheckRoleHasPrivilege(int64(roleId), entry.objType, int64(entry.objId), int64(entry.privilegeId))
			sql2result[sql] = newMrsForCheckRoleHasPrivilege(rowsOfMoRolePrivs[i][j])
		}
	}

	for i, roleId := range roleIdsInMoRoleGrant {
		sql := getSqlForInheritedRoleIdOfRoleId(int64(roleId))
		sql2result[sql] = newMrsForInheritedRoleIdOfRoleId(rowsOfMoRoleGrant[i])
	}
	return sql2result
}
