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
	"bytes"
	"context"
	"fmt"
	"go/constant"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/fagongzi/goetty/v2"
	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"

	"github.com/matrixorigin/matrixone/pkg/container/batch"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

func TestGetTenantInfo(t *testing.T) {
	convey.Convey("tenant", t, func() {
		type input struct {
			input   string
			output  string
			wantErr bool
		}
		args := []input{
			{"u1", "{account sys:u1: -- 0:0:0}", false},
			{"tenant1:u1", "{account tenant1:u1: -- 0:0:0}", false},
			{"tenant1:u1:r1", "{account tenant1:u1:r1 -- 0:0:0}", false},
			{":u1:r1", "{account tenant1:u1:r1 -- 0:0:0}", true},
			{"tenant1:u1:", "{account tenant1:u1:moadmin -- 0:0:0}", true},
			{"tenant1::r1", "{account tenant1::r1 -- 0:0:0}", true},
			{"tenant1:    :r1", "{account tenant1:    :r1 -- 0:0:0}", false},
			{"     : :r1", "{account      : :r1 -- 0:0:0}", false},
			{"   tenant1   :   u1   :   r1    ", "{account    tenant1   :   u1   :   r1     -- 0:0:0}", false},
			{"u1", "{account sys:u1: -- 0:0:0}", false},
			{"tenant1#u1", "{account tenant1#u1# -- 0#0#0}", false},
			{"tenant1#u1#r1", "{account tenant1#u1#r1 -- 0#0#0}", false},
			{"#u1#r1", "{account tenant1#u1#r1 -- 0#0#0}", true},
			{"tenant1#u1#", "{account tenant1#u1#moadmin -- 0#0#0}", true},
			{"tenant1##r1", "{account tenant1##r1 -- 0#0#0}", true},
			{"tenant1#    #r1", "{account tenant1#    #r1 -- 0#0#0}", false},
			{"     # #r1", "{account      # #r1 -- 0#0#0}", false},
			{"   tenant1   #   u1   #   r1    ", "{account    tenant1   #   u1   #   r1     -- 0#0#0}", false},
		}

		for _, arg := range args {
			ti, err := GetTenantInfo(context.TODO(), arg.input)
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
		sql, _ := getSqlForCheckTenant(context.TODO(), "a")
		convey.So(sql, convey.ShouldEqual, fmt.Sprintf(checkTenantFormat, "a"))
		sql, _ = getSqlForPasswordOfUser(context.TODO(), "u")
		convey.So(sql, convey.ShouldEqual, fmt.Sprintf(getPasswordOfUserFormat, "u"))
		sql, _ = getSqlForCheckRoleExists(context.TODO(), 0, "r")
		convey.So(sql, convey.ShouldEqual, fmt.Sprintf(checkRoleExistsFormat, 0, "r"))
		sql, _ = getSqlForRoleIdOfRole(context.TODO(), "r")
		convey.So(sql, convey.ShouldEqual, fmt.Sprintf(roleIdOfRoleFormat, "r"))
		sql, _ = getSqlForRoleOfUser(context.TODO(), 0, "r")
		convey.So(sql, convey.ShouldEqual, fmt.Sprintf(getRoleOfUserFormat, 0, "r"))
	})
}

func Test_checkSysExistsOrNot(t *testing.T) {
	convey.Convey("check sys tenant exists or not", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()

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
		mrs1.EXPECT().GetString(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx2 context.Context, r uint64, c uint64) (string, error) {
			return dbs[r], nil
		}).AnyTimes()

		mrs2 := mock_frontend.NewMockExecResult(ctrl)
		tables := make([]string, 0)
		for k := range sysWantedTables {
			tables = append(tables, k)
		}

		mrs2.EXPECT().GetRowCount().Return(uint64(len(sysWantedTables))).AnyTimes()
		mrs2.EXPECT().GetString(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx2 context.Context, r uint64, c uint64) (string, error) {
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

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		exists, err := checkSysExistsOrNot(ctx, bh)
		convey.So(exists, convey.ShouldBeTrue)
		convey.So(err, convey.ShouldBeNil)

		// A mock autoIncrCaches.
		aicm := &defines.AutoIncrCacheManager{}
		finalVersion := "1.2.0"
		err = InitSysTenantOld(ctx, aicm, finalVersion)
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_checkTenantExistsOrNot(t *testing.T) {
	convey.Convey("check tenant exists or not", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		setGlobalPu(pu)

		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		mrs1 := mock_frontend.NewMockExecResult(ctrl)
		mrs1.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()

		bh.EXPECT().GetExecResultSet().Return([]interface{}{mrs1}).AnyTimes()
		bh.EXPECT().ClearExecResultSet().Return().AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		exists, err := checkTenantExistsOrNot(ctx, bh, "test")
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

		ses := newSes(nil, ctrl)
		ses.tenant = tenant

		err = InitGeneralTenant(ctx, ses, &createAccount{
			Name:        "test",
			IfNotExists: true,
			AdminName:   "root",
			IdentTyp:    tree.AccountIdentifiedByPassword,
			IdentStr:    "123456",
		})
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_checkDatabaseExistsOrNot(t *testing.T) {
	convey.Convey("check databse exists or not", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()

		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		mrs1 := mock_frontend.NewMockExecResult(ctrl)
		mrs1.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()

		bh.EXPECT().GetExecResultSet().Return([]interface{}{mrs1}).AnyTimes()
		bh.EXPECT().ClearExecResultSet().Return().AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		exists, err := checkDatabaseExistsOrNot(ctx, bh, "test")
		convey.So(exists, convey.ShouldBeTrue)
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_createTablesInMoCatalogOfGeneralTenant(t *testing.T) {
	convey.Convey("createTablesInMoCatalog", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()

		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		bh.EXPECT().ClearExecResultSet().Return().AnyTimes()
		msr := newMrsForCheckTenant([][]interface{}{
			{1, "test"},
		})
		bh.EXPECT().GetExecResultSet().Return([]interface{}{msr}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		//tenant := &TenantInfo{
		//	Tenant:        sysAccountName,
		//	User:          rootName,
		//	DefaultRole:   moAdminRoleName,
		//	TenantID:      sysAccountID,
		//	UserID:        rootID,
		//	DefaultRoleID: moAdminRoleID,
		//}

		ca := &createAccount{
			Name:        "test",
			IfNotExists: true,
			AdminName:   "test_root",
			IdentTyp:    tree.AccountIdentifiedByPassword,
			IdentStr:    "123",
			Comment:     tree.AccountComment{Exist: true, Comment: "test acccount"},
		}
		finalVersion := "1.2.0"
		_, _, err := createTablesInMoCatalogOfGeneralTenant(ctx, bh, finalVersion, ca)
		convey.So(err, convey.ShouldBeNil)

		err = createTablesInInformationSchemaOfGeneralTenant(ctx, bh)
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_initFunction(t *testing.T) {
	convey.Convey("init function", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		setGlobalPu(pu)

		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().ClearExecResultSet().AnyTimes()
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		rs := mock_frontend.NewMockExecResult(ctrl)
		rs.EXPECT().GetRowCount().Return(uint64(0)).AnyTimes()
		bh.EXPECT().GetExecResultSet().Return([]interface{}{rs}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		locale := ""

		cu := &tree.CreateFunction{
			Name: tree.NewFuncName("testFunc",
				tree.ObjectNamePrefix{
					SchemaName:      tree.Identifier("db"),
					CatalogName:     tree.Identifier(""),
					ExplicitSchema:  true,
					ExplicitCatalog: false,
				},
			),
			Args: nil,
			ReturnType: tree.NewReturnType(&tree.T{
				InternalType: tree.InternalType{
					Family:       tree.IntFamily,
					FamilyString: "INT",
					Width:        24,
					Locale:       &locale,
					Oid:          uint32(defines.MYSQL_TYPE_INT24),
				},
			}),
			Body:     "",
			Language: "sql",
		}

		tenant := &TenantInfo{
			Tenant:        sysAccountName,
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      sysAccountID,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}

		ses := &Session{
			feSessionImpl: feSessionImpl{
				tenant: tenant,
			},
		}
		err := InitFunction(ses, newTestExecCtx(ctx, ctrl), tenant, cu)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func Test_initUser(t *testing.T) {
	convey.Convey("init user", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()

		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		sql2result := make(map[string]ExecResult)

		cu := &createUser{
			IfNotExists: true,
			Users: []*user{
				{
					Username:  "u1",
					AuthExist: true,
					IdentTyp:  tree.AccountIdentifiedByPassword,
					IdentStr:  "123",
				},
				{
					Username:  "u2",
					AuthExist: true,
					IdentTyp:  tree.AccountIdentifiedByPassword,
					IdentStr:  "123",
				},
			},
			Role:    &tree.Role{UserName: "test_role"},
			MiscOpt: &tree.UserMiscOptionAccountUnlock{},
		}

		mrs := newMrsForRoleIdOfRole([][]interface{}{
			{10},
		})

		sql, _ := getSqlForRoleIdOfRole(context.TODO(), cu.Role.UserName)
		sql2result[sql] = mrs

		for _, user := range cu.Users {
			sql, _ = getSqlForPasswordOfUser(context.TODO(), user.Username)
			mrs = newMrsForPasswordOfUser([][]interface{}{})
			sql2result[sql] = mrs

			sql, _ := getSqlForRoleIdOfRole(context.TODO(), user.Username)
			mrs = newMrsForRoleIdOfRole([][]interface{}{})
			sql2result[sql] = mrs
		}

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		tenant := &TenantInfo{
			Tenant:        sysAccountName,
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      sysAccountID,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}

		ses := &Session{}
		err := InitUser(ctx, ses, tenant, cu)
		convey.So(err, convey.ShouldBeError)
	})
}

func Test_initRole(t *testing.T) {
	convey.Convey("init role", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()

		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().ClearExecResultSet().AnyTimes()
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		rs := mock_frontend.NewMockExecResult(ctrl)
		rs.EXPECT().GetRowCount().Return(uint64(0)).AnyTimes()
		bh.EXPECT().GetExecResultSet().Return([]interface{}{rs}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
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
		ses := &Session{}

		err := InitRole(ctx, ses, tenant, cr)
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
		{stmt: &tree.ShowBackendServers{}},
	}

	for i := 0; i < len(args); i++ {
		args[i].priv = determinePrivilegeSetOfStatement(args[i].stmt)
	}

	convey.Convey("privilege of statement", t, func() {
		for i := 0; i < len(args); i++ {
			priv := determinePrivilegeSetOfStatement(args[i].stmt)
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
		ses := newSes(priv, ctrl)

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

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("create/drop/alter account fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.CreateAccount{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

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

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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
		ses := newSes(priv, ctrl)

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

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, nil, nil, nil, nil)

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})
	convey.Convey("create user succ 2", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.CreateUser{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

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

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, roleIdsInMoRoleGrant, rowsOfMoRoleGrant, nil, nil)

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})
	convey.Convey("create user fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.CreateUser{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

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

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, roleIdsInMoRoleGrant, rowsOfMoRoleGrant, nil, nil)

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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
		ses := newSes(priv, ctrl)

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

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, nil, nil, nil, nil)

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})
	convey.Convey("drop/alter user succ 2", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.DropUser{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

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

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, roleIdsInMoRoleGrant, rowsOfMoRoleGrant, nil, nil)

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})
	convey.Convey("drop/alter user fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.DropUser{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

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

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, roleIdsInMoRoleGrant, rowsOfMoRoleGrant, nil, nil)

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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
		ses := newSes(priv, ctrl)

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

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, nil, nil, nil, nil)

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})
	convey.Convey("create role succ 2", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.CreateRole{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

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

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, roleIdsInMoRoleGrant, rowsOfMoRoleGrant, nil, nil)

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})
	convey.Convey("create role fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.CreateRole{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

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

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, roleIdsInMoRoleGrant, rowsOfMoRoleGrant, nil, nil)

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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
		ses := newSes(priv, ctrl)

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

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, nil, nil, nil, nil)

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})
	convey.Convey("drop/alter role succ 2", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.DropRole{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

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

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, roleIdsInMoRoleGrant, rowsOfMoRoleGrant, nil, nil)

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})
	convey.Convey("drop/alter role fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.DropRole{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

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

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, roleIdsInMoRoleGrant, rowsOfMoRoleGrant, nil, nil)

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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
		ses := newSes(priv, ctrl)

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
			nil, nil, nil, nil)

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("grant role succ 2", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.GrantRole{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

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
			roleIdsInMoRoleGrant, rowsOfMoRoleGrant, nil, nil)
		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		g := &tree.Grant{
			Typ: tree.GrantTypeRole,
		}
		gr := &g.GrantRole
		for _, name := range roleNames {
			gr.Roles = append(gr.Roles, &tree.Role{UserName: name})
		}
		priv := determinePrivilegeSetOfStatement(g)
		ses := newSes(priv, ctrl)

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

		grantedIds := []int{0, 1, 5, 6, 7}
		granteeRows := make([][][]interface{}, len(grantedIds))
		granteeRows[0] = [][]interface{}{}
		granteeRows[1] = [][]interface{}{}
		granteeRows[2] = [][]interface{}{
			{0},
		}
		granteeRows[3] = [][]interface{}{
			{0},
		}
		granteeRows[4] = [][]interface{}{
			{0},
		}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant,
			roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs,
			roleIdsInMoRoleGrant, rowsOfMoRoleGrant,
			grantedIds, granteeRows)

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

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, g)
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

		g := &tree.Grant{
			Typ: tree.GrantTypeRole,
		}
		gr := &g.GrantRole
		for _, name := range roleNames {
			gr.Roles = append(gr.Roles, &tree.Role{UserName: name})
		}
		priv := determinePrivilegeSetOfStatement(g)
		ses := newSes(priv, ctrl)

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

		grantedIds := []int{0, 1, 5, 6, 7}
		granteeRows := make([][][]interface{}, len(grantedIds))
		granteeRows[0] = [][]interface{}{}
		granteeRows[1] = [][]interface{}{}
		granteeRows[2] = [][]interface{}{
			{0},
		}
		granteeRows[3] = [][]interface{}{
			{0},
		}
		granteeRows[4] = [][]interface{}{
			{0},
		}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant,
			roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs,
			roleIdsInMoRoleGrant, rowsOfMoRoleGrant,
			grantedIds, granteeRows)

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

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, g)
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

		g := &tree.Grant{
			Typ: tree.GrantTypeRole,
		}
		gr := &g.GrantRole
		for _, name := range roleNames {
			gr.Roles = append(gr.Roles, &tree.Role{UserName: name})
		}
		priv := determinePrivilegeSetOfStatement(g)
		ses := newSes(priv, ctrl)

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

		grantedIds := []int{0, 1, 5, 6, 7}
		granteeRows := make([][][]interface{}, len(grantedIds))
		granteeRows[0] = [][]interface{}{}
		granteeRows[1] = [][]interface{}{}
		granteeRows[2] = [][]interface{}{
			{0},
		}
		granteeRows[3] = [][]interface{}{}
		granteeRows[4] = [][]interface{}{
			{0},
		}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant,
			roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs,
			roleIdsInMoRoleGrant, rowsOfMoRoleGrant, grantedIds, granteeRows)

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

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, g)
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

		g := &tree.Grant{
			Typ: tree.GrantTypeRole,
		}
		gr := &g.GrantRole
		for _, name := range roleNames {
			gr.Roles = append(gr.Roles, &tree.Role{UserName: name})
		}
		priv := determinePrivilegeSetOfStatement(g)
		ses := newSes(priv, ctrl)

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

		grantedIds := []int{0, 1, 5, 6, 7}
		granteeRows := make([][][]interface{}, len(grantedIds))
		granteeRows[0] = [][]interface{}{}
		granteeRows[1] = [][]interface{}{}
		granteeRows[2] = [][]interface{}{
			{0},
		}
		granteeRows[3] = [][]interface{}{
			{0},
		}
		granteeRows[4] = [][]interface{}{}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant,
			roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs,
			roleIdsInMoRoleGrant, rowsOfMoRoleGrant,
			grantedIds, granteeRows)

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

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, g)
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
		ses := newSes(priv, ctrl)

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

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, nil, nil, nil, nil)

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("revoke role succ 2", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.RevokeRole{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

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

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, roleIdsInMoRoleGrant, rowsOfMoRoleGrant, nil, nil)

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("revoke role fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.RevokeRole{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

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

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, roleIdsInMoRoleGrant, rowsOfMoRoleGrant, nil, nil)

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})
}

func Test_determineGrantPrivilege(t *testing.T) {
	convey.Convey("convert ast privilege", t, func() {
		type arg struct {
			pt   tree.PrivilegeType
			want PrivilegeType
		}

		args := []arg{
			{tree.PRIVILEGE_TYPE_STATIC_SELECT, PrivilegeTypeSelect},
		}

		for _, a := range args {
			w, err := convertAstPrivilegeTypeToPrivilegeType(context.TODO(), a.pt, tree.OBJECT_TYPE_TABLE)
			convey.So(err, convey.ShouldBeNil)
			convey.So(w, convey.ShouldEqual, a.want)
		}
	})

	convey.Convey("grant privilege [ObjectType: Table] AdminRole succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmts := []*tree.GrantPrivilege{
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
					{Type: tree.PRIVILEGE_TYPE_STATIC_INSERT},
				},
				ObjType: tree.OBJECT_TYPE_TABLE,
				Level: &tree.PrivilegeLevel{
					Level: tree.PRIVILEGE_LEVEL_TYPE_STAR,
				},
			},
		}

		for _, stmt := range stmts {
			priv := determinePrivilegeSetOfStatement(stmt)
			ses := newSes(priv, ctrl)
			ses.SetDatabaseName("db")

			ok, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
			convey.So(err, convey.ShouldBeNil)
			convey.So(ok, convey.ShouldBeTrue)
		}
	})

	convey.Convey("grant privilege [ObjectType: Table] succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmts := []*tree.GrantPrivilege{
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
					{Type: tree.PRIVILEGE_TYPE_STATIC_INSERT},
				},
				ObjType: tree.OBJECT_TYPE_TABLE,
				Level: &tree.PrivilegeLevel{
					Level: tree.PRIVILEGE_LEVEL_TYPE_STAR,
				},
			},
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
					{Type: tree.PRIVILEGE_TYPE_STATIC_INSERT},
				},
				ObjType: tree.OBJECT_TYPE_TABLE,
				Level: &tree.PrivilegeLevel{
					Level: tree.PRIVILEGE_LEVEL_TYPE_STAR_STAR,
				},
			},
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
					{Type: tree.PRIVILEGE_TYPE_STATIC_INSERT},
				},
				ObjType: tree.OBJECT_TYPE_TABLE,
				Level: &tree.PrivilegeLevel{
					Level: tree.PRIVILEGE_LEVEL_TYPE_DATABASE_STAR,
				},
			},
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
					{Type: tree.PRIVILEGE_TYPE_STATIC_INSERT},
				},
				ObjType: tree.OBJECT_TYPE_TABLE,
				Level: &tree.PrivilegeLevel{
					Level: tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE,
				},
			},
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
					{Type: tree.PRIVILEGE_TYPE_STATIC_INSERT},
				},
				ObjType: tree.OBJECT_TYPE_TABLE,
				Level: &tree.PrivilegeLevel{
					Level: tree.PRIVILEGE_LEVEL_TYPE_TABLE,
				},
			},
		}

		for _, stmt := range stmts {
			priv := determinePrivilegeSetOfStatement(stmt)
			ses := newSes(priv, ctrl)
			ses.tenant = &TenantInfo{
				Tenant:        "xxx",
				User:          "xxx",
				DefaultRole:   "xxx",
				TenantID:      1001,
				UserID:        1001,
				DefaultRoleID: 1001,
			}
			ses.SetDatabaseName("db")
			//TODO: make sql2result
			bh.init()
			for _, p := range stmt.Privileges {
				sql, err := formSqlFromGrantPrivilege(context.TODO(), ses, stmt, p)
				convey.So(err, convey.ShouldBeNil)
				makeRowsOfWithGrantOptionPrivilege(bh.sql2result, sql, [][]interface{}{
					{1, true},
				})

				var privType PrivilegeType
				privType, err = convertAstPrivilegeTypeToPrivilegeType(context.TODO(), p.Type, stmt.ObjType)
				convey.So(err, convey.ShouldBeNil)
				sql = getSqlForCheckRoleHasPrivilegeWGODependsOnPrivType(privType)

				rows := [][]interface{}{
					{ses.GetTenantInfo().GetDefaultRoleID()},
				}

				bh.sql2result[sql] = newMrsForPrivilegeWGO(rows)
			}

			ok, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
			convey.So(err, convey.ShouldBeNil)
			convey.So(ok, convey.ShouldBeTrue)
		}
	})

	convey.Convey("grant privilege [ObjectType: Table] fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmts := []*tree.GrantPrivilege{
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
					{Type: tree.PRIVILEGE_TYPE_STATIC_INSERT},
				},
				ObjType: tree.OBJECT_TYPE_TABLE,
				Level: &tree.PrivilegeLevel{
					Level: tree.PRIVILEGE_LEVEL_TYPE_STAR,
				},
			},
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
					{Type: tree.PRIVILEGE_TYPE_STATIC_INSERT},
				},
				ObjType: tree.OBJECT_TYPE_TABLE,
				Level: &tree.PrivilegeLevel{
					Level: tree.PRIVILEGE_LEVEL_TYPE_STAR_STAR,
				},
			},
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
					{Type: tree.PRIVILEGE_TYPE_STATIC_INSERT},
				},
				ObjType: tree.OBJECT_TYPE_TABLE,
				Level: &tree.PrivilegeLevel{
					Level: tree.PRIVILEGE_LEVEL_TYPE_DATABASE_STAR,
				},
			},
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
					{Type: tree.PRIVILEGE_TYPE_STATIC_INSERT},
				},
				ObjType: tree.OBJECT_TYPE_TABLE,
				Level: &tree.PrivilegeLevel{
					Level: tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE,
				},
			},
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
					{Type: tree.PRIVILEGE_TYPE_STATIC_INSERT},
				},
				ObjType: tree.OBJECT_TYPE_TABLE,
				Level: &tree.PrivilegeLevel{
					Level: tree.PRIVILEGE_LEVEL_TYPE_TABLE,
				},
			},
		}

		for _, stmt := range stmts {
			priv := determinePrivilegeSetOfStatement(stmt)
			ses := newSes(priv, ctrl)
			ses.tenant = &TenantInfo{
				Tenant:        "xxx",
				User:          "xxx",
				DefaultRole:   "xxx",
				TenantID:      1001,
				UserID:        1001,
				DefaultRoleID: 1001,
			}
			ses.SetDatabaseName("db")
			//TODO: make sql2result
			bh.init()
			var privType PrivilegeType
			for i, p := range stmt.Privileges {
				sql, err := formSqlFromGrantPrivilege(context.TODO(), ses, stmt, p)
				convey.So(err, convey.ShouldBeNil)
				var rows [][]interface{}
				if i == 0 {
					rows = [][]interface{}{}
				} else {
					rows = [][]interface{}{
						{1, true},
					}
				}
				makeRowsOfWithGrantOptionPrivilege(bh.sql2result, sql, rows)

				privType, err = convertAstPrivilegeTypeToPrivilegeType(context.TODO(), p.Type, stmt.ObjType)
				convey.So(err, convey.ShouldBeNil)
				sql = getSqlForCheckRoleHasPrivilegeWGODependsOnPrivType(privType)
				if i == 0 {
					rows = [][]interface{}{}
				} else {
					rows = [][]interface{}{
						{ses.GetTenantInfo().GetDefaultRoleID()},
					}
				}

				bh.sql2result[sql] = newMrsForPrivilegeWGO(rows)
			}

			ok, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
			convey.So(err, convey.ShouldBeNil)
			convey.So(ok, convey.ShouldBeFalse)
		}
	})

	convey.Convey("grant privilege [ObjectType: Database] succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmts := []*tree.GrantPrivilege{
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
					{Type: tree.PRIVILEGE_TYPE_STATIC_INSERT},
				},
				ObjType: tree.OBJECT_TYPE_DATABASE,
				Level: &tree.PrivilegeLevel{
					Level: tree.PRIVILEGE_LEVEL_TYPE_STAR,
				},
			},
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
					{Type: tree.PRIVILEGE_TYPE_STATIC_INSERT},
				},
				ObjType: tree.OBJECT_TYPE_DATABASE,
				Level: &tree.PrivilegeLevel{
					Level: tree.PRIVILEGE_LEVEL_TYPE_STAR_STAR,
				},
			},
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
					{Type: tree.PRIVILEGE_TYPE_STATIC_INSERT},
				},
				ObjType: tree.OBJECT_TYPE_DATABASE,
				Level: &tree.PrivilegeLevel{
					Level: tree.PRIVILEGE_LEVEL_TYPE_DATABASE,
				},
			},
		}

		var privType PrivilegeType
		for _, stmt := range stmts {
			priv := determinePrivilegeSetOfStatement(stmt)
			ses := newSes(priv, ctrl)
			ses.tenant = &TenantInfo{
				Tenant:        "xxx",
				User:          "xxx",
				DefaultRole:   "xxx",
				TenantID:      1001,
				UserID:        1001,
				DefaultRoleID: 1001,
			}
			ses.SetDatabaseName("db")
			//TODO: make sql2result
			bh.init()
			for _, p := range stmt.Privileges {
				sql, err := formSqlFromGrantPrivilege(context.TODO(), ses, stmt, p)
				convey.So(err, convey.ShouldBeNil)
				makeRowsOfWithGrantOptionPrivilege(bh.sql2result, sql, [][]interface{}{
					{1, true},
				})

				privType, err = convertAstPrivilegeTypeToPrivilegeType(context.TODO(), p.Type, stmt.ObjType)
				convey.So(err, convey.ShouldBeNil)
				sql = getSqlForCheckRoleHasPrivilegeWGODependsOnPrivType(privType)

				rows := [][]interface{}{
					{ses.GetTenantInfo().GetDefaultRoleID()},
				}

				bh.sql2result[sql] = newMrsForPrivilegeWGO(rows)
			}

			ok, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
			convey.So(err, convey.ShouldBeNil)
			convey.So(ok, convey.ShouldBeTrue)
		}
	})

	convey.Convey("grant privilege [ObjectType: Database] fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmts := []*tree.GrantPrivilege{
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
					{Type: tree.PRIVILEGE_TYPE_STATIC_INSERT},
				},
				ObjType: tree.OBJECT_TYPE_DATABASE,
				Level: &tree.PrivilegeLevel{
					Level: tree.PRIVILEGE_LEVEL_TYPE_STAR,
				},
			},
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
					{Type: tree.PRIVILEGE_TYPE_STATIC_INSERT},
				},
				ObjType: tree.OBJECT_TYPE_DATABASE,
				Level: &tree.PrivilegeLevel{
					Level: tree.PRIVILEGE_LEVEL_TYPE_STAR_STAR,
				},
			},
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
					{Type: tree.PRIVILEGE_TYPE_STATIC_INSERT},
				},
				ObjType: tree.OBJECT_TYPE_DATABASE,
				Level: &tree.PrivilegeLevel{
					Level: tree.PRIVILEGE_LEVEL_TYPE_DATABASE,
				},
			},
		}

		for _, stmt := range stmts {
			priv := determinePrivilegeSetOfStatement(stmt)
			ses := newSes(priv, ctrl)
			ses.tenant = &TenantInfo{
				Tenant:        "xxx",
				User:          "xxx",
				DefaultRole:   "xxx",
				TenantID:      1001,
				UserID:        1001,
				DefaultRoleID: 1001,
			}
			ses.SetDatabaseName("db")
			//TODO: make sql2result
			bh.init()
			for i, p := range stmt.Privileges {
				sql, err := formSqlFromGrantPrivilege(context.TODO(), ses, stmt, p)
				convey.So(err, convey.ShouldBeNil)
				var rows [][]interface{}
				if i == 0 {
					rows = [][]interface{}{}
				} else {
					rows = [][]interface{}{
						{1, true},
					}
				}
				makeRowsOfWithGrantOptionPrivilege(bh.sql2result, sql, rows)

				var privType PrivilegeType
				privType, err = convertAstPrivilegeTypeToPrivilegeType(context.TODO(), p.Type, stmt.ObjType)
				convey.So(err, convey.ShouldBeNil)
				sql = getSqlForCheckRoleHasPrivilegeWGODependsOnPrivType(privType)

				if i == 0 {
					rows = [][]interface{}{}
				} else {
					rows = [][]interface{}{
						{ses.GetTenantInfo().GetDefaultRoleID()},
					}
				}

				bh.sql2result[sql] = newMrsForPrivilegeWGO(rows)
			}

			ok, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
			convey.So(err, convey.ShouldBeNil)
			convey.So(ok, convey.ShouldBeFalse)
		}
	})

	convey.Convey("grant privilege [ObjectType: Account] succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmts := []*tree.GrantPrivilege{
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
					{Type: tree.PRIVILEGE_TYPE_STATIC_INSERT},
				},
				ObjType: tree.OBJECT_TYPE_ACCOUNT,
				Level: &tree.PrivilegeLevel{
					Level: tree.PRIVILEGE_LEVEL_TYPE_STAR,
				},
			},
		}

		for _, stmt := range stmts {
			priv := determinePrivilegeSetOfStatement(stmt)
			ses := newSes(priv, ctrl)
			ses.tenant = &TenantInfo{
				Tenant:        "xxx",
				User:          "xxx",
				DefaultRole:   "xxx",
				TenantID:      1001,
				UserID:        1001,
				DefaultRoleID: 1001,
			}
			ses.SetDatabaseName("db")
			//TODO: make sql2result
			bh.init()
			for _, p := range stmt.Privileges {
				sql, err := formSqlFromGrantPrivilege(context.TODO(), ses, stmt, p)
				convey.So(err, convey.ShouldBeNil)
				makeRowsOfWithGrantOptionPrivilege(bh.sql2result, sql, [][]interface{}{
					{1, true},
				})

				var privType PrivilegeType
				privType, err = convertAstPrivilegeTypeToPrivilegeType(context.TODO(), p.Type, stmt.ObjType)
				convey.So(err, convey.ShouldBeNil)
				sql = getSqlForCheckRoleHasPrivilegeWGODependsOnPrivType(privType)
				rows := [][]interface{}{
					{ses.GetTenantInfo().GetDefaultRoleID()},
				}

				bh.sql2result[sql] = newMrsForPrivilegeWGO(rows)
			}

			ok, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
			convey.So(err, convey.ShouldBeNil)
			convey.So(ok, convey.ShouldBeTrue)
		}
	})

	convey.Convey("grant privilege [ObjectType: Account] fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmts := []*tree.GrantPrivilege{
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
					{Type: tree.PRIVILEGE_TYPE_STATIC_INSERT},
				},
				ObjType: tree.OBJECT_TYPE_ACCOUNT,
				Level: &tree.PrivilegeLevel{
					Level: tree.PRIVILEGE_LEVEL_TYPE_STAR,
				},
			},
		}

		for _, stmt := range stmts {
			priv := determinePrivilegeSetOfStatement(stmt)
			ses := newSes(priv, ctrl)
			ses.tenant = &TenantInfo{
				Tenant:        "xxx",
				User:          "xxx",
				DefaultRole:   "xxx",
				TenantID:      1001,
				UserID:        1001,
				DefaultRoleID: 1001,
			}
			ses.SetDatabaseName("db")
			//TODO: make sql2result
			bh.init()
			for i, p := range stmt.Privileges {
				sql, err := formSqlFromGrantPrivilege(context.TODO(), ses, stmt, p)
				convey.So(err, convey.ShouldBeNil)
				var rows [][]interface{}
				if i == 0 {
					rows = [][]interface{}{}
				} else {
					rows = [][]interface{}{
						{1, true},
					}
				}
				makeRowsOfWithGrantOptionPrivilege(bh.sql2result, sql, rows)

				var privType PrivilegeType
				privType, err = convertAstPrivilegeTypeToPrivilegeType(context.TODO(), p.Type, stmt.ObjType)
				convey.So(err, convey.ShouldBeNil)
				sql = getSqlForCheckRoleHasPrivilegeWGODependsOnPrivType(privType)

				if i == 0 {
					rows = [][]interface{}{}
				} else {
					rows = [][]interface{}{
						{ses.GetTenantInfo().GetDefaultRoleID()},
					}
				}

				bh.sql2result[sql] = newMrsForPrivilegeWGO(rows)
			}

			ok, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
			convey.So(err, convey.ShouldBeNil)
			convey.So(ok, convey.ShouldBeFalse)
		}
	})
}

func Test_determineRevokePrivilege(t *testing.T) {
	convey.Convey("revoke privilege [ObjectType: Table] AdminRole succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		var stmts []*tree.RevokePrivilege

		for _, stmt := range stmts {
			priv := determinePrivilegeSetOfStatement(stmt)
			ses := newSes(priv, ctrl)

			ok, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
			convey.So(err, convey.ShouldBeNil)
			convey.So(ok, convey.ShouldBeTrue)
		}
	})
	convey.Convey("revoke privilege [ObjectType: Table] not AdminRole fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		var stmts []*tree.RevokePrivilege

		for _, stmt := range stmts {
			priv := determinePrivilegeSetOfStatement(stmt)
			ses := newSes(priv, ctrl)
			ses.tenant = &TenantInfo{
				Tenant:        "xxx",
				User:          "xxx",
				DefaultRole:   "xxx",
				TenantID:      1001,
				UserID:        1001,
				DefaultRoleID: 1001,
			}

			ok, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
			convey.So(err, convey.ShouldBeNil)
			convey.So(ok, convey.ShouldBeFalse)
		}
	})
}

func TestBackUpStatementPrivilege(t *testing.T) {
	convey.Convey("backup success", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		stmt := &tree.BackupStart{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		tenant := &TenantInfo{
			Tenant:        sysAccountName,
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      sysAccountID,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}
		ses.SetTenantInfo(tenant)

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("backup fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		stmt := &tree.BackupStart{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		tenant := &TenantInfo{
			Tenant:        "test_account",
			User:          "test_user",
			DefaultRole:   "role1",
			TenantID:      3001,
			UserID:        3,
			DefaultRoleID: 5,
		}
		ses.SetTenantInfo(tenant)

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})
}

func Test_determineCreateDatabase(t *testing.T) {
	convey.Convey("create database succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.CreateDatabase{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//without privilege create database, all
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{
			{0, true},
		}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, nil, nil, nil, nil)

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})
	convey.Convey("create database succ 2", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.CreateDatabase{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0, 1}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//role 0 without privilege create database, all, ownership
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		//role 1 with privilege create database
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

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, roleIdsInMoRoleGrant, rowsOfMoRoleGrant, nil, nil)

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})
	convey.Convey("create database fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.CreateDatabase{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0, 1, 2}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//role 0 without privilege create database, all, ownership
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		//role 1 without privilege create database, all, ownership
		rowsOfMoRolePrivs[1][0] = [][]interface{}{}
		rowsOfMoRolePrivs[1][1] = [][]interface{}{}

		//role 2 without privilege create database, all, ownership
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

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, roleIdsInMoRoleGrant, rowsOfMoRoleGrant, nil, nil)

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})
}

func Test_determineDropDatabase(t *testing.T) {
	convey.Convey("drop/alter database succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.DropDatabase{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//with privilege drop database
		rowsOfMoRolePrivs[0][0] = [][]interface{}{
			{0, true},
		}
		//without privilege all
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, nil, nil, nil, nil)

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})
	convey.Convey("drop/alter database succ 2", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.DropDatabase{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0, 1}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//role 0 without privilege drop database, all, account/user ownership
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		//role 1 with privilege drop database
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

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, roleIdsInMoRoleGrant, rowsOfMoRoleGrant, nil, nil)

		bh := newBh(ctrl, sql2result)
		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})
	convey.Convey("drop/alter database fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.DropDatabase{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0, 1, 2}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//role 0 without privilege drop database, all, ownership
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		//role 1 without privilege drop database, all, ownership
		rowsOfMoRolePrivs[1][0] = [][]interface{}{}
		rowsOfMoRolePrivs[1][1] = [][]interface{}{}

		//role 2 without privilege drop database, all, ownership
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

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, roleIdsInMoRoleGrant, rowsOfMoRoleGrant, nil, nil)
		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})
}

func Test_determineShowDatabase(t *testing.T) {
	convey.Convey("show database succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.ShowDatabases{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//with privilege show databases
		rowsOfMoRolePrivs[0][0] = [][]interface{}{
			{0, true},
		}
		//without privilege all
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, nil, nil, nil, nil)

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})
	convey.Convey("show database succ 2", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.ShowDatabases{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0, 1}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//role 0 without privilege show databases, all, account/user ownership
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		//role 1 with privilege show databases
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

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, roleIdsInMoRoleGrant, rowsOfMoRoleGrant, nil, nil)

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})
	convey.Convey("show database fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.ShowDatabases{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0, 1, 2}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//role 0 without privilege show databases, all, ownership
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		//role 1 without privilege show databases, all, ownership
		rowsOfMoRolePrivs[1][0] = [][]interface{}{}
		rowsOfMoRolePrivs[1][1] = [][]interface{}{}

		//role 2 without privilege show databases, all, ownership
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

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, roleIdsInMoRoleGrant, rowsOfMoRoleGrant, nil, nil)

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})
}

func Test_determineUseDatabase(t *testing.T) {
	convey.Convey("use database succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.Use{
			Name: tree.NewCStr("db", 1),
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//with privilege show databases
		rowsOfMoRolePrivs[0][0] = [][]interface{}{
			{0, true},
		}
		//without privilege all
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, nil, nil, nil, nil)

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})
	convey.Convey("use database succ 2", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.Use{
			Name: tree.NewCStr("db", 1),
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0, 1}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//role 0 without privilege show databases, all, account/user ownership
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		//role 1 with privilege show databases
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

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, roleIdsInMoRoleGrant, rowsOfMoRoleGrant, nil, nil)

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})
	convey.Convey("use database fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.Use{
			Name: tree.NewCStr("db", 1),
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0, 1, 2}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//role 0 without privilege show databases, all, ownership
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		//role 1 without privilege show databases, all, ownership
		rowsOfMoRolePrivs[1][0] = [][]interface{}{}
		rowsOfMoRolePrivs[1][1] = [][]interface{}{}

		//role 2 without privilege show databases, all, ownership
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

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, roleIdsInMoRoleGrant, rowsOfMoRoleGrant, nil, nil)

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})
}

func Test_determineUseRole(t *testing.T) {
	//TODO:add ut
}

func Test_determineCreateTable(t *testing.T) {
	convey.Convey("create table succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.CreateTable{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//without privilege create table, all
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{
			{0, true},
		}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, nil, nil, nil, nil)

		var rows [][]interface{}
		roles := []int{0}
		for _, entry := range priv.entries {
			pls, err := getPrivilegeLevelsOfObjectType(context.TODO(), entry.objType)
			convey.So(err, convey.ShouldBeNil)
			for _, pl := range pls {
				for _, roleId := range roles {
					sql, err := getSqlForPrivilege(context.TODO(), int64(roleId), entry, pl)
					convey.So(err, convey.ShouldBeNil)
					if entry.privilegeId == PrivilegeTypeCreateTable {
						rows = [][]interface{}{
							{0, true},
						}
					} else {
						rows = [][]interface{}{}
					}
					sql2result[sql] = newMrsForWithGrantOptionPrivilege(rows)
				}
			}
		}

		sql := getSqlForInheritedRoleIdOfRoleId(0)
		sql2result[sql] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{})

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("create table succ 2", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.CreateTable{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0, 1}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//role 0 without privilege create table, all, ownership
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		//role 1 with privilege create table
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

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, roleIdsInMoRoleGrant, rowsOfMoRoleGrant, nil, nil)

		var rows [][]interface{}
		roles := []int{0, 1}
		for _, entry := range priv.entries {
			pls, err := getPrivilegeLevelsOfObjectType(context.TODO(), entry.objType)
			convey.So(err, convey.ShouldBeNil)
			for _, pl := range pls {
				for _, roleId := range roles {
					sql, err := getSqlForPrivilege(context.TODO(), int64(roleId), entry, pl)
					convey.So(err, convey.ShouldBeNil)
					if roleId == 1 && entry.privilegeId == PrivilegeTypeCreateTable {
						rows = [][]interface{}{
							{1, true},
						}
					} else {
						rows = [][]interface{}{}
					}
					sql2result[sql] = newMrsForWithGrantOptionPrivilege(rows)
				}
			}
		}

		sql := getSqlForInheritedRoleIdOfRoleId(0)
		sql2result[sql] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{
			{1, true},
		})
		sql = getSqlForInheritedRoleIdOfRoleId(1)
		sql2result[sql] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{})

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("create table fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.CreateTable{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0, 1, 2}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//role 0 without privilege create table, all, ownership
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		//role 1 without privilege create table, all, ownership
		rowsOfMoRolePrivs[1][0] = [][]interface{}{}
		rowsOfMoRolePrivs[1][1] = [][]interface{}{}

		//role 2 without privilege create table, all, ownership
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

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, roleIdsInMoRoleGrant, rowsOfMoRoleGrant, nil, nil)

		var rows [][]interface{}
		roles := []int{0, 1, 2}
		for _, entry := range priv.entries {
			pls, err := getPrivilegeLevelsOfObjectType(context.TODO(), entry.objType)
			convey.So(err, convey.ShouldBeNil)
			for _, pl := range pls {
				for _, roleId := range roles {
					sql, err := getSqlForPrivilege(context.TODO(), int64(roleId), entry, pl)
					convey.So(err, convey.ShouldBeNil)
					rows = [][]interface{}{}
					sql2result[sql] = newMrsForWithGrantOptionPrivilege(rows)
				}
			}
		}

		sql := getSqlForInheritedRoleIdOfRoleId(0)
		sql2result[sql] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{
			{1, true},
		})
		sql = getSqlForInheritedRoleIdOfRoleId(1)
		sql2result[sql] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{
			{2, true},
		})
		sql = getSqlForInheritedRoleIdOfRoleId(2)
		sql2result[sql] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{})

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})
}

func Test_determineDropTable(t *testing.T) {
	convey.Convey("drop/alter table succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.DropTable{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//with privilege drop table
		rowsOfMoRolePrivs[0][0] = [][]interface{}{
			{0, true},
		}
		//without privilege all
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, nil, nil, nil, nil)

		var rows [][]interface{}
		roles := []int{0}
		for _, entry := range priv.entries {
			pls, err := getPrivilegeLevelsOfObjectType(context.TODO(), entry.objType)
			convey.So(err, convey.ShouldBeNil)
			for _, pl := range pls {
				for _, roleId := range roles {
					sql, err := getSqlForPrivilege(context.TODO(), int64(roleId), entry, pl)
					convey.So(err, convey.ShouldBeNil)
					if entry.privilegeId == PrivilegeTypeDropTable {
						rows = [][]interface{}{
							{0, true},
						}
					} else {
						rows = [][]interface{}{}
					}
					sql2result[sql] = newMrsForWithGrantOptionPrivilege(rows)
				}
			}
		}

		sql := getSqlForInheritedRoleIdOfRoleId(0)
		sql2result[sql] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{})

		bh := newBh(ctrl, sql2result)
		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("drop/alter table succ 2", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.DropTable{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0, 1}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//role 0 without privilege drop table, all, account/user ownership
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		//role 1 with privilege drop table
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

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, roleIdsInMoRoleGrant, rowsOfMoRoleGrant, nil, nil)

		var rows [][]interface{}
		roles := []int{0, 1}
		for _, entry := range priv.entries {
			pls, err := getPrivilegeLevelsOfObjectType(context.TODO(), entry.objType)
			convey.So(err, convey.ShouldBeNil)
			for _, pl := range pls {
				for _, roleId := range roles {
					sql, err := getSqlForPrivilege(context.TODO(), int64(roleId), entry, pl)
					convey.So(err, convey.ShouldBeNil)
					if roleId == 1 && entry.privilegeId == PrivilegeTypeDropTable {
						rows = [][]interface{}{
							{1, true},
						}
					} else {
						rows = [][]interface{}{}
					}
					sql2result[sql] = newMrsForWithGrantOptionPrivilege(rows)
				}
			}
		}

		sql := getSqlForInheritedRoleIdOfRoleId(0)
		sql2result[sql] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{
			{1, true},
		})
		sql = getSqlForInheritedRoleIdOfRoleId(1)
		sql2result[sql] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{})

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("drop/alter table fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.DropTable{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		rowsOfMoUserGrant := [][]interface{}{
			{0, false},
		}
		roleIdsInMoRolePrivs := []int{0, 1, 2}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}

		//role 0 without privilege drop table, all, ownership
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}

		//role 1 without privilege drop table, all, ownership
		rowsOfMoRolePrivs[1][0] = [][]interface{}{}
		rowsOfMoRolePrivs[1][1] = [][]interface{}{}

		//role 2 without privilege drop table, all, ownership
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

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, roleIdsInMoRoleGrant, rowsOfMoRoleGrant, nil, nil)

		var rows [][]interface{}
		roles := []int{0, 1, 2}
		for _, entry := range priv.entries {
			pls, err := getPrivilegeLevelsOfObjectType(context.TODO(), entry.objType)
			convey.So(err, convey.ShouldBeNil)
			for _, pl := range pls {
				for _, roleId := range roles {
					sql, err := getSqlForPrivilege(context.TODO(), int64(roleId), entry, pl)
					convey.So(err, convey.ShouldBeNil)
					rows = [][]interface{}{}
					sql2result[sql] = newMrsForWithGrantOptionPrivilege(rows)
				}
			}
		}

		sql := getSqlForInheritedRoleIdOfRoleId(0)
		sql2result[sql] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{
			{1, true},
		})
		sql = getSqlForInheritedRoleIdOfRoleId(1)
		sql2result[sql] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{
			{2, true},
		})
		sql = getSqlForInheritedRoleIdOfRoleId(2)
		sql2result[sql] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{})

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})
}

func Test_determineDML(t *testing.T) {
	type arg struct {
		stmt tree.Statement
		p    *plan2.Plan
	}

	args := []arg{
		{
			stmt: &tree.Select{},
			p: &plan2.Plan{
				Plan: &plan2.Plan_Query{
					Query: &plan2.Query{
						Nodes: []*plan2.Node{
							{NodeType: plan.Node_TABLE_SCAN, ObjRef: &plan2.ObjectRef{SchemaName: "t", ObjName: "a"}},
							{NodeType: plan.Node_TABLE_SCAN, ObjRef: &plan2.ObjectRef{SchemaName: "s", ObjName: "b"}},
						},
					},
				},
			},
		},
		{
			stmt: &tree.Update{},
			p: &plan2.Plan{
				Plan: &plan2.Plan_Query{
					Query: &plan2.Query{
						Nodes: []*plan2.Node{
							{NodeType: plan.Node_TABLE_SCAN, ObjRef: &plan2.ObjectRef{SchemaName: "t", ObjName: "a"}},
							{NodeType: plan.Node_TABLE_SCAN, ObjRef: &plan2.ObjectRef{SchemaName: "s", ObjName: "b"}},
							{NodeType: plan.Node_INSERT},
						},
					},
				},
			},
		},
		{
			stmt: &tree.Delete{},
			p: &plan2.Plan{
				Plan: &plan2.Plan_Query{
					Query: &plan2.Query{
						Nodes: []*plan2.Node{
							{NodeType: plan.Node_TABLE_SCAN, ObjRef: &plan2.ObjectRef{SchemaName: "t", ObjName: "a"}},
							{NodeType: plan.Node_TABLE_SCAN, ObjRef: &plan2.ObjectRef{SchemaName: "s", ObjName: "b"}},
							{NodeType: plan.Node_DELETE},
						},
					},
				},
			},
		},
		{ //insert into select
			stmt: &tree.Insert{},
			p: &plan2.Plan{
				Plan: &plan2.Plan_Query{
					Query: &plan2.Query{
						Nodes: []*plan2.Node{
							{NodeType: plan.Node_TABLE_SCAN, ObjRef: &plan2.ObjectRef{SchemaName: "t", ObjName: "a"}},
							{NodeType: plan.Node_TABLE_SCAN, ObjRef: &plan2.ObjectRef{SchemaName: "s", ObjName: "b"}},
							{NodeType: plan.Node_INSERT, ObjRef: &plan2.ObjectRef{SchemaName: "s", ObjName: "b"}},
						},
					},
				},
			},
		},
	}

	convey.Convey("select/update/delete/insert succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		for _, a := range args {
			priv := determinePrivilegeSetOfStatement(a.stmt)
			ses := newSes(priv, ctrl)

			rowsOfMoUserGrant := [][]interface{}{
				{0, false},
			}

			sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, nil, nil, nil, nil, nil, nil, nil)

			arr := extractPrivilegeTipsFromPlan(a.p)
			convertPrivilegeTipsToPrivilege(priv, arr)

			roleIds := []int{
				int(ses.GetTenantInfo().GetDefaultRoleID()),
			}

			for _, roleId := range roleIds {
				for _, entry := range priv.entries {
					sql, _ := getSqlForCheckRoleHasTableLevelPrivilege(context.TODO(), int64(roleId), entry.privilegeId, entry.databaseName, entry.tableName)
					sql2result[sql] = newMrsForWithGrantOptionPrivilege([][]interface{}{
						{entry.privilegeId, true},
					})
				}
			}

			var rows [][]interface{}
			makeSql := func(entry privilegeEntry) {
				pls, err := getPrivilegeLevelsOfObjectType(context.TODO(), entry.objType)
				convey.So(err, convey.ShouldBeNil)
				for i, pl := range pls {
					for _, roleId := range roleIds {
						sql, err := getSqlForPrivilege(context.TODO(), int64(roleId), entry, pl)
						convey.So(err, convey.ShouldBeNil)
						if i == 0 {
							rows = [][]interface{}{
								{0, true},
							}
						} else {
							rows = [][]interface{}{}
						}
						sql2result[sql] = newMrsForWithGrantOptionPrivilege(rows)
					}
				}
			}
			for _, entry := range priv.entries {
				if entry.privilegeEntryTyp == privilegeEntryTypeGeneral {
					makeSql(entry)
				} else if entry.privilegeEntryTyp == privilegeEntryTypeCompound {
					for _, mi := range entry.compound.items {
						tempEntry := privilegeEntriesMap[mi.privilegeTyp]
						tempEntry.databaseName = mi.dbName
						tempEntry.tableName = mi.tableName
						tempEntry.privilegeEntryTyp = privilegeEntryTypeGeneral
						tempEntry.compound = nil
						makeSql(tempEntry)
					}
				}
			}

			sql := getSqlForInheritedRoleIdOfRoleId(0)
			sql2result[sql] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{})

			bh := newBh(ctrl, sql2result)
			bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
			defer bhStub.Reset()

			ok, err := authenticateUserCanExecuteStatementWithObjectTypeDatabaseAndTable(ses.GetTxnHandler().GetTxnCtx(), ses, a.stmt, a.p)
			convey.So(err, convey.ShouldBeNil)
			convey.So(ok, convey.ShouldBeTrue)
		}

	})

	convey.Convey("select/update/delete/insert succ 2", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		for _, a := range args {
			priv := determinePrivilegeSetOfStatement(a.stmt)
			ses := newSes(priv, ctrl)

			rowsOfMoUserGrant := [][]interface{}{
				{0, false},
			}

			//grant role 1 to role 0
			roleIdsInMoRoleGrant := []int{0}
			rowsOfMoRoleGrant := make([][][]interface{}, len(roleIdsInMoRoleGrant))
			rowsOfMoRoleGrant[0] = [][]interface{}{
				{1, true},
			}

			sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, nil, nil, nil, roleIdsInMoRoleGrant, rowsOfMoRoleGrant, nil, nil)

			arr := extractPrivilegeTipsFromPlan(a.p)
			convertPrivilegeTipsToPrivilege(priv, arr)

			//role 0 does not have the select
			//role 1 has the select
			roleIds := []int{
				int(ses.GetTenantInfo().GetDefaultRoleID()), 1,
			}

			for _, roleId := range roleIds {
				for _, entry := range priv.entries {
					sql, _ := getSqlFromPrivilegeEntry(context.TODO(), int64(roleId), entry)
					var rows [][]interface{}
					if roleId == 1 {
						rows = [][]interface{}{
							{entry.privilegeId, true},
						}
					}
					sql2result[sql] = newMrsForWithGrantOptionPrivilege(rows)
				}
			}

			var rows [][]interface{}
			roles := []int{0, 1}
			makeSql := func(entry privilegeEntry) {
				pls, err := getPrivilegeLevelsOfObjectType(context.TODO(), entry.objType)
				convey.So(err, convey.ShouldBeNil)
				for _, pl := range pls {
					for _, roleId := range roles {
						sql, err := getSqlForPrivilege(context.TODO(), int64(roleId), entry, pl)
						convey.So(err, convey.ShouldBeNil)
						if roleId == 1 {
							rows = [][]interface{}{
								{1, true},
							}
						} else {
							rows = [][]interface{}{}
						}
						sql2result[sql] = newMrsForWithGrantOptionPrivilege(rows)
					}
				}
			}

			for _, entry := range priv.entries {
				if entry.privilegeEntryTyp == privilegeEntryTypeGeneral {
					makeSql(entry)
				} else if entry.privilegeEntryTyp == privilegeEntryTypeCompound {
					for _, mi := range entry.compound.items {
						tempEntry := privilegeEntriesMap[mi.privilegeTyp]
						tempEntry.databaseName = mi.dbName
						tempEntry.tableName = mi.tableName
						tempEntry.privilegeEntryTyp = privilegeEntryTypeGeneral
						tempEntry.compound = nil
						makeSql(tempEntry)
					}
				}
			}

			sql := getSqlForInheritedRoleIdOfRoleId(0)
			sql2result[sql] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{
				{1, true},
			})
			sql = getSqlForInheritedRoleIdOfRoleId(1)
			sql2result[sql] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{})

			bh := newBh(ctrl, sql2result)

			bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
			defer bhStub.Reset()

			ok, err := authenticateUserCanExecuteStatementWithObjectTypeDatabaseAndTable(ses.GetTxnHandler().GetTxnCtx(), ses, a.stmt, a.p)
			convey.So(err, convey.ShouldBeNil)
			convey.So(ok, convey.ShouldBeTrue)
		}
	})

	convey.Convey("select/update/delete/insert fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		for _, a := range args {
			priv := determinePrivilegeSetOfStatement(a.stmt)
			ses := newSes(priv, ctrl)

			rowsOfMoUserGrant := [][]interface{}{
				{0, false},
			}

			//grant role 1 to role 0
			roleIdsInMoRoleGrant := []int{0, 1}
			rowsOfMoRoleGrant := make([][][]interface{}, len(roleIdsInMoRoleGrant))
			rowsOfMoRoleGrant[0] = [][]interface{}{
				{1, true},
			}
			rowsOfMoRoleGrant[0] = [][]interface{}{}

			sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, nil, nil, nil, roleIdsInMoRoleGrant, rowsOfMoRoleGrant, nil, nil)

			arr := extractPrivilegeTipsFromPlan(a.p)
			convertPrivilegeTipsToPrivilege(priv, arr)

			//role 0,1 does not have the select
			roleIds := []int{
				int(ses.GetTenantInfo().GetDefaultRoleID()), 1,
			}

			for _, roleId := range roleIds {
				for _, entry := range priv.entries {
					sql, _ := getSqlFromPrivilegeEntry(context.TODO(), int64(roleId), entry)
					rows := make([][]interface{}, 0)
					sql2result[sql] = newMrsForWithGrantOptionPrivilege(rows)
				}
			}

			var rows [][]interface{}
			roles := []int{0, 1, 2}
			makeSql := func(entry privilegeEntry) {
				pls, err := getPrivilegeLevelsOfObjectType(context.TODO(), entry.objType)
				convey.So(err, convey.ShouldBeNil)
				for _, pl := range pls {
					for _, roleId := range roles {
						sql, err := getSqlForPrivilege(context.TODO(), int64(roleId), entry, pl)
						convey.So(err, convey.ShouldBeNil)
						rows = [][]interface{}{}
						sql2result[sql] = newMrsForWithGrantOptionPrivilege(rows)
					}
				}
			}

			for _, entry := range priv.entries {
				if entry.privilegeEntryTyp == privilegeEntryTypeGeneral {
					makeSql(entry)
				} else if entry.privilegeEntryTyp == privilegeEntryTypeCompound {
					for _, mi := range entry.compound.items {
						tempEntry := privilegeEntriesMap[mi.privilegeTyp]
						tempEntry.databaseName = mi.dbName
						tempEntry.tableName = mi.tableName
						tempEntry.privilegeEntryTyp = privilegeEntryTypeGeneral
						tempEntry.compound = nil
						makeSql(tempEntry)
					}
				}
			}

			sql := getSqlForInheritedRoleIdOfRoleId(0)
			sql2result[sql] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{
				{1, true},
			})
			sql = getSqlForInheritedRoleIdOfRoleId(1)
			sql2result[sql] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{
				{2, true},
			})
			sql = getSqlForInheritedRoleIdOfRoleId(2)
			sql2result[sql] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{})

			bh := newBh(ctrl, sql2result)

			bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
			defer bhStub.Reset()

			ok, err := authenticateUserCanExecuteStatementWithObjectTypeDatabaseAndTable(ses.GetTxnHandler().GetTxnCtx(), ses, a.stmt, a.p)
			convey.So(err, convey.ShouldBeNil)
			convey.So(ok, convey.ShouldBeFalse)
		}
	})
}

func Test_doGrantRole(t *testing.T) {
	convey.Convey("grant role to role succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantRole{
			Roles: []*tree.Role{
				{UserName: "r1"},
				{UserName: "r2"},
				{UserName: "r3"},
			},
			Users: []*tree.User{
				{Username: "r4"},
				{Username: "r5"},
				{Username: "r6"},
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		//init from roles
		for i, role := range stmt.Roles {
			sql, _ := getSqlForRoleIdOfRole(context.TODO(), role.UserName)
			mrs := newMrsForRoleIdOfRole([][]interface{}{
				{i},
			})
			bh.sql2result[sql] = mrs
		}

		//init to roles
		for i, user := range stmt.Users {
			sql, _ := getSqlForRoleIdOfRole(context.TODO(), user.Username)
			mrs := newMrsForRoleIdOfRole([][]interface{}{
				{i + len(stmt.Roles)},
			})

			bh.sql2result[sql] = mrs
		}

		//has "ro roles", need init mo_role_grant (assume empty)
		sql := getSqlForGetAllStuffRoleGrantFormat()
		mrs := newMrsForGetAllStuffRoleGrant([][]interface{}{})

		bh.sql2result[sql] = mrs

		//loop on from ... to
		for fromId := range stmt.Roles {
			for toId := range stmt.Users {
				toId = toId + len(stmt.Roles)
				sql = getSqlForCheckRoleGrant(int64(fromId), int64(toId))
				mrs = newMrsForCheckRoleGrant([][]interface{}{})
				bh.sql2result[sql] = mrs
			}
		}

		err := doGrantRole(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("grant role to user succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantRole{
			Roles: []*tree.Role{
				{UserName: "r1"},
				{UserName: "r2"},
				{UserName: "r3"},
			},
			Users: []*tree.User{
				{Username: "u4"},
				{Username: "u5"},
				{Username: "u6"},
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		//init from roles
		for i, role := range stmt.Roles {
			sql, _ := getSqlForRoleIdOfRole(context.TODO(), role.UserName)
			mrs := newMrsForRoleIdOfRole([][]interface{}{
				{i},
			})
			bh.sql2result[sql] = mrs
		}

		//init to empty roles,
		//init to users
		for i, user := range stmt.Users {
			sql, _ := getSqlForRoleIdOfRole(context.TODO(), user.Username)
			mrs := newMrsForRoleIdOfRole([][]interface{}{})

			bh.sql2result[sql] = mrs

			sql, _ = getSqlForPasswordOfUser(context.TODO(), user.Username)
			mrs = newMrsForPasswordOfUser([][]interface{}{
				{i, "111", i},
			})
			bh.sql2result[sql] = mrs

			sql, _ = getSqlForRoleOfUser(context.TODO(), int64(i), moAdminRoleName)
			bh.sql2result[sql] = newMrsForRoleOfUser([][]interface{}{})
		}

		//has "ro roles", need init mo_role_grant (assume empty)
		sql := getSqlForGetAllStuffRoleGrantFormat()
		mrs := newMrsForGetAllStuffRoleGrant([][]interface{}{})

		bh.sql2result[sql] = mrs

		//loop on from ... to
		for fromId := range stmt.Roles {
			for toId := range stmt.Users {
				sql = getSqlForCheckRoleGrant(int64(fromId), int64(toId))
				mrs = newMrsForCheckRoleGrant([][]interface{}{})
				bh.sql2result[sql] = mrs

				sql = getSqlForCheckUserGrant(int64(fromId), int64(toId))
				mrs = newMrsForCheckUserGrant([][]interface{}{})
				bh.sql2result[sql] = mrs
			}
		}

		err := doGrantRole(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("grant role to role+user succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantRole{
			Roles: []*tree.Role{
				{UserName: "r1"},
				{UserName: "r2"},
				{UserName: "r3"},
			},
			Users: []*tree.User{
				{Username: "u4"},
				{Username: "u5"},
				{Username: "u6"},
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		//init from roles
		for i, role := range stmt.Roles {
			sql, _ := getSqlForRoleIdOfRole(context.TODO(), role.UserName)
			mrs := newMrsForRoleIdOfRole([][]interface{}{
				{i},
			})
			bh.sql2result[sql] = mrs
		}

		//init to 2 roles,
		//init to 1 users
		for i, user := range stmt.Users {
			if i < 2 { //roles
				sql, _ := getSqlForRoleIdOfRole(context.TODO(), user.Username)
				mrs := newMrsForRoleIdOfRole([][]interface{}{
					{i + len(stmt.Roles)},
				})

				bh.sql2result[sql] = mrs

				sql, _ = getSqlForPasswordOfUser(context.TODO(), user.Username)
				mrs = newMrsForPasswordOfUser([][]interface{}{})
				bh.sql2result[sql] = mrs
			} else { //users
				sql, _ := getSqlForRoleIdOfRole(context.TODO(), user.Username)
				mrs := newMrsForRoleIdOfRole([][]interface{}{})

				bh.sql2result[sql] = mrs

				sql, _ = getSqlForPasswordOfUser(context.TODO(), user.Username)
				mrs = newMrsForPasswordOfUser([][]interface{}{
					{i, "111", i},
				})
				bh.sql2result[sql] = mrs

				sql, _ = getSqlForRoleOfUser(context.TODO(), int64(i), moAdminRoleName)
				bh.sql2result[sql] = newMrsForRoleOfUser([][]interface{}{})
			}

		}

		//has "ro roles", need init mo_role_grant (assume empty)
		sql := getSqlForGetAllStuffRoleGrantFormat()
		mrs := newMrsForGetAllStuffRoleGrant([][]interface{}{})

		bh.sql2result[sql] = mrs

		//loop on from ... to
		for fromId := range stmt.Roles {
			for toId := range stmt.Users {
				if toId < 2 { //roles
					toId = toId + len(stmt.Roles)
					sql = getSqlForCheckRoleGrant(int64(fromId), int64(toId))
					mrs = newMrsForCheckRoleGrant([][]interface{}{})
					bh.sql2result[sql] = mrs

					sql = getSqlForCheckUserGrant(int64(fromId), int64(toId))
					mrs = newMrsForCheckUserGrant([][]interface{}{})
					bh.sql2result[sql] = mrs
				} else { //users
					sql = getSqlForCheckRoleGrant(int64(fromId), int64(toId))
					mrs = newMrsForCheckRoleGrant([][]interface{}{})
					bh.sql2result[sql] = mrs

					sql = getSqlForCheckUserGrant(int64(fromId), int64(toId))
					mrs = newMrsForCheckUserGrant([][]interface{}{})
					bh.sql2result[sql] = mrs
				}

			}
		}

		err := doGrantRole(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("grant role to role+user 2 (insert) succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantRole{
			Roles: []*tree.Role{
				{UserName: "r1"},
				{UserName: "r2"},
				{UserName: "r3"},
			},
			Users: []*tree.User{
				{Username: "u4"},
				{Username: "u5"},
				{Username: "u6"},
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		//init from roles
		for i, role := range stmt.Roles {
			sql, _ := getSqlForRoleIdOfRole(context.TODO(), role.UserName)
			mrs := newMrsForRoleIdOfRole([][]interface{}{
				{i},
			})
			bh.sql2result[sql] = mrs
		}

		//init to 2 roles,
		//init to 1 users
		for i, user := range stmt.Users {
			if i < 2 { //roles
				sql, _ := getSqlForRoleIdOfRole(context.TODO(), user.Username)
				mrs := newMrsForRoleIdOfRole([][]interface{}{
					{i + len(stmt.Roles)},
				})

				bh.sql2result[sql] = mrs

				sql, _ = getSqlForPasswordOfUser(context.TODO(), user.Username)
				mrs = newMrsForPasswordOfUser([][]interface{}{})
				bh.sql2result[sql] = mrs
			} else { //users
				sql, _ := getSqlForRoleIdOfRole(context.TODO(), user.Username)
				mrs := newMrsForRoleIdOfRole([][]interface{}{})

				bh.sql2result[sql] = mrs

				sql, _ = getSqlForPasswordOfUser(context.TODO(), user.Username)
				mrs = newMrsForPasswordOfUser([][]interface{}{
					{i, "111", i},
				})
				bh.sql2result[sql] = mrs

				sql, _ = getSqlForRoleOfUser(context.TODO(), int64(i), moAdminRoleName)
				bh.sql2result[sql] = newMrsForRoleOfUser([][]interface{}{})
			}

		}

		//has "ro roles", need init mo_role_grant (assume empty)
		sql := getSqlForGetAllStuffRoleGrantFormat()
		mrs := newMrsForGetAllStuffRoleGrant([][]interface{}{
			{0, 1, true},
			{1, 2, true},
			{3, 4, true},
		})

		bh.sql2result[sql] = mrs

		//loop on from ... to
		for fromId := range stmt.Roles {
			for toId := range stmt.Users {
				if toId < 2 { //roles
					toId = toId + len(stmt.Roles)
					sql = getSqlForCheckRoleGrant(int64(fromId), int64(toId))
					mrs = newMrsForCheckRoleGrant([][]interface{}{})
					bh.sql2result[sql] = mrs

					sql = getSqlForCheckUserGrant(int64(fromId), int64(toId))
					mrs = newMrsForCheckUserGrant([][]interface{}{})
					bh.sql2result[sql] = mrs
				} else { //users
					sql = getSqlForCheckRoleGrant(int64(fromId), int64(toId))
					mrs = newMrsForCheckRoleGrant([][]interface{}{})
					bh.sql2result[sql] = mrs

					sql = getSqlForCheckUserGrant(int64(fromId), int64(toId))
					mrs = newMrsForCheckUserGrant([][]interface{}{})
					bh.sql2result[sql] = mrs
				}
			}
		}

		err := doGrantRole(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("grant role to role+user 3 (update) succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantRole{
			Roles: []*tree.Role{
				{UserName: "r1"},
				{UserName: "r2"},
				{UserName: "r3"},
			},
			Users: []*tree.User{
				{Username: "u4"},
				{Username: "u5"},
				{Username: "u6"},
			},
			GrantOption: true,
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		//init from roles
		for i, role := range stmt.Roles {
			sql, _ := getSqlForRoleIdOfRole(context.TODO(), role.UserName)
			mrs := newMrsForRoleIdOfRole([][]interface{}{
				{i},
			})
			bh.sql2result[sql] = mrs
		}

		//init to 2 roles,
		//init to 1 users
		for i, user := range stmt.Users {
			if i < 2 { //roles
				sql, _ := getSqlForRoleIdOfRole(context.TODO(), user.Username)
				mrs := newMrsForRoleIdOfRole([][]interface{}{
					{i + len(stmt.Roles)},
				})

				bh.sql2result[sql] = mrs

				sql, _ = getSqlForPasswordOfUser(context.TODO(), user.Username)
				mrs = newMrsForPasswordOfUser([][]interface{}{})
				bh.sql2result[sql] = mrs
			} else { //users
				sql, _ := getSqlForRoleIdOfRole(context.TODO(), user.Username)
				mrs := newMrsForRoleIdOfRole([][]interface{}{})

				bh.sql2result[sql] = mrs

				sql, _ = getSqlForPasswordOfUser(context.TODO(), user.Username)
				mrs = newMrsForPasswordOfUser([][]interface{}{
					{i, "111", i},
				})
				bh.sql2result[sql] = mrs

				sql, _ = getSqlForRoleOfUser(context.TODO(), int64(i), moAdminRoleName)
				bh.sql2result[sql] = newMrsForRoleOfUser([][]interface{}{})
			}

		}

		//has "ro roles", need init mo_role_grant (assume empty)
		sql := getSqlForGetAllStuffRoleGrantFormat()
		mrs := newMrsForGetAllStuffRoleGrant([][]interface{}{
			{0, 1, true},
			{1, 2, true},
			{3, 4, true},
		})

		bh.sql2result[sql] = mrs

		//loop on from ... to
		for fromId := range stmt.Roles {
			for toId := range stmt.Users {
				if toId < 2 { //roles
					toId = toId + len(stmt.Roles)
					sql = getSqlForCheckRoleGrant(int64(fromId), int64(toId))
					mrs = newMrsForCheckRoleGrant([][]interface{}{
						{fromId, toId, false},
					})
					bh.sql2result[sql] = mrs

					//sql = getSqlForCheckUserGrant(int64(fromId), int64(toId))
					//mrs = newMrsForCheckUserGrant([][]interface{}{})
					//bh.sql2result[sql] = mrs
				} else { //users
					//sql = getSqlForCheckRoleGrant(int64(fromId), int64(toId))
					//mrs = newMrsForCheckRoleGrant([][]interface{}{})
					//bh.sql2result[sql] = mrs

					sql = getSqlForCheckUserGrant(int64(fromId), int64(toId))
					mrs = newMrsForCheckUserGrant([][]interface{}{
						{fromId, toId, false},
					})
					bh.sql2result[sql] = mrs
				}

			}
		}

		err := doGrantRole(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("grant role to role fail direct loop", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantRole{
			Roles: []*tree.Role{
				{UserName: "r1"},
			},
			Users: []*tree.User{
				{Username: "r1"},
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		//init from roles
		for i, role := range stmt.Roles {
			sql, _ := getSqlForRoleIdOfRole(context.TODO(), role.UserName)
			mrs := newMrsForRoleIdOfRole([][]interface{}{
				{i},
			})
			bh.sql2result[sql] = mrs
		}

		//init to roles
		for i, user := range stmt.Users {
			sql, _ := getSqlForRoleIdOfRole(context.TODO(), user.Username)
			mrs := newMrsForRoleIdOfRole([][]interface{}{
				{i + len(stmt.Roles)},
			})

			bh.sql2result[sql] = mrs
		}

		//has "ro roles", need init mo_role_grant (assume empty)
		sql := getSqlForGetAllStuffRoleGrantFormat()
		mrs := newMrsForGetAllStuffRoleGrant([][]interface{}{})

		bh.sql2result[sql] = mrs

		//loop on from ... to
		for fromId := range stmt.Roles {
			for toId := range stmt.Users {
				toId = toId + len(stmt.Roles)
				sql = getSqlForCheckRoleGrant(int64(fromId), int64(toId))
				mrs = newMrsForCheckRoleGrant([][]interface{}{})
				bh.sql2result[sql] = mrs
			}
		}

		err := doGrantRole(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeError)
	})

	convey.Convey("grant role to role+user fail indirect loop", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantRole{
			Roles: []*tree.Role{
				{UserName: "r1"},
				{UserName: "r2"},
				{UserName: "r3"},
			},
			Users: []*tree.User{
				{Username: "r4"},
				{Username: "r5"},
				{Username: "u6"},
			},
			GrantOption: true,
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		//init from roles
		for i, role := range stmt.Roles {
			sql, _ := getSqlForRoleIdOfRole(context.TODO(), role.UserName)
			mrs := newMrsForRoleIdOfRole([][]interface{}{
				{i},
			})
			bh.sql2result[sql] = mrs
		}

		//init to 2 roles,
		//init to 1 users
		for i, user := range stmt.Users {
			if i < 2 { //roles
				sql, _ := getSqlForRoleIdOfRole(context.TODO(), user.Username)
				mrs := newMrsForRoleIdOfRole([][]interface{}{
					{i + len(stmt.Roles)},
				})

				bh.sql2result[sql] = mrs

				sql, _ = getSqlForPasswordOfUser(context.TODO(), user.Username)
				mrs = newMrsForPasswordOfUser([][]interface{}{})
				bh.sql2result[sql] = mrs
			} else { //users
				sql, _ := getSqlForRoleIdOfRole(context.TODO(), user.Username)
				mrs := newMrsForRoleIdOfRole([][]interface{}{})

				bh.sql2result[sql] = mrs

				sql, _ = getSqlForPasswordOfUser(context.TODO(), user.Username)
				mrs = newMrsForPasswordOfUser([][]interface{}{
					{i, "111", i},
				})
				bh.sql2result[sql] = mrs

				sql, _ = getSqlForRoleOfUser(context.TODO(), int64(i), moAdminRoleName)
				bh.sql2result[sql] = newMrsForRoleOfUser([][]interface{}{})
			}

		}

		//has "ro roles", need init mo_role_grant (assume empty)
		sql := getSqlForGetAllStuffRoleGrantFormat()
		mrs := newMrsForGetAllStuffRoleGrant([][]interface{}{
			{1, 0, true},
			{2, 1, true},
			{3, 2, true},
			{4, 2, true},
		})

		bh.sql2result[sql] = mrs

		//loop on from ... to
		for fromId := range stmt.Roles {
			for toId := range stmt.Users {
				if toId < 2 { //roles
					toId = toId + len(stmt.Roles)
					sql = getSqlForCheckRoleGrant(int64(fromId), int64(toId))
					mrs = newMrsForCheckRoleGrant([][]interface{}{
						{fromId, toId, false},
					})
					bh.sql2result[sql] = mrs

					//sql = getSqlForCheckUserGrant(int64(fromId), int64(toId))
					//mrs = newMrsForCheckUserGrant([][]interface{}{})
					//bh.sql2result[sql] = mrs
				} else { //users
					//sql = getSqlForCheckRoleGrant(int64(fromId), int64(toId))
					//mrs = newMrsForCheckRoleGrant([][]interface{}{})
					//bh.sql2result[sql] = mrs

					sql = getSqlForCheckUserGrant(int64(fromId), int64(toId))
					mrs = newMrsForCheckUserGrant([][]interface{}{
						{fromId, toId, false},
					})
					bh.sql2result[sql] = mrs
				}

			}
		}

		err := doGrantRole(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeError)
	})

	convey.Convey("grant role to role fail no role", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantRole{
			Roles: []*tree.Role{
				{UserName: "r1"},
				{UserName: "r2"},
				{UserName: "r3"},
			},
			Users: []*tree.User{
				{Username: "r4"},
				{Username: "r5"},
				{Username: "r6"},
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		//init from roles
		for _, role := range stmt.Roles {
			sql, _ := getSqlForRoleIdOfRole(context.TODO(), role.UserName)
			mrs := newMrsForRoleIdOfRole([][]interface{}{})
			bh.sql2result[sql] = mrs
		}

		//init to roles
		for i, user := range stmt.Users {
			sql, _ := getSqlForRoleIdOfRole(context.TODO(), user.Username)
			mrs := newMrsForRoleIdOfRole([][]interface{}{
				{i + len(stmt.Roles)},
			})

			bh.sql2result[sql] = mrs
		}

		//has "ro roles", need init mo_role_grant (assume empty)
		sql := getSqlForGetAllStuffRoleGrantFormat()
		mrs := newMrsForGetAllStuffRoleGrant([][]interface{}{})

		bh.sql2result[sql] = mrs

		//loop on from ... to
		for fromId := range stmt.Roles {
			for toId := range stmt.Users {
				toId = toId + len(stmt.Roles)
				sql = getSqlForCheckRoleGrant(int64(fromId), int64(toId))
				mrs = newMrsForCheckRoleGrant([][]interface{}{})
				bh.sql2result[sql] = mrs
			}
		}

		err := doGrantRole(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeError)
	})

	convey.Convey("grant role to user fail no user", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantRole{
			Roles: []*tree.Role{
				{UserName: "r1"},
				{UserName: "r2"},
				{UserName: "r3"},
			},
			Users: []*tree.User{
				{Username: "u4"},
				{Username: "u5"},
				{Username: "u6"},
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		//init from roles
		for i, role := range stmt.Roles {
			sql, _ := getSqlForRoleIdOfRole(context.TODO(), role.UserName)
			mrs := newMrsForRoleIdOfRole([][]interface{}{
				{i},
			})
			bh.sql2result[sql] = mrs
		}

		//init to empty roles,
		//init to users
		for _, user := range stmt.Users {
			sql, _ := getSqlForRoleIdOfRole(context.TODO(), user.Username)
			mrs := newMrsForRoleIdOfRole([][]interface{}{})

			bh.sql2result[sql] = mrs

			sql, _ = getSqlForPasswordOfUser(context.TODO(), user.Username)
			mrs = newMrsForPasswordOfUser([][]interface{}{})
			bh.sql2result[sql] = mrs
		}

		//has "ro roles", need init mo_role_grant (assume empty)
		sql := getSqlForGetAllStuffRoleGrantFormat()
		mrs := newMrsForGetAllStuffRoleGrant([][]interface{}{})

		bh.sql2result[sql] = mrs

		//loop on from ... to
		for fromId := range stmt.Roles {
			for toId := range stmt.Users {
				sql = getSqlForCheckRoleGrant(int64(fromId), int64(toId))
				mrs = newMrsForCheckRoleGrant([][]interface{}{})
				bh.sql2result[sql] = mrs

				sql = getSqlForCheckUserGrant(int64(fromId), int64(toId))
				mrs = newMrsForCheckUserGrant([][]interface{}{})
				bh.sql2result[sql] = mrs
			}
		}

		err := doGrantRole(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeError)
	})
}

func Test_doRevokeRole(t *testing.T) {
	convey.Convey("revoke role from role succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.RevokeRole{
			Roles: []*tree.Role{
				{UserName: "r1"},
				{UserName: "r2"},
				{UserName: "r3"},
			},
			Users: []*tree.User{
				{Username: "r4"},
				{Username: "r5"},
				{Username: "r6"},
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		//init from roles
		for i, role := range stmt.Roles {
			sql, _ := getSqlForRoleIdOfRole(context.TODO(), role.UserName)
			mrs := newMrsForRoleIdOfRole([][]interface{}{
				{i},
			})
			bh.sql2result[sql] = mrs
		}

		//init to roles
		for i, user := range stmt.Users {
			sql, _ := getSqlForRoleIdOfRole(context.TODO(), user.Username)
			mrs := newMrsForRoleIdOfRole([][]interface{}{
				{i + len(stmt.Roles)},
			})

			bh.sql2result[sql] = mrs
		}

		//loop on from ... to
		for fromId := range stmt.Roles {
			for toId := range stmt.Users {
				toId = toId + len(stmt.Roles)
				sql := getSqlForDeleteRoleGrant(int64(fromId), int64(toId))
				bh.sql2result[sql] = nil
			}
		}

		err := doRevokeRole(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("revoke role from role succ (if exists = true, miss role before FROM)", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.RevokeRole{
			IfExists: true,
			Roles: []*tree.Role{
				{UserName: "r1"},
				{UserName: "r2"},
				{UserName: "r3"},
			},
			Users: []*tree.User{
				{Username: "r4"},
				{Username: "r5"},
				{Username: "r6"},
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		//init from roles
		var mrs *MysqlResultSet
		for i, role := range stmt.Roles {
			sql, _ := getSqlForRoleIdOfRole(context.TODO(), role.UserName)
			mrs = newMrsForRoleIdOfRole([][]interface{}{
				{i},
			})
			bh.sql2result[sql] = mrs
		}

		//init to roles
		for i, user := range stmt.Users {
			sql, _ := getSqlForRoleIdOfRole(context.TODO(), user.Username)
			mrs := newMrsForRoleIdOfRole([][]interface{}{
				{i + len(stmt.Roles)},
			})

			bh.sql2result[sql] = mrs
		}

		//loop on from ... to
		for fromId := range stmt.Roles {
			for toId := range stmt.Users {
				toId = toId + len(stmt.Roles)
				sql := getSqlForDeleteRoleGrant(int64(fromId), int64(toId))
				bh.sql2result[sql] = nil
			}
		}

		err := doRevokeRole(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("revoke role from role fail (if exists = false,miss role before FROM)", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.RevokeRole{
			Roles: []*tree.Role{
				{UserName: "r1"},
				{UserName: "r2"},
				{UserName: "r3"},
			},
			Users: []*tree.User{
				{Username: "r4"},
				{Username: "r5"},
				{Username: "r6"},
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		//init from roles
		var mrs *MysqlResultSet
		for i, role := range stmt.Roles {
			sql, _ := getSqlForRoleIdOfRole(context.TODO(), role.UserName)
			if i == 0 {
				mrs = newMrsForRoleIdOfRole([][]interface{}{})
			} else {
				mrs = newMrsForRoleIdOfRole([][]interface{}{
					{i},
				})
			}

			bh.sql2result[sql] = mrs
		}

		//init to roles
		for i, user := range stmt.Users {
			sql, _ := getSqlForRoleIdOfRole(context.TODO(), user.Username)
			mrs := newMrsForRoleIdOfRole([][]interface{}{
				{i + len(stmt.Roles)},
			})

			bh.sql2result[sql] = mrs
		}

		//loop on from ... to
		for fromId := range stmt.Roles {
			for toId := range stmt.Users {
				toId = toId + len(stmt.Roles)
				sql := getSqlForDeleteRoleGrant(int64(fromId), int64(toId))
				bh.sql2result[sql] = nil
			}
		}

		err := doRevokeRole(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeError)
	})

	convey.Convey("revoke role from user fail (if exists = false,miss role after FROM)", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.RevokeRole{
			Roles: []*tree.Role{
				{UserName: "r1"},
				{UserName: "r2"},
				{UserName: "r3"},
			},
			Users: []*tree.User{
				{Username: "u1"},
				{Username: "u2"},
				{Username: "u3"},
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		//init from roles
		var mrs *MysqlResultSet
		for i, role := range stmt.Roles {
			sql, _ := getSqlForRoleIdOfRole(context.TODO(), role.UserName)
			mrs = newMrsForRoleIdOfRole([][]interface{}{
				{i},
			})

			bh.sql2result[sql] = mrs
		}

		//init to roles
		for i, user := range stmt.Users {
			//sql := sql, _ := getSqlForRoleIdOfRole(context.TODO(),user.Username)
			//mrs = newMrsForRoleIdOfRole([][]interface{}{})

			sql, _ := getSqlForPasswordOfUser(context.TODO(), user.Username)
			//miss u2
			if i == 1 {
				mrs = newMrsForPasswordOfUser([][]interface{}{})
			} else {
				mrs = newMrsForPasswordOfUser([][]interface{}{
					{i + len(stmt.Roles)},
				})
			}

			bh.sql2result[sql] = mrs
		}

		//loop on from ... to
		for fromId := range stmt.Roles {
			for toId := range stmt.Users {
				toId = toId + len(stmt.Roles)
				sql := getSqlForDeleteUserGrant(int64(fromId), int64(toId))
				bh.sql2result[sql] = nil
			}
		}

		err := doRevokeRole(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeError)
	})

	convey.Convey("revoke role from user succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.RevokeRole{
			Roles: []*tree.Role{
				{UserName: "r1"},
				{UserName: "r2"},
				{UserName: "r3"},
			},
			Users: []*tree.User{
				{Username: "u4"},
				{Username: "u5"},
				{Username: "u6"},
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		//init from roles
		for i, role := range stmt.Roles {
			sql, _ := getSqlForRoleIdOfRole(context.TODO(), role.UserName)
			mrs := newMrsForRoleIdOfRole([][]interface{}{
				{i},
			})
			bh.sql2result[sql] = mrs
		}

		//init to roles
		for i, user := range stmt.Users {
			sql, _ := getSqlForRoleIdOfRole(context.TODO(), user.Username)
			mrs := newMrsForRoleIdOfRole([][]interface{}{})

			bh.sql2result[sql] = mrs

			sql, _ = getSqlForPasswordOfUser(context.TODO(), user.Username)
			mrs = newMrsForPasswordOfUser([][]interface{}{
				{i},
			})

			bh.sql2result[sql] = mrs
		}

		//loop on from ... to
		for fromId := range stmt.Roles {
			for toId := range stmt.Users {
				toId = toId + len(stmt.Roles)
				sql := getSqlForDeleteRoleGrant(int64(fromId), int64(toId))
				bh.sql2result[sql] = nil
			}
		}

		err := doRevokeRole(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_doGrantPrivilege(t *testing.T) {
	convey.Convey("grant account, role succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantPrivilege{
			Privileges: []*tree.Privilege{
				{Type: tree.PRIVILEGE_TYPE_STATIC_CREATE_DATABASE},
			},
			ObjType: tree.OBJECT_TYPE_ACCOUNT,
			Level:   &tree.PrivilegeLevel{Level: tree.PRIVILEGE_LEVEL_TYPE_STAR},
			Roles: []*tree.Role{
				{UserName: "r1"},
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		//init from roles
		for i, role := range stmt.Roles {
			sql, _ := getSqlForRoleIdOfRole(context.TODO(), role.UserName)
			mrs := newMrsForRoleIdOfRole([][]interface{}{
				{i},
			})
			bh.sql2result[sql] = mrs
		}

		for _, p := range stmt.Privileges {
			privType, err := convertAstPrivilegeTypeToPrivilegeType(context.TODO(), p.Type, tree.OBJECT_TYPE_ACCOUNT)
			convey.So(err, convey.ShouldBeNil)
			for j := range stmt.Roles {
				sql := getSqlForCheckRoleHasPrivilege(int64(j), objectTypeAccount, objectIDAll, int64(privType))
				mrs := newMrsForCheckRoleHasPrivilege([][]interface{}{})
				bh.sql2result[sql] = mrs
			}
		}

		err := doGrantPrivilege(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("grant database, role succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmts := []*tree.GrantPrivilege{
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SHOW_TABLES},
				},
				ObjType: tree.OBJECT_TYPE_DATABASE,
				Level: &tree.PrivilegeLevel{
					Level: tree.PRIVILEGE_LEVEL_TYPE_STAR,
				},
				Roles: []*tree.Role{
					{UserName: "r1"},
				},
			},
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SHOW_TABLES},
				},
				ObjType: tree.OBJECT_TYPE_DATABASE,
				Level: &tree.PrivilegeLevel{
					Level: tree.PRIVILEGE_LEVEL_TYPE_STAR_STAR,
				},
				Roles: []*tree.Role{
					{UserName: "r1"},
				},
			},
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SHOW_TABLES},
				},
				ObjType: tree.OBJECT_TYPE_DATABASE,
				Level: &tree.PrivilegeLevel{
					Level:  tree.PRIVILEGE_LEVEL_TYPE_DATABASE,
					DbName: "d",
				},
				Roles: []*tree.Role{
					{UserName: "r1"},
				},
			},
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SHOW_TABLES},
				},
				ObjType: tree.OBJECT_TYPE_DATABASE,
				Level: &tree.PrivilegeLevel{
					Level:   tree.PRIVILEGE_LEVEL_TYPE_TABLE,
					TabName: "d",
				},
				Roles: []*tree.Role{
					{UserName: "r1"},
				},
			},
		}

		for _, stmt := range stmts {
			priv := determinePrivilegeSetOfStatement(stmt)
			ses := newSes(priv, ctrl)

			//no result set
			bh.sql2result["begin;"] = nil
			bh.sql2result["commit;"] = nil
			bh.sql2result["rollback;"] = nil

			//init from roles
			for i, role := range stmt.Roles {
				sql, _ := getSqlForRoleIdOfRole(context.TODO(), role.UserName)
				mrs := newMrsForRoleIdOfRole([][]interface{}{
					{i},
				})
				bh.sql2result[sql] = mrs
			}

			objType, err := convertAstObjectTypeToObjectType(context.TODO(), stmt.ObjType)
			convey.So(err, convey.ShouldBeNil)

			if stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_DATABASE {
				sql, _ := getSqlForCheckDatabase(context.TODO(), stmt.Level.DbName)
				mrs := newMrsForCheckDatabase([][]interface{}{
					{0},
				})
				bh.sql2result[sql] = mrs
			} else if stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_TABLE {
				sql, _ := getSqlForCheckDatabase(context.TODO(), stmt.Level.TabName)
				mrs := newMrsForCheckDatabase([][]interface{}{
					{0},
				})
				bh.sql2result[sql] = mrs
			}

			_, objId, err := checkPrivilegeObjectTypeAndPrivilegeLevel(context.TODO(), ses, bh, stmt.ObjType, *stmt.Level)
			convey.So(err, convey.ShouldBeNil)

			for _, p := range stmt.Privileges {
				privType, err := convertAstPrivilegeTypeToPrivilegeType(context.TODO(), p.Type, stmt.ObjType)
				convey.So(err, convey.ShouldBeNil)
				for j := range stmt.Roles {
					sql := getSqlForCheckRoleHasPrivilege(int64(j), objType, objId, int64(privType))
					mrs := newMrsForCheckRoleHasPrivilege([][]interface{}{})
					bh.sql2result[sql] = mrs
				}
			}

			err = doGrantPrivilege(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
			convey.So(err, convey.ShouldBeNil)
		}
	})
	convey.Convey("grant table, role succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		dbName := "d"
		tableName := "t"
		stmts := []*tree.GrantPrivilege{
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
				},
				ObjType: tree.OBJECT_TYPE_TABLE,
				Level: &tree.PrivilegeLevel{
					Level: tree.PRIVILEGE_LEVEL_TYPE_STAR,
				},
				Roles: []*tree.Role{
					{UserName: "r1"},
				},
			},
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
				},
				ObjType: tree.OBJECT_TYPE_TABLE,
				Level: &tree.PrivilegeLevel{
					Level: tree.PRIVILEGE_LEVEL_TYPE_STAR_STAR,
				},
				Roles: []*tree.Role{
					{UserName: "r1"},
				},
			},
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
				},
				ObjType: tree.OBJECT_TYPE_TABLE,
				Level: &tree.PrivilegeLevel{
					Level:  tree.PRIVILEGE_LEVEL_TYPE_DATABASE_STAR,
					DbName: dbName,
				},
				Roles: []*tree.Role{
					{UserName: "r1"},
				},
			},
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
				},
				ObjType: tree.OBJECT_TYPE_TABLE,
				Level: &tree.PrivilegeLevel{
					Level:   tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE,
					DbName:  dbName,
					TabName: tableName,
				},
				Roles: []*tree.Role{
					{UserName: "r1"},
				},
			},
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
				},
				ObjType: tree.OBJECT_TYPE_TABLE,
				Level: &tree.PrivilegeLevel{
					Level:   tree.PRIVILEGE_LEVEL_TYPE_TABLE,
					TabName: tableName,
				},
				Roles: []*tree.Role{
					{UserName: "r1"},
				},
			},
		}

		for _, stmt := range stmts {
			priv := determinePrivilegeSetOfStatement(stmt)
			ses := newSes(priv, ctrl)
			ses.SetDatabaseName("d")

			//no result set
			bh.sql2result["begin;"] = nil
			bh.sql2result["commit;"] = nil
			bh.sql2result["rollback;"] = nil

			//init from roles
			for i, role := range stmt.Roles {
				sql, _ := getSqlForRoleIdOfRole(context.TODO(), role.UserName)
				mrs := newMrsForRoleIdOfRole([][]interface{}{
					{i},
				})
				bh.sql2result[sql] = mrs
			}

			objType, err := convertAstObjectTypeToObjectType(context.TODO(), stmt.ObjType)
			convey.So(err, convey.ShouldBeNil)

			if stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_STAR ||
				stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_DATABASE_STAR {
				sql, _ := getSqlForCheckDatabase(context.TODO(), dbName)
				mrs := newMrsForCheckDatabase([][]interface{}{
					{0},
				})
				bh.sql2result[sql] = mrs
			} else if stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_TABLE ||
				stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE {
				sql, _ := getSqlForCheckDatabaseTable(context.TODO(), dbName, tableName)
				mrs := newMrsForCheckDatabaseTable([][]interface{}{
					{0},
				})
				bh.sql2result[sql] = mrs
			}

			_, objId, err := checkPrivilegeObjectTypeAndPrivilegeLevel(context.TODO(), ses, bh, stmt.ObjType, *stmt.Level)
			convey.So(err, convey.ShouldBeNil)

			for _, p := range stmt.Privileges {
				privType, err := convertAstPrivilegeTypeToPrivilegeType(context.TODO(), p.Type, stmt.ObjType)
				convey.So(err, convey.ShouldBeNil)
				for j := range stmt.Roles {
					sql := getSqlForCheckRoleHasPrivilege(int64(j), objType, objId, int64(privType))
					mrs := newMrsForCheckRoleHasPrivilege([][]interface{}{})
					bh.sql2result[sql] = mrs
				}
			}

			err = doGrantPrivilege(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
			convey.So(err, convey.ShouldBeNil)
		}
	})
}

func Test_doRevokePrivilege(t *testing.T) {
	convey.Convey("revoke account, role succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.RevokePrivilege{
			Privileges: []*tree.Privilege{
				{Type: tree.PRIVILEGE_TYPE_STATIC_CREATE_DATABASE},
			},
			ObjType: tree.OBJECT_TYPE_ACCOUNT,
			Level:   &tree.PrivilegeLevel{Level: tree.PRIVILEGE_LEVEL_TYPE_STAR},
			Roles: []*tree.Role{
				{UserName: "r1"},
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		//init from roles
		for i, role := range stmt.Roles {
			sql, _ := getSqlForRoleIdOfRole(context.TODO(), role.UserName)
			mrs := newMrsForRoleIdOfRole([][]interface{}{
				{i},
			})
			bh.sql2result[sql] = mrs
		}

		for _, p := range stmt.Privileges {
			privType, err := convertAstPrivilegeTypeToPrivilegeType(context.TODO(), p.Type, tree.OBJECT_TYPE_ACCOUNT)
			convey.So(err, convey.ShouldBeNil)
			for j := range stmt.Roles {
				sql := getSqlForCheckRoleHasPrivilege(int64(j), objectTypeAccount, objectIDAll, int64(privType))
				mrs := newMrsForCheckRoleHasPrivilege([][]interface{}{})
				bh.sql2result[sql] = mrs
			}
		}

		err := doRevokePrivilege(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("revoke database, role succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmts := []*tree.RevokePrivilege{
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SHOW_TABLES},
				},
				ObjType: tree.OBJECT_TYPE_DATABASE,
				Level: &tree.PrivilegeLevel{
					Level: tree.PRIVILEGE_LEVEL_TYPE_STAR,
				},
				Roles: []*tree.Role{
					{UserName: "r1"},
				},
			},
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SHOW_TABLES},
				},
				ObjType: tree.OBJECT_TYPE_DATABASE,
				Level: &tree.PrivilegeLevel{
					Level: tree.PRIVILEGE_LEVEL_TYPE_STAR_STAR,
				},
				Roles: []*tree.Role{
					{UserName: "r1"},
				},
			},
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SHOW_TABLES},
				},
				ObjType: tree.OBJECT_TYPE_DATABASE,
				Level: &tree.PrivilegeLevel{
					Level:  tree.PRIVILEGE_LEVEL_TYPE_DATABASE,
					DbName: "d",
				},
				Roles: []*tree.Role{
					{UserName: "r1"},
				},
			},
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SHOW_TABLES},
				},
				ObjType: tree.OBJECT_TYPE_DATABASE,
				Level: &tree.PrivilegeLevel{
					Level:   tree.PRIVILEGE_LEVEL_TYPE_TABLE,
					TabName: "d",
				},
				Roles: []*tree.Role{
					{UserName: "r1"},
				},
			},
		}

		for _, stmt := range stmts {
			priv := determinePrivilegeSetOfStatement(stmt)
			ses := newSes(priv, ctrl)

			//no result set
			bh.sql2result["begin;"] = nil
			bh.sql2result["commit;"] = nil
			bh.sql2result["rollback;"] = nil

			//init from roles
			for i, role := range stmt.Roles {
				sql, _ := getSqlForRoleIdOfRole(context.TODO(), role.UserName)
				mrs := newMrsForRoleIdOfRole([][]interface{}{
					{i},
				})
				bh.sql2result[sql] = mrs
			}

			objType, err := convertAstObjectTypeToObjectType(context.TODO(), stmt.ObjType)
			convey.So(err, convey.ShouldBeNil)

			if stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_DATABASE {
				sql, _ := getSqlForCheckDatabase(context.TODO(), stmt.Level.DbName)
				mrs := newMrsForCheckDatabase([][]interface{}{
					{0},
				})
				bh.sql2result[sql] = mrs
			} else if stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_TABLE {
				sql, _ := getSqlForCheckDatabase(context.TODO(), stmt.Level.TabName)
				mrs := newMrsForCheckDatabase([][]interface{}{
					{0},
				})
				bh.sql2result[sql] = mrs
			}

			_, objId, err := checkPrivilegeObjectTypeAndPrivilegeLevel(context.TODO(), ses, bh, stmt.ObjType, *stmt.Level)
			convey.So(err, convey.ShouldBeNil)

			for _, p := range stmt.Privileges {
				privType, err := convertAstPrivilegeTypeToPrivilegeType(context.TODO(), p.Type, stmt.ObjType)
				convey.So(err, convey.ShouldBeNil)
				for j := range stmt.Roles {
					sql := getSqlForCheckRoleHasPrivilege(int64(j), objType, objId, int64(privType))
					mrs := newMrsForCheckRoleHasPrivilege([][]interface{}{})
					bh.sql2result[sql] = mrs
				}
			}

			err = doRevokePrivilege(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
			convey.So(err, convey.ShouldBeNil)
		}
	})
	convey.Convey("revoke table, role succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		dbName := "d"
		tableName := "t"
		stmts := []*tree.RevokePrivilege{
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
				},
				ObjType: tree.OBJECT_TYPE_TABLE,
				Level: &tree.PrivilegeLevel{
					Level: tree.PRIVILEGE_LEVEL_TYPE_STAR,
				},
				Roles: []*tree.Role{
					{UserName: "r1"},
				},
			},
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
				},
				ObjType: tree.OBJECT_TYPE_TABLE,
				Level: &tree.PrivilegeLevel{
					Level: tree.PRIVILEGE_LEVEL_TYPE_STAR_STAR,
				},
				Roles: []*tree.Role{
					{UserName: "r1"},
				},
			},
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
				},
				ObjType: tree.OBJECT_TYPE_TABLE,
				Level: &tree.PrivilegeLevel{
					Level:  tree.PRIVILEGE_LEVEL_TYPE_DATABASE_STAR,
					DbName: dbName,
				},
				Roles: []*tree.Role{
					{UserName: "r1"},
				},
			},
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
				},
				ObjType: tree.OBJECT_TYPE_TABLE,
				Level: &tree.PrivilegeLevel{
					Level:   tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE,
					DbName:  dbName,
					TabName: tableName,
				},
				Roles: []*tree.Role{
					{UserName: "r1"},
				},
			},
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
				},
				ObjType: tree.OBJECT_TYPE_TABLE,
				Level: &tree.PrivilegeLevel{
					Level:   tree.PRIVILEGE_LEVEL_TYPE_TABLE,
					TabName: tableName,
				},
				Roles: []*tree.Role{
					{UserName: "r1"},
				},
			},
		}

		for _, stmt := range stmts {
			priv := determinePrivilegeSetOfStatement(stmt)
			ses := newSes(priv, ctrl)
			ses.SetDatabaseName("d")

			//no result set
			bh.sql2result["begin;"] = nil
			bh.sql2result["commit;"] = nil
			bh.sql2result["rollback;"] = nil

			//init from roles
			for i, role := range stmt.Roles {
				sql, _ := getSqlForRoleIdOfRole(context.TODO(), role.UserName)
				mrs := newMrsForRoleIdOfRole([][]interface{}{
					{i},
				})
				bh.sql2result[sql] = mrs
			}

			objType, err := convertAstObjectTypeToObjectType(context.TODO(), stmt.ObjType)
			convey.So(err, convey.ShouldBeNil)

			if stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_STAR ||
				stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_DATABASE_STAR {
				sql, _ := getSqlForCheckDatabase(context.TODO(), dbName)
				mrs := newMrsForCheckDatabase([][]interface{}{
					{0},
				})
				bh.sql2result[sql] = mrs
			} else if stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_TABLE ||
				stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE {
				sql, _ := getSqlForCheckDatabaseTable(context.TODO(), dbName, tableName)
				mrs := newMrsForCheckDatabaseTable([][]interface{}{
					{0},
				})
				bh.sql2result[sql] = mrs
			}

			_, objId, err := checkPrivilegeObjectTypeAndPrivilegeLevel(context.TODO(), ses, bh, stmt.ObjType, *stmt.Level)
			convey.So(err, convey.ShouldBeNil)

			for _, p := range stmt.Privileges {
				privType, err := convertAstPrivilegeTypeToPrivilegeType(context.TODO(), p.Type, stmt.ObjType)
				convey.So(err, convey.ShouldBeNil)
				for j := range stmt.Roles {
					sql := getSqlForCheckRoleHasPrivilege(int64(j), objType, objId, int64(privType))
					mrs := newMrsForCheckRoleHasPrivilege([][]interface{}{})
					bh.sql2result[sql] = mrs
				}
			}

			err = doRevokePrivilege(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
			convey.So(err, convey.ShouldBeNil)
		}
	})
}

func Test_doDropFunctionWithDB(t *testing.T) {
	convey.Convey("drop function with db", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()

		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		bh := &backgroundExecTest{}
		bh.init()
		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.DropDatabase{
			Name: tree.Identifier("abc"),
		}

		ses := &Session{}
		sql := getSqlForCheckUdfWithDb(string(stmt.Name))

		bh.sql2result[sql] = nil
		err := doDropFunctionWithDB(ctx, ses, stmt, nil)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func Test_doDropFunction(t *testing.T) {
	convey.Convey("drop function", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()

		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().ClearExecResultSet().AnyTimes()
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		rs := mock_frontend.NewMockExecResult(ctrl)
		rs.EXPECT().GetRowCount().Return(uint64(0)).AnyTimes()
		bh.EXPECT().GetExecResultSet().Return([]interface{}{rs}).AnyTimes()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		cu := &tree.DropFunction{
			Name: tree.NewFuncName("testFunc",
				tree.ObjectNamePrefix{
					SchemaName:      tree.Identifier("db"),
					CatalogName:     tree.Identifier(""),
					ExplicitSchema:  true,
					ExplicitCatalog: false,
				},
			),
			Args: nil,
		}

		ses := &Session{}
		err := doDropFunction(ctx, ses, cu, nil)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func Test_doDropRole(t *testing.T) {
	convey.Convey("drop role succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.DropRole{
			Roles: []*tree.Role{
				{UserName: "r1"},
				{UserName: "r2"},
				{UserName: "r3"},
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		//init from roles
		for i, role := range stmt.Roles {
			sql, _ := getSqlForRoleIdOfRole(context.TODO(), role.UserName)
			mrs := newMrsForRoleIdOfRole([][]interface{}{
				{i},
			})
			bh.sql2result[sql] = mrs
		}

		for i := range stmt.Roles {
			sqls := getSqlForDeleteRole(int64(i))
			for _, sql := range sqls {
				bh.sql2result[sql] = nil
			}
		}

		err := doDropRole(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("drop role succ (if exists)", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.DropRole{
			IfExists: true,
			Roles: []*tree.Role{
				{UserName: "r1"},
				{UserName: "r2"},
				{UserName: "r3"},
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		var mrs *MysqlResultSet
		//init from roles
		for i, role := range stmt.Roles {
			sql, _ := getSqlForRoleIdOfRole(context.TODO(), role.UserName)
			if i == 0 {
				mrs = newMrsForRoleIdOfRole([][]interface{}{})
			} else {
				mrs = newMrsForRoleIdOfRole([][]interface{}{
					{i},
				})
			}

			bh.sql2result[sql] = mrs
		}

		for i := range stmt.Roles {
			sqls := getSqlForDeleteRole(int64(i))
			for _, sql := range sqls {
				bh.sql2result[sql] = nil
			}
		}

		err := doDropRole(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("drop role fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.DropRole{
			IfExists: false,
			Roles: []*tree.Role{
				{UserName: "r1"},
				{UserName: "r2"},
				{UserName: "r3"},
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		var mrs *MysqlResultSet
		//init from roles
		for i, role := range stmt.Roles {
			sql, _ := getSqlForRoleIdOfRole(context.TODO(), role.UserName)
			if i == 0 {
				mrs = newMrsForRoleIdOfRole([][]interface{}{})
			} else {
				mrs = newMrsForRoleIdOfRole([][]interface{}{
					{i},
				})
			}

			bh.sql2result[sql] = mrs
		}

		for i := range stmt.Roles {
			sqls := getSqlForDeleteRole(int64(i))
			for _, sql := range sqls {
				bh.sql2result[sql] = nil
			}
		}

		err := doDropRole(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeError)
	})
}

func Test_doDropUser(t *testing.T) {
	convey.Convey("drop user succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.DropUser{
			Users: []*tree.User{
				{Username: "u1"},
				{Username: "u2"},
				{Username: "u3"},
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		for i, user := range stmt.Users {
			sql, _ := getSqlForPasswordOfUser(context.TODO(), user.Username)
			mrs := newMrsForPasswordOfUser([][]interface{}{
				{i, "111", "public"},
			})
			bh.sql2result[sql] = mrs

			sql, _ = getSqlForCheckUserHasRole(context.TODO(), user.Username, moAdminRoleID)
			mrs = newMrsForSqlForCheckUserHasRole([][]interface{}{})
			bh.sql2result[sql] = mrs
		}

		for i := range stmt.Users {
			sqls := getSqlForDeleteUser(int64(i))
			for _, sql := range sqls {
				bh.sql2result[sql] = nil
			}
		}

		err := doDropUser(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("drop user succ (if exists)", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.DropUser{
			IfExists: true,
			Users: []*tree.User{
				{Username: "u1"},
				{Username: "u2"},
				{Username: "u3"},
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		var mrs *MysqlResultSet
		//init from roles
		for i, user := range stmt.Users {
			sql, _ := getSqlForPasswordOfUser(context.TODO(), user.Username)
			if i == 0 {
				mrs = newMrsForPasswordOfUser([][]interface{}{})
			} else {
				mrs = newMrsForPasswordOfUser([][]interface{}{
					{i, "111", "public"},
				})
			}

			bh.sql2result[sql] = mrs
			sql, _ = getSqlForCheckUserHasRole(context.TODO(), user.Username, moAdminRoleID)
			mrs = newMrsForSqlForCheckUserHasRole([][]interface{}{})
			bh.sql2result[sql] = mrs
		}

		for i := range stmt.Users {
			sqls := getSqlForDeleteUser(int64(i))
			for _, sql := range sqls {
				bh.sql2result[sql] = nil
			}
		}

		err := doDropUser(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("drop user fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.DropUser{
			IfExists: false,
			Users: []*tree.User{
				{Username: "u1"},
				{Username: "u2"},
				{Username: "u3"},
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		var mrs *MysqlResultSet
		//init from roles
		for i, user := range stmt.Users {
			sql, _ := getSqlForPasswordOfUser(context.TODO(), user.Username)
			if i == 0 {
				mrs = newMrsForPasswordOfUser([][]interface{}{})
			} else {
				mrs = newMrsForPasswordOfUser([][]interface{}{
					{i, "111", "public"},
				})
			}

			bh.sql2result[sql] = mrs

			sql, _ = getSqlForCheckUserHasRole(context.TODO(), user.Username, moAdminRoleID)
			mrs = newMrsForSqlForCheckUserHasRole([][]interface{}{})
			bh.sql2result[sql] = mrs
		}

		for i := range stmt.Users {
			sqls := getSqlForDeleteUser(int64(i))
			for _, sql := range sqls {
				bh.sql2result[sql] = nil
			}
		}

		err := doDropUser(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeError)
	})
}

func Test_doInterpretCall(t *testing.T) {
	convey.Convey("call precedure (not exist)fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()
		call := &tree.CallStmt{
			Name: tree.NewProcedureName("test_if_hit_elseif_first_elseif", tree.ObjectNamePrefix{}),
		}

		priv := determinePrivilegeSetOfStatement(call)
		ses := newSes(priv, ctrl)
		proc := testutil.NewProcess()
		proc.FileService = getGlobalPu().FileService
		proc.SessionInfo = process.SessionInfo{Account: sysAccountName}
		ses.GetTxnCompileCtx().execCtx = &ExecCtx{
			proc: proc,
		}
		ses.SetDatabaseName("procedure_test")
		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx)
		ses.rm = rm

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForSpBody(ses.GetTxnHandler().GetConnCtx(), string(call.Name.Name.ObjectName), ses.GetDatabaseName())
		convey.So(err, convey.ShouldBeNil)
		mrs := newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err = doInterpretCall(ctx, ses, call)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("call precedure (not support)fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()
		call := &tree.CallStmt{
			Name: tree.NewProcedureName("test_if_hit_elseif_first_elseif", tree.ObjectNamePrefix{}),
		}

		priv := determinePrivilegeSetOfStatement(call)
		ses := newSes(priv, ctrl)
		proc := testutil.NewProcess()
		proc.FileService = getGlobalPu().FileService
		proc.SessionInfo = process.SessionInfo{Account: sysAccountName}
		ses.SetDatabaseName("procedure_test")
		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		ses.GetTxnCompileCtx().execCtx = &ExecCtx{reqCtx: ctx, proc: proc, ses: ses}
		rm, _ := NewRoutineManager(ctx)
		ses.rm = rm

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForSpBody(ses.GetTxnHandler().GetConnCtx(), string(call.Name.Name.ObjectName), ses.GetDatabaseName())
		convey.So(err, convey.ShouldBeNil)
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{"begin set sid = 1000; end", "{}"},
		})
		bh.sql2result[sql] = mrs

		sql = getSystemVariablesWithAccount(uint64(ses.GetTenantInfo().GetTenantID()))
		mrs = newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = getSqlForGetSystemVariableValueWithDatabase("procedure_test", "version_compatibility")
		mrs = newMrsForPasswordOfUser([][]interface{}{
			{"0.7"},
		})
		bh.sql2result[sql] = mrs

		_, err = doInterpretCall(ctx, ses, call)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("call precedure (not support)fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()
		call := &tree.CallStmt{
			Name: tree.NewProcedureName("test_if_hit_elseif_first_elseif", tree.ObjectNamePrefix{}),
		}

		priv := determinePrivilegeSetOfStatement(call)
		ses := newSes(priv, ctrl)
		proc := testutil.NewProcess()
		proc.FileService = getGlobalPu().FileService
		proc.SessionInfo = process.SessionInfo{Account: sysAccountName}
		ses.SetDatabaseName("procedure_test")
		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		ses.GetTxnCompileCtx().execCtx = &ExecCtx{reqCtx: ctx, proc: proc,
			ses: ses}
		rm, _ := NewRoutineManager(ctx)
		ses.rm = rm

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForSpBody(ses.GetTxnHandler().GetConnCtx(), string(call.Name.Name.ObjectName), ses.GetDatabaseName())
		convey.So(err, convey.ShouldBeNil)
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{"begin DECLARE v1 INT; SET v1 = 10; IF v1 > 5 THEN select * from tbh1; ELSEIF v1 = 5 THEN select * from tbh2; ELSEIF v1 = 4 THEN select * from tbh2 limit 1; ELSE select * from tbh3; END IF; end", "{}"},
		})
		bh.sql2result[sql] = mrs

		sql = getSystemVariablesWithAccount(uint64(ses.GetTenantInfo().GetTenantID()))
		mrs = newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = getSqlForGetSystemVariableValueWithDatabase("procedure_test", "version_compatibility")
		mrs = newMrsForPasswordOfUser([][]interface{}{
			{"0.7"},
		})
		bh.sql2result[sql] = mrs

		sql = "select v1 > 5"
		mrs = newMrsForPasswordOfUser([][]interface{}{
			{"1"},
		})
		bh.sql2result[sql] = mrs

		sql = "select * from tbh1"
		mrs = newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err = doInterpretCall(ctx, ses, call)
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_initProcedure(t *testing.T) {
	convey.Convey("init precedure fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()
		cp := &tree.CreateProcedure{
			Name: tree.NewProcedureName("test_if_hit_elseif_first_elseif", tree.ObjectNamePrefix{}),
			Args: nil,
			Body: "'begin DECLARE v1 INT; SET v1 = 5; IF v1 > 5 THEN select * from tbh1; ELSEIF v1 = 5 THEN select * from tbh2; ELSEIF v1 = 4 THEN select * from tbh2 limit 1; ELSE select * from tbh3; END IF; end'",
		}

		priv := determinePrivilegeSetOfStatement(cp)
		ses := newSes(priv, ctrl)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		err := InitProcedure(ses.GetTxnHandler().GetConnCtx(), ses, ses.GetTenantInfo(), cp)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("init precedure succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()
		cp := &tree.CreateProcedure{
			Name: tree.NewProcedureName("test_if_hit_elseif_first_elseif", tree.ObjectNamePrefix{}),
			Args: nil,
			Body: "'begin DECLARE v1 INT; SET v1 = 5; IF v1 > 5 THEN select * from tbh1; ELSEIF v1 = 5 THEN select * from tbh2; ELSEIF v1 = 4 THEN select * from tbh2 limit 1; ELSE select * from tbh3; END IF; end'",
		}

		priv := determinePrivilegeSetOfStatement(cp)
		ses := newSes(priv, ctrl)
		ses.SetDatabaseName("test_procedure")

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql := getSqlForCheckProcedureExistence(string(cp.Name.Name.ObjectName), ses.GetDatabaseName())
		mrs := newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := InitProcedure(ses.GetTxnHandler().GetConnCtx(), ses, ses.GetTenantInfo(), cp)
		convey.So(err, convey.ShouldBeNil)
	})
}
func TestDoSetSecondaryRoleAll(t *testing.T) {
	convey.Convey("do set secondary role succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.SetRole{
			SecondaryRole: false,
		}

		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		tenant := &TenantInfo{
			Tenant:        "test_account",
			User:          "test_user",
			DefaultRole:   "role1",
			TenantID:      3001,
			UserID:        3,
			DefaultRoleID: 5,
		}
		ses.SetTenantInfo(tenant)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql := getSqlForgetUserRolesExpectPublicRole(publicRoleID, ses.GetTenantInfo().UserID)
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{"6", "role5"},
		})
		bh.sql2result[sql] = mrs

		err := doSetSecondaryRoleAll(ses.GetTxnHandler().GetTxnCtx(), ses)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("do set secondary role succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.SetRole{
			SecondaryRole: false,
		}

		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		tenant := &TenantInfo{
			Tenant:        "test_account",
			User:          "test_user",
			DefaultRole:   "role1",
			TenantID:      3001,
			UserID:        3,
			DefaultRoleID: 5,
		}
		ses.SetTenantInfo(tenant)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql := getSqlForgetUserRolesExpectPublicRole(publicRoleID, ses.GetTenantInfo().UserID)
		mrs := newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := doSetSecondaryRoleAll(ses.GetTxnHandler().GetTxnCtx(), ses)
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestDoGrantPrivilegeImplicitly(t *testing.T) {
	convey.Convey("do grant privilege implicitly for create database succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.CreateDatabase{
			Name: tree.Identifier("abc"),
		}

		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		tenant := &TenantInfo{
			Tenant:        "test_account",
			User:          "test_user",
			DefaultRole:   "role1",
			TenantID:      3001,
			UserID:        3,
			DefaultRoleID: 5,
		}
		ses.SetTenantInfo(tenant)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql := getSqlForGrantOwnershipOnDatabase(string(stmt.Name), ses.GetTenantInfo().GetDefaultRole())
		mrs := newMrsForSqlForCheckUserHasRole([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := doGrantPrivilegeImplicitly(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("do grant privilege implicitly for create table succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.CreateTable{}

		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		tenant := &TenantInfo{
			Tenant:        "test_account",
			User:          "test_user",
			DefaultRole:   "role1",
			TenantID:      3001,
			UserID:        3,
			DefaultRoleID: 5,
		}
		ses.SetTenantInfo(tenant)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql := getSqlForGrantOwnershipOnTable("abd", "t1", ses.GetTenantInfo().GetDefaultRole())
		mrs := newMrsForSqlForCheckUserHasRole([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := doGrantPrivilegeImplicitly(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("do grant privilege implicitly for create database succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.CreateDatabase{
			Name: tree.Identifier("abc"),
		}

		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		tenant := &TenantInfo{
			Tenant:        "test_account",
			User:          "test_user",
			DefaultRole:   "",
			TenantID:      3001,
			UserID:        3,
			DefaultRoleID: 5,
		}
		ses.SetTenantInfo(tenant)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql := getSqlForGrantOwnershipOnDatabase(string(stmt.Name), ses.GetTenantInfo().GetDefaultRole())
		mrs := newMrsForSqlForCheckUserHasRole([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := doGrantPrivilegeImplicitly(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("do grant privilege implicitly for create table succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.CreateTable{}

		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		tenant := &TenantInfo{
			Tenant:        "test_account",
			User:          "test_user",
			DefaultRole:   "",
			TenantID:      3001,
			UserID:        3,
			DefaultRoleID: 5,
		}
		ses.SetTenantInfo(tenant)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql := getSqlForGrantOwnershipOnTable("abd", "t1", ses.GetTenantInfo().GetDefaultRole())
		mrs := newMrsForSqlForCheckUserHasRole([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := doGrantPrivilegeImplicitly(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestDoRevokePrivilegeImplicitly(t *testing.T) {
	convey.Convey("do revoke privilege implicitly for drop database succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.DropDatabase{
			Name: tree.Identifier("abc"),
		}

		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		tenant := &TenantInfo{
			Tenant:        "test_account",
			User:          "test_user",
			DefaultRole:   "role1",
			TenantID:      3001,
			UserID:        3,
			DefaultRoleID: 5,
		}
		ses.SetTenantInfo(tenant)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql := getSqlForRevokeOwnershipFromDatabase(string(stmt.Name), ses.GetTenantInfo().GetDefaultRole())
		mrs := newMrsForSqlForCheckUserHasRole([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := doRevokePrivilegeImplicitly(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("do grant privilege implicitly for drop table succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.DropTable{
			Names: tree.TableNames{
				tree.NewTableName(tree.Identifier("test1"), tree.ObjectNamePrefix{}, nil),
			},
		}

		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		tenant := &TenantInfo{
			Tenant:        "test_account",
			User:          "test_user",
			DefaultRole:   "role1",
			TenantID:      3001,
			UserID:        3,
			DefaultRoleID: 5,
		}
		ses.SetTenantInfo(tenant)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql := getSqlForRevokeOwnershipFromTable("abd", "t1", ses.GetTenantInfo().GetDefaultRole())
		mrs := newMrsForSqlForCheckUserHasRole([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := doRevokePrivilegeImplicitly(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("do revoke privilege implicitly for drop database succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.DropDatabase{
			Name: tree.Identifier("abc"),
		}

		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		tenant := &TenantInfo{
			Tenant:        "test_account",
			User:          "test_user",
			DefaultRole:   "",
			TenantID:      3001,
			UserID:        3,
			DefaultRoleID: 5,
		}
		ses.SetTenantInfo(tenant)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql := getSqlForRevokeOwnershipFromDatabase(string(stmt.Name), ses.GetTenantInfo().GetDefaultRole())
		mrs := newMrsForSqlForCheckUserHasRole([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := doRevokePrivilegeImplicitly(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("do grant privilege implicitly for drop table succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.DropTable{
			Names: tree.TableNames{
				tree.NewTableName(tree.Identifier("test1"), tree.ObjectNamePrefix{}, nil),
			},
		}

		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		tenant := &TenantInfo{
			Tenant:        "test_account",
			User:          "test_user",
			DefaultRole:   "",
			TenantID:      3001,
			UserID:        3,
			DefaultRoleID: 5,
		}
		ses.SetTenantInfo(tenant)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql := getSqlForRevokeOwnershipFromTable("abd", "t1", ses.GetTenantInfo().GetDefaultRole())
		mrs := newMrsForSqlForCheckUserHasRole([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := doRevokePrivilegeImplicitly(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
	})

}

func TestDoGetGlobalSystemVariable(t *testing.T) {
	convey.Convey("get global system variable succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.ShowVariables{
			Global: true,
		}

		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql := getSystemVariablesWithAccount(uint64(ses.GetTenantInfo().GetTenantID()))
		mrs := newMrsForSqlForCheckUserHasRole([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err := doGetGlobalSystemVariable(ses.GetTxnHandler().GetTxnCtx(), ses)
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestDoSetGlobalSystemVariable(t *testing.T) {
	convey.Convey("set global system variable succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.SetVar{
			Assignments: []*tree.VarAssignmentExpr{
				{
					System: true,
					Global: true,
					Name:   "sql_mode",
					Value:  tree.NewStrVal(""),
				},
			},
		}

		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql := getSqlForUpdateSystemVariableValue(getVariableValue(stmt.Assignments[0].Value), uint64(ses.GetTenantInfo().GetTenantID()), stmt.Assignments[0].Name)
		mrs := newMrsForSqlForCheckUserHasRole([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := doSetGlobalSystemVariable(ses.GetTxnHandler().GetTxnCtx(), ses, stmt.Assignments[0].Name, stmt.Assignments[0].Value)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("set global system variable succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.SetVar{
			Assignments: []*tree.VarAssignmentExpr{
				{
					System: true,
					Global: true,
					Name:   "sql_mode",
					Value:  tree.NewStrVal("NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"),
				},
			},
		}

		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql := getSqlForUpdateSystemVariableValue(getVariableValue(stmt.Assignments[0].Value), uint64(ses.GetTenantInfo().GetTenantID()), stmt.Assignments[0].Name)
		mrs := newMrsForSqlForCheckUserHasRole([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := doSetGlobalSystemVariable(ses.GetTxnHandler().GetTxnCtx(), ses, stmt.Assignments[0].Name, stmt.Assignments[0].Value)
		convey.So(err, convey.ShouldBeNil)
	})
}

func boxExprStr(s string) tree.Expr {
	return tree.NewNumValWithType(constant.MakeString(s), s, false, tree.P_char)
}

func mustUnboxExprStr(e tree.Expr) string {
	if e == nil {
		return ""
	}
	return e.(*tree.NumVal).OrigString()
}

func Test_doAlterUser(t *testing.T) {

	alterUserFrom := func(stmt *tree.AlterUser) *alterUser {
		au := &alterUser{}
		for _, su := range stmt.Users {
			u := &user{
				Username: su.Username,
				Hostname: su.Hostname,
			}
			if su.AuthOption != nil {
				u.AuthExist = true
				u.IdentTyp = su.AuthOption.Typ
				u.IdentStr = mustUnboxExprStr(su.AuthOption.Str)
			}
			au.Users = append(au.Users, u)
		}
		return au
	}

	convey.Convey("alter user success", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.AlterUser{
			Users: []*tree.User{
				{Username: "u1", Hostname: "%", AuthOption: &tree.AccountIdentified{Typ: tree.AccountIdentifiedByPassword, Str: boxExprStr("123456")}},
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx)
		ses.rm = rm

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		for i, user := range stmt.Users {
			sql, _ := getSqlForPasswordOfUser(context.TODO(), user.Username)
			mrs := newMrsForPasswordOfUser([][]interface{}{
				{i, "111", 0},
			})
			bh.sql2result[sql] = mrs

			sql, _ = getSqlForCheckUserHasRole(context.TODO(), "root", moAdminRoleID)
			mrs = newMrsForSqlForCheckUserHasRole([][]interface{}{
				{0, 0},
			})
			bh.sql2result[sql] = mrs
		}

		for _, user := range stmt.Users {
			sql, _ := getSqlForUpdatePasswordOfUser(context.TODO(), mustUnboxExprStr(user.AuthOption.Str), user.Username)
			bh.sql2result[sql] = nil
		}

		err := doAlterUser(ctx, ses, alterUserFrom(stmt))
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("alter user fail for alter multi user", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.AlterUser{
			Users: []*tree.User{
				{Username: "u1", Hostname: "%", AuthOption: &tree.AccountIdentified{Typ: tree.AccountIdentifiedByPassword, Str: boxExprStr("123456")}},
				{Username: "u2", Hostname: "%", AuthOption: &tree.AccountIdentified{Typ: tree.AccountIdentifiedByPassword, Str: boxExprStr("123456")}},
				{Username: "u3", Hostname: "%", AuthOption: &tree.AccountIdentified{Typ: tree.AccountIdentifiedByPassword, Str: boxExprStr("123456")}},
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx)
		ses.rm = rm

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		for i, user := range stmt.Users {
			sql, _ := getSqlForPasswordOfUser(context.TODO(), user.Username)
			mrs := newMrsForPasswordOfUser([][]interface{}{
				{i, "111", "public"},
			})
			bh.sql2result[sql] = mrs

			sql, _ = getSqlForCheckUserHasRole(context.TODO(), user.Username, moAdminRoleID)
			mrs = newMrsForSqlForCheckUserHasRole([][]interface{}{})
			bh.sql2result[sql] = mrs
		}

		for _, user := range stmt.Users {
			sql, _ := getSqlForUpdatePasswordOfUser(context.TODO(), mustUnboxExprStr(user.AuthOption.Str), user.Username)
			bh.sql2result[sql] = nil
		}

		err := doAlterUser(ctx, ses, alterUserFrom(stmt))
		convey.So(err, convey.ShouldBeError)
	})

	convey.Convey("alter user fail for privilege", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.AlterUser{
			Users: []*tree.User{
				{Username: "u1", Hostname: "%", AuthOption: &tree.AccountIdentified{Typ: tree.AccountIdentifiedByPassword, Str: boxExprStr("123456")}},
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx)
		ses.rm = rm

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		for i, user := range stmt.Users {
			sql, _ := getSqlForPasswordOfUser(context.TODO(), user.Username)
			mrs := newMrsForPasswordOfUser([][]interface{}{
				{i, "111", "public"},
			})
			bh.sql2result[sql] = mrs

			sql, _ = getSqlForCheckUserHasRole(context.TODO(), user.Username, moAdminRoleID)
			mrs = newMrsForSqlForCheckUserHasRole([][]interface{}{})
			bh.sql2result[sql] = mrs
		}

		for _, user := range stmt.Users {
			sql, _ := getSqlForUpdatePasswordOfUser(context.TODO(), mustUnboxExprStr(user.AuthOption.Str), user.Username)
			bh.sql2result[sql] = nil
		}

		err := doAlterUser(ctx, ses, alterUserFrom(stmt))
		convey.So(err, convey.ShouldBeError)
	})
}

func Test_doAlterAccount(t *testing.T) {
	alterAcountFromStmt := func(stmt *tree.AlterAccount) *alterAccount {
		aa := &alterAccount{
			IfExists:     stmt.IfExists,
			AuthExist:    stmt.AuthOption.Exist,
			StatusOption: stmt.StatusOption,
			Comment:      stmt.Comment,
		}
		aa.Name = mustUnboxExprStr(stmt.Name)
		if stmt.AuthOption.Exist {
			aa.AdminName = mustUnboxExprStr(stmt.AuthOption.AdminName)
			aa.IdentTyp = stmt.AuthOption.IdentifiedType.Typ
			aa.IdentStr = mustUnboxExprStr(stmt.AuthOption.IdentifiedType.Str)
		}
		return aa
	}

	convey.Convey("alter account (auth_option) succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.AlterAccount{
			Name: boxExprStr("acc"),
			AuthOption: tree.AlterAccountAuthOption{
				Exist:     true,
				AdminName: boxExprStr("rootx"),
				IdentifiedType: tree.AccountIdentified{
					Typ: tree.AccountIdentifiedByPassword,
					Str: boxExprStr("111"),
				},
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx)
		ses.rm = rm

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckTenant(context.TODO(), mustUnboxExprStr(stmt.Name))
		mrs := newMrsForCheckTenant([][]interface{}{
			{0, 0, 0, 0},
		})
		bh.sql2result[sql] = mrs

		sql, _ = getSqlForPasswordOfUser(context.TODO(), mustUnboxExprStr(stmt.AuthOption.AdminName))
		bh.sql2result[sql] = newMrsForPasswordOfUser([][]interface{}{
			{10, "111", 0},
		})

		sql, _ = getSqlForUpdatePasswordOfUser(context.TODO(), mustUnboxExprStr(stmt.AuthOption.IdentifiedType.Str), mustUnboxExprStr(stmt.AuthOption.AdminName))
		bh.sql2result[sql] = nil

		err := doAlterAccount(ses.GetTxnHandler().GetTxnCtx(), ses, alterAcountFromStmt(stmt))
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("alter account (auth_option) failed (wrong identifiedBy)", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.AlterAccount{
			Name: boxExprStr("acc"),
			AuthOption: tree.AlterAccountAuthOption{
				Exist:     true,
				AdminName: boxExprStr("rootx"),
				IdentifiedType: tree.AccountIdentified{
					Typ: tree.AccountIdentifiedByRandomPassword,
					Str: boxExprStr("111"),
				},
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx)
		ses.rm = rm

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckTenant(context.TODO(), mustUnboxExprStr(stmt.Name))
		mrs := newMrsForCheckTenant([][]interface{}{
			{0, 0, 0, 0},
		})
		bh.sql2result[sql] = mrs

		sql, _ = getSqlForPasswordOfUser(context.TODO(), mustUnboxExprStr(stmt.AuthOption.AdminName))
		bh.sql2result[sql] = newMrsForPasswordOfUser([][]interface{}{
			{10, "111", 0},
		})

		sql, _ = getSqlForUpdatePasswordOfUser(context.TODO(), mustUnboxExprStr(stmt.AuthOption.IdentifiedType.Str), mustUnboxExprStr(stmt.AuthOption.AdminName))
		bh.sql2result[sql] = nil

		err := doAlterAccount(ses.GetTxnHandler().GetTxnCtx(), ses, alterAcountFromStmt(stmt))
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("alter account (auth_option) failed (no account)", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.AlterAccount{
			Name: boxExprStr("acc"),
			AuthOption: tree.AlterAccountAuthOption{
				Exist:     true,
				AdminName: boxExprStr("rootx"),
				IdentifiedType: tree.AccountIdentified{
					Typ: tree.AccountIdentifiedByRandomPassword,
					Str: boxExprStr("111"),
				},
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx)
		ses.rm = rm

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckTenant(context.TODO(), mustUnboxExprStr(stmt.Name))
		bh.sql2result[sql] = nil

		sql, _ = getSqlForPasswordOfUser(context.TODO(), mustUnboxExprStr(stmt.AuthOption.AdminName))
		bh.sql2result[sql] = nil

		sql, _ = getSqlForUpdatePasswordOfUser(context.TODO(), mustUnboxExprStr(stmt.AuthOption.IdentifiedType.Str), mustUnboxExprStr(stmt.AuthOption.AdminName))
		bh.sql2result[sql] = nil

		err := doAlterAccount(ses.GetTxnHandler().GetTxnCtx(), ses, alterAcountFromStmt(stmt))
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("alter account (auth_option) succ (no account, if exists)", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.AlterAccount{
			IfExists: true,
			Name:     boxExprStr("acc"),
			AuthOption: tree.AlterAccountAuthOption{
				Exist:     true,
				AdminName: boxExprStr("rootx"),
				IdentifiedType: tree.AccountIdentified{
					Typ: tree.AccountIdentifiedByPassword,
					Str: boxExprStr("111"),
				},
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx)
		ses.rm = rm

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckTenant(context.TODO(), mustUnboxExprStr(stmt.Name))
		mrs := newMrsForCheckTenant([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql, _ = getSqlForPasswordOfUser(context.TODO(), mustUnboxExprStr(stmt.AuthOption.AdminName))
		bh.sql2result[sql] = nil

		sql, _ = getSqlForUpdatePasswordOfUser(context.TODO(), mustUnboxExprStr(stmt.AuthOption.IdentifiedType.Str), mustUnboxExprStr(stmt.AuthOption.AdminName))
		bh.sql2result[sql] = nil

		err := doAlterAccount(ses.GetTxnHandler().GetTxnCtx(), ses, alterAcountFromStmt(stmt))
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("alter account (auth_option) failed (has account,if exists, no user)", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.AlterAccount{
			IfExists: true,
			Name:     boxExprStr("acc"),
			AuthOption: tree.AlterAccountAuthOption{
				Exist:     true,
				AdminName: boxExprStr("rootx"),
				IdentifiedType: tree.AccountIdentified{
					Typ: tree.AccountIdentifiedByPassword,
					Str: boxExprStr("111"),
				},
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx)
		ses.rm = rm

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckTenant(context.TODO(), mustUnboxExprStr(stmt.Name))
		mrs := newMrsForCheckTenant([][]interface{}{
			{0, "0", "open", 0},
		})
		bh.sql2result[sql] = mrs

		sql, _ = getSqlForPasswordOfUser(context.TODO(), mustUnboxExprStr(stmt.AuthOption.AdminName))
		bh.sql2result[sql] = newMrsForPasswordOfUser([][]interface{}{})

		sql, _ = getSqlForUpdatePasswordOfUser(context.TODO(), mustUnboxExprStr(stmt.AuthOption.IdentifiedType.Str), mustUnboxExprStr(stmt.AuthOption.AdminName))
		bh.sql2result[sql] = newMrsForCheckTenant([][]interface{}{
			{0, 0, 0, 0},
		})

		err := doAlterAccount(ses.GetTxnHandler().GetTxnCtx(), ses, alterAcountFromStmt(stmt))
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("alter account (auth_option) failed (no option)", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.AlterAccount{
			IfExists: true,
			Name:     boxExprStr("acc"),
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx)
		ses.rm = rm

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckTenant(context.TODO(), mustUnboxExprStr(stmt.Name))
		bh.sql2result[sql] = nil

		sql, _ = getSqlForPasswordOfUser(context.TODO(), mustUnboxExprStr(stmt.AuthOption.AdminName))
		bh.sql2result[sql] = nil

		sql, _ = getSqlForUpdatePasswordOfUser(context.TODO(), mustUnboxExprStr(stmt.AuthOption.IdentifiedType.Str), mustUnboxExprStr(stmt.AuthOption.AdminName))
		bh.sql2result[sql] = nil

		err := doAlterAccount(ses.GetTxnHandler().GetTxnCtx(), ses, alterAcountFromStmt(stmt))
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("alter account (auth_option) failed (two options)", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.AlterAccount{
			IfExists: true,
			Name:     boxExprStr("acc"),
			AuthOption: tree.AlterAccountAuthOption{
				Exist:     true,
				AdminName: boxExprStr("rootx"),
				IdentifiedType: tree.AccountIdentified{
					Typ: tree.AccountIdentifiedByPassword,
					Str: boxExprStr("111"),
				},
			},
			StatusOption: tree.AccountStatus{
				Exist:  true,
				Option: tree.AccountStatusOpen,
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx)
		ses.rm = rm

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckTenant(context.TODO(), mustUnboxExprStr(stmt.Name))
		bh.sql2result[sql] = nil
		sql, _ = getSqlForPasswordOfUser(context.TODO(), mustUnboxExprStr(stmt.AuthOption.AdminName))
		bh.sql2result[sql] = nil

		sql, _ = getSqlForUpdatePasswordOfUser(context.TODO(), mustUnboxExprStr(stmt.AuthOption.IdentifiedType.Str), mustUnboxExprStr(stmt.AuthOption.AdminName))
		bh.sql2result[sql] = nil

		err := doAlterAccount(ses.GetTxnHandler().GetTxnCtx(), ses, alterAcountFromStmt(stmt))
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("alter account (auth_option) succ Comments", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.AlterAccount{
			Name: boxExprStr("acc"),
			Comment: tree.AccountComment{
				Exist:   true,
				Comment: "new account",
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx)
		ses.rm = rm

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckTenant(context.TODO(), mustUnboxExprStr(stmt.Name))
		mrs := newMrsForCheckTenant([][]interface{}{
			{0, 0, 0, 0},
		})
		bh.sql2result[sql] = mrs

		sql, _ = getSqlForPasswordOfUser(context.TODO(), mustUnboxExprStr(stmt.AuthOption.AdminName))
		bh.sql2result[sql] = nil

		sql, _ = getSqlForUpdateCommentsOfAccount(context.TODO(), stmt.Comment.Comment, mustUnboxExprStr(stmt.Name))
		bh.sql2result[sql] = nil

		err := doAlterAccount(ses.GetTxnHandler().GetTxnCtx(), ses, alterAcountFromStmt(stmt))
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("alter account (auth_option) succ Status", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.AlterAccount{
			Name: boxExprStr("acc"),
			StatusOption: tree.AccountStatus{
				Exist:  true,
				Option: tree.AccountStatusSuspend,
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx)
		ses.rm = rm

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckTenant(context.TODO(), mustUnboxExprStr(stmt.Name))
		mrs := newMrsForCheckTenant([][]interface{}{
			{0, 0, 0, 0},
		})
		bh.sql2result[sql] = mrs

		sql, _ = getSqlForPasswordOfUser(context.TODO(), mustUnboxExprStr(stmt.AuthOption.AdminName))
		bh.sql2result[sql] = nil

		sql, _ = getSqlForUpdateStatusOfAccount(context.TODO(), stmt.StatusOption.Option.String(), types.CurrentTimestamp().String2(time.UTC, 0), mustUnboxExprStr(stmt.Name))
		bh.sql2result[sql] = nil

		err := doAlterAccount(ses.GetTxnHandler().GetTxnCtx(), ses, alterAcountFromStmt(stmt))
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("alter account (status_option) fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.AlterAccount{
			Name: boxExprStr("sys"),
			StatusOption: tree.AccountStatus{
				Exist:  true,
				Option: tree.AccountStatusSuspend,
			},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx)
		ses.rm = rm

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckTenant(context.TODO(), mustUnboxExprStr(stmt.Name))
		bh.sql2result[sql] = nil

		sql, _ = getSqlForPasswordOfUser(context.TODO(), mustUnboxExprStr(stmt.AuthOption.AdminName))
		bh.sql2result[sql] = nil

		sql, _ = getSqlForUpdateStatusOfAccount(context.TODO(), stmt.StatusOption.Option.String(), types.CurrentTimestamp().String2(time.UTC, 0), mustUnboxExprStr(stmt.Name))
		bh.sql2result[sql] = nil

		err := doAlterAccount(ses.GetTxnHandler().GetTxnCtx(), ses, alterAcountFromStmt(stmt))
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func newMrsForShowTables(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col2 := &MysqlColumn{}
	col2.SetName("table")
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mrs.AddColumn(col2)

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return mrs
}

func Test_doDropAccount(t *testing.T) {
	convey.Convey("drop account", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.DropAccount{
			Name: boxExprStr("acc"),
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx)
		ses.rm = rm

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckTenant(context.TODO(), mustUnboxExprStr(stmt.Name))
		mrs := newMrsForCheckTenant([][]interface{}{
			{0, "0", "open", 0},
		})
		bh.sql2result[sql] = mrs

		sql, _ = getSqlForDeleteAccountFromMoAccount(context.TODO(), mustUnboxExprStr(stmt.Name))
		bh.sql2result[sql] = nil

		for _, sql = range getSqlForDropAccount() {
			bh.sql2result[sql] = nil
		}

		sql = "show databases;"
		bh.sql2result[sql] = newMrsForSqlForShowDatabases([][]interface{}{})

		bh.sql2result["show tables from mo_catalog;"] = newMrsForShowTables([][]interface{}{})

		err := doDropAccount(ses.GetTxnHandler().GetTxnCtx(), ses, &dropAccount{
			IfExists: stmt.IfExists,
			Name:     mustUnboxExprStr(stmt.Name),
		})
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("drop account (if exists)", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.DropAccount{
			IfExists: true,
			Name:     boxExprStr("acc"),
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx)
		ses.rm = rm

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckTenant(context.TODO(), mustUnboxExprStr(stmt.Name))
		mrs := newMrsForCheckTenant([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql, _ = getSqlForDeleteAccountFromMoAccount(context.TODO(), mustUnboxExprStr(stmt.Name))
		bh.sql2result[sql] = nil

		for _, sql = range getSqlForDropAccount() {
			bh.sql2result[sql] = nil
		}

		bh.sql2result["show tables from mo_catalog;"] = newMrsForShowTables([][]interface{}{})

		err := doDropAccount(ses.GetTxnHandler().GetTxnCtx(), ses, &dropAccount{
			IfExists: stmt.IfExists,
			Name:     mustUnboxExprStr(stmt.Name),
		})
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("drop account fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.DropAccount{
			Name: boxExprStr("acc"),
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx)
		ses.rm = rm

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckTenant(context.TODO(), mustUnboxExprStr(stmt.Name))
		mrs := newMrsForCheckTenant([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql, _ = getSqlForDeleteAccountFromMoAccount(context.TODO(), mustUnboxExprStr(stmt.Name))
		bh.sql2result[sql] = nil

		for _, sql = range getSqlForDropAccount() {
			bh.sql2result[sql] = nil
		}

		err := doDropAccount(ses.GetTxnHandler().GetTxnCtx(), ses, &dropAccount{
			IfExists: stmt.IfExists,
			Name:     mustUnboxExprStr(stmt.Name),
		})
		convey.So(err, convey.ShouldBeError)
	})
}

func generateGrantPrivilege(grant, to string, exists bool, roleNames []string, withGrantOption bool) {
	names := ""
	for i, name := range roleNames {
		if i > 0 {
			names += ","
		}
		names += name
	}
	levels := make(map[objectType][]privilegeLevelType)
	levels[objectTypeTable] = []privilegeLevelType{
		privilegeLevelStar,
		privilegeLevelStarStar,
		privilegeLevelDatabaseStar,
		privilegeLevelDatabaseTable,
		privilegeLevelTable,
	}
	levels[objectTypeDatabase] = []privilegeLevelType{
		privilegeLevelStar,
		privilegeLevelStarStar,
		privilegeLevelDatabase,
	}
	levels[objectTypeAccount] = []privilegeLevelType{
		privilegeLevelStar,
	}
	for i := PrivilegeTypeCreateAccount; i <= PrivilegeTypeExecute; i++ {
		switch i {
		case PrivilegeTypeCreateObject, PrivilegeTypeDropObject, PrivilegeTypeAlterObject:
			continue
		}
		for j := objectTypeDatabase; j <= objectTypeAccount; j++ {
			switch i.Scope() {
			case PrivilegeScopeSys, PrivilegeScopeAccount, PrivilegeScopeUser, PrivilegeScopeRole:
				if j != objectTypeAccount {
					continue
				}
			case PrivilegeScopeDatabase:
				if j != objectTypeDatabase {
					continue
				}
			case PrivilegeScopeTable:
				if j != objectTypeTable {
					continue
				}
			case PrivilegeScopeRoutine:
				if j != objectTypeFunction {
					continue
				}
			}
			if j == objectTypeFunction {
				continue
			}

			for _, k := range levels[j] {
				bb := bytes.Buffer{}
				bb.WriteString(grant)
				if exists {
					bb.WriteString(" ")
					bb.WriteString("if exists")
				}

				bb.WriteString(" ")
				s := fmt.Sprintf("%v on %v %v", i, j, k)
				bb.WriteString(s)
				bb.WriteString(" ")
				bb.WriteString(to)
				bb.WriteString(" ")
				bb.WriteString(names)
				if withGrantOption {
					bb.WriteString(" with grant option")
				}
				bb.WriteString(";")
				convey.So(len(s) != 0, convey.ShouldBeTrue)
				//fmt.Println(bb.String())
			}
		}
	}
}

func Test_generateGrantPrivilege(t *testing.T) {
	convey.Convey("grant privilege combination", t, func() {
		generateGrantPrivilege("grant", "to", false, []string{"role_r1"}, false)
	})
}

func Test_generateRevokePrivilege(t *testing.T) {
	convey.Convey("grant privilege combination", t, func() {
		generateGrantPrivilege("revoke", "from", true, []string{"role_r1", "rx"}, false)
		generateGrantPrivilege("revoke", "from", false, []string{"role_r1", "rx"}, false)
	})
}

func Test_Name(t *testing.T) {
	convey.Convey("test", t, func() {
		type arg struct {
			input string
			want  string
		}

		args := []arg{
			{" abc ", "abc"},
		}

		for _, a := range args {
			ret, _ := normalizeName(context.TODO(), a.input)
			convey.So(ret == a.want, convey.ShouldBeTrue)
		}
	})

	convey.Convey("test2", t, func() {
		type arg struct {
			input string
			want  bool
		}

		args := []arg{
			{"abc", false},
			{"a:bc", true},
			{"  a:bc  ", true},
		}

		for _, a := range args {
			ret := nameIsInvalid(a.input)
			convey.So(ret == a.want, convey.ShouldBeTrue)
		}
	})
}

func genRevokeCases1(A [][]string, path []string, cur int, exists bool, out *[]string) {
	if cur == len(A) {
		bb := bytes.Buffer{}
		bb.WriteString("revoke ")
		if exists {
			bb.WriteString("if exists ")
		}
		bb.WriteString(path[0])
		bb.WriteString(",")
		bb.WriteString(path[1])
		bb.WriteString(" ")
		bb.WriteString("from ")
		bb.WriteString(path[2])
		bb.WriteString(",")
		bb.WriteString(path[3])
		bb.WriteString(";")
		*out = append(*out, bb.String())
	} else {
		for i := 0; i < len(A[cur]); i++ {
			path[cur] = A[cur][i]
			genRevokeCases1(A, path, cur+1, exists, out)
		}
	}
}

func Test_genRevokeCases(t *testing.T) {
	A := [][]string{
		{"r1", "role_r1"},
		{"r2", "role_r2"},
		{"u1", "role_u1"},
		{"u2", "role_u2"},
	}
	Path := []string{"", "", "", ""}
	Out := []string{}
	genRevokeCases1(A, Path, 0, true, &Out)
	genRevokeCases1(A, Path, 0, false, &Out)
	for _, s := range Out {
		fmt.Println(s)
	}
}

func newSes(priv *privilege, ctrl *gomock.Controller) *Session {
	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	pu.SV.SetDefaultValues()
	setGlobalPu(pu)

	ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
	ctx = defines.AttachAccountId(ctx, 0)
	ioses := mock_frontend.NewMockIOSession(ctrl)
	ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
	ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
	ioses.EXPECT().Ref().AnyTimes()
	proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)

	ses := NewSession(ctx, proto, nil, GSysVariables, true, nil)
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

	rm, _ := NewRoutineManager(ctx)
	rm.baseService = new(MockBaseService)
	ses.rm = rm

	return ses
}

var _ BaseService = &MockBaseService{}

type MockBaseService struct {
}

func (m *MockBaseService) ID() string {
	//TODO implement me
	panic("implement me")
}

func (m *MockBaseService) SQLAddress() string {
	//TODO implement me
	panic("implement me")
}

func (m *MockBaseService) SessionMgr() *queryservice.SessionManager {
	//TODO implement me
	panic("implement me")
}

func (m *MockBaseService) CheckTenantUpgrade(ctx context.Context, tenantID int64) error {
	//TODO implement me
	panic("implement me")
}

func (m *MockBaseService) GetFinalVersion() string {
	return "1.2.0"
}

func (s *MockBaseService) UpgradeTenant(ctx context.Context, tenantName string, retryCount uint32, isALLAccount bool) error {
	//TODO implement me
	panic("implement me")
}

func newBh(ctrl *gomock.Controller, sql2result map[string]ExecResult) BackgroundExec {
	var currentSql string
	bh := mock_frontend.NewMockBackgroundExec(ctrl)
	bh.EXPECT().ClearExecResultSet().AnyTimes()
	bh.EXPECT().Close().Return().AnyTimes()
	bh.EXPECT().Exec(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, sql string) error {
		currentSql = sql
		return nil
	}).AnyTimes()
	bh.EXPECT().GetExecResultSet().DoAndReturn(func() []interface{} {
		return []interface{}{sql2result[currentSql]}
	}).AnyTimes()
	return bh
}

type backgroundExecTest struct {
	currentSql string
	sql2result map[string]ExecResult
}

func (bt *backgroundExecTest) ExecStmt(ctx context.Context, statement tree.Statement) error {
	//TODO implement me
	panic("implement me")
}

func (bt *backgroundExecTest) GetExecResultBatches() []*batch.Batch {
	//TODO implement me
	panic("implement me")
}

func (bt *backgroundExecTest) ClearExecResultBatches() {
	//TODO implement me
	panic("implement me")
}

func (bt *backgroundExecTest) init() {
	bt.sql2result = make(map[string]ExecResult)
}

func (bt *backgroundExecTest) Close() {
}

func (bt *backgroundExecTest) Clear() {}

func (bt *backgroundExecTest) Exec(ctx context.Context, s string) error {
	bt.currentSql = s
	return nil
}

func (bt *backgroundExecTest) ExecRestore(context.Context, string, uint32, uint32) error {
	panic("unimplement")
}

func (bt *backgroundExecTest) GetExecResultSet() []interface{} {
	return []interface{}{bt.sql2result[bt.currentSql]}
}

func (bt *backgroundExecTest) ClearExecResultSet() {
	//bt.init()
}

var _ BackgroundExec = &backgroundExecTest{}

func newMrsForSqlForShowDatabases(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("Database")
	col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mrs.AddColumn(col1)

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return mrs
}

func newMrsForSqlForCheckUserHasRole(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("user_id")
	col1.SetColumnType(defines.MYSQL_TYPE_LONGLONG)

	col2 := &MysqlColumn{}
	col2.SetName("role_id")
	col2.SetColumnType(defines.MYSQL_TYPE_LONGLONG)

	mrs.AddColumn(col1)
	mrs.AddColumn(col2)

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return mrs
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

func newMrsForGetAllStuffRoleGrant(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("granted_id")
	col1.SetColumnType(defines.MYSQL_TYPE_LONGLONG)

	col2 := &MysqlColumn{}
	col2.SetName("grantee_id")
	col2.SetColumnType(defines.MYSQL_TYPE_LONGLONG)

	col3 := &MysqlColumn{}
	col3.SetName("with_grant_option")
	col3.SetColumnType(defines.MYSQL_TYPE_BOOL)

	mrs.AddColumn(col1)
	mrs.AddColumn(col2)
	mrs.AddColumn(col3)

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return mrs
}

func newMrsForCheckRoleGrant(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("granted_id")
	col1.SetColumnType(defines.MYSQL_TYPE_LONGLONG)

	col2 := &MysqlColumn{}
	col2.SetName("grantee_id")
	col2.SetColumnType(defines.MYSQL_TYPE_LONGLONG)

	col3 := &MysqlColumn{}
	col3.SetName("with_grant_option")
	col3.SetColumnType(defines.MYSQL_TYPE_BOOL)

	mrs.AddColumn(col1)
	mrs.AddColumn(col2)
	mrs.AddColumn(col3)

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return mrs
}

func newMrsForPasswordOfUser(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("user_id")
	col1.SetColumnType(defines.MYSQL_TYPE_LONGLONG)

	col2 := &MysqlColumn{}
	col2.SetName("authentication_string")
	col2.SetColumnType(defines.MYSQL_TYPE_LONGLONG)

	col3 := &MysqlColumn{}
	col3.SetName("default_role")
	col3.SetColumnType(defines.MYSQL_TYPE_BOOL)

	mrs.AddColumn(col1)
	mrs.AddColumn(col2)
	mrs.AddColumn(col3)

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return mrs
}

func newMrsForCheckUserGrant(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("role_id")
	col1.SetColumnType(defines.MYSQL_TYPE_LONGLONG)

	col2 := &MysqlColumn{}
	col2.SetName("user_id")
	col2.SetColumnType(defines.MYSQL_TYPE_LONGLONG)

	col3 := &MysqlColumn{}
	col3.SetName("with_grant_option")
	col3.SetColumnType(defines.MYSQL_TYPE_BOOL)

	mrs.AddColumn(col1)
	mrs.AddColumn(col2)
	mrs.AddColumn(col3)

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return mrs
}

func newMrsForRoleOfUser(rows [][]interface{}) *MysqlResultSet {
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

func newMrsForCheckDatabase(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("dat_id")
	col1.SetColumnType(defines.MYSQL_TYPE_LONGLONG)

	mrs.AddColumn(col1)

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return mrs
}

func newMrsForCheckDatabaseTable(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("rel_id")
	col1.SetColumnType(defines.MYSQL_TYPE_LONGLONG)

	mrs.AddColumn(col1)

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return mrs
}

func newMrsForCheckTenant(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("account_id")
	col1.SetColumnType(defines.MYSQL_TYPE_LONGLONG)

	col2 := &MysqlColumn{}
	col2.SetName("account_name")
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	col3 := &MysqlColumn{}
	col3.SetName("status")
	col3.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	col4 := &MysqlColumn{}
	col4.SetName("version")
	col4.SetColumnType(defines.MYSQL_TYPE_LONG)

	mrs.AddColumn(col1)
	mrs.AddColumn(col2)
	mrs.AddColumn(col3)
	mrs.AddColumn(col4)

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return mrs
}

func makeRowsOfMoRole(sql2result map[string]ExecResult, roleNames []string, rows [][][]interface{}) {
	for i, name := range roleNames {
		sql, _ := getSqlForRoleIdOfRole(context.TODO(), name)
		sql2result[sql] = newMrsForRoleIdOfRole(rows[i])
	}
}

func makeRowsOfMoUserGrant(sql2result map[string]ExecResult, userId int, rows [][]interface{}) {
	sql2result[getSqlForRoleIdOfUserId(userId)] = newMrsForRoleIdOfUserId(rows)
}

func makeRowsOfMoRolePrivs(sql2result map[string]ExecResult, roleIds []int, entries []privilegeEntry, rowsOfMoRolePrivs [][]interface{}) {
	for _, roleId := range roleIds {
		for _, entry := range entries {
			sql, _ := getSqlFromPrivilegeEntry(context.TODO(), int64(roleId), entry)
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

func newMrsForWithGrantOptionPrivilege(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("privilege_id")
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

func newMrsForRoleWGO(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("grantee_id")
	col1.SetColumnType(defines.MYSQL_TYPE_LONGLONG)

	mrs.AddColumn(col1)

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return mrs
}

func newMrsForPrivilegeWGO(rows [][]interface{}) *MysqlResultSet {
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

func newMrsForSystemVariablesOfAccount(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("variable_name")
	col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	col2 := &MysqlColumn{}
	col2.SetName("variable_value")
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	mrs.AddColumn(col1)
	mrs.AddColumn(col2)

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return mrs
}

func makeRowsOfWithGrantOptionPrivilege(sql2result map[string]ExecResult, sql string, rows [][]interface{}) {
	sql2result[sql] = newMrsForWithGrantOptionPrivilege(rows)
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

func makeSql2ExecResult2(userId int, rowsOfMoUserGrant [][]interface{}, roleIdsInMoRolePrivs []int, entries []privilegeEntry, rowsOfMoRolePrivs [][][][]interface{}, roleIdsInMoRoleGrant []int, rowsOfMoRoleGrant [][][]interface{}, grantedIds []int, granteeRows [][][]interface{}) map[string]ExecResult {
	sql2result := make(map[string]ExecResult)
	makeRowsOfMoUserGrant(sql2result, userId, rowsOfMoUserGrant)
	for i, roleId := range roleIdsInMoRolePrivs {
		for j, entry := range entries {
			sql, _ := getSqlFromPrivilegeEntry(context.TODO(), int64(roleId), entry)
			sql2result[sql] = newMrsForCheckRoleHasPrivilege(rowsOfMoRolePrivs[i][j])
		}
	}

	for i, roleId := range roleIdsInMoRoleGrant {
		sql := getSqlForInheritedRoleIdOfRoleId(int64(roleId))
		sql2result[sql] = newMrsForInheritedRoleIdOfRoleId(rowsOfMoRoleGrant[i])
	}

	for i, id := range grantedIds {
		sql := getSqlForCheckRoleGrantWGO(int64(id))
		sql2result[sql] = newMrsForRoleWGO(granteeRows[i])
	}

	return sql2result
}

func Test_graph(t *testing.T) {
	convey.Convey("create graph", t, func() {
		g := NewGraph()

		g.addEdge(1, 2)
		g.addEdge(2, 3)
		g.addEdge(3, 4)

		convey.So(g.hasLoop(1), convey.ShouldBeFalse)

		g2 := NewGraph()
		g2.addEdge(1, 2)
		g2.addEdge(2, 3)
		g2.addEdge(3, 4)
		e1 := g2.addEdge(4, 1)

		convey.So(g2.hasLoop(1), convey.ShouldBeTrue)

		g2.removeEdge(e1)
		convey.So(g2.hasLoop(1), convey.ShouldBeFalse)

		g2.addEdge(4, 1)
		convey.So(g2.hasLoop(1), convey.ShouldBeTrue)
	})
}

func Test_cache(t *testing.T) {
	type arg struct {
		db    string
		table string
	}
	cnt := 10
	args := make([]arg, 10)
	for i := 0; i < cnt; i++ {
		args[i].db = fmt.Sprintf("db%d", i)
		args[i].table = fmt.Sprintf("table%d", i)
	}

	cache1 := &privilegeCache{}
	convey.Convey("has", t, func() {
		for _, a := range args {
			ret := cache1.has(objectTypeTable, privilegeLevelStar, a.db, a.table, PrivilegeTypeCreateAccount)
			convey.So(ret, convey.ShouldBeFalse)
		}
	})

	//add some privilege
	for _, a := range args {
		for i := PrivilegeTypeCreateAccount; i < PrivilegeTypeCreateObject; i++ {
			cache1.add(objectTypeTable, privilegeLevelStar, a.db, a.table, i)
		}
	}

	convey.Convey("has2", t, func() {
		for _, a := range args {
			ret := cache1.has(objectTypeTable, privilegeLevelStar, a.db, a.table, PrivilegeTypeCreateAccount)
			convey.So(ret, convey.ShouldBeTrue)
			ret = cache1.has(objectTypeTable, privilegeLevelStar, a.db, a.table, PrivilegeTypeCreateObject)
			convey.So(ret, convey.ShouldBeFalse)
		}
	})

	for _, a := range args {
		for i := PrivilegeTypeCreateObject; i < PrivilegeTypeExecute; i++ {
			cache1.add(objectTypeTable, privilegeLevelStar, a.db, a.table, i)
		}
	}

	convey.Convey("has3", t, func() {
		for _, a := range args {
			ret := cache1.has(objectTypeTable, privilegeLevelStar, a.db, a.table, PrivilegeTypeCreateAccount)
			convey.So(ret, convey.ShouldBeTrue)
			ret = cache1.has(objectTypeTable, privilegeLevelStar, a.db, a.table, PrivilegeTypeCreateObject)
			convey.So(ret, convey.ShouldBeTrue)
		}
	})

	//set
	for _, a := range args {
		for i := PrivilegeTypeCreateObject; i < PrivilegeTypeExecute; i++ {
			cache1.set(objectTypeTable, privilegeLevelStar, a.db, a.table)
		}
	}

	convey.Convey("has4", t, func() {
		for _, a := range args {
			ret := cache1.has(objectTypeTable, privilegeLevelStar, a.db, a.table, PrivilegeTypeCreateAccount)
			convey.So(ret, convey.ShouldBeFalse)
			ret = cache1.has(objectTypeTable, privilegeLevelStar, a.db, a.table, PrivilegeTypeCreateObject)
			convey.So(ret, convey.ShouldBeFalse)
		}
	})

	for _, a := range args {
		for i := PrivilegeTypeCreateAccount; i < PrivilegeTypeExecute; i++ {
			cache1.add(objectTypeTable, privilegeLevelStarStar, a.db, a.table, i)
		}
	}

	convey.Convey("has4", t, func() {
		for _, a := range args {
			ret := cache1.has(objectTypeTable, privilegeLevelStar, a.db, a.table, PrivilegeTypeCreateAccount)
			convey.So(ret, convey.ShouldBeFalse)
			ret = cache1.has(objectTypeTable, privilegeLevelStarStar, a.db, a.table, PrivilegeTypeCreateObject)
			convey.So(ret, convey.ShouldBeTrue)
		}
	})

	cache1.invalidate()
	convey.Convey("has4", t, func() {
		for _, a := range args {
			ret := cache1.has(objectTypeTable, privilegeLevelStar, a.db, a.table, PrivilegeTypeCreateAccount)
			convey.So(ret, convey.ShouldBeFalse)
			ret = cache1.has(objectTypeTable, privilegeLevelStar, a.db, a.table, PrivilegeTypeCreateObject)
			convey.So(ret, convey.ShouldBeFalse)
		}
	})
}

func Test_DropDatabaseOfAccount(t *testing.T) {
	convey.Convey("drop account", t, func() {
		var db string
		databases := map[string]int8{
			"abc":        0,
			"mo_catalog": 0,
			"system":     0,
			"ABC":        0,
		}
		var sqlsForDropDatabases []string
		prefix := "drop database if exists "
		for db = range databases {
			if db == "mo_catalog" {
				continue
			}
			bb := &bytes.Buffer{}
			bb.WriteString(prefix)
			//handle the database annotated by '`'
			if db != strings.ToLower(db) {
				bb.WriteString("`")
				bb.WriteString(db)
				bb.WriteString("`")
			} else {
				bb.WriteString(db)
			}
			bb.WriteString(";")
			sqlsForDropDatabases = append(sqlsForDropDatabases, bb.String())
		}

		has := func(s string) bool {
			for _, sql := range sqlsForDropDatabases {
				if strings.Contains(sql, s) {
					return true
				}
			}
			return false
		}

		convey.So(has("ABC"), convey.ShouldBeTrue)
		convey.So(has("system"), convey.ShouldBeTrue)
		convey.So(has("mo_catalog"), convey.ShouldBeFalse)
	})
}

func TestGetSqlForGetDbIdAndType(t *testing.T) {
	ctx := context.TODO()
	kases := []struct {
		pubName string
		want    string
		err     bool
	}{
		{
			pubName: "abc",
			want:    "select dat_id,dat_type from mo_catalog.mo_database where datname = 'abc' and account_id = 0;",
			err:     false,
		},
		{
			pubName: "abc\t",
			want:    "",
			err:     true,
		},
	}
	for _, k := range kases {
		sql, err := getSqlForGetDbIdAndType(ctx, k.pubName, true, 0)
		require.Equal(t, k.err, err != nil)
		require.Equal(t, k.want, sql)
	}
}

func TestGetSqlForInsertIntoMoPubs(t *testing.T) {
	ctx := context.TODO()
	kases := []struct {
		pubName      string
		databaseName string
		err          bool
	}{
		{
			pubName:      "abc",
			databaseName: "abc",
			err:          false,
		},
		{
			pubName:      "abc\t",
			databaseName: "abc",
			err:          true,
		},
		{
			pubName:      "abc",
			databaseName: "abc\t",
			err:          true,
		},
	}
	for _, k := range kases {
		_, err := getSqlForInsertIntoMoPubs(ctx, k.pubName, k.databaseName, 0, false, "", "", 1, 1, "", true)
		require.Equal(t, k.err, err != nil)
	}
}

func TestDoCreatePublication(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	ses := newTestSession(t, ctrl)
	defer ses.Close()

	tenant := &TenantInfo{
		Tenant:        sysAccountName,
		User:          rootName,
		DefaultRole:   moAdminRoleName,
		TenantID:      sysAccountID,
		UserID:        rootID,
		DefaultRoleID: moAdminRoleID,
	}
	ses.SetTenantInfo(tenant)

	sa := &tree.CreatePublication{
		Name:     "pub1",
		Database: "db1",
		Comment:  "124",
		AccountsSet: &tree.AccountsSetOption{
			SetAccounts: tree.IdentifierList{"a1", "a2"},
		},
	}
	sql1, err := getSqlForGetDbIdAndType(ctx, string(sa.Database), true, 0)
	require.NoError(t, err)
	bh := &backgroundExecTest{}
	bh.init()
	sql2, err := getSqlForInsertIntoMoPubs(ctx, string(sa.Name), string(sa.Database), 0, true, "", "a1, a2", tenant.GetDefaultRoleID(), tenant.GetUserID(), sa.Comment, true)
	require.NoError(t, err)
	bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
	defer bhStub.Reset()

	bh.sql2result["begin;"] = nil
	bh.sql2result[sql1] = &MysqlResultSet{
		Columns: []Column{
			&MysqlColumn{
				ColumnImpl: ColumnImpl{
					name:       "dat_id",
					columnType: defines.MYSQL_TYPE_VARCHAR,
				},
			},
			&MysqlColumn{
				ColumnImpl: ColumnImpl{
					name:       "dat_type",
					columnType: defines.MYSQL_TYPE_VARCHAR,
				},
			},
		},
		Data: [][]interface{}{{0, ""}},
	}
	bh.sql2result[sql2] = nil
	bh.sql2result["commit;"] = nil
	bh.sql2result["rollback;"] = nil

	err = doCreatePublication(ctx, ses, sa)
	require.NoError(t, err)
}

func TestDoDropPublication(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	ses := newTestSession(t, ctrl)
	defer ses.Close()

	tenant := &TenantInfo{
		Tenant:        sysAccountName,
		User:          rootName,
		DefaultRole:   moAdminRoleName,
		TenantID:      sysAccountID,
		UserID:        rootID,
		DefaultRoleID: moAdminRoleID,
	}
	ses.SetTenantInfo(tenant)

	sa := &tree.DropPublication{
		Name: "pub1",
	}
	sql1, err := getSqlForGetPubInfo(ctx, string(sa.Name), true)
	require.NoError(t, err)
	sql2, err := getSqlForDropPubInfo(ctx, string(sa.Name), true)
	require.NoError(t, err)
	bh := &backgroundExecTest{}
	bh.init()
	bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
	defer bhStub.Reset()

	bh.sql2result["begin;"] = nil
	bh.sql2result[sql1] = &MysqlResultSet{
		Data: [][]any{{"db1", 0, "a1, a2", "124"}},
	}
	bh.sql2result[sql2] = nil
	bh.sql2result["commit;"] = nil
	bh.sql2result["rollback;"] = nil

	err = doDropPublication(ctx, ses, sa)
	require.NoError(t, err)

}

func TestDoAlterPublication(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	ses := newTestSession(t, ctrl)
	defer ses.Close()

	tenant := &TenantInfo{
		Tenant:        sysAccountName,
		User:          rootName,
		DefaultRole:   moAdminRoleName,
		TenantID:      sysAccountID,
		UserID:        rootID,
		DefaultRoleID: moAdminRoleID,
	}
	ses.SetTenantInfo(tenant)
	columns := []Column{
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "account_list",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "comment",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "database_name",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "database_id",
				columnType: defines.MYSQL_TYPE_LONGLONG,
			},
		},
	}
	dbColumns := []Column{
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "dat_id",
				columnType: defines.MYSQL_TYPE_LONGLONG,
			},
		},
		&MysqlColumn{
			ColumnImpl: ColumnImpl{
				name:       "dat_type",
				columnType: defines.MYSQL_TYPE_VARCHAR,
			},
		},
	}
	kases := []struct {
		pubName     string
		dbName      string
		dbId        uint64
		dbType      string
		comment     string
		accountsSet *tree.AccountsSetOption
		accountList string
		data        [][]any
		err         bool
	}{
		{
			pubName: "pub1",
			comment: "124",
			accountsSet: &tree.AccountsSetOption{
				All: true,
			},
			accountList: "121",
			data:        [][]any{{"all", "121", "db1", 1}},
			err:         false,
		},
		{
			pubName: "pub1",
			comment: "124",
			accountsSet: &tree.AccountsSetOption{
				AddAccounts: tree.IdentifierList{
					tree.Identifier("a1"),
				},
			},
			accountList: "a0",
			data:        [][]any{{"a0", "121", "db1", 1}},
			err:         false,
		},
		{
			pubName: "pub1",
			comment: "124",
			accountsSet: &tree.AccountsSetOption{
				SetAccounts: tree.IdentifierList{
					tree.Identifier("a1"),
				},
			},
			accountList: "a0",
			data:        [][]any{{"a0", "121", "db1", 1}},
			err:         false,
		},
		{
			pubName: "pub1",
			comment: "124",
			accountsSet: &tree.AccountsSetOption{
				DropAccounts: tree.IdentifierList{
					tree.Identifier("a1"),
				},
			},
			accountList: "all",
			data:        [][]any{{"all", "121", "db1", 1}},
			err:         true,
		},
		{
			pubName: "pub1",
			comment: "124",
			dbName:  "db2",
			dbId:    2,
			dbType:  "",
			data:    [][]any{{"all", "121", "db1", 1}},
			err:     false,
		},
		{
			pubName: "pub1",
			comment: "124",
			dbName:  "db2",
			dbId:    2,
			dbType:  "",
			accountsSet: &tree.AccountsSetOption{
				AddAccounts: tree.IdentifierList{
					tree.Identifier("a1"),
				},
			},
			accountList: "a0,a1",
			data:        [][]any{{"a0", "121", "db1", 1}},
			err:         false,
		},
	}

	for _, kase := range kases {
		sa := &tree.AlterPublication{
			Name:        tree.Identifier(kase.pubName),
			Comment:     kase.comment,
			AccountsSet: kase.accountsSet,
			DbName:      kase.dbName,
		}
		sql1, err := getSqlForGetPubInfo(ctx, string(sa.Name), true)
		require.NoError(t, err)

		sql3, err := getSqlForGetDbIdAndType(ctx, kase.dbName, true, 0)
		require.NoError(t, err)

		sql2, err := getSqlForUpdatePubInfo(ctx, string(sa.Name), kase.accountList, sa.Comment, kase.dbName, kase.dbId, true)
		require.NoError(t, err)

		bh := &backgroundExecTest{}
		bh.init()
		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		bh.sql2result["begin;"] = nil
		bh.sql2result[sql1] = &MysqlResultSet{
			Data:    kase.data,
			Columns: columns,
		}
		bh.sql2result[sql2] = nil
		bh.sql2result[sql3] = &MysqlResultSet{
			Data:    [][]any{{kase.dbId, kase.dbType}},
			Columns: dbColumns,
		}
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		err = doAlterPublication(ctx, ses, sa)
		if kase.err {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
	}

}

func TestCheckSubscriptionValid(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	_ = ses.SetGlobalVar(context.TODO(), "lower_case_table_names", int64(1))
	defer ses.Close()

	bh := &backgroundExecTest{}
	bh.init()

	bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
	defer bhStub.Reset()

	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	pu.SV.SetDefaultValues()
	ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

	rm, _ := NewRoutineManager(ctx)
	ses.rm = rm

	proc := testutil.NewProcess()
	proc.FileService = getGlobalPu().FileService
	ses.GetTxnCompileCtx().execCtx = &ExecCtx{
		proc: proc,
	}
	ses.GetTxnCompileCtx().GetProcess().SessionInfo = process.SessionInfo{Account: sysAccountName}

	columns := [][]Column{
		{
			&MysqlColumn{
				ColumnImpl: ColumnImpl{
					name:       "account_id",
					columnType: defines.MYSQL_TYPE_LONGLONG,
				},
			},
			&MysqlColumn{
				ColumnImpl: ColumnImpl{
					name:       "status",
					columnType: defines.MYSQL_TYPE_VARCHAR,
				},
			},
		},
		{
			&MysqlColumn{
				ColumnImpl: ColumnImpl{
					name:       "database_name",
					columnType: defines.MYSQL_TYPE_VARCHAR,
				},
			},
			&MysqlColumn{
				ColumnImpl: ColumnImpl{
					name:       "all_account",
					columnType: defines.MYSQL_TYPE_BOOL,
				},
			},
			&MysqlColumn{
				ColumnImpl: ColumnImpl{
					name:       "account_list",
					columnType: defines.MYSQL_TYPE_VARCHAR,
				},
			},
		},
	}

	kases := []struct {
		createSql string

		accName   string
		pubName   string
		pubExists bool
		accExists bool

		subName string

		accId     uint32
		accStatus string

		databaseName string
		accountList  string

		sqls  []string
		datas [][][]interface{}

		err bool
	}{
		{
			createSql: "create database sub1 from acc0 publication",
			accName:   "acc0",
			pubName:   "",
			subName:   "sub1",
			accId:     1,
			err:       true,
		},
		{
			createSql: "create database sub1 from sys publication pub1",
			accName:   "sys",
			pubName:   "pub1",
			subName:   "sub1",
			accId:     0,
			accStatus: "",
			err:       true,
		},
		{
			createSql: "create database sub1 from acc0 publication pub1",
			accName:   "acc0",
			pubName:   "pub1",
			subName:   "sub1",
			pubExists: true,
			accExists: true,
			accId:     1,
			accStatus: "",

			databaseName: "t1",
			accountList:  "all",

			sqls: []string{},
			err:  false,
		},
		{
			createSql: "create database sub1 from acc0 publication pub1",
			accName:   "acc0",
			pubName:   "pub1",
			subName:   "sub1",
			pubExists: true,
			accExists: true,
			accId:     1,
			accStatus: "",

			databaseName: "t1",
			accountList:  "sys",

			sqls: []string{},
			err:  false,
		},
		{
			createSql: "create database sub1 from acc0 publication pub1",
			accName:   "acc0",
			pubName:   "pub1",
			subName:   "sub1",
			pubExists: true,
			accExists: false,
			accId:     1,
			accStatus: "",

			databaseName: "t1",
			accountList:  "sys",

			sqls: []string{},
			err:  true,
		},
		{
			createSql: "create database sub1 from acc0 publication pub1",
			accName:   "acc0",
			pubName:   "pub1",
			subName:   "sub1",
			pubExists: true,
			accExists: true,
			accId:     1,
			accStatus: tree.AccountStatusSuspend.String(),

			databaseName: "t1",
			accountList:  "sys",

			sqls: []string{},
			err:  true,
		},
		{
			createSql: "create database sub1 from acc0 publication pub1",
			accName:   "acc0",
			pubName:   "pub1",
			subName:   "sub1",
			pubExists: false,
			accExists: true,
			accId:     1,
			accStatus: tree.AccountStatusSuspend.String(),

			databaseName: "t1",
			accountList:  "sys",

			sqls: []string{},
			err:  true,
		},
	}

	initData := func(idx int) {
		sql1, _ := getSqlForAccountIdAndStatus(ctx, kases[idx].accName, true)
		sql2, _ := getSqlForPubInfoForSub(ctx, kases[idx].pubName, true)
		kases[idx].sqls = []string{
			sql1, sql2,
		}
		kases[idx].datas = [][][]interface{}{
			{{kases[idx].accId, kases[idx].accStatus}},
			{{kases[idx].databaseName, kases[idx].accountList}},
		}

		if !kases[idx].accExists {
			kases[idx].datas[0] = nil
		}
		if !kases[idx].pubExists {
			kases[idx].datas[1] = nil
		}
	}

	for idx := range kases {
		initData(idx)

		bh := &backgroundExecTest{}
		bh.init()
		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil
		for i := range kases[idx].sqls {
			bh.sql2result[kases[idx].sqls[i]] = &MysqlResultSet{
				Data:    kases[idx].datas[i],
				Columns: columns[i],
			}
		}

		_, err := checkSubscriptionValid(ctx, ses, kases[idx].createSql)
		if kases[idx].err {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
	}

}

func TestDoCheckRole(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	ses := newTestSession(t, ctrl)
	defer ses.Close()

	tenant := &TenantInfo{
		Tenant:        sysAccountName,
		User:          rootName,
		DefaultRole:   moAdminRoleName,
		TenantID:      sysAccountID,
		UserID:        rootID,
		DefaultRoleID: moAdminRoleID,
	}

	ses.SetTenantInfo(tenant)
	err := doCheckRole(ctx, ses)
	require.NoError(t, err)

	tenant = &TenantInfo{
		Tenant:        "default_1",
		User:          "admin",
		DefaultRole:   accountAdminRoleName,
		TenantID:      3001,
		UserID:        2,
		DefaultRoleID: accountAdminRoleID,
	}
	ses.SetTenantInfo(tenant)
	err = doCheckRole(ctx, ses)
	require.NoError(t, err)

	tenant = &TenantInfo{
		Tenant:        "default_1",
		User:          "admin",
		DefaultRole:   "role1",
		TenantID:      3001,
		UserID:        2,
		DefaultRoleID: 10,
	}
	ses.SetTenantInfo(tenant)
	err = doCheckRole(ctx, ses)
	require.Error(t, err)
}

func TestGetUserPart(t *testing.T) {
	user1 := "user1"
	require.Equal(t, "user1", getUserPart(user1))
	user1 = "user1?"
	require.Equal(t, "user1", getUserPart(user1))
	user1 = "user1?a:b"
	require.Equal(t, "user1", getUserPart(user1))
}

func TestCheckRoleWhetherTableOwner(t *testing.T) {
	convey.Convey("checkRoleWhetherTableOwner success", t, func() {
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

		sql := getSqlForGetOwnerOfTable("db1", "t1")
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{0},
		})
		bh.sql2result[sql] = mrs

		_, err := checkRoleWhetherTableOwner(ses.GetTxnHandler().GetTxnCtx(), ses, "db1", "t1", true)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("checkRoleWhetherTableOwner success", t, func() {
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
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx)
		ses.rm = rm

		tenant := &TenantInfo{
			Tenant:              sysAccountName,
			User:                rootName,
			DefaultRole:         moAdminRoleName,
			TenantID:            sysAccountID,
			UserID:              rootID,
			DefaultRoleID:       moAdminRoleID,
			useAllSecondaryRole: true,
		}
		ses.SetTenantInfo(tenant)

		sql := getSqlForGetOwnerOfTable("db1", "t1")
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{0},
		})
		bh.sql2result[sql] = mrs

		sql = getSqlForGetRolesOfCurrentUser(int64(ses.GetTenantInfo().GetUserID()))
		mrs = newMrsForPasswordOfUser([][]interface{}{
			{0},
		})
		bh.sql2result[sql] = mrs

		ok, err := checkRoleWhetherTableOwner(ses.GetTxnHandler().GetTxnCtx(), ses, "db1", "t1", true)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("checkRoleWhetherTableOwner fail", t, func() {
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

		sql := getSqlForGetOwnerOfTable("db1", "t1")
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{1},
		})
		bh.sql2result[sql] = mrs

		ok, err := checkRoleWhetherTableOwner(ses.GetTxnHandler().GetTxnCtx(), ses, "db1", "t1", false)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})
}

func TestCheckRoleWhetherDatabaseOwner(t *testing.T) {
	convey.Convey("checkRoleWhetherDatabaseOwner success", t, func() {
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

		sql := getSqlForGetOwnerOfDatabase("db1")
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{0},
		})
		bh.sql2result[sql] = mrs

		ok, err := checkRoleWhetherDatabaseOwner(ses.GetTxnHandler().GetTxnCtx(), ses, "db1", true)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("checkRoleWhetherDatabaseOwner success", t, func() {
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
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx)
		ses.rm = rm

		tenant := &TenantInfo{
			Tenant:              sysAccountName,
			User:                rootName,
			DefaultRole:         moAdminRoleName,
			TenantID:            sysAccountID,
			UserID:              rootID,
			DefaultRoleID:       moAdminRoleID,
			useAllSecondaryRole: true,
		}
		ses.SetTenantInfo(tenant)

		sql := getSqlForGetOwnerOfDatabase("db1")
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{0},
		})
		bh.sql2result[sql] = mrs

		sql = getSqlForGetRolesOfCurrentUser(int64(ses.GetTenantInfo().GetUserID()))
		mrs = newMrsForPasswordOfUser([][]interface{}{
			{0},
		})
		bh.sql2result[sql] = mrs

		ok, err := checkRoleWhetherDatabaseOwner(ses.GetTxnHandler().GetTxnCtx(), ses, "db1", true)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("checkRoleWhetherDatabaseOwner fail", t, func() {
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

		sql := getSqlForGetOwnerOfDatabase("db1")
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{1},
		})
		bh.sql2result[sql] = mrs

		ok, err := checkRoleWhetherDatabaseOwner(ses.GetTxnHandler().GetTxnCtx(), ses, "db1", false)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})

}

func TestGetSqlForCheckRoleHasPrivilegeWGODependsOnPrivType(t *testing.T) {
	convey.Convey("getetSqlForCheckRoleHasPrivilegeWGODependsOnPrivType", t, func() {
		out1 := "select distinct role_id from mo_catalog.mo_role_privs where (with_grant_option = true and (privilege_id = 0 or privilege_id = 14)) or privilege_id = 15;"
		sql := getSqlForCheckRoleHasPrivilegeWGODependsOnPrivType(PrivilegeTypeCreateAccount)
		convey.So(out1, convey.ShouldEqual, sql)
	})

	convey.Convey("getetSqlForCheckRoleHasPrivilegeWGODependsOnPrivType", t, func() {
		out2 := "select distinct role_id from mo_catalog.mo_role_privs where (with_grant_option = true and (privilege_id = 18 or privilege_id = 28)) or privilege_id = 29;"
		sql := getSqlForCheckRoleHasPrivilegeWGODependsOnPrivType(PrivilegeTypeShowTables)
		convey.So(out2, convey.ShouldEqual, sql)
	})

	convey.Convey("getetSqlForCheckRoleHasPrivilegeWGODependsOnPrivType", t, func() {
		out3 := "select distinct role_id from mo_catalog.mo_role_privs where (with_grant_option = true and (privilege_id = 30 or privilege_id = 37)) or privilege_id = 38;"
		sql := getSqlForCheckRoleHasPrivilegeWGODependsOnPrivType(PrivilegeTypeSelect)
		convey.So(out3, convey.ShouldEqual, sql)
	})
}

func TestDoAlterDatabaseConfig(t *testing.T) {
	convey.Convey("doAlterDatabaseConfig success", t, func() {
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

		ad := &tree.AlterDataBaseConfig{
			AccountName:    sysAccountName,
			DbName:         "db1",
			IsAccountLevel: false,
			UpdateConfig:   "0.7",
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckDatabaseWithOwner(ctx, ad.DbName, int64(ses.GetTenantInfo().GetTenantID()))
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{0, 0},
		})
		bh.sql2result[sql] = mrs

		sql, _ = getSqlForupdateConfigurationByDbNameAndAccountName(ctx, ad.UpdateConfig, ses.GetTenantInfo().GetTenant(), ad.DbName, "version_compatibility")
		mrs = newMrsForPasswordOfUser([][]interface{}{{}})
		bh.sql2result[sql] = mrs

		err := doAlterDatabaseConfig(ses.GetTxnHandler().GetTxnCtx(), ses, ad)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("doAlterDatabaseConfig fail", t, func() {
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

		ad := &tree.AlterDataBaseConfig{
			AccountName:    sysAccountName,
			DbName:         "db1",
			IsAccountLevel: false,
			UpdateConfig:   "0.7",
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckDatabaseWithOwner(ctx, ad.DbName, int64(ses.GetTenantInfo().GetTenantID()))
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{0, 1},
		})
		bh.sql2result[sql] = mrs

		sql, _ = getSqlForupdateConfigurationByDbNameAndAccountName(ctx, ad.UpdateConfig, ses.GetTenantInfo().GetTenant(), ad.DbName, "version_compatibility")
		mrs = newMrsForPasswordOfUser([][]interface{}{{}})
		bh.sql2result[sql] = mrs

		err := doAlterDatabaseConfig(ses.GetTxnHandler().GetTxnCtx(), ses, ad)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestDoAlterAccountConfig(t *testing.T) {
	convey.Convey("doAlterAccountConfig success", t, func() {
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

		ad := &tree.AlterDataBaseConfig{
			AccountName:    sysAccountName,
			IsAccountLevel: true,
			UpdateConfig:   "0.7",
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckTenant(ctx, ad.AccountName)
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{0, 0},
		})
		bh.sql2result[sql] = mrs

		sql, _ = getSqlForupdateConfigurationByAccount(ctx, ad.UpdateConfig, ses.GetTenantInfo().GetTenant(), "version_compatibility")
		mrs = newMrsForPasswordOfUser([][]interface{}{{}})
		bh.sql2result[sql] = mrs

		err := doAlterAccountConfig(ctx, ses, ad)
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestInsertRecordToMoMysqlCompatibilityMode(t *testing.T) {
	convey.Convey("insertRecordToMoMysqlCompatibilityMode success", t, func() {
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

		stmt := &tree.CreateDatabase{
			Name: tree.Identifier("abc"),
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql := fmt.Sprintf(initMoMysqlCompatbilityModeFormat, tenant.TenantID, tenant.GetTenant(), "abc", "", "", false)
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{0, 0},
		})
		bh.sql2result[sql] = mrs

		err := insertRecordToMoMysqlCompatibilityMode(ctx, ses, stmt)
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestDeleteRecordToMoMysqlCompatbilityMode(t *testing.T) {
	convey.Convey("insertRecordToMoMysqlCompatibilityMode success", t, func() {
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

		stmt := &tree.DropDatabase{
			Name: tree.Identifier("abc"),
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql := getSqlForDeleteMysqlCompatbilityMode("abc")
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{0, 0},
		})
		bh.sql2result[sql] = mrs

		err := deleteRecordToMoMysqlCompatbilityMode(ctx, ses, stmt)
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestGetVersionCompatibility(t *testing.T) {
	convey.Convey("getVersionCompatibility success", t, func() {
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

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql := getSqlForGetSystemVariableValueWithDatabase("db1", "version_compatibility")
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{0, 0},
		})
		bh.sql2result[sql] = mrs

		_, err := GetVersionCompatibility(ctx, ses, "db1")
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestCheckStageExistOrNot(t *testing.T) {
	convey.Convey("checkStageExistOrNot success", t, func() {
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

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckStage(ctx, "my_stage_test")
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{0, 0},
		})
		bh.sql2result[sql] = mrs

		rst, err := checkStageExistOrNot(ctx, bh, "my_stage_test")
		convey.So(err, convey.ShouldBeNil)
		convey.So(rst, convey.ShouldBeTrue)
	})

	convey.Convey("checkStageExistOrNot success", t, func() {
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

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckStage(ctx, "my_stage_test")
		mrs := newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		rst, err := checkStageExistOrNot(ctx, bh, "my_stage_test")
		convey.So(err, convey.ShouldBeNil)
		convey.So(rst, convey.ShouldBeFalse)
	})
}

func TestFormatCredentials(t *testing.T) {
	convey.Convey("formatCredentials success", t, func() {
		ta := tree.StageCredentials{
			Exist: false,
		}
		rstr := formatCredentials(ta)
		testRstr := ""
		convey.So(rstr, convey.ShouldEqual, testRstr)
	})

	convey.Convey("formatCredentials success", t, func() {
		ta := tree.StageCredentials{
			Exist:       true,
			Credentials: []string{"AWS_KEY_ID", "1a2b3c"},
		}
		rstr := formatCredentials(ta)
		testRstr := "AWS_KEY_ID=1a2b3c"
		convey.So(rstr, convey.ShouldEqual, testRstr)
	})

	convey.Convey("formatCredentials success", t, func() {
		ta := tree.StageCredentials{
			Exist:       true,
			Credentials: []string{"AWS_KEY_ID", "1a2b3c", "AWS_SECRET_KEY", "4x5y6z"},
		}
		rstr := formatCredentials(ta)
		testRstr := "AWS_KEY_ID=1a2b3c,AWS_SECRET_KEY=4x5y6z"
		convey.So(rstr, convey.ShouldEqual, testRstr)
	})
}

func TestDoDropStage(t *testing.T) {
	convey.Convey("doDropStage success", t, func() {
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

		ds := &tree.DropStage{
			IfNotExists: false,
			Name:        tree.Identifier("my_stage_test"),
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckStage(ctx, "my_stage_test")
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{0, 0},
		})
		bh.sql2result[sql] = mrs

		sql = getSqlForDropStage(string(ds.Name))
		mrs = newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := doDropStage(ctx, ses, ds)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("doDropStage success", t, func() {
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

		ds := &tree.DropStage{
			IfNotExists: true,
			Name:        tree.Identifier("my_stage_test"),
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckStage(ctx, "my_stage_test")
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{0, 0},
		})
		bh.sql2result[sql] = mrs

		sql = getSqlForDropStage(string(ds.Name))
		mrs = newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := doDropStage(ctx, ses, ds)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("doDropStage fail", t, func() {
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

		ds := &tree.DropStage{
			IfNotExists: false,
			Name:        tree.Identifier("my_stage_test"),
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckStage(ctx, "my_stage_test")
		mrs := newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = getSqlForDropStage(string(ds.Name))
		mrs = newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := doDropStage(ctx, ses, ds)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("doDropStage fail", t, func() {
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
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx)
		ses.rm = rm

		tenant := &TenantInfo{
			Tenant:        "test_account",
			User:          rootName,
			DefaultRole:   "test_role",
			TenantID:      sysAccountID,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}
		ses.SetTenantInfo(tenant)

		ds := &tree.DropStage{
			IfNotExists: true,
			Name:        tree.Identifier("my_stage_test"),
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckStage(ctx, "my_stage_test")
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{0, 0},
		})
		bh.sql2result[sql] = mrs

		sql = getSqlForDropStage(string(ds.Name))
		mrs = newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := doDropStage(ctx, ses, ds)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestDoCreateStage(t *testing.T) {
	convey.Convey("doCreateStage success", t, func() {
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

		cs := &tree.CreateStage{
			IfNotExists: false,
			Name:        tree.Identifier("my_stage_test"),
			Url:         "'s3://load/files/'",
			Credentials: tree.StageCredentials{
				Exist: false,
			},
			Status: tree.StageStatus{
				Exist: false,
			},
			Comment: tree.StageComment{
				Exist: false,
			},
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckStage(ctx, "my_stage_test")
		mrs := newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := doCreateStage(ctx, ses, cs)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("doCreateStage fail", t, func() {
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

		cs := &tree.CreateStage{
			IfNotExists: false,
			Name:        tree.Identifier("my_stage_test"),
			Url:         "'s3://load/files/'",
			Credentials: tree.StageCredentials{
				Exist: false,
			},
			Status: tree.StageStatus{
				Exist: false,
			},
			Comment: tree.StageComment{
				Exist: false,
			},
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckStage(ctx, "my_stage_test")
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{0, 0},
		})
		bh.sql2result[sql] = mrs

		err := doCreateStage(ctx, ses, cs)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("doCreateStage success", t, func() {
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

		cs := &tree.CreateStage{
			IfNotExists: true,
			Name:        tree.Identifier("my_stage_test"),
			Url:         "'s3://load/files/'",
			Credentials: tree.StageCredentials{
				Exist: false,
			},
			Status: tree.StageStatus{
				Exist: false,
			},
			Comment: tree.StageComment{
				Exist: false,
			},
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckStage(ctx, "my_stage_test")
		mrs := newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := doCreateStage(ctx, ses, cs)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("doCreateStage success", t, func() {
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

		cs := &tree.CreateStage{
			IfNotExists: false,
			Name:        tree.Identifier("my_stage_test"),
			Url:         "'s3://load/files/'",
			Credentials: tree.StageCredentials{
				Exist:       true,
				Credentials: []string{"'AWS_KEY_ID'", "'1a2b3c'", "'AWS_SECRET_KEY'", "'4x5y6z'"},
			},
			Status: tree.StageStatus{
				Exist:  true,
				Option: tree.StageStatusEnabled,
			},
			Comment: tree.StageComment{
				Exist: false,
			},
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckStage(ctx, "my_stage_test")
		mrs := newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := doCreateStage(ctx, ses, cs)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("doCreateStage fail", t, func() {
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
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx)
		ses.rm = rm

		tenant := &TenantInfo{
			Tenant:        "test_account",
			User:          rootName,
			DefaultRole:   "test_role",
			TenantID:      sysAccountID,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}
		ses.SetTenantInfo(tenant)

		cs := &tree.CreateStage{
			IfNotExists: false,
			Name:        tree.Identifier("my_stage_test"),
			Url:         "'s3://load/files/'",
			Credentials: tree.StageCredentials{
				Exist: false,
			},
			Status: tree.StageStatus{
				Exist: false,
			},
			Comment: tree.StageComment{
				Exist: false,
			},
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckStage(ctx, "my_stage_test")
		mrs := newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := doCreateStage(ctx, ses, cs)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestDoAlterStage(t *testing.T) {
	convey.Convey("doAlterStage success", t, func() {
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

		as := &tree.AlterStage{
			IfNotExists: false,
			Name:        tree.Identifier("my_stage_test"),
			UrlOption: tree.StageUrl{
				Exist: true,
				Url:   "'s3://load/files/'",
			},
			CredentialsOption: tree.StageCredentials{
				Exist: false,
			},
			StatusOption: tree.StageStatus{
				Exist: false,
			},
			Comment: tree.StageComment{
				Exist: false,
			},
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckStage(ctx, "my_stage_test")
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{0, 0},
		})
		bh.sql2result[sql] = mrs

		err := doAlterStage(ctx, ses, as)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("doAlterStage success", t, func() {
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

		as := &tree.AlterStage{
			IfNotExists: false,
			Name:        tree.Identifier("my_stage_test"),
			UrlOption: tree.StageUrl{
				Exist: false,
			},
			CredentialsOption: tree.StageCredentials{
				Exist:       true,
				Credentials: []string{"'AWS_KEY_ID'", "'1a2b3c'", "'AWS_SECRET_KEY'", "'4x5y6z'"},
			},
			StatusOption: tree.StageStatus{
				Exist: false,
			},
			Comment: tree.StageComment{
				Exist: false,
			},
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckStage(ctx, "my_stage_test")
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{0, 0},
		})
		bh.sql2result[sql] = mrs

		err := doAlterStage(ctx, ses, as)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("doAlterStage success", t, func() {
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

		as := &tree.AlterStage{
			IfNotExists: true,
			Name:        tree.Identifier("my_stage_test"),
			UrlOption: tree.StageUrl{
				Exist: true,
				Url:   "'s3://load/files/'",
			},
			CredentialsOption: tree.StageCredentials{
				Exist: false,
			},
			StatusOption: tree.StageStatus{
				Exist: false,
			},
			Comment: tree.StageComment{
				Exist: false,
			},
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckStage(ctx, "my_stage_test")
		mrs := newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := doAlterStage(ctx, ses, as)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("doAlterStage fail", t, func() {
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

		as := &tree.AlterStage{
			IfNotExists: false,
			Name:        tree.Identifier("my_stage_test"),
			UrlOption: tree.StageUrl{
				Exist: true,
				Url:   "'s3://load/files/'",
			},
			CredentialsOption: tree.StageCredentials{
				Exist: false,
			},
			StatusOption: tree.StageStatus{
				Exist: false,
			},
			Comment: tree.StageComment{
				Exist: false,
			},
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckStage(ctx, "my_stage_test")
		mrs := newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := doAlterStage(ctx, ses, as)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("doAlterStage fail", t, func() {
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

		as := &tree.AlterStage{
			IfNotExists: true,
			Name:        tree.Identifier("my_stage_test"),
			UrlOption: tree.StageUrl{
				Exist: true,
				Url:   "'s3://load/files/'",
			},
			CredentialsOption: tree.StageCredentials{
				Exist:       true,
				Credentials: []string{"'AWS_KEY_ID'", "'1a2b3c'", "'AWS_SECRET_KEY'", "'4x5y6z'"},
			},
			StatusOption: tree.StageStatus{
				Exist: false,
			},
			Comment: tree.StageComment{
				Exist: false,
			},
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckStage(ctx, "my_stage_test")
		mrs := newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := doAlterStage(ctx, ses, as)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("doAlterStage fail", t, func() {
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
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx)
		ses.rm = rm

		tenant := &TenantInfo{
			Tenant:        "test_account",
			User:          rootName,
			DefaultRole:   "test_role",
			TenantID:      sysAccountID,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}
		ses.SetTenantInfo(tenant)

		as := &tree.AlterStage{
			IfNotExists: false,
			Name:        tree.Identifier("my_stage_test"),
			UrlOption: tree.StageUrl{
				Exist: false,
			},
			CredentialsOption: tree.StageCredentials{
				Exist:       true,
				Credentials: []string{"'AWS_KEY_ID'", "'1a2b3c'", "'AWS_SECRET_KEY'", "'4x5y6z'"},
			},
			StatusOption: tree.StageStatus{
				Exist: false,
			},
			Comment: tree.StageComment{
				Exist: false,
			},
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckStage(ctx, "my_stage_test")
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{0, 0},
		})
		bh.sql2result[sql] = mrs

		err := doAlterStage(ctx, ses, as)
		convey.So(err, convey.ShouldNotBeNil)
	})

}

func TestDoCheckFilePath(t *testing.T) {
	convey.Convey("doCheckFilePath success", t, func() {
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

		cs := &tree.Select{}
		ses.InitExportConfig(cs.Ep)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		err := doCheckFilePath(ctx, ses, cs.Ep)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("doCheckFilePath success", t, func() {
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

		cs := &tree.Select{
			Ep: &tree.ExportParam{
				FilePath: "/mnt/disk1/t1.csv",
			},
		}
		ses.InitExportConfig(cs.Ep)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql := getSqlForCheckStageStatus(ctx, "enabled")
		mrs := newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := doCheckFilePath(ctx, ses, cs.Ep)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("doCheckFilePath success", t, func() {
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

		cs := &tree.Select{
			Ep: &tree.ExportParam{
				FilePath: "/mnt/disk1/t1.csv",
			},
		}
		ses.InitExportConfig(cs.Ep)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql := getSqlForCheckStageStatus(ctx, "enabled")
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{0, 0},
		})
		bh.sql2result[sql] = mrs

		err := doCheckFilePath(ctx, ses, cs.Ep)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("doCheckFilePath fail", t, func() {
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

		cs := &tree.Select{
			Ep: &tree.ExportParam{
				FilePath: "stage1:/t1.csv",
			},
		}
		ses.InitExportConfig(cs.Ep)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckStageStatusWithStageName(ctx, "stage1")
		mrs := newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := doCheckFilePath(ctx, ses, cs.Ep)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("doCheckFilePath fail", t, func() {
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

		cs := &tree.Select{
			Ep: &tree.ExportParam{
				FilePath: "stage1:/t1.csv",
			},
		}
		ses.InitExportConfig(cs.Ep)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckStageStatusWithStageName(ctx, "stage1")
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{"/tmp", "disabled"},
		})
		bh.sql2result[sql] = mrs

		err := doCheckFilePath(ctx, ses, cs.Ep)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("doCheckFilePath success", t, func() {
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

		cs := &tree.Select{
			Ep: &tree.ExportParam{
				FilePath: "stage1:/t1.csv",
			},
		}
		ses.InitExportConfig(cs.Ep)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckStageStatusWithStageName(ctx, "stage1")
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{"/tmp", "enabled"},
		})
		bh.sql2result[sql] = mrs

		err := doCheckFilePath(ctx, ses, cs.Ep)
		convey.So(err, convey.ShouldBeNil)
		convey.So(cs.Ep.FilePath, convey.ShouldEqual, "stage1:/t1.csv")
	})
}

func TestGetLabelPart(t *testing.T) {
	user1 := "user1"
	require.Equal(t, "", getLabelPart(user1))
	user1 = "user1?"
	require.Equal(t, "", getLabelPart(user1))
	user1 = "user1?a:b"
	require.Equal(t, "a:b", getLabelPart(user1))
}

func TestParseLabel(t *testing.T) {
	cases := []struct {
		str string
		ret map[string]string
		err bool
	}{
		{
			str: "",
			ret: map[string]string{},
			err: false,
		},
		{
			str: "a=1",
			ret: map[string]string{"a": "1"},
			err: false,
		},
		{
			str: "a=1,",
			err: true,
		},
		{
			str: "a=1,b=2",
			ret: map[string]string{"b": "2", "a": "1"},
			err: false,
		},
		{
			str: "a=1,b",
			err: true,
		},
		{
			str: "a=1,b=",
			err: true,
		},
		{
			str: "a=1,=2",
			err: true,
		},
		{
			str: "a=1,=",
			err: true,
		},
		{
			str: "a",
			err: true,
		},
		{
			str: "a=1,b:2",
			err: true,
		},
	}

	for _, item := range cases {
		lb, err := ParseLabel(item.str)
		if item.err {
			require.Error(t, err)
		} else {
			require.True(t, reflect.DeepEqual(item.ret, lb))
			require.NoError(t, err)
		}
	}
}

func TestUpload(t *testing.T) {
	convey.Convey("call upload func", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ioses := mock_frontend.NewMockIOSession(ctrl)
		proc := testutil.NewProc()
		cnt := 0
		ioses.EXPECT().Read(gomock.Any()).DoAndReturn(func(options goetty.ReadOptions) (pkt any, err error) {
			if cnt == 0 {
				pkt = &Packet{Length: 5, Payload: []byte("def add(a, b):\n"), SequenceID: 1}
			} else if cnt == 1 {
				pkt = &Packet{Length: 5, Payload: []byte("  return a + b"), SequenceID: 2}
			} else {
				err = moerr.NewInvalidInput(context.TODO(), "length 0")
			}
			cnt++
			return
		}).AnyTimes()
		proto := &FakeProtocol{
			ioses: ioses,
		}
		fs, err := fileservice.NewLocalFS(context.TODO(), defines.SharedFileServiceName, t.TempDir(), fileservice.DisabledCacheConfig, nil)
		convey.So(err, convey.ShouldBeNil)
		proc.FileService = fs

		//parameter
		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		_, err = toml.DecodeFile("test/system_vars_config.toml", pu.SV)
		assert.Nil(t, err)
		pu.SV.SetDefaultValues()
		pu.SV.SaveQueryResult = "on"
		if err != nil {
			assert.Nil(t, err)
		}
		//file service
		pu.FileService = fs
		setGlobalPu(pu)
		ses := &Session{
			feSessionImpl: feSessionImpl{
				proto: proto,
			},
			proc: proc,
		}
		ec := newTestExecCtx(context.TODO(), ctrl)
		fp, err := Upload(ses, ec, "test.py", "test")
		convey.So(err, convey.ShouldBeNil)
		iovec := &fileservice.IOVector{
			FilePath: fp,
			Entries: []fileservice.IOEntry{
				{
					Offset: 0,
					Size:   -1,
				},
			},
		}
		err = fs.Read(context.TODO(), iovec)
		convey.So(err, convey.ShouldBeNil)
		convey.So(iovec.Entries[0].Data, convey.ShouldResemble, []byte("def add(a, b):\n  return a + b"))
	})
}

func TestCheckSnapshotExistOrNot(t *testing.T) {
	convey.Convey("checkSnapshotExistOrNot success", t, func() {
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
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		setGlobalPu(pu)

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

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckSnapshot(ctx, "snapshot_test")
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{0, 0},
		})
		bh.sql2result[sql] = mrs

		rst, err := checkSnapShotExistOrNot(ctx, bh, "snapshot_test")
		convey.So(err, convey.ShouldBeNil)
		convey.So(rst, convey.ShouldBeTrue)
	})

	convey.Convey("checkSnapshotExistOrNot success", t, func() {
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

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckSnapshot(ctx, "snapshot_test")
		mrs := newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		rst, err := checkSnapShotExistOrNot(ctx, bh, "snapshot_test")
		convey.So(err, convey.ShouldBeNil)
		convey.So(rst, convey.ShouldBeFalse)
	})
}

func TestDoDropSnapshot(t *testing.T) {
	convey.Convey("doDropSnapshot success", t, func() {
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

		ds := &tree.DropSnapShot{
			IfExists: false,
			Name:     tree.Identifier("snapshot_test"),
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckSnapshot(ctx, "snapshot_test")
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{0, 0},
		})
		bh.sql2result[sql] = mrs

		sql = getSqlForDropSnapshot(string(ds.Name))
		mrs = newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := doDropSnapshot(ctx, ses, ds)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("doDropSnapshot success", t, func() {
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

		ds := &tree.DropSnapShot{
			IfExists: true,
			Name:     tree.Identifier("snapshot_test"),
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckSnapshot(ctx, "snapshot_test")
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{0, 0},
		})
		bh.sql2result[sql] = mrs

		sql = getSqlForDropSnapshot(string(ds.Name))
		mrs = newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := doDropSnapshot(ctx, ses, ds)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("doDropSnapshot fail", t, func() {
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

		ds := &tree.DropSnapShot{
			IfExists: false,
			Name:     tree.Identifier("snapshot_test"),
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckSnapshot(ctx, "snapshot_test")
		mrs := newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = getSqlForDropSnapshot(string(ds.Name))
		mrs = newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := doDropSnapshot(ctx, ses, ds)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("doDropSnapshot fail", t, func() {
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
			Tenant:        "test_account",
			User:          rootName,
			DefaultRole:   "test_role",
			TenantID:      sysAccountID,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}
		ses.SetTenantInfo(tenant)

		ds := &tree.DropSnapShot{
			IfExists: true,
			Name:     tree.Identifier("snapshot_test"),
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckSnapshot(ctx, "snapshot_test")
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{0, 0},
		})
		bh.sql2result[sql] = mrs

		sql = getSqlForDropSnapshot(string(ds.Name))
		mrs = newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := doDropSnapshot(ctx, ses, ds)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestDoCreateSnapshot(t *testing.T) {
	convey.Convey("doCreateSnapshot success", t, func() {
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
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		timeStamp, _ := timestamp.ParseTimestamp("2021-01-01 00:00:00")
		txnOperator.EXPECT().SnapshotTS().Return(timeStamp).AnyTimes()
		// process.
		ses.proc = testutil.NewProc()
		ses.proc.TxnOperator = txnOperator
		cs := &tree.CreateSnapShot{
			IfNotExists: false,
			Name:        tree.Identifier("snapshot_test"),
			Object: tree.ObjectInfo{
				SLevel: tree.SnapshotLevelType{
					Level: tree.SNAPSHOTLEVELCLUSTER,
				},
				ObjName: "",
			},
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckSnapshot(ctx, "snapshot_test")
		mrs := newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := doCreateSnapshot(ctx, ses, cs)
		convey.So(err, convey.ShouldBeNil)
	})

	// non-system tenant can't create cluster level snapshot
	convey.Convey("doCreateSnapshot fail", t, func() {
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
			Tenant:        "test_account",
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      sysAccountID,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}
		ses.SetTenantInfo(tenant)
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		timeStamp, _ := timestamp.ParseTimestamp("2021-01-01 00:00:00")
		txnOperator.EXPECT().SnapshotTS().Return(timeStamp).AnyTimes()
		// process.
		ses.proc = testutil.NewProc()
		ses.proc.TxnOperator = txnOperator
		cs := &tree.CreateSnapShot{
			IfNotExists: false,
			Name:        tree.Identifier("snapshot_test"),
			Object: tree.ObjectInfo{
				SLevel: tree.SnapshotLevelType{
					Level: tree.SNAPSHOTLEVELCLUSTER,
				},
				ObjName: "",
			},
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckSnapshot(ctx, "snapshot_test")
		mrs := newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		err := doCreateSnapshot(ctx, ses, cs)
		convey.So(err, convey.ShouldNotBeNil)
	})

	// system tenant create account level snapshot for other tenant
	convey.Convey("doCreateSnapshot success", t, func() {
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
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		timeStamp, _ := timestamp.ParseTimestamp("2021-01-01 00:00:00")
		txnOperator.EXPECT().SnapshotTS().Return(timeStamp).AnyTimes()
		// process.
		ses.proc = testutil.NewProc()
		ses.proc.TxnOperator = txnOperator
		cs := &tree.CreateSnapShot{
			IfNotExists: false,
			Name:        tree.Identifier("snapshot_test"),
			Object: tree.ObjectInfo{
				SLevel: tree.SnapshotLevelType{
					Level: tree.SNAPSHOTLEVELACCOUNT,
				},
				ObjName: "acc1",
			},
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckSnapshot(ctx, "snapshot_test")
		mrs := newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql2, _ := getSqlForCheckTenant(ctx, "acc1")
		mrs2 := newMrsForPasswordOfUser([][]interface{}{{1}})
		bh.sql2result[sql2] = mrs2

		err := doCreateSnapshot(ctx, ses, cs)
		convey.So(err, convey.ShouldBeNil)
	})

	// non-system tenant can't create acccount level snapshot for other tenant
	convey.Convey("doCreateSnapshot fail", t, func() {
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
			Tenant:        "test_account",
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      sysAccountID,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}
		ses.SetTenantInfo(tenant)
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		timeStamp, _ := timestamp.ParseTimestamp("2021-01-01 00:00:00")
		txnOperator.EXPECT().SnapshotTS().Return(timeStamp).AnyTimes()
		// process.
		ses.proc = testutil.NewProc()
		ses.proc.TxnOperator = txnOperator
		cs := &tree.CreateSnapShot{
			IfNotExists: false,
			Name:        tree.Identifier("snapshot_test"),
			Object: tree.ObjectInfo{
				SLevel: tree.SnapshotLevelType{
					Level: tree.SNAPSHOTLEVELACCOUNT,
				},
				ObjName: "acc1",
			},
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckSnapshot(ctx, "snapshot_test")
		mrs := newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql2, _ := getSqlForCheckTenant(ctx, "acc1")
		mrs2 := newMrsForPasswordOfUser([][]interface{}{{1}})
		bh.sql2result[sql2] = mrs2

		err := doCreateSnapshot(ctx, ses, cs)
		convey.So(err, convey.ShouldNotBeNil)
	})

	// no-admin user can't create acccount level snapshot for himself tenant
	convey.Convey("doCreateSnapshot fail", t, func() {
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
			Tenant:        "test_account",
			User:          rootName,
			DefaultRole:   "test_role",
			TenantID:      sysAccountID,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}
		ses.SetTenantInfo(tenant)
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		timeStamp, _ := timestamp.ParseTimestamp("2021-01-01 00:00:00")
		txnOperator.EXPECT().SnapshotTS().Return(timeStamp).AnyTimes()
		// process.
		ses.proc = testutil.NewProc()
		ses.proc.TxnOperator = txnOperator
		cs := &tree.CreateSnapShot{
			IfNotExists: false,
			Name:        tree.Identifier("snapshot_test"),
			Object: tree.ObjectInfo{
				SLevel: tree.SnapshotLevelType{
					Level: tree.SNAPSHOTLEVELACCOUNT,
				},
				ObjName: "test_account",
			},
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckSnapshot(ctx, "snapshot_test")
		mrs := newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql2, _ := getSqlForCheckTenant(ctx, "acc1")
		mrs2 := newMrsForPasswordOfUser([][]interface{}{{1}})
		bh.sql2result[sql2] = mrs2

		err := doCreateSnapshot(ctx, ses, cs)
		convey.So(err, convey.ShouldNotBeNil)
	})

	// system tenant  create account level snapshot for other tenant
	// but the snapshot name already exists
	convey.Convey("doCreateSnapshot fail", t, func() {
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
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		timeStamp, _ := timestamp.ParseTimestamp("2021-01-01 00:00:00")
		txnOperator.EXPECT().SnapshotTS().Return(timeStamp).AnyTimes()
		// process.
		ses.proc = testutil.NewProc()
		ses.proc.TxnOperator = txnOperator
		cs := &tree.CreateSnapShot{
			IfNotExists: false,
			Name:        tree.Identifier("snapshot_test"),
			Object: tree.ObjectInfo{
				SLevel: tree.SnapshotLevelType{
					Level: tree.SNAPSHOTLEVELACCOUNT,
				},
				ObjName: "acc1",
			},
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckSnapshot(ctx, "snapshot_test")
		mrs := newMrsForPasswordOfUser([][]interface{}{{1}})
		bh.sql2result[sql] = mrs

		sql2, _ := getSqlForCheckTenant(ctx, "acc1")
		mrs2 := newMrsForPasswordOfUser([][]interface{}{{1}})
		bh.sql2result[sql2] = mrs2

		err := doCreateSnapshot(ctx, ses, cs)
		convey.So(err, convey.ShouldNotBeNil)
	})

	// system tenant  create account level snapshot for other tenant
	// but the account not exists
	convey.Convey("doCreateSnapshot fail", t, func() {
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
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		timeStamp, _ := timestamp.ParseTimestamp("2021-01-01 00:00:00")
		txnOperator.EXPECT().SnapshotTS().Return(timeStamp).AnyTimes()
		// process.
		ses.proc = testutil.NewProc()
		ses.proc.TxnOperator = txnOperator
		cs := &tree.CreateSnapShot{
			IfNotExists: false,
			Name:        tree.Identifier("snapshot_test"),
			Object: tree.ObjectInfo{
				SLevel: tree.SnapshotLevelType{
					Level: tree.SNAPSHOTLEVELACCOUNT,
				},
				ObjName: "acc1",
			},
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckSnapshot(ctx, "snapshot_test")
		mrs := newMrsForPasswordOfUser([][]interface{}{{}})
		bh.sql2result[sql] = mrs

		sql2, _ := getSqlForCheckTenant(ctx, "acc1")
		mrs2 := newMrsForPasswordOfUser([][]interface{}{{1}})
		bh.sql2result[sql2] = mrs2

		err := doCreateSnapshot(ctx, ses, cs)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestDoResolveSnapshotTsWithSnapShotName(t *testing.T) {
	convey.Convey("doResolveSnapshotWithSnapshotName success", t, func() {
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

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForGetSnapshotTsWithSnapshotName(ctx, "test_sp")
		mrs := newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err := doResolveSnapshotWithSnapshotName(ctx, ses, "test_sp")
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestCheckTimeStampValid(t *testing.T) {
	convey.Convey("checkTimeStampValid success", t, func() {
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

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql := getSqlForCheckSnapshotTs(1713235646865937000)
		mrs := newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		valid, err := checkTimeStampValid(ctx, ses, 1713235646865937000)
		convey.So(err, convey.ShouldBeNil)
		convey.So(valid, convey.ShouldBeFalse)
	})

	convey.Convey("checkTimeStampValid success", t, func() {
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

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql := getSqlForCheckSnapshotTs(1713235646865937000)
		mrs := newMrsForPasswordOfUser([][]interface{}{{"018ee4cd-5991-7caa-b75d-f9290144bd9f"}})
		bh.sql2result[sql] = mrs

		valid, err := checkTimeStampValid(ctx, ses, 1713235646865937000)
		convey.So(err, convey.ShouldBeNil)
		convey.So(valid, convey.ShouldBeTrue)
	})
}
