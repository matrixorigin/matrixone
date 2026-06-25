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
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/btree"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	mysqlparser "github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/stage"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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

func TestEscapeSQLStringForDoubleQuotes(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{"empty", "", ""},
		{"plain", "abc", "abc"},
		{"double_quote", `a"b`, `a\"b`},
		{"backslash", `a\\b`, `a\\\\b`},
		{"single_quote", "a'b", "a'b"},
		{"newline", "a\nb", "a\\nb"},
		{"carriage_return", "a\rb", "a\\rb"},
		{"tab", "a\tb", "a\\tb"},
		{"nul", "a\x00b", "a\\0b"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := escapeSQLStringForDoubleQuotes(tc.input)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestEscapeSQLStringForDoubleQuotes_PythonUdfBodyRoundTrip(t *testing.T) {
	payload := map[string]any{
		"handler": "pdf_to_markdown",
		"import":  false,
		"body":    "def f():\n    return \"ok\"\n",
	}
	rawBytes, err := json.Marshal(payload)
	require.NoError(t, err)
	raw := string(rawBytes)

	scanStringLiteral := func(s string) string {
		scanner := mysqlparser.NewScanner(dialect.MYSQL, `"`+escapeSQLStringForDoubleQuotes(s)+`"`)
		typ, val := scanner.Scan()
		require.Equal(t, mysqlparser.STRING, typ)
		return val
	}

	// Regression check: quoting JSON before SQL escaping leaves `\"` in storage,
	// then python_udf json.Unmarshal fails with "invalid character '\\' ...".
	bad := strconv.Quote(raw)
	bad = bad[1 : len(bad)-1]
	badStored := scanStringLiteral(bad)
	var decoded map[string]any
	require.Error(t, json.Unmarshal([]byte(badStored), &decoded))

	goodStored := scanStringLiteral(raw)
	require.Equal(t, raw, goodStored)
	require.NoError(t, json.Unmarshal([]byte(goodStored), &decoded))
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

func Test_checkTenantExistsOrNot(t *testing.T) {
	convey.Convey("check tenant exists or not", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		pu.SV.KillRountinesInterval = 0
		setPu("", pu)

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

		err = InitGeneralTenant(ctx, bh, ses, &createAccount{
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
		pu.SV.KillRountinesInterval = 0

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
		pu.SV.KillRountinesInterval = 0

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
		pu.SV.KillRountinesInterval = 0
		setPu("", pu)

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

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		ses := newSes(nil, ctrl)

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		pu.SV.KillRountinesInterval = 0

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

		tenant := &TenantInfo{
			Tenant:        sysAccountName,
			User:          rootName,
			DefaultRole:   moAdminRoleName,
			TenantID:      sysAccountID,
			UserID:        rootID,
			DefaultRoleID: moAdminRoleID,
		}

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
		pu.SV.KillRountinesInterval = 0

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
		{stmt: &tree.AlterRole{}},
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
		{stmt: &tree.SavePoint{}},
		{stmt: &tree.ReleaseSavePoint{}},
		{stmt: &tree.RollbackToSavePoint{}},
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("sys account all can create account", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.CreateAccount{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		ses.SetTenantInfo(&TenantInfo{
			Tenant:        sysAccountName,
			User:          "mocadmin",
			DefaultRole:   "mocadmin_role",
			TenantID:      sysAccountID,
			UserID:        2,
			DefaultRoleID: 2,
		})

		sql2result := map[string]ExecResult{
			getSqlForRoleIdOfUserId(2): newMrsForRoleIdOfUserId([][]interface{}{
				{2, false},
			}),
			getSqlForCheckRoleHasAccountLevelForStarWithSysScope(2, PrivilegeTypeCreateAccount, true): newMrsForCheckRoleHasPrivilege([][]interface{}{
				{PrivilegeTypeAccountAll, false},
			}),
		}

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("sys account all can alter account", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.AlterAccount{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		ses.SetTenantInfo(&TenantInfo{
			Tenant:        sysAccountName,
			User:          "mocadmin",
			DefaultRole:   "mocadmin_role",
			TenantID:      sysAccountID,
			UserID:        2,
			DefaultRoleID: 2,
		})

		sql2result := map[string]ExecResult{
			getSqlForRoleIdOfUserId(2): newMrsForRoleIdOfUserId([][]interface{}{
				{2, false},
			}),
			getSqlForCheckRoleHasAccountLevelForStarWithSysScope(2, PrivilegeTypeAlterAccount, true): newMrsForCheckRoleHasPrivilege([][]interface{}{
				{PrivilegeTypeAccountAll, false},
			}),
		}

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("non sys account all cannot create account", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.CreateAccount{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		ses.SetTenantInfo(&TenantInfo{
			Tenant:        "acc1",
			User:          "admin",
			DefaultRole:   accountAdminRoleName,
			TenantID:      1001,
			UserID:        2,
			DefaultRoleID: accountAdminRoleID,
		})

		sql2result := map[string]ExecResult{
			getSqlForRoleIdOfUserId(2): newMrsForRoleIdOfUserId([][]interface{}{
				{accountAdminRoleID, false},
			}),
			getSqlForCheckRoleHasAccountLevelForStar(int64(accountAdminRoleID), PrivilegeTypeCreateAccount): newMrsForCheckRoleHasPrivilege([][]interface{}{}),
			getSqlForInheritedRoleIdOfRoleId(int64(accountAdminRoleID)):                                     newMrsForInheritedRoleIdOfRoleId([][]interface{}{}),
		}

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})

	convey.Convey("non sys account all cannot alter account", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.AlterAccount{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		ses.SetTenantInfo(&TenantInfo{
			Tenant:        "acc1",
			User:          "admin",
			DefaultRole:   accountAdminRoleName,
			TenantID:      1001,
			UserID:        2,
			DefaultRoleID: accountAdminRoleID,
		})

		sql2result := map[string]ExecResult{
			getSqlForRoleIdOfUserId(2): newMrsForRoleIdOfUserId([][]interface{}{
				{accountAdminRoleID, false},
			}),
			getSqlForCheckRoleHasAccountLevelForStar(int64(accountAdminRoleID), PrivilegeTypeAlterAccount): newMrsForCheckRoleHasPrivilege([][]interface{}{}),
			getSqlForInheritedRoleIdOfRoleId(int64(accountAdminRoleID)):                                    newMrsForInheritedRoleIdOfRoleId([][]interface{}{}),
		}

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})

	convey.Convey("non sys account all cannot drop account", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.DropAccount{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		ses.SetTenantInfo(&TenantInfo{
			Tenant:        "acc1",
			User:          "admin",
			DefaultRole:   accountAdminRoleName,
			TenantID:      1001,
			UserID:        2,
			DefaultRoleID: accountAdminRoleID,
		})

		sql2result := map[string]ExecResult{
			getSqlForRoleIdOfUserId(2): newMrsForRoleIdOfUserId([][]interface{}{
				{accountAdminRoleID, false},
			}),
			getSqlForCheckRoleHasAccountLevelForStar(int64(accountAdminRoleID), PrivilegeTypeDropAccount): newMrsForCheckRoleHasPrivilege([][]interface{}{}),
			getSqlForInheritedRoleIdOfRoleId(int64(accountAdminRoleID)):                                   newMrsForInheritedRoleIdOfRoleId([][]interface{}{}),
		}

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})

	convey.Convey("sys account all can drop account", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.DropAccount{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		ses.SetTenantInfo(&TenantInfo{
			Tenant:        sysAccountName,
			User:          "mocadmin",
			DefaultRole:   "mocadmin_role",
			TenantID:      sysAccountID,
			UserID:        2,
			DefaultRoleID: 2,
		})

		sql2result := map[string]ExecResult{
			getSqlForRoleIdOfUserId(2): newMrsForRoleIdOfUserId([][]interface{}{
				{2, false},
			}),
			getSqlForCheckRoleHasAccountLevelForStarWithSysScope(2, PrivilegeTypeDropAccount, true): newMrsForCheckRoleHasPrivilege([][]interface{}{
				{PrivilegeTypeAccountAll, false},
			}),
		}

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("sys account all can upgrade account", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.UpgradeStatement{
			Target: &tree.Target{AccountName: "acc1"},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		ses.SetTenantInfo(&TenantInfo{
			Tenant:        sysAccountName,
			User:          "mocadmin",
			DefaultRole:   "mocadmin_role",
			TenantID:      sysAccountID,
			UserID:        2,
			DefaultRoleID: 2,
		})

		sql2result := map[string]ExecResult{
			getSqlForRoleIdOfUserId(2): newMrsForRoleIdOfUserId([][]interface{}{
				{2, false},
			}),
			getSqlForCheckRoleHasAccountLevelForStarWithSysScope(2, PrivilegeTypeUpgradeAccount, true): newMrsForCheckRoleHasPrivilege([][]interface{}{
				{PrivilegeTypeAccountAll, false},
			}),
		}

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("sys account all cannot upgrade all accounts", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.UpgradeStatement{
			Target: &tree.Target{IsALLAccount: true},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		ses.SetTenantInfo(&TenantInfo{
			Tenant:        sysAccountName,
			User:          "mocadmin",
			DefaultRole:   "mocadmin_role",
			TenantID:      sysAccountID,
			UserID:        2,
			DefaultRoleID: 2,
		})

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})

	convey.Convey("non sys account all cannot upgrade account", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.UpgradeStatement{
			Target: &tree.Target{AccountName: "acc1"},
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		ses.SetTenantInfo(&TenantInfo{
			Tenant:        "acc1",
			User:          "admin",
			DefaultRole:   accountAdminRoleName,
			TenantID:      1001,
			UserID:        2,
			DefaultRoleID: accountAdminRoleID,
		})

		sql2result := map[string]ExecResult{
			getSqlForRoleIdOfUserId(2): newMrsForRoleIdOfUserId([][]interface{}{
				{accountAdminRoleID, false},
			}),
			getSqlForCheckRoleHasAccountLevelForStar(int64(accountAdminRoleID), PrivilegeTypeUpgradeAccount): newMrsForCheckRoleHasPrivilege([][]interface{}{}),
			getSqlForInheritedRoleIdOfRoleId(int64(accountAdminRoleID)):                                      newMrsForInheritedRoleIdOfRoleId([][]interface{}{}),
		}

		bh := newBh(ctrl, sql2result)

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, g)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, g)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, g)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, g)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

			ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
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
					Level:  tree.PRIVILEGE_LEVEL_TYPE_DATABASE_STAR,
					DbName: "db",
				},
			},
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
					{Type: tree.PRIVILEGE_TYPE_STATIC_INSERT},
				},
				ObjType: tree.OBJECT_TYPE_TABLE,
				Level: &tree.PrivilegeLevel{
					Level:   tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE,
					DbName:  "db",
					TabName: "t1",
				},
			},
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
					{Type: tree.PRIVILEGE_TYPE_STATIC_INSERT},
				},
				ObjType: tree.OBJECT_TYPE_TABLE,
				Level: &tree.PrivilegeLevel{
					Level:   tree.PRIVILEGE_LEVEL_TYPE_TABLE,
					TabName: "t1",
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

			// Mock getDatabaseOrTableId for scoped levels.
			if stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_STAR ||
				stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_DATABASE_STAR {
				dbName := stmt.Level.DbName
				if dbName == "" {
					dbName = ses.GetDatabaseName()
				}
				checkSql, _ := getSqlForCheckDatabase(ses.GetTxnHandler().GetTxnCtx(), dbName)
				bh.sql2result[checkSql] = newMrsForCheckDatabase([][]interface{}{{10001}})
				scopedLevel := privilegeLevelStar
				if stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_DATABASE_STAR {
					scopedLevel = privilegeLevelDatabaseStar
				}

				for _, p := range stmt.Privileges {
					privType, _ := convertAstPrivilegeTypeToPrivilegeType(context.TODO(), p.Type, stmt.ObjType)
					scopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
						int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
						objectTypeTable, 10001, scopedLevel)
					bh.sql2result[scopedSql] = newMrsForPrivilegeWGO([][]interface{}{
						{ses.GetTenantInfo().GetDefaultRoleID()},
					})
				}
			} else if stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE ||
				stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_TABLE {
				dbName := stmt.Level.DbName
				tabName := stmt.Level.TabName
				if dbName == "" {
					dbName = ses.GetDatabaseName()
				}
				checkSql, _ := getSqlForCheckDatabaseTable(ses.GetTxnHandler().GetTxnCtx(), dbName, tabName)
				bh.sql2result[checkSql] = newMrsForCheckDatabaseTable([][]interface{}{{10001}})
				checkDbSql, _ := getSqlForCheckDatabase(ses.GetTxnHandler().GetTxnCtx(), dbName)
				bh.sql2result[checkDbSql] = newMrsForCheckDatabase([][]interface{}{{10002}})
				objScopedLevel := privilegeLevelDatabaseTable
				if stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_TABLE {
					objScopedLevel = privilegeLevelTable
				}

				for _, p := range stmt.Privileges {
					privType, _ := convertAstPrivilegeTypeToPrivilegeType(context.TODO(), p.Type, stmt.ObjType)
					scopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
						int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
						objectTypeTable, 10001, objScopedLevel)
					bh.sql2result[scopedSql] = newMrsForPrivilegeWGO([][]interface{}{
						{ses.GetTenantInfo().GetDefaultRoleID()},
					})
					dbScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
						int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
						objectTypeTable, 10002, privilegeLevelDatabaseStar)
					bh.sql2result[dbScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
				}
			}

			for _, p := range stmt.Privileges {
				sql, err := formSqlFromGrantPrivilege(context.TODO(), ses, stmt, p)
				convey.So(err, convey.ShouldBeNil)
				makeRowsOfWithGrantOptionPrivilege(bh.sql2result, sql, [][]interface{}{
					{1, true},
				})

				var privType PrivilegeType
				privType, err = convertAstPrivilegeTypeToPrivilegeType(context.TODO(), p.Type, stmt.ObjType)
				convey.So(err, convey.ShouldBeNil)

				unscopedSql := getSqlForCheckRoleHasPrivilegeWGODependsOnPrivType(privType)
				bh.sql2result[unscopedSql] = newMrsForPrivilegeWGO([][]interface{}{})

				objType := objectTypeTable
				if stmt.ObjType == tree.OBJECT_TYPE_VIEW {
					objType = objectTypeView
				}
				objTypeScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjType(
					int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership), objType)
				bh.sql2result[objTypeScopedSql] = newMrsForPrivilegeWGO([][]interface{}{
					{ses.GetTenantInfo().GetDefaultRoleID()},
				})
				globalScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
					int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
					objType, objectIDAll, privilegeLevelStarStar)
				if stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_STAR_STAR {
					bh.sql2result[globalScopedSql] = newMrsForPrivilegeWGO([][]interface{}{
						{ses.GetTenantInfo().GetDefaultRoleID()},
					})
				} else {
					bh.sql2result[globalScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
				}
			}

			ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
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
					Level:  tree.PRIVILEGE_LEVEL_TYPE_DATABASE_STAR,
					DbName: "db",
				},
			},
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
					{Type: tree.PRIVILEGE_TYPE_STATIC_INSERT},
				},
				ObjType: tree.OBJECT_TYPE_TABLE,
				Level: &tree.PrivilegeLevel{
					Level:   tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE,
					DbName:  "db",
					TabName: "t1",
				},
			},
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
					{Type: tree.PRIVILEGE_TYPE_STATIC_INSERT},
				},
				ObjType: tree.OBJECT_TYPE_TABLE,
				Level: &tree.PrivilegeLevel{
					Level:   tree.PRIVILEGE_LEVEL_TYPE_TABLE,
					TabName: "t1",
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

			// Mock getDatabaseOrTableId for scoped levels.
			if stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_STAR ||
				stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_DATABASE_STAR {
				dbName := stmt.Level.DbName
				if dbName == "" {
					dbName = ses.GetDatabaseName()
				}
				checkSql, _ := getSqlForCheckDatabase(ses.GetTxnHandler().GetTxnCtx(), dbName)
				bh.sql2result[checkSql] = newMrsForCheckDatabase([][]interface{}{{10001}})
			} else if stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE ||
				stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_TABLE {
				dbName := stmt.Level.DbName
				tabName := stmt.Level.TabName
				if dbName == "" {
					dbName = ses.GetDatabaseName()
				}
				checkSql, _ := getSqlForCheckDatabaseTable(ses.GetTxnHandler().GetTxnCtx(), dbName, tabName)
				bh.sql2result[checkSql] = newMrsForCheckDatabaseTable([][]interface{}{{10001}})
				checkDbSql, _ := getSqlForCheckDatabase(ses.GetTxnHandler().GetTxnCtx(), dbName)
				bh.sql2result[checkDbSql] = newMrsForCheckDatabase([][]interface{}{{10002}})

				for _, p := range stmt.Privileges {
					privType, _ := convertAstPrivilegeTypeToPrivilegeType(context.TODO(), p.Type, stmt.ObjType)
					scopedLevel := privilegeLevelDatabaseTable
					if stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_TABLE {
						scopedLevel = privilegeLevelTable
					}
					scopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
						int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
						objectTypeTable, 10001, scopedLevel)
					bh.sql2result[scopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
				}
			}
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
				bh.sql2result[sql] = newMrsForPrivilegeWGO([][]interface{}{})

				objType := objectTypeTable
				if stmt.ObjType == tree.OBJECT_TYPE_VIEW {
					objType = objectTypeView
				}
				objTypeScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjType(
					int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership), objType)
				if i == 0 {
					bh.sql2result[objTypeScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
				} else {
					bh.sql2result[objTypeScopedSql] = newMrsForPrivilegeWGO([][]interface{}{
						{ses.GetTenantInfo().GetDefaultRoleID()},
					})
				}
				globalScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
					int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
					objType, objectIDAll, privilegeLevelStarStar)
				bh.sql2result[globalScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})

				// Mock obj-scoped WGO query for DATABASE_TABLE/TABLE levels
				if stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_STAR ||
					stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_DATABASE_STAR ||
					stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE ||
					stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_TABLE {
					scopedLevel := privilegeLevelStar
					if stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_DATABASE_STAR {
						scopedLevel = privilegeLevelDatabaseStar
					} else if stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE {
						scopedLevel = privilegeLevelDatabaseTable
					} else if stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_TABLE {
						scopedLevel = privilegeLevelTable
					}
					scopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
						int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
						objectTypeTable, 10001, scopedLevel)
					if i == 0 {
						bh.sql2result[scopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
					} else {
						bh.sql2result[scopedSql] = newMrsForPrivilegeWGO([][]interface{}{
							{ses.GetTenantInfo().GetDefaultRoleID()},
						})
					}
					if stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE ||
						stmt.Level.Level == tree.PRIVILEGE_LEVEL_TYPE_TABLE {
						dbScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
							int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
							objectTypeTable, 10002, privilegeLevelDatabaseStar)
						if i == 0 {
							bh.sql2result[dbScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
						} else {
							bh.sql2result[dbScopedSql] = newMrsForPrivilegeWGO([][]interface{}{
								{ses.GetTenantInfo().GetDefaultRoleID()},
							})
						}
					}
				}
			}

			ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
			convey.So(err, convey.ShouldBeNil)
			convey.So(ok, convey.ShouldBeFalse)
		}
	})

	convey.Convey("grant privilege [ObjectType: Table] global star star covers database star", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantPrivilege{
			Privileges: []*tree.Privilege{
				{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
			},
			ObjType: tree.OBJECT_TYPE_TABLE,
			Level: &tree.PrivilegeLevel{
				Level:  tree.PRIVILEGE_LEVEL_TYPE_DATABASE_STAR,
				DbName: "db",
			},
		}

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
		bh.init()

		checkSql, err := getSqlForCheckDatabase(ses.GetTxnHandler().GetTxnCtx(), "db")
		convey.So(err, convey.ShouldBeNil)
		bh.sql2result[checkSql] = newMrsForCheckDatabase([][]interface{}{{10001}})

		privType, err := convertAstPrivilegeTypeToPrivilegeType(context.TODO(), stmt.Privileges[0].Type, stmt.ObjType)
		convey.So(err, convey.ShouldBeNil)
		objScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10001, privilegeLevelDatabaseStar)
		bh.sql2result[objScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		globalScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[globalScopedSql] = newMrsForPrivilegeWGO([][]interface{}{
			{ses.GetTenantInfo().GetDefaultRoleID()},
		})

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("grant privilege [ObjectType: Table] database star covers same database table", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantPrivilege{
			Privileges: []*tree.Privilege{
				{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
			},
			ObjType: tree.OBJECT_TYPE_TABLE,
			Level: &tree.PrivilegeLevel{
				Level:   tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE,
				DbName:  "db",
				TabName: "t1",
			},
		}

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
		ctx := ses.GetTxnHandler().GetTxnCtx()
		bh.init()

		checkTableSql, err := getSqlForCheckDatabaseTable(ctx, "db", "t1")
		convey.So(err, convey.ShouldBeNil)
		bh.sql2result[checkTableSql] = newMrsForCheckDatabaseTable([][]interface{}{{10001}})
		checkDbSql, err := getSqlForCheckDatabase(ctx, "db")
		convey.So(err, convey.ShouldBeNil)
		bh.sql2result[checkDbSql] = newMrsForCheckDatabase([][]interface{}{{10002}})

		privType, err := convertAstPrivilegeTypeToPrivilegeType(context.TODO(), stmt.Privileges[0].Type, stmt.ObjType)
		convey.So(err, convey.ShouldBeNil)
		objScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10001, privilegeLevelDatabaseTable)
		bh.sql2result[objScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		globalScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[globalScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		dbScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10002, privilegeLevelDatabaseStar)
		bh.sql2result[dbScopedSql] = newMrsForPrivilegeWGO([][]interface{}{
			{ses.GetTenantInfo().GetDefaultRoleID()},
		})

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ctx, ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("grant privilege [ObjectType: Table] database star does not cover global star star", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantPrivilege{
			Privileges: []*tree.Privilege{
				{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
			},
			ObjType: tree.OBJECT_TYPE_TABLE,
			Level: &tree.PrivilegeLevel{
				Level: tree.PRIVILEGE_LEVEL_TYPE_STAR_STAR,
			},
		}

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
		ctx := ses.GetTxnHandler().GetTxnCtx()
		bh.init()

		privType, err := convertAstPrivilegeTypeToPrivilegeType(context.TODO(), stmt.Privileges[0].Type, stmt.ObjType)
		convey.So(err, convey.ShouldBeNil)
		globalScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[globalScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		objTypeScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjType(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership), objectTypeTable)
		bh.sql2result[objTypeScopedSql] = newMrsForPrivilegeWGO([][]interface{}{
			{ses.GetTenantInfo().GetDefaultRoleID()},
		})
		unscopedSql := getSqlForCheckRoleHasPrivilegeWGODependsOnPrivType(privType)
		bh.sql2result[unscopedSql] = newMrsForPrivilegeWGO([][]interface{}{
			{ses.GetTenantInfo().GetDefaultRoleID()},
		})

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ctx, ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})

	convey.Convey("grant privilege [ObjectType: Table] table grant option does not cover global star star", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantPrivilege{
			Privileges: []*tree.Privilege{
				{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
			},
			ObjType: tree.OBJECT_TYPE_TABLE,
			Level: &tree.PrivilegeLevel{
				Level: tree.PRIVILEGE_LEVEL_TYPE_STAR_STAR,
			},
		}

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
		ctx := ses.GetTxnHandler().GetTxnCtx()
		bh.init()

		privType, err := convertAstPrivilegeTypeToPrivilegeType(context.TODO(), stmt.Privileges[0].Type, stmt.ObjType)
		convey.So(err, convey.ShouldBeNil)
		globalScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[globalScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		objTypeScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjType(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership), objectTypeTable)
		bh.sql2result[objTypeScopedSql] = newMrsForPrivilegeWGO([][]interface{}{
			{ses.GetTenantInfo().GetDefaultRoleID()},
		})
		unscopedSql := getSqlForCheckRoleHasPrivilegeWGODependsOnPrivType(privType)
		bh.sql2result[unscopedSql] = newMrsForPrivilegeWGO([][]interface{}{
			{ses.GetTenantInfo().GetDefaultRoleID()},
		})

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ctx, ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})

	convey.Convey("grant privilege [ObjectType: Table] other database star does not cover database star", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantPrivilege{
			Privileges: []*tree.Privilege{
				{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
			},
			ObjType: tree.OBJECT_TYPE_TABLE,
			Level: &tree.PrivilegeLevel{
				Level:  tree.PRIVILEGE_LEVEL_TYPE_DATABASE_STAR,
				DbName: "db1",
			},
		}

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
		bh.init()

		checkSql, err := getSqlForCheckDatabase(ses.GetTxnHandler().GetTxnCtx(), "db1")
		convey.So(err, convey.ShouldBeNil)
		bh.sql2result[checkSql] = newMrsForCheckDatabase([][]interface{}{{10001}})

		privType, err := convertAstPrivilegeTypeToPrivilegeType(context.TODO(), stmt.Privileges[0].Type, stmt.ObjType)
		convey.So(err, convey.ShouldBeNil)
		objScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10001, privilegeLevelDatabaseStar)
		bh.sql2result[objScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		globalScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[globalScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		otherDbScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10002, privilegeLevelDatabaseStar)
		bh.sql2result[otherDbScopedSql] = newMrsForPrivilegeWGO([][]interface{}{
			{ses.GetTenantInfo().GetDefaultRoleID()},
		})

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})

	convey.Convey("grant privilege [ObjectType: Table] other database star does not cover database table", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantPrivilege{
			Privileges: []*tree.Privilege{
				{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
			},
			ObjType: tree.OBJECT_TYPE_TABLE,
			Level: &tree.PrivilegeLevel{
				Level:   tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE,
				DbName:  "db1",
				TabName: "t1",
			},
		}

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
		ctx := ses.GetTxnHandler().GetTxnCtx()
		bh.init()

		checkTableSql, err := getSqlForCheckDatabaseTable(ctx, "db1", "t1")
		convey.So(err, convey.ShouldBeNil)
		bh.sql2result[checkTableSql] = newMrsForCheckDatabaseTable([][]interface{}{{10001}})
		checkDbSql, err := getSqlForCheckDatabase(ctx, "db1")
		convey.So(err, convey.ShouldBeNil)
		bh.sql2result[checkDbSql] = newMrsForCheckDatabase([][]interface{}{{10002}})

		privType, err := convertAstPrivilegeTypeToPrivilegeType(context.TODO(), stmt.Privileges[0].Type, stmt.ObjType)
		convey.So(err, convey.ShouldBeNil)
		objScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10001, privilegeLevelDatabaseTable)
		bh.sql2result[objScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		globalScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[globalScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		dbScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10002, privilegeLevelDatabaseStar)
		bh.sql2result[dbScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		otherDbScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10003, privilegeLevelDatabaseStar)
		bh.sql2result[otherDbScopedSql] = newMrsForPrivilegeWGO([][]interface{}{
			{ses.GetTenantInfo().GetDefaultRoleID()},
		})

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ctx, ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})

	convey.Convey("grant privilege [ObjectType: View] database star covers same database view", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantPrivilege{
			Privileges: []*tree.Privilege{
				{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
			},
			ObjType: tree.OBJECT_TYPE_VIEW,
			Level: &tree.PrivilegeLevel{
				Level:   tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE,
				DbName:  "db",
				TabName: "v1",
			},
		}

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
		ctx := ses.GetTxnHandler().GetTxnCtx()
		bh.init()

		checkViewSql, err := getSqlForCheckDatabaseView(ctx, "db", "v1")
		convey.So(err, convey.ShouldBeNil)
		bh.sql2result[checkViewSql] = newMrsForCheckDatabaseTable([][]interface{}{{10002}})
		checkDbSql, err := getSqlForCheckDatabase(ctx, "db")
		convey.So(err, convey.ShouldBeNil)
		bh.sql2result[checkDbSql] = newMrsForCheckDatabase([][]interface{}{{10001}})

		privType, err := convertAstPrivilegeTypeToPrivilegeType(context.TODO(), stmt.Privileges[0].Type, stmt.ObjType)
		convey.So(err, convey.ShouldBeNil)
		viewObjScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeView, 10002, privilegeLevelDatabaseTable)
		bh.sql2result[viewObjScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		viewGlobalScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeView, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[viewGlobalScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		viewDbScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeView, 10001, privilegeLevelDatabaseStar)
		bh.sql2result[viewDbScopedSql] = newMrsForPrivilegeWGO([][]interface{}{
			{ses.GetTenantInfo().GetDefaultRoleID()},
		})
		legacyObjScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10002, privilegeLevelDatabaseTable)
		bh.sql2result[legacyObjScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		legacyGlobalScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[legacyGlobalScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		legacyDbScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10001, privilegeLevelDatabaseStar)
		bh.sql2result[legacyDbScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ctx, ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("grant privilege [ObjectType: View] database scoped view grant succeeds on legacy table record only", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantPrivilege{
			Privileges: []*tree.Privilege{
				{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
			},
			ObjType: tree.OBJECT_TYPE_VIEW,
			Level: &tree.PrivilegeLevel{
				Level:   tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE,
				DbName:  "db",
				TabName: "v2",
			},
		}

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
		ctx := ses.GetTxnHandler().GetTxnCtx()
		bh.init()

		checkViewSql, err := getSqlForCheckDatabaseView(ctx, "db", "v2")
		require.NoError(t, err)
		bh.sql2result[checkViewSql] = newMrsForCheckDatabaseTable([][]interface{}{{10002}})
		checkDbSql, err := getSqlForCheckDatabase(ctx, "db")
		require.NoError(t, err)
		bh.sql2result[checkDbSql] = newMrsForCheckDatabase([][]interface{}{{10001}})

		privType, err := convertAstPrivilegeTypeToPrivilegeType(context.TODO(), stmt.Privileges[0].Type, stmt.ObjType)
		require.NoError(t, err)
		viewObjScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeView, 10002, privilegeLevelDatabaseTable)
		bh.sql2result[viewObjScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		viewGlobalScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeView, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[viewGlobalScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		viewDbScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeView, 10001, privilegeLevelDatabaseStar)
		bh.sql2result[viewDbScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		legacyObjScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10002, privilegeLevelDatabaseTable)
		bh.sql2result[legacyObjScopedSql] = newMrsForPrivilegeWGO([][]interface{}{
			{ses.GetTenantInfo().GetDefaultRoleID()},
		})
		legacyGlobalScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[legacyGlobalScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		legacyDbScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10001, privilegeLevelDatabaseStar)
		bh.sql2result[legacyDbScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ctx, ses, stmt)
		require.NoError(t, err)
		require.True(t, ok)
	})

	convey.Convey("grant privilege [ObjectType: View] legacy table database star does not cover view grant", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantPrivilege{
			Privileges: []*tree.Privilege{
				{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
			},
			ObjType: tree.OBJECT_TYPE_VIEW,
			Level: &tree.PrivilegeLevel{
				Level:   tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE,
				DbName:  "db",
				TabName: "v3",
			},
		}

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
		ctx := ses.GetTxnHandler().GetTxnCtx()
		bh.init()

		checkViewSql, err := getSqlForCheckDatabaseView(ctx, "db", "v3")
		require.NoError(t, err)
		bh.sql2result[checkViewSql] = newMrsForCheckDatabaseTable([][]interface{}{{10002}})
		checkDbSql, err := getSqlForCheckDatabase(ctx, "db")
		require.NoError(t, err)
		bh.sql2result[checkDbSql] = newMrsForCheckDatabase([][]interface{}{{10001}})

		privType, err := convertAstPrivilegeTypeToPrivilegeType(context.TODO(), stmt.Privileges[0].Type, stmt.ObjType)
		require.NoError(t, err)
		viewObjScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeView, 10002, privilegeLevelDatabaseTable)
		bh.sql2result[viewObjScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		viewGlobalScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeView, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[viewGlobalScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		viewDbScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeView, 10001, privilegeLevelDatabaseStar)
		bh.sql2result[viewDbScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		legacyObjScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10002, privilegeLevelDatabaseTable)
		bh.sql2result[legacyObjScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		legacyGlobalScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[legacyGlobalScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		legacyDbScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10001, privilegeLevelDatabaseStar)
		bh.sql2result[legacyDbScopedSql] = newMrsForPrivilegeWGO([][]interface{}{
			{ses.GetTenantInfo().GetDefaultRoleID()},
		})

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ctx, ses, stmt)
		require.NoError(t, err)
		require.False(t, ok)
	})

	convey.Convey("grant privilege [ObjectType: View] legacy table global star star does not cover view grant", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantPrivilege{
			Privileges: []*tree.Privilege{
				{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
			},
			ObjType: tree.OBJECT_TYPE_VIEW,
			Level: &tree.PrivilegeLevel{
				Level: tree.PRIVILEGE_LEVEL_TYPE_STAR_STAR,
			},
		}

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
		ctx := ses.GetTxnHandler().GetTxnCtx()
		bh.init()

		privType, err := convertAstPrivilegeTypeToPrivilegeType(context.TODO(), stmt.Privileges[0].Type, stmt.ObjType)
		require.NoError(t, err)
		viewGlobalScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeView, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[viewGlobalScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		legacyGlobalScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[legacyGlobalScopedSql] = newMrsForPrivilegeWGO([][]interface{}{
			{ses.GetTenantInfo().GetDefaultRoleID()},
		})

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ctx, ses, stmt)
		require.NoError(t, err)
		require.False(t, ok)
	})

	convey.Convey("grant privilege [ObjectType: View] same database view ownership covers view grant", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantPrivilege{
			Privileges: []*tree.Privilege{
				{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
			},
			ObjType: tree.OBJECT_TYPE_VIEW,
			Level: &tree.PrivilegeLevel{
				Level:   tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE,
				DbName:  "db",
				TabName: "v1",
			},
		}

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
		ctx := ses.GetTxnHandler().GetTxnCtx()
		bh.init()

		checkViewSql, err := getSqlForCheckDatabaseView(ctx, "db", "v1")
		convey.So(err, convey.ShouldBeNil)
		bh.sql2result[checkViewSql] = newMrsForCheckDatabaseTable([][]interface{}{{10002}})
		checkDbSql, err := getSqlForCheckDatabase(ctx, "db")
		convey.So(err, convey.ShouldBeNil)
		bh.sql2result[checkDbSql] = newMrsForCheckDatabase([][]interface{}{{10001}})

		ownershipObjSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeView, 10002, privilegeLevelDatabaseTable)
		bh.sql2result[ownershipObjSql] = newMrsForPrivilegeWGO([][]interface{}{
			{ses.GetTenantInfo().GetDefaultRoleID()},
		})
		ownershipGlobalSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeView, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[ownershipGlobalSql] = newMrsForPrivilegeWGO([][]interface{}{})
		ownershipDbSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeView, 10001, privilegeLevelDatabaseStar)
		bh.sql2result[ownershipDbSql] = newMrsForPrivilegeWGO([][]interface{}{})
		legacyObjSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10002, privilegeLevelDatabaseTable)
		bh.sql2result[legacyObjSql] = newMrsForPrivilegeWGO([][]interface{}{})
		legacyGlobalSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[legacyGlobalSql] = newMrsForPrivilegeWGO([][]interface{}{})
		legacyDbSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10001, privilegeLevelDatabaseStar)
		bh.sql2result[legacyDbSql] = newMrsForPrivilegeWGO([][]interface{}{})

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ctx, ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("grant privilege [ObjectType: View] other view grant option does not cover view grant", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantPrivilege{
			Privileges: []*tree.Privilege{
				{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
			},
			ObjType: tree.OBJECT_TYPE_VIEW,
			Level: &tree.PrivilegeLevel{
				Level:   tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE,
				DbName:  "db",
				TabName: "v1",
			},
		}

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
		ctx := ses.GetTxnHandler().GetTxnCtx()
		bh.init()

		checkViewSql, err := getSqlForCheckDatabaseView(ctx, "db", "v1")
		convey.So(err, convey.ShouldBeNil)
		bh.sql2result[checkViewSql] = newMrsForCheckDatabaseTable([][]interface{}{{10002}})
		checkDbSql, err := getSqlForCheckDatabase(ctx, "db")
		convey.So(err, convey.ShouldBeNil)
		bh.sql2result[checkDbSql] = newMrsForCheckDatabase([][]interface{}{{10001}})

		privType, err := convertAstPrivilegeTypeToPrivilegeType(context.TODO(), stmt.Privileges[0].Type, stmt.ObjType)
		convey.So(err, convey.ShouldBeNil)
		viewObjScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeView, 10002, privilegeLevelDatabaseTable)
		bh.sql2result[viewObjScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		viewGlobalScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeView, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[viewGlobalScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		viewDbScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeView, 10001, privilegeLevelDatabaseStar)
		bh.sql2result[viewDbScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		otherViewScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeView, 10003, privilegeLevelDatabaseTable)
		bh.sql2result[otherViewScopedSql] = newMrsForPrivilegeWGO([][]interface{}{
			{ses.GetTenantInfo().GetDefaultRoleID()},
		})
		legacyObjScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10002, privilegeLevelDatabaseTable)
		bh.sql2result[legacyObjScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		legacyGlobalScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[legacyGlobalScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		legacyDbScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10001, privilegeLevelDatabaseStar)
		bh.sql2result[legacyDbScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		legacyOtherViewScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10003, privilegeLevelDatabaseTable)
		bh.sql2result[legacyOtherViewScopedSql] = newMrsForPrivilegeWGO([][]interface{}{
			{ses.GetTenantInfo().GetDefaultRoleID()},
		})

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ctx, ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})

	convey.Convey("grant privilege [ObjectType: Table] missing table is allowed by global grant option", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantPrivilege{
			Privileges: []*tree.Privilege{
				{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
			},
			ObjType: tree.OBJECT_TYPE_TABLE,
			Level: &tree.PrivilegeLevel{
				Level:   tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE,
				DbName:  "db",
				TabName: "missing_t",
			},
		}

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
		ctx := ses.GetTxnHandler().GetTxnCtx()
		bh.init()

		checkTableSql, err := getSqlForCheckDatabaseTable(ctx, "db", "missing_t")
		convey.So(err, convey.ShouldBeNil)
		bh.sql2result[checkTableSql] = newMrsForCheckDatabaseTable([][]interface{}{})

		privType, err := convertAstPrivilegeTypeToPrivilegeType(context.TODO(), stmt.Privileges[0].Type, stmt.ObjType)
		convey.So(err, convey.ShouldBeNil)

		// Holding the global *.* grant option still authorizes the grant on a
		// missing object; the obj_id-independent scope does not depend on the table.
		globalScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[globalScopedSql] = newMrsForPrivilegeWGO([][]interface{}{
			{ses.GetTenantInfo().GetDefaultRoleID()},
		})
		checkDbSql, err := getSqlForCheckDatabase(ctx, "db")
		convey.So(err, convey.ShouldBeNil)
		bh.sql2result[checkDbSql] = newMrsForCheckDatabase([][]interface{}{{10002}})
		dbScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10002, privilegeLevelDatabaseStar)
		bh.sql2result[dbScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ctx, ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("grant privilege [ObjectType: Table] missing table is not authorized by an unrelated same-type grant", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantPrivilege{
			Privileges: []*tree.Privilege{
				{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
			},
			ObjType: tree.OBJECT_TYPE_TABLE,
			Level: &tree.PrivilegeLevel{
				Level:   tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE,
				DbName:  "db",
				TabName: "missing_t",
			},
		}

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
		ctx := ses.GetTxnHandler().GetTxnCtx()
		bh.init()

		checkTableSql, err := getSqlForCheckDatabaseTable(ctx, "db", "missing_t")
		convey.So(err, convey.ShouldBeNil)
		bh.sql2result[checkTableSql] = newMrsForCheckDatabaseTable([][]interface{}{})

		privType, err := convertAstPrivilegeTypeToPrivilegeType(context.TODO(), stmt.Privileges[0].Type, stmt.ObjType)
		convey.So(err, convey.ShouldBeNil)

		// Neither the global nor the db-wide scope grants this role grant option.
		globalScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[globalScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		checkDbSql, err := getSqlForCheckDatabase(ctx, "db")
		convey.So(err, convey.ShouldBeNil)
		bh.sql2result[checkDbSql] = newMrsForCheckDatabase([][]interface{}{{10002}})
		dbScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10002, privilegeLevelDatabaseStar)
		bh.sql2result[dbScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})

		// The role only holds grant option on an unrelated same-type object. The
		// obj_type-only query would return it, but it must not be consulted nor
		// satisfy authorization for a missing object.
		objTypeScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjType(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership), objectTypeTable)
		bh.sql2result[objTypeScopedSql] = newMrsForPrivilegeWGO([][]interface{}{
			{ses.GetTenantInfo().GetDefaultRoleID()},
		})

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ctx, ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
		convey.So(strings.Join(bh.executedSQLs, "\n"), convey.ShouldNotContainSubstring, objTypeScopedSql)
	})

	convey.Convey("grant privilege [ObjectType: View] global view star star covers database view", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantPrivilege{
			Privileges: []*tree.Privilege{
				{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
			},
			ObjType: tree.OBJECT_TYPE_VIEW,
			Level: &tree.PrivilegeLevel{
				Level:   tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE,
				DbName:  "db",
				TabName: "v1",
			},
		}

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
		ctx := ses.GetTxnHandler().GetTxnCtx()
		bh.init()

		checkSql, err := getSqlForCheckDatabaseView(ctx, "db", "v1")
		convey.So(err, convey.ShouldBeNil)
		bh.sql2result[checkSql] = newMrsForCheckDatabaseTable([][]interface{}{{10002}})
		checkDbSql, err := getSqlForCheckDatabase(ctx, "db")
		convey.So(err, convey.ShouldBeNil)
		bh.sql2result[checkDbSql] = newMrsForCheckDatabase([][]interface{}{{10001}})

		privType, err := convertAstPrivilegeTypeToPrivilegeType(context.TODO(), stmt.Privileges[0].Type, stmt.ObjType)
		convey.So(err, convey.ShouldBeNil)
		viewObjScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeView, 10002, privilegeLevelDatabaseTable)
		bh.sql2result[viewObjScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		viewGlobalScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeView, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[viewGlobalScopedSql] = newMrsForPrivilegeWGO([][]interface{}{
			{ses.GetTenantInfo().GetDefaultRoleID()},
		})
		viewDbScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeView, 10001, privilegeLevelDatabaseStar)
		bh.sql2result[viewDbScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		legacyObjScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10002, privilegeLevelDatabaseTable)
		bh.sql2result[legacyObjScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		legacyGlobalScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[legacyGlobalScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		legacyDbScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10001, privilegeLevelDatabaseStar)
		bh.sql2result[legacyDbScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ctx, ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("grant privilege [ObjectType: Database] same database grant option covers create table", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantPrivilege{
			Privileges: []*tree.Privilege{
				{Type: tree.PRIVILEGE_TYPE_STATIC_CREATE_TABLE},
			},
			ObjType: tree.OBJECT_TYPE_DATABASE,
			Level: &tree.PrivilegeLevel{
				Level:   tree.PRIVILEGE_LEVEL_TYPE_TABLE,
				TabName: "db1",
			},
		}

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
		ctx := ses.GetTxnHandler().GetTxnCtx()
		bh.init()

		checkDbSql, err := getSqlForCheckDatabase(ctx, "db1")
		convey.So(err, convey.ShouldBeNil)
		bh.sql2result[checkDbSql] = newMrsForCheckDatabase([][]interface{}{{10001}})

		scopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndExactLevel(
			int64(PrivilegeTypeCreateTable), int64(PrivilegeTypeDatabaseAll), int64(PrivilegeTypeDatabaseOwnership),
			objectTypeDatabase, 10001, privilegeLevelDatabase)
		bh.sql2result[scopedSql] = newMrsForPrivilegeWGO([][]interface{}{
			{ses.GetTenantInfo().GetDefaultRoleID()},
		})
		globalSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndExactLevel(
			int64(PrivilegeTypeCreateTable), int64(PrivilegeTypeDatabaseAll), int64(PrivilegeTypeDatabaseOwnership),
			objectTypeDatabase, objectIDAll, privilegeLevelStar)
		bh.sql2result[globalSql] = newMrsForPrivilegeWGO([][]interface{}{})
		globalStarStarSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndExactLevel(
			int64(PrivilegeTypeCreateTable), int64(PrivilegeTypeDatabaseAll), int64(PrivilegeTypeDatabaseOwnership),
			objectTypeDatabase, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[globalStarStarSql] = newMrsForPrivilegeWGO([][]interface{}{})

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ctx, ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("grant privilege [ObjectType: Database] other database grant option does not cover create table", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantPrivilege{
			Privileges: []*tree.Privilege{
				{Type: tree.PRIVILEGE_TYPE_STATIC_CREATE_TABLE},
			},
			ObjType: tree.OBJECT_TYPE_DATABASE,
			Level: &tree.PrivilegeLevel{
				Level:   tree.PRIVILEGE_LEVEL_TYPE_TABLE,
				TabName: "db1",
			},
		}

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
		ctx := ses.GetTxnHandler().GetTxnCtx()
		bh.init()

		checkDbSql, err := getSqlForCheckDatabase(ctx, "db1")
		convey.So(err, convey.ShouldBeNil)
		bh.sql2result[checkDbSql] = newMrsForCheckDatabase([][]interface{}{{10001}})

		scopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndExactLevel(
			int64(PrivilegeTypeCreateTable), int64(PrivilegeTypeDatabaseAll), int64(PrivilegeTypeDatabaseOwnership),
			objectTypeDatabase, 10001, privilegeLevelDatabase)
		bh.sql2result[scopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		globalSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndExactLevel(
			int64(PrivilegeTypeCreateTable), int64(PrivilegeTypeDatabaseAll), int64(PrivilegeTypeDatabaseOwnership),
			objectTypeDatabase, objectIDAll, privilegeLevelStar)
		bh.sql2result[globalSql] = newMrsForPrivilegeWGO([][]interface{}{})
		globalStarStarSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndExactLevel(
			int64(PrivilegeTypeCreateTable), int64(PrivilegeTypeDatabaseAll), int64(PrivilegeTypeDatabaseOwnership),
			objectTypeDatabase, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[globalStarStarSql] = newMrsForPrivilegeWGO([][]interface{}{})
		otherDbScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndExactLevel(
			int64(PrivilegeTypeCreateTable), int64(PrivilegeTypeDatabaseAll), int64(PrivilegeTypeDatabaseOwnership),
			objectTypeDatabase, 10002, privilegeLevelDatabase)
		bh.sql2result[otherDbScopedSql] = newMrsForPrivilegeWGO([][]interface{}{
			{ses.GetTenantInfo().GetDefaultRoleID()},
		})
		unscopedSql := getSqlForCheckRoleHasPrivilegeWGODependsOnPrivType(PrivilegeTypeCreateTable)
		bh.sql2result[unscopedSql] = newMrsForPrivilegeWGO([][]interface{}{
			{ses.GetTenantInfo().GetDefaultRoleID()},
		})

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ctx, ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
		convey.So(strings.Join(bh.executedSQLs, "\n"), convey.ShouldNotContainSubstring, otherDbScopedSql)
		convey.So(strings.Join(bh.executedSQLs, "\n"), convey.ShouldNotContainSubstring, unscopedSql)
	})

	convey.Convey("grant privilege [ObjectType: Database] global star star grant option covers database star", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantPrivilege{
			Privileges: []*tree.Privilege{
				{Type: tree.PRIVILEGE_TYPE_STATIC_DROP_TABLE},
			},
			ObjType: tree.OBJECT_TYPE_DATABASE,
			Level: &tree.PrivilegeLevel{
				Level: tree.PRIVILEGE_LEVEL_TYPE_STAR,
			},
		}

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
		ctx := ses.GetTxnHandler().GetTxnCtx()
		bh.init()

		globalSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndExactLevel(
			int64(PrivilegeTypeDropTable), int64(PrivilegeTypeDatabaseAll), int64(PrivilegeTypeDatabaseOwnership),
			objectTypeDatabase, objectIDAll, privilegeLevelStar)
		bh.sql2result[globalSql] = newMrsForPrivilegeWGO([][]interface{}{})
		globalStarStarSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndExactLevel(
			int64(PrivilegeTypeDropTable), int64(PrivilegeTypeDatabaseAll), int64(PrivilegeTypeDatabaseOwnership),
			objectTypeDatabase, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[globalStarStarSql] = newMrsForPrivilegeWGO([][]interface{}{
			{ses.GetTenantInfo().GetDefaultRoleID()},
		})
		unscopedSql := getSqlForCheckRoleHasPrivilegeWGODependsOnPrivType(PrivilegeTypeDropTable)
		bh.sql2result[unscopedSql] = newMrsForPrivilegeWGO([][]interface{}{})

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ctx, ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(strings.Join(bh.executedSQLs, "\n"), convey.ShouldContainSubstring, globalSql)
		convey.So(strings.Join(bh.executedSQLs, "\n"), convey.ShouldContainSubstring, globalStarStarSql)
		convey.So(strings.Join(bh.executedSQLs, "\n"), convey.ShouldNotContainSubstring, unscopedSql)
	})

	convey.Convey("grant privilege [ObjectType: Database] inherited role grant option covers database star", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantPrivilege{
			Privileges: []*tree.Privilege{
				{Type: tree.PRIVILEGE_TYPE_STATIC_DROP_TABLE},
			},
			ObjType: tree.OBJECT_TYPE_DATABASE,
			Level: &tree.PrivilegeLevel{
				Level: tree.PRIVILEGE_LEVEL_TYPE_STAR,
			},
		}

		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		internRoleID := int64(1001)
		devRoleID := int64(1002)
		testRoleID := int64(1003)
		ses.tenant = &TenantInfo{
			Tenant:        "xxx",
			User:          "xxx",
			DefaultRole:   "intern",
			TenantID:      1001,
			UserID:        1001,
			DefaultRoleID: uint32(internRoleID),
		}
		ctx := ses.GetTxnHandler().GetTxnCtx()
		bh.init()

		globalSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndExactLevel(
			int64(PrivilegeTypeDropTable), int64(PrivilegeTypeDatabaseAll), int64(PrivilegeTypeDatabaseOwnership),
			objectTypeDatabase, objectIDAll, privilegeLevelStar)
		bh.sql2result[globalSql] = newMrsForPrivilegeWGO([][]interface{}{
			{testRoleID},
		})
		globalStarStarSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndExactLevel(
			int64(PrivilegeTypeDropTable), int64(PrivilegeTypeDatabaseAll), int64(PrivilegeTypeDatabaseOwnership),
			objectTypeDatabase, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[globalStarStarSql] = newMrsForPrivilegeWGO([][]interface{}{})
		bh.sql2result[getSqlForInheritedRoleIdOfRoleId(internRoleID)] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{
			{devRoleID, false},
		})
		bh.sql2result[getSqlForInheritedRoleIdOfRoleId(devRoleID)] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{
			{testRoleID, false},
		})
		bh.sql2result[getSqlForInheritedRoleIdOfRoleId(testRoleID)] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{})

		roleGrantWGOSql := getSqlForCheckRoleGrantWGO(testRoleID)

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ctx, ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(strings.Join(bh.executedSQLs, "\n"), convey.ShouldNotContainSubstring, roleGrantWGOSql)
	})

	convey.Convey("grant privilege [ObjectType: Table] inherited role grant option covers database table", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantPrivilege{
			Privileges: []*tree.Privilege{
				{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
			},
			ObjType: tree.OBJECT_TYPE_TABLE,
			Level: &tree.PrivilegeLevel{
				Level:   tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE,
				DbName:  "db",
				TabName: "t1",
			},
		}

		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		internRoleID := int64(1001)
		devRoleID := int64(1002)
		testRoleID := int64(1003)
		ses.tenant = &TenantInfo{
			Tenant:        "xxx",
			User:          "xxx",
			DefaultRole:   "intern",
			TenantID:      1001,
			UserID:        1001,
			DefaultRoleID: uint32(internRoleID),
		}
		ctx := ses.GetTxnHandler().GetTxnCtx()
		bh.init()

		checkTableSql, err := getSqlForCheckDatabaseTable(ctx, "db", "t1")
		convey.So(err, convey.ShouldBeNil)
		bh.sql2result[checkTableSql] = newMrsForCheckDatabaseTable([][]interface{}{{10001}})
		checkDbSql, err := getSqlForCheckDatabase(ctx, "db")
		convey.So(err, convey.ShouldBeNil)
		bh.sql2result[checkDbSql] = newMrsForCheckDatabase([][]interface{}{{10002}})

		objScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10001, privilegeLevelDatabaseTable)
		bh.sql2result[objScopedSql] = newMrsForPrivilegeWGO([][]interface{}{
			{testRoleID},
		})
		globalScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[globalScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		dbScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10002, privilegeLevelDatabaseStar)
		bh.sql2result[dbScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		bh.sql2result[getSqlForInheritedRoleIdOfRoleId(internRoleID)] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{
			{devRoleID, false},
		})
		bh.sql2result[getSqlForInheritedRoleIdOfRoleId(devRoleID)] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{
			{testRoleID, false},
		})
		bh.sql2result[getSqlForInheritedRoleIdOfRoleId(testRoleID)] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{})

		roleGrantWGOSql := getSqlForCheckRoleGrantWGO(testRoleID)

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ctx, ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(strings.Join(bh.executedSQLs, "\n"), convey.ShouldNotContainSubstring, roleGrantWGOSql)
	})

	convey.Convey("grant privilege [ObjectType: View] inherited role grant option covers database view", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantPrivilege{
			Privileges: []*tree.Privilege{
				{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
			},
			ObjType: tree.OBJECT_TYPE_VIEW,
			Level: &tree.PrivilegeLevel{
				Level:   tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE,
				DbName:  "db",
				TabName: "v1",
			},
		}

		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		internRoleID := int64(1001)
		devRoleID := int64(1002)
		testRoleID := int64(1003)
		ses.tenant = &TenantInfo{
			Tenant:        "xxx",
			User:          "xxx",
			DefaultRole:   "intern",
			TenantID:      1001,
			UserID:        1001,
			DefaultRoleID: uint32(internRoleID),
		}
		ctx := ses.GetTxnHandler().GetTxnCtx()
		bh.init()

		checkViewSql, err := getSqlForCheckDatabaseView(ctx, "db", "v1")
		convey.So(err, convey.ShouldBeNil)
		bh.sql2result[checkViewSql] = newMrsForCheckDatabaseTable([][]interface{}{{10002}})
		checkDbSql, err := getSqlForCheckDatabase(ctx, "db")
		convey.So(err, convey.ShouldBeNil)
		bh.sql2result[checkDbSql] = newMrsForCheckDatabase([][]interface{}{{10001}})

		viewObjScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeView, 10002, privilegeLevelDatabaseTable)
		bh.sql2result[viewObjScopedSql] = newMrsForPrivilegeWGO([][]interface{}{
			{testRoleID},
		})
		viewGlobalScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeView, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[viewGlobalScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		viewDbScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeView, 10001, privilegeLevelDatabaseStar)
		bh.sql2result[viewDbScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		legacyObjScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10002, privilegeLevelDatabaseTable)
		bh.sql2result[legacyObjScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		legacyGlobalScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[legacyGlobalScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		legacyDbScopedSql := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10001, privilegeLevelDatabaseStar)
		bh.sql2result[legacyDbScopedSql] = newMrsForPrivilegeWGO([][]interface{}{})
		bh.sql2result[getSqlForInheritedRoleIdOfRoleId(internRoleID)] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{
			{devRoleID, false},
		})
		bh.sql2result[getSqlForInheritedRoleIdOfRoleId(devRoleID)] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{
			{testRoleID, false},
		})
		bh.sql2result[getSqlForInheritedRoleIdOfRoleId(testRoleID)] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{})

		roleGrantWGOSql := getSqlForCheckRoleGrantWGO(testRoleID)

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ctx, ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(strings.Join(bh.executedSQLs, "\n"), convey.ShouldNotContainSubstring, roleGrantWGOSql)
	})

	convey.Convey("grant privilege [ObjectType: Account] inherited role grant option does not cover account star", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.GrantPrivilege{
			Privileges: []*tree.Privilege{
				{Type: tree.PRIVILEGE_TYPE_STATIC_DROP_DATABASE},
			},
			ObjType: tree.OBJECT_TYPE_ACCOUNT,
			Level: &tree.PrivilegeLevel{
				Level: tree.PRIVILEGE_LEVEL_TYPE_STAR,
			},
		}

		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		internRoleID := int64(1001)
		devRoleID := int64(1002)
		ses.tenant = &TenantInfo{
			Tenant:        "xxx",
			User:          "xxx",
			DefaultRole:   "intern",
			TenantID:      1001,
			UserID:        1001,
			DefaultRoleID: uint32(internRoleID),
		}
		ctx := ses.GetTxnHandler().GetTxnCtx()
		bh.init()

		wgoSql := getSqlForCheckRoleHasPrivilegeWGODependsOnPrivType(PrivilegeTypeDropDatabase)
		bh.sql2result[wgoSql] = newMrsForPrivilegeWGO([][]interface{}{
			{devRoleID},
		})
		roleGrantWGOSql := getSqlForCheckRoleGrantWGO(devRoleID)
		bh.sql2result[roleGrantWGOSql] = newMrsForRoleWGO([][]interface{}{})
		inheritedSql := getSqlForInheritedRoleIdOfRoleId(internRoleID)
		bh.sql2result[inheritedSql] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{
			{devRoleID, false},
		})

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ctx, ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
		convey.So(strings.Join(bh.executedSQLs, "\n"), convey.ShouldNotContainSubstring, inheritedSql)
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

			ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
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

			ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
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

			ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
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

			ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
			convey.So(err, convey.ShouldBeNil)
			convey.So(ok, convey.ShouldBeFalse)
		}
	})
}

func Test_determineRevokePrivilege(t *testing.T) {
	convey.Convey("revoke privilege [ObjectType: Table] AdminRole succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		// var stmts []*tree.RevokePrivilege

		// XXX FIXME: Go compiler is correct -- this test is busted.
		// we are looping over nil.
		// for _, stmt := range stmts {
		// 	priv := determinePrivilegeSetOfStatement(stmt)
		//	ses := newSes(priv, ctrl)

		//	ok, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		//	convey.So(err, convey.ShouldBeNil)
		//	convey.So(ok, convey.ShouldBeTrue)
		// }
	})
	convey.Convey("revoke privilege [ObjectType: Table] not AdminRole fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		// var stmts []*tree.RevokePrivilege

		// XXX FIXME: Go compiler is correct -- this test is busted.
		// we are looping over nil.
		// for _, stmt := range stmts {
		// 	priv := determinePrivilegeSetOfStatement(stmt)
		// 	ses := newSes(priv, ctrl)
		// 	ses.tenant = &TenantInfo{
		// 		Tenant:        "xxx",
		// 		User:          "xxx",
		// 		DefaultRole:   "xxx",
		// 		TenantID:      1001,
		// 		UserID:        1001,
		// 		DefaultRoleID: 1001,
		// 	}

		// 	ok, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		// 	convey.So(err, convey.ShouldBeNil)
		// 	convey.So(ok, convey.ShouldBeFalse)
		// }
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})
}

func TestCreateDatabaseOwnerRoleFollowsActiveInheritedRoleRoot(t *testing.T) {
	convey.Convey("create database owner role follows active inherited role root", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.CreateDatabase{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		ses.GetTenantInfo().SetDefaultRoleID(5)

		rowsOfMoUserGrant := [][]interface{}{
			{5, false},
		}
		roleIdsInMoRolePrivs := []int{5, 7}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}
		rowsOfMoRolePrivs[1][0] = [][]interface{}{{7, false}}
		rowsOfMoRolePrivs[1][1] = [][]interface{}{}

		roleIdsInMoRoleGrant := []int{5, 7}
		rowsOfMoRoleGrant := [][][]interface{}{
			{{7, false}},
			{},
		}
		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, roleIdsInMoRoleGrant, rowsOfMoRoleGrant, nil, nil)

		for _, entry := range priv.entries {
			pls, err := getPrivilegeLevelsOfObjectType(context.TODO(), entry.objType)
			convey.So(err, convey.ShouldBeNil)
			for _, pl := range pls {
				for _, roleId := range roleIdsInMoRolePrivs {
					sql, err := getSqlForPrivilege(context.TODO(), int64(roleId), entry, pl)
					convey.So(err, convey.ShouldBeNil)
					rows := [][]interface{}{}
					if roleId == 7 && entry.privilegeId == PrivilegeTypeCreateDatabase {
						rows = [][]interface{}{{roleId, false}}
					}
					sql2result[sql] = newMrsForWithGrantOptionPrivilege(rows)
				}
			}
		}

		bh := newBh(ctrl, sql2result)
		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(ses.GetDDLOwnerRoleID(), convey.ShouldEqual, uint32(5))
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})
}

func TestCreateTableOwnerRoleFollowsPrivilegeRole(t *testing.T) {
	convey.Convey("create table owner role follows secondary role privilege", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.CreateTable{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		ses.GetTenantInfo().SetDefaultRoleID(5)
		ses.GetTenantInfo().SetUseSecondaryRole(true)

		rowsOfMoUserGrant := [][]interface{}{
			{5, false},
			{7, false},
		}
		roleIdsInMoRolePrivs := []int{5, 7}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}
		rowsOfMoRolePrivs[0][2] = [][]interface{}{}
		rowsOfMoRolePrivs[1][0] = [][]interface{}{{7, false}}
		rowsOfMoRolePrivs[1][1] = [][]interface{}{}
		rowsOfMoRolePrivs[1][2] = [][]interface{}{}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, nil, nil, nil, nil)

		for _, entry := range priv.entries {
			pls, err := getPrivilegeLevelsOfObjectType(context.TODO(), entry.objType)
			convey.So(err, convey.ShouldBeNil)
			for _, pl := range pls {
				for _, roleId := range roleIdsInMoRolePrivs {
					sql, err := getSqlForPrivilege(context.TODO(), int64(roleId), entry, pl)
					convey.So(err, convey.ShouldBeNil)
					rows := [][]interface{}{}
					if roleId == 7 && entry.privilegeId == PrivilegeTypeCreateTable {
						rows = [][]interface{}{{7, false}}
					}
					sql2result[sql] = newMrsForWithGrantOptionPrivilege(rows)
				}
			}
		}
		sql2result[getSqlForInheritedRoleIdOfRoleId(5)] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{})
		sql2result[getSqlForInheritedRoleIdOfRoleId(7)] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{})

		bh := newBh(ctrl, sql2result)
		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(ses.GetDDLOwnerRoleID(), convey.ShouldEqual, uint32(7))
	})

	convey.Convey("create table owner role prefers primary role privilege", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.CreateTable{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		ses.GetTenantInfo().SetDefaultRoleID(5)
		ses.GetTenantInfo().SetUseSecondaryRole(true)

		rowsOfMoUserGrant := [][]interface{}{
			{5, false},
			{7, false},
		}
		roleIdsInMoRolePrivs := []int{5, 7}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}
		rowsOfMoRolePrivs[0][0] = [][]interface{}{{5, false}}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}
		rowsOfMoRolePrivs[0][2] = [][]interface{}{}
		rowsOfMoRolePrivs[1][0] = [][]interface{}{{7, false}}
		rowsOfMoRolePrivs[1][1] = [][]interface{}{}
		rowsOfMoRolePrivs[1][2] = [][]interface{}{}

		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, nil, nil, nil, nil)

		for _, entry := range priv.entries {
			pls, err := getPrivilegeLevelsOfObjectType(context.TODO(), entry.objType)
			convey.So(err, convey.ShouldBeNil)
			for _, pl := range pls {
				for _, roleId := range roleIdsInMoRolePrivs {
					sql, err := getSqlForPrivilege(context.TODO(), int64(roleId), entry, pl)
					convey.So(err, convey.ShouldBeNil)
					rows := [][]interface{}{}
					if entry.privilegeId == PrivilegeTypeCreateTable {
						rows = [][]interface{}{{roleId, false}}
					}
					sql2result[sql] = newMrsForWithGrantOptionPrivilege(rows)
				}
			}
		}
		sql2result[getSqlForInheritedRoleIdOfRoleId(5)] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{})
		sql2result[getSqlForInheritedRoleIdOfRoleId(7)] = newMrsForInheritedRoleIdOfRoleId([][]interface{}{})

		bh := newBh(ctrl, sql2result)
		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(ses.GetDDLOwnerRoleID(), convey.ShouldEqual, uint32(5))
	})

	convey.Convey("create table owner role follows active inherited role root", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.CreateTable{}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		ses.GetTenantInfo().SetDefaultRoleID(5)

		rowsOfMoUserGrant := [][]interface{}{
			{5, false},
		}
		roleIdsInMoRolePrivs := []int{5, 7}
		rowsOfMoRolePrivs := make([][][][]interface{}, len(roleIdsInMoRolePrivs))
		for i := 0; i < len(roleIdsInMoRolePrivs); i++ {
			rowsOfMoRolePrivs[i] = make([][][]interface{}, len(priv.entries))
		}
		rowsOfMoRolePrivs[0][0] = [][]interface{}{}
		rowsOfMoRolePrivs[0][1] = [][]interface{}{}
		rowsOfMoRolePrivs[0][2] = [][]interface{}{}
		rowsOfMoRolePrivs[1][0] = [][]interface{}{{7, false}}
		rowsOfMoRolePrivs[1][1] = [][]interface{}{}
		rowsOfMoRolePrivs[1][2] = [][]interface{}{}

		roleIdsInMoRoleGrant := []int{5, 7}
		rowsOfMoRoleGrant := [][][]interface{}{
			{{7, false}},
			{},
		}
		sql2result := makeSql2ExecResult2(0, rowsOfMoUserGrant, roleIdsInMoRolePrivs, priv.entries, rowsOfMoRolePrivs, roleIdsInMoRoleGrant, rowsOfMoRoleGrant, nil, nil)

		for _, entry := range priv.entries {
			pls, err := getPrivilegeLevelsOfObjectType(context.TODO(), entry.objType)
			convey.So(err, convey.ShouldBeNil)
			for _, pl := range pls {
				for _, roleId := range roleIdsInMoRolePrivs {
					sql, err := getSqlForPrivilege(context.TODO(), int64(roleId), entry, pl)
					convey.So(err, convey.ShouldBeNil)
					rows := [][]interface{}{}
					if roleId == 7 && entry.privilegeId == PrivilegeTypeCreateTable {
						rows = [][]interface{}{{roleId, false}}
					}
					sql2result[sql] = newMrsForWithGrantOptionPrivilege(rows)
				}
			}
		}

		bh := newBh(ctrl, sql2result)
		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(ses.GetDDLOwnerRoleID(), convey.ShouldEqual, uint32(5))
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
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

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(ses.GetTxnHandler().GetTxnCtx(), ses, nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})
}

func Test_getDbNameForPrivilege(t *testing.T) {
	tests := []struct {
		name   string
		objRef *plan2.ObjectRef
		want   string
	}{
		{
			name:   "normal table",
			objRef: &plan2.ObjectRef{SchemaName: "db1", ObjName: "t1"},
			want:   "db1",
		},
		{
			name:   "subscription table uses sub name",
			objRef: &plan2.ObjectRef{SchemaName: "pub_db", ObjName: "t1", SubscriptionName: "sub2"},
			want:   "sub2",
		},
		{
			name:   "empty subscription name",
			objRef: &plan2.ObjectRef{SchemaName: "db1", ObjName: "t1", SubscriptionName: ""},
			want:   "db1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getDbNameForPrivilege(tt.objRef)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_extractPrivilegeTipsFromPlan_Subscription(t *testing.T) {
	// When a TABLE_SCAN node has SubscriptionName set, the extracted
	// privilegeTips should use the subscription database name instead
	// of the publisher's original database name.
	p := &plan2.Plan{
		Plan: &plan2.Plan_Query{
			Query: &plan2.Query{
				Nodes: []*plan2.Node{
					{
						NodeType: plan.Node_TABLE_SCAN,
						ObjRef: &plan2.ObjectRef{
							SchemaName:       "pub_original_db",
							ObjName:          "t1",
							SubscriptionName: "sub2",
						},
					},
				},
			},
		},
	}
	arr := extractPrivilegeTipsFromPlan(p)
	assert.Equal(t, 1, len(arr))
	assert.Equal(t, "sub2", arr[0].databaseName)
	assert.Equal(t, "t1", arr[0].tableName)
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

			ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeDatabaseAndTable(ses.GetTxnHandler().GetTxnCtx(), ses, a.stmt, a.p)
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

			ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeDatabaseAndTable(ses.GetTxnHandler().GetTxnCtx(), ses, a.stmt, a.p)
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

			ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeDatabaseAndTable(ses.GetTxnHandler().GetTxnCtx(), ses, a.stmt, a.p)
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

		err := doGrantPrivilege(ses.GetTxnHandler().GetTxnCtx(), ses, stmt, bh)
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

			err = doGrantPrivilege(ses.GetTxnHandler().GetTxnCtx(), ses, stmt, bh)
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

		ctx := context.WithValue(context.TODO(), defines.TenantIDKey{}, uint32(sysAccountID))

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
				if stmt.ObjType == tree.OBJECT_TYPE_VIEW {
					sql, _ := getSqlForCheckDatabaseView(ctx, dbName, tableName)
					mrs := newMrsForCheckDatabaseTable([][]interface{}{
						{0},
					})
					bh.sql2result[sql] = mrs
				} else {
					sql, _ := getSqlForCheckDatabaseTable(ctx, dbName, tableName)
					mrs := newMrsForCheckDatabaseTable([][]interface{}{
						{0},
					})
					bh.sql2result[sql] = mrs
				}
			}

			_, objId, err := checkPrivilegeObjectTypeAndPrivilegeLevel(ctx, ses, bh, stmt.ObjType, *stmt.Level)
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

			err = doGrantPrivilege(ses.GetTxnHandler().GetTxnCtx(), ses, stmt, bh)
			convey.So(err, convey.ShouldBeNil)
		}
	})
	convey.Convey("grant view, role succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		dbName := "d"
		tableName := "v"
		stmts := []*tree.GrantPrivilege{
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
				},
				ObjType: tree.OBJECT_TYPE_VIEW,
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
				ObjType: tree.OBJECT_TYPE_VIEW,
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
				ObjType: tree.OBJECT_TYPE_VIEW,
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
				ObjType: tree.OBJECT_TYPE_VIEW,
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
				ObjType: tree.OBJECT_TYPE_VIEW,
				Level: &tree.PrivilegeLevel{
					Level:   tree.PRIVILEGE_LEVEL_TYPE_TABLE,
					TabName: tableName,
				},
				Roles: []*tree.Role{
					{UserName: "r1"},
				},
			},
		}

		ctx := context.WithValue(context.TODO(), defines.TenantIDKey{}, uint32(sysAccountID))

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
				if stmt.ObjType == tree.OBJECT_TYPE_VIEW {
					sql, _ := getSqlForCheckDatabaseView(ctx, dbName, tableName)
					mrs := newMrsForCheckDatabaseTable([][]interface{}{
						{0},
					})
					bh.sql2result[sql] = mrs
				} else {
					sql, _ := getSqlForCheckDatabaseTable(ctx, dbName, tableName)
					mrs := newMrsForCheckDatabaseTable([][]interface{}{
						{0},
					})
					bh.sql2result[sql] = mrs
				}
			}

			_, objId, err := checkPrivilegeObjectTypeAndPrivilegeLevel(ctx, ses, bh, stmt.ObjType, *stmt.Level)
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

			err = doGrantPrivilege(ses.GetTxnHandler().GetTxnCtx(), ses, stmt, bh)
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

		err := doRevokePrivilege(ses.GetTxnHandler().GetTxnCtx(), ses, stmt, bh)
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

			err = doRevokePrivilege(ses.GetTxnHandler().GetTxnCtx(), ses, stmt, bh)
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

		ctx := context.WithValue(context.TODO(), defines.TenantIDKey{}, uint32(sysAccountID))

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
				if stmt.ObjType == tree.OBJECT_TYPE_VIEW {
					sql, _ := getSqlForCheckDatabaseView(ctx, dbName, tableName)
					mrs := newMrsForCheckDatabaseTable([][]interface{}{
						{0},
					})
					bh.sql2result[sql] = mrs
				} else {
					sql, _ := getSqlForCheckDatabaseTable(ctx, dbName, tableName)
					mrs := newMrsForCheckDatabaseTable([][]interface{}{
						{0},
					})
					bh.sql2result[sql] = mrs
				}
			}

			_, objId, err := checkPrivilegeObjectTypeAndPrivilegeLevel(ctx, ses, bh, stmt.ObjType, *stmt.Level)
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

			err = doRevokePrivilege(ses.GetTxnHandler().GetTxnCtx(), ses, stmt, bh)
			convey.So(err, convey.ShouldBeNil)
		}
	})
	convey.Convey("revoke view, role succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		dbName := "d"
		tableName := "v"
		stmts := []*tree.RevokePrivilege{
			{
				Privileges: []*tree.Privilege{
					{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
				},
				ObjType: tree.OBJECT_TYPE_VIEW,
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
				ObjType: tree.OBJECT_TYPE_VIEW,
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
				ObjType: tree.OBJECT_TYPE_VIEW,
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
				ObjType: tree.OBJECT_TYPE_VIEW,
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
				ObjType: tree.OBJECT_TYPE_VIEW,
				Level: &tree.PrivilegeLevel{
					Level:   tree.PRIVILEGE_LEVEL_TYPE_TABLE,
					TabName: tableName,
				},
				Roles: []*tree.Role{
					{UserName: "r1"},
				},
			},
		}

		ctx := context.WithValue(context.TODO(), defines.TenantIDKey{}, uint32(sysAccountID))

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
				if stmt.ObjType == tree.OBJECT_TYPE_VIEW {
					sql, _ := getSqlForCheckDatabaseView(ctx, dbName, tableName)
					mrs := newMrsForCheckDatabaseTable([][]interface{}{
						{0},
					})
					bh.sql2result[sql] = mrs
				} else {
					sql, _ := getSqlForCheckDatabaseTable(ctx, dbName, tableName)
					mrs := newMrsForCheckDatabaseTable([][]interface{}{
						{0},
					})
					bh.sql2result[sql] = mrs
				}
			}

			_, objId, err := checkPrivilegeObjectTypeAndPrivilegeLevel(ctx, ses, bh, stmt.ObjType, *stmt.Level)
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

			err = doRevokePrivilege(ses.GetTxnHandler().GetTxnCtx(), ses, stmt, bh)
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
		pu.SV.KillRountinesInterval = 0

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
		pu.SV.KillRountinesInterval = 0

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
	t.Skip("skip doInterpretCall")
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
		proc := testutil.NewProcess(t)
		proc.Base.FileService = getPu(ses.GetService()).FileService
		proc.Base.SessionInfo = process.SessionInfo{Account: sysAccountName}
		ses.GetTxnCompileCtx().execCtx = &ExecCtx{
			proc: proc,
		}
		ses.SetDatabaseName("procedure_test")
		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		pu.SV.KillRountinesInterval = 0
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx, "")
		ses.rm = rm

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, err := getSqlForSpBody(ses.GetTxnHandler().GetConnCtx(), string(call.Name.Name.ObjectName), ses.GetDatabaseName())
		convey.So(err, convey.ShouldBeNil)
		mrs := newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err = doInterpretCall(ctx, ses, call, false)
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
		proc := testutil.NewProcess(t)
		proc.Base.FileService = getPu(ses.GetService()).FileService
		proc.Base.SessionInfo = process.SessionInfo{Account: sysAccountName}
		ses.SetDatabaseName("procedure_test")
		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		pu.SV.KillRountinesInterval = 0
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		ses.GetTxnCompileCtx().execCtx = &ExecCtx{reqCtx: ctx, proc: proc, ses: ses}
		rm, _ := NewRoutineManager(ctx, "")
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

		sql = getSqlForGetSystemVariablesWithAccount(uint64(ses.GetTenantInfo().GetTenantID()))
		mrs = newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		sql = getSqlForGetSystemVariableValueWithDatabase("procedure_test", "version_compatibility")
		mrs = newMrsForPasswordOfUser([][]interface{}{
			{"0.7"},
		})
		bh.sql2result[sql] = mrs

		_, err = doInterpretCall(ctx, ses, call, false)
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
		proc := testutil.NewProcess(t)
		proc.Base.FileService = getPu(ses.GetService()).FileService
		proc.Base.SessionInfo = process.SessionInfo{Account: sysAccountName}
		ses.SetDatabaseName("procedure_test")
		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		pu.SV.KillRountinesInterval = 0
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		ses.GetTxnCompileCtx().execCtx = &ExecCtx{reqCtx: ctx, proc: proc,
			ses: ses}
		rm, _ := NewRoutineManager(ctx, "")
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

		sql = getSqlForGetSystemVariablesWithAccount(uint64(ses.GetTenantInfo().GetTenantID()))
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

		_, err = doInterpretCall(ctx, ses, call, false)
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
		convey.So(tenant.GetDefaultRoleID(), convey.ShouldEqual, uint32(5))
		convey.So(tenant.GetDefaultRole(), convey.ShouldEqual, "role1")
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
		convey.So(tenant.GetDefaultRoleID(), convey.ShouldEqual, uint32(5))
		convey.So(tenant.GetDefaultRole(), convey.ShouldEqual, "role1")
	})
}

func TestDoSwitchRoleSecondaryRoleAllInvalidatesRuleCache(t *testing.T) {
	convey.Convey("set secondary role all invalidates rule cache", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ses := newSes(&privilege{}, ctrl)
		tenant := &TenantInfo{
			Tenant:        "test_account",
			User:          "test_user",
			DefaultRole:   "role1",
			TenantID:      3001,
			UserID:        3,
			DefaultRoleID: 5,
		}
		ses.SetTenantInfo(tenant)
		ses.ruleCache = map[string]string{"db1.t1": "select a from db1.t1"}

		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil
		bh.sql2result[getSqlForgetUserRolesExpectPublicRole(publicRoleID, tenant.UserID)] = newMrsForPasswordOfUser([][]interface{}{
			{"6", "role5"},
		})

		err := doSwitchRole(ses.GetTxnHandler().GetTxnCtx(), ses, &tree.SetRole{
			SecondaryRole:     true,
			SecondaryRoleType: tree.SecondaryRoleTypeAll,
		})
		convey.So(err, convey.ShouldBeNil)
		convey.So(tenant.GetUseSecondaryRole(), convey.ShouldBeTrue)
		convey.So(tenant.GetDefaultRoleID(), convey.ShouldEqual, uint32(5))
		convey.So(tenant.GetDefaultRole(), convey.ShouldEqual, "role1")

		ses.ruleCacheMu.RLock()
		cacheIsNil := ses.ruleCache == nil
		ses.ruleCacheMu.RUnlock()
		convey.So(cacheIsNil, convey.ShouldBeTrue)
	})

	convey.Convey("set secondary role all returns setup error", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ses := newSes(&privilege{}, ctrl)
		tenant := &TenantInfo{
			Tenant:        "test_account",
			User:          "test_user",
			DefaultRole:   "role1",
			TenantID:      3001,
			UserID:        3,
			DefaultRoleID: 5,
		}
		ses.SetTenantInfo(tenant)

		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil
		bh.sql2err[getSqlForgetUserRolesExpectPublicRole(publicRoleID, tenant.UserID)] = fmt.Errorf("load roles failed")

		err := doSwitchRole(ses.GetTxnHandler().GetTxnCtx(), ses, &tree.SetRole{
			SecondaryRole:     true,
			SecondaryRoleType: tree.SecondaryRoleTypeAll,
		})
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(tenant.GetUseSecondaryRole(), convey.ShouldBeFalse)
	})
}

func TestDoSwitchRoleSecondaryRoleNoneInvalidatesRuleCache(t *testing.T) {
	convey.Convey("set secondary role none invalidates rule cache", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newSes(&privilege{}, ctrl)
		tenant := &TenantInfo{
			Tenant:        "test_account",
			User:          "test_user",
			DefaultRole:   "role1",
			TenantID:      3001,
			UserID:        3,
			DefaultRoleID: 5,
		}
		tenant.SetUseSecondaryRole(true)
		ses.SetTenantInfo(tenant)
		ses.ruleCache = map[string]string{"db1.t1": "select a from db1.t1"}

		err := doSwitchRole(ses.GetTxnHandler().GetTxnCtx(), ses, &tree.SetRole{
			SecondaryRole:     true,
			SecondaryRoleType: tree.SecondaryRoleTypeNone,
		})
		convey.So(err, convey.ShouldBeNil)
		convey.So(tenant.GetUseSecondaryRole(), convey.ShouldBeFalse)

		ses.ruleCacheMu.RLock()
		cacheIsNil := ses.ruleCache == nil
		ses.ruleCacheMu.RUnlock()
		convey.So(cacheIsNil, convey.ShouldBeTrue)
	})
}

func TestDoSwitchRolePrimaryRoleInvalidatesRuleCache(t *testing.T) {
	convey.Convey("set role switches current role and invalidates rule cache", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		ses := newSes(&privilege{}, ctrl)
		tenant := &TenantInfo{
			Tenant:        "test_account",
			User:          "test_user",
			DefaultRole:   "role1",
			TenantID:      3001,
			UserID:        3,
			DefaultRoleID: 5,
		}
		tenant.SetUseSecondaryRole(true)
		ses.SetTenantInfo(tenant)
		ses.ruleCache = map[string]string{"db1.t1": "select a from db1.t1"}

		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		roleSQL, err := getSqlForRoleIdOfRole(context.Background(), "role2")
		convey.So(err, convey.ShouldBeNil)
		bh.sql2result[roleSQL] = newMrsForRoleIdOfRole([][]interface{}{{int64(6)}})
		bh.sql2result[getSqlForCheckUserGrant(6, int64(tenant.UserID))] = newMrsForCheckUserGrant([][]interface{}{
			{int64(6), int64(tenant.UserID), false},
		})

		err = doSwitchRole(ses.GetTxnHandler().GetTxnCtx(), ses, &tree.SetRole{
			Role: &tree.Role{UserName: "role2"},
		})
		convey.So(err, convey.ShouldBeNil)
		convey.So(tenant.GetDefaultRoleID(), convey.ShouldEqual, uint32(6))
		convey.So(tenant.GetDefaultRole(), convey.ShouldEqual, "role2")
		convey.So(tenant.GetUseSecondaryRole(), convey.ShouldBeFalse)

		ses.ruleCacheMu.RLock()
		cacheIsNil := ses.ruleCache == nil
		ses.ruleCacheMu.RUnlock()
		convey.So(cacheIsNil, convey.ShouldBeTrue)
	})
}

func TestGetSessionSysVar(t *testing.T) {
	convey.Convey("get session system variable succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		ses := newSes(nil, ctrl)

		value, err := ses.GetSessionSysVar("port")
		convey.So(err, convey.ShouldBeNil)
		convey.So(value, convey.ShouldEqual, 6001)
		value, err = ses.GetSessionSysVar("host")
		convey.So(err, convey.ShouldBeNil)
		convey.So(value, convey.ShouldEqual, "0.0.0.0")
		_, err = ses.GetSessionSysVar("not exists sys var")
		convey.So(err, convey.ShouldNotBeNil)
	})
}

// TestGetSessionSysVar_MapMissFallsBackToDefault: a sysvar that's
// registered in gSysVarsDefs but absent from ses.sesSysVars (the
// per-account snapshot doesn't contain it — typical of sysvars added
// to gSysVarsDefs without a backing mo_mysql_compatibility_mode row,
// or in a brand-new account whose snapshot pre-dates the sysvar
// registration) must resolve to the registered Default rather than
// surfacing interface{}(nil). Reproduces the CREATE TABLE CLONE
// regression that ate "ivf_threads_build" as nil.
func TestGetSessionSysVar_MapMissFallsBackToDefault(t *testing.T) {
	convey.Convey("session sysvar map miss falls back to gSysVarsDefs default", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()
		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		ses := newSes(nil, ctrl)

		// Force the map-miss scenario: empty per-session map. The
		// var IS registered in gSysVarsDefs (default int64(0)) but
		// absent from this empty map.
		ses.sesSysVars = &SystemVariables{mp: make(map[string]interface{})}

		v, err := ses.GetSessionSysVar("ivf_threads_build")
		convey.So(err, convey.ShouldBeNil)
		convey.So(v, convey.ShouldEqual, int64(0))

		v, err = ses.GetSessionSysVar("kmeans_train_percent")
		convey.So(err, convey.ShouldBeNil)
		convey.So(v, convey.ShouldEqual, float64(10))
	})
}

func TestSetSessionSysVar(t *testing.T) {
	convey.Convey("set session system variable succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		ses0 := newSes(nil, ctrl)
		ses1 := newSes(nil, ctrl)

		// set ScopeGlobal var, err
		err := ses0.SetSessionSysVar(context.TODO(), "port", 6002)
		convey.So(err, convey.ShouldNotBeNil)

		// before set
		value, err := ses0.GetSessionSysVar("autocommit")
		convey.So(err, convey.ShouldBeNil)
		convey.So(value, convey.ShouldEqual, 1)
		value, err = ses1.GetSessionSysVar("autocommit")
		convey.So(err, convey.ShouldBeNil)
		convey.So(value, convey.ShouldEqual, 1)
		// set
		err = ses0.SetSessionSysVar(context.TODO(), "autocommit", "off")
		convey.So(err, convey.ShouldBeNil)
		// after set
		value, err = ses0.GetSessionSysVar("autocommit")
		convey.So(err, convey.ShouldBeNil)
		convey.So(value, convey.ShouldEqual, 0)
		// do not affect other existing session
		value, err = ses1.GetSessionSysVar("autocommit")
		convey.So(err, convey.ShouldBeNil)
		convey.So(value, convey.ShouldEqual, 1)
		// do not affect new session
		ses2 := newSes(nil, ctrl)
		value, err = ses2.GetSessionSysVar("autocommit")
		convey.So(err, convey.ShouldBeNil)
		convey.So(value, convey.ShouldEqual, 1)

		err = ses0.SetSessionSysVar(context.TODO(), "not exists sys var", "xxxx")
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestGetGlobalSysVar(t *testing.T) {
	convey.Convey("get global system variable succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		ses := newSes(nil, ctrl)

		value, err := ses.GetGlobalSysVar("port")
		convey.So(err, convey.ShouldBeNil)
		convey.So(value, convey.ShouldEqual, 6001)
		// err when get from a ScopeSession var
		_, err = ses.GetGlobalSysVar("debug_sync")
		convey.So(err, convey.ShouldNotBeNil)
		_, err = ses.GetGlobalSysVar("not exists sys var")
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestSetGlobalSysVar(t *testing.T) {
	convey.Convey("set global system variable succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil
		sql := getSqlForGetSysVarWithAccount(sysAccountID, "autocommit")
		mrs := newMrsForSystemVariableNameOfAccount([][]interface{}{})
		bh.sql2result[sql] = mrs
		sql = getSqlForInsertSysVarWithAccount(sysAccountID, "sys", "autocommit", "0")
		bh.sql2result[sql] = nil

		ses0 := newSes(nil, ctrl)
		ses1 := newSes(nil, ctrl)

		// set ScopeSession var, err
		err := ses0.SetGlobalSysVar(context.TODO(), "rand_seed1", 1)
		convey.So(err, convey.ShouldNotBeNil)

		// before set
		value, err := ses0.GetSessionSysVar("autocommit")
		convey.So(err, convey.ShouldBeNil)
		convey.So(value, convey.ShouldEqual, 1)
		value, err = ses0.GetGlobalSysVar("autocommit")
		convey.So(err, convey.ShouldBeNil)
		convey.So(value, convey.ShouldEqual, 1)
		value, err = ses1.GetSessionSysVar("autocommit")
		convey.So(err, convey.ShouldBeNil)
		convey.So(value, convey.ShouldEqual, 1)
		value, err = ses1.GetGlobalSysVar("autocommit")
		convey.So(err, convey.ShouldBeNil)
		convey.So(value, convey.ShouldEqual, 1)

		// set
		err = ses0.SetGlobalSysVar(context.TODO(), "autocommit", 0)
		convey.So(err, convey.ShouldBeNil)

		// after set, only affect global level var
		value, err = ses0.GetSessionSysVar("autocommit")
		convey.So(err, convey.ShouldBeNil)
		convey.So(value, convey.ShouldEqual, 1)
		value, err = ses0.GetGlobalSysVar("autocommit")
		convey.So(err, convey.ShouldBeNil)
		convey.So(value, convey.ShouldEqual, 0)

		// affect other existing session's global level var
		value, err = ses1.GetSessionSysVar("autocommit")
		convey.So(err, convey.ShouldBeNil)
		convey.So(value, convey.ShouldEqual, 1)
		value, err = ses1.GetGlobalSysVar("autocommit")
		convey.So(err, convey.ShouldBeNil)
		convey.So(value, convey.ShouldEqual, 0)

		// new session, both GetSession/GlobalSysVar equal 0
		ses2 := newSes(nil, ctrl)
		ses2.sesSysVars.mp["autocommit"] = 0
		ses2.gSysVars.mp["autocommit"] = 0
		value, err = ses2.GetSessionSysVar("autocommit")
		convey.So(err, convey.ShouldBeNil)
		convey.So(value, convey.ShouldEqual, 0)
		value, err = ses2.GetGlobalSysVar("autocommit")
		convey.So(err, convey.ShouldBeNil)
		convey.So(value, convey.ShouldEqual, 0)

		err = ses0.SetGlobalSysVar(context.TODO(), "not exists sys var", "xxxx")
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func boxExprStr(s string) tree.Expr {
	return tree.NewNumVal(s, s, false, tree.P_char)
}

func mustUnboxExprStr(e tree.Expr) string {
	if e == nil {
		return ""
	}
	return e.(*tree.NumVal).String()
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
		au.MiscOpt = stmt.MiscOpt
		return au
	}
	convey.Convey("alter user lock success", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		stmt := &tree.AlterUser{
			Users: []*tree.User{
				{Username: "u1", Hostname: "%", AuthOption: nil},
			},
			MiscOpt: tree.NewUserMiscOptionAccountLock(),
		}
		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		pu.SV.KillRountinesInterval = 0
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx, "")
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

			sql, _ = getSqlForCheckUserHasRole(context.TODO(), "u1", moAdminRoleID)
			mrs = newMrsForSqlForCheckUserHasRole([][]interface{}{
				{0, 0},
			})
			bh.sql2result[sql] = mrs

			sql = getSqlForUpdateStatusLockOfUserForever(userStatusLockForever, user.Username)
			bh.sql2result[sql] = &MysqlResultSet{}
		}

		err := doAlterUser(ctx, ses, alterUserFrom(stmt))
		convey.So(err, convey.ShouldBeNil)
	})

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
		pu.SV.KillRountinesInterval = 0
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx, "")
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

			sql, _ = getSqlForCheckUserHasRole(context.TODO(), "u1", moAdminRoleID)
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
		pu.SV.KillRountinesInterval = 0
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx, "")
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
		pu.SV.KillRountinesInterval = 0
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx, "")
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
		pu.SV.KillRountinesInterval = 0
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx, "")
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
		pu.SV.KillRountinesInterval = 0
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx, "")
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
		pu.SV.KillRountinesInterval = 0
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx, "")
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
		pu.SV.KillRountinesInterval = 0
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx, "")
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
		pu.SV.KillRountinesInterval = 0
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx, "")
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
		pu.SV.KillRountinesInterval = 0
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx, "")
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
		pu.SV.KillRountinesInterval = 0
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx, "")
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
		pu.SV.KillRountinesInterval = 0
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx, "")
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
		pu.SV.KillRountinesInterval = 0
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx, "")
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
		pu.SV.KillRountinesInterval = 0
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx, "")
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

func newMrsForShowDatabases(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col := &MysqlColumn{}
	col.SetName("Database")
	col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mrs.AddColumn(col)

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
		pu.SV.KillRountinesInterval = 0
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		ctx = defines.AttachAccountId(ctx, 0)

		rm, _ := NewRoutineManager(ctx, "")
		ses.rm = rm

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckTenant(ctx, "acc")
		mrs := newMrsForGetAllAccounts([][]interface{}{
			{uint64(1), "acc", "open", uint64(1), nil},
		})
		bh.sql2result[sql] = mrs

		sql, _ = getSqlForDeleteAccountFromMoAccount(context.TODO(), mustUnboxExprStr(stmt.Name))
		bh.sql2result[sql] = nil

		for _, sql = range getSqlForDropAccount() {
			bh.sql2result[sql] = nil
		}

		sql = fmt.Sprintf(getPubInfoSql, 1) + " order by update_time desc, created_time desc"
		bh.sql2result[sql] = newMrsForSqlForGetPubs([][]interface{}{})

		sql = "select 1 from mo_catalog.mo_columns where att_database = 'mo_catalog' and att_relname = 'mo_subs' and attname = 'sub_account_name'"
		bh.sql2result[sql] = newMrsForSqlForGetSubs([][]interface{}{{1}})

		sql = getSubsSql + " and sub_account_id = 1"
		bh.sql2result[sql] = newMrsForSqlForGetSubs([][]interface{}{})

		sql = "show databases;"
		bh.sql2result[sql] = newMrsForSqlForShowDatabases([][]interface{}{})

		bh.sql2result["show tables from mo_catalog;"] = newMrsForShowTables([][]interface{}{})

		err := doDropAccount(ses.GetTxnHandler().GetTxnCtx(), bh, ses, &dropAccount{
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
		pu.SV.KillRountinesInterval = 0
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx, "")
		ses.rm = rm

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql, _ := getSqlForCheckTenant(ctx, "acc")
		bh.sql2result[sql] = newMrsForGetAllAccounts([][]interface{}{})

		sql, _ = getSqlForDeleteAccountFromMoAccount(context.TODO(), mustUnboxExprStr(stmt.Name))
		bh.sql2result[sql] = nil

		for _, sql = range getSqlForDropAccount() {
			bh.sql2result[sql] = nil
		}

		bh.sql2result["show tables from mo_catalog;"] = newMrsForShowTables([][]interface{}{})

		err := doDropAccount(ses.GetTxnHandler().GetTxnCtx(), bh, ses, &dropAccount{
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
		pu.SV.KillRountinesInterval = 0
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx, "")
		ses.rm = rm

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql := getAccountIdNamesSql + " for update"
		bh.sql2result[sql] = newMrsForGetAllAccounts([][]interface{}{})

		sql, _ = getSqlForDeleteAccountFromMoAccount(context.TODO(), mustUnboxExprStr(stmt.Name))
		bh.sql2result[sql] = nil

		for _, sql = range getSqlForDropAccount() {
			bh.sql2result[sql] = nil
		}

		err := doDropAccount(ses.GetTxnHandler().GetTxnCtx(), bh, ses, &dropAccount{
			IfExists: stmt.IfExists,
			Name:     mustUnboxExprStr(stmt.Name),
		})
		convey.So(err, convey.ShouldBeError)
	})
}

func Test_doDropAccount_InTransaction(t *testing.T) {
	convey.Convey("doDropAccount with inTransaction parameter", t, func() {
		// Test case 1: inTransaction=false (default behavior - creates new transaction)
		convey.Convey("inTransaction=false should create new transaction", func() {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			bh := &backgroundExecTestWithHistory{}
			bh.init()

			stmt := &tree.DropAccount{
				Name: boxExprStr("test_acc"),
			}
			priv := determinePrivilegeSetOfStatement(stmt)
			ses := newSes(priv, ctrl)

			pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
			pu.SV.SetDefaultValues()
			pu.SV.KillRountinesInterval = 0
			ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
			ctx = defines.AttachAccountId(ctx, 0)

			rm, _ := NewRoutineManager(ctx, "")
			ses.rm = rm

			// Setup SQL results
			bh.sql2result["begin;"] = nil
			bh.sql2result["commit;"] = nil
			bh.sql2result["rollback;"] = nil

			sql, _ := getSqlForLockMoAccountNameFormat(ctx, "test_acc")
			bh.sql2result[sql] = nil

			sql, _ = getSqlForCheckTenant(ctx, "test_acc")
			mrs := newMrsForGetAllAccounts([][]interface{}{
				{uint64(1), "test_acc", "open", uint64(1), nil},
			})
			bh.sql2result[sql] = mrs

			sql, _ = getSqlForDeleteAccountFromMoAccount(context.TODO(), "test_acc")
			bh.sql2result[sql] = nil

			for _, sql = range getSqlForDropAccount() {
				bh.sql2result[sql] = nil
			}

			bh.sql2result["show tables from mo_catalog;"] = newMrsForShowTables([][]interface{}{})

			sql = fmt.Sprintf(getPubInfoSql, 1) + " order by update_time desc, created_time desc"
			bh.sql2result[sql] = newMrsForSqlForGetPubs([][]interface{}{})

			sql = "select 1 from mo_catalog.mo_columns where att_database = 'mo_catalog' and att_relname = 'mo_subs' and attname = 'sub_account_name'"
			bh.sql2result[sql] = newMrsForSqlForGetSubs([][]interface{}{{1}})

			sql = getSubsSql
			bh.sql2result[sql] = newMrsForSqlForGetSubs([][]interface{}{})

			sql = getSubsSql + " and sub_account_id = 1"
			bh.sql2result[sql] = newMrsForSqlForGetSubs([][]interface{}{})

			bh.sql2result["show databases;"] = newMrsForShowDatabases([][]interface{}{})

			err := doDropAccount(ses.GetTxnHandler().GetTxnCtx(), bh, ses, &dropAccount{
				IfExists: stmt.IfExists,
				Name:     mustUnboxExprStr(stmt.Name),
			}, false) // inTransaction=false

			convey.So(err, convey.ShouldBeNil)
			// Verify that "begin;" was executed
			convey.So(bh.hasExecuted("begin;"), convey.ShouldBeTrue)
		})

		// Test case 2: inTransaction=true (restore scenario - uses existing transaction)
		convey.Convey("inTransaction=true should not create new transaction", func() {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			bh := &backgroundExecTestWithHistory{}
			bh.init()

			stmt := &tree.DropAccount{
				Name: boxExprStr("test_acc"),
			}
			priv := determinePrivilegeSetOfStatement(stmt)
			ses := newSes(priv, ctrl)

			pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
			pu.SV.SetDefaultValues()
			pu.SV.KillRountinesInterval = 0
			ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
			ctx = defines.AttachAccountId(ctx, 0)

			rm, _ := NewRoutineManager(ctx, "")
			ses.rm = rm

			// Setup SQL results (no begin; needed)
			bh.sql2result["commit;"] = nil
			bh.sql2result["rollback;"] = nil

			sql, _ := getSqlForLockMoAccountNameFormat(ctx, "test_acc")
			bh.sql2result[sql] = nil

			sql, _ = getSqlForCheckTenant(ctx, "test_acc")
			mrs := newMrsForGetAllAccounts([][]interface{}{
				{uint64(1), "test_acc", "open", uint64(1), nil},
			})
			bh.sql2result[sql] = mrs

			sql, _ = getSqlForDeleteAccountFromMoAccount(context.TODO(), "test_acc")
			bh.sql2result[sql] = nil

			for _, sql = range getSqlForDropAccount() {
				bh.sql2result[sql] = nil
			}

			bh.sql2result["show tables from mo_catalog;"] = newMrsForShowTables([][]interface{}{})

			sql = fmt.Sprintf(getPubInfoSql, 1) + " order by update_time desc, created_time desc"
			bh.sql2result[sql] = newMrsForSqlForGetPubs([][]interface{}{})

			sql = "select 1 from mo_catalog.mo_columns where att_database = 'mo_catalog' and att_relname = 'mo_subs' and attname = 'sub_account_name'"
			bh.sql2result[sql] = newMrsForSqlForGetSubs([][]interface{}{{1}})

			sql = getSubsSql
			bh.sql2result[sql] = newMrsForSqlForGetSubs([][]interface{}{})

			sql = getSubsSql + " and sub_account_id = 1"
			bh.sql2result[sql] = newMrsForSqlForGetSubs([][]interface{}{})

			bh.sql2result["show databases;"] = newMrsForShowDatabases([][]interface{}{})

			err := doDropAccount(ses.GetTxnHandler().GetTxnCtx(), bh, ses, &dropAccount{
				IfExists: stmt.IfExists,
				Name:     mustUnboxExprStr(stmt.Name),
			}, true) // inTransaction=true

			convey.So(err, convey.ShouldBeNil)
			// Verify that "begin;" was NOT executed
			convey.So(bh.hasExecuted("begin;"), convey.ShouldBeFalse)
		})
	})
}

// backgroundExecTestWithHistory extends backgroundExecTest to track SQL execution history
type backgroundExecTestWithHistory struct {
	backgroundExecTest
	executedSqls []string
}

func (bt *backgroundExecTestWithHistory) init() {
	bt.backgroundExecTest.init()
	bt.executedSqls = make([]string, 0)
}

func (bt *backgroundExecTestWithHistory) Exec(ctx context.Context, s string) error {
	bt.executedSqls = append(bt.executedSqls, s)
	return bt.backgroundExecTest.Exec(ctx, s)
}

func (bt *backgroundExecTestWithHistory) hasExecuted(sql string) bool {
	for _, executed := range bt.executedSqls {
		if executed == sql {
			return true
		}
	}
	return false
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

func Test_shouldSkipImplicitOwnershipRevoke(t *testing.T) {
	convey.Convey("skip implicit ownership revoke for admin owner roles", t, func() {
		sysSes := &Session{}
		sysSes.SetTenantInfo(&TenantInfo{
			Tenant:      sysAccountName,
			DefaultRole: "r1",
			TenantID:    sysAccountID,
		})
		convey.So(shouldSkipImplicitOwnershipRevoke(sysSes, moAdminRoleName), convey.ShouldBeTrue)
		convey.So(shouldSkipImplicitOwnershipRevoke(sysSes, "r1"), convey.ShouldBeFalse)

		accountSes := &Session{}
		accountSes.SetTenantInfo(&TenantInfo{
			Tenant:      "acc1",
			DefaultRole: "r1",
			TenantID:    1,
		})
		convey.So(shouldSkipImplicitOwnershipRevoke(accountSes, accountAdminRoleName), convey.ShouldBeTrue)
		convey.So(shouldSkipImplicitOwnershipRevoke(accountSes, "r1"), convey.ShouldBeFalse)

		convey.So(shouldSkipImplicitOwnershipRevoke(nil, "r1"), convey.ShouldBeTrue)
		convey.So(shouldSkipImplicitOwnershipRevoke(sysSes, ""), convey.ShouldBeTrue)
	})
}

func TestGetObjectOwnerRoleNameSkipsDeletedOwnerRole(t *testing.T) {
	ctx := context.TODO()

	for _, tc := range []struct {
		name     string
		ownerSQL string
	}{
		{
			name:     "database",
			ownerSQL: getSqlForGetOwnerOfDatabase("db1"),
		},
		{
			name:     "table",
			ownerSQL: getSqlForGetOwnerOfTable("db1", "t1"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bh := &backgroundExecTest{}
			bh.init()

			bh.sql2result[tc.ownerSQL] = newMrsForOwner([][]interface{}{{int64(100)}})
			bh.sql2result[getSqlForRoleNameOfRoleId(100)] = newMrsForRoleName([][]interface{}{})

			roleName, err := getObjectOwnerRoleName(ctx, bh, tc.ownerSQL)
			require.NoError(t, err)
			require.Empty(t, roleName)
		})
	}
}

func TestGetObjectOwnerRoleNameReturnsExistingOwnerRole(t *testing.T) {
	ctx := context.TODO()
	bh := &backgroundExecTest{}
	bh.init()

	ownerSQL := getSqlForGetOwnerOfTable("db1", "t1")
	bh.sql2result[ownerSQL] = newMrsForOwner([][]interface{}{{int64(100)}})
	bh.sql2result[getSqlForRoleNameOfRoleId(100)] = newMrsForRoleName([][]interface{}{{"r1"}})

	roleName, err := getObjectOwnerRoleName(ctx, bh, ownerSQL)
	require.NoError(t, err)
	require.Equal(t, "r1", roleName)
}

func newSes(priv *privilege, ctrl *gomock.Controller) *Session {
	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	pu.SV.SetDefaultValues()
	pu.SV.KillRountinesInterval = 0
	setPu("", pu)
	setSessionAlloc("", NewLeakCheckAllocator())

	ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
	ctx = defines.AttachAccountId(ctx, 0)

	ioses, err := NewIOSession(&testConn{}, pu, "")
	if err != nil {
		panic(err)
	}
	proto := NewMysqlClientProtocol("", 0, ioses, 1024, pu.SV)

	ses := NewSession(ctx, "", proto, nil)
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

	stubs := gostub.StubFunc(&ExeSqlInBgSes, nil, nil)
	defer stubs.Reset()

	_ = ses.InitSystemVariables(ctx, nil)

	rm, _ := NewRoutineManager(ctx, "")
	rm.baseService = new(MockBaseService)
	ses.rm = rm

	return ses
}

var _ BaseService = &MockBaseService{}

type MockBaseService struct {
}

func (m *MockBaseService) ID() string {
	return "mock base service"
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
	bh.EXPECT().GetExecStatsArray().DoAndReturn(func() statistic.StatsArray {
		var stats statistic.StatsArray
		stats.Reset()
		return stats
	}).AnyTimes()
	return bh
}

type backgroundExecTest struct {
	currentSql   string
	sql2result   map[string]ExecResult
	sql2err      map[string]error
	executedSQLs []string
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
	bt.sql2err = make(map[string]error)
}

func (bt *backgroundExecTest) Close() {
}

func (bt *backgroundExecTest) Clear() {}

func (bt *backgroundExecTest) GetExecStatsArray() statistic.StatsArray {
	var stats statistic.StatsArray
	stats.Reset()
	return stats
}

func (bt *backgroundExecTest) Exec(ctx context.Context, s string) error {
	bt.currentSql = s
	bt.executedSQLs = append(bt.executedSQLs, s)
	return bt.sql2err[s]
}

func (bt *backgroundExecTest) ExecRestore(ctx context.Context, s string, from uint32, to uint32) error {
	bt.currentSql = s
	bt.executedSQLs = append(bt.executedSQLs, s)
	return bt.sql2err[s]
}

func (bt *backgroundExecTest) GetExecResultSet() []interface{} {
	if _, ok := bt.sql2result[bt.currentSql]; !ok &&
		strings.HasPrefix(bt.currentSql, "select granted_id,with_grant_option from mo_catalog.mo_role_grant where grantee_id = ") {
		return []interface{}{newMrsForInheritedRoleIdOfRoleId([][]interface{}{})}
	}
	return []interface{}{bt.sql2result[bt.currentSql]}
}

func (bt *backgroundExecTest) ClearExecResultSet() {
	//bt.init()
}

func (bt *backgroundExecTest) Service() string {
	return ""
}

func (bt *backgroundExecTest) SetRestore(b bool) {
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

func newMrsForRoleName(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("role_name")
	col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	mrs.AddColumn(col1)

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return mrs
}

func newMrsForOwner(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("owner")
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

func newMrsForFeatureRegistry(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("enabled")
	col1.SetColumnType(defines.MYSQL_TYPE_TINY)

	col2 := &MysqlColumn{}
	col2.SetName("scope_spec")
	col2.SetColumnType(defines.MYSQL_TYPE_TEXT)

	mrs.AddColumn(col1)
	mrs.AddColumn(col2)

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return mrs
}

func newMrsForFeatureLimit(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("quota")
	col1.SetColumnType(defines.MYSQL_TYPE_LONGLONG)

	mrs.AddColumn(col1)

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return mrs
}

func newMrsForSnapshotCount(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("count")
	col1.SetColumnType(defines.MYSQL_TYPE_LONGLONG)

	mrs.AddColumn(col1)

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return mrs
}

func mockSnapshotQuotaResult(
	bh *backgroundExecTest,
	accountName string,
	accountId uint32,
	scope string,
) error {
	scopeSpec, err := bytejson.ParseJsonByteFromString(fmt.Sprintf(`{"allowed_scope":["%s"]}`, scope))
	if err != nil {
		return err
	}

	registrySQL := fmt.Sprintf(
		"select enabled, scope_spec from %s.%s where feature_code = '%s'",
		catalog.MO_CATALOG, catalog.MO_FEATURE_REGISTRY, featureCodeSnapshot,
	)
	bh.sql2result[registrySQL] = newMrsForFeatureRegistry([][]interface{}{
		{int8(1), string(scopeSpec)},
	})

	quota := int64(defaultSnapshotLimit)
	if accountId == sysAccountID {
		quota = defaultFeatureLimitForSys
	}
	limitSQL := fmt.Sprintf(
		"select quota from %s.%s where account_id = %d and feature_code = '%s' and scope = '%s'",
		catalog.MO_CATALOG, catalog.MO_FEATURE_LIMIT, accountId, featureCodeSnapshot, scope,
	)
	bh.sql2result[limitSQL] = newMrsForFeatureLimit([][]interface{}{
		{quota},
	})

	countSQL := fmt.Sprintf(
		"select count(*) from %s.%s where account_name = '%s' and level = '%s'",
		catalog.MO_CATALOG, catalog.MO_SNAPSHOTS, accountName, scope,
	)
	bh.sql2result[countSQL] = newMrsForSnapshotCount([][]interface{}{
		{int64(0)},
	})

	return nil
}

func newMrsForPitrRecord(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("pitr_id")
	col1.SetColumnType(defines.MYSQL_TYPE_UUID)

	col2 := &MysqlColumn{}
	col2.SetName("pitr_name")
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	// bigint unsigned
	col3 := &MysqlColumn{}
	col3.SetName("create_account")
	col3.SetColumnType(defines.MYSQL_TYPE_LONGLONG)

	col4 := &MysqlColumn{}
	col4.SetName("create_time")
	col4.SetColumnType(defines.MYSQL_TYPE_TIMESTAMP)

	// modified_time
	col5 := &MysqlColumn{}
	col5.SetName("modified_time")
	col5.SetColumnType(defines.MYSQL_TYPE_TIMESTAMP)

	// level varchar(10)
	col6 := &MysqlColumn{}
	col6.SetName("level")
	col6.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	// account_id bigint unsigned
	col7 := &MysqlColumn{}
	col7.SetName("account_id")
	col7.SetColumnType(defines.MYSQL_TYPE_LONGLONG)

	// account name
	col8 := &MysqlColumn{}
	col8.SetName("account_name")
	col8.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	// db_name
	col9 := &MysqlColumn{}
	col9.SetName("db_name")
	col9.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	// table_name
	col10 := &MysqlColumn{}
	col10.SetName("table_name")
	col10.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	// object_id
	col11 := &MysqlColumn{}
	col11.SetName("object_id")
	col11.SetColumnType(defines.MYSQL_TYPE_LONGLONG)

	// pitr length
	// tiny int unsigned
	col12 := &MysqlColumn{}
	col12.SetName("pitr_length")
	col12.SetColumnType(defines.MYSQL_TYPE_TINY)

	// pitr uint
	// varchar
	col13 := &MysqlColumn{}
	col13.SetName("pitr_uint")
	col13.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	mrs.AddColumn(col1)
	mrs.AddColumn(col2)
	mrs.AddColumn(col3)
	mrs.AddColumn(col4)
	mrs.AddColumn(col5)
	mrs.AddColumn(col6)
	mrs.AddColumn(col7)
	mrs.AddColumn(col8)
	mrs.AddColumn(col9)
	mrs.AddColumn(col10)
	mrs.AddColumn(col11)
	mrs.AddColumn(col12)
	mrs.AddColumn(col13)

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

func newMrsForGetAllAccounts(rows [][]interface{}) *MysqlResultSet {
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

	col5 := &MysqlColumn{}
	col5.SetName("suspended_time")
	col5.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	mrs.AddColumn(col1)
	mrs.AddColumn(col2)
	mrs.AddColumn(col3)
	mrs.AddColumn(col4)
	mrs.AddColumn(col5)

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return mrs
}

func newMrsForSqlForGetPubs(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("pub_name")
	col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col2 := &MysqlColumn{}
	col2.SetName("database_name")
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col3 := &MysqlColumn{}
	col3.SetName("database_id")
	col3.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	col4 := &MysqlColumn{}
	col4.SetName("table_list")
	col4.SetColumnType(defines.MYSQL_TYPE_TEXT)
	col5 := &MysqlColumn{}
	col5.SetName("account_list")
	col5.SetColumnType(defines.MYSQL_TYPE_TEXT)
	col6 := &MysqlColumn{}
	col6.SetName("created_time")
	col6.SetColumnType(defines.MYSQL_TYPE_TIMESTAMP)
	col7 := &MysqlColumn{}
	col7.SetName("update_time")
	col7.SetColumnType(defines.MYSQL_TYPE_TIMESTAMP)
	col8 := &MysqlColumn{}
	col8.SetName("owner")
	col8.SetColumnType(defines.MYSQL_TYPE_LONG)
	col9 := &MysqlColumn{}
	col9.SetName("creator")
	col9.SetColumnType(defines.MYSQL_TYPE_LONG)
	col10 := &MysqlColumn{}
	col10.SetName("account_list")
	col10.SetColumnType(defines.MYSQL_TYPE_TEXT)

	mrs.AddColumn(col1)
	mrs.AddColumn(col2)
	mrs.AddColumn(col3)
	mrs.AddColumn(col4)
	mrs.AddColumn(col5)
	mrs.AddColumn(col6)
	mrs.AddColumn(col7)
	mrs.AddColumn(col8)
	mrs.AddColumn(col9)
	mrs.AddColumn(col10)

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return mrs
}

func newMrsForSqlForGetSubs(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("sub_account_id")
	col1.SetColumnType(defines.MYSQL_TYPE_LONG)
	col2 := &MysqlColumn{}
	col2.SetName("sub_name")
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col3 := &MysqlColumn{}
	col3.SetName("sub_time")
	col3.SetColumnType(defines.MYSQL_TYPE_TIMESTAMP)
	col4 := &MysqlColumn{}
	col4.SetName("pub_account_name")
	col4.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col5 := &MysqlColumn{}
	col5.SetName("pub_name")
	col5.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col6 := &MysqlColumn{}
	col6.SetName("pub_database")
	col6.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col7 := &MysqlColumn{}
	col7.SetName("pub_tables")
	col7.SetColumnType(defines.MYSQL_TYPE_TEXT)
	col8 := &MysqlColumn{}
	col8.SetName("pub_time")
	col8.SetColumnType(defines.MYSQL_TYPE_TIMESTAMP)
	col9 := &MysqlColumn{}
	col9.SetName("pub_comment")
	col9.SetColumnType(defines.MYSQL_TYPE_TIMESTAMP)
	col10 := &MysqlColumn{}
	col10.SetName("status")
	col10.SetColumnType(defines.MYSQL_TYPE_TINY)

	mrs.AddColumn(col1)
	mrs.AddColumn(col2)
	mrs.AddColumn(col3)
	mrs.AddColumn(col4)
	mrs.AddColumn(col5)
	mrs.AddColumn(col6)
	mrs.AddColumn(col7)
	mrs.AddColumn(col8)
	mrs.AddColumn(col9)
	mrs.AddColumn(col10)

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
			if entry.objType == objectTypeAccount && entry.privilegeLevel == privilegeLevelStar {
				sql = getSqlForCheckRoleHasAccountLevelForStarWithSysScope(int64(roleId), entry.privilegeId, true)
				sql2result[sql] = newMrsForCheckRoleHasPrivilege(rowsOfMoRolePrivs)
			}
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

func newMrsForSystemVariableNameOfAccount(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("variable_name")
	col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	mrs.AddColumn(col1)

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
			if entry.objType == objectTypeAccount && entry.privilegeLevel == privilegeLevelStar {
				sql = getSqlForCheckRoleHasAccountLevelForStarWithSysScope(int64(roleId), entry.privilegeId, true)
				sql2result[sql] = newMrsForCheckRoleHasPrivilege(rowsOfMoRolePrivs[i][j])
			}
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

func TestDoRemoveStageFiles(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	ses := newTestSession(t, ctrl)
	defer ses.Close()

	dir := t.TempDir()
	stageURL := &url.URL{Scheme: stage.FILE_PROTOCOL, Path: dir}
	ses.proc.GetStageCache().Set("mystage", stage.StageDef{Id: 1, Name: "mystage", Url: stageURL})

	filePath := filepath.Join(dir, "a.txt")
	require.NoError(t, os.WriteFile(filePath, []byte("a"), 0600))

	rs := tree.NewRemoveStageFiles(false, "stage://mystage/a.txt")
	err := doRemoveStageFiles(ctx, ses, rs)
	require.NoError(t, err)
	_, err = os.Stat(filePath)
	require.True(t, os.IsNotExist(err))

	rs = tree.NewRemoveStageFiles(false, "file:///tmp/a.txt")
	err = doRemoveStageFiles(ctx, ses, rs)
	require.Error(t, err)

	tenant := &TenantInfo{
		Tenant:        "default_1",
		User:          "admin",
		DefaultRole:   "role1",
		TenantID:      3001,
		UserID:        2,
		DefaultRoleID: 10,
	}
	ses.SetTenantInfo(tenant)
	rs = tree.NewRemoveStageFiles(false, "stage://mystage/a.txt")
	err = doRemoveStageFiles(ctx, ses, rs)
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
		pu.SV.KillRountinesInterval = 0
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

		sql := getSqlForGetOwnerOfTable("db1", "t1")
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{0},
		})
		bh.sql2result[sql] = mrs

		_, _, err := checkRoleWhetherTableOwner(ses.GetTxnHandler().GetTxnCtx(), ses, "db1", "t1", true)
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
		pu.SV.KillRountinesInterval = 0
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx, "")
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

		ok, _, err := checkRoleWhetherTableOwner(ses.GetTxnHandler().GetTxnCtx(), ses, "db1", "t1", true)
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
		pu.SV.KillRountinesInterval = 0
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

		sql := getSqlForGetOwnerOfTable("db1", "t1")
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{1},
		})
		bh.sql2result[sql] = mrs

		ok, _, err := checkRoleWhetherTableOwner(ses.GetTxnHandler().GetTxnCtx(), ses, "db1", "t1", false)
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
		pu.SV.KillRountinesInterval = 0
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

		sql := getSqlForGetOwnerOfDatabase("db1")
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{0},
		})
		bh.sql2result[sql] = mrs

		ok, _, err := checkRoleWhetherDatabaseOwner(ses.GetTxnHandler().GetTxnCtx(), ses, "db1", true)
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
		pu.SV.KillRountinesInterval = 0
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx, "")
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

		ok, _, err := checkRoleWhetherDatabaseOwner(ses.GetTxnHandler().GetTxnCtx(), ses, "db1", true)
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
		pu.SV.KillRountinesInterval = 0
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

		sql := getSqlForGetOwnerOfDatabase("db1")
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{1},
		})
		bh.sql2result[sql] = mrs

		ok, _, err := checkRoleWhetherDatabaseOwner(ses.GetTxnHandler().GetTxnCtx(), ses, "db1", false)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeFalse)
	})

}

func TestGetSqlForCheckRoleHasPrivilegeWGODependsOnPrivType(t *testing.T) {
	t.Run("account scoped privileges include account all or ownership", func(t *testing.T) {
		privTypes := []PrivilegeType{
			PrivilegeTypeCreateAccount,
			PrivilegeTypeDropAccount,
			PrivilegeTypeAlterAccount,
			PrivilegeTypeUpgradeAccount,
			PrivilegeTypeCreateUser,
			PrivilegeTypeDropUser,
			PrivilegeTypeAlterUser,
			PrivilegeTypeCreateRole,
			PrivilegeTypeDropRole,
			PrivilegeTypeAlterRole,
			PrivilegeTypeCreateDatabase,
			PrivilegeTypeDropDatabase,
			PrivilegeTypeShowDatabases,
			PrivilegeTypeConnect,
			PrivilegeTypeManageGrants,
			PrivilegeTypeAccountAll,
		}
		for _, privType := range privTypes {
			require.Equal(
				t,
				getSqlForCheckRoleHasPrivilegeWGOOrWithOwnership(
					int64(privType), int64(PrivilegeTypeAccountAll), int64(PrivilegeTypeAccountOwnership)),
				getSqlForCheckRoleHasPrivilegeWGODependsOnPrivType(privType),
			)
		}
	})

	t.Run("database scoped privileges include database all or ownership", func(t *testing.T) {
		privTypes := []PrivilegeType{
			PrivilegeTypeShowTables,
			PrivilegeTypeCreateTable,
			PrivilegeTypeDropTable,
			PrivilegeTypeCreateView,
			PrivilegeTypeDropView,
			PrivilegeTypeAlterView,
			PrivilegeTypeAlterTable,
			PrivilegeTypeDatabaseAll,
		}
		for _, privType := range privTypes {
			require.Equal(
				t,
				getSqlForCheckRoleHasPrivilegeWGOOrWithOwnership(
					int64(privType), int64(PrivilegeTypeDatabaseAll), int64(PrivilegeTypeDatabaseOwnership)),
				getSqlForCheckRoleHasPrivilegeWGODependsOnPrivType(privType),
			)
		}
	})

	t.Run("table scoped privileges include table all or ownership", func(t *testing.T) {
		privTypes := []PrivilegeType{
			PrivilegeTypeSelect,
			PrivilegeTypeInsert,
			PrivilegeTypeUpdate,
			PrivilegeTypeTruncate,
			PrivilegeTypeDelete,
			PrivilegeTypeReference,
			PrivilegeTypeIndex,
			PrivilegeTypeTableAll,
		}
		for _, privType := range privTypes {
			require.Equal(
				t,
				getSqlForCheckRoleHasPrivilegeWGOOrWithOwnership(
					int64(privType), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership)),
				getSqlForCheckRoleHasPrivilegeWGODependsOnPrivType(privType),
			)
		}
	})

	t.Run("ownership and other privileges use direct grant option sql", func(t *testing.T) {
		privTypes := []PrivilegeType{
			PrivilegeTypeAccountOwnership,
			PrivilegeTypeDatabaseOwnership,
			PrivilegeTypeTableOwnership,
			PrivilegeTypeExecute,
			PrivilegeTypeValues,
			PrivilegeTypeUserOwnership,
		}
		for _, privType := range privTypes {
			require.Equal(
				t,
				getSqlForCheckRoleHasPrivilegeWGO(int64(privType)),
				getSqlForCheckRoleHasPrivilegeWGODependsOnPrivType(privType),
			)
		}
	})
}

func TestAccountAllCoversOnlyAccountScopePrivileges(t *testing.T) {
	sql := getSqlForCheckRoleHasAccountLevelForStar(42, PrivilegeTypeCreateAccount)
	require.Contains(t, sql, fmt.Sprintf("rp.privilege_id in (%d)", PrivilegeTypeCreateAccount))
	require.NotContains(t, sql, fmt.Sprintf("%d,%d", PrivilegeTypeCreateAccount, PrivilegeTypeAccountAll))

	sql = getSqlForCheckRoleHasAccountLevelForStarWithSysScope(42, PrivilegeTypeCreateAccount, true)
	require.Contains(t, sql, fmt.Sprintf("rp.privilege_id in (%d,%d)",
		PrivilegeTypeCreateAccount,
		PrivilegeTypeAccountAll))

	sql = getSqlForCheckRoleHasAccountLevelForStar(42, PrivilegeTypeCreateUser)
	require.Contains(t, sql, fmt.Sprintf("rp.privilege_id in (%d,%d)",
		PrivilegeTypeCreateUser,
		PrivilegeTypeAccountAll))

	cache := &privilegeCache{}
	cache.add(objectTypeAccount, privilegeLevelStar, "", "", PrivilegeTypeAccountAll)
	require.True(t, cache.has(objectTypeAccount, privilegeLevelStar, "", "", PrivilegeTypeCreateUser))
	require.True(t, cache.has(objectTypeAccount, privilegeLevelStar, "", "", PrivilegeTypeDropUser))
	require.True(t, cache.has(objectTypeAccount, privilegeLevelStar, "", "", PrivilegeTypeAccountAll))
	require.False(t, cache.has(objectTypeAccount, privilegeLevelStar, "", "", PrivilegeTypeCreateAccount))
	require.False(t, cache.has(objectTypeAccount, privilegeLevelStar, "", "", PrivilegeTypeDropAccount))
	require.False(t, cache.has(objectTypeAccount, privilegeLevelStar, "", "", PrivilegeTypeAccountOwnership))
}

func TestGetSqlForScopedGrantOptionHelpers(t *testing.T) {
	require.Equal(
		t,
		`select role_id from mo_catalog.mo_role_privs where with_grant_option = true and privilege_id = 38 and obj_type = "table" and obj_id = 10001;`,
		getSqlForCheckRoleHasPrivilegeWGOWithObj(int64(PrivilegeTypeTableOwnership), objectTypeTable, 10001),
	)
	require.Equal(
		t,
		`select role_id from mo_catalog.mo_role_privs where with_grant_option = true and privilege_id = 38 and obj_type = "view";`,
		getSqlForCheckRoleHasPrivilegeWGOWithObjType(int64(PrivilegeTypeTableOwnership), objectTypeView),
	)
	require.Equal(
		t,
		`select role_id from mo_catalog.mo_role_privs where with_grant_option = true and privilege_id = 38 and obj_type = "table" and obj_id = 10001 and privilege_level in ("d.t","t");`,
		getSqlForCheckRoleHasPrivilegeWGOWithObjAndLevel(int64(PrivilegeTypeTableOwnership), objectTypeTable, 10001, privilegeLevelDatabaseTable),
	)
	require.Equal(
		t,
		`select distinct role_id from mo_catalog.mo_role_privs where ((with_grant_option = true and (privilege_id = 30 or privilege_id = 37)) or privilege_id = 38) and obj_type = "view" and obj_id = 10002 and privilege_level in ("d.*","*");`,
		getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership), objectTypeView, 10002, privilegeLevelDatabaseStar),
	)
}

func TestGetRoleSetThatPrivilegeGrantedToWGOScopedCoverageEdges(t *testing.T) {
	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, uint32(1001))
	roleID := int64(1001)

	t.Run("non table privilege falls back to unscoped query", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		sql := getSqlForCheckRoleHasPrivilegeWGODependsOnPrivType(PrivilegeTypeCreateDatabase)
		bh.sql2result[sql] = newMrsForPrivilegeWGO([][]interface{}{{roleID}})

		roleSet, err := getRoleSetThatPrivilegeGrantedToWGOScoped(
			ctx,
			nil,
			bh,
			PrivilegeTypeCreateDatabase,
			tree.OBJECT_TYPE_ACCOUNT,
			tree.PrivilegeLevel{Level: tree.PRIVILEGE_LEVEL_TYPE_STAR},
		)
		require.NoError(t, err)
		require.True(t, roleSet.Contains(roleID))
	})

	t.Run("database privilege uses database scoped grant option", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		checkDbSQL, err := getSqlForCheckDatabase(ctx, "db1")
		require.NoError(t, err)
		bh.sql2result[checkDbSQL] = newMrsForCheckDatabase([][]interface{}{{10001}})

		scopedSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndExactLevel(
			int64(PrivilegeTypeCreateTable), int64(PrivilegeTypeDatabaseAll), int64(PrivilegeTypeDatabaseOwnership),
			objectTypeDatabase, 10001, privilegeLevelDatabase)
		bh.sql2result[scopedSQL] = newMrsForPrivilegeWGO([][]interface{}{{roleID}})
		globalSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndExactLevel(
			int64(PrivilegeTypeCreateTable), int64(PrivilegeTypeDatabaseAll), int64(PrivilegeTypeDatabaseOwnership),
			objectTypeDatabase, objectIDAll, privilegeLevelStar)
		bh.sql2result[globalSQL] = newMrsForPrivilegeWGO([][]interface{}{})
		globalStarStarSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndExactLevel(
			int64(PrivilegeTypeCreateTable), int64(PrivilegeTypeDatabaseAll), int64(PrivilegeTypeDatabaseOwnership),
			objectTypeDatabase, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[globalStarStarSQL] = newMrsForPrivilegeWGO([][]interface{}{})

		roleSet, err := getRoleSetThatPrivilegeGrantedToWGOScoped(
			ctx,
			nil,
			bh,
			PrivilegeTypeCreateTable,
			tree.OBJECT_TYPE_DATABASE,
			tree.PrivilegeLevel{
				Level:   tree.PRIVILEGE_LEVEL_TYPE_TABLE,
				TabName: "db1",
			},
		)
		require.NoError(t, err)
		require.True(t, roleSet.Contains(roleID))
	})

	t.Run("database privilege ignores other database grant option and unscoped fallback", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		checkDbSQL, err := getSqlForCheckDatabase(ctx, "db1")
		require.NoError(t, err)
		bh.sql2result[checkDbSQL] = newMrsForCheckDatabase([][]interface{}{{10001}})

		scopedSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndExactLevel(
			int64(PrivilegeTypeCreateTable), int64(PrivilegeTypeDatabaseAll), int64(PrivilegeTypeDatabaseOwnership),
			objectTypeDatabase, 10001, privilegeLevelDatabase)
		bh.sql2result[scopedSQL] = newMrsForPrivilegeWGO([][]interface{}{})
		globalSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndExactLevel(
			int64(PrivilegeTypeCreateTable), int64(PrivilegeTypeDatabaseAll), int64(PrivilegeTypeDatabaseOwnership),
			objectTypeDatabase, objectIDAll, privilegeLevelStar)
		bh.sql2result[globalSQL] = newMrsForPrivilegeWGO([][]interface{}{})
		globalStarStarSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndExactLevel(
			int64(PrivilegeTypeCreateTable), int64(PrivilegeTypeDatabaseAll), int64(PrivilegeTypeDatabaseOwnership),
			objectTypeDatabase, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[globalStarStarSQL] = newMrsForPrivilegeWGO([][]interface{}{})

		otherDbSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndExactLevel(
			int64(PrivilegeTypeCreateTable), int64(PrivilegeTypeDatabaseAll), int64(PrivilegeTypeDatabaseOwnership),
			objectTypeDatabase, 10002, privilegeLevelDatabase)
		bh.sql2result[otherDbSQL] = newMrsForPrivilegeWGO([][]interface{}{{roleID}})
		unscopedSQL := getSqlForCheckRoleHasPrivilegeWGODependsOnPrivType(PrivilegeTypeCreateTable)
		bh.sql2result[unscopedSQL] = newMrsForPrivilegeWGO([][]interface{}{{roleID}})

		roleSet, err := getRoleSetThatPrivilegeGrantedToWGOScoped(
			ctx,
			nil,
			bh,
			PrivilegeTypeCreateTable,
			tree.OBJECT_TYPE_DATABASE,
			tree.PrivilegeLevel{
				Level:   tree.PRIVILEGE_LEVEL_TYPE_TABLE,
				TabName: "db1",
			},
		)
		require.NoError(t, err)
		require.False(t, roleSet.Contains(roleID))
		require.NotContains(t, strings.Join(bh.executedSQLs, "\n"), otherDbSQL)
		require.NotContains(t, strings.Join(bh.executedSQLs, "\n"), unscopedSQL)
	})

	t.Run("database privilege is covered by global database grant option", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		checkDbSQL, err := getSqlForCheckDatabase(ctx, "db1")
		require.NoError(t, err)
		bh.sql2result[checkDbSQL] = newMrsForCheckDatabase([][]interface{}{{10001}})

		scopedSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndExactLevel(
			int64(PrivilegeTypeCreateTable), int64(PrivilegeTypeDatabaseAll), int64(PrivilegeTypeDatabaseOwnership),
			objectTypeDatabase, 10001, privilegeLevelDatabase)
		bh.sql2result[scopedSQL] = newMrsForPrivilegeWGO([][]interface{}{})
		globalSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndExactLevel(
			int64(PrivilegeTypeCreateTable), int64(PrivilegeTypeDatabaseAll), int64(PrivilegeTypeDatabaseOwnership),
			objectTypeDatabase, objectIDAll, privilegeLevelStar)
		bh.sql2result[globalSQL] = newMrsForPrivilegeWGO([][]interface{}{{roleID}})
		globalStarStarSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndExactLevel(
			int64(PrivilegeTypeCreateTable), int64(PrivilegeTypeDatabaseAll), int64(PrivilegeTypeDatabaseOwnership),
			objectTypeDatabase, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[globalStarStarSQL] = newMrsForPrivilegeWGO([][]interface{}{{roleID + 1}})

		roleSet, err := getRoleSetThatPrivilegeGrantedToWGOScoped(
			ctx,
			nil,
			bh,
			PrivilegeTypeCreateTable,
			tree.OBJECT_TYPE_DATABASE,
			tree.PrivilegeLevel{
				Level:   tree.PRIVILEGE_LEVEL_TYPE_TABLE,
				TabName: "db1",
			},
		)
		require.NoError(t, err)
		require.True(t, roleSet.Contains(roleID))
		require.True(t, roleSet.Contains(roleID+1))
	})

	t.Run("missing database checks only global database grant option", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		checkDbSQL, err := getSqlForCheckDatabase(ctx, "missing_db")
		require.NoError(t, err)
		bh.sql2result[checkDbSQL] = newMrsForCheckDatabase([][]interface{}{})

		globalSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndExactLevel(
			int64(PrivilegeTypeCreateTable), int64(PrivilegeTypeDatabaseAll), int64(PrivilegeTypeDatabaseOwnership),
			objectTypeDatabase, objectIDAll, privilegeLevelStar)
		bh.sql2result[globalSQL] = newMrsForPrivilegeWGO([][]interface{}{})
		globalStarStarSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndExactLevel(
			int64(PrivilegeTypeCreateTable), int64(PrivilegeTypeDatabaseAll), int64(PrivilegeTypeDatabaseOwnership),
			objectTypeDatabase, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[globalStarStarSQL] = newMrsForPrivilegeWGO([][]interface{}{})
		otherDbSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndExactLevel(
			int64(PrivilegeTypeCreateTable), int64(PrivilegeTypeDatabaseAll), int64(PrivilegeTypeDatabaseOwnership),
			objectTypeDatabase, 10002, privilegeLevelDatabase)
		bh.sql2result[otherDbSQL] = newMrsForPrivilegeWGO([][]interface{}{{roleID}})

		roleSet, err := getRoleSetThatPrivilegeGrantedToWGOScoped(
			ctx,
			nil,
			bh,
			PrivilegeTypeCreateTable,
			tree.OBJECT_TYPE_DATABASE,
			tree.PrivilegeLevel{
				Level:   tree.PRIVILEGE_LEVEL_TYPE_TABLE,
				TabName: "missing_db",
			},
		)
		require.NoError(t, err)
		require.False(t, roleSet.Contains(roleID))
		require.NotContains(t, strings.Join(bh.executedSQLs, "\n"), otherDbSQL)
	})

	t.Run("database star privilege uses database global star and star star grant option", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		globalSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndExactLevel(
			int64(PrivilegeTypeCreateTable), int64(PrivilegeTypeDatabaseAll), int64(PrivilegeTypeDatabaseOwnership),
			objectTypeDatabase, objectIDAll, privilegeLevelStar)
		bh.sql2result[globalSQL] = newMrsForPrivilegeWGO([][]interface{}{})
		globalStarStarSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndExactLevel(
			int64(PrivilegeTypeCreateTable), int64(PrivilegeTypeDatabaseAll), int64(PrivilegeTypeDatabaseOwnership),
			objectTypeDatabase, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[globalStarStarSQL] = newMrsForPrivilegeWGO([][]interface{}{{roleID}})

		roleSet, err := getRoleSetThatPrivilegeGrantedToWGOScoped(
			ctx,
			nil,
			bh,
			PrivilegeTypeCreateTable,
			tree.OBJECT_TYPE_DATABASE,
			tree.PrivilegeLevel{Level: tree.PRIVILEGE_LEVEL_TYPE_STAR},
		)
		require.NoError(t, err)
		require.True(t, roleSet.Contains(roleID))
		require.Contains(t, strings.Join(bh.executedSQLs, "\n"), globalSQL)
		require.Contains(t, strings.Join(bh.executedSQLs, "\n"), globalStarStarSQL)
	})

	t.Run("database star star privilege uses database global star and star star grant option", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		globalSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndExactLevel(
			int64(PrivilegeTypeCreateTable), int64(PrivilegeTypeDatabaseAll), int64(PrivilegeTypeDatabaseOwnership),
			objectTypeDatabase, objectIDAll, privilegeLevelStar)
		bh.sql2result[globalSQL] = newMrsForPrivilegeWGO([][]interface{}{{roleID}})
		globalStarStarSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndExactLevel(
			int64(PrivilegeTypeCreateTable), int64(PrivilegeTypeDatabaseAll), int64(PrivilegeTypeDatabaseOwnership),
			objectTypeDatabase, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[globalStarStarSQL] = newMrsForPrivilegeWGO([][]interface{}{})

		roleSet, err := getRoleSetThatPrivilegeGrantedToWGOScoped(
			ctx,
			nil,
			bh,
			PrivilegeTypeCreateTable,
			tree.OBJECT_TYPE_DATABASE,
			tree.PrivilegeLevel{Level: tree.PRIVILEGE_LEVEL_TYPE_STAR_STAR},
		)
		require.NoError(t, err)
		require.True(t, roleSet.Contains(roleID))
		require.Contains(t, strings.Join(bh.executedSQLs, "\n"), globalSQL)
		require.Contains(t, strings.Join(bh.executedSQLs, "\n"), globalStarStarSQL)
	})

	t.Run("database ownership uses exact scoped ownership grant option", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		checkDbSQL, err := getSqlForCheckDatabase(ctx, "db1")
		require.NoError(t, err)
		bh.sql2result[checkDbSQL] = newMrsForCheckDatabase([][]interface{}{{10001}})

		scopedSQL := getSqlForCheckRoleHasPrivilegeWGOWithObjAndExactLevel(
			int64(PrivilegeTypeDatabaseOwnership), objectTypeDatabase, 10001, privilegeLevelDatabase)
		bh.sql2result[scopedSQL] = newMrsForPrivilegeWGO([][]interface{}{{roleID}})
		globalSQL := getSqlForCheckRoleHasPrivilegeWGOWithObjAndExactLevel(
			int64(PrivilegeTypeDatabaseOwnership), objectTypeDatabase, objectIDAll, privilegeLevelStar)
		bh.sql2result[globalSQL] = newMrsForPrivilegeWGO([][]interface{}{})
		globalStarStarSQL := getSqlForCheckRoleHasPrivilegeWGOWithObjAndExactLevel(
			int64(PrivilegeTypeDatabaseOwnership), objectTypeDatabase, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[globalStarStarSQL] = newMrsForPrivilegeWGO([][]interface{}{})

		roleSet, err := getRoleSetThatPrivilegeGrantedToWGOScoped(
			ctx,
			nil,
			bh,
			PrivilegeTypeDatabaseOwnership,
			tree.OBJECT_TYPE_DATABASE,
			tree.PrivilegeLevel{
				Level:   tree.PRIVILEGE_LEVEL_TYPE_TABLE,
				TabName: "db1",
			},
		)
		require.NoError(t, err)
		require.True(t, roleSet.Contains(roleID))
	})

	t.Run("database scoped privilege rejects unsupported database star level", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()

		roleSet, err := getRoleSetThatPrivilegeGrantedToWGOScoped(
			ctx,
			nil,
			bh,
			PrivilegeTypeCreateTable,
			tree.OBJECT_TYPE_DATABASE,
			tree.PrivilegeLevel{
				Level:  tree.PRIVILEGE_LEVEL_TYPE_DATABASE_STAR,
				DbName: "db1",
			},
		)
		require.Error(t, err)
		require.Nil(t, roleSet)
	})

	t.Run("view legacy exact ignores missing view", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		checkViewSQL, err := getSqlForCheckDatabaseView(ctx, "db1", "missing_v")
		require.NoError(t, err)
		bh.sql2result[checkViewSQL] = newMrsForCheckDatabaseTable([][]interface{}{})

		roleSet, err := getRoleSetThatViewPrivilegeGrantedToWGOLegacyExact(
			ctx,
			nil,
			bh,
			PrivilegeTypeSelect,
			tree.PrivilegeLevel{
				Level:   tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE,
				DbName:  "db1",
				TabName: "missing_v",
			},
		)
		require.NoError(t, err)
		require.Equal(t, 0, roleSet.Len())
	})

	t.Run("table ownership uses object scoped ownership query", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		sql := getSqlForCheckRoleHasPrivilegeWGOWithObj(int64(PrivilegeTypeTableOwnership), objectTypeTable, 10001)
		bh.sql2result[sql] = newMrsForPrivilegeWGO([][]interface{}{{roleID}})

		roleSet, err := getRoleSetThatPrivilegeGrantedToWGOWithObj(
			ctx, bh, PrivilegeTypeTableOwnership, objectTypeTable, 10001)
		require.NoError(t, err)
		require.True(t, roleSet.Contains(roleID))
	})

	t.Run("table ownership uses object type scoped ownership query", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		sql := getSqlForCheckRoleHasPrivilegeWGOWithObjType(int64(PrivilegeTypeTableOwnership), objectTypeView)
		bh.sql2result[sql] = newMrsForPrivilegeWGO([][]interface{}{{roleID}})

		roleSet, err := getRoleSetThatPrivilegeGrantedToWGOWithObjType(
			ctx, bh, PrivilegeTypeTableOwnership, objectTypeView)
		require.NoError(t, err)
		require.True(t, roleSet.Contains(roleID))
	})

	t.Run("unsupported scoped privilege falls back to unscoped WGO sql", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		sql := getSqlForCheckRoleHasPrivilegeWGODependsOnPrivType(PrivilegeTypeCreateDatabase)
		bh.sql2result[sql] = newMrsForPrivilegeWGO([][]interface{}{{roleID}})

		roleSet, err := getRoleSetThatPrivilegeGrantedToWGOWithObj(
			ctx, bh, PrivilegeTypeCreateDatabase, objectTypeTable, objectIDAll)
		require.NoError(t, err)
		require.True(t, roleSet.Contains(roleID))
	})

	t.Run("unsupported object type scoped privilege falls back to unscoped WGO sql", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		sql := getSqlForCheckRoleHasPrivilegeWGODependsOnPrivType(PrivilegeTypeCreateDatabase)
		bh.sql2result[sql] = newMrsForPrivilegeWGO([][]interface{}{{roleID}})

		roleSet, err := getRoleSetThatPrivilegeGrantedToWGOWithObjType(
			ctx, bh, PrivilegeTypeCreateDatabase, objectTypeView)
		require.NoError(t, err)
		require.True(t, roleSet.Contains(roleID))
	})

	t.Run("object scoped exec error is returned", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		sql := getSqlForCheckRoleHasPrivilegeWGOWithObj(int64(PrivilegeTypeTableOwnership), objectTypeTable, 10001)
		bh.sql2err[sql] = moerr.NewInternalError(ctx, "object scoped WGO failed")

		roleSet, err := getRoleSetThatPrivilegeGrantedToWGOWithObj(
			ctx, bh, PrivilegeTypeTableOwnership, objectTypeTable, 10001)
		require.Error(t, err)
		require.Nil(t, roleSet)
	})

	t.Run("object scoped row decode error is returned", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		sql := getSqlForCheckRoleHasPrivilegeWGOWithObj(int64(PrivilegeTypeTableOwnership), objectTypeTable, 10001)
		bh.sql2result[sql] = newMrsForPrivilegeWGO([][]interface{}{{"bad-role-id"}})

		roleSet, err := getRoleSetThatPrivilegeGrantedToWGOWithObj(
			ctx, bh, PrivilegeTypeTableOwnership, objectTypeTable, 10001)
		require.Error(t, err)
		require.Nil(t, roleSet)
	})

	t.Run("object type scoped exec error is returned", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		sql := getSqlForCheckRoleHasPrivilegeWGOWithObjType(int64(PrivilegeTypeTableOwnership), objectTypeTable)
		bh.sql2err[sql] = moerr.NewInternalError(ctx, "object type scoped WGO failed")

		roleSet, err := getRoleSetThatPrivilegeGrantedToWGOWithObjType(
			ctx, bh, PrivilegeTypeTableOwnership, objectTypeTable)
		require.Error(t, err)
		require.Nil(t, roleSet)
	})

	t.Run("object type scoped get result error is returned", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		sql := getSqlForCheckRoleHasPrivilegeWGOWithObjType(int64(PrivilegeTypeTableOwnership), objectTypeTable)

		roleSet, err := getRoleSetThatPrivilegeGrantedToWGOWithObjType(
			ctx, bh, PrivilegeTypeTableOwnership, objectTypeTable)
		require.Error(t, err)
		require.Nil(t, roleSet)
		require.Equal(t, sql, bh.currentSql)
	})

	t.Run("object type scoped row decode error is returned", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		sql := getSqlForCheckRoleHasPrivilegeWGOWithObjType(int64(PrivilegeTypeTableOwnership), objectTypeTable)
		bh.sql2result[sql] = newMrsForPrivilegeWGO([][]interface{}{{"bad-role-id"}})

		roleSet, err := getRoleSetThatPrivilegeGrantedToWGOWithObjType(
			ctx, bh, PrivilegeTypeTableOwnership, objectTypeTable)
		require.Error(t, err)
		require.Nil(t, roleSet)
	})

	t.Run("unresolved object falls back to broader scope and honors global grant option", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newSes(nil, ctrl)
		ses.SetDatabaseName("db")

		bh := &backgroundExecTest{}
		bh.init()
		checkTableSQL, err := getSqlForCheckDatabaseTable(ctx, "db", "missing_table")
		require.NoError(t, err)
		bh.sql2result[checkTableSQL] = newMrsForCheckDatabaseTable([][]interface{}{})

		globalSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[globalSQL] = newMrsForPrivilegeWGO([][]interface{}{{roleID}})

		checkDbSQL, err := getSqlForCheckDatabase(ctx, "db")
		require.NoError(t, err)
		bh.sql2result[checkDbSQL] = newMrsForCheckDatabase([][]interface{}{{10002}})
		dbStarSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10002, privilegeLevelDatabaseStar)
		bh.sql2result[dbStarSQL] = newMrsForPrivilegeWGO([][]interface{}{})

		// The legacy obj_type-only fallback must never be consulted for a missing object.
		objTypeSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjType(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable)
		bh.sql2result[objTypeSQL] = newMrsForPrivilegeWGO([][]interface{}{{roleID}})

		roleSet, err := getRoleSetThatPrivilegeGrantedToWGOScoped(
			ctx,
			ses,
			bh,
			PrivilegeTypeSelect,
			tree.OBJECT_TYPE_TABLE,
			tree.PrivilegeLevel{
				Level:   tree.PRIVILEGE_LEVEL_TYPE_TABLE,
				TabName: "missing_table",
			},
		)
		require.NoError(t, err)
		require.True(t, roleSet.Contains(roleID))
		require.NotContains(t, strings.Join(bh.executedSQLs, "\n"), objTypeSQL)
	})

	t.Run("unresolved object is not satisfied by unrelated same-type grant", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newSes(nil, ctrl)
		ses.SetDatabaseName("db")

		bh := &backgroundExecTest{}
		bh.init()
		checkTableSQL, err := getSqlForCheckDatabaseTable(ctx, "db", "missing_table")
		require.NoError(t, err)
		bh.sql2result[checkTableSQL] = newMrsForCheckDatabaseTable([][]interface{}{})

		// Neither the global nor the db-wide scope grants this role grant option.
		globalSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[globalSQL] = newMrsForPrivilegeWGO([][]interface{}{})

		checkDbSQL, err := getSqlForCheckDatabase(ctx, "db")
		require.NoError(t, err)
		bh.sql2result[checkDbSQL] = newMrsForCheckDatabase([][]interface{}{{10002}})
		dbStarSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10002, privilegeLevelDatabaseStar)
		bh.sql2result[dbStarSQL] = newMrsForPrivilegeWGO([][]interface{}{})

		// The role only holds grant option on an unrelated same-type object, which the
		// obj_type-only query would return; it must not satisfy the missing-object check.
		objTypeSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjType(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable)
		bh.sql2result[objTypeSQL] = newMrsForPrivilegeWGO([][]interface{}{{roleID}})

		roleSet, err := getRoleSetThatPrivilegeGrantedToWGOScoped(
			ctx,
			ses,
			bh,
			PrivilegeTypeSelect,
			tree.OBJECT_TYPE_TABLE,
			tree.PrivilegeLevel{
				Level:   tree.PRIVILEGE_LEVEL_TYPE_TABLE,
				TabName: "missing_table",
			},
		)
		require.NoError(t, err)
		require.False(t, roleSet.Contains(roleID))
		require.Equal(t, 0, roleSet.Len())
		require.NotContains(t, strings.Join(bh.executedSQLs, "\n"), objTypeSQL)
	})

	t.Run("unresolved object with missing database checks only the global scope", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newSes(nil, ctrl)
		ses.SetDatabaseName("db")

		bh := &backgroundExecTest{}
		bh.init()
		checkTableSQL, err := getSqlForCheckDatabaseTable(ctx, "missing_db", "missing_table")
		require.NoError(t, err)
		bh.sql2result[checkTableSQL] = newMrsForCheckDatabaseTable([][]interface{}{})

		globalSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[globalSQL] = newMrsForPrivilegeWGO([][]interface{}{{roleID}})

		// The database itself is missing, so the db.* scope must be skipped, not errored.
		checkDbSQL, err := getSqlForCheckDatabase(ctx, "missing_db")
		require.NoError(t, err)
		bh.sql2result[checkDbSQL] = newMrsForCheckDatabase([][]interface{}{})
		dbStarLevelFragment := scopedGrantOptionPrivilegeLevelsSQL(privilegeLevelDatabaseStar)

		roleSet, err := getRoleSetThatPrivilegeGrantedToWGOScoped(
			ctx,
			ses,
			bh,
			PrivilegeTypeSelect,
			tree.OBJECT_TYPE_TABLE,
			tree.PrivilegeLevel{
				Level:   tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE,
				DbName:  "missing_db",
				TabName: "missing_table",
			},
		)
		require.NoError(t, err)
		require.True(t, roleSet.Contains(roleID))
		// We attempted to resolve the database, found it missing, and skipped db.*.
		require.Contains(t, strings.Join(bh.executedSQLs, "\n"), checkDbSQL)
		require.NotContains(t, strings.Join(bh.executedSQLs, "\n"), dbStarLevelFragment)
	})

	t.Run("object resolution exec error is returned without fallback", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newSes(nil, ctrl)
		ses.SetDatabaseName("db")

		bh := &backgroundExecTest{}
		bh.init()
		checkTableSQL, err := getSqlForCheckDatabaseTable(ctx, "db", "t1")
		require.NoError(t, err)
		bh.sql2err[checkTableSQL] = moerr.NewInternalError(ctx, "resolve table failed")
		objTypeSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjType(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable)
		bh.sql2result[objTypeSQL] = newMrsForPrivilegeWGO([][]interface{}{{roleID}})

		roleSet, err := getRoleSetThatPrivilegeGrantedToWGOScoped(
			ctx,
			ses,
			bh,
			PrivilegeTypeSelect,
			tree.OBJECT_TYPE_TABLE,
			tree.PrivilegeLevel{
				Level:   tree.PRIVILEGE_LEVEL_TYPE_TABLE,
				TabName: "t1",
			},
		)
		require.Error(t, err)
		require.Nil(t, roleSet)
		require.NotContains(t, strings.Join(bh.executedSQLs, "\n"), objTypeSQL)
	})

	t.Run("view primary path error is returned before legacy lookup", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newSes(nil, ctrl)
		ses.SetDatabaseName("db")

		bh := &backgroundExecTest{}
		bh.init()

		checkViewSQL, err := getSqlForCheckDatabaseView(ctx, "db", "v")
		require.NoError(t, err)
		bh.sql2result[checkViewSQL] = newMrsForCheckDatabaseTable([][]interface{}{{10001}})

		viewObjSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeView, 10001, privilegeLevelDatabaseTable)
		bh.sql2err[viewObjSQL] = moerr.NewInternalError(ctx, "view WGO failed")

		roleSet, err := getRoleSetThatPrivilegeGrantedToWGOScoped(
			ctx,
			ses,
			bh,
			PrivilegeTypeSelect,
			tree.OBJECT_TYPE_VIEW,
			tree.PrivilegeLevel{
				Level:   tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE,
				DbName:  "db",
				TabName: "v",
			},
		)
		require.Error(t, err)
		require.Nil(t, roleSet)
		require.Equal(t, viewObjSQL, bh.currentSql)
	})

	t.Run("view legacy path propagates legacy query error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newSes(nil, ctrl)
		ses.SetDatabaseName("db")

		bh := &backgroundExecTest{}
		bh.init()

		checkViewSQL, err := getSqlForCheckDatabaseView(ctx, "db", "v")
		require.NoError(t, err)
		bh.sql2result[checkViewSQL] = newMrsForCheckDatabaseTable([][]interface{}{{10001}})

		viewObjSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeView, 10001, privilegeLevelDatabaseTable)
		bh.sql2result[viewObjSQL] = newMrsForPrivilegeWGO([][]interface{}{})
		viewGlobalSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeView, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[viewGlobalSQL] = newMrsForPrivilegeWGO([][]interface{}{})
		checkDbSQL, err := getSqlForCheckDatabase(ctx, "db")
		require.NoError(t, err)
		bh.sql2result[checkDbSQL] = newMrsForCheckDatabase([][]interface{}{{10002}})
		viewDbSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeView, 10002, privilegeLevelDatabaseStar)
		bh.sql2result[viewDbSQL] = newMrsForPrivilegeWGO([][]interface{}{})

		legacyObjSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10001, privilegeLevelDatabaseTable)
		bh.sql2err[legacyObjSQL] = moerr.NewInternalError(ctx, "legacy view WGO failed")

		roleSet, err := getRoleSetThatPrivilegeGrantedToWGOScoped(
			ctx,
			ses,
			bh,
			PrivilegeTypeSelect,
			tree.OBJECT_TYPE_VIEW,
			tree.PrivilegeLevel{
				Level:   tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE,
				DbName:  "db",
				TabName: "v",
			},
		)
		require.Error(t, err)
		require.Nil(t, roleSet)
		require.Equal(t, legacyObjSQL, bh.currentSql)
	})

	t.Run("database star object scoped WGO error is returned", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newSes(nil, ctrl)
		ses.SetDatabaseName("db")

		bh := &backgroundExecTest{}
		bh.init()

		checkDbSQL, err := getSqlForCheckDatabase(ctx, "db")
		require.NoError(t, err)
		bh.sql2result[checkDbSQL] = newMrsForCheckDatabase([][]interface{}{{10001}})

		objSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10001, privilegeLevelDatabaseStar)
		bh.sql2err[objSQL] = moerr.NewInternalError(ctx, "object scoped WGO failed")

		roleSet, err := getRoleSetThatPrivilegeGrantedToWGOScoped(
			ctx,
			ses,
			bh,
			PrivilegeTypeSelect,
			tree.OBJECT_TYPE_TABLE,
			tree.PrivilegeLevel{
				Level:  tree.PRIVILEGE_LEVEL_TYPE_DATABASE_STAR,
				DbName: "db",
			},
		)
		require.Error(t, err)
		require.Nil(t, roleSet)
		require.Equal(t, objSQL, bh.currentSql)
	})

	t.Run("global scoped WGO error is returned", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newSes(nil, ctrl)
		ses.SetDatabaseName("db")

		bh := &backgroundExecTest{}
		bh.init()

		checkDbSQL, err := getSqlForCheckDatabase(ctx, "db")
		require.NoError(t, err)
		bh.sql2result[checkDbSQL] = newMrsForCheckDatabase([][]interface{}{{10001}})

		objSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10001, privilegeLevelDatabaseStar)
		bh.sql2result[objSQL] = newMrsForPrivilegeWGO([][]interface{}{})
		globalSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, objectIDAll, privilegeLevelStarStar)
		bh.sql2err[globalSQL] = moerr.NewInternalError(ctx, "global WGO failed")

		roleSet, err := getRoleSetThatPrivilegeGrantedToWGOScoped(
			ctx,
			ses,
			bh,
			PrivilegeTypeSelect,
			tree.OBJECT_TYPE_TABLE,
			tree.PrivilegeLevel{
				Level:  tree.PRIVILEGE_LEVEL_TYPE_DATABASE_STAR,
				DbName: "db",
			},
		)
		require.Error(t, err)
		require.Nil(t, roleSet)
		require.Equal(t, globalSQL, bh.currentSql)
	})

	t.Run("missing database id returns exact and global role set", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newSes(nil, ctrl)
		ses.SetDatabaseName("db")

		bh := &backgroundExecTest{}
		bh.init()

		checkTableSQL, err := getSqlForCheckDatabaseTable(ctx, "db", "t1")
		require.NoError(t, err)
		bh.sql2result[checkTableSQL] = newMrsForCheckDatabaseTable([][]interface{}{{10001}})
		checkDbSQL, err := getSqlForCheckDatabase(ctx, "db")
		require.NoError(t, err)
		bh.sql2result[checkDbSQL] = newMrsForCheckDatabase([][]interface{}{})

		objSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10001, privilegeLevelDatabaseTable)
		bh.sql2result[objSQL] = newMrsForPrivilegeWGO([][]interface{}{{roleID}})
		globalSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[globalSQL] = newMrsForPrivilegeWGO([][]interface{}{{roleID + 1}})

		roleSet, err := getRoleSetThatPrivilegeGrantedToWGOScoped(
			ctx,
			ses,
			bh,
			PrivilegeTypeSelect,
			tree.OBJECT_TYPE_TABLE,
			tree.PrivilegeLevel{
				Level:   tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE,
				DbName:  "db",
				TabName: "t1",
			},
		)
		require.NoError(t, err)
		require.True(t, roleSet.Contains(roleID))
		require.True(t, roleSet.Contains(roleID+1))
	})

	t.Run("database scoped WGO query error is returned", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newSes(nil, ctrl)
		ses.SetDatabaseName("db")

		bh := &backgroundExecTest{}
		bh.init()

		checkTableSQL, err := getSqlForCheckDatabaseTable(ctx, "db", "t1")
		require.NoError(t, err)
		bh.sql2result[checkTableSQL] = newMrsForCheckDatabaseTable([][]interface{}{{10001}})
		checkDbSQL, err := getSqlForCheckDatabase(ctx, "db")
		require.NoError(t, err)
		bh.sql2result[checkDbSQL] = newMrsForCheckDatabase([][]interface{}{{10002}})

		objSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10001, privilegeLevelDatabaseTable)
		bh.sql2result[objSQL] = newMrsForPrivilegeWGO([][]interface{}{})
		globalSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, objectIDAll, privilegeLevelStarStar)
		bh.sql2result[globalSQL] = newMrsForPrivilegeWGO([][]interface{}{})
		dbSQL := getSqlForCheckRoleHasPrivilegeWGOOrWithOwnershipWithObjAndLevel(
			int64(PrivilegeTypeSelect), int64(PrivilegeTypeTableAll), int64(PrivilegeTypeTableOwnership),
			objectTypeTable, 10002, privilegeLevelDatabaseStar)
		bh.sql2err[dbSQL] = moerr.NewInternalError(ctx, "database scoped WGO failed")

		roleSet, err := getRoleSetThatPrivilegeGrantedToWGOScoped(
			ctx,
			ses,
			bh,
			PrivilegeTypeSelect,
			tree.OBJECT_TYPE_TABLE,
			tree.PrivilegeLevel{
				Level:   tree.PRIVILEGE_LEVEL_TYPE_DATABASE_TABLE,
				DbName:  "db",
				TabName: "t1",
			},
		)
		require.Error(t, err)
		require.Nil(t, roleSet)
	})

	t.Run("merge tolerates nil and empty role sets", func(t *testing.T) {
		dst := &btree.Set[int64]{}
		dst.Insert(1)
		mergeRoleSets(nil, dst)
		mergeRoleSets(dst, nil)
		mergeRoleSets(dst, &btree.Set[int64]{})
		require.Equal(t, 1, dst.Len())

		src := &btree.Set[int64]{}
		src.Insert(2)
		mergeRoleSets(dst, src)
		require.True(t, dst.Contains(1))
		require.True(t, dst.Contains(2))
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
		pu.SV.KillRountinesInterval = 0
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
		pu.SV.KillRountinesInterval = 0
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
		pu.SV.KillRountinesInterval = 0
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
		pu.SV.KillRountinesInterval = 0
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

		stmt := &tree.CreateDatabase{
			Name: tree.Identifier("abc"),
		}

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql := fmt.Sprintf(initMoMysqlCompatibilityModeFormat, tenant.TenantID, tenant.GetTenant(), "abc", "", "", false)
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
		pu.SV.KillRountinesInterval = 0
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
		pu.SV.KillRountinesInterval = 0
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
		pu.SV.KillRountinesInterval = 0
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
		pu.SV.KillRountinesInterval = 0
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
		pu.SV.KillRountinesInterval = 0
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
		pu.SV.KillRountinesInterval = 0
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
		pu.SV.KillRountinesInterval = 0
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
		pu.SV.KillRountinesInterval = 0
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx, "")
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
		pu.SV.KillRountinesInterval = 0
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

		cs := &tree.CreateStage{
			IfNotExists: false,
			Name:        tree.Identifier("my_stage_test"),
			Url:         "s3://load/files/",
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
		pu.SV.KillRountinesInterval = 0
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

		cs := &tree.CreateStage{
			IfNotExists: false,
			Name:        tree.Identifier("my_stage_test"),
			Url:         "s3://load/files/",
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
		pu.SV.KillRountinesInterval = 0
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

		cs := &tree.CreateStage{
			IfNotExists: true,
			Name:        tree.Identifier("my_stage_test"),
			Url:         "file:///load/files/",
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
		pu.SV.KillRountinesInterval = 0
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

		cs := &tree.CreateStage{
			IfNotExists: false,
			Name:        tree.Identifier("my_stage_test"),
			Url:         "s3://load/files/",
			Credentials: tree.StageCredentials{
				Exist:       true,
				Credentials: []string{"AWS_KEY_ID", "1a2b3c", "AWS_SECRET_KEY", "4x5y6z"},
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
		pu.SV.KillRountinesInterval = 0
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx, "")
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
			Url:         "s3://load/files/",
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
		pu.SV.KillRountinesInterval = 0
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

		as := &tree.AlterStage{
			IfNotExists: false,
			Name:        tree.Identifier("my_stage_test"),
			UrlOption: tree.StageUrl{
				Exist: true,
				Url:   "s3://load/files/",
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
		pu.SV.KillRountinesInterval = 0
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

		as := &tree.AlterStage{
			IfNotExists: false,
			Name:        tree.Identifier("my_stage_test"),
			UrlOption: tree.StageUrl{
				Exist: false,
			},
			CredentialsOption: tree.StageCredentials{
				Exist:       true,
				Credentials: []string{"AWS_KEY_ID", "1a2b3c", "AWS_SECRET_KEY", "4x5y6z"},
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
		pu.SV.KillRountinesInterval = 0
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

		as := &tree.AlterStage{
			IfNotExists: true,
			Name:        tree.Identifier("my_stage_test"),
			UrlOption: tree.StageUrl{
				Exist: true,
				Url:   "s3://load/files/",
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
		pu.SV.KillRountinesInterval = 0
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

		as := &tree.AlterStage{
			IfNotExists: false,
			Name:        tree.Identifier("my_stage_test"),
			UrlOption: tree.StageUrl{
				Exist: true,
				Url:   "s3://load/files/",
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
		pu.SV.KillRountinesInterval = 0
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

		as := &tree.AlterStage{
			IfNotExists: true,
			Name:        tree.Identifier("my_stage_test"),
			UrlOption: tree.StageUrl{
				Exist: true,
				Url:   "s3://load/files/",
			},
			CredentialsOption: tree.StageCredentials{
				Exist:       true,
				Credentials: []string{"AWS_KEY_ID", "1a2b3c", "AWS_SECRET_KEY", "4x5y6z"},
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
		pu.SV.KillRountinesInterval = 0
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

		rm, _ := NewRoutineManager(ctx, "")
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
				Credentials: []string{"AWS_KEY_ID", "1a2b3c", "AWS_SECRET_KEY", "4x5y6z"},
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
		proc := testutil.NewProc(t)
		tConn := &testConn{}
		defer tConn.Close()
		writeExceptResult(tConn, []*Packet{
			{Length: 5, Payload: []byte("def add(a, b):\n"), SequenceID: 1},
			{Length: 5, Payload: []byte("  return a + b"), SequenceID: 2},
			{Length: 0, Payload: []byte(""), SequenceID: 3},
		})

		fs, err := fileservice.NewLocalFS(context.TODO(), defines.SharedFileServiceName, t.TempDir(), fileservice.DisabledCacheConfig, nil)
		convey.So(err, convey.ShouldBeNil)
		proc.Base.FileService = fs

		//parameter
		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		_, err = toml.DecodeFile("test/system_vars_config.toml", pu.SV)
		assert.Nil(t, err)
		pu.SV.SetDefaultValues()
		pu.SV.KillRountinesInterval = 0
		pu.SV.SaveQueryResult = "on"
		//file service
		pu.FileService = fs
		setPu("", pu)

		ioses, err := NewIOSession(tConn, pu, "")
		assert.Nil(t, err)
		proto := &testMysqlWriter{
			ioses: ioses,
		}

		ses := &Session{
			feSessionImpl: feSessionImpl{
				respr: NewMysqlResp(proto),
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
		pu.SV.KillRountinesInterval = 0
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		setPu("", pu)

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

		sql = fmt.Sprintf(
			"select kind from mo_catalog.mo_snapshots where sname = '%s' order by snapshot_id limit 1",
			string(ds.Name),
		)
		bh.sql2result[sql] = newMrsForPasswordOfUser([][]interface{}{})

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

		sql = fmt.Sprintf(
			"select kind from mo_catalog.mo_snapshots where sname = '%s' order by snapshot_id limit 1",
			string(ds.Name),
		)
		bh.sql2result[sql] = newMrsForPasswordOfUser([][]interface{}{})

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
		pu.SV.KillRountinesInterval = 0
		setPu("", pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx, "")
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
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		timeStamp, _ := timestamp.ParseTimestamp("2021-01-01 00:00:00")
		txnOperator.EXPECT().SnapshotTS().Return(timeStamp).AnyTimes()
		// process.
		ses.proc = testutil.NewProc(t)
		ses.proc.Base.TxnOperator = txnOperator
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
		pu.SV.KillRountinesInterval = 0
		setPu("", pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx, "")
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
		ses.proc = testutil.NewProc(t)
		ses.proc.Base.TxnOperator = txnOperator
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
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		timeStamp, _ := timestamp.ParseTimestamp("2021-01-01 00:00:00")
		txnOperator.EXPECT().SnapshotTS().Return(timeStamp).AnyTimes()
		// process.
		ses.proc = testutil.NewProc(t)
		ses.proc.Base.TxnOperator = txnOperator
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

		quotaErr := mockSnapshotQuotaResult(bh, tenant.Tenant, tenant.TenantID, tree.SNAPSHOTLEVELACCOUNT.String())
		convey.So(quotaErr, convey.ShouldBeNil)

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
		pu.SV.KillRountinesInterval = 0
		setPu("", pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx, "")
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
		ses.proc = testutil.NewProc(t)
		ses.proc.Base.TxnOperator = txnOperator
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

		quotaErr := mockSnapshotQuotaResult(bh, tenant.Tenant, tenant.TenantID, tree.SNAPSHOTLEVELACCOUNT.String())
		convey.So(quotaErr, convey.ShouldBeNil)

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
		pu.SV.KillRountinesInterval = 0
		setPu("", pu)
		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		rm, _ := NewRoutineManager(ctx, "")
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
		ses.proc = testutil.NewProc(t)
		ses.proc.Base.TxnOperator = txnOperator
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

		quotaErr := mockSnapshotQuotaResult(bh, tenant.Tenant, tenant.TenantID, tree.SNAPSHOTLEVELACCOUNT.String())
		convey.So(quotaErr, convey.ShouldBeNil)

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
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		timeStamp, _ := timestamp.ParseTimestamp("2021-01-01 00:00:00")
		txnOperator.EXPECT().SnapshotTS().Return(timeStamp).AnyTimes()
		// process.
		ses.proc = testutil.NewProc(t)
		ses.proc.Base.TxnOperator = txnOperator
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

		quotaErr := mockSnapshotQuotaResult(bh, tenant.Tenant, tenant.TenantID, tree.SNAPSHOTLEVELACCOUNT.String())
		convey.So(quotaErr, convey.ShouldBeNil)

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
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		timeStamp, _ := timestamp.ParseTimestamp("2021-01-01 00:00:00")
		txnOperator.EXPECT().SnapshotTS().Return(timeStamp).AnyTimes()
		// process.
		ses.proc = testutil.NewProc(t)
		ses.proc.Base.TxnOperator = txnOperator
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

		quotaErr := mockSnapshotQuotaResult(bh, tenant.Tenant, tenant.TenantID, tree.SNAPSHOTLEVELACCOUNT.String())
		convey.So(quotaErr, convey.ShouldBeNil)

		err := doCreateSnapshot(ctx, ses, cs)
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

func Test_getSqlForCheckDupPitrFormat(t *testing.T) {
	sql := getSqlForCheckDupPitrFormat(123, 456)
	assert.Equal(t, "select pitr_id from mo_catalog.mo_pitr where create_account = 123 and obj_id = 456;", sql)
}

func Test_checkPitrDup(t *testing.T) {
	convey.Convey("checkPitrDup false", t, func() {
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
			Level: tree.PITRLEVELACCOUNT,
		}

		sql := getSqlForCheckPitrDup(tenant.Tenant, 0, stmt)
		mrs := newMrsForPasswordOfUser([][]interface{}{})
		bh.sql2result[sql] = mrs

		isDup, err := checkPitrDup(ctx, bh, tenant.Tenant, 0, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(isDup, convey.ShouldBeFalse)
	})

	convey.Convey("checkPitrDup true", t, func() {
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
			Level: tree.PITRLEVELACCOUNT,
		}

		sql := getSqlForCheckPitrDup(tenant.Tenant, 0, stmt)
		mrs := newMrsForPasswordOfUser([][]interface{}{
			{1},
		})
		bh.sql2result[sql] = mrs

		isDup, err := checkPitrDup(ctx, bh, tenant.Tenant, 0, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(isDup, convey.ShouldBeTrue)
	})
}

func Test_determineUserHasPrivilegeSet(t *testing.T) {
	convey.Convey("determineUserHasPrivilegeSet error test", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newTestSession(t, ctrl)
		defer ses.Close()

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

		priv := &privilege{}
		ret, _, err := determineUserHasPrivilegeSet(ctx, ses, priv)
		convey.ShouldBeFalse(ret, false)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("determineUserHasPrivilegeSet error test", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newTestSession(t, ctrl)
		defer ses.Close()

		privStub := gostub.Stub(&privilegeCacheIsEnabled, func(ctx context.Context, ses *Session) (bool, error) {
			return false, moerr.NewInternalErrorNoCtx("")
		})
		defer privStub.Reset()

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

		priv := determinePrivilegeSetOfStatement(&tree.ShowDatabases{})
		_, _, err := determineUserHasPrivilegeSet(ctx, ses, priv)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("determineUserHasPrivilegeSet error test", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newTestSession(t, ctrl)
		defer ses.Close()

		privStub := gostub.Stub(&checkPrivilegeInCache, func(ctx context.Context, ses *Session, priv *privilege, enableCache bool) (bool, error) {
			return false, moerr.NewInternalErrorNoCtx("")
		})
		defer privStub.Reset()

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

		priv := determinePrivilegeSetOfStatement(&tree.CreateTable{})
		_, _, err := determineUserHasPrivilegeSet(ctx, ses, priv)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("determineUserHasPrivilegeSet error test", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newTestSession(t, ctrl)
		defer ses.Close()

		privStub := gostub.Stub(&checkPrivilegeInCache, func(ctx context.Context, ses *Session, priv *privilege, enableCache bool) (bool, error) {
			return true, nil
		})
		defer privStub.Reset()

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

		priv := determinePrivilegeSetOfStatement(&tree.ShowDatabases{})
		ret, _, err := determineUserHasPrivilegeSet(ctx, ses, priv)
		convey.ShouldBeFalse(ret, false)
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_determineUserCanGrantRolesToOthers(t *testing.T) {
	//gotRet, gotStats, err := determineUserCanGrantRolesToOthers(tt.args.ctx, tt.args.ses, tt.args.fromRoles)

	convey.Convey("determineUserCanGrantRolesToOthers error test", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := newTestSession(t, ctrl)
		defer ses.Close()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		lasStub := gostub.Stub(&loadAllSecondaryRoles, func(ctx context.Context, bh BackgroundExec, account *TenantInfo, roleSetOfCurrentUser *btree.Set[int64]) error {
			return moerr.NewInternalErrorNoCtx("")
		})
		defer lasStub.Reset()

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

		_, _, err := determineUserCanGrantPrivilegesToOthers(ctx, ses, nil)
		convey.So(err, convey.ShouldNotBeNil)

		_, _, err = determineUserCanGrantPrivilegesToOthers(ctx, ses, &tree.GrantPrivilege{})
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func Test_authenticateUserCanExecuteStatementWithObjectTypeNone(t *testing.T) {
	convey.Convey("Test authenticateUserCanExecuteStatementWithObjectTypeNone", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.RevokePrivilege{
			Privileges: []*tree.Privilege{
				{Type: tree.PRIVILEGE_TYPE_STATIC_SELECT},
				{Type: tree.PRIVILEGE_TYPE_STATIC_INSERT},
			},
			ObjType: tree.OBJECT_TYPE_TABLE,
			Level: &tree.PrivilegeLevel{
				Level: tree.PRIVILEGE_LEVEL_TYPE_STAR,
			},
		}

		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		ses.SetDatabaseName("db")

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("Test authenticateUserCanExecuteStatementWithObjectTypeNone", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.ShowAccounts{}

		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		ses.SetDatabaseName("db")

		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)

	})

	convey.Convey("Test authenticateUserCanExecuteStatementWithObjectTypeNone", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.ShowAccountUpgrade{}

		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		ses.SetDatabaseName("db")
		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("Test authenticateUserCanExecuteStatementWithObjectTypeNone", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.ShowLogserviceReplicas{}

		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		ses.SetDatabaseName("db")
		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("Test authenticateUserCanExecuteStatementWithObjectTypeNone", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.UpgradeStatement{}

		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		ses.SetDatabaseName("db")
		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})

	convey.Convey("Test authenticateUserCanExecuteStatementWithObjectTypeNone", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		stmt := &tree.DropCDC{
			Option: &tree.AllOrNotCDC{
				TaskName: "task1",
			},
		}

		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)
		ses.SetDatabaseName("db")
		ok, _, err := authenticateUserCanExecuteStatementWithObjectTypeNone(ses.GetTxnHandler().GetTxnCtx(), ses, stmt)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)
	})
}

func Test_determinePrivilegeSetOfStatement_CreateTableAsSelect(t *testing.T) {
	stmt := &tree.CreateTable{
		IsAsSelect: true,
	}
	priv := determinePrivilegeSetOfStatement(stmt)

	require.Equal(t, objectTypeDatabase, priv.objectType())
	require.NotEmpty(t, priv.entries)

	seen := make(map[PrivilegeType]bool)
	for _, entry := range priv.entries {
		seen[entry.privilegeId] = true
	}

	require.True(t, seen[PrivilegeTypeCreateTable])
	require.True(t, seen[PrivilegeTypeDatabaseAll])
	require.True(t, seen[PrivilegeTypeDatabaseOwnership])
	require.False(t, seen[PrivilegeTypeSelect])
	require.False(t, seen[PrivilegeTypeInsert])
}
