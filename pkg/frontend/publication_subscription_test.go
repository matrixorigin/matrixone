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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"
)

func Test_doCreatePublication(t *testing.T) {
	mockedAccountsResults := func(ctrl *gomock.Controller) []interface{} {
		er := mock_frontend.NewMockExecResult(ctrl)
		er.EXPECT().GetRowCount().Return(uint64(2)).AnyTimes()
		er.EXPECT().GetInt64(gomock.Any(), uint64(0), uint64(0)).Return(int64(0), nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(1)).Return("sys", nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(2)).Return("open", nil).AnyTimes()
		er.EXPECT().GetUint64(gomock.Any(), uint64(0), uint64(3)).Return(uint64(1), nil).AnyTimes()
		er.EXPECT().ColumnIsNull(gomock.Any(), uint64(0), uint64(4)).Return(true, nil).AnyTimes()

		er.EXPECT().GetInt64(gomock.Any(), uint64(1), uint64(0)).Return(int64(1), nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(1), uint64(1)).Return("acc1", nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(1), uint64(2)).Return("open", nil).AnyTimes()
		er.EXPECT().GetUint64(gomock.Any(), uint64(1), uint64(3)).Return(uint64(1), nil).AnyTimes()
		er.EXPECT().ColumnIsNull(gomock.Any(), uint64(1), uint64(4)).Return(true, nil).AnyTimes()
		return []interface{}{er}
	}

	mockedDbResults := func(ctrl *gomock.Controller) []interface{} {
		er := mock_frontend.NewMockExecResult(ctrl)
		er.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()
		er.EXPECT().GetUint64(gomock.Any(), uint64(0), uint64(0)).Return(uint64(0), nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(1)).Return("", nil).AnyTimes()
		return []interface{}{er}
	}

	mockedTblResults := func(ctrl *gomock.Controller) []interface{} {
		er := mock_frontend.NewMockExecResult(ctrl)
		er.EXPECT().GetRowCount().Return(uint64(2)).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(0)).Return("t1", nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(1), uint64(0)).Return("t2", nil).AnyTimes()
		return []interface{}{er}
	}

	mockedSubInfoResults := func(ctrl *gomock.Controller) []interface{} {
		er := mock_frontend.NewMockExecResult(ctrl)
		er.EXPECT().GetRowCount().Return(uint64(0)).AnyTimes()
		return []interface{}{er}
	}

	convey.Convey("check create publication", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		setGlobalPu(pu)

		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		ctx = defines.AttachAccount(ctx, sysAccountID, rootID, moAdminRoleID)

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().ClearExecResultSet().Return().AnyTimes()
		// get all accounts
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		bh.EXPECT().GetExecResultSet().Return(mockedAccountsResults(ctrl))
		// get db id and type
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		bh.EXPECT().GetExecResultSet().Return(mockedDbResults(ctrl))
		// show tables
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		bh.EXPECT().GetExecResultSet().Return(mockedTblResults(ctrl))
		// insert into mo_pubs
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		// getSubInfosFromPub
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		bh.EXPECT().GetExecResultSet().Return(mockedSubInfoResults(ctrl))
		// insertMoSubs
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		stmts, err := mysql.Parse(ctx, "create publication pub1 database db1 table t1 account all comment 'this is comment'", 1)
		if err != nil {
			return
		}

		err = createPublication(ctx, bh, stmts[0].(*tree.CreatePublication))
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_doAlterPublication(t *testing.T) {
	mockedAccountsResults := func(ctrl *gomock.Controller) []interface{} {
		er := mock_frontend.NewMockExecResult(ctrl)
		er.EXPECT().GetRowCount().Return(uint64(2)).AnyTimes()
		er.EXPECT().GetInt64(gomock.Any(), uint64(0), uint64(0)).Return(int64(0), nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(1)).Return("sys", nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(2)).Return("open", nil).AnyTimes()
		er.EXPECT().GetUint64(gomock.Any(), uint64(0), uint64(3)).Return(uint64(1), nil).AnyTimes()
		er.EXPECT().ColumnIsNull(gomock.Any(), uint64(0), uint64(4)).Return(true, nil).AnyTimes()

		er.EXPECT().GetInt64(gomock.Any(), uint64(1), uint64(0)).Return(int64(1), nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(1), uint64(1)).Return("acc1", nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(1), uint64(2)).Return("open", nil).AnyTimes()
		er.EXPECT().GetUint64(gomock.Any(), uint64(1), uint64(3)).Return(uint64(1), nil).AnyTimes()
		er.EXPECT().ColumnIsNull(gomock.Any(), uint64(1), uint64(4)).Return(true, nil).AnyTimes()
		return []interface{}{er}
	}

	mockedPubInfoResults := func(ctrl *gomock.Controller) []interface{} {
		er := mock_frontend.NewMockExecResult(ctrl)
		er.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(0)).Return("pub1", nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(1)).Return("db1", nil).AnyTimes()
		er.EXPECT().GetUint64(gomock.Any(), uint64(0), uint64(2)).Return(uint64(0), nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(3)).Return("*", nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(4)).Return("*", nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(5)).Return("", nil).AnyTimes()
		er.EXPECT().ColumnIsNull(gomock.Any(), uint64(0), uint64(6)).Return(true, nil).AnyTimes()
		er.EXPECT().GetUint64(gomock.Any(), uint64(0), uint64(7)).Return(uint64(0), nil).AnyTimes()
		er.EXPECT().GetUint64(gomock.Any(), uint64(0), uint64(8)).Return(uint64(0), nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(9)).Return("", nil).AnyTimes()
		return []interface{}{er}
	}

	mockedDbResults := func(ctrl *gomock.Controller) []interface{} {
		er := mock_frontend.NewMockExecResult(ctrl)
		er.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()
		er.EXPECT().GetUint64(gomock.Any(), uint64(0), uint64(0)).Return(uint64(0), nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(1)).Return("", nil).AnyTimes()
		return []interface{}{er}
	}

	mockedTblResults := func(ctrl *gomock.Controller) []interface{} {
		er := mock_frontend.NewMockExecResult(ctrl)
		er.EXPECT().GetRowCount().Return(uint64(2)).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(0)).Return("t1", nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(1), uint64(0)).Return("t2", nil).AnyTimes()
		return []interface{}{er}
	}

	mockedSubInfoResults := func(ctrl *gomock.Controller) []interface{} {
		er := mock_frontend.NewMockExecResult(ctrl)
		er.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()
		er.EXPECT().GetInt64(gomock.Any(), uint64(0), uint64(0)).Return(int64(1), nil).AnyTimes()
		er.EXPECT().ColumnIsNull(gomock.Any(), uint64(0), uint64(1)).Return(true, nil).AnyTimes()
		er.EXPECT().ColumnIsNull(gomock.Any(), uint64(0), uint64(2)).Return(true, nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(3)).Return("sys", nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(4)).Return("pub1", nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(5)).Return("db1", nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(6)).Return("*", nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(7)).Return("", nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(8)).Return("", nil).AnyTimes()
		er.EXPECT().GetInt64(gomock.Any(), uint64(0), uint64(9)).Return(int64(0), nil).AnyTimes()
		return []interface{}{er}
	}

	convey.Convey("check alter publication", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

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

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		setGlobalPu(pu)

		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		ctx = defines.AttachAccount(ctx, sysAccountID, rootID, moAdminRoleID)

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().ClearExecResultSet().Return().AnyTimes()
		// begin; commit; rollback
		bh.EXPECT().Exec(gomock.Any(), "begin;").Return(nil).AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), "commit;").Return(nil).AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), "rollback;").Return(nil).AnyTimes()
		// get all accounts
		bh.EXPECT().Exec(gomock.Any(), getAccountIdNamesSql).Return(nil).AnyTimes()
		bh.EXPECT().GetExecResultSet().Return(mockedAccountsResults(ctrl))
		// get pub info
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		bh.EXPECT().GetExecResultSet().Return(mockedPubInfoResults(ctrl))
		// get db id and type
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		bh.EXPECT().GetExecResultSet().Return(mockedDbResults(ctrl))
		// show tables
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		bh.EXPECT().GetExecResultSet().Return(mockedTblResults(ctrl))
		// getSubInfosFromPub
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		bh.EXPECT().GetExecResultSet().Return(mockedSubInfoResults(ctrl))
		// updateMoSubs
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		stmts, err := mysql.Parse(ctx, "alter publication pub1  account acc1 database db2 table t2 comment 'this is new comment'", 1)
		if err != nil {
			return
		}

		err = doAlterPublication(ctx, ses, stmts[0].(*tree.AlterPublication))
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_doDropPublication(t *testing.T) {
	mockedAccountsResults := func(ctrl *gomock.Controller) []interface{} {
		er := mock_frontend.NewMockExecResult(ctrl)
		er.EXPECT().GetRowCount().Return(uint64(2)).AnyTimes()
		er.EXPECT().GetInt64(gomock.Any(), uint64(0), uint64(0)).Return(int64(0), nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(1)).Return("sys", nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(2)).Return("open", nil).AnyTimes()
		er.EXPECT().GetUint64(gomock.Any(), uint64(0), uint64(3)).Return(uint64(1), nil).AnyTimes()
		er.EXPECT().ColumnIsNull(gomock.Any(), uint64(0), uint64(4)).Return(true, nil).AnyTimes()

		er.EXPECT().GetInt64(gomock.Any(), uint64(1), uint64(0)).Return(int64(1), nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(1), uint64(1)).Return("acc1", nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(1), uint64(2)).Return("open", nil).AnyTimes()
		er.EXPECT().GetUint64(gomock.Any(), uint64(1), uint64(3)).Return(uint64(1), nil).AnyTimes()
		er.EXPECT().ColumnIsNull(gomock.Any(), uint64(1), uint64(4)).Return(true, nil).AnyTimes()
		return []interface{}{er}
	}

	mockedPubInfoResults := func(ctrl *gomock.Controller) []interface{} {
		er := mock_frontend.NewMockExecResult(ctrl)
		er.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(0)).Return("pub1", nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(1)).Return("db1", nil).AnyTimes()
		er.EXPECT().GetUint64(gomock.Any(), uint64(0), uint64(2)).Return(uint64(0), nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(3)).Return("*", nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(4)).Return("*", nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(5)).Return("", nil).AnyTimes()
		er.EXPECT().ColumnIsNull(gomock.Any(), uint64(0), uint64(6)).Return(true, nil).AnyTimes()
		er.EXPECT().GetUint64(gomock.Any(), uint64(0), uint64(7)).Return(uint64(0), nil).AnyTimes()
		er.EXPECT().GetUint64(gomock.Any(), uint64(0), uint64(8)).Return(uint64(0), nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(9)).Return("", nil).AnyTimes()
		return []interface{}{er}
	}

	mockedSubInfoResults := func(ctrl *gomock.Controller) []interface{} {
		er := mock_frontend.NewMockExecResult(ctrl)
		er.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()
		er.EXPECT().GetInt64(gomock.Any(), uint64(0), uint64(0)).Return(int64(1), nil).AnyTimes()
		er.EXPECT().ColumnIsNull(gomock.Any(), uint64(0), uint64(1)).Return(true, nil).AnyTimes()
		er.EXPECT().ColumnIsNull(gomock.Any(), uint64(0), uint64(2)).Return(true, nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(3)).Return("sys", nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(4)).Return("pub1", nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(5)).Return("db1", nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(6)).Return("*", nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(7)).Return("", nil).AnyTimes()
		er.EXPECT().GetString(gomock.Any(), uint64(0), uint64(8)).Return("", nil).AnyTimes()
		er.EXPECT().GetInt64(gomock.Any(), uint64(0), uint64(9)).Return(int64(0), nil).AnyTimes()
		return []interface{}{er}
	}

	convey.Convey("check drop publication", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		setGlobalPu(pu)

		ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
		ctx = defines.AttachAccount(ctx, sysAccountID, rootID, moAdminRoleID)

		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().ClearExecResultSet().Return().AnyTimes()
		// get all accounts
		bh.EXPECT().Exec(gomock.Any(), getAccountIdNamesSql).Return(nil).AnyTimes()
		bh.EXPECT().GetExecResultSet().Return(mockedAccountsResults(ctrl))
		// get pub info
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		bh.EXPECT().GetExecResultSet().Return(mockedPubInfoResults(ctrl))
		// getSubInfosFromPub
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		bh.EXPECT().GetExecResultSet().Return(mockedSubInfoResults(ctrl))
		// deleteMoSubs
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		err := dropPublication(ctx, bh, true, "pub")
		convey.So(err, convey.ShouldBeNil)
	})
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
		_, err := getSqlForInsertIntoMoPubs(ctx, k.pubName, k.databaseName, 0, false, "", "", "", true)
		require.Equal(t, k.err, err != nil)
	}
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
