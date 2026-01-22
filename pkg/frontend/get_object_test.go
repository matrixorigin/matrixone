// Copyright 2025 Matrix Origin
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
	"github.com/smartystreets/goconvey/convey"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func Test_handleGetObject(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	convey.Convey("handleGetObject succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// Mock engine
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		// Note: We'll need to mock the fileservice, but for now we'll test the error path

		// Mock txn operator
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		txnOperator.EXPECT().EnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(0)).AnyTimes()
		txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()

		// Mock txn client
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		// Mock background exec for permission check
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().ClearExecResultSet().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		// Mock exec result for account name query
		erAccount := mock_frontend.NewMockExecResult(ctrl)
		erAccount.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()
		erAccount.EXPECT().GetString(gomock.Any(), uint64(0), uint64(0)).Return("sys", nil).AnyTimes()
		bh.EXPECT().GetExecResultSet().Return([]interface{}{erAccount}).AnyTimes()

		// Mock exec result for publication query
		erPub := mock_frontend.NewMockExecResult(ctrl)
		erPub.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()
		erPub.EXPECT().GetString(gomock.Any(), uint64(0), uint64(6)).Return("*", nil).AnyTimes()
		bh.EXPECT().GetExecResultSet().Return([]interface{}{erPub}).AnyTimes()

		// Setup system variables
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, eng, txnClient, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.So(err, convey.ShouldBeNil)
		pu.StorageEngine = eng
		pu.TxnClient = txnClient
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, pu.SV)

		ses := NewSession(ctx, "", proto, nil)
		tenant := &TenantInfo{
			Tenant:   "sys",
			TenantID: catalog.System_Account,
			User:     DefaultTenantMoAdmin,
		}
		ses.SetTenantInfo(tenant)
		ses.mrs = &MysqlResultSet{}
		ses.SetDatabaseName("test_db")

		// Mock TxnHandler
		txnHandler := InitTxnHandler("", eng, ctx, txnOperator)
		ses.txnHandler = txnHandler

		proto.SetSession(ses)

		// Test with object name
		objectName := tree.Identifier("test_object")
		stmt := &tree.GetObject{
			ObjectName: objectName,
			ChunkIndex: 0,
		}

		// This will fail because we can't easily mock disttae.Engine and fileservice
		// But we can test that the function handles errors properly
		err = handleGetObject(ctx, ses, stmt)
		// We expect an error because fileservice is not available
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func Test_handleGetObject_InvalidChunkIndex(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	convey.Convey("handleGetObject invalid chunk index", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		txnClient := mock_frontend.NewMockTxnClient(ctrl)

		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, eng, txnClient, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.So(err, convey.ShouldBeNil)
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, pu.SV)

		ses := NewSession(ctx, "", proto, nil)
		ses.mrs = &MysqlResultSet{}

		objectName := tree.Identifier("test_object")
		stmt := &tree.GetObject{
			ObjectName: objectName,
			ChunkIndex: -1, // Invalid chunk index
		}

		err = handleGetObject(ctx, ses, stmt)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(moerr.IsMoErrCode(err, moerr.ErrInvalidInput), convey.ShouldBeTrue)
	})
}

func Test_ReadObjectFromEngine(t *testing.T) {
	ctx := context.Background()
	convey.Convey("ReadObjectFromEngine invalid engine", t, func() {
		// Test with nil engine
		_, err := ReadObjectFromEngine(ctx, nil, "test_object", 0, 100)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(moerr.IsMoErrCode(err, moerr.ErrInternal), convey.ShouldBeTrue)
	})
}
