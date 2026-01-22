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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
)

func Test_handleCheckSnapshotFlushed(t *testing.T) {
	ctx := defines.AttachAccountId(context.TODO(), catalog.System_Account)
	convey.Convey("handleCheckSnapshotFlushed succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// Mock engine
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		// Mock txn operator
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		txnOperator.EXPECT().EnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(0)).AnyTimes()
		txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()
		txnOperator.EXPECT().CloneSnapshotOp(gomock.Any()).Return(txnOperator).AnyTimes()

		// Mock txn client
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		// Mock background exec
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().Close().Return().AnyTimes()
		bh.EXPECT().ClearExecResultSet().Return().AnyTimes()
		bh.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		// Mock exec result for snapshot query - snapshot not found
		erSnapshot := mock_frontend.NewMockExecResult(ctrl)
		erSnapshot.EXPECT().GetRowCount().Return(uint64(0)).AnyTimes()
		bh.EXPECT().GetExecResultSet().Return([]interface{}{erSnapshot}).AnyTimes()

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

		ec := newTestExecCtx(ctx, ctrl)
		stmt := &tree.CheckSnapshotFlushed{
			Name: tree.Identifier("test_snapshot"),
		}

		// Test with snapshot not found - should return false
		err = handleCheckSnapshotFlushed(ses, ec, stmt)
		convey.So(err, convey.ShouldBeNil)
		// Result should be false (snapshot not found)
		convey.So(ses.mrs.GetRowCount(), convey.ShouldEqual, 1)
	})
}

func Test_CheckSnapshotFlushed(t *testing.T) {
	ctx := context.Background()
	convey.Convey("CheckSnapshotFlushed invalid level", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		snapshotTS := types.BuildTS(1000, 0)
		record := &snapshotRecord{
			level: "invalid_level",
			ts:    1000,
		}
		de := &disttae.Engine{}

		_, err := CheckSnapshotFlushed(ctx, txnOperator, snapshotTS, de, record, nil)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(moerr.IsMoErrCode(err, moerr.ErrInternal), convey.ShouldBeTrue)
	})
}

func Test_GetSnapshotTS(t *testing.T) {
	convey.Convey("GetSnapshotTS succ", t, func() {
		record := &snapshotRecord{
			ts: 12345,
		}
		ts := GetSnapshotTS(record)
		convey.So(ts, convey.ShouldEqual, int64(12345))
	})
}

// errorStubExecutor implements executor.SQLExecutor and always returns an error
type errorStubExecutor struct{}

func (e *errorStubExecutor) Exec(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
	return executor.Result{}, moerr.NewInternalError(ctx, "executor error")
}

func (e *errorStubExecutor) ExecTxn(ctx context.Context, execFunc func(txn executor.TxnExecutor) error, opts executor.Options) error {
	return moerr.NewInternalError(ctx, "executor error")
}

func Test_GetSnapshotRecordByName(t *testing.T) {
	convey.Convey("GetSnapshotRecordByName invalid executor", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)

		// Use stub executor that returns an error to test error handling
		stubExecutor := &errorStubExecutor{}
		_, err := GetSnapshotRecordByName(context.Background(), stubExecutor, txnOperator, "test_snapshot")
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func Test_checkTableFlushTS(t *testing.T) {
	ctx := context.Background()
	convey.Convey("checkTableFlushTS succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockRel := mock_frontend.NewMockRelation(ctrl)

		record := &snapshotRecord{
			ts: 1000,
		}

		mockDb.EXPECT().Relation(ctx, "test_table", nil).Return(mockRel, nil)
		tableDef := &plan.TableDef{
			Indexes: []*plan.IndexDef{},
		}
		mockRel.EXPECT().GetTableDef(ctx).Return(tableDef)
		mockRel.EXPECT().GetFlushTS(ctx).Return(types.BuildTS(2000, 0), nil)

		flushed, err := checkTableFlushTS(ctx, mockDb, "test_table", record)
		convey.So(err, convey.ShouldBeNil)
		convey.So(flushed, convey.ShouldBeTrue)
	})
}

func Test_checkDBFlushTS(t *testing.T) {
	ctx := context.Background()
	convey.Convey("checkDBFlushTS succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)

		record := &snapshotRecord{
			ts: 1000,
		}

		// Note: checkDBFlushTS requires *disttae.Engine, not mock engine
		// For now, we'll test the error path with nil engine
		_, err := checkDBFlushTS(ctx, txnOperator, "test_db", nil, record)
		convey.So(err, convey.ShouldNotBeNil)
	})
}
