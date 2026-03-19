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
	"sync"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/golang/mock/gomock"
	"github.com/smartystreets/goconvey/convey"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var _ client.Workspace = (*testWorkspace)(nil)

type testWorkspace struct {
	start      bool
	incr       bool
	mu         sync.Mutex
	stack      []uint64
	stmtId     uint64
	reportErr1 bool
}

func (txn *testWorkspace) SetCloneTxn(snapshot int64) {}

func (txn *testWorkspace) Readonly() bool {
	panic("implement me")
}

func (txn *testWorkspace) PPString() string {
	//TODO implement me
	// panic("implement me")
	return ""
}

func (txn *testWorkspace) UpdateSnapshotWriteOffset() {
	//TODO implement me
	// panic("implement me")
}

func (txn *testWorkspace) GetSnapshotWriteOffset() int {
	//TODO implement me
	// panic("implement me")
	return 0
}

func newTestWorkspace() *testWorkspace {
	return &testWorkspace{}
}

func (txn *testWorkspace) StartStatement() {
	if txn.start {
		panic("BUG: StartStatement called twice")
	}
	txn.start = true
	txn.incr = false
}

func (txn *testWorkspace) EndStatement() {
	if !txn.start {
		panic("BUG: StartStatement not called")
	}

	txn.start = false
	txn.incr = false
}

func (txn *testWorkspace) IncrStatementID(ctx context.Context, commit bool) error {
	if !commit {
		if !txn.start {
			panic("BUG: StartStatement not called")
		}
		if txn.incr {
			panic("BUG: IncrStatementID called twice")
		}
		txn.incr = true
	}
	txn.mu.Lock()
	defer txn.mu.Unlock()
	txn.stack = append(txn.stack, txn.stmtId)
	txn.stmtId++
	return nil
}

func (txn *testWorkspace) RollbackLastStatement(ctx context.Context) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	if txn.reportErr1 {
		return moerr.NewInternalError(ctx, "rollback statement failed.")
	}
	if len(txn.stack) == 0 {
		panic("BUG: unbalance happens")
	}
	txn.stmtId--
	lastStmtId := txn.stack[len(txn.stack)-1]
	if txn.stmtId != lastStmtId {
		panic("BUG: wrong stmt id")
	}
	txn.stack = txn.stack[:len(txn.stack)-1]
	txn.incr = false
	return nil
}

func (t *testWorkspace) WriteOffset() uint64 {
	//TODO implement me
	panic("implement me")
}

func (t *testWorkspace) Adjust(writeOffset uint64) error {
	return nil
}

func (t *testWorkspace) Commit(ctx context.Context) ([]txn.TxnRequest, error) {
	//TODO implement me
	panic("implement me")
}

func (t *testWorkspace) Rollback(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (t *testWorkspace) IncrSQLCount() {
}

func (t *testWorkspace) GetSQLCount() uint64 {
	//TODO implement me
	panic("implement me")
}

func (t *testWorkspace) CloneSnapshotWS() client.Workspace {
	//TODO implement me
	panic("implement me")
}

func (t *testWorkspace) BindTxnOp(op client.TxnOperator) {
	//TODO implement me
	panic("implement me")
}

func (t *testWorkspace) SetHaveDDL(flag bool) {
	//TODO implement me
}

func (t *testWorkspace) GetHaveDDL() bool {
	return false
}

func TestWorkspace(t *testing.T) {
	convey.Convey("no panic", t, func() {
		convey.So(
			func() {
				wsp := newTestWorkspace()
				wsp.StartStatement()
				wsp.EndStatement()
			},
			convey.ShouldNotPanic,
		)
	})
	convey.Convey("end panic", t, func() {
		convey.So(
			func() {
				wsp := newTestWorkspace()
				wsp.EndStatement()
			},
			convey.ShouldPanic,
		)
	})
	convey.Convey("start panic 1", t, func() {
		convey.So(
			func() {
				wsp := newTestWorkspace()
				wsp.StartStatement()
				wsp.StartStatement()
			},
			convey.ShouldPanic,
		)
	})
	convey.Convey("incr panic 1", t, func() {
		convey.So(
			func() {
				wsp := newTestWorkspace()
				//no start
				err := wsp.IncrStatementID(context.TODO(), false)
				convey.So(err, convey.ShouldBeNil)
			},
			convey.ShouldPanic,
		)
	})
	convey.Convey("incr panic 2", t, func() {
		convey.So(
			func() {
				wsp := newTestWorkspace()
				wsp.StartStatement()
				err := wsp.IncrStatementID(context.TODO(), false)
				convey.So(err, convey.ShouldBeNil)
				//incr twice
				err = wsp.IncrStatementID(context.TODO(), false)
				convey.So(err, convey.ShouldBeNil)
			},
			convey.ShouldPanic,
		)
	})
	convey.Convey("rollback last statement panic 1", t, func() {
		convey.So(
			func() {
				wsp := newTestWorkspace()
				wsp.StartStatement()
				err := wsp.RollbackLastStatement(context.TODO())
				convey.So(err, convey.ShouldBeNil)
			},
			convey.ShouldPanic,
		)
	})
	convey.Convey("rollback last statement panic 2", t, func() {
		convey.So(
			func() {
				wsp := newTestWorkspace()
				wsp.StartStatement()
				err := wsp.IncrStatementID(context.TODO(), false)
				convey.So(err, convey.ShouldBeNil)
				err = wsp.RollbackLastStatement(context.TODO())
				convey.So(err, convey.ShouldBeNil)
				err = wsp.RollbackLastStatement(context.TODO())
				convey.So(err, convey.ShouldBeNil)
			},
			convey.ShouldPanic,
		)
	})
}

func newMockErrSession(t *testing.T, ctx context.Context, ctrl *gomock.Controller) *Session {
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, commitTS timestamp.Timestamp, options ...TxnOption) (client.TxnOperator, error) {
			txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
			txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
			txnOperator.EXPECT().Rollback(gomock.Any()).Return(moerr.NewInternalError(ctx, "throw error")).AnyTimes()
			txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
			txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
			txnOperator.EXPECT().EnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(0)).AnyTimes()
			txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()
			wsp := newTestWorkspace()
			txnOperator.EXPECT().GetWorkspace().Return(wsp).AnyTimes()
			txnOperator.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).Return().AnyTimes()
			return txnOperator, nil
		}).AnyTimes()
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()

	ses := newTestSession(t, ctrl)
	getPu("").TxnClient = txnClient
	getPu("").StorageEngine = eng
	ses.txnHandler.storage = eng
	var c clock.Clock
	_ = ses.GetTxnHandler().CreateTempStorage(c)
	return ses
}

func newMockErrSession2(t *testing.T, ctx context.Context, ctrl *gomock.Controller) *Session {
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, commitTS timestamp.Timestamp, options ...TxnOption) (client.TxnOperator, error) {
			txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
			txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
			txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
			txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
			txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
			txnOperator.EXPECT().EnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(0)).AnyTimes()
			txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()
			wsp := newTestWorkspace()
			wsp.reportErr1 = true
			txnOperator.EXPECT().GetWorkspace().Return(wsp).AnyTimes()
			txnOperator.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).Return().AnyTimes()
			return txnOperator, nil
		}).AnyTimes()
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()

	ses := newTestSession(t, ctrl)
	getPu("").TxnClient = txnClient
	getPu("").StorageEngine = eng
	ses.txnHandler.storage = eng

	var c clock.Clock
	_ = ses.GetTxnHandler().CreateTempStorage(c)
	return ses
}

func newMockErrSession3(t *testing.T, ctx context.Context, ctrl *gomock.Controller) *Session {
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, commitTS timestamp.Timestamp, options ...TxnOption) (client.TxnOperator, error) {
			txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
			txnOperator.EXPECT().Txn().Return(txn.TxnMeta{
				ID: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			}).AnyTimes()
			txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
			txnOperator.EXPECT().Commit(gomock.Any()).Return(moerr.NewInternalError(ctx, "r-w conflicts")).AnyTimes()
			txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
			txnOperator.EXPECT().EnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(0)).AnyTimes()
			txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()
			wsp := newTestWorkspace()
			wsp.reportErr1 = true
			txnOperator.EXPECT().GetWorkspace().Return(wsp).AnyTimes()
			txnOperator.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).Return().AnyTimes()
			return txnOperator, nil
		}).AnyTimes()
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()

	ses := newTestSession(t, ctrl)
	getPu("").TxnClient = txnClient
	getPu("").StorageEngine = eng
	ses.txnHandler.storage = eng

	var c clock.Clock
	_ = ses.GetTxnHandler().CreateTempStorage(c)
	return ses
}

func newMockErrSession4(t *testing.T, ctx context.Context, ctrl *gomock.Controller,
	newFunc func(ctx context.Context, commitTS timestamp.Timestamp, options ...TxnOption) (client.TxnOperator, error),
	restartTxnFunc func(ctx context.Context, txnOp TxnOperator, commitTS any, options ...any) (client.TxnOperator, error),
) *Session {
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(newFunc).AnyTimes()
	txnClient.EXPECT().RestartTxn(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(restartTxnFunc).AnyTimes()
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()

	ses := newTestSession(t, ctrl)
	getPu("").TxnClient = txnClient
	getPu("").StorageEngine = eng
	ses.txnHandler.storage = eng
	var c clock.Clock
	_ = ses.GetTxnHandler().CreateTempStorage(c)
	return ses
}

func Test_rollbackStatement(t *testing.T) {
	convey.Convey("normal rollback", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := defines.AttachAccountId(context.TODO(), sysAccountID)
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, commitTS timestamp.Timestamp, options ...TxnOption) (client.TxnOperator, error) {
				return newTestTxnOp(), nil
			}).AnyTimes()
		txnClient.EXPECT().RestartTxn(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, txnOp TxnOperator, commitTS timestamp.Timestamp, options ...TxnOption) (client.TxnOperator, error) {
				tTxnOp := txnOp.(*testTxnOp)
				tTxnOp.meta.Status = txn.TxnStatus_Active
				return txnOp, nil
			}).AnyTimes()
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Hints().Return(engine.Hints{
			CommitOrRollbackTimeout: time.Second,
		}).AnyTimes()

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()

		ses := newTestSession(t, ctrl)
		getPu("").TxnClient = txnClient
		ses.txnHandler.storage = eng

		ec := newTestExecCtx(ctx, ctrl)
		ec.ses = ses
		//case1. autocommit && not_begin. Insert Stmt (need not to be committed in the active txn)
		ec.txnOpt = FeTxnOption{
			autoCommit: true,
		}

		err := ses.GetTxnHandler().Create(ec)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_BEGIN), convey.ShouldBeFalse)
		convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_AUTOCOMMIT), convey.ShouldBeTrue)
		convey.So(!ses.GetTxnHandler().InMultiStmtTransactionMode(), convey.ShouldBeTrue)
		ec.stmt = &tree.Insert{}
		err = ses.GetTxnHandler().Rollback(ec)
		convey.So(err, convey.ShouldBeNil)
		t2 := ses.txnHandler.GetTxn()
		convey.So(t2, convey.ShouldBeNil)

		//case2.1 autocommit && begin && CreateSequence (need to be committed in the active txn)
		ec.txnOpt = FeTxnOption{
			autoCommit: true,
			byBegin:    true,
		}
		err = ses.GetTxnHandler().Create(ec)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_BEGIN), convey.ShouldBeTrue)
		convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_NOT_AUTOCOMMIT), convey.ShouldBeFalse)
		convey.So(!ses.GetTxnHandler().InMultiStmtTransactionMode(), convey.ShouldBeFalse)
		convey.So(ses.GetTxnHandler().InActiveTxn() &&
			NeedToBeCommittedInActiveTransaction(&tree.CreateSequence{}), convey.ShouldBeTrue)
		ec.stmt = &tree.CreateSequence{}
		err = ses.GetTxnHandler().Rollback(ec)
		convey.So(err, convey.ShouldBeNil)
		t2 = ses.txnHandler.GetTxn()
		convey.So(t2, convey.ShouldBeNil)

		//case2.2 not_autocommit && not_begin && CreateSequence (need to be committed in the active txn)
		ec.txnOpt = FeTxnOption{
			autoCommit: false,
		}
		err = ses.txnHandler.Create(ec)
		convey.So(err, convey.ShouldBeNil)
		err = ses.GetTxnHandler().SetAutocommit(ec, true, false)
		convey.So(err, convey.ShouldBeNil)
		_ = ses.txnHandler.GetTxn()
		convey.So(err, convey.ShouldBeNil)
		convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_BEGIN), convey.ShouldBeFalse)
		convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_NOT_AUTOCOMMIT), convey.ShouldBeTrue)
		convey.So(!ses.GetTxnHandler().InMultiStmtTransactionMode(), convey.ShouldBeFalse)
		convey.So(ses.GetTxnHandler().InActiveTxn() &&
			NeedToBeCommittedInActiveTransaction(&tree.CreateSequence{}), convey.ShouldBeTrue)
		ec.stmt = &tree.CreateSequence{}
		err = ses.GetTxnHandler().Rollback(ec)
		convey.So(err, convey.ShouldBeNil)
		t2 = ses.txnHandler.GetTxn()
		convey.So(t2, convey.ShouldBeNil)

		//case3.1 not_autocommit && not_begin && Insert Stmt (need not to be committed in the active txn)
		ec.txnOpt = FeTxnOption{
			autoCommit: false,
		}
		err = ses.txnHandler.Create(ec)
		convey.So(err, convey.ShouldBeNil)
		err = ses.GetTxnHandler().SetAutocommit(ec, true, false)
		var txnOp TxnOperator
		convey.So(err, convey.ShouldBeNil)
		txnOp = ses.txnHandler.GetTxn()
		convey.So(err, convey.ShouldBeNil)
		convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_BEGIN), convey.ShouldBeFalse)
		convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_NOT_AUTOCOMMIT), convey.ShouldBeTrue)
		convey.So(!ses.GetTxnHandler().InMultiStmtTransactionMode(), convey.ShouldBeFalse)
		convey.So(ses.GetTxnHandler().InActiveTxn() &&
			NeedToBeCommittedInActiveTransaction(&tree.Insert{}), convey.ShouldBeFalse)
		convey.So(txnOp != nil && !ses.IsDerivedStmt(), convey.ShouldBeTrue)
		//called incrStatement
		txnOp.GetWorkspace().StartStatement()
		err = txnOp.GetWorkspace().IncrStatementID(ctx, false)
		convey.So(err, convey.ShouldBeNil)
		ec.stmt = &tree.Insert{}
		err = ses.GetTxnHandler().Rollback(ec)
		convey.So(err, convey.ShouldBeNil)
		t2 = ses.txnHandler.GetTxn()
		convey.So(t2, convey.ShouldNotBeNil)
		txnOp.GetWorkspace().EndStatement()

		//case3.2 not_autocommit && begin && Insert Stmt (need not to be committed in the active txn)
		ec.txnOpt = FeTxnOption{
			autoCommit: false,
			byBegin:    true,
		}
		err = ses.txnHandler.Create(ec)
		convey.So(err, convey.ShouldBeNil)
		err = ses.GetTxnHandler().SetAutocommit(ec, true, false)
		convey.So(err, convey.ShouldBeNil)
		err = ses.GetTxnHandler().Create(ec)
		convey.So(err, convey.ShouldBeNil)
		txnOp = ses.GetTxnHandler().GetTxn()
		convey.So(err, convey.ShouldBeNil)
		convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_BEGIN), convey.ShouldBeTrue)
		convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_NOT_AUTOCOMMIT), convey.ShouldBeTrue)
		convey.So(!ses.GetTxnHandler().InMultiStmtTransactionMode(), convey.ShouldBeFalse)
		convey.So(ses.GetTxnHandler().InActiveTxn() &&
			NeedToBeCommittedInActiveTransaction(&tree.Insert{}), convey.ShouldBeFalse)
		convey.So(txnOp != nil && !ses.IsDerivedStmt(), convey.ShouldBeTrue)
		//called incrStatement
		txnOp.GetWorkspace().StartStatement()
		err = txnOp.GetWorkspace().IncrStatementID(ctx, false)
		convey.So(err, convey.ShouldBeNil)
		ec.stmt = &tree.Insert{}
		err = ses.GetTxnHandler().Rollback(ec)
		convey.So(err, convey.ShouldBeNil)
		t2 = ses.txnHandler.GetTxn()
		convey.So(t2, convey.ShouldNotBeNil)
		txnOp.GetWorkspace().EndStatement()

	})

	convey.Convey("abnormal rollback", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := defines.AttachAccountId(context.TODO(), sysAccountID)
		newFunc := func(ctx context.Context, commitTS timestamp.Timestamp, options ...TxnOption) (client.TxnOperator, error) {
			txnOp := newTestTxnOp()
			txnOp.mod = modRollbackError
			return txnOp, nil
		}
		restartTxnFunc := func(ctx context.Context, txnOp TxnOperator, commitTS any, options ...any) (client.TxnOperator, error) {
			tTxnOp := txnOp.(*testTxnOp)
			tTxnOp.meta.Status = txn.TxnStatus_Active
			return txnOp, nil
		}
		ses := newMockErrSession4(t, ctx, ctrl, newFunc, restartTxnFunc)
		ec := newTestExecCtx(ctx, ctrl)
		ec.ses = ses
		//case1. autocommit && not_begin. Insert Stmt (need not to be committed in the active txn)
		ec.txnOpt = FeTxnOption{
			autoCommit: true,
		}
		err := ses.GetTxnHandler().Create(ec)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_BEGIN), convey.ShouldBeFalse)
		convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_AUTOCOMMIT), convey.ShouldBeTrue)
		convey.So(!ses.GetTxnHandler().InMultiStmtTransactionMode(), convey.ShouldBeTrue)
		ec.stmt = &tree.Insert{}
		err = ses.GetTxnHandler().Rollback(ec)
		convey.So(err, convey.ShouldNotBeNil)
		t2 := ses.txnHandler.GetTxn()
		convey.So(t2, convey.ShouldBeNil)
	})
}

func Test_rollbackStatement2(t *testing.T) {
	convey.Convey("abnormal rollback", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := defines.AttachAccountId(context.TODO(), sysAccountID)
		ses := newMockErrSession(t, ctx, ctrl)
		ec := newTestExecCtx(ctx, ctrl)
		ec.ses = ses

		//case1. autocommit && not_begin. Insert Stmt (need not to be committed in the active txn)
		ec.txnOpt = FeTxnOption{
			autoCommit: true,
		}
		err := ses.GetTxnHandler().Create(ec)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_BEGIN), convey.ShouldBeFalse)
		convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_AUTOCOMMIT), convey.ShouldBeTrue)
		convey.So(!ses.GetTxnHandler().InMultiStmtTransactionMode(), convey.ShouldBeTrue)
		ec.stmt = &tree.Insert{}
		err = ses.GetTxnHandler().Rollback(ec)
		convey.So(err, convey.ShouldNotBeNil)
		t2 := ses.txnHandler.GetTxn()
		convey.So(t2, convey.ShouldBeNil)
	})
}

func Test_rollbackStatement3(t *testing.T) {
	convey.Convey("abnormal rollback", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := defines.AttachAccountId(context.TODO(), sysAccountID)
		ses := newMockErrSession(t, ctx, ctrl)
		ec := newTestExecCtx(ctx, ctrl)
		ec.ses = ses

		//case2.1 autocommit && begin && CreateSequence (need to be committed in the active txn)
		ec.txnOpt = FeTxnOption{
			autoCommit: true,
			byBegin:    true,
		}
		err := ses.GetTxnHandler().Create(ec)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_BEGIN), convey.ShouldBeTrue)
		convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_NOT_AUTOCOMMIT), convey.ShouldBeFalse)
		convey.So(!ses.GetTxnHandler().InMultiStmtTransactionMode(), convey.ShouldBeFalse)
		convey.So(ses.GetTxnHandler().InActiveTxn() &&
			NeedToBeCommittedInActiveTransaction(&tree.CreateSequence{}), convey.ShouldBeTrue)
		ec.stmt = &tree.CreateSequence{}
		err = ses.GetTxnHandler().Rollback(ec)
		convey.So(err, convey.ShouldNotBeNil)
		t2 := ses.txnHandler.GetTxn()
		convey.So(t2, convey.ShouldBeNil)
	})
}

func Test_rollbackStatement4(t *testing.T) {
	convey.Convey("abnormal rollback", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := defines.AttachAccountId(context.TODO(), sysAccountID)
		ses := newMockErrSession(t, ctx, ctrl)
		ec := newTestExecCtx(ctx, ctrl)
		ec.ses = ses
		//case2.2 not_autocommit && not_begin && CreateSequence (need to be committed in the active txn)
		err := ses.GetTxnHandler().Create(ec)
		convey.So(err, convey.ShouldBeNil)
		err = ses.GetTxnHandler().SetAutocommit(ec, true, false)
		convey.So(err, convey.ShouldBeNil)
		_ = ses.txnHandler.GetTxn()
		convey.So(err, convey.ShouldBeNil)
		convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_BEGIN), convey.ShouldBeFalse)
		convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_NOT_AUTOCOMMIT), convey.ShouldBeTrue)
		convey.So(!ses.GetTxnHandler().InMultiStmtTransactionMode(), convey.ShouldBeFalse)
		convey.So(ses.GetTxnHandler().InActiveTxn() &&
			NeedToBeCommittedInActiveTransaction(&tree.CreateSequence{}), convey.ShouldBeTrue)
		ec.stmt = &tree.CreateSequence{}
		err = ses.GetTxnHandler().Rollback(ec)
		convey.So(err, convey.ShouldNotBeNil)
		t2 := ses.txnHandler.GetTxn()
		convey.So(t2, convey.ShouldBeNil)
	})
}

func Test_rollbackStatement5(t *testing.T) {
	convey.Convey("abnormal rollback", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := defines.AttachAccountId(context.TODO(), sysAccountID)
		ses := newMockErrSession2(t, ctx, ctrl)
		var txnOp TxnOperator
		ec := newTestExecCtx(ctx, ctrl)
		ec.ses = ses
		//case3.1 not_autocommit && not_begin && Insert Stmt (need not to be committed in the active txn)
		err := ses.GetTxnHandler().Create(ec)
		convey.So(err, convey.ShouldBeNil)
		err = ses.GetTxnHandler().SetAutocommit(ec, true, false)
		convey.So(err, convey.ShouldBeNil)
		txnOp = ses.txnHandler.GetTxn()
		convey.So(err, convey.ShouldBeNil)
		convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_BEGIN), convey.ShouldBeFalse)
		convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_NOT_AUTOCOMMIT), convey.ShouldBeTrue)
		convey.So(!ses.GetTxnHandler().InMultiStmtTransactionMode(), convey.ShouldBeFalse)
		convey.So(ses.GetTxnHandler().InActiveTxn() &&
			NeedToBeCommittedInActiveTransaction(&tree.Insert{}), convey.ShouldBeFalse)
		convey.So(txnOp != nil && !ses.IsDerivedStmt(), convey.ShouldBeTrue)
		//called incrStatement
		txnOp.GetWorkspace().StartStatement()
		err = txnOp.GetWorkspace().IncrStatementID(ctx, false)
		convey.So(err, convey.ShouldBeNil)
		ec.stmt = &tree.Insert{}
		err = ses.GetTxnHandler().Rollback(ec)
		convey.So(err, convey.ShouldNotBeNil)
		t2 := ses.txnHandler.GetTxn()
		convey.So(t2, convey.ShouldBeNil)
		txnOp.GetWorkspace().EndStatement()
	})
}

func Test_rollbackStatement6(t *testing.T) {
	convey.Convey("abnormal rollback", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := defines.AttachAccountId(context.TODO(), sysAccountID)
		ses := newMockErrSession2(t, ctx, ctrl)
		var txnOp TxnOperator
		ec := newTestExecCtx(ctx, ctrl)
		ec.ses = ses

		//case3.2 not_autocommit && begin && Insert Stmt (need not to be committed in the active txn)
		err := ses.GetTxnHandler().SetAutocommit(ec, true, false)
		convey.So(err, convey.ShouldBeNil)
		ec.txnOpt = FeTxnOption{
			byBegin: true,
		}
		err = ses.GetTxnHandler().Create(ec)
		convey.So(err, convey.ShouldBeNil)
		txnOp = ses.GetTxnHandler().GetTxn()
		convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_BEGIN), convey.ShouldBeTrue)
		convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_NOT_AUTOCOMMIT), convey.ShouldBeTrue)
		convey.So(!ses.GetTxnHandler().InMultiStmtTransactionMode(), convey.ShouldBeFalse)
		convey.So(ses.GetTxnHandler().InActiveTxn() &&
			NeedToBeCommittedInActiveTransaction(&tree.Insert{}), convey.ShouldBeFalse)
		convey.So(txnOp != nil && !ses.IsDerivedStmt(), convey.ShouldBeTrue)
		//called incrStatement
		txnOp.GetWorkspace().StartStatement()
		err = txnOp.GetWorkspace().IncrStatementID(ctx, false)
		convey.So(err, convey.ShouldBeNil)
		ec.stmt = &tree.Insert{}
		err = ses.GetTxnHandler().Rollback(ec)
		convey.So(err, convey.ShouldNotBeNil)
		t2 := ses.txnHandler.GetTxn()
		convey.So(t2, convey.ShouldBeNil)
		txnOp.GetWorkspace().EndStatement()
	})
	convey.Convey("abnormal rollback -- rollback whole txn", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := defines.AttachAccountId(context.TODO(), sysAccountID)
		ses := newMockErrSession(t, ctx, ctrl)
		var txnOp TxnOperator
		ec := newTestExecCtx(ctx, ctrl)
		ec.ses = ses
		//case3.2 not_autocommit && begin && Insert Stmt (need not to be committed in the active txn)
		err := ses.GetTxnHandler().SetAutocommit(ec, true, false)
		convey.So(err, convey.ShouldBeNil)
		ec.txnOpt = FeTxnOption{
			byBegin: true,
		}
		err = ses.GetTxnHandler().Create(ec)
		convey.So(err, convey.ShouldBeNil)
		txnOp = ses.GetTxnHandler().GetTxn()
		convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_BEGIN), convey.ShouldBeTrue)
		convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_NOT_AUTOCOMMIT), convey.ShouldBeTrue)
		convey.So(!ses.GetTxnHandler().InMultiStmtTransactionMode(), convey.ShouldBeFalse)
		convey.So(ses.GetTxnHandler().InActiveTxn() &&
			NeedToBeCommittedInActiveTransaction(&tree.Insert{}), convey.ShouldBeFalse)
		convey.So(txnOp != nil && !ses.IsDerivedStmt(), convey.ShouldBeTrue)
		//called incrStatement
		txnOp.GetWorkspace().StartStatement()
		err = txnOp.GetWorkspace().IncrStatementID(ctx, false)
		convey.So(err, convey.ShouldBeNil)
		ec.stmt = &tree.Insert{}
		ec.txnOpt.byRollback = isErrorRollbackWholeTxn(getRandomErrorRollbackWholeTxn())
		err = ses.GetTxnHandler().Rollback(ec)
		convey.So(err, convey.ShouldNotBeNil)
		t2 := ses.txnHandler.GetTxn()
		convey.So(t2, convey.ShouldBeNil)
		txnOp.GetWorkspace().EndStatement()
	})
}

func Test_commit(t *testing.T) {
	convey.Convey("commit txn", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := defines.AttachAccountId(context.TODO(), sysAccountID)
		ses := newMockErrSession3(t, ctx, ctrl)
		var txnOp TxnOperator
		ec := newTestExecCtx(ctx, ctrl)
		ec.ses = ses
		ec.txnOpt = FeTxnOption{
			autoCommit: true,
		}
		err := ses.GetTxnHandler().Create(ec)
		convey.So(err, convey.ShouldBeNil)
		txnOp = ses.GetTxnHandler().GetTxn()
		convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_BEGIN), convey.ShouldBeFalse)
		convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_NOT_AUTOCOMMIT), convey.ShouldBeFalse)
		convey.So(!ses.GetTxnHandler().InMultiStmtTransactionMode(), convey.ShouldBeTrue)
		convey.So(ses.GetTxnHandler().InActiveTxn() &&
			NeedToBeCommittedInActiveTransaction(&tree.Insert{}), convey.ShouldBeFalse)
		convey.So(txnOp != nil && !ses.IsDerivedStmt(), convey.ShouldBeTrue)
		err = ses.GetTxnHandler().Commit(ec)
		fmt.Println(err)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

var _ TxnOperator = new(testTxnOp)

const (
	modRollbackError = 1
)

type testTxnOp struct {
	meta txn.TxnMeta
	wp   *testWorkspace
	mod  int
}

func newTestTxnOp() *testTxnOp {
	return &testTxnOp{
		wp: newTestWorkspace(),
	}
}

func (txnop *testTxnOp) GetOverview() client.TxnOverview {
	//TODO implement me
	panic("implement me")
}

func (txnop *testTxnOp) CloneSnapshotOp(snapshot timestamp.Timestamp) client.TxnOperator {
	//TODO implement me
	panic("implement me")
}

func (txnop *testTxnOp) IsSnapOp() bool {
	//TODO implement me
	panic("implement me")
}

func (txnop *testTxnOp) Txn() txn.TxnMeta {
	return txnop.meta
}

func (txnop *testTxnOp) TxnOptions() txn.TxnOptions {
	//TODO implement me
	panic("implement me")
}

func (txnop *testTxnOp) TxnRef() *txn.TxnMeta {
	//TODO implement me
	panic("implement me")
}

func (txnop *testTxnOp) Snapshot() (txn.CNTxnSnapshot, error) {
	//TODO implement me
	panic("implement me")
}

func (txnop *testTxnOp) UpdateSnapshot(ctx context.Context, ts timestamp.Timestamp) error {
	//TODO implement me
	panic("implement me")
}

func (txnop *testTxnOp) SnapshotTS() timestamp.Timestamp {
	//TODO implement me
	panic("implement me")
}

func (txnop *testTxnOp) CreateTS() timestamp.Timestamp {
	//TODO implement me
	panic("implement me")
}

func (txnop *testTxnOp) Status() txn.TxnStatus {
	return txnop.meta.Status
}

func (txnop *testTxnOp) ApplySnapshot(data []byte) error {
	//TODO implement me
	panic("implement me")
}

func (txnop *testTxnOp) Read(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {
	//TODO implement me
	panic("implement me")
}

func (txnop *testTxnOp) Write(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {
	//TODO implement me
	panic("implement me")
}

func (txnop *testTxnOp) WriteAndCommit(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {
	//TODO implement me
	panic("implement me")
}

func (txnop *testTxnOp) Commit(ctx context.Context) error {
	txnop.meta.Status = txn.TxnStatus_Committed
	return nil
}

func (txnop *testTxnOp) Rollback(ctx context.Context) error {
	if txnop.mod == modRollbackError {
		return moerr.NewInternalErrorNoCtx("throw error")
	}
	txnop.meta.Status = txn.TxnStatus_Aborted
	return nil
}

func (txnop *testTxnOp) AddLockTable(locktable lock.LockTable) error {
	//TODO implement me
	panic("implement me")
}

func (txnop *testTxnOp) HasLockTable(table uint64) bool {
	//TODO implement me
	panic("implement me")
}

func (txnop *testTxnOp) AddWaitLock(tableID uint64, rows [][]byte, opt lock.LockOptions) uint64 {
	//TODO implement me
	panic("implement me")
}

func (txnop *testTxnOp) RemoveWaitLock(key uint64) {
	//TODO implement me
	panic("implement me")
}

func (txnop *testTxnOp) LockSkipped(tableID uint64, mode lock.LockMode) bool {
	//TODO implement me
	panic("implement me")
}

func (txnop *testTxnOp) GetWaitActiveCost() time.Duration {
	//TODO implement me
	panic("implement me")
}

func (txnop *testTxnOp) AddWorkspace(workspace client.Workspace) {
	//TODO implement me
	panic("implement me")
}

func (txnop *testTxnOp) GetWorkspace() client.Workspace {
	return txnop.wp
}

func (txnop *testTxnOp) AppendEventCallback(event client.EventType, callbacks ...func(client.TxnEvent)) {
	//TODO implement me
	panic("implement me")
}

func (txnop *testTxnOp) Debug(ctx context.Context, ops []txn.TxnRequest) (*rpc.SendResult, error) {
	//TODO implement me
	panic("implement me")
}

func (txnop *testTxnOp) NextSequence() uint64 {
	return 0
}

func (txnop *testTxnOp) EnterRunSqlWithTokenAndSQL(_ context.CancelFunc, _ string) uint64 {
	return 0
}

func (txnop *testTxnOp) ExitRunSqlWithToken(_ uint64) {
}

func (txnop *testTxnOp) EnterIncrStmt() {
	//TODO implement me
	panic("implement me")
}

func (txnop *testTxnOp) ExitIncrStmt() {
	//TODO implement me
	panic("implement me")
}

func (txnop *testTxnOp) EnterRollbackStmt() {
	//TODO implement me
	panic("implement me")
}

func (txnop *testTxnOp) ExitRollbackStmt() {
	//TODO implement me
	panic("implement me")
}

func (txnop *testTxnOp) SetFootPrints(id int, enter bool) {

}

// TestAutocommitStatusSync tests that autocommit status is correctly preserved
// after transaction commit/rollback. This is the fix for the issue where
// SET autocommit=0 changes were being overwritten by invalidateTxnUnsafe.
func TestAutocommitStatusSync(t *testing.T) {
	convey.Convey("autocommit status sync after SET autocommit=0", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := defines.AttachAccountId(context.TODO(), sysAccountID)
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, commitTS timestamp.Timestamp, options ...TxnOption) (client.TxnOperator, error) {
				return newTestTxnOp(), nil
			}).AnyTimes()
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Hints().Return(engine.Hints{
			CommitOrRollbackTimeout: time.Second,
		}).AnyTimes()

		ses := newTestSession(t, ctrl)
		getPu("").TxnClient = txnClient
		ses.txnHandler.storage = eng

		ec := newTestExecCtx(ctx, ctrl)
		ec.ses = ses

		// Case 1: SET autocommit=0 should preserve status after commit
		// This is the main bug case - autocommit status was being reset to ON
		convey.Convey("SET autocommit=0 preserves status after commit", func() {
			// Start with autocommit=true (default)
			ec.txnOpt = FeTxnOption{autoCommit: true}
			err := ses.GetTxnHandler().Create(ec)
			convey.So(err, convey.ShouldBeNil)

			// Verify initial state: autocommit is ON
			serverStatus := ses.GetTxnHandler().GetServerStatus()
			convey.So(serverStatus&SERVER_STATUS_AUTOCOMMIT, convey.ShouldEqual, SERVER_STATUS_AUTOCOMMIT)

			// Execute SET autocommit=0 (on -> off)
			err = ses.GetTxnHandler().SetAutocommit(ec, true, false)
			convey.So(err, convey.ShouldBeNil)

			// Verify: autocommit should be OFF in serverStatus
			serverStatus = ses.GetTxnHandler().GetServerStatus()
			convey.So(serverStatus&SERVER_STATUS_AUTOCOMMIT, convey.ShouldEqual, uint16(0))
			convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_NOT_AUTOCOMMIT), convey.ShouldBeTrue)
			convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_AUTOCOMMIT), convey.ShouldBeFalse)

			// The fix ensures that after SetAutocommit commits the txn,
			// invalidateTxnUnsafe preserves the autocommit=OFF status
			// Before the fix: serverStatus would be reset to 0x0002 (AUTOCOMMIT=ON)
			// After the fix: serverStatus should remain 0x0000 (AUTOCOMMIT=OFF)
			serverStatus = ses.GetTxnHandler().GetServerStatus()
			convey.So(serverStatus&SERVER_STATUS_AUTOCOMMIT, convey.ShouldEqual, uint16(0))
		})
	})

	convey.Convey("autocommit status sync after SET autocommit=1", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := defines.AttachAccountId(context.TODO(), sysAccountID)
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, commitTS timestamp.Timestamp, options ...TxnOption) (client.TxnOperator, error) {
				return newTestTxnOp(), nil
			}).AnyTimes()
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Hints().Return(engine.Hints{
			CommitOrRollbackTimeout: time.Second,
		}).AnyTimes()

		ses := newTestSession(t, ctrl)
		getPu("").TxnClient = txnClient
		ses.txnHandler.storage = eng

		ec := newTestExecCtx(ctx, ctrl)
		ec.ses = ses

		// Case 2: SET autocommit=1 (off -> on) should work correctly
		convey.Convey("SET autocommit=1 from OFF state", func() {
			// Start with autocommit=false
			ec.txnOpt = FeTxnOption{autoCommit: false}
			err := ses.GetTxnHandler().Create(ec)
			convey.So(err, convey.ShouldBeNil)

			// Verify: autocommit is OFF
			serverStatus := ses.GetTxnHandler().GetServerStatus()
			convey.So(serverStatus&SERVER_STATUS_AUTOCOMMIT, convey.ShouldEqual, uint16(0))

			// Execute SET autocommit=1 (off -> on)
			err = ses.GetTxnHandler().SetAutocommit(ec, false, true)
			convey.So(err, convey.ShouldBeNil)

			// Verify: autocommit should be ON
			serverStatus = ses.GetTxnHandler().GetServerStatus()
			convey.So(serverStatus&SERVER_STATUS_AUTOCOMMIT, convey.ShouldEqual, SERVER_STATUS_AUTOCOMMIT)
			convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_NOT_AUTOCOMMIT), convey.ShouldBeFalse)
		})
	})

	convey.Convey("autocommit status multiple transitions", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := defines.AttachAccountId(context.TODO(), sysAccountID)
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, commitTS timestamp.Timestamp, options ...TxnOption) (client.TxnOperator, error) {
				return newTestTxnOp(), nil
			}).AnyTimes()
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Hints().Return(engine.Hints{
			CommitOrRollbackTimeout: time.Second,
		}).AnyTimes()

		ses := newTestSession(t, ctrl)
		getPu("").TxnClient = txnClient
		ses.txnHandler.storage = eng

		ec := newTestExecCtx(ctx, ctrl)
		ec.ses = ses

		// Case 3: Multiple transitions ON -> OFF -> ON
		convey.Convey("ON -> OFF -> ON transitions", func() {
			// Start with autocommit=true
			ec.txnOpt = FeTxnOption{autoCommit: true}
			err := ses.GetTxnHandler().Create(ec)
			convey.So(err, convey.ShouldBeNil)

			// Verify initial: autocommit ON
			serverStatus := ses.GetTxnHandler().GetServerStatus()
			convey.So(serverStatus&SERVER_STATUS_AUTOCOMMIT, convey.ShouldEqual, SERVER_STATUS_AUTOCOMMIT)

			// Transition 1: ON -> OFF
			err = ses.GetTxnHandler().SetAutocommit(ec, true, false)
			convey.So(err, convey.ShouldBeNil)
			serverStatus = ses.GetTxnHandler().GetServerStatus()
			convey.So(serverStatus&SERVER_STATUS_AUTOCOMMIT, convey.ShouldEqual, uint16(0))

			// Create a new transaction with autocommit=false
			ec.txnOpt = FeTxnOption{autoCommit: false}
			err = ses.GetTxnHandler().Create(ec)
			convey.So(err, convey.ShouldBeNil)

			// Verify: still OFF after new txn creation
			serverStatus = ses.GetTxnHandler().GetServerStatus()
			convey.So(serverStatus&SERVER_STATUS_AUTOCOMMIT, convey.ShouldEqual, uint16(0))

			// Transition 2: OFF -> ON
			err = ses.GetTxnHandler().SetAutocommit(ec, false, true)
			convey.So(err, convey.ShouldBeNil)
			serverStatus = ses.GetTxnHandler().GetServerStatus()
			convey.So(serverStatus&SERVER_STATUS_AUTOCOMMIT, convey.ShouldEqual, SERVER_STATUS_AUTOCOMMIT)
		})
	})
}

// TestInvalidateTxnUnsafePreservesAutocommit tests that invalidateTxnUnsafe
// correctly preserves autocommit-related flags while clearing transaction flags.
func TestInvalidateTxnUnsafePreservesAutocommit(t *testing.T) {
	convey.Convey("invalidateTxnUnsafe preserves autocommit flags", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := defines.AttachAccountId(context.TODO(), sysAccountID)
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, commitTS timestamp.Timestamp, options ...TxnOption) (client.TxnOperator, error) {
				return newTestTxnOp(), nil
			}).AnyTimes()
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Hints().Return(engine.Hints{
			CommitOrRollbackTimeout: time.Second,
		}).AnyTimes()

		ses := newTestSession(t, ctrl)
		getPu("").TxnClient = txnClient
		ses.txnHandler.storage = eng

		ec := newTestExecCtx(ctx, ctrl)
		ec.ses = ses

		// Test case: After commit, SERVER_STATUS_IN_TRANS should be cleared
		// but SERVER_STATUS_AUTOCOMMIT should be preserved based on session setting
		convey.Convey("commit clears IN_TRANS but preserves AUTOCOMMIT=OFF", func() {
			// Create txn with autocommit=false
			ec.txnOpt = FeTxnOption{autoCommit: false}
			err := ses.GetTxnHandler().Create(ec)
			convey.So(err, convey.ShouldBeNil)

			// Verify: IN_TRANS is set, AUTOCOMMIT is not set
			serverStatus := ses.GetTxnHandler().GetServerStatus()
			convey.So(serverStatus&SERVER_STATUS_IN_TRANS, convey.ShouldEqual, SERVER_STATUS_IN_TRANS)
			convey.So(serverStatus&SERVER_STATUS_AUTOCOMMIT, convey.ShouldEqual, uint16(0))

			// Commit the transaction using byCommit flag (simulates COMMIT statement)
			ec.txnOpt.byCommit = true
			err = ses.GetTxnHandler().Commit(ec)
			convey.So(err, convey.ShouldBeNil)

			// After commit: IN_TRANS should be cleared, AUTOCOMMIT should still be OFF
			serverStatus = ses.GetTxnHandler().GetServerStatus()
			convey.So(serverStatus&SERVER_STATUS_IN_TRANS, convey.ShouldEqual, uint16(0))
			convey.So(serverStatus&SERVER_STATUS_AUTOCOMMIT, convey.ShouldEqual, uint16(0))
		})

		convey.Convey("commit clears IN_TRANS but preserves AUTOCOMMIT=ON", func() {
			// Create txn with autocommit=true (single-statement mode)
			ec.txnOpt = FeTxnOption{autoCommit: true}
			err := ses.GetTxnHandler().Create(ec)
			convey.So(err, convey.ShouldBeNil)

			// Verify: IN_TRANS is set, AUTOCOMMIT is set
			serverStatus := ses.GetTxnHandler().GetServerStatus()
			convey.So(serverStatus&SERVER_STATUS_IN_TRANS, convey.ShouldEqual, SERVER_STATUS_IN_TRANS)
			convey.So(serverStatus&SERVER_STATUS_AUTOCOMMIT, convey.ShouldEqual, SERVER_STATUS_AUTOCOMMIT)

			// Commit the transaction (in single-statement mode, any statement commits)
			ec.stmt = &tree.Select{}
			err = ses.GetTxnHandler().Commit(ec)
			convey.So(err, convey.ShouldBeNil)

			// After commit: IN_TRANS should be cleared, AUTOCOMMIT should still be ON
			serverStatus = ses.GetTxnHandler().GetServerStatus()
			convey.So(serverStatus&SERVER_STATUS_IN_TRANS, convey.ShouldEqual, uint16(0))
			convey.So(serverStatus&SERVER_STATUS_AUTOCOMMIT, convey.ShouldEqual, SERVER_STATUS_AUTOCOMMIT)
		})

		convey.Convey("rollback clears IN_TRANS but preserves AUTOCOMMIT=OFF", func() {
			// Create txn with autocommit=false
			ec.txnOpt = FeTxnOption{autoCommit: false}
			err := ses.GetTxnHandler().Create(ec)
			convey.So(err, convey.ShouldBeNil)

			// Verify: IN_TRANS is set, AUTOCOMMIT is not set
			serverStatus := ses.GetTxnHandler().GetServerStatus()
			convey.So(serverStatus&SERVER_STATUS_IN_TRANS, convey.ShouldEqual, SERVER_STATUS_IN_TRANS)
			convey.So(serverStatus&SERVER_STATUS_AUTOCOMMIT, convey.ShouldEqual, uint16(0))

			// Rollback the transaction using byRollback flag (simulates ROLLBACK statement)
			ec.txnOpt.byRollback = true
			err = ses.GetTxnHandler().Rollback(ec)
			convey.So(err, convey.ShouldBeNil)

			// After rollback: IN_TRANS should be cleared, AUTOCOMMIT should still be OFF
			serverStatus = ses.GetTxnHandler().GetServerStatus()
			convey.So(serverStatus&SERVER_STATUS_IN_TRANS, convey.ShouldEqual, uint16(0))
			convey.So(serverStatus&SERVER_STATUS_AUTOCOMMIT, convey.ShouldEqual, uint16(0))
		})
	})
}

// TestOptionBitsPreservedAfterInvalidate tests that OPTION_AUTOCOMMIT and
// OPTION_NOT_AUTOCOMMIT are preserved after transaction invalidation,
// while OPTION_BEGIN is correctly cleared.
func TestOptionBitsPreservedAfterInvalidate(t *testing.T) {
	convey.Convey("option bits preserved after invalidate", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := defines.AttachAccountId(context.TODO(), sysAccountID)
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, commitTS timestamp.Timestamp, options ...TxnOption) (client.TxnOperator, error) {
				return newTestTxnOp(), nil
			}).AnyTimes()
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Hints().Return(engine.Hints{
			CommitOrRollbackTimeout: time.Second,
		}).AnyTimes()

		ses := newTestSession(t, ctrl)
		getPu("").TxnClient = txnClient
		ses.txnHandler.storage = eng

		ec := newTestExecCtx(ctx, ctrl)
		ec.ses = ses

		convey.Convey("OPTION_BEGIN is cleared but OPTION_NOT_AUTOCOMMIT preserved after commit", func() {
			// Create txn with BEGIN and autocommit=false
			ec.txnOpt = FeTxnOption{autoCommit: false, byBegin: true}
			err := ses.GetTxnHandler().Create(ec)
			convey.So(err, convey.ShouldBeNil)

			// Verify: OPTION_BEGIN and OPTION_NOT_AUTOCOMMIT are set
			convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_BEGIN), convey.ShouldBeTrue)
			convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_NOT_AUTOCOMMIT), convey.ShouldBeTrue)

			// Commit by COMMIT statement
			ec.txnOpt.byCommit = true
			err = ses.GetTxnHandler().Commit(ec)
			convey.So(err, convey.ShouldBeNil)

			// After commit: OPTION_BEGIN should be cleared, OPTION_NOT_AUTOCOMMIT should be preserved
			convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_BEGIN), convey.ShouldBeFalse)
			convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_NOT_AUTOCOMMIT), convey.ShouldBeTrue)
		})

		convey.Convey("OPTION_AUTOCOMMIT preserved after commit with autocommit=true", func() {
			// Create txn with BEGIN and autocommit=true
			ec.txnOpt = FeTxnOption{autoCommit: true, byBegin: true}
			err := ses.GetTxnHandler().Create(ec)
			convey.So(err, convey.ShouldBeNil)

			// Verify: OPTION_BEGIN is set, OPTION_AUTOCOMMIT should be default
			convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_BEGIN), convey.ShouldBeTrue)

			// Commit by COMMIT statement
			ec.txnOpt.byCommit = true
			err = ses.GetTxnHandler().Commit(ec)
			convey.So(err, convey.ShouldBeNil)

			// After commit: OPTION_BEGIN should be cleared
			convey.So(ses.GetTxnHandler().OptionBitsIsSet(OPTION_BEGIN), convey.ShouldBeFalse)
		})
	})
}

// TestSetAutocommitStatusInResponse verifies that the server status returned
// to the client correctly reflects the autocommit state after SET autocommit.
// This tests the end-to-end scenario described in the bug report.
func TestSetAutocommitStatusInResponse(t *testing.T) {
	convey.Convey("server status in response reflects autocommit state", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := defines.AttachAccountId(context.TODO(), sysAccountID)
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, commitTS timestamp.Timestamp, options ...TxnOption) (client.TxnOperator, error) {
				return newTestTxnOp(), nil
			}).AnyTimes()
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Hints().Return(engine.Hints{
			CommitOrRollbackTimeout: time.Second,
		}).AnyTimes()

		ses := newTestSession(t, ctrl)
		getPu("").TxnClient = txnClient
		ses.txnHandler.storage = eng

		ec := newTestExecCtx(ctx, ctrl)
		ec.ses = ses

		// Simulate the bug scenario:
		// 1. Connection starts with autocommit=true
		// 2. Execute SET autocommit=0
		// 3. The server status returned should have AUTOCOMMIT=false
		convey.Convey("SET autocommit=0 returns correct server status", func() {
			// Start with autocommit=true
			ec.txnOpt = FeTxnOption{autoCommit: true}
			err := ses.GetTxnHandler().Create(ec)
			convey.So(err, convey.ShouldBeNil)

			// Execute SET autocommit=0
			err = ses.GetTxnHandler().SetAutocommit(ec, true, false)
			convey.So(err, convey.ShouldBeNil)

			// Get the server status (this is what would be sent to the client)
			serverStatus := ses.GetTxnHandler().GetServerStatus()

			// Bug scenario: Before fix, serverStatus would have AUTOCOMMIT bit set (0x0002)
			// After fix, serverStatus should NOT have AUTOCOMMIT bit set
			convey.So(serverStatus&SERVER_STATUS_AUTOCOMMIT, convey.ShouldEqual, uint16(0))

			// Note: We don't check IN_TRANS here because after SetAutocommit commits the
			// current transaction, a new one may be started depending on the session state
		})

		convey.Convey("SET autocommit=1 returns correct server status", func() {
			// Start with autocommit=false
			ec.txnOpt = FeTxnOption{autoCommit: false}
			err := ses.GetTxnHandler().Create(ec)
			convey.So(err, convey.ShouldBeNil)

			// Verify autocommit is OFF
			serverStatus := ses.GetTxnHandler().GetServerStatus()
			convey.So(serverStatus&SERVER_STATUS_AUTOCOMMIT, convey.ShouldEqual, uint16(0))

			// Execute SET autocommit=1
			err = ses.GetTxnHandler().SetAutocommit(ec, false, true)
			convey.So(err, convey.ShouldBeNil)

			// Get the server status
			serverStatus = ses.GetTxnHandler().GetServerStatus()

			// serverStatus should have AUTOCOMMIT bit set
			convey.So(serverStatus&SERVER_STATUS_AUTOCOMMIT, convey.ShouldEqual, SERVER_STATUS_AUTOCOMMIT)
		})
	})
}
