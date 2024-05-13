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
	"sync"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/golang/mock/gomock"
	"github.com/smartystreets/goconvey/convey"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
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

func (txn *testWorkspace) UpdateSnapshotWriteOffset() {
	//TODO implement me
	panic("implement me")
}

func (txn *testWorkspace) GetSnapshotWriteOffset() int {
	//TODO implement me
	panic("implement me")
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
	//TODO implement me
	panic("implement me")
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
	//TODO implement me
	panic("implement me")
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
			wsp := newTestWorkspace()
			txnOperator.EXPECT().GetWorkspace().Return(wsp).AnyTimes()
			return txnOperator, nil
		}).AnyTimes()
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()

	var gSys GlobalSystemVariables
	InitGlobalSystemVariables(&gSys)

	ses := newTestSession(t, ctrl)
	getGlobalPu().TxnClient = txnClient
	getGlobalPu().StorageEngine = eng
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
			wsp := newTestWorkspace()
			wsp.reportErr1 = true
			txnOperator.EXPECT().GetWorkspace().Return(wsp).AnyTimes()
			return txnOperator, nil
		}).AnyTimes()
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()

	var gSys GlobalSystemVariables
	InitGlobalSystemVariables(&gSys)
	ses := newTestSession(t, ctrl)
	getGlobalPu().TxnClient = txnClient
	getGlobalPu().StorageEngine = eng
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
				txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
				txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
				txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
				txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
				wsp := newTestWorkspace()
				txnOperator.EXPECT().GetWorkspace().Return(wsp).AnyTimes()
				return txnOperator, nil
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

		var gSys GlobalSystemVariables
		InitGlobalSystemVariables(&gSys)

		ses := newTestSession(t, ctrl)
		getGlobalPu().TxnClient = txnClient
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

	})

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
	})
}
