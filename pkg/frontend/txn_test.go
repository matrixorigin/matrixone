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
	"io"
	"sync"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/golang/mock/gomock"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var _ client.Workspace = (*testWorkspace)(nil)

type testWorkspace struct {
	start               bool
	incr                bool
	mu                  sync.Mutex
	stack               []uint64
	stmtId              uint64
	reportErr1          bool
	haveDDL             bool
	readonly            bool
	protectedCloneFiles []string
	trackedLoadFiles    []string
}

func (txn *testWorkspace) ProtectCloneFiles(names ...string) {
	txn.protectedCloneFiles = append(txn.protectedCloneFiles, names...)
}

func (txn *testWorkspace) TrackLoadFiles(names ...string) {
	txn.trackedLoadFiles = append(txn.trackedLoadFiles, names...)
}

func (txn *testWorkspace) SetCloneTxn(snapshot int64) {}

func (txn *testWorkspace) SetCCPRTxn() {}

func (txn *testWorkspace) IsCCPRTxn() bool { return false }

func (txn *testWorkspace) SetCCPRTaskID(taskID string) {}

func (txn *testWorkspace) GetCCPRTaskID() string { return "" }

func (txn *testWorkspace) SetSyncProtectionJobID(jobID string) {}

func (txn *testWorkspace) GetSyncProtectionJobID() string { return "" }

func (txn *testWorkspace) Readonly() bool {
	return txn.readonly
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
	return &testWorkspace{readonly: true}
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

func (txn *testWorkspace) AdvanceSnapshot(context.Context, timestamp.Timestamp) error {
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

func (t *testWorkspace) FinalizeCommit(ctx context.Context) {
}

func (t *testWorkspace) FinalizeCommitWithUnknownResult(ctx context.Context) {
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
	t.haveDDL = flag
}

func (t *testWorkspace) GetHaveDDL() bool {
	return t.haveDDL
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
			txnOperator.EXPECT().TryEnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()
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
			txnOperator.EXPECT().TryEnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()
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
			txnOperator.EXPECT().TryEnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()
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

	return ses
}

func newMockErrSession4(t *testing.T, ctx context.Context, ctrl *gomock.Controller,
	newFunc func(ctx context.Context, commitTS timestamp.Timestamp, options ...TxnOption) (client.TxnOperator, error),
) *Session {
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(newFunc).AnyTimes()
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()

	ses := newTestSession(t, ctrl)
	getPu("").TxnClient = txnClient
	getPu("").StorageEngine = eng
	ses.txnHandler.storage = eng

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
		ses := newMockErrSession4(t, ctx, ctrl, newFunc)
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

func TestCommitUsesFinalCommitTSForNextTxn(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := defines.AttachAccountId(context.Background(), sysAccountID)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()
	ses.txnHandler.storage = eng

	commitTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	committedOp := newTestTxnOp()
	committedOp.meta = txn.TxnMeta{
		ID:     []byte{1, 2, 3, 4},
		Status: txn.TxnStatus_Active,
	}
	committedOp.commitTS = commitTS
	ses.txnHandler.txnOp = committedOp
	ses.txnHandler.txnCtx = ctx

	execCtx := newTestExecCtx(ctx, ctrl)
	execCtx.ses = ses
	execCtx.stmt = &tree.Select{}
	execCtx.txnOpt = FeTxnOption{autoCommit: true}
	if err := ses.GetTxnHandler().Commit(execCtx); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	if got := ses.getLastCommitTS(); !got.Equal(commitTS) {
		t.Fatalf("unexpected session commit timestamp: got %s, want %s", got.DebugString(), commitTS.DebugString())
	}

	originalTxnClient := getPu("").TxnClient
	t.Cleanup(func() { getPu("").TxnClient = originalTxnClient })
	nextOp := newTestTxnOp()
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), commitTS, gomock.Any()).Return(nextOp, nil)
	getPu("").TxnClient = txnClient

	handler := ses.GetTxnHandler()
	handler.mu.Lock()
	err := handler.createTxnOpUnsafe(&ExecCtx{reqCtx: ctx, ses: ses})
	handler.txnOp = nil
	handler.mu.Unlock()
	if err != nil {
		t.Fatalf("create next transaction failed: %v", err)
	}
}

func TestFinishTxnRollsBackWhenRequestIsCancelled(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := defines.AttachAccountId(context.Background(), sysAccountID)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()
	ses.txnHandler.storage = eng

	txnOp := newTestTxnOp()
	txnOp.meta = txn.TxnMeta{
		ID:     []byte{1, 2, 3, 4},
		Status: txn.TxnStatus_Active,
	}
	txnOp.wp.readonly = false
	ses.txnHandler.txnOp = txnOp
	ses.txnHandler.txnCtx = ctx
	ses.txnHandler.shareTxn = false

	reqCtx, cancel := context.WithCancel(ctx)
	cancel()
	execCtx := newTestExecCtx(reqCtx, ctrl)
	execCtx.ses = ses
	execCtx.stmt = &tree.Insert{}
	execCtx.txnOpt = FeTxnOption{autoCommit: true}

	err := finishTxnFunc(ses, nil, execCtx)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 0, txnOp.commitCalls)
	require.Equal(t, 1, txnOp.rollbackCalls)
}

func TestCommitUsesRequestContext(t *testing.T) {
	type requestContextKey struct{}

	ctrl := gomock.NewController(t)
	txnCtx := defines.AttachAccountId(context.Background(), sysAccountID)
	marker := "request-context"
	reqCtx := context.WithValue(txnCtx, requestContextKey{}, marker)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()
	ses.txnHandler.storage = eng

	txnOp := newTestTxnOp()
	txnOp.meta = txn.TxnMeta{
		ID:     []byte{1, 2, 3, 4},
		Status: txn.TxnStatus_Active,
	}
	txnOp.wp.readonly = false
	ses.txnHandler.txnOp = txnOp
	ses.txnHandler.txnCtx = txnCtx
	ses.txnHandler.shareTxn = false

	execCtx := newTestExecCtx(reqCtx, ctrl)
	execCtx.ses = ses
	execCtx.stmt = &tree.Insert{}
	execCtx.txnOpt = FeTxnOption{autoCommit: true}

	require.NoError(t, finishTxnFunc(ses, nil, execCtx))
	require.Equal(t, 1, txnOp.commitCalls)
	require.Equal(t, marker, txnOp.commitCtx.Value(requestContextKey{}))
}

func TestReadOnlyCommitIgnoresKillDuringCommit(t *testing.T) {
	type requestContextKey struct{}

	ctrl := gomock.NewController(t)
	txnCtx := defines.AttachAccountId(context.Background(), sysAccountID)
	marker := "request-context"
	reqCtx, cancel := context.WithCancel(context.WithValue(txnCtx, requestContextKey{}, marker))
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	ses.setRoutine(&Routine{})
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()
	ses.txnHandler.storage = eng

	txnOp := newTestTxnOp()
	txnOp.meta = txn.TxnMeta{
		ID:     []byte{1, 2, 3, 4},
		Status: txn.TxnStatus_Active,
	}
	txnOp.commitCheckContext = true
	txnOp.commitHook = cancel
	txnOp.wp.readonly = true
	ses.txnHandler.txnOp = txnOp
	ses.txnHandler.txnCtx = txnCtx
	ses.txnHandler.shareTxn = false

	execCtx := newTestExecCtx(reqCtx, ctrl)
	execCtx.ses = ses
	execCtx.stmt = &tree.Select{}
	execCtx.txnOpt = FeTxnOption{autoCommit: true}

	require.NoError(t, finishTxnFunc(ses, nil, execCtx))
	require.ErrorIs(t, reqCtx.Err(), context.Canceled)
	require.NotNil(t, txnOp.commitCtx)
	require.Nil(t, txnOp.commitCtx.Value(requestContextKey{}))
	require.Equal(t, 1, txnOp.commitCalls)
	require.Equal(t, 0, txnOp.rollbackCalls)
}

func TestWritableDQLCommitPropagatesKillDuringCommit(t *testing.T) {
	ctrl := gomock.NewController(t)
	txnCtx := defines.AttachAccountId(context.Background(), sysAccountID)
	reqCtx, cancel := context.WithCancel(txnCtx)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()
	ses.txnHandler.storage = eng

	txnOp := newTestTxnOp()
	txnOp.meta = txn.TxnMeta{
		ID:     []byte{1, 2, 3, 4},
		Status: txn.TxnStatus_Active,
	}
	txnOp.commitCheckContext = true
	txnOp.commitHook = cancel
	txnOp.wp.readonly = false
	ses.txnHandler.txnOp = txnOp
	ses.txnHandler.txnCtx = txnCtx
	ses.txnHandler.shareTxn = false

	execCtx := newTestExecCtx(reqCtx, ctrl)
	execCtx.ses = ses
	execCtx.stmt = &tree.Select{}
	execCtx.txnOpt = FeTxnOption{autoCommit: true}

	err := finishTxnFunc(ses, nil, execCtx)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, reqCtx.Err(), context.Canceled)
	require.Equal(t, 1, txnOp.commitCalls)
	require.Equal(t, 0, txnOp.rollbackCalls)
	require.Equal(t, txn.TxnStatus_Active, txnOp.meta.Status)
}

func TestCommitTxnUnknownInvalidatesTxnOperator(t *testing.T) {
	convey.Convey("commit ErrTxnUnknown invalidates the frontend txn operator", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := defines.AttachAccountId(context.TODO(), sysAccountID)
		ses := newTestSession(t, ctrl)
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Hints().Return(engine.Hints{
			CommitOrRollbackTimeout: time.Second,
		}).AnyTimes()
		ses.txnHandler.storage = eng

		txnOp := newTestTxnOp()
		txnOp.meta = txn.TxnMeta{
			ID:     []byte{1, 2, 3, 4},
			Status: txn.TxnStatus_Active,
		}
		txnOp.commitErr = moerr.NewTxnUnknown(ctx, "test")
		txnOp.wp.SetHaveDDL(true)
		ses.txnHandler.txnOp = txnOp
		ses.txnHandler.txnCtx = ctx
		ses.txnHandler.shareTxn = false

		ec := newTestExecCtx(ctx, ctrl)
		ec.ses = ses
		ec.stmt = &tree.Insert{}
		ec.txnOpt = FeTxnOption{autoCommit: true}

		err := finishTxnFunc(ses, nil, ec)
		convey.So(moerr.IsMoErrCode(err, moerr.ErrTxnUnknown), convey.ShouldBeTrue)
		convey.So(txnOp.commitCalls, convey.ShouldEqual, 1)
		convey.So(txnOp.rollbackCalls, convey.ShouldEqual, 0)
		convey.So(ses.getDDLVersion(), convey.ShouldEqual, uint64(1))
		convey.So(ses.GetTxnHandler().GetTxn(), convey.ShouldBeNil)
		convey.So(ses.GetTxnHandler().InActiveTxn(), convey.ShouldBeFalse)
	})
}

func TestFinishTxnPreservesEOFOnRollback(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := defines.AttachAccountId(context.Background(), sysAccountID)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()
	ses.txnHandler.storage = eng

	txnOp := newTestTxnOp()
	txnOp.meta = txn.TxnMeta{
		ID:     []byte{1, 2, 3, 4},
		Status: txn.TxnStatus_Active,
	}
	ses.txnHandler.txnOp = txnOp
	ses.txnHandler.txnCtx = ctx
	ses.txnHandler.shareTxn = false

	execCtx := newTestExecCtx(ctx, ctrl)
	execCtx.ses = ses
	execCtx.stmt = &tree.Select{}
	execCtx.txnOpt = FeTxnOption{autoCommit: true}

	var err error
	require.NotPanics(t, func() {
		err = finishTxnFunc(ses, io.EOF, execCtx)
	})
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 1, txnOp.rollbackCalls)
	require.Nil(t, ses.GetTxnHandler().GetTxn())
}

func TestCommitFailureAdvancesSessionGeneration(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := defines.AttachAccountId(context.Background(), sysAccountID)
	ses := newTestSession(t, ctrl)
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()
	ses.txnHandler.storage = eng

	txnOp := newTestTxnOp()
	txnOp.meta = txn.TxnMeta{
		ID:     []byte{1, 2, 3, 4},
		Status: txn.TxnStatus_Active,
	}
	txnOp.wp.SetHaveDDL(true)
	txnOp.commitErr = moerr.NewInternalError(ctx, "commit failed")
	ses.txnHandler.txnOp = txnOp
	ses.txnHandler.txnCtx = ctx
	ses.txnHandler.shareTxn = false

	ec := newTestExecCtx(ctx, ctrl)
	ec.ses = ses
	ec.stmt = &tree.Select{}
	ec.txnOpt = FeTxnOption{autoCommit: true}

	err := finishTxnFunc(ses, nil, ec)
	if err == nil {
		t.Fatal("expected commit failure")
	}
	if txnOp.commitCalls != 1 {
		t.Fatalf("expected one commit call, got %d", txnOp.commitCalls)
	}
	if got := ses.getDDLVersion(); got != 1 {
		t.Fatalf("unexpected DDL generation: got %d, want 1", got)
	}
}

func TestRollbackDDLAdvancesSessionGeneration(t *testing.T) {
	tests := []struct {
		name            string
		haveDDL         bool
		rollbackError   bool
		statementError  bool
		expectedVersion uint64
	}{
		{
			name:            "explicit rollback with DDL",
			haveDDL:         true,
			expectedVersion: 1,
		},
		{
			name:            "error rollback with DDL",
			haveDDL:         true,
			statementError:  true,
			expectedVersion: 1,
		},
		{
			name:            "rollback without DDL",
			expectedVersion: 0,
		},
		{
			name:            "failed rollback with DDL",
			haveDDL:         true,
			rollbackError:   true,
			expectedVersion: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			ctx := defines.AttachAccountId(context.Background(), sysAccountID)
			ses := newTestSession(t, ctrl)
			eng := mock_frontend.NewMockEngine(ctrl)
			eng.EXPECT().Hints().Return(engine.Hints{
				CommitOrRollbackTimeout: time.Second,
			}).AnyTimes()
			ses.txnHandler.storage = eng

			txnOp := newTestTxnOp()
			txnOp.meta = txn.TxnMeta{
				ID:     []byte{1, 2, 3, 4},
				Status: txn.TxnStatus_Active,
			}
			txnOp.wp.SetHaveDDL(test.haveDDL)
			if test.rollbackError {
				txnOp.mod = modRollbackError
			}
			ses.txnHandler.txnOp = txnOp
			ses.txnHandler.txnCtx = ctx
			ses.txnHandler.shareTxn = false

			ec := newTestExecCtx(ctx, ctrl)
			ec.ses = ses
			ec.stmt = &tree.RollbackTransaction{}
			ec.txnOpt = FeTxnOption{autoCommit: true}

			var err error
			if test.statementError {
				ec.stmt = &tree.Select{}
				err = finishTxnFunc(
					ses,
					moerr.NewInternalError(ctx, "statement failed"),
					ec,
				)
			} else {
				ec.txnOpt.byRollback = true
				err = ses.GetTxnHandler().Rollback(ec)
			}
			if test.rollbackError && err == nil {
				t.Fatal("expected rollback failure")
			}
			if !test.rollbackError && test.statementError && err == nil {
				t.Fatal("expected the original statement failure")
			}
			if !test.rollbackError && !test.statementError && err != nil {
				t.Fatalf("rollback failed: %v", err)
			}
			if txnOp.rollbackCalls != 1 {
				t.Fatalf("expected one rollback call, got %d", txnOp.rollbackCalls)
			}
			if got := ses.getDDLVersion(); got != test.expectedVersion {
				t.Fatalf(
					"unexpected DDL generation: got %d, want %d",
					got,
					test.expectedVersion,
				)
			}
		})
	}
}

func TestCommitPanicRollbackAdvancesSessionGeneration(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := defines.AttachAccountId(context.Background(), sysAccountID)
	ses := newTestSession(t, ctrl)
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()
	ses.txnHandler.storage = eng

	txnOp := newTestTxnOp()
	txnOp.meta = txn.TxnMeta{
		ID:     []byte{1, 2, 3, 4},
		Status: txn.TxnStatus_Active,
	}
	txnOp.wp.SetHaveDDL(true)
	txnOp.commitPanic = true
	ses.txnHandler.txnOp = txnOp
	ses.txnHandler.txnCtx = ctx
	ses.txnHandler.shareTxn = false

	ec := newTestExecCtx(ctx, ctrl)
	ec.ses = ses
	ec.stmt = &tree.Select{}
	ec.txnOpt = FeTxnOption{autoCommit: true}

	err := finishTxnFunc(ses, nil, ec)
	if err == nil {
		t.Fatal("expected commit panic error")
	}
	if txnOp.commitCalls != 1 {
		t.Fatalf("expected one commit call, got %d", txnOp.commitCalls)
	}
	if txnOp.rollbackCalls != 1 {
		t.Fatalf("expected one rollback call, got %d", txnOp.rollbackCalls)
	}
	if got := ses.getDDLVersion(); got != 1 {
		t.Fatalf("unexpected DDL generation: got %d, want 1", got)
	}
}

func TestTempTableAliasesFollowTransactionLifecycle(t *testing.T) {
	type testState struct {
		ses   *Session
		op    *testTxnOp
		exec  *ExecCtx
		close func()
	}
	newState := func(t *testing.T, txnID byte) testState {
		ctrl := gomock.NewController(t)
		ctx := defines.AttachAccountId(context.Background(), sysAccountID)
		ses := newTestSession(t, ctrl)
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Hints().Return(engine.Hints{
			CommitOrRollbackTimeout: time.Second,
		}).AnyTimes()
		ses.txnHandler.storage = eng

		op := newTestTxnOp()
		op.meta = txn.TxnMeta{
			ID:     []byte{txnID},
			Status: txn.TxnStatus_Active,
		}
		ses.txnHandler.txnOp = op
		ses.txnHandler.txnCtx = ctx
		ses.txnHandler.shareTxn = false
		ses.txnHandler.optionBits = OPTION_BEGIN | OPTION_AUTOCOMMIT

		execCtx := newTestExecCtx(ctx, ctrl)
		execCtx.ses = ses
		execCtx.stmt = &tree.Select{}
		execCtx.txnOpt = FeTxnOption{autoCommit: true}
		return testState{
			ses:  ses,
			op:   op,
			exec: execCtx,
			close: func() {
				execCtx.Close()
				ctrl.Finish()
			},
		}
	}

	t.Run("commit preserves aliases", func(t *testing.T) {
		state := newState(t, 1)
		defer state.close()
		state.ses.AddTempTable("db", "created", "real-created")
		state.exec.txnOpt.byCommit = true

		require.NoError(t, state.ses.GetTxnHandler().Commit(state.exec))

		realName, ok := state.ses.GetTempTable("db", "created")
		require.True(t, ok)
		require.Equal(t, "real-created", realName)
		require.Empty(t, state.ses.tempTableTxnJournals)
	})

	t.Run("rollback restores aliases", func(t *testing.T) {
		state := newState(t, 2)
		defer state.close()
		state.ses.AddTempTable("db", "created", "real-created")
		state.exec.txnOpt.byRollback = true

		require.NoError(t, state.ses.GetTxnHandler().Rollback(state.exec))

		_, ok := state.ses.GetTempTable("db", "created")
		require.False(t, ok)
		require.Empty(t, state.ses.tempTableTxnJournals)
	})

	t.Run("statement rollback preserves prior transaction changes", func(t *testing.T) {
		state := newState(t, 3)
		defer state.close()
		state.ses.AddTempTable("db", "committed-statement", "real-committed")
		state.ses.commitTempTableStatement(
			tempTableTxnKey(state.op),
			tempTableStatementKey(state.ses, false),
		)
		state.op.wp.StartStatement()
		require.NoError(t, state.op.wp.IncrStatementID(context.Background(), false))
		state.ses.AddTempTable("db", "failed-statement", "real-failed")

		require.NoError(t, state.ses.GetTxnHandler().Rollback(state.exec))

		_, ok := state.ses.GetTempTable("db", "failed-statement")
		require.False(t, ok)
		realName, ok := state.ses.GetTempTable("db", "committed-statement")
		require.True(t, ok)
		require.Equal(t, "real-committed", realName)
		require.True(t, state.ses.GetTxnHandler().InActiveTxn())
		state.op.wp.EndStatement()
	})

	t.Run("known commit failure restores aliases", func(t *testing.T) {
		state := newState(t, 4)
		defer state.close()
		state.ses.addTempTable("db", "existing", "real-existing", "", "")
		state.ses.RemoveTempTable("db", "existing")
		state.ses.AddTempTable("db", "created", "real-created")
		state.op.commitErr = moerr.NewInternalErrorNoCtx("commit failed")
		state.exec.txnOpt.byCommit = true

		require.ErrorContains(t, state.ses.GetTxnHandler().Commit(state.exec), "commit failed")

		realName, ok := state.ses.GetTempTable("db", "existing")
		require.True(t, ok)
		require.Equal(t, "real-existing", realName)
		_, ok = state.ses.GetTempTable("db", "created")
		require.False(t, ok)
		require.Empty(t, state.ses.tempTableTxnJournals)
	})

	t.Run("unknown commit result preserves aliases for cleanup", func(t *testing.T) {
		state := newState(t, 5)
		defer state.close()
		state.ses.AddTempTable("db", "created", "real-created")
		state.op.commitErr = moerr.NewTxnUnknown(state.exec.reqCtx, "test")
		state.exec.txnOpt.byCommit = true

		err := state.ses.GetTxnHandler().Commit(state.exec)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnUnknown))

		realName, ok := state.ses.GetTempTable("db", "created")
		require.True(t, ok)
		require.Equal(t, "real-created", realName)
		require.Empty(t, state.ses.tempTableTxnJournals)
	})
}

var _ TxnOperator = new(testTxnOp)

const (
	modRollbackError = 1
)

type testTxnOp struct {
	meta                 txn.TxnMeta
	wp                   *testWorkspace
	mod                  int
	commitTS             timestamp.Timestamp
	commitErr            error
	commitPanic          bool
	commitCalls          int
	commitCtx            context.Context
	commitHook           func()
	commitCheckContext   bool
	rollbackCalls        int
	checkLockTableBinds  func(context.Context) error
	checkLockTableChecks int
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

func (txnop *testTxnOp) SetSnapshotTS(ts timestamp.Timestamp) {
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
	txnop.commitCalls++
	txnop.commitCtx = ctx
	if txnop.commitHook != nil {
		txnop.commitHook()
	}
	if txnop.commitCheckContext {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if txnop.commitPanic {
		panic("commit panic")
	}
	if txnop.commitErr != nil {
		return txnop.commitErr
	}
	txnop.meta.CommitTS = txnop.commitTS
	txnop.meta.Status = txn.TxnStatus_Committed
	return nil
}

func (txnop *testTxnOp) Rollback(ctx context.Context) error {
	txnop.rollbackCalls++
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

func (txnop *testTxnOp) CheckLockTableBinds(ctx context.Context) error {
	txnop.checkLockTableChecks++
	if txnop.checkLockTableBinds != nil {
		return txnop.checkLockTableBinds(ctx)
	}
	return nil
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

func (txnop *testTxnOp) AppendEventCallback(event client.EventType, callbacks ...client.TxnEventCallback) {
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
	return 1
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

func (txnop *testTxnOp) Set(string, any) {
	//TODO implement me
	panic("implement me")
}

func (txnop *testTxnOp) Get(string) (any, bool) {
	//TODO implement me
	panic("implement me")
}

func (txnop *testTxnOp) Delete(string) {
	//TODO implement me
	panic("implement me")
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
