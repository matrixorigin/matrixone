// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/query"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func TestTxnHandler_NewTxn(t *testing.T) {
	convey.Convey("new txn", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := defines.AttachAccountId(context.TODO(), sysAccountID)
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().SetFootPrints(gomock.Any()).Return().AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		txnOperator.EXPECT().GetWorkspace().Return(&testWorkspace{}).AnyTimes()
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		cnt := 0
		txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, _ timestamp.Timestamp, ootions ...client.TxnOption) (client.TxnOperator, error) {
				cnt++
				if cnt%2 != 0 {
					return txnOperator, nil
				} else {
					return nil, moerr.NewInternalError(ctx, "startTxn failed")
				}
			}).AnyTimes()
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Hints().Return(engine.Hints{
			CommitOrRollbackTimeout: time.Second,
		}).AnyTimes()

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()

		pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		convey.So(err, convey.ShouldBeNil)
		setGlobalPu(pu)

		ec := newTestExecCtx(ctx, ctrl)
		ec.reqCtx = ctx
		ec.ses = &Session{}
		txn := InitTxnHandler(eng, ctx, nil)

		var c clock.Clock
		err = txn.CreateTempStorage(c)
		convey.So(err, convey.ShouldBeNil)
		err = txn.Create(ec)
		convey.So(err, convey.ShouldBeNil)
		txn1 := txn.GetTxn()
		err = txn.Create(ec)
		convey.So(err, convey.ShouldBeNil)
		txn2 := txn.GetTxn()
		convey.So(txn1, convey.ShouldEqual, txn2)
		err = txn.Create(ec)
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestTxnHandler_CommitTxn(t *testing.T) {
	convey.Convey("commit txn", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := defines.AttachAccountId(context.TODO(), sysAccountID)
		idx := int64(1)
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{
			CommitTS: timestamp.Timestamp{PhysicalTime: idx},
		}).AnyTimes()
		cnt := 0
		txnOperator.EXPECT().Commit(gomock.Any()).DoAndReturn(
			func(ctx context.Context) error {
				cnt++
				if cnt%2 != 0 {
					return nil
				} else {
					return moerr.NewInternalError(ctx, "commit failed")
				}
			}).AnyTimes()
		txnOperator.EXPECT().SetFootPrints(gomock.Any()).Return().AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		txnOperator.EXPECT().GetWorkspace().Return(&testWorkspace{}).AnyTimes()
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Hints().Return(engine.Hints{
			CommitOrRollbackTimeout: time.Second,
		}).AnyTimes()

		txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		_, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		if err != nil {
			t.Error(err)
		}

		pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		convey.So(err, convey.ShouldBeNil)
		setGlobalPu(pu)
		ec := newTestExecCtx(ctx, ctrl)
		ec.reqCtx = ctx
		ec.ses = &Session{}

		txn := InitTxnHandler(eng, ctx, nil)
		var c clock.Clock
		_ = txn.CreateTempStorage(c)
		err = txn.Create(ec)
		convey.So(err, convey.ShouldBeNil)
		err = txn.Commit(ec)
		convey.So(err, convey.ShouldBeNil)

		convey.ShouldEqual(timestamp.Timestamp{PhysicalTime: idx}, ec.ses.getLastCommitTS())

		err = txn.Create(ec)
		convey.So(err, convey.ShouldBeNil)

		idx++
		err = txn.Commit(ec)
		convey.So(err, convey.ShouldBeNil)
		convey.ShouldEqual(timestamp.Timestamp{PhysicalTime: idx}, ec.ses.getLastCommitTS())
	})
}

func TestTxnHandler_RollbackTxn(t *testing.T) {
	convey.Convey("rollback txn", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := defines.AttachAccountId(context.TODO(), sysAccountID)
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		cnt := 0
		txnOperator.EXPECT().Rollback(gomock.Any()).DoAndReturn(
			func(ctc context.Context) error {
				cnt++
				if cnt%2 != 0 {
					return nil
				} else {
					return moerr.NewInternalError(ctx, "rollback failed")
				}
			}).AnyTimes()
		txnOperator.EXPECT().SetFootPrints(gomock.Any()).Return().AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		wp := mock_frontend.NewMockWorkspace(ctrl)
		wp.EXPECT().RollbackLastStatement(gomock.Any()).Return(moerr.NewInternalError(ctx, "rollback last stmt")).AnyTimes()
		txnOperator.EXPECT().GetWorkspace().Return(wp).AnyTimes()
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Hints().Return(engine.Hints{
			CommitOrRollbackTimeout: time.Second,
		}).AnyTimes()

		txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		convey.So(err, convey.ShouldBeNil)

		txn := InitTxnHandler(eng, ctx, nil)
		setGlobalPu(pu)
		ec := newTestExecCtx(ctx, ctrl)
		ec.reqCtx = ctx
		ec.ses = &Session{}

		var c clock.Clock
		_ = txn.CreateTempStorage(c)
		ec.txnOpt = FeTxnOption{autoCommit: true}
		err = txn.Create(ec)
		convey.So(err, convey.ShouldBeNil)
		err = txn.Rollback(ec)
		convey.So(err, convey.ShouldBeNil)
		ec.txnOpt = FeTxnOption{autoCommit: false}
		err = txn.Create(ec)
		convey.So(err, convey.ShouldBeNil)
		err = txn.Rollback(ec)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestSession_TxnBegin(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), sysAccountID)
	genSession := func(ctrl *gomock.Controller) *Session {
		ioSes := mock_frontend.NewMockIOSession(ctrl)
		ioSes.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioSes.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioSes.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioSes.EXPECT().Ref().AnyTimes()
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		setGlobalPu(&config.ParameterUnit{
			SV: sv,
		})

		proto := NewMysqlClientProtocol(0, ioSes, 1024, sv)
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().SetFootPrints(gomock.Any()).Return().AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		txnOperator.EXPECT().GetWorkspace().Return(&testWorkspace{}).AnyTimes()
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()
		eng := mock_frontend.NewMockEngine(ctrl)
		hints := engine.Hints{CommitOrRollbackTimeout: time.Second * 10}
		eng.EXPECT().Hints().Return(hints).AnyTimes()
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		getGlobalPu().TxnClient = txnClient
		getGlobalPu().StorageEngine = eng
		session := NewSession(ctx, proto, nil)

		var c clock.Clock
		_ = session.GetTxnHandler().CreateTempStorage(c)
		return session
	}
	convey.Convey("new session", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ses := genSession(ctrl)
		ec := newTestExecCtx(ctx, ctrl)
		ec.ses = ses
		err := ses.GetTxnHandler().Create(ec)
		convey.So(err, convey.ShouldBeNil)
		err = ses.GetTxnHandler().Commit(ec)
		convey.So(err, convey.ShouldBeNil)
		err = ses.GetTxnHandler().Create(ec)
		convey.So(err, convey.ShouldBeNil)
		err = ses.GetTxnHandler().SetAutocommit(ec, true, false)
		convey.So(err, convey.ShouldBeNil)
		err = ses.GetTxnHandler().Commit(ec)
		convey.So(err, convey.ShouldBeNil)
		_ = ses.GetTxnHandler().GetTxn()
		convey.So(err, convey.ShouldBeNil)

		err = ses.GetTxnHandler().Commit(ec)
		convey.So(err, convey.ShouldBeNil)

		err = ses.GetTxnHandler().SetAutocommit(ec, false, true)
		convey.So(err, convey.ShouldBeNil)

		err = ses.GetTxnHandler().SetAutocommit(ec, false, false)
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestSession_TxnCompilerContext(t *testing.T) {
	genSession := func(ctrl *gomock.Controller, pu *config.ParameterUnit) *Session {
		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		proto := NewMysqlClientProtocol(0, ioses, 1024, sv)
		ctx := defines.AttachAccountId(context.Background(), sysAccountID)
		session := NewSession(ctx, proto, nil)
		return session
	}

	convey.Convey("test", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		testutil.SetupAutoIncrService()
		ctx := context.TODO()
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		txnOperator.EXPECT().Commit(ctx).Return(nil).AnyTimes()
		txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Hints().Return(engine.Hints{
			CommitOrRollbackTimeout: time.Second,
		}).AnyTimes()

		db := mock_frontend.NewMockDatabase(ctrl)
		db.EXPECT().Relations(gomock.Any()).Return(nil, nil).AnyTimes()

		table := mock_frontend.NewMockRelation(ctrl)
		table.EXPECT().Ranges(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
		table.EXPECT().TableDefs(gomock.Any()).Return(nil, nil).AnyTimes()
		table.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{}).AnyTimes()
		table.EXPECT().CopyTableDef(gomock.Any()).Return(&plan.TableDef{}).AnyTimes()
		table.EXPECT().GetPrimaryKeys(gomock.Any()).Return(nil, nil).AnyTimes()
		table.EXPECT().GetHideKeys(gomock.Any()).Return(nil, nil).AnyTimes()
		table.EXPECT().Stats(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
		table.EXPECT().TableColumns(gomock.Any()).Return(nil, nil).AnyTimes()
		table.EXPECT().GetTableID(gomock.Any()).Return(uint64(10)).AnyTimes()
		table.EXPECT().GetEngineType().Return(engine.Disttae).AnyTimes()
		table.EXPECT().ApproxObjectsNum(gomock.Any()).Return(0).AnyTimes()
		db.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(table, nil).AnyTimes()
		db.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(db, nil).AnyTimes()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, eng, txnClient, nil)
		setGlobalPu(pu)

		ses := genSession(ctrl, pu)

		var ts *timestamp.Timestamp
		tcc := ses.GetTxnCompileCtx()
		tcc.execCtx = &ExecCtx{reqCtx: ctx, ses: ses}
		defDBName := tcc.DefaultDatabase()
		convey.So(defDBName, convey.ShouldEqual, "")
		convey.So(tcc.DatabaseExists("abc", plan2.Snapshot{TS: ts}), convey.ShouldBeTrue)

		_, _, err := tcc.getRelation("abc", "t1", nil, plan2.Snapshot{TS: ts})
		convey.So(err, convey.ShouldBeNil)

		object, tableRef := tcc.Resolve("abc", "t1", plan2.Snapshot{TS: ts})
		convey.So(object, convey.ShouldNotBeNil)
		convey.So(tableRef, convey.ShouldNotBeNil)

		pkd := tcc.GetPrimaryKeyDef("abc", "t1", plan2.Snapshot{TS: ts})
		convey.So(len(pkd), convey.ShouldBeZeroValue)

		stats, err := tcc.Stats(&plan2.ObjectRef{SchemaName: "abc", ObjName: "t1"}, plan2.Snapshot{TS: ts})
		convey.So(err, convey.ShouldBeNil)
		convey.So(stats, convey.ShouldBeNil)
	})
}

func TestSession_GetTempTableStorage(t *testing.T) {
	genSession := func(ctrl *gomock.Controller, pu *config.ParameterUnit) *Session {
		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		proto := NewMysqlClientProtocol(0, ioses, 1024, sv)
		session := NewSession(context.Background(), proto, nil)
		return session
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	eng := mock_frontend.NewMockEngine(ctrl)
	pu := config.NewParameterUnit(&config.FrontendParameters{}, eng, txnClient, nil)
	setGlobalPu(pu)
	ses := genSession(ctrl, pu)
	assert.Panics(t, func() {
		_ = ses.GetTxnHandler().GetTempStorage()
	})
}

func TestIfInitedTempEngine(t *testing.T) {
	genSession := func(ctrl *gomock.Controller, pu *config.ParameterUnit) *Session {
		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		proto := NewMysqlClientProtocol(0, ioses, 1024, sv)
		session := NewSession(context.Background(), proto, nil)
		return session
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	eng := mock_frontend.NewMockEngine(ctrl)
	pu := config.NewParameterUnit(&config.FrontendParameters{}, eng, txnClient, nil)
	setGlobalPu(pu)

	ses := genSession(ctrl, pu)
	assert.False(t, ses.GetTxnHandler().HasTempEngine())
}

func TestSetTempTableStorage(t *testing.T) {
	genSession := func(ctrl *gomock.Controller, pu *config.ParameterUnit) *Session {
		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		proto := NewMysqlClientProtocol(0, ioses, 1024, sv)
		session := NewSession(context.Background(), proto, nil)
		return session
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	eng := mock_frontend.NewMockEngine(ctrl)
	pu := config.NewParameterUnit(&config.FrontendParameters{}, eng, txnClient, nil)
	setGlobalPu(pu)
	ses := genSession(ctrl, pu)

	ck := clock.NewHLCClock(func() int64 {
		return time.Now().Unix()
	}, math.MaxInt)
	_ = ses.GetTxnHandler().CreateTempStorage(ck)
	tnStore := ses.GetTxnHandler().GetTempTNService()

	assert.Equal(t, defines.TEMPORARY_TABLE_TN_ADDR, tnStore.TxnServiceAddress)
}

func TestSession_updateTimeZone(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newSes(nil, ctrl)
	ctx := context.Background()
	err := updateTimeZone(ctx, ses, ses.GetSessionSysVars(), "time_zone", "system")
	assert.NoError(t, err)
	assert.Equal(t, ses.GetTimeZone().String(), "Local")

	err = updateTimeZone(ctx, ses, ses.GetSessionSysVars(), "time_zone", "+00:00")
	assert.NoError(t, err)
	assert.Equal(t, ses.GetTimeZone().String(), "FixedZone")

	err = updateTimeZone(ctx, ses, ses.GetSessionSysVars(), "time_zone", "+08:00")
	assert.NoError(t, err)
	assert.Equal(t, ses.GetTimeZone().String(), "FixedZone")

	err = updateTimeZone(ctx, ses, ses.GetSessionSysVars(), "time_zone", "-08:00")
	assert.NoError(t, err)
	assert.Equal(t, ses.GetTimeZone().String(), "FixedZone")

	//ci fails the case
	//err = updateTimeZone(ses, ses.GetSysVars(), "time_zone", "UTC")
	//assert.NoError(t, err)
	//assert.Equal(t, ses.GetTimeZone().String(), "utc")

	err = updateTimeZone(ctx, ses, ses.GetSessionSysVars(), "time_zone", "")
	assert.NoError(t, err)
	assert.Equal(t, ses.GetTimeZone().String(), "UTC")
}

func TestSession_Migrate(t *testing.T) {
	genSession := func(ctrl *gomock.Controller) *Session {
		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		sv.SkipCheckPrivilege = true
		proto := NewMysqlClientProtocol(0, ioses, 1024, sv)
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		txnOperator.EXPECT().SetFootPrints(gomock.Any()).Return().AnyTimes()
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(any, any, ...any) (TxnOperator, error) {
			txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
			txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
			txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
			txnOperator.EXPECT().GetWorkspace().Return(newTestWorkspace()).AnyTimes()
			txnOperator.EXPECT().NextSequence().Return(uint64(0)).AnyTimes()
			txnOperator.EXPECT().SetFootPrints(gomock.Any()).Return().AnyTimes()
			txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
			return txnOperator, nil
		}).AnyTimes()
		eng := mock_frontend.NewMockEngine(ctrl)
		hints := engine.Hints{CommitOrRollbackTimeout: time.Second * 10}
		db := mock_frontend.NewMockDatabase(ctrl)
		eng.EXPECT().Hints().Return(hints).AnyTimes()
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(db, nil).AnyTimes()
		rel := mock_frontend.NewMockRelation(ctrl)
		rel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{}).AnyTimes()
		rel.EXPECT().TableDefs(gomock.Any()).Return(nil, nil).AnyTimes()
		var tid uint64
		rel.EXPECT().GetTableID(gomock.Any()).Return(tid).AnyTimes()
		db.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
		db.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(rel, nil).AnyTimes()
		setGlobalPu(&config.ParameterUnit{
			SV:            sv,
			TxnClient:     txnClient,
			StorageEngine: eng,
		})
		ctx := defines.AttachAccountId(context.Background(), sysAccountID)
		session := NewSession(ctx, proto, nil)
		session.tenant = &TenantInfo{
			Tenant:   GetDefaultTenant(),
			TenantID: GetSysTenantId(),
		}
		session.txnCompileCtx.execCtx = &ExecCtx{reqCtx: ctx, proc: testutil.NewProc(), ses: session}
		proto.ses = session
		return session
	}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bh := &backgroundExecTest{}
	bh.init()

	bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
	defer bhStub.Reset()

	s := genSession(ctrl)
	err := Migrate(s, &query.MigrateConnToRequest{
		DB: "d1",
		PrepareStmts: []*query.PrepareStmt{
			{Name: "p1", SQL: `select ?`},
			{Name: "p2", SQL: `select ?`},
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, "d1", s.GetDatabaseName())
	assert.Equal(t, 2, len(s.prepareStmts))
}
