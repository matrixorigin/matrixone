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

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	catalog2 "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
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
		txnOperator.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).Return().AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		txnOperator.EXPECT().EnterRunSql().Return().AnyTimes()
		txnOperator.EXPECT().ExitRunSql().Return().AnyTimes()
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
		setPu("", pu)
		catalog2.SetupDefines("")
		ec := newTestExecCtx(ctx, ctrl)
		ec.reqCtx = ctx
		ec.ses = &Session{}
		txn := InitTxnHandler("", eng, ctx, nil)

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
		txnOperator.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).Return().AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		txnOperator.EXPECT().EnterRunSql().Return().AnyTimes()
		txnOperator.EXPECT().ExitRunSql().Return().AnyTimes()
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
		setPu("", pu)
		ec := newTestExecCtx(ctx, ctrl)
		ec.reqCtx = ctx
		ec.ses = &Session{}
		catalog2.SetupDefines("")
		txn := InitTxnHandler("", eng, ctx, nil)
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
		txnOperator.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).Return().AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		txnOperator.EXPECT().EnterRunSql().Return().AnyTimes()
		txnOperator.EXPECT().ExitRunSql().Return().AnyTimes()
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

		txn := InitTxnHandler("", eng, ctx, nil)
		setPu("", pu)
		ec := newTestExecCtx(ctx, ctrl)
		ec.reqCtx = ctx
		ec.ses = &Session{}
		catalog2.SetupDefines("")
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
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, nil, nil, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		setSessionAlloc("", NewLeakCheckAllocator())
		catalog2.SetupDefines("")
		ioSes, err := NewIOSession(&testConn{}, pu, "")
		if err != nil {
			panic(err)
		}
		proto := NewMysqlClientProtocol("", 0, ioSes, 1024, sv)
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).Return().AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		txnOperator.EXPECT().EnterRunSql().Return().AnyTimes()
		txnOperator.EXPECT().ExitRunSql().Return().AnyTimes()
		txnOperator.EXPECT().GetWorkspace().Return(&testWorkspace{}).AnyTimes()
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()
		eng := mock_frontend.NewMockEngine(ctrl)
		hints := engine.Hints{CommitOrRollbackTimeout: time.Second * 10}
		eng.EXPECT().Hints().Return(hints).AnyTimes()
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		getPu("").TxnClient = txnClient
		getPu("").StorageEngine = eng
		session := NewSession(ctx, "", proto, nil)

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
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}

		ioses, err := NewIOSession(&testConn{}, pu, "")
		if err != nil {
			t.Error(err)
		}
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, sv)
		ctx := defines.AttachAccountId(context.Background(), sysAccountID)
		session := NewSession(ctx, "", proto, nil)
		return session
	}

	convey.Convey("test", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		testutil.SetupAutoIncrService("")
		ctx := context.TODO()
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		txnOperator.EXPECT().Commit(ctx).Return(nil).AnyTimes()
		txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		txnOperator.EXPECT().EnterRunSql().Return().AnyTimes()
		txnOperator.EXPECT().ExitRunSql().Return().AnyTimes()
		txnOperator.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).Return().AnyTimes()
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
		table.EXPECT().Ranges(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
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
		setPu("", pu)
		setSessionAlloc("", NewLeakCheckAllocator())
		ses := genSession(ctrl, pu)
		ses.GetTxnHandler().txnOp = txnOperator

		var ts *timestamp.Timestamp
		tcc := ses.GetTxnCompileCtx()
		tcc.execCtx = &ExecCtx{reqCtx: ctx, ses: ses}
		defDBName := tcc.DefaultDatabase()
		convey.So(defDBName, convey.ShouldEqual, "")
		convey.So(tcc.DatabaseExists("abc", &plan2.Snapshot{TS: ts}), convey.ShouldBeTrue)

		_, _, err := tcc.getRelation("abc", "t1", nil, &plan2.Snapshot{TS: ts})
		convey.So(err, convey.ShouldBeNil)

		object, tableRef := tcc.Resolve("abc", "t1", &plan2.Snapshot{TS: ts})
		convey.So(object, convey.ShouldNotBeNil)
		convey.So(tableRef, convey.ShouldNotBeNil)

		pkd := tcc.GetPrimaryKeyDef("abc", "t1", &plan2.Snapshot{TS: ts})
		convey.So(len(pkd), convey.ShouldBeZeroValue)

		stats, err := tcc.Stats(&plan2.ObjectRef{SchemaName: "abc", ObjName: "t1"}, &plan2.Snapshot{TS: ts})
		convey.So(err, convey.ShouldBeNil)
		convey.So(stats, convey.ShouldBeNil)
	})
}

var genSession1 = func(t *testing.T, ctrl *gomock.Controller, pu *config.ParameterUnit) *Session {
	sv, err := getSystemVariables("test/system_vars_config.toml")
	if err != nil {
		t.Error(err)
	}
	ioses, err := NewIOSession(&testConn{}, pu, "")
	if err != nil {
		t.Error(err)
	}
	proto := NewMysqlClientProtocol("", 0, ioses, 1024, sv)
	session := NewSession(context.Background(), "", proto, nil)
	return session
}

func TestSession_GetTempTableStorage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	eng := mock_frontend.NewMockEngine(ctrl)
	pu := config.NewParameterUnit(&config.FrontendParameters{}, eng, txnClient, nil)
	setPu("", pu)
	setSessionAlloc("", NewLeakCheckAllocator())
	ses := genSession1(t, ctrl, pu)
	assert.Panics(t, func() {
		_ = ses.GetTxnHandler().GetTempStorage()
	})
}

func TestIfInitedTempEngine(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	eng := mock_frontend.NewMockEngine(ctrl)
	pu := config.NewParameterUnit(&config.FrontendParameters{}, eng, txnClient, nil)
	setPu("", pu)
	setSessionAlloc("", NewLeakCheckAllocator())
	ses := genSession1(t, ctrl, pu)
	assert.False(t, ses.GetTxnHandler().HasTempEngine())
}

func TestSetTempTableStorage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	eng := mock_frontend.NewMockEngine(ctrl)
	pu := config.NewParameterUnit(&config.FrontendParameters{}, eng, txnClient, nil)
	setPu("", pu)
	setSessionAlloc("", NewLeakCheckAllocator())
	ses := genSession1(t, ctrl, pu)

	ck := clock.NewHLCClock(func() int64 {
		return time.Now().Unix()
	}, math.MaxInt)
	catalog2.SetupDefines("")
	_ = ses.GetTxnHandler().CreateTempStorage(ck)
	tnStore := ses.GetTxnHandler().GetTempTNService()

	assert.Equal(t, defines.TEMPORARY_TABLE_TN_ADDR, tnStore.TxnServiceAddress)
}

func TestSession_updateTimeZone(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newSes(nil, ctrl)
	ctx := context.Background()
	/*
		err := updateTimeZone(ctx, ses, ses.GetSessionSysVars(), "time_zone", "system")
		assert.NoError(t, err)
		assert.Equal(t, "Local", ses.GetTimeZone().String())
	*/

	err := updateTimeZone(ctx, ses, ses.GetSessionSysVars(), "time_zone", "+00:00")
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
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		sv.SkipCheckPrivilege = true
		sv.SessionTimeout = toml.Duration{Duration: 10 * time.Second}
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		txnOperator.EXPECT().EnterRunSql().Return().AnyTimes()
		txnOperator.EXPECT().ExitRunSql().Return().AnyTimes()
		txnOperator.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).Return().AnyTimes()
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(any, any, ...any) (TxnOperator, error) {
			txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
			txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
			txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
			txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
			txnOperator.EXPECT().GetWorkspace().Return(newTestWorkspace()).AnyTimes()
			txnOperator.EXPECT().NextSequence().Return(uint64(0)).AnyTimes()
			txnOperator.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).Return().AnyTimes()
			txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
			txnOperator.EXPECT().EnterRunSql().Return().AnyTimes()
			txnOperator.EXPECT().ExitRunSql().Return().AnyTimes()
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
		setPu("", &config.ParameterUnit{
			SV:            sv,
			TxnClient:     txnClient,
			StorageEngine: eng,
		})

		mockBS := MockBaseService{}
		rm, _ := NewRoutineManager(context.Background(), "")
		rm.baseService = &mockBS
		setRtMgr("", rm)
		ctx := defines.AttachAccountId(context.Background(), sysAccountID)
		ioses, err := NewIOSession(&testConn{}, getPu(""), "")
		if err != nil {
			panic(err)
		}
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, sv)
		session := NewSession(ctx, "", proto, nil)
		session.tenant = &TenantInfo{
			Tenant:   GetDefaultTenant(),
			TenantID: GetSysTenantId(),
		}
		session.txnCompileCtx.execCtx = &ExecCtx{reqCtx: ctx, proc: testutil.NewProc(), ses: session}
		proto.ses = session
		session.setRoutineManager(rm)
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

func Test_connectionid(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	s := newSes(nil, ctrl)
	s.SetConnectionID(10)
	x := s.GetConnectionID()
	assert.Equal(t, uint32(10), x)
}

func TestCheckPasswordExpired(t *testing.T) {
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

	// password never expires
	expired, err := checkPasswordExpired(0, "2022-01-01 00:00:00")
	assert.NoError(t, err)
	assert.False(t, expired)

	// password not expires
	ses.gSysVars.Set(DefaultPasswordLifetime, int64(30))
	expired, err = checkPasswordExpired(30, time.Now().AddDate(0, 0, -10).Format("2006-01-02 15:04:05"))
	assert.NoError(t, err)
	assert.False(t, expired)

	// password not expires
	expired, err = checkPasswordExpired(30, time.Now().AddDate(0, 0, -31).Format("2006-01-02 15:04:05"))
	assert.NoError(t, err)
	assert.True(t, expired)

	// exexpir can not execute stmt
	ses.setRoutine(&Routine{})
	ses.getRoutine().setExpired(true)
	sql := "select 1"
	rp, err := mysql.Parse(ctx, sql, 1)
	defer rp[0].Free()
	assert.NoError(t, err)
	err = authenticateUserCanExecuteStatement(ctx, ses, rp[0])
	assert.Error(t, err)

	// exexpir can execute stmt
	sql = "alter user dump identified by '123456'"
	rp, err = mysql.Parse(ctx, sql, 1)
	defer rp[0].Free()
	assert.NoError(t, err)
	err = authenticateUserCanExecuteStatement(ctx, ses, rp[0])
	assert.Error(t, err)

	// getPasswordLifetime error
	ses.gSysVars.Set(DefaultPasswordLifetime, int64(-1))
	_, err = checkPasswordExpired(1, "1")
	assert.Error(t, err)
	assert.True(t, expired)
}

func Test_CheckLockTimeExpired(t *testing.T) {
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

	// lock time expires
	ses.gSysVars.Set(ConnectionControlMaxConnectionDelay, int64(30000000))
	_, err := checkLockTimeExpired(ctx, ses, time.Now().Add(time.Hour*-3).Format("2006-01-02 15:04:05"))
	assert.NoError(t, err)

	// lock time not expires
	_, err = checkLockTimeExpired(ctx, ses, time.Now().Add(time.Second*-20).Format("2006-01-02 15:04:05"))
	assert.NoError(t, err)

	// lock time parse error
	_, err = checkLockTimeExpired(ctx, ses, "1")
	assert.Error(t, err)
}

func Test_OperatorLock(t *testing.T) {
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

	// lock
	err := setUserUnlock(ctx, "user1", bh)
	assert.NoError(t, err)

	// increaseLoginAttempts
	err = increaseLoginAttempts(ctx, "user1", bh)
	assert.NoError(t, err)

	// updateLockTime
	err = updateLockTime(ctx, "user1", bh)
	assert.NoError(t, err)

	// unlock
	err = setUserLock(ctx, "user1", bh)
	assert.NoError(t, err)
}

func TestReserveConnAndClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	rm, _ := NewRoutineManager(context.Background(), "")
	ses.rm = rm
	rm = ses.getRoutineManager()
	rm.sessionManager.AddSession(ses)

	ses.ReserveConnAndClose()
	assert.Equal(t, 0, len(rm.sessionManager.GetAllSessions()))
}
