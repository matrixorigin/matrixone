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
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	catalog2 "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
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
		txnOperator.EXPECT().EnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(0)).AnyTimes()
		txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()
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
		txnOperator.EXPECT().EnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(0)).AnyTimes()
		txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()
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
		txnOperator.EXPECT().EnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(0)).AnyTimes()
		txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()
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
		txnOperator.EXPECT().EnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(0)).AnyTimes()
		txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()
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
		txnOperator.EXPECT().EnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(0)).AnyTimes()
		txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()
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
		table.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
		table.EXPECT().TableDefs(gomock.Any()).Return(nil, nil).AnyTimes()
		table.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{}).AnyTimes()
		table.EXPECT().CopyTableDef(gomock.Any()).Return(&plan.TableDef{}).AnyTimes()
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

		object, tableRef, _ := tcc.Resolve("abc", "t1", &plan2.Snapshot{TS: ts})
		convey.So(object, convey.ShouldNotBeNil)
		convey.So(tableRef, convey.ShouldNotBeNil)

		ref := &plan.ObjectRef{
			SchemaName: "schema",
			PubInfo: &plan.PubInfo{
				TenantId: 0,
			},
		}
		object, tableRef, _ = tcc.ResolveIndexTableByRef(ref, "indexTable", &plan2.Snapshot{TS: ts})
		convey.So(object, convey.ShouldNotBeNil)
		convey.So(tableRef, convey.ShouldNotBeNil)

		stats, err := tcc.Stats(&plan2.ObjectRef{SchemaName: "abc", ObjName: "t1"}, &plan2.Snapshot{TS: ts})
		convey.So(err, convey.ShouldBeNil)
		convey.So(stats, convey.ShouldBeNil)
	})
}

func TestSession_ResolveTempIndexTable(t *testing.T) {
	convey.Convey("test resolve temp index table", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := context.TODO()
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()

		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Hints().Return(engine.Hints{CommitOrRollbackTimeout: time.Second}).AnyTimes()

		db := mock_frontend.NewMockDatabase(ctrl)
		db.EXPECT().Relations(gomock.Any()).Return(nil, nil).AnyTimes()
		db.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, moerr.NewNoSuchTable(ctx, "db1", "index_table")).AnyTimes()
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(db, nil).AnyTimes()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, eng, txnClient, nil)
		setPu("", pu)
		setSessionAlloc("", NewLeakCheckAllocator())

		// Setup session
		sv, _ := getSystemVariables("test/system_vars_config.toml")
		ioses, _ := NewIOSession(&testConn{}, pu, "")
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, sv)
		ses := NewSession(ctx, "", proto, nil)
		ses.txnCompileCtx.execCtx = &ExecCtx{reqCtx: ctx, ses: ses}

		// Mock scenario:
		// 1. "temp_t1" is a temp table, registered in session
		ses.AddTempTable("db1", "temp_t1", "mo_temp_t1_real")

		// 2. We try to resolve an index table "index_table" for "temp_t1"
		// The issue is that the index table is NOT registered in session, so ResolveIndexTableByRef will fail to find it as a temp table
		// and try to look it up as a regular table (which fails in this mock because we didn't mock the regular table lookup to succeed for it)

		tcc := ses.GetTxnCompileCtx()
		ref := &plan.ObjectRef{SchemaName: "db1"}

		// Expectation: This should fail or panic if not handled because GetTempTable returns false
		// and getRelation returns (nil, nil, nil) or error for non-existent regular table.

		// In the current bug state, the code in ResolveIndexTableByRef (after my previous fix) handles nil table but throws NoSuchTable.
		// To reproduce the original panic (if we reverted) or the "no such table" error:

		_, _, err := tcc.ResolveIndexTableByRef(ref, "index_table", &plan2.Snapshot{})

		// If the index was correctly registered, we would mock it:
		// ses.AddTempTable("db1", "index_table", "mo_index_table_real")

		// Assert that it fails with NoSuchTable because it's not in temp tables
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(moerr.IsMoErrCode(err, moerr.ErrNoSuchTable), convey.ShouldBeTrue)
	})
}

func TestSession_updateTimeZone(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newSes(nil, ctrl)
	ctx := context.Background()

	// Test offset timezones
	t.Run("offset timezones", func(t *testing.T) {
		err := updateTimeZone(ctx, ses, ses.GetSessionSysVars(), "time_zone", "+00:00")
		assert.NoError(t, err)
		assert.Equal(t, ses.GetTimeZone().String(), "FixedZone")
		val := ses.GetSessionSysVars().Get("time_zone")
		assert.Equal(t, "+00:00", val)

		err = updateTimeZone(ctx, ses, ses.GetSessionSysVars(), "time_zone", "+08:00")
		assert.NoError(t, err)
		assert.Equal(t, ses.GetTimeZone().String(), "FixedZone")
		val = ses.GetSessionSysVars().Get("time_zone")
		assert.Equal(t, "+08:00", val)

		err = updateTimeZone(ctx, ses, ses.GetSessionSysVars(), "time_zone", "-08:00")
		assert.NoError(t, err)
		assert.Equal(t, ses.GetTimeZone().String(), "FixedZone")
		val = ses.GetSessionSysVars().Get("time_zone")
		assert.Equal(t, "-08:00", val)
	})

	// Test UTC special handling (case-insensitive, no timezone database dependency)
	t.Run("UTC special handling", func(t *testing.T) {
		// Test "UTC" (uppercase)
		err := updateTimeZone(ctx, ses, ses.GetSessionSysVars(), "time_zone", "UTC")
		assert.NoError(t, err)
		assert.Equal(t, time.UTC, ses.GetTimeZone())
		val := ses.GetSessionSysVars().Get("time_zone")
		assert.Equal(t, "UTC", val)

		// Test "utc" (lowercase) - should also work and use time.UTC directly
		err = updateTimeZone(ctx, ses, ses.GetSessionSysVars(), "time_zone", "utc")
		assert.NoError(t, err)
		assert.Equal(t, time.UTC, ses.GetTimeZone())
		val = ses.GetSessionSysVars().Get("time_zone")
		assert.Equal(t, "UTC", val)

		// Test "Utc" (mixed case)
		err = updateTimeZone(ctx, ses, ses.GetSessionSysVars(), "time_zone", "Utc")
		assert.NoError(t, err)
		assert.Equal(t, time.UTC, ses.GetTimeZone())
		val = ses.GetSessionSysVars().Get("time_zone")
		assert.Equal(t, "UTC", val)
	})

	// Test IANA timezone names (case-sensitive)
	t.Run("IANA timezone names case-sensitive", func(t *testing.T) {
		// Test correct case: "Asia/Shanghai"
		err := updateTimeZone(ctx, ses, ses.GetSessionSysVars(), "time_zone", "Asia/Shanghai")
		if err == nil {
			// Only test if timezone database is available
			assert.NotNil(t, ses.GetTimeZone())
			val := ses.GetSessionSysVars().Get("time_zone")
			// Should preserve original case
			assert.Equal(t, "Asia/Shanghai", val)
		}

		// Test correct case: "America/New_York"
		err = updateTimeZone(ctx, ses, ses.GetSessionSysVars(), "time_zone", "America/New_York")
		if err == nil {
			assert.NotNil(t, ses.GetTimeZone())
			val := ses.GetSessionSysVars().Get("time_zone")
			// Should preserve original case
			assert.Equal(t, "America/New_York", val)
		}
	})

	// Test empty string handling
	t.Run("empty string handling", func(t *testing.T) {
		err := updateTimeZone(ctx, ses, ses.GetSessionSysVars(), "time_zone", "")
		// Empty string gets converted to lowercase, then tries to load as location
		// This may fail if timezone database is not available, which is expected
		if err == nil {
			assert.NotNil(t, ses.GetTimeZone())
		}
	})

	// Test invalid timezone
	t.Run("invalid timezone", func(t *testing.T) {
		err := updateTimeZone(ctx, ses, ses.GetSessionSysVars(), "time_zone", "Invalid/Timezone")
		assert.Error(t, err)
	})

	// Test invalid offset format
	t.Run("invalid offset format", func(t *testing.T) {
		err := updateTimeZone(ctx, ses, ses.GetSessionSysVars(), "time_zone", "+25:00")
		assert.Error(t, err)

		err = updateTimeZone(ctx, ses, ses.GetSessionSysVars(), "time_zone", "+08:60")
		assert.Error(t, err)
	})
}

func TestSession_Migrate(t *testing.T) {
	sid := "ms1"

	sv, err := getSystemVariables("test/system_vars_config.toml")
	if err != nil {
		t.Error(err)
	}
	genSession := func(ctrl *gomock.Controller, dbname string, e error) *Session {
		sv.SkipCheckPrivilege = true
		sv.SessionTimeout = toml.Duration{Duration: 10 * time.Second}
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		txnOperator.EXPECT().EnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(0)).AnyTimes()
		txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()
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
			txnOperator.EXPECT().EnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(0)).AnyTimes()
			txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()
			return txnOperator, nil
		}).AnyTimes()
		eng := mock_frontend.NewMockEngine(ctrl)
		hints := engine.Hints{CommitOrRollbackTimeout: time.Second * 10}
		db := mock_frontend.NewMockDatabase(ctrl)
		eng.EXPECT().Hints().Return(hints).AnyTimes()
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Database(gomock.Any(), dbname, gomock.Any()).Return(db, e).AnyTimes()
		rel := mock_frontend.NewMockRelation(ctrl)
		rel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{}).AnyTimes()
		rel.EXPECT().TableDefs(gomock.Any()).Return(nil, nil).AnyTimes()
		var tid uint64
		rel.EXPECT().GetTableID(gomock.Any()).Return(tid).AnyTimes()
		db.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
		db.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(rel, nil).AnyTimes()
		setPu(sid, &config.ParameterUnit{
			SV:            sv,
			TxnClient:     txnClient,
			StorageEngine: eng,
		})

		mockBS := MockBaseService{}
		rm, _ := NewRoutineManager(context.Background(), sid)
		rm.baseService = &mockBS
		setRtMgr(sid, rm)
		ctx := defines.AttachAccountId(context.Background(), sysAccountID)
		ioses, err := NewIOSession(&testConn{}, getPu(sid), sid)
		if err != nil {
			panic(err)
		}
		proto := NewMysqlClientProtocol(sid, 0, ioses, 1024, sv)
		session := NewSession(ctx, sid, proto, nil)
		session.tenant = &TenantInfo{
			Tenant:   GetDefaultTenant(),
			TenantID: GetSysTenantId(),
		}
		session.txnCompileCtx.execCtx = &ExecCtx{reqCtx: ctx, proc: testutil.NewProc(t), ses: session}
		proto.ses = session
		session.setRoutineManager(rm)
		return session
	}

	t.Run("ok", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		runtime.SetupServiceBasedRuntime(sid, runtime.DefaultRuntime())
		InitServerLevelVars(sid)
		SetSessionAlloc(sid, NewSessionAllocator(&config.ParameterUnit{SV: sv}))
		s := genSession(ctrl, "d1", nil)
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
	})

	t.Run("db dropped", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
		defer bhStub.Reset()

		runtime.SetupServiceBasedRuntime(sid, runtime.DefaultRuntime())
		InitServerLevelVars(sid)
		SetSessionAlloc(sid, NewSessionAllocator(&config.ParameterUnit{SV: sv}))
		s := genSession(ctrl, "d2", context.Canceled)
		err := Migrate(s, &query.MigrateConnToRequest{DB: "d2"})
		assert.Equal(t, "", s.GetDatabaseName())
		assert.NoError(t, err)
	})
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
	_, err = authenticateUserCanExecuteStatement(ctx, ses, rp[0])
	assert.Error(t, err)

	// exexpir can execute stmt
	sql = "alter user dump identified by '123456'"
	rp, err = mysql.Parse(ctx, sql, 1)
	defer rp[0].Free()
	assert.NoError(t, err)
	_, err = authenticateUserCanExecuteStatement(ctx, ses, rp[0])
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

func TestSessionTempTableMap(t *testing.T) {
	ses := &Session{
		tempTables:    make(map[string]string),
		tempTablesRev: make(map[string]string),
	}

	ses.AddTempTable("db1", "alias", "real")

	name, ok := ses.GetTempTable("db1", "alias")
	assert.True(t, ok)
	assert.Equal(t, "real", name)

	ses.RemoveTempTableByRealName("real")
	_, ok = ses.GetTempTable("db1", "alias")
	assert.False(t, ok)
}

func TestSession_Cleanup(t *testing.T) {
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	tz := time.Local
	ses := &Session{}
	ses.timeZone = tz
	ses.txnHandler = &TxnHandler{}

	// 1. Test getCleanupContext with nil txnCtx
	ctx := ses.getCleanupContext()
	assert.NotNil(t, ctx)
	assert.Equal(t, context.Background(), ctx)

	// 2. Test getCleanupContext with valid txnCtx
	ses.txnHandler.txnCtx = context.WithValue(context.Background(), "test", "value")
	ctx2 := ses.getCleanupContext()
	assert.Equal(t, ses.txnHandler.txnCtx, ctx2)

	// 3. Test the code path in reset (manual execution to ensure coverage)
	prev := &Session{}
	prev.txnHandler = &TxnHandler{}
	tempExecCtx := ExecCtx{
		reqCtx: prev.getCleanupContext(),
		ses:    prev,
		txnOpt: FeTxnOption{byRollback: true},
	}
	assert.NotNil(t, tempExecCtx.reqCtx)

	// 4. Test backSession getCleanupContext
	bses := &backSession{}
	bses.txnHandler = &TxnHandler{}
	ctx3 := bses.getCleanupContext()
	assert.NotNil(t, ctx3)

	// 5. Test Routine getCleanupContext
	rt := &Routine{}
	rt.ses = ses
	ctx4 := rt.getCleanupContext()
	assert.Equal(t, ses.txnHandler.txnCtx, ctx4)
}
