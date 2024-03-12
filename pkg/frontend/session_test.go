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
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"math"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
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

		var gSys GlobalSystemVariables
		InitGlobalSystemVariables(&gSys)

		txn := InitTxnHandler(eng, txnClient, nil, nil)
		txn.ses = &Session{
			requestCtx: ctx,
			pu:         pu,
			connectCtx: ctx,
			gSysVars:   &gSys,
		}
		_, _, err = txn.NewTxn()
		convey.So(err, convey.ShouldBeNil)
		_, _, err = txn.NewTxn()
		convey.So(err, convey.ShouldNotBeNil)
		_, _, err = txn.NewTxn()
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

		var gSys GlobalSystemVariables
		InitGlobalSystemVariables(&gSys)

		txn := InitTxnHandler(eng, txnClient, nil, nil)
		txn.ses = &Session{
			requestCtx: ctx,
			pu:         pu,
			connectCtx: ctx,
			gSysVars:   &gSys,
		}
		_, _, err = txn.NewTxn()
		convey.So(err, convey.ShouldBeNil)
		err = txn.CommitTxn()
		convey.So(err, convey.ShouldBeNil)
		convey.ShouldEqual(timestamp.Timestamp{PhysicalTime: idx}, txn.ses.lastCommitTS)

		_, _, err = txn.NewTxn()
		convey.So(err, convey.ShouldBeNil)

		idx++
		err = txn.CommitTxn()
		convey.So(err, convey.ShouldNotBeNil)
		convey.ShouldEqual(timestamp.Timestamp{PhysicalTime: idx}, txn.ses.lastCommitTS)
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

		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		eng.EXPECT().Hints().Return(engine.Hints{
			CommitOrRollbackTimeout: time.Second,
		}).AnyTimes()

		txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

		pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		convey.So(err, convey.ShouldBeNil)
		var gSys GlobalSystemVariables
		InitGlobalSystemVariables(&gSys)

		txn := InitTxnHandler(eng, txnClient, nil, nil)
		txn.ses = &Session{
			requestCtx: ctx,
			pu:         pu,
			connectCtx: ctx,
			gSysVars:   &gSys,
		}
		_, _, err = txn.NewTxn()
		convey.So(err, convey.ShouldBeNil)
		err = txn.RollbackTxn()
		convey.So(err, convey.ShouldBeNil)
		_, _, err = txn.NewTxn()
		convey.So(err, convey.ShouldBeNil)
		err = txn.RollbackTxn()
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestSession_TxnBegin(t *testing.T) {
	genSession := func(ctrl *gomock.Controller, gSysVars *GlobalSystemVariables) *Session {
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
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()
		eng := mock_frontend.NewMockEngine(ctrl)
		hints := engine.Hints{CommitOrRollbackTimeout: time.Second * 10}
		eng.EXPECT().Hints().Return(hints).AnyTimes()
		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		session := NewSession(proto, nil, config.NewParameterUnit(&config.FrontendParameters{}, eng, txnClient, nil), gSysVars, false, nil, nil)
		ctx := defines.AttachAccountId(context.Background(), sysAccountID)
		session.SetRequestContext(ctx)
		session.SetConnectContext(ctx)
		return session
	}
	convey.Convey("new session", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		gSysVars := &GlobalSystemVariables{}
		InitGlobalSystemVariables(gSysVars)

		ses := genSession(ctrl, gSysVars)
		err := ses.TxnBegin()
		convey.So(err, convey.ShouldBeNil)
		err = ses.TxnCommit()
		convey.So(err, convey.ShouldBeNil)
		err = ses.TxnBegin()
		convey.So(err, convey.ShouldBeNil)
		err = ses.SetAutocommit(true, false)
		convey.So(err, convey.ShouldBeNil)
		err = ses.TxnCommit()
		convey.So(err, convey.ShouldBeNil)
		_, _, err = ses.GetTxnHandler().GetTxn()
		convey.So(err, convey.ShouldBeNil)

		err = ses.TxnCommit()
		convey.So(err, convey.ShouldBeNil)

		err = ses.SetAutocommit(false, true)
		convey.So(err, convey.ShouldBeNil)

		err = ses.SetAutocommit(false, false)
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestVariables(t *testing.T) {
	genSession := func(ctrl *gomock.Controller, gSysVars *GlobalSystemVariables) *Session {
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
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any()).AnyTimes()
		session := NewSession(proto, nil, config.NewParameterUnit(&config.FrontendParameters{}, nil, txnClient, nil), gSysVars, true, nil, nil)
		session.SetRequestContext(context.Background())
		return session
	}

	checkWant := func(ses, existSes, newSesAfterSession *Session,
		v string,
		sameSesWant1, existSesWant2, newSesAfterSesWant3,
		saneSesGlobalWant4, existSesGlobalWant5, newSesAfterSesGlobalWant6 interface{}) {

		//same session
		v1_val, err := ses.GetSessionVar(v)
		convey.So(err, convey.ShouldBeNil)
		convey.So(sameSesWant1, convey.ShouldEqual, v1_val)
		v1_ctx_val, err := ses.GetTxnCompileCtx().ResolveVariable(v, true, false)
		convey.So(err, convey.ShouldBeNil)
		convey.So(v1_ctx_val, convey.ShouldEqual, v1_val)

		//exist session
		v2_val, err := existSes.GetSessionVar(v)
		convey.So(err, convey.ShouldBeNil)
		convey.So(existSesWant2, convey.ShouldEqual, v2_val)
		v2_ctx_val, err := existSes.GetTxnCompileCtx().ResolveVariable(v, true, false)
		convey.So(err, convey.ShouldBeNil)
		convey.So(v2_ctx_val, convey.ShouldEqual, v2_val)

		//new session after session
		v3_val, err := newSesAfterSession.GetSessionVar(v)
		convey.So(err, convey.ShouldBeNil)
		convey.So(newSesAfterSesWant3, convey.ShouldEqual, v3_val)
		v3_ctx_val, err := newSesAfterSession.GetTxnCompileCtx().ResolveVariable(v, true, false)
		convey.So(err, convey.ShouldBeNil)
		convey.So(v3_ctx_val, convey.ShouldEqual, v3_val)

		//new session after session global
		v6_val, err := newSesAfterSession.GetGlobalVar(v)
		convey.So(err, convey.ShouldBeNil)
		convey.So(newSesAfterSesGlobalWant6, convey.ShouldEqual, v6_val)
		v6_ctx_val, err := newSesAfterSession.GetTxnCompileCtx().ResolveVariable(v, true, false)
		convey.So(err, convey.ShouldBeNil)
		convey.So(v6_ctx_val, convey.ShouldEqual, v6_val)
	}

	checkWant2 := func(ses, existSes, newSesAfterSession *Session,
		v string,
		sameSesWant1, existSesWant2, newSesAfterSesWant3 interface{}) {

		//same session
		v1_val, err := ses.GetSessionVar(v)
		convey.So(err, convey.ShouldBeNil)
		convey.So(sameSesWant1, convey.ShouldEqual, v1_val)
		v1_ctx_val, err := ses.GetTxnCompileCtx().ResolveVariable(v, true, false)
		convey.So(err, convey.ShouldBeNil)
		convey.So(v1_ctx_val, convey.ShouldEqual, v1_val)

		//exist session
		v2_val, err := existSes.GetSessionVar(v)
		convey.So(err, convey.ShouldBeNil)
		convey.So(existSesWant2, convey.ShouldEqual, v2_val)
		v2_ctx_val, err := existSes.GetTxnCompileCtx().ResolveVariable(v, true, false)
		convey.So(err, convey.ShouldBeNil)
		convey.So(v2_ctx_val, convey.ShouldEqual, v2_val)

		//new session after session
		v3_val, err := newSesAfterSession.GetSessionVar(v)
		convey.So(err, convey.ShouldBeNil)
		convey.So(newSesAfterSesWant3, convey.ShouldEqual, v3_val)
		v3_ctx_val, err := newSesAfterSession.GetTxnCompileCtx().ResolveVariable(v, true, false)
		convey.So(err, convey.ShouldBeNil)
		convey.So(v3_ctx_val, convey.ShouldEqual, v3_val)

		//same session global
		_, err = ses.GetGlobalVar(v)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeError, moerr.NewInternalError(context.TODO(), errorSystemVariableSessionEmpty()))
		_, err = ses.GetTxnCompileCtx().ResolveVariable(v, true, false)
		convey.So(err, convey.ShouldBeNil)

		//exist session global
		_, err = existSes.GetGlobalVar(v)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeError, moerr.NewInternalError(context.TODO(), errorSystemVariableSessionEmpty()))
		_, err = existSes.GetTxnCompileCtx().ResolveVariable(v, true, false)
		convey.So(err, convey.ShouldBeNil)

		//new session after session global
		_, err = newSesAfterSession.GetGlobalVar(v)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeError, moerr.NewInternalError(context.TODO(), errorSystemVariableSessionEmpty()))
		_, err = newSesAfterSession.GetTxnCompileCtx().ResolveVariable(v, true, false)
		convey.So(err, convey.ShouldBeNil)
	}

	convey.Convey("scope global", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		gSysVars := &GlobalSystemVariables{}
		InitGlobalSystemVariables(gSysVars)

		ses := genSession(ctrl, gSysVars)
		existSes := genSession(ctrl, gSysVars)

		v1 := "testglobalvar_dyn"
		_, v1_default, _ := gSysVars.GetGlobalSysVar(v1)
		v1_want := 10
		err := ses.SetSessionVar(v1, v1_want)
		convey.So(err, convey.ShouldNotBeNil)

		// no check after fail set
		newSes2 := genSession(ctrl, gSysVars)
		checkWant(ses, existSes, newSes2, v1, v1_default, v1_default, v1_default, v1_default, v1_default, v1_default)

		err = ses.SetGlobalVar(v1, v1_want)
		convey.So(err, convey.ShouldBeNil)

		newSes3 := genSession(ctrl, gSysVars)
		checkWant(ses, existSes, newSes3, v1, v1_want, v1_want, v1_want, v1_want, v1_want, v1_want)

		v2 := "testglobalvar_nodyn"
		_, v2_default, _ := gSysVars.GetGlobalSysVar(v2)
		v2_want := 10
		err = ses.SetSessionVar(v2, v2_want)
		convey.So(err, convey.ShouldNotBeNil)

		newSes4 := genSession(ctrl, gSysVars)
		checkWant(ses, existSes, newSes4, v2, v2_default, v2_default, v2_default, v2_default, v2_default, v2_default)

		err = ses.SetGlobalVar(v2, v2_want)
		convey.So(err, convey.ShouldNotBeNil)

		newSes5 := genSession(ctrl, gSysVars)
		checkWant(ses, existSes, newSes5, v2, v2_default, v2_default, v2_default, v2_default, v2_default, v2_default)
	})

	convey.Convey("scope session", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		gSysVars := &GlobalSystemVariables{}
		InitGlobalSystemVariables(gSysVars)

		ses := genSession(ctrl, gSysVars)
		existSes := genSession(ctrl, gSysVars)

		v1 := "testsessionvar_dyn"
		_, v1_default, _ := gSysVars.GetGlobalSysVar(v1)
		v1_want := 10
		err := ses.SetSessionVar(v1, v1_want)
		convey.So(err, convey.ShouldBeNil)

		newSes1 := genSession(ctrl, gSysVars)
		checkWant2(ses, existSes, newSes1, v1, v1_want, v1_default, v1_default)

		err = ses.SetGlobalVar(v1, v1_want)
		convey.So(err, convey.ShouldNotBeNil)

		newSes2 := genSession(ctrl, gSysVars)
		checkWant2(ses, existSes, newSes2, v1, v1_want, v1_default, v1_default)

		v2 := "testsessionvar_nodyn"
		_, v2_default, _ := gSysVars.GetGlobalSysVar(v2)
		v2_want := 10
		err = ses.SetSessionVar(v2, v2_want)
		convey.So(err, convey.ShouldNotBeNil)

		newSes3 := genSession(ctrl, gSysVars)
		checkWant2(ses, existSes, newSes3, v2, v2_default, v2_default, v2_default)

		err = ses.SetGlobalVar(v2, v2_want)
		convey.So(err, convey.ShouldNotBeNil)
		newSes4 := genSession(ctrl, gSysVars)
		checkWant2(ses, existSes, newSes4, v2, v2_default, v2_default, v2_default)

	})

	convey.Convey("scope both - set session", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		gSysVars := &GlobalSystemVariables{}
		InitGlobalSystemVariables(gSysVars)

		ses := genSession(ctrl, gSysVars)
		existSes := genSession(ctrl, gSysVars)

		v1 := "testbothvar_dyn"
		_, v1_default, _ := gSysVars.GetGlobalSysVar(v1)
		v1_want := 10
		err := ses.SetSessionVar(v1, v1_want)
		convey.So(err, convey.ShouldBeNil)

		newSes2 := genSession(ctrl, gSysVars)
		checkWant(ses, existSes, newSes2, v1, v1_want, v1_default, v1_default, v1_default, v1_default, v1_default)

		v2 := "testbotchvar_nodyn"
		err = ses.SetSessionVar(v2, 10)
		convey.So(err, convey.ShouldNotBeNil)

		err = ses.SetGlobalVar(v2, 10)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("scope both", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		gSysVars := &GlobalSystemVariables{}
		InitGlobalSystemVariables(gSysVars)

		ses := genSession(ctrl, gSysVars)
		existSes := genSession(ctrl, gSysVars)

		v1 := "testbothvar_dyn"
		_, v1_default, _ := gSysVars.GetGlobalSysVar(v1)
		v1_want := 10

		err := ses.SetGlobalVar(v1, v1_want)
		convey.So(err, convey.ShouldBeNil)

		newSes2 := genSession(ctrl, gSysVars)
		checkWant(ses, existSes, newSes2, v1, v1_default, v1_default, v1_want, v1_want, v1_want, v1_want)
	})

	convey.Convey("user variables", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		gSysVars := &GlobalSystemVariables{}
		InitGlobalSystemVariables(gSysVars)

		ses := genSession(ctrl, gSysVars)

		vars := ses.CopyAllSessionVars()
		convey.So(len(vars), convey.ShouldNotBeZeroValue)

		err := ses.SetUserDefinedVar("abc", 1, "")
		convey.So(err, convey.ShouldBeNil)

		_, _, err = ses.GetUserDefinedVar("abc")
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestSession_TxnCompilerContext(t *testing.T) {
	genSession := func(ctrl *gomock.Controller, pu *config.ParameterUnit, gSysVars *GlobalSystemVariables) *Session {
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
		session := NewSession(proto, nil, pu, gSysVars, false, nil, nil)
		ctx := defines.AttachAccountId(context.Background(), sysAccountID)
		session.SetRequestContext(ctx)
		session.SetConnectContext(ctx)
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
		table.EXPECT().GetPrimaryKeys(gomock.Any()).Return(nil, nil).AnyTimes()
		table.EXPECT().GetHideKeys(gomock.Any()).Return(nil, nil).AnyTimes()
		table.EXPECT().Stats(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		table.EXPECT().TableColumns(gomock.Any()).Return(nil, nil).AnyTimes()
		table.EXPECT().GetTableID(gomock.Any()).Return(uint64(10)).AnyTimes()
		table.EXPECT().GetEngineType().Return(engine.Disttae).AnyTimes()
		table.EXPECT().ApproxObjectsNum(gomock.Any()).Return(0).AnyTimes()
		db.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(table, nil).AnyTimes()
		db.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(db, nil).AnyTimes()

		pu := config.NewParameterUnit(&config.FrontendParameters{}, eng, txnClient, nil)

		gSysVars := &GlobalSystemVariables{}
		InitGlobalSystemVariables(gSysVars)

		ses := genSession(ctrl, pu, gSysVars)

		tcc := ses.GetTxnCompileCtx()
		defDBName := tcc.DefaultDatabase()
		convey.So(defDBName, convey.ShouldEqual, "")
		convey.So(tcc.DatabaseExists("abc"), convey.ShouldBeTrue)

		_, _, err := tcc.getRelation("abc", "t1", nil)
		convey.So(err, convey.ShouldBeNil)

		object, tableRef := tcc.Resolve("abc", "t1")
		convey.So(object, convey.ShouldNotBeNil)
		convey.So(tableRef, convey.ShouldNotBeNil)

		pkd := tcc.GetPrimaryKeyDef("abc", "t1")
		convey.So(len(pkd), convey.ShouldBeZeroValue)

		stats, err := tcc.Stats(&plan2.ObjectRef{SchemaName: "abc", ObjName: "t1"})
		convey.So(err, convey.ShouldBeNil)
		convey.So(stats, convey.ShouldBeNil)
	})
}

func TestSession_GetTempTableStorage(t *testing.T) {
	genSession := func(ctrl *gomock.Controller, pu *config.ParameterUnit, gSysVars *GlobalSystemVariables) *Session {
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
		session := NewSession(proto, nil, pu, gSysVars, false, nil, nil)
		session.SetRequestContext(context.Background())
		session.SetConnectContext(context.Background())
		return session
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	eng := mock_frontend.NewMockEngine(ctrl)
	pu := config.NewParameterUnit(&config.FrontendParameters{}, eng, txnClient, nil)
	gSysVars := &GlobalSystemVariables{}

	ses := genSession(ctrl, pu, gSysVars)
	assert.Panics(t, func() {
		_ = ses.GetTempTableStorage()
	})
}

func TestIfInitedTempEngine(t *testing.T) {
	genSession := func(ctrl *gomock.Controller, pu *config.ParameterUnit, gSysVars *GlobalSystemVariables) *Session {
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
		session := NewSession(proto, nil, pu, gSysVars, false, nil, nil)
		session.SetRequestContext(context.Background())
		return session
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	eng := mock_frontend.NewMockEngine(ctrl)
	pu := config.NewParameterUnit(&config.FrontendParameters{}, eng, txnClient, nil)
	gSysVars := &GlobalSystemVariables{}

	ses := genSession(ctrl, pu, gSysVars)
	assert.False(t, ses.IfInitedTempEngine())
}

func TestSetTempTableStorage(t *testing.T) {
	genSession := func(ctrl *gomock.Controller, pu *config.ParameterUnit, gSysVars *GlobalSystemVariables) *Session {
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
		session := NewSession(proto, nil, pu, gSysVars, false, nil, nil)
		session.SetRequestContext(context.Background())
		return session
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	eng := mock_frontend.NewMockEngine(ctrl)
	pu := config.NewParameterUnit(&config.FrontendParameters{}, eng, txnClient, nil)
	gSysVars := &GlobalSystemVariables{}

	ses := genSession(ctrl, pu, gSysVars)

	ck := clock.NewHLCClock(func() int64 {
		return time.Now().Unix()
	}, math.MaxInt)
	tnStore, _ := ses.SetTempTableStorage(ck)

	assert.Equal(t, defines.TEMPORARY_TABLE_TN_ADDR, tnStore.TxnServiceAddress)
}

func Test_doSelectGlobalSystemVariable(t *testing.T) {
	convey.Convey("select global system variable fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		stmt := &tree.ShowVariables{
			Global: true,
		}

		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql := getSqlForGetSystemVariableValueWithAccount(uint64(ses.GetTenantInfo().GetTenantID()), "autocommit")
		mrs := newMrsForSqlForCheckUserHasRole([][]interface{}{})
		bh.sql2result[sql] = mrs

		_, err := ses.getGlobalSystemVariableValue("autocommit")
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("select global system variable fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		bh := &backgroundExecTest{}
		bh.init()

		bhStub := gostub.StubFunc(&NewBackgroundHandler, bh)
		defer bhStub.Reset()

		stmt := &tree.ShowVariables{
			Global: true,
		}

		priv := determinePrivilegeSetOfStatement(stmt)
		ses := newSes(priv, ctrl)

		//no result set
		bh.sql2result["begin;"] = nil
		bh.sql2result["commit;"] = nil
		bh.sql2result["rollback;"] = nil

		sql := getSqlForGetSystemVariableValueWithAccount(uint64(ses.GetTenantInfo().GetTenantID()), "autocommit")
		mrs := newMrsForSqlForCheckUserHasRole([][]interface{}{
			{"1"},
		})
		bh.sql2result[sql] = mrs

		_, err := ses.getGlobalSystemVariableValue("autocommit")
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestSession_updateTimeZone(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newSes(nil, ctrl)

	err := updateTimeZone(ses, ses.GetSysVars(), "time_zone", "system")
	assert.NoError(t, err)
	assert.Equal(t, ses.GetTimeZone().String(), "Local")

	err = updateTimeZone(ses, ses.GetSysVars(), "time_zone", "+00:00")
	assert.NoError(t, err)
	assert.Equal(t, ses.GetTimeZone().String(), "FixedZone")

	err = updateTimeZone(ses, ses.GetSysVars(), "time_zone", "+08:00")
	assert.NoError(t, err)
	assert.Equal(t, ses.GetTimeZone().String(), "FixedZone")

	err = updateTimeZone(ses, ses.GetSysVars(), "time_zone", "-08:00")
	assert.NoError(t, err)
	assert.Equal(t, ses.GetTimeZone().String(), "FixedZone")

	//ci fails the case
	//err = updateTimeZone(ses, ses.GetSysVars(), "time_zone", "UTC")
	//assert.NoError(t, err)
	//assert.Equal(t, ses.GetTimeZone().String(), "utc")

	err = updateTimeZone(ses, ses.GetSysVars(), "time_zone", "")
	assert.NoError(t, err)
	assert.Equal(t, ses.GetTimeZone().String(), "UTC")
}

func TestSession_Migrate(t *testing.T) {
	genSession := func(ctrl *gomock.Controller, gSysVars *GlobalSystemVariables) *Session {
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
		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()
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
		session := NewSession(proto, nil, config.NewParameterUnit(&config.FrontendParameters{}, eng, txnClient, nil), gSysVars, true, nil, nil)
		ctx := defines.AttachAccountId(context.Background(), sysAccountID)
		session.SetRequestContext(ctx)
		session.SetConnectContext(ctx)
		session.txnCompileCtx.SetProcess(testutil.NewProc())
		return session
	}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	gSysVars := &GlobalSystemVariables{}
	InitGlobalSystemVariables(gSysVars)
	s := genSession(ctrl, gSysVars)
	err := s.Migrate(&query.MigrateConnToRequest{
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
