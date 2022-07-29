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
	"errors"
	"github.com/fagongzi/goetty/buf"
	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/config"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"
	"github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestTxnHandler_NewTxn(t *testing.T) {
	convey.Convey("new txn", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		taeTxn := mock_frontend.NewMockTxn(ctrl)
		taeTxn.EXPECT().String().Return("").AnyTimes()
		taeTxn.EXPECT().Commit().Return(nil).AnyTimes()
		storage := mock_frontend.NewMockTxnEngine(ctrl)
		cnt := 0
		storage.EXPECT().StartTxn(gomock.Any()).DoAndReturn(
			func(x interface{}) (moengine.Txn, error) {
				cnt++
				if cnt%2 != 0 {
					return taeTxn, nil
				} else {
					return nil, errors.New("startTxn failed")
				}
			}).AnyTimes()

		txn := InitTxnHandler(storage)
		err := txn.NewTxn()
		convey.So(err, convey.ShouldBeNil)
		err = txn.NewTxn()
		convey.So(err, convey.ShouldNotBeNil)
		err = txn.NewTxn()
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestTxnHandler_CommitTxn(t *testing.T) {
	convey.Convey("commit txn", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		taeTxn := mock_frontend.NewMockTxn(ctrl)
		taeTxn.EXPECT().String().Return("").AnyTimes()
		cnt := 0
		taeTxn.EXPECT().Commit().DoAndReturn(
			func() error {
				cnt++
				if cnt%2 != 0 {
					return nil
				} else {
					return errors.New("commit failed")
				}
			}).AnyTimes()
		storage := mock_frontend.NewMockTxnEngine(ctrl)

		storage.EXPECT().StartTxn(gomock.Any()).Return(taeTxn, nil).AnyTimes()

		txn := InitTxnHandler(storage)
		err := txn.NewTxn()
		convey.So(err, convey.ShouldBeNil)
		err = txn.CommitTxn()
		convey.So(err, convey.ShouldBeNil)
		err = txn.NewTxn()
		convey.So(err, convey.ShouldBeNil)
		err = txn.CommitTxn()
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestTxnHandler_RollbackTxn(t *testing.T) {
	convey.Convey("rollback txn", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		taeTxn := mock_frontend.NewMockTxn(ctrl)
		taeTxn.EXPECT().String().Return("").AnyTimes()
		cnt := 0
		taeTxn.EXPECT().Rollback().DoAndReturn(
			func() error {
				cnt++
				if cnt%2 != 0 {
					return nil
				} else {
					return errors.New("rollback failed")
				}
			}).AnyTimes()
		storage := mock_frontend.NewMockTxnEngine(ctrl)

		storage.EXPECT().StartTxn(gomock.Any()).Return(taeTxn, nil).AnyTimes()

		txn := InitTxnHandler(storage)
		err := txn.NewTxn()
		convey.So(err, convey.ShouldBeNil)
		err = txn.RollbackTxn()
		convey.So(err, convey.ShouldBeNil)
		err = txn.NewTxn()
		convey.So(err, convey.ShouldBeNil)
		err = txn.RollbackTxn()
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestSession_TxnBegin(t *testing.T) {
	genSession := func(ctrl *gomock.Controller, gSysVars *GlobalSystemVariables) *Session {
		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().WriteAndFlush(gomock.Any()).Return(nil).AnyTimes()
		proto := NewMysqlClientProtocol(0, ioses, 1024, nil)
		return NewSession(proto, nil, nil, nil, gSysVars)
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
		err = ses.SetAutocommit(false)
		convey.So(err, convey.ShouldNotBeNil)
		err = ses.TxnCommit()
		convey.So(err, convey.ShouldBeNil)
		_ = ses.txnHandler.GetTxn()

		err = ses.SetAutocommit(true)
		convey.So(err, convey.ShouldBeNil)

		err = ses.SetAutocommit(false)
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestVariables(t *testing.T) {
	genSession := func(ctrl *gomock.Controller, gSysVars *GlobalSystemVariables) *Session {
		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().WriteAndFlush(gomock.Any()).Return(nil).AnyTimes()
		proto := NewMysqlClientProtocol(0, ioses, 1024, nil)
		return NewSession(proto, nil, nil, nil, gSysVars)
	}

	checkWant := func(ses, existSes, newSesAfterSession *Session,
		v string,
		sameSesWant1, existSesWant2, newSesAfterSesWant3,
		saneSesGlobalWant4, existSesGlobalWant5, newSesAfterSesGlobalWant6 interface{}) {

		//same session
		v1_val, err := ses.GetSessionVar(v)
		convey.So(err, convey.ShouldBeNil)
		convey.So(sameSesWant1, convey.ShouldEqual, v1_val)
		v1_ctx_val, err := ses.txnCompileCtx.ResolveVariable(v, true, false)
		convey.So(err, convey.ShouldBeNil)
		convey.So(v1_ctx_val, convey.ShouldEqual, v1_val)

		//exist session
		v2_val, err := existSes.GetSessionVar(v)
		convey.So(err, convey.ShouldBeNil)
		convey.So(existSesWant2, convey.ShouldEqual, v2_val)
		v2_ctx_val, err := existSes.txnCompileCtx.ResolveVariable(v, true, false)
		convey.So(err, convey.ShouldBeNil)
		convey.So(v2_ctx_val, convey.ShouldEqual, v2_val)

		//new session after session
		v3_val, err := newSesAfterSession.GetSessionVar(v)
		convey.So(err, convey.ShouldBeNil)
		convey.So(newSesAfterSesWant3, convey.ShouldEqual, v3_val)
		v3_ctx_val, err := newSesAfterSession.txnCompileCtx.ResolveVariable(v, true, false)
		convey.So(err, convey.ShouldBeNil)
		convey.So(v3_ctx_val, convey.ShouldEqual, v3_val)

		//same session global
		v4_val, err := ses.GetGlobalVar(v)
		convey.So(err, convey.ShouldBeNil)
		convey.So(saneSesGlobalWant4, convey.ShouldEqual, v4_val)
		v4_ctx_val, err := ses.txnCompileCtx.ResolveVariable(v, true, true)
		convey.So(err, convey.ShouldBeNil)
		convey.So(v4_ctx_val, convey.ShouldEqual, v4_val)

		//exist session global
		v5_val, err := existSes.GetGlobalVar(v)
		convey.So(err, convey.ShouldBeNil)
		convey.So(existSesGlobalWant5, convey.ShouldEqual, v5_val)
		v5_ctx_val, err := existSes.txnCompileCtx.ResolveVariable(v, true, true)
		convey.So(err, convey.ShouldBeNil)
		convey.So(v5_ctx_val, convey.ShouldEqual, v5_val)

		//new session after session global
		v6_val, err := newSesAfterSession.GetGlobalVar(v)
		convey.So(err, convey.ShouldBeNil)
		convey.So(newSesAfterSesGlobalWant6, convey.ShouldEqual, v6_val)
		v6_ctx_val, err := newSesAfterSession.txnCompileCtx.ResolveVariable(v, true, true)
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
		v1_ctx_val, err := ses.txnCompileCtx.ResolveVariable(v, true, false)
		convey.So(err, convey.ShouldBeNil)
		convey.So(v1_ctx_val, convey.ShouldEqual, v1_val)

		//exist session
		v2_val, err := existSes.GetSessionVar(v)
		convey.So(err, convey.ShouldBeNil)
		convey.So(existSesWant2, convey.ShouldEqual, v2_val)
		v2_ctx_val, err := existSes.txnCompileCtx.ResolveVariable(v, true, false)
		convey.So(err, convey.ShouldBeNil)
		convey.So(v2_ctx_val, convey.ShouldEqual, v2_val)

		//new session after session
		v3_val, err := newSesAfterSession.GetSessionVar(v)
		convey.So(err, convey.ShouldBeNil)
		convey.So(newSesAfterSesWant3, convey.ShouldEqual, v3_val)
		v3_ctx_val, err := newSesAfterSession.txnCompileCtx.ResolveVariable(v, true, false)
		convey.So(err, convey.ShouldBeNil)
		convey.So(v3_ctx_val, convey.ShouldEqual, v3_val)

		//same session global
		_, err = ses.GetGlobalVar(v)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeError, errorSystemVariableSessionEmpty)
		_, err = ses.txnCompileCtx.ResolveVariable(v, true, true)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeError, errorSystemVariableSessionEmpty)

		//exist session global
		_, err = existSes.GetGlobalVar(v)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeError, errorSystemVariableSessionEmpty)
		_, err = existSes.txnCompileCtx.ResolveVariable(v, true, true)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeError, errorSystemVariableSessionEmpty)

		//new session after session global
		_, err = newSesAfterSession.GetGlobalVar(v)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeError, errorSystemVariableSessionEmpty)
		_, err = newSesAfterSession.txnCompileCtx.ResolveVariable(v, true, true)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeError, errorSystemVariableSessionEmpty)
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

		err := ses.SetUserDefinedVar("abc", 1)
		convey.So(err, convey.ShouldBeNil)

		_, _, err = ses.GetUserDefinedVar("abc")
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestSession_TxnCompilerContext(t *testing.T) {
	genSession := func(ctrl *gomock.Controller, gSysVars *GlobalSystemVariables) *Session {
		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().WriteAndFlush(gomock.Any()).Return(nil).AnyTimes()
		proto := NewMysqlClientProtocol(0, ioses, 1024, nil)
		return NewSession(proto, nil, nil, nil, gSysVars)
	}

	convey.Convey("test", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		taeTxn := mock_frontend.NewMockTxn(ctrl)
		taeTxn.EXPECT().String().Return("").AnyTimes()
		taeTxn.EXPECT().Commit().Return(nil).AnyTimes()
		txn := mock_frontend.NewMockTxn(ctrl)
		txn.EXPECT().GetCtx().Return(nil).AnyTimes()
		txn.EXPECT().Commit().Return(nil).AnyTimes()
		txn.EXPECT().Rollback().Return(nil).AnyTimes()
		txn.EXPECT().String().Return("txn0").AnyTimes()
		storage := mock_frontend.NewMockTxnEngine(ctrl)
		storage.EXPECT().StartTxn(gomock.Any()).Return(txn, nil).AnyTimes()

		db := mock_frontend.NewMockDatabase(ctrl)
		db.EXPECT().Relations(gomock.Any()).Return(nil, nil).AnyTimes()

		table := mock_frontend.NewMockRelation(ctrl)
		table.EXPECT().TableDefs(gomock.Any()).Return(nil, nil).AnyTimes()
		table.EXPECT().GetPrimaryKeys(gomock.Any()).Return(nil, nil).AnyTimes()
		table.EXPECT().GetHideKeys(gomock.Any()).Return(nil, nil).AnyTimes()
		table.EXPECT().Rows().Return(int64(1000000)).AnyTimes()
		db.EXPECT().Relation(gomock.Any(), gomock.Any()).Return(table, nil).AnyTimes()
		storage.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(db, nil).AnyTimes()

		config.StorageEngine = storage
		defer func() {
			config.StorageEngine = nil
		}()

		gSysVars := &GlobalSystemVariables{}
		InitGlobalSystemVariables(gSysVars)

		ses := genSession(ctrl, gSysVars)

		tcc := ses.GetTxnCompilerContext()
		defDBName := tcc.DefaultDatabase()
		convey.So(defDBName, convey.ShouldEqual, "")
		convey.So(tcc.DatabaseExists("abc"), convey.ShouldBeTrue)

		_, err := tcc.getRelation("abc", "t1")
		convey.So(err, convey.ShouldBeNil)

		object, tableRef := tcc.Resolve("abc", "t1")
		convey.So(object, convey.ShouldNotBeNil)
		convey.So(tableRef, convey.ShouldNotBeNil)

		pkd := tcc.GetPrimaryKeyDef("abc", "t1")
		convey.So(len(pkd), convey.ShouldBeZeroValue)

		hkd := tcc.GetHideKeyDef("abc", "t1")
		convey.So(hkd, convey.ShouldBeNil)

		cost := tcc.Cost(&plan2.ObjectRef{SchemaName: "abc", ObjName: "t1"}, &plan2.Expr{})
		convey.So(cost, convey.ShouldNotBeNil)
	})
}
