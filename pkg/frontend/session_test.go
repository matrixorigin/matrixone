package frontend

import (
	"errors"
	"github.com/fagongzi/goetty/buf"
	"github.com/golang/mock/gomock"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"
	"github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestTxnHandler(t *testing.T) {
	convey.Convey("init", t, func() {
		txn := InitTxnHandler(nil)
		convey.So(txn.IsInTaeTxn(), convey.ShouldBeFalse)
		convey.So(txn.IsTaeEngine(), convey.ShouldBeFalse)
		convey.So(txn.isTxnState(TxnInit), convey.ShouldBeTrue)
		convey.So(txn.GetStorage(), convey.ShouldBeNil)
	})

	convey.Convey("aoe begin  end", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		aoe := mock_frontend.NewMockEngine(ctrl)
		{
			txn := InitTxnHandler(aoe)
			convey.So(txn.IsInTaeTxn(), convey.ShouldBeFalse)
			convey.So(txn.IsTaeEngine(), convey.ShouldBeFalse)
			convey.So(txn.isTxnState(TxnInit), convey.ShouldBeTrue)
			convey.So(txn.GetStorage(), convey.ShouldNotBeNil)
		}
		{
			txn := InitTxnHandler(aoe)
			err := txn.StartByBegin()
			convey.So(err, convey.ShouldBeNil)

			err = txn.StartByBegin()
			convey.So(err, convey.ShouldBeNil)

			err = txn.CommitAfterBegin()
			convey.So(err, convey.ShouldBeNil)
		}
	})

	convey.Convey("tae begin end", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		txnImpl := mock_frontend.NewMockTxn(ctrl)
		txnImpl.EXPECT().Commit().Return(nil)

		tae := mock_frontend.NewMockTxnEngine(ctrl)
		tae.EXPECT().StartTxn(gomock.Any()).Return(txnImpl, nil)
		txn := InitTxnHandler(tae)
		convey.So(txn.IsInTaeTxn(), convey.ShouldBeFalse)
		convey.So(txn.IsTaeEngine(), convey.ShouldBeTrue)
		convey.So(txn.isTxnState(TxnInit), convey.ShouldBeTrue)
		convey.So(txn.GetStorage(), convey.ShouldNotBeNil)
		convey.So(txn.CleanTxn(), convey.ShouldBeNil)
		convey.So(txn.StartByBegin(), convey.ShouldBeNil)
		convey.So(txn.CommitAfterBegin(), convey.ShouldBeNil)
	})

	convey.Convey("tae begin ... begin/autocommit", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		tae := mock_frontend.NewMockTxnEngine(ctrl)
		txn := InitTxnHandler(tae)
		txnImpl := mock_frontend.NewMockTxn(ctrl)
		txnImpl.EXPECT().GetError().Return(nil).AnyTimes()

		tae.EXPECT().StartTxn(gomock.Any()).Return(txnImpl, nil).AnyTimes()
		err := txn.StartByBegin()
		convey.So(err, convey.ShouldBeNil)

		err = txn.StartByBegin()
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(txn.getTxnState(), convey.ShouldEqual, TxnErr)

		err = txn.StartByAutocommit()
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(txn.getTxnState(), convey.ShouldEqual, TxnErr)

		err = txn.CleanTxn()
		convey.So(err, convey.ShouldBeNil)
		convey.So(txn.getTxnState(), convey.ShouldEqual, TxnInit)
	})

	convey.Convey("tae begin ... commit ... commit", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		tae := mock_frontend.NewMockTxnEngine(ctrl)
		txn := InitTxnHandler(tae)
		txnImpl := mock_frontend.NewMockTxn(ctrl)
		txnImpl.EXPECT().GetError().Return(nil).AnyTimes()
		txnImpl.EXPECT().Commit().Return(nil).AnyTimes()

		tae.EXPECT().StartTxn(gomock.Any()).Return(txnImpl, nil).AnyTimes()
		err := txn.StartByBegin()
		convey.So(err, convey.ShouldBeNil)

		err = txn.CommitAfterBegin()
		convey.So(err, convey.ShouldBeNil)
		convey.So(txn.getTxnState(), convey.ShouldEqual, TxnEnd)

		err = txn.CommitAfterBegin()
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(txn.getTxnState(), convey.ShouldEqual, TxnErr)

		err = txn.CleanTxn()
		convey.So(err, convey.ShouldBeNil)
		convey.So(txn.getTxnState(), convey.ShouldEqual, TxnInit)
	})

	convey.Convey("tae begin ... rollback ... rollback", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		tae := mock_frontend.NewMockTxnEngine(ctrl)
		txn := InitTxnHandler(tae)
		txnImpl := mock_frontend.NewMockTxn(ctrl)
		txnImpl.EXPECT().GetError().Return(nil).AnyTimes()
		txnImpl.EXPECT().Commit().Return(nil).AnyTimes()
		txnImpl.EXPECT().Rollback().Return(nil).AnyTimes()

		tae.EXPECT().StartTxn(gomock.Any()).Return(txnImpl, nil).AnyTimes()
		err := txn.StartByBegin()
		convey.So(err, convey.ShouldBeNil)

		err = txn.Rollback()
		convey.So(err, convey.ShouldBeNil)
		convey.So(txn.getTxnState(), convey.ShouldEqual, TxnEnd)

		err = txn.Rollback()
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(txn.getTxnState(), convey.ShouldEqual, TxnErr)

		err = txn.CleanTxn()
		convey.So(err, convey.ShouldBeNil)
		convey.So(txn.getTxnState(), convey.ShouldEqual, TxnInit)
	})

	convey.Convey("tae begin ... clean ... clean", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		tae := mock_frontend.NewMockTxnEngine(ctrl)
		txn := InitTxnHandler(tae)
		txnImpl := mock_frontend.NewMockTxn(ctrl)
		txnImpl.EXPECT().GetError().Return(nil).AnyTimes()
		txnImpl.EXPECT().Commit().Return(nil).AnyTimes()
		txnImpl.EXPECT().Rollback().Return(nil).AnyTimes()

		tae.EXPECT().StartTxn(gomock.Any()).Return(txnImpl, nil).AnyTimes()
		err := txn.StartByBegin()
		convey.So(err, convey.ShouldBeNil)

		err = txn.CleanTxn()
		convey.So(err, convey.ShouldBeNil)
		convey.So(txn.getTxnState(), convey.ShouldEqual, TxnBegan)

		err = txn.CleanTxn()
		convey.So(err, convey.ShouldBeNil)
		convey.So(txn.getTxnState(), convey.ShouldEqual, TxnBegan)
	})
}

func TestTxnHandler_StartByBegin(t *testing.T) {
	convey.Convey("storage is nil", t, func() {
		txn := InitTxnHandler(nil)
		err := txn.StartByBegin()
		convey.So(err, convey.ShouldBeNil)
		convey.So(txn.txnState.isState(TxnBegan), convey.ShouldBeTrue)
		convey.So(txn.taeTxn, convey.ShouldNotBeNil)
	})

	convey.Convey("storage is not TxnEngine", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		storage := mock_frontend.NewMockEngine(ctrl)
		txn := InitTxnHandler(storage)
		err := txn.StartByBegin()
		convey.So(err, convey.ShouldBeNil)
		convey.So(txn.txnState.isState(TxnBegan), convey.ShouldBeTrue)
		convey.So(txn.taeTxn, convey.ShouldNotBeNil)
	})

	convey.Convey("storage is TxnEngine, from Init,End", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		taeTxn := mock_frontend.NewMockTxn(ctrl)
		taeTxn.EXPECT().String().Return("").AnyTimes()
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
		err := txn.StartByBegin()
		convey.So(err, convey.ShouldBeNil)
		convey.So(txn.txnState.isState(TxnBegan), convey.ShouldBeTrue)
		convey.So(txn.taeTxn, convey.ShouldNotBeNil)

		txn2 := InitTxnHandler(storage)
		err = txn2.StartByBegin()
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(txn2.txnState.isState(TxnErr), convey.ShouldBeTrue)
		convey.So(txn2.taeTxn, convey.ShouldNotBeNil)

		txn3 := InitTxnHandler(storage)
		txn3.txnState.switchToState(TxnEnd, nil)
		err = txn3.StartByBegin()
		convey.So(err, convey.ShouldBeNil)
		convey.So(txn3.txnState.isState(TxnBegan), convey.ShouldBeTrue)
		convey.So(txn3.taeTxn, convey.ShouldNotBeNil)

		txn4 := InitTxnHandler(storage)
		txn4.txnState.switchToState(TxnEnd, nil)
		err = txn4.StartByBegin()
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(txn4.txnState.isState(TxnErr), convey.ShouldBeTrue)
		convey.So(txn4.taeTxn, convey.ShouldNotBeNil)
	})

	convey.Convey("storage is TxnEngine, from Began,Autocommit,Err", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		taeTxn := mock_frontend.NewMockTxn(ctrl)
		taeTxn.EXPECT().String().Return("").AnyTimes()
		storage := mock_frontend.NewMockTxnEngine(ctrl)

		txn := InitTxnHandler(storage)
		txn.txnState.switchToState(TxnBegan, nil)
		err := txn.StartByBegin()
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(txn.txnState.isState(TxnErr), convey.ShouldBeTrue)
		convey.So(txn.taeTxn, convey.ShouldNotBeNil)

		txn2 := InitTxnHandler(storage)
		txn2.txnState.switchToState(TxnAutocommit, nil)
		err = txn2.StartByBegin()
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(txn2.txnState.isState(TxnErr), convey.ShouldBeTrue)
		convey.So(txn2.taeTxn, convey.ShouldNotBeNil)

		txn3 := InitTxnHandler(storage)
		txn3.txnState.switchToState(TxnErr, nil)
		err = txn3.StartByBegin()
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(txn3.txnState.isState(TxnErr), convey.ShouldBeTrue)
		convey.So(txn3.taeTxn, convey.ShouldNotBeNil)
	})
}

func TestTxnHandler_StartByAutocommit(t *testing.T) {
	convey.Convey("storage is nil", t, func() {
		txn := InitTxnHandler(nil)
		err := txn.StartByAutocommit()
		convey.So(err, convey.ShouldBeNil)
		convey.So(txn.txnState.isState(TxnAutocommit), convey.ShouldBeTrue)
		convey.So(txn.taeTxn, convey.ShouldNotBeNil)
	})

	convey.Convey("storage is not TxnEngine", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		storage := mock_frontend.NewMockEngine(ctrl)
		txn := InitTxnHandler(storage)
		err := txn.StartByAutocommit()
		convey.So(err, convey.ShouldBeNil)
		convey.So(txn.txnState.isState(TxnAutocommit), convey.ShouldBeTrue)
		convey.So(txn.taeTxn, convey.ShouldNotBeNil)
	})

	convey.Convey("storage is TxnEngine, from Init,End", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		taeTxn := mock_frontend.NewMockTxn(ctrl)
		taeTxn.EXPECT().String().Return("").AnyTimes()
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
		err := txn.StartByAutocommit()
		convey.So(err, convey.ShouldBeNil)
		convey.So(txn.txnState.isState(TxnAutocommit), convey.ShouldBeTrue)
		convey.So(txn.taeTxn, convey.ShouldNotBeNil)

		txn2 := InitTxnHandler(storage)
		err = txn2.StartByAutocommit()
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(txn2.txnState.isState(TxnErr), convey.ShouldBeTrue)
		convey.So(txn2.taeTxn, convey.ShouldNotBeNil)

		txn3 := InitTxnHandler(storage)
		txn3.txnState.switchToState(TxnEnd, nil)
		err = txn3.StartByAutocommit()
		convey.So(err, convey.ShouldBeNil)
		convey.So(txn3.txnState.isState(TxnAutocommit), convey.ShouldBeTrue)
		convey.So(txn3.taeTxn, convey.ShouldNotBeNil)

		txn4 := InitTxnHandler(storage)
		txn4.txnState.switchToState(TxnEnd, nil)
		err = txn4.StartByAutocommit()
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(txn4.txnState.isState(TxnErr), convey.ShouldBeTrue)
		convey.So(txn4.taeTxn, convey.ShouldNotBeNil)
	})

	convey.Convey("storage is TxnEngine, from Began,Autocommit,Err", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		taeTxn := mock_frontend.NewMockTxn(ctrl)
		taeTxn.EXPECT().String().Return("").AnyTimes()
		storage := mock_frontend.NewMockTxnEngine(ctrl)

		txn := InitTxnHandler(storage)
		txn.txnState.switchToState(TxnBegan, nil)
		err := txn.StartByAutocommit()
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(txn.txnState.isState(TxnErr), convey.ShouldBeTrue)
		convey.So(txn.taeTxn, convey.ShouldNotBeNil)

		txn2 := InitTxnHandler(storage)
		txn2.txnState.switchToState(TxnAutocommit, nil)
		err = txn2.StartByAutocommit()
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(txn2.txnState.isState(TxnErr), convey.ShouldBeTrue)
		convey.So(txn2.taeTxn, convey.ShouldNotBeNil)

		txn3 := InitTxnHandler(storage)
		txn3.txnState.switchToState(TxnErr, nil)
		err = txn3.StartByAutocommit()
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(txn3.txnState.isState(TxnErr), convey.ShouldBeTrue)
		convey.So(txn3.taeTxn, convey.ShouldNotBeNil)
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
}
