package frontend

import (
	"errors"
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
