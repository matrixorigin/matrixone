package frontend

import (
	"github.com/golang/mock/gomock"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
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
}
