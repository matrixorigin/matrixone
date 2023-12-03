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
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/smartystreets/goconvey/convey"
)

func TestTxnHandler_TxnOpenLog(t *testing.T) {
	convey.Convey("txn open log", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ctx := context.TODO()
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
