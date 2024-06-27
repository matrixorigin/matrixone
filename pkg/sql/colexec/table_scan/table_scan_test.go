// Copyright 2024 Matrix Origin
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

package table_scan

import (
	"bytes"
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	arg := &Argument{}
	arg.String(buf)
}

func TestPrepare(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	reader := mock_frontend.NewMockReader(ctrl)
	reader.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, attrs []string, expr *plan.Expr, b, c interface{}) (*batch.Batch, error) {
		bat := batch.NewWithSize(3)
		bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_uint64.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_varchar.ToType())

		err := vector.AppendFixed(bat.GetVector(0), types.Rowid([types.RowidSize]byte{}), false, testutil.TestUtilMp)
		if err != nil {
			require.Nil(t, err)
		}

		err = vector.AppendFixed(bat.GetVector(1), uint64(272464), false, testutil.TestUtilMp)
		if err != nil {
			require.Nil(t, err)
		}

		err = vector.AppendBytes(bat.GetVector(2), []byte("empno"), false, testutil.TestUtilMp)
		if err != nil {
			require.Nil(t, err)
		}
		bat.SetRowCount(bat.GetVector(1).Length())
		return bat, nil
	}).AnyTimes()
	reader.EXPECT().Close().Return(nil).AnyTimes()
	reader.EXPECT().GetOrderBy().Return(nil).AnyTimes()
	arg := &Argument{
		Reader: reader,
	}
	proc := testutil.NewProc()
	err := arg.Prepare(proc)
	require.NoError(t, err)
}

func TestCall(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.TODO()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	txnOperator.EXPECT().ResetRetry(gomock.Any()).AnyTimes()
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
	txnOperator.EXPECT().NextSequence().Return(uint64(0)).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	proc := testutil.NewProc()
	proc.Base.TxnClient = txnClient
	proc.Ctx = ctx
	proc.Base.TxnOperator = txnOperator

	reader := mock_frontend.NewMockReader(ctrl)
	reader.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, attrs []string, expr *plan.Expr, b, c interface{}) (*batch.Batch, error) {
		bat := batch.NewWithSize(3)
		bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_uint64.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_varchar.ToType())

		err := vector.AppendFixed(bat.GetVector(0), types.Rowid([types.RowidSize]byte{}), false, testutil.TestUtilMp)
		if err != nil {
			require.Nil(t, err)
		}

		err = vector.AppendFixed(bat.GetVector(1), uint64(272464), false, testutil.TestUtilMp)
		if err != nil {
			require.Nil(t, err)
		}

		err = vector.AppendBytes(bat.GetVector(2), []byte("empno"), false, testutil.TestUtilMp)
		if err != nil {
			require.Nil(t, err)
		}
		bat.SetRowCount(bat.GetVector(1).Length())
		return bat, nil
	}).AnyTimes()
	reader.EXPECT().Close().Return(nil).AnyTimes()
	reader.EXPECT().GetOrderBy().Return(nil).AnyTimes()
	arg := &Argument{
		Reader: reader,
	}

	err := arg.Prepare(proc)
	require.NoError(t, err)
	_, err = arg.Call(proc)
	require.NoError(t, err)
	arg.Free(proc, false, nil)
	proc.FreeVectors()
	require.Equal(t, int64(0), proc.GetMPool().CurrNB())
}
