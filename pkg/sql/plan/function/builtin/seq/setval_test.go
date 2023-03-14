// Copyright 2021 - 2022 Matrix Origin
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

package seq

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestSetVal(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProc()

	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New().Return(txnOperator, nil).AnyTimes()

	db := mock_frontend.NewMockDatabase(ctrl)
	db.EXPECT().Relations(gomock.Any()).Return(nil, nil).AnyTimes()

	reader := mock_frontend.NewMockReader(ctrl)
	reader.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, attrs []string, b, c interface{}) (*batch.Batch, error) {
		bat := batch.NewWithSize(8)
		bat.Zs = []int64{1}
		// Last_seq_num
		bat.Vecs[0] = vector.NewConstFixed(types.T_int64.ToType(), 1, int64(20), testutil.TestUtilMp)
		// min_value
		bat.Vecs[1] = vector.NewConstFixed(types.T_int64.ToType(), 1, int64(1), testutil.TestUtilMp)
		// max_value
		bat.Vecs[2] = vector.NewConstFixed(types.T_int64.ToType(), 1, int64(10000), testutil.TestUtilMp)
		// start_value
		bat.Vecs[3] = vector.NewConstFixed(types.T_int64.ToType(), 1, int64(5), testutil.TestUtilMp)
		// increment_value
		bat.Vecs[4] = vector.NewConstFixed(types.T_int64.ToType(), 1, int64(1), testutil.TestUtilMp)
		// cycle
		bat.Vecs[5] = vector.NewConstFixed(types.T_bool.ToType(), 1, false, testutil.TestUtilMp)
		// is_called
		bat.Vecs[6] = vector.NewConstFixed(types.T_bool.ToType(), 1, false, testutil.TestUtilMp)
		// row_id just 30
		bat.Vecs[7] = vector.NewConstFixed(types.T_Rowid.ToType(), 1, types.BuildRowid(1, 2), testutil.TestUtilMp)
		//err = bat.Vecs[3].Append(int64(1), false, testutil.TestUtilMp)
		// if err != nil {
		// require.Nil(t, err)
		// }
		return bat, nil
	}).AnyTimes()

	table := mock_frontend.NewMockRelation(ctrl)
	table.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	table.EXPECT().NewReader(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]engine.Reader{reader}, nil).AnyTimes()
	table.EXPECT().GetTableID(gomock.Any()).Return(uint64(10)).AnyTimes()
	table.EXPECT().Rows(gomock.Any()).Return(int64(10), nil).AnyTimes()
	table.EXPECT().Size(gomock.Any(), gomock.Any()).Return(int64(0), nil).AnyTimes()
	table.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	table.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	db.EXPECT().Relation(gomock.Any(), gomock.Any()).Return(table, nil).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Commit(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Rollback(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()
	eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(db, nil).AnyTimes()
	proc.Ctx = context.WithValue(proc.Ctx, defines.EngineKey{}, eng)
	proc.TxnClient = txnClient

	tests := []struct {
		name    string
		vectors []*vector.Vector
	}{
		{
			name: "test01",
			vectors: []*vector.Vector{
				testutil.MakeVarcharVector([]string{"t1"}, []uint64{}),
				testutil.MakeVarcharVector([]string{"100"}, []uint64{}),
				testutil.MakeBoolVector([]bool{false}),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := Setval(tt.vectors, proc)
			if err != nil {
				t.Errorf("Setval() error = %v", err)
				return
			}
			require.Nil(t, err)
			require.Equal(t, "100", r.String())
		})
	}
}
