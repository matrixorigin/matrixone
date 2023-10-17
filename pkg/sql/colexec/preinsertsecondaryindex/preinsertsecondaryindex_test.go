// Copyright 2022 Matrix Origin
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

package preinsertsecondaryindex

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestPreInsertSecondaryIndex(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.TODO()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()

	proc := testutil.NewProc()
	proc.TxnClient = txnClient
	proc.Ctx = ctx
	// create table t1(
	// col1 int primary key,
	// col2 int key,
	// col3 int
	// );
	// (1, 11, 23)
	// (2, 22, 23)
	// (3, 33, 23)
	inputBatch := &batch.Batch{
		Vecs: []*vector.Vector{
			testutil.MakeInt64Vector([]int64{1, 2, 3}, nil),
			testutil.MakeInt64Vector([]int64{11, 22, 33}, nil),
			testutil.MakeInt64Vector([]int64{23, 23, 23}, nil),
		},
		Cnt: 1,
	}
	inputBatch.SetRowCount(3)

	argument := Argument{
		PreInsertCtx: &plan.PreInsertUkCtx{
			Columns:  []int32{1, 0}, //NOTE: columns is always >=2 for PreInsertSK since PK will be appended to SK list.
			PkColumn: 0,
			PkType:   &plan.Type{Id: int32(types.T_uint64), Width: types.T_int64.ToType().Width, Scale: -1},
			UkType:   &plan.Type{Id: int32(types.T_uint64), Width: types.T_int64.ToType().Width, Scale: -1},
		},
	}

	types.T_int64.ToType()
	proc.Reg.InputBatch = inputBatch
	_, err := Call(0, proc, &argument, false, false)
	require.NoError(t, err)
}
