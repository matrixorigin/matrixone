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

package preinsertunique

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type preinsertuniqueTestCase struct {
	arg *PreInsertUnique
}

var (
	tcs []preinsertuniqueTestCase
)

func init() {
	tcs = []preinsertuniqueTestCase{
		{
			arg: &PreInsertUnique{
				ctr: container{},
				PreInsertCtx: &plan.PreInsertUkCtx{
					Columns:  []int32{1},
					PkColumn: 0,
					PkType:   plan.Type{Id: int32(types.T_uint64), Width: types.T_int64.ToType().Width, Scale: -1},
					UkType:   plan.Type{Id: int32(types.T_uint64), Width: types.T_int64.ToType().Width, Scale: -1},
				},
				OperatorBase: vm.OperatorBase{
					OperatorInfo: vm.OperatorInfo{
						Idx:     0,
						IsFirst: false,
						IsLast:  false,
					},
				},
			},
		},
		{
			arg: &PreInsertUnique{
				ctr: container{},
				PreInsertCtx: &plan.PreInsertUkCtx{
					Columns:  []int32{1, 2},
					PkColumn: 0,
					PkType:   plan.Type{Id: int32(types.T_uint64), Width: types.T_int64.ToType().Width, Scale: -1},
					UkType:   plan.Type{Id: int32(types.T_uint64), Width: types.T_int64.ToType().Width, Scale: -1},
				},
				OperatorBase: vm.OperatorBase{
					OperatorInfo: vm.OperatorInfo{
						Idx:     0,
						IsFirst: false,
						IsLast:  false,
					},
				},
			},
		},
	}
}

func TestPreInsertUnique(t *testing.T) {
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

	proc := testutil.NewProc(t)
	proc.Base.TxnClient = txnClient
	proc.Ctx = ctx
	// create table t1(
	// col1 int primary key,
	// col2 int unique key,
	// col3 int
	// );
	// (1, 11, 23)
	// (2, 22, 23)
	// (3, 33, 23)

	var err error
	for _, tc := range tcs {
		resetChildren(tc.arg, proc.Mp())
		err = tc.arg.Prepare(proc)
		require.NoError(t, err)
		_, err = vm.Exec(tc.arg, proc)
		require.NoError(t, err)
		tc.arg.Reset(proc, false, nil)
		resetChildren(tc.arg, proc.Mp())
		err = tc.arg.Prepare(proc)
		require.NoError(t, err)
		_, err = vm.Exec(tc.arg, proc)
		require.NoError(t, err)
		tc.arg.Free(proc, false, nil)
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}

}

func TestPreInsertUniqueSingleVarcharUsesSizedUkType(t *testing.T) {
	proc := testutil.NewProc(t)
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(1), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("CODE001"), false, proc.Mp()))
	bat.SetRowCount(1)
	defer bat.Clean(proc.Mp())

	arg := &PreInsertUnique{
		PreInsertCtx: &plan.PreInsertUkCtx{
			Columns:  []int32{1},
			PkColumn: 0,
			UkType: plan.Type{
				Id:    int32(types.T_varchar),
				Width: 50,
			},
		},
	}
	arg.initBuf(bat, arg.PreInsertCtx.Columns, int(arg.PreInsertCtx.PkColumn), false)
	defer arg.Free(proc, false, nil)

	require.NotZero(t, arg.ctr.buf.Vecs[indexColPos].GetType().TypeSize())
	_, err := util.CompactSingleIndexCol(bat.Vecs[1], arg.ctr.buf.Vecs[indexColPos], proc)
	require.NoError(t, err)
	require.Equal(t, 1, arg.ctr.buf.Vecs[indexColPos].Length())
}

func TestPreInsertUniqueUpdateKeepsCompactedRowsAligned(t *testing.T) {
	testCases := []struct {
		name         string
		indexColumns [][]int64
		nullRows     [][]bool
		expectedRows []int
	}{
		{
			name:         "single column mixed nulls",
			indexColumns: [][]int64{{100, 200, 300, 400}},
			nullRows:     [][]bool{{true, false, true, false}},
			expectedRows: []int{1, 3},
		},
		{
			name: "composite key excludes a row when any part is null",
			indexColumns: [][]int64{
				{100, 200, 300, 400},
				{101, 201, 301, 401},
			},
			nullRows: [][]bool{
				{true, false, false, false},
				{false, false, true, false},
			},
			expectedRows: []int{1, 3},
		},
		{
			name:         "all rows survive",
			indexColumns: [][]int64{{100, 200, 300, 400}},
			nullRows:     [][]bool{{false, false, false, false}},
			expectedRows: []int{0, 1, 2, 3},
		},
		{
			name:         "all rows filtered",
			indexColumns: [][]int64{{100, 200, 300, 400}},
			nullRows:     [][]bool{{true, true, true, true}},
			expectedRows: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProc(t)
			pks := []int64{10, 20, 30, 40}
			rowIDs := []types.Rowid{
				types.BuildTestRowid(1, 1),
				types.BuildTestRowid(1, 2),
				types.BuildTestRowid(1, 3),
				types.BuildTestRowid(1, 4),
			}
			input := batch.NewWithSize(len(tc.indexColumns) + 2)
			input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			require.NoError(t, vector.AppendFixedList(input.Vecs[0], pks, nil, proc.Mp()))
			for colIdx, values := range tc.indexColumns {
				input.Vecs[colIdx+1] = vector.NewVec(types.T_int64.ToType())
				for rowIdx, value := range values {
					require.NoError(t, vector.AppendFixed(
						input.Vecs[colIdx+1], value, tc.nullRows[colIdx][rowIdx], proc.Mp()))
				}
			}
			input.Vecs[len(input.Vecs)-1] = vector.NewVec(types.T_Rowid.ToType())
			require.NoError(t, vector.AppendFixedList(input.Vecs[len(input.Vecs)-1], rowIDs, nil, proc.Mp()))
			input.SetRowCount(len(pks))
			defer input.Clean(proc.Mp())

			indexPositions := make([]int32, len(tc.indexColumns))
			for i := range indexPositions {
				indexPositions[i] = int32(i + 1)
			}
			arg := &PreInsertUnique{
				PreInsertCtx: &plan.PreInsertUkCtx{
					Columns:  indexPositions,
					PkColumn: 0,
					UkType:   plan.Type{Id: int32(types.T_int64)},
				},
			}
			arg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
			require.NoError(t, arg.Prepare(proc))
			defer arg.Free(proc, false, nil)

			result, err := arg.Call(proc)
			require.NoError(t, err)
			require.Equal(t, len(tc.expectedRows), result.Batch.RowCount())
			for _, vec := range result.Batch.Vecs {
				require.Equal(t, result.Batch.RowCount(), vec.Length())
			}

			var expectedPKs []int64
			var expectedRowIDs []types.Rowid
			for _, inputRow := range tc.expectedRows {
				expectedPKs = append(expectedPKs, pks[inputRow])
				expectedRowIDs = append(expectedRowIDs, rowIDs[inputRow])
			}
			require.Equal(t, expectedPKs,
				vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[pkColPos]))
			require.Equal(t, expectedRowIDs,
				vector.MustFixedColNoTypeCheck[types.Rowid](result.Batch.Vecs[rowIdColPos]))
		})
	}
}

func TestInsertIgnoreMultiDedupArbitratesWholeRowsInInputOrder(t *testing.T) {
	testCases := []struct {
		name        string
		ids         []int32
		uniqueKeys  []int32
		uniqueNulls []bool
		pkConflicts []bool
		ukConflicts []bool
		wantIDs     []int32
		wantKeys    []int32
	}{
		{
			name:        "existing unique conflict does not reserve primary key",
			ids:         []int32{3, 3},
			uniqueKeys:  []int32{20, 30},
			pkConflicts: []bool{false, false},
			ukConflicts: []bool{true, false},
			wantIDs:     []int32{3},
			wantKeys:    []int32{30},
		},
		{
			name:        "primary conflict loser does not reserve unique key",
			ids:         []int32{1, 1, 2},
			uniqueKeys:  []int32{10, 20, 20},
			pkConflicts: []bool{false, false, false},
			ukConflicts: []bool{false, false, false},
			wantIDs:     []int32{1, 2},
			wantKeys:    []int32{10, 20},
		},
		{
			name:        "unique conflict loser does not reserve primary key",
			ids:         []int32{1, 2, 2},
			uniqueKeys:  []int32{10, 10, 20},
			pkConflicts: []bool{false, false, false},
			ukConflicts: []bool{false, false, false},
			wantIDs:     []int32{1, 2},
			wantKeys:    []int32{10, 20},
		},
		{
			name:        "nullable unique keys do not conflict",
			ids:         []int32{1, 2},
			uniqueKeys:  []int32{0, 0},
			uniqueNulls: []bool{true, true},
			pkConflicts: []bool{false, false},
			ukConflicts: []bool{false, false},
			wantIDs:     []int32{1, 2},
			wantKeys:    []int32{0, 0},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProc(t)
			input := makeInsertIgnoreMultiDedupBatch(
				t, proc, tc.ids, tc.uniqueKeys, tc.uniqueNulls, tc.pkConflicts, tc.ukConflicts)
			arg := newInsertIgnoreMultiDedupArgument(input)
			require.NoError(t, arg.Prepare(proc))

			result, err := arg.Call(proc)
			require.NoError(t, err)
			require.Equal(t, tc.wantIDs,
				vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])[:result.Batch.RowCount()])
			require.Equal(t, tc.wantKeys,
				vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[1])[:result.Batch.RowCount()])

			arg.Free(proc, false, nil)
			input.Clean(proc.Mp())
			require.Equal(t, int64(0), proc.Mp().CurrNB())
		})
	}
}

func TestInsertIgnoreMultiDedupCarriesAcceptedKeysAcrossBatchesAndReset(t *testing.T) {
	proc := testutil.NewProc(t)
	first := makeInsertIgnoreMultiDedupBatch(t, proc,
		[]int32{1}, []int32{10}, nil, []bool{false}, []bool{false})
	second := makeInsertIgnoreMultiDedupBatch(t, proc,
		[]int32{1, 2}, []int32{20, 20}, nil, []bool{false, false}, []bool{false, false})
	arg := newInsertIgnoreMultiDedupArgument(first, second)
	require.NoError(t, arg.Prepare(proc))

	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, []int32{1},
		vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])[:result.Batch.RowCount()])
	result, err = arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, []int32{2},
		vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])[:result.Batch.RowCount()])
	require.Equal(t, []int32{20},
		vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[1])[:result.Batch.RowCount()])

	arg.Reset(proc, false, nil)
	arg.Children = nil
	arg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{second}))
	require.NoError(t, arg.Prepare(proc))
	result, err = arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, []int32{1},
		vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])[:result.Batch.RowCount()])

	arg.Free(proc, false, nil)
	first.Clean(proc.Mp())
	second.Clean(proc.Mp())
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func makeInsertIgnoreMultiDedupBatch(
	t *testing.T,
	proc *process.Process,
	ids, uniqueKeys []int32,
	uniqueNulls, pkConflicts, ukConflicts []bool,
) *batch.Batch {
	t.Helper()
	input := batch.NewWithSize(4)
	input.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	input.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	input.Vecs[2] = vector.NewVec(types.T_bool.ToType())
	input.Vecs[3] = vector.NewVec(types.T_bool.ToType())
	for row := range ids {
		nullUnique := len(uniqueNulls) > row && uniqueNulls[row]
		require.NoError(t, vector.AppendFixed(input.Vecs[0], ids[row], false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(input.Vecs[1], uniqueKeys[row], nullUnique, proc.Mp()))
		require.NoError(t, vector.AppendFixed(input.Vecs[2], pkConflicts[row], false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(input.Vecs[3], ukConflicts[row], false, proc.Mp()))
	}
	input.SetRowCount(len(ids))
	return input
}

func newInsertIgnoreMultiDedupArgument(inputs ...*batch.Batch) *PreInsertUnique {
	arg := &PreInsertUnique{
		PreInsertCtx: &plan.PreInsertUkCtx{
			InsertIgnoreMultiDedup: true,
			KeyColumns:             []int32{0, 1},
			ConflictColumns:        []int32{2, 3},
			OutputColumns:          2,
		},
	}
	arg.AppendChild(colexec.NewMockOperator().WithBatchs(inputs))
	return arg
}

func resetChildren(arg *PreInsertUnique, m *mpool.MPool) {
	bat := colexec.MakeMockBatchs(m)
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}
