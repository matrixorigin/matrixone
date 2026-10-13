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

func TestInsertIgnoreReordersGeneratedAutoIncrementCandidates(t *testing.T) {
	proc := testutil.NewProc(t)
	input := makeInsertIgnoreAutoIncrementBatch(t, proc,
		[]int32{2, 3, 4}, []int32{20, 10, 30},
		[]bool{false, false, false}, []bool{false, true, false},
		[]bool{true, true, true})
	arg := newInsertIgnoreAutoIncrementArgument(input)
	require.NoError(t, arg.Prepare(proc))

	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, []int32{2, 3},
		vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])[:result.Batch.RowCount()],
		"the ignored row's candidate must be reused by the next accepted row")
	require.Equal(t, []int32{20, 30},
		vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[1])[:result.Batch.RowCount()])

	arg.Free(proc, false, nil)
	input.Clean(proc.Mp())
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestInsertIgnoreReorderCarriesCandidatesAcrossBatchesAndHonorsExplicitKeys(t *testing.T) {
	proc := testutil.NewProc(t)
	first := makeInsertIgnoreAutoIncrementBatch(t, proc,
		[]int32{2, 3}, []int32{10, 20},
		[]bool{false, false}, []bool{true, true},
		[]bool{true, true})
	second := makeInsertIgnoreAutoIncrementBatch(t, proc,
		[]int32{2, 4}, []int32{25, 30},
		[]bool{false, false}, []bool{false, false},
		[]bool{false, true})
	arg := newInsertIgnoreAutoIncrementArgument(first, second)
	require.NoError(t, arg.Prepare(proc))

	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.True(t, result.Batch.IsEmpty(), "the first batch only contributes reusable candidates")
	result, err = arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, []int32{2, 3},
		vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])[:result.Batch.RowCount()],
		"a pending candidate colliding with an accepted explicit key must be skipped")
	require.Equal(t, []int32{25, 30},
		vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[1])[:result.Batch.RowCount()])

	arg.Free(proc, false, nil)
	first.Clean(proc.Mp())
	second.Clean(proc.Mp())
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestInsertIgnoreReorderPreservesEarlierGeneratedOwnership(t *testing.T) {
	proc := testutil.NewProc(t)
	// The first batch contributes only reusable candidates. The generated row
	// in the next batch must consume candidate 2 before the later explicit row
	// with id=2 is considered; otherwise the explicit row would steal the
	// recycled key and the generated row would incorrectly become id=3.
	first := makeInsertIgnoreAutoIncrementBatch(t, proc,
		[]int32{2, 3}, []int32{10, 10},
		[]bool{false, false}, []bool{true, true},
		[]bool{true, true})
	second := makeInsertIgnoreAutoIncrementBatch(t, proc,
		[]int32{4, 2}, []int32{20, 30},
		[]bool{false, false}, []bool{false, false},
		[]bool{true, false})
	arg := newInsertIgnoreAutoIncrementArgument(first, second)
	require.NoError(t, arg.Prepare(proc))

	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.True(t, result.Batch.IsEmpty())
	result, err = arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, []int32{2},
		vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])[:result.Batch.RowCount()])
	require.Equal(t, []int32{20},
		vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[1])[:result.Batch.RowCount()])
	require.Equal(t, uint64(2), proc.GetStatementLastInsertID())

	arg.Free(proc, false, nil)
	first.Clean(proc.Mp())
	second.Clean(proc.Mp())
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestInsertIgnoreReorderAdvancesPastExplicitKeyInSameBatch(t *testing.T) {
	proc := testutil.NewProc(t)
	input := makeInsertIgnoreAutoIncrementBatch(t, proc,
		[]int32{2, 100, 101}, []int32{10, 20, 30},
		[]bool{false, false, false}, []bool{true, false, false},
		[]bool{true, false, true})
	arg := newInsertIgnoreAutoIncrementArgument(input)
	require.NoError(t, arg.Prepare(proc))

	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, []int32{100, 101},
		vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])[:result.Batch.RowCount()],
		"an accepted explicit high key must invalidate older recycled candidates")
	require.Equal(t, []int32{20, 30},
		vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[1])[:result.Batch.RowCount()])
	require.Equal(t, uint64(101), proc.GetStatementLastInsertID())

	arg.Free(proc, false, nil)
	input.Clean(proc.Mp())
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestInsertIgnoreReorderAdvancesPastExplicitKeyAcrossBatches(t *testing.T) {
	proc := testutil.NewProc(t)
	first := makeInsertIgnoreAutoIncrementBatch(t, proc,
		[]int32{2}, []int32{10},
		[]bool{false}, []bool{true}, []bool{true})
	second := makeInsertIgnoreAutoIncrementBatch(t, proc,
		[]int32{100, 101}, []int32{20, 30},
		[]bool{false, false}, []bool{false, false}, []bool{false, true})
	arg := newInsertIgnoreAutoIncrementArgument(first, second)
	require.NoError(t, arg.Prepare(proc))

	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.True(t, result.Batch.IsEmpty())
	result, err = arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, []int32{100, 101},
		vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])[:result.Batch.RowCount()],
		"the explicit bound must apply to candidates retained by a prior batch")
	require.Equal(t, []int32{20, 30},
		vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[1])[:result.Batch.RowCount()])
	require.Equal(t, uint64(101), proc.GetStatementLastInsertID())

	arg.Free(proc, false, nil)
	first.Clean(proc.Mp())
	second.Clean(proc.Mp())
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestInsertIgnoreReorderDoesNotAdvancePastNegativeExplicitKey(t *testing.T) {
	proc := testutil.NewProc(t)
	input := makeInsertIgnoreAutoIncrementBatch(t, proc,
		[]int32{2, -1, 3}, []int32{10, 20, 30},
		[]bool{false, false, false}, []bool{true, false, false},
		[]bool{true, false, true})
	arg := newInsertIgnoreAutoIncrementArgument(input)
	require.NoError(t, arg.Prepare(proc))

	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, []int32{-1, 2},
		vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])[:result.Batch.RowCount()],
		"a negative explicit key must not discard a retained positive candidate")
	require.Equal(t, []int32{20, 30},
		vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[1])[:result.Batch.RowCount()])
	require.Equal(t, uint64(2), proc.GetStatementLastInsertID())

	arg.Free(proc, false, nil)
	input.Clean(proc.Mp())
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestInsertIgnoreReorderDoesNotAdvancePastNegativeExplicitKeyAcrossBatches(t *testing.T) {
	proc := testutil.NewProc(t)
	first := makeInsertIgnoreAutoIncrementBatch(t, proc,
		[]int32{2}, []int32{10},
		[]bool{false}, []bool{true}, []bool{true})
	second := makeInsertIgnoreAutoIncrementBatch(t, proc,
		[]int32{-1, 3}, []int32{20, 30},
		[]bool{false, false}, []bool{false, false}, []bool{false, true})
	arg := newInsertIgnoreAutoIncrementArgument(first, second)
	require.NoError(t, arg.Prepare(proc))

	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.True(t, result.Batch.IsEmpty())
	result, err = arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, []int32{-1, 2},
		vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])[:result.Batch.RowCount()],
		"negative explicit keys must preserve candidates retained by an earlier batch")
	require.Equal(t, []int32{20, 30},
		vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[1])[:result.Batch.RowCount()])
	require.Equal(t, uint64(2), proc.GetStatementLastInsertID())

	arg.Free(proc, false, nil)
	first.Clean(proc.Mp())
	second.Clean(proc.Mp())
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestAutoIncrementCandidateStreamDiscardThroughKeepsHigherCandidates(t *testing.T) {
	proc := testutil.NewProc(t)
	stream := autoIncrementCandidateStream{}
	require.NoError(t, stream.append(types.T_int32.ToType(), 2, proc.Mp()))
	require.NoError(t, stream.append(types.T_int32.ToType(), 5, proc.Mp()))
	require.NoError(t, stream.append(types.T_int32.ToType(), 8, proc.Mp()))
	require.NoError(t, stream.discardThrough(5))

	arg := newInsertIgnoreAutoIncrementArgument()
	require.NoError(t, arg.Prepare(proc))
	defer arg.Free(proc, false, nil)
	arg.ctr.autoIncrementCandidates = stream
	candidate, ok, err := arg.popAutoIncrementCandidate()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(8), candidate.value)
	require.NoError(t, arg.ctr.autoIncrementCandidates.append(types.T_int32.ToType(), 4, proc.Mp()))
	candidate, ok, err = arg.popAutoIncrementCandidate()
	require.NoError(t, err)
	require.False(t, ok, "a later batch must not reintroduce a candidate below the bound")
}

func TestInsertIgnoreReorderTracksFinalGeneratedPrimaryKeysAcrossBatches(t *testing.T) {
	proc := testutil.NewProc(t)
	// Candidate 2 is rejected by the secondary unique key, so the accepted
	// candidate 3 is published as final primary key 2. A later explicit 2 must
	// therefore be rejected even though the input-candidate hash state only saw
	// the original value 3.
	first := makeInsertIgnoreAutoIncrementBatch(t, proc,
		[]int32{2, 3}, []int32{10, 20},
		[]bool{false, false}, []bool{true, false},
		[]bool{true, true})
	second := makeInsertIgnoreAutoIncrementBatch(t, proc,
		[]int32{2}, []int32{30},
		[]bool{false}, []bool{false},
		[]bool{false})
	arg := newInsertIgnoreAutoIncrementArgument(first, second)
	require.NoError(t, arg.Prepare(proc))

	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, []int32{2},
		vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])[:result.Batch.RowCount()])
	result, err = arg.Call(proc)
	require.NoError(t, err)
	require.True(t, result.Batch.IsEmpty(), "explicit key must conflict with the final generated key")

	arg.Free(proc, false, nil)
	first.Clean(proc.Mp())
	second.Clean(proc.Mp())
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestInsertIgnoreReorderAllowsInputCandidateAfterFinalRemap(t *testing.T) {
	proc := testutil.NewProc(t)
	first := makeInsertIgnoreAutoIncrementBatch(t, proc,
		[]int32{2, 3}, []int32{10, 20},
		[]bool{false, false}, []bool{true, false},
		[]bool{true, true})
	second := makeInsertIgnoreAutoIncrementBatch(t, proc,
		[]int32{3}, []int32{30},
		[]bool{false}, []bool{false},
		[]bool{false})
	arg := newInsertIgnoreAutoIncrementArgument(first, second)
	require.NoError(t, arg.Prepare(proc))

	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, []int32{2},
		vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])[:result.Batch.RowCount()])
	result, err = arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, []int32{3},
		vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])[:result.Batch.RowCount()],
		"the old generated candidate 3 was remapped to final key 2 and must not block explicit 3")

	arg.Free(proc, false, nil)
	first.Clean(proc.Mp())
	second.Clean(proc.Mp())
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestInsertIgnoreReorderPublishesLastInsertIDOnlyForAcceptedRows(t *testing.T) {
	for _, tc := range []struct {
		name       string
		ukConflict bool
		want       uint64
	}{
		{name: "accepted generated row", want: 2},
		{name: "all generated rows ignored", ukConflict: true, want: 700},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProc(t)
			proc.SetStatementLastInsertID(700)
			input := makeInsertIgnoreAutoIncrementBatch(t, proc,
				[]int32{2}, []int32{20},
				[]bool{false}, []bool{tc.ukConflict},
				[]bool{true})
			arg := newInsertIgnoreAutoIncrementArgument(input)
			require.NoError(t, arg.Prepare(proc))

			_, err := arg.Call(proc)
			require.NoError(t, err)
			require.Equal(t, tc.want, proc.GetStatementLastInsertID())

			arg.Free(proc, false, nil)
			input.Clean(proc.Mp())
			require.Equal(t, int64(0), proc.Mp().CurrNB())
		})
	}
}

func TestAutoIncrementCandidateStreamCompressesArithmeticRuns(t *testing.T) {
	proc := testutil.NewProc(t)
	source := vector.NewVec(types.T_int32.ToType())
	arg := &PreInsertUnique{}
	const count = 10000
	for i := int32(0); i < count; i++ {
		require.NoError(t, vector.AppendFixed(source, i+2, false, proc.Mp()))
		require.NoError(t, arg.appendAutoIncrementCandidate(proc, source, int(i)))
	}
	require.Len(t, arg.ctr.autoIncrementCandidates.runs, 1)
	require.Equal(t, uint64(count), arg.ctr.autoIncrementCandidates.runs[0].count)

	arg.Free(proc, false, nil)
	source.Free(proc.Mp())
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestAutoIncrementCandidateCompactionRetainsStatementState(t *testing.T) {
	proc := testutil.NewProc(t)
	arg := newInsertIgnoreAutoIncrementArgument()
	require.NoError(t, arg.Prepare(proc))
	defer arg.Free(proc, false, nil)
	stream := &arg.ctr.autoIncrementCandidates
	require.NoError(t, stream.discardThrough(10))
	require.NoError(t, stream.append(types.T_int32.ToType(), 11, proc.Mp()))
	_, ok, err := arg.popAutoIncrementCandidate()
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, arg.compactAutoIncrementCandidates(proc))
	require.NoError(t, stream.append(types.T_int32.ToType(), 9, proc.Mp()))
	_, ok, err = arg.popAutoIncrementCandidate()
	require.NoError(t, err)
	require.False(t, ok, "compaction must not reintroduce a fenced candidate")
	require.Error(t, stream.append(types.T_int64.ToType(), 12, proc.Mp()))
	stream.reset(proc.Mp())
	require.NoError(t, stream.append(types.T_int64.ToType(), 9, proc.Mp()))
	_, ok, err = arg.popAutoIncrementCandidate()
	require.NoError(t, err)
	require.True(t, ok, "only statement reset clears the fence and type")
}

func TestODKUTargetArbitrationUsesOrderedStatementLocalState(t *testing.T) {
	testCases := []struct {
		name              string
		ids               []int32
		uniqueKeys        []int32
		uniqueNulls       []bool
		existingPKTargets []int32
		existingPKNulls   []bool
		existingUKTargets []int32
		existingUKNulls   []bool
		wantTargets       []int32
	}{
		{
			name: "repeated new primary key is insert then update",
			ids:  []int32{1, 1}, uniqueKeys: []int32{10, 11},
			existingPKNulls: []bool{true, true}, existingUKNulls: []bool{true, true},
			wantTargets: []int32{1, 1},
		},
		{
			name: "repeated new secondary key updates first inserted row",
			ids:  []int32{1, 2}, uniqueKeys: []int32{10, 10},
			existingPKNulls: []bool{true, true}, existingUKNulls: []bool{true, true},
			wantTargets: []int32{1, 1},
		},
		{
			name: "update action does not reserve its unused incoming primary key",
			ids:  []int32{1, 2, 2}, uniqueKeys: []int32{10, 10, 20},
			existingPKNulls: []bool{true, true, true}, existingUKNulls: []bool{true, true, true},
			wantTargets: []int32{1, 1, 2},
		},
		{
			name: "primary constraint wins when existing constraints name different rows",
			ids:  []int32{1}, uniqueKeys: []int32{10},
			existingPKTargets: []int32{100}, existingUKTargets: []int32{200},
			wantTargets: []int32{100},
		},
		{
			name: "nullable unique keys do not conflict",
			ids:  []int32{1, 2}, uniqueKeys: []int32{0, 0}, uniqueNulls: []bool{true, true},
			existingPKNulls: []bool{true, true}, existingUKNulls: []bool{true, true},
			wantTargets: []int32{1, 2},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProc(t)
			input := makeODKUTargetArbitrationBatch(t, proc, tc.ids, tc.uniqueKeys,
				tc.uniqueNulls, tc.existingPKTargets, tc.existingPKNulls,
				tc.existingUKTargets, tc.existingUKNulls)
			arg := newODKUTargetArbitrationArgument(input)
			account := installODKUTestAllocation(t, arg)
			require.NoError(t, arg.Prepare(proc))

			result, err := arg.Call(proc)
			require.NoError(t, err)
			require.Equal(t, tc.wantTargets,
				vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[2])[:result.Batch.RowCount()])

			arg.Free(proc, false, nil)
			require.NoError(t, arg.ClearAllocationAccount(account))
			input.Clean(proc.Mp())
			require.Equal(t, int64(0), proc.Mp().CurrNB())
		})
	}
}

func TestODKUTargetArbitrationCarriesStateAcrossBatchesAndReset(t *testing.T) {
	proc := testutil.NewProc(t)
	first := makeODKUTargetArbitrationBatch(t, proc,
		[]int32{1}, []int32{10}, nil, nil, []bool{true}, nil, []bool{true})
	second := makeODKUTargetArbitrationBatch(t, proc,
		[]int32{2}, []int32{10}, nil, nil, []bool{true}, nil, []bool{true})
	arg := newODKUTargetArbitrationArgument(first, second)
	account := installODKUTestAllocation(t, arg)
	require.NoError(t, arg.Prepare(proc))

	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, []int32{1}, vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[2]))
	result, err = arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, []int32{1}, vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[2]))

	arg.Reset(proc, false, nil)
	arg.Children = nil
	arg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{second}))
	require.NoError(t, arg.Prepare(proc))
	result, err = arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, []int32{2}, vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[2]),
		"Reset must begin a fresh statement-local arbitration generation")

	arg.Free(proc, false, nil)
	require.NoError(t, arg.ClearAllocationAccount(account))
	first.Clean(proc.Mp())
	second.Clean(proc.Mp())
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestODKUTargetArbitrationSupportsSingleSyntheticIdentityConstraint(t *testing.T) {
	proc := testutil.NewProc(t)
	input := batch.NewWithSize(2)
	input.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	input.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	for _, value := range []int32{10, 10} {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], value, false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(input.Vecs[1], int32(0), true, proc.Mp()))
	}
	input.SetRowCount(2)
	arg := &PreInsertUnique{PreInsertCtx: &plan.PreInsertUkCtx{
		PkColumn: 0, OdkuTargetArbitration: true,
		KeyColumns: []int32{0}, TargetColumns: []int32{1}, OutputColumns: 1,
	}}
	arg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	account := installODKUTestAllocation(t, arg)
	require.NoError(t, arg.Prepare(proc))

	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, []int32{10, 10},
		vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[1]))

	arg.Free(proc, false, nil)
	require.NoError(t, arg.ClearAllocationAccount(account))
	input.Clean(proc.Mp())
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestODKUTargetArbitrationHandlesConstNullSnapshotTargets(t *testing.T) {
	proc := testutil.NewProc(t)
	input := makeODKUTargetArbitrationBatch(t, proc,
		[]int32{1, 2}, []int32{10, 10}, nil, nil, []bool{true, true}, nil, []bool{true, true})
	// Hash joins over INSERT ... SELECT can represent an unmatched lookup column
	// as a const-NULL vector with no physical values. Reading only its raw null
	// bitmap misclassifies it as a pre-statement target.
	input.Vecs[2].Free(proc.Mp())
	input.Vecs[2] = vector.NewConstNull(types.T_int32.ToType(), input.RowCount(), proc.Mp())
	input.Vecs[3].Free(proc.Mp())
	input.Vecs[3] = vector.NewConstNull(types.T_int32.ToType(), input.RowCount(), proc.Mp())
	arg := newODKUTargetArbitrationArgument(input)
	account := installODKUTestAllocation(t, arg)
	require.NoError(t, arg.Prepare(proc))

	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, []int32{1, 1},
		vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[2]))

	arg.Free(proc, false, nil)
	require.NoError(t, arg.ClearAllocationAccount(account))
	input.Clean(proc.Mp())
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestODKUTargetArbitrationRejectsDifferentTargetRepresentation(t *testing.T) {
	proc := testutil.NewProc(t)
	input := makeODKUTargetArbitrationBatch(t, proc,
		[]int32{1}, []int32{10}, nil, nil, []bool{true}, nil, []bool{true})
	// A planner metadata bug must fail closed before UnionOne copies bytes using
	// the destination representation.  Fix canonical metadata at the producer;
	// do not weaken this guard to make incompatible key types look equivalent.
	input.Vecs[3].Free(proc.Mp())
	input.Vecs[3] = vector.NewConstNull(types.T_int64.ToType(), input.RowCount(), proc.Mp())
	arg := newODKUTargetArbitrationArgument(input)
	account := installODKUTestAllocation(t, arg)
	require.NoError(t, arg.Prepare(proc))

	_, err := arg.Call(proc)
	require.ErrorContains(t, err, "ODKU target primary-key type mismatch")

	arg.Free(proc, true, err)
	require.NoError(t, arg.ClearAllocationAccount(account))
	input.Clean(proc.Mp())
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestODKUTargetArbitrationAllocationAccountLifecycle(t *testing.T) {
	proc := testutil.NewProc(t)
	input := makeODKUTargetArbitrationBatch(t, proc,
		[]int32{1, 2}, []int32{10, 10}, nil, nil, []bool{true, true}, nil, []bool{true, true})
	arg := newODKUTargetArbitrationArgument(input)

	require.ErrorIs(t, arg.Prepare(proc), mpool.ErrAllocationAccountInvalid)
	account := installODKUTestAllocation(t, arg)
	require.NoError(t, arg.Prepare(proc))
	_, err := arg.Call(proc)
	require.NoError(t, err)
	require.Positive(t, account.Snapshot().Used,
		"statement-local hash keys, target rows, and ordinals must share the statement account")
	require.ErrorIs(t, arg.ClearAllocationAccount(account), mpool.ErrAllocationAccountInvariant)

	arg.Reset(proc, false, nil)
	require.Zero(t, account.Snapshot().Used)
	require.NoError(t, arg.ClearAllocationAccount(account))
	arg.Free(proc, false, nil)
	input.Clean(proc.Mp())
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestODKUTargetArbitrationHonorsAllocationCapacity(t *testing.T) {
	proc := testutil.NewProc(t)
	input := makeODKUTargetArbitrationBatch(t, proc,
		[]int32{1}, []int32{10}, nil, nil, []bool{true}, nil, []bool{true})
	arg := newODKUTargetArbitrationArgument(input)
	registry, err := mpool.NewAllocationAccountRegistry(1, 16)
	require.NoError(t, err)
	account, err := registry.Open(1)
	require.NoError(t, err)
	require.NoError(t, arg.SetAllocationAccount(account))

	err = arg.Prepare(proc)
	if err == nil {
		_, err = arg.Call(proc)
	}
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	arg.Reset(proc, true, err)
	require.Zero(t, account.Snapshot().Used)
	require.NoError(t, arg.ClearAllocationAccount(account))
	arg.Free(proc, true, err)
	input.Clean(proc.Mp())
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestOrderedUniqueKeyArbitrationRejectsMalformedContext(t *testing.T) {
	for _, tc := range []struct {
		name string
		ctx  *plan.PreInsertUkCtx
		want string
	}{
		{name: "missing context", want: "missing pre-insert unique context"},
		{name: "conflicting modes", ctx: &plan.PreInsertUkCtx{
			InsertIgnoreMultiDedup: true, OdkuTargetArbitration: true,
			KeyColumns: []int32{0}, ConflictColumns: []int32{1}, TargetColumns: []int32{2}, OutputColumns: 1,
		}, want: "conflicting ordered unique-key arbitration modes"},
		{name: "mismatched ODKU metadata", ctx: &plan.PreInsertUkCtx{
			OdkuTargetArbitration: true, KeyColumns: []int32{0, 1}, TargetColumns: []int32{2}, OutputColumns: 1,
		}, want: "invalid ODKU target arbitration context"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProc(t)
			arg := &PreInsertUnique{PreInsertCtx: tc.ctx}
			err := arg.Prepare(proc)
			require.ErrorContains(t, err, tc.want)
			arg.Free(proc, true, err)
			require.Equal(t, int64(0), proc.Mp().CurrNB())
		})
	}
}

func makeODKUTargetArbitrationBatch(
	t *testing.T,
	proc *process.Process,
	ids, uniqueKeys []int32,
	uniqueNulls []bool,
	existingPKTargets []int32,
	existingPKNulls []bool,
	existingUKTargets []int32,
	existingUKNulls []bool,
) *batch.Batch {
	t.Helper()
	input := batch.NewWithSize(4)
	for i := range input.Vecs {
		input.Vecs[i] = vector.NewVec(types.T_int32.ToType())
	}
	for row := range ids {
		uniqueNull := len(uniqueNulls) > row && uniqueNulls[row]
		pkTargetNull := len(existingPKNulls) > row && existingPKNulls[row]
		ukTargetNull := len(existingUKNulls) > row && existingUKNulls[row]
		var pkTarget, ukTarget int32
		if len(existingPKTargets) > row {
			pkTarget = existingPKTargets[row]
		}
		if len(existingUKTargets) > row {
			ukTarget = existingUKTargets[row]
		}
		require.NoError(t, vector.AppendFixed(input.Vecs[0], ids[row], false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(input.Vecs[1], uniqueKeys[row], uniqueNull, proc.Mp()))
		require.NoError(t, vector.AppendFixed(input.Vecs[2], pkTarget, pkTargetNull, proc.Mp()))
		require.NoError(t, vector.AppendFixed(input.Vecs[3], ukTarget, ukTargetNull, proc.Mp()))
	}
	input.SetRowCount(len(ids))
	return input
}

func newODKUTargetArbitrationArgument(inputs ...*batch.Batch) *PreInsertUnique {
	arg := &PreInsertUnique{
		PreInsertCtx: &plan.PreInsertUkCtx{
			PkColumn:              0,
			OdkuTargetArbitration: true,
			KeyColumns:            []int32{0, 1},
			TargetColumns:         []int32{2, 3},
			OutputColumns:         2,
		},
	}
	arg.AppendChild(colexec.NewMockOperator().WithBatchs(inputs))
	return arg
}

func installODKUTestAllocation(t testing.TB, arg *PreInsertUnique) *mpool.AllocationAccount {
	t.Helper()
	registry, err := mpool.NewAllocationAccountRegistry(1, 4_096)
	require.NoError(t, err)
	account, err := registry.Open(1 << 60)
	require.NoError(t, err)
	require.NoError(t, arg.SetAllocationAccount(account))
	return account
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

func makeInsertIgnoreAutoIncrementBatch(
	t *testing.T,
	proc *process.Process,
	ids, uniqueKeys []int32,
	pkConflicts, ukConflicts, generated []bool,
) *batch.Batch {
	t.Helper()
	input := batch.NewWithSize(5)
	input.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	input.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	input.Vecs[2] = vector.NewVec(types.T_bool.ToType())
	input.Vecs[3] = vector.NewVec(types.T_bool.ToType())
	input.Vecs[4] = vector.NewVec(types.T_bool.ToType())
	for row := range ids {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], ids[row], false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(input.Vecs[1], uniqueKeys[row], false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(input.Vecs[2], pkConflicts[row], false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(input.Vecs[3], ukConflicts[row], false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(input.Vecs[4], generated[row], false, proc.Mp()))
	}
	input.SetRowCount(len(ids))
	return input
}

func newInsertIgnoreAutoIncrementArgument(inputs ...*batch.Batch) *PreInsertUnique {
	arg := &PreInsertUnique{
		PreInsertCtx: &plan.PreInsertUkCtx{
			InsertIgnoreMultiDedup:       true,
			KeyColumns:                   []int32{0, 1},
			ConflictColumns:              []int32{2, 3},
			OutputColumns:                2,
			AutoIncrementReorder:         true,
			AutoIncrementColumn:          0,
			AutoIncrementGeneratedColumn: 4,
			AutoIncrementKeyIndex:        0,
		},
	}
	arg.AppendChild(colexec.NewMockOperator().WithBatchs(inputs))
	return arg
}

func newInsertIgnoreMultiDedupArgument(inputs ...*batch.Batch) *PreInsertUnique {
	arg := &PreInsertUnique{
		PreInsertCtx: &plan.PreInsertUkCtx{
			InsertIgnoreMultiDedup: true,
			KeyColumns:             []int32{0, 1},
			ConflictColumns:        []int32{2, 3},
			OutputColumns:          2,
			KeyNames:               []string{"id", "v"},
			KeyTypes: []*plan.Type{
				{Id: int32(types.T_int32)},
				{Id: int32(types.T_int32)},
			},
			KeyTypeCounts: []int32{1, 1},
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
