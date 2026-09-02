// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggexec

import (
	"bytes"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestCountDistinctArgumentDrainCommitAndRestore(t *testing.T) {
	mp := mpool.MustNewZero()
	input := testutil.NewInt64Vector(
		5,
		types.T_int64.ToType(),
		mp,
		false,
		nil,
		[]int64{1, 1, 2, 3, 3},
	)
	defer input.Free(mp)
	baseline := mp.CurrNB()

	exec := newCountColumnExec(
		mp,
		AggIdOfCountColumn,
		true,
		[]types.Type{types.T_int64.ToType()},
	)
	restored, ok := AggFuncExec(exec).(ExactCountDistinctSpillState)
	require.True(t, ok)
	require.NoError(t, restored.GroupGrow(2))
	require.NoError(t, restored.BatchFill(
		0,
		[]uint64{1, 1, 1, 2, 2},
		[]*vector.Vector{input},
	))

	drain, err := restored.BeginArgumentDrain(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(3), drain.KeyCount())
	require.Positive(t, drain.RetainedBytes())
	payloads := make(map[int][][]byte)
	require.NoError(t, drain.ForEach(func(group int, payload []byte) error {
		payloads[group] = append(payloads[group], bytes.Clone(payload))
		return nil
	}))
	require.Len(t, payloads[0], 2)
	require.Len(t, payloads[1], 1)
	require.NoError(t, drain.Commit())

	zero, err := restored.Flush()
	require.NoError(t, err)
	require.Equal(t, []int64{0, 0},
		vector.MustFixedColNoTypeCheck[int64](zero[0]))
	zero[0].Free(mp)
	require.NoError(t, restored.AddDistinctCountContribution(1, 5, nil))

	for group, values := range payloads {
		for _, payload := range values {
			require.NoError(t, restored.InsertDistinctArgument(group, payload))
			require.NoError(t, restored.InsertDistinctArgument(group, payload))
		}
	}
	result, err := restored.Flush()
	require.NoError(t, err)
	require.Equal(t, []int64{2, 6},
		vector.MustFixedColNoTypeCheck[int64](result[0]))
	result[0].Free(mp)
	restored.Free()
	require.Equal(t, baseline, mp.CurrNB())
}

func TestCountDistinctArgumentDrainAbortKeepsResidentOwner(t *testing.T) {
	mp := mpool.MustNewZero()
	input := testutil.NewInt64Vector(
		3,
		types.T_int64.ToType(),
		mp,
		false,
		nil,
		[]int64{7, 7, 8},
	)
	defer input.Free(mp)
	baseline := mp.CurrNB()

	exec := newCountColumnExec(
		mp,
		AggIdOfCountColumn,
		true,
		[]types.Type{types.T_int64.ToType()},
	)
	spill, ok := exec.(ExactCountDistinctSpillState)
	require.True(t, ok)
	require.NoError(t, spill.GroupGrow(1))
	require.NoError(t, spill.BatchFill(
		0,
		[]uint64{1, 1, 1},
		[]*vector.Vector{input},
	))

	drain, err := spill.BeginArgumentDrain(nil)
	require.NoError(t, err)
	wantErr := errors.New("injected drain failure")
	err = drain.ForEach(func(int, []byte) error { return wantErr })
	require.ErrorIs(t, err, wantErr)
	drain.Abort()

	result, err := spill.Flush()
	require.NoError(t, err)
	require.Equal(t, []int64{2},
		vector.MustFixedColNoTypeCheck[int64](result[0]))
	result[0].Free(mp)
	spill.Free()
	require.Equal(t, baseline, mp.CurrNB())
}

func TestCountDistinctDrainPreparationFailureKeepsResidentOwner(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	account, err := registry.Open(20 << 10)
	require.NoError(t, err)
	allocation, err := NewAllocationAccount(
		account,
		mpool.AllocationOwnerGroup,
		AllocationAccountSites{
			VectorData: 1, VectorArea: 2, VectorNulls: 3,
			VectorGrouping: 4, ArgumentCount: 5, ArgumentArena: 6,
		},
	)
	require.NoError(t, err)
	exec, err := MakeSingleGroupAgg(
		mp,
		AggIdOfCountColumn,
		true,
		allocation,
		nil,
		types.T_int64.ToType(),
	)
	require.NoError(t, err)
	SyncAggregatorsToChunkSize([]GroupAggFuncExec{exec}, 1)
	require.NoError(t, exec.GroupGrow(1))
	input := testutil.NewInt64Vector(
		1, types.T_int64.ToType(), mp, false, nil, []int64{9})
	require.NoError(t, exec.PreflightBatchFill(
		0, []uint64{1}, []*vector.Vector{input}))
	require.NoError(t, exec.BatchFill(
		0, []uint64{1}, []*vector.Vector{input}))
	spill := exec.(ExactCountDistinctSpillState)
	_, err = spill.BeginArgumentDrain(allocation)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)

	result, err := spill.Flush()
	require.NoError(t, err)
	require.Equal(t, []int64{1},
		vector.MustFixedColNoTypeCheck[int64](result[0]))
	result[0].Free(mp)
	input.Free(mp)
	spill.Free()
	require.NoError(t, spill.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}
