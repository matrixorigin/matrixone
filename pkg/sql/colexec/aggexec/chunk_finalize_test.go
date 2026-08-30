// Copyright 2026 Matrix Origin
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

package aggexec

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

type cancelFinalizeAfterChecks struct {
	context.Context
	remaining int
}

func (ctx *cancelFinalizeAfterChecks) Err() error {
	ctx.remaining--
	if ctx.remaining <= 0 {
		return context.Canceled
	}
	return nil
}

func TestCountStarFinalizeChunkOwnershipAndCancellation(t *testing.T) {
	mp := mpool.MustNewZero()
	exec := newCountStarExec(
		mp, AggIdOfCountStar, false, []types.Type{types.T_int64.ToType()})
	defer func() {
		exec.Free()
		require.Zero(t, mp.CurrNB())
	}()

	require.True(t, SupportsChunkFinalization(exec))
	require.NoError(t, exec.GroupGrow(AggBatchSize+1))
	require.NoError(t, exec.BatchFill(
		0, []uint64{1, 1, AggBatchSize + 1}, nil))

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := FinalizeChunk(canceled, exec, 0)
	require.ErrorIs(t, err, context.Canceled)

	first, err := FinalizeChunk(context.Background(), exec, 0)
	require.NoError(t, err)
	require.Equal(t, int64(2),
		vector.MustFixedColNoTypeCheck[int64](first)[0])
	first.Free(mp)

	_, err = FinalizeChunk(context.Background(), exec, 0)
	require.ErrorContains(t, err, "already finalized")
	_, err = FinalizeChunk(context.Background(), exec, 2)
	require.ErrorContains(t, err, "out of range")

	last, err := FinalizeChunk(context.Background(), exec, 1)
	require.NoError(t, err)
	require.Equal(t, int64(1),
		vector.MustFixedColNoTypeCheck[int64](last)[0])
	last.Free(mp)
}

func TestCountFinalizeCancellationInsideExtraLoopRetainsOwnership(t *testing.T) {
	mp := mpool.MustNewZero()
	exec := newCountStarExec(
		mp, AggIdOfCountStar, false,
		[]types.Type{types.T_int64.ToType()}).(*countStarExec)
	require.NoError(t, exec.GroupGrow(AggBatchSize))
	require.NoError(t, exec.SetExtraInformation(int64(1), 0))

	ctx := &cancelFinalizeAfterChecks{
		Context: context.Background(),
		// Entry, row 0, then row 256.
		remaining: 3,
	}
	result, err := exec.FinalizeChunk(ctx, 0)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result)
	require.NotNil(t, exec.state[0].vecs[0])

	exec.Free()
	require.Zero(t, mp.CurrNB())
}

func TestSupportedAggregateFamiliesFinalizeOneChunk(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()

	for _, test := range []struct {
		name string
		id   int64
		typ  types.Type
	}{
		{name: "count", id: AggIdOfCountColumn, typ: types.T_int32.ToType()},
		{name: "sum", id: AggIdOfSum, typ: types.T_int32.ToType()},
		{name: "min", id: AggIdOfMin, typ: types.T_varchar.ToType()},
		{name: "max", id: AggIdOfMax, typ: types.T_int64.ToType()},
	} {
		t.Run(test.name, func(t *testing.T) {
			exec, err := MakeAgg(mp, test.id, false, test.typ)
			require.NoError(t, err)
			defer exec.Free()
			require.True(t, SupportsChunkFinalization(exec))
			require.NoError(t, exec.GroupGrow(1))
			result, err := FinalizeChunk(context.Background(), exec, 0)
			require.NoError(t, err)
			require.Equal(t, 1, result.Length())
			result.Free(mp)
		})
	}
}

func TestFinalizeChunkMatchesFlushAcrossSupportedFamilies(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()

	const groupsCount = AggBatchSize + 3
	groups := make([]uint64, groupsCount)
	for i := range groups {
		groups[i] = uint64(i + 1)
	}
	nulls := make([]bool, groupsCount)
	for i := range nulls {
		nulls[i] = i%17 == 0
	}

	for _, test := range []struct {
		name  string
		id    int64
		typ   types.Type
		input func() *vector.Vector
	}{
		{
			name: "count fixed nulls", id: AggIdOfCountColumn,
			typ: types.T_int32.ToType(),
			input: func() *vector.Vector {
				values := make([]int32, groupsCount)
				for i := range values {
					values[i] = int32(i)
				}
				vec := vector.NewVec(types.T_int32.ToType())
				require.NoError(t, vector.AppendFixedList(vec, values, nulls, mp))
				return vec
			},
		},
		{
			name: "sum fixed nulls", id: AggIdOfSum,
			typ: types.T_int32.ToType(),
			input: func() *vector.Vector {
				values := make([]int32, groupsCount)
				for i := range values {
					values[i] = int32(i - 100)
				}
				vec := vector.NewVec(types.T_int32.ToType())
				require.NoError(t, vector.AppendFixedList(vec, values, nulls, mp))
				return vec
			},
		},
		{
			name: "min variable nulls", id: AggIdOfMin,
			typ: types.T_varchar.ToType(),
			input: func() *vector.Vector {
				vec := vector.NewVec(types.T_varchar.ToType())
				for i := range groupsCount {
					require.NoError(t, vector.AppendBytes(
						vec, []byte(fmt.Sprintf("value-%05d", i)), nulls[i], mp))
				}
				return vec
			},
		},
		{
			name: "max fixed nulls", id: AggIdOfMax,
			typ: types.T_int64.ToType(),
			input: func() *vector.Vector {
				values := make([]int64, groupsCount)
				for i := range values {
					values[i] = int64(i)
				}
				vec := vector.NewVec(types.T_int64.ToType())
				require.NoError(t, vector.AppendFixedList(vec, values, nulls, mp))
				return vec
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := test.input()
			defer input.Free(mp)
			flushed, err := MakeAgg(mp, test.id, false, test.typ)
			require.NoError(t, err)
			chunked, err := MakeAgg(mp, test.id, false, test.typ)
			require.NoError(t, err)
			defer flushed.Free()
			defer chunked.Free()
			for _, exec := range []AggFuncExec{flushed, chunked} {
				groupExec, ok := exec.(GroupAggFuncExec)
				require.True(t, ok)
				require.NoError(t, exec.GroupGrow(groupsCount))
				require.NoError(t, groupExec.PreflightBatchFill(
					0, groups, []*vector.Vector{input}))
				require.NoError(t, groupExec.BatchFill(
					0, groups, []*vector.Vector{input}))
			}

			want, err := flushed.Flush()
			require.NoError(t, err)
			defer func() {
				for _, vec := range want {
					vec.Free(mp)
				}
			}()
			require.Len(t, want, 2)
			for chunk := range want {
				got, err := FinalizeChunk(
					context.Background(), chunked, chunk)
				require.NoError(t, err)
				wantBytes, err := want[chunk].MarshalBinary()
				require.NoError(t, err)
				gotBytes, err := got.MarshalBinary()
				require.NoError(t, err)
				require.Equal(t, wantBytes, gotBytes)
				got.Free(mp)
			}
		})
	}
}
