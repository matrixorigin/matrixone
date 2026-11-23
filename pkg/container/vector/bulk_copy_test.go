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

package vector

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestBatchCopySelection(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	flat := NewVec(types.T_int64.ToType(), mp)
	t.Cleanup(func() { flat.Free(mp) })
	require.NoError(t, AppendFixedList(flat, []int64{11, 22, 33}, nil, mp))
	flat.SetNull(1)
	constant, err := NewConstBytes(types.T_varchar.ToType(), []byte("a non-inline constant payload"), 3, mp)
	require.NoError(t, err)
	t.Cleanup(func() { constant.Free(mp) })

	// Reuse the batch's vector slots across representation generations. Each
	// fresh selection must honor the current source, including reordered rows.
	sources := make([]*Vector, 1)
	for _, src := range []*Vector{flat, constant, flat} {
		sources[0] = src
		multi := GetUnionMultiFunction(sources)
		batch := GetUnionBatchFunction(sources)
		for _, rangeCopy := range []bool{false, true} {
			func() {
				got, want := NewVec(*src.GetType(), mp), NewVec(*src.GetType(), mp)
				defer got.Free(mp)
				defer want.Free(mp)
				if rangeCopy {
					flags := []uint8{1, 0, 1}
					require.NoError(t, batch(got, src, 0, 3, flags, mp))
					require.NoError(t, want.UnionBatch(src, 0, 3, flags, mp))
				} else {
					require.NoError(t, multi(got, src, 2, 3, mp))
					require.NoError(t, want.UnionMulti(src, 2, 3, mp))
				}
				a, err := got.MarshalBinary()
				require.NoError(t, err)
				b, err := want.MarshalBinary()
				require.NoError(t, err)
				require.Equal(t, b, a)
			}()
		}
	}
	// Unknown/mixed batches retain a callable generic fallback.
	for _, unknown := range [][]*Vector{nil, {nil}, {flat, constant}, {{class: 255}}} {
		out := NewVec(types.T_int64.ToType(), mp)
		require.NoError(t, GetUnionBatchFunction(unknown)(out, flat, 0, 3, nil, mp))
		values := MustFixedColNoTypeCheck[int64](out)
		require.Equal(t, int64(11), values[0])
		require.Equal(t, int64(33), values[2])
		require.True(t, out.IsNull(1))
		out.Free(mp)
	}
	var multi UnionMultiFunc
	var batch UnionBatchFunc
	require.Zero(t, testing.AllocsPerRun(100, func() {
		multi, batch = GetUnionMultiFunction(sources), GetUnionBatchFunction(sources)
	}))
	require.NotNil(t, multi)
	require.NotNil(t, batch)
}

func TestBatchCopySelectionAliasAndAdmission(t *testing.T) {
	for _, rangeCopy := range []bool{false, true} {
		t.Run(map[bool]string{false: "repeat", true: "range"}[rangeCopy], func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			source := NewVec(types.T_int64.ToType(), mp)
			t.Cleanup(func() { source.Free(mp) })
			require.NoError(t, AppendFixedList(source, []int64{11, 22, 33}, nil, mp))
			multi, batch := GetUnionMultiFunction([]*Vector{source}), GetUnionBatchFunction([]*Vector{source})
			if rangeCopy {
				require.NoError(t, batch(source, source, 1, 2, nil, mp))
				require.Equal(t, []int64{11, 22, 33, 22, 33}, MustFixedColNoTypeCheck[int64](source))
			} else {
				require.NoError(t, multi(source, source, 1, 2, mp))
				require.Equal(t, []int64{11, 22, 33, 22, 22}, MustFixedColNoTypeCheck[int64](source))
			}
			state := newTestVectorAllocationAccount(t, 1, 128)
			t.Cleanup(func() { finalizeTestVectorAllocationAccount(t, state) })
			dmp := state.view(t, mp)
			out := newAccountedTestVector(t, types.T_int64.ToType(), dmp)
			t.Cleanup(func() { out.Free(dmp) })
			var err error
			if rangeCopy {
				err = batch(out, source, 0, 3, nil, dmp)
			} else {
				err = multi(out, source, 0, 3, dmp)
			}
			require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
			require.Zero(t, out.Length())
			require.Zero(t, state.account.Snapshot().Used)
		})
	}
}
