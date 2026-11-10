// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vector

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDictionaryColumnsShareMappingsAndBacking(t *testing.T) {
	mp := mpool.MustNewZero()
	state := newTestVectorAllocationAccount(t, 1<<20, 128)
	t.Cleanup(func() { finalizeTestVectorAllocationAccount(t, state); require.Zero(t, mp.CurrNB()) })
	source := newAccountedTestVector(t, types.T_int64.ToType(), state.selection)
	t.Cleanup(func() { source.Free(mp) })
	require.NoError(t, AppendFixedList(source, []int64{10, 20, 30, 40}, nil, mp))
	first := make([]*Vector, 2)
	admitted, err := TrySelectDictionaryVectors([]*Vector{source, source}, first, []int64{3, 0, 3}, 4, mp, state.selection)
	require.NoError(t, err)
	require.True(t, admitted)
	t.Cleanup(func() {
		for _, v := range first {
			v.Free(mp)
		}
	})
	require.NotSame(t, first[0], first[1])
	require.Same(t, first[0].dictionaryState().base, first[1].dictionaryState().base)
	require.Same(t, first[0].dictionaryState().rows, first[1].dictionaryState().rows)
	next := make([]*Vector, 2)
	admitted, err = TrySelectDictionaryVectors(first, next, []int64{2, 1}, 3, mp, state.selection)
	require.NoError(t, err)
	require.True(t, admitted)
	t.Cleanup(func() {
		for _, v := range next {
			v.Free(mp)
		}
	})
	require.Same(t, next[0].dictionaryState().rows, next[1].dictionaryState().rows)
	require.Same(t, first[0].dictionaryState().base, next[0].dictionaryState().base)
	source.CleanOnlyData()
	for _, v := range next {
		require.Equal(t, int64(40), GetFixedAtNoTypeCheck[int64](v, 0))
		require.Equal(t, int64(10), GetFixedAtNoTypeCheck[int64](v, 1))
	}
}

func TestDictionaryRetentionDeclinesCopyOnlySource(t *testing.T) {
	mp := mpool.MustNewZero()
	state := newTestVectorAllocationAccount(t, 1<<20, 128)
	t.Cleanup(func() { finalizeTestVectorAllocationAccount(t, state); require.Zero(t, mp.CurrNB()) })
	source := NewVec(types.T_int64.ToType())
	t.Cleanup(func() { source.Free(mp) })
	require.NoError(t, AppendFixedList(source, []int64{1, 2, 3}, nil, mp))
	// Moving unaccounted unique payload into this account would copy the whole
	// source. Decline so the caller can gather its smaller logical selection.
	output := make([]*Vector, 1)
	admitted, err := TrySelectDictionaryVectors([]*Vector{source}, output, []int64{2}, 3, mp, state.selection)
	require.NoError(t, err)
	require.False(t, admitted)
	require.Nil(t, output[0])
	require.Equal(t, []int64{1, 2, 3}, MustFixedColNoTypeCheck[int64](source))
}
