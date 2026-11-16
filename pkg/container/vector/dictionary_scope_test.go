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

func TestDictionarySelectionScopeReadAndRetire(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	source := NewVec(types.T_int64.ToType())
	t.Cleanup(func() { source.Free(mp) })
	require.NoError(t, AppendFixedList(source, []int64{10, 20, 30}, nil, mp))
	source.GetNulls().Add(1)
	source.GetGrouping().Add(2)
	source.SetSorted(true)
	scope := &DictionarySelectionScope{}
	t.Cleanup(func() { scope.Free(mp) })
	rows := []int64{2, 0, 2, 1}
	sources, outputs := []*Vector{source}, make([]*Vector, 1)
	admitted, err := scope.Bind(sources, outputs, []int{0}, rows, 3, mp, nil)
	require.NoError(t, err)
	require.True(t, admitted)
	view := outputs[0]
	read := ResolveFixedReadView[int64](view)
	require.Same(t, &MustFixedColNoTypeCheck[int64](source)[0], &read.Values[0], "values must alias the pinned source")
	require.Same(t, &rows[0], &read.Selection[0], "the row map must not be copied")
	require.Equal(t, int64(30), GetFixedAtNoTypeCheck[int64](view, 0))
	require.True(t, view.GetGrouping().Contains(0))
	require.True(t, view.GetGrouping().Contains(2))
	require.True(t, view.IsNull(3))
	require.False(t, view.GetSorted())
	require.False(t, view.dictionaryState().Retain(), "a scoped view is never an immutable published owner")
	flat := NewVec(types.T_int64.ToType())
	t.Cleanup(func() { flat.Free(mp) })
	require.NoError(t, flat.UnionBatch(view, 0, len(rows), nil, mp))
	scope.End()
	require.True(t, flat.IsNull(3))
	require.Zero(t, view.Length())
	require.Nil(t, scope.columns[0].base.vec)
	require.Nil(t, scope.storage.data)
	source.GetNulls().Clear()
	source.GetGrouping().Clear()
	admitted, err = scope.Bind(sources, outputs, []int{0}, []int64{0, 1}, 3, mp, nil)
	require.NoError(t, err)
	require.True(t, admitted)
	require.Same(t, view, outputs[0])
	require.False(t, view.HasNull())
	require.False(t, view.HasGrouping())
	require.True(t, view.GetSorted())
	require.Equal(t, []int64{30, 10, 30}, MustFixedColNoTypeCheck[int64](flat)[:3], "owned output survives scope reuse")
	_, err = scope.Bind(sources, outputs, []int{0}, []int64{3}, 3, mp, nil)
	require.Error(t, err)
	require.Nil(t, scope.columns[0].base.vec)
}

func TestDictionarySelectionScopeHasNoPayloadAllocation(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	source := NewVec(types.T_int64.ToType())
	t.Cleanup(func() { source.Free(mp) })
	require.NoError(t, AppendFixedList(source, []int64{10, 20, 30}, nil, mp))
	scope := &DictionarySelectionScope{}
	t.Cleanup(func() { scope.Free(mp) })
	sources, outputs, columns, rows := []*Vector{source}, make([]*Vector, 1), []int{0}, []int64{2, 0}
	var admitted bool
	var err error
	allocs := testing.AllocsPerRun(50, func() { admitted, err = scope.Bind(sources, outputs, columns, rows, 3, mp, nil); scope.End() })
	require.NoError(t, err)
	require.True(t, admitted)
	require.Zero(t, allocs)
	require.Zero(t, scope.columns[0].view.Allocated(), "NULL-free fixed views need no metadata or payload buffer")
	base, err := FreezeVector(source, mp, nil)
	require.NoError(t, err)
	t.Cleanup(base.Release)
	selection, err := NewRowSelection(rows, 3, mp, nil)
	require.NoError(t, err)
	t.Cleanup(selection.Release)
	dictionary, err := NewDictionaryVector(base, selection, mp, nil)
	require.NoError(t, err)
	t.Cleanup(func() { dictionary.Free(mp) })
	admitted, err = scope.Bind([]*Vector{dictionary}, outputs, columns, []int64{0}, 2, mp, nil)
	require.NoError(t, err)
	require.False(t, admitted, "composition keeps the ordinary owned fallback")
	require.True(t, base.Retain(), "public immutable handles still retain normally")
	base.Release()
}

func TestDictionarySelectionScopeStringsAndFailure(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	source := NewVec(types.T_varchar.ToType())
	t.Cleanup(func() { source.Free(mp) })
	require.NoError(t, AppendBytesList(source, [][]byte{[]byte("inline"), []byte("a string larger than the inline representation")}, nil, mp))
	scope := &DictionarySelectionScope{}
	t.Cleanup(func() { scope.Free(mp) })
	outputs := make([]*Vector, 1)
	admitted, err := scope.Bind([]*Vector{source}, outputs, []int{0}, []int64{1, 0, 1}, 2, mp, nil)
	require.NoError(t, err)
	require.True(t, admitted)
	view := ResolveVarlenaReadView(outputs[0])
	require.Same(t, &source.GetArea()[0], &view.Area[0])
	require.Equal(t, source.GetBytesAt(1), outputs[0].GetBytesAt(0))
	require.Equal(t, source.GetBytesAt(1), outputs[0].GetBytesAt(2))
	scope.End()
	registry, err := mpool.NewAllocationAccountRegistry(1, 32)
	require.NoError(t, err)
	account, err := registry.Open(1)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.Zero(t, account.Snapshot().Used)
		_, _, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
	})
	allocation, err := NewAllocationAccountSelection(account, mpool.AllocationOwnerSelection, 1, 2, 3, 4)
	require.NoError(t, err)
	many := NewVec(types.T_int64.ToType())
	t.Cleanup(func() { many.Free(mp) })
	require.NoError(t, AppendFixedList(many, make([]int64, 512), nil, mp))
	many.GetNulls().Add(511)
	rows := make([]int64, 512)
	for i := range rows {
		rows[i] = int64(i)
	}
	admitted, err = scope.Bind([]*Vector{many}, outputs, []int{0}, rows, 512, mp, allocation)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.False(t, admitted)
	require.Nil(t, outputs[0])
	require.Nil(t, scope.columns[0].base.vec)
	scope.Free(mp)
	require.Zero(t, account.Snapshot().Used)
}
