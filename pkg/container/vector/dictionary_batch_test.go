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
	t.Run("decline-after-first-column", func(t *testing.T) {
		first := newAccountedTestVector(t, types.T_int64.ToType(), state.selection)
		t.Cleanup(func() { first.Free(mp) })
		require.NoError(t, AppendFixedList(first, []int64{10, 20, 30}, nil, mp))
		used := state.account.Snapshot().Used
		destinations := make([]*Vector, 2)
		spare, err := NewOffHeapVecWithTypeAndAllocation(types.T_int64.ToType(), state.selection)
		require.NoError(t, err)
		reuse := []*Vector{spare, nil}
		t.Cleanup(func() {
			for _, v := range reuse {
				if v != nil {
					v.Free(mp)
				}
			}
		})
		admitted, err := TrySelectDictionaryVectorsReuse([]*Vector{first, source}, destinations, []int64{2}, 3, mp, state.selection, reuse)
		require.NoError(t, err)
		require.False(t, admitted)
		require.Nil(t, destinations[0], "partial first-column publication must roll back")
		require.Nil(t, destinations[1])
		require.Nil(t, reuse[0], "the consumed first header must be cleaned on second-column decline")
		require.Equal(t, used, state.account.Snapshot().Used)
		require.Equal(t, []int64{10, 20, 30}, MustFixedColNoTypeCheck[int64](first))
	})
}

func TestDictionaryWorkspaceReuseAndHeldView(t *testing.T) {
	for _, held := range []bool{false, true} {
		t.Run(map[bool]string{false: "reuse", true: "held-view"}[held], func(t *testing.T) {
			mp := mpool.MustNewZero()
			state := newTestVectorAllocationAccount(t, 1<<20, 128)
			t.Cleanup(func() { finalizeTestVectorAllocationAccount(t, state); require.Zero(t, mp.CurrNB()) })
			source := newAccountedTestVector(t, types.T_int64.ToType(), state.selection)
			t.Cleanup(func() { source.Free(mp) })
			require.NoError(t, AppendFixedList(source, []int64{10, 20, 30}, nil, mp))
			original := &source.data[0]
			selected := make([]*Vector, 1)
			admitted, err := TrySelectDictionaryVectors([]*Vector{source}, selected, []int64{2, 0}, 3, mp, state.selection)
			require.NoError(t, err)
			require.True(t, admitted)
			if !held {
				selected[0].Free(mp)
				selected[0] = nil
			}
			t.Cleanup(func() {
				if selected[0] != nil {
					selected[0].Free(mp)
				}
			})
			source.CleanOnlyData()
			if !held {
				require.Equal(t, OwnedMPoolUnique, source.DataBackingKind())
				require.Same(t, original, &source.data[0])
			}
			require.NoError(t, AppendFixedList(source, []int64{40, 50, 60}, nil, mp))
			if held {
				borrowed := selected[0].dictionaryState().base.vec
				borrowed.ReclaimDictionaryWorkspace()
				require.Equal(t, BorrowedLease, borrowed.DataBackingKind(), "a surviving snapshot cannot take the original workspace's release authority")
				require.NotSame(t, original, &source.data[0])
				require.Equal(t, int64(30), GetFixedAtNoTypeCheck[int64](selected[0], 0))
				require.Equal(t, int64(10), GetFixedAtNoTypeCheck[int64](selected[0], 1))
			} else {
				require.Same(t, original, &source.data[0])
			}
		})
	}
}

func TestDictionaryBulkFixedWidthsOffsetsAndFlags(t *testing.T) {
	for _, typ := range []types.T{types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_TS, types.T_decimal128, types.T_Blockid, types.T_Rowid, types.T_decimal256} {
		for _, nullable := range []bool{false, true} {
			t.Run(typ.String()+map[bool]string{false: "/plain", true: "/nullable"}[nullable], func(t *testing.T) {
				mp := mpool.MustNewZero()
				t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
				source := NewVec(typ.ToType())
				t.Cleanup(func() { source.Free(mp) })
				require.NoError(t, source.PreExtend(3, mp))
				source.SetLength(3)
				width := source.GetType().TypeSize()
				raw := source.GetData()
				for row := 0; row < 3; row++ {
					for col := 0; col < width; col++ {
						raw[row*width+col] = byte(1 + row*37 + col)
					}
				}
				if nullable {
					source.SetNull(1)
				}
				base, err := FreezeVector(source, mp, nil)
				require.NoError(t, err)
				t.Cleanup(base.Release)
				mapping := []int64{2, 1, 0, 2}
				rows, err := NewRowSelection(mapping, 3, mp, nil)
				require.NoError(t, err)
				t.Cleanup(rows.Release)
				selected, err := NewDictionaryVector(base, rows, mp, nil)
				require.NoError(t, err)
				t.Cleanup(func() { selected.Free(mp) })
				for _, tc := range []struct {
					start, count int
					flags        []uint8
					logical      []int
				}{
					{0, 4, nil, []int{0, 1, 2, 3}}, {1, 2, nil, []int{1, 2}}, {0, 3, []uint8{1, 0, 1, 1}, []int{0, 2, 3}},
				} {
					output := NewVec(typ.ToType())
					func() {
						defer output.Free(mp)
						require.NoError(t, output.UnionOne(source, 0, mp))
						require.NoError(t, output.UnionBatch(selected, int64(tc.start), tc.count, tc.flags, mp))
						require.Equal(t, 1+len(tc.logical), output.Length())
						for row, logical := range tc.logical {
							original := int(mapping[logical])
							isNull := nullable && original == 1
							require.Equal(t, isNull, output.IsNull(uint64(row+1)))
							if !isNull {
								require.Equal(t, raw[original*width:(original+1)*width], output.GetData()[(row+1)*width:(row+2)*width])
							}
						}
					}()
				}
			})
		}
	}
}

func TestDictionaryWorkspaceRetiresOnlyAfterFinalBorrower(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	source := NewVec(types.T_int64.ToType())
	t.Cleanup(func() { source.Free(mp) })
	require.NoError(t, AppendFixedList(source, []int64{10, 20}, nil, mp))
	snapshots := &DictionaryVectorSlot{}
	rowsSlot := &DictionaryVectorSlot{}
	t.Cleanup(func() { snapshots.Free(mp); rowsSlot.Free(mp) })
	first, ok, err := TryFreezeVectorReuse(source, mp, nil, snapshots)
	require.NoError(t, err)
	require.True(t, ok)
	oldHeader := first.vec
	rows, err := NewRowSelectionReuse([]int64{1, 0}, 2, mp, nil, rowsSlot)
	require.NoError(t, err)
	oldRows := rows.storage
	view, err := NewDictionaryVector(first, rows, mp, nil)
	require.NoError(t, err)
	defer view.Free(mp)
	first.Release()
	rows.Release()
	next, ok, err := TryFreezeVectorReuse(source, mp, nil, snapshots)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotSame(t, oldHeader, next.vec, "a live view must prevent snapshot reuse")
	spare := next.vec
	next.Release()
	next, ok, err = TryFreezeVectorReuse(source, mp, nil, snapshots)
	require.NoError(t, err)
	require.True(t, ok)
	require.Same(t, spare, next.vec)
	next.Release()
	otherRows, err := NewRowSelectionReuse([]int64{0}, 2, mp, nil, rowsSlot)
	require.NoError(t, err)
	require.NotSame(t, oldRows, otherRows.storage)
	spareRows := otherRows.storage
	otherRows.Release()
	otherRows, err = NewRowSelectionReuse([]int64{1}, 2, mp, nil, rowsSlot)
	require.NoError(t, err)
	require.Same(t, spareRows, otherRows.storage)
	require.Equal(t, []int64{1}, otherRows.storageFixed())
	otherRows.Release()
	require.Equal(t, int64(20), GetFixedAtNoTypeCheck[int64](view, 0))
	snapshots.Free(mp)
	rowsSlot.Free(mp)
	view.Free(mp) // final release after owner shutdown cannot reopen either slot
	require.Same(t, &closedDictionaryVectorSlot, snapshots.spare.Load())
	require.Same(t, &closedDictionaryVectorSlot, rowsSlot.spare.Load())
}

func TestDictionaryWorkspaceFinalReleaseRacesClose(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	source := NewVec(types.T_int64.ToType())
	t.Cleanup(func() { source.Free(mp) })
	require.NoError(t, AppendFixed(source, int64(7), false, mp))
	slot := &DictionaryVectorSlot{}
	base, ok, err := TryFreezeVectorReuse(source, mp, nil, slot)
	require.NoError(t, err)
	require.True(t, ok)
	start, done := make(chan struct{}), make(chan struct{})
	go func() { <-start; base.Release(); close(done) }()
	close(start)
	slot.Free(mp)
	<-done
	require.Same(t, &closedDictionaryVectorSlot, slot.spare.Load())
}
