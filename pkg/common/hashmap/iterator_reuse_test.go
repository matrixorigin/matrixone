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

package hashmap

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestIntIteratorFindInsertReuse(t *testing.T) {
	for _, typ := range []types.Type{types.T_uint8.ToType(), types.T_uint16.ToType(), types.T_uint32.ToType(), types.T_uint64.ToType()} {
		for _, constant := range []bool{false, true} {
			for _, count := range []int{1, 255, UnitLimit} {
				t.Run(fmt.Sprintf("width-%d/const-%t/rows-%d", typ.TypeSize(), constant, count), func(t *testing.T) {
					mp := mpool.MustNewZero()
					h, err := NewIntHashMap(false, mp)
					require.NoError(t, err)
					var v *vector.Vector
					if constant {
						switch typ.TypeSize() {
						case 1:
							v, err = vector.NewConstFixed(typ, uint8(1), max(2, count), mp)
						case 2:
							v, err = vector.NewConstFixed(typ, uint16(1), max(2, count), mp)
						case 4:
							v, err = vector.NewConstFixed(typ, uint32(1), max(2, count), mp)
						default:
							v, err = vector.NewConstFixed(typ, uint64(1), max(2, count), mp)
						}
						require.NoError(t, err)
					} else {
						v = vector.NewVec(typ)
						for i := range max(2, count) {
							n := uint64(i + 1)
							switch typ.TypeSize() {
							case 1:
								err = vector.AppendFixed(v, uint8(n), false, mp)
							case 2:
								err = vector.AppendFixed(v, uint16(n), false, mp)
							case 4:
								err = vector.AppendFixed(v, uint32(n), false, mp)
							default:
								err = vector.AppendFixed(v, n, false, mp)
							}
							require.NoError(t, err)
						}
					}
					t.Cleanup(func() { v.Free(mp); h.Free(); require.Zero(t, mp.CurrNB()) })
					itr := h.NewIterator()
					itr.ensureCapacity(count)
					// A spare physical slot makes the old eight-byte offset error
					// observable by a canary, without writing outside this object.
					itr.keys = make([]uint64, count, count+1)
					vecs := []*vector.Vector{v}
					ids, _, err := itr.Insert(0, count, vecs)
					require.NoError(t, err)
					want := append([]uint64(nil), ids...)
					ids, _, err = itr.Find(0, count, vecs)
					require.NoError(t, err)
					require.Equal(t, want, ids)
					const canary = uint64(0x8badf00dfeed1234)
					itr.keys[:cap(itr.keys)][count] = canary
					ids, _, err = itr.Insert(0, count, vecs)
					require.NoError(t, err)
					require.Equal(t, canary, itr.keys[:cap(itr.keys)][count], "encoding must not consume an inactive key slot")
					require.Equal(t, want, ids, "Find must not change the next Insert's key encoding")
					_, _, err = itr.Find(0, 1, vecs)
					require.NoError(t, err)
					isNew, err := itr.DetectDup(vecs, 0)
					require.NoError(t, err)
					require.False(t, isNew)
					ids, _, err = itr.Find(0, count, vecs)
					require.NoError(t, err)
					require.Equal(t, want, ids)
				})
			}
		}
	}
}

func TestIntIteratorFindInsertKeepsCanaryAfterNullableGuard(t *testing.T) {
	mp := mpool.MustNewZero()
	h, err := NewIntHashMap(true, mp)
	require.NoError(t, err)
	v, err := vector.NewConstFixed(types.T_uint64.ToType(), uint64(0x123456789abcdef), 1, mp)
	require.NoError(t, err)
	t.Cleanup(func() { v.Free(mp); h.Free(); require.Zero(t, mp.CurrNB()) })
	itr := h.NewIterator()
	itr.ensureCapacity(1)
	itr.keys = make([]uint64, 2, 3) // one active key, its guard, then a canary.
	vecs := []*vector.Vector{v}
	_, _, err = itr.Insert(0, 1, vecs)
	require.NoError(t, err)
	_, _, err = itr.Find(0, 1, vecs)
	require.NoError(t, err)
	const canary = uint64(0x8badf00dfeed1234)
	itr.keys[:cap(itr.keys)][2] = canary
	isNew, err := itr.DetectDup(vecs, 0)
	require.NoError(t, err)
	require.False(t, isNew)
	require.Equal(t, canary, itr.keys[:cap(itr.keys)][2])
}

func TestIntIteratorReuseAcrossZeroInvalidAndOwnerChange(t *testing.T) {
	mp := mpool.MustNewZero()
	nonNull, err := NewIntHashMap(false, mp)
	require.NoError(t, err)
	nullable, err := NewIntHashMap(true, mp)
	require.NoError(t, err)
	v := vector.NewVec(types.T_uint16.ToType())
	for i, n := range []uint16{7, 9, 0} {
		require.NoError(t, vector.AppendFixed(v, n, i == 2, mp))
	}
	t.Cleanup(func() { v.Free(mp); nonNull.Free(); nullable.Free(); require.Zero(t, mp.CurrNB()) })
	itr := nonNull.NewIterator()
	itr.ensureCapacity(2) // owner changes must not hide the defect by growing.
	vecs := []*vector.Vector{v}
	_, _, err = itr.Insert(0, 1, vecs)
	require.NoError(t, err)
	_, _, err = itr.Find(0, 1, vecs)
	require.NoError(t, err)
	ids, zs, err := itr.Insert(1, 0, vecs)
	require.NoError(t, err)
	require.Empty(t, ids)
	require.Empty(t, zs)
	_, _, err = itr.Insert(-1, 1, vecs)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	ids, _, err = itr.Insert(1, 1, vecs)
	require.NoError(t, err)
	require.Equal(t, []uint64{2}, ids)
	ids, _, err = itr.Find(1, 1, vecs)
	require.NoError(t, err)
	require.Equal(t, []uint64{2}, ids)
	IteratorChangeOwner(itr, nullable)
	ids, _, err = itr.Insert(2, 1, vecs)
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, ids)
	ids, _, err = itr.Find(2, 1, vecs)
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, ids)
	IteratorChangeOwner(itr, nonNull)
	ids, _, err = itr.Insert(1, 1, vecs)
	require.NoError(t, err)
	require.Equal(t, []uint64{2}, ids)
}

func TestIntIteratorReuseAfterAdmissionFailure(t *testing.T) {
	mp := mpool.MustNewZero()
	h, err := NewIntHashMap(false, mp)
	require.NoError(t, err)
	v := vector.NewVec(types.T_uint64.ToType())
	rows := int(h.hashMap.PlanResize(1).CurrentCellCount) + UnitLimit
	for n := range rows {
		require.NoError(t, vector.AppendFixed(v, uint64(n+1), false, mp))
	}
	t.Cleanup(func() { v.Free(mp); h.Free(); require.Zero(t, mp.CurrNB()) })
	h.SetResizeAdmission(func(hashtable.ResizePlan) (hashtable.ResizeReservation, error) {
		return nil, mpool.ErrAllocationAccountCapacity
	})
	itr := h.NewIterator()
	itr.ensureCapacity(UnitLimit)
	itr.keys = make([]uint64, UnitLimit, UnitLimit+1)
	failedAt := -1
	vecs := []*vector.Vector{v}
	for start := 0; start+UnitLimit <= rows; start += UnitLimit {
		_, _, err = itr.Find(start, UnitLimit, vecs)
		require.NoError(t, err)
		_, _, err = itr.Insert(start, UnitLimit, vecs)
		if err != nil {
			require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
			failedAt = start
			break
		}
	}
	require.NotEqual(t, -1, failedAt)
	for _, off := range itr.keyOffs {
		require.Zero(t, off, "failed encoding must release its scratch state")
	}
	h.SetResizeAdmission(nil)
	ids, _, err := itr.Insert(failedAt, UnitLimit, vecs)
	require.NoError(t, err)
	for n, id := range ids {
		require.Equal(t, uint64(failedAt+n+1), id)
	}
}
