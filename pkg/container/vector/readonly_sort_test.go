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

package vector

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func readonlySortArrayBytes[T types.ArrayElement](head T, length int) []byte {
	values := make([]T, length)
	values[0] = head
	return types.ArrayToBytes(values)
}

func TestReadonlySortAndCompact(t *testing.T) {
	families := []struct {
		name      string
		typ       types.Type
		low, high []byte
	}{
		{"text-area", types.T_text.ToType(), bytes.Repeat([]byte{'a'}, 64), bytes.Repeat([]byte{'b'}, 64)},
		{"text-inline", types.T_text.ToType(), []byte("a"), []byte("b")},
		{"text-mixed", types.T_text.ToType(), []byte("a"), bytes.Repeat([]byte{'b'}, 64)},
		{"float32", types.T_array_float32.ToType(), readonlySortArrayBytes(float32(-1), 8), readonlySortArrayBytes(float32(2), 8)},
		{"float64", types.T_array_float64.ToType(), readonlySortArrayBytes(float64(-1), 4), readonlySortArrayBytes(float64(2), 4)},
		{"bf16", types.T_array_bf16.ToType(), readonlySortArrayBytes(types.BF16(0x3f80), 16), readonlySortArrayBytes(types.BF16(0x4000), 16)},
		{"float16", types.T_array_float16.ToType(), readonlySortArrayBytes(types.Float16(0x3c00), 16), readonlySortArrayBytes(types.Float16(0x4000), 16)},
		{"int8", types.T_array_int8.ToType(), readonlySortArrayBytes(int8(-1), 24), readonlySortArrayBytes(int8(2), 24)},
		{"uint8", types.T_array_uint8.ToType(), readonlySortArrayBytes(uint8(1), 24), readonlySortArrayBytes(uint8(2), 24)},
	}
	for _, family := range families {
		for _, shape := range []struct {
			name         string
			values, want [][]byte
		}{
			{"duplicates", [][]byte{family.high, family.low, family.low}, [][]byte{family.low, family.high}},
			{"unique", [][]byte{family.high, family.low}, [][]byte{family.low, family.high}},
			{"all-same", [][]byte{family.low, family.low}, [][]byte{family.low}},
			{"single", [][]byte{family.low}, [][]byte{family.low}},
			{"empty", nil, nil},
		} {
			for _, leased := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/leased=%t", family.name, shape.name, leased), func(t *testing.T) {
					mp := mpool.MustNewZero()
					t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
					controller := &readonlyWriteController{}
					state := readonlyWriteAccount(t, controller)
					source := NewOffHeapVecWithType(family.typ)
					t.Cleanup(func() { source.Free(mp) })
					for _, value := range shape.values {
						require.NoError(t, AppendBytes(source, value, false, mp))
					}
					destination := newAccountedTestVector(t, family.typ, state.selection)
					t.Cleanup(func() { destination.Free(mp) })
					if leased {
						require.NoError(t, destination.CopyWithReadonlyArea(source, mp))
					} else {
						require.NoError(t, destination.UnionBatch(source, 0, source.Length(), nil, mp))
					}
					var snapshot *Vector
					var err error
					if destination.HasBorrowedBacking() {
						snapshot, err = destination.RetainedReadonlyViewWithMP(mp)
					} else {
						snapshot, err = destination.CloneWindowWithAllocation(0, destination.Length(), mp, state.selection)
					}
					require.NoError(t, err)
					t.Cleanup(func() { snapshot.Free(mp) })
					data, area, lease := destination.data, destination.area, destination.areaLease
					beforeMP, beforeAccount, calls := mp.CurrNB(), state.account.Snapshot().Used, controller.calls
					controller.failAt = calls + 1
					require.NoError(t, destination.InplaceSortAndCompact(nil))
					require.Equal(t, len(shape.want), destination.Length())
					require.True(t, destination.GetSorted())
					for row, value := range shape.want {
						require.Equal(t, value, destination.GetBytesAt(row))
					}
					require.Equal(t, len(data), len(destination.data))
					require.Equal(t, cap(data), cap(destination.data))
					if cap(data) > 0 {
						require.True(t, &data[:cap(data)][0] == &destination.data[:cap(destination.data)][0], "descriptor backing must stay in place")
					}
					require.Equal(t, len(area), len(destination.area))
					require.Equal(t, cap(area), cap(destination.area))
					if cap(area) > 0 {
						require.True(t, &area[:cap(area)][0] == &destination.area[:cap(destination.area)][0], "readonly payload must stay in place")
					}
					require.True(t, lease == destination.areaLease, "compaction must retain the same lease")
					require.Equal(t, beforeMP, mp.CurrNB())
					require.Equal(t, beforeAccount, state.account.Snapshot().Used)
					require.Equal(t, calls, controller.calls)
					require.False(t, controller.rejected)
					if len(shape.want) < len(shape.values) {
						require.False(t, destination.areaDisjoint)
					}
					require.Equal(t, len(shape.values), snapshot.Length())
					for row, value := range shape.values {
						require.Equal(t, value, source.GetBytesAt(row))
						require.Equal(t, value, snapshot.GetBytesAt(row))
					}
					// Repeated compaction is idempotent and still cannot allocate.
					require.NoError(t, destination.InplaceSortAndCompact(nil))
					require.Equal(t, len(shape.want), destination.Length())
					require.Equal(t, calls, controller.calls)
					for row, value := range shape.want {
						require.Equal(t, value, destination.GetBytesAt(row))
					}
				})
			}
		}
	}
}
