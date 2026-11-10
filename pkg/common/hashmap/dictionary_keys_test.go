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

package hashmap

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

func TestDictionaryIntegerHashKeys(t *testing.T) {
	for _, nullable := range []bool{false, true} {
		for _, wide := range []bool{false, true} {
			t.Run(map[bool]string{false: "reject-null", true: "match-null"}[nullable]+map[bool]string{false: "-narrow", true: "-wide"}[wide], func(t *testing.T) {
				mp := mpool.MustNewZero()
				t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
				typ := types.T_int8.ToType()
				if wide {
					typ = types.T_int64.ToType()
					if nullable {
						typ = types.T_int32.ToType()
					}
				}
				source := vector.NewVec(typ)
				t.Cleanup(func() { source.Free(mp) })
				if wide && nullable {
					require.NoError(t, vector.AppendFixedList(source, []int32{10, 20, 0}, []bool{false, false, true}, mp))
				} else if wide {
					require.NoError(t, vector.AppendFixedList(source, []int64{10, 20, 0}, []bool{false, false, true}, mp))
				} else {
					require.NoError(t, vector.AppendFixedList(source, []int8{10, 20, 0}, []bool{false, false, true}, mp))
				}
				table, err := NewIntHashMap(nullable, mp)
				require.NoError(t, err)
				t.Cleanup(table.Free)
				itr := table.NewIterator()
				_, _, err = itr.Insert(0, 3, []*vector.Vector{source})
				require.NoError(t, err)
				base, err := vector.FreezeVector(source, mp, nil)
				require.NoError(t, err)
				t.Cleanup(base.Release)
				rows, err := vector.NewRowSelection([]int64{1, 0, 1, 2}, 3, mp, nil)
				require.NoError(t, err)
				t.Cleanup(rows.Release)
				dict, err := vector.NewDictionaryVector(base, rows, mp, nil)
				require.NoError(t, err)
				t.Cleanup(func() { dict.Free(mp) })
				found, _, err := itr.Find(0, 4, []*vector.Vector{dict})
				require.NoError(t, err)
				last := uint64(0)
				if nullable {
					last = 3
				}
				require.Equal(t, []uint64{2, 1, 2, last}, found)
				found, _, err = itr.Find(1, 3, []*vector.Vector{dict})
				require.NoError(t, err)
				require.Equal(t, []uint64{1, 2, last}, found)
			})
		}
	}
}

func TestDictionaryCompositeStringHashKeys(t *testing.T) {
	for _, nullable := range []bool{false, true} {
		t.Run(map[bool]string{false: "reject-null", true: "match-null"}[nullable], func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			fixed := vector.NewVec(types.T_int64.ToType())
			t.Cleanup(func() { fixed.Free(mp) })
			text := vector.NewVec(types.T_char.ToType())
			t.Cleanup(func() { text.Free(mp) })
			require.NoError(t, vector.AppendFixedList(fixed, []int64{10, 20, 0}, []bool{false, false, true}, mp))
			for _, v := range []string{"a ", "a string longer than inline storage", "unused"} {
				require.NoError(t, vector.AppendBytes(text, []byte(v), false, mp))
			}
			table, err := NewStrHashMap(nullable, mp)
			require.NoError(t, err)
			t.Cleanup(table.Free)
			itr := table.NewIterator()
			_, _, err = itr.Insert(0, 3, []*vector.Vector{fixed, text})
			require.NoError(t, err)
			rows, err := vector.NewRowSelection([]int64{1, 0, 1, 2}, 3, mp, nil)
			require.NoError(t, err)
			t.Cleanup(rows.Release)
			selected := make([]*vector.Vector, 0, 2)
			for _, source := range []*vector.Vector{fixed, text} {
				base, err := vector.FreezeVector(source, mp, nil)
				require.NoError(t, err)
				t.Cleanup(base.Release)
				dict, err := vector.NewDictionaryVector(base, rows, mp, nil)
				require.NoError(t, err)
				t.Cleanup(func() { dict.Free(mp) })
				selected = append(selected, dict)
			}
			found, _, err := itr.Find(0, 4, selected)
			require.NoError(t, err)
			last := uint64(0)
			if nullable {
				last = 3
			}
			require.Equal(t, []uint64{2, 1, 2, last}, found)
			found, _, err = itr.Find(1, 3, selected)
			require.NoError(t, err)
			require.Equal(t, []uint64{1, 2, last}, found)
		})
	}
}

func TestDictionaryFloatHashKeysPreserveZeroAndNaN(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	source := vector.NewVec(types.T_float64.ToType())
	t.Cleanup(func() { source.Free(mp) })
	require.NoError(t, vector.AppendFixedList(source, []float64{math.Copysign(0, -1), 0, math.NaN()}, nil, mp))
	table, err := NewStrHashMap(false, mp)
	require.NoError(t, err)
	t.Cleanup(table.Free)
	require.NoError(t, table.SetRejectNaN())
	itr := table.NewIterator()
	_, _, err = itr.Insert(0, 3, []*vector.Vector{source})
	require.NoError(t, err)
	base, err := vector.FreezeVector(source, mp, nil)
	require.NoError(t, err)
	t.Cleanup(base.Release)
	rows, err := vector.NewRowSelection([]int64{2, 1, 0}, 3, mp, nil)
	require.NoError(t, err)
	t.Cleanup(rows.Release)
	dict, err := vector.NewDictionaryVector(base, rows, mp, nil)
	require.NoError(t, err)
	t.Cleanup(func() { dict.Free(mp) })
	found, _, err := itr.Find(0, 3, []*vector.Vector{dict})
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1, 1}, found)
}
