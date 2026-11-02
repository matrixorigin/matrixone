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
	"encoding/binary"
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestStrHashMapFlatFixedCompositeKeyGrammar(t *testing.T) {
	for _, tc := range []struct {
		name          string
		hasNull       bool
		groupingAware bool
		wide          bool
		many          bool
	}{
		{name: "join-padded"},
		{name: "nullable-padded", hasNull: true},
		{name: "nullable-mixed-width", hasNull: true, wide: true},
		{name: "grouping-domain-mixed-width", groupingAware: true, wide: true},
		{name: "max-fixed-columns", hasNull: true, many: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			m, err := NewStrHashMap(tc.hasNull, mp)
			require.NoError(t, err)
			t.Cleanup(m.Free)
			if tc.groupingAware {
				require.NoError(t, m.SetGroupingAware())
			}
			first := []int32{99, 1, 2, 1, 3}
			second := []int32{99, 10, 20, 10, 30}
			third := []int64{999, 100, 200, 100, 300}
			vecs := []*vector.Vector{vector.NewVec(types.T_int32.ToType()), vector.NewVec(types.T_int32.ToType())}
			if tc.wide {
				vecs = append(vecs, vector.NewVec(types.T_int64.ToType()))
			}
			if tc.many {
				for len(vecs) < 16 {
					vecs = append(vecs, vector.NewVec(types.T_int32.ToType()))
				}
			}
			for _, vec := range vecs {
				t.Cleanup(func() { vec.Free(mp) })
			}
			require.NoError(t, vector.AppendFixedList(vecs[0], first, nil, mp))
			require.NoError(t, vector.AppendFixedList(vecs[1], second, nil, mp))
			if tc.wide {
				require.NoError(t, vector.AppendFixedList(vecs[2], third, nil, mp))
			}
			if tc.many {
				for _, vec := range vecs[2:] {
					require.NoError(t, vector.AppendFixedList(vec, second, nil, mp))
				}
			}
			itr := m.NewIterator().(*strHashmapIterator)
			require.NoError(t, itr.prepareHashKeys(vecs, 1, 4))
			require.True(t, itr.encodeFlatFixedKeyRows(vecs, 1, 4))
			for row := 0; row < 4; row++ {
				var want []byte
				if tc.hasNull || tc.groupingAware {
					want = append(want, 0)
				}
				want = binary.NativeEndian.AppendUint32(want, uint32(first[row+1]))
				if tc.hasNull || tc.groupingAware {
					want = append(want, 0)
				}
				want = binary.NativeEndian.AppendUint32(want, uint32(second[row+1]))
				if tc.wide {
					if tc.hasNull || tc.groupingAware {
						want = append(want, 0)
					}
					want = binary.NativeEndian.AppendUint64(want, uint64(third[row+1]))
				}
				if tc.many {
					for range vecs[2:] {
						want = append(want, 0)
						want = binary.NativeEndian.AppendUint32(want, uint32(second[row+1]))
					}
				}
				if len(want) < 16 {
					want = append(want, make([]byte, 16-len(want))...)
				}
				require.Equal(t, want, itr.keys[row], "row %d", row)
			}
			transactional := m.NewTransactionalIterator()
			var plan InsertPlan
			require.NoError(t, transactional.PreviewInsert(1, 4, vecs, 0, &plan))
			require.Equal(t, uint64(3), plan.NewGroups())
			ids, zs, err := transactional.CommitPreview(&plan)
			require.NoError(t, err)
			require.Equal(t, []uint64{1, 2, 1, 3}, ids)
			require.Equal(t, []int64{1, 1, 1, 1}, zs)
			ids, zs, err = itr.Find(1, 4, vecs)
			require.NoError(t, err)
			require.Equal(t, []uint64{1, 2, 1, 3}, ids)
			require.Equal(t, []int64{1, 1, 1, 1}, zs)
			ids, zs, err = itr.Insert(1, 4, vecs)
			require.NoError(t, err)
			require.Equal(t, []uint64{1, 2, 1, 3}, ids)
			require.Equal(t, []int64{1, 1, 1, 1}, zs)
			require.Equal(t, uint64(3), m.GroupCount())
		})
	}
	t.Run("full-batch-iterator", func(t *testing.T) {
		mp := mpool.MustNewZero()
		t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
		m, err := NewStrHashMap(true, mp)
		require.NoError(t, err)
		t.Cleanup(m.Free)
		vecs := []*vector.Vector{vector.NewVec(types.T_int32.ToType()), vector.NewVec(types.T_int32.ToType())}
		for _, vec := range vecs {
			t.Cleanup(func() { vec.Free(mp) })
		}
		first, second := make([]int32, UnitLimit+1), make([]int32, UnitLimit+1)
		want := make([]uint64, UnitLimit)
		for row := range UnitLimit {
			first[row+1] = int32(row % 3)
			second[row+1] = int32(row%3) * 10
			want[row] = uint64(row%3 + 1)
		}
		require.NoError(t, vector.AppendFixedList(vecs[0], first, nil, mp))
		require.NoError(t, vector.AppendFixedList(vecs[1], second, nil, mp))
		transactional := m.NewTransactionalIterator()
		var plan InsertPlan
		require.NoError(t, transactional.PreviewInsert(1, UnitLimit, vecs, 0, &plan))
		require.Equal(t, uint64(3), plan.NewGroups())
		ids, _, err := transactional.CommitPreview(&plan)
		require.NoError(t, err)
		require.Equal(t, want, ids)
		itr := m.NewIterator()
		ids, _, err = itr.Find(1, UnitLimit, vecs)
		require.NoError(t, err)
		require.Equal(t, want, ids)
		ids, _, err = itr.Insert(1, UnitLimit, vecs)
		require.NoError(t, err)
		require.Equal(t, want, ids)
		require.Equal(t, uint64(3), m.GroupCount())
	})
}

func TestStrHashMapFlatFixedCompositeFallbacks(t *testing.T) {
	for _, shape := range []string{"null-in-range", "null-outside", "constant", "grouping", "float", "varlen", "single", "wide"} {
		t.Run(shape, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			m, err := NewStrHashMap(true, mp)
			require.NoError(t, err)
			t.Cleanup(m.Free)
			if shape == "grouping" {
				require.NoError(t, m.SetGroupingAware())
			}
			first := vector.NewVec(types.T_int32.ToType())
			if shape == "float" {
				first = vector.NewVec(types.T_float64.ToType())
			} else if shape == "varlen" {
				first = vector.NewVec(types.T_varchar.ToType())
			} else if shape == "constant" {
				first, err = vector.NewConstFixed(types.T_int32.ToType(), int32(1), 3, mp)
				require.NoError(t, err)
			}
			vecs := []*vector.Vector{first}
			t.Cleanup(func() { first.Free(mp) })
			switch shape {
			case "constant":
			case "float":
				require.NoError(t, vector.AppendFixedList(first, []float64{math.Float64frombits(1 << 63), 0, 0}, nil, mp))
			case "varlen":
				for range 3 {
					require.NoError(t, vector.AppendBytes(first, []byte("one"), false, mp))
				}
			default:
				require.NoError(t, vector.AppendFixedList(first, []int32{1, 1, 1}, nil, mp))
			}
			if shape == "null-in-range" {
				first.GetNulls().Add(0)
			} else if shape == "null-outside" {
				first.GetNulls().Add(2)
			} else if shape == "grouping" {
				first.GetGrouping().Add(0)
			}
			columnCount := 2
			if shape == "single" {
				columnCount = 1
			} else if shape == "wide" {
				columnCount = 17
			}
			for len(vecs) < columnCount {
				vec := vector.NewVec(types.T_int32.ToType())
				t.Cleanup(func() { vec.Free(mp) })
				require.NoError(t, vector.AppendFixedList(vec, []int32{7, 7, 7}, nil, mp))
				vecs = append(vecs, vec)
			}
			itr := m.NewIterator().(*strHashmapIterator)
			require.NoError(t, itr.prepareHashKeys(vecs, 0, 2))
			require.False(t, itr.encodeFlatFixedKeyRows(vecs, 0, 2))
			for _, key := range itr.keys[:2] {
				require.Empty(t, key, "fallback must not publish a partial key")
			}
			ids, zs, err := itr.Insert(0, 2, vecs)
			require.NoError(t, err)
			want := []uint64{1, 1}
			if shape == "null-in-range" || shape == "grouping" {
				want = []uint64{1, 2}
			}
			require.Equal(t, want, ids)
			require.Equal(t, []int64{1, 1}, zs)
			ids, zs, err = itr.Find(0, 2, vecs)
			require.NoError(t, err)
			require.Equal(t, want, ids)
			require.Equal(t, []int64{1, 1}, zs)
		})
	}
}

func TestStrHashMapFlatRawStringBatchGrammar(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		char                    bool
		hasNull, groupingAware  bool
		nullInside, nullOutside bool
	}{
		{name: "join-composite-offset"},
		{name: "nullable-bitmap-outside", hasNull: true, nullOutside: true},
		{name: "nullable-inside-outside", hasNull: true, nullInside: true, nullOutside: true},
		{name: "join-null-skip", nullInside: true},
		{name: "grouping-aware-null-skip", groupingAware: true, nullInside: true},
		{name: "nullable-grouping-aware", hasNull: true, groupingAware: true, nullInside: true},
		{name: "char-join-composite-offset", char: true},
		{name: "char-nullable-inside-outside", char: true, hasNull: true, nullInside: true, nullOutside: true},
		{name: "char-grouping-aware-null-skip", char: true, groupingAware: true, nullInside: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			m, err := NewStrHashMap(tc.hasNull, mp)
			require.NoError(t, err)
			t.Cleanup(m.Free)
			if tc.groupingAware {
				require.NoError(t, m.SetGroupingAware())
			}
			wide := strings.Repeat("w", 65536)
			values := []string{"prefix", "", "a\x00b  ", strings.Repeat("i", 23), strings.Repeat("a", 24), wide, wide, "last", "suffix"}
			oid := types.T_varchar
			if tc.char {
				oid = types.T_char
				values = []string{"prefix", "   ", " a\x00b  ", strings.Repeat("i", 23) + " ", strings.Repeat("a", 24) + "  ", wide + "   ", wide, "last\t ", "suffix"}
			}
			vecs := []*vector.Vector{vector.NewVec(oid.ToType()), vector.NewVec(types.T_binary.ToType())}
			for column, vec := range vecs {
				t.Cleanup(func() { vec.Free(mp) })
				for _, value := range values {
					if column == 1 {
						value = "\x00\xff"
					}
					require.NoError(t, vector.AppendBytes(vec, []byte(value), false, mp))
				}
			}
			if tc.nullOutside {
				vecs[0].GetNulls().Add(0)
				vecs[0].GetNulls().Add(8)
			}
			if tc.nullInside {
				vecs[0].GetNulls().Add(7)
				physical, _ := vector.MustVarlenaRawData(vecs[0])
				// This descriptor is invalid if read. NULL must be checked first
				// in both the sizing and value passes.
				physical[7].SetOffsetLen(1<<30, 65536)
			}
			itr := m.NewIterator().(*strHashmapIterator)
			require.NoError(t, itr.prepareHashKeys(vecs, 1, 7))
			copy(itr.zValues[:7], OneInt64s[:7])
			itr.encodeHashKeys(vecs, 1, 7)
			for row := 0; row < 7; row++ {
				var want []byte
				for column := range vecs {
					if column == 0 && row == 6 && tc.nullInside {
						if tc.hasNull {
							want = append(want, 1)
						}
						continue
					}
					if tc.hasNull || tc.groupingAware {
						want = append(want, 0)
					}
					value := values[row+1]
					if column == 1 {
						value = "\x00\xff"
					} else if tc.char {
						value = strings.TrimRight(value, " ")
					}
					var size [4]byte
					binary.LittleEndian.PutUint32(size[:], uint32(len(value)))
					want = append(want, size[:]...)
					want = append(want, value...)
				}
				if len(want) < 16 {
					want = append(want, make([]byte, 16-len(want))...)
				}
				require.Equal(t, want, itr.keys[row], "row %d encoded grammar", row)
				require.Equal(t, len(want), cap(itr.keys[row]), "row %d sizing", row)
			}
			if tc.char && !tc.nullInside {
				transactional := m.NewTransactionalIterator()
				var plan InsertPlan
				require.NoError(t, transactional.PreviewInsert(1, 7, vecs, 0, &plan))
				require.Zero(t, m.GroupCount(), "sizing and preview do not publish groups")
				require.Equal(t, uint64(6), plan.NewGroups())
				ids, zs, err := transactional.CommitPreview(&plan)
				require.NoError(t, err)
				require.Equal(t, []uint64{1, 2, 3, 4, 5, 5, 6}, ids)
				require.Equal(t, []int64{1, 1, 1, 1, 1, 1, 1}, zs)
			}
			ids, zs, err := itr.Insert(1, 7, vecs)
			require.NoError(t, err)
			wantIDs := []uint64{1, 2, 3, 4, 5, 5, 6}
			wantZs := []int64{1, 1, 1, 1, 1, 1, 1}
			if tc.nullInside && !tc.hasNull {
				wantIDs[6], wantZs[6] = 0, 0
			}
			for row := range wantIDs {
				// Unmatched join rows carry no published group; their ID scratch
				// is deliberately unspecified by the iterator API.
				if wantZs[row] != 0 {
					require.Equal(t, wantIDs[row], ids[row], "insert row %d", row)
				}
			}
			require.Equal(t, wantZs, zs)
			wantGroups := uint64(6)
			if tc.nullInside && !tc.hasNull {
				wantGroups = 5
			}
			require.Equal(t, wantGroups, m.GroupCount())
			ids, zs, err = itr.Find(1, 7, vecs)
			require.NoError(t, err)
			require.Equal(t, wantIDs, ids)
			require.Equal(t, wantZs, zs)
		})
	}
}

func TestStrHashMapFlatCharFallbacks(t *testing.T) {
	for _, shape := range []string{"const", "grouping", "unicode"} {
		t.Run(shape, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			vec := vector.NewVec(types.T_char.ToType())
			t.Cleanup(func() { vec.Free(mp) })
			values := []string{"A ", "A", "B "}
			want := []uint64{1, 1, 2}
			if shape == "unicode" {
				values[1] = "a"
				vec.SetType(types.NewWithCharset(types.T_char, 8, 0, types.CharsetUTF8MB4UnicodeCI))
			}
			if shape == "const" {
				values = values[:1]
				want = []uint64{1, 1, 1}
			}
			for _, value := range values {
				require.NoError(t, vector.AppendBytes(vec, []byte(value), false, mp))
			}
			if shape == "const" {
				vec.ToConst()
				vec.SetLength(3)
			} else if shape == "grouping" {
				vec.GetGrouping().Add(1)
				want = []uint64{1, 2, 3}
			}
			m, err := NewStrHashMap(true, mp)
			require.NoError(t, err)
			t.Cleanup(m.Free)
			itr := m.NewIterator()
			ids, zs, err := itr.Insert(0, 3, []*vector.Vector{vec})
			require.NoError(t, err)
			require.Equal(t, want, ids)
			require.Equal(t, []int64{1, 1, 1}, zs)
			ids, zs, err = itr.Find(0, 3, []*vector.Vector{vec})
			require.NoError(t, err)
			require.Equal(t, want, ids)
			require.Equal(t, []int64{1, 1, 1}, zs)
		})
	}
}

// The same existing iterator APIs run on base/head. No timing or allocation
// threshold is a UT; CHAR cases include inline and external padded payloads.
func BenchmarkNullableStrHashEncodeFlatString(b *testing.B) {
	for _, tc := range []struct {
		name    string
		oid     types.T
		nullRow int
		value   string
	}{
		{name: "raw-flat", oid: types.T_varchar, nullRow: -1},
		{name: "raw-null-outside", oid: types.T_varchar, nullRow: UnitLimit + 1},
		{name: "raw-null-inside", oid: types.T_varchar, nullRow: 17},
		{name: "char-inline", oid: types.T_char, nullRow: -1, value: "value   "},
		{name: "char-external", oid: types.T_char, nullRow: -1, value: strings.Repeat("x", 96) + "   "},
		{name: "char-null-outside", oid: types.T_char, nullRow: UnitLimit + 1, value: "value   "},
		{name: "char-null-inside", oid: types.T_char, nullRow: 17, value: "value   "},
	} {
		b.Run(tc.name, func(b *testing.B) {
			mp := mpool.MustNewZero()
			m, err := NewStrHashMap(true, mp)
			if err != nil {
				b.Fatal(err)
			}
			defer m.Free()
			vec := vector.NewVec(tc.oid.ToType())
			defer vec.Free(mp)
			value := []byte(tc.value)
			if tc.value == "" {
				value = []byte(strings.Repeat("x", 96))
			}
			for row := 0; row < UnitLimit+2; row++ {
				if err := vector.AppendBytes(vec, value, false, mp); err != nil {
					b.Fatal(err)
				}
			}
			if tc.nullRow >= 0 {
				vec.GetNulls().Add(uint64(tc.nullRow))
			}
			vecs := []*vector.Vector{vec}
			itr := m.NewIterator().(*strHashmapIterator)
			if err := itr.prepareHashKeys(vecs, 1, UnitLimit); err != nil {
				b.Fatal(err)
			}
			payloadRows := UnitLimit
			if tc.nullRow >= 1 && tc.nullRow <= UnitLimit {
				payloadRows--
			}
			b.ReportAllocs()
			b.SetBytes(int64(payloadRows * len(value)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := itr.prepareHashKeys(vecs, 1, UnitLimit); err != nil {
					b.Fatal(err)
				}
				copy(itr.zValues[:UnitLimit], OneInt64s[:])
				itr.encodeHashKeys(vecs, 1, UnitLimit)
			}
		})
	}
}
