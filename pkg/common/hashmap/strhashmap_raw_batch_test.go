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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestStrHashMapFlatRawStringBatchGrammar(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		hasNull, groupingAware  bool
		nullInside, nullOutside bool
	}{
		{name: "join-composite-offset"},
		{name: "nullable-bitmap-outside", hasNull: true, nullOutside: true},
		{name: "nullable-inside-outside", hasNull: true, nullInside: true, nullOutside: true},
		{name: "join-null-skip", nullInside: true},
		{name: "grouping-aware-null-skip", groupingAware: true, nullInside: true},
		{name: "nullable-grouping-aware", hasNull: true, groupingAware: true, nullInside: true},
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
			values := []string{"prefix", "", "a\x00b", strings.Repeat("i", 23), strings.Repeat("a", 24), wide, wide, "last", "suffix"}
			vecs := []*vector.Vector{vector.NewVec(types.T_varchar.ToType()), vector.NewVec(types.T_binary.ToType())}
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

// The same existing iterator APIs run on base/head. CHAR is an explicit
// canonicalization fallback control; no timing or allocation threshold is a UT.
func BenchmarkNullableStrHashEncodeFlatString(b *testing.B) {
	for _, tc := range []struct {
		name    string
		oid     types.T
		nullRow int
	}{
		{name: "raw-flat", oid: types.T_varchar, nullRow: -1},
		{name: "raw-null-outside", oid: types.T_varchar, nullRow: UnitLimit + 1},
		{name: "raw-null-inside", oid: types.T_varchar, nullRow: 17},
		{name: "char-fallback", oid: types.T_char, nullRow: -1},
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
			value := []byte(strings.Repeat("x", 96))
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
