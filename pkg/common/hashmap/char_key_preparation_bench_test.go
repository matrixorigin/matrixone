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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// Exercise the existing preflight/encoder boundary, including the complete
// sizing pass. Both baseline and candidate compile this same fixture.
func BenchmarkCharKeyPreparation(b *testing.B) {
	for _, shape := range []string{"flags", "high-ndv", "nullable", "all-null", "area", "unicode", "varchar", "constant", "grouping", "tiny", "empty"} {
		b.Run(shape, func(b *testing.B) {
			mp := mpool.MustNewZero()
			m, err := NewStrHashMap(true, mp)
			if err != nil {
				b.Fatal(err)
			}
			defer m.Free()
			n := UnitLimit
			if shape == "tiny" {
				n = 1
			}
			if shape == "empty" {
				n = 0
			}
			typ := types.New(types.T_char, 8, 0)
			if shape == "unicode" {
				typ = types.NewWithCharset(types.T_char, 8, 0, types.CharsetUTF8MB4UnicodeCI)
			}
			if shape == "varchar" {
				typ = types.T_varchar.ToType()
			}
			vecs := make([]*vector.Vector, 2)
			for col := range vecs {
				vec := vector.NewVec(typ)
				vecs[col] = vec
				defer vec.Free(mp)
				if shape == "constant" {
					if err := vector.SetConstBytes(vec, []byte("A  "), n+2, mp); err != nil {
						b.Fatal(err)
					}
					vec.SetClass(vector.CONSTANT)
				} else {
					for row := range n + 2 {
						value := string(rune('A'+(row>>col)&1)) + "  "
						if shape == "high-ndv" {
							value = string(rune('A' + (row/(1+25*col))%26))
						}
						if shape == "area" {
							value = strings.Repeat("x", 96) + value
						}
						if shape == "unicode" {
							value = "é" + value
						}
						isNull := shape == "all-null" || shape == "nullable" && row%17 == 0
						if err := vector.AppendBytes(vec, []byte(value), isNull, mp); err != nil {
							b.Fatal(err)
						}
					}
				}
				if shape == "grouping" {
					vec.GetGrouping().Add(17)
				}
			}
			itr := m.NewIterator().(*strHashmapIterator)
			if err := itr.prepareHashKeys(vecs, 1, n); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := itr.prepareHashKeys(vecs, 1, n); err != nil {
					b.Fatal(err)
				}
				copy(itr.zValues[:n], OneInt64s[:n])
				itr.encodeHashKeys(vecs, 1, n)
			}
		})
	}
}
