// Copyright 2021 Matrix Origin
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
	"fmt"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// BenchmarkMaskGather compares gathering the selected rows of one 8192-row
// column by mask (UnionByMask) and by a row list built from it (Union), and
// what building the list costs, at several random densities.
func BenchmarkMaskGather(b *testing.B) {
	const n = 8192
	mp := mpool.MustNewZero()
	cols := []struct {
		name string
		typ  types.Type
	}{
		{"int32", types.T_int32.ToType()},
		{"int64", types.T_int64.ToType()},
		{"varchar10", types.T_varchar.ToType()},
		{"varchar40", types.T_varchar.ToType()},
	}
	for _, c := range cols {
		src := NewVec(c.typ, mp)
		for i := range n {
			var err error
			switch c.name {
			case "int32":
				err = AppendFixed(src, int32(i), i%11 == 0, mp)
			case "int64":
				err = AppendFixed(src, int64(i), false, mp)
			case "varchar10":
				err = AppendBytes(src, fmt.Appendf(nil, "v%09d", i), false, mp)
			default:
				err = AppendBytes(src, fmt.Appendf(nil, "%030d-%09d", 0, i), false, mp)
			}
			require.NoError(b, err)
		}
		for _, pct := range []int{2, 14, 50, 90} {
			rng := rand.New(rand.NewSource(int64(pct)))
			var mask bitmap.Bitmap
			mask.Init(mp)
			require.NoError(b, mask.InitWithSize(n, mp))
			for i := range n {
				if rng.Intn(100) < pct {
					mask.Add(uint64(i))
				}
			}
			rows := bitmap.AppendRows(&mask, make([]int64, 0, n))
			dst := NewVec(c.typ, mp)
			b.Run(fmt.Sprintf("%s/%d%%/mask", c.name, pct), func(b *testing.B) {
				for range b.N {
					dst.CleanOnlyData()
					if err := dst.UnionByMask(src, &mask, mp); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run(fmt.Sprintf("%s/%d%%/rows", c.name, pct), func(b *testing.B) {
				for range b.N {
					dst.CleanOnlyData()
					if err := dst.Union(src, rows, mp); err != nil {
						b.Fatal(err)
					}
				}
			})
			if c.name == "int32" {
				b.Run(fmt.Sprintf("decode/%d%%", pct), func(b *testing.B) {
					for range b.N {
						rows = bitmap.AppendRows(&mask, rows[:0])
					}
				})
			}
		}
	}
}
