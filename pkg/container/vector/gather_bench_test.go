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
	"fmt"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

const gatherBenchRows = 8192

// gatherBenchSource builds n rows of typ, one row in nullEvery NULL (none when
// nullEvery is 0). Strings are short enough to be inline.
func gatherBenchSource(tb testing.TB, typ types.Type, n, nullEvery int, mp *mpool.MPool) *Vector {
	v := NewVec(typ, mp)
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < n; i++ {
		isNull := nullEvery > 0 && i%nullEvery == 0
		var err error
		switch typ.Oid {
		case types.T_int64:
			err = AppendFixed(v, rng.Int63(), isNull, mp)
		case types.T_int32:
			err = AppendFixed(v, rng.Int31(), isNull, mp)
		case types.T_decimal128:
			err = AppendFixed(v, types.Decimal128{B0_63: rng.Uint64()}, isNull, mp)
		default:
			err = AppendBytes(v, []byte(fmt.Sprintf("s%d", rng.Intn(1000000))), isNull, mp)
		}
		require.NoError(tb, err)
	}
	return v
}

// gatherBenchRowLists are the row lists the RemapList consumers produce: a
// sort permutation, and half the rows kept in order (a TopN or search result).
func gatherBenchRowLists() map[string][]int64 {
	rng := rand.New(rand.NewSource(2))
	perm := make([]int64, gatherBenchRows)
	for i, p := range rng.Perm(gatherBenchRows) {
		perm[i] = int64(p)
	}
	half := make([]int64, 0, gatherBenchRows/2)
	for i := 0; i < gatherBenchRows; i++ {
		if rng.Intn(2) == 0 {
			half = append(half, int64(i))
		}
	}
	return map[string][]int64{"perm": perm, "half": half}
}

var gatherBenchTypes = []struct {
	name      string
	typ       types.Type
	nullEvery int
}{
	{"int64", types.T_int64.ToType(), 0},
	{"int64-null", types.T_int64.ToType(), 10},
	{"int32-null", types.T_int32.ToType(), 10},
	{"decimal128-null", types.T_decimal128.ToType(), 10},
	{"varchar-null", types.T_varchar.ToType(), 10},
}

// BenchmarkUnionRows appends 8192-row sources by a row list into a reused
// destination: the gather every RemapList consumer runs.
func BenchmarkUnionRows(b *testing.B) {
	lists := gatherBenchRowLists()
	for _, tc := range gatherBenchTypes {
		for _, ln := range []string{"perm", "half"} {
			rows := lists[ln]
			b.Run(tc.name+"/"+ln, func(b *testing.B) {
				mp := mpool.MustNewZero()
				src := gatherBenchSource(b, tc.typ, gatherBenchRows, tc.nullEvery, mp)
				dst := NewVec(tc.typ, mp)
				require.NoError(b, dst.Union(src, rows, mp))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					dst.CleanOnlyData()
					if err := dst.Union(src, rows, mp); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
