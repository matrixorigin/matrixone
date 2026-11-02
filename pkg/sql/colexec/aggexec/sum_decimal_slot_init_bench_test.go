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

package aggexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func BenchmarkDecimal64GroupedUpdates(b *testing.B) {
	for _, tc := range []struct {
		name                     string
		cardinality, rows, chunk int
		order                    string
		avg                      bool
	}{
		{"groups_4", 4, 8192, hashmap.UnitLimit, "sequential", false},
		{"groups_254", 254, 8192, hashmap.UnitLimit, "sequential", false},
		{"groups_255", 255, 8192, hashmap.UnitLimit, "sequential", false},
		{"groups_256", 256, 8192, hashmap.UnitLimit, "sequential", false},
		{"sequential", 8192, 8192, hashmap.UnitLimit, "sequential", false},
		{"permuted", 8192, 8192, hashmap.UnitLimit, "permuted", false},
		{"endpoint", 8192, 8192, hashmap.UnitLimit, "endpoint", false},
		{"sparse_repeat", AggBatchSize + 2, 8192, hashmap.UnitLimit, "sparse", false},
		{"large_sequential", 8192, 8192, 8192, "sequential", false},
		{"large_permuted", 8192, 8192, 8192, "permuted", false},
		{"nullable", 256, 8192, hashmap.UnitLimit, "nullable", false},
		{"constant", 256, 8192, hashmap.UnitLimit, "constant", false},
		{"const_null", 256, 8192, hashmap.UnitLimit, "const_null", false},
		{"tiny", 4, 1, hashmap.UnitLimit, "sequential", false},
		{"empty", 4, 0, hashmap.UnitLimit, "sequential", false},
		{"avg_low", 4, 8192, hashmap.UnitLimit, "sequential", true},
		{"avg_sparse", AggBatchSize + 2, 8192, hashmap.UnitLimit, "sparse", true},
		{"warm_prefix_sparse", AggBatchSize + 2, 8192, hashmap.UnitLimit, "warm-sparse", false},
		{"late_transition", AggBatchSize + 2, 8192, hashmap.UnitLimit, "late", false},
		{"many_chunks_sparse", 64*AggBatchSize + 4, 8192, hashmap.UnitLimit, "many-sparse", false},
		{"unmatched_prefix", AggBatchSize + 2, 8192, hashmap.UnitLimit, "unmatched-prefix", false},
		{"all_unmatched", 4, 8192, hashmap.UnitLimit, "unmatched", false},
	} {
		b.Run(tc.name, func(b *testing.B) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			param := types.New(types.T_decimal64, 15, 2)
			var input *vector.Vector
			if tc.order == "constant" {
				var err error
				input, err = vector.NewConstFixed(param, types.Decimal64(1), tc.rows+2, mp)
				if err != nil {
					b.Fatal(err)
				}
			} else if tc.order == "const_null" {
				input = vector.NewConstNull(param, tc.rows+2, mp)
			} else {
				input = vector.NewVec(param)
			}
			defer input.Free(mp)
			if !input.IsConst() {
				for row := range tc.rows + 2 {
					if err := vector.AppendFixed(input, types.Decimal64(1), tc.order == "nullable" && row%17 == 0, mp); err != nil {
						b.Fatal(err)
					}
				}
			}
			groups := make([]uint64, tc.rows)
			for i := range groups {
				group := i%tc.cardinality + 1
				switch tc.order {
				case "permuted":
					group = (1999*i)%tc.cardinality + 1
				case "endpoint":
					group = i%hashmap.UnitLimit + 1
					if group == hashmap.UnitLimit {
						group = tc.cardinality
					}
				case "sparse":
					group = i%4 + 1
					if i%2 == 0 {
						group = tc.cardinality
					}
				}
				if tc.order == "warm-sparse" {
					group = i%4 + 1
					if i%hashmap.UnitLimit >= 4 && i%2 == 0 {
						group = tc.cardinality
					}
				}
				if tc.order == "late" {
					group = i%4 + 1
					if i%hashmap.UnitLimit == hashmap.UnitLimit-1 {
						group = tc.cardinality
					}
				}
				if tc.order == "many-sparse" {
					group = (i%64)*AggBatchSize + 1
				}
				if tc.order == "unmatched-prefix" {
					group = 0
					if i%hashmap.UnitLimit >= hashmap.UnitLimit-4 {
						group = i%4 + 1
						if i%2 == 0 {
							group = tc.cardinality
						}
					}
				}
				if tc.order == "unmatched" {
					group = GroupNotMatched
				}
				groups[i] = uint64(group)
			}
			id := AggIdOfSum
			if tc.avg {
				id = AggIdOfAvg
			}
			exec, err := MakeAgg(mp, id, false, param)
			if err != nil {
				b.Fatal(err)
			}
			defer exec.Free()
			if _, ok := exec.(*sumDecimal64FastExec); !ok {
				b.Fatalf("wrong executor: %T", exec)
			}
			if err := exec.GroupGrow(tc.cardinality); err != nil {
				b.Fatal(err)
			}
			vectors := []*vector.Vector{input}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if len(groups) == 0 {
					if err := exec.BatchFill(2, groups, vectors); err != nil {
						b.Fatal(err)
					}
				}
				for offset := 0; offset < len(groups); offset += tc.chunk {
					if err := exec.BatchFill(2+offset, groups[offset:min(offset+tc.chunk, len(groups))], vectors); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
