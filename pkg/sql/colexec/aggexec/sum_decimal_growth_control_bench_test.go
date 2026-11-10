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

package aggexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func BenchmarkDecimal64GrowthAndUpdates(b *testing.B) {
	for _, tc := range []struct {
		name string
		rows int
		grow bool
	}{
		{"repeated_4", AggBatchSize, false},
		{"growing_one_chunk", AggBatchSize, true},
		{"growing_two_chunks", AggBatchSize + hashmap.UnitLimit, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			typ := types.New(types.T_decimal64, 18, 0)
			input := vector.NewVec(typ)
			defer input.Free(mp)
			groups := make([]uint64, tc.rows)
			for i := range groups {
				if err := vector.AppendFixed(input, types.Decimal64(i%17-8), false, mp); err != nil {
					b.Fatal(err)
				}
				if tc.grow {
					groups[i] = uint64(i + 1)
				} else {
					groups[i] = uint64(i%4 + 1)
				}
			}
			vectors := []*vector.Vector{input}
			run := func() {
				exec := newSumDecimal64FastExec(mp, true, AggIdOfSum, false, typ)
				defer exec.Free()
				if !tc.grow {
					if err := exec.GroupGrow(4); err != nil {
						b.Fatal(err)
					}
				}
				for offset := 0; offset < len(groups); offset += hashmap.UnitLimit {
					end := min(offset+hashmap.UnitLimit, len(groups))
					if tc.grow {
						if err := exec.GroupGrow(end - offset); err != nil {
							b.Fatal(err)
						}
					}
					if err := exec.BatchFill(offset, groups[offset:end], vectors); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				run()
			}
			b.StopTimer()
		})
	}
}
