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

func BenchmarkDecimal64DistinctBatchUpdates(b *testing.B) {
	for _, tc := range []struct {
		name   string
		groups int
		null   bool
	}{
		{"one_group", 1, false},
		{"four_groups", 4, false},
		{"fixed_index", 1024, false},
		{"two_chunks", AggBatchSize + 2, false},
		{"constant_null", 4, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			typ := types.New(types.T_decimal64, 12, 2)
			const rows = AggBatchSize
			var input *vector.Vector
			if tc.null {
				input = vector.NewConstNull(typ, rows, mp)
			} else {
				input = vector.NewVec(typ)
			}
			defer input.Free(mp)
			groups := make([]uint64, rows)
			for i := range groups {
				if !tc.null {
					if err := vector.AppendFixed(input, types.Decimal64(i%17-8), false, mp); err != nil {
						b.Fatal(err)
					}
				}
				groups[i] = uint64(i%min(tc.groups, 4) + 1)
				if tc.groups > AggBatchSize && i%2 == 0 {
					groups[i] = uint64(tc.groups)
				}
			}
			exec := newSumDecimal64FastExec(mp, true, AggIdOfSum, true, typ)
			defer exec.Free()
			if err := exec.GroupGrow(tc.groups); err != nil {
				b.Fatal(err)
			}
			vectors := []*vector.Vector{input}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for offset := 0; offset < len(groups); offset += hashmap.UnitLimit {
					if err := exec.BatchFill(offset, groups[offset:min(offset+hashmap.UnitLimit, len(groups))], vectors); err != nil {
						b.Fatal(err)
					}
				}
			}
			b.StopTimer()
		})
	}
}
