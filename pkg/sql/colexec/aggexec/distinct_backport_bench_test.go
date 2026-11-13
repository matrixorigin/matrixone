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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"testing"
)

func BenchmarkCountDistinctBackport(b *testing.B) {
	const rows = 65536
	const groupCount = 128
	for _, merge := range []bool{false, true} {
		name := "fill"
		if merge {
			name = "merge-16"
		}
		b.Run(name, func(b *testing.B) {
			mp := mpool.MustNewZero()
			vec := vector.NewVec(types.T_int64.ToType())
			values := make([]int64, rows)
			groups := make([]uint64, rows)
			for i := range values {
				values[i] = int64(i)
				groups[i] = uint64(i%groupCount + 1)
			}
			if err := vector.AppendFixedList(vec, values, nil, mp); err != nil {
				b.Fatal(err)
			}
			var parts []*countColumnExec
			if merge {
				for i := 0; i < 16; i++ {
					part := newCountColumnExec(mp, 0, true, []types.Type{types.T_int64.ToType()}).(*countColumnExec)
					if err := part.GroupGrow(groupCount); err != nil {
						b.Fatal(err)
					}
					start := i * rows / 16
					if err := part.BatchFill(start, groups[start:start+rows/16], []*vector.Vector{vec}); err != nil {
						b.Fatal(err)
					}
					parts = append(parts, part)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				exec := newCountColumnExec(mp, 0, true, []types.Type{types.T_int64.ToType()}).(*countColumnExec)
				if err := exec.GroupGrow(groupCount); err != nil {
					b.Fatal(err)
				}
				if merge {
					targetGroups := make([]uint64, groupCount)
					for j := range targetGroups {
						targetGroups[j] = uint64(j + 1)
					}
					for _, part := range parts {
						if err := exec.BatchMerge(part, 0, targetGroups); err != nil {
							b.Fatal(err)
						}
					}
				} else {
					for start := 0; start < rows; start += AggBatchSize {
						if err := exec.BatchFill(start, groups[start:start+AggBatchSize], []*vector.Vector{vec}); err != nil {
							b.Fatal(err)
						}
					}
				}
				result, err := exec.Flush()
				if err != nil {
					b.Fatal(err)
				}
				if got := vector.MustFixedColNoTypeCheck[int64](result[0])[0]; got != rows/groupCount {
					b.Fatalf("distinct count=%d", got)
				}
				for _, v := range result {
					v.Free(mp)
				}
				exec.Free()
			}
			b.StopTimer()
			for _, part := range parts {
				part.Free()
			}
			vec.Free(mp)
			if mp.CurrNB() != 0 {
				b.Fatalf("retained bytes=%d", mp.CurrNB())
			}
		})
	}
}
