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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"testing"
)

func BenchmarkDecimalContiguousReduction(b *testing.B) {
	benchmarkDecimalReduction(b, false)
}

func BenchmarkDecimalUniformBatch(b *testing.B) {
	benchmarkDecimalReduction(b, true)
}

func benchmarkDecimalReduction(b *testing.B, batch bool) {
	oids := []types.T{types.T_decimal64, types.T_decimal128, types.T_decimal256}
	shapes := []string{"flat", "null", "wide"}
	if batch {
		oids = append(oids, types.T_int64)
		shapes = append(shapes, "mixed", "late", "unmatched")
	}
	for _, oid := range oids {
		widths := []int32{15}
		if oid == types.T_decimal128 {
			widths = []int32{38}
			if batch {
				widths = []int32{18, 38}
			}
		}
		if oid == types.T_decimal256 {
			widths = []int32{65}
		}
		for _, width := range widths {
			for _, n := range []int{1, 256, 8192} {
				for _, shape := range shapes {
					if shape == "wide" && (oid == types.T_int64 || width < 38) {
						continue
					}
					name := fmt.Sprintf("%s/%d/%s", oid.String(), n, shape)
					if batch {
						name = fmt.Sprintf("%s/p%d/%d/%s", oid.String(), width, n, shape)
					}
					b.Run(name, func(b *testing.B) {
						mp := mpool.MustNewZero()
						defer mpool.DeleteMPool(mp)
						typ := types.New(oid, width, 2)
						if oid == types.T_int64 {
							typ = oid.ToType()
						}
						v := vector.NewVec(typ)
						defer v.Free(mp)
						for i := 0; i < n; i++ {
							value := int64(i%7 - 3)
							null := shape == "null" && i%17 == 0
							var err error
							switch oid {
							case types.T_int64:
								err = vector.AppendFixed(v, value, null, mp)
							case types.T_decimal64:
								err = vector.AppendFixed(v, types.Decimal64(value), null, mp)
							case types.T_decimal128:
								x := types.Decimal128FromInt64(value)
								if shape == "wide" {
									x = types.Decimal128{B64_127: 1}
								}
								err = vector.AppendFixed(v, x, null, mp)
							case types.T_decimal256:
								x := types.Decimal256FromInt64(value)
								if shape == "wide" {
									x = types.Decimal256{B128_191: 1}
								}
								err = vector.AppendFixed(v, x, null, mp)
							}
							if err != nil {
								b.Fatal(err)
							}
						}
						exec := makeSumAvgExec(mp, true, AggIdOfSum, false, typ)
						defer exec.Free()
						if err := exec.GroupGrow(4); err != nil {
							b.Fatal(err)
						}
						input := []*vector.Vector{v}
						groups := make([]uint64, n)
						for i := range groups {
							groups[i] = 1
							if shape == "mixed" {
								groups[i] = uint64(i%4 + 1)
							}
						}
						if shape == "late" {
							groups[n-1] = 2
						}
						if shape == "unmatched" {
							groups[n-1] = GroupNotMatched
						}
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							var err error
							if batch {
								err = exec.BatchFill(0, groups, input)
							} else {
								err = exec.BulkFill(0, input)
							}
							if err != nil {
								b.Fatal(err)
							}
						}
					})
				}
			}
		}
	}
}
