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

package aggexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// Each op performs 64 work units with identical preallocated destinations.
// Completion isolates reconstruction versus retained state; range isolates
// temporary windows versus original-vector offsets, with one common finalize.
func BenchmarkSingleGroupReuseMechanisms(b *testing.B) {
	const groups = 64
	for _, phase := range []string{"completion", "range"} {
		for _, agg := range []struct {
			name string
			id   int64
		}{{"count", AggIdOfCountColumn}, {"sum", AggIdOfSum}, {"avg", AggIdOfAvg}} {
			modes := []string{"reconstruct", "reuse"}
			if phase == "range" {
				modes = []string{"window", "range"}
			}
			for _, mode := range modes {
				b.Run(phase+"/"+agg.name+"/"+mode, func(b *testing.B) {
					mp := mpool.MustNewZero()
					defer mpool.DeleteMPool(mp)
					defer func() {
						if used := mp.CurrNB(); used != 0 {
							b.Fatalf("retained %d mpool bytes", used)
						}
					}()
					check := func(err error) {
						if err != nil {
							b.Fatal(err)
						}
					}
					input := vector.NewVec(types.T_int32.ToType())
					defer input.Free(mp)
					for i := 0; i < groups+2; i++ {
						check(vector.AppendFixed(input, int32(7), false, mp))
					}
					args := []*vector.Vector{input}
					makeExec := func() GroupAggFuncExec {
						exec, err := MakeSingleGroupAgg(mp, agg.id, false, nil, nil, types.T_int32.ToType())
						check(err)
						SyncAggregatorsToChunkSize([]GroupAggFuncExec{exec}, 1)
						if err := exec.GroupGrow(1); err != nil {
							exec.Free()
							b.Fatal(err)
						}
						return exec
					}
					exec := makeExec()
					defer exec.Free()
					dst, err := AppendSingleGroupResultAndReset(exec, nil)
					check(err)
					defer dst.Free(mp)
					check(dst.PreExtend(groups, mp))
					one, err := input.WindowByLogicalRows(1, 2)
					check(err)
					defer one.Free(mp)
					oneArg := []*vector.Vector{one}
					run := func() {
						dst.CleanOnlyData()
						for i := 0; i < groups; i++ {
							if phase == "range" {
								if mode == "window" {
									window, err := input.WindowByLogicalRows(i+1, i+2)
									check(err)
									err = exec.BulkFill(0, []*vector.Vector{window})
									window.Free(mp)
									check(err)
								} else {
									check(BulkFillSingleGroupRange(exec, i+1, 1, args))
								}
							} else if mode == "reconstruct" {
								old := makeExec()
								if err := old.BulkFill(0, oneArg); err != nil {
									old.Free()
									b.Fatal(err)
								}
								result, err := old.Flush()
								old.Free()
								check(err)
								err = dst.UnionOne(result[0], 0, mp)
								for _, v := range result {
									v.Free(mp)
								}
								check(err)
							} else {
								check(exec.BulkFill(0, oneArg))
								_, err := AppendSingleGroupResultAndReset(exec, dst)
								check(err)
							}
						}
						if phase == "range" {
							_, err := AppendSingleGroupResultAndReset(exec, dst)
							check(err)
						}
					}
					verify := func() {
						rows, count, sum := groups, int64(1), int64(7)
						if phase == "range" {
							rows, count, sum = 1, groups, groups*7
						}
						if dst.Length() != rows {
							b.Fatalf("got %d rows, want %d", dst.Length(), rows)
						}
						for row := 0; row < rows; row++ {
							if dst.IsNull(uint64(row)) {
								b.Fatal("unexpected NULL")
							}
							if agg.id == AggIdOfAvg {
								if dst.GetType().Oid != types.T_decimal128 || dst.GetType().Scale != 4 ||
									vector.GetFixedAtNoTypeCheck[types.Decimal128](dst, row) != types.Decimal128FromInt64(70000) {
									b.Fatal("wrong AVG value or type")
								}
							} else {
								want := sum
								if agg.id == AggIdOfCountColumn {
									want = count
								}
								if got := vector.GetFixedAtNoTypeCheck[int64](dst, row); got != want {
									b.Fatalf("got %d, want %d", got, want)
								}
							}
						}
					}
					run()
					verify()
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						run()
					}
					b.StopTimer()
					verify()
				})
			}
		}
	}
}
