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

package window

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
)

// BenchmarkLocalRank includes first-output latency (one output batch) and full
// consumption, because partition indexing can affect those differently.
func BenchmarkLocalRank(b *testing.B) {
	cases := []struct {
		name                      string
		size, width, cardinality  int
		first, allNull, constNull bool
	}{
		{name: "cum_512_first", size: 512, cardinality: 10007, first: true},
		{name: "cum_512_all", size: 512, cardinality: 10007},
		{name: "cum_16384_first", size: 16384, cardinality: 10007, first: true},
		{name: "cum_16384_all", size: 16384, cardinality: 10007},
		{name: "cum_low_card_first", size: 16384, cardinality: 4, first: true},
		{name: "cum_low_card_all", size: 16384, cardinality: 4},
		{name: "cum_65536_first", size: 65536, cardinality: 10007, first: true},
		{name: "cum_1m_first", size: 1048576, cardinality: 10007, first: true},
		{name: "mrank_127_all", size: 16384, width: 127, cardinality: 10007},
		{name: "mrank_128_all", size: 16384, width: 128, cardinality: 10007},
		{name: "mrank_128_64k_first", size: 65536, width: 128, cardinality: 10007, first: true},
		{name: "mrank_128_1m_first", size: 1048576, width: 128, cardinality: 10007, first: true},
		{name: "mrank_128_1m_all", size: 1048576, width: 128, cardinality: 10007},
		{name: "mrank_4096_first", size: 16384, width: 4096, cardinality: 10007, first: true},
		{name: "mrank_4096_all", size: 16384, width: 4096, cardinality: 10007},
		{name: "cum_bitmap_null_first", size: 100000, allNull: true, first: true},
		{name: "cum_bitmap_null_all", size: 100000, allNull: true},
		{name: "cum_const_null_first", size: 100000, allNull: true, constNull: true, first: true},
		{name: "cum_const_null_all", size: 100000, allNull: true, constNull: true},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				func() {
					b.StopTimer()
					mp := mpool.MustNewZero()
					proc := testutil.NewProcessWithMPool(b, "", mp)
					var input *vector.Vector
					var arg *Window
					var op *colexec.MockOperator
					defer func() {
						b.StopTimer()
						if arg != nil {
							arg.Free(proc, false, nil)
						}
						if op != nil {
							op.Free(proc, false, nil)
						} else if input != nil {
							input.Free(mp)
						}
						proc.Free()
						leaked := mp.CurrNB()
						mpool.DeleteMPool(mp)
						if leaked != 0 {
							b.Errorf("unreleased bytes: %d", leaked)
						}
					}()
					if tc.constNull {
						input = vector.NewConstNull(types.T_int32.ToType(), tc.size, mp)
					} else {
						input = vector.NewVec(types.T_int32.ToType())
						values := make([]int32, tc.size)
						for i := range values {
							if tc.cardinality != 0 {
								values[i] = int32((i * 7919) % tc.cardinality)
							}
						}
						if err := vector.AppendFixedList(input, values, nil, mp); err != nil {
							b.Fatal(err)
						}
						if tc.allNull {
							input.SetAllNulls(tc.size)
						}
					}
					bat := batch.NewWithSize(1)
					bat.Vecs[0] = input
					bat.SetRowCount(tc.size)
					name := "cumrank"
					args := []*plan.Expr{newColExprWithType(0, types.T_int32.ToType())}
					if tc.width > 0 {
						name = "mrank"
						args = append(args, intervalInt64(int64(tc.width)))
					}
					spec := localRankSpec(name, args, -1, types.Type{}, nil)
					arg = &Window{WinSpecList: []*plan.Expr{spec}, Aggs: []aggexec.AggFuncExecExpression{
						aggexec.MakeAggFunctionExpression(0, false, spec.GetW().GetWindowFunc().GetF().Args, nil),
					}}
					op = colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
					arg.AppendChild(op)
					if err := arg.Prepare(proc); err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
					for {
						res, err := vm.Exec(arg, proc)
						if err != nil {
							b.Fatal(err)
						}
						if res.Batch == nil || tc.first {
							break
						}
					}
				}()
			}
		})
	}
}
