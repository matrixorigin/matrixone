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

package window

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
)

// BenchmarkWindowFirstBatch models the #23107 LIMIT consumer: it asks Window
// for only the first output batch of a large cumulative frame and then resets
// the pipeline.
func BenchmarkWindowFirstBatch(b *testing.B) {
	const rows = colexec.DefaultBatchSize * 8

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
		values := make([]int32, rows)
		for row := range values {
			values[row] = int32(row + 1)
		}
		input := batch.NewWithSize(1)
		input.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
		input.SetRowCount(rows)

		spec := makeWindowSpec()
		spec.Expr.(*plan.Expr_W).W.Frame = makeFiniteCumulativeFrame(2147483647)
		arg := &Window{
			WinSpecList: []*plan.Expr{spec},
			Aggs:        []aggexec.AggFuncExecExpression{newAggExpr()},
			OperatorBase: vm.OperatorBase{
				OperatorInfo: vm.OperatorInfo{Idx: 0},
			},
		}
		op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
		arg.AppendChild(op)

		if err := arg.Prepare(proc); err != nil {
			b.Fatal(err)
		}
		result, err := vm.Exec(arg, proc)
		if err != nil {
			b.Fatal(err)
		}
		if result.Batch == nil || result.Batch.RowCount() == 0 {
			b.Fatal("window returned no rows")
		}

		arg.Reset(proc, false, nil)
		arg.Free(proc, false, nil)
		op.Free(proc, false, nil)
		proc.Free()
		if got := proc.Mp().CurrNB(); got != 0 {
			b.Fatalf("mpool leak: %d bytes", got)
		}
	}
}

// BenchmarkWindowBoundedRowsSum covers the finite sliding shape from #27352.
// Runtime should remain approximately flat as the frame width grows because
// each output row performs at most one add and one remove.
func BenchmarkWindowBoundedRowsSum(b *testing.B) {
	const rows = 80_000
	widths := []struct {
		name  string
		bound uint64
	}{
		{name: "preceding_0", bound: 0},
		{name: "preceding_31", bound: 31},
		{name: "preceding_128", bound: 128},
		{name: "preceding_512", bound: 512},
		{name: "preceding_1024", bound: 1024},
	}

	for _, width := range widths {
		b.Run(width.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
				values := make([]int32, rows)
				for row := range values {
					values[row] = 1
				}
				input := batch.NewWithSize(1)
				input.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
				input.SetRowCount(rows)

				spec := makeWindowSpec()
				spec.Expr.(*plan.Expr_W).W.Frame = makeFiniteCumulativeFrame(width.bound)
				arg := &Window{
					WinSpecList: []*plan.Expr{spec},
					Aggs:        []aggexec.AggFuncExecExpression{newAggExpr()},
					OperatorBase: vm.OperatorBase{
						OperatorInfo: vm.OperatorInfo{Idx: 0},
					},
				}
				op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
				arg.AppendChild(op)

				if err := arg.Prepare(proc); err != nil {
					b.Fatal(err)
				}
				var last int64
				for {
					result, err := vm.Exec(arg, proc)
					if err != nil {
						b.Fatal(err)
					}
					if result.Batch == nil {
						break
					}
					values := vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[1])
					last = values[len(values)-1]
				}
				if want := int64(width.bound + 1); last != want {
					b.Fatalf("last sliding sum: got %d, want %d", last, want)
				}

				arg.Reset(proc, false, nil)
				arg.Free(proc, false, nil)
				op.Free(proc, false, nil)
				proc.Free()
				if got := proc.Mp().CurrNB(); got != 0 {
					b.Fatalf("mpool leak: %d bytes", got)
				}
			}
		})
	}
}
