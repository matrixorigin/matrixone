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

package colexec

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// BenchmarkEvalSelection evaluates expressions over one 8192-row batch under a
// selection: every row (nil), half the rows and a tenth of them. Flow-control
// functions (IF, CASE, COALESCE) also build a selection per branch.
func BenchmarkEvalSelection(b *testing.B) {
	const rows = 8192
	proc := testutil.NewProcess(b)
	defer proc.Free()

	col := func(pos int32) *plan.Expr {
		return &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}}}
	}
	positive := func() *plan.Expr {
		return bindTestFunction(b, proc, ">", col(0), makePlan2Int64ConstExprWithType(0))
	}
	exprs := []struct {
		name string
		expr func() *plan.Expr
	}{
		{"plus", func() *plan.Expr { return bindTestFunction(b, proc, "+", col(0), col(1)) }},
		{"if", func() *plan.Expr {
			return bindTestFunction(b, proc, "if", positive(), bindTestFunction(b, proc, "+", col(0), col(1)), col(1))
		}},
		{"case", func() *plan.Expr {
			return bindTestFunction(b, proc, "case", positive(), col(0),
				bindTestFunction(b, proc, "<", col(1), makePlan2Int64ConstExprWithType(100)), col(1),
				makePlan2Int64ConstExprWithType(0))
		}},
		{"coalesce", func() *plan.Expr { return bindTestFunction(b, proc, "coalesce", col(0), col(1)) }},
	}

	input := batch.NewWithSize(2)
	a := make([]int64, rows)
	c := make([]int64, rows)
	var aNulls []uint64
	for i := range a {
		a[i] = int64(i%7 - 3)
		c[i] = int64(i % 200)
		if i%5 == 0 {
			aNulls = append(aNulls, uint64(i))
		}
	}
	input.Vecs[0] = testutil.MakeInt64Vector(a, aNulls, proc.Mp())
	input.Vecs[1] = testutil.MakeInt64Vector(c, nil, proc.Mp())
	input.SetRowCount(rows)
	defer input.Clean(proc.Mp())
	batches := []*batch.Batch{input}

	masks := []struct {
		name  string
		every int
	}{{"all", 0}, {"half", 2}, {"tenth", 10}}
	for _, e := range exprs {
		for _, m := range masks {
			b.Run(fmt.Sprintf("%s/%s", e.name, m.name), func(b *testing.B) {
				var bools []bool
				if m.every > 0 {
					bools = make([]bool, rows)
					// A fixed pseudo-random pattern, so runs of selected rows vary.
					x := uint32(12345)
					for i := range bools {
						x = x*1103515245 + 12345
						bools[i] = int((x>>16)%uint32(m.every)) == 0
					}
				}
				sel := boolSelection(bools)
				executor, err := NewExpressionExecutor(proc, e.expr())
				require.NoError(b, err)
				defer executor.Free()
				var result *vector.Vector
				result, err = executor.Eval(proc, batches, sel)
				require.NoError(b, err)
				require.Equal(b, rows, result.Length())
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if _, err = executor.Eval(proc, batches, sel); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
