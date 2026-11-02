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

package plan

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func featureScanFoldTree(n int, balanced bool) *planpb.Expr {
	typ := types.T_bool.ToType().PlanType()
	if n == 0 {
		return &planpb.Expr{Typ: typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
	}
	left := 0
	if balanced {
		left = (n - 1) / 2
	}
	return &planpb.Expr{Typ: typ, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{Obj: function.AndFunctionEncodedID, ObjName: "and"},
		Args: []*planpb.Expr{featureScanFoldTree(left, balanced), featureScanFoldTree(n-1-left, balanced)},
	}}}
}

func featureScanFoldNodeCount(e *planpb.Expr) int {
	if e.GetCol() != nil {
		return 0
	}
	f := e.GetF()
	if f == nil || f.Func.Obj != function.AndFunctionEncodedID || len(f.Args) != 2 {
		panic("benchmark fixture no longer describes a bound column AND tree")
	}
	return 1 + featureScanFoldNodeCount(f.Args[0]) + featureScanFoldNodeCount(f.Args[1])
}

func BenchmarkConstantFoldFeatureScan(b *testing.B) {
	for _, balanced := range []bool{false, true} {
		for _, n := range []int{128, 1024} {
			b.Run(fmt.Sprintf("balanced%t/n%d", balanced, n), func(b *testing.B) {
				b.StopTimer()
				proc := testutil.NewProcess(b)
				expr := featureScanFoldTree(n, balanced)
				if featureScanFoldNodeCount(expr) != n {
					b.Fatal("incorrect fixture cardinality")
				}
				b.ReportAllocs()
				b.ResetTimer()
				b.StartTimer()
				for i := 0; i < b.N; i++ {
					out, err := ConstantFold(batch.EmptyForConstFoldBatch, expr, proc, false, true)
					if err != nil || out != expr {
						b.Fatal("column expression changed during real constant folding")
					}
				}
				b.StopTimer()
				if featureScanFoldNodeCount(expr) != n {
					b.Fatal("later iterations measured an already-folded tree")
				}
			})
		}
	}
}
