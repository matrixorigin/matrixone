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

package colexec_test

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// Measure the public binder/executor path, including any implicit cast. Setup
// and the first evaluation are excluded; each operation evaluates 1024 rows.
func BenchmarkTemporalStringExpressions(b *testing.B) {
	for _, input := range []struct{ name, value string }{
		{"complete_date", "2024-02-29"},
		{"complete_datetime", "2024-02-29 12:34:56"},
		{"incomplete_date", "2024-00-00"},
	} {
		for _, name := range []string{"date_format", "to_days", "week"} {
			b.Run(input.name+"/"+name, func(b *testing.B) {
				proc := testutil.NewProcess(b, testutil.WithFileService(nil))
				bat := batch.NewWithSize(1)
				defer bat.Clean(proc.Mp())
				bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
				const rows = 1024
				for i := 0; i < rows; i++ {
					require.NoError(b, vector.AppendBytes(bat.Vecs[0], []byte(input.value), false, proc.Mp()))
				}
				bat.SetRowCount(rows)
				args := []*plan.Expr{{Typ: types.T_varchar.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}}
				switch name {
				case "date_format":
					args = append(args, plan2.MakePlan2StringConstExprWithType("%Y-%m-%d"))
				case "week":
					args = append(args, plan2.MakePlan2Int64ConstExprWithType(3))
				}
				expr, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, name, args)
				require.NoError(b, err)
				executor, err := colexec.NewExpressionExecutor(proc, expr)
				require.NoError(b, err)
				defer executor.Free()
				batches := []*batch.Batch{bat}
				result, err := executor.Eval(proc, batches, nil)
				require.NoError(b, err)
				require.Equal(b, rows, result.Length())
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := executor.Eval(proc, batches, nil); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*rows), "ns/row")
			})
		}
	}
}
