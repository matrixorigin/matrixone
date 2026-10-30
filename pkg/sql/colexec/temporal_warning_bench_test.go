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

package colexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func BenchmarkTemporalConstantEvaluation(b *testing.B) {
	for _, tc := range []struct {
		name, value string
		rows        int
	}{{"valid-1", "2024-02-29", 1}, {"valid-1024", "2024-02-29", 1024}, {"invalid-1", "bad", 1}, {"invalid-1024", "bad", 1024}} {
		b.Run(tc.name, func(b *testing.B) {
			proc := testutil.NewProcess(b, testutil.WithFileService(nil))
			defer proc.Free()
			proc.SetStmtProfile(&process.StmtProfile{})
			proc.GetStmtProfile().SetStatementRuntimeProfile("Select", "DQL", false)
			proc.SetBaseProcessRunningStatus(true)
			proc.WarningSink = &preparedCastWarningSession{}
			input := &plan.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: tc.value}}}}
			expr := bindTestFunction(b, proc, "date", input)
			executor, err := NewExpressionExecutor(proc, expr)
			if err != nil {
				b.Fatal(err)
			}
			defer executor.Free()
			batchInput := batch.New(nil)
			batchInput.SetRowCount(tc.rows)
			batches := []*batch.Batch{batchInput}
			if _, err = executor.Eval(proc, batches, nil); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err = executor.Eval(proc, batches, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
