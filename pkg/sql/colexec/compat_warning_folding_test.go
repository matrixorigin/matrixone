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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// PR #30373 review: folded scalar warning-bearing calls collapsed the
// per-evaluated-row MySQL warnings (LN and FORMAT). These cases run the
// constant expression over a multi-row batch and verify the guard keeps the
// warning-bearing form out of the fold cache, while a proven safe constant
// still folds.
func TestCompatWarningRuntimeFolding(t *testing.T) {
	f64 := func(v float64) *plan.Expr {
		return &plan.Expr{Typ: types.T_float64.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Dval{Dval: v}}}}
	}
	i64 := func(v int64) *plan.Expr {
		return &plan.Expr{Typ: types.T_int64.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: v}}}}
	}
	sval := func(v string) *plan.Expr {
		return &plan.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: v}}}}
	}

	for _, tc := range []struct {
		name         string
		fn           string
		args         []*plan.Expr
		wantWarnings int
		wantFold     bool
	}{
		{"ln invalid argument warns per row", "ln", []*plan.Expr{f64(0)}, 3, false},
		{"ln valid argument folds", "ln", []*plan.Expr{f64(2)}, 0, true},
		{"log invalid base warns per row", "log", []*plan.Expr{f64(1), f64(2)}, 3, false},
		{"format unknown locale warns per row", "format", []*plan.Expr{i64(1234567), i64(2), sval("zz_ZZ")}, 3, false},
		{"format known locale folds", "format", []*plan.Expr{i64(1234567), i64(2), sval("de_DE")}, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t, testutil.WithFileService(nil))
			defer proc.Free()
			proc.SetStmtProfile(&process.StmtProfile{})
			proc.GetStmtProfile().SetStatementRuntimeProfile("Select", "DQL", false)
			proc.SetBaseProcessRunningStatus(true)
			sink := &preparedCastWarningSession{}
			proc.WarningSink = sink

			expr := bindTestFunction(t, proc, tc.fn, tc.args...)
			executor, err := NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer executor.Free()

			input := batch.New(nil)
			input.SetRowCount(3)
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, 3, result.Length())
			require.Equal(t, tc.wantWarnings, sink.warningCount)
			require.Equal(t, tc.wantFold, executor.(*FunctionExpressionExecutor).folded.canFold)
		})
	}
}
