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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestDivisionWarningRuntimeFolding(t *testing.T) {
	for _, name := range []string{"/", "div", "%", "unsigned decimal div"} {
		for _, parent := range []string{"direct", "coalesce"} {
			t.Run(name+"/"+parent, func(t *testing.T) {
				proc := testutil.NewProcess(t, testutil.WithFileService(nil))
				defer proc.Free()
				proc.SetStmtProfile(&process.StmtProfile{})
				proc.GetStmtProfile().SetStatementRuntimeProfile("Select", "DQL", false)
				proc.SetBaseProcessRunningStatus(true)
				sink := &preparedCastWarningSession{}
				proc.WarningSink = sink
				literal := func(n int64) *plan.Expr {
					if name == "unsigned decimal div" {
						return &plan.Expr{Typ: types.T_uint64.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: uint64(n)}}}}
					}
					if name == "/" {
						return &plan.Expr{Typ: types.T_float64.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Dval{Dval: float64(n)}}}}
					}
					return &plan.Expr{Typ: types.T_int64.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: n}}}}
				}
				var expr *plan.Expr
				if name == "unsigned decimal div" {
					dividend := &plan.Expr{Typ: types.New(types.T_decimal64, 10, 3).PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Decimal64Val{Decimal64Val: &plan.Decimal64{A: 1000}}}}}
					expr = bindTestFunction(t, proc, "div", dividend, literal(0))
					expr.GetF().Args[0] = bindTestCast(t, proc, 0, dividend, types.New(types.T_decimal256, 10, 3))
					expr.GetF().Args[1] = bindTestCast(t, proc, 0, literal(0), types.New(types.T_decimal256, 20, 0))
				} else {
					expr = bindTestFunction(t, proc, name, literal(1), literal(0))
				}
				if parent == "coalesce" {
					expr = bindTestFunction(t, proc, "coalesce", expr, literal(7))
				}
				executor, err := NewExpressionExecutor(proc, expr)
				require.NoError(t, err)
				defer executor.Free()
				input := batch.New(nil)
				for _, mode := range []string{"", "ERROR_FOR_DIVISION_BY_ZERO", ""} {
					executor.ResetForNextQuery()
					proc.GetSessionInfo().SqlMode = mode
					sink.warningCount = 0
					want := 0
					for _, evaluation := range []struct {
						rows     int
						mask     []bool
						warnings int
					}{
						{3, nil, 3},
						{3, []bool{false, true, true}, 2},
						{2, nil, 2},
						{0, nil, 0},
						{3, []bool{false, false, false}, 0},
					} {
						input.SetRowCount(evaluation.rows)
						result, err := executor.Eval(proc, []*batch.Batch{input}, evaluation.mask)
						require.NoError(t, err)
						require.Equal(t, evaluation.rows, result.Length())
						if mode != "" {
							want += evaluation.warnings
						}
						require.Equal(t, want, sink.warningCount, "mode %q, rows %d mask %v", mode, evaluation.rows, evaluation.mask)
						for row := 0; row < evaluation.rows; row++ {
							if evaluation.mask == nil || evaluation.mask[row] {
								require.Equal(t, parent == "direct", result.IsNull(uint64(row)))
							}
						}
					}
					require.Equal(t, mode == "", executor.(*FunctionExpressionExecutor).folded.canFold)
				}
			})
		}
	}
}

func TestDivisionWarningRuntimeFoldingControls(t *testing.T) {
	for _, tc := range []struct {
		name                string
		denominator         int64
		leftNull, rightNull bool
		dml                 bool
	}{
		{name: "nonzero", denominator: 2},
		{name: "NULL dividend", leftNull: true},
		{name: "NULL divisor", rightNull: true},
		{name: "strict DML", dml: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t, testutil.WithFileService(nil))
			defer proc.Free()
			proc.SetStmtProfile(&process.StmtProfile{})
			proc.GetStmtProfile().SetStatementRuntimeProfile("Select", "DQL", false)
			if tc.dml {
				proc.GetStmtProfile().SetStatementRuntimeProfile("Insert", "DML", false)
			}
			proc.GetSessionInfo().SqlMode = "TRADITIONAL"
			proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) { return "TRADITIONAL", nil })
			proc.SetBaseProcessRunningStatus(true)
			sink := &preparedCastWarningSession{}
			proc.WarningSink = sink
			literal := func(n int64, null bool) *plan.Expr {
				return &plan.Expr{Typ: types.T_int64.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: null, Value: &plan.Literal_I64Val{I64Val: n}}}}
			}
			expr := bindTestFunction(t, proc, "div", literal(1, tc.leftNull), literal(tc.denominator, tc.rightNull))
			executor, err := NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer executor.Free()
			input := batch.New(nil)
			input.SetRowCount(3)
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			if tc.dml {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrDivByZero))
			} else {
				require.NoError(t, err)
				require.True(t, executor.(*FunctionExpressionExecutor).folded.canFold)
				require.Equal(t, tc.leftNull || tc.rightNull, result.IsNull(0))
				require.Equal(t, 3, result.Length())
			}
			require.Zero(t, sink.warningCount)
		})
	}
}
