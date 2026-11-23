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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func binaryLiteralTestText(value string, isBin bool) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_varchar), Width: 65535},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_Sval{Sval: value},
			IsBin: isBin,
		}},
	}
}

func binaryLiteralTestCast(t *testing.T, proc *process.Process, source *plan.Expr, target types.Type) *plan.Expr {
	t.Helper()
	cast := bindTestFunction(t, proc, "cast", source, &plan.Expr{
		Typ:  plan.Type{Id: int32(target.Oid), Width: target.Width, Scale: target.Scale},
		Expr: &plan.Expr_T{T: &plan.TargetType{}},
	})
	cast.Typ = plan.Type{Id: int32(target.Oid), Width: target.Width, Scale: target.Scale}
	return cast
}

// binaryLiteralTestRows evaluates expr over a three-row batch, once for all
// rows and once through the selected-row path, and returns row 0 as text.
func binaryLiteralTestRows(t *testing.T, proc *process.Process, expr *plan.Expr) []string {
	t.Helper()
	executor, err := NewExpressionExecutor(proc, expr)
	require.NoError(t, err)
	defer executor.Free()
	input := batch.NewWithSize(0)
	input.SetRowCount(3)
	var got []string
	for _, selectList := range [][]bool{nil, {true, false, true}} {
		result, err := executor.Eval(proc, []*batch.Batch{input}, selectList)
		require.NoError(t, err)
		require.Equal(t, 3, result.Length())
		require.False(t, result.IsNull(0))
		switch result.GetType().Oid {
		case types.T_int64:
			got = append(got, fmt.Sprint(vector.GetFixedAtNoTypeCheck[int64](result, 0)))
		case types.T_float64:
			got = append(got, fmt.Sprint(vector.GetFixedAtNoTypeCheck[float64](result, 0)))
		case types.T_decimal64:
			got = append(got, vector.GetFixedAtNoTypeCheck[types.Decimal64](result, 0).Format(result.GetType().Scale))
		default:
			t.Fatalf("unexpected result type %s", result.GetType())
		}
	}
	return got
}

// A hex/bit literal reaches a numeric conversion as the argument flag its
// executor publishes, on the whole-batch and the selected-row paths.
func TestBinaryLiteralArgumentConvertsAsNumber(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		name      string
		target    types.Type
		bin, text string
	}{
		{"int64", types.T_int64.ToType(), "53", "5"},
		{"float64", types.T_float64.ToType(), "53", "5"},
		// Binary bytes decode as the unscaled decimal value.
		{"decimal", types.New(types.T_decimal64, 10, 2), "0.53", "5.00"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bin := binaryLiteralTestRows(t, proc, binaryLiteralTestCast(t, proc, binaryLiteralTestText("5", true), tc.target))
			require.Equal(t, []string{tc.bin, tc.bin}, bin)
			text := binaryLiteralTestRows(t, proc, binaryLiteralTestCast(t, proc, binaryLiteralTestText("5", false), tc.target))
			require.Equal(t, []string{tc.text, tc.text}, text)
		})
	}
}

// A function result is never a binary literal, even of a binary literal.
func TestBinaryLiteralFunctionResultIsNotBinary(t *testing.T) {
	proc := testutil.NewProcess(t)
	concat := bindTestFunction(t, proc, "concat", binaryLiteralTestText("5", true))
	got := binaryLiteralTestRows(t, proc, binaryLiteralTestCast(t, proc, concat, types.T_int64.ToType()))
	require.Equal(t, []string{"5", "5"}, got)
}

func TestBinaryPreparedParamArgument(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	state := proc.DetachPrepareParams()
	defer proc.RestorePrepareParams(state)
	params := vector.NewVec(types.T_text.ToType(), proc.Mp())
	defer params.Free(proc.Mp())
	require.NoError(t, vector.AppendBytes(params, []byte("5"), false, proc.Mp()))
	param := &plan.Expr{Typ: plan.Type{Id: int32(types.T_text)}, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}}
	for _, tc := range []struct {
		isBin bool
		want  string
	}{{true, "53"}, {false, "5"}} {
		proc.SetPrepareParamsWithIsBin(params, []bool{tc.isBin})
		got := binaryLiteralTestRows(t, proc, binaryLiteralTestCast(t, proc, param, types.T_int64.ToType()))
		require.Equal(t, []string{tc.want, tc.want}, got)
		isBin, err := ExprIsBinLiteral(proc, param)
		require.NoError(t, err)
		require.Equal(t, tc.isBin, isBin)
	}
	proc.SetPrepareParams(nil)
}

func TestBinaryUserVariableArgument(t *testing.T) {
	proc := testutil.NewProcess(t)
	isBin := true
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) { return "5", nil })
	proc.SetResolveVariableIsBinFunc(func(string, bool, bool) (bool, error) { return isBin, nil })
	variable := &plan.Expr{Typ: plan.Type{Id: int32(types.T_text)}, Expr: &plan.Expr_V{V: &plan.VarRef{Name: "v"}}}
	for _, tc := range []struct {
		isBin bool
		want  string
	}{{true, "53"}, {false, "5"}} {
		isBin = tc.isBin
		got := binaryLiteralTestRows(t, proc, binaryLiteralTestCast(t, proc, variable, types.T_int64.ToType()))
		require.Equal(t, []string{tc.want, tc.want}, got)
		derived, err := ExprIsBinLiteral(proc, variable)
		require.NoError(t, err)
		require.Equal(t, tc.isBin, derived)
	}
}

func TestExprIsBinLiteral(t *testing.T) {
	proc := testutil.NewProcess(t)
	concat := bindTestFunction(t, proc, "concat", binaryLiteralTestText("A", true))
	column := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{}}}
	null := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true, IsBin: true}}}
	intParam := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}}
	for _, tc := range []struct {
		name string
		expr *plan.Expr
		want bool
	}{
		{"nil", nil, false},
		{"hex literal", binaryLiteralTestText("A", true), true},
		{"text literal", binaryLiteralTestText("A", false), false},
		{"null literal", null, false},
		{"function", concat, false},
		{"column", column, false},
		{"typed parameter", intParam, false},
	} {
		got, err := ExprIsBinLiteral(proc, tc.expr)
		require.NoError(t, err, tc.name)
		require.Equal(t, tc.want, got, tc.name)
	}
	got, err := ExprIsBinLiteral(nil, binaryLiteralTestText("A", true))
	require.NoError(t, err)
	require.True(t, got, "a literal needs no process")
	got, err = ExprIsBinLiteral(nil, &plan.Expr{Typ: plan.Type{Id: int32(types.T_text)}, Expr: &plan.Expr_V{V: &plan.VarRef{Name: "v"}}})
	require.NoError(t, err)
	require.False(t, got)
}

// Only the leaf vector its executor produced is a binary literal; a
// substituted placeholder is not.
func TestIsBinaryLiteralResultIdentity(t *testing.T) {
	proc := testutil.NewProcess(t)
	executor, err := NewExpressionExecutor(proc, binaryLiteralTestText("A", true))
	require.NoError(t, err)
	defer executor.Free()
	input := batch.NewWithSize(0)
	input.SetRowCount(1)
	vec, err := executor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.True(t, isBinaryLiteralResult(executor, vec))
	other := vector.NewConstNull(types.T_varchar.ToType(), 1, proc.Mp())
	defer other.Free(proc.Mp())
	require.False(t, isBinaryLiteralResult(executor, other))
	require.False(t, isBinaryLiteralResult(executor, nil))
	memo := &memoExpressionExecutor{state: &memoExpressionState{executor: executor}}
	require.True(t, isBinaryLiteralResult(memo, vec))
}
