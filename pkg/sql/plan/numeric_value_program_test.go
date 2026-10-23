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
	"context"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestNumericValueProgramOutputsAndPassengerCast(t *testing.T) {
	ctx := context.Background()
	source := &planpb.Expr{
		Typ:  types.T_varchar.ToType().PlanType(),
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 3}},
	}
	value, kind, err := makeNumericValueProgramOutputs(ctx, source)
	require.NoError(t, err)
	require.Equal(t, int32(function.SCALAR_PROGRAM_REF), numericProgramFunctionID(value))
	require.Equal(t, int32(function.SCALAR_PROGRAM_REF), numericProgramFunctionID(kind))
	definition := value.GetF().Args[0]
	require.Equal(t, int32(function.SCALAR_PROGRAM), numericProgramFunctionID(definition))
	require.Equal(t, source, definition.GetF().Args[1])
	valueScope := definition.GetF().Args[0].GetLit().GetI64Val()
	kindArgs := kind.GetF().Args
	require.Len(t, kindArgs, 2)
	kindScope := kindArgs[0].GetLit().GetI64Val()
	require.Equal(t, valueScope, kindScope)
	require.Equal(t, int64(0), scalarProgramSlotForTest(value))
	require.Equal(t, int64(1), scalarProgramSlotForTest(kind))

	rawColumn := &planpb.Expr{
		Typ:  types.T_varchar.ToType().PlanType(),
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
	}
	kindColumn := &planpb.Expr{
		Typ:  types.T_bool.ToType().PlanType(),
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}},
	}
	cast, err := appendNumericValueCastWithKind(ctx, rawColumn, kindColumn,
		makeSimplePlan2Type(types.T_int64), function.NumericValueCastPolicyComparison)
	require.NoError(t, err)
	args := cast.GetF().Args
	require.Len(t, args, 4)
	require.Equal(t, rawColumn, args[0], "the consumer must read the producer value column directly")
	require.Equal(t, kindColumn, args[2], "the consumer must read the ordinary BOOL passenger directly")
	require.Nil(t, args[0].GetF(), "the consumer must not add a second producer program around a column")

	analysisSource, policy, ok := numericValueCastAnalysisSource(cast)
	require.True(t, ok, "CAST9 over ordinary producer columns is a valid source view")
	require.Equal(t, function.NumericValueCastPolicyComparison, policy)
	require.Equal(t, rawColumn, analysisSource, "analysis keeps the value ColRef, not its BOOL passenger")
}

func numericProgramFunctionID(expr *planpb.Expr) int32 {
	if expr == nil || expr.GetF() == nil || expr.GetF().Func == nil {
		return 0
	}
	id, _ := function.DecodeOverloadID(expr.GetF().Func.GetObj())
	return id
}

func scalarProgramSlotForTest(expr *planpb.Expr) int64 {
	args := expr.GetF().Args
	return args[len(args)-1].GetLit().GetI64Val()
}

var numericValueProgramBenchmarkSink float64

// BenchmarkNumericValueProgramExpressionExecution measures batch expression
// evaluation for native numeric, ordinary text, and mixed HEX/text selector
// inputs. It is a current-path allocation/latency sample, not a before/after
// comparison or an end-to-end SQL benchmark.
func BenchmarkNumericValueProgramExpressionExecution(b *testing.B) {
	const rows = 128
	ctx := context.Background()
	proc := newPlanTestProcess(b)
	b.Cleanup(func() {
		proc.Free()
		require.Zero(b, proc.Mp().CurrNB())
	})

	input := batch.NewWithSize(4)
	input.Vecs[0] = vector.NewVec(types.T_bool.ToType())
	input.Vecs[1] = vector.NewVec(types.T_float64.ToType())
	input.Vecs[2] = vector.NewVec(types.T_varchar.ToType())
	input.Vecs[3] = vector.NewVec(types.T_bool.ToType())
	conditions := make([]bool, rows)
	numbers := make([]float64, rows)
	kinds := make([]bool, rows)
	for i := 0; i < rows; i++ {
		conditions[i] = i%2 == 0
		numbers[i] = float64(i + 1)
	}
	if err := vector.AppendFixedList(input.Vecs[0], conditions, nil, proc.Mp()); err != nil {
		b.Fatal(err)
	}
	if err := vector.AppendFixedList(input.Vecs[1], numbers, nil, proc.Mp()); err != nil {
		b.Fatal(err)
	}
	if err := vector.AppendFixedList(input.Vecs[3], kinds, nil, proc.Mp()); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < rows; i++ {
		if err := vector.AppendBytes(input.Vecs[2], []byte("1"), false, proc.Mp()); err != nil {
			b.Fatal(err)
		}
	}
	input.SetRowCount(rows)
	b.Cleanup(func() { input.Clean(proc.Mp()) })

	column := func(pos int, typ types.Type) *planpb.Expr {
		return &planpb.Expr{
			Typ:  typ.PlanType(),
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: int32(pos)}},
		}
	}
	native, err := BindFuncExprImplByPlanExpr(ctx, "abs", []*planpb.Expr{
		column(1, types.T_float64.ToType()),
	})
	if err != nil {
		b.Fatal(err)
	}
	text, err := BindFuncExprImplByPlanExpr(ctx, "abs", []*planpb.Expr{
		column(2, types.T_varchar.ToType()),
	})
	if err != nil {
		b.Fatal(err)
	}
	selector, err := BindFuncExprImplByPlanExpr(ctx, "case", []*planpb.Expr{
		column(0, types.T_bool.ToType()),
		makePlan2StringConstExprWithType("1", true),
		makePlan2StringConstExprWithType("1"),
	})
	if err != nil {
		b.Fatal(err)
	}
	ordinarySelector, err := BindFuncExprImplByPlanExpr(ctx, "case", []*planpb.Expr{
		column(0, types.T_bool.ToType()),
		makePlan2StringConstExprWithType("1"),
		makePlan2StringConstExprWithType("2"),
	})
	if err != nil {
		b.Fatal(err)
	}
	ordinaryCase, err := BindFuncExprImplByPlanExpr(ctx, "abs", []*planpb.Expr{ordinarySelector})
	if err != nil {
		b.Fatal(err)
	}
	ordinaryRaw, ordinaryKind, err := makeNumericValueProgramOutputsWithKind(ctx, ordinarySelector,
		column(3, types.T_bool.ToType()))
	if err != nil {
		b.Fatal(err)
	}
	ordinaryCast, err := appendNumericValueCastWithKind(ctx, ordinaryRaw, ordinaryKind,
		makeSimplePlan2Type(types.T_float64), function.NumericValueCastPolicyComparison)
	if err != nil {
		b.Fatal(err)
	}
	ordinaryProgram, err := BindFuncExprImplByPlanExpr(ctx, "abs", []*planpb.Expr{ordinaryCast})
	if err != nil {
		b.Fatal(err)
	}
	legacyCast := ordinaryCase.GetF().Args[0]
	legacyFID, legacyOverload := function.DecodeOverloadID(legacyCast.GetF().Func.GetObj())
	if legacyFID != function.CAST || legacyOverload != function.NumericValueCastPolicyComparison {
		b.Fatalf("ordinary CASE comparison control must use CAST2, got function %d overload %d", legacyFID, legacyOverload)
	}
	if !proto.Equal(legacyCast.GetF().Args[0], ordinarySelector) {
		b.Fatal("ordinary CASE comparison control must consume the same selector")
	}
	pairedCast := ordinaryProgram.GetF().Args[0]
	pairedFID, pairedOverload := function.DecodeOverloadID(pairedCast.GetF().Func.GetObj())
	if pairedFID != function.CAST || pairedOverload != function.NumericValueCastOverload {
		b.Fatalf("paired CASE control must use CAST9, got function %d overload %d", pairedFID, pairedOverload)
	}
	if !proto.Equal(pairedCast.GetF().Args[1], legacyCast.GetF().Args[1]) {
		b.Fatal("paired CASE comparison control must use the same cast target")
	}
	pairedSource, pairedKind, ok := function.NumericValueProgramCastOwner(pairedCast)
	if !ok || !proto.Equal(pairedSource, ordinarySelector) ||
		!proto.Equal(pairedKind, column(3, types.T_bool.ToType())) {
		b.Fatal("paired CASE comparison control must consume the same selector and kind input")
	}
	policyLiteral := pairedCast.GetF().Args[3].GetLit()
	policyValue, policyOK := policyLiteral.Value.(*planpb.Literal_I32Val)
	if !policyOK || policyValue == nil || policyValue.I32Val != function.NumericValueCastPolicyComparison {
		b.Fatalf("paired CASE control must use the same comparison policy, got %v", policyLiteral)
	}
	raw, kind, err := makeNumericValueProgramOutputs(ctx, selector)
	if err != nil {
		b.Fatal(err)
	}
	cast, err := appendNumericValueCastWithKind(ctx, raw, kind,
		makeSimplePlan2Type(types.T_float64), function.NumericValueCastPolicyOrdinary)
	if err != nil {
		b.Fatal(err)
	}
	mixed, err := BindFuncExprImplByPlanExpr(ctx, "abs", []*planpb.Expr{cast})
	if err != nil {
		b.Fatal(err)
	}

	for _, tc := range []struct {
		name                string
		expr                *planpb.Expr
		wantFirst, wantNext float64
	}{
		{name: "native_numeric", expr: native, wantFirst: 1, wantNext: 2},
		{name: "ordinary_text", expr: text, wantFirst: 1, wantNext: 1},
		{name: "ordinary_text_case_cast2_comparison", expr: ordinaryCase, wantFirst: 1, wantNext: 2},
		{name: "ordinary_text_case_program_cast9_comparison", expr: ordinaryProgram, wantFirst: 1, wantNext: 2},
		{name: "mixed_hex_text_program", expr: mixed, wantFirst: 49, wantNext: 1},
	} {
		b.Run(tc.name, func(b *testing.B) {
			executor, err := colexec.NewExpressionExecutor(proc, tc.expr)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(executor.Free)
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			if err != nil {
				b.Fatal(err)
			}
			if got := vector.GetFixedAtNoTypeCheck[float64](result, 0); got != tc.wantFirst {
				b.Fatalf("setup row 0 = %v, want %v", got, tc.wantFirst)
			}
			if got := vector.GetFixedAtNoTypeCheck[float64](result, 1); got != tc.wantNext {
				b.Fatalf("setup row 1 = %v, want %v", got, tc.wantNext)
			}

			b.ReportAllocs()
			b.ReportMetric(rows, "rows/op")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err = executor.Eval(proc, []*batch.Batch{input}, nil)
				if err != nil {
					b.Fatal(err)
				}
				numericValueProgramBenchmarkSink = vector.GetFixedAtNoTypeCheck[float64](result, 0)
			}
		})
	}
}
