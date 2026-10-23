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
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestReplaceFoldExprPreservesCastTargetMetadata(t *testing.T) {
	for _, tc := range []struct {
		name     string
		source   types.Type
		value    uint64
		wantHex  string
		ordinary bool
	}{
		{name: "hex BIT8 unsigned", source: types.New(types.T_bit, 8, 0), value: 170, wantHex: "AA"},
		{name: "hex BIT64 upper boundary", source: types.New(types.T_bit, 64, 0), value: ^uint64(0), wantHex: "FFFFFFFFFFFFFFFF"},
		{name: "hex BOOL signed", source: types.T_bool.ToType(), wantHex: "1"},
		{name: "ordinary CAST column", source: types.New(types.T_bit, 8, 0), value: 170, ordinary: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			column := &planpb.Expr{
				Typ: makePlan2Type(&tc.source),
				Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
					Name: "v", ColPos: 0,
				}},
			}
			var left, cast, right *planpb.Expr
			var err error
			if tc.ordinary {
				target := types.T_uint64.ToType()
				left, err = BindFuncExprImplByPlanExpr(t.Context(), "cast", []*planpb.Expr{
					column,
					{Typ: makePlan2Type(&target), Expr: &planpb.Expr_T{T: &planpb.TargetType{}}},
				})
				require.NoError(t, err)
				cast = left
				right = makePlan2Uint64ConstExprWithType(tc.value)
			} else {
				left, err = BindFuncExprImplByPlanExpr(t.Context(), "hex", []*planpb.Expr{column})
				require.NoError(t, err)
				cast = left.GetF().Args[0]
				_, overload := function.DecodeOverloadID(cast.GetF().Func.Obj)
				require.Equal(t, function.IntegerArgumentCastOverload, overload,
					"the real binder must reach the private integer CAST")
				right = makePlan2StringConstExprWithType(tc.wantHex)
			}
			require.Equal(t, "cast", cast.GetF().Func.ObjName)
			targetMarker := cast.GetF().Args[1]
			require.NotNil(t, targetMarker.GetT())
			filter, err := BindFuncExprImplByPlanExpr(t.Context(), "=", []*planpb.Expr{left, right})
			require.NoError(t, err)
			var executors []colexec.ExpressionExecutor
			t.Cleanup(func() {
				for _, executor := range executors {
					executor.Free()
				}
			})
			canFold, err := ReplaceFoldExpr(proc, filter, &executors)
			require.NoError(t, err)
			require.False(t, canFold, "a column-dependent predicate is not a constant")
			require.Same(t, targetMarker, cast.GetF().Args[1],
				"a CAST type marker is metadata, not an execution-time scalar")
			require.NotNil(t, cast.GetF().Args[1].GetT())
			require.NotNil(t, filter.GetF().Args[1].GetFold(),
				"preserving metadata must not disable ordinary constant folding")
			require.Len(t, executors, 1, "only the comparison value needs a fold executor")
			require.NoError(t, EvalFoldExpr(proc, filter, &executors))
			foldedValue := filter.GetF().Args[1].GetFold()
			require.True(t, foldedValue.IsConst)
			if tc.ordinary {
				require.Len(t, foldedValue.Data, 8)
				require.Equal(t, tc.value, types.DecodeUint64(foldedValue.Data))
			} else {
				require.Equal(t, tc.wantHex, string(foldedValue.Data))
			}

			input := batch.NewWithSize(1)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			input.Vecs[0] = vector.NewVec(tc.source)
			if tc.source.Oid == types.T_bool {
				require.NoError(t, vector.AppendFixedList(input.Vecs[0], []bool{true, false, false}, []bool{false, false, true}, proc.Mp()))
			} else {
				require.NoError(t, vector.AppendFixedList(input.Vecs[0], []uint64{tc.value, 0, 0}, []bool{false, false, true}, proc.Mp()))
			}
			input.SetRowCount(3)
			// Fold nodes belong to the storage-filter consumer, not the generic
			// expression executor. Check their bytes above and independently run
			// the preserved row-dependent CAST/HEX with ordinary batch evaluation.
			result, free, err := colexec.GetReadonlyResultFromExpression(proc, left, []*batch.Batch{input})
			require.NoError(t, err)
			t.Cleanup(free)
			if tc.ordinary {
				require.Equal(t, []uint64{tc.value, 0}, vector.MustFixedColWithTypeCheck[uint64](result)[:2])
			} else {
				require.Equal(t, tc.wantHex, result.GetStringAt(0))
				require.Equal(t, "0", result.GetStringAt(1))
			}
			require.False(t, result.IsNull(0))
			require.False(t, result.IsNull(1))
			require.True(t, result.IsNull(2), "a NULL source must remain NULL through CAST/HEX")
		})
	}
}

func TestReplaceFoldExprStillFoldsWholeConstantCast(t *testing.T) {
	proc := testutil.NewProcess(t)
	target := types.T_uint64.ToType()
	cast, err := BindFuncExprImplByPlanExpr(t.Context(), "cast", []*planpb.Expr{
		makePlan2StringConstExprWithType("7"),
		{Typ: makePlan2Type(&target), Expr: &planpb.Expr_T{T: &planpb.TargetType{}}},
	})
	require.NoError(t, err)
	var executors []colexec.ExpressionExecutor
	t.Cleanup(func() {
		for _, executor := range executors {
			executor.Free()
		}
	})
	canFold, err := ReplaceFoldExpr(proc, cast, &executors)
	require.NoError(t, err)
	require.True(t, canFold, "the type marker must not make a constant CAST non-foldable")
	require.Empty(t, executors)

	column := &planpb.Expr{
		Typ: makePlan2Type(&target),
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			Name: "v", ColPos: 0,
		}},
	}
	filter, err := BindFuncExprImplByPlanExpr(t.Context(), "=", []*planpb.Expr{column, cast})
	require.NoError(t, err)
	canFold, err = ReplaceFoldExpr(proc, filter, &executors)
	require.NoError(t, err)
	require.False(t, canFold)
	require.NotNil(t, filter.GetF().Args[1].GetFold())
	require.Len(t, executors, 1, "the whole constant CAST still has one cached value")
	require.NoError(t, EvalFoldExpr(proc, filter, &executors))
	foldedValue := filter.GetF().Args[1].GetFold()
	require.True(t, foldedValue.IsConst)
	require.Len(t, foldedValue.Data, 8)
	require.Equal(t, uint64(7), types.DecodeUint64(foldedValue.Data))
	result, err := executors[0].Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	require.NoError(t, err)
	require.False(t, result.IsConstNull())
	require.Equal(t, uint64(7), vector.GetFixedAtWithTypeCheck[uint64](result, 0))
}

func TestReplaceFoldExprKeepsNumericValueProgramCastAtomic(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
		null  bool
	}{
		{name: "bound value", value: "945"},
		{name: "bound NULL", null: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			t.Cleanup(func() { proc.Free() })
			params := vector.NewVec(types.T_text.ToType())
			require.NoError(t, vector.AppendBytes(params, []byte(tc.value), tc.null, proc.Mp()))
			proc.SetPrepareParams(params)
			t.Cleanup(func() {
				proc.SetPrepareParams(nil)
				params.Free(proc.Mp())
			})

			source := &planpb.Expr{
				Typ:  types.T_text.ToType().PlanType(),
				Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
			}
			value, kind, err := makeNumericValueProgramOutputs(t.Context(), source)
			require.NoError(t, err)
			targetType := types.T_int32
			cast, err := appendNumericValueCastWithKind(t.Context(), value, kind,
				makeSimplePlan2Type(targetType), function.NumericValueCastPolicyOrdinary)
			require.NoError(t, err)
			ownerSource, ownerKind, ok := function.NumericValueProgramCastOwner(cast)
			require.True(t, ok, "the RHS must be the complete lowered CAST9 owner")
			require.Equal(t, source, ownerSource, "the definition keeps the original bound value source")
			require.Nil(t, ownerKind, "the two-output definition computes its BOOL result from the selected value")
			require.Equal(t, int32(function.SCALAR_PROGRAM_REF), numericProgramFunctionID(cast.GetF().Args[2]))
			require.Equal(t, int64(1), scalarProgramSlotForTest(cast.GetF().Args[2]), "the cast keeps the same program's BOOL output")

			column := &planpb.Expr{
				Typ:  targetType.ToType().PlanType(),
				Expr: &planpb.Expr_Col{Col: &planpb.ColRef{Name: "o_w_id", ColPos: 0}},
			}
			filter, err := BindFuncExprImplByPlanExpr(t.Context(), "=", []*planpb.Expr{column, cast})
			require.NoError(t, err)
			var executors []colexec.ExpressionExecutor
			t.Cleanup(func() {
				for _, executor := range executors {
					executor.Free()
				}
			})
			canFold, err := ReplaceFoldExpr(proc, filter, &executors)
			require.NoError(t, err)
			require.False(t, canFold, "the predicate still depends on the table column")
			if tc.null {
				rhs := filter.GetF().Args[1]
				if fold := rhs.GetFold(); fold != nil {
					require.NoError(t, EvalFoldExpr(proc, filter, &executors))
					require.True(t, fold.IsConst)
					require.Empty(t, fold.Data, "a folded SQL NULL has no scalar payload")
				} else if vec := rhs.GetVec(); vec != nil {
					decoded := vector.NewVec(types.T_any.ToType())
					require.NoError(t, decoded.UnmarshalBinary(vec.Data))
					t.Cleanup(func() { decoded.Free(proc.Mp()) })
					require.True(t, decoded.IsConstNull())
				} else {
					require.NotNil(t, rhs.GetLit())
					require.True(t, rhs.GetLit().Isnull, "a bound NULL remains a NULL scalar")
				}
			} else {
				require.NotNil(t, filter.GetF().Args[1].GetFold(), "the complete CAST9 result folds as one value")
				require.NoError(t, EvalFoldExpr(proc, filter, &executors))
				folded := filter.GetF().Args[1].GetFold()
				require.NotNil(t, folded)
				require.True(t, folded.IsConst)
				require.Equal(t, int32(945), types.DecodeInt32(folded.Data))
			}
			require.LessOrEqual(t, len(executors), 1, "the original program tree is never split into private function executors")
		})
	}
}

func TestReplaceFoldExprDoesNotSplitNonconstantNumericProgramOwners(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { proc.Free() })
	ctx := context.Background()
	makeCast := func(t *testing.T, source *planpb.Expr, kind *planpb.Expr) *planpb.Expr {
		t.Helper()
		cast, err := appendNumericValueCastWithKind(ctx, source, kind,
			makeSimplePlan2Type(types.T_int32), function.NumericValueCastPolicyOrdinary)
		require.NoError(t, err)
		return cast
	}
	assertUnchanged := func(t *testing.T, expr *planpb.Expr) {
		t.Helper()
		before := DeepCopyExpr(expr)
		var executors []colexec.ExpressionExecutor
		defer func() {
			for _, executor := range executors {
				executor.Free()
			}
		}()
		canFold, err := ReplaceFoldExpr(proc, expr, &executors)
		require.NoError(t, err)
		require.False(t, canFold)
		require.Empty(t, executors, "an incomplete owner must not create a partial executor")
		require.True(t, proto.Equal(before, expr), "the value/kind owner must remain unchanged")
	}

	column := &planpb.Expr{
		Typ:  types.T_text.ToType().PlanType(),
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
	}
	value, kind, err := makeNumericValueProgramOutputs(ctx, column)
	require.NoError(t, err)
	rowDependent := makeCast(t, value, kind)
	require.False(t, function.IsStatementConstantInput(rowDependent))
	assertUnchanged(t, rowDependent)

	param := &planpb.Expr{
		Typ:  types.T_text.ToType().PlanType(),
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
	}
	value, kind, err = makeNumericValueProgramOutputs(ctx, param)
	require.NoError(t, err)
	rowDiagnosticCast, err := appendNumericValueCastWithKind(ctx, value, kind,
		makeSimplePlan2Type(types.T_decimal64), function.NumericValueCastPolicyExplicit)
	require.NoError(t, err)
	require.True(t, function.IsStatementConstantInput(rowDiagnosticCast))
	require.True(t, function.ContainsRowScopedConversion(rowDiagnosticCast))
	assertUnchanged(t, rowDiagnosticCast)

	physicalCast := makeCast(t,
		&planpb.Expr{Typ: types.T_text.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}},
		&planpb.Expr{Typ: types.T_bool.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}})
	assertUnchanged(t, physicalCast)

	value, kind, err = makeNumericValueProgramOutputs(ctx, param)
	require.NoError(t, err)
	definition := value.GetF().Args[0]
	assertUnchanged(t, value)
	assertUnchanged(t, kind)
	assertUnchanged(t, definition)
}
