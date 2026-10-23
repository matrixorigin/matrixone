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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestScalarNumericProgramPairsValueAndSelectedBranchKind(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	input := batch.NewWithSize(3)
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	input.Vecs[0] = testutil.NewVector(2, types.T_bool.ToType(), proc.Mp(), false, []bool{true, false})
	input.Vecs[1] = testutil.NewVector(2, types.T_bool.ToType(), proc.Mp(), false, []bool{false, true})
	input.Vecs[2] = testutil.NewVectorWithNulls(2, types.T_varchar.ToType(), proc.Mp(), false, []bool{false, true}, []string{"2", ""})
	input.SetRowCount(2)

	boolColumn := func(pos int) *plan.Expr {
		return &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: int32(pos)}}}
	}
	binaryText := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_Sval{Sval: "1"},
			IsBin: true,
		}},
	}
	ordinaryText := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "1"}}},
	}
	makeProgramCast := func(source *plan.Expr, scope int64) *plan.Expr {
		t.Helper()
		int64Literal := func(value int64) *plan.Expr {
			return &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: value}}}}
		}
		int32Literal := func(value int32) *plan.Expr {
			return &plan.Expr{Typ: plan.Type{Id: int32(types.T_int32)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I32Val{I32Val: value}}}}
		}
		programRef := func(args []*plan.Expr, typ types.Type) *plan.Expr {
			return &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: "_scalar_numeric_program_ref", Obj: int64(function.SCALAR_PROGRAM_REF) << 32},
				Args: args,
			}}}
		}
		definition := &plan.Expr{Typ: plan.Type{Id: int32(types.T_any)}, Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "_scalar_numeric_program", Obj: int64(function.SCALAR_PROGRAM) << 32},
			Args: []*plan.Expr{int64Literal(scope), source},
		}}}
		valueRef := programRef([]*plan.Expr{definition, int64Literal(scope), int64Literal(0)}, types.MustTypeFromPlan(source.Typ))
		kindRef := programRef([]*plan.Expr{int64Literal(scope), int64Literal(1)}, types.T_bool.ToType())
		target := types.T_int64.ToType()
		args := []*plan.Expr{
			valueRef,
			{Typ: target.PlanType(), Expr: &plan.Expr_T{T: &plan.TargetType{}}},
			kindRef,
			int32Literal(function.NumericValueCastPolicyComparison),
		}
		argTypes := []types.Type{types.MustTypeFromPlan(valueRef.Typ), target, types.T_bool.ToType(), types.T_int32.ToType()}
		resolved, err := function.GetFunctionByNameWithOverload(proc.Ctx, "cast", argTypes, function.NumericValueCastOverload)
		require.NoError(t, err)
		return &plan.Expr{Typ: target.PlanType(), Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "cast", Obj: resolved.GetEncodedOverloadID()},
			Args: args,
		}}}
	}

	direct := bindTestFunction(t, proc, "if", boolColumn(0), binaryText, ordinaryText)
	directCast := makeProgramCast(direct, 101)
	directExecutor, err := NewExpressionExecutor(proc, directCast)
	require.NoError(t, err)
	t.Cleanup(directExecutor.Free)
	directResult, err := directExecutor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, []int64{49, 1}, vector.MustFixedColWithTypeCheck[int64](directResult))

	explicitString := bindTestCast(t, proc, 1, binaryText, types.T_varchar.ToType())
	explicitExecutor, err := NewExpressionExecutor(proc, makeProgramCast(explicitString, 106))
	require.NoError(t, err)
	t.Cleanup(explicitExecutor.Free)
	explicitResult, err := explicitExecutor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 1}, vector.MustFixedColWithTypeCheck[int64](explicitResult))

	preparedParams := vector.NewVec(types.T_varchar.ToType())
	t.Cleanup(func() { preparedParams.Free(proc.Mp()) })
	previousParams := proc.DetachPrepareParams()
	t.Cleanup(func() { proc.RestorePrepareParams(previousParams) })
	require.NoError(t, vector.AppendBytes(preparedParams, []byte("1"), false, proc.Mp()))
	proc.SetPrepareParamsWithIsBin(preparedParams, []bool{true})
	parameter := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}}
	preparedExecutor, err := NewExpressionExecutor(proc, makeProgramCast(parameter, 107))
	require.NoError(t, err)
	t.Cleanup(preparedExecutor.Free)
	preparedResult, err := preparedExecutor.Eval(proc, nil, nil)
	require.NoError(t, err)
	require.Equal(t, []int64{49}, vector.MustFixedColWithTypeCheck[int64](preparedResult))
	preparedExecutor.ResetForNextQuery()
	proc.SetPrepareParamsWithIsBin(preparedParams, []bool{false})
	preparedResult, err = preparedExecutor.Eval(proc, nil, nil)
	require.NoError(t, err)
	require.Equal(t, []int64{1}, vector.MustFixedColWithTypeCheck[int64](preparedResult))

	caseSource := bindTestFunction(t, proc, "case", boolColumn(1), binaryText, ordinaryText)
	caseExecutor, err := NewExpressionExecutor(proc, makeProgramCast(caseSource, 104))
	require.NoError(t, err)
	t.Cleanup(caseExecutor.Free)
	caseResult, err := caseExecutor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 49}, vector.MustFixedColWithTypeCheck[int64](caseResult))

	coalesceSource := bindTestFunction(t, proc, "coalesce", &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 2}},
	}, binaryText)
	coalesceExecutor, err := NewExpressionExecutor(proc, makeProgramCast(coalesceSource, 105))
	require.NoError(t, err)
	t.Cleanup(coalesceExecutor.Free)
	coalesceResult, err := coalesceExecutor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, []int64{2, 49}, vector.MustFixedColWithTypeCheck[int64](coalesceResult))

	maskedSource := bindTestFunction(t, proc, "if", boolColumn(1), binaryText, ordinaryText)
	maskedCast := makeProgramCast(maskedSource, 102)
	outer := bindTestFunction(t, proc, "if", boolColumn(1), maskedCast, makePlan2Int64ConstExprWithType(0))
	outerExecutor, err := NewExpressionExecutor(proc, outer)
	require.NoError(t, err)
	t.Cleanup(outerExecutor.Free)
	maskedResult, err := outerExecutor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 49}, vector.MustFixedColWithTypeCheck[int64](maskedResult))

	partialSource := bindTestFunction(t, proc, "if", boolColumn(0), ordinaryText, binaryText)
	partialCast := makeProgramCast(partialSource, 103)
	partialOuter := bindTestFunction(t, proc, "if", boolColumn(1), partialCast, makePlan2Int64ConstExprWithType(0))
	partialExecutor, err := NewExpressionExecutor(proc, partialOuter)
	require.NoError(t, err)
	t.Cleanup(partialExecutor.Free)
	partialResult, err := partialExecutor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 49}, vector.MustFixedColWithTypeCheck[int64](partialResult))
}

func TestProjectionProgramGroupCarriesOrdinaryKindPassenger(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	input := batch.NewWithSize(1)
	input.Vecs[0] = testutil.NewVector(2, types.T_bool.ToType(), proc.Mp(), false, []bool{true, false})
	input.SetRowCount(2)
	t.Cleanup(func() { input.Clean(proc.Mp()) })

	int64Literal := func(value int64) *plan.Expr {
		return &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: value}}}}
	}
	programRef := func(scope, slot int64, typ types.Type, definition *plan.Expr) *plan.Expr {
		args := []*plan.Expr{int64Literal(scope), int64Literal(slot)}
		if definition != nil {
			args = append([]*plan.Expr{definition}, args...)
		}
		return &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "_scalar_numeric_program_ref", Obj: int64(function.SCALAR_PROGRAM_REF) << 32},
			Args: args,
		}}}
	}
	programDefinition := func(scope int64, args ...*plan.Expr) *plan.Expr {
		return &plan.Expr{Typ: plan.Type{Id: int32(types.T_any)}, Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "_scalar_numeric_program", Obj: int64(function.SCALAR_PROGRAM) << 32},
			Args: append([]*plan.Expr{int64Literal(scope)}, args...),
		}}}
	}
	column := func(pos int, typ types.Type) *plan.Expr {
		return &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: int32(pos)}}}
	}

	condition := column(0, types.T_bool.ToType())
	binary := &plan.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{
		Value: &plan.Literal_Sval{Sval: "1"}, IsBin: true,
	}}}
	ordinary := &plan.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "1"}}}}
	producer := bindTestFunction(t, proc, "if", condition, binary, ordinary)
	scope := int64(301)
	definition := programDefinition(scope, producer)
	projectionExprs := []*plan.Expr{
		programRef(scope, 0, types.MustTypeFromPlan(producer.Typ), definition),
		programRef(scope, 1, types.T_bool.ToType(), nil),
	}
	executors, group, err := NewProjectionExpressionExecutorsWithAllocation(proc, projectionExprs, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, executor := range executors {
			executor.Free()
		}
		group.ResetForNextQuery()
	})
	projected := batch.NewWithSize(len(executors))
	projected.SetRowCount(input.RowCount())
	group.BeginBatch()
	for i, executor := range executors {
		projected.Vecs[i], err = executor.Eval(proc, []*batch.Batch{input}, nil)
		require.NoError(t, err)
	}
	require.Equal(t, []string{"1", "1"}, []string{
		string(projected.Vecs[0].GetBytesAt(0)),
		string(projected.Vecs[0].GetBytesAt(1)),
	})
	require.Equal(t, []bool{true, false}, vector.MustFixedColWithTypeCheck[bool](projected.Vecs[1]))

	castScope := int64(302)
	castDefinition := programDefinition(castScope,
		column(0, types.MustTypeFromPlan(producer.Typ)),
		column(1, types.T_bool.ToType()),
	)
	valueRef := programRef(castScope, 0, types.MustTypeFromPlan(producer.Typ), castDefinition)
	kindRef := programRef(castScope, 1, types.T_bool.ToType(), nil)
	target := types.T_int64.ToType()
	targetArg := &plan.Expr{Typ: target.PlanType(), Expr: &plan.Expr_T{T: &plan.TargetType{}}}
	policy := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int32)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I32Val{I32Val: function.NumericValueCastPolicyComparison}}}}
	args := []*plan.Expr{valueRef, targetArg, kindRef, policy}
	argTypes := []types.Type{types.MustTypeFromPlan(valueRef.Typ), target, types.T_bool.ToType(), types.T_int32.ToType()}
	cast, err := function.GetFunctionByNameWithOverload(proc.Ctx, "cast", argTypes, function.NumericValueCastOverload)
	require.NoError(t, err)
	castExpr := &plan.Expr{Typ: target.PlanType(), Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: "cast", Obj: cast.GetEncodedOverloadID()}, Args: args,
	}}}
	consumer, err := NewExpressionExecutor(proc, castExpr)
	require.NoError(t, err)
	t.Cleanup(consumer.Free)
	result, err := consumer.Eval(proc, []*batch.Batch{projected}, nil)
	require.NoError(t, err)
	require.Equal(t, []int64{49, 1}, vector.MustFixedColWithTypeCheck[int64](result))
}

func TestScalarNumericProgramEvalWithoutResultReusingReturnsOwnedValueAndKind(t *testing.T) {
	proc, selection, _ := dependentTestAllocation(t)
	input := batch.NewWithSize(2)
	input.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte("31"), false, proc.Mp()))
	input.Vecs[1] = testutil.NewVector(1, types.T_bool.ToType(), proc.Mp(), false, []bool{true})
	input.SetRowCount(1)
	t.Cleanup(func() { input.Clean(proc.Mp()) })

	int64Literal := func(value int64) *plan.Expr {
		return &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: value}}}}
	}
	borrowedText := &plan.Expr{
		Typ:  types.T_varchar.ToType().PlanType(),
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}},
	}
	borrowedKind := &plan.Expr{
		Typ:  types.T_bool.ToType().PlanType(),
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 1}},
	}
	scope := int64(701)
	definition := &plan.Expr{Typ: plan.Type{Id: int32(types.T_any)}, Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: "_scalar_numeric_program", Obj: int64(function.SCALAR_PROGRAM) << 32},
		Args: []*plan.Expr{int64Literal(scope), borrowedText, borrowedKind},
	}}}
	programRef := func(args []*plan.Expr, typ types.Type) *plan.Expr {
		return &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "_scalar_numeric_program_ref", Obj: int64(function.SCALAR_PROGRAM_REF) << 32},
			Args: args,
		}}}
	}
	valueRef := programRef([]*plan.Expr{definition, int64Literal(scope), int64Literal(0)}, types.T_varchar.ToType())
	kindRef := programRef([]*plan.Expr{int64Literal(scope), int64Literal(1)}, types.T_bool.ToType())
	executors, group, err := NewProjectionExpressionExecutorsWithAllocation(proc, []*plan.Expr{valueRef, kindRef}, selection)
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, executor := range executors {
			executor.Free()
		}
	})
	group.BeginBatch()
	value, err := executors[0].EvalWithoutResultReusing(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	kind, err := executors[1].EvalWithoutResultReusing(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.NotNil(t, value)
	require.NotNil(t, kind)
	require.Equal(t, selection, value.AllocationAccountSelection())
	require.Equal(t, selection, kind.AllocationAccountSelection())
	t.Cleanup(func() {
		value.Free(proc.Mp())
		kind.Free(proc.Mp())
	})
	require.Len(t, group.programs, 1)
	state := group.programs[0]
	require.NotSame(t, state.valueRows, value, "the returned raw vector must not alias state-owned storage")
	require.NotSame(t, state.kindRows, kind, "the returned kind vector must not alias state-owned storage")
	require.Equal(t, "31", value.GetStringAt(0))
	require.True(t, vector.GetFixedAtNoTypeCheck[bool](kind, 0))

	// Re-evaluating an output invalidates the cache but does not overwrite the
	// independently owned values transferred by the previous call.
	group.BeginBatch()
	next, err := executors[0].EvalWithoutResultReusing(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, "31", value.GetStringAt(0), "the previous transferred vector remains owned by its caller")
	next.Free(proc.Mp())
}
