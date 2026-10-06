// Copyright 2021 Matrix Origin
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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// bindTestFunction builds a typed test plan through the production registry.
// It owns no executor state and does not calculate expected results.
func bindTestFunction(t testing.TB, proc *process.Process, name string, args ...*plan.Expr) *plan.Expr {
	t.Helper()
	argTypes := make([]types.Type, len(args))
	var err error
	for i, arg := range args {
		argTypes[i], err = types.TypeFromPlan(arg.Typ)
		require.NoError(t, err)
		argTypes[i].SetNotNull(arg.Typ.NotNullable)
	}
	resolved, err := function.GetFunctionByName(proc.Ctx, name, argTypes)
	require.NoError(t, err)
	typ := resolved.GetReturnType()
	planType := typ.PlanType()
	planType.NotNullable = typ.GetNotNull()
	return &plan.Expr{
		Typ:  planType,
		Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: resolved.GetEncodedOverloadID(), ObjName: name}, Args: args}},
	}
}

// bindTestCast retains the requested registered identity, including private casts.
func bindTestCast(t *testing.T, proc *process.Process, overload int32, source *plan.Expr, target types.Type) *plan.Expr {
	t.Helper()
	sourceType, err := types.TypeFromPlan(source.Typ)
	require.NoError(t, err)
	resolved, err := function.GetFunctionByNameWithOverload(proc.Ctx, "cast", []types.Type{sourceType, target}, overload)
	require.NoError(t, err)
	typ := plan.Type{Id: int32(target.Oid), Width: target.Width, Scale: target.Scale}
	return &plan.Expr{Typ: typ, Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: "cast", Obj: resolved.GetEncodedOverloadID()},
		Args: []*plan.Expr{source, {Typ: typ, Expr: &plan.Expr_T{T: &plan.TargetType{}}}},
	}}}
}

// evalTestTextParameter scopes transport ownership and restores the exact prior state.
func evalTestTextParameter(t *testing.T, proc *process.Process, executor ExpressionExecutor, value string, isNull bool, kind vector.PrepareParamKind, batches []*batch.Batch, mask []bool) *vector.Vector {
	t.Helper()
	params := vector.NewVec(types.T_text.ToType())
	defer params.Free(proc.Mp())
	previous := proc.DetachPrepareParams()
	defer proc.RestorePrepareParams(previous)
	require.NoError(t, vector.AppendBytes(params, []byte(value), isNull, proc.Mp()))
	proc.SetPrepareParamsWithMeta(params, nil, []vector.PrepareParamKind{kind})
	result, err := executor.Eval(proc, batches, mask)
	require.NoError(t, err)
	return result
}

// checkExpressionStorageAfterCleanup must be registered before scenario resources.
func checkExpressionStorageAfterCleanup(t *testing.T, proc *process.Process) {
	t.Helper()
	t.Cleanup(func() {
		assert.Zero(t, proc.Mp().CurrNB(), "native storage after scenario cleanup")
		bytes, objects := proc.Mp().OnHeapOutstanding()
		assert.Zero(t, bytes, "heap storage after scenario cleanup")
		assert.Zero(t, objects, "heap objects after scenario cleanup")
	})
}

type failingExpressionExecutor struct {
	calls int
}

func (e *failingExpressionExecutor) Eval(_ *process.Process, _ []*batch.Batch, _ []bool) (*vector.Vector, error) {
	e.calls++
	return nil, moerr.NewInvalidInputNoCtx("unexpected branch evaluation")
}

func (e *failingExpressionExecutor) EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch, selectList []bool) (*vector.Vector, error) {
	return e.Eval(proc, batches, selectList)
}

func (e *failingExpressionExecutor) ResetForNextQuery() {}
func (e *failingExpressionExecutor) Free()              {}
func (e *failingExpressionExecutor) IsColumnExpr() bool { return false }
func (e *failingExpressionExecutor) TypeName() string   { return "failing" }

type failAfterFirstExpressionExecutor struct {
	calls    int
	delegate ExpressionExecutor
}

func (e *failAfterFirstExpressionExecutor) Eval(proc *process.Process, batches []*batch.Batch, selectList []bool) (*vector.Vector, error) {
	e.calls++
	if e.calls > 1 {
		return nil, moerr.NewInvalidInputNoCtx("inactive branch evaluated after batch shrink")
	}
	return e.delegate.Eval(proc, batches, selectList)
}

func (e *failAfterFirstExpressionExecutor) EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch, selectList []bool) (*vector.Vector, error) {
	return e.Eval(proc, batches, selectList)
}

func (e *failAfterFirstExpressionExecutor) ResetForNextQuery() { e.delegate.ResetForNextQuery() }
func (e *failAfterFirstExpressionExecutor) Free()              { e.delegate.Free() }
func (e *failAfterFirstExpressionExecutor) IsColumnExpr() bool { return false }
func (e *failAfterFirstExpressionExecutor) TypeName() string   { return "failAfterFirst" }

func TestMemoExpressionLifecycle(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	checkExpressionStorageAfterCleanup(t, proc)
	t.Run("cache_error_recovery_transfer", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		previous := proc.GetResolveVariableFunc()
		t.Cleanup(func() { proc.SetResolveVariableFunc(previous) })
		value, calls := int64(1), 0
		var failure error
		proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) { calls++; return value, failure })
		shared := &plan.Expr{AuxId: -1, Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_V{V: &plan.VarRef{Name: "memo"}}}
		second := *shared
		executor, err := NewExpressionExecutor(proc, bindTestFunction(t, proc, "+", shared, &second))
		t.Cleanup(func() {
			if executor != nil {
				executor.Free()
			}
		})
		require.NoError(t, err)
		root := executor.(*memoRootExpressionExecutor)
		require.Len(t, root.states, 1)
		require.Equal(t, 2, root.states[0].refs)

		check := func(v *vector.Vector, want int64) {
			require.Equal(t, types.Type{Oid: types.T_int64, Size: 8}, *v.GetType())
			require.Equal(t, 1, v.Length())
			require.False(t, v.IsNull(0))
			require.Equal(t, want, vector.GetFixedAtNoTypeCheck[int64](v, 0))
		}
		result, err := executor.Eval(proc, nil, nil)
		require.NoError(t, err)
		check(result, 2)
		require.Equal(t, 1, calls)
		parameters := root.executor.(*FunctionExpressionExecutor).parameterResults
		require.Same(t, parameters[0], parameters[1])
		failure = moerr.NewInvalidInputNoCtx("memo source unavailable")
		_, err = executor.Eval(proc, nil, nil)
		require.ErrorIs(t, err, failure)
		require.Equal(t, 2, calls)
		failure, value = nil, 2
		result, err = executor.Eval(proc, nil, nil)
		require.NoError(t, err)
		check(result, 4)
		require.Equal(t, 3, calls)
		value = 3
		owned, err := executor.EvalWithoutResultReusing(proc, nil, nil)
		t.Cleanup(func() {
			if owned != nil && (executor == nil || root.executor.(*FunctionExpressionExecutor).resultVector.GetResultVector() != owned) {
				owned.Free(proc.Mp())
			}
		})
		require.NoError(t, err)
		require.Equal(t, 4, calls)
		require.Nil(t, root.executor.(*FunctionExpressionExecutor).resultVector.GetResultVector())
		executor.Free()
		executor = nil
		check(owned, 6)
	})
	for _, tc := range []struct {
		name  string
		auxID int32
	}{{"direct_column", 0}, {"memo_column", -1}} {
		t.Run(tc.name, func(t *testing.T) {
			checkExpressionStorageAfterCleanup(t, proc)
			input := batch.NewWithSize(1)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			input.Vecs[0] = vector.NewVec(types.T_int32.ToType())
			require.NoError(t, vector.AppendFixedList(input.Vecs[0], []int32{0, 10, 20}, []bool{true, false, false}, proc.Mp()))
			input.SetRowCount(3)
			column := &plan.Expr{AuxId: tc.auxID, Typ: plan.Type{Id: int32(types.T_int32)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{}}}
			executor, err := NewExpressionExecutor(proc, bindTestFunction(t, proc, "cast", column,
				&plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_T{T: &plan.TargetType{}}}))
			if executor != nil {
				t.Cleanup(executor.Free)
			}
			require.NoError(t, err)
			result, err := executor.Eval(proc, []*batch.Batch{input}, []bool{false, true, true})
			require.NoError(t, err)
			require.Equal(t, types.Type{Oid: types.T_int64, Size: 8}, *result.GetType())
			require.Equal(t, 3, result.Length())
			require.True(t, result.IsNull(0))
			for row, want := range []int64{10, 20} {
				require.False(t, result.IsNull(uint64(row+1)))
				require.Equal(t, want, vector.GetFixedAtNoTypeCheck[int64](result, row+1))
			}
		})
	}
}

func TestMemoRowAlignmentClassification(t *testing.T) {
	column := &ColumnExpressionExecutor{}
	functionValue := &FunctionExpressionExecutor{}
	folded := &FunctionExpressionExecutor{}
	folded.folded.canFold = true
	for _, test := range []struct {
		name string
		root ExpressionExecutor
		want bool
	}{
		{name: "column", root: column, want: true},
		{name: "memo column", root: &memoExpressionExecutor{state: &memoExpressionState{executor: column}}, want: true},
		{name: "memo root column", root: &memoRootExpressionExecutor{executor: &memoExpressionExecutor{state: &memoExpressionState{executor: column}}}, want: true},
		{name: "function", root: functionValue, want: true},
		{name: "memo function", root: &memoExpressionExecutor{state: &memoExpressionState{executor: functionValue}}, want: true},
		{name: "folded function", root: &memoExpressionExecutor{state: &memoExpressionState{executor: folded}}},
		{name: "list", root: &memoExpressionExecutor{state: &memoExpressionState{executor: &ListExpressionExecutor{}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, isRowAlignedExpressionExecutor(test.root))
		})
	}
}

func TestConstantExpressionMaterialization(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	checkExpressionStorageAfterCleanup(t, proc)
	t.Run("list_reset_transfer", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		bat := batch.New(nil)
		t.Cleanup(func() { bat.Clean(proc.Mp()) })
		bat.SetRowCount(10)
		expr := &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true},
			Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{
				makePlan2Int64ConstExprWithType(1), makePlan2Int64ConstExprWithType(2),
			}}},
		}
		executor, err := NewExpressionExecutor(proc, expr)
		t.Cleanup(func() {
			if executor != nil {
				executor.Free()
			}
		})
		require.NoError(t, err)
		require.False(t, executor.IsColumnExpr())
		_, err = DebugShowExecutor(executor)
		require.NoError(t, err)
		check := func(vec *vector.Vector) {
			require.Equal(t, types.Type{Oid: types.T_int64, Size: 8}, *vec.GetType())
			require.Equal(t, 2, vec.Length())
			for i, want := range []int64{1, 2} {
				require.False(t, vec.IsNull(uint64(i)))
				require.Equal(t, want, vector.GetFixedAtNoTypeCheck[int64](vec, i))
			}
		}
		vec, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
		require.NoError(t, err)
		check(vec)
		executor.ResetForNextQuery()
		vec, err = executor.Eval(proc, []*batch.Batch{bat}, nil)
		require.NoError(t, err)
		check(vec)
		_, err = DebugShowExecutor(executor)
		require.NoError(t, err)
		owned, err := executor.EvalWithoutResultReusing(proc, []*batch.Batch{bat}, nil)
		t.Cleanup(func() {
			if owned != nil && (executor == nil || executor.(*ListExpressionExecutor).resultVector != owned) {
				owned.Free(proc.Mp())
			}
		})
		require.NoError(t, err)
		require.Nil(t, executor.(*ListExpressionExecutor).resultVector)
		executor.Free()
		executor = nil
		check(owned)
	})
	t.Run("int64_reuse_duplication", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		bat := batch.New(nil)
		t.Cleanup(func() { bat.Clean(proc.Mp()) })
		bat.SetRowCount(10)
		executor, err := NewExpressionExecutor(proc, makePlan2Int64ConstExprWithType(218311))
		t.Cleanup(func() {
			if executor != nil {
				executor.Free()
			}
		})
		require.NoError(t, err)
		_, err = DebugShowExecutor(executor)
		require.NoError(t, err)
		check := func(vec *vector.Vector) {
			require.Equal(t, types.Type{Oid: types.T_int64, Size: 8}, *vec.GetType())
			require.True(t, vec.IsConst())
			require.Equal(t, 10, vec.Length())
			for i := 0; i < 10; i++ {
				require.False(t, vec.IsNull(uint64(i)))
				require.Equal(t, int64(218311), vector.GetFixedAtNoTypeCheck[int64](vec, i))
			}
		}
		vec, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
		require.NoError(t, err)
		check(vec)
		native := proc.Mp().CurrNB()
		heap, objects := proc.Mp().OnHeapOutstanding()
		reused, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
		require.NoError(t, err)
		require.Same(t, vec, reused)
		check(reused)
		require.Equal(t, native, proc.Mp().CurrNB())
		afterHeap, afterObjects := proc.Mp().OnHeapOutstanding()
		require.Equal(t, heap, afterHeap)
		require.Equal(t, objects, afterObjects)
		_, err = DebugShowExecutor(executor)
		require.NoError(t, err)
		owned, err := executor.EvalWithoutResultReusing(proc, []*batch.Batch{bat}, nil)
		t.Cleanup(func() {
			if owned != nil && (executor == nil || executor.(*FixedVectorExpressionExecutor).resultVector != owned) {
				owned.Free(proc.Mp())
			}
		})
		require.NoError(t, err)
		require.NotSame(t, vec, owned)
		require.Same(t, vec, executor.(*FixedVectorExpressionExecutor).resultVector)
		check(vec)
		executor.Free()
		executor = nil
		check(owned)
	})
	for _, tc := range []struct {
		name  string
		expr  *plan.Expr
		typ   types.Type
		rows  int
		null  bool
		check func(*testing.T, *vector.Vector, int)
	}{
		{"decimal128_target_null", &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_decimal128), Width: 30, Scale: 6, NotNullable: true},
			Expr: &plan.Expr_T{T: &plan.TargetType{}},
		}, types.Type{Oid: types.T_decimal128, Size: 16, Width: 30, Scale: 6}, 5, true, nil},
		{"geometry_literal", &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_geometry), NotNullable: true},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: false, Value: &plan.Literal_Sval{Sval: "POINT(1 1)"}}},
		}, types.Type{Oid: types.T_geometry, Size: 24, Charset: types.CharsetBinary}, 3, false,
			func(t *testing.T, v *vector.Vector, i int) { require.Equal(t, "POINT(1 1)", v.GetStringAt(i)) }},
		{"decimal64_literal", &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_decimal64), Width: 2, Scale: 1},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Decimal64Val{Decimal64Val: &plan.Decimal64{A: -15}}}},
		}, types.Type{Oid: types.T_decimal64, Size: 8, Width: 2, Scale: 1}, 1, false,
			func(t *testing.T, v *vector.Vector, i int) {
				require.Equal(t, types.Decimal64(0xfffffffffffffff1), vector.GetFixedAtNoTypeCheck[types.Decimal64](v, i))
			}},
		{"int64_literal", &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: -42}}},
		}, types.Type{Oid: types.T_int64, Size: 8}, 1, false,
			func(t *testing.T, v *vector.Vector, i int) {
				require.Equal(t, int64(-42), vector.GetFixedAtNoTypeCheck[int64](v, i))
			}},
		{"year_literal", &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_year)},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I32Val{I32Val: 2026}}},
		}, types.Type{Oid: types.T_year, Size: 2}, 1, false,
			func(t *testing.T, v *vector.Vector, i int) {
				require.Equal(t, types.MoYear(2026), vector.GetFixedAtNoTypeCheck[types.MoYear](v, i))
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checkExpressionStorageAfterCleanup(t, proc)
			bat := batch.New(nil)
			t.Cleanup(func() { bat.Clean(proc.Mp()) })
			bat.SetRowCount(tc.rows)
			executor, err := NewExpressionExecutor(proc, tc.expr)
			t.Cleanup(func() {
				if executor != nil {
					executor.Free()
				}
			})
			require.NoError(t, err)
			vec, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
			require.NoError(t, err)
			require.Equal(t, tc.typ, *vec.GetType())
			require.True(t, vec.IsConst())
			require.Equal(t, tc.rows, vec.Length())
			_, err = DebugShowExecutor(executor)
			require.NoError(t, err)
			for i := 0; i < tc.rows; i++ {
				require.Equal(t, tc.null, vec.IsNull(uint64(i)))
				if tc.check != nil {
					tc.check(t, vec, i)
				}
			}
		})
	}
}

func TestFlowControlMetadataMethods(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	t.Run("PreservesSelectedBinaryStringRows", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		binary := vector.NewVec(types.T_varchar.ToType())
		text := vector.NewVec(types.T_varchar.ToType())
		result := vector.NewVec(types.T_varchar.ToType())
		defer binary.Free(proc.Mp())
		defer text.Free(proc.Mp())
		defer result.Free(proc.Mp())
		require.NoError(t, vector.AppendBytes(binary, []byte("binary"), false, proc.Mp()))
		require.NoError(t, vector.AppendBytes(binary, []byte("inactive"), false, proc.Mp()))
		require.NoError(t, binary.SetIsBinaryStringAt(0, true, proc.Mp()))
		require.NoError(t, vector.AppendBytes(text, []byte("inactive"), false, proc.Mp()))
		require.NoError(t, vector.AppendBytes(text, []byte("text"), false, proc.Mp()))
		require.NoError(t, vector.AppendBytes(result, []byte("binary"), false, proc.Mp()))
		require.NoError(t, vector.AppendBytes(result, []byte("text"), false, proc.Mp()))

		expr := &FunctionExpressionExecutor{resultType: types.T_varchar.ToType()}
		expr.resetFlowControlPrepareParamKind()
		expr.observeFlowControlPrepareParamKind(binary, nil, []bool{true, false})
		expr.observeFlowControlPrepareParamKind(text, nil, []bool{false, true})
		require.NoError(t, expr.applyFlowControlPrepareParamKinds(result, 2, proc.Mp()))
		require.True(t, result.GetBinaryStringMetadataAt(0))
		require.False(t, result.GetBinaryStringMetadataAt(1))
	})

	t.Run("PromotesSelectedStaticTextUnderBinaryResult", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		text := vector.NewVec(types.T_varchar.ToType())
		result := vector.NewVec(types.T_varbinary.ToType())
		defer text.Free(proc.Mp())
		defer result.Free(proc.Mp())
		require.NoError(t, vector.AppendBytes(text, []byte("text"), false, proc.Mp()))
		require.NoError(t, vector.AppendBytes(result, []byte("text"), false, proc.Mp()))

		expr := &FunctionExpressionExecutor{resultType: types.T_varbinary.ToType()}
		expr.resetFlowControlPrepareParamKind()
		expr.observeFlowControlPrepareParamKind(text, nil, []bool{true})
		require.NoError(t, expr.applyFlowControlPrepareParamKinds(result, 1, proc.Mp()))
		require.Equal(t, types.RuntimeStringText, result.GetRuntimeStringDomainAt(0))
	})

	t.Run("SameDomainWithoutProvenanceKeepsFastPath", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		value, err := vector.NewConstBytes(
			types.T_varchar.ToType(), []byte("ordinary"), 2, proc.Mp())
		require.NoError(t, err)
		defer value.Free(proc.Mp())
		selection := make([]bool, value.Length())
		for i := range selection {
			selection[i] = true
		}

		for _, fid := range []int32{function.IFF, function.CASE, function.COALESCE} {
			expr := &FunctionExpressionExecutor{resultType: types.T_varchar.ToType()}
			expr.fid = fid
			expr.observeFlowControlPrepareParamKind(value, nil, selection)
			require.Nil(t, expr.flowControlStringDomains)
			require.Nil(t, expr.flowControlKinds)
			require.True(t, expr.flowControlKindSeen)
			require.Equal(t, vector.PrepareParamNone, expr.flowControlKind)
		}
	})

	t.Run("UsesSourceDomainBeforeImplicitCast", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		tests := []struct {
			name       string
			sourceType types.Type
			castType   types.Type
			want       types.RuntimeStringDomain
		}{
			{name: "text under binary result", sourceType: types.T_varchar.ToType(), castType: types.T_varbinary.ToType(), want: types.RuntimeStringText},
			{name: "binary under text result", sourceType: types.T_varbinary.ToType(), castType: types.T_varchar.ToType(), want: types.RuntimeStringBinary},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				source := vector.NewVec(tt.sourceType)
				casted := vector.NewVec(tt.castType)
				result := vector.NewVec(tt.castType)
				defer source.Free(proc.Mp())
				defer casted.Free(proc.Mp())
				defer result.Free(proc.Mp())
				require.NoError(t, vector.AppendBytes(source, []byte("selected"), false, proc.Mp()))
				require.NoError(t, vector.AppendBytes(casted, []byte("selected"), false, proc.Mp()))
				require.NoError(t, vector.AppendBytes(result, []byte("selected"), false, proc.Mp()))

				castExecutor := &FunctionExpressionExecutor{
					functionInformationForEval: functionInformationForEval{
						fid:        function.CAST,
						overloadID: function.EncodeOverloadID(function.CAST, 0),
					},
					parameterResults:  []*vector.Vector{source},
					parameterExecutor: []ExpressionExecutor{nil},
				}
				expr := &FunctionExpressionExecutor{resultType: tt.castType}
				expr.observeFlowControlPrepareParamKind(casted, castExecutor, []bool{true})
				require.NoError(t, expr.applyFlowControlPrepareParamKinds(result, 1, proc.Mp()))
				require.Equal(t, tt.want, result.GetRuntimeStringDomainAt(0))
			})
		}
	})

	t.Run("KeepsExplicitCastAsSemanticBoundary", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		source := vector.NewVec(types.T_varchar.ToType())
		casted := vector.NewVec(types.T_varbinary.ToType())
		result := vector.NewVec(types.T_varbinary.ToType())
		defer source.Free(proc.Mp())
		defer casted.Free(proc.Mp())
		defer result.Free(proc.Mp())
		for _, vec := range []*vector.Vector{source, casted, result} {
			require.NoError(t, vector.AppendBytes(vec, []byte("selected"), false, proc.Mp()))
		}
		castExecutor := &FunctionExpressionExecutor{
			functionInformationForEval: functionInformationForEval{
				fid:        function.CAST,
				overloadID: function.EncodeOverloadID(function.CAST, 1),
			},
			parameterResults:  []*vector.Vector{source},
			parameterExecutor: []ExpressionExecutor{nil},
		}
		expr := &FunctionExpressionExecutor{resultType: types.T_varbinary.ToType()}
		expr.observeFlowControlPrepareParamKind(casted, castExecutor, []bool{true})
		require.NoError(t, expr.applyFlowControlPrepareParamKinds(result, 1, proc.Mp()))
		require.Equal(t, types.RuntimeStringInherit, result.GetRuntimeStringDomainAt(0))
	})
}

func TestLiteralMaterialization(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	checkExpressionStorageAfterCleanup(t, proc)
	t.Run("forms", func(t *testing.T) {
		tests := []struct {
			name          string
			typ           types.Type
			form          plan.StringLiteralForm
			want          types.RuntimeStringDomain
			wantEffective types.StringDomain
			wantVarchar   bool
		}{
			{name: "text on text", typ: types.T_varchar.ToType(), form: plan.StringLiteralForm_STRING_LITERAL_TEXT, want: types.RuntimeStringInherit, wantEffective: types.StringDomainText},
			{name: "text on binary", typ: types.T_varbinary.ToType(), form: plan.StringLiteralForm_STRING_LITERAL_TEXT, want: types.RuntimeStringText, wantEffective: types.StringDomainText},
			{name: "binary on text", typ: types.T_varchar.ToType(), form: plan.StringLiteralForm_STRING_LITERAL_BINARY_INTRODUCER, want: types.RuntimeStringBinary, wantEffective: types.StringDomainBinary},
			{name: "binary on binary", typ: types.T_varbinary.ToType(), form: plan.StringLiteralForm_STRING_LITERAL_BINARY_INTRODUCER, want: types.RuntimeStringInherit, wantEffective: types.StringDomainBinary},
			{name: "raw hex", typ: types.NewWithCharset(types.T_varchar, 0, 0, types.CharsetBinary), form: plan.StringLiteralForm_STRING_LITERAL_HEX, want: types.RuntimeStringInherit, wantEffective: types.StringDomainBinary, wantVarchar: true},
			{name: "raw bit", typ: types.NewWithCharset(types.T_varchar, 0, 0, types.CharsetBinary), form: plan.StringLiteralForm_STRING_LITERAL_BIT, want: types.RuntimeStringInherit, wantEffective: types.StringDomainBinary, wantVarchar: true},
			{name: "char text uses varchar container", typ: types.NewWithCharset(types.T_char, 0, 0, types.CharsetUTF8), form: plan.StringLiteralForm_STRING_LITERAL_TEXT, want: types.RuntimeStringInherit, wantEffective: types.StringDomainText, wantVarchar: true},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				checkExpressionStorageAfterCleanup(t, proc)
				vec, err := generateConstExpressionExecutor(proc, test.typ, &plan.Literal{
					Value:       &plan.Literal_Sval{Sval: "selected"},
					LiteralForm: test.form,
				}, nil)
				if vec != nil {
					t.Cleanup(func() { vec.Free(proc.Mp()) })
				}
				require.NoError(t, err)
				require.Equal(t, types.StringSourceLiteral, vec.GetStringSourceAt(0))
				require.Equal(t, test.want, vec.GetRuntimeStringDomainAt(0))
				effective := types.StaticStringDomain(*vec.GetType())
				if test.want == types.RuntimeStringText {
					effective = types.StringDomainText
				} else if test.want == types.RuntimeStringBinary {
					effective = types.StringDomainBinary
				}
				require.Equal(t, test.wantEffective, effective)
				if test.wantVarchar {
					require.Equal(t, types.T_varchar, vec.GetType().Oid)
				}
			})
		}
	})
	t.Run("resolved binary", func(t *testing.T) {
		for _, typ := range []types.Type{types.New(types.T_binary, 3, 0), types.T_varbinary.ToType(), types.T_blob.ToType()} {
			t.Run(typ.Oid.String(), func(t *testing.T) {
				checkExpressionStorageAfterCleanup(t, proc)
				vec, err := generateConstExpressionExecutor(proc, typ, &plan.Literal{Value: &plan.Literal_Sval{Sval: "a\x00b"}}, nil)
				if vec != nil {
					t.Cleanup(func() { vec.Free(proc.Mp()) })
				}
				require.NoError(t, err)
				require.Equal(t, typ.Oid, vec.GetType().Oid)
				require.Equal(t, []byte("a\x00b"), vec.GetBytesAt(0))
			})
		}
	})
	t.Run("scalar rejection", func(t *testing.T) {
		for _, isNull := range []bool{false, true} {
			t.Run(fmt.Sprintf("null=%t", isNull), func(t *testing.T) {
				checkExpressionStorageAfterCleanup(t, proc)
				before := proc.Mp().CurrNB()
				bytes, objects := proc.Mp().OnHeapOutstanding()
				executor, err := NewExpressionExecutor(proc, &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: isNull, Value: &plan.Literal_Sval{Sval: "value"}, StringSource: 257}}})
				if executor != nil {
					t.Cleanup(executor.Free)
				}
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "%v", err)
				require.ErrorContains(t, err, "invalid literal string source 257")
				require.Nil(t, executor)
				require.Equal(t, before, proc.Mp().CurrNB())
				afterBytes, afterObjects := proc.Mp().OnHeapOutstanding()
				require.Equal(t, bytes, afterBytes)
				require.Equal(t, objects, afterObjects)
			})
		}
	})
	t.Run("list and vector", func(t *testing.T) {
		var data []byte
		if !t.Run("direct", func(t *testing.T) {
			checkExpressionStorageAfterCleanup(t, proc)
			exprs := []*plan.Expr{
				{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "a"}}}},
				{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "b"}}}},
			}
			vec, err := GenerateConstListExpressionExecutor(proc, exprs)
			if vec != nil {
				t.Cleanup(func() { vec.Free(proc.Mp()) })
			}
			require.NoError(t, err)
			require.Equal(t, 2, vec.Length())
			for row, want := range []string{"a", "b"} {
				require.Equal(t, types.StringSourceLiteral, vec.GetStringSourceAt(row))
				require.Equal(t, want, vec.GetStringAt(row))
			}
			data, err = vec.MarshalBinary()
			require.NoError(t, err)
		}) {
			return
		}
		for _, source := range []uint32{uint32(types.StringSourceLiteral), uint32(types.StringSourceCOMStmt) + 1, 256, 257, ^uint32(0)} {
			name := fmt.Sprintf("rejected source %d", source)
			if source == uint32(types.StringSourceLiteral) {
				name = "decoded"
			}
			t.Run(name, func(t *testing.T) {
				checkExpressionStorageAfterCleanup(t, proc)
				before := proc.Mp().CurrNB()
				bytes, objects := proc.Mp().OnHeapOutstanding()
				executor, err := NewExpressionExecutor(proc, &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Len: 2, Data: data, StringSource: source}}})
				if executor != nil {
					t.Cleanup(executor.Free)
				}
				if source != uint32(types.StringSourceLiteral) {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "%v", err)
					require.ErrorContains(t, err, fmt.Sprintf("invalid literal vector string source %d", source))
					require.Nil(t, executor)
					require.Equal(t, before, proc.Mp().CurrNB())
					afterBytes, afterObjects := proc.Mp().OnHeapOutstanding()
					require.Equal(t, bytes, afterBytes)
					require.Equal(t, objects, afterObjects)
					return
				}
				require.NoError(t, err)
				result, err := executor.Eval(proc, nil, nil)
				require.NoError(t, err)
				require.Equal(t, 2, result.Length())
				for row, want := range []string{"a", "b"} {
					require.Equal(t, types.StringSourceLiteral, result.GetStringSourceAt(row))
					require.Equal(t, want, result.GetStringAt(row))
				}
			})
		}
	})
	t.Run("list failure ownership", func(t *testing.T) {
		for _, test := range []struct {
			name  string
			later *plan.Expr
			code  uint16
		}{
			{"nonliteral", &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}, moerr.ErrInternal},
			{"timestamp precision", &plan.Expr{Typ: plan.Type{Id: int32(types.T_timestamp), Scale: 7}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Timestampval{Timestampval: 1}}}}, moerr.ErrTooBigPrecision},
			{"unsupported payload", &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{}}}, moerr.ErrNYI},
			{"payload write", &plan.Expr{Typ: plan.Type{Id: int32(types.T_json)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: ""}}}}, moerr.ErrInvalidInput},
			{"invalid source", &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "b"}, StringSource: 257}}}, moerr.ErrInvalidInput},
		} {
			t.Run(test.name, func(t *testing.T) {
				checkExpressionStorageAfterCleanup(t, proc)
				first := &plan.Expr{Typ: test.later.Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "a"}}}}
				if test.later.Typ.Id == int32(types.T_timestamp) {
					first.Expr = &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Timestampval{Timestampval: 1}}}
					first.Typ.Scale = 6
				}
				if test.later.Typ.Id == int32(types.T_json) {
					first.Expr = &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}
				}
				before := proc.Mp().CurrNB()
				bytes, objects := proc.Mp().OnHeapOutstanding()
				vec, err := GenerateConstListExpressionExecutor(proc, []*plan.Expr{first, test.later})
				if vec != nil {
					t.Cleanup(func() { vec.Free(proc.Mp()) })
				}
				require.True(t, moerr.IsMoErrCode(err, test.code), "%v", err)
				require.Nil(t, vec)
				require.Equal(t, before, proc.Mp().CurrNB())
				afterBytes, afterObjects := proc.Mp().OnHeapOutstanding()
				require.Equal(t, bytes, afterBytes)
				require.Equal(t, objects, afterObjects)
			})
		}
	})
	t.Run("sidecar capacity", func(t *testing.T) {
		const capacity = 1 << 20
		pool, err := mpool.NewMPool("literal list sidecar", capacity, mpool.NoFixed)
		if pool != nil {
			t.Cleanup(func() { mpool.DeleteMPool(pool) })
		}
		require.NoError(t, err)
		proc := testutil.NewProcess(t, testutil.WithMPool(pool), testutil.WithFileService(nil))
		checkExpressionStorageAfterCleanup(t, proc)
		pressure, err := pool.Alloc(capacity-1, true)
		if pressure != nil {
			t.Cleanup(func() { pool.Free(pressure) })
		}
		require.NoError(t, err)
		exprs := []*plan.Expr{
			{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "a"}}}},
			{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "b"}, StringSource: uint32(types.StringSourceSQLPrepare) + 1}}},
		}
		before := pool.CurrNB()
		bytes, objects := pool.OnHeapOutstanding()
		vec, err := GenerateConstListExpressionExecutor(proc, exprs)
		if vec != nil {
			t.Cleanup(func() { vec.Free(pool) })
		}
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrMPoolCapacity), "%v", err)
		require.Nil(t, vec)
		require.Equal(t, before, pool.CurrNB())
		afterBytes, afterObjects := pool.OnHeapOutstanding()
		require.Equal(t, bytes, afterBytes)
		require.Equal(t, objects, afterObjects)
		pool.Free(pressure)
		pressure = nil
		recovered, err := GenerateConstListExpressionExecutor(proc, exprs)
		if recovered != nil {
			t.Cleanup(func() { recovered.Free(pool) })
		}
		require.NoError(t, err)
		require.Equal(t, 2, recovered.Length())
		require.Equal(t, "a", recovered.GetStringAt(0))
		require.Equal(t, "b", recovered.GetStringAt(1))
		require.Equal(t, types.StringSourceLiteral, recovered.GetStringSourceAt(0))
		require.Equal(t, types.StringSourceSQLPrepare, recovered.GetStringSourceAt(1))
	})

	t.Run("timestamp scale", func(t *testing.T) {
		for _, scale := range []int32{-1, 0, 1, 2, 3, 4, 5, 6, 7} {
			t.Run(fmt.Sprintf("scale_%d", scale), func(t *testing.T) {
				checkExpressionStorageAfterCleanup(t, proc)
				expr := &plan.Expr{
					Typ: plan.Type{Id: int32(types.T_timestamp), Scale: scale, NotNullable: true},
					Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Timestampval{
						Timestampval: 1609459200000000,
					}}},
				}
				executor, err := NewExpressionExecutor(proc, expr)
				if executor != nil {
					t.Cleanup(executor.Free)
				}
				if scale < 0 || scale > 6 {
					require.Nil(t, executor)
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrTooBigPrecision), "%v", err)
					require.EqualError(t, err, fmt.Sprintf("Too-big precision %d specified for 'TIMESTAMP'. Maximum is 6.", scale))
					return
				}
				require.NoError(t, err)
				vec, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
				require.NoError(t, err)
				require.Equal(t, types.Type{Oid: types.T_timestamp, Size: 8, Scale: scale}, *vec.GetType())
				require.True(t, vec.IsConst())
				require.Equal(t, 1, vec.Length())
				require.False(t, vec.IsNull(0))
				require.Equal(t, types.Timestamp(1609459200000000), vector.GetFixedAtNoTypeCheck[types.Timestamp](vec, 0))
			})
		}
	})

}

func TestLiteralStringSourceRejectsWideWireValuesBeforeNarrowing(t *testing.T) {
	for _, rawSource := range []uint32{256, 257, ^uint32(0)} {
		_, err := DecodeLiteralStringSource(&plan.Literal{StringSource: rawSource})
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "%v", err)
		require.ErrorContains(t, err, "invalid literal string source")
	}
	for source := types.StringSourceExpression; source <= types.StringSourceCOMStmt; source++ {
		encoded := uint32(source) + 1
		if source == types.StringSourceLiteral {
			encoded = 0
		}
		decoded, err := DecodeLiteralStringSource(&plan.Literal{StringSource: encoded})
		require.NoError(t, err)
		require.Equal(t, source, decoded)
	}
}

func TestCastStringSourceOwnership(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	t.Cleanup(func() {
		assert.Nil(t, proc.GetPrepareParams())
		assert.False(t, proc.GetBaseProcessRunningStatus())
	})

	for _, test := range []struct {
		name     string
		overload int32
		want     types.StringSource
	}{
		{name: "implicit cast is transparent", overload: 0, want: types.StringSourceSQLPrepare},
		{name: "explicit cast owns result", overload: 1, want: types.StringSourceExpression},
	} {
		t.Run("column "+test.name, func(t *testing.T) {
			checkExpressionStorageAfterCleanup(t, proc)

			input := batch.NewWithSize(1)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			input.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
			for _, value := range []string{"5", "6"} {
				require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte(value), false, proc.Mp()))
			}
			input.SetRowCount(2)

			column := &plan.Expr{
				Typ:  plan.Type{Id: int32(types.T_varchar)},
				Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
			}

			expression := bindTestCast(t, proc, test.overload, column, types.T_varbinary.ToType())
			executor, err := NewExpressionExecutor(proc, expression)
			if executor != nil {
				t.Cleanup(executor.Free)
			}
			require.NoError(t, err)

			require.NoError(t, input.Vecs[0].SetStringSource(types.StringSourceSQLPrepare))
			input.Vecs[0].SetPrepareParamType(types.T_enum)
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, test.want, result.GetStringSourceAt(0))
			require.Equal(t, test.want, result.GetStringSourceAt(1))
			require.Equal(t, "5", result.GetStringAt(0))
			require.Equal(t, "6", result.GetStringAt(1))

			result, err = executor.Eval(proc, []*batch.Batch{input}, []bool{true, false})
			require.NoError(t, err)
			require.Equal(t, test.want, result.GetStringSourceAt(0))
			require.Equal(t, types.StringSourceExpression, result.GetStringSourceAt(1))
			require.False(t, result.IsNull(0))
			require.True(t, result.IsNull(1))
			require.Equal(t, "5", result.GetStringAt(0))
			wantType := types.T_enum
			if test.overload == 1 {
				wantType = types.T_any
			}
			require.Equal(t, wantType, result.GetPrepareParamType())
			require.False(t, result.IsPreparedJSONComparisonParam())

			executor.ResetForNextQuery()
			require.NoError(t, input.Vecs[0].SetStringSource(types.StringSourceLiteral))
			result, err = executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			wantAfterReset := types.StringSourceLiteral
			if test.overload == 1 {
				wantAfterReset = types.StringSourceExpression
			}
			require.Equal(t, wantAfterReset, result.GetStringSourceAt(0))
			require.Equal(t, wantAfterReset, result.GetStringSourceAt(1))
			require.Equal(t, "5", result.GetStringAt(0))
			require.Equal(t, "6", result.GetStringAt(1))
		})
	}
	for _, test := range []struct {
		name     string
		overload int32
		want     types.StringSource
	}{
		{name: "implicit", overload: 0, want: types.StringSourceLiteral},
		{name: "explicit", overload: 1, want: types.StringSourceExpression},
	} {
		t.Run("literal "+test.name, func(t *testing.T) {
			checkExpressionStorageAfterCleanup(t, proc)
			executor, err := NewExpressionExecutor(proc, bindTestCast(t, proc, test.overload, &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_varchar)},
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{
					Value: &plan.Literal_Sval{Sval: "5"},
				}},
			}, types.T_varbinary.ToType()))
			if executor != nil {
				t.Cleanup(executor.Free)
			}
			require.NoError(t, err)
			result, err := executor.Eval(proc, nil, nil)
			require.NoError(t, err)
			require.Equal(t, test.want, result.GetStringSourceAt(0))
			require.Equal(t, "5", result.GetStringAt(0))
		})
	}

	for _, test := range []struct {
		name     string
		overload int32
		want     types.StringSource
	}{
		{name: "implicit", overload: 0, want: types.StringSourceSQLPrepare},
		{name: "explicit", overload: 1, want: types.StringSourceExpression},
	} {
		t.Run("runtime parameter "+test.name, func(t *testing.T) {
			checkExpressionStorageAfterCleanup(t, proc)
			wasRunning := proc.GetBaseProcessRunningStatus()
			t.Cleanup(func() { proc.SetBaseProcessRunningStatus(wasRunning) })
			proc.SetBaseProcessRunningStatus(true)
			params := vector.NewVec(types.T_varchar.ToType())
			t.Cleanup(func() { params.Free(proc.Mp()) })
			state := proc.DetachPrepareParams()
			t.Cleanup(func() { proc.RestorePrepareParams(state) })
			require.NoError(t, vector.AppendBytes(params, []byte("5"), false, proc.Mp()))
			require.NoError(t, params.SetStringSource(types.StringSourceSQLPrepare))
			proc.SetPrepareParams(params)

			executor, err := NewExpressionExecutor(proc, bindTestCast(t, proc, test.overload, &plan.Expr{
				Typ:  plan.Type{Id: int32(types.T_varchar)},
				Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
			}, types.T_varbinary.ToType()))
			if executor != nil {
				t.Cleanup(executor.Free)
			}
			require.NoError(t, err)
			result, err := executor.Eval(proc, nil, nil)
			require.NoError(t, err)
			require.Equal(t, test.want, result.GetStringSourceAt(0))
			require.Equal(t, "5", result.GetStringAt(0))

			executor.ResetForNextQuery()
			require.NoError(t, params.SetStringSource(types.StringSourceCOMStmt))
			result, err = executor.Eval(proc, nil, nil)
			require.NoError(t, err)
			wantAfterReset := types.StringSourceCOMStmt
			if test.overload == 1 {
				wantAfterReset = types.StringSourceExpression
			}
			require.Equal(t, wantAfterReset, result.GetStringSourceAt(0))
			require.Equal(t, "5", result.GetStringAt(0))
		})
	}
}

func TestStringSourceConsumerTotality(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	checkExpressionStorageAfterCleanup(t, proc)
	sources := []types.StringSource{
		types.StringSourceExpression,
		types.StringSourceLiteral,
		types.StringSourceUserVariable,
		types.StringSourceSQLPrepare,
		types.StringSourceCOMStmt,
	}
	input := batch.NewWithSize(1)
	defer input.Clean(proc.Mp())
	input.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	for range sources {
		require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte("5"), false, proc.Mp()))
	}
	require.NoError(t, input.Vecs[0].SetStringSourcesWithMP(sources, proc.Mp()))
	input.SetRowCount(len(sources))

	column := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}
	literal := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_Sval{Sval: "x"},
		}},
	}

	castTo := func(targetType types.Type) *plan.Expr {
		return bindTestFunction(t, proc, "cast", column, &plan.Expr{
			Typ:  plan.Type{Id: int32(targetType.Oid), Width: targetType.Width, Scale: targetType.Scale},
			Expr: &plan.Expr_T{T: &plan.TargetType{}},
		})
	}

	for name, expression := range map[string]*plan.Expr{
		"numeric": castTo(types.T_int64.ToType()),
		"bit":     castTo(types.New(types.T_bit, 8, 0)),
		"json":    bindTestFunction(t, proc, "json_valid", column),
		"string":  bindTestFunction(t, proc, "concat", column, literal),
	} {
		t.Run(name, func(t *testing.T) {
			executor, err := NewExpressionExecutor(proc, expression)
			require.NoError(t, err)
			defer executor.Free()
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, len(sources), result.Length())
			for row := range sources {
				require.False(t, result.IsNull(uint64(row)))
				switch name {
				case "numeric":
					require.Equal(t, int64(5), vector.GetFixedAtNoTypeCheck[int64](result, row))
				case "bit":
					require.Equal(t, uint64(53), vector.GetFixedAtNoTypeCheck[uint64](result, row))
				case "json":
					require.True(t, vector.GetFixedAtNoTypeCheck[bool](result, row))
				case "string":
					require.Equal(t, "5x", result.GetStringAt(row))
				}
			}
			if name == "numeric" {
				defer input.Vecs[0].SetIsBin(false)
				for _, test := range []struct {
					binary bool
					want   int64
				}{{true, 53}, {false, 5}} {
					input.Vecs[0].SetIsBin(test.binary)
					result, err := executor.Eval(proc, []*batch.Batch{input}, []bool{true, false, true, false, true})
					require.NoError(t, err)
					for row := range sources {
						require.Equal(t, row%2 != 0, result.IsNull(uint64(row)))
						if row%2 == 0 {
							require.Equal(t, test.want, vector.GetFixedAtNoTypeCheck[int64](result, row))
						}
					}
				}
			}
		})
	}
}

func TestFlowControlBranchPreparation(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	t.Run("SkipsUnselectedBranch", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		expr := NewFunctionExpressionExecutor()
		defer expr.Free()
		require.NoError(t, expr.Init(proc, 3, types.T_int64.ToType()))
		bat := batch.New(nil)
		bat.SetRowCount(3)

		condition, err := vector.NewConstFixed(types.T_bool.ToType(), false, 1, proc.Mp())
		require.NoError(t, err)
		expr.SetParameter(0, NewFixedVectorExpressionExecutor(proc.Mp(), false, condition))
		elseValue, err := vector.NewConstFixed(types.T_int64.ToType(), int64(42), 1, proc.Mp())
		require.NoError(t, err)
		expr.SetParameter(2, NewFixedVectorExpressionExecutor(proc.Mp(), false, elseValue))
		thenExecutor := &failingExpressionExecutor{}

		expr.SetParameter(1, thenExecutor)

		require.NoError(t, expr.EvalIff(proc, []*batch.Batch{bat}, nil))
		require.Zero(t, thenExecutor.calls)
		require.Equal(t, 3, expr.parameterResults[1].Length())
		require.True(t, expr.parameterResults[1].IsConstNull())
		require.Equal(t, types.T_int64, expr.parameterResults[1].GetType().Oid)
		require.Equal(t, int64(42), vector.GetFixedAtNoTypeCheck[int64](expr.parameterResults[2], 0))
	})

	t.Run("PropagatesSelectedBranchError", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		expr := NewFunctionExpressionExecutor()
		defer expr.Free()
		require.NoError(t, expr.Init(proc, 3, types.T_int64.ToType()))
		bat := batch.New(nil)
		bat.SetRowCount(2)

		condition := vector.NewVec(types.T_bool.ToType())
		expr.SetParameter(0, NewFixedVectorExpressionExecutor(proc.Mp(), false, condition))
		require.NoError(t, vector.AppendFixedList(condition, []bool{true, false}, nil, proc.Mp()))
		thenExecutor := &failingExpressionExecutor{}
		elseValue, err := vector.NewConstFixed(types.T_int64.ToType(), int64(42), 1, proc.Mp())
		require.NoError(t, err)
		later := &failAfterFirstExpressionExecutor{delegate: NewFixedVectorExpressionExecutor(proc.Mp(), false, elseValue)}
		expr.SetParameter(2, later)

		expr.SetParameter(1, thenExecutor)

		err = expr.EvalIff(proc, []*batch.Batch{bat}, nil)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		require.Equal(t, 1, thenExecutor.calls)
		require.Zero(t, later.calls, "a selected failure must stop later evaluation")
	})

	t.Run("UsesStatementCompatibilityMode", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		expr := NewFunctionExpressionExecutor()
		defer expr.Free()
		require.NoError(t, expr.Init(proc, 3, types.T_int64.ToType()))
		nativeMode := proc.GetSessionInfo().MatrixOneNativeMode
		defer func() { proc.GetSessionInfo().MatrixOneNativeMode = nativeMode }()
		bat := batch.New(nil)
		bat.SetRowCount(2)

		condition := testutil.MakeVarlenaVector(
			[][]byte{[]byte("1abc"), []byte("abc")}, nil, types.T_varchar.ToType(), proc.Mp())
		expr.SetParameter(0, NewFixedVectorExpressionExecutor(proc.Mp(), false, condition))
		thenValue, err := vector.NewConstFixed(types.T_int64.ToType(), int64(11), 2, proc.Mp())
		require.NoError(t, err)
		expr.SetParameter(1, NewFixedVectorExpressionExecutor(proc.Mp(), false, thenValue))
		elseValue, err := vector.NewConstFixed(types.T_int64.ToType(), int64(22), 2, proc.Mp())
		require.NoError(t, err)
		expr.SetParameter(2, NewFixedVectorExpressionExecutor(proc.Mp(), false, elseValue))

		require.NoError(t, expr.EvalIff(proc, []*batch.Batch{bat}, nil))
		require.Equal(t, []bool{true, false}, expr.selectList1)
		require.Equal(t, []bool{false, true}, expr.selectList2)

		proc.GetSessionInfo().MatrixOneNativeMode = true
		err = expr.EvalIff(proc, []*batch.Batch{bat}, nil)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	})

	t.Run("ShrinkingBatchDoesNotReuseStaleBranchSelection", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		expr := NewFunctionExpressionExecutor()
		defer expr.Free()
		require.NoError(t, expr.Init(proc, 3, types.T_int64.ToType()))

		newConditionBatch := func(values []bool) *batch.Batch {
			bat := batch.NewWithSize(1)
			t.Cleanup(func() { bat.Clean(proc.Mp()) })
			bat.Vecs[0] = vector.NewVec(types.T_bool.ToType())
			require.NoError(t, vector.AppendFixedList(bat.Vecs[0], values, nil, proc.Mp()))
			bat.SetRowCount(len(values))
			return bat
		}
		largeBatch := newConditionBatch([]bool{true, true, true})
		smallBatch := newConditionBatch([]bool{false})

		conditionExecutor, err := NewExpressionExecutor(proc, &plan.Expr{
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}},
			Typ:  plan.Type{Id: int32(types.T_bool), NotNullable: true},
		})
		require.NoError(t, err)
		expr.SetParameter(0, conditionExecutor)
		thenValue, err := vector.NewConstFixed(types.T_int64.ToType(), int64(11), 1, proc.Mp())
		require.NoError(t, err)
		thenExecutor := &failAfterFirstExpressionExecutor{
			delegate: NewFixedVectorExpressionExecutor(proc.Mp(), false, thenValue),
		}
		expr.SetParameter(1, thenExecutor)
		elseValue, err := vector.NewConstFixed(types.T_int64.ToType(), int64(22), 1, proc.Mp())
		require.NoError(t, err)
		expr.SetParameter(2, NewFixedVectorExpressionExecutor(proc.Mp(), false, elseValue))

		require.NoError(t, expr.EvalIff(proc, []*batch.Batch{largeBatch}, nil))
		require.NoError(t, expr.EvalIff(proc, []*batch.Batch{smallBatch}, nil))
		require.Equal(t, 1, thenExecutor.calls)
	})
}

func TestFlowControlFolding(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	t.Run("inactive registered argument", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		invalid := bindTestFunction(t, proc, "abs", makePlan2Int64ConstExprWithType(math.MinInt64))
		executor, err := NewExpressionExecutor(proc, bindTestFunction(t, proc, "if",
			makePlan2BoolConstExprWithType(false), invalid, makePlan2Int64ConstExprWithType(42)))
		require.NoError(t, err)
		defer executor.Free()
		expr := executor.(*FunctionExpressionExecutor)
		branch := expr.parameterExecutor[1].(*FunctionExpressionExecutor)
		realEval := branch.evalFn
		foldCalls := 0
		branch.evalFn = func(params []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selection *function.FunctionSelectList) error {
			foldCalls++
			return realEval(params, result, proc, length, selection)
		}
		bat := batch.New(nil)
		bat.SetRowCount(1)
		vec, err := expr.Eval(proc, []*batch.Batch{bat}, nil)
		require.NoError(t, err)
		require.Zero(t, foldCalls)
		require.True(t, expr.folded.canFold)
		require.Equal(t, int64(42), vector.GetFixedAtNoTypeCheck[int64](vec, 0))

		// Selecting the same registered branch proves the inactive probe is discriminating.
		_, err = branch.Eval(proc, []*batch.Batch{bat}, nil)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange))
		require.Equal(t, 1, foldCalls)
	})

	t.Run("selected metadata", func(t *testing.T) {
		tests := []struct {
			name       string
			fid        int32
			sourceType types.Type
			resultType types.Type
			wantDomain types.RuntimeStringDomain
			wantSource types.StringSource
		}{
			{name: "if text to binary common-domain", fid: function.IFF, sourceType: types.T_varchar.ToType(), resultType: types.T_varbinary.ToType(), wantDomain: types.RuntimeStringText, wantSource: types.StringSourceExpression},
			{name: "case text to binary common-domain", fid: function.CASE, sourceType: types.T_varchar.ToType(), resultType: types.T_varbinary.ToType(), wantDomain: types.RuntimeStringText, wantSource: types.StringSourceExpression},
			{name: "coalesce text to binary selected-value", fid: function.COALESCE, sourceType: types.T_varchar.ToType(), resultType: types.T_varbinary.ToType(), wantDomain: types.RuntimeStringText, wantSource: types.StringSourceSQLPrepare},
			{name: "if binary to text common-domain", fid: function.IFF, sourceType: types.T_varbinary.ToType(), resultType: types.T_varchar.ToType(), wantDomain: types.RuntimeStringBinary, wantSource: types.StringSourceExpression},
			{name: "case binary to text common-domain", fid: function.CASE, sourceType: types.T_varbinary.ToType(), resultType: types.T_varchar.ToType(), wantDomain: types.RuntimeStringBinary, wantSource: types.StringSourceExpression},
			{name: "coalesce binary to text selected-value", fid: function.COALESCE, sourceType: types.T_varbinary.ToType(), resultType: types.T_varchar.ToType(), wantDomain: types.RuntimeStringBinary, wantSource: types.StringSourceSQLPrepare},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				checkExpressionStorageAfterCleanup(t, proc)
				name := "coalesce"
				argTypes := []types.Type{test.sourceType, test.resultType}
				if test.fid != function.COALESCE {
					name = "if"
					if test.fid == function.CASE {
						name = "case"
					}
					argTypes = append([]types.Type{types.T_bool.ToType()}, argTypes...)
				}
				resolved, err := function.GetFunctionByName(proc.Ctx, name, argTypes)
				require.NoError(t, err)
				overload, err := function.GetFunctionById(proc.Ctx, resolved.GetEncodedOverloadID())
				require.NoError(t, err)
				expr := NewFunctionExpressionExecutor()
				defer expr.Free()
				require.NoError(t, expr.Init(proc, len(argTypes), test.resultType))
				expr.fid = test.fid
				expr.overloadID = resolved.GetEncodedOverloadID()
				expr.evalFn, expr.resetFn, expr.freeFn, expr.retainedBytesFn = overload.GetExecuteMethod()
				expr.folded.needFoldingCheck = true
				offset := 0
				if test.fid != function.COALESCE {
					condition, err := vector.NewConstFixed(types.T_bool.ToType(), true, 1, proc.Mp())
					require.NoError(t, err)
					expr.SetParameter(0, NewFixedVectorExpressionExecutor(proc.Mp(), false, condition))
					offset = 1
				}
				selected, err := vector.NewConstBytes(test.sourceType, []byte("selected"), 1, proc.Mp())
				require.NoError(t, err)
				expr.SetParameter(offset, NewFixedVectorExpressionExecutor(proc.Mp(), false, selected))
				selected.SetPrepareParamKind(vector.PrepareParamFloat)
				require.NoError(t, selected.SetStringSource(types.StringSourceSQLPrepare))
				fallback, err := vector.NewConstBytes(test.resultType, []byte("fallback"), 1, proc.Mp())
				require.NoError(t, err)
				expr.SetParameter(offset+1, NewFixedVectorExpressionExecutor(proc.Mp(), false, fallback))

				result, err := expr.Eval(proc, nil, nil)
				require.NoError(t, err)
				require.True(t, expr.folded.canFold)
				require.Equal(t, "selected", result.GetStringAt(0))
				require.Equal(t, test.wantDomain, result.GetRuntimeStringDomainAt(0))
				require.Equal(t, vector.PrepareParamFloat, result.GetPrepareParamKindAt(0))
				require.Equal(t, test.wantSource, result.GetStringSourceAt(0))

				zeroBatch := batch.New(nil)
				zeroBatch.SetRowCount(0)
				result, err = expr.Eval(proc, []*batch.Batch{zeroBatch}, nil)
				require.NoError(t, err)
				require.Zero(t, result.Length())

				nonemptyBatch := batch.New(nil)
				nonemptyBatch.SetRowCount(4)
				result, err = expr.Eval(proc, []*batch.Batch{nonemptyBatch}, nil)
				require.NoError(t, err)
				require.Equal(t, 4, result.Length())
				for row := 0; row < result.Length(); row++ {
					require.Equal(t, "selected", result.GetStringAt(row))
					require.Equal(t, test.wantDomain, result.GetRuntimeStringDomainAt(row))
					require.Equal(t, vector.PrepareParamFloat, result.GetPrepareParamKindAt(row))
					require.Equal(t, test.wantSource, result.GetStringSourceAt(row))
				}
			})
		}
	})
}

func TestFlowControlStringSourcePolicyAcrossSelectionAndReset(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	checkExpressionStorageAfterCleanup(t, proc)
	input := batch.NewWithSize(2)
	defer input.Clean(proc.Mp())
	input.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	input.Vecs[1] = vector.NewVec(types.T_bool.ToType())
	for range 2 {
		require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte("selected"), false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(input.Vecs[1], true, false, proc.Mp()))
	}
	input.SetRowCount(2)

	column := func(pos int32, typ types.Type) *plan.Expr {
		return &plan.Expr{
			Typ:  plan.Type{Id: int32(typ.Oid)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}},
		}
	}
	literal := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_Sval{Sval: "fallback"},
		}},
	}

	valueCol := column(0, types.T_varchar.ToType())
	conditionCol := column(1, types.T_bool.ToType())
	isNull := bindTestFunction(t, proc, "isnull", valueCol)
	for _, test := range []struct {
		name string
		expr *plan.Expr
		want func(types.StringSource) types.StringSource
	}{
		{name: "if common-domain", expr: bindTestFunction(t, proc, "if", conditionCol, valueCol, literal), want: func(types.StringSource) types.StringSource { return types.StringSourceExpression }},
		{name: "case common-domain", expr: bindTestFunction(t, proc, "case", conditionCol, valueCol, literal), want: func(types.StringSource) types.StringSource { return types.StringSourceExpression }},
		{name: "ifnull rewrite common-domain", expr: bindTestFunction(t, proc, "case", isNull, literal, valueCol), want: func(types.StringSource) types.StringSource { return types.StringSourceExpression }},
		{name: "coalesce selected-value", expr: bindTestFunction(t, proc, "coalesce", valueCol, literal), want: func(source types.StringSource) types.StringSource { return source }},
	} {
		t.Run(test.name, func(t *testing.T) {
			executor, err := NewExpressionExecutor(proc, test.expr)
			require.NoError(t, err)
			defer executor.Free()
			for _, source := range []types.StringSource{
				types.StringSourceSQLPrepare, types.StringSourceCOMStmt,
			} {
				require.NoError(t, input.Vecs[0].SetStringSource(source))
				result, err := executor.Eval(proc, []*batch.Batch{input}, []bool{true, false})
				require.NoError(t, err)
				require.Equal(t, test.want(source), result.GetStringSourceAt(0))
				require.Equal(t, types.StringSourceExpression, result.GetStringSourceAt(1))
				executor.ResetForNextQuery()
			}
		})
	}
}

func TestParamExpressionExecutorLifecycle(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	t.Cleanup(func() {
		assert.Nil(t, proc.GetPrepareParams())
		assert.False(t, proc.GetBaseProcessRunningStatus())
	})
	t.Run("protocol metadata", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		tests := []struct {
			name, value string
			source      types.StringSource
			kind        vector.PrepareParamKind
			binary      bool
		}{
			{"binary", "AB\x00\x00", types.StringSourceCOMStmt, vector.PrepareParamNone, true},
			{"integer", "5", types.StringSourceSQLPrepare, vector.PrepareParamInteger, false},
			{"float", "5.5", types.StringSourceUserVariable, vector.PrepareParamFloat, false},
			{"decimal", "5.9", types.StringSourceLiteral, vector.PrepareParamDecimal, false},
			{"boolean", "true", types.StringSourceExpression, vector.PrepareParamBoolean, false},
			{"text", "text", types.StringSourceCOMStmt, vector.PrepareParamNone, false},
		}
		params := vector.NewVec(types.T_text.ToType())
		t.Cleanup(func() { params.Free(proc.Mp()) })
		state := proc.DetachPrepareParams()
		t.Cleanup(func() { proc.RestorePrepareParams(state) })
		sources := make([]types.StringSource, len(tests))
		kinds := make([]vector.PrepareParamKind, len(tests))
		binary := make([]bool, len(tests))
		for row, test := range tests {
			require.NoError(t, vector.AppendBytes(params, []byte(test.value), false, proc.Mp()))
			sources[row] = test.source
			kinds[row] = test.kind
			binary[row] = test.binary
		}
		require.NoError(t, params.SetStringSourcesWithMP(sources, proc.Mp()))
		proc.SetPrepareParamsWithMeta(params, binary, kinds)
		for row, test := range tests {
			func() {
				executor := NewParamExpressionExecutor(proc.Mp(), row, types.T_text.ToType())
				defer executor.Free()
				result, err := executor.Eval(proc, nil, nil)
				require.NoError(t, err, test.name)
				require.Equal(t, test.value, result.GetStringAt(0), test.name)
				require.Equal(t, test.source, result.GetStringSource(), test.name)
				require.Equal(t, test.kind, result.GetPrepareParamKind(), test.name)
				require.Equal(t, test.binary, result.GetIsBin(), test.name)
			}()
		}
	})
	t.Run("batch cardinality", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		params := vector.NewVec(types.T_varchar.ToType())
		t.Cleanup(func() { params.Free(proc.Mp()) })
		state := proc.DetachPrepareParams()
		t.Cleanup(func() { proc.RestorePrepareParams(state) })
		require.NoError(t, vector.AppendBytes(params, []byte("UTC"), false, proc.Mp()))
		proc.SetPrepareParams(params)

		executor := NewParamExpressionExecutor(proc.Mp(), 0, types.T_varchar.ToType())
		t.Cleanup(executor.Free)
		input := batch.New(nil)
		input.SetRowCount(4)

		vec, err := executor.Eval(proc, []*batch.Batch{input}, nil)
		require.NoError(t, err)
		require.True(t, vec.IsConst())
		require.Equal(t, input.RowCount(), vec.Length())
		require.Equal(t, "UTC", vec.GetStringAt(3))

		input.SetRowCount(2)
		vec, err = executor.Eval(proc, []*batch.Batch{input}, nil)
		require.NoError(t, err)
		require.Equal(t, input.RowCount(), vec.Length())
	})
	t.Run("lookup failure recovery", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		params := vector.NewVec(types.T_varchar.ToType())
		t.Cleanup(func() { params.Free(proc.Mp()) })
		state := proc.DetachPrepareParams()
		t.Cleanup(func() { proc.RestorePrepareParams(state) })

		executor := NewParamExpressionExecutor(proc.Mp(), 0, types.T_varchar.ToType())
		t.Cleanup(executor.Free)

		result, err := executor.Eval(proc, nil, nil)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal), "%v", err)
		require.ErrorContains(t, err, "get prepare params error, index 0 not exists")
		require.Nil(t, result)
		require.False(t, executor.folded)

		require.NoError(t, vector.AppendBytes(params, []byte("recovered"), false, proc.Mp()))
		proc.SetPrepareParams(params)

		result, err = executor.Eval(proc, nil, nil)
		require.NoError(t, err)
		require.True(t, executor.folded)
		require.False(t, executor.foldedNull)
		require.Equal(t, "recovered", result.GetStringAt(0))
	})
	t.Run("typed generations", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		expr := &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
		}
		executor, err := NewExpressionExecutor(proc, expr)
		if executor != nil {
			t.Cleanup(executor.Free)
		}
		require.NoError(t, err)
		for _, tc := range []struct {
			value              string
			null, masked, fail bool
			want               int64
		}{
			{value: "2147483648", want: 2147483648},
			{value: "invalid", masked: true},
			{value: "invalid", fail: true},
			{null: true},
			{value: "-2147483649", want: -2147483649},
		} {
			func() {
				executor.ResetForNextQuery()
				params := vector.NewVec(types.T_text.ToType())
				defer params.Free(proc.Mp())
				state := proc.DetachPrepareParams()
				defer proc.RestorePrepareParams(state)
				require.NoError(t, vector.AppendBytes(params, []byte(tc.value), tc.null, proc.Mp()))
				proc.SetPrepareParams(params)
				var selected []bool
				if tc.masked {
					selected = []bool{false}
				}
				result, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, selected)
				if tc.fail {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				require.Equal(t, types.T_int64, result.GetType().Oid)
				if tc.null || tc.masked {
					require.True(t, result.IsNull(0))
				} else {
					require.Equal(t, tc.want, vector.GetFixedAtNoTypeCheck[int64](result, 0))
				}
			}()
		}
	})
	tests := []struct {
		name  string
		null  bool
		value string
	}{
		{name: "non-null", value: "repeated"},
		{name: "null", null: true},
	}
	for _, test := range tests {
		t.Run("transfer "+test.name, func(t *testing.T) {
			checkExpressionStorageAfterCleanup(t, proc)

			params := vector.NewVec(types.T_varchar.ToType())
			t.Cleanup(func() { params.Free(proc.Mp()) })
			state := proc.DetachPrepareParams()
			t.Cleanup(func() { proc.RestorePrepareParams(state) })
			require.NoError(t, vector.AppendBytes(params, []byte(test.value), test.null, proc.Mp()))
			proc.SetPrepareParams(params)

			executor := NewParamExpressionExecutor(proc.Mp(), 0, types.T_varchar.ToType())
			t.Cleanup(executor.Free)

			for i := 0; i < 2; i++ {
				func() {
					result, err := executor.EvalWithoutResultReusing(proc, nil, nil)
					if result != nil {
						defer result.Free(proc.Mp())
					}
					require.NoError(t, err)
					require.NotNil(t, result)
					require.Equal(t, test.null, result.IsNull(0))
					if !test.null {
						require.Equal(t, test.value, result.GetStringAt(0))
					}
				}()
			}
		})
	}
}

func TestPrivateIntegerArgumentScalarSourceClassification(t *testing.T) {
	// Classification reads representation and ancestry, never payload or pool state.
	scalar := &vector.Vector{}
	scalar.ToConst()
	fixed := &FixedVectorExpressionExecutor{resultVector: scalar}
	cast := &FunctionExpressionExecutor{functionInformationForEval: functionInformationForEval{fid: function.CAST}, parameterExecutor: []ExpressionExecutor{fixed}}
	for _, tc := range []struct {
		name   string
		source ExpressionExecutor
		want   bool
	}{
		{"cast_constant", cast, true},
		{"flat", &FixedVectorExpressionExecutor{resultVector: &vector.Vector{}}, false},
		{"plus_constant", &FunctionExpressionExecutor{functionInformationForEval: functionInformationForEval{fid: function.PLUS}, parameterExecutor: []ExpressionExecutor{fixed}}, false},
		{"cast_column", &FunctionExpressionExecutor{functionInformationForEval: functionInformationForEval{fid: function.CAST}, parameterExecutor: []ExpressionExecutor{&ColumnExpressionExecutor{}}}, false},
		{"parameter", &ParamExpressionExecutor{}, true},
		{"variable", &VarExpressionExecutor{}, true},
		{"memo_cast", &memoExpressionExecutor{state: &memoExpressionState{executor: cast}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.want, scalarIntegerArgumentSource(tc.source)) })
	}
	for _, overload := range []int32{0, 1, function.IntegerArgumentCastOverload, function.TruncatedIntegerArgumentCastOverload, 7, 8} {
		cast.overloadID = function.EncodeOverloadID(function.CAST, overload)
		require.Equal(t, overload == function.IntegerArgumentCastOverload || overload == function.TruncatedIntegerArgumentCastOverload, cast.hasScalarIntegerArgumentSource(), "overload %d", overload)
	}
}

func TestPrivateIntegerArgumentScalarLifecycle(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	checkExpressionStorageAfterCleanup(t, proc)
	checkScalar := func(t *testing.T, got *vector.Vector, row int, want int64) {
		t.Helper()
		require.Equal(t, types.Type{Oid: types.T_int64, Size: 8}, *got.GetType())
		require.True(t, got.IsConst())
		require.Equal(t, 3, got.Length())
		require.False(t, got.IsNull(uint64(row)))
		require.Equal(t, want, vector.GetFixedAtNoTypeCheck[int64](got, row))
	}
	t.Run("variable_selection_error_recovery", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		previous := proc.GetResolveVariableFunc()
		t.Cleanup(func() { proc.SetResolveVariableFunc(previous) })
		value := any(float64(2.5))
		calls := 0
		proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) { calls++; return value, nil })
		variable := &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_float64)},
			Expr: &plan.Expr_V{V: &plan.VarRef{Name: "precision"}},
		}
		precision := bindTestCast(t, proc, function.IntegerArgumentCastOverload, variable, types.T_int64.ToType())
		executor, err := NewExpressionExecutor(proc, precision)
		if executor != nil {
			t.Cleanup(executor.Free)
		}
		require.NoError(t, err)
		bat := batch.New(nil)
		t.Cleanup(func() { bat.Clean(proc.Mp()) })
		bat.SetRowCount(3)
		for row, mask := range [][]bool{{true, false, false}, {false, true, false}} {
			got, err := executor.Eval(proc, []*batch.Batch{bat}, mask)
			require.NoError(t, err)
			checkScalar(t, got, row, 2)
		}
		value = math.Inf(1)
		before := calls
		got, err := executor.Eval(proc, []*batch.Batch{bat}, []bool{false, false, false})
		require.NoError(t, err)
		require.Equal(t, before, calls)
		require.Equal(t, types.Type{Oid: types.T_int64, Size: 8}, *got.GetType())
		require.Equal(t, 3, got.Length())
		for row := 0; row < 3; row++ {
			require.True(t, got.IsNull(uint64(row)))
		}
		for _, mask := range [][]bool{{false, true, false}, {true, true, true}} {
			value = math.Inf(1)
			_, err = executor.Eval(proc, []*batch.Batch{bat}, mask)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "%v", err)
			value = float64(2.5)
			got, err = executor.Eval(proc, []*batch.Batch{bat}, mask)
			require.NoError(t, err)
			checkScalar(t, got, 1, 2)
		}
	})
	for _, tc := range []struct {
		name     string
		overload int32
		want     int64
	}{
		{"parameter_nearest", function.IntegerArgumentCastOverload, 4},
		{"parameter_truncated", function.TruncatedIntegerArgumentCastOverload, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checkExpressionStorageAfterCleanup(t, proc)
			expr := bindTestCast(t, proc, tc.overload,
				&plan.Expr{Typ: plan.Type{Id: int32(types.T_float64)}, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}}, types.T_int64.ToType())
			executor, err := NewExpressionExecutor(proc, expr)
			if executor != nil {
				t.Cleanup(executor.Free)
			}
			require.NoError(t, err)
			bat := batch.New(nil)
			t.Cleanup(func() { bat.Clean(proc.Mp()) })
			bat.SetRowCount(3)
			for _, mask := range [][]bool{nil, {true, true, true}, {false, true, false}} {
				got := evalTestTextParameter(t, proc, executor, "3.5", false, vector.PrepareParamFloat, []*batch.Batch{bat}, mask)
				checkScalar(t, got, 1, tc.want)
				require.False(t, executor.(*FunctionExpressionExecutor).folded.canFold)
				if mask == nil || mask[0] {
					require.False(t, executor.(*FunctionExpressionExecutor).parameterResults[0].IsConst())
				}
			}
			for _, mask := range [][]bool{nil, {false, true, false}} {
				executor.ResetForNextQuery()
				got := evalTestTextParameter(t, proc, executor, "", true, vector.PrepareParamFloat, []*batch.Batch{bat}, mask)
				require.Equal(t, types.Type{Oid: types.T_int64, Size: 8}, *got.GetType())
				require.True(t, got.IsConstNull())
				require.Equal(t, 3, got.Length())
				executor.ResetForNextQuery()
				got = evalTestTextParameter(t, proc, executor, "3.5", false, vector.PrepareParamFloat, []*batch.Batch{bat}, mask)
				checkScalar(t, got, 1, tc.want)
			}
			bat.SetRowCount(0)
			for _, mask := range [][]bool{nil, {}} {
				got := evalTestTextParameter(t, proc, executor, "3.5", false, vector.PrepareParamFloat, []*batch.Batch{bat}, mask)
				require.Equal(t, 0, got.Length())
				require.False(t, got.IsConst())
			}
			bat.SetRowCount(3)
			got := evalTestTextParameter(t, proc, executor, "3.5", false, vector.PrepareParamFloat, []*batch.Batch{bat}, nil)
			checkScalar(t, got, 1, tc.want)
		})
	}
	t.Run("column_stays_flat", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		input := vector.NewVec(types.T_float64.ToType())
		bat := testutil.NewBatchWithVectors([]*vector.Vector{input}, nil)
		t.Cleanup(func() { bat.Clean(proc.Mp()) })
		require.NoError(t, vector.AppendFixedList(input, []float64{2.5, 3.5, -1.5}, nil, proc.Mp()))
		column := &plan.Expr{Typ: plan.Type{Id: int32(types.T_float64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
		executor, err := NewExpressionExecutor(proc, bindTestCast(t, proc, function.IntegerArgumentCastOverload, column, types.T_int64.ToType()))
		if executor != nil {
			t.Cleanup(executor.Free)
		}
		require.NoError(t, err)
		for _, rows := range []int{3, 1, 3} {
			bat.SetRowCount(rows)
			got, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
			require.NoError(t, err)
			require.Equal(t, types.Type{Oid: types.T_int64, Size: 8}, *got.GetType())
			require.False(t, got.IsConst())
			require.Equal(t, rows, got.Length())
			for row, want := range []int64{2, 4, -2}[:rows] {
				require.False(t, got.IsNull(uint64(row)))
				require.Equal(t, want, vector.GetFixedAtNoTypeCheck[int64](got, row))
			}
		}
	})
	t.Run("parameter_unsigned", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		parameter := &plan.Expr{Typ: plan.Type{Id: int32(types.T_float64)}, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}}
		executor, err := NewExpressionExecutor(proc, bindTestCast(t, proc, function.IntegerArgumentCastOverload, parameter, types.T_uint64.ToType()))
		if executor != nil {
			t.Cleanup(executor.Free)
		}
		require.NoError(t, err)
		bat := batch.New(nil)
		t.Cleanup(func() { bat.Clean(proc.Mp()) })
		bat.SetRowCount(3)
		got := evalTestTextParameter(t, proc, executor, "3.5", false, vector.PrepareParamFloat, []*batch.Batch{bat}, nil)
		require.Equal(t, types.Type{Oid: types.T_uint64, Size: 8}, *got.GetType())
		require.True(t, got.IsConst())
		require.Equal(t, 3, got.Length())
		for row := 0; row < 3; row++ {
			require.False(t, got.IsNull(uint64(row)))
			require.Equal(t, uint64(4), vector.GetFixedAtNoTypeCheck[uint64](got, row))
		}
	})
	t.Run("floor_consumes_parameter", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		input := vector.NewVec(types.T_varchar.ToType())
		bat := testutil.NewBatchWithVectors([]*vector.Vector{input}, nil)
		t.Cleanup(func() { bat.Clean(proc.Mp()) })
		require.NoError(t, vector.AppendBytesList(input, [][]byte{[]byte("1.23456"), []byte("-2.34561")}, nil, proc.Mp()))
		bat.SetRowCount(2)
		column := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
		parameter := &plan.Expr{Typ: plan.Type{Id: int32(types.T_float64)}, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}}
		precision := bindTestCast(t, proc, function.IntegerArgumentCastOverload, parameter, types.T_int64.ToType())
		executor, err := NewExpressionExecutor(proc, bindTestFunction(t, proc, "floor", bindTestCast(t, proc, 0, column, types.T_float64.ToType()), precision))
		if executor != nil {
			t.Cleanup(executor.Free)
		}
		require.NoError(t, err)
		got := evalTestTextParameter(t, proc, executor, "3.5", false, vector.PrepareParamFloat, []*batch.Batch{bat}, nil)
		require.Equal(t, types.Type{Oid: types.T_float64, Size: 8}, *got.GetType())
		require.False(t, got.IsConst())
		require.Equal(t, 2, got.Length())
		for row, want := range []float64{1.2345, -2.3457} {
			require.False(t, got.IsNull(uint64(row)))
			require.Equal(t, want, vector.GetFixedAtNoTypeCheck[float64](got, row))
		}
	})

}

func TestFlowControlPreservesPreparedParamKindOnPartialSelection(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	checkExpressionStorageAfterCleanup(t, proc)
	params := vector.NewVec(types.T_text.ToType())
	defer params.Free(proc.Mp())
	require.NoError(t, vector.AppendBytes(params, []byte("5.5"), false, proc.Mp()))
	params.SetPrepareParamKind(vector.PrepareParamFloat)
	proc.SetPrepareParamsWithMeta(params, nil, []vector.PrepareParamKind{vector.PrepareParamFloat}, []bool{true})

	column := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_bool), NotNullable: true},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}
	parameter := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_text)},
		Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
	}
	stringConst := func(value string) *plan.Expr {
		return &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_varchar), NotNullable: true},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				Value: &plan.Literal_Sval{Sval: value},
			}},
		}
	}

	input := testutil.NewBatchWithVectors([]*vector.Vector{
		testutil.NewVector(2, types.T_bool.ToType(), proc.Mp(), false, []bool{true, true}),
	}, nil)
	defer input.Clean(proc.Mp())
	for name, expression := range map[string]*plan.Expr{
		"if":       bindTestFunction(t, proc, "if", column, parameter, stringConst("fallback")),
		"case":     bindTestFunction(t, proc, "case", column, parameter),
		"coalesce": bindTestFunction(t, proc, "coalesce", parameter, stringConst("fallback")),
	} {
		t.Run(name, func(t *testing.T) {
			executor, err := NewExpressionExecutor(proc, expression)
			require.NoError(t, err)
			defer executor.Free()

			result, err := executor.Eval(proc, []*batch.Batch{input}, []bool{true, false})
			require.NoError(t, err)
			require.Equal(t, vector.PrepareParamFloat, result.GetPrepareParamKind())
			require.Equal(t, "5.5", result.GetStringAt(0))
			require.True(t, result.GetBinaryStringMetadataAt(0))
			require.True(t, result.IsNull(1))

			result, err = executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, vector.PrepareParamFloat, result.GetPrepareParamKind(),
				"full selection must retain the active prepared branch")
			require.True(t, result.GetBinaryStringMetadataAt(0))
			require.True(t, result.GetBinaryStringMetadataAt(1))

			result, err = executor.Eval(proc, []*batch.Batch{input}, []bool{false, true})
			require.NoError(t, err)
			require.Equal(t, vector.PrepareParamFloat, result.GetPrepareParamKind(),
				"reused selection buffers must retain only the active branch lineage")
			require.Equal(t, "5.5", result.GetStringAt(1))
			require.True(t, result.GetBinaryStringMetadataAt(1))
			require.True(t, result.IsNull(0))
		})
	}

	mixedInput := testutil.NewBatchWithVectors([]*vector.Vector{
		testutil.NewVector(2, types.T_bool.ToType(), proc.Mp(), false, []bool{true, false}),
	}, nil)
	defer mixedInput.Clean(proc.Mp())
	for name, expression := range map[string]*plan.Expr{
		"if-mixed":   bindTestFunction(t, proc, "if", column, parameter, stringConst("fallback")),
		"case-mixed": bindTestFunction(t, proc, "case", column, parameter, stringConst("fallback")),
	} {
		t.Run(name, func(t *testing.T) {
			executor, err := NewExpressionExecutor(proc, expression)
			require.NoError(t, err)
			defer executor.Free()
			result, err := executor.Eval(proc, []*batch.Batch{mixedInput}, nil)
			require.NoError(t, err)
			require.Equal(t, vector.PrepareParamNone, result.GetPrepareParamKind(),
				"active branches with mixed source categories must be conservative")
			require.True(t, result.GetBinaryStringMetadataAt(0))
			require.False(t, result.GetBinaryStringMetadataAt(1))
		})
	}

	t.Run("mixed materialization keeps row lineage for a later bit cast", func(t *testing.T) {
		flowExpr := bindTestFunction(t, proc, "if", column, parameter, stringConst("5"))
		flowExecutor, err := NewExpressionExecutor(proc, flowExpr)
		require.NoError(t, err)
		defer flowExecutor.Free()
		flowResult, err := flowExecutor.Eval(proc, []*batch.Batch{mixedInput}, nil)
		require.NoError(t, err)
		require.Equal(t, vector.PrepareParamFloat, flowResult.GetPrepareParamKindAt(0))
		require.Equal(t, vector.PrepareParamNone, flowResult.GetPrepareParamKindAt(1))

		materialized := batch.NewWithSize(1)
		materialized.Vecs[0] = flowResult
		materialized.SetRowCount(flowResult.Length())
		textColumn := &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_text)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
		}
		target := &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_bit), Width: 64, NotNullable: true},
			Expr: &plan.Expr_T{T: &plan.TargetType{}},
		}
		castExpr := bindTestFunction(t, proc, "cast", textColumn, target)
		castExecutor, err := NewExpressionExecutor(proc, castExpr)
		require.NoError(t, err)
		defer castExecutor.Free()
		castResult, err := castExecutor.Eval(proc, []*batch.Batch{materialized}, nil)
		require.NoError(t, err)
		require.Equal(t, []uint64{6, 53}, vector.MustFixedColWithTypeCheck[uint64](castResult))
		materialized.Vecs[0] = nil
	})

	maskedBranchColumn := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}},
	}
	maskedBranchInput := testutil.NewBatchWithVectors([]*vector.Vector{
		testutil.NewVector(2, types.T_bool.ToType(), proc.Mp(), false, []bool{true, false}),
		testutil.MakeVarcharVector([]string{"", "outside-selection"}, []uint64{0}, proc.Mp()),
	}, nil)
	defer maskedBranchInput.Clean(proc.Mp())
	maskedBranchExecutor, err := NewExpressionExecutor(
		proc,
		bindTestFunction(t, proc, "if", column, maskedBranchColumn, parameter),
	)
	require.NoError(t, err)
	defer maskedBranchExecutor.Free()
	maskedResult, err := maskedBranchExecutor.Eval(proc, []*batch.Batch{maskedBranchInput}, nil)
	require.NoError(t, err)
	require.Equal(t, vector.PrepareParamFloat, maskedResult.GetPrepareParamKind(),
		"non-NULL values outside an IF arm's selected rows must be ignored")
	require.True(t, maskedResult.IsNull(0))
	require.Equal(t, "5.5", maskedResult.GetStringAt(1))
	maskedResult, err = maskedBranchExecutor.Eval(
		proc, []*batch.Batch{maskedBranchInput}, []bool{true, false})
	require.NoError(t, err)
	require.Equal(t, vector.PrepareParamNone, maskedResult.GetPrepareParamKind())

	coalesceColumn := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}},
	}
	coalesceInput := testutil.NewBatchWithVectors([]*vector.Vector{
		testutil.NewVector(2, types.T_bool.ToType(), proc.Mp(), false, []bool{true, true}),
		testutil.MakeVarcharVector([]string{"", "ordinary"}, []uint64{0}, proc.Mp()),
	}, nil)
	defer coalesceInput.Clean(proc.Mp())
	executor, err := NewExpressionExecutor(
		proc,
		bindTestFunction(t, proc, "coalesce", coalesceColumn, parameter),
	)
	require.NoError(t, err)
	defer executor.Free()
	result, err := executor.Eval(proc, []*batch.Batch{coalesceInput}, nil)
	require.NoError(t, err)
	require.Equal(t, vector.PrepareParamNone, result.GetPrepareParamKind(),
		"coalesce must fold all active source categories")
	require.Equal(t, []string{"5.5", "ordinary"}, []string{result.GetStringAt(0), result.GetStringAt(1)})
	require.Equal(t, vector.PrepareParamFloat, result.GetPrepareParamKindAt(0))
	require.Equal(t, vector.PrepareParamNone, result.GetPrepareParamKindAt(1))
}

func TestVariableExpressionLifecycle(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	checkExpressionStorageAfterCleanup(t, proc)
	checkResolvers := func(t *testing.T) {
		t.Helper()
		assert.Nil(t, proc.GetResolveVariableFunc())
		assert.Nil(t, proc.GetResolveVariableIsBinFunc())
		assert.Nil(t, proc.GetResolveVariableStringDomainFunc())
		assert.Nil(t, proc.GetResolveVariablePrepareParamKindFunc())
	}
	t.Cleanup(func() { checkResolvers(t) })
	runLeaf := func(name string, scenario func(t *testing.T)) {
		t.Run(name, func(t *testing.T) {
			checkExpressionStorageAfterCleanup(t, proc)
			valueBefore, binBefore := proc.GetResolveVariableFunc(), proc.GetResolveVariableIsBinFunc()
			domainBefore, kindBefore := proc.GetResolveVariableStringDomainFunc(), proc.GetResolveVariablePrepareParamKindFunc()
			t.Cleanup(func() {
				proc.SetResolveVariableFunc(valueBefore)
				proc.SetResolveVariableIsBinFunc(binBefore)
				proc.SetResolveVariableStringDomainFunc(domainBefore)
				proc.SetResolveVariablePrepareParamKindFunc(kindBefore)
			})
			checkResolvers(t)
			scenario(t)
		})
	}

	runLeaf("integer reuse", func(t *testing.T) {

		// Create a variable expression
		varExpr := &plan.Expr{
			Expr: &plan.Expr_V{
				V: &plan.VarRef{
					Name:   "test_var",
					System: false,
					Global: false,
				},
			},
			Typ: plan.Type{
				Id:          int32(types.T_int64),
				NotNullable: true,
			},
		}

		// Mock the variable resolution function
		proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
			if name == "test_var" {
				return int64(12345), nil
			}
			return nil, moerr.NewInternalErrorNoCtx("variable not found")
		})

		varExprExecutor, err := NewExpressionExecutor(proc, varExpr)
		if varExprExecutor != nil {
			t.Cleanup(func() {
				if varExprExecutor != nil {
					varExprExecutor.Free()
				}
			})
		}
		require.NoError(t, err)
		tree, err := DebugShowExecutor(varExprExecutor)
		require.NoError(t, err)
		t.Log(tree)

		input := &batch.Batch{}
		input.SetRowCount(4)
		vec, err := varExprExecutor.Eval(proc, []*batch.Batch{input}, nil)
		require.NoError(t, err)
		require.Equal(t, input.RowCount(), vec.Length())
		require.Equal(t, types.T_int64.ToType(), *vec.GetType())
		require.Equal(t, int64(12345), vector.MustFixedColNoTypeCheck[int64](vec)[0])
		require.False(t, vec.GetNulls().Contains(0))

		// A reused executor must reparse the new value into the fixed-width vector,
		// rather than treating its backing memory as a varlena descriptor.
		proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
			return int64(67890), nil
		})
		input.SetRowCount(2)
		vec, err = varExprExecutor.Eval(proc, []*batch.Batch{input}, nil)
		require.NoError(t, err)
		require.Equal(t, input.RowCount(), vec.Length())
		require.Equal(t, int64(67890), vector.MustFixedColNoTypeCheck[int64](vec)[0])

		owned, err := varExprExecutor.EvalWithoutResultReusing(proc, []*batch.Batch{input}, nil)
		if owned != nil {
			defer owned.Free(proc.Mp())
		}
		require.NoError(t, err)
		varExprExecutor.Free()
		varExprExecutor = nil
		require.Equal(t, 2, owned.Length())
		require.Equal(t, int64(67890), vector.GetFixedAtNoTypeCheck[int64](owned, 0))
		require.False(t, owned.IsNull(0))

	})

	for _, tc := range []struct {
		name   string
		typ    types.Type
		domain types.RuntimeStringDomain
		binary bool
	}{
		{"text", types.T_varchar.ToType(), types.RuntimeStringInherit, false},
		{"binary", types.T_blob.ToType(), types.RuntimeStringInherit, true},
		{"binary charset on varchar", types.NewWithCharset(types.T_varchar, 0, 0, types.CharsetBinary), types.RuntimeStringInherit, true},
		{"text charset on varbinary", types.NewWithCharset(types.T_varbinary, 0, 0, types.CharsetUTF8), types.RuntimeStringInherit, false},
		{"bound text on binary charset", types.NewWithCharset(types.T_varchar, 0, 0, types.CharsetBinary), types.RuntimeStringText, false},
		{"bound binary on text", types.T_varchar.ToType(), types.RuntimeStringBinary, true},
	} {
		runLeaf("domain "+tc.name, func(t *testing.T) {
			var value any = "你"
			var resolveErr error
			runtimeDomain := types.RuntimeStringBinary
			proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
				return value, resolveErr
			})
			domainCalls := 0
			proc.SetResolveVariableStringDomainFunc(func(string, bool, bool) (types.RuntimeStringDomain, error) {
				domainCalls++
				return runtimeDomain, moerr.NewInternalErrorNoCtx("a bound variable must not resolve the current domain")
			})
			executor, err := NewExpressionExecutor(proc, &plan.Expr{
				Expr: &plan.Expr_V{V: &plan.VarRef{Name: "domain_var", BoundStringDomain: uint32(tc.domain) + 1}},
				Typ:  plan.Type{Id: int32(tc.typ.Oid), Width: tc.typ.Width, Charset: uint32(tc.typ.Charset)},
			})
			if executor != nil {
				t.Cleanup(func() {
					if executor != nil {
						executor.Free()
					}
				})
			}
			require.NoError(t, err)

			input := batch.New(nil)
			input.SetRowCount(2)
			for _, step := range []struct {
				value  any
				domain types.RuntimeStringDomain
			}{
				{"你", types.RuntimeStringBinary},
				{"text", types.RuntimeStringText},
				{nil, types.RuntimeStringText},
				{nil, types.RuntimeStringBinary},
				{[]byte("你"), types.RuntimeStringBinary},
				{"你", types.RuntimeStringInherit},
			} {
				value, runtimeDomain = step.value, step.domain
				vec, err := executor.Eval(proc, []*batch.Batch{input}, nil)
				require.NoError(t, err)
				require.Equal(t, 2, vec.Length())
				require.Equal(t, tc.typ, *vec.GetType())
				for row := 0; row < 2; row++ {
					if value != nil {
						require.Equal(t, tc.binary, vec.GetIsBinaryStringAt(row))
					}
					wantDomain := types.RuntimeStringInherit
					if value != nil {
						wantDomain = tc.domain
						if tc.typ.Oid == types.T_varbinary && !tc.binary {
							wantDomain = types.RuntimeStringText
						}
					}
					require.Equal(t, wantDomain, vec.GetRuntimeStringDomainAt(row))
					require.Equal(t, types.StringSourceUserVariable, vec.GetStringSourceAt(row))
				}
				require.Equal(t, value == nil, vec.IsConstNull())
				if value != nil {
					want := "你"
					if value == "text" {
						want = "text"
					}
					require.Equal(t, want, vec.GetStringAt(1))
				}
			}

			executor.ResetForNextQuery()
			resolveErr = moerr.NewInternalErrorNoCtx("variable resolver failed")
			masked, err := executor.Eval(proc, []*batch.Batch{input}, []bool{false, false})
			require.NoError(t, err)
			require.True(t, masked.IsConstNull())
			_, err = executor.Eval(proc, []*batch.Batch{input}, nil)
			require.ErrorIs(t, err, resolveErr)
			resolveErr = nil
			vec, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, "你", vec.GetStringAt(0))
			require.Equal(t, tc.binary, vec.GetIsBinaryStringAt(0))
			require.Zero(t, domainCalls)
			if tc.name == "text" {
				value = nil
				owned, err := executor.EvalWithoutResultReusing(proc, []*batch.Batch{input}, nil)
				if owned != nil {
					defer owned.Free(proc.Mp())
				}
				require.NoError(t, err)
				executor.Free()
				executor = nil
				require.Equal(t, 2, owned.Length())
				require.True(t, owned.IsConstNull())
			}

		})
	}

	runLeaf("json reuse", func(t *testing.T) {
		var value any = `{"a":1}`
		proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
			return value, nil
		})

		jsonExpr := &plan.Expr{
			Expr: &plan.Expr_V{V: &plan.VarRef{Name: "json_var"}},
			Typ:  plan.Type{Id: int32(types.T_json)},
		}
		jsonExecutor, err := NewExpressionExecutor(proc, jsonExpr)
		if jsonExecutor != nil {
			t.Cleanup(jsonExecutor.Free)
		}
		require.NoError(t, err)

		vec, err := jsonExecutor.Eval(proc, nil, nil)
		require.NoError(t, err)
		require.Equal(t, `{"a": 1}`, types.DecodeJson(vec.GetBytesAt(0)).String())

		value = `{"a":2}`
		vec, err = jsonExecutor.Eval(proc, nil, nil)
		require.NoError(t, err)
		require.Equal(t, `{"a": 2}`, types.DecodeJson(vec.GetBytesAt(0)).String())

		value = `{"a":}`
		vec, err = jsonExecutor.Eval(proc, nil, nil)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "%v", err)
		require.Nil(t, vec)
		require.Zero(t, proc.Mp().CurrNB())
		bytes, objects := proc.Mp().OnHeapOutstanding()
		require.Zero(t, bytes)
		require.Zero(t, objects)
		value = `{"a":3}`
		vec, err = jsonExecutor.Eval(proc, nil, nil)
		require.NoError(t, err)
		require.Equal(t, `{"a": 3}`, types.DecodeJson(vec.GetBytesAt(0)).String())

	})
	runLeaf("uuid value", func(t *testing.T) {
		uuid := types.Uuid{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
		value := "ffffffff-ffff-ffff-ffff-ffffffffffff"
		proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) { return value, nil })
		uuidExpr := &plan.Expr{
			Expr: &plan.Expr_V{V: &plan.VarRef{Name: "uuid_var"}},
			Typ:  plan.Type{Id: int32(types.T_uuid)},
		}
		uuidExecutor, err := NewExpressionExecutor(proc, uuidExpr)
		if uuidExecutor != nil {
			t.Cleanup(uuidExecutor.Free)
		}
		require.NoError(t, err)

		vec, err := uuidExecutor.Eval(proc, nil, nil)
		require.NoError(t, err)
		require.Equal(t, uuid, vector.MustFixedColNoTypeCheck[types.Uuid](vec)[0])

		value = "ffffffff-ffff-ffff-ffff-fffffffffff"
		vec, err = uuidExecutor.Eval(proc, nil, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid UUID length")
		require.Nil(t, vec)
		require.Zero(t, proc.Mp().CurrNB())
		bytes, objects := proc.Mp().OnHeapOutstanding()
		require.Zero(t, bytes)
		require.Zero(t, objects)
		value = "ffffffff-ffff-ffff-ffff-ffffffffffff"
		vec, err = uuidExecutor.Eval(proc, nil, nil)
		require.NoError(t, err)
		require.Equal(t, uuid, vector.GetFixedAtNoTypeCheck[types.Uuid](vec, 0))

	})

	testCases := []struct {
		name   string
		typ    types.Type
		first  any
		second any
		check  func(t *testing.T, vec *vector.Vector, second bool)
	}{
		{
			name:   "vecf32",
			typ:    types.New(types.T_array_float32, 3, 0),
			first:  []float32{1, 2, 3},
			second: []float32{4, 5, 6},
			check: func(t *testing.T, vec *vector.Vector, second bool) {
				want := []float32{1, 2, 3}
				if second {
					want = []float32{4, 5, 6}
				}
				require.Equal(t, want, vector.GetArrayAt[float32](vec, 0))
			},
		},
		{
			name:   "vecf64",
			typ:    types.New(types.T_array_float64, 3, 0),
			first:  []float64{1, 2, 3},
			second: []float64{4, 5, 6},
			check: func(t *testing.T, vec *vector.Vector, second bool) {
				want := []float64{1, 2, 3}
				if second {
					want = []float64{4, 5, 6}
				}
				require.Equal(t, want, vector.GetArrayAt[float64](vec, 0))
			},
		},
		{
			name:   "vecbf16",
			typ:    types.New(types.T_array_bf16, 3, 0),
			first:  types.Float32ToBF16Slice([]float32{1, 2, 3}),
			second: types.Float32ToBF16Slice([]float32{4, 5, 6}),
			check: func(t *testing.T, vec *vector.Vector, second bool) {
				want := []float32{1, 2, 3}
				if second {
					want = []float32{4, 5, 6}
				}
				require.Equal(t, want, types.BF16ToFloat32Slice(vector.GetArrayAt[types.BF16](vec, 0)))
			},
		},
		{
			name:   "vecf16",
			typ:    types.New(types.T_array_float16, 3, 0),
			first:  types.Float32ToFloat16Slice([]float32{1, 2, 3}),
			second: types.Float32ToFloat16Slice([]float32{4, 5, 6}),
			check: func(t *testing.T, vec *vector.Vector, second bool) {
				want := []float32{1, 2, 3}
				if second {
					want = []float32{4, 5, 6}
				}
				require.Equal(t, want, types.Float16ToFloat32Slice(vector.GetArrayAt[types.Float16](vec, 0)))
			},
		},
		{
			name:   "vecint8",
			typ:    types.New(types.T_array_int8, 3, 0),
			first:  []int8{1, 2, 3},
			second: []int8{4, 5, 6},
			check: func(t *testing.T, vec *vector.Vector, second bool) {
				want := []int8{1, 2, 3}
				if second {
					want = []int8{4, 5, 6}
				}
				require.Equal(t, want, vector.GetArrayAt[int8](vec, 0))
			},
		},
		{
			name:   "vecuint8",
			typ:    types.New(types.T_array_uint8, 3, 0),
			first:  []uint8{1, 128, 255},
			second: []uint8{4, 5, 6},
			check: func(t *testing.T, vec *vector.Vector, second bool) {
				want := []uint8{1, 128, 255}
				if second {
					want = []uint8{4, 5, 6}
				}
				require.Equal(t, want, vector.GetArrayAt[uint8](vec, 0))
			},
		},
	}

	for _, testCase := range testCases {
		runLeaf("array "+testCase.name, func(t *testing.T) {
			value := testCase.first
			proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
				return value, nil
			})
			expr := &plan.Expr{
				Expr: &plan.Expr_V{V: &plan.VarRef{Name: testCase.name}},
				Typ: plan.Type{
					Id:    int32(testCase.typ.Oid),
					Width: testCase.typ.Width,
					Scale: testCase.typ.Scale,
				},
			}
			executor, err := NewExpressionExecutor(proc, expr)
			if executor != nil {
				t.Cleanup(executor.Free)
			}
			require.NoError(t, err)

			vec, err := executor.Eval(proc, nil, nil)
			require.NoError(t, err)
			testCase.check(t, vec, false)

			value = testCase.second
			vec, err = executor.Eval(proc, nil, nil)
			require.NoError(t, err)
			testCase.check(t, vec, true)
			if testCase.name == "vecf32" {
				value = []float32{4, 5}
				vec, err = executor.Eval(proc, nil, nil)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "%v", err)
				require.Nil(t, vec)
				require.Zero(t, proc.Mp().CurrNB())
				bytes, objects := proc.Mp().OnHeapOutstanding()
				require.Zero(t, bytes)
				require.Zero(t, objects)
				value = testCase.second
				vec, err = executor.Eval(proc, nil, nil)
				require.NoError(t, err)
				testCase.check(t, vec, true)
			}

		})
	}

	runLeaf("protocol metadata reuse", func(t *testing.T) {
		value := "AB\x00\x00"
		isBin := true
		prepareParamKind := vector.PrepareParamNone
		proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
			return value, nil
		})
		proc.SetResolveVariableIsBinFunc(func(string, bool, bool) (bool, error) {
			return isBin, nil
		})
		proc.SetResolveVariablePrepareParamKindFunc(func(string, bool, bool) (vector.PrepareParamKind, error) {
			return prepareParamKind, nil
		})
		expr := &plan.Expr{
			Expr: &plan.Expr_V{V: &plan.VarRef{Name: "copied_var"}},
			Typ:  plan.Type{Id: int32(types.T_text)},
		}
		executor, err := NewExpressionExecutor(proc, expr)
		if executor != nil {
			t.Cleanup(executor.Free)
		}
		require.NoError(t, err)

		vec, err := executor.Eval(proc, nil, nil)
		require.NoError(t, err)
		require.True(t, vec.GetIsBin())
		require.Equal(t, vector.PrepareParamNone, vec.GetPrepareParamKind())
		require.Equal(t, "AB\x00\x00", vec.GetStringAt(0))

		value, isBin, prepareParamKind = "5.0", false, vector.PrepareParamFloat
		vec, err = executor.Eval(proc, nil, nil)
		require.NoError(t, err)
		require.False(t, vec.GetIsBin())
		require.Equal(t, vector.PrepareParamFloat, vec.GetPrepareParamKind())
		require.Equal(t, "5.0", vec.GetStringAt(0))

		value, prepareParamKind = "text", vector.PrepareParamNone
		vec, err = executor.Eval(proc, nil, nil)
		require.NoError(t, err)
		require.False(t, vec.GetIsBin())
		require.Equal(t, vector.PrepareParamNone, vec.GetPrepareParamKind())
		require.Equal(t, "text", vec.GetStringAt(0))

		value, isBin = "CD\x00\x00", true
		vec, err = executor.Eval(proc, nil, nil)
		require.NoError(t, err)
		require.True(t, vec.GetIsBin())
		require.Equal(t, vector.PrepareParamNone, vec.GetPrepareParamKind())
		require.Equal(t, "CD\x00\x00", vec.GetStringAt(0))

		// Resolver errors must stop before later metadata callbacks or publication.
		resolverErr := moerr.NewInternalErrorNoCtx("variable metadata resolver failed")
		calls := [4]int{}
		failStage := -1
		failure := func(stage int) error {
			calls[stage]++
			if failStage == stage || failStage == -2 {
				return resolverErr
			}
			return nil
		}
		proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) { return "7", failure(0) })
		proc.SetResolveVariableIsBinFunc(func(string, bool, bool) (bool, error) { return true, failure(1) })
		proc.SetResolveVariableStringDomainFunc(func(string, bool, bool) (types.RuntimeStringDomain, error) {
			return types.RuntimeStringText, failure(2)
		})
		proc.SetResolveVariablePrepareParamKindFunc(func(string, bool, bool) (vector.PrepareParamKind, error) {
			return vector.PrepareParamInteger, failure(3)
		})
		expectedText := types.Type{Oid: types.T_text, Charset: types.CharsetLegacy, Size: 24}
		checkRecovered := func(result *vector.Vector, rows int) {
			t.Helper()
			require.Equal(t, expectedText, *result.GetType())
			require.Equal(t, rows, result.Length())
			require.True(t, result.IsConst())
			for row := 0; row < rows; row++ {
				require.False(t, result.IsNull(uint64(row)))
				require.Equal(t, "7", result.GetStringAt(row))
				require.Equal(t, types.StringSourceUserVariable, result.GetStringSourceAt(row))
				require.Equal(t, types.RuntimeStringText, result.GetRuntimeStringDomainAt(row))
			}
			require.True(t, result.GetIsBin())
			require.Equal(t, vector.PrepareParamInteger, result.GetPrepareParamKind())
		}
		for stage, expectedCalls := range [][4]int{{1, 0, 0, 0}, {1, 1, 0, 0}, {1, 1, 1, 0}, {1, 1, 1, 1}} {
			failStage, calls = stage, [4]int{}
			result, err := executor.Eval(proc, nil, nil)
			require.ErrorIs(t, err, resolverErr)
			require.Nil(t, result)
			require.Equal(t, expectedCalls, calls)
			failStage, calls = -1, [4]int{}
			func() {
				owned, err := executor.EvalWithoutResultReusing(proc, nil, nil)
				if owned != nil {
					defer owned.Free(proc.Mp())
				}
				require.NoError(t, err)
				require.Equal(t, [4]int{1, 1, 1, 1}, calls)
				checkRecovered(owned, 1)
			}()
			require.Zero(t, proc.Mp().CurrNB())
			bytes, objects := proc.Mp().OnHeapOutstanding()
			require.Zero(t, bytes)
			require.Zero(t, objects)
		}
		failStage, calls = -2, [4]int{}
		input := batch.New(nil)
		input.SetRowCount(3)
		masked, err := executor.Eval(proc, []*batch.Batch{input}, []bool{false, false, false})
		require.NoError(t, err)
		require.Equal(t, [4]int{}, calls)
		require.Equal(t, expectedText, *masked.GetType())
		require.Equal(t, 3, masked.Length())
		require.True(t, masked.IsConstNull())
		failStage = -1
		result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
		require.NoError(t, err)
		require.Equal(t, [4]int{1, 1, 1, 1}, calls)
		checkRecovered(result, 3)

	})
	runLeaf("missing system resolver", func(t *testing.T) {
		varExpr := &plan.Expr{
			Expr: &plan.Expr_V{
				V: &plan.VarRef{
					Name:   "test_var",
					System: true,
				},
			},
			Typ: plan.Type{
				Id: int32(types.T_text),
			},
		}

		varExprExecutor, err := NewExpressionExecutor(proc, varExpr)
		if varExprExecutor != nil {
			t.Cleanup(varExprExecutor.Free)
		}
		require.NoError(t, err)
		_, err = varExprExecutor.Eval(proc, nil, nil)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal), "%v", err)
		require.Contains(t, err.Error(), "resolve variable function is not set")

	})
}

func TestColumnExpressionLifecycle(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))

	t.Run("ordinary borrowed", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		bat := batch.NewWithSize(4)
		t.Cleanup(func() { bat.Clean(proc.Mp()) })
		bat.Vecs[0] = testutil.MakeInt8Vector([]int8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil, proc.Mp())
		bat.Vecs[1] = testutil.MakeInt16Vector([]int16{10, 9, 8, 7, 6, 5, 4, 3, 2, 1}, nil, proc.Mp())
		bat.Vecs[2] = testutil.MakeInt32Vector([]int32{17, -3, 0, 42, 8, 99, -11, 5, 2, 31}, nil, proc.Mp())
		bat.Vecs[3] = testutil.MakeInt64Vector([]int64{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, nil, proc.Mp())
		bat.SetRowCount(10)
		executor, err := NewExpressionExecutor(proc, &plan.Expr{
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 2}},
			Typ:  plan.Type{Id: int32(types.T_int32), NotNullable: true},
		})
		if executor != nil {
			t.Cleanup(func() {
				if executor != nil {
					executor.Free()
				}
			})
		}
		require.NoError(t, err)
		_, err = DebugShowExecutor(executor)
		require.NoError(t, err)
		result, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
		require.NoError(t, err)
		require.Same(t, bat.Vecs[2], result)
		require.Equal(t, types.T_int32.ToType(), *result.GetType())
		require.Equal(t, 10, result.Length())
		require.Equal(t, []int32{17, -3, 0, 42, 8, 99, -11, 5, 2, 31}, vector.MustFixedColNoTypeCheck[int32](result))
		_, err = DebugShowExecutor(executor)
		require.NoError(t, err)
		borrowed, err := executor.EvalWithoutResultReusing(proc, []*batch.Batch{bat}, nil)
		require.NoError(t, err)
		require.Same(t, result, borrowed)
		native := proc.Mp().CurrNB()
		heap, objects := proc.Mp().OnHeapOutstanding()
		executor.Free()
		executor = nil
		require.Equal(t, native, proc.Mp().CurrNB())
		afterHeap, afterObjects := proc.Mp().OnHeapOutstanding()
		require.Equal(t, heap, afterHeap)
		require.Equal(t, objects, afterObjects)
		require.Equal(t, types.T_int32.ToType(), *borrowed.GetType())
		require.Equal(t, 10, borrowed.Length())
		require.Equal(t, []int32{17, -3, 0, 42, 8, 99, -11, 5, 2, 31}, vector.MustFixedColNoTypeCheck[int32](borrowed))
	})

	t.Run("NULL cache reuse and transfer", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		input := batch.NewWithSize(1)
		t.Cleanup(func() { input.Clean(proc.Mp()) })
		input.Vecs[0] = vector.NewConstNull(types.T_int32.ToType(), 3, proc.Mp())
		input.SetRowCount(3)
		executor, err := NewExpressionExecutor(proc, &plan.Expr{
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}},
			Typ:  plan.Type{Id: int32(types.T_int32)},
		})
		if executor != nil {
			t.Cleanup(func() {
				if executor != nil {
					executor.Free()
				}
			})
		}
		require.NoError(t, err)
		checkNull := func(vec *vector.Vector, typ types.Type, length int, source types.StringSource) {
			t.Helper()
			require.Equal(t, typ, *vec.GetType())
			require.Equal(t, length, vec.Length())
			require.True(t, vec.IsConstNull())
			require.False(t, vec.IsGrouping())
			for row := 0; row < length; row++ {
				require.True(t, vec.IsNull(uint64(row)))
				require.Equal(t, source, vec.GetStringSourceAt(row))
			}
		}
		require.NoError(t, input.Vecs[0].SetStringSource(types.StringSourceSQLPrepare))
		first, err := executor.Eval(proc, []*batch.Batch{input}, nil)
		require.NoError(t, err)
		require.NotSame(t, input.Vecs[0], first)
		checkNull(first, types.T_int32.ToType(), 3, types.StringSourceSQLPrepare)
		checkNull(input.Vecs[0], types.T_int32.ToType(), 3, types.StringSourceSQLPrepare)
		input.Vecs[0].SetType(types.T_int64.ToType())
		input.Vecs[0].SetLength(1)
		input.SetRowCount(1)
		require.NoError(t, input.Vecs[0].SetStringSource(types.StringSourceCOMStmt))
		second, err := executor.Eval(proc, []*batch.Batch{input}, nil)
		require.NoError(t, err)
		require.Same(t, first, second)
		checkNull(second, types.T_int32.ToType(), 1, types.StringSourceCOMStmt)
		checkNull(input.Vecs[0], types.T_int64.ToType(), 1, types.StringSourceCOMStmt)
		owned, err := executor.EvalWithoutResultReusing(proc, []*batch.Batch{input}, nil)
		if owned != nil {
			t.Cleanup(func() { owned.Free(proc.Mp()) })
		}
		require.NoError(t, err)
		require.Same(t, second, owned)
		checkNull(owned, types.T_int32.ToType(), 1, types.StringSourceCOMStmt)
		input.Vecs[0].SetLength(2)
		input.SetRowCount(2)
		require.NoError(t, input.Vecs[0].SetStringSource(types.StringSourceExpression))
		replacement, err := executor.Eval(proc, []*batch.Batch{input}, nil)
		require.NoError(t, err)
		require.NotSame(t, owned, replacement)
		checkNull(replacement, types.T_int32.ToType(), 2, types.StringSourceExpression)
		checkNull(input.Vecs[0], types.T_int64.ToType(), 2, types.StringSourceExpression)
		checkNull(owned, types.T_int32.ToType(), 1, types.StringSourceCOMStmt)
		executor.Free()
		executor = nil
		checkNull(owned, types.T_int32.ToType(), 1, types.StringSourceCOMStmt)
	})

	t.Run("grouping borrowed", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		bat := batch.NewWithSize(1)
		t.Cleanup(func() { bat.Clean(proc.Mp()) })
		bat.Vecs[0] = vector.NewRollupConst(types.T_varchar.ToType(), 3, proc.Mp())
		bat.SetRowCount(3)
		executor, err := NewExpressionExecutor(proc, &plan.Expr{
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}},
			Typ:  plan.Type{Id: int32(types.T_varchar)},
		})
		if executor != nil {
			t.Cleanup(executor.Free)
		}
		require.NoError(t, err)
		result, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
		require.NoError(t, err)
		require.Same(t, bat.Vecs[0], result)
		require.Equal(t, types.T_varchar.ToType(), *result.GetType())
		require.Equal(t, 3, result.Length())
		require.True(t, result.IsConstNull())
		require.True(t, result.IsGrouping())
		require.Equal(t, 3, result.GetGrouping().Count())
	})

	t.Run("relation error and recovery", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		bat := batch.NewWithSize(1)
		t.Cleanup(func() { bat.Clean(proc.Mp()) })
		bat.Vecs[0] = testutil.MakeInt32Vector([]int32{5, -7, 0, 19, 42}, nil, proc.Mp())
		bat.SetRowCount(5)
		executor, err := NewExpressionExecutor(proc, &plan.Expr{
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 2, ColPos: 0}},
			Typ:  plan.Type{Id: int32(types.T_int32), NotNullable: true},
		})
		if executor != nil {
			t.Cleanup(executor.Free)
		}
		require.NoError(t, err)
		native := proc.Mp().CurrNB()
		heap, objects := proc.Mp().OnHeapOutstanding()
		result, err := executor.Eval(proc, []*batch.Batch{bat, bat}, nil)
		require.Nil(t, result)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal), "%v", err)
		require.Contains(t, err.Error(), "relIndex 2 out of range")
		require.Equal(t, native, proc.Mp().CurrNB())
		afterHeap, afterObjects := proc.Mp().OnHeapOutstanding()
		require.Equal(t, heap, afterHeap)
		require.Equal(t, objects, afterObjects)
		require.Equal(t, []int32{5, -7, 0, 19, 42}, vector.MustFixedColNoTypeCheck[int32](bat.Vecs[0]))
		// The existing single-batch compatibility path must recover on the same executor.
		result, err = executor.Eval(proc, []*batch.Batch{bat}, nil)
		require.NoError(t, err)
		require.Same(t, bat.Vecs[0], result)
		require.Equal(t, types.T_int32.ToType(), *result.GetType())
		require.Equal(t, 5, result.Length())
		require.Equal(t, []int32{5, -7, 0, 19, 42}, vector.MustFixedColNoTypeCheck[int32](result))
	})
}

func TestFunctionExpressionLifecycle(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))

	t.Run("ordinary", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		bat := batch.NewWithSize(1)
		t.Cleanup(func() { bat.Clean(proc.Mp()) })
		bat.Vecs[0] = testutil.MakeInt64Vector([]int64{1, 2}, nil, proc.Mp())
		bat.SetRowCount(2)
		native := proc.Mp().CurrNB()
		heap, objects := proc.Mp().OnHeapOutstanding()
		columnType := types.T_int64.ToType().PlanType()
		columnType.NotNullable = true
		expression := bindTestFunction(t, proc, "+",
			&plan.Expr{Typ: columnType, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}},
			makePlan2Int64ConstExprWithType(100))
		expression.Typ.NotNullable = true
		executor, err := NewExpressionExecutor(proc, expression)
		if executor != nil {
			t.Cleanup(func() {
				if executor != nil {
					executor.Free()
				}
			})
		}
		require.NoError(t, err)
		_, err = DebugShowExecutor(executor)
		require.NoError(t, err)
		first, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
		require.NoError(t, err)
		require.Equal(t, types.Type{Oid: types.T_int64, Size: 8}, *first.GetType())
		require.Equal(t, 2, first.Length())
		require.Equal(t, []int64{101, 102}, vector.MustFixedColNoTypeCheck[int64](first))
		require.False(t, first.IsNull(0))
		require.False(t, first.IsNull(1))
		_, err = DebugShowExecutor(executor)
		require.NoError(t, err)
		reuseNative := proc.Mp().CurrNB()
		reuseHeap, reuseObjects := proc.Mp().OnHeapOutstanding()
		second, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
		require.NoError(t, err)
		require.Same(t, first, second)
		require.Equal(t, types.Type{Oid: types.T_int64, Size: 8}, *second.GetType())
		require.Equal(t, 2, second.Length())
		require.Equal(t, []int64{101, 102}, vector.MustFixedColNoTypeCheck[int64](second))
		require.False(t, second.IsNull(0))
		require.False(t, second.IsNull(1))
		require.Equal(t, reuseNative, proc.Mp().CurrNB())
		afterHeap, afterObjects := proc.Mp().OnHeapOutstanding()
		require.Equal(t, reuseHeap, afterHeap)
		require.Equal(t, reuseObjects, afterObjects)
		executor.Free()
		executor = nil
		require.Equal(t, native, proc.Mp().CurrNB())
		afterHeap, afterObjects = proc.Mp().OnHeapOutstanding()
		require.Equal(t, heap, afterHeap)
		require.Equal(t, objects, afterObjects)
		require.Equal(t, []int64{1, 2}, vector.MustFixedColNoTypeCheck[int64](bat.Vecs[0]))
	})

	t.Run("folded_reset", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		input := batch.NewWithSize(0)
		t.Cleanup(func() { input.Clean(proc.Mp()) })
		input.SetRowCount(100)
		expression := bindTestFunction(t, proc, "and", makePlan2BoolConstExprWithType(true), makePlan2BoolConstExprWithType(true))
		expression.Typ.NotNullable = true
		executor, err := NewExpressionExecutor(proc, expression)
		if executor != nil {
			t.Cleanup(executor.Free)
		}
		require.NoError(t, err)
		_, err = DebugShowExecutor(executor)
		require.NoError(t, err)
		checkTrue := func(result *vector.Vector, length int) {
			t.Helper()
			require.Equal(t, types.Type{Oid: types.T_bool, Size: 1}, *result.GetType())
			require.True(t, result.IsConst())
			require.Equal(t, length, result.Length())
			for row := 0; row < length; row++ {
				require.False(t, result.IsNull(uint64(row)))
				require.True(t, vector.GetFixedAtNoTypeCheck[bool](result, row))
			}
		}
		result, err := executor.Eval(proc, nil, nil)
		require.NoError(t, err)
		checkTrue(result, 1)
		_, err = DebugShowExecutor(executor)
		require.NoError(t, err)
		result, err = executor.Eval(proc, []*batch.Batch{input}, nil)
		require.NoError(t, err)
		checkTrue(result, 100)
		executor.ResetForNextQuery()
		functionExecutor, ok := executor.(*FunctionExpressionExecutor)
		require.True(t, ok)
		require.True(t, functionExecutor.folded.needFoldingCheck)
		require.False(t, functionExecutor.folded.canFold)
		require.Len(t, functionExecutor.parameterResults, 2)
		require.Nil(t, functionExecutor.parameterResults[0])
		require.Nil(t, functionExecutor.parameterResults[1])
		result, err = executor.Eval(proc, nil, nil)
		require.NoError(t, err)
		checkTrue(result, 1)
	})

	for _, test := range []struct {
		name, op  string
		wantLarge []float64
		wantSmall float64
	}{
		{"divide", "/", []float64{2.5, 4.5}, 2.5},
		{"add", "+", []float64{7, 11}, 7},
		{"multiply", "*", []float64{10, 18}, 10},
	} {
		t.Run(test.name, func(t *testing.T) {
			checkExpressionStorageAfterCleanup(t, proc)
			large := batch.NewWithSize(1)
			t.Cleanup(func() { large.Clean(proc.Mp()) })
			large.Vecs[0] = testutil.MakeFloat64Vector([]float64{5, 9, 5}, nil, proc.Mp())
			large.SetRowCount(3)
			small := batch.NewWithSize(1)
			t.Cleanup(func() { small.Clean(proc.Mp()) })
			small.Vecs[0] = testutil.MakeFloat64Vector([]float64{5, 5}, nil, proc.Mp())
			small.SetRowCount(2)
			inputType := types.T_float64.ToType().PlanType()
			inputType.NotNullable = true
			expression := bindTestFunction(t, proc, test.op,
				&plan.Expr{Typ: inputType, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}},
				&plan.Expr{Typ: inputType, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Dval{Dval: 2}}}})
			expression.Typ.NotNullable = false
			executor, err := NewExpressionExecutor(proc, expression)
			if executor != nil {
				t.Cleanup(executor.Free)
			}
			require.NoError(t, err)
			result, err := executor.Eval(proc, []*batch.Batch{large}, []bool{true, true, false})
			require.NoError(t, err)
			require.Equal(t, types.Type{Oid: types.T_float64, Size: 8}, *result.GetType())
			require.Equal(t, 3, result.Length())
			require.Equal(t, test.wantLarge, vector.MustFixedColNoTypeCheck[float64](result)[:2])
			require.False(t, result.IsNull(0))
			require.False(t, result.IsNull(1))
			require.True(t, result.IsNull(2))
			result, err = executor.Eval(proc, []*batch.Batch{small}, []bool{true, false})
			require.NoError(t, err)
			require.Equal(t, types.Type{Oid: types.T_float64, Size: 8}, *result.GetType())
			require.Equal(t, 2, result.Length())
			require.Equal(t, test.wantSmall, vector.GetFixedAtNoTypeCheck[float64](result, 0))
			require.False(t, result.GetNulls().Contains(0))
			require.True(t, result.GetNulls().Contains(1))
			require.False(t, result.GetNulls().Contains(2))
		})
	}
}

func TestFunctionExpressionExecutorSelectedRowsPreservesJSONComparisonIdentity(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	checkExpressionStorageAfterCleanup(t, proc)
	bat := batch.NewWithSize(1)
	defer bat.Clean(proc.Mp())
	bat.Vecs[0] = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendStringList(bat.Vecs[0], []string{"1", "invalid integer", "2"}, nil, proc.Mp()))
	bat.Vecs[0].SetPrepareParamKind(vector.PrepareParamInteger)
	bat.Vecs[0].SetPrepareParamType(types.T_int64)
	bat.SetRowCount(3)
	column := &plan.Expr{
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
		Typ:  plan.Type{Id: int32(types.T_text)},
	}
	expr, err := NewExpressionExecutor(proc, bindTestFunction(t, proc, function.JsonComparisonParamFunctionName, column))
	require.NoError(t, err)
	defer expr.Free()
	result, err := expr.Eval(proc, []*batch.Batch{bat}, []bool{true, false, true})
	require.NoError(t, err)
	require.True(t, result.IsPreparedJSONComparisonParam())
	require.Equal(t, types.T_int64, result.GetPrepareParamType())
	require.Equal(t, vector.PrepareParamInteger, result.GetPrepareParamKind())
	require.False(t, result.IsNull(0))
	require.True(t, result.IsNull(1))
	require.False(t, result.IsNull(2))
	require.Equal(t, "1", types.DecodeJson(result.GetBytesAt(0)).String())
	require.Equal(t, "2", types.DecodeJson(result.GetBytesAt(2)).String())
}

func TestPreparedJSONComparisonSelection(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	for _, test := range []struct {
		name, parameter, jsonValue string
		typ                        types.T
		reject                     bool
	}{
		{name: "string JSON uses parameter conversion", parameter: "1", jsonValue: `"1"`, typ: types.T_int64},
		{name: "narrow parameter retains range rejection", parameter: "127", jsonValue: `128`, typ: types.T_int8, reject: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			checkExpressionStorageAfterCleanup(t, proc)
			previous := proc.DetachPrepareParams()
			defer proc.RestorePrepareParams(previous)
			params := vector.NewVec(types.T_text.ToType())
			defer params.Free(proc.Mp())
			require.NoError(t, vector.AppendBytes(params, []byte(test.parameter), false, proc.Mp()))
			proc.SetPrepareParamsWithTypedMeta(params, nil, []vector.PrepareParamKind{vector.PrepareParamInteger}, []types.T{test.typ})
			bat := batch.NewWithSize(1)
			defer bat.Clean(proc.Mp())
			bat.Vecs[0] = vector.NewVec(types.T_json.ToType())
			fill := func(text string, rows int) {
				t.Helper()
				bat.Vecs[0].ResetWithSameType()
				value, err := types.ParseStringToByteJson(text)
				require.NoError(t, err)
				encoded, err := types.EncodeJson(value)
				require.NoError(t, err)
				for range rows {
					require.NoError(t, vector.AppendBytes(bat.Vecs[0], encoded, false, proc.Mp()))
				}
				bat.SetRowCount(rows)
			}
			fill(test.jsonValue, 2)
			column := &plan.Expr{Typ: plan.Type{Id: int32(types.T_json)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
			parameter := &plan.Expr{Typ: plan.Type{Id: int32(types.T_text)}, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}}
			adapter := bindTestFunction(t, proc, function.JsonComparisonParamFunctionName, parameter)
			executor, err := NewExpressionExecutor(proc, bindTestFunction(t, proc, "=", column, adapter))
			require.NoError(t, err)
			defer executor.Free()
			for _, selection := range [][]bool{nil, {false, true}} {
				result, err := executor.Eval(proc, []*batch.Batch{bat}, selection)
				if test.reject {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidArg), "expected exact-width conversion failure: %v", err)
				} else {
					require.NoError(t, err)
					require.Equal(t, 2, result.Length())
					require.True(t, vector.GetFixedAtNoTypeCheck[bool](result, 1))
					require.False(t, result.IsNull(1))
					require.Equal(t, selection != nil, result.IsNull(0))
					if selection == nil {
						require.True(t, vector.GetFixedAtNoTypeCheck[bool](result, 0))
					}
				}
			}
			compact := executor.(*FunctionExpressionExecutor).selectedParameterVectors[1]
			require.True(t, compact.IsPreparedJSONComparisonParam())
			require.Equal(t, test.typ, compact.GetPrepareParamType())
			empty, err := executor.Eval(proc, []*batch.Batch{bat}, []bool{false, false})
			require.NoError(t, err)
			require.Equal(t, 2, empty.Length())
			require.True(t, empty.IsNull(0))
			require.True(t, empty.IsNull(1))
			bat.SetRowCount(0)
			empty, err = executor.Eval(proc, []*batch.Batch{bat}, []bool{})
			require.NoError(t, err)
			require.Zero(t, empty.Length())
			// Retry the same compaction/scatter path after a selected conversion error.
			fill(test.parameter, 2)
			result, err := executor.Eval(proc, []*batch.Batch{bat}, []bool{false, true})
			require.NoError(t, err)
			require.Equal(t, 2, result.Length())
			require.True(t, result.IsNull(0))
			require.False(t, result.IsNull(1))
			require.True(t, vector.GetFixedAtNoTypeCheck[bool](result, 1))
			if test.reject {
				fill(test.jsonValue, 2)
				proc.SetPrepareParamsWithTypedMeta(params, nil, []vector.PrepareParamKind{vector.PrepareParamInteger}, []types.T{types.T_int64})
				executor.ResetForNextQuery()
				result, err = executor.Eval(proc, []*batch.Batch{bat}, []bool{false, true})
				require.NoError(t, err)
				require.False(t, result.IsNull(1))
				require.False(t, vector.GetFixedAtNoTypeCheck[bool](result, 1))
				proc.SetPrepareParamsWithTypedMeta(params, nil, []vector.PrepareParamKind{vector.PrepareParamInteger}, []types.T{test.typ})
				executor.ResetForNextQuery()
				_, err = executor.Eval(proc, []*batch.Batch{bat}, []bool{false, true})
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidArg))
			}
		})
	}
	t.Run("selected NULL retains typed adapter identity", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		bat := batch.NewWithSize(2)
		defer bat.Clean(proc.Mp())
		bat.Vecs[0] = vector.NewVec(types.T_text.ToType())
		require.NoError(t, vector.AppendStringList(bat.Vecs[0], []string{"1", ""}, []bool{false, true}, proc.Mp()))
		bat.Vecs[0].SetPrepareParamKind(vector.PrepareParamInteger)
		bat.Vecs[0].SetPrepareParamType(types.T_int64)
		bat.Vecs[1] = vector.NewVec(types.T_json.ToType())
		value, err := types.ParseStringToByteJson(`"1"`)
		require.NoError(t, err)
		encoded, err := types.EncodeJson(value)
		require.NoError(t, err)
		for range 2 {
			require.NoError(t, vector.AppendBytes(bat.Vecs[1], encoded, false, proc.Mp()))
		}
		bat.SetRowCount(2)
		parameter := &plan.Expr{Typ: types.T_text.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
		jsonColumn := &plan.Expr{Typ: types.T_json.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}}}
		for _, op := range []string{"=", "<=>"} {
			t.Run(op, func(t *testing.T) {
				executor, err := NewExpressionExecutor(proc, bindTestFunction(t, proc, op, jsonColumn,
					bindTestFunction(t, proc, function.JsonComparisonParamFunctionName, parameter)))
				require.NoError(t, err)
				defer executor.Free()
				for _, selection := range [][]bool{nil, {false, true}, {true, false}, {false, false}} {
					result, err := executor.Eval(proc, []*batch.Batch{bat}, selection)
					require.NoError(t, err)
					require.Equal(t, 2, result.Length())
					selected0 := selection == nil || selection[0]
					selected1 := selection == nil || selection[1]
					require.Equal(t, !selected0, result.IsNull(0))
					if selected0 {
						require.True(t, vector.GetFixedAtNoTypeCheck[bool](result, 0))
					}
					require.Equal(t, !selected1 || op == "=", result.IsNull(1))
					if selected1 && op == "<=>" {
						require.False(t, vector.GetFixedAtNoTypeCheck[bool](result, 1))
					}
					if !selected0 && selected1 {
						compact := executor.(*FunctionExpressionExecutor).selectedParameterVectors[1]
						require.True(t, compact.AllNull())
						require.Equal(t, vector.PrepareParamNone, compact.GetPrepareParamKind())
						require.Equal(t, types.T_int64, compact.GetPrepareParamType())
						require.True(t, compact.IsPreparedJSONComparisonParam())
					}
				}
				executor.ResetForNextQuery()
				result, err := executor.Eval(proc, []*batch.Batch{bat}, []bool{true, false})
				require.NoError(t, err)
				require.False(t, result.IsNull(0))
				require.True(t, vector.GetFixedAtNoTypeCheck[bool](result, 0))
				require.True(t, result.IsNull(1))
			})
		}
	})
}

func TestFlowControlShortCircuitInvalidCast(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))

	stringConst := func(value string) *plan.Expr {
		return &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_varchar), NotNullable: true},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				Value: &plan.Literal_Sval{Sval: value},
			}},
		}
	}
	uint8Const := func(value uint8) *plan.Expr {
		return &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_uint8), NotNullable: true},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				Value: &plan.Literal_U8Val{U8Val: uint32(value)},
			}},
		}
	}

	invalidCast := func() *plan.Expr {
		target := &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_int64), NotNullable: true},
			Expr: &plan.Expr_T{T: &plan.TargetType{}},
		}
		return bindTestFunction(t, proc, "cast", stringConst("bad"), target)
	}

	tests := []struct {
		name string
		expr *plan.Expr
		want int64
	}{
		{
			name: "if skips true branch",
			expr: bindTestFunction(t, proc, "if", makePlan2BoolConstExprWithType(false), invalidCast(), makePlan2Int64ConstExprWithType(7)),
			want: 7,
		},
		{
			name: "case skips then branch",
			expr: bindTestFunction(t, proc, "case", makePlan2BoolConstExprWithType(false), invalidCast(), makePlan2Int64ConstExprWithType(7)),
			want: 7,
		},
		{
			name: "coalesce skips later argument",
			expr: bindTestFunction(t, proc, "coalesce", makePlan2Int64ConstExprWithType(5), invalidCast()),
			want: 5,
		},
		{
			name: "ifnull rewrite skips second argument",
			expr: bindTestFunction(t, proc, "case",
				bindTestFunction(t, proc, "isnull", makePlan2Int64ConstExprWithType(5)),
				invalidCast(),
				makePlan2Int64ConstExprWithType(5)),
			want: 5,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor, err := NewExpressionExecutor(proc, test.expr)
			require.NoError(t, err)
			defer executor.Free()

			result, err := executor.Eval(proc, nil, nil)
			require.NoError(t, err)
			require.Equal(t, test.want, vector.MustFixedColWithTypeCheck[int64](result)[0])
			require.True(t, executor.(*FunctionExpressionExecutor).folded.canFold)
		})
	}

	t.Run("if evaluates selected branch", func(t *testing.T) {
		expr := bindTestFunction(t, proc, "if", makePlan2BoolConstExprWithType(true), invalidCast(), makePlan2Int64ConstExprWithType(7))
		executor, err := NewExpressionExecutor(proc, expr)
		require.NoError(t, err)
		defer executor.Free()

		_, err = executor.Eval(proc, nil, nil)
		require.ErrorContains(t, err, "invalid argument cast to int")
	})

	t.Run("coalesce evaluates remaining argument", func(t *testing.T) {
		nullInt64 := &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}},
		}
		expr := bindTestFunction(t, proc, "coalesce", nullInt64, invalidCast())
		executor, err := NewExpressionExecutor(proc, expr)
		require.NoError(t, err)
		defer executor.Free()

		_, err = executor.Eval(proc, nil, nil)
		require.ErrorContains(t, err, "invalid argument cast to int")
	})

	t.Run("skipped varlen function preserves batch length", func(t *testing.T) {
		input := batch.New(nil)
		input.SetRowCount(2)
		expr := bindTestFunction(t, proc, "concat", stringConst("a"), stringConst("b"))
		executor, err := NewExpressionExecutor(proc, expr)
		require.NoError(t, err)
		defer executor.Free()

		result, err := executor.Eval(proc, []*batch.Batch{input}, []bool{false, false})
		require.NoError(t, err)
		require.Equal(t, 2, result.Length())
		require.True(t, result.GetNulls().Contains(0))
		require.True(t, result.GetNulls().Contains(1))
	})

	column := func(pos int32, typ types.Type) *plan.Expr {
		return &plan.Expr{
			Typ:  plan.Type{Id: int32(typ.Oid), Width: typ.Width, Scale: typ.Scale},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: pos}},
		}
	}
	castTo := func(source *plan.Expr, typ types.Type) *plan.Expr {
		target := &plan.Expr{
			Typ:  plan.Type{Id: int32(typ.Oid), Width: typ.Width, Scale: typ.Scale, NotNullable: true},
			Expr: &plan.Expr_T{T: &plan.TargetType{}},
		}
		return bindTestFunction(t, proc, "cast", source, target)
	}
	castToInt64 := func(source *plan.Expr) *plan.Expr {
		return castTo(source, types.T_int64.ToType())
	}
	typedNull := func(typ types.Type) *plan.Expr {
		return &plan.Expr{
			Typ:  plan.Type{Id: int32(typ.Oid), Width: typ.Width, Scale: typ.Scale},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}},
		}
	}

	t.Run("if skips unresolved variable leaf across reuse", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		previousResolver := proc.GetResolveVariableFunc()
		defer proc.SetResolveVariableFunc(previousResolver)
		resolveCalls := 0
		proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
			resolveCalls++
			return nil, moerr.NewInternalErrorNoCtx("missing variable")
		})

		variable := &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_varchar)},
			Expr: &plan.Expr_V{V: &plan.VarRef{
				Name: "missing_user_variable",
			}},
		}
		expr := bindTestFunction(t, proc, "if",
			column(0, types.T_bool.ToType()),
			variable,
			stringConst("ok"))
		executor, err := NewExpressionExecutor(proc, expr)
		require.NoError(t, err)
		defer executor.Free()

		eval := func(condition bool) (*vector.Vector, error) {
			input := testutil.NewBatchWithVectors([]*vector.Vector{
				testutil.NewVector(1, types.T_bool.ToType(), proc.Mp(), false, []bool{condition}),
			}, nil)
			defer input.Clean(proc.Mp())
			return executor.Eval(proc, []*batch.Batch{input}, nil)
		}

		result, err := eval(false)
		require.NoError(t, err)
		require.Equal(t, "ok", result.GetStringAt(0))
		require.Zero(t, resolveCalls)

		_, err = eval(true)
		require.ErrorContains(t, err, "missing variable")
		require.Equal(t, 1, resolveCalls)

		result, err = eval(false)
		require.NoError(t, err)
		require.Equal(t, "ok", result.GetStringAt(0))
		require.Equal(t, 1, resolveCalls)
	})

	t.Run("case and coalesce skip unresolved variable leaves", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		previousResolver := proc.GetResolveVariableFunc()
		defer proc.SetResolveVariableFunc(previousResolver)
		resolveCalls := 0
		proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
			resolveCalls++
			return nil, moerr.NewInternalErrorNoCtx("missing variable")
		})

		variable := &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_varchar)},
			Expr: &plan.Expr_V{V: &plan.VarRef{
				Name: "missing_user_variable",
			}},
		}
		tests := []struct {
			name  string
			expr  *plan.Expr
			input *vector.Vector
		}{
			{
				name: "case",
				expr: bindTestFunction(t, proc, "case",
					column(0, types.T_bool.ToType()),
					variable,
					stringConst("ok")),
				input: testutil.NewVector(1, types.T_bool.ToType(), proc.Mp(), false, []bool{false}),
			},
			{
				name: "coalesce",
				expr: bindTestFunction(t, proc, "coalesce",
					column(0, types.T_varchar.ToType()),
					variable),
				input: testutil.NewVector(1, types.T_varchar.ToType(), proc.Mp(), false, []string{"ok"}),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				input := testutil.NewBatchWithVectors([]*vector.Vector{test.input}, nil)
				defer input.Clean(proc.Mp())
				executor, err := NewExpressionExecutor(proc, test.expr)
				require.NoError(t, err)
				defer executor.Free()

				result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
				require.NoError(t, err)
				require.Equal(t, "ok", result.GetStringAt(0))
			})
		}
		require.Zero(t, resolveCalls)
	})

	t.Run("if skips missing parameter leaf", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		previousParams := proc.DetachPrepareParams()
		defer proc.RestorePrepareParams(previousParams)
		params := vector.NewVec(types.T_text.ToType())
		defer params.Free(proc.Mp())
		proc.SetPrepareParams(params)

		parameter := &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_varchar)},
			Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
		}
		expr := bindTestFunction(t, proc, "if",
			column(0, types.T_bool.ToType()),
			parameter,
			stringConst("ok"))
		executor, err := NewExpressionExecutor(proc, expr)
		require.NoError(t, err)
		defer executor.Free()

		input := testutil.NewBatchWithVectors([]*vector.Vector{
			testutil.NewVector(1, types.T_bool.ToType(), proc.Mp(), false, []bool{false}),
		}, nil)
		defer input.Clean(proc.Mp())
		result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
		require.NoError(t, err)
		require.Equal(t, "ok", result.GetStringAt(0))
	})

	t.Run("parameter leaf remains valid after a skipped generation", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		previousParams := proc.DetachPrepareParams()
		defer proc.RestorePrepareParams(previousParams)
		params := vector.NewVec(types.T_text.ToType())
		defer params.Free(proc.Mp())
		require.NoError(t, vector.AppendBytes(params, []byte("parameter"), false, proc.Mp()))
		proc.SetPrepareParams(params)

		parameter := &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_varchar)},
			Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
		}
		expr := bindTestFunction(t, proc, "if",
			column(0, types.T_bool.ToType()),
			parameter,
			stringConst("fallback"))
		executor, err := NewExpressionExecutor(proc, expr)
		require.NoError(t, err)
		defer executor.Free()

		eval := func(condition bool) string {
			t.Helper()
			input := testutil.NewBatchWithVectors([]*vector.Vector{
				testutil.NewVector(1, types.T_bool.ToType(), proc.Mp(), false, []bool{condition}),
			}, nil)
			defer input.Clean(proc.Mp())
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			return result.GetStringAt(0)
		}

		require.Equal(t, "fallback", eval(false))
		require.Equal(t, "parameter", eval(true))
		require.Equal(t, "parameter", eval(true))
	})

	t.Run("runtime parameter folding follows prepared statement reset", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		previousParams := proc.DetachPrepareParams()
		defer proc.RestorePrepareParams(previousParams)
		running := proc.GetBaseProcessRunningStatus()
		defer proc.SetBaseProcessRunningStatus(running)
		proc.SetBaseProcessRunningStatus(true)

		parameter := &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_varchar)},
			Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
		}
		expr := bindTestFunction(t, proc, "if",
			makePlan2BoolConstExprWithType(true),
			parameter,
			stringConst("fallback"))
		executor, err := NewExpressionExecutor(proc, expr)
		require.NoError(t, err)
		defer executor.Free()

		eval := func(value string) string {
			t.Helper()
			params := vector.NewVec(types.T_text.ToType())
			defer params.Free(proc.Mp())
			require.NoError(t, vector.AppendBytes(params, []byte(value), false, proc.Mp()))
			proc.SetPrepareParams(params)
			defer func() {
				proc.SetPrepareParams(nil)
			}()

			result, evalErr := executor.Eval(proc, nil, nil)
			require.NoError(t, evalErr)
			require.True(t, executor.(*FunctionExpressionExecutor).folded.canFold)
			return result.GetStringAt(0)
		}

		require.Equal(t, "first", eval("first"))
		executor.ResetForNextQuery()
		require.Equal(t, "second", eval("second"))
	})

	t.Run("case without else and coalesce all null still fold", func(t *testing.T) {
		nullInt64 := typedNull(types.T_int64.ToType())
		expressions := []*plan.Expr{
			bindTestFunction(t, proc, "case", makePlan2BoolConstExprWithType(false), makePlan2Int64ConstExprWithType(7)),
			bindTestFunction(t, proc, "coalesce", nullInt64, typedNull(types.T_int64.ToType())),
		}
		for _, expr := range expressions {
			executor, err := NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer executor.Free()

			result, err := executor.Eval(proc, nil, nil)
			require.NoError(t, err)
			require.True(t, result.IsConstNull())
			require.True(t, executor.(*FunctionExpressionExecutor).folded.canFold)
		}
	})

	t.Run("constant flow control stays allocation-free after folding", func(t *testing.T) {
		expr := bindTestFunction(t, proc, "if",
			makePlan2BoolConstExprWithType(true),
			makePlan2Int64ConstExprWithType(7),
			makePlan2Int64ConstExprWithType(9))
		executor, err := NewExpressionExecutor(proc, expr)
		require.NoError(t, err)
		defer executor.Free()

		input := batch.New(nil)
		input.SetRowCount(8192)
		batches := []*batch.Batch{input}
		result, err := executor.Eval(proc, batches, nil)
		require.NoError(t, err)
		require.True(t, result.IsConst())
		require.Equal(t, 8192, result.Length())
		require.Equal(t, int64(7), vector.MustFixedColWithTypeCheck[int64](result)[0])
		require.True(t, executor.(*FunctionExpressionExecutor).folded.canFold)

		var evalErr error
		allocations := testing.AllocsPerRun(100, func() {
			_, evalErr = executor.Eval(proc, batches, nil)
		})
		require.NoError(t, evalErr)
		require.LessOrEqual(t, allocations, 1.0)
	})

	t.Run("partial evaluation preserves runtime result type", func(t *testing.T) {
		sourceType := types.New(types.T_float32, 10, 2)
		input := testutil.NewBatchWithVectors([]*vector.Vector{
			testutil.NewVector(2, sourceType, proc.Mp(), false, []float32{1.25, 2.5}),
		}, nil)
		defer input.Clean(proc.Mp())

		expr := castTo(column(0, sourceType), types.T_float64.ToType())
		evalType := func(selectList []bool) types.Type {
			t.Helper()
			executor, err := NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer executor.Free()

			result, err := executor.Eval(proc, []*batch.Batch{input}, selectList)
			require.NoError(t, err)
			return *result.GetType()
		}

		fullType := evalType(nil)
		partialType := evalType([]bool{false, true})
		require.Equal(t, fullType, partialType)
		require.Equal(t, int32(10), partialType.Width)
		require.Equal(t, int32(2), partialType.Scale)
	})

	t.Run("partial runtime result type updates across reuse", func(t *testing.T) {
		expr := bindTestFunction(t, proc, "sysdate", column(0, types.T_int64.ToType()))
		executor, err := NewExpressionExecutor(proc, expr)
		require.NoError(t, err)
		defer executor.Free()

		evalScale := func(scale int64) {
			t.Helper()
			input := testutil.NewBatchWithVectors([]*vector.Vector{
				testutil.NewVector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{6, scale}),
			}, nil)
			defer input.Clean(proc.Mp())

			result, err := executor.Eval(proc, []*batch.Batch{input}, []bool{false, true})
			require.NoError(t, err)
			require.Equal(t, int32(scale), result.GetType().Scale)
		}

		evalScale(3)
		evalScale(1)
		evalScale(5)
	})

	t.Run("nested consumer observes partial runtime result type", func(t *testing.T) {
		input := testutil.NewBatchWithVectors([]*vector.Vector{
			testutil.NewVector(2, types.T_bool.ToType(), proc.Mp(), false, []bool{false, true}),
		}, nil)
		defer input.Clean(proc.Mp())

		sysdate := bindTestFunction(t, proc, "sysdate", makePlan2Int64ConstExprWithType(3))
		asChar := castTo(sysdate, types.New(types.T_char, 64, 0))
		directExecutor, err := NewExpressionExecutor(proc, asChar)
		require.NoError(t, err)
		defer directExecutor.Free()
		direct, err := directExecutor.Eval(proc, []*batch.Batch{input}, nil)
		require.NoError(t, err)

		ifExpr := bindTestFunction(t, proc, "if",
			column(0, types.T_bool.ToType()),
			asChar,
			stringConst("fallback"))
		ifExecutor, err := NewExpressionExecutor(proc, ifExpr)
		require.NoError(t, err)
		defer ifExecutor.Free()
		partial, err := ifExecutor.Eval(proc, []*batch.Batch{input}, nil)
		require.NoError(t, err)

		require.Equal(t, 23, len(direct.GetStringAt(0)))
		require.Equal(t, "fallback", partial.GetStringAt(0))
		require.Equal(t, len(direct.GetStringAt(0)), len(partial.GetStringAt(1)))
	})

	t.Run("if skips invalid rows within a batch", func(t *testing.T) {
		input := testutil.NewBatchWithVectors([]*vector.Vector{
			testutil.NewVector(2, types.T_bool.ToType(), proc.Mp(), false, []bool{false, true}),
			testutil.NewVector(2, types.T_varchar.ToType(), proc.Mp(), false, []string{"bad", "9"}),
		}, nil)
		defer input.Clean(proc.Mp())

		expr := bindTestFunction(t, proc, "if",
			column(0, types.T_bool.ToType()),
			castToInt64(column(1, types.T_varchar.ToType())),
			makePlan2Int64ConstExprWithType(7))
		executor, err := NewExpressionExecutor(proc, expr)
		require.NoError(t, err)
		defer executor.Free()

		result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
		require.NoError(t, err)
		require.Equal(t, []int64{7, 9}, vector.MustFixedColWithTypeCheck[int64](result))
	})

	t.Run("if skips invalid regexp rows within a batch", func(t *testing.T) {
		input := testutil.NewBatchWithVectors([]*vector.Vector{
			testutil.NewVector(2, types.T_bool.ToType(), proc.Mp(), false, []bool{false, true}),
			testutil.NewVector(2, types.T_varchar.ToType(), proc.Mp(), false, []string{"x", "a"}),
			testutil.NewVector(2, types.T_varchar.ToType(), proc.Mp(), false, []string{"[", "a"}),
			testutil.NewVector(2, types.T_varchar.ToType(), proc.Mp(), false, []string{"c", "c"}),
		}, nil)
		defer input.Clean(proc.Mp())

		expr := bindTestFunction(t, proc, "if",
			column(0, types.T_bool.ToType()),
			bindTestFunction(t, proc, "regexp_like",
				column(1, types.T_varchar.ToType()),
				column(2, types.T_varchar.ToType()),
				column(3, types.T_varchar.ToType())),
			makePlan2BoolConstExprWithType(false))
		executor, err := NewExpressionExecutor(proc, expr)
		require.NoError(t, err)
		defer executor.Free()

		result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
		require.NoError(t, err)
		require.Equal(t, []bool{false, true}, vector.MustFixedColWithTypeCheck[bool](result))
	})

	t.Run("if still evaluates invalid selected regexp row", func(t *testing.T) {
		input := testutil.NewBatchWithVectors([]*vector.Vector{
			testutil.NewVector(2, types.T_bool.ToType(), proc.Mp(), false, []bool{true, false}),
			testutil.NewVector(2, types.T_varchar.ToType(), proc.Mp(), false, []string{"x", "a"}),
			testutil.NewVector(2, types.T_varchar.ToType(), proc.Mp(), false, []string{"[", "a"}),
			testutil.NewVector(2, types.T_varchar.ToType(), proc.Mp(), false, []string{"c", "c"}),
		}, nil)
		defer input.Clean(proc.Mp())

		expr := bindTestFunction(t, proc, "if",
			column(0, types.T_bool.ToType()),
			bindTestFunction(t, proc, "regexp_like",
				column(1, types.T_varchar.ToType()),
				column(2, types.T_varchar.ToType()),
				column(3, types.T_varchar.ToType())),
			makePlan2BoolConstExprWithType(false))
		executor, err := NewExpressionExecutor(proc, expr)
		require.NoError(t, err)
		defer executor.Free()

		_, err = executor.Eval(proc, []*batch.Batch{input}, nil)
		require.Error(t, err)
	})

	t.Run("if does not execute sleep on unselected rows", func(t *testing.T) {
		input := testutil.NewBatchWithVectors([]*vector.Vector{
			testutil.NewVector(2, types.T_bool.ToType(), proc.Mp(), false, []bool{false, true}),
			testutil.NewVector(2, types.T_float64.ToType(), proc.Mp(), false, []float64{-1, 0}),
		}, nil)
		defer input.Clean(proc.Mp())

		expr := bindTestFunction(t, proc, "if",
			column(0, types.T_bool.ToType()),
			bindTestFunction(t, proc, "sleep", column(1, types.T_float64.ToType())),
			uint8Const(0))
		executor, err := NewExpressionExecutor(proc, expr)
		require.NoError(t, err)
		defer executor.Free()

		result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
		require.NoError(t, err)
		require.Equal(t, []uint8{0, 0}, vector.MustFixedColWithTypeCheck[uint8](result))
	})

	t.Run("if preserves non-row-aligned in-list parameters", func(t *testing.T) {
		input := testutil.NewBatchWithVectors([]*vector.Vector{
			testutil.NewVector(3, types.T_bool.ToType(), proc.Mp(), false, []bool{false, true, false}),
			testutil.NewVector(3, types.T_varchar.ToType(), proc.Mp(), false, []string{"x", "a", "b"}),
		}, nil)
		defer input.Clean(proc.Mp())

		list := &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_varchar)},
			Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{
				stringConst("a"),
				stringConst("b"),
			}}},
		}
		expr := bindTestFunction(t, proc, "if",
			column(0, types.T_bool.ToType()),
			bindTestFunction(t, proc, "in", column(1, types.T_varchar.ToType()), list),
			makePlan2BoolConstExprWithType(false))
		executor, err := NewExpressionExecutor(proc, expr)
		require.NoError(t, err)
		defer executor.Free()

		result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
		require.NoError(t, err)
		require.Equal(t, []bool{false, true, false}, vector.MustFixedColWithTypeCheck[bool](result))
	})

	t.Run("case preserves first match across multiple when clauses and reuse", func(t *testing.T) {
		expr := bindTestFunction(t, proc, "case",
			column(0, types.T_bool.ToType()),
			makePlan2Int64ConstExprWithType(1),
			column(1, types.T_bool.ToType()),
			castToInt64(column(2, types.T_varchar.ToType())),
			makePlan2Int64ConstExprWithType(7))
		executor, err := NewExpressionExecutor(proc, expr)
		require.NoError(t, err)
		defer executor.Free()

		tests := []struct {
			name            string
			firstCondition  []bool
			firstNulls      []bool
			secondCondition []bool
			secondNulls     []bool
			values          []string
			parentSelect    []bool
			want            []int64
		}{
			{
				name:            "multiple rows choose first later else and null conditions",
				firstCondition:  []bool{true, false, false, false, true},
				firstNulls:      []bool{false, false, true, true, false},
				secondCondition: []bool{true, true, false, true, true},
				secondNulls:     []bool{false, false, true, false, false},
				values:          []string{"bad", "9", "bad", "11", "bad"},
				want:            []int64{1, 9, 7, 11, 1},
			},
			{
				name:            "changing and shrinking batch selects later branch",
				firstCondition:  []bool{false, true},
				secondCondition: []bool{true, true},
				values:          []string{"13", "bad"},
				want:            []int64{13, 1},
			},
			{
				name:            "subsequent reuse keeps first match state",
				firstCondition:  []bool{true, false, false},
				secondCondition: []bool{true, false, true},
				values:          []string{"bad", "bad", "17"},
				want:            []int64{1, 7, 17},
			},
			{
				name:            "parent partial selection cannot be reselected",
				firstCondition:  []bool{true, false, false, true},
				secondCondition: []bool{true, true, true, true},
				values:          []string{"bad", "19", "bad", "bad"},
				parentSelect:    []bool{true, true, false, false},
				want:            []int64{1, 19, 0, 0},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				require.Len(t, test.secondCondition, len(test.firstCondition))
				require.Len(t, test.values, len(test.firstCondition))
				input := testutil.NewBatchWithVectors([]*vector.Vector{
					testutil.NewVectorWithNulls(len(test.firstCondition), types.T_bool.ToType(), proc.Mp(), false, test.firstNulls, test.firstCondition),
					testutil.NewVectorWithNulls(len(test.secondCondition), types.T_bool.ToType(), proc.Mp(), false, test.secondNulls, test.secondCondition),
					testutil.NewVector(len(test.values), types.T_varchar.ToType(), proc.Mp(), false, test.values),
				}, nil)
				defer input.Clean(proc.Mp())

				result, err := executor.Eval(proc, []*batch.Batch{input}, test.parentSelect)
				require.NoError(t, err)
				values := vector.MustFixedColWithTypeCheck[int64](result)
				require.Len(t, values, len(test.want))
				for row := range test.want {
					if test.parentSelect != nil && !test.parentSelect[row] {
						continue
					}
					require.False(t, result.IsNull(uint64(row)), "row %d", row)
					require.Equal(t, test.want[row], values[row], "row %d", row)
				}
			})
		}
	})

	for _, test := range []struct {
		name       string
		targetType types.Type
		validValue string
		fallback   *plan.Expr
	}{
		{
			name:       "bool",
			targetType: types.T_bool.ToType(),
			validValue: "true",
			fallback:   makePlan2BoolConstExprWithType(false),
		},
		{
			name:       "uuid",
			targetType: types.T_uuid.ToType(),
			validValue: "00000000-0000-0000-0000-000000000001",
			fallback:   typedNull(types.T_uuid.ToType()),
		},
		{
			name:       "json",
			targetType: types.T_json.ToType(),
			validValue: `{"ok":true}`,
			fallback:   typedNull(types.T_json.ToType()),
		},
	} {
		t.Run("if skips unselected "+test.name+" cast rows", func(t *testing.T) {
			input := testutil.NewBatchWithVectors([]*vector.Vector{
				testutil.NewVector(2, types.T_bool.ToType(), proc.Mp(), false, []bool{false, true}),
				testutil.NewVector(2, types.T_varchar.ToType(), proc.Mp(), false, []string{"bad", test.validValue}),
			}, nil)
			defer input.Clean(proc.Mp())

			expr := bindTestFunction(t, proc, "if",
				column(0, types.T_bool.ToType()),
				castTo(column(1, types.T_varchar.ToType()), test.targetType),
				test.fallback)
			executor, err := NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer executor.Free()

			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.False(t, result.IsNull(1))
			if test.targetType.Oid == types.T_bool {
				require.Equal(t, []bool{false, true}, vector.MustFixedColWithTypeCheck[bool](result))
			} else {
				require.True(t, result.IsNull(0))
			}
		})
	}

	t.Run("if still evaluates invalid selected bool cast row", func(t *testing.T) {
		input := testutil.NewBatchWithVectors([]*vector.Vector{
			testutil.NewVector(2, types.T_bool.ToType(), proc.Mp(), false, []bool{true, false}),
			testutil.NewVector(2, types.T_varchar.ToType(), proc.Mp(), false, []string{"bad", "true"}),
		}, nil)
		defer input.Clean(proc.Mp())

		expr := bindTestFunction(t, proc, "if",
			column(0, types.T_bool.ToType()),
			castTo(column(1, types.T_varchar.ToType()), types.T_bool.ToType()),
			makePlan2BoolConstExprWithType(false))
		executor, err := NewExpressionExecutor(proc, expr)
		require.NoError(t, err)
		defer executor.Free()

		_, err = executor.Eval(proc, []*batch.Batch{input}, nil)
		require.ErrorContains(t, err, "not a valid bool expression")
	})

	t.Run("coalesce skips invalid rows within a batch", func(t *testing.T) {
		input := testutil.NewBatchWithVectors([]*vector.Vector{
			testutil.NewVectorWithNulls(2, types.T_int64.ToType(), proc.Mp(), false, []bool{false, true}, []int64{5, 0}),
			testutil.NewVector(2, types.T_varchar.ToType(), proc.Mp(), false, []string{"bad", "9"}),
		}, nil)
		defer input.Clean(proc.Mp())

		expr := bindTestFunction(t, proc, "coalesce",
			column(0, types.T_int64.ToType()),
			castToInt64(column(1, types.T_varchar.ToType())))
		executor, err := NewExpressionExecutor(proc, expr)
		require.NoError(t, err)
		defer executor.Free()

		result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
		require.NoError(t, err)
		require.Equal(t, []int64{5, 9}, vector.MustFixedColWithTypeCheck[int64](result))
	})

	for _, test := range []struct {
		name string
		expr *plan.Expr
	}{
		{
			name: "if",
			expr: bindTestFunction(t, proc, "if",
				column(0, types.T_bool.ToType()),
				castToInt64(column(1, types.T_varchar.ToType())),
				makePlan2Int64ConstExprWithType(7)),
		},
		{
			name: "case",
			expr: bindTestFunction(t, proc, "case",
				column(0, types.T_bool.ToType()),
				castToInt64(column(1, types.T_varchar.ToType())),
				makePlan2Int64ConstExprWithType(7)),
		},
	} {
		t.Run(test.name+" reuses executor across shrinking batches", func(t *testing.T) {
			executor, err := NewExpressionExecutor(proc, test.expr)
			require.NoError(t, err)
			defer executor.Free()

			eval := func(conditions []bool, values []string, expected []int64) {
				t.Helper()
				require.Len(t, values, len(conditions))
				input := testutil.NewBatchWithVectors([]*vector.Vector{
					testutil.NewVector(len(conditions), types.T_bool.ToType(), proc.Mp(), false, conditions),
					testutil.NewVector(len(values), types.T_varchar.ToType(), proc.Mp(), false, values),
				}, nil)
				defer input.Clean(proc.Mp())

				result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
				require.NoError(t, err)
				require.Equal(t, expected, vector.MustFixedColWithTypeCheck[int64](result))
			}

			eval(
				[]bool{false, false, false, false, false},
				[]string{"bad", "bad", "bad", "bad", "bad"},
				[]int64{7, 7, 7, 7, 7},
			)
			eval(
				[]bool{true, true},
				[]string{"8", "9"},
				[]int64{8, 9},
			)
			eval(
				[]bool{true, false, true},
				[]string{"10", "bad", "12"},
				[]int64{10, 7, 12},
			)
		})
	}
}

func BenchmarkConstantFlowControlExpression(b *testing.B) {
	proc := testutil.NewProcess(b)
	defer proc.Free()

	fn, err := function.GetFunctionByName(proc.Ctx, "if", []types.Type{
		types.T_bool.ToType(),
		types.T_int64.ToType(),
		types.T_int64.ToType(),
	})
	require.NoError(b, err)
	expr := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: fn.GetEncodedOverloadID(), ObjName: "if"},
			Args: []*plan.Expr{
				makePlan2BoolConstExprWithType(true),
				makePlan2Int64ConstExprWithType(7),
				makePlan2Int64ConstExprWithType(9),
			},
		}},
	}
	executor, err := NewExpressionExecutor(proc, expr)
	require.NoError(b, err)
	defer executor.Free()
	input := batch.New(nil)
	input.SetRowCount(8192)
	batches := []*batch.Batch{input}
	_, err = executor.Eval(proc, batches, nil)
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err = executor.Eval(proc, batches, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func TestPreparedCastLifecycle(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	initialSession, initialMode, initialRunning := proc.Session, proc.Base.SessionInfo.SqlMode, proc.GetBaseProcessRunningStatus()
	initialParams := proc.DetachPrepareParams()
	proc.RestorePrepareParams(initialParams)
	t.Cleanup(func() {
		actualParams := proc.DetachPrepareParams()
		proc.RestorePrepareParams(actualParams)
		assert.Equal(t, initialParams, actualParams)
		assert.Equal(t, initialSession, proc.Session)
		assert.Equal(t, initialMode, proc.Base.SessionInfo.SqlMode)
		assert.Equal(t, initialRunning, proc.GetBaseProcessRunningStatus())
	})

	makeNumericExecutor := func(t *testing.T, targetType types.Type) *FunctionExpressionExecutor {
		expr := bindTestFunction(t, proc, "cast",
			&plan.Expr{Typ: plan.Type{Id: int32(types.T_text)}, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}},
			&plan.Expr{Typ: plan.Type{Id: int32(targetType.Oid), Width: targetType.Width, Scale: targetType.Scale}, Expr: &plan.Expr_T{T: &plan.TargetType{}}})
		executor, err := NewExpressionExecutor(proc, expr)
		if executor != nil {
			t.Cleanup(executor.Free)
		}
		require.NoError(t, err)
		result, ok := executor.(*FunctionExpressionExecutor)
		require.True(t, ok)
		return result
	}

	targets := []struct {
		name string
		typ  types.Type
	}{
		{name: "char", typ: types.New(types.T_char, 20, 0)},
		{name: "varchar", typ: types.New(types.T_varchar, 20, 0)},
		{name: "tinytext", typ: types.New(types.T_text, types.MaxTinyTextLen, 0)},
	}
	for _, target := range targets {
		t.Run("assignment "+target.name, func(t *testing.T) {

			checkExpressionStorageAfterCleanup(t, proc)
			sessionBefore, modeBefore, runningBefore := proc.Session, proc.Base.SessionInfo.SqlMode, proc.GetBaseProcessRunningStatus()
			t.Cleanup(func() {
				proc.Session = sessionBefore
				proc.Base.SessionInfo.SqlMode = modeBefore
				proc.SetBaseProcessRunningStatus(runningBefore)
			})

			proc.SetBaseProcessRunningStatus(true)
			proc.Base.SessionInfo.SqlMode = "STRICT_TRANS_TABLES"

			sourceType := types.T_text.ToType()
			expr := bindTestFunction(t, proc, "cast_assign",
				&plan.Expr{Typ: plan.Type{Id: int32(sourceType.Oid), Width: sourceType.Width, Scale: sourceType.Scale}, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}},
				&plan.Expr{Typ: plan.Type{Id: int32(target.typ.Oid), Width: target.typ.Width, Scale: target.typ.Scale}, Expr: &plan.Expr_T{T: &plan.TargetType{}}})
			executor, err := NewExpressionExecutor(proc, expr)
			if executor != nil {
				t.Cleanup(executor.Free)
			}
			require.NoError(t, err)

			for generation, tc := range []struct {
				value string
				null  bool
				kind  vector.PrepareParamKind
			}{
				{"before", false, vector.PrepareParamNone}, {"", true, vector.PrepareParamNone},
				{"after", false, vector.PrepareParamNone}, {"", true, vector.PrepareParamNone},
				{"7", false, vector.PrepareParamInteger},
			} {
				if generation > 0 {
					executor.ResetForNextQuery()
				}
				result := evalTestTextParameter(t, proc, executor, tc.value, tc.null, tc.kind, nil, nil)
				require.False(t, executor.(*FunctionExpressionExecutor).folded.canFold)
				require.Equal(t, tc.null, result.IsNull(0), "generation %d", generation)
				if !tc.null {
					require.Equal(t, tc.value, result.GetStringAt(0))
				}
			}
		})
	}

	t.Run("numeric", func(t *testing.T) {
		wasRunning := proc.GetBaseProcessRunningStatus()
		t.Cleanup(func() { proc.SetBaseProcessRunningStatus(wasRunning) })
		proc.SetBaseProcessRunningStatus(true)
		for _, tc := range []struct {
			name   string
			kind   vector.PrepareParamKind
			fold   bool
			values []int32
		}{
			{"integer provenance folds", vector.PrepareParamInteger, true, []int32{42, 43}},
			{"ordinary text does not fold", vector.PrepareParamNone, false, []int32{42}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				checkExpressionStorageAfterCleanup(t, proc)
				executor := makeNumericExecutor(t, types.T_int32.ToType())
				for generation, want := range tc.values {
					if generation > 0 {
						executor.ResetForNextQuery()
					}
					value := "42"
					if want == 43 {
						value = "43"
					}
					result := evalTestTextParameter(t, proc, executor, value, false, tc.kind, nil, nil)
					require.Equal(t, tc.fold, executor.folded.canFold)
					require.Equal(t, tc.fold, result.IsConst())
					require.Equal(t, want, vector.GetFixedAtNoTypeCheck[int32](result, 0))
				}
			})
		}

		t.Run("owned constant warning multiplicity", func(t *testing.T) {
			checkExpressionStorageAfterCleanup(t, proc)
			sessionBefore := proc.Session
			t.Cleanup(func() { proc.Session = sessionBefore })
			warnings := &preparedCastWarningSession{}
			proc.Session = warnings
			targetType := types.T_float64.ToType()
			expr := bindTestFunction(t, proc, "cast",
				&plan.Expr{Typ: plan.Type{Id: int32(types.T_text)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "12suffix"}}}},
				&plan.Expr{Typ: plan.Type{Id: int32(targetType.Oid)}, Expr: &plan.Expr_T{T: &plan.TargetType{}}})

			executors, err := NewOwnedConstantFilterExecutors(proc, []*plan.Expr{expr})
			for _, executor := range executors {
				if executor != nil {
					t.Cleanup(executor.Free)
				}
			}
			require.NoError(t, err)
			require.Len(t, executors, 1)
			executor := executors[0]
			input := batch.New(nil)
			input.SetRowCount(4)
			checkTwelve := func(result *vector.Vector) {
				t.Helper()
				require.Equal(t, types.T_float64.ToType(), *result.GetType())
				require.Equal(t, 4, result.Length())
				for row := 0; row < 4; row++ {
					require.False(t, result.IsNull(uint64(row)))
					require.Equal(t, float64(12), vector.GetFixedAtNoTypeCheck[float64](result, row))
				}
			}

			result, err := executor.Eval(proc, []*batch.Batch{input}, []bool{false, false, false, false})
			require.NoError(t, err)
			require.Equal(t, types.T_float64.ToType(), *result.GetType())
			require.Equal(t, 4, result.Length())
			require.Zero(t, warnings.warningCount)
			for row := 0; row < 4; row++ {
				require.True(t, result.IsNull(uint64(row)))
			}

			result, err = executor.Eval(proc, []*batch.Batch{input}, []bool{true, false, true, false})
			require.NoError(t, err)
			require.True(t, result.IsConst())
			require.Equal(t, 1, warnings.warningCount)
			checkTwelve(result)
			result, err = executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, 1, warnings.warningCount)
			checkTwelve(result)

			executor.ResetForNextQuery()
			result, err = executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, 2, warnings.warningCount)
			checkTwelve(result)

			defaultExecutor, err := NewExpressionExecutor(proc, expr)
			if defaultExecutor != nil {
				t.Cleanup(defaultExecutor.Free)
			}
			require.NoError(t, err)
			result, err = defaultExecutor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, 6, warnings.warningCount, "ordinary expressions retain four row diagnostics")
			checkTwelve(result)

		})

		selectedRows := []bool{true, false, true, false}
		tests := []struct {
			name     string
			first    string
			kind     vector.PrepareParamKind
			last     string
			lastKind vector.PrepareParamKind
		}{
			{name: "integer then ordinary text", first: "7", kind: vector.PrepareParamInteger, last: "12abc", lastKind: vector.PrepareParamNone},
			{name: "ordinary text then integer", first: "12abc", kind: vector.PrepareParamNone, last: "7", lastKind: vector.PrepareParamInteger},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {

				checkExpressionStorageAfterCleanup(t, proc)
				sessionBefore := proc.Session
				t.Cleanup(func() { proc.Session = sessionBefore })
				session := &preparedCastWarningSession{}
				proc.Session = session
				executor := makeNumericExecutor(t, types.T_float64.ToType())

				input := batch.New(nil)
				t.Cleanup(func() { input.Clean(proc.Mp()) })
				input.SetRowCount(len(selectedRows))
				eval := func(value string, kind vector.PrepareParamKind) *vector.Vector {
					t.Helper()
					result := evalTestTextParameter(t, proc, executor, value, false, kind, []*batch.Batch{input}, selectedRows)
					require.Equal(t, len(selectedRows), result.Length())
					require.Equal(t, types.T_float64.ToType(), *result.GetType())
					expected := float64(12)
					if kind == vector.PrepareParamInteger {
						expected = 7
					}
					for row, selected := range selectedRows {
						if kind != vector.PrepareParamInteger && !selected {
							require.True(t, result.IsNull(uint64(row)))
							continue
						}
						require.False(t, result.IsNull(uint64(row)))
						require.Equal(t, expected, vector.GetFixedAtNoTypeCheck[float64](result, row))
					}
					return result
				}

				result := eval(test.first, test.kind)
				if test.kind == vector.PrepareParamInteger {
					require.True(t, executor.folded.canFold)
					require.True(t, result.IsConst())
					require.Zero(t, session.warningCount)
				} else {
					require.False(t, executor.folded.canFold)
					require.False(t, result.IsConst())
					require.Equal(t, 2, session.warningCount)
				}

				executor.ResetForNextQuery()
				result = eval(test.last, test.lastKind)
				if test.lastKind == vector.PrepareParamInteger {
					require.True(t, executor.folded.canFold)
					require.True(t, result.IsConst())
				} else {
					require.False(t, executor.folded.canFold)
					require.False(t, result.IsConst())
				}
				require.Equal(t, 2, session.warningCount)
			})
		}

	})
}

type preparedCastWarningSession struct {
	warningCount int
}

func (*preparedCastWarningSession) GetTempTable(string, string) (string, bool) { return "", false }
func (*preparedCastWarningSession) AddTempTable(string, string, string)        {}
func (*preparedCastWarningSession) RemoveTempTable(string, string)             {}
func (*preparedCastWarningSession) RemoveTempTableByRealName(string)           {}
func (*preparedCastWarningSession) GetSqlModeNoAutoValueOnZero() (bool, bool)  { return false, false }
func (s *preparedCastWarningSession) AppendWarningDiagnostic(uint16, string) {
	s.warningCount++
}

func TestJsonOrderingWithTextPrepareParamExact(t *testing.T) {
	tests := []struct {
		name       string
		op         string
		jsonOnLeft bool
		jsonValue  string
		paramValue string
		paramNull  bool
		want       bool
		wantNull   bool
		wantErr    bool
	}{
		{name: "adjacent integers json left", op: "<", jsonOnLeft: true, jsonValue: "9007199254740992", paramValue: "9007199254740993", want: true},
		{name: "adjacent integers json right", op: ">", jsonOnLeft: false, jsonValue: "9007199254740992", paramValue: "9007199254740993", want: true},
		{name: "maximum uint64", op: "<", jsonOnLeft: true, jsonValue: "18446744073709551614", paramValue: "18446744073709551615", want: true},
		{name: "precise decimals", op: "<", jsonOnLeft: true, jsonValue: "0.123456789123456788", paramValue: "0.123456789123456789", want: true},
		{name: "null parameter", op: "<", jsonOnLeft: true, jsonValue: "1", paramNull: true, wantNull: true},
		{name: "invalid string parameter", op: "<", jsonOnLeft: true, jsonValue: "1", paramValue: "not-json", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			params := vector.NewVec(types.T_text.ToType())
			require.NoError(t, vector.AppendBytes(params, []byte(test.paramValue), test.paramNull, proc.Mp()))
			proc.SetPrepareParams(params)

			jsonType := types.T_json.ToType()
			textType := types.T_text.ToType()
			normalizeFn, err := function.GetFunctionByName(proc.Ctx, function.JsonOrderingParamFunctionName, []types.Type{textType})
			require.NoError(t, err)
			paramExpr := &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_json)},
				Expr: &plan.Expr_F{F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: function.JsonOrderingParamFunctionName, Obj: normalizeFn.GetEncodedOverloadID()},
					Args: []*plan.Expr{
						{Typ: plan.Type{Id: int32(types.T_text)}, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}},
					},
				}},
			}
			jsonExpr := &plan.Expr{
				Typ:  plan.Type{Id: int32(types.T_json)},
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}},
			}
			args := []*plan.Expr{jsonExpr, paramExpr}
			if !test.jsonOnLeft {
				args[0], args[1] = args[1], args[0]
			}
			compareFn, err := function.GetFunctionByName(proc.Ctx, test.op, []types.Type{jsonType, jsonType})
			require.NoError(t, err)
			expr := &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_bool)},
				Expr: &plan.Expr_F{F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: test.op, Obj: compareFn.GetEncodedOverloadID()},
					Args: args,
				}},
			}

			json, err := types.ParseStringToByteJson(test.jsonValue)
			require.NoError(t, err)
			encoded, err := types.EncodeJson(json)
			require.NoError(t, err)
			jsonVec := vector.NewVec(jsonType)
			require.NoError(t, vector.AppendBytes(jsonVec, encoded, false, proc.Mp()))
			input := batch.NewWithSize(1)
			input.Vecs[0] = jsonVec
			input.SetRowCount(1)

			executor, err := NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			if test.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.wantNull, result.GetNulls().Contains(0))
				if !test.wantNull {
					require.Equal(t, test.want, vector.MustFixedColWithTypeCheck[bool](result)[0])
				}
			}

			executor.Free()
			input.Clean(proc.Mp())
			proc.SetPrepareParams(nil)
			params.Free(proc.Mp())
			proc.Free()
		})
	}
}

func TestModifyResultOwnerToOuter(t *testing.T) {
	// we cannot modify the column expression's memory owner.
	// because its owner is never own to executor.
	columnExecutor := &ColumnExpressionExecutor{}
	require.False(t, modifyResultOwnerToOuter(columnExecutor))
}

// some util code copied from package `plan`.
func makePlan2Int64ConstExprWithType(v int64) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2Int64ConstExpr(v),
		Typ: plan.Type{
			Id:          int32(types.T_int64),
			NotNullable: true,
		},
	}
}

func makePlan2Int64ConstExpr(v int64) *plan.Expr_Lit {
	return &plan.Expr_Lit{Lit: &plan.Literal{
		Isnull: false,
		Value: &plan.Literal_I64Val{
			I64Val: v,
		},
	}}
}

func makePlan2BoolConstExprWithType(b bool) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2BoolConstExpr(b),
		Typ: plan.Type{
			Id:          int32(types.T_bool),
			NotNullable: true,
		},
	}
}

func makePlan2BoolConstExpr(b bool) *plan.Expr_Lit {
	return &plan.Expr_Lit{Lit: &plan.Literal{
		Isnull: false,
		Value: &plan.Literal_Bval{
			Bval: b,
		},
	}}
}

func TestOneShotExpressionOwnership(t *testing.T) {
	for _, writable := range []bool{false, true} {
		for _, tc := range []struct {
			name, function string
			args           []int64
			want           int64
			panics, fails  bool
		}{
			{"round panic", "round", []int64{5000000000000000000, -19}, 0, true, false},
			{"returned error", "abs", []int64{math.MinInt64}, 0, false, true},
			{"function result", "round", []int64{-11, -1}, -10, false, false},
			{"literal result", "", []int64{math.MinInt64}, math.MinInt64, false, false},
			{"column result", "column", []int64{42}, 42, false, false},
		} {
			t.Run(fmt.Sprintf("writable=%t/%s", writable, tc.name), func(t *testing.T) {
				proc := testutil.NewProcess(t)
				t.Cleanup(func() {
					defer proc.Free()
					require.Equal(t, [2]int64{}, [2]int64{proc.Mp().CurrNB(), proc.Mp().OnHeapCurrNB()}, "native and on-heap ownership")
				})
				args := make([]*plan.Expr, len(tc.args))
				argTypes := make([]types.Type, len(tc.args))
				for i, value := range tc.args {
					argTypes[i] = types.T_int64.ToType()
					args[i] = &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: value}}}}
				}
				expression := args[0]
				data := fixedOnlyOneRowBatch
				if tc.function == "column" {
					input := batch.NewWithSize(1)
					input.Vecs[0] = testutil.MakeInt64Vector(tc.args, nil, proc.Mp())
					input.SetRowCount(1)
					t.Cleanup(func() { input.Clean(proc.Mp()) })
					data = []*batch.Batch{input}
					expression = &plan.Expr{Typ: expression.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
				} else if tc.function != "" {
					fn, err := function.GetFunctionByName(proc.Ctx, tc.function, argTypes)
					require.NoError(t, err)
					expression = &plan.Expr{Typ: plan.Type{Id: int32(fn.GetReturnType().Oid)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: fn.GetEncodedOverloadID(), ObjName: tc.function}, Args: args}}}
				}
				var result *vector.Vector
				var free func()
				var err error
				var escaped any
				func() {
					defer func() { escaped = recover() }()
					if writable {
						result, err = GetWritableResultFromExpression(proc, expression, data)
						if err == nil {
							free = func() { result.Free(proc.Mp()) }
						}
					} else {
						result, free, err = GetReadonlyResultFromExpression(proc, expression, data)
					}
				}()
				if free != nil {
					t.Cleanup(free)
				}
				if tc.panics {
					panicErr, ok := escaped.(error)
					require.True(t, ok, "evaluation must preserve the kernel panic")
					require.True(t, moerr.IsMoErrCode(panicErr, moerr.ErrOutOfRange))
				} else {
					require.Nil(t, escaped)
					if tc.fails {
						require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange))
					} else {
						require.NoError(t, err)
						require.NotNil(t, free)
						require.Equal(t, tc.want, vector.MustFixedColNoTypeCheck[int64](result)[0])
						if tc.function == "column" {
							if writable {
								require.NotSame(t, data[0].Vecs[0], result)
							} else {
								require.Same(t, data[0].Vecs[0], result)
							}
						}
					}
				}
				if tc.panics || tc.fails {
					require.Equal(t, [2]int64{}, [2]int64{proc.Mp().CurrNB(), proc.Mp().OnHeapCurrNB()})
				}
			})
		}
	}
}

func TestGetExprZoneMapConstantFold(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	literal := func(value int64) *plan.Expr {
		return &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: makePlan2Int64ConstExpr(value)}
	}
	zms := make([]index.ZM, 2)
	vecs := make([]*vector.Vector, 2)
	cleanScratch := func() {
		for i, vec := range vecs {
			if vec != nil {
				vec.Free(proc.Mp())
				vecs[i] = nil
			}
		}
	}
	defer cleanScratch()
	shared := literal(42)
	for _, tc := range []struct {
		name     string
		expr     *plan.Expr
		want     int64
		overflow bool
	}{
		{"literal", bindTestFunction(t, proc, "abs", literal(-42)), 42, false},
		{"nested", bindTestFunction(t, proc, "abs", bindTestFunction(t, proc, "round", literal(-11), literal(-1))), 10, false},
		{"nested overflow", bindTestFunction(t, proc, "abs", bindTestFunction(t, proc, "round", literal(5000000000000000000), literal(-19))), 0, true},
		{"later argument failure", bindTestFunction(t, proc, "greatest", literal(42), bindTestFunction(t, proc, "round", literal(5000000000000000000), literal(-19))), 0, true},
		{"later unavailable Fold", bindTestFunction(t, proc, "round", literal(-11), &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{IsConst: true}}}), 0, true},
		{"repeated argument", bindTestFunction(t, proc, "greatest", shared, shared), 42, false},
		{"nested reuse", bindTestFunction(t, proc, "abs", bindTestFunction(t, proc, "round", literal(-11), literal(-1))), 10, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expression := tc.expr
			expression.AuxId = 1
			var zm index.ZM
			var escaped any
			func() {
				defer func() { escaped = recover() }()
				zm = GetExprZoneMap(proc.Ctx, proc, expression, nil, nil, zms, vecs)
			}()
			cleanScratch()
			require.Nil(t, escaped, "speculative argument failure must return unknown")
			require.Equal(t, !tc.overflow, zm.IsInited())
			if !tc.overflow {
				require.Equal(t, tc.want, types.DecodeInt64(zm.GetMinBuf()))
				require.Equal(t, tc.want, types.DecodeInt64(zm.GetMaxBuf()))
			}
			require.Equal(t, [2]int64{}, [2]int64{proc.Mp().CurrNB(), proc.Mp().OnHeapCurrNB()}, "argument tree and metadata result must be released")
		})
	}
}

func TestLastDayPersistedVarcharABI(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	input := batch.NewWithSize(1)
	defer input.Clean(proc.Mp())
	input.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendStringList(input.Vecs[0], []string{"2024-02-10", "0000-00-00", "bad"}, nil, proc.Mp()))
	input.SetRowCount(3)
	for _, oid := range []int64{0, 1} {
		func() {
			expr := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{Obj: int64(function.LAST_DAY)<<32 | oid, ObjName: "last_day"},
				Args: []*plan.Expr{{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}},
			}}}
			encoded, err := expr.Marshal()
			require.NoError(t, err)
			var restored plan.Expr
			require.NoError(t, restored.Unmarshal(encoded))
			executor, err := NewExpressionExecutor(proc, &restored)
			require.NoError(t, err)
			defer executor.Free()
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, types.T_varchar, result.GetType().Oid)
			require.Equal(t, "2024-02-29", result.GetStringAt(0))
			require.True(t, result.IsNull(1))
			require.True(t, result.IsNull(2))
		}()
	}
}

func TestDecimalCastLifecycle(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	initialSession, initialMode, initialRunning := proc.Session, proc.Base.SessionInfo.SqlMode, proc.GetBaseProcessRunningStatus()
	initialParams := proc.DetachPrepareParams()
	proc.RestorePrepareParams(initialParams)
	t.Cleanup(func() {
		actualParams := proc.DetachPrepareParams()
		proc.RestorePrepareParams(actualParams)
		assert.Equal(t, initialParams, actualParams)
		assert.Equal(t, initialSession, proc.Session)
		assert.Equal(t, initialMode, proc.Base.SessionInfo.SqlMode)
		assert.Equal(t, initialRunning, proc.GetBaseProcessRunningStatus())
	})

	t.Run("narrowing error recovery", func(t *testing.T) {
		checkExpressionStorageAfterCleanup(t, proc)
		sourceType := types.New(types.T_decimal128, 38, 30)
		targetType := types.New(types.T_decimal64, 3, 2)
		input := batch.NewWithSize(1)
		t.Cleanup(func() { input.Clean(proc.Mp()) })
		input.Vecs[0] = vector.NewVec(sourceType)
		values := make([]types.Decimal128, 3)
		for i, literal := range []string{"1.499999999999999999999999999999", "9.995000000000000000000000000000"} {
			var err error
			values[i], err = types.ParseDecimal128(literal, 38, 30)
			require.NoError(t, err)
		}
		require.NoError(t, vector.AppendFixedList(input.Vecs[0], values, []bool{false, false, true}, proc.Mp()))
		input.SetRowCount(3)
		expr := bindTestFunction(t, proc, "cast",
			&plan.Expr{Typ: plan.Type{Id: int32(sourceType.Oid), Width: sourceType.Width, Scale: sourceType.Scale}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}}},
			&plan.Expr{Typ: plan.Type{Id: int32(targetType.Oid), Width: targetType.Width, Scale: targetType.Scale}, Expr: &plan.Expr_T{T: &plan.TargetType{}}})
		executor, err := NewExpressionExecutor(proc, expr)
		if executor != nil {
			t.Cleanup(executor.Free)
		}
		require.NoError(t, err)
		result, err := executor.Eval(proc, []*batch.Batch{input}, []bool{true, false, true})
		require.NoError(t, err)
		require.Equal(t, targetType, *result.GetType())
		require.Equal(t, 3, result.Length())
		require.Equal(t, types.Decimal64(150), vector.GetFixedAtNoTypeCheck[types.Decimal64](result, 0))
		require.False(t, result.IsNull(0))
		require.True(t, result.IsNull(1))
		require.True(t, result.IsNull(2))
		_, err = executor.Eval(proc, []*batch.Batch{input}, nil)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "%v", err)
		input.SetRowCount(1)
		result, err = executor.Eval(proc, []*batch.Batch{input}, nil)
		require.NoError(t, err)
		require.Equal(t, targetType, *result.GetType())
		require.Equal(t, 1, result.Length())
		require.False(t, result.IsNull(0))
		require.Equal(t, types.Decimal64(150), vector.GetFixedAtNoTypeCheck[types.Decimal64](result, 0))
		require.Equal(t, sourceType, *input.Vecs[0].GetType())
	})

	for _, nullable := range []bool{false, true} {
		t.Run(fmt.Sprintf("nullable=%v", nullable), func(t *testing.T) {
			checkExpressionStorageAfterCleanup(t, proc)
			sourceType := types.New(types.T_decimal64, 18, 2)
			targetType := types.New(types.T_decimal128, 38, 2)
			input := batch.NewWithSize(1)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			input.Vecs[0] = vector.NewVec(sourceType)
			require.NoError(t, vector.AppendFixedList(input.Vecs[0], []types.Decimal64{149, 200}, []bool{false, nullable}, proc.Mp()))
			expr := bindTestFunction(t, proc, "cast",
				&plan.Expr{Typ: plan.Type{Id: int32(sourceType.Oid), Width: sourceType.Width, Scale: sourceType.Scale}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}}},
				&plan.Expr{Typ: plan.Type{Id: int32(targetType.Oid), Width: targetType.Width, Scale: targetType.Scale}, Expr: &plan.Expr_T{T: &plan.TargetType{}}})
			executor, err := NewExpressionExecutor(proc, expr)
			if executor != nil {
				t.Cleanup(executor.Free)
			}
			require.NoError(t, err)
			for i, size := range []int{0, 2, 0, 2} {
				if i == 3 {
					input.Vecs[0].GetNulls().Reset()
					require.NoError(t, vector.SetFixedAtWithTypeCheck(input.Vecs[0], 1, types.Decimal64(200)))
				}
				input.SetRowCount(size)
				result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
				require.NoError(t, err)
				require.Equal(t, size, result.Length())
				require.Equal(t, targetType, *result.GetType())
				require.Equal(t, sourceType, *input.Vecs[0].GetType())
				if size == 0 {
					require.True(t, result.GetNulls().IsEmpty())
					continue
				}
				require.Equal(t, types.Decimal128{B0_63: 149}, vector.GetFixedAtNoTypeCheck[types.Decimal128](result, 0))
				require.False(t, result.IsNull(0))
				require.Equal(t, nullable && i == 1, result.IsNull(1))
				if !result.IsNull(1) {
					require.Equal(t, types.Decimal128{B0_63: 200}, vector.GetFixedAtNoTypeCheck[types.Decimal128](result, 1))
				}
			}
		})
	}

}

func TestRegisteredXorSelectionAndFolding(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() {
		proc.GetFileService().Close(proc.Ctx)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
		require.Zero(t, proc.Mp().OnHeapCurrNB())
	})
	input := batch.NewWithSize(2)
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	input.Vecs[0] = testutil.NewVectorWithNulls(3, types.T_bool.ToType(), proc.Mp(), false, []bool{false, false, true}, []bool{false, false, false})
	input.Vecs[1] = testutil.NewVector(3, types.T_bool.ToType(), proc.Mp(), false, []bool{true, false, true})
	input.SetRowCount(3)
	resolved, err := function.GetFunctionByName(proc.Ctx, "xor", []types.Type{types.T_bool.ToType(), types.T_bool.ToType()})
	require.NoError(t, err)
	require.Equal(t, function.EncodeOverloadID(function.XOR, 0), resolved.GetEncodedOverloadID())
	require.Equal(t, types.T_bool.ToType(), resolved.GetReturnType())
	left := &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	right := &plan.Expr{Typ: left.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}}}
	expr := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: resolved.GetEncodedOverloadID(), ObjName: "xor"},
			Args: []*plan.Expr{left, right},
		}},
	}
	t.Run("selected rows and reuse", func(t *testing.T) {
		executor, err := NewExpressionExecutor(proc, expr)
		require.NoError(t, err)
		t.Cleanup(executor.Free)
		for _, tc := range []struct {
			selection []bool
			nulls     []bool
		}{
			{[]bool{false, true, true}, []bool{true, false, true}},
			{nil, []bool{false, false, true}},
			{[]bool{false, false, false}, []bool{true, true, true}},
			{nil, []bool{false, false, true}},
		} {
			result, err := executor.Eval(proc, []*batch.Batch{input}, tc.selection)
			require.NoError(t, err)
			require.Equal(t, types.T_bool.ToType(), *result.GetType())
			require.Equal(t, 3, result.Length())
			for row, wantNull := range tc.nulls {
				require.Equal(t, wantNull, result.IsNull(uint64(row)))
				if !wantNull {
					require.Equal(t, row == 0, vector.GetFixedAtWithTypeCheck[bool](result, row))
				}
			}
		}
	})
	t.Run("folded constants", func(t *testing.T) {
		folded := *expr
		folded.Expr = &plan.Expr_F{F: &plan.Function{
			Func: expr.GetF().Func,
			Args: []*plan.Expr{makePlan2BoolConstExprWithType(false), makePlan2BoolConstExprWithType(true)},
		}}
		executor, err := NewExpressionExecutor(proc, &folded)
		require.NoError(t, err)
		t.Cleanup(executor.Free)
		for _, selection := range [][]bool{nil, {false, true, false}} {
			result, err := executor.Eval(proc, []*batch.Batch{input}, selection)
			require.NoError(t, err)
			require.Equal(t, types.T_bool.ToType(), *result.GetType())
			require.Equal(t, 3, result.Length())
			require.True(t, result.IsConst())
			require.False(t, result.IsNull(1))
			require.True(t, vector.GetFixedAtWithTypeCheck[bool](result, 1))
		}
	})
}
