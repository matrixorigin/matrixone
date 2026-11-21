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
func bindTestCast(t testing.TB, proc *process.Process, overload int32, source *plan.Expr, target types.Type) *plan.Expr {
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

func TestSemanticParameterVectorsRestoresUnicodeMetadataWithoutMutatingInput(t *testing.T) {
	mp := mpool.MustNewZero()
	physical := vector.NewVec(types.T_varchar.ToType())
	defer physical.Free(mp)
	require.NoError(t, vector.AppendBytes(physical, []byte("A"), false, mp))

	semantic := types.NewWithCharset(types.T_varchar, 64, 0, types.CharsetUTF8MB4UnicodeCI)
	expr := &FunctionExpressionExecutor{parameterTypes: []types.Type{semantic}}
	view := expr.semanticParameterVectors([]*vector.Vector{physical})[0]
	require.Equal(t, semantic, *view.GetType())
	require.Equal(t, types.T_varchar.ToType(), *physical.GetType())
	require.Equal(t, []byte("A"), view.GetBytesAt(0))
}

// evalTestTextParameter scopes transport ownership and restores the exact prior state.
func evalTestTextParameter(t *testing.T, proc *process.Process, executor ExpressionExecutor, value string, isNull bool, batches []*batch.Batch, mask []bool) *vector.Vector {
	t.Helper()
	return evalTestTypedParameter(t, proc, executor, value, isNull, nil, batches, mask)
}

// evalTestTypedParameter installs parameter 0 as its TEXT spelling plus, when
// typed is non-nil, its typed value (ownership of typed moves to proc). The
// prior parameter state is restored before returning.
func evalTestTypedParameter(t *testing.T, proc *process.Process, executor ExpressionExecutor, value string, isNull bool, typed *vector.Vector, batches []*batch.Batch, mask []bool) *vector.Vector {
	t.Helper()
	params := vector.NewVec(types.T_text.ToType())
	defer params.Free(proc.Mp())
	previous := proc.DetachPrepareParams()
	defer proc.RestorePrepareParams(previous)
	require.NoError(t, vector.AppendBytes(params, []byte(value), isNull, proc.Mp()))
	proc.SetPrepareParams(params)
	if typed != nil {
		typedBatch := batch.NewWithSize(1)
		typedBatch.Vecs[0] = typed
		typedBatch.SetRowCount(1)
		proc.SetTypedPrepareParams(typedBatch)
	}
	result, err := executor.Eval(proc, batches, mask)
	require.NoError(t, err)
	return result
}

// evalTestFloatParameter installs parameter 0 as the DOUBLE value 3.5.
func evalTestFloatParameter(t *testing.T, proc *process.Process, executor ExpressionExecutor, batches []*batch.Batch, mask []bool) *vector.Vector {
	t.Helper()
	typed, err := vector.NewConstFixed(types.T_float64.ToType(), 3.5, 1, proc.Mp())
	require.NoError(t, err)
	return evalTestTypedParameter(t, proc, executor, "3.5", false, typed, batches, mask)
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

func TestLiteralMaterialization(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	checkExpressionStorageAfterCleanup(t, proc)
	t.Run("forms", func(t *testing.T) {
		tests := []struct {
			name          string
			typ           types.Type
			form          plan.StringLiteralForm
			wantEffective types.StringDomain
			wantVarchar   bool
		}{
			{name: "text on text", typ: types.T_varchar.ToType(), form: plan.StringLiteralForm_STRING_LITERAL_TEXT, wantEffective: types.StringDomainText},
			{name: "binary on binary", typ: types.T_varbinary.ToType(), form: plan.StringLiteralForm_STRING_LITERAL_BINARY_INTRODUCER, wantEffective: types.StringDomainBinary},
			{name: "raw hex", typ: types.NewWithCharset(types.T_varchar, 0, 0, types.CharsetBinary), form: plan.StringLiteralForm_STRING_LITERAL_HEX, wantEffective: types.StringDomainBinary, wantVarchar: true},
			{name: "raw bit", typ: types.NewWithCharset(types.T_varchar, 0, 0, types.CharsetBinary), form: plan.StringLiteralForm_STRING_LITERAL_BIT, wantEffective: types.StringDomainBinary, wantVarchar: true},
			{name: "char text uses varchar container", typ: types.NewWithCharset(types.T_char, 0, 0, types.CharsetUTF8), form: plan.StringLiteralForm_STRING_LITERAL_TEXT, wantEffective: types.StringDomainText, wantVarchar: true},
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
				// The literal form no longer stamps a runtime domain; the
				// static type alone carries the string semantics.
				require.Equal(t, test.wantEffective, types.StaticStringDomain(*vec.GetType()))
				require.Equal(t, "selected", vec.GetStringAt(0))
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
		checkExpressionStorageAfterCleanup(t, proc)
		data := func() []byte {
			vec := vector.NewVec(types.T_varchar.ToType())
			defer vec.Free(proc.Mp())
			require.NoError(t, vector.AppendStringList(vec, []string{"a", "b"}, nil, proc.Mp()))
			data, err := vec.MarshalBinary()
			require.NoError(t, err)
			return data
		}()
		t.Run("direct", func(t *testing.T) {
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
				require.Equal(t, want, vec.GetStringAt(row))
			}
		})
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

	t.Run("selected value", func(t *testing.T) {
		tests := []struct {
			name       string
			fid        int32
			sourceType types.Type
			resultType types.Type
		}{
			{name: "if text to binary common-domain", fid: function.IFF, sourceType: types.T_varchar.ToType(), resultType: types.T_varbinary.ToType()},
			{name: "case text to binary common-domain", fid: function.CASE, sourceType: types.T_varchar.ToType(), resultType: types.T_varbinary.ToType()},
			{name: "coalesce text to binary selected-value", fid: function.COALESCE, sourceType: types.T_varchar.ToType(), resultType: types.T_varbinary.ToType()},
			{name: "if binary to text common-domain", fid: function.IFF, sourceType: types.T_varbinary.ToType(), resultType: types.T_varchar.ToType()},
			{name: "case binary to text common-domain", fid: function.CASE, sourceType: types.T_varbinary.ToType(), resultType: types.T_varchar.ToType()},
			{name: "coalesce binary to text selected-value", fid: function.COALESCE, sourceType: types.T_varbinary.ToType(), resultType: types.T_varchar.ToType()},
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
				expr.evalFn, expr.resetFn, expr.freeFn, _ = overload.GetExecuteMethod()
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
				fallback, err := vector.NewConstBytes(test.resultType, []byte("fallback"), 1, proc.Mp())
				require.NoError(t, err)
				expr.SetParameter(offset+1, NewFixedVectorExpressionExecutor(proc.Mp(), false, fallback))

				result, err := expr.Eval(proc, nil, nil)
				require.NoError(t, err)
				require.True(t, expr.folded.canFold)
				require.Equal(t, "selected", result.GetStringAt(0))

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
				}
			})
		}
	})
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
			binary      bool
		}{
			{"binary", "AB\x00\x00", true},
			{"integer", "5", false},
			{"float", "5.5", false},
			{"decimal", "5.9", false},
			{"boolean", "true", false},
			{"text", "text", false},
		}
		params := vector.NewVec(types.T_text.ToType())
		t.Cleanup(func() { params.Free(proc.Mp()) })
		state := proc.DetachPrepareParams()
		t.Cleanup(func() { proc.RestorePrepareParams(state) })
		binary := make([]bool, len(tests))
		for row, test := range tests {
			require.NoError(t, vector.AppendBytes(params, []byte(test.value), false, proc.Mp()))
			binary[row] = test.binary
		}
		proc.SetPrepareParamsWithIsBin(params, binary)
		for row, test := range tests {
			func() {
				executor := NewParamExpressionExecutor(proc.Mp(), row, types.T_text.ToType())
				defer executor.Free()
				result, err := executor.Eval(proc, nil, nil)
				require.NoError(t, err, test.name)
				require.Equal(t, test.value, result.GetStringAt(0), test.name)
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
				// Without a typed value the DOUBLE parameter is cast from its
				// TEXT spelling, which may materialize a flat vector.
				got := evalTestTextParameter(t, proc, executor, "3.5", false, []*batch.Batch{bat}, mask)
				checkScalar(t, got, 1, tc.want)
				require.False(t, executor.(*FunctionExpressionExecutor).folded.canFold)
				if mask == nil || mask[0] {
					require.False(t, executor.(*FunctionExpressionExecutor).parameterResults[0].IsConst())
				}
				// A typed value stays physically constant.
				executor.ResetForNextQuery()
				got = evalTestFloatParameter(t, proc, executor, []*batch.Batch{bat}, mask)
				checkScalar(t, got, 1, tc.want)
				require.False(t, executor.(*FunctionExpressionExecutor).folded.canFold)
				executor.ResetForNextQuery()
			}
			for _, mask := range [][]bool{nil, {false, true, false}} {
				executor.ResetForNextQuery()
				got := evalTestTextParameter(t, proc, executor, "", true, []*batch.Batch{bat}, mask)
				require.Equal(t, types.Type{Oid: types.T_int64, Size: 8}, *got.GetType())
				require.True(t, got.IsConstNull())
				require.Equal(t, 3, got.Length())
				executor.ResetForNextQuery()
				got = evalTestFloatParameter(t, proc, executor, []*batch.Batch{bat}, mask)
				checkScalar(t, got, 1, tc.want)
			}
			bat.SetRowCount(0)
			for _, mask := range [][]bool{nil, {}} {
				got := evalTestFloatParameter(t, proc, executor, []*batch.Batch{bat}, mask)
				require.Equal(t, 0, got.Length())
				require.False(t, got.IsConst())
			}
			bat.SetRowCount(3)
			got := evalTestFloatParameter(t, proc, executor, []*batch.Batch{bat}, nil)
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
		got := evalTestFloatParameter(t, proc, executor, []*batch.Batch{bat}, nil)
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
		got := evalTestFloatParameter(t, proc, executor, []*batch.Batch{bat}, nil)
		require.Equal(t, types.Type{Oid: types.T_float64, Size: 8}, *got.GetType())
		require.False(t, got.IsConst())
		require.Equal(t, 2, got.Length())
		for row, want := range []float64{1.2345, -2.3457} {
			require.False(t, got.IsNull(uint64(row)))
			require.Equal(t, want, vector.GetFixedAtNoTypeCheck[float64](got, row))
		}
	})

}

func TestFlowControlPreparedParamOnPartialSelection(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	checkExpressionStorageAfterCleanup(t, proc)
	params := vector.NewVec(types.T_text.ToType())
	defer params.Free(proc.Mp())
	require.NoError(t, vector.AppendBytes(params, []byte("5.5"), false, proc.Mp()))
	proc.SetPrepareParams(params)
	defer proc.SetPrepareParams(nil)

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
			require.Equal(t, "5.5", result.GetStringAt(0))
			require.True(t, result.IsNull(1))

			result, err = executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, "5.5", result.GetStringAt(0))
			require.Equal(t, "5.5", result.GetStringAt(1))

			result, err = executor.Eval(proc, []*batch.Batch{input}, []bool{false, true})
			require.NoError(t, err)
			require.Equal(t, "5.5", result.GetStringAt(1))
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
			require.Equal(t, "5.5", result.GetStringAt(0))
			require.Equal(t, "fallback", result.GetStringAt(1))
		})
	}

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
	require.True(t, maskedResult.IsNull(0))
	require.Equal(t, "5.5", maskedResult.GetStringAt(1))
	maskedResult, err = maskedBranchExecutor.Eval(
		proc, []*batch.Batch{maskedBranchInput}, []bool{true, false})
	require.NoError(t, err)
	require.Equal(t, 2, maskedResult.Length())

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
	require.Equal(t, []string{"5.5", "ordinary"}, []string{result.GetStringAt(0), result.GetStringAt(1)})
}

func TestVariableExpressionLifecycle(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	checkExpressionStorageAfterCleanup(t, proc)
	checkResolvers := func(t *testing.T) {
		t.Helper()
		assert.Nil(t, proc.GetResolveVariableFunc())
		assert.Nil(t, proc.GetResolveVariableIsBinFunc())
		assert.Nil(t, proc.GetResolveVariableStringDomainFunc())
	}
	t.Cleanup(func() { checkResolvers(t) })
	runLeaf := func(name string, scenario func(t *testing.T)) {
		t.Run(name, func(t *testing.T) {
			checkExpressionStorageAfterCleanup(t, proc)
			valueBefore, binBefore := proc.GetResolveVariableFunc(), proc.GetResolveVariableIsBinFunc()
			domainBefore := proc.GetResolveVariableStringDomainFunc()
			t.Cleanup(func() {
				proc.SetResolveVariableFunc(valueBefore)
				proc.SetResolveVariableIsBinFunc(binBefore)
				proc.SetResolveVariableStringDomainFunc(domainBefore)
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
				require.Equal(t, tc.binary, types.StaticStringDomain(*vec.GetType()) == types.StringDomainBinary)
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
			require.Equal(t, tc.binary, types.StaticStringDomain(*vec.GetType()) == types.StringDomainBinary)
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
		proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) {
			return value, nil
		})
		proc.SetResolveVariableIsBinFunc(func(string, bool, bool) (bool, error) {
			return isBin, nil
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
		require.Equal(t, "AB\x00\x00", vec.GetStringAt(0))

		value, isBin = "5.0", false
		vec, err = executor.Eval(proc, nil, nil)
		require.NoError(t, err)
		require.False(t, vec.GetIsBin())
		require.Equal(t, "5.0", vec.GetStringAt(0))

		value = "text"
		vec, err = executor.Eval(proc, nil, nil)
		require.NoError(t, err)
		require.False(t, vec.GetIsBin())
		require.Equal(t, "text", vec.GetStringAt(0))

		value, isBin = "CD\x00\x00", true
		vec, err = executor.Eval(proc, nil, nil)
		require.NoError(t, err)
		require.True(t, vec.GetIsBin())
		require.Equal(t, "CD\x00\x00", vec.GetStringAt(0))

		// Resolver errors must stop before later metadata callbacks or publication.
		resolverErr := moerr.NewInternalErrorNoCtx("variable metadata resolver failed")
		calls := [2]int{}
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
		expectedText := types.Type{Oid: types.T_text, Charset: types.CharsetLegacy, Size: 24}
		checkRecovered := func(result *vector.Vector, rows int) {
			t.Helper()
			require.Equal(t, expectedText, *result.GetType())
			require.Equal(t, rows, result.Length())
			require.True(t, result.IsConst())
			for row := 0; row < rows; row++ {
				require.False(t, result.IsNull(uint64(row)))
				require.Equal(t, "7", result.GetStringAt(row))
			}
			require.True(t, result.GetIsBin())
		}
		for stage, expectedCalls := range [][2]int{{1, 0}, {1, 1}} {
			failStage, calls = stage, [2]int{}
			result, err := executor.Eval(proc, nil, nil)
			require.ErrorIs(t, err, resolverErr)
			require.Nil(t, result)
			require.Equal(t, expectedCalls, calls)
			failStage, calls = -1, [2]int{}
			func() {
				owned, err := executor.EvalWithoutResultReusing(proc, nil, nil)
				if owned != nil {
					defer owned.Free(proc.Mp())
				}
				require.NoError(t, err)
				require.Equal(t, [2]int{1, 1}, calls)
				checkRecovered(owned, 1)
			}()
			require.Zero(t, proc.Mp().CurrNB())
			bytes, objects := proc.Mp().OnHeapOutstanding()
			require.Zero(t, bytes)
			require.Zero(t, objects)
		}
		failStage, calls = -2, [2]int{}
		input := batch.New(nil)
		input.SetRowCount(3)
		masked, err := executor.Eval(proc, []*batch.Batch{input}, []bool{false, false, false})
		require.NoError(t, err)
		require.Equal(t, [2]int{}, calls)
		require.Equal(t, expectedText, *masked.GetType())
		require.Equal(t, 3, masked.Length())
		require.True(t, masked.IsConstNull())
		failStage = -1
		result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
		require.NoError(t, err)
		require.Equal(t, [2]int{1, 1}, calls)
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
		checkNull := func(vec *vector.Vector, typ types.Type, length int) {
			t.Helper()
			require.Equal(t, typ, *vec.GetType())
			require.Equal(t, length, vec.Length())
			require.True(t, vec.IsConstNull())
			require.False(t, vec.IsGrouping())
			for row := 0; row < length; row++ {
				require.True(t, vec.IsNull(uint64(row)))
			}
		}
		first, err := executor.Eval(proc, []*batch.Batch{input}, nil)
		require.NoError(t, err)
		require.NotSame(t, input.Vecs[0], first)
		checkNull(first, types.T_int32.ToType(), 3)
		checkNull(input.Vecs[0], types.T_int32.ToType(), 3)
		input.Vecs[0].SetType(types.T_int64.ToType())
		input.Vecs[0].SetLength(1)
		input.SetRowCount(1)
		second, err := executor.Eval(proc, []*batch.Batch{input}, nil)
		require.NoError(t, err)
		require.Same(t, first, second)
		checkNull(second, types.T_int32.ToType(), 1)
		checkNull(input.Vecs[0], types.T_int64.ToType(), 1)
		owned, err := executor.EvalWithoutResultReusing(proc, []*batch.Batch{input}, nil)
		if owned != nil {
			t.Cleanup(func() { owned.Free(proc.Mp()) })
		}
		require.NoError(t, err)
		require.Same(t, second, owned)
		checkNull(owned, types.T_int32.ToType(), 1)
		input.Vecs[0].SetLength(2)
		input.SetRowCount(2)
		replacement, err := executor.Eval(proc, []*batch.Batch{input}, nil)
		require.NoError(t, err)
		require.NotSame(t, owned, replacement)
		checkNull(replacement, types.T_int32.ToType(), 2)
		checkNull(input.Vecs[0], types.T_int64.ToType(), 2)
		checkNull(owned, types.T_int32.ToType(), 1)
		executor.Free()
		executor = nil
		checkNull(owned, types.T_int32.ToType(), 1)
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

	t.Run("case null else selection and reuse", func(t *testing.T) {
		input := testutil.NewBatchWithVectors([]*vector.Vector{
			testutil.NewVector(4, types.T_bool.ToType(), proc.Mp(), false, []bool{true, false, true, false}),
			testutil.NewVector(4, types.T_varchar.ToType(), proc.Mp(), false, []string{"42", "bad", "bad", "bad"}),
		}, nil)
		defer input.Clean(proc.Mp())
		expr := bindTestFunction(t, proc, "case", column(0, types.T_bool.ToType()),
			castToInt64(column(1, types.T_varchar.ToType())), typedNull(types.T_int64.ToType()))
		executor, err := NewExpressionExecutor(proc, expr)
		require.NoError(t, err)
		defer executor.Free()

		result, err := executor.Eval(proc, []*batch.Batch{input}, []bool{true, true, false, false})
		require.NoError(t, err)
		require.False(t, result.IsNull(0))
		require.Equal(t, int64(42), vector.MustFixedColWithTypeCheck[int64](result)[0])
		require.True(t, result.IsNull(1))

		result, err = executor.Eval(proc, []*batch.Batch{input}, []bool{false, true, false, true})
		require.NoError(t, err)
		require.True(t, result.IsNull(1))
		require.True(t, result.IsNull(3))
		result, err = executor.Eval(proc, []*batch.Batch{input}, []bool{false, false, false, false})
		require.NoError(t, err)
		for row := 0; row < 4; row++ {
			require.True(t, result.IsNull(uint64(row)))
		}
		_, err = executor.Eval(proc, []*batch.Batch{input}, []bool{false, false, true, false})
		require.Error(t, err)
	})

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
		t.Cleanup(func() { proc.SetResolveVariableFunc(previousResolver) })
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
			name       string
			expr       *plan.Expr
			inputType  types.Type
			inputValue any
		}{
			{
				name: "case",
				expr: bindTestFunction(t, proc, "case",
					column(0, types.T_bool.ToType()),
					variable,
					stringConst("ok")),
				inputType:  types.T_bool.ToType(),
				inputValue: false,
			},
			{
				name: "coalesce",
				expr: bindTestFunction(t, proc, "coalesce",
					column(0, types.T_varchar.ToType()),
					variable),
				inputType:  types.T_varchar.ToType(),
				inputValue: []byte("ok"),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				checkExpressionStorageAfterCleanup(t, proc)
				input := batch.NewWithSize(1)
				t.Cleanup(func() { input.Clean(proc.Mp()) })
				input.Vecs[0] = vector.NewVec(test.inputType)
				require.NoError(t, vector.AppendAny(input.Vecs[0], test.inputValue, false, proc.Mp()))
				input.SetRowCount(1)
				executor, err := NewExpressionExecutor(proc, test.expr)
				if executor != nil {
					t.Cleanup(executor.Free)
				}
				require.NoError(t, err)

				beforeCalls := resolveCalls
				result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
				require.NoError(t, err)
				require.Equal(t, "ok", result.GetStringAt(0))
				require.Equal(t, beforeCalls, resolveCalls)
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

	t.Run("integer in-list compaction and reset", func(t *testing.T) {
		list := vector.NewVec(types.T_int64.ToType())
		defer list.Free(proc.Mp())
		for i := int64(0); i < 32; i += 2 {
			require.NoError(t, vector.AppendFixed(list, i, false, proc.Mp()))
		}
		list.SetSorted(true)
		data, err := list.MarshalBinary()
		require.NoError(t, err)
		values := []int64{-1, 0, 1, 2, 30, 31}
		input := testutil.NewBatchWithVectors([]*vector.Vector{
			testutil.NewVector(6, types.T_bool.ToType(), proc.Mp(), false, []bool{false, true, true, true, true, true}),
			testutil.NewVector(6, types.T_int64.ToType(), proc.Mp(), false, values),
		}, nil)
		defer input.Clean(proc.Mp())
		input.Vecs[1].GetNulls().Add(5)
		for _, name := range []string{"in", "not_in"} {
			t.Run(name, func(t *testing.T) {
				listExpr := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Len: 16, Data: data}}}
				expr := bindTestFunction(t, proc, "if", column(0, types.T_bool.ToType()), bindTestFunction(t, proc, name, column(1, types.T_int64.ToType()), listExpr), makePlan2BoolConstExprWithType(false))
				executor, err := NewExpressionExecutor(proc, expr)
				require.NoError(t, err)
				defer executor.Free()
				for _, mask := range [][]bool{nil, {true, false, true, true, true, true}, {false, false, false, false, false, false}, nil} {
					result, err := executor.Eval(proc, []*batch.Batch{input}, mask)
					require.NoError(t, err)
					for i, v := range values {
						wantNull := i == 5 || mask != nil && !mask[i]
						require.Equal(t, wantNull, result.IsNull(uint64(i)), "row %d", i)
						if !wantNull {
							want := false
							if i > 0 {
								want = (v >= 0 && v < 32 && v%2 == 0) != (name == "not_in")
							}
							require.Equal(t, want, vector.GetFixedAtWithTypeCheck[bool](result, i), "row %d", i)
						}
					}
					executor.ResetForNextQuery()
				}
			})
		}
	})
	t.Run("integer in-list refreshes prepared values", func(t *testing.T) {
		input := testutil.NewBatchWithVectors([]*vector.Vector{
			testutil.NewVector(3, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2, 3}),
		}, nil)
		defer input.Clean(proc.Mp())
		param := &plan.Expr{Typ: plan.Type{Id: int32(types.T_text)}, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}}
		list := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{
			bindTestCast(t, proc, 0, param, types.T_int64.ToType()), makePlan2Int64ConstExprWithType(2),
		}}}}
		executor, err := NewExpressionExecutor(proc, bindTestFunction(t, proc, "in", column(0, types.T_int64.ToType()), list))
		require.NoError(t, err)
		defer executor.Free()
		for _, tc := range []struct {
			value string
			want  []bool
		}{{"1", []bool{true, true, false}}, {"3", []bool{false, true, true}}, {"1", []bool{true, true, false}}} {
			result := evalTestTextParameter(t, proc, executor, tc.value, false, []*batch.Batch{input}, nil)
			require.Equal(t, tc.want, vector.MustFixedColWithTypeCheck[bool](result))
			executor.ResetForNextQuery()
		}
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
			}{
				{"before", false}, {"", true},
				{"after", false}, {"", true},
				{"7", false},
			} {
				if generation > 0 {
					executor.ResetForNextQuery()
				}
				result := evalTestTextParameter(t, proc, executor, tc.value, tc.null, nil, nil)
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
			fold   bool
			values []int32
		}{
			{"ordinary text does not fold", false, []int32{42, 43}},
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
					result := evalTestTextParameter(t, proc, executor, value, false, nil, nil)
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

	})
}

func TestExplicitDecimalCastRowDiagnostics(t *testing.T) {
	for _, targetOID := range []types.T{types.T_decimal64, types.T_decimal128, types.T_decimal256} {
		for _, source := range []struct {
			name string
			typ  types.Type
			lit  *plan.Literal
		}{
			{"integer", types.T_int64.ToType(), &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1000}}},
			{"float", types.T_float64.ToType(), &plan.Literal{Value: &plan.Literal_Dval{Dval: 1000}}},
			{"decimal", types.New(types.T_decimal64, 7, 3), &plan.Literal{Value: &plan.Literal_Decimal64Val{Decimal64Val: &plan.Decimal64{A: 1000000}}}},
			{"string control", types.T_varchar.ToType(), &plan.Literal{Value: &plan.Literal_Sval{Sval: "1000"}}},
			{"prepared integer transport", types.T_int64.ToType(), nil},
		} {
			t.Run(fmt.Sprintf("%s/%s", targetOID, source.name), func(t *testing.T) {
				proc := testutil.NewProcess(t, testutil.WithFileService(nil))
				checkExpressionStorageAfterCleanup(t, proc)
				proc.SetBaseProcessRunningStatus(true)
				sink := &decimalCastWarningSink{}
				proc.WarningSink = sink
				arg := &plan.Expr{Typ: source.typ.PlanType(), Expr: &plan.Expr_Lit{Lit: source.lit}}
				if source.lit == nil {
					arg.Expr = &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}
				}
				expr := bindTestCast(t, proc, 1, arg, types.New(targetOID, 5, 2))
				executor, err := NewExpressionExecutor(proc, expr)
				require.NoError(t, err)
				t.Cleanup(executor.Free)
				input := batch.New(nil)
				t.Cleanup(func() { input.Clean(proc.Mp()) })
				eval := func(rows int, mask []bool) *vector.Vector {
					t.Helper()
					input.SetRowCount(rows)
					if source.lit == nil {
						typed, err := vector.NewConstFixed(types.T_int64.ToType(), int64(1000), 1, proc.Mp())
						require.NoError(t, err)
						return evalTestTypedParameter(t, proc, executor, "1000", false, typed, []*batch.Batch{input}, mask)
					}
					result, err := executor.Eval(proc, []*batch.Batch{input}, mask)
					require.NoError(t, err)
					return result
				}
				result := eval(4, []bool{false, false, false, false})
				require.Empty(t, sink.codes)
				for row := range 4 {
					require.True(t, result.IsNull(uint64(row)))
				}
				for _, stage := range []struct {
					rows int
					mask []bool
					want int
				}{
					{4, []bool{true, false, true, false}, 2},
					{3, nil, 5},
				} {
					result = eval(stage.rows, stage.mask)
					require.Len(t, sink.codes, stage.want)
					for row := range stage.rows {
						if stage.mask != nil && !stage.mask[row] {
							require.True(t, result.IsNull(uint64(row)))
							continue
						}
						require.False(t, result.IsNull(uint64(row)))
						var value string
						switch targetOID {
						case types.T_decimal64:
							value = vector.GetFixedAtNoTypeCheck[types.Decimal64](result, row).Format(2)
						case types.T_decimal128:
							value = vector.GetFixedAtNoTypeCheck[types.Decimal128](result, row).Format(2)
						case types.T_decimal256:
							value = vector.GetFixedAtNoTypeCheck[types.Decimal256](result, row).Format(2)
						}
						require.Equal(t, "999.99", value)
					}
				}
				executor.ResetForNextQuery()
				eval(1, nil)
				require.Equal(t, []uint16{1264, 1264, 1264, 1264, 1264, 1264}, sink.codes)
			})
		}
	}
}

type decimalCastWarningSink struct{ codes []uint16 }

func (s *decimalCastWarningSink) AppendWarningDiagnostic(code uint16, _ string) {
	s.codes = append(s.codes, code)
}

func TestDecimalCastFoldingBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name     string
		overload int32
		null     bool
		fold     bool
	}{
		{"silent implicit cast", 0, false, true},
		{"silent explicit cast", 1, false, false},
		{"NULL explicit cast", 1, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t, testutil.WithFileService(nil))
			checkExpressionStorageAfterCleanup(t, proc)
			sink := &decimalCastWarningSink{}
			proc.WarningSink = sink
			source := &plan.Expr{Typ: types.T_int64.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				Isnull: tc.null, Value: &plan.Literal_I64Val{I64Val: 999},
			}}}
			executor, err := NewExpressionExecutor(proc, bindTestCast(t, proc, tc.overload, source, types.New(types.T_decimal64, 5, 2)))
			require.NoError(t, err)
			t.Cleanup(executor.Free)
			input := batch.New(nil)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			input.SetRowCount(2)
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, tc.fold, executor.(*FunctionExpressionExecutor).folded.canFold)
			for row := range 2 {
				require.Equal(t, tc.null, result.IsNull(uint64(row)))
				if !tc.null {
					require.Equal(t, types.Decimal64(99900), vector.GetFixedAtNoTypeCheck[types.Decimal64](result, row))
				}
			}
			require.Empty(t, sink.codes)
		})
	}

	for _, joinBuild := range []bool{false, true} {
		t.Run(fmt.Sprintf("diagnostic owner/join=%v", joinBuild), func(t *testing.T) {
			proc := testutil.NewProcess(t, testutil.WithFileService(nil))
			checkExpressionStorageAfterCleanup(t, proc)
			sink := &decimalCastWarningSink{}
			proc.WarningSink = sink
			source := &plan.Expr{Typ: types.T_int64.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1000}}}}
			cast := bindTestCast(t, proc, 1, source, types.New(types.T_decimal64, 5, 2))
			expr := bindTestFunction(t, proc, "abs", cast)
			owner := new(DeferredJoinDiagnostic)
			owner.Prepare(proc)
			t.Cleanup(owner.Reset)
			var executors []ExpressionExecutor
			var err error
			if joinBuild {
				executors, err = NewJoinBuildExpressionExecutors(proc, []*plan.Expr{expr}, nil, owner)
			} else {
				executors, err = NewOwnedConstantFilterExecutors(proc, []*plan.Expr{expr})
			}
			require.NoError(t, err)
			for _, executor := range executors {
				t.Cleanup(executor.Free)
			}
			input := batch.New(nil)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			for _, rows := range []int{2, 3} {
				input.SetRowCount(rows)
				_, err := executors[0].Eval(proc, []*batch.Batch{input}, nil)
				require.NoError(t, err)
			}
			require.Equal(t, []uint16{1264, 1264, 1264, 1264, 1264}, sink.codes,
				"the owner must not fold or defer a nested row-scoped conversion")
			require.NoError(t, owner.Activate(proc))
			require.Len(t, sink.codes, 5, "activation must not replay row diagnostics")
		})
	}
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

func TestJSONStringEltEvaluatesOnlySelectedSubtree(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	checkExpressionStorageAfterCleanup(t, proc)
	input := batch.NewWithSize(3)
	defer input.Clean(proc.Mp())
	input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	input.Vecs[1] = vector.NewVec(types.T_json.ToType())
	input.Vecs[2] = vector.NewVec(types.T_varchar.ToType())
	for _, n := range []int64{1, 2} {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], n, false, proc.Mp()))
	}
	for _, value := range []string{`{"p":1}`, `{"p":2}`} {
		encoded, err := types.ParseStringToByteJson(value)
		require.NoError(t, err)
		data, err := encoded.Marshal()
		require.NoError(t, err)
		require.NoError(t, vector.AppendBytes(input.Vecs[1], data, false, proc.Mp()))
	}
	for _, value := range []string{`not json`, `{"q":3}`} {
		require.NoError(t, vector.AppendBytes(input.Vecs[2], []byte(value), false, proc.Mp()))
	}
	input.SetRowCount(2)
	column := func(pos int32, typ types.Type) *plan.Expr {
		return &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}}}
	}
	cast := bindTestCast(t, proc, 1, column(2, types.T_varchar.ToType()), types.T_json.ToType())
	bound := bindTestFunction(t, proc, "elt", column(0, types.T_int64.ToType()), column(1, types.T_json.ToType()), cast)
	_, overload := function.DecodeOverloadID(bound.GetF().GetFunc().GetObj())
	require.Equal(t, int32(1), overload)
	executor, err := NewExpressionExecutor(proc, bound)
	require.NoError(t, err)
	defer executor.Free()
	result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, `{"p": 1}`, string(result.GetBytesAt(0)))
	require.Equal(t, `{"q": 3}`, string(result.GetBytesAt(1)))
	executor.ResetForNextQuery()
	// Selecting the malformed conversion must still report its error.
	vector.MustFixedColNoTypeCheck[int64](input.Vecs[0])[0] = 2
	_, err = executor.Eval(proc, []*batch.Batch{input}, nil)
	require.Error(t, err)
	executor.ResetForNextQuery()
	// The masked bad row cannot poison a valid selected row.
	result, err = executor.Eval(proc, []*batch.Batch{input}, []bool{false, true})
	require.NoError(t, err)
	require.True(t, result.IsNull(0))
	require.Equal(t, `{"q": 3}`, string(result.GetBytesAt(1)))
	executor.ResetForNextQuery()
	// Invalid selectors do not evaluate any value subtree.
	indices := vector.MustFixedColNoTypeCheck[int64](input.Vecs[0])
	indices[0], indices[1] = 0, 3
	result, err = executor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.True(t, result.IsNull(0))
	require.True(t, result.IsNull(1))
}

func TestJSONStringEltDoesNotFoldUnselectedCast(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	checkExpressionStorageAfterCleanup(t, proc)
	input := batch.NewWithSize(2)
	defer input.Clean(proc.Mp())
	input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	input.Vecs[1] = vector.NewVec(types.T_json.ToType())
	require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(1), false, proc.Mp()))
	value, err := types.ParseStringToByteJson(`{"p":1}`)
	require.NoError(t, err)
	data, err := value.Marshal()
	require.NoError(t, err)
	require.NoError(t, vector.AppendBytes(input.Vecs[1], data, false, proc.Mp()))
	input.SetRowCount(1)
	column := func(pos int32, typ types.Type) *plan.Expr {
		return &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}}}
	}
	malformed := &plan.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "not json"}}}}
	cast := bindTestCast(t, proc, 1, malformed, types.T_json.ToType())
	expression := bindTestFunction(t, proc, "elt", column(0, types.T_int64.ToType()), column(1, types.T_json.ToType()), cast)
	executor, err := NewExpressionExecutor(proc, expression)
	require.NoError(t, err)
	defer executor.Free()
	result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, `{"p": 1}`, string(result.GetBytesAt(0)))
	executor.ResetForNextQuery()
	vector.MustFixedColNoTypeCheck[int64](input.Vecs[0])[0] = 2
	_, err = executor.Eval(proc, []*batch.Batch{input}, nil)
	require.Error(t, err)
}

// Exercise the registered overload through runtime folding, rather than calling
// its kernel directly: literal diagnostics belong to every selected input row.
func TestBitCountDiagnosticFolding(t *testing.T) {
	for _, tc := range []struct {
		name   string
		source *plan.Expr
		want   uint64
		fold   bool
	}{
		{"text", &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "3x"}}}}, 2, false},
		{"valid_text", &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "3 "}}}}, 2, true},
		{"binary", &plan.Expr{Typ: plan.Type{Id: int32(types.T_varbinary)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "3x"}}}}, 8, true},
		{"numeric", makePlan2Int64ConstExprWithType(3), 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t, testutil.WithFileService(nil))
			checkExpressionStorageAfterCleanup(t, proc)
			session := &preparedCastWarningSession{}
			proc.Session = session
			executor, err := NewExpressionExecutor(proc, bindTestFunction(t, proc, "bit_count", tc.source))
			if executor != nil {
				t.Cleanup(executor.Free)
			}
			require.NoError(t, err)
			input := batch.New(nil)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			input.SetRowCount(3)
			for _, mask := range [][]bool{nil, {false, true, true}, {false, false, false}} {
				executor.ResetForNextQuery()
				session.warningCount = 0
				result, err := executor.Eval(proc, []*batch.Batch{input}, mask)
				require.NoError(t, err)
				require.Equal(t, 3, result.Length())
				selected := 0
				for row := 0; row < 3; row++ {
					if mask == nil || mask[row] {
						selected++
						require.False(t, result.IsNull(uint64(row)))
						require.Equal(t, tc.want, vector.GetFixedAtNoTypeCheck[uint64](result, row))
					}
				}
				if tc.fold {
					require.Zero(t, session.warningCount)
				} else {
					require.Equal(t, selected, session.warningCount)
				}
				if selected > 0 {
					require.Equal(t, tc.fold, executor.(*FunctionExpressionExecutor).folded.canFold)
					require.True(t, result.IsConst(), "value constancy must survive diagnostics")
				}
			}
			executor.ResetForNextQuery()
			session.warningCount = 0
			input.SetRowCount(0)
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Zero(t, result.Length())
			require.Zero(t, session.warningCount)
		})
	}
}

func TestDiagnosticScalarValues(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	checkExpressionStorageAfterCleanup(t, proc)
	sink := &preparedCastWarningSession{}
	proc.WarningSink = sink
	text := func(value string) *plan.Expr {
		return &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: value}}}}
	}
	bits := bindTestFunction(t, proc, "bit_count", text("3x"))
	converted := bindTestCast(t, proc, 0, bits, types.T_varchar.ToType())
	joined := bindTestFunction(t, proc, "concat", text("["), converted, text("]"))
	decimal := bindTestCast(t, proc, 1, bits, types.New(types.T_decimal64, 18, 0))
	decimalText := bindTestCast(t, proc, 0, decimal, types.T_varchar.ToType())
	for _, expr := range []*plan.Expr{
		joined,
		bindTestFunction(t, proc, "concat", text("["), decimalText, text("]")),
		bindTestFunction(t, proc, "if", makePlan2BoolConstExprWithType(true), joined, text("unused")),
	} {
		func() {
			executor, err := NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer executor.Free()
			input := batch.New(nil)
			defer input.Clean(proc.Mp())
			input.SetRowCount(3)
			// Reuse without reset: a scalar output must never cache away effects.
			for _, mask := range [][]bool{nil, {false, true, true}, nil, {false, false, false}} {
				sink.warningCount = 0
				got, err := executor.Eval(proc, []*batch.Batch{input}, mask)
				require.NoError(t, err)
				selected := 0
				for row := 0; row < 3; row++ {
					if mask == nil || mask[row] {
						selected++
						require.Equal(t, "[2]", got.GetStringAt(row))
					}
				}
				require.Equal(t, selected, sink.warningCount)
				require.False(t, executor.(*FunctionExpressionExecutor).folded.canFold)
				if selected > 0 {
					require.True(t, got.IsConst())
				}
			}
		}()
	}

	// Physical batch constancy does not make a column a scalar SQL expression.
	column := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	executor, err := NewExpressionExecutor(proc, bindTestFunction(t, proc, "bit_count", column))
	require.NoError(t, err)
	defer executor.Free()
	input := batch.NewWithSize(1)
	defer input.Clean(proc.Mp())
	input.Vecs[0], err = vector.NewConstBytes(types.T_varchar.ToType(), []byte("3x"), 3, proc.Mp())
	require.NoError(t, err)
	input.SetRowCount(3)
	sink.warningCount = 0
	got, err := executor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.False(t, got.IsConst())
	require.Equal(t, 3, sink.warningCount)
	for row := 0; row < 3; row++ {
		require.Equal(t, uint64(2), vector.GetFixedAtNoTypeCheck[uint64](got, row))
	}

	conditions := batch.NewWithSize(1)
	defer conditions.Clean(proc.Mp())
	conditions.Vecs[0] = vector.NewVec(types.T_bool.ToType())
	require.NoError(t, vector.AppendFixedList(conditions.Vecs[0], []bool{true, false, true}, nil, proc.Mp()))
	conditions.SetRowCount(3)
	condition := &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	for _, tc := range []struct {
		name     string
		expr     *plan.Expr
		warnings int
	}{
		{"volatile", bindTestFunction(t, proc, "rand"), 0},
		{"column_condition", bindTestFunction(t, proc, "if", condition, bits, bindTestFunction(t, proc, "bit_count", text("3"))), 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			executor, err := NewExpressionExecutor(proc, tc.expr)
			require.NoError(t, err)
			defer executor.Free()
			sink.warningCount = 0
			got, err := executor.Eval(proc, []*batch.Batch{conditions}, nil)
			require.NoError(t, err)
			require.False(t, got.IsConst())
			require.Equal(t, tc.warnings, sink.warningCount)
			if tc.name == "column_condition" {
				for row := 0; row < 3; row++ {
					require.Equal(t, uint64(2), vector.GetFixedAtNoTypeCheck[uint64](got, row))
				}
			}
		})
	}
}

func TestBitCountDiagnosticFoldPreparedReuse(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	checkExpressionStorageAfterCleanup(t, proc)
	wasRunning := proc.GetBaseProcessRunningStatus()
	t.Cleanup(func() { proc.SetBaseProcessRunningStatus(wasRunning) })
	proc.SetBaseProcessRunningStatus(true)
	sink := &preparedCastWarningSession{}
	proc.WarningSink = sink
	parameter := &plan.Expr{Typ: plan.Type{Id: int32(types.T_text)}, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}}
	// Bind the text overload explicitly; untyped BIT_COUNT markers keep their
	// separate binary dispatch contract in the binder.
	executor, err := NewExpressionExecutor(proc, bindTestFunction(t, proc, "bit_count", parameter))
	if executor != nil {
		t.Cleanup(executor.Free)
	}
	require.NoError(t, err)
	input := batch.New(nil)
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	input.SetRowCount(3)
	for _, tc := range []struct {
		value string
		null  bool
		fold  bool
	}{
		{"3", false, true}, {"3x", false, false}, {"", true, true}, {"3 ", false, true}, {"3x", false, false},
	} {
		executor.ResetForNextQuery()
		sink.warningCount = 0
		result := evalTestTextParameter(t, proc, executor, tc.value, tc.null, []*batch.Batch{input}, nil)
		require.Equal(t, tc.fold, executor.(*FunctionExpressionExecutor).folded.canFold, tc.value)
		require.True(t, result.IsConst(), tc.value)
		require.Equal(t, 3, result.Length())
		for row := 0; row < 3; row++ {
			require.Equal(t, tc.null, result.IsNull(uint64(row)))
			if !tc.null {
				require.Equal(t, uint64(2), vector.GetFixedAtNoTypeCheck[uint64](result, row))
			}
		}
		if tc.fold {
			require.Zero(t, sink.warningCount)
		} else {
			require.Equal(t, 3, sink.warningCount, "the scalar probe must not leak a fourth warning")
		}
		require.Same(t, sink, proc.WarningSink)
	}
}

// TestVecBlockVectorLiteral checks that a vecf8/vecf4 vector literal is a constant of its
// stored cell, and that a vector literal of a type without a constant form is an error.
func TestVecBlockVectorLiteral(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	checkExpressionStorageAfterCleanup(t, proc)
	for _, f := range []types.BlockScaledFormat{types.BlockScaledMXFP8, types.BlockScaledNVFP4} {
		cell, err := types.AppendBlockScaled(nil, f, []float32{1, -2, 0.5})
		require.NoError(t, err)
		oid := types.T_array_float8
		if f == types.BlockScaledNVFP4 {
			oid = types.T_array_float4
		}
		vec, err := generateConstExpressionExecutor(proc, types.New(oid, 3, 0), &plan.Literal{
			Value: &plan.Literal_VecVal{VecVal: string(cell)},
		}, nil)
		require.NoError(t, err)
		require.True(t, vec.IsConst())
		require.Equal(t, oid, vec.GetType().Oid)
		require.Equal(t, cell, vec.GetBytesAt(0))
		vec.Free(proc.Mp())
	}
	_, err := generateConstExpressionExecutor(proc, types.T_int64.ToType(), &plan.Literal{
		Value: &plan.Literal_VecVal{VecVal: "\x00"},
	}, nil)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNYI))
}
