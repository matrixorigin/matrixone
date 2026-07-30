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

package function

import (
	"context"
	"math"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// staleSelectList models a reused executor that first evaluated three rows and
// then two. Row 2 is outside the current batch but retains its old mask bit.
func staleSelectList() *FunctionSelectList {
	return &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, false}}
}

func TestHasEvaluableRows(t *testing.T) {
	t.Run("zero length", func(t *testing.T) {
		require.False(t, hasEvaluableRows(nulls.NewWithSize(0), 0))
	})

	t.Run("empty bitmap fast path", func(t *testing.T) {
		require.True(t, hasEvaluableRows(nulls.NewWithSize(2), 2))
	})

	t.Run("all current rows null with stale tail", func(t *testing.T) {
		rsNull := nulls.NewWithSize(3)
		rsNull.Add(0)
		rsNull.Add(1)
		rsNull.Add(2)
		require.False(t, hasEvaluableRows(rsNull, 2))
	})

	t.Run("partial null", func(t *testing.T) {
		rsNull := nulls.NewWithSize(2)
		rsNull.Add(1)
		require.True(t, hasEvaluableRows(rsNull, 2))
	})

	t.Run("stale tail only", func(t *testing.T) {
		rsNull := nulls.NewWithSize(3)
		rsNull.Add(2)
		require.True(t, hasEvaluableRows(rsNull, 2))
	})
}

func runWithStaleSelectList(
	t *testing.T,
	procMode int32,
	inputs []FunctionTestInput,
	resultType types.Type,
	fn fEvalFn,
) (*vector.Vector, error) {
	t.Helper()
	proc := testutil.NewProcess(t)
	atomic.StoreInt32(&proc.Base.DivByZeroErrorMode, procMode)
	t.Cleanup(func() {
		atomic.StoreInt32(&proc.Base.DivByZeroErrorMode, -1)
		proc.Free()
	})

	tcc := NewFunctionTestCase(proc, inputs, NewFunctionTestResult(resultType, false, nil, nil), fn)
	require.NoError(t, tcc.result.PreExtendAndReset(2))
	err := tcc.fn(tcc.parameters, tcc.result, proc, 2, staleSelectList())
	return tcc.GetResultVectorDirectly(), err
}

func TestArithmeticStaleSelectListOutsideLength(t *testing.T) {
	t.Run("decimal addition keeps current valid row", func(t *testing.T) {
		decType := types.New(types.T_decimal64, 18, 2)
		resultType := resolvedReturnType(t, PLUS, []types.Type{decType, decType})
		vec, err := runWithStaleSelectList(t, 0,
			[]FunctionTestInput{
				NewFunctionTestInput(decType, []types.Decimal64{100, 0}, []bool{false, false}),
				NewFunctionTestConstInput(decType, []types.Decimal64{200}, []bool{false}),
			}, resultType, plusFn)
		require.NoError(t, err)
		require.Equal(t, resultType, *vec.GetType())
		require.Equal(t, types.Decimal64(300), vector.MustFixedColWithTypeCheck[types.Decimal64](vec)[0])
		require.False(t, vec.GetNulls().Contains(0))
		require.True(t, vec.GetNulls().Contains(1))
	})

	for _, tc := range []struct {
		name string
		id   int
		fn   fEvalFn
	}{
		{name: "divide", id: DIV, fn: divFn},
		{name: "integer divide", id: INTEGER_DIV, fn: integerDivFn},
		{name: "modulo", id: MOD, fn: modFn},
	} {
		t.Run("float64 "+tc.name, func(t *testing.T) {
			floatType := types.T_float64.ToType()
			resultType := resolvedReturnType(t, tc.id, []types.Type{floatType, floatType})
			vec, err := runWithStaleSelectList(t, 0,
				[]FunctionTestInput{
					NewFunctionTestInput(floatType, []float64{5, 0}, []bool{false, false}),
					NewFunctionTestConstInput(floatType, []float64{2}, []bool{false}),
				}, resultType, tc.fn)
			require.NoError(t, err)
			require.Equal(t, resultType, *vec.GetType())
			if tc.id == INTEGER_DIV {
				require.Equal(t, int64(2), vector.MustFixedColWithTypeCheck[int64](vec)[0])
			} else if tc.id == DIV {
				require.Equal(t, 2.5, vector.MustFixedColWithTypeCheck[float64](vec)[0])
			} else {
				require.Equal(t, float64(1), vector.MustFixedColWithTypeCheck[float64](vec)[0])
			}
			require.False(t, vec.GetNulls().Contains(0))
			require.True(t, vec.GetNulls().Contains(1))
		})
	}

	t.Run("strict divide by zero still errors", func(t *testing.T) {
		floatType := types.T_float64.ToType()
		resultType := resolvedReturnType(t, DIV, []types.Type{floatType, floatType})
		_, err := runWithStaleSelectList(t, 1,
			[]FunctionTestInput{
				NewFunctionTestInput(floatType, []float64{5, 0}, []bool{false, false}),
				NewFunctionTestConstInput(floatType, []float64{0}, []bool{false}),
			}, resultType, divFn)
		require.Error(t, err)
	})

	t.Run("decimal overflow still errors", func(t *testing.T) {
		decType := types.New(types.T_decimal64, 18, 0)
		resultType := resolvedReturnType(t, PLUS, []types.Type{decType, decType})
		_, err := runWithStaleSelectList(t, 0,
			[]FunctionTestInput{
				NewFunctionTestInput(decType, []types.Decimal64{types.Decimal64(math.MaxInt64), 0}, []bool{false, false}),
				NewFunctionTestConstInput(decType, []types.Decimal64{1}, []bool{false}),
			}, resultType, plusFn)
		require.Error(t, err)
	})

	t.Run("strict decimal divide by zero still errors", func(t *testing.T) {
		decType := types.New(types.T_decimal64, 18, 2)
		resultType := resolvedReturnType(t, DIV, []types.Type{decType, decType})
		_, err := runWithStaleSelectList(t, 1,
			[]FunctionTestInput{
				NewFunctionTestInput(decType, []types.Decimal64{500, 0}, []bool{false, false}),
				NewFunctionTestConstInput(decType, []types.Decimal64{0}, []bool{false}),
			}, resultType, divFn)
		require.Error(t, err)
	})

	testDecimalStaleSelectList[types.Decimal64](t, types.New(types.T_decimal64, 18, 2),
		[]types.Decimal64{500, 0}, []types.Decimal64{200})
	testDecimalStaleSelectList[types.Decimal128](t, types.New(types.T_decimal128, 38, 2),
		[]types.Decimal128{{B0_63: 500}, {}}, []types.Decimal128{{B0_63: 200}})
	testDecimalStaleSelectList[types.Decimal256](t, types.New(types.T_decimal256, 65, 2),
		[]types.Decimal256{{B0_63: 500}, {}}, []types.Decimal256{{B0_63: 200}})
}

func testDecimalStaleSelectList[T templateDec](t *testing.T, decType types.Type, dividend, divisor []T) {
	t.Helper()
	for _, tc := range []struct {
		name string
		id   int
		fn   fEvalFn
	}{
		{name: "divide", id: DIV, fn: divFn},
		{name: "integer divide", id: INTEGER_DIV, fn: integerDivFn},
		{name: "modulo", id: MOD, fn: modFn},
	} {
		t.Run(decType.Oid.String()+" "+tc.name, func(t *testing.T) {
			resultType := resolvedReturnType(t, tc.id, []types.Type{decType, decType})
			vec, err := runWithStaleSelectList(t, 0,
				[]FunctionTestInput{
					NewFunctionTestInput(decType, dividend, []bool{false, false}),
					NewFunctionTestConstInput(decType, divisor, []bool{false}),
				}, resultType, tc.fn)
			require.NoError(t, err)
			require.Equal(t, resultType, *vec.GetType())
			require.False(t, vec.GetNulls().Contains(0))
			require.True(t, vec.GetNulls().Contains(1))

			if tc.id == INTEGER_DIV {
				require.Equal(t, int64(2), vector.MustFixedColWithTypeCheck[int64](vec)[0])
				return
			}
			switch decType.Oid {
			case types.T_decimal64:
				if tc.id == DIV {
					got := vector.MustFixedColWithTypeCheck[types.Decimal128](vec)[0]
					require.Equal(t, "2.50000000", got.Format(resultType.Scale))
				} else {
					require.Equal(t, types.Decimal64(100), vector.MustFixedColWithTypeCheck[types.Decimal64](vec)[0])
				}
			case types.T_decimal128:
				got := vector.MustFixedColWithTypeCheck[types.Decimal128](vec)[0]
				if tc.id == DIV {
					require.Equal(t, "2.50000000", got.Format(resultType.Scale))
				} else {
					require.Equal(t, "1.00", got.Format(resultType.Scale))
				}
			case types.T_decimal256:
				got := vector.MustFixedColWithTypeCheck[types.Decimal256](vec)[0]
				if tc.id == DIV {
					require.Equal(t, "2.50000000", got.Format(resultType.Scale))
				} else {
					require.Equal(t, "1.00", got.Format(resultType.Scale))
				}
			}
		})
	}
}

type arithmeticMatrixInput interface {
	int64 | uint64 | float32 | float64 | types.Decimal64 | types.Decimal128 | types.Decimal256
}

type arithmeticMatrixOperation struct {
	name string
	id   int
	fn   fEvalFn
}

func TestArithmeticDivisionByZeroMatrix(t *testing.T) {
	divMod := []arithmeticMatrixOperation{
		{name: "div", id: INTEGER_DIV, fn: integerDivFn},
		{name: "mod", id: MOD, fn: modFn},
	}
	allOps := []arithmeticMatrixOperation{
		{name: "slash", id: DIV, fn: divFn},
		{name: "div", id: INTEGER_DIV, fn: integerDivFn},
		{name: "mod", id: MOD, fn: modFn},
	}

	runArithmeticMatrixType(t, types.T_int64.ToType(), int64(1), int64(0), divMod)
	runArithmeticMatrixType(t, types.T_uint64.ToType(), uint64(1), uint64(0), divMod)
	runArithmeticMatrixType(t, types.T_float32.ToType(), float32(1), float32(0), allOps)
	runArithmeticMatrixType(t, types.T_float64.ToType(), float64(1), float64(0), allOps)
	runArithmeticMatrixType(t, types.New(types.T_decimal64, 18, 2), types.Decimal64(100), types.Decimal64(0), allOps)
	runArithmeticMatrixType(t, types.New(types.T_decimal128, 38, 2),
		types.Decimal128{B0_63: 100}, types.Decimal128{}, allOps)
	runArithmeticMatrixType(t, types.New(types.T_decimal256, 65, 2),
		types.Decimal256{B0_63: 100}, types.Decimal256{}, allOps)
}

func runArithmeticMatrixType[T arithmeticMatrixInput](
	t *testing.T,
	typ types.Type,
	one T,
	zero T,
	operations []arithmeticMatrixOperation,
) {
	t.Helper()
	type shape struct {
		name                  string
		leftConst, rightConst bool
	}
	shapes := []shape{
		{name: "const_const", leftConst: true, rightConst: true},
		{name: "const_vector", leftConst: true},
		{name: "vector_const", rightConst: true},
		{name: "vector_vector"},
	}
	type rowState struct {
		name string
	}
	states := []rowState{{name: "all_null"}, {name: "mixed_null"}, {name: "partial_mask"}}
	modes := []struct {
		name   string
		strict bool
	}{{name: "strict", strict: true}, {name: "non_strict"}}

	for _, op := range operations {
		for _, shape := range shapes {
			for _, state := range states {
				// A const/const pair has no row-varying operand, so mixed NULL is
				// not a representable execution shape. all-null and partial-mask
				// still cover its two reachable row-eligibility states.
				if shape.leftConst && shape.rightConst && state.name == "mixed_null" {
					continue
				}
				for _, mode := range modes {
					name := typ.Oid.String() + "/" + op.name + "/" + shape.name + "/" + state.name + "/" + mode.name
					t.Run(name, func(t *testing.T) {
						leftValues := []T{one, one}
						rightValues := []T{zero, zero}
						leftNulls := []bool{false, false}
						rightNulls := []bool{false, false}
						var selectList *FunctionSelectList

						switch state.name {
						case "all_null":
							leftNulls = []bool{true, true}
						case "mixed_null":
							if !shape.leftConst {
								leftNulls = []bool{true, false}
							} else {
								rightNulls = []bool{true, false}
							}
						case "partial_mask":
							selectList = &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}}
							if !shape.rightConst {
								rightValues[0] = one
							}
						}

						if shape.leftConst {
							leftValues = leftValues[:1]
							leftNulls = leftNulls[:1]
						}
						if shape.rightConst {
							rightValues = rightValues[:1]
							rightNulls = rightNulls[:1]
						}

						makeInput := func(isConst bool, values []T, nullList []bool) FunctionTestInput {
							if isConst {
								return NewFunctionTestConstInput(typ, values, nullList)
							}
							return NewFunctionTestInput(typ, values, nullList)
						}
						inputs := []FunctionTestInput{
							makeInput(shape.leftConst, leftValues, leftNulls),
							makeInput(shape.rightConst, rightValues, rightNulls),
						}
						resultType := resolvedReturnType(t, op.id, []types.Type{typ, typ})
						proc := testutil.NewProcess(t)
						defer proc.Free()
						if mode.strict {
							atomic.StoreInt32(&proc.Base.DivByZeroErrorMode, 1)
						} else {
							atomic.StoreInt32(&proc.Base.DivByZeroErrorMode, 0)
						}
						tcc := NewFunctionTestCase(proc, inputs,
							NewFunctionTestResult(resultType, false, nil, nil), op.fn)
						require.NoError(t, tcc.result.PreExtendAndReset(2))
						err := tcc.fn(tcc.parameters, tcc.result, proc, 2, selectList)

						wantError := mode.strict && state.name != "all_null" &&
							!(state.name == "partial_mask" && !shape.rightConst)
						if wantError {
							require.Error(t, err)
							return
						}
						require.NoError(t, err)
						rsVec := tcc.GetResultVectorDirectly()
						require.Equal(t, resultType, *rsVec.GetType())
						require.False(t, rsVec.GetNulls().Contains(2), "must not retain a NULL outside length")
						if state.name == "partial_mask" && !shape.rightConst {
							require.False(t, rsVec.GetNulls().Contains(0))
							require.True(t, rsVec.GetNulls().Contains(1))
						} else {
							require.True(t, rsVec.GetNulls().Contains(0))
							require.True(t, rsVec.GetNulls().Contains(1))
						}
					})
				}
			}
		}
	}
}

func TestIntegerSlashResolverMatrix(t *testing.T) {
	for _, typ := range []types.Type{types.T_int64.ToType(), types.T_uint64.ToType()} {
		t.Run(typ.Oid.String(), func(t *testing.T) {
			got, err := GetFunctionByName(context.Background(), "/", []types.Type{typ, typ})
			require.NoError(t, err)
			targets, needCast := got.ShouldDoImplicitTypeCast()
			require.True(t, needCast)
			require.Len(t, targets, 2)
			require.Equal(t, got.GetReturnType(), resolvedReturnType(t, DIV, targets))
		})
	}
}

func TestMaskedVectorConstFloatOverflow(t *testing.T) {
	maskOverflowRow := &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}}
	for _, tc := range []struct {
		name   string
		fn     fEvalFn
		left   []float64
		right  float64
		wanted float64
	}{
		{name: "multiply", fn: multiFn, left: []float64{1, math.MaxFloat64}, right: 2, wanted: 2},
		{name: "subtract", fn: minusFn, left: []float64{1, math.MaxFloat64}, right: -math.MaxFloat64, wanted: math.MaxFloat64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			inputs := []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(), tc.left, []bool{false, false}),
				NewFunctionTestConstInput(types.T_float64.ToType(), []float64{tc.right}, []bool{false}),
			}
			tcc := NewFunctionTestCase(proc, inputs,
				NewFunctionTestResult(types.T_float64.ToType(), false, nil, nil), tc.fn)
			require.NoError(t, tcc.result.PreExtendAndReset(2))
			require.NoError(t, tcc.fn(tcc.parameters, tcc.result, proc, 2, maskOverflowRow))
			rsVec := tcc.GetResultVectorDirectly()
			require.Equal(t, tc.wanted, vector.MustFixedColWithTypeCheck[float64](rsVec)[0])
			require.False(t, rsVec.GetNulls().Contains(0))
			require.True(t, rsVec.GetNulls().Contains(1))
		})
	}
}
