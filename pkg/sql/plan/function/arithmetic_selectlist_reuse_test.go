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
	"math"
	"sync/atomic"
	"testing"

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
