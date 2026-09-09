// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/constraints"
)

func checkUnaryRows[T, R constraints.Signed | constraints.Float](t *testing.T, name string, inputType, resultType types.T, values []T, want []R) {
	t.Helper()
	for _, mode := range []struct {
		name      string
		selection *FunctionSelectList
		nulls     []bool
	}{
		{"no_selection", nil, []bool{false, true, false}},
		{"all_selected", &FunctionSelectList{}, []bool{false, true, false}},
		{"partial", &FunctionSelectList{AnyNull: true, SelectList: []bool{false, true, true}}, []bool{true, true, false}},
		{"none_selected", &FunctionSelectList{AnyNull: true, AllNull: true}, []bool{true, true, true}},
	} {
		t.Run(mode.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			resolved, err := GetFunctionByName(proc.Ctx, name, []types.Type{inputType.ToType()})
			require.NoError(t, err)
			require.Equal(t, resultType, resolved.GetReturnType().Oid)
			ov, err := GetFunctionById(proc.Ctx, resolved.GetEncodedOverloadID())
			require.NoError(t, err)
			exec, _, _, _ := ov.GetExecuteMethod()
			in := vector.NewVec(inputType.ToType())
			defer in.Free(proc.Mp())
			require.NoError(t, vector.AppendFixedList(in, values, nil, proc.Mp()))
			in.GetNulls().Add(1)
			out := vector.NewFunctionResultWrapper(resolved.GetReturnType(), proc.Mp())
			defer out.Free()
			require.NoError(t, out.PreExtendAndReset(len(values)))
			require.NoError(t, exec([]*vector.Vector{in}, out, proc, len(values), mode.selection))
			v := out.GetResultVector()
			require.Equal(t, len(values), v.Length())
			got := vector.MustFixedColNoTypeCheck[R](v)
			for i := range values {
				require.Equal(t, mode.nulls[i], v.GetNulls().Contains(uint64(i)))
				if !mode.nulls[i] {
					require.Equal(t, want[i], got[i])
				}
			}
		})
	}
}

func TestUnaryRegisteredTypeAndSelection(t *testing.T) {
	t.Run("int8", func(t *testing.T) {
		checkUnaryRows(t, "unary_minus", types.T_int8, types.T_int64, []int8{math.MinInt8, 0, math.MaxInt8}, []int64{128, 0, -127})
	})
	t.Run("int16", func(t *testing.T) {
		checkUnaryRows(t, "unary_minus", types.T_int16, types.T_int64, []int16{math.MinInt16, 0, math.MaxInt16}, []int64{32768, 0, -32767})
	})
	t.Run("int32", func(t *testing.T) {
		checkUnaryRows(t, "unary_minus", types.T_int32, types.T_int64, []int32{math.MinInt32, 0, math.MaxInt32}, []int64{2147483648, 0, -2147483647})
	})
	t.Run("int64", func(t *testing.T) {
		checkUnaryRows(t, "unary_minus", types.T_int64, types.T_int64, []int64{math.MinInt64 + 1, 0, math.MaxInt64}, []int64{math.MaxInt64, 0, -math.MaxInt64})
	})
	t.Run("float32", func(t *testing.T) {
		checkUnaryRows(t, "unary_minus", types.T_float32, types.T_float32, []float32{math.MinInt64, 0, 1.5}, []float32{-float32(math.MinInt64), 0, -1.5})
	})
	t.Run("float64", func(t *testing.T) {
		checkUnaryRows(t, "unary_minus", types.T_float64, types.T_float64, []float64{math.MinInt64, 0, 1.5}, []float64{-float64(math.MinInt64), 0, -1.5})
	})
	t.Run("plus_int8", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		resolved, err := GetFunctionByName(proc.Ctx, "unary_plus", []types.Type{types.T_int8.ToType()})
		require.NoError(t, err)
		require.Equal(t, types.T_int8, resolved.GetReturnType().Oid)
		tc := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestInput(types.T_int8.ToType(), []int8{-128, 0, 127}, []bool{false, true, false})}, NewFunctionTestResult(resolved.GetReturnType(), false, []int8{-128, 0, 127}, []bool{false, true, false}), operatorUnaryPlus[int8])
		defer tc.result.Free()
		defer tc.parameters[0].Free(proc.Mp())
		ok, info := tc.Run()
		require.True(t, ok, info)
	})
}

func TestUnaryMinusInt64Overflow(t *testing.T) {
	for _, constant := range []bool{false, true} {
		for _, mode := range []string{"overflow", "null", "masked", "all_masked"} {
			t.Run(mode+map[bool]string{false: "/vector", true: "/constant"}[constant], func(t *testing.T) {
				proc := testutil.NewProcess(t)
				var in *vector.Vector
				if constant {
					var err error
					in, err = vector.NewConstFixed(types.T_int64.ToType(), int64(math.MinInt64), 3, proc.Mp())
					require.NoError(t, err)
				} else {
					in = vector.NewVec(types.T_int64.ToType())
					require.NoError(t, vector.AppendFixedList(in, []int64{math.MinInt64, math.MinInt64, math.MinInt64}, nil, proc.Mp()))
				}
				defer in.Free(proc.Mp())
				var selection *FunctionSelectList
				switch mode {
				case "null":
					in.GetNulls().Add(0, 1, 2)
				case "masked":
					selection = &FunctionSelectList{AnyNull: true, SelectList: []bool{false, false, false}}
				case "all_masked":
					selection = &FunctionSelectList{AnyNull: true, AllNull: true}
				}
				out := vector.NewFunctionResultWrapper(types.T_int64.ToType(), proc.Mp())
				defer out.Free()
				require.NoError(t, out.PreExtendAndReset(3))
				err := operatorUnaryMinus[int64]([]*vector.Vector{in}, out, proc, 3, selection)
				if mode == "overflow" {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "%v", err)
					return
				}
				require.NoError(t, err)
				require.Equal(t, 3, out.GetResultVector().Length())
				for i := uint64(0); i < 3; i++ {
					require.True(t, out.GetResultVector().GetNulls().Contains(i))
				}
			})
		}
	}
}
