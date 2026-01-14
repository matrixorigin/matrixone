// Copyright 2021 - 2022 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// TestShouldUseTypeMismatchPath tests the type mismatch detection function
func TestShouldUseTypeMismatchPath(t *testing.T) {
	mp := mpool.MustNewZero()

	tests := []struct {
		name     string
		t1       types.T
		t2       types.T
		expected bool
	}{
		{"int64 vs int64 - same type", types.T_int64, types.T_int64, false},
		{"int64 vs float64 - mismatch", types.T_int64, types.T_float64, true},
		{"float64 vs int64 - mismatch", types.T_float64, types.T_int64, true},
		{"int32 vs int64 - mismatch", types.T_int32, types.T_int64, true},
		{"uint64 vs float64 - mismatch", types.T_uint64, types.T_float64, true},
		{"int8 vs uint8 - mismatch", types.T_int8, types.T_uint8, true},
		{"float32 vs float64 - mismatch", types.T_float32, types.T_float64, true},
		{"varchar vs int64 - not numeric", types.T_varchar, types.T_int64, false},
		{"int64 vs varchar - not numeric", types.T_int64, types.T_varchar, false},
		{"varchar vs varchar - not numeric", types.T_varchar, types.T_varchar, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v1 := vector.NewVec(tt.t1.ToType())
			v2 := vector.NewVec(tt.t2.ToType())
			defer v1.Free(mp)
			defer v2.Free(mp)

			result := shouldUseTypeMismatchPath(v1, v2)
			require.Equal(t, tt.expected, result, "shouldUseTypeMismatchPath(%v, %v)", tt.t1, tt.t2)
		})
	}
}

// TestIsNumericType tests the numeric type detection function
func TestIsNumericType(t *testing.T) {
	tests := []struct {
		typ      types.T
		expected bool
	}{
		{types.T_int8, true},
		{types.T_int16, true},
		{types.T_int32, true},
		{types.T_int64, true},
		{types.T_uint8, true},
		{types.T_uint16, true},
		{types.T_uint32, true},
		{types.T_uint64, true},
		{types.T_float32, true},
		{types.T_float64, true},
		{types.T_varchar, false},
		{types.T_char, false},
		{types.T_date, false},
		{types.T_datetime, false},
		{types.T_decimal64, false},
		{types.T_decimal128, false},
		{types.T_bool, false},
	}

	for _, tt := range tests {
		t.Run(tt.typ.String(), func(t *testing.T) {
			result := isNumericType(tt.typ)
			require.Equal(t, tt.expected, result, "isNumericType(%v)", tt.typ)
		})
	}
}

// TestGetAsFloat64Slice tests the float64 conversion function
func TestGetAsFloat64Slice(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()

	t.Run("int64 to float64", func(t *testing.T) {
		v := testutil.NewInt64Vector(3, types.T_int64.ToType(), mp, false, []int64{1, 2, 3})
		defer v.Free(mp)
		result := getAsFloat64Slice(v)
		require.Equal(t, []float64{1.0, 2.0, 3.0}, result)
	})

	t.Run("int32 to float64", func(t *testing.T) {
		v := testutil.NewInt32Vector(3, types.T_int32.ToType(), mp, false, []int32{10, 20, 30})
		defer v.Free(mp)
		result := getAsFloat64Slice(v)
		require.Equal(t, []float64{10.0, 20.0, 30.0}, result)
	})

	t.Run("int16 to float64", func(t *testing.T) {
		v := testutil.NewInt16Vector(3, types.T_int16.ToType(), mp, false, []int16{10, 20, 30})
		defer v.Free(mp)
		result := getAsFloat64Slice(v)
		require.Equal(t, []float64{10.0, 20.0, 30.0}, result)
	})

	t.Run("int8 to float64", func(t *testing.T) {
		v := testutil.NewInt8Vector(3, types.T_int8.ToType(), mp, false, []int8{1, 2, 3})
		defer v.Free(mp)
		result := getAsFloat64Slice(v)
		require.Equal(t, []float64{1.0, 2.0, 3.0}, result)
	})

	t.Run("float64 direct", func(t *testing.T) {
		v := testutil.NewFloat64Vector(3, types.T_float64.ToType(), mp, false, []float64{1.5, 2.5, 3.5})
		defer v.Free(mp)
		result := getAsFloat64Slice(v)
		require.Equal(t, []float64{1.5, 2.5, 3.5}, result)
	})

	t.Run("float32 to float64", func(t *testing.T) {
		v := testutil.NewFloat32Vector(3, types.T_float32.ToType(), mp, false, []float32{1.5, 2.5, 3.5})
		defer v.Free(mp)
		result := getAsFloat64Slice(v)
		require.InDeltaSlice(t, []float64{1.5, 2.5, 3.5}, result, 0.0001)
	})

	t.Run("uint64 to float64", func(t *testing.T) {
		v := testutil.NewUInt64Vector(3, types.T_uint64.ToType(), mp, false, []uint64{100, 200, 300})
		defer v.Free(mp)
		result := getAsFloat64Slice(v)
		require.Equal(t, []float64{100.0, 200.0, 300.0}, result)
	})

	t.Run("uint32 to float64", func(t *testing.T) {
		v := testutil.NewUInt32Vector(3, types.T_uint32.ToType(), mp, false, []uint32{100, 200, 300})
		defer v.Free(mp)
		result := getAsFloat64Slice(v)
		require.Equal(t, []float64{100.0, 200.0, 300.0}, result)
	})

	t.Run("uint16 to float64", func(t *testing.T) {
		v := testutil.NewUInt16Vector(3, types.T_uint16.ToType(), mp, false, []uint16{100, 200, 300})
		defer v.Free(mp)
		result := getAsFloat64Slice(v)
		require.Equal(t, []float64{100.0, 200.0, 300.0}, result)
	})

	t.Run("uint8 to float64", func(t *testing.T) {
		v := testutil.NewUInt8Vector(3, types.T_uint8.ToType(), mp, false, []uint8{10, 20, 30})
		defer v.Free(mp)
		result := getAsFloat64Slice(v)
		require.Equal(t, []float64{10.0, 20.0, 30.0}, result)
	})

	t.Run("unsupported type returns nil", func(t *testing.T) {
		v := vector.NewVec(types.T_varchar.ToType())
		defer v.Free(mp)
		result := getAsFloat64Slice(v)
		require.Nil(t, result)
	})
}

// TestLessEqualFnWithTypeMismatch tests lessEqualFn with mismatched numeric types
func TestLessEqualFnWithTypeMismatch(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test: int64 <= float64 (type mismatch scenario)
	t.Run("int64 <= float64", func(t *testing.T) {
		tc := tcTemp{
			info: "int64 <= float64 type mismatch",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1, 2, 3, 5}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{2.0, 2.0, 2.0, 2.0}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, true, false, false}, []bool{false, false, false, false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, lessEqualFn)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	// Test: float64 <= int64 (type mismatch scenario)
	t.Run("float64 <= int64", func(t *testing.T) {
		tc := tcTemp{
			info: "float64 <= int64 type mismatch",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1.5, 2.0, 2.5, 3.0}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{2, 2, 2, 2}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, true, false, false}, []bool{false, false, false, false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, lessEqualFn)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	// Test: int32 <= float64 with nulls
	t.Run("int32 <= float64 with nulls", func(t *testing.T) {
		tc := tcTemp{
			info: "int32 <= float64 with nulls",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{1, 2, 3}, []bool{false, true, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{2.0, 2.0, 2.0}, []bool{false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, false}, []bool{false, true, true}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, lessEqualFn)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})
}

// TestLessThanFnWithTypeMismatch tests lessThanFn with mismatched numeric types
func TestLessThanFnWithTypeMismatch(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("int64 < float64", func(t *testing.T) {
		tc := tcTemp{
			info: "int64 < float64 type mismatch",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1, 2, 3}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{2.0, 2.0, 2.0}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, false}, []bool{false, false, false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, lessThanFn)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})
}

// TestGreatEqualFnWithTypeMismatch tests greatEqualFn with mismatched numeric types
func TestGreatEqualFnWithTypeMismatch(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("int64 >= float64", func(t *testing.T) {
		tc := tcTemp{
			info: "int64 >= float64 type mismatch",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1, 2, 3}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{2.0, 2.0, 2.0}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, true, true}, []bool{false, false, false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, greatEqualFn)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})
}

// TestGreatThanFnWithTypeMismatch tests greatThanFn with mismatched numeric types
func TestGreatThanFnWithTypeMismatch(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("int64 > float64", func(t *testing.T) {
		tc := tcTemp{
			info: "int64 > float64 type mismatch",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1, 2, 3}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{2.0, 2.0, 2.0}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, false, true}, []bool{false, false, false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, greatThanFn)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})
}

// TestEqualFnWithTypeMismatch tests equalFn with mismatched numeric types
func TestEqualFnWithTypeMismatch(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("int64 == float64", func(t *testing.T) {
		tc := tcTemp{
			info: "int64 == float64 type mismatch",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1, 2, 3}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1.0, 2.5, 3.0}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, true}, []bool{false, false, false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, equalFn)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})
}

// TestNotEqualFnWithTypeMismatch tests notEqualFn with mismatched numeric types
func TestNotEqualFnWithTypeMismatch(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("int64 != float64", func(t *testing.T) {
		tc := tcTemp{
			info: "int64 != float64 type mismatch",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1, 2, 3}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1.0, 2.5, 3.0}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, true, false}, []bool{false, false, false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, notEqualFn)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})
}

// TestNumericCompareWithConstVector tests comparison with const vectors
func TestNumericCompareWithConstVector(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test: const int64 <= float64 vector
	t.Run("const int64 <= float64 vector", func(t *testing.T) {
		tc := tcTemp{
			info: "const int64 <= float64 vector",
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_int64.ToType(),
					[]int64{2}, []bool{false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1.0, 2.0, 3.0}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, true, true}, []bool{false, false, false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, lessEqualFn)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	// Test: int64 vector <= const float64
	t.Run("int64 vector <= const float64", func(t *testing.T) {
		tc := tcTemp{
			info: "int64 vector <= const float64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1, 2, 3}, []bool{false, false, false}),
				NewFunctionTestConstInput(types.T_float64.ToType(),
					[]float64{2.0}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, true, false}, []bool{false, false, false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, lessEqualFn)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	// Test: both const
	t.Run("both const int64 <= float64", func(t *testing.T) {
		tc := tcTemp{
			info: "both const int64 <= float64",
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_int64.ToType(),
					[]int64{2}, []bool{false}),
				NewFunctionTestConstInput(types.T_float64.ToType(),
					[]float64{3.0}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true}, []bool{false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, lessEqualFn)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})
}

// TestNumericCompareWithAllNulls tests comparison when all values are null
func TestNumericCompareWithAllNulls(t *testing.T) {
	proc := testutil.NewProcess(t)

	t.Run("const null int64 <= float64", func(t *testing.T) {
		tc := tcTemp{
			info: "const null int64 <= float64",
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_int64.ToType(),
					[]int64{0}, []bool{true}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1.0, 2.0, 3.0}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, false, false}, []bool{true, true, true}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, lessEqualFn)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	t.Run("int64 <= const null float64", func(t *testing.T) {
		tc := tcTemp{
			info: "int64 <= const null float64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1, 2, 3}, []bool{false, false, false}),
				NewFunctionTestConstInput(types.T_float64.ToType(),
					[]float64{0}, []bool{true}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, false, false}, []bool{true, true, true}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, lessEqualFn)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})
}

// TestVariousNumericTypeMismatches tests various numeric type combinations
func TestVariousNumericTypeMismatches(t *testing.T) {
	proc := testutil.NewProcess(t)

	// int8 vs float64
	t.Run("int8 <= float64", func(t *testing.T) {
		tc := tcTemp{
			info: "int8 <= float64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{1, 2, 3}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{2.0, 2.0, 2.0}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, true, false}, []bool{false, false, false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, lessEqualFn)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	// uint32 vs int64
	t.Run("uint32 <= int64", func(t *testing.T) {
		tc := tcTemp{
			info: "uint32 <= int64",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint32.ToType(),
					[]uint32{1, 2, 3}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{2, 2, 2}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, true, false}, []bool{false, false, false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, lessEqualFn)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})

	// float32 vs int32
	t.Run("float32 <= int32", func(t *testing.T) {
		tc := tcTemp{
			info: "float32 <= int32",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{1.5, 2.0, 2.5}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_int32.ToType(),
					[]int32{2, 2, 2}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, true, false}, []bool{false, false, false}),
		}
		fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, lessEqualFn)
		s, info := fcTC.Run()
		require.True(t, s, info)
	})
}
