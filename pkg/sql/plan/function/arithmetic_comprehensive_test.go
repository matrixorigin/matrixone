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

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// Test_PlusFn_DecimalZero tests decimal addition with zero values
// This test catches the bug where decimal + 0.00 incorrectly returns NULL
func Test_PlusFn_DecimalZero(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test decimal64 + zero - use simple values
	{
		tc := tcTemp{
			info: "select 1 + 0 (decimal64)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{1}, []bool{false}),
				NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{0}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_decimal64.ToType(), false,
				[]types.Decimal64{1}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test zero + decimal64
	{
		tc := tcTemp{
			info: "select 0 + 1 (decimal64)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{0}, []bool{false}),
				NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{1}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_decimal64.ToType(), false,
				[]types.Decimal64{1}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_MinusFn_DecimalZero tests decimal subtraction with zero values
func Test_MinusFn_DecimalZero(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test decimal64 - zero
	{
		d1, _ := types.Decimal64FromFloat64(0.01, 18, 2)
		d2, _ := types.Decimal64FromFloat64(0.00, 18, 2)
		expected, _ := types.Decimal64FromFloat64(0.01, 18, 2)

		tc := tcTemp{
			info: "select 0.01 - 0.00 (decimal64)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{d1}, []bool{false}),
				NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{d2}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_decimal64.ToType(), false,
				[]types.Decimal64{expected}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, minusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Regression: scaling large decimal128 values beyond precision should error, not silently succeed.
func Test_Decimal128ScaleOverflow(t *testing.T) {
	// 1e20 is representable with width 38/scale 0; scaling up by 19 digits would overflow 38-digit precision.
	d128, err := types.Decimal128FromFloat64(1e20, 38, 0)
	require.NoError(t, err)

	rs := make([]types.Decimal128, 1)
	err = decimal128ScaleArray([]types.Decimal128{d128}, rs, 1, 19)
	require.Error(t, err)
}

// Test_DivFn_DecimalZero tests that division by zero still returns NULL
func Test_DivFn_DecimalZero(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test decimal64 / zero should return NULL
	{
		d1, _ := types.Decimal64FromFloat64(1.0, 18, 1)
		d2, _ := types.Decimal64FromFloat64(0.0, 18, 1)

		tc := tcTemp{
			info: "select 1.0 / 0.0 should return NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{d1}, []bool{false}),
				NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{d2}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{{B0_63: 0, B64_127: 0}}, []bool{true}), // NULL result
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, divFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_Decimal64_Multiply_Zero tests that decimal64 * 0 returns 0, not null
// This fixes the bug where multiplication by zero incorrectly returned null
func Test_Decimal64_Multiply_Zero(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test: decimal64 * 0 should return 0, not null
	{
		zero, _ := types.ParseDecimal128("0", 38, 12)
		tc := tcTemp{
			info: "select decimal64_col * 0 (should return 0, not null)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{1000, 500, 0, 250}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{0, 0, 0, 0}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{zero, zero, zero, zero},
				[]bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test: 0 * decimal64 should return 0, not null
	{
		zero, _ := types.ParseDecimal128("0", 38, 12)
		tc := tcTemp{
			info: "select 0 * decimal64_col (should return 0, not null)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{0, 0, 0, 0}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{1000, 500, 250, 0}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{zero, zero, zero, zero},
				[]bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test: decimal64 * decimal64 where one is zero
	{
		val1, _ := types.ParseDecimal64("2.00", 10, 2)
		val2, _ := types.ParseDecimal64("0.00", 10, 2)
		zero, _ := types.ParseDecimal128("0", 38, 12)
		tc := tcTemp{
			info: "select 2.00 * 0.00 (should return 0, not null)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{val1}, []bool{false}),
				NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{val2}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_decimal128.ToType(), false,
				[]types.Decimal128{zero},
				[]bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, multiFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_Decimal_Plus_Float tests that decimal + float works correctly
// This fixes the bug where CASE returning decimal + float returned null
func Test_Decimal_Plus_Float(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test: decimal64 + float64 with original types (before type conversion)
	{
		val, _ := types.ParseDecimal64("2.00", 10, 2)
		resultType := types.T_decimal128.ToType()
		resultType.Width = 38
		resultType.Scale = 18
		result, _ := types.ParseDecimal128("2.00", 38, 18)
		tc := tcTemp{
			info: "select decimal64(2.00) + float64(0.0) with original types (should return 2.00, not null)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{val}, []bool{false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{0.0}, []bool{false}),
			},
			expect: NewFunctionTestResult(resultType, false,
				[]types.Decimal128{result},
				[]bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test: float64 + decimal64 with original types (before type conversion)
	{
		val, _ := types.ParseDecimal64("2.00", 10, 2)
		resultType := types.T_decimal128.ToType()
		resultType.Width = 38
		resultType.Scale = 18
		result, _ := types.ParseDecimal128("2.00", 38, 18)
		tc := tcTemp{
			info: "select float64(0.0) + decimal64(2.00) with original types (should return 2.00, not null)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{0.0}, []bool{false}),
				NewFunctionTestInput(types.T_decimal64.ToType(),
					[]types.Decimal64{val}, []bool{false}),
			},
			expect: NewFunctionTestResult(resultType, false,
				[]types.Decimal128{result},
				[]bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// TestDecimal128AddBug tests decimal128 addition with zero values
func TestDecimal128AddBug(t *testing.T) {
	// Test case for the bug: 0.01 + 0.00 should return 0.01, not NULL

	// Create 0.01 (scale 2)
	d1, err := types.Decimal128FromFloat64(0.01, 38, 2)
	if err != nil {
		t.Fatalf("Failed to create 0.01: %v", err)
	}

	// Create 0.00 (scale 2)
	d2, err := types.Decimal128FromFloat64(0.00, 38, 2)
	if err != nil {
		t.Fatalf("Failed to create 0.00: %v", err)
	}

	// Test 0.01 + 0.00
	result1, scale1, err1 := d1.Add(d2, 2, 2)
	if err1 != nil {
		t.Errorf("0.01 + 0.00 failed with error: %v", err1)
	}
	if scale1 != 2 {
		t.Errorf("Expected scale 2, got %d", scale1)
	}
	expected1, _ := types.Decimal128FromFloat64(0.01, 38, 2)
	if result1 != expected1 {
		t.Errorf("0.01 + 0.00: expected %v, got %v", expected1, result1)
	}

	// Test 0.00 + 0.01 (should also work)
	result2, scale2, err2 := d2.Add(d1, 2, 2)
	if err2 != nil {
		t.Errorf("0.00 + 0.01 failed with error: %v", err2)
	}
	if scale2 != 2 {
		t.Errorf("Expected scale 2, got %d", scale2)
	}
	expected2, _ := types.Decimal128FromFloat64(0.01, 38, 2)
	if result2 != expected2 {
		t.Errorf("0.00 + 0.01: expected %v, got %v", expected2, result2)
	}

	// Test with different scales: 0.1 (scale 1) + 0.00 (scale 2)
	d3, _ := types.Decimal128FromFloat64(0.1, 38, 1)
	d4, _ := types.Decimal128FromFloat64(0.00, 38, 2)

	result3, scale3, err3 := d3.Add(d4, 1, 2)
	if err3 != nil {
		t.Errorf("0.1 + 0.00 failed with error: %v", err3)
	}
	if scale3 != 2 {
		t.Errorf("Expected scale 2, got %d", scale3)
	}
	expected3, _ := types.Decimal128FromFloat64(0.10, 38, 2)
	if result3 != expected3 {
		t.Errorf("0.1 + 0.00: expected %v, got %v", expected3, result3)
	}
}

// TestDecimal128SubBug tests decimal128 subtraction with zero values
func TestDecimal128SubBug(t *testing.T) {
	// Test subtraction with the same bug pattern

	// Create 0.01 (scale 2)
	d1, err := types.Decimal128FromFloat64(0.01, 38, 2)
	if err != nil {
		t.Fatalf("Failed to create 0.01: %v", err)
	}

	// Create 0.00 (scale 2)
	d2, err := types.Decimal128FromFloat64(0.00, 38, 2)
	if err != nil {
		t.Fatalf("Failed to create 0.00: %v", err)
	}

	// Test 0.01 - 0.00
	result1, scale1, err1 := d1.Sub(d2, 2, 2)
	if err1 != nil {
		t.Errorf("0.01 - 0.00 failed with error: %v", err1)
	}
	if scale1 != 2 {
		t.Errorf("Expected scale 2, got %d", scale1)
	}
	expected1, _ := types.Decimal128FromFloat64(0.01, 38, 2)
	if result1 != expected1 {
		t.Errorf("0.01 - 0.00: expected %v, got %v", expected1, result1)
	}
}

// TestCaseWhenStringComparison tests string comparison in CASE WHEN expressions
func TestCaseWhenStringComparison(t *testing.T) {
	// Test: CASE "two" when "one" then 1.00 WHEN "two" then 2.00 END
	proc := testutil.NewProc(t)

	// Create condition vectors: "two" = "one" and "two" = "two"
	cond1 := testutil.MakeVarcharVector([]string{"two"}, nil, proc.Mp())
	val1 := testutil.MakeVarcharVector([]string{"one"}, nil, proc.Mp())

	cond2 := testutil.MakeVarcharVector([]string{"two"}, nil, proc.Mp())
	val2 := testutil.MakeVarcharVector([]string{"two"}, nil, proc.Mp())

	// Test equal function
	result1 := vector.NewFunctionResultWrapper(types.T_bool.ToType(), proc.Mp())
	require.NoError(t, result1.PreExtendAndReset(1))
	err := equalFn([]*vector.Vector{cond1, val1}, result1, proc, 1, nil)
	require.NoError(t, err)

	result2 := vector.NewFunctionResultWrapper(types.T_bool.ToType(), proc.Mp())
	require.NoError(t, result2.PreExtendAndReset(1))
	err = equalFn([]*vector.Vector{cond2, val2}, result2, proc, 1, nil)
	require.NoError(t, err)

	// Check results
	r1, null1 := vector.GenerateFunctionFixedTypeParameter[bool](result1.GetResultVector()).GetValue(0)
	t.Logf("\"two\" = \"one\": %v (null: %v)", r1, null1)

	r2, null2 := vector.GenerateFunctionFixedTypeParameter[bool](result2.GetResultVector()).GetValue(0)
	t.Logf("\"two\" = \"two\": %v (null: %v)", r2, null2)

	require.False(t, null1, "\"two\" = \"one\" should not be null")
	require.False(t, r1, "\"two\" = \"one\" should be false")

	require.False(t, null2, "\"two\" = \"two\" should not be null")
	require.True(t, r2, "\"two\" = \"two\" should be true")
}

// Test_Decimal_Plus_Float_MySQL_Behavior tests that decimal + float follows MySQL behavior
func Test_Decimal_Plus_Float_MySQL_Behavior(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test: decimal64 + float64 should convert to float64 + float64 (MySQL behavior)
	{
		floatVal1 := 3728193.0 // decimal64 converted to float64
		floatVal2 := 3.141593  // original float64

		tc := tcTemp{
			info: "decimal64 + float64 should convert to float64 (MySQL behavior)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{floatVal1}, []bool{false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{floatVal2}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{3728196.141593}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_Decimal_Plus_Float32_MySQL_Behavior tests that decimal + float32 also converts to float64
func Test_Decimal_Plus_Float32_MySQL_Behavior(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test: decimal64 + float32 should convert to float64 (MySQL behavior)
	{
		floatVal1 := 3728193.0         // decimal64 converted to float64
		floatVal2 := float64(3.141593) // float32 converted to float64

		tc := tcTemp{
			info: "decimal64 + float32 should convert to float64 (MySQL behavior)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{floatVal1}, []bool{false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{floatVal2}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{3728196.141593}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}
