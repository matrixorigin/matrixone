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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// Test_DivFn_Float32 tests float32 division
func Test_DivFn_Float32(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select float32_col1 / float32_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{10.0, 100.0, 0.0, 15.0}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{2.0, 4.0, 5.0, 3.0}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{5.0, 25.0, 0.0, 5.0}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, divFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test division by zero (should return error)
	{
		tc := tcTemp{
			info: "select float32_col / 0",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{10.0}, []bool{false}),
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{0.0}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), true,
				[]float32{0.0}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, divFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test with NULL
	{
		tc := tcTemp{
			info: "select float32_col1 / float32_col2 with NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{10.0, 0.0}, []bool{false, true}),
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{2.0, 5.0}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{5.0, 0.0}, []bool{false, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, divFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_DivFn_Float64 tests float64 division
func Test_DivFn_Float64(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select float64_col1 / float64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{10.0, 100.0, 0.0, 22.5, 1.0}, []bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{2.0, 4.0, 5.0, 4.5, 3.0}, []bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{5.0, 25.0, 0.0, 5.0, 0.3333333333333333}, []bool{false, false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, divFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test division by zero (should return error)
	{
		tc := tcTemp{
			info: "select float64_col / 0",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{10.0}, []bool{false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{0.0}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), true,
				[]float64{0.0}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, divFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_DivFn_ArrayFloat32 tests array_float32 element-wise division
func Test_DivFn_ArrayFloat32(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select array_float32_col1 / array_float32_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(),
					[][]float32{
						{10.0, 20.0, 30.0},
						{100.0, 50.0, 25.0},
					},
					[]bool{false, false}),
				NewFunctionTestInput(types.T_array_float32.ToType(),
					[][]float32{
						{2.0, 4.0, 5.0},
						{10.0, 5.0, 5.0},
					},
					[]bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{
					{5.0, 5.0, 6.0},
					{10.0, 10.0, 5.0},
				},
				[]bool{false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, divFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_DivFn_ArrayFloat64 tests array_float64 element-wise division
func Test_DivFn_ArrayFloat64(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select array_float64_col1 / array_float64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float64.ToType(),
					[][]float64{
						{10.0, 20.0, 30.0},
						{100.0, 50.0, 25.0},
					},
					[]bool{false, false}),
				NewFunctionTestInput(types.T_array_float64.ToType(),
					[][]float64{
						{2.0, 4.0, 5.0},
						{10.0, 5.0, 5.0},
					},
					[]bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_array_float64.ToType(), false,
				[][]float64{
					{5.0, 5.0, 6.0},
					{10.0, 10.0, 5.0},
				},
				[]bool{false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, divFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_IntegerDivFn tests integer division (DIV operator)
func Test_IntegerDivFn(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test float32 integer division
	{
		tc := tcTemp{
			info: "select float32_col1 DIV float32_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{10.5, 25.7, 100.0, 7.2}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{2.0, 3.0, 7.0, 2.5}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{5, 8, 14, 2}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test float64 integer division
	{
		tc := tcTemp{
			info: "select float64_col1 DIV float64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{10.5, 25.7, 100.0, 7.2}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{2.0, 3.0, 7.0, 2.5}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{5, 8, 14, 2}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test with NULL values
	{
		tc := tcTemp{
			info: "select float64_col1 DIV float64_col2 with NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{10.0, 0.0}, []bool{false, true}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{2.0, 3.0}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{5, 0}, []bool{false, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_ModFn tests modulo operation
func Test_ModFn(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test int8 modulo
	{
		tc := tcTemp{
			info: "select int8_col1 % int8_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{10, 25, 100, -10, 7}, []bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{3, 7, 13, 3, -2}, []bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int8.ToType(), false,
				[]int8{1, 4, 9, -1, 1}, []bool{false, false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, modFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test int64 modulo
	{
		tc := tcTemp{
			info: "select int64_col1 % int64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1000, 2500, 10000, -1000}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{7, 13, 127, 7}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				// 1000 % 7 = 6, 2500 % 13 = 4, 10000 % 127 = 94, -1000 % 7 = -6
				[]int64{6, 4, 94, -6}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, modFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test uint64 modulo
	{
		tc := tcTemp{
			info: "select uint64_col1 % uint64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{1000, 2500, 10000}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{7, 13, 127}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_uint64.ToType(), false,
				// 1000 % 7 = 6, 2500 % 13 = 4, 10000 % 127 = 94
				[]uint64{6, 4, 94}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, modFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test float32 modulo
	{
		tc := tcTemp{
			info: "select float32_col1 % float32_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{10.5, 7.0, 100.0}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{3.0, 3.0, 13.0}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_float32.ToType(), false,
				[]float32{
					float32(math.Mod(10.5, 3.0)),   // 1.5
					float32(math.Mod(7.0, 3.0)),    // 1.0
					float32(math.Mod(100.0, 13.0)), // 9.0
				}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, modFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test float64 modulo
	{
		tc := tcTemp{
			info: "select float64_col1 % float64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{10.5, 7.0, 100.0, 8.0}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{3.0, 3.0, 13.0, 2.5}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{
					math.Mod(10.5, 3.0),   // 1.5
					math.Mod(7.0, 3.0),    // 1.0
					math.Mod(100.0, 13.0), // 9.0
					math.Mod(8.0, 2.5),    // 0.5
				}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, modFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test with NULL values
	{
		tc := tcTemp{
			info: "select int64_col1 % int64_col2 with NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{100, 0}, []bool{false, true}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{7, 13}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{2, 0}, []bool{false, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, modFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}
