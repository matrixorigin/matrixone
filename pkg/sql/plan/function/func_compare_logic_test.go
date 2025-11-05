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
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// Test_NotFn tests logical NOT operation
func Test_NotFn(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select NOT bool_col",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, true, false}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, true, false, true}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, notFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test with NULL values
	{
		tc := tcTemp{
			info: "select NOT bool_col with NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_bool.ToType(),
					[]bool{true, false, false}, []bool{false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, true, false}, []bool{false, false, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, notFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_EqualFn tests equality comparison
func Test_EqualFn(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test int64 equality
	{
		tc := tcTemp{
			info: "select int64_col1 = int64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10, 20, 30, 40, 50}, []bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10, 25, 30, 35, 50}, []bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, true, false, true}, []bool{false, false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, equalFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test float64 equality
	{
		tc := tcTemp{
			info: "select float64_col1 = float64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1.0, 2.5, 3.0, 0.0}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1.0, 2.0, 3.0, 0.0}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, true, true}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, equalFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test string equality
	{
		tc := tcTemp{
			info: "select varchar_col1 = varchar_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"hello", "world", "test", "abc"}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"hello", "WORLD", "test", "xyz"}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, true, false}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, equalFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test with NULL values
	{
		tc := tcTemp{
			info: "select int64_col1 = int64_col2 with NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10, 20, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10, 0, 30}, []bool{false, true, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, false}, []bool{false, true, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, equalFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_NotEqualFn tests inequality comparison
func Test_NotEqualFn(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select int64_col1 != int64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10, 20, 30, 40}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10, 25, 30, 35}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, true, false, true}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, notEqualFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test string inequality
	{
		tc := tcTemp{
			info: "select varchar_col1 != varchar_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"hello", "world", "test"}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"hello", "WORLD", "test"}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, true, false}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, notEqualFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_GreatThanFn tests greater than comparison
func Test_GreatThanFn(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select int64_col1 > int64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10, 20, 30, 40, 50}, []bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5, 20, 35, 30, 60}, []bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, false, true, false}, []bool{false, false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, greatThanFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test float64 greater than
	{
		tc := tcTemp{
			info: "select float64_col1 > float64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1.5, 2.5, 3.0, 4.5}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1.0, 2.5, 3.5, 4.0}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, false, true}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, greatThanFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test string greater than (lexicographic)
	{
		tc := tcTemp{
			info: "select varchar_col1 > varchar_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"b", "a", "z", "abc"}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"a", "b", "y", "abd"}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, true, false}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, greatThanFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_GreatEqualFn tests greater than or equal comparison
func Test_GreatEqualFn(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select int64_col1 >= int64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10, 20, 30, 40, 50}, []bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5, 20, 35, 30, 60}, []bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, true, false, true, false}, []bool{false, false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, greatEqualFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test float64 greater equal
	{
		tc := tcTemp{
			info: "select float64_col1 >= float64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1.5, 2.5, 3.0, 4.5}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1.0, 2.5, 3.5, 4.0}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, true, false, true}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, greatEqualFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_LessThanFn tests less than comparison
func Test_LessThanFn(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select int64_col1 < int64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10, 20, 30, 40, 50}, []bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{15, 20, 25, 50, 40}, []bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, false, true, false}, []bool{false, false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, lessThanFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test float64 less than
	{
		tc := tcTemp{
			info: "select float64_col1 < float64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1.0, 2.5, 3.5, 4.0}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1.5, 2.5, 3.0, 4.5}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, false, true}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, lessThanFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test string less than
	{
		tc := tcTemp{
			info: "select varchar_col1 < varchar_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"a", "b", "y", "abd"}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"b", "a", "z", "abc"}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, true, false}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, lessThanFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_LessEqualFn tests less than or equal comparison
func Test_LessEqualFn(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select int64_col1 <= int64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10, 20, 30, 40, 50}, []bool{false, false, false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{15, 20, 25, 50, 40}, []bool{false, false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, true, false, true, false}, []bool{false, false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, lessEqualFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test float64 less equal
	{
		tc := tcTemp{
			info: "select float64_col1 <= float64_col2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1.0, 2.5, 3.5, 4.0}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1.5, 2.5, 3.0, 4.5}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, true, false, true}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, lessEqualFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_CompareEdgeCases tests edge cases for comparison operations
func Test_CompareEdgeCases(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test comparing zeros
	{
		tc := tcTemp{
			info: "compare zeros",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{0, 0, 0}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{0, 1, -1}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, false}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, equalFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test comparing negative numbers
	{
		tc := tcTemp{
			info: "compare negative numbers",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					// -10 < -5 is true, -20 < -20 is false, -30 < -40 is false
					[]int64{-10, -20, -30}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-5, -20, -40}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, false}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, lessThanFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test empty strings
	{
		tc := tcTemp{
			info: "compare empty strings",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"", "a", ""}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"", "", "b"}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{true, false, false}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, equalFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}
