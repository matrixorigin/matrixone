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

// Test_BuiltInConcat tests the CONCAT function with multiple parameters
// This is a complex function that calls many other string handling functions
func Test_BuiltInConcat(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test concatenating 2 strings
	{
		tc := tcTemp{
			info: "select concat(str1, str2)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"Hello", "Good", "", "ABC"}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"World", "Morning", "Test", "123"}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"HelloWorld", "GoodMorning", "Test", "ABC123"}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInConcat)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test concatenating 3 strings
	{
		tc := tcTemp{
			info: "select concat(str1, str2, str3)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"A", "Hello", ""}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"B", " ", "X"}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"C", "World", "Y"}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"ABC", "Hello World", "XY"}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInConcat)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test concat with NULL (any NULL makes result NULL)
	{
		tc := tcTemp{
			info: "select concat(str1, str2) with NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"Hello", "Good", ""}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"World", "", "Test"}, []bool{false, true, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"HelloWorld", "", ""}, []bool{false, true, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInConcat)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test concat with 4 strings (testing multi-parameter handling)
	{
		tc := tcTemp{
			info: "select concat(str1, str2, str3, str4)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"A", "Matrix"}, []bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"B", "One"}, []bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"C", " "}, []bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"D", "Database"}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"ABCD", "MatrixOne Database"}, []bool{false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInConcat)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test concat with empty strings
	{
		tc := tcTemp{
			info: "select concat with empty strings",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"", "Hello", ""}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"", "", "World"}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"", "Hello", "World"}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInConcat)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_ConcatWs tests CONCAT_WS function (concat with separator)
// This is a complex function with conditional logic for NULL handling
func Test_ConcatWs(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test concat_ws with 2 strings
	{
		tc := tcTemp{
			info: "select concat_ws(sep, str1, str2)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{",", "-", " | "}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"A", "Hello", "X"}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"B", "World", "Y"}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"A,B", "Hello-World", "X | Y"}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, ConcatWs)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test concat_ws with 3 strings
	{
		tc := tcTemp{
			info: "select concat_ws(sep, str1, str2, str3)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{",", "-"}, []bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"A", "a"}, []bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"B", "b"}, []bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"C", "c"}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"A,B,C", "a-b-c"}, []bool{false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, ConcatWs)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test concat_ws with NULL separator (result is NULL)
	{
		tc := tcTemp{
			info: "select concat_ws(NULL, str1, str2)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"", ""}, []bool{true, true}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"A", "Hello"}, []bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"B", "World"}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"", ""}, []bool{true, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, ConcatWs)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test concat_ws skipping NULL values in strings (but not separator)
	{
		tc := tcTemp{
			info: "select concat_ws(sep, str1, NULL, str2)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{",", "-"}, []bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"A", "a"}, []bool{false, false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"", ""}, []bool{true, true}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"C", "c"}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				// NULL values are skipped, only non-NULL are concatenated
				[]string{"A,C", "a-c"}, []bool{false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, ConcatWs)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test concat_ws with all NULL strings (result is empty string)
	{
		tc := tcTemp{
			info: "select concat_ws(sep, NULL, NULL)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{","}, []bool{false}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""}, []bool{true}),
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{""}, []bool{true}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{""}, []bool{true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, ConcatWs)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_SubStringWith3Args tests SUBSTRING function with start and length
// This is a complex function with different logic for positive/negative start positions
func Test_SubStringWith3Args(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test substring with positive start position
	{
		tc := tcTemp{
			info: "select substring(str, start, length) with positive start",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"HelloWorld", "MatrixOne", "Database"}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1, 1, 5}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5, 6, 4}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"Hello", "Matrix", "base"}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, SubStringWith3Args)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test substring with negative start position (from end)
	{
		tc := tcTemp{
			info: "select substring(str, start, length) with negative start",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"HelloWorld", "MatrixOne", "Database"}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-5, -3, -4}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5, 3, 4}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"World", "One", "base"}, []bool{false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, SubStringWith3Args)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test substring with start position 0 (returns empty string)
	{
		tc := tcTemp{
			info: "select substring(str, 0, length)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"HelloWorld"}, []bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{0}, []bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{5}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{""}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, SubStringWith3Args)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test substring with NULL values
	{
		tc := tcTemp{
			info: "select substring(str, start, length) with NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"Hello", "", "World"}, []bool{false, true, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1, 1, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{3, 5, 3}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"Hel", "", ""}, []bool{false, true, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, SubStringWith3Args)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test substring with length 0
	{
		tc := tcTemp{
			info: "select substring(str, start, 0)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"HelloWorld"}, []bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1}, []bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{0}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{""}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, SubStringWith3Args)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test substring with very large length
	{
		tc := tcTemp{
			info: "select substring(str, start, very_large_length)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"Hello", "World"}, []bool{false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{2, 1}, []bool{false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{1000, 1000}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"ello", "World"}, []bool{false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, SubStringWith3Args)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_BuiltInDateDiff tests DATEDIFF function
// This tests date arithmetic which calls many date handling functions
func Test_BuiltInDateDiff(t *testing.T) {
	proc := testutil.NewProcess(t)

	{
		tc := tcTemp{
			info: "select datediff(date1, date2)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{100, 200, 365, 1000}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{90, 190, 365, 900}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{10, 10, 0, 100}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInDateDiff)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test datediff with NULL
	{
		tc := tcTemp{
			info: "select datediff(date1, date2) with NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{100, 0, 0}, []bool{false, true, false}),
				NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{90, 200, 0}, []bool{false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{10, 0, 0}, []bool{false, true, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInDateDiff)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test datediff with negative result
	{
		tc := tcTemp{
			info: "select datediff where date1 < date2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{100, 50}, []bool{false, false}),
				NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{200, 100}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{-100, -50}, []bool{false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInDateDiff)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Test_BuiltInChar tests CHAR function
func Test_BuiltInChar(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Test CHAR with basic ASCII codes (3 arguments)
	{
		tc := tcTemp{
			info: "select char(65, 66, 67)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{65},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{66},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{67},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"ABC"},
				[]bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInChar)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test CHAR with single argument
	{
		tc := tcTemp{
			info: "select char(65)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{65},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"A"},
				[]bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInChar)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test CHAR with multiple arguments (Hello)
	{
		tc := tcTemp{
			info: "select char(72, 101, 108, 108, 111)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{72},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{101},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{108},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{108},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{111},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"Hello"},
				[]bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInChar)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test CHAR with NULL (any NULL makes result NULL)
	{
		tc := tcTemp{
			info: "select char(65, 66, NULL)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{65},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{66},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{67},
					[]bool{true}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{""},
				[]bool{true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInChar)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test CHAR with digits
	{
		tc := tcTemp{
			info: "select char(48, 49, 50)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{48},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{49},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{50},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"012"},
				[]bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInChar)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Test CHAR with UTF-8 multi-byte character (Chinese character "中")
	{
		tc := tcTemp{
			info: "select char(228, 184, 173)",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{228},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{184},
					[]bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{173},
					[]bool{false}),
			},
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"中"},
				[]bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInChar)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}
