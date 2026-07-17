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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// Characterization tests for DIV/MOD boundary semantics, verified against
// MySQL 8.4.8 (t has a=-9223372036854775808 BIGINT, u=18446744073709551615
// BIGINT UNSIGNED, f=1e308 DOUBLE):
//   mysql> SELECT a DIV -1 FROM t;   -- ERROR 1690: BIGINT value is out of range
//   mysql> SELECT a DIV 1 FROM t;    -- ERROR 1690 (DIV works on unsigned
//                                    -- magnitudes; |quotient|=2^63 overflows)
//   mysql> SELECT a DIV 2 FROM t;    -- -4611686018427387904
//   mysql> SELECT f DIV 1 FROM t;    -- ERROR 1690
//   mysql> SELECT -9.223372036854775808e18 DIV 1;  -- ERROR 1690 (magnitude 2^63)
//   mysql> SELECT u DIV 2 FROM t;    -- 9223372036854775807
//   mysql> SELECT a MOD -1 FROM t;   -- 0 (no overflow error for MOD)
//   mysql> SELECT -7 DIV 2, -7 MOD 3, -7.5 MOD 2;  -- -3, -1, -1.5
//   mysql> SELECT 10 DIV 0, 10 MOD 0;              -- NULL, NULL
//
// Known deviation kept out of scope (result-type inference, issue #25751):
// MySQL types `u DIV 1` as BIGINT UNSIGNED and returns 18446744073709551615,
// while MatrixOne's DIV result type is signed BIGINT and raises out-of-range.

func Test_IntegerDivFn_Int64Boundary(t *testing.T) {
	proc := testutil.NewProcess(t)

	// MinInt64 DIV ±1 overflows BIGINT (MySQL ERROR 1690, unsigned-magnitude rule)
	for _, divisor := range []int64{-1, 1} {
		tc := tcTemp{
			info: "select min_int64_col DIV ±1",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{math.MinInt64}, []bool{false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{divisor}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), true,
				[]int64{0}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Neighboring values stay in range
	{
		tc := tcTemp{
			info: "select int64_col DIV int64_col2 near boundary",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{math.MinInt64 + 1, math.MinInt64, math.MinInt64, -7}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-1, 2, -2, 2}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{math.MaxInt64, -4611686018427387904, 4611686018427387904, -3}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// Smaller signed widths are computed in int64 and cannot overflow BIGINT
	{
		tc := tcTemp{
			info: "select min_int8_col DIV -1",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{math.MinInt8}, []bool{false}),
				NewFunctionTestInput(types.T_int8.ToType(),
					[]int8{-1}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{128}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// uint64 quotient above MaxInt64 overflows BIGINT
	{
		tc := tcTemp{
			info: "select max_uint64_col DIV 1",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{math.MaxUint64}, []bool{false}),
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{1}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), true,
				[]int64{0}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
	{
		tc := tcTemp{
			info: "select max_uint64_col DIV 2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{math.MaxUint64}, []bool{false}),
				NewFunctionTestInput(types.T_uint64.ToType(),
					[]uint64{2}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{math.MaxInt64}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_IntegerDivFn_FloatOverflow(t *testing.T) {
	proc := testutil.NewProcess(t)

	// float64 quotient above BIGINT range errors (MySQL: SELECT 1e308 DIV 1 -> ERROR 1690)
	{
		tc := tcTemp{
			info: "select 1e308 DIV 1",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1e308}, []bool{false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), true,
				[]int64{0}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// negative overflow
	{
		tc := tcTemp{
			info: "select -1e19 DIV 1",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{-1e19}, []bool{false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), true,
				[]int64{0}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// a float quotient of exactly -2^63 errors like MySQL (unsigned-magnitude rule)
	{
		tc := tcTemp{
			info: "select -9.223372036854775808e18 DIV 1",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{-float64(1 << 63)}, []bool{false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), true,
				[]int64{0}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// in-range quotients truncate toward zero
	{
		tc := tcTemp{
			info: "select float64_col DIV float64_col2 in range",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{float64(1 << 62), -9e18, 7.9, -7.9}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1, 1, 2, 2}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{1 << 62, -9000000000000000000, 3, -3}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// float32 overflow and normal case
	{
		tc := tcTemp{
			info: "select 3.4e38_float32 DIV 1",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{3.4e38}, []bool{false}),
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{1}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), true,
				[]int64{0}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
	{
		tc := tcTemp{
			info: "select float32_col DIV 3",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{100.5, 0}, []bool{false, false}),
				NewFunctionTestInput(types.T_float32.ToType(),
					[]float32{3, 3}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{33, 0}, []bool{false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// float DIV 0 returns NULL (non-strict SELECT behavior)
	{
		tc := tcTemp{
			info: "select float64_col DIV 0",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{10}, []bool{false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{0}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{0}, []bool{true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, integerDivFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_ModFn_IntegerBoundary(t *testing.T) {
	proc := testutil.NewProcess(t)

	// MinInt64 MOD -1 = 0 (matches MySQL, no overflow error for MOD)
	{
		tc := tcTemp{
			info: "select int64 boundary MOD cases",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{math.MinInt64, math.MinInt64, 7, -7}, []bool{false, false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{-1, 1, 3, 3}, []bool{false, false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{0, 0, 1, -1}, []bool{false, false, false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, modFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// MOD by zero returns NULL (non-strict SELECT behavior)
	{
		tc := tcTemp{
			info: "select int64_col MOD 0",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10, 7}, []bool{false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{0, 3}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{0, 1}, []bool{true, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, modFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// const MOD const (covers the const/const template branch)
	{
		tc := tcTemp{
			info: "select 10 MOD 3",
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_int64.ToType(),
					[]int64{10}, []bool{false}),
				NewFunctionTestConstInput(types.T_int64.ToType(),
					[]int64{3}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{1}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, modFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
	{
		tc := tcTemp{
			info: "select 10 MOD 0",
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_int64.ToType(),
					[]int64{10}, []bool{false}),
				NewFunctionTestConstInput(types.T_int64.ToType(),
					[]int64{0}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{0}, []bool{true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, modFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// const MOD vector (with zero divisor and null)
	{
		tc := tcTemp{
			info: "select 10 MOD int64_col",
			inputs: []FunctionTestInput{
				// the test harness takes the row count from the first parameter,
				// so the const input carries one value per row
				NewFunctionTestConstInput(types.T_int64.ToType(),
					[]int64{10, 10, 10}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{3, 0, 4}, []bool{false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{1, 0, 0}, []bool{false, true, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, modFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// vector MOD const (with null in dividend)
	{
		tc := tcTemp{
			info: "select int64_col MOD 3",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10, 0}, []bool{false, true}),
				NewFunctionTestConstInput(types.T_int64.ToType(),
					[]int64{3}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{1, 0}, []bool{false, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, modFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// vector MOD const zero -> all NULL
	{
		tc := tcTemp{
			info: "select int64_col MOD 0",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_int64.ToType(),
					[]int64{10, 7}, []bool{false, false}),
				NewFunctionTestConstInput(types.T_int64.ToType(),
					[]int64{0}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{0, 0}, []bool{true, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, modFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

func Test_ModFn_FloatSemantics(t *testing.T) {
	proc := testutil.NewProcess(t)

	// float MOD keeps the sign of the dividend (MySQL: -7.5 MOD 2 -> -1.5)
	{
		tc := tcTemp{
			info: "select float64 MOD cases",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{7.5, -7.5}, []bool{false, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{2, 2}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{1.5, -1.5}, []bool{false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, modFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// float MOD 0 returns NULL
	{
		tc := tcTemp{
			info: "select float64_col MOD 0",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{7.5}, []bool{false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{0}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0}, []bool{true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, modFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Branch coverage for specialTemplateForDivFunction with error-returning kernels.
func Test_DivFn_TemplateBranches(t *testing.T) {
	proc := testutil.NewProcess(t)

	// const / const
	{
		tc := tcTemp{
			info: "select 10.0 / 2.0",
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_float64.ToType(),
					[]float64{10}, []bool{false}),
				NewFunctionTestConstInput(types.T_float64.ToType(),
					[]float64{2}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{5}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, divFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// const / const overflow
	{
		tc := tcTemp{
			info: "select 1e308 / 1e-308",
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_float64.ToType(),
					[]float64{1e308}, []bool{false}),
				NewFunctionTestConstInput(types.T_float64.ToType(),
					[]float64{1e-308}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), true,
				[]float64{0}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, divFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// const / vector (with null and zero divisor)
	{
		tc := tcTemp{
			info: "select 10.0 / float64_col",
			inputs: []FunctionTestInput{
				// the test harness takes the row count from the first parameter,
				// so the const input carries one value per row
				NewFunctionTestConstInput(types.T_float64.ToType(),
					[]float64{10, 10, 10}, []bool{false, false, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{2, 0, 4}, []bool{false, false, true}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{5, 0, 0}, []bool{false, true, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, divFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// const / vector without nulls, overflow in loop
	{
		tc := tcTemp{
			info: "select 1e308 / float64_col overflow",
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_float64.ToType(),
					[]float64{1e308, 1e308}, []bool{false, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{2, 1e-308}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), true,
				[]float64{0, 0}, []bool{false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, divFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// NULL const / vector
	{
		tc := tcTemp{
			info: "select NULL / float64_col",
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_float64.ToType(),
					[]float64{0, 0}, []bool{true, true}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{2, 4}, []bool{false, false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0, 0}, []bool{true, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, divFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// vector / const (zero divisor -> all NULL)
	{
		tc := tcTemp{
			info: "select float64_col / 0.0",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1, 2}, []bool{false, false}),
				NewFunctionTestConstInput(types.T_float64.ToType(),
					[]float64{0}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0, 0}, []bool{true, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, divFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// vector with nulls / const
	{
		tc := tcTemp{
			info: "select float64_col / 2.0 with NULL",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1, 0}, []bool{false, true}),
				NewFunctionTestConstInput(types.T_float64.ToType(),
					[]float64{2}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0.5, 0}, []bool{false, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, divFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// vector / const overflow
	{
		tc := tcTemp{
			info: "select float64_col / 1e-308 overflow",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1, 1e308}, []bool{false, false}),
				NewFunctionTestConstInput(types.T_float64.ToType(),
					[]float64{1e-308}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), true,
				[]float64{0, 0}, []bool{false, false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, divFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// vector / vector with nulls and zero divisor
	{
		tc := tcTemp{
			info: "select float64_col1 / float64_col2 mixed",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{10, 10, 0}, []bool{false, false, true}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{2, 0, 5}, []bool{false, false, false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{5, 0, 0}, []bool{false, true, true}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, divFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	// vector / vector overflow
	{
		tc := tcTemp{
			info: "select float64_col1 / float64_col2 overflow",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1e308}, []bool{false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1e-308}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_float64.ToType(), true,
				[]float64{0}, []bool{false}),
		}
		tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, divFn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}

// Rows masked by selectList (e.g. short-circuited by CASE/IF) must not be
// evaluated, so an overflow on a masked row must not raise an error. The
// vector/const branch of specialTemplateForDivFunction used to check only
// p1.WithAnyNullValue() and evaluated masked rows anyway.
func Test_DivFn_SelectListSkipsMaskedRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	maskRow1 := &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}}

	runMasked := func(info string, fn fEvalFn, inputs []FunctionTestInput, rsTyp types.Type) *vector.Vector {
		tcc := NewFunctionTestCase(proc, inputs,
			NewFunctionTestResult(rsTyp, false, nil, nil), fn)
		require.NoError(t, tcc.result.PreExtendAndReset(2), info)
		require.NoError(t, tcc.fn(tcc.parameters, tcc.result, proc, 2, maskRow1), info)
		rsVec := tcc.GetResultVectorDirectly()
		require.True(t, rsVec.GetNulls().Contains(1), info)
		require.False(t, rsVec.GetNulls().Contains(0), info)
		return rsVec
	}

	// divFn vector / const: masked row 1 would overflow to +Inf
	{
		rsVec := runMasked("select case when .. then float64_col / 1e-308 end",
			divFn,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1, 1e308}, []bool{false, false}),
				NewFunctionTestConstInput(types.T_float64.ToType(),
					[]float64{1e-308, 1e-308}, []bool{false, false}),
			},
			types.T_float64.ToType())
		require.Equal(t, 1e308, vector.MustFixedColNoTypeCheck[float64](rsVec)[0])
	}

	// integerDivFn vector / const: masked row 1 would exceed BIGINT range
	{
		rsVec := runMasked("select case when .. then float64_col DIV 1 end",
			integerDivFn,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{7, 1e19}, []bool{false, false}),
				NewFunctionTestConstInput(types.T_float64.ToType(),
					[]float64{1, 1}, []bool{false, false}),
			},
			types.T_int64.ToType())
		require.Equal(t, int64(7), vector.MustFixedColNoTypeCheck[int64](rsVec)[0])
	}

	// divFn vector / vector: masked-row overflow must be skipped too
	{
		rsVec := runMasked("select case when .. then float64_col1 / float64_col2 end",
			divFn,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{1, 1e308}, []bool{false, false}),
				NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{2, 1e-308}, []bool{false, false}),
			},
			types.T_float64.ToType())
		require.Equal(t, 0.5, vector.MustFixedColNoTypeCheck[float64](rsVec)[0])
	}
}
