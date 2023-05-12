// Copyright 2022 Matrix Origin
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

package function2

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

func Test_BuiltIn_Math(t *testing.T) {
	proc := testutil.NewProcess()
	{
		tc := tcTemp{
			info: "test ln",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						1, math.Exp(0), math.Exp(1), math.Exp(10), math.Exp(100), math.Exp(99), math.Exp(-1),
					},
					nil),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0, 0, 1, 10, 100, 99, -1}, nil),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInLn)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "test exp",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						-1, 0, 1, 2, 10, 100,
					},
					nil),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{math.Exp(-1), math.Exp(0), math.Exp(1), math.Exp(2), math.Exp(10), math.Exp(100)}, nil),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInExp)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "test sin",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						-math.Pi / 2, 0, math.Pi / 2,
					},
					nil),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{-1, 0, 1}, nil),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInSin)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "test cos",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						-math.Pi, 0, math.Pi,
					},
					nil),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{-1, 1, -1}, nil),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInCos)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "test tan",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						0,
					},
					nil),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0}, nil),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInTan)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "test sinh",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						0,
					},
					nil),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0}, nil),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInSinh)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "test acos",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						1,
					},
					nil),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0}, nil),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInACos)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "test atan",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						0,
					},
					nil),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0}, nil),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInATan)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "test atan with 2 args",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						-1, 1, 1, 1, 1.0, 1.0,
					},
					nil),
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						1, 0, -1, 1, -1.0, 1.0,
					},
					nil),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{-0.7853981633974483, 0, -0.7853981633974483, 0.7853981633974483, -0.7853981633974483, 0.7853981633974483}, nil),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInATan2)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "test log",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						5,
					},
					nil),
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						3,
					},
					nil),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0.6826061944859853}, nil),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInLog)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}

	{
		tc := tcTemp{
			info: "test log with err",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						1,
					},
					nil),
				testutil.NewFunctionTestInput(types.T_float64.ToType(),
					[]float64{
						1,
					},
					nil),
			},
			expect: testutil.NewFunctionTestResult(types.T_float64.ToType(), true,
				nil, nil),
		}
		tcc := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInLog)
		succeed, info := tcc.Run()
		require.True(t, succeed, tc.info, info)
	}
}
