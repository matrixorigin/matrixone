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
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestFloatArithmeticOverflowReturnsError(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := tcTemp{
		info: "float64 addition overflow returns out-of-range error",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_float64.ToType(), []float64{math.MaxFloat64}, nil),
			NewFunctionTestInput(types.T_float64.ToType(), []float64{math.MaxFloat64}, nil),
		},
		expect: NewFunctionTestResult(types.T_float64.ToType(), true, []float64{0}, nil),
	}

	tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, plusFn)
	succeed, info := tcc.Run()
	require.True(t, succeed, tc.info, info)
}

func TestExpOverflowReturnsError(t *testing.T) {
	proc := testutil.NewProcess(t)
	tc := tcTemp{
		info: "exp overflow returns out-of-range error",
		inputs: []FunctionTestInput{
			NewFunctionTestInput(types.T_float64.ToType(), []float64{1000}, nil),
		},
		expect: NewFunctionTestResult(types.T_float64.ToType(), true, []float64{0}, nil),
	}

	tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, builtInExp)
	succeed, info := tcc.Run()
	require.True(t, succeed, tc.info, info)
}
