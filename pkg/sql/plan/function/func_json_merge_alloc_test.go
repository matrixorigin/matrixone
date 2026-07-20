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

//go:build !race

package function

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

// Allocation counts are not a stable production invariant under race
// instrumentation. Keep this regression check in non-race builds while the
// JSON merge functional tests continue to run in both configurations.
func TestJsonMergePatchWrapperAllocationsDoNotScaleWithRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	trueJSON := mustJsonBinaryString(t, `true`)
	falseJSON := mustJsonBinaryString(t, `false`)

	measure := func(rows int) float64 {
		left := make([]string, rows)
		right := make([]string, rows)
		for i := range rows {
			left[i] = trueJSON
			right[i] = falseJSON
		}
		testCase := NewFunctionTestCase(
			proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), left, nil),
				NewFunctionTestInput(types.T_json.ToType(), right, nil),
			},
			NewFunctionTestResult(types.T_json.ToType(), false, nil, nil),
			newOpBuiltInJsonMerge().buildJsonMergePatch,
		)
		var runErr error
		allocs := testing.AllocsPerRun(3, func() {
			runErr = testCase.result.PreExtendAndReset(rows)
			if runErr == nil {
				runErr = testCase.fn(
					testCase.parameters,
					testCase.result,
					testCase.proc,
					testCase.fnLength,
					nil,
				)
			}
		})
		require.NoError(t, runErr)
		return allocs
	}

	oneRow := measure(1)
	manyRows := measure(8192)
	require.LessOrEqual(t, manyRows, oneRow+10,
		"wrapper allocations must not grow with rows: one=%f many=%f", oneRow, manyRows)
}
