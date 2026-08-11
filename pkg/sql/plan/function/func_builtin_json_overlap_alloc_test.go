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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestJSONOverlapsAccessorWrappersAreResultScoped(t *testing.T) {
	proc := testutil.NewProcess(t)
	trueJSON := mustJsonBinaryString(t, `true`)
	falseJSON := mustJsonBinaryString(t, `false`)

	const rows = 8192
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
		NewFunctionTestResult(types.T_int64.ToType(), false, nil, nil),
		jsonOverlaps,
	)

	firstLeft, firstRight := newJSONOverlapOperands(testCase.parameters, testCase.result)
	secondLeft, secondRight := newJSONOverlapOperands(testCase.parameters, testCase.result)
	require.Same(t, firstLeft.wrapper, secondLeft.wrapper)
	require.Same(t, firstRight.wrapper, secondRight.wrapper)

	require.NoError(t, testCase.result.PreExtendAndReset(rows))
	require.NoError(t, testCase.fn(testCase.parameters, testCase.result, proc, rows, nil))
	values := vector.MustFixedColNoTypeCheck[int64](testCase.result.GetResultVector())
	require.Len(t, values, rows)
	for _, value := range values {
		require.Zero(t, value)
	}
}
