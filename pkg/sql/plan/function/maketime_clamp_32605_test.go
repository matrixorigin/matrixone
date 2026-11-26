// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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

// #32605: MAKETIME clamps an out-of-range result to +-838:59:59 with warning
// 1292, like MySQL, instead of returning NULL. Invalid minutes still give NULL.
func TestMakeTimeClampsOutOfRangeHour32605(t *testing.T) {
	proc := testutil.NewProcess(t)
	sink := &numericWarningSession{}
	proc.WarningSink = sink
	maxTime := types.TimeFromClock(false, 838, 59, 59, 0)
	fc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{850, -850, 838, 838, 12}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{0, 0, 59, 0, 60}, nil),
			NewFunctionTestInput(types.T_float64.ToType(), []float64{0, 0, 59.5, 1, 0}, nil),
		},
		NewFunctionTestResult(types.T_time.ToTypeWithScale(1), false,
			[]types.Time{maxTime, -maxTime, maxTime, types.TimeFromClock(false, 838, 0, 1, 0), 0},
			[]bool{false, false, false, false, true}),
		MakeTime)
	ok, info := fc.RunAndFree()
	require.True(t, ok, info)
	require.Equal(t, []numericWarning{
		{1292, "Truncated incorrect time value: '850:00:00'"},
		{1292, "Truncated incorrect time value: '-850:00:00'"},
		{1292, "Truncated incorrect time value: '838:59:59.5'"},
	}, sink.warnings)
}
