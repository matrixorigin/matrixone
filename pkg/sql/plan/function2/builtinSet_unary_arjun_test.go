// Copyright 2023 Matrix Origin
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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

func initDateToMonthTestCase() []tcTemp {
	d1, _ := types.ParseDateCast("2004-04-03")
	d2, _ := types.ParseDateCast("2004-08-03")
	d3, _ := types.ParseDateCast("2004-01-03")
	return []tcTemp{
		{
			info: "test date to month",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_date.ToType(),
					[]types.Date{d1, d2, d3},
					[]bool{false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{4, 8, 1},
				[]bool{false, false, false}),
		},
	}
}

func TestDateToMonth(t *testing.T) {
	testCases := initDateToMonthTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, DateToMonth)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initDateTimeToMonthTestCase() []tcTemp {
	d1, _ := types.ParseDatetime("2004-04-03 13:11:10", 6)
	d2, _ := types.ParseDatetime("1999-08-05 11:01:02", 6)
	d3, _ := types.ParseDatetime("2004-01-03 23:15:08", 6)
	return []tcTemp{
		{
			info: "test datetime to month",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_datetime.ToType(),
					[]types.Datetime{d1, d2, d3},
					[]bool{false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{4, 8, 1},
				[]bool{false, false, false}),
		},
	}
}

func TestDateTimeToMonth(t *testing.T) {
	testCases := initDateTimeToMonthTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, DatetimeToMonth)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
}

func initDateStringToMonthTestCase() []tcTemp {
	return []tcTemp{
		{
			info: "test datestring to month",
			inputs: []testutil.FunctionTestInput{
				testutil.NewFunctionTestInput(types.T_varchar.ToType(),
					[]string{"2014-04-03", "2009-11-03", "2012-07-03", "2012-02-03 18:23:15"},
					[]bool{false, false, false, false}),
			},
			expect: testutil.NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{4, 11, 7, 2},
				[]bool{false, false, false, false}),
		},
	}
}

func TestDateStringToMonth(t *testing.T) {
	testCases := initDateStringToMonthTestCase()

	proc := testutil.NewProcess()
	for _, tc := range testCases {
		fcTC := testutil.NewFunctionTestCase(proc, tc.inputs, tc.expect, DateStringToMonth)
		s, info := fcTC.Run()
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", tc.info, info))
	}
	//TODO: Ingoring Scalar Nulls: Original code: https://github.com/m-schen/matrixone/blob/823b5524f1c6eb189ee9652013bdf86b99e5571e/pkg/sql/plan/function/builtin/unary/month_test.go#L150
}
