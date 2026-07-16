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
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestCastZeroTemporalToNumericUsesZero(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().TimeZone = time.FixedZone("UTC+8", 8*60*60)
	zeroDecimal, err := types.ParseDecimal128("0.000000", 20, 6)
	require.NoError(t, err)

	for _, tc := range []struct {
		name   string
		inputs []FunctionTestInput
		expect FunctionTestResult
	}{
		{
			name: "zero date to signed",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_date.ToType(), []types.Date{types.ZeroDate}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{}, nil),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, nil),
		},
		{
			name: "zero datetime to decimal",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_datetime.ToTypeWithScale(6), []types.Datetime{types.ZeroDatetime}, nil),
				NewFunctionTestInput(types.New(types.T_decimal128, 20, 6), []types.Decimal128{}, nil),
			},
			expect: NewFunctionTestResult(types.New(types.T_decimal128, 20, 6), false, []types.Decimal128{zeroDecimal}, nil),
		},
		{
			name: "zero timestamp to decimal",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_timestamp.ToTypeWithScale(6), []types.Timestamp{types.ZeroTimestamp}, nil),
				NewFunctionTestInput(types.New(types.T_decimal128, 20, 6), []types.Decimal128{}, nil),
			},
			expect: NewFunctionTestResult(types.New(types.T_decimal128, 20, 6), false, []types.Decimal128{zeroDecimal}, nil),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, NewCast)
			succeed, info := fcTC.Run()
			require.True(t, succeed, info)
		})
	}
}

func TestCastZeroTemporalStringsProducesNonNullSentinels(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().TimeZone = time.FixedZone("UTC+8", 8*60*60)

	for _, tc := range []struct {
		name   string
		inputs []FunctionTestInput
		expect FunctionTestResult
	}{
		{
			name: "string to zero date",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"0000-00-00"}, nil),
				NewFunctionTestInput(types.T_date.ToType(), []types.Date{}, nil),
			},
			expect: NewFunctionTestResult(types.T_date.ToType(), false, []types.Date{types.ZeroDate}, nil),
		},
		{
			name: "zero datetime string to zero date",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"0000-00-00 00:00:00"}, nil),
				NewFunctionTestInput(types.T_date.ToType(), []types.Date{}, nil),
			},
			expect: NewFunctionTestResult(types.T_date.ToType(), false, []types.Date{types.ZeroDate}, nil),
		},
		{
			name: "string to zero datetime",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"0000-00-00 00:00:00"}, nil),
				NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{}, nil),
			},
			expect: NewFunctionTestResult(types.T_datetime.ToType(), false, []types.Datetime{types.ZeroDatetime}, nil),
		},
		{
			name: "string to zero timestamp",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"0000-00-00 00:00:00"}, nil),
				NewFunctionTestInput(types.T_timestamp.ToType(), []types.Timestamp{}, nil),
			},
			expect: NewFunctionTestResult(types.T_timestamp.ToType(), false, []types.Timestamp{types.ZeroTimestamp}, nil),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, NewCast)
			succeed, info := fcTC.Run()
			require.True(t, succeed, info)
		})
	}
}

func TestUnixTimestampZeroValueReturnsNull(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().TimeZone = time.UTC
	varcharInput := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"0000-00-00 00:00:00"}, nil),
	}

	for _, tc := range []struct {
		name   string
		inputs []FunctionTestInput
		expect FunctionTestResult
		fn     fEvalFn
	}{
		{
			name: "typed timestamp",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_timestamp.ToType(), []types.Timestamp{types.ZeroTimestamp}, nil),
			},
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, []bool{true}),
			fn:     builtInUnixTimestamp,
		},
		{
			name:   "varchar int64",
			inputs: varcharInput,
			expect: NewFunctionTestResult(types.T_int64.ToType(), false, []int64{0}, []bool{true}),
			fn:     builtInUnixTimestampVarcharToInt64,
		},
		{
			name:   "varchar float64",
			inputs: varcharInput,
			expect: NewFunctionTestResult(types.T_float64.ToType(), false, []float64{0}, []bool{true}),
			fn:     builtInUnixTimestampVarcharToFloat64,
		},
		{
			name:   "varchar decimal128",
			inputs: varcharInput,
			expect: NewFunctionTestResult(types.New(types.T_decimal128, 38, 6), false, []types.Decimal128{{}}, []bool{true}),
			fn:     builtInUnixTimestampVarcharToDecimal128,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fcTC := NewFunctionTestCase(proc, tc.inputs, tc.expect, tc.fn)
			succeed, info := fcTC.Run()
			require.True(t, succeed, info)
		})
	}
}
