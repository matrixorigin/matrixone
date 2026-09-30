// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestIntegerArgumentTemporalCast(t *testing.T) {
	proc := testutil.NewProcess(t)
	uuid, err := types.ParseUuid("00000256-0000-0000-0000-000000000000")
	require.NoError(t, err)
	date, err := types.ParseDateCast("2024-01-02")
	require.NoError(t, err)
	dt, err := types.ParseDatetime("2023-12-31 23:59:59.5", 6)
	require.NoError(t, err)
	maxDT, err := types.ParseDatetime("9999-12-31 23:59:59.999999", 6)
	require.NoError(t, err)
	for _, tc := range []struct {
		input   FunctionTestInput
		want    []int64
		nulls   []bool
		mask    *FunctionSelectList
		wantErr bool
	}{
		{input: NewFunctionTestInput(types.T_uuid.ToType(), []types.Uuid{uuid}, nil), want: []int64{256}},
		{input: NewFunctionTestInput(types.T_enum.ToType(), []types.Enum{0, 1, 65535}, nil), want: []int64{0, 1, 65535}},
		{input: NewFunctionTestInput(types.T_date.ToType(), []types.Date{date}, nil), want: []int64{20240102}},
		{input: NewFunctionTestInput(types.T_datetime.ToTypeWithScale(6), []types.Datetime{dt, maxDT}, nil), want: []int64{20240101000000, 99991231000000}},
		{input: NewFunctionTestInput(types.T_time.ToTypeWithScale(6), []types.Time{175500000, 3599500000, -1500000, 3020399999999, -3020399999999, 3020400000000}, nil), want: []int64{256, 10000, -2, 8385959, -8385959, 8390000}},
		{input: NewFunctionTestInput(types.T_time.ToTypeWithScale(6), []types.Time{1500000, 1500000, 1500000}, []bool{false, false, true}), want: []int64{0, 2, 0}, nulls: []bool{true, false, true}, mask: &FunctionSelectList{AnyNull: true, SelectList: []bool{false, true, true}}},
		{input: NewFunctionTestConstInput(types.T_time.ToType(), []types.Time{math.MaxInt64}, []bool{true}), want: []int64{0}, nulls: []bool{true}},
		{input: NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{math.MaxInt64}, nil), want: []int64{0}, nulls: []bool{true}, mask: &FunctionSelectList{AllNull: true}},
		{input: NewFunctionTestInput(types.T_float64.ToType(), []float64{1.5, 2.5}, nil), want: []int64{2, 2}},
		{input: NewFunctionTestInput(types.T_varchar.ToType(), []string{"1.9tail", "2.5"}, nil), want: []int64{1, 2}},
		{input: NewFunctionTestInput(types.T_varchar.ToType(), []string{"9223372036854775808"}, nil), wantErr: true},
	} {
		test := NewFunctionTestCase(proc, []FunctionTestInput{tc.input, NewFunctionTestInput(types.T_int64.ToType(), []int64{}, nil)}, NewFunctionTestResult(types.T_int64.ToType(), tc.wantErr, tc.want, tc.nulls), NewTemporalIntegerArgumentCast).WithSelectList(tc.mask)
		ok, info := test.Run()
		require.True(t, ok, info)
	}
	ts, err := types.ParseTimestamp(time.UTC, "2024-01-01 23:59:59.5", 6)
	require.NoError(t, err)
	for _, tc := range []struct {
		zone *time.Location
		want int64
	}{
		{time.UTC, 20240102000000}, {time.FixedZone("UTC+8", 8*3600), 20240102080000},
	} {
		proc.GetSessionInfo().TimeZone = tc.zone
		test := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestInput(types.T_timestamp.ToTypeWithScale(6), []types.Timestamp{ts}, nil), NewFunctionTestInput(types.T_int64.ToType(), []int64{}, nil)}, NewFunctionTestResult(types.T_int64.ToType(), false, []int64{tc.want}, nil), NewTemporalIntegerArgumentCast)
		ok, info := test.Run()
		require.True(t, ok, info)
	}
	// Permission is in the execution identity, not inferred from a runtime value.
	for _, kernel := range []fEvalFn{NewIntegerArgumentCast, NewTruncatedIntegerArgumentCast} {
		test := NewFunctionTestCase(proc, []FunctionTestInput{NewFunctionTestInput(types.T_time.ToType(), []types.Time{1500000}, nil), NewFunctionTestInput(types.T_int64.ToType(), []int64{}, nil)}, NewFunctionTestResult(types.T_int64.ToType(), true, nil, nil), kernel)
		ok, info := test.Run()
		require.True(t, ok, info)
	}
}
