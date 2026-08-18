// Copyright 2024 Matrix Origin
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

func TestBetweenDatetimeTimestampPreservesInstantSemantics(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	zone, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	proc.GetSessionInfo().TimeZone = zone

	datetime, err := types.ParseDatetime("2024-11-03 01:30:00", 6)
	require.NoError(t, err)
	lower, err := types.ParseTimestamp(time.UTC, "2024-11-03 05:00:00", 6)
	require.NoError(t, err)
	upper, err := types.ParseTimestamp(time.UTC, "2024-11-03 06:15:00", 6)
	require.NoError(t, err)
	want := datetime.ToTimestamp(zone) >= lower && datetime.ToTimestamp(zone) <= upper

	testCase := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_datetime.ToTypeWithScale(6), []types.Datetime{datetime, datetime}, []bool{false, true}),
			NewFunctionTestConstInput(types.T_timestamp.ToTypeWithScale(6), []types.Timestamp{lower}, nil),
			NewFunctionTestConstInput(types.T_timestamp.ToTypeWithScale(6), []types.Timestamp{upper}, nil),
		},
		NewFunctionTestResult(types.T_bool.ToType(), false, []bool{want, false}, []bool{false, true}),
		betweenImpl,
	)
	ok, info := testCase.Run()
	require.True(t, ok, info)
}

func TestBetweenDatetimeTimestampPreservesCommonValueScale(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.GetSessionInfo().TimeZone = time.UTC

	value, err := types.ParseDatetime("2026-08-10 12:00:00.123456", 6)
	require.NoError(t, err)
	lower := value.ToTimestamp(time.UTC).TruncateToScale(3)
	upper, err := types.ParseTimestamp(time.UTC, "2026-08-10 12:00:00.123100", 6)
	require.NoError(t, err)

	testCase := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_datetime.ToTypeWithScale(6), []types.Datetime{value}, nil),
			NewFunctionTestConstInput(types.T_timestamp.ToTypeWithScale(3), []types.Timestamp{lower}, nil),
			NewFunctionTestConstInput(types.T_timestamp.ToTypeWithScale(6), []types.Timestamp{upper}, nil),
		},
		NewFunctionTestResult(types.T_bool.ToType(), false, []bool{true}, nil),
		betweenImpl,
	)
	ok, info := testCase.Run()
	require.True(t, ok, info)
}

func TestBetweenDatetimeTimestampTypeArrangements(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.GetSessionInfo().TimeZone = time.UTC

	valueDatetime, err := types.ParseDatetime("2026-08-10 12:00:00", 6)
	require.NoError(t, err)
	lowerDatetime, err := types.ParseDatetime("2026-08-10 11:00:00", 6)
	require.NoError(t, err)
	upperDatetime, err := types.ParseDatetime("2026-08-10 13:00:00", 6)
	require.NoError(t, err)
	valueTimestamp := valueDatetime.ToTimestamp(time.UTC)
	lowerTimestamp := lowerDatetime.ToTimestamp(time.UTC)
	upperTimestamp := upperDatetime.ToTimestamp(time.UTC)

	datetimeType := types.T_datetime.ToTypeWithScale(6)
	timestampType := types.T_timestamp.ToTypeWithScale(6)
	datetimeInput := func(value types.Datetime, nulls []bool) FunctionTestInput {
		return NewFunctionTestInput(datetimeType, []types.Datetime{value, value}, nulls)
	}
	timestampInput := func(value types.Timestamp, nulls []bool) FunctionTestInput {
		return NewFunctionTestInput(timestampType, []types.Timestamp{value, value}, nulls)
	}

	tests := []struct {
		name   string
		inputs []FunctionTestInput
	}{
		{
			name: "datetime value datetime lower timestamp upper",
			inputs: []FunctionTestInput{
				datetimeInput(valueDatetime, []bool{false, true}),
				datetimeInput(lowerDatetime, nil),
				timestampInput(upperTimestamp, nil),
			},
		},
		{
			name: "datetime value timestamp lower datetime upper",
			inputs: []FunctionTestInput{
				datetimeInput(valueDatetime, []bool{false, true}),
				timestampInput(lowerTimestamp, nil),
				datetimeInput(upperDatetime, nil),
			},
		},
		{
			name: "timestamp value timestamp lower datetime upper",
			inputs: []FunctionTestInput{
				timestampInput(valueTimestamp, []bool{false, true}),
				timestampInput(lowerTimestamp, nil),
				datetimeInput(upperDatetime, nil),
			},
		},
		{
			name: "timestamp value datetime lower timestamp upper",
			inputs: []FunctionTestInput{
				timestampInput(valueTimestamp, []bool{false, true}),
				datetimeInput(lowerDatetime, nil),
				timestampInput(upperTimestamp, nil),
			},
		},
		{
			name: "timestamp value datetime bounds",
			inputs: []FunctionTestInput{
				timestampInput(valueTimestamp, []bool{false, true}),
				datetimeInput(lowerDatetime, nil),
				datetimeInput(upperDatetime, nil),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testCase := NewFunctionTestCase(
				proc,
				test.inputs,
				NewFunctionTestResult(
					types.T_bool.ToType(), false,
					[]bool{true, false}, []bool{false, true},
				),
				betweenImpl,
			)
			ok, info := testCase.Run()
			require.True(t, ok, info)
		})
	}
}

func TestOpBetweenBool(t *testing.T) {
	proc := testutil.NewProcess(t)
	boolType := types.T_bool.ToType()

	cases := []struct {
		name    string
		input   FunctionTestInput
		lb, ub  FunctionTestInput
		want    []bool
		wantNul []bool
	}{
		{
			name:  "BETWEEN FALSE AND TRUE is always true",
			input: NewFunctionTestInput(boolType, []bool{true, false}, nil),
			lb:    NewFunctionTestConstInput(boolType, []bool{false}, nil),
			ub:    NewFunctionTestConstInput(boolType, []bool{true}, nil),
			want:  []bool{true, true}, wantNul: nil,
		},
		{
			name:  "BETWEEN TRUE AND FALSE is always false",
			input: NewFunctionTestInput(boolType, []bool{true, false}, nil),
			lb:    NewFunctionTestConstInput(boolType, []bool{true}, nil),
			ub:    NewFunctionTestConstInput(boolType, []bool{false}, nil),
			want:  []bool{false, false}, wantNul: nil,
		},
		{
			name:  "BETWEEN TRUE AND TRUE matches only true",
			input: NewFunctionTestInput(boolType, []bool{true, false}, nil),
			lb:    NewFunctionTestConstInput(boolType, []bool{true}, nil),
			ub:    NewFunctionTestConstInput(boolType, []bool{true}, nil),
			want:  []bool{true, false}, wantNul: nil,
		},
		{
			name:  "null input with BETWEEN FALSE AND TRUE",
			input: NewFunctionTestInput(boolType, []bool{true, false}, []bool{false, true}),
			lb:    NewFunctionTestConstInput(boolType, []bool{false}, nil),
			ub:    NewFunctionTestConstInput(boolType, []bool{true}, nil),
			want:  []bool{true, false}, wantNul: []bool{false, true},
		},
		{
			name:  "null lower bound produces all nulls",
			input: NewFunctionTestInput(boolType, []bool{true, false}, nil),
			lb:    NewFunctionTestConstInput(boolType, []bool{false}, []bool{true}),
			ub:    NewFunctionTestConstInput(boolType, []bool{true}, nil),
			want:  []bool{false, false}, wantNul: []bool{true, true},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fn := NewFunctionTestCase(proc,
				[]FunctionTestInput{tc.input, tc.lb, tc.ub},
				NewFunctionTestResult(types.T_bool.ToType(), false, tc.want, tc.wantNul),
				betweenImpl,
			)
			ok, info := fn.Run()
			require.True(t, ok, info)
		})
	}
}

func TestOpBetweenFixedNullBound(t *testing.T) {
	proc := testutil.NewProcess(t)
	int64Type := types.T_int64.ToType()

	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(int64Type, []int64{1, 2, 3}, nil),
			NewFunctionTestConstInput(int64Type, []int64{0}, []bool{true}),
			NewFunctionTestConstInput(int64Type, []int64{5}, nil),
		},
		NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{false, false, false}, []bool{true, true, true}),
		betweenImpl,
	)
	ok, info := tc.Run()
	require.True(t, ok, info)
}

func TestOpBetweenFixedRowBounds(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	int64Type := types.T_int64.ToType()

	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(int64Type, []int64{1, 2, 3, 4}, nil),
			NewFunctionTestInput(int64Type, []int64{1, 1, 4, 4}, []bool{false, false, true, false}),
			NewFunctionTestInput(int64Type, []int64{1, 2, 5, 3}, nil),
		},
		NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, true, false, false}, []bool{false, false, true, false}),
		betweenImpl,
	)
	ok, info := tc.Run()
	require.True(t, ok, info)
}

func TestInRangeBool(t *testing.T) {
	proc := testutil.NewProcess(t)
	boolType := types.T_bool.ToType()
	uint8Type := types.T_uint8.ToType()

	cases := []struct {
		name string
		flag uint8
		want []bool
	}{
		{"[F,T] all match", 0, []bool{true, true}},
		{"(F,T] only true", 1, []bool{true, false}},
		{"[F,T) only false", 2, []bool{false, true}},
		{"(F,T) nothing", 3, []bool{false, false}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fn := NewFunctionTestCase(proc,
				[]FunctionTestInput{
					NewFunctionTestInput(boolType, []bool{true, false}, nil),
					NewFunctionTestConstInput(boolType, []bool{false}, nil),
					NewFunctionTestConstInput(boolType, []bool{true}, nil),
					NewFunctionTestConstInput(uint8Type, []uint8{tc.flag}, nil),
				},
				NewFunctionTestResult(types.T_bool.ToType(), false, tc.want, nil),
				inRangeImpl,
			)
			ok, info := fn.Run()
			require.True(t, ok, info)
		})
	}

	// null bound produces all nulls
	t.Run("null bound", func(t *testing.T) {
		fn := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(boolType, []bool{true, false}, nil),
				NewFunctionTestConstInput(boolType, []bool{false}, nil),
				NewFunctionTestConstInput(boolType, []bool{true}, []bool{true}),
				NewFunctionTestConstInput(uint8Type, []uint8{0}, nil),
			},
			NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{false, false}, []bool{true, true}),
			inRangeImpl,
		)
		ok, info := fn.Run()
		require.True(t, ok, info)
	})
}

func TestInRangeFixedNullBound(t *testing.T) {
	proc := testutil.NewProcess(t)
	int64Type := types.T_int64.ToType()
	uint8Type := types.T_uint8.ToType()

	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(int64Type, []int64{1, 2, 3}, nil),
			NewFunctionTestConstInput(int64Type, []int64{0}, []bool{true}),
			NewFunctionTestConstInput(int64Type, []int64{5}, nil),
			NewFunctionTestConstInput(uint8Type, []uint8{0}, nil),
		},
		NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{false, false, false}, []bool{true, true, true}),
		inRangeImpl,
	)
	ok, info := tc.Run()
	require.True(t, ok, info)
}
