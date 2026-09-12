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
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestFormatExtractMinute(t *testing.T) {
	for minute := -100; minute <= 100; minute++ {
		got := formatExtractMinute(minute)
		want := fmt.Sprintf("%02d", minute)
		if got != want {
			t.Fatalf("formatExtractMinute(%d) = %q, want %q", minute, got, want)
		}
	}
	for _, minute := range []int{math.MinInt64, math.MaxInt64} {
		got := formatExtractMinute(minute)
		want := fmt.Sprintf("%02d", minute)
		if got != want {
			t.Fatalf("formatExtractMinute(%d) = %q, want %q", minute, got, want)
		}
	}
}

func TestExtractMinuteDatetimeVector(t *testing.T) {
	proc := testutil.NewProcess(t)
	values := make([]types.Datetime, 62)
	units := make([]string, len(values))
	want := make([]string, len(values))
	for minute := 0; minute < 60; minute++ {
		values[minute] = types.DatetimeFromClock(2026, 9, 12, 12, uint8(minute), 0, 0)
		units[minute] = "minute"
		want[minute] = fmt.Sprintf("%02d", minute)
	}
	values[60] = types.ZeroDatetime
	values[61] = types.Datetime(0)
	units[60], units[61] = "minute", "minute"
	want[60], want[61] = "00", "00"

	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestConstInput(types.T_varchar.ToType(), units, nil),
			NewFunctionTestInput(types.T_datetime.ToType(), values, nil),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false, want, nil),
		ExtractFromDatetime,
	)
	succeed, info := testCase.Run()
	if !succeed {
		t.Fatal(info)
	}
}

func TestExtractMinuteDatetimeVectorFallbackAndNulls(t *testing.T) {
	proc := testutil.NewProcess(t)
	minuteMicros := int64(types.SecsPerMinute * types.MicroSecsPerSec)
	values := []types.Datetime{
		types.Datetime(-minuteMicros),
		types.Datetime(-59 * minuteMicros),
		types.Datetime(math.MinInt64),
		types.Datetime(math.MaxInt64),
		types.DatetimeFromClock(2026, 9, 12, 12, 59, 0, 0),
		types.DatetimeFromClock(2026, 9, 12, 12, 0, 0, 0),
	}
	units := []string{"minute", "minute", "minute", "minute", "minute", "minute"}
	want := make([]string, len(values))
	for i, value := range values {
		want[i] = fmt.Sprintf("%02d", int(value.Minute()))
	}
	unitNulls := []bool{false, false, false, false, false, false}
	valueNulls := []bool{false, false, false, false, false, true}
	resultNulls := []bool{false, false, false, false, false, true}

	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestConstInput(types.T_varchar.ToType(), units, unitNulls),
			NewFunctionTestInput(types.T_datetime.ToType(), values, valueNulls),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false, want, resultNulls),
		ExtractFromDatetime,
	)
	succeed, info := testCase.Run()
	if !succeed {
		t.Fatal(info)
	}
}

func TestExtractMinuteTimestampUsesSessionTimezone(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().TimeZone = time.FixedZone("UTC+05:45", 5*60*60+45*60)
	first, err := types.ParseTimestamp(time.UTC, "2024-01-01 18:30:00", 0)
	if err != nil {
		t.Fatal(err)
	}
	second, err := types.ParseTimestamp(time.UTC, "2024-01-01 23:59:00", 0)
	if err != nil {
		t.Fatal(err)
	}
	units := []string{"minute", "minute", "minute", "minute"}
	values := []types.Timestamp{first, types.ZeroTimestamp, first, second}
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestConstInput(types.T_varchar.ToType(), units, nil),
			NewFunctionTestInput(types.T_timestamp.ToType(), values, []bool{false, false, true, false}),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"15", "00", "", "44"}, []bool{false, false, true, false}),
		ExtractFromTimestamp,
	)
	succeed, info := testCase.Run()
	if !succeed {
		t.Fatal(info)
	}

	constantTimestamp := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"minute", "minute", "minute"}, nil),
			NewFunctionTestConstInput(types.T_timestamp.ToType(), []types.Timestamp{first, first, first}, nil),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"15", "15", "15"}, nil),
		ExtractFromTimestamp,
	)
	succeed, info = constantTimestamp.Run()
	if !succeed {
		t.Fatal(info)
	}
}

func TestExtractMinuteVectorConstantTemporalAndNullUnit(t *testing.T) {
	proc := testutil.NewProcess(t)
	unit := NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"minute", "minute", "minute"}, nil)
	constantDatetime := NewFunctionTestConstInput(
		types.T_datetime.ToType(),
		[]types.Datetime{types.DatetimeFromClock(2026, 9, 12, 12, 7, 0, 0), types.DatetimeFromClock(2026, 9, 12, 12, 7, 0, 0), types.DatetimeFromClock(2026, 9, 12, 12, 7, 0, 0)},
		nil,
	)
	testCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{unit, constantDatetime},
		NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"07", "07", "07"}, nil),
		ExtractFromDatetime,
	)
	succeed, info := testCase.Run()
	if !succeed {
		t.Fatal(info)
	}

	unitWithNull := NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"minute", "minute", "minute"}, []bool{true, true, true})
	values := NewFunctionTestInput(
		types.T_datetime.ToType(),
		[]types.Datetime{
			types.DatetimeFromClock(2026, 9, 12, 12, 7, 0, 0),
			types.DatetimeFromClock(2026, 9, 12, 12, 8, 0, 0),
			types.DatetimeFromClock(2026, 9, 12, 12, 9, 0, 0),
		},
		nil,
	)
	nullUnitCase := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{unitWithNull, values},
		NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"", "", ""}, []bool{true, true, true}),
		ExtractFromDatetime,
	)
	succeed, info = nullUnitCase.Run()
	if !succeed {
		t.Fatal(info)
	}

	emptyBatch := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestConstInput(types.T_varchar.ToType(), []string{"minute"}, nil),
			NewFunctionTestInput(types.T_datetime.ToType(), []types.Datetime{}, nil),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false, []string{}, nil),
		ExtractFromDatetime,
	)
	if err := emptyBatch.result.PreExtendAndReset(0); err != nil {
		t.Fatal(err)
	}
	if err := ExtractFromDatetime(emptyBatch.parameters, emptyBatch.result, proc, 0, nil); err != nil {
		t.Fatal(err)
	}
	if got := emptyBatch.result.GetResultVector().Length(); got != 0 {
		t.Fatalf("empty batch produced %d rows", got)
	}
}

func TestExtractMinuteFastPathLeavesOtherUnitsUnchanged(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		unit string
		want string
	}{
		{unit: "second", want: "06"},
		{unit: "not-a-unit", want: ""},
	} {
		t.Run(tc.unit, func(t *testing.T) {
			testCase := NewFunctionTestCase(
				proc,
				[]FunctionTestInput{
					NewFunctionTestConstInput(types.T_varchar.ToType(), []string{tc.unit}, nil),
					NewFunctionTestInput(
						types.T_datetime.ToType(),
						[]types.Datetime{types.DatetimeFromClock(2026, 9, 12, 12, 7, 6, 0)},
						nil,
					),
				},
				NewFunctionTestResult(types.T_varchar.ToType(), false, []string{tc.want}, nil),
				ExtractFromDatetime,
			)
			succeed, info := testCase.Run()
			if !succeed {
				t.Fatal(info)
			}
		})
	}
}

func BenchmarkExtractMinuteVector(b *testing.B) {
	const rows = 8192
	b.Run("datetime", func(b *testing.B) {
		values := make([]types.Datetime, rows)
		for i := range values {
			values[i] = types.DatetimeFromClock(2026, 9, 12, 12, uint8(i%60), 0, 0)
		}
		benchmarkExtractMinuteVector(b, types.T_datetime.ToType(), values, ExtractFromDatetime, nil, (rows-1)%60)
	})
	b.Run("timestamp", func(b *testing.B) {
		values := make([]types.Timestamp, rows)
		for i := range values {
			values[i] = types.UnixMicroToTimestamp(int64(i%60) * int64(types.SecsPerMinute) * int64(types.MicroSecsPerSec))
		}
		zone := time.FixedZone("UTC+05:45", 5*60*60+45*60)
		minute := ((rows - 1) % 60) + 45
		benchmarkExtractMinuteVector(b, types.T_timestamp.ToType(), values, ExtractFromTimestamp, zone, minute%60)
	})
}

func benchmarkExtractMinuteVector(
	b *testing.B,
	inputType types.Type,
	values any,
	fn fEvalFn,
	zone *time.Location,
	wantLastMinute int,
) {
	const rows = 8192
	proc := testutil.NewProcess(b)
	if zone != nil {
		proc.GetSessionInfo().TimeZone = zone
	}
	units := make([]string, rows)
	for i := range units {
		units[i] = "minute"
	}
	fc := NewFunctionTestCase(
		proc,
		[]FunctionTestInput{
			NewFunctionTestConstInput(types.T_varchar.ToType(), units, nil),
			NewFunctionTestInput(inputType, values, nil),
		},
		NewFunctionTestResult(types.T_varchar.ToType(), false, nil, nil),
		fn,
	)
	runBatch := func() {
		b.Helper()
		if err := fc.result.PreExtendAndReset(rows); err != nil {
			b.Fatal(err)
		}
		if err := fc.fn(fc.parameters, fc.result, proc, rows, nil); err != nil {
			b.Fatal(err)
		}
	}
	runBatch()
	if got := fc.result.GetResultVector().GetStringAt(rows - 1); got != fmt.Sprintf("%02d", wantLastMinute) {
		b.Fatalf("last result = %q, want %02d", got, wantLastMinute)
	}
	b.ReportAllocs()
	b.SetBytes(rows * 2)
	b.ResetTimer()
	for b.Loop() {
		runBatch()
	}
	b.StopTimer()
	if got := fc.result.GetResultVector().GetStringAt(rows - 1); got != fmt.Sprintf("%02d", wantLastMinute) {
		b.Fatalf("last result = %q, want %02d", got, wantLastMinute)
	}
}
