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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// DECIMAL(p,s) with p > 38 is DECIMAL256. Temporal and JSON sources must cast
// to it the same way they cast to DECIMAL(<=38): the packed numeric value.
func TestCastTemporalAndJSONToDecimal256(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, from := range []types.T{types.T_date, types.T_time, types.T_datetime,
		types.T_timestamp, types.T_year, types.T_json} {
		require.True(t, IfTypeCastSupported(from, types.T_decimal256), from.String())
		_, err := GetFunctionByName(context.Background(), "cast",
			[]types.Type{from.ToType(), types.New(types.T_decimal256, 50, 2)})
		require.NoError(t, err, from.String())
	}

	dec := func(s string, width, scale int32) types.Decimal256 {
		d, err := types.ParseDecimal256(s, width, scale)
		require.NoError(t, err)
		return d
	}
	run := func(name string, in FunctionTestInput, to types.Type, expected []string, nulls []bool) {
		t.Helper()
		want := make([]types.Decimal256, len(expected))
		for i, s := range expected {
			if !nulls[i] {
				want[i] = dec(s, to.Width, to.Scale)
			}
		}
		fcTC := NewFunctionTestCase(proc,
			[]FunctionTestInput{in, NewFunctionTestInput(to, []types.Decimal256{}, []bool{})},
			NewFunctionTestResult(to, false, want, nulls), NewCast)
		succeed, info := fcTC.RunAndFree()
		require.True(t, succeed, "%s: %s", name, info)
	}
	d50s2 := types.New(types.T_decimal256, 50, 2)

	date, err := types.ParseDateCast("2026-10-28")
	require.NoError(t, err)
	run("date", NewFunctionTestInput(types.T_date.ToType(), []types.Date{date, 0}, []bool{false, true}),
		d50s2, []string{"20261028.00", ""}, []bool{false, true})

	dt, err := types.ParseDatetime("2026-10-28 01:02:03.5", 1)
	require.NoError(t, err)
	run("datetime", NewFunctionTestInput(types.New(types.T_datetime, 0, 1), []types.Datetime{dt}, []bool{false}),
		d50s2, []string{"20261028010203.50"}, []bool{false})
	// 14 integer digits + 30 fraction digits do not fit Decimal128.
	run("datetime wide scale", NewFunctionTestInput(types.New(types.T_datetime, 0, 1), []types.Datetime{dt}, []bool{false}),
		types.New(types.T_decimal256, 65, 30), []string{"20261028010203.5"}, []bool{false})

	ts, err := types.ParseTimestamp(proc.GetSessionInfo().TimeZone, "2026-10-28 01:02:03.5", 1)
	require.NoError(t, err)
	run("timestamp", NewFunctionTestInput(types.New(types.T_timestamp, 0, 1), []types.Timestamp{ts}, []bool{false}),
		d50s2, []string{"20261028010203.50"}, []bool{false})

	tm, err := types.ParseTime("-01:02:03.5", 1)
	require.NoError(t, err)
	run("time", NewFunctionTestInput(types.New(types.T_time, 0, 1), []types.Time{tm}, []bool{false}),
		d50s2, []string{"-10203.50"}, []bool{false})

	run("year", NewFunctionTestInput(types.T_year.ToType(), []types.MoYear{2026}, []bool{false}),
		d50s2, []string{"2026.00"}, []bool{false})

	jsonTexts := []string{`1.5`, `null`, `1`}
	jsonNulls := []bool{false, false, true}
	run("json", NewFunctionTestInput(types.T_json.ToType(), makeJSONEncodedFromText(t, jsonTexts, jsonNulls), jsonNulls),
		d50s2, []string{"1.50", "", ""}, []bool{false, true, true})

	// Overflow still errors like the narrower decimal casts.
	fcTC := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_datetime, 0, 1), []types.Datetime{dt}, []bool{false}),
			NewFunctionTestInput(types.New(types.T_decimal256, 40, 30), []types.Decimal256{}, []bool{})},
		NewFunctionTestResult(types.New(types.T_decimal256, 40, 30), true, []types.Decimal256{{}}, []bool{false}), NewCast)
	succeed, info := fcTC.RunAndFree()
	require.True(t, succeed, "overflow: %s", info)
}
