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

// CAST(json AS DATE/TIME/DATETIME/TIMESTAMP) reads a JSON string scalar as the
// same SQL string; JSON null is SQL NULL.
func TestCastJsonToTemporal(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, oid := range []types.T{types.T_date, types.T_time, types.T_datetime, types.T_timestamp} {
		require.True(t, IfTypeCastSupported(types.T_json, oid), oid.String())
		_, err := GetFunctionByName(context.Background(), "cast", []types.Type{types.T_json.ToType(), oid.ToType()})
		require.NoError(t, err, oid.String())
	}

	jsonTexts := []string{`"2026-01-31"`, `"2024-02-29 13:14:15"`, `null`, `"not a date"`, `"x"`}
	nulls := []bool{false, false, false, false, true}
	encoded := makeJSONEncodedFromText(t, jsonTexts, nulls)

	d1, err := types.ParseDateCast("2026-01-31")
	require.NoError(t, err)
	d2, err := types.ParseDateCast("2024-02-29")
	require.NoError(t, err)
	run := func(name string, to types.Type, empty any, expected any, expectedNulls []bool) {
		t.Helper()
		inputs := []FunctionTestInput{
			NewFunctionTestInput(types.T_json.ToType(), encoded, nulls),
			NewFunctionTestInput(to, empty, []bool{}),
		}
		expect := NewFunctionTestResult(to, false, expected, expectedNulls)
		fcTC := NewFunctionTestCase(proc, inputs, expect, NewCast)
		succeed, info := fcTC.RunAndFree()
		require.True(t, succeed, "%s: %s", name, info)
	}
	run("date", types.T_date.ToType(), []types.Date{}, []types.Date{d1, d2, 0, 0, 0},
		[]bool{false, false, true, true, true})

	dt1, err := types.ParseDatetime("2026-01-31 00:00:00", 0)
	require.NoError(t, err)
	dt2, err := types.ParseDatetime("2024-02-29 13:14:15", 0)
	require.NoError(t, err)
	run("datetime", types.T_datetime.ToType(), []types.Datetime{}, []types.Datetime{dt1, dt2, 0, 0, 0},
		[]bool{false, false, true, true, true})

	timeTexts := []string{`"12:34:56.5"`, `"-01:00:00"`, `null`}
	timeEncoded := makeJSONEncodedFromText(t, timeTexts, nil)
	t1, err := types.ParseTime("12:34:56.5", 1)
	require.NoError(t, err)
	t2, err := types.ParseTime("-01:00:00", 1)
	require.NoError(t, err)
	timeType := types.New(types.T_time, 0, 1)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_json.ToType(), timeEncoded, nil),
		NewFunctionTestInput(timeType, []types.Time{}, []bool{}),
	}
	expect := NewFunctionTestResult(timeType, false, []types.Time{t1, t2, 0}, []bool{false, false, true})
	fcTC := NewFunctionTestCase(proc, inputs, expect, NewCast)
	succeed, info := fcTC.RunAndFree()
	require.True(t, succeed, "time: %s", info)

	// Numbers, booleans, arrays and objects have no temporal reading.
	for _, text := range []string{`20260131`, `true`, `["2026-01-31"]`, `{"d":"2026-01-31"}`} {
		inputs := []FunctionTestInput{
			NewFunctionTestInput(types.T_json.ToType(), makeJSONEncodedFromText(t, []string{text}, nil), nil),
			NewFunctionTestInput(types.T_date.ToType(), []types.Date{}, []bool{}),
		}
		expect := NewFunctionTestResult(types.T_date.ToType(), true, []types.Date{}, nil)
		fcTC := NewFunctionTestCase(proc, inputs, expect, NewCast)
		succeed, info := fcTC.RunAndFree()
		require.True(t, succeed, "%s: %s", text, info)
	}
}
