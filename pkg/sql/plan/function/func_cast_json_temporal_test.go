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

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
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

// CAST(date/time/datetime/timestamp AS JSON), JSON_ARRAY and JSON_OBJECT store
// temporal values as typed DATE/TIME/DATETIME JSON scalars, and JSON_EXTRACT
// keeps the tag. Casting such a scalar back to a temporal type must work and
// mean the same as the SQL temporal cast, next to string and NULL controls.
func TestCastTypedJsonTemporalToTemporal(t *testing.T) {
	proc := testutil.NewProcess(t)
	zone := proc.GetSessionInfo().TimeZone

	date, err := types.ParseDateCast("2026-01-31")
	require.NoError(t, err)
	tm, err := types.ParseTime("12:34:56.5", 6)
	require.NoError(t, err)
	dt, err := types.ParseDatetime("2024-02-29 13:14:15.25", 6)
	require.NoError(t, err)
	ts, err := types.ParseTimestamp(zone, "2024-02-29 13:14:15.25", 6)
	require.NoError(t, err)

	encode := func(bj bytejson.ByteJson) string {
		enc, err := types.EncodeJson(bj)
		require.NoError(t, err)
		return string(enc)
	}
	// Built exactly as castToJSON builds them.
	jDate := encode(newTypedByteJson(bytejson.TpCodeDate, date.String()))
	jTime := encode(newTypedByteJson(bytejson.TpCodeTime, tm.String2(6)))
	jDatetime := encode(newTypedByteJson(bytejson.TpCodeDatetime, dt.String2(6)))
	jTimestamp := encode(newTypedByteJson(bytejson.TpCodeDatetime, ts.String2(zone, 6)))
	controls := makeJSONEncodedFromText(t, []string{`"2026-01-31 01:02:03"`, `null`}, nil)

	run := func(name string, in []string, to types.Type, expected any, nulls []bool) {
		t.Helper()
		fcTC := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(types.T_json.ToType(), in, nil),
				NewFunctionTestInput(to, expected, []bool{}),
			},
			NewFunctionTestResult(to, false, expected, nulls), NewCast)
		succeed, info := fcTC.RunAndFree()
		require.True(t, succeed, "%s: %s", name, info)
	}
	rows := []string{jDate, jTime, jDatetime, jTimestamp, controls[0], controls[1]}
	nulls := []bool{false, false, false, false, false, true}

	ctrlDT, err := types.ParseDatetime("2026-01-31 01:02:03", 6)
	require.NoError(t, err)

	run("date", rows, types.T_date.ToType(),
		[]types.Date{date, tm.ToDate(), dt.ToDate(), dt.ToDate(), date, 0}, nulls)

	dt6 := types.New(types.T_datetime, 0, 6)
	run("datetime", rows, dt6,
		[]types.Datetime{date.ToDatetime(), tm.ToDatetime(6), dt, dt, ctrlDT, 0}, nulls)

	ts6 := types.New(types.T_timestamp, 0, 6)
	run("timestamp", rows, ts6,
		[]types.Timestamp{date.ToDatetime().ToTimestamp(zone), tm.ToDatetime(6).ToTimestamp(zone),
			ts, ts, ctrlDT.ToTimestamp(zone), 0}, nulls)

	time6 := types.New(types.T_time, 0, 6)
	run("time", rows, time6,
		[]types.Time{0, tm, dt.ToTime(6), dt.ToTime(6), ctrlDT.ToTime(6), 0}, nulls)
}
