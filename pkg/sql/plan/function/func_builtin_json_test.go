// Copyright 2022 Matrix Origin
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
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestJsonArray(t *testing.T) {
	proc := testutil.NewProcess(t)

	// empty
	check(t, proc,
		[]FunctionTestInput{},
		NewFunctionTestResult(types.T_json.ToType(), false, []string{"[]"}, nil),
		newOpBuiltInJsonArray().jsonArray)

	// single null
	check(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{""}, []bool{true}),
		},
		NewFunctionTestResult(types.T_json.ToType(), false, []string{"[null]"}, nil),
		newOpBuiltInJsonArray().jsonArray)

	// int
	check(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1, 2, 3}, []bool{false, false, false}),
		},
		NewFunctionTestResult(types.T_json.ToType(), false,
			[]string{"[1]", "[2]", "[3]"}, nil),
		newOpBuiltInJsonArray().jsonArray)

	// multi-type
	check(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1, 2}, []bool{false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"abc", "def"}, []bool{false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"", ""}, []bool{true, false}),
			NewFunctionTestInput(types.T_bool.ToType(),
				[]bool{true, false}, []bool{false, false}),
		},
		NewFunctionTestResult(types.T_json.ToType(), false,
			[]string{`[1, "abc", null, true]`, `[2, "def", "", false]`}, nil),
		newOpBuiltInJsonArray().jsonArray)

	// float
	check(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_float64.ToType(),
				[]float64{1.5, 2.5}, []bool{false, false}),
		},
		NewFunctionTestResult(types.T_json.ToType(), false,
			[]string{"[1.5]", "[2.5]"}, nil),
		newOpBuiltInJsonArray().jsonArray)

	// bool
	check(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_bool.ToType(),
				[]bool{true, false}, []bool{false, false}),
		},
		NewFunctionTestResult(types.T_json.ToType(), false,
			[]string{"[true]", "[false]"}, nil),
		newOpBuiltInJsonArray().jsonArray)

	// date/time/datetime
	check(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_date.ToType(),
				[]types.Date{types.DateFromCalendar(2021, 2, 1)}, []bool{false}),
			NewFunctionTestInput(types.T_time.ToType(),
				[]types.Time{types.TimeFromClock(false, 11, 11, 11, 0)}, []bool{false}),
		},
		NewFunctionTestResult(types.T_json.ToType(), false,
			[]string{`["2021-02-01", "11:11:11"]`}, nil),
		newOpBuiltInJsonArray().jsonArray)

	// uuid
	uid := types.Uuid{}
	copy(uid[:], "550e8400e29b41d4a716446655440000")
	check(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_uuid.ToType(),
				[]types.Uuid{uid}, []bool{false}),
		},
		NewFunctionTestResult(types.T_json.ToType(), false,
			[]string{`["550e8400-e29b-41d4-a716-446655440000"]`}, nil),
		newOpBuiltInJsonArray().jsonArray)

	// binary -> base64
	check(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varbinary.ToType(),
				[]string{"hello", "world"}, []bool{false, false}),
		},
		NewFunctionTestResult(types.T_json.ToType(), false,
			[]string{fmt.Sprintf(`["%s"]`, base64.StdEncoding.EncodeToString([]byte("hello"))),
				fmt.Sprintf(`["%s"]`, base64.StdEncoding.EncodeToString([]byte("world")))}, nil),
		newOpBuiltInJsonArray().jsonArray)
}

func TestJsonObject(t *testing.T) {
	proc := testutil.NewProcess(t)

	// empty
	check(t, proc,
		[]FunctionTestInput{},
		NewFunctionTestResult(types.T_json.ToType(), false, []string{"{}"}, nil),
		newOpBuiltInJsonObject().jsonObject)

	// simple key-value
	check(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"id", "name"}, []bool{false, false}),
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{87, 99}, []bool{false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"", ""}, []bool{false, false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"carrot", "potato"}, []bool{false, false}),
		},
		NewFunctionTestResult(types.T_json.ToType(), false,
			[]string{`{"id": 87, "name": "carrot"}`, `{"id": 99, "name": "potato"}`}, nil),
		newOpBuiltInJsonObject().jsonObject)

	// null value
	check(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"a"}, []bool{false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{""}, []bool{true}),
		},
		NewFunctionTestResult(types.T_json.ToType(), false,
			[]string{`{"a": null}`}, nil),
		newOpBuiltInJsonObject().jsonObject)

	// bool value
	check(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"flag"}, []bool{false}),
			NewFunctionTestInput(types.T_bool.ToType(),
				[]bool{true}, []bool{false}),
		},
		NewFunctionTestResult(types.T_json.ToType(), false,
			[]string{`{"flag": true}`}, nil),
		newOpBuiltInJsonObject().jsonObject)

	// key overwrite
	check(t, proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"id", "id"}, []bool{false, false}),
			NewFunctionTestInput(types.T_int64.ToType(),
				[]int64{1, 2}, []bool{false, false}),
		},
		NewFunctionTestResult(types.T_json.ToType(), false,
			[]string{`{"id": 2}`, `{"id": 2}`}, nil),
		newOpBuiltInJsonObject().jsonObject)
}

func TestJsonObjectNullKeyError(t *testing.T) {
	proc := testutil.NewProcess(t)

	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"", ""}, []bool{true, false}),
			NewFunctionTestInput(types.T_varchar.ToType(),
				[]string{"", ""}, []bool{false, false}),
		},
		NewFunctionTestResult(types.T_json.ToType(), false,
			[]string{"", ""}, []bool{false, false}),
		newOpBuiltInJsonObject().jsonObject)
	s, _ := tc.Run()
	require.False(t, s)
}

func check(t *testing.T, proc *process.Process, inputs []FunctionTestInput, expect FunctionTestResult,
	fn func(params []*vector.Vector, result vector.FunctionResultWrapper,
		proc *process.Process, length int, selectList *FunctionSelectList) error) {
	t.Helper()
	tc := NewFunctionTestCase(proc, inputs, expect, fn)
	s, info := tc.Run()
	require.True(t, s, info)
}
