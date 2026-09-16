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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

func TestMakeDateIntegerCalendarBounds(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		year, day int64
		want      string
	}{
		{0, 1, "2000-01-01"}, {69, 1, "2069-01-01"}, {70, 1, "1970-01-01"}, {100, 1, "0100-01-01"},
		{2024, 60, "2024-02-29"}, {2023, 366, "2024-01-01"}, {9999, 365, "9999-12-31"},
		{9999, 366, ""}, {10000, 1, ""}, {-1, 1, ""}, {2024, 0, ""}, {2024, -1, ""},
		{2024, 1<<32 + 1, ""}, {2024, math.MaxInt64, ""}, {math.MinInt64, 1, ""},
	} {
		test := NewFunctionTestCase(proc, []FunctionTestInput{
			NewFunctionTestInput(types.T_int64.ToType(), []int64{tc.year}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{tc.day}, nil),
		}, NewFunctionTestResult(types.T_varchar.ToType(), false, []string{tc.want}, []bool{tc.want == ""}), MakeDateInteger)
		ok, info := test.Run()
		require.True(t, ok, "year=%d day=%d: %s", tc.year, tc.day, info)
	}
	for _, mask := range []*FunctionSelectList{nil, {AllNull: true}, {AnyNull: true, SelectList: []bool{false, true, false}}} {
		wants, nulls := []string{"2024-01-01", "2024-01-02", ""}, []bool{false, false, true}
		if mask != nil {
			wants[0] = ""
			nulls[0] = true
			if mask.AllNull {
				wants[1] = ""
				nulls[1] = true
			}
		}
		test := NewFunctionTestCase(proc, []FunctionTestInput{
			NewFunctionTestConstInput(types.T_int64.ToType(), []int64{2024}, nil),
			NewFunctionTestInput(types.T_int64.ToType(), []int64{1, 2, math.MaxInt64}, []bool{false, false, true}),
		}, NewFunctionTestResult(types.T_varchar.ToType(), false, wants, nulls), MakeDateInteger).WithSelectList(mask)
		ok, info := test.Run()
		require.True(t, ok, info)
	}
}

func TestMakeDateIntegerAndLegacyIdentities(t *testing.T) {
	proc := testutil.NewProcess(t)
	require.Len(t, allSupportedFunctions[MAKEDATE].Overloads, 2)
	for _, source := range []types.T{types.T_int64, types.T_uint64, types.T_float64, types.T_decimal128, types.T_varchar, types.T_enum, types.T_uuid, types.T_date, types.T_time, types.T_datetime, types.T_timestamp} {
		resolved, err := GetFunctionByName(proc.Ctx, "makedate", []types.Type{source.ToType(), types.T_int64.ToType()})
		require.NoError(t, err, source.String())
		require.Equal(t, int32(IntegerMakeDateOverload), resolved.overloadId)
	}
	for _, source := range []types.T{types.T_json, types.T_array_float32, types.T_array_float64} {
		_, err := GetFunctionByName(proc.Ctx, "makedate", []types.Type{source.ToType(), types.T_int64.ToType()})
		require.Error(t, err, source.String())
	}
	year := newVectorByType(proc.Mp(), types.T_varchar.ToType(), []string{"2024"}, nil)
	defer year.Free(proc.Mp())
	day := newVectorByType(proc.Mp(), types.T_varchar.ToType(), []string{"1.5"}, nil)
	defer day.Free(proc.Mp())
	out, err := RunFunctionDirectly(proc, EncodeOverloadID(MAKEDATE, 0), []*vector.Vector{year, day}, 1)
	require.NoError(t, err)
	defer out.Free(proc.Mp())
	require.Equal(t, "2024-01-01", out.GetStringAt(0), "legacy VARCHAR identity still truncates")
}
