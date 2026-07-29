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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestDateExtractFunctionsAcceptVarchar(t *testing.T) {
	proc := testutil.NewProcess(t)
	input := newVectorByType(proc.Mp(), types.T_varchar.ToType(), []string{"2024-01-15"}, nil)
	defer input.Free(proc.Mp())

	for _, tc := range []struct {
		name       string
		returnType types.T
		want       any
	}{
		{
			name:       "dayofmonth",
			returnType: types.T_uint8,
			want:       uint8(15),
		},
		{
			name:       "dayname",
			returnType: types.T_varchar,
			want:       "Monday",
		},
		{
			name:       "monthname",
			returnType: types.T_varchar,
			want:       "January",
		},
		{
			name:       "quarter",
			returnType: types.T_uint8,
			want:       uint8(1),
		},
		{
			name:       "weekofyear",
			returnType: types.T_int64,
			want:       int64(3),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fn, err := GetFunctionByName(proc.Ctx, tc.name, []types.Type{types.T_varchar.ToType()})
			require.NoError(t, err)
			require.Equal(t, tc.returnType, fn.GetReturnType().Oid)

			out, err := RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{input}, 1)
			require.NoError(t, err)
			defer out.Free(proc.Mp())

			switch want := tc.want.(type) {
			case uint8:
				require.Equal(t, want, vector.GetFixedAtNoTypeCheck[uint8](out, 0))
			case int64:
				require.Equal(t, want, vector.GetFixedAtNoTypeCheck[int64](out, 0))
			case string:
				require.Equal(t, want, string(out.GetBytesAt(0)))
			default:
				t.Fatalf("unsupported expected type %T", want)
			}
		})
	}

	boundaryInput := NewFunctionTestInput(types.T_varchar.ToType(), []string{"2005-01-01"}, nil)
	boundaryExpect := NewFunctionTestResult(types.T_int64.ToType(), false, []int64{53}, nil)
	boundary := NewFunctionTestCase(proc, []FunctionTestInput{boundaryInput}, boundaryExpect, DateStringToWeekOfYear)
	success, info := boundary.Run()
	require.True(t, success, info)
}

func TestDateExtractFunctionsAcceptRelaxedDateDelimiters(t *testing.T) {
	proc := testutil.NewProcess(t)
	input := NewFunctionTestInput(types.T_varchar.ToType(),
		[]string{"10:11:12", "69:01:01", "70:01:01", "2024/01/15 12*34*56"}, nil)

	for _, tc := range []struct {
		name   string
		fn     fEvalFn
		expect FunctionTestResult
	}{
		{
			name: "dayofmonth",
			fn:   DateStringToDay,
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{12, 1, 1, 15}, nil),
		},
		{
			name: "dayname",
			fn:   DateStringToDayName,
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"Friday", "Tuesday", "Thursday", "Monday"}, nil),
		},
		{
			name: "monthname",
			fn:   DateStringToMonthName,
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"November", "January", "January", "January"}, nil),
		},
		{
			name: "quarter",
			fn:   DateStringToQuarter,
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{4, 1, 1, 1}, nil),
		},
		{
			name: "weekofyear",
			fn:   DateStringToWeekOfYear,
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{45, 1, 1, 3}, nil),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ftc := NewFunctionTestCase(proc, []FunctionTestInput{input}, tc.expect, tc.fn)
			success, info := ftc.Run()
			require.True(t, success, info)
		})
	}
}

func TestDateExtractFunctionsIncompleteDateVarchar(t *testing.T) {
	proc := testutil.NewProcess(t)
	input := newVectorByType(proc.Mp(), types.T_varchar.ToType(), []string{"2001-11-00"}, nil)
	defer input.Free(proc.Mp())

	for _, tc := range []struct {
		name       string
		returnType types.T
		want       any
		wantNull   bool
	}{
		{name: "dayofmonth", returnType: types.T_uint8, want: uint8(0)},
		{name: "quarter", returnType: types.T_uint8, want: uint8(4)},
		{name: "monthname", returnType: types.T_varchar, want: "November"},
		{name: "weekofyear", returnType: types.T_int64, wantNull: true},
		{name: "dayname", returnType: types.T_varchar, wantNull: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fn, err := GetFunctionByName(proc.Ctx, tc.name, []types.Type{types.T_varchar.ToType()})
			require.NoError(t, err)
			require.Equal(t, tc.returnType, fn.GetReturnType().Oid)

			out, err := RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{input}, 1)
			require.NoError(t, err)
			defer out.Free(proc.Mp())
			require.Equal(t, tc.wantNull, out.IsNull(0))
			if tc.wantNull {
				return
			}

			switch want := tc.want.(type) {
			case uint8:
				require.Equal(t, want, vector.GetFixedAtNoTypeCheck[uint8](out, 0))
			case string:
				require.Equal(t, want, string(out.GetBytesAt(0)))
			default:
				t.Fatalf("unsupported expected type %T", want)
			}
		})
	}
}

func TestQuarterTimestampRegisteredOverload(t *testing.T) {
	proc := testutil.NewProcess(t)
	timestamp, err := types.ParseTimestamp(time.UTC, "2024-10-15 12:30:00", 6)
	require.NoError(t, err)
	input := newVectorByType(proc.Mp(), types.T_timestamp.ToType(), []types.Timestamp{timestamp}, nil)
	defer input.Free(proc.Mp())

	fn, err := GetFunctionByName(proc.Ctx, "quarter", []types.Type{types.T_timestamp.ToType()})
	require.NoError(t, err)
	require.Equal(t, types.T_uint8, fn.GetReturnType().Oid)

	out, err := RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{input}, 1)
	require.NoError(t, err)
	defer out.Free(proc.Mp())
	require.Equal(t, uint8(4), vector.GetFixedAtNoTypeCheck[uint8](out, 0))
}

func TestTimestampToWeekOfYearValidValue(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().TimeZone = nil
	timestamp, err := types.ParseTimestamp(time.Local, "2024-01-01 12:30:00", 6)
	require.NoError(t, err)
	input := NewFunctionTestInput(types.T_timestamp.ToType(), []types.Timestamp{timestamp}, nil)
	expect := NewFunctionTestResult(types.T_int64.ToType(), false, []int64{1}, nil)
	ftc := NewFunctionTestCase(proc, []FunctionTestInput{input}, expect, TimestampToWeekOfYear)
	success, info := ftc.Run()
	require.True(t, success, info)
}

func TestDateExtractStringFunctionsNullAndInvalidInputs(t *testing.T) {
	proc := testutil.NewProcess(t)
	input := NewFunctionTestInput(types.T_varchar.ToType(),
		[]string{"2024-01-15", "ignored", "not-a-date", "0000-00-00", "2024-12-31"},
		[]bool{false, true, false, false, false})

	for _, tc := range []struct {
		name   string
		fn     fEvalFn
		expect FunctionTestResult
	}{
		{
			name: "dayofmonth",
			fn:   DateStringToDay,
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{15, 0, 0, 0, 31}, []bool{false, true, true, false, false}),
		},
		{
			name: "quarter",
			fn:   DateStringToQuarter,
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{1, 0, 0, 0, 4}, []bool{false, true, true, false, false}),
		},
		{
			name: "weekofyear",
			fn:   DateStringToWeekOfYear,
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{3, 0, 0, 0, 1}, []bool{false, true, true, true, false}),
		},
		{
			name: "dayname",
			fn:   DateStringToDayName,
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"Monday", "", "", "", "Tuesday"}, []bool{false, true, true, true, false}),
		},
		{
			name: "monthname",
			fn:   DateStringToMonthName,
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"January", "", "", "", "December"}, []bool{false, true, true, true, false}),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ftc := NewFunctionTestCase(proc, []FunctionTestInput{input}, tc.expect, tc.fn)
			success, info := ftc.Run()
			require.True(t, success, info)
		})
	}
}

func TestDateExtractStringFunctionsIncompleteDates(t *testing.T) {
	proc := testutil.NewProcess(t)
	input := NewFunctionTestInput(types.T_varchar.ToType(),
		[]string{"2001-11-00 12:34:56", "20011100", "2001-11-0", "0000-00-00 12:34:56", "2024-13-01"}, nil)

	for _, tc := range []struct {
		name   string
		fn     fEvalFn
		expect FunctionTestResult
	}{
		{
			name: "dayofmonth",
			fn:   DateStringToDay,
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{0, 0, 0, 0, 0}, []bool{false, false, false, false, true}),
		},
		{
			name: "quarter",
			fn:   DateStringToQuarter,
			expect: NewFunctionTestResult(types.T_uint8.ToType(), false,
				[]uint8{4, 4, 4, 0, 0}, []bool{false, false, false, false, true}),
		},
		{
			name: "weekofyear",
			fn:   DateStringToWeekOfYear,
			expect: NewFunctionTestResult(types.T_int64.ToType(), false,
				[]int64{0, 0, 0, 0, 0}, []bool{true, true, true, true, true}),
		},
		{
			name: "dayname",
			fn:   DateStringToDayName,
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"", "", "", "", ""}, []bool{true, true, true, true, true}),
		},
		{
			name: "monthname",
			fn:   DateStringToMonthName,
			expect: NewFunctionTestResult(types.T_varchar.ToType(), false,
				[]string{"November", "November", "November", "", ""}, []bool{false, false, false, true, true}),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ftc := NewFunctionTestCase(proc, []FunctionTestInput{input}, tc.expect, tc.fn)
			success, info := ftc.Run()
			require.True(t, success, info)
		})
	}
}

func TestDateStringToStringCallbackInvalid(t *testing.T) {
	proc := testutil.NewProcess(t)
	input := NewFunctionTestInput(types.T_varchar.ToType(), []string{"2024-01-15"}, nil)
	fn := func(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
		return dateStringToStringWithNullOnError(ivecs, result, proc, length, selectList, func(dateExtractParts) (string, bool) {
			return "", false
		})
	}
	ftc := NewFunctionTestCase(proc, []FunctionTestInput{input},
		NewFunctionTestResult(types.T_varchar.ToType(), false, []string{""}, []bool{true}), fn)
	success, info := ftc.Run()
	require.True(t, success, info)
}

func TestDateExtractStringFunctionsIgnoreAllRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	input := NewFunctionTestInput(types.T_varchar.ToType(),
		[]string{"2024-01-15", "not-a-date"}, nil)

	for _, tc := range []struct {
		name       string
		resultType types.Type
		fn         fEvalFn
	}{
		{
			name:       "dayofmonth",
			resultType: types.T_uint8.ToType(),
			fn:         DateStringToDay,
		},
		{
			name:       "dayname",
			resultType: types.T_varchar.ToType(),
			fn:         DateStringToDayName,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ftc := NewFunctionTestCase(proc, []FunctionTestInput{input},
				NewFunctionTestResult(tc.resultType, false, nil, nil), tc.fn)
			require.NoError(t, ftc.result.PreExtendAndReset(ftc.fnLength))
			require.NoError(t, tc.fn(ftc.parameters, ftc.result, ftc.proc, ftc.fnLength,
				&FunctionSelectList{AllNull: true}))

			result := ftc.result.GetResultVector()
			require.Equal(t, ftc.fnLength, result.Length())
			for i := uint64(0); i < uint64(ftc.fnLength); i++ {
				require.Truef(t, result.GetNulls().Contains(i), "row %d should be NULL", i)
			}
		})
	}
}

func TestDateExtractStringFunctionsSelectList(t *testing.T) {
	proc := testutil.NewProcess(t)
	input := NewFunctionTestInput(types.T_varchar.ToType(),
		[]string{"2024-01-15", "2024-12-31"}, nil)

	for _, tc := range []struct {
		name       string
		resultType types.Type
		fn         fEvalFn
	}{
		{name: "dayofmonth", resultType: types.T_uint8.ToType(), fn: DateStringToDay},
		{name: "dayname", resultType: types.T_varchar.ToType(), fn: DateStringToDayName},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ftc := NewFunctionTestCase(proc, []FunctionTestInput{input},
				NewFunctionTestResult(tc.resultType, false, nil, nil), tc.fn)
			require.NoError(t, ftc.result.PreExtendAndReset(ftc.fnLength))
			require.NoError(t, tc.fn(ftc.parameters, ftc.result, ftc.proc, ftc.fnLength,
				&FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}}))

			result := ftc.result.GetResultVector()
			require.False(t, result.GetNulls().Contains(0))
			require.True(t, result.GetNulls().Contains(1))
		})
	}
}
