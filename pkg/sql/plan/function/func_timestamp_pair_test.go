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
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func parseTimestampPairDate(t *testing.T, value string) types.Date {
	t.Helper()
	date, err := types.ParseDateCast(value)
	require.NoError(t, err)
	return date
}

func parseTimestampPairDatetime(t *testing.T, value string, scale int32) types.Datetime {
	t.Helper()
	datetime, err := types.ParseDatetime(value, scale)
	require.NoError(t, err)
	return datetime
}

func parseTimestampPairTime(t *testing.T, value string, scale int32) types.Time {
	t.Helper()
	timeValue, err := types.ParseTime(value, scale)
	require.NoError(t, err)
	return timeValue
}

func runTimestampPairCase(
	t *testing.T,
	proc *process.Process,
	inputs []FunctionTestInput,
	wanted FunctionTestResult,
	selectList *FunctionSelectList,
) *vector.Vector {
	t.Helper()
	testCase := NewFunctionTestCase(proc, inputs, wanted, timestampWithTime).WithSelectList(selectList)
	succeed, errInfo := testCase.Run()
	require.True(t, succeed, errInfo)
	return testCase.GetResultVectorDirectly()
}

func TestTimestampPairTypeResolution(t *testing.T) {
	proc := testutil.NewProcess(t)

	dateTypes := []types.Type{
		types.T_date.ToType(),
		types.T_datetime.ToTypeWithScale(3),
		types.T_timestamp.ToTypeWithScale(3),
		types.T_char.ToType(),
		types.T_varchar.ToType(),
		types.T_text.ToType(),
	}
	timeTypes := []types.Type{
		types.T_time.ToTypeWithScale(6),
		types.T_char.ToType(),
		types.T_varchar.ToType(),
		types.T_text.ToType(),
	}
	for _, dateType := range dateTypes {
		for _, timeType := range timeTypes {
			fn, err := GetFunctionByName(proc.Ctx, "timestamp", []types.Type{dateType, timeType})
			require.NoError(t, err)
			require.Equal(t, EncodeOverloadID(TIMESTAMP, 5), fn.GetEncodedOverloadID())
			wantScale := int32(6)
			if dateType.Oid == types.T_datetime || dateType.Oid == types.T_timestamp {
				if timeType.Oid == types.T_time {
					wantScale = max(dateType.Scale, timeType.Scale)
				}
			}
			require.Equal(t, types.New(types.T_datetime, wantScale, wantScale), fn.GetReturnType())
			_, needCast := fn.ShouldDoImplicitTypeCast()
			require.False(t, needCast)
		}
	}
	for _, test := range []struct {
		args      []types.Type
		wantScale int32
	}{
		{args: []types.Type{types.T_date.ToType(), types.T_time.ToTypeWithScale(0)}, wantScale: 0},
		{args: []types.Type{types.T_datetime.ToTypeWithScale(3), types.T_time.ToTypeWithScale(0)}, wantScale: 3},
		{args: []types.Type{types.T_timestamp.ToTypeWithScale(3), types.T_time.ToTypeWithScale(6)}, wantScale: 6},
	} {
		fn, err := GetFunctionByName(proc.Ctx, "timestamp", test.args)
		require.NoError(t, err)
		require.Equal(t, types.New(types.T_datetime, test.wantScale, test.wantScale), fn.GetReturnType())
	}

	unary, err := GetFunctionByName(proc.Ctx, "timestamp", []types.Type{types.T_date.ToType()})
	require.NoError(t, err)
	require.Equal(t, EncodeOverloadID(TIMESTAMP, 0), unary.GetEncodedOverloadID())
	require.Equal(t, types.T_timestamp, unary.GetReturnType().Oid)

	for _, args := range [][]types.Type{
		{types.T_time.ToType(), types.T_time.ToType()},
		{types.T_date.ToType(), types.T_date.ToType()},
		{types.T_int64.ToType(), types.T_time.ToType()},
		{types.T_date.ToType(), types.T_int64.ToType()},
		{types.T_blob.ToType(), types.T_time.ToType()},
		{types.T_binary.ToType(), types.T_time.ToType()},
		{types.T_varbinary.ToType(), types.T_time.ToType()},
		{types.T_date.ToType(), types.T_blob.ToType()},
		{types.T_date.ToType(), types.T_binary.ToType()},
		{types.T_date.ToType(), types.T_varbinary.ToType()},
	} {
		_, err = GetFunctionByName(proc.Ctx, "timestamp", args)
		require.Error(t, err, "timestamp(%v) must be rejected", args)
	}

	nullPair, err := GetFunctionByName(proc.Ctx, "timestamp", []types.Type{
		types.T_any.ToType(), types.T_any.ToType(),
	})
	require.NoError(t, err)
	targets, needCast := nullPair.ShouldDoImplicitTypeCast()
	require.True(t, needCast)
	require.Equal(t, []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()}, targets)
	require.Equal(t, types.New(types.T_datetime, 6, 6), nullPair.GetReturnType())
}

func TestTimestampPairTypedExecution(t *testing.T) {
	proc := testutil.NewProcess(t)

	dateType := types.T_date.ToType()
	timeType := types.T_time.ToTypeWithScale(6)
	wantedType := types.New(types.T_datetime, 6, 6)
	wanted := []types.Datetime{
		parseTimestampPairDatetime(t, "2024-01-15 12:30:00.123456", 6),
		parseTimestampPairDatetime(t, "2023-12-31 23:00:00.000000", 6),
	}
	result := runTimestampPairCase(t, proc, []FunctionTestInput{
		NewFunctionTestInput(dateType, []types.Date{
			parseTimestampPairDate(t, "2024-01-15"),
			parseTimestampPairDate(t, "2024-01-02"),
		}, nil),
		NewFunctionTestInput(timeType, []types.Time{
			parseTimestampPairTime(t, "12:30:00.123456", 6),
			parseTimestampPairTime(t, "-25:00:00", 6),
		}, nil),
	}, NewFunctionTestResult(wantedType, false, wanted, nil), nil)
	require.Equal(t, wantedType, *result.GetType())

	datetimeType := types.T_datetime.ToTypeWithScale(3)
	result = runTimestampPairCase(t, proc, []FunctionTestInput{
		NewFunctionTestInput(datetimeType, []types.Datetime{
			parseTimestampPairDatetime(t, "2024-01-15 10:00:00.123", 3),
		}, nil),
		NewFunctionTestInput(timeType, []types.Time{
			parseTimestampPairTime(t, "00:00:00.000456", 6),
		}, nil),
	}, NewFunctionTestResult(wantedType, false, []types.Datetime{
		parseTimestampPairDatetime(t, "2024-01-15 10:00:00.123456", 6),
	}, nil), nil)
	require.Equal(t, wantedType, *result.GetType())
}

func TestTimestampPairStringNullAndRangeHandling(t *testing.T) {
	proc := testutil.NewProcess(t)
	wantedType := types.New(types.T_datetime, 6, 6)
	result := runTimestampPairCase(t, proc, []FunctionTestInput{
		NewFunctionTestInput(types.T_text.ToType(), []string{
			"2024-01-15",
			"not-a-date",
			"9999-12-31 23:59:59.999999",
			"0001-01-01",
			"2024-01-15",
			"2024-01-15",
			"2024-01-15",
			"2024-01-15",
			"2024-01-02",
			"2024-01-02",
		}, []bool{false, false, false, false, false, true, false, false, false, false}),
		NewFunctionTestInput(types.T_text.ToType(), []string{
			"12:30:00.123456",
			"12:00:00",
			"00:00:00.000001",
			"-00:00:00.000001",
			"not-a-time",
			"12:00:00",
			"2024-01-15 12:00:00",
			"",
			"1 01:00:00",
			"-1 01:00:00",
		}, nil),
	}, NewFunctionTestResult(wantedType, false, []types.Datetime{
		parseTimestampPairDatetime(t, "2024-01-15 12:30:00.123456", 6),
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		parseTimestampPairDatetime(t, "2024-01-03 01:00:00", 6),
		parseTimestampPairDatetime(t, "2023-12-31 23:00:00", 6),
	}, []bool{false, true, true, true, true, true, true, true, false, false}), nil)
	require.Equal(t, wantedType, *result.GetType())
}

func TestTimestampPairUsesSessionTimezone(t *testing.T) {
	proc := testutil.NewProcess(t)
	location := time.FixedZone("UTC+8", 8*60*60)
	proc.GetSessionInfo().TimeZone = location
	timestamp, err := types.ParseTimestamp(location, "2024-01-15 10:00:00.123", 3)
	require.NoError(t, err)

	wantedType := types.New(types.T_datetime, 6, 6)
	result := runTimestampPairCase(t, proc, []FunctionTestInput{
		NewFunctionTestInput(types.T_timestamp.ToTypeWithScale(3), []types.Timestamp{timestamp}, nil),
		NewFunctionTestInput(types.T_time.ToTypeWithScale(6), []types.Time{
			parseTimestampPairTime(t, "02:30:00.000456", 6),
		}, nil),
	}, NewFunctionTestResult(wantedType, false, []types.Datetime{
		parseTimestampPairDatetime(t, "2024-01-15 12:30:00.123456", 6),
	}, nil), nil)
	require.Equal(t, wantedType, *result.GetType())
}

func TestTimestampPairConstantAndSelectList(t *testing.T) {
	proc := testutil.NewProcess(t)
	dateVector, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("2024-01-15"), 3, proc.Mp())
	require.NoError(t, err)
	defer dateVector.Free(proc.Mp())
	timeVector, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("12:30:00.123456"), 3, proc.Mp())
	require.NoError(t, err)
	defer timeVector.Free(proc.Mp())
	fn, err := GetFunctionByName(proc.Ctx, "timestamp", []types.Type{*dateVector.GetType(), *timeVector.GetType()})
	require.NoError(t, err)
	constantResult, err := RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{dateVector, timeVector}, 3)
	require.NoError(t, err)
	defer constantResult.Free(proc.Mp())
	require.True(t, constantResult.IsConst())
	require.Equal(t, 3, constantResult.Length())
	require.Equal(t, types.T_datetime, constantResult.GetType().Oid)
	require.Equal(t, int32(6), constantResult.GetType().Scale)
	require.Equal(t,
		parseTimestampPairDatetime(t, "2024-01-15 12:30:00.123456", 6),
		vector.MustFixedColWithTypeCheck[types.Datetime](constantResult)[0])

	wantedType := types.New(types.T_datetime, 6, 6)
	maskedResult := runTimestampPairCase(t, proc, []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"not-a-date", "2024-01-15"}, nil),
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"not-a-time", "01:00:00"}, nil),
	}, NewFunctionTestResult(wantedType, false, []types.Datetime{
		0,
		parseTimestampPairDatetime(t, "2024-01-15 01:00:00.000000", 6),
	}, []bool{true, false}), &FunctionSelectList{AnyNull: true, SelectList: []bool{false, true}})
	require.Equal(t, wantedType, *maskedResult.GetType())
}

func TestTimestampPairNullability(t *testing.T) {
	notNull := &planpb.Expr{Typ: planpb.Type{NotNullable: true}}
	require.False(t, DeduceNotNullable(EncodeOverloadID(TIMESTAMP, 5), []*planpb.Expr{notNull, notNull}))
	require.True(t, DeduceNotNullable(EncodeOverloadID(TIMESTAMP, 0), []*planpb.Expr{notNull}))
}
