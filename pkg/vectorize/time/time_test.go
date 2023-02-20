// Copyright 2022 Matrix Origin
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

package time

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestDateToTime(t *testing.T) {
	testCases := []struct {
		name    string
		args    []types.Date
		want    []types.Time
		success bool
	}{
		{
			name:    "TestDateToTime01",
			args:    []types.Date{types.DateFromCalendar(2022, 3, 30)},
			want:    []types.Time{types.TimeFromClock(false, 0, 0, 0, 0)},
			success: true,
		},
	}

	for _, v := range testCases {
		reply := make([]types.Time, len(v.args))
		ns := &nulls.Nulls{}
		reply = DateToTime(v.args, reply)
		require.Equal(t, reply, v.want)
		require.Equal(t, !nulls.Contains(ns, 0), v.success)
	}
}

func TestDatetimeToTime(t *testing.T) {
	testCases := []struct {
		name      string
		inputStr  string
		want      []types.Time
		precision int32
		success   bool
	}{
		{
			name:      "TestDatetimeToTime01",
			inputStr:  "2022-12-12 11:22:33",
			want:      []types.Time{types.TimeFromClock(false, 11, 22, 33, 0)},
			precision: 0,
			success:   true,
		},
		{
			name:      "TestDatetimeToTime02",
			inputStr:  "2022-12-12 11:22:33",
			want:      []types.Time{types.TimeFromClock(false, 11, 22, 33, 0)},
			precision: 3,
			success:   true,
		},
		{
			name:      "TestDatetimeToTime03",
			inputStr:  "2022-12-12 11:22:33.1234",
			want:      []types.Time{types.TimeFromClock(false, 11, 22, 33, 123000)},
			precision: 3,
			success:   true,
		},
		{
			name:      "TestDatetimeToTime03",
			inputStr:  "2022-12-12 11:22:33.1235",
			want:      []types.Time{types.TimeFromClock(false, 11, 22, 33, 124000)},
			precision: 3,
			success:   true,
		},
		{
			name:      "TestDatetimeToTime04",
			inputStr:  "20221212112233",
			want:      []types.Time{types.TimeFromClock(false, 11, 22, 33, 0)},
			precision: 0,
			success:   true,
		},
	}

	for _, v := range testCases {
		// only 1 input
		reply := make([]types.Time, 1)
		dtArr := make([]types.Datetime, 1)
		ns := &nulls.Nulls{}

		var err error
		dtArr[0], err = types.ParseDatetime(v.inputStr, v.precision)
		require.NoError(t, err)

		reply = DatetimeToTime(dtArr, reply, v.precision)
		require.Equal(t, reply, v.want)
		require.Equal(t, !nulls.Contains(ns, 0), v.success)
	}
}

func TestStringToTime(t *testing.T) {
	testCases := []struct {
		name     string
		inputStr string
		want     []types.Time
		success  bool
	}{
		{
			name:     "TestStringToTime01",
			inputStr: "2022-12-12 11:22:33",
			want:     []types.Time{types.TimeFromClock(false, 11, 22, 33, 0)},
			success:  true,
		},
		{
			name:     "TestStringToTime02",
			inputStr: "2022-12-12 11:22:33",
			want:     []types.Time{types.TimeFromClock(false, 11, 22, 33, 0)},
			success:  true,
		},
		{
			name:     "TestStringToTime02",
			inputStr: "2022-12-12 11:22:33.1234",
			want:     []types.Time{types.TimeFromClock(false, 11, 22, 33, 123400)},
			success:  true,
		},
		{
			name:     "TestStringToTime03",
			inputStr: "2022-12-12 11:22:33.1235",
			want:     []types.Time{types.TimeFromClock(false, 11, 22, 33, 123500)},
			success:  true,
		},
		{
			name:     "TestStringToTime04",
			inputStr: "20221212112233",
			want:     []types.Time{types.TimeFromClock(false, 2022121211, 22, 33, 0)},
			success:  true,
		},
		{
			name:     "TestStringToTime05",
			inputStr: "20221212112233.1235",
			want:     []types.Time{types.TimeFromClock(false, 2022121211, 22, 33, 123500)},
			success:  true,
		},
		{
			name:     "TestStringToTime06",
			inputStr: "1122.1235",
			want:     []types.Time{types.TimeFromClock(false, 0, 11, 22, 123500)},
			success:  true,
		},
		{
			name:     "TestStringToTime07",
			inputStr: "-1122.1235",
			want:     []types.Time{types.TimeFromClock(true, 0, 11, 22, 123500)},
			success:  true,
		},
		{
			name:     "TestStringToTime08",
			inputStr: "-3.1235",
			want:     []types.Time{types.TimeFromClock(true, 0, 0, 3, 123500)},
			success:  true,
		},
	}

	for _, v := range testCases {
		// only 1 input
		reply := make([]types.Time, 1)
		strArr := make([]string, 1)
		strArr[0] = v.inputStr
		ns := &nulls.Nulls{}

		var err error
		reply, err = DateStringToTime(strArr, reply)
		require.NoError(t, err)
		require.Equal(t, reply, v.want)
		require.Equal(t, !nulls.Contains(ns, 0), v.success)
	}
}
