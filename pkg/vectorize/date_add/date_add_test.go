// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package date_add

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestDateAdd(t *testing.T) {
	testCases := []struct {
		name    string
		args1   []types.Date
		args2   []int64
		args3   []int64
		want    []types.Date
		success bool
	}{
		{
			args1:   []types.Date{types.DateFromCalendar(2021, 8, 13)},
			args2:   []int64{1},
			args3:   []int64{int64(types.Day)},
			want:    []types.Date{types.DateFromCalendar(2021, 8, 14)},
			success: true,
		},
		{
			args1:   []types.Date{types.DateFromCalendar(2021, 1, 31)},
			args2:   []int64{1},
			args3:   []int64{int64(types.Month)},
			want:    []types.Date{types.DateFromCalendar(2021, 2, 28)},
			success: true,
		},
		{
			args1:   []types.Date{types.DateFromCalendar(9999, 12, 31)},
			args2:   []int64{1},
			args3:   []int64{int64(types.Day)},
			want:    []types.Date{types.DateFromCalendar(1, 1, 1)},
			success: false,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			got := make([]types.Date, len(c.args1))
			xnu := &nulls.Nulls{}
			ynu := &nulls.Nulls{}
			rnu := &nulls.Nulls{}
			d, e := DateAdd(c.args1, c.args2, c.args3, xnu, ynu, rnu, got)
			if c.success {
				require.Equal(t, e, nil)
			} else {
				require.NotEqual(t, e, nil)
			}
			require.Equal(t, c.want, d)
		})
	}

}

func TestDatetimeAdd(t *testing.T) {
	testCases := []struct {
		name    string
		args1   []types.Datetime
		args2   []int64
		args3   []int64
		want    []types.Datetime
		success bool
	}{
		{
			args1:   []types.Datetime{types.DatetimeFromClock(2020, 1, 1, 1, 1, 1, 1)},
			args2:   []int64{1},
			args3:   []int64{int64(types.MicroSecond)},
			want:    []types.Datetime{types.DatetimeFromClock(2020, 1, 1, 1, 1, 1, 2)},
			success: true,
		},
		{
			args1:   []types.Datetime{types.DatetimeFromClock(2020, 1, 1, 1, 1, 1, 1)},
			args2:   []int64{1},
			args3:   []int64{int64(types.Second)},
			want:    []types.Datetime{types.DatetimeFromClock(2020, 1, 1, 1, 1, 2, 1)},
			success: true,
		},
		{
			args1:   []types.Datetime{types.DatetimeFromClock(9999, 1, 1, 1, 1, 1, 1)},
			args2:   []int64{1},
			args3:   []int64{int64(types.Year)},
			want:    []types.Datetime{types.DatetimeFromClock(1, 1, 1, 0, 0, 0, 0)},
			success: false,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			got := make([]types.Datetime, len(c.args1))
			xnu := &nulls.Nulls{}
			ynu := &nulls.Nulls{}
			rnu := &nulls.Nulls{}
			d, e := DatetimeAdd(c.args1, c.args2, c.args3, xnu, ynu, rnu, got)
			if c.success {
				require.Equal(t, e, nil)
			} else {
				require.NotEqual(t, e, nil)
			}
			require.Equal(t, c.want, d)
		})
	}

}

func TestDateStringAdd(t *testing.T) {
	testCases := []struct {
		name    string
		args1   []string
		args2   []int64
		args3   []int64
		want    []types.Datetime
		success bool
	}{
		{
			args1:   []string{"2018-01-01"},
			args2:   []int64{1},
			args3:   []int64{int64(types.Day)},
			want:    []types.Datetime{types.DatetimeFromClock(2018, 1, 2, 0, 0, 0, 0)},
			success: true,
		},
		{
			args1:   []string{"2018-01-01"},
			args2:   []int64{1},
			args3:   []int64{int64(types.Second)},
			want:    []types.Datetime{types.DatetimeFromClock(2018, 1, 1, 0, 0, 1, 0)},
			success: true,
		},
		{
			args1:   []string{"2018-01-01 00:00:01"},
			args2:   []int64{1},
			args3:   []int64{int64(types.Second)},
			want:    []types.Datetime{types.DatetimeFromClock(2018, 1, 1, 0, 0, 2, 0)},
			success: true,
		},
		{
			args1:   []string{"xxxx"},
			args2:   []int64{1},
			args3:   []int64{int64(types.Second)},
			want:    []types.Datetime{types.DatetimeFromClock(1, 1, 1, 0, 0, 0, 0)},
			success: false,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			got := make([]types.Datetime, len(c.args1))
			xnu := &nulls.Nulls{}
			ynu := &nulls.Nulls{}
			rnu := &nulls.Nulls{}
			d, e := DateStringAdd(c.args1, c.args2, c.args3, xnu, ynu, rnu, got)
			if c.success {
				require.Equal(t, e, nil)
			} else {
				require.NotEqual(t, e, nil)
			}
			require.Equal(t, c.want, d)
		})
	}

}

func TestTimeStampAdd(t *testing.T) {
	testCases := []struct {
		name    string
		args1   []types.Timestamp
		args2   []int64
		args3   []int64
		want    []types.Timestamp
		success bool
	}{
		{
			args1:   []types.Timestamp{types.FromClockUTC(2020, 1, 1, 1, 1, 1, 1)},
			args2:   []int64{1},
			args3:   []int64{int64(types.MicroSecond)},
			want:    []types.Timestamp{types.FromClockUTC(2020, 1, 1, 1, 1, 1, 2)},
			success: true,
		},
		{
			args1:   []types.Timestamp{types.FromClockUTC(2038, 1, 19, 3, 14, 7, 999999)},
			args2:   []int64{1},
			args3:   []int64{int64(types.MicroSecond)},
			want:    []types.Timestamp{types.FromClockUTC(2038, 1, 19, 3, 14, 8, 0)},
			success: true,
		},
	}

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			got := make([]types.Timestamp, len(c.args1))
			xnu := &nulls.Nulls{}
			ynu := &nulls.Nulls{}
			rnu := &nulls.Nulls{}
			rs, err := TimestampAdd(time.Local, c.args1, c.args2, c.args3, xnu, ynu, rnu, got)
			require.Equal(t, c.want, rs)
			if c.success {
				require.Equal(t, err, nil)
			} else {
				require.NotEqual(t, err, nil)
			}
		})
	}

}
