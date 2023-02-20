// Copyright 2021 Matrix Origin
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

package types

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var dayInMonth []int = []int{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}

func TestDate(t *testing.T) {
	fmt.Println(DateFromCalendar(1215, 6, 15).Calendar(true))
	fmt.Println(DateFromCalendar(1776, 7, 4).Calendar(true))
	fmt.Println(DateFromCalendar(1989, 4, 26).Calendar(true))
	fmt.Println(DateFromCalendar(2019, 6, 9).Calendar(true))

	for i := 1; i <= 3000; i++ {
		for j := 1; j <= len(dayInMonth); j++ {
			for k := 1; k < days(i, j); k++ {
				tsys := time.Date(i, time.Month(j), k, 0, 0, 0, 0, time.UTC)
				y, m, d := tsys.Date()
				yw, w := tsys.ISOWeek()
				wd := tsys.Weekday()

				t := DateFromCalendar(int32(i), uint8(j), uint8(k))
				y1, m1, d1, _ := t.Calendar(true)
				yw1, w1 := t.WeekOfYear()
				wd1 := t.DayOfWeek()

				if y != int(y1) || m != time.Month(m1) || d != int(d1) || yw != int(yw1) || w != int(w1) || wd != time.Weekday(wd1) {
					fmt.Printf("=== %v %v %v", i, j, k)
				}
			}
		}
	}
}

func days(year, mon int) int {
	if mon == 2 && (year%4 == 0 && (year%100 != 0 || year%400 == 0)) {
		return 29
	} else {
		return dayInMonth[mon-1]
	}
}

func TestDatetime(t *testing.T) {
	dt := DatetimeFromClock(2021, 8, 13, 17, 55, 34, 0)
	fmt.Println(dt.ToDate().Calendar(true))
	fmt.Println(dt.Clock())

	dt = Now(time.UTC)
	fmt.Println(dt.ToDate().Calendar(true))
	fmt.Println(dt.Clock())

	loc, _ := time.LoadLocation("America/Los_Angeles")
	dt = Now(loc)
	fmt.Println(dt.ToDate().Calendar(true))
	fmt.Println(dt.Clock())
}

func TestAddDatetime(t *testing.T) {
	addDateTimeTbl := []struct {
		Input              string
		InputIntervalNum   string
		InputIntervalTypes IntervalType
		expect             string
		success            bool
	}{
		{"2022-01-31 00:00:00", "1", MicroSecond, "2022-01-31 00:00:00.000001", true},
		{"2022-01-31 00:00:00.000001", "1", MicroSecond, "2022-01-31 00:00:00.000002", true},
		{"2022-01-31 00:00:00.000001", "-1", MicroSecond, "2022-01-31 00:00:00.000000", true},
		{"2022-01-31 00:00:00", "1", Second, "2022-01-31 00:00:01.000000", true},
		{"2022-01-31 00:00:00", "1", Minute, "2022-01-31 00:01:00.000000", true},
		{"2022-01-31 00:00:00", "1", Hour, "2022-01-31 01:00:00.000000", true},
		{"2022-01-31 00:00:00", "1", Day, "2022-02-01 00:00:00.000000", true},
		{"2022-01-31 00:00:00", "1", Week, "2022-02-07 00:00:00.000000", true},
		{"2022-01-01 00:00:00", "1", Month, "2022-02-01 00:00:00.000000", true},
		{"2022-01-31 00:00:00", "1", Month, "2022-02-28 00:00:00.000000", true},
		{"2022-01-01 00:00:00", "1", Quarter, "2022-04-01 00:00:00.000000", true},
		{"2022-01-31 00:00:00", "1", Quarter, "2022-04-30 00:00:00.000000", true},
		{"2022-01-01 00:00:00", "1", Year, "2023-01-01 00:00:00.000000", true},
		{"2020-02-29 00:00:00", "1", Year, "2021-02-28 00:00:00.000000", true},

		{"2022-01-01 00:00:00", "1.1", Second_MicroSecond, "2022-01-01 00:00:01.100000", true},
		{"2022-01-01 00:00:00", "1:1.1", Minute_MicroSecond, "2022-01-01 00:01:01.100000", true},
		{"2022-01-01 00:00:00", "1:1", Minute_Second, "2022-01-01 00:01:01.000000", true},
		{"2022-01-01 00:00:00", "1:1:1.1", Hour_MicroSecond, "2022-01-01 01:01:01.100000", true},
		{"2022-01-01 00:00:00", "1:1:1", Hour_Second, "2022-01-01 01:01:01.000000", true},
		{"2022-01-01 00:00:00", "1:1", Hour_Minute, "2022-01-01 01:01:00.000000", true},
		{"2022-01-01 00:00:00", "1 1:1:1.1", Day_MicroSecond, "2022-01-02 01:01:01.100000", true},
		{"2022-01-01 00:00:00", "1 1:1:1", Day_Second, "2022-01-02 01:01:01.000000", true},
		{"2022-01-01 00:00:00", "1 1:1", Day_Minute, "2022-01-02 01:01:00.000000", true},
		{"2022-01-01 00:00:00", "1 1", Day_Hour, "2022-01-02 01:00:00.000000", true},
		{"2022-01-01 00:00:00", "1-1", Year_Month, "2023-02-01 00:00:00.000000", true},
		{"2022-02-01 00:00:00", "-1-1", Year_Month, "2021-01-01 00:00:00.000000", true},

		{"2022-01-31 00:00:00", "1-1", Year_Month, "2023-02-28 00:00:00.000000", true},

		{"2020-12-31 23:59:59", "1", Second, "2021-01-01 00:00:00.000000", true},
		{"2100-12-31 23:59:59", "1:1", Minute_Second, "2101-01-01 00:01:00.000000", true},
		{"1992-12-31 23:59:59.000002", "1.999999", Second_MicroSecond, "1993-01-01 00:00:01.000001", true},
		{"1992-12-31 23:59:59.1", "1.1", Second_MicroSecond, "1993-01-01 00:00:00.200000", true},
		{"2022-01-31 00:00:00.1", "100000", Month, "0001-01-01 00:00:00.000000", false},
		{"2022-01-31 00:00:00.999999", "1", MicroSecond, "2022-01-31 00:00:01.000000", true},
		{"2022-01-31 00:00:00.888888", "1", MicroSecond, "2022-01-31 00:00:00.888889", true},
	}
	for _, test := range addDateTimeTbl {
		ret, rettype, _ := NormalizeInterval(test.InputIntervalNum, test.InputIntervalTypes)
		d, err := ParseDatetime(test.Input, 6)
		require.Equal(t, err, nil)
		d, b := d.AddInterval(ret, rettype, DateTimeType)
		require.Equal(t, d.String2(6), test.expect)
		require.Equal(t, b, test.success)

	}
}

func TestSubDateTime(t *testing.T) {
	subDateTimeTbl := []struct {
		Input              string
		InputIntervalNum   string
		InputIntervalTypes IntervalType
		expect             string
		success            bool
	}{
		{"2022-01-31 00:00:00", "1", MicroSecond, "2022-01-30 23:59:59.999999", true},
		{"2022-01-31 00:00:00.000001", "1", MicroSecond, "2022-01-31 00:00:00.000000", true},
		{"2022-01-31 00:00:00", "1", Second, "2022-01-30 23:59:59.000000", true},
		{"2022-01-31 00:00:00", "1", Minute, "2022-01-30 23:59:00.000000", true},
		{"2022-01-31 00:00:00", "1", Hour, "2022-01-30 23:00:00.000000", true},
		{"2022-01-31 00:00:00", "1", Day, "2022-01-30 00:00:00.000000", true},
		{"2022-01-31 00:00:00", "1", Week, "2022-01-24 00:00:00.000000", true},
		{"2022-01-01 00:00:00", "1", Month, "2021-12-01 00:00:00.000000", true},
		{"2022-03-31 00:00:00", "1", Month, "2022-02-28 00:00:00.000000", true},
		{"2022-01-01 00:00:00", "1", Quarter, "2021-10-01 00:00:00.000000", true},
		{"2022-01-31 00:00:00", "1", Quarter, "2021-10-31 00:00:00.000000", true},
		{"2022-01-01 00:00:00", "1", Year, "2021-01-01 00:00:00.000000", true},
		{"2022-01-01 00:00:00", "-1", Year, "2023-01-01 00:00:00.000000", true},
		{"2020-02-29 00:00:00", "1", Year, "2019-02-28 00:00:00.000000", true},

		{"2022-01-01 00:00:00", "1.1", Second_MicroSecond, "2021-12-31 23:59:58.900000", true},
		{"2022-01-01 00:00:00", "-1.1", Second_MicroSecond, "2022-01-01 00:00:01.100000", true},
		{"2022-01-01 00:00:00", "1:1.1", Minute_MicroSecond, "2021-12-31 23:58:58.900000", true},
		{"2022-01-01 00:00:00", "1:1", Minute_Second, "2021-12-31 23:58:59.000000", true},
		{"2022-01-01 00:00:00", "1:1:1.1", Hour_MicroSecond, "2021-12-31 22:58:58.900000", true},
		{"2022-01-01 00:00:00", "1:1:1", Hour_Second, "2021-12-31 22:58:59.000000", true},
		{"2022-01-01 00:00:00", "1:1", Hour_Minute, "2021-12-31 22:59:00.000000", true},
		{"2022-01-01 00:00:00", "1 1:1:1.1", Day_MicroSecond, "2021-12-30 22:58:58.900000", true},
		{"2022-01-01 00:00:00", "1 1:1:1", Day_Second, "2021-12-30 22:58:59.000000", true},
		{"2022-01-01 00:00:00", "1 1:1", Day_Minute, "2021-12-30 22:59:00.000000", true},
		{"2022-01-01 00:00:00", "1 1", Day_Hour, "2021-12-30 23:00:00.000000", true},
		{"2022-01-01 00:00:00", "1-1", Year_Month, "2020-12-01 00:00:00.000000", true},
		{"2022-01-31 00:00:00.1", "1", Second, "2022-01-30 23:59:59.100000", true},
		{"2022-01-31 00:00:00.1", "100000", Month, "0001-01-01 00:00:00.000000", false},
	}
	for _, test := range subDateTimeTbl {
		ret, rettype, _ := NormalizeInterval(test.InputIntervalNum, test.InputIntervalTypes)
		d, err := ParseDatetime(test.Input, 6)
		require.Equal(t, err, nil)
		d, b := d.AddInterval(-ret, rettype, DateType)
		require.Equal(t, d.String2(6), test.expect)
		require.Equal(t, b, test.success)
	}
}

func TestParseDatetime(t *testing.T) {
	tests := []struct {
		name    string
		args    string
		want    string
		wantErr bool
	}{
		// 1. yyyy-mm-dd hh:mm:ss(.msec)
		{
			name: "yyyy-mm-dd hh:mm:ss",
			args: "1987-08-25 00:00:00.000000",
			want: "1987-08-25 00:00:00.000000",
		},
		{
			name: "yyyy-mm-dd hh:mm:ss",
			args: "1997-12-31 23:59:59",
			want: "1997-12-31 23:59:59.000000",
		},
		{
			name: "yyyy-mm-dd hh:mm:ss.sec",
			args: "1987-08-25 00:00:00.1",
			want: "1987-08-25 00:00:00.100000",
		},
		// 2. yyyymmddhhmmss(.msec)
		{
			name: "yyyymmddhhmmss",
			args: "19870825000000",
			want: "1987-08-25 00:00:00.000000",
		},
		{
			name: "yyyymmddhhmmss.sec",
			args: "19870825000000.5",
			want: "1987-08-25 00:00:00.500000",
		},
		// 3. out of range
		{
			name:    "out of range 1",
			args:    "1987-13-12 00:00:00",
			wantErr: true,
		},
		{
			name:    "out of range 2",
			args:    "1987-11-31 00:00:00",
			wantErr: true,
		},
		{
			name:    "out of range 3",
			args:    "1987-08-25 24:00:00",
			wantErr: true,
		},
		{
			name:    "out of range 4",
			args:    "1987-13-12 23:60:00",
			wantErr: true,
		},
		{
			name:    "out of range 5",
			args:    "1987-13-12 23:59:60",
			wantErr: true,
		},
		// 4. new format
		{
			name: "new format",
			args: "1987-2-12 11:20:03",
			want: "1987-02-12 11:20:03.000000",
		},
		{
			name: "new format",
			args: "1987-02-2 11:20:03",
			want: "1987-02-02 11:20:03.000000",
		},
		{
			name: "new format",
			args: "1987-12-12 1:20:03",
			want: "1987-12-12 01:20:03.000000",
		},
		{
			name: "new format",
			args: "1987-12-12 11:2:03",
			want: "1987-12-12 11:02:03.000000",
		},
		{
			name: "new format",
			args: "1987-12-12 11:02:3",
			want: "1987-12-12 11:02:03.000000",
		},
		{
			name: "bug",
			args: "1987-12-12 00:00:00.0000006",
			want: "1987-12-12 00:00:00.000001",
		},
		{
			name:    "wrong format",
			args:    "2022-01-02 00:00.00.000050",
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDatetime(tt.args, 6)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDatetime() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil && tt.wantErr {
				return
			}
			if got.String2(6) != tt.want {
				t.Errorf("ParseDatetime() got = %v, want %v", got.String2(6), tt.want)
			}
		})
	}
}

func TestUnix(t *testing.T) {
	for _, timestr := range []string{"1955-08-25 09:21:34", "2012-01-25 09:21:34"} {
		motime, _ := ParseDatetime(timestr, 6)
		motimeUnix := motime.UnixTimestamp(time.UTC)
		goLcoalTime, _ := time.ParseInLocation("2006-01-02 15:04:05", timestr, time.UTC)
		goUnix := goLcoalTime.Unix()

		require.Equal(t, motimeUnix, goUnix)

		parse_time := DatetimeFromUnix(time.UTC, motimeUnix)
		require.Equal(t, motime, parse_time)
	}
}

func TestDatetime_DayOfYear(t *testing.T) {
	cases := []struct {
		name    string
		datestr string
	}{
		{
			name:    "Test01",
			datestr: "1955-08-25 09:21:34",
		},
		{
			name:    "Test02",
			datestr: "2012-01-25 09:21:34",
		},
		{
			name:    "Test03",
			datestr: "1987-08-25 00:00:00",
		},
		{
			name:    "Test04",
			datestr: "2022-01-31 00:00:00",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			datetime, err := ParseDatetime(c.datestr, 6)
			if err != nil {
				t.Fatalf("parse datatime err %+v", err)
			}
			t.Logf("date string:%+v \n", c.datestr)
			year := datetime.DayOfYear()
			t.Logf("day of year:%+v \n", year)

			year2, week := datetime.WeekOfYear()
			t.Logf("weekofYear, year:%+v, week:%+v \n", year2, week)

			dayOfWeek := datetime.DayOfWeek()
			t.Logf("dayOfWeek:%+v \n", dayOfWeek)
		})
	}
}
