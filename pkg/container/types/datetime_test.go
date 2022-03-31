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
)

var dayInMonth []int = []int{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}

func TestDate(t *testing.T) {
	fmt.Println(FromCalendar(1215, 6, 15).Calendar(true))
	fmt.Println(FromCalendar(1776, 7, 4).Calendar(true))
	fmt.Println(FromCalendar(1989, 4, 26).Calendar(true))
	fmt.Println(FromCalendar(2019, 6, 9).Calendar(true))

	for i := 1; i <= 3000; i++ {
		for j := 1; j <= len(dayInMonth); j++ {
			for k := 1; k < days(i, j); k++ {
				tsys := time.Date(i, time.Month(j), k, 0, 0, 0, 0, time.Local)
				y, m, d := tsys.Date()
				yw, w := tsys.ISOWeek()
				wd := tsys.Weekday()

				t := FromCalendar(int32(i), uint8(j), uint8(k))
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
	dt := FromClock(2021, 8, 13, 17, 55, 34, 0)
	fmt.Println(dt.ToDate().Calendar(true))
	fmt.Println(dt.Clock())

	dt = Now()
	fmt.Println(dt.ToDate().Calendar(true))
	fmt.Println(dt.Clock())
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
			args: "1987-08-25 00:00:00",
			want: "1987-08-25 00:00:00",
		},
		{
			name: "yyyy-mm-dd hh:mm:ss.sec",
			args: "1987-08-25 00:00:00.1",
			want: "1987-08-25 00:00:00",
		},
		// 2. yyyymmddhhmmss(.msec)
		{
			name: "yyyymmddhhmmss",
			args: "19870825000000",
			want: "1987-08-25 00:00:00",
		},
		{
			name: "yyyymmddhhmmss.sec",
			args: "19870825000000.5",
			want: "1987-08-25 00:00:00",
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
		// 4. wrong format
		{
			name:    "wrong format",
			args:    "1987-12-12 11:20:3",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDatetime(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDatetime() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil && tt.wantErr {
				return
			}
			if got.String() != tt.want {
				t.Errorf("ParseDatetime() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUTC(t *testing.T) {
	args, _ := ParseDatetime("1987-08-25 00:00:00")
	utc := args.UTC()
	_, offset := time.Now().Local().Zone()
	if args.sec()-utc.sec() != localTZ {
		t.Errorf("UTC() args %v got %v and time zone UTC+%v", args, utc, offset/secsPerHour)
	}
}
