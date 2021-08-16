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
