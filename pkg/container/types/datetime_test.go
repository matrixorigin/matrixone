package types

import (
	"fmt"
	"testing"
)

func TestDate(t *testing.T) {
	fmt.Println(FromCalendar(1215, 6, 15).Calendar(true))
	fmt.Println(FromCalendar(1776, 7, 4).Calendar(true))
	fmt.Println(FromCalendar(1989, 4, 26).Calendar(true))
	fmt.Println(FromCalendar(2019, 6, 9).Calendar(true))
}

func TestDatetime(t *testing.T) {
	dt := FromClock(2021, 8, 13, 17, 55, 34, 0)
	fmt.Println(dt.ToDate().Calendar(true))
	fmt.Println(dt.Clock())
}
