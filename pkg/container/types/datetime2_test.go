package types

import (
	"testing"
)

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
