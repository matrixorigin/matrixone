// Copyright 2022 Matrix Origin
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

package extract

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

//func extractFromDate()

var validDateUnit = map[string]struct{}{
	"year":       {},
	"month":      {},
	"day":        {},
	"year_month": {},
	"quarter":    {},
}

var validDatetimeUnit = map[string]struct{}{
	"microsecond":        {},
	"second":             {},
	"minute":             {},
	"hour":               {},
	"day":                {},
	"week":               {},
	"month":              {},
	"quarter":            {},
	"year":               {},
	"second_microsecond": {},
	"minute_microsecond": {},
	"minute_second":      {},
	"hour_microsecond":   {},
	"hour_second":        {},
	"hour_minute":        {},
	"day_microsecond":    {},
	"day_second":         {},
	"day_minute":         {},
	"day_hour":           {},
	"year_month":         {},
}

var validTimeUnit = map[string]struct{}{
	"microsecond":        {},
	"second":             {},
	"minute":             {},
	"hour":               {},
	"second_microsecond": {},
	"minute_microsecond": {},
	"minute_second":      {},
	"hour_microsecond":   {},
	"hour_second":        {},
	"hour_minute":        {},
	"day_microsecond":    {},
	"day_second":         {},
	"day_minute":         {},
	"day_hour":           {},
}

func ExtractFromOneDate(unit string, date types.Date) uint32 {
	switch unit {
	case "day":
		return uint32(date.Day())
	case "week":
		return uint32(date.WeekOfYear2())
	case "month":
		return uint32(date.Month())
	case "quarter":
		return date.Quarter()
	case "year":
		return uint32(date.Year())
	case "year_month":
		return date.YearMonth()
	default:
		return 0
	}
}

func ExtractFromDate(unit string, dates []types.Date, results []uint32) ([]uint32, error) {
	if _, ok := validDateUnit[unit]; !ok {
		return []uint32{}, moerr.NewInternalErrorNoCtx("invalid unit")
	}
	switch unit {
	case "day":
		for i, d := range dates {
			results[i] = uint32(d.Day())
		}
	case "week":
		for i, d := range dates {
			results[i] = uint32(d.WeekOfYear2())
		}
	case "month":
		for i, d := range dates {
			results[i] = uint32(d.Month())
		}
	case "quarter":
		for i, d := range dates {
			results[i] = d.Quarter()
		}
	case "year_month":
		for i, d := range dates {
			results[i] = d.YearMonth()
		}
	case "year":
		for i, d := range dates {
			results[i] = uint32(d.Year())
		}
	}
	return results, nil
}

func ExtractFromDatetime(unit string, datetimes []types.Datetime, results []string) ([]string, error) {
	if _, ok := validDatetimeUnit[unit]; !ok {
		return nil, moerr.NewInternalErrorNoCtx("invalid unit")
	}
	switch unit {
	case "microsecond":
		for i, d := range datetimes {
			value := fmt.Sprintf("%d", int(d.MicroSec()))
			results[i] = value
		}
	case "second":
		for i, d := range datetimes {
			value := fmt.Sprintf("%02d", int(d.Sec()))
			results[i] = value
		}
	case "minute":
		for i, d := range datetimes {
			value := fmt.Sprintf("%02d", int(d.Minute()))
			results[i] = value
		}
	case "hour":
		for i, d := range datetimes {
			value := fmt.Sprintf("%02d", int(d.Hour()))
			results[i] = value
		}
	case "day":
		for i, d := range datetimes {
			value := fmt.Sprintf("%02d", int(d.ToDate().Day()))
			results[i] = value
		}
	case "week":
		for i, d := range datetimes {
			value := fmt.Sprintf("%02d", int(d.ToDate().WeekOfYear2()))
			results[i] = value
		}
	case "month":
		for i, d := range datetimes {
			value := fmt.Sprintf("%02d", int(d.ToDate().Month()))
			results[i] = value
		}
	case "quarter":
		for i, d := range datetimes {
			value := fmt.Sprintf("%d", int(d.ToDate().Quarter()))
			results[i] = value
		}
	case "year":
		for i, dt := range datetimes {
			value := fmt.Sprintf("%04d", int(dt.ToDate().Year()))
			results[i] = value
		}
	case "second_microsecond":
		for i, dt := range datetimes {
			value := dt.SecondMicrosecondStr()
			results[i] = value
		}
	case "minute_microsecond":
		for i, dt := range datetimes {
			value := dt.MinuteMicrosecondStr()
			results[i] = value
		}
	case "minute_second":
		for i, dt := range datetimes {
			value := dt.MinuteSecondStr()
			results[i] = value
		}
	case "hour_microsecond":
		for i, dt := range datetimes {
			value := dt.HourMicrosecondStr()
			results[i] = value
		}
	case "hour_second":
		for i, dt := range datetimes {
			value := dt.HourSecondStr()
			results[i] = value
		}
	case "hour_minute":
		for i, dt := range datetimes {
			value := dt.HourMinuteStr()
			results[i] = value
		}
	case "day_microsecond":
		for i, dt := range datetimes {
			value := dt.DayMicrosecondStr()
			results[i] = value
		}
	case "day_second":
		for i, dt := range datetimes {
			value := dt.DaySecondStr()
			results[i] = value
		}
	case "day_minute":
		for i, dt := range datetimes {
			value := dt.DayMinuteStr()
			results[i] = value
		}
	case "day_hour":
		for i, dt := range datetimes {
			value := dt.DayHourStr()
			results[i] = value
		}
	case "year_month":
		for i, d := range datetimes {
			value := d.ToDate().YearMonthStr()
			results[i] = value
		}
	}
	return results, nil
}

func ExtractFromTime(unit string, times []types.Time, results []string) ([]string, error) {
	if _, ok := validTimeUnit[unit]; !ok {
		return []string{}, moerr.NewInternalErrorNoCtx("invalid unit")
	}
	switch unit {
	case "microsecond":
		for i, t := range times {
			value := fmt.Sprintf("%d", int(t))
			results[i] = value
		}
	case "second":
		for i, t := range times {
			value := fmt.Sprintf("%02d", int(t.Sec()))
			results[i] = value
		}
	case "minute":
		for i, t := range times {
			value := fmt.Sprintf("%02d", int(t.Minute()))
			results[i] = value
		}
	case "hour", "day_hour":
		for i, t := range times {
			value := fmt.Sprintf("%02d", int(t.Hour()))
			results[i] = value
		}

	case "second_microsecond":
		for i, t := range times {
			microSec := fmt.Sprintf("%0*d", 6, int(t.MicroSec()))
			value := fmt.Sprintf("%2d%s", int(t.Sec()), microSec)
			results[i] = value
		}
	case "minute_microsecond":
		for i, t := range times {
			microSec := fmt.Sprintf("%0*d", 6, int(t.MicroSec()))
			value := fmt.Sprintf("%2d%2d%s", int(t.Minute()), int(t.Sec()), microSec)
			results[i] = value
		}
	case "minute_second":
		for i, t := range times {
			value := fmt.Sprintf("%2d%2d", int(t.Minute()), int(t.Sec()))
			results[i] = value
		}
	case "hour_microsecond", "day_microsecond":
		for i, t := range times {
			microSec := fmt.Sprintf("%0*d", 6, int(t.MicroSec()))
			value := fmt.Sprintf("%2d%2d%2d%s", int(t.Hour()), int(t.Minute()), int(t.Sec()), microSec)
			results[i] = value
		}
	case "hour_second", "day_second":
		for i, t := range times {
			value := fmt.Sprintf("%2d%2d%2d", int(t.Hour()), int(t.Minute()), int(t.Sec()))
			results[i] = value
		}
	case "hour_minute", "day_minute":
		for i, t := range times {
			value := fmt.Sprintf("%2d%2d", int(t.Hour()), int(t.Minute()))
			results[i] = value
		}
	}
	return results, nil
}

func ExtractFromString(unit string, times []string, results []string, scale int32) ([]string, error) {
	for i, t := range times {
		if len(t) == 0 {
			results[i] = t
		} else if value, err := types.ParseDatetime(t, scale); err == nil {
			results[i], err = extractFromDatetme(unit, value)
			if err != nil {
				return nil, err
			}
		} else if value, err := types.ParseTime(t, scale); err == nil {
			results[i], err = extractFromTime(unit, value)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, moerr.NewInternalErrorNoCtx("invalid input")
		}
	}
	return results, nil
}

func extractFromDatetme(unit string, datetime types.Datetime) (string, error) {
	result := make([]string, 1)
	result, err := ExtractFromDatetime(unit, []types.Datetime{datetime}, result)
	if err != nil {
		return "", err
	}
	return result[0], nil
}

func extractFromTime(unit string, time types.Time) (string, error) {
	result := make([]string, 1)
	result, err := ExtractFromTime(unit, []types.Time{time}, result)
	if err != nil {
		return "", err
	}
	return result[0], nil
}
