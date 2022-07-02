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
	"errors"
	"fmt"

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

var dtMock, _ = types.ParseDatetime("2006-01-02 15:04:05", 6)
var (
	secResultLength             = uint32(len(fmt.Sprintf("%02d", int(dtMock.Sec()))))
	minuteResultLength          = uint32(len(fmt.Sprintf("%02d", int(dtMock.Minute()))))
	hourResultLength            = uint32(len(fmt.Sprintf("%02d", int(dtMock.Hour()))))
	dayResultLength             = uint32(len(fmt.Sprintf("%02d", int(dtMock.ToDate().Day()))))
	weekResultLength            = uint32(len(fmt.Sprintf("%02d", int(dtMock.ToDate().WeekOfYear2()))))
	monthResultLength           = uint32(len(fmt.Sprintf("%02d", int(dtMock.ToDate().Month()))))
	quarterResultLength         = uint32(len(fmt.Sprintf("%d", int(dtMock.ToDate().Quarter()))))
	yearResultLength            = uint32(len(fmt.Sprintf("%04d", int(dtMock.ToDate().Year()))))
	secondMicrosecondLength     = uint32(len(dtMock.SecondMicrosecondStr()))
	minuteMicrosecondLength     = uint32(len(dtMock.MinuteMicrosecondStr()))
	minuteSecondResultLength    = uint32(len(dtMock.MinuteSecondStr()))
	hourMicrosecondResultLength = uint32(len(dtMock.HourMicrosecondStr()))
	hourSecondResultLength      = uint32(len(dtMock.HourSecondStr()))
	hourMinuteResultLength      = uint32(len(dtMock.HourMinuteStr()))
	dayMicrosecondResultLength  = uint32(len(dtMock.DayMicrosecondStr()))
	daySecondResultLength       = uint32(len(dtMock.DaySecondStr()))
	dayMinuteResultLength       = uint32(len(dtMock.DayMinuteStr()))
	dayHourResultLength         = uint32(len(dtMock.DayHourStr()))
	yearMonthResultLength       = uint32(len(dtMock.YearMonthStr()))
)

/*
func ExtractFromInputBytes(unit string, inputBytes *types.Bytes, inputNsp *nulls.Nulls, results []uint32) ([]uint32, *nulls.Nulls, error) {
	resultNsp := new(nulls.Nulls)
	if _, ok := validDateUnit[unit]; !ok {
		return []uint32{}, nil, errors.New("invalid unit")
	}
	for i := range inputBytes.Lengths {
		if nulls.Contains(inputNsp, uint64(i)) {
			nulls.Add(resultNsp, uint64(i))
			continue
		}
		inputValue := string(inputBytes.Get(int64(i)))
		date, err := types.ParseDate(inputValue)
		if err != nil {
			return []uint32{}, nil, errors.New("invalid date string")
		}
		results[i] = ExtractFromOneDate(unit, date)
	}
	return results, resultNsp, nil
}
*/

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
		return []uint32{}, errors.New("invalid unit")
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

func ExtractFromDatetime(unit string, datetimes []types.Datetime, results *types.Bytes) (*types.Bytes, error) {
	if _, ok := validDatetimeUnit[unit]; !ok {
		return nil, errors.New("invalid unit")
	}
	switch unit {
	case "microsecond":
		offsetAll := uint32(0)
		for i, d := range datetimes {
			value := fmt.Sprintf("%d", int(d.MicroSec()))
			l := uint32(len(value))
			results.Data = append(results.Data, []byte(value)...)
			results.Lengths[i] = l
			results.Offsets[i] = offsetAll
			offsetAll += l
		}
	case "second":
		offsetAll := uint32(0)
		l := secResultLength
		for i, d := range datetimes {
			value := fmt.Sprintf("%02d", int(d.Sec()))
			results.Data = append(results.Data, []byte(value)...)
			results.Lengths[i] = l
			results.Offsets[i] = offsetAll
			offsetAll += l
		}
	case "minute":
		offsetAll := uint32(0)
		l := minuteResultLength
		for i, d := range datetimes {
			value := fmt.Sprintf("%02d", int(d.Minute()))
			results.Data = append(results.Data, []byte(value)...)
			results.Lengths[i] = l
			results.Offsets[i] = offsetAll
			offsetAll += l
		}
	case "hour":
		offsetAll := uint32(0)
		l := hourResultLength
		for i, d := range datetimes {
			value := fmt.Sprintf("%02d", int(d.Hour()))
			results.Data = append(results.Data, []byte(value)...)
			results.Lengths[i] = l
			results.Offsets[i] = offsetAll
			offsetAll += l
		}
	case "day":
		offsetAll := uint32(0)
		l := dayResultLength
		for i, d := range datetimes {
			value := fmt.Sprintf("%02d", int(d.ToDate().Day()))
			results.Data = append(results.Data, []byte(value)...)
			results.Lengths[i] = l
			results.Offsets[i] = offsetAll
			offsetAll += l
		}
	case "week":
		offsetAll := uint32(0)
		l := weekResultLength
		for i, d := range datetimes {
			value := fmt.Sprintf("%02d", int(d.ToDate().WeekOfYear2()))
			results.Data = append(results.Data, []byte(value)...)
			results.Lengths[i] = l
			results.Offsets[i] = offsetAll
			offsetAll += l
		}
	case "month":
		offsetAll := uint32(0)
		l := monthResultLength
		for i, d := range datetimes {
			value := fmt.Sprintf("%02d", int(d.ToDate().Month()))
			results.Data = append(results.Data, []byte(value)...)
			results.Lengths[i] = l
			results.Offsets[i] = offsetAll
			offsetAll += l
		}
	case "quarter":
		offsetAll := uint32(0)
		l := quarterResultLength
		for i, d := range datetimes {
			value := fmt.Sprintf("%d", int(d.ToDate().Quarter()))
			results.Data = append(results.Data, []byte(value)...)
			results.Lengths[i] = l
			results.Offsets[i] = offsetAll
			offsetAll += l
		}
	case "year":
		offsetAll := uint32(0)
		l := yearResultLength
		for i, dt := range datetimes {
			value := fmt.Sprintf("%04d", int(dt.ToDate().Year()))
			results.Data = append(results.Data, []byte(value)...)
			results.Lengths[i] = l
			results.Offsets[i] = offsetAll
			offsetAll += l
		}
	case "second_microsecond":
		offsetAll := uint32(0)
		l := secondMicrosecondLength
		for i, dt := range datetimes {
			value := dt.SecondMicrosecondStr()
			results.Data = append(results.Data, []byte(value)...)
			results.Lengths[i] = l
			results.Offsets[i] = offsetAll
			offsetAll += l
		}
	case "minute_microsecond":
		offsetAll := uint32(0)
		l := minuteMicrosecondLength
		for i, dt := range datetimes {
			value := dt.MinuteMicrosecondStr()
			results.Data = append(results.Data, []byte(value)...)
			results.Lengths[i] = l
			results.Offsets[i] = offsetAll
			offsetAll += l
		}
	case "minute_second":
		offsetAll := uint32(0)
		l := minuteSecondResultLength
		for i, dt := range datetimes {
			value := dt.MinuteSecondStr()
			results.Data = append(results.Data, []byte(value)...)
			results.Lengths[i] = l
			results.Offsets[i] = offsetAll
			offsetAll += l
		}
	case "hour_microsecond":
		offsetAll := uint32(0)
		l := hourMicrosecondResultLength
		for i, dt := range datetimes {
			value := dt.HourMicrosecondStr()
			results.Data = append(results.Data, []byte(value)...)
			results.Lengths[i] = l
			results.Offsets[i] = offsetAll
			offsetAll += l
		}
	case "hour_second":
		offsetAll := uint32(0)
		l := hourSecondResultLength
		for i, dt := range datetimes {
			value := dt.HourSecondStr()
			results.Data = append(results.Data, []byte(value)...)
			results.Lengths[i] = l
			results.Offsets[i] = offsetAll
			offsetAll += l
		}
	case "hour_minute":
		offsetAll := uint32(0)
		l := hourMinuteResultLength
		for i, dt := range datetimes {
			value := dt.HourMinuteStr()
			results.Data = append(results.Data, []byte(value)...)
			results.Lengths[i] = l
			results.Offsets[i] = offsetAll
			offsetAll += l
		}
	case "day_microsecond":
		offsetAll := uint32(0)
		l := dayMicrosecondResultLength
		for i, dt := range datetimes {
			value := dt.DayMicrosecondStr()
			results.Data = append(results.Data, []byte(value)...)
			results.Lengths[i] = l
			results.Offsets[i] = offsetAll
			offsetAll += l
		}
	case "day_second":
		offsetAll := uint32(0)
		l := daySecondResultLength
		for i, dt := range datetimes {
			value := dt.DaySecondStr()
			results.Data = append(results.Data, []byte(value)...)
			results.Lengths[i] = l
			results.Offsets[i] = offsetAll
			offsetAll += l
		}
	case "day_minute":
		offsetAll := uint32(0)
		l := dayMinuteResultLength
		for i, dt := range datetimes {
			value := dt.DayMinuteStr()
			results.Data = append(results.Data, []byte(value)...)
			results.Lengths[i] = l
			results.Offsets[i] = offsetAll
			offsetAll += l
		}
	case "day_hour":
		offsetAll := uint32(0)
		l := dayHourResultLength
		for i, dt := range datetimes {
			value := dt.DayHourStr()
			results.Data = append(results.Data, []byte(value)...)
			results.Lengths[i] = l
			results.Offsets[i] = offsetAll
			offsetAll += l
		}
	case "year_month":
		offsetAll := uint32(0)
		l := yearMonthResultLength
		for i, d := range datetimes {
			value := d.ToDate().YearMonthStr()
			results.Data = append(results.Data, []byte(value)...)
			results.Lengths[i] = l
			results.Offsets[i] = offsetAll
			offsetAll += l
		}
	}
	return results, nil
}
