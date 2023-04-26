// Copyright 2023 Matrix Origin
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

package unary

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// 24-hour seconds
const SecondsIn24Hours = 86400

// The number of days in the year 0000 AD
const ADZeroDays = 366

const (
	intervalYEAR        = "YEAR"
	intervalQUARTER     = "QUARTER"
	intervalMONTH       = "MONTH"
	intervalWEEK        = "WEEK"
	intervalDAY         = "DAY"
	intervalHOUR        = "HOUR"
	intervalMINUTE      = "MINUTE"
	intervalSECOND      = "SECOND"
	intervalMICROSECOND = "MICROSECOND"
)

// ToDays: InMySQL: Given a date date, returns a day number (the number of days since year 0). Returns NULL if date is NULL.
// note:  but Matrxone think the date of the first year of the year is 0001-01-01, this function selects compatibility with MySQL
// reference linking: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_to-days
func ToDays(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	dateVector := vectors[0]
	rtyp := types.T_int64.ToType()

	if dateVector.IsConstNull() {
		rvec := vector.NewConstNull(rtyp, dateVector.Length(), proc.Mp())
		return rvec, nil
	}

	if dateVector.IsConst() {
		// XXX Null handling maybe broken.
		datetimes := vector.MustFixedCol[types.Datetime](dateVector)
		resCol, err := CalcToDays(proc.Ctx, datetimes, dateVector.GetNulls())
		if err != nil {
			return nil, err
		}
		return vector.NewConstFixed(rtyp, resCol[0], dateVector.Length(), proc.Mp()), nil
	} else {
		datetimes := vector.MustFixedCol[types.Datetime](dateVector)
		resCol, err := CalcToDays(proc.Ctx, datetimes, dateVector.GetNulls())
		if err != nil {
			return nil, err
		}
		rvec := vector.NewVec(rtyp)
		nulls.Set(rvec.GetNulls(), dateVector.GetNulls())
		vector.AppendFixedList(rvec, resCol, nil, proc.Mp())
		return rvec, nil
	}
}

// CalcToDays: CalcToDays is used to return a day number (the number of days since year 0)
func CalcToDays(ctx context.Context, datetimes []types.Datetime, ns *nulls.Nulls) ([]int64, error) {
	res := make([]int64, len(datetimes))
	for idx, datetime := range datetimes {
		if nulls.Contains(ns, uint64(idx)) {
			continue
		}
		res[idx] = DateTimeDiff(intervalDAY, types.ZeroDatetime, datetime) + ADZeroDays
	}
	return res, nil
}

// DateTimeDiff returns t2 - t1 where t1 and t2 are datetime expressions.
// The unit for the result (an integer) is given by the unit argument.
// The legal values for unit are "YEAR" "QUARTER" "MONTH" "DAY" "HOUR" "SECOND" and so on.
func DateTimeDiff(intervalType string, t1, t2 types.Datetime) int64 {
	seconds, microseconds, negative := calcDateTimeInterval(t2, t1, 1)
	months := uint(0)
	if intervalType == intervalYEAR || intervalType == intervalQUARTER ||
		intervalType == intervalMONTH {
		var (
			yearBegin, yearEnd, monthBegin, monthEnd, dayBegin, dayEnd uint
			secondBegin, secondEnd, microsecondBegin, microsecondEnd   uint
		)

		if negative {
			yearBegin = uint(t2.Year())
			yearEnd = uint(t1.Year())
			monthBegin = uint(t2.Month())
			monthEnd = uint(t1.Month())
			dayBegin = uint(t2.Day())
			dayEnd = uint(t1.Day())
			secondBegin = uint(int(t2.Hour())*3600 + int(t2.Minute())*60 + int(t2.Sec()))
			secondEnd = uint(int(t1.Hour())*3600 + int(t1.Minute())*60 + int(t1.Sec()))
			microsecondBegin = uint(t2.MicroSec())
			microsecondEnd = uint(t1.MicroSec())
		} else {
			yearBegin = uint(t1.Year())
			yearEnd = uint(t2.Year())
			monthBegin = uint(t1.Month())
			monthEnd = uint(t2.Month())
			dayBegin = uint(t1.Day())
			dayEnd = uint(t2.Day())
			secondBegin = uint(int(t1.Hour())*3600 + int(t1.Minute())*60 + int(t1.Sec()))
			secondEnd = uint(int(t2.Hour())*3600 + int(t2.Minute())*60 + int(t2.Sec()))
			microsecondBegin = uint(t1.MicroSec())
			microsecondEnd = uint(t2.MicroSec())
		}

		// calculate years
		years := yearEnd - yearBegin
		if monthEnd < monthBegin ||
			(monthEnd == monthBegin && dayEnd < dayBegin) {
			years--
		}

		// calculate months
		months = 12 * years
		if monthEnd < monthBegin ||
			(monthEnd == monthBegin && dayEnd < dayBegin) {
			months += 12 - (monthBegin - monthEnd)
		} else {
			months += monthEnd - monthBegin
		}

		if dayEnd < dayBegin {
			months--
		} else if (dayEnd == dayBegin) &&
			((secondEnd < secondBegin) ||
				(secondEnd == secondBegin && microsecondEnd < microsecondBegin)) {
			months--
		}
	}

	// negative
	negV := int64(1)
	if negative {
		negV = -1
	}
	switch intervalType {
	case intervalYEAR:
		return int64(months) / 12 * negV
	case intervalQUARTER:
		return int64(months) / 3 * negV
	case intervalMONTH:
		return int64(months) * negV
	case intervalWEEK:
		return int64(seconds) / SecondsIn24Hours / 7 * negV
	case intervalDAY:
		return int64(seconds) / SecondsIn24Hours * negV
	case intervalHOUR:
		return int64(seconds) / 3600 * negV
	case intervalMINUTE:
		return int64(seconds) / 60 * negV
	case intervalSECOND:
		return int64(seconds) * negV
	case intervalMICROSECOND:
		return int64(seconds*1000000+microseconds) * negV
	}
	return 0
}

// calcDateTimeInterval: calculates time interval between two datetime values
// sign can be +1 or -1,
func calcDateTimeInterval(t1, t2 types.Datetime, sign int) (seconds, microseconds int, neg bool) {
	// Obtain the year, month, day, hour, minute, and second of the t2 datetime
	year := int(t2.Year())
	month := int(t2.Month())
	day := int(t2.Day())
	hour := int(t2.Hour())
	minute := int(t2.Minute())
	second := int(t2.Sec())
	microsecond := int(t2.MicroSec())

	days1 := calcDaysSinceZero(int(t1.Year()), int(t1.Month()), int(t1.Day()))
	days2 := calcDaysSinceZero(year, month, day)
	days1 -= sign * days2

	tmp := (int64(days1)*SecondsIn24Hours+
		int64(t1.Hour())*3600+int64(t1.Minute())*60+
		int64(t1.Sec())-
		int64(sign)*(int64(hour)*3600+
			int64(minute)*60+
			int64(second)))*1e6 +
		t1.MicroSec() - int64(sign)*int64(microsecond)

	if tmp < 0 {
		tmp = -tmp
		neg = true
	}
	seconds = int(tmp / 1e6)
	microseconds = int(tmp % 1e6)
	return
}

// calcDaynr calculates days since 0000-00-00.
func calcDaysSinceZero(year, month, day int) int {
	if year == 0 && month == 0 {
		return 0
	}

	delsum := 365*year + 31*(month-1) + day
	if month <= 2 {
		year--
	} else {
		delsum -= (month*4 + 23) / 10
	}
	temp := ((year/100 + 1) * 3) / 4
	return delsum + year/4 - temp
}
