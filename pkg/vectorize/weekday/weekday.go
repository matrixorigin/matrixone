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

package weekday

import "github.com/matrixorigin/matrixone/pkg/container/types"

// vectorize weekday function
var (
	dateToWeekday     func([]types.Date, []uint8) []uint8
	datetimeToWeekday func([]types.Datetime, []uint8) []uint8
)

func init() {
	dateToWeekday = dateToWeekdayPure
	datetimeToWeekday = datetimeToWeekdayPure
}

// Returns the weekday index for date (0 = Monday, 1 = Tuesday, … 6 = Sunday)
func weekdayToIndex(weekday types.Weekday) uint8 {
	return uint8((weekday + 6) % 7)
}

func DateToWeekday(xs []types.Date, rs []uint8) []uint8 {
	return dateToWeekday(xs, rs)
}

func dateToWeekdayPure(xs []types.Date, rs []uint8) []uint8 {
	for i, x := range xs {
		rs[i] = weekdayToIndex(x.DayOfWeek())
	}
	return rs
}

func DatetimeToWeekday(xs []types.Datetime, rs []uint8) []uint8 {
	return datetimeToWeekday(xs, rs)
}

func datetimeToWeekdayPure(xs []types.Datetime, rs []uint8) []uint8 {
	for i, x := range xs {
		rs[i] = weekdayToIndex(x.ToDate().DayOfWeek())
	}
	return rs
}
