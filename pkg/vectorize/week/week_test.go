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

package week

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestDateToWeek(t *testing.T) {
	cases := []struct {
		name      string
		inputDate []types.Date
		expected  []uint8
	}{
		{
			name: "Date to week for ",
			inputDate: []types.Date{
				types.DateFromCalendar(2003, 12, 30),
				types.DateFromCalendar(2004, 1, 2),

				types.DateFromCalendar(2004, 12, 31),
				types.DateFromCalendar(2005, 1, 1),

				types.DateFromCalendar(2001, 2, 16),
				types.DateFromCalendar(2012, 6, 18),
				types.DateFromCalendar(2015, 9, 25),
				types.DateFromCalendar(2022, 12, 5),
			},
			expected: []uint8{1, 1, 53, 53, 7, 25, 39, 49},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result := make([]uint8, len(c.inputDate))
			require.Equal(t, c.expected, DateToWeek(c.inputDate, result))
		})
	}
}

func TestDatetimeToWeek(t *testing.T) {
	cases := []struct {
		name          string
		inputDatetime []types.Datetime
		expected      []uint8
	}{
		{
			name: "Datetime to day test",
			inputDatetime: []types.Datetime{
				types.DatetimeFromClock(2003, 12, 30, 0, 0, 0, 0),
				types.DatetimeFromClock(2004, 1, 2, 0, 0, 0, 0),

				types.DatetimeFromClock(2004, 12, 31, 0, 0, 0, 0),
				types.DatetimeFromClock(2005, 1, 1, 0, 0, 0, 0),

				types.DatetimeFromClock(2001, 2, 16, 0, 0, 0, 0),
				types.DatetimeFromClock(2012, 6, 18, 0, 0, 0, 0),
				types.DatetimeFromClock(2015, 9, 25, 0, 0, 0, 0),
				types.DatetimeFromClock(2022, 12, 5, 0, 0, 0, 0),
			},
			expected: []uint8{1, 1, 53, 53, 7, 25, 39, 49},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result := make([]uint8, len(c.inputDatetime))
			require.Equal(t, c.expected, DatetimeToWeek(c.inputDatetime, result))
		})
	}
}
