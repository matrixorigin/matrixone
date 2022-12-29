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

package day

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestDateToDay(t *testing.T) {
	cases := []struct {
		name      string
		inputDate []types.Date
		expected  []uint8
	}{
		{
			name: "Date to day test",
			inputDate: []types.Date{
				types.DateFromCalendar(2022, 1, 1),
				types.DateFromCalendar(2022, 2, 28),
				types.DateFromCalendar(2022, 3, 3),
				types.DateFromCalendar(2022, 4, 11),
				types.DateFromCalendar(2022, 6, 18),
				types.DateFromCalendar(2022, 8, 22),
				types.DateFromCalendar(2022, 9, 25),
				types.DateFromCalendar(2022, 12, 31),
			},
			expected: []uint8{1, 28, 03, 11, 18, 22, 25, 31},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result := make([]uint8, len(c.inputDate))
			require.Equal(t, c.expected, DateToDay(c.inputDate, result))
		})
	}
}

func TestDatetimeToDay(t *testing.T) {
	cases := []struct {
		name          string
		inputDatetime []types.Datetime
		expected      []uint8
	}{
		{
			name: "Datetime to day test",
			inputDatetime: []types.Datetime{
				types.DatetimeFromClock(2022, 1, 1, 0, 0, 0, 0),
				types.DatetimeFromClock(2022, 2, 28, 0, 0, 0, 0),
				types.DatetimeFromClock(2022, 3, 3, 0, 0, 0, 0),
				types.DatetimeFromClock(2022, 4, 11, 0, 0, 0, 0),
				types.DatetimeFromClock(2022, 6, 18, 0, 0, 0, 0),
				types.DatetimeFromClock(2022, 8, 22, 0, 0, 0, 0),
				types.DatetimeFromClock(2022, 9, 25, 0, 0, 0, 0),
				types.DatetimeFromClock(2022, 12, 31, 0, 0, 0, 0),
			},
			expected: []uint8{1, 28, 03, 11, 18, 22, 25, 31},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result := make([]uint8, len(c.inputDatetime))
			require.Equal(t, c.expected, DatetimeToDay(c.inputDatetime, result))
		})
	}
}
