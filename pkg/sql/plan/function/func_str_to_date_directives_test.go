// Copyright 2026 Matrix Origin
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

package function

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestStrToDateDayAndWeekDirectives(t *testing.T) {
	for _, tc := range []struct {
		name, input, format, want string
		valid                     bool
	}{
		{"ordinal", "29th February 2024", "%D %M %Y", "2024-02-29", true},
		{"ordinal day overridden", "32nd 2024 060", "%D %Y %j", "2024-02-29", true},
		{"day of year", "2024 060", "%Y %j", "2024-02-29", true},
		{"ordinal rollover", "2023 366", "%Y %j", "2024-01-01", true},
		{"ordinal rollover sets week year", "2023-366-01-1", "%Y-%j-%u-%w", "2024-01-01", true},
		{"zero ordinal preserves date", "2024-02-29 000", "%Y-%m-%d %j", "2024-02-29", true},
		{"year zero ordinal cannot invent a day", "0000 060", "%Y %j", "", false},
		{"year zero ordinal then strict week", "0000 060 2024 09 1", "%Y %j %x %v %w", "2024-02-26", true},
		{"absent ordinal preserves date", "2024-02-29", "%Y-%m-%d %j", "2024-02-29", true},
		{"weekday full", "2024-02-29 Thursday", "%Y-%m-%d %W", "2024-02-29", true},
		{"weekday short", "2024-02-29 Thu", "%Y-%m-%d %a", "2024-02-29", true},
		{"weekday unique short prefix", "2024-09-Mo", "%Y-%u-%a", "2024-02-26", true},
		{"weekday unique full prefix", "2024-09-Mo", "%Y-%u-%W", "2024-02-26", true},
		{"weekday ambiguous prefix", "2024-09-T", "%Y-%u-%W", "", false},
		{"weekday overlong name", "2024-09-Mondayfoo", "%Y-%u-%W", "", false},
		{"abbreviated rejects full name", "2024-09-Monday", "%Y-%u-%a", "", false},
		{"iso week year", "2024-09-1", "%x-%v-%w", "2024-02-26", true},
		{"sunday week year", "2024-09-1", "%X-%V-%w", "2024-03-04", true},
		{"monday week", "2024-09-1", "%Y-%u-%w", "2024-02-26", true},
		{"sunday week", "2024-09-1", "%Y-%U-%w", "2024-03-04", true},
		{"week zero", "2024-00-0", "%Y-%U-%w", "2023-12-31", true},
		{"week 53", "2024-53-0", "%X-%V-%w", "2025-01-05", true},
		{"invalid week", "2024-54-0", "%Y-%U-%w", "", false},
		{"strict week missing year", "2024-09-1", "%Y-%V-%w", "", false},
		{"mismatched week year", "2024-09-1", "%X-%v-%w", "", false},
		{"short strict week year is literal", "24-01-1", "%x-%v-%w", "0024-01-01", true},
		{"weekday does not validate full date", "2024-02-29 Sun", "%Y-%m-%d %a", "2024-02-29", true},
		{"ordinal overrides month and day", "2024-12-31-060", "%Y-%m-%d-%j", "2024-02-29", true},
		{"week overrides ordinal", "2024-060-09-1", "%Y-%j-%u-%w", "2024-02-26", true},
		{"midnight with ordinal", "2024 060 12 AM", "%Y %j %h %p", "2024-02-29", true},
		{"noon with week", "2024-09-1 12 PM", "%Y-%u-%w %h %p", "2024-02-26", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := NewGeneralTime()
			ok := coreStrToDate(context.Background(), got, tc.input, tc.format)
			if ok {
				ok = types.ValidDate(int32(got.year), got.month, got.day)
			}
			require.Equal(t, tc.valid, ok)
			if ok {
				require.Equal(t, tc.want, types.DateFromCalendar(int32(got.year), got.month, got.day).String())
			}
		})
	}
}
