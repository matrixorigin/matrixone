// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeWindowInterval(t *testing.T) {
	for _, tc := range []struct {
		text    string
		unit    IntervalType
		numeric bool
		value   int64
		outUnit IntervalType
		null    bool
	}{
		{"0 00:00:01.5", Day_Second, false, 0, Day_Second, true},
		{"00:00:01.5", Hour_Second, false, 0, Hour_Second, true},
		{"00:01.5", Minute_Second, false, 0, Minute_Second, true},
		{"1/2/3", Minute_Second, false, 0, Minute_Second, true},
		{"1.5", Minute_Second, false, 65, Second, false},
		{"0 00:00:01", Day_Second, false, 1, Second, false},
		{"0:01", Minute_Second, false, 1, Second, false},
		{"1-2", Year_Month, false, 14, Month, false},
		{"1.000001", Second_MicroSecond, false, 1000001, MicroSecond, false},
		{"abc", Day, false, 0, Day, false},
		{"", Day, false, 0, Day, false},
		{"1abc2", Day, false, 1, Day, false},
		{"  +1.5junk", Second, false, 1500000, MicroSecond, false},
		{"0.000001", Second, true, 1, MicroSecond, false},
		{"0.0000009", Second, true, 0, MicroSecond, false},
		{"1e2", Second, true, 100000000, MicroSecond, false},
		{"1e-1000000000000000", Second, true, 0, MicroSecond, false},
		{"9223372036854.775807", Second, true, math.MaxInt64, MicroSecond, false},
		{"9007199254740993", MicroSecond, true, 9007199254740993, MicroSecond, false},
		{"1.5", Day, true, 2, Day, false},
		{"1.5", Day, false, 1, Day, false},
	} {
		t.Run(tc.text+"/"+tc.unit.String()+"/"+map[bool]string{true: "numeric", false: "text"}[tc.numeric], func(t *testing.T) {
			value, unit, null, err := NormalizeWindowInterval(tc.text, tc.unit, tc.numeric)
			require.NoError(t, err)
			require.Equal(t, tc.value, value)
			require.Equal(t, tc.outUnit, unit)
			require.Equal(t, tc.null, null)
		})
	}
	for _, tc := range []struct {
		text string
		unit IntervalType
	}{
		{"-0.0000001", Second}, {"-0:01", Minute_Second},
		{"9223372036854.775808", Second}, {"1e10000000000000000", Second},
		{"9223372036854775808", Day}, {"9223372036854775807:59", Minute_Second},
		{"NaN", Second}, {"+Inf", Second},
		{"NaN", Day_Second}, {"+Inf", Minute_Second},
	} {
		t.Run("invalid/"+tc.text, func(t *testing.T) {
			_, _, _, err := NormalizeWindowInterval(tc.text, tc.unit, true)
			require.Error(t, err)
		})
	}
}
