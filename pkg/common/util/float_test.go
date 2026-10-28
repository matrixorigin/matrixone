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

package util

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAppendMySQLFloat(t *testing.T) {
	for _, tc := range []struct {
		value float64
		bits  int
		want  string
	}{
		{0, 64, "0"}, {math.Copysign(0, -1), 64, "-0"},
		{1e20, 64, "1e20"}, {-1e20, 64, "-1e20"},
		{1e14, 64, "100000000000000"}, {1e15, 64, "1e15"}, {1e16, 64, "1e16"},
		{1000000000000000.2, 64, "1000000000000000.2"},
		{1e-7, 64, "0.0000001"}, {1e-8, 64, "0.00000001"},
		{1e-13, 64, "0.0000000000001"}, {1e-14, 64, "0.00000000000001"},
		{1e-15, 64, "0.000000000000001"}, {1e-16, 64, "1e-16"},
		{-1e-16, 64, "-1e-16"}, {9007199254740992, 64, "9.007199254740992e15"},
		{math.Nextafter(1e-15, 0), 64, "9.999999999999999e-16"},
		{math.Nextafter(1e15, 0), 64, "999999999999999.9"},
		{math.SmallestNonzeroFloat64, 64, "5e-324"}, {math.MaxFloat64, 64, "1.7976931348623157e308"},
		{1.23456789, 32, "1.23457"}, {1e20, 32, "1e20"}, {1e-7, 32, "0.0000001"},
		{999999.75, 32, "1000000"}, {9.999999e14, 32, "1e15"},
		{math.SmallestNonzeroFloat32, 32, "1.4013e-45"}, {math.MaxFloat32, 32, "3.40282e38"},
		{math.NaN(), 64, "NaN"}, {math.Inf(1), 64, "+Inf"}, {math.Inf(-1), 32, "-Inf"},
	} {
		t.Run(strconv.FormatFloat(tc.value, 'g', -1, 64)+"/"+strconv.Itoa(tc.bits), func(t *testing.T) {
			require.Equal(t, "prefix:"+tc.want, string(AppendMySQLFloat([]byte("prefix:"), tc.value, tc.bits)))
		})
	}
}
