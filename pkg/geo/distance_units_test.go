// Copyright 2021 - 2024 Matrix Origin
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

package geo

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDistanceUnitScale(t *testing.T) {
	for _, tc := range []struct {
		unit  string
		want  float64
		valid bool
	}{
		{unit: "metre", want: 1, valid: true},
		{unit: "KILOMETRE", want: 1000, valid: true},
		{unit: "centimetre", want: 0.01, valid: true},
		{unit: "millimetre", want: 0.001, valid: true},
		{unit: "foot", want: 0.3048, valid: true},
		{unit: "US survey foot", want: 0.30480060960121924, valid: true},
		{unit: "Clarke's foot", want: 0.3047972654, valid: true},
		{unit: "fathom", want: 1.8288, valid: true},
		{unit: "nautical mile", want: 1852, valid: true},
		{unit: "German legal metre", want: 1.0000135965, valid: true},
		{unit: "US survey chain", want: 20.11684023368047, valid: true},
		{unit: "US survey link", want: 0.2011684023368047, valid: true},
		{unit: "US survey mile", want: 1609.3472186944375, valid: true},
		{unit: "Clarke's yard", want: 0.9143917962, valid: true},
		{unit: "Clarke's chain", want: 20.1166195164, valid: true},
		{unit: "Clarke's link", want: 0.201166195164, valid: true},
		{unit: "British yard (Sears 1922)", want: 0.9143984146160287, valid: true},
		{unit: "British foot (Sears 1922)", want: 0.3047994715386762, valid: true},
		{unit: "British chain (Sears 1922)", want: 20.116765121552632, valid: true},
		{unit: "British link (Sears 1922)", want: 0.2011676512155263, valid: true},
		{unit: "British yard (Benoit 1895 A)", want: 0.9143992, valid: true},
		{unit: "British foot (Benoit 1895 A)", want: 0.3047997333333333, valid: true},
		{unit: "British chain (Benoit 1895 A)", want: 20.1167824, valid: true},
		{unit: "British link (Benoit 1895 A)", want: 0.201167824, valid: true},
		{unit: "British yard (Benoit 1895 B)", want: 0.9143992042898124, valid: true},
		{unit: "British foot (Benoit 1895 B)", want: 0.30479973476327077, valid: true},
		{unit: "British chain (Benoit 1895 B)", want: 20.116782494375872, valid: true},
		{unit: "British link (Benoit 1895 B)", want: 0.2011678249437587, valid: true},
		{unit: "British foot (1865)", want: 0.30480083333333335, valid: true},
		{unit: "Indian foot", want: 0.30479951024814694, valid: true},
		{unit: "Indian foot (1937)", want: 0.30479841, valid: true},
		{unit: "Indian foot (1962)", want: 0.3047996, valid: true},
		{unit: "Indian foot (1975)", want: 0.3047995, valid: true},
		{unit: "Indian yard", want: 0.9143985307444408, valid: true},
		{unit: "Indian yard (1937)", want: 0.91439523, valid: true},
		{unit: "Indian yard (1962)", want: 0.9143988, valid: true},
		{unit: "Indian yard (1975)", want: 0.9143985, valid: true},
		{unit: "Statute mile", want: 1609.344, valid: true},
		{unit: "mètre", want: 1, valid: true},
		{unit: "MÈTRE", want: 1, valid: true},
		{unit: "føot", want: 0.3048, valid: true},
		{unit: "Gold Coast foot", want: 0.3047997101815088, valid: true},
		{unit: "British foot (1936)", want: 0.3048007491, valid: true},
		{unit: "yard", want: 0.9144, valid: true},
		{unit: "chain", want: 20.1168, valid: true},
		{unit: "link", want: 0.201168, valid: true},
		{unit: "British yard (Sears 1922 truncated)", want: 0.914398, valid: true},
		{unit: "British foot (Sears 1922 truncated)", want: 0.30479933333333337, valid: true},
		{unit: "British chain (Sears 1922 truncated)", want: 20.116756, valid: true},
		{unit: "British link (Sears 1922 truncated)", want: 0.20116756, valid: true},
		{unit: "INCH", valid: false},
		{unit: "micrometre", valid: false},
		{unit: " foot", valid: false},
		{unit: "foot ", valid: false},
		{unit: "clarkes foot", valid: false},
		{unit: "meter", valid: false},
		{unit: "km", valid: false},
		{unit: "mi", valid: false},
		{unit: "not-a-unit", valid: false},
	} {
		t.Run(tc.unit, func(t *testing.T) {
			got, ok := DistanceUnitScale(tc.unit)
			require.Equal(t, tc.valid, ok)
			if tc.valid {
				require.Equal(t, tc.want, got)
			}
		})
	}
}
