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
		{unit: "foot", want: 0.3048, valid: true},
		{unit: "nautical mile", want: 1852, valid: true},
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
