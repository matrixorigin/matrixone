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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestTemporalLocaleResolution(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		if name == "lc_time_names" {
			return "fr_FR", nil
		}
		return "", nil
	})

	require.Equal(t, "dimanche", localizedWeekday(proc, 0))
	require.Equal(t, "décembre", localizedMonth(proc, 12))
	require.Equal(t, "déc", localizedMonthAbbrev(proc, 12))
}

func TestTemporalLocaleFallbackAndBounds(t *testing.T) {
	require.Equal(t, "Sunday", localizedWeekday(nil, 0))
	require.Equal(t, "", localizedWeekday(nil, -1))
	require.Equal(t, "", localizedMonth(nil, 13))
}

func TestMakeDateRoundedInteger(t *testing.T) {
	cases := []struct {
		value string
		want  int64
	}{
		{value: "2024.49", want: 2024},
		{value: "2024.50", want: 2025},
		{value: "-1.50", want: -2},
	}
	for _, tc := range cases {
		got, ok := makeDateRoundedInteger(tc.value)
		require.True(t, ok, tc.value)
		require.Equal(t, tc.want, got, tc.value)
	}
}
