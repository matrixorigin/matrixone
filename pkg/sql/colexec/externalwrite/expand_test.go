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

package externalwrite

import (
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExpandFilePattern_Strftime(t *testing.T) {
	ts := time.Date(2026, 6, 8, 9, 5, 7, 0, time.UTC)
	got, err := ExpandFilePattern("stage://s/dt=%Y-%m-%d/h%H%M%S.csv", ts)
	require.NoError(t, err)
	require.Equal(t, "stage://s/dt=2026-06-08/h090507.csv", got)

	// literal percent and day-of-year
	got, err = ExpandFilePattern("p%%-%j", ts)
	require.NoError(t, err)
	require.Equal(t, "p%-159", got)
}

func TestExpandFilePattern_RandomDigits(t *testing.T) {
	ts := time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC)
	got, err := ExpandFilePattern("part-%6N.csv", ts)
	require.NoError(t, err)
	re := regexp.MustCompile(`^part-[0-9]{6}\.csv$`)
	require.Regexp(t, re, got)

	// two expansions should (almost surely) differ
	a, _ := ExpandFilePattern("%12N", ts)
	b, _ := ExpandFilePattern("%12N", ts)
	require.Len(t, a, 12)
	require.NotEqual(t, a, b)
}

func TestExpandFilePattern_UUID(t *testing.T) {
	ts := time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC)
	got, err := ExpandFilePattern("part-%U.csv", ts)
	require.NoError(t, err)
	re := regexp.MustCompile(`^part-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.csv$`)
	require.Regexp(t, re, got)

	a, _ := ExpandFilePattern("%U", ts)
	b, _ := ExpandFilePattern("%U", ts)
	require.NotEqual(t, a, b)
}

func TestStrftimeDirectives(t *testing.T) {
	// Afternoon so %p == PM and %I wraps the 24h hour into 12h form.
	ts := time.Date(2026, 1, 5, 14, 3, 9, 0, time.UTC) // Monday, Jan 5 2026
	cases := map[string]string{
		"%Y": "2026",
		"%y": "26",
		"%m": "01",
		"%d": "05",
		"%H": "14",
		"%I": "02",
		"%M": "03",
		"%S": "09",
		"%p": "PM",
		"%A": "Monday",
		"%a": "Mon",
		"%B": "January",
		"%b": "Jan",
	}
	for pat, want := range cases {
		got, err := ExpandFilePattern(pat, ts)
		require.NoError(t, err, "pattern %q", pat)
		require.Equal(t, want, got, "pattern %q", pat)
	}

	// %I at midnight and noon: hour 0 -> 12 (AM), hour 12 -> 12 (PM).
	midnight := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
	got, err := ExpandFilePattern("%I%p", midnight)
	require.NoError(t, err)
	require.Equal(t, "12AM", got)

	noon := time.Date(2026, 1, 5, 12, 0, 0, 0, time.UTC)
	got, err = ExpandFilePattern("%I%p", noon)
	require.NoError(t, err)
	require.Equal(t, "12PM", got)
}

func TestExpandFilePattern_UTC(t *testing.T) {
	// Time directives render in UTC regardless of the input time's zone, so
	// every CN expands the same instant to the same path.
	utc := time.Date(2026, 6, 11, 23, 30, 0, 0, time.UTC)
	plus8 := utc.In(time.FixedZone("UTC+8", 8*3600)) // 2026-06-12 07:30 local
	a, err := ExpandFilePattern("dt=%Y-%m-%d/h%H", utc)
	require.NoError(t, err)
	b, err := ExpandFilePattern("dt=%Y-%m-%d/h%H", plus8)
	require.NoError(t, err)
	require.Equal(t, "dt=2026-06-11/h23", a)
	require.Equal(t, a, b)
}

func TestPatternHasUniqueDirective(t *testing.T) {
	require.True(t, PatternHasUniqueDirective("part-%U.csv"))
	require.True(t, PatternHasUniqueDirective("part-%6N.csv"))
	require.True(t, PatternHasUniqueDirective("%Y/%m/%d/p-%12N.jl"))

	// too little entropy to keep parallel writers apart
	require.False(t, PatternHasUniqueDirective("part-%1N.csv"))
	require.False(t, PatternHasUniqueDirective("part-%5N.csv"))

	require.False(t, PatternHasUniqueDirective("out-%Y%m%d.csv"))
	require.False(t, PatternHasUniqueDirective("plain.csv"))
	require.False(t, PatternHasUniqueDirective("p%%U.csv"))  // literal %U
	require.False(t, PatternHasUniqueDirective("p-%3X.csv")) // digits without N
	require.False(t, PatternHasUniqueDirective("trailing%"))
}

func TestExpandFilePattern_Errors(t *testing.T) {
	ts := time.Date(2026, 6, 8, 0, 0, 0, 0, time.UTC)
	cases := []string{
		"trailing%", // dangling percent
		"%Q",        // unknown directive
		"%3",        // digits not followed by N
		"%0N",       // zero count
		"%99N",      // count too large
	}
	for _, c := range cases {
		_, err := ExpandFilePattern(c, ts)
		require.Error(t, err, "pattern %q should error", c)
	}
}
