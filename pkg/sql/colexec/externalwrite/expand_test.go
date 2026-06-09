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
