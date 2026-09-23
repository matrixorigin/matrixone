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

package fulltext2

import (
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A range probe is only correct if the tuple encoding sorts by VALUE. If it did
// not, a range would silently omit qualifying documents — the one failure mode a
// prefilter may never have.
func TestNumericTermsSortByValue(t *testing.T) {
	vals := []float64{
		math.Inf(-1), -1e300, -12345.5, -1, -0.5, 0, 0.5, 1, 3.14, 42, 12345.5, 1e300, math.Inf(1),
	}
	terms := make([]string, len(vals))
	for i, v := range vals {
		terms[i] = JSONFloatTerm("n", v)
	}
	sorted := append([]string(nil), terms...)
	sort.Strings(sorted)
	require.Equal(t, terms, sorted, "encoded numeric terms must already be in value order")
}

func TestStringTermsSortByValue(t *testing.T) {
	vals := []string{"", "a", "aa", "ab", "b", "bar", "baz", "foo", "z", "\xff"}
	terms := make([]string, len(vals))
	for i, v := range vals {
		terms[i] = JSONStringTerm("k", v)
	}
	sorted := append([]string(nil), terms...)
	sort.Strings(sorted)
	require.Equal(t, terms, sorted, "encoded string terms must already be in value order")
}

// The per-tag bounds must bracket every value of that leaf type, so an open-ended
// inequality really is open-ended.
func TestTermBoundsBracketTheirType(t *testing.T) {
	nlo, nhi := JSONNumericTermBounds("n")
	for _, v := range []float64{math.Inf(-1), -1e308, -1, 0, 1, 1e308, math.Inf(1)} {
		term := JSONFloatTerm("n", v)
		require.LessOrEqual(t, nlo, term, "value %v below the low numeric bound", v)
		require.LessOrEqual(t, term, nhi, "value %v above the high numeric bound", v)
	}

	slo, shi := JSONStringTermBounds("k")
	for _, v := range []string{"", "a", "zzz", "\xff\xff", strings.Repeat("q", 500)} {
		term := JSONStringTerm("k", v)
		require.LessOrEqual(t, slo, term, "value %q below the low string bound", v)
		require.LessOrEqual(t, term, shi, "value %q above the high string bound", v)
	}
}

// The two leaf types must occupy DISJOINT stretches of the term space, or a
// numeric range would sweep up string terms (and vice versa) and the probe would
// match documents whose leaf has the wrong type.
func TestNumericAndStringRangesAreDisjoint(t *testing.T) {
	nlo, nhi := JSONNumericTermBounds("k")
	slo, shi := JSONStringTermBounds("k")
	require.True(t, nhi < slo || shi < nlo,
		"numeric [%q,%q] and string [%q,%q] term ranges overlap", nlo, nhi, slo, shi)

	// and a concrete term of one type never lands inside the other's range
	for _, v := range []float64{-1, 0, 3.14, 1e300} {
		term := JSONFloatTerm("k", v)
		require.False(t, slo <= term && term <= shi, "numeric term for %v is inside the string range", v)
	}
	for _, v := range []string{"", "0", "3.14", "zzz"} {
		term := JSONStringTerm("k", v)
		require.False(t, nlo <= term && term <= nhi, "string term for %q is inside the numeric range", v)
	}
}

// Truncation must be MONOTONE: if w < v then trunc(w) <= trunc(v). This is what
// lets a bound longer than maxTermValueBytes stay a superset instead of cutting
// qualifying rows out of the range.
func TestLongValueTruncationIsMonotone(t *testing.T) {
	base := strings.Repeat("a", maxTermValueBytes)

	// differing INSIDE the kept prefix: order must survive
	lo := JSONStringTerm("k", strings.Repeat("a", maxTermValueBytes-1)+"a"+"zzz")
	hi := JSONStringTerm("k", strings.Repeat("a", maxTermValueBytes-1)+"b"+"aaa")
	require.Less(t, lo, hi)

	// differing only PAST the cut: the terms collapse, and an inclusive bound is
	// what keeps such a row in range
	x := JSONStringTerm("k", base+"aaa")
	y := JSONStringTerm("k", base+"zzz")
	require.Equal(t, x, y)
	require.Equal(t, JSONStringTerm("k", base), x)

	// a tag is never confused with a longer tag sharing its prefix
	require.NotEqual(t, JSONStringTerm("k", "ab"), JSONStringTerm("ka", "b"))
}
