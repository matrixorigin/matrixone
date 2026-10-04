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
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func segmentWithTerms(terms ...string) *Segment {
	s := NewSegment("seg0", 0)
	m := make(map[string]*termPostings, len(terms))
	for i, t := range terms {
		// distinct df per term so we can assert Lookup returns the right posting
		m[t] = &termPostings{docIDs: make([]int64, i+1)}
	}
	s.setTerms(m)
	return s
}

// TestSetTermsSorted: setTerms derives an ascending key list consistent with the
// map (the invariant PrefixRange's binary search relies on).
func TestSetTermsSorted(t *testing.T) {
	s := segmentWithTerms("banana", "apple", "cherry", "apricot")
	require.Equal(t, 4, s.NumTerms())
	require.True(t, sort.StringsAreSorted(s.sortedTerms), "sortedTerms must be ascending")
	require.Equal(t, []string{"apple", "apricot", "banana", "cherry"}, s.sortedTerms)
	// every sorted key resolves in the map
	for _, term := range s.sortedTerms {
		_, ok := s.Lookup(term)
		require.True(t, ok, term)
	}
}

// TestLookup: exact hit returns the term's own posting; miss returns false.
func TestLookup(t *testing.T) {
	s := segmentWithTerms("apple", "apricot")
	p, ok := s.Lookup("apple")
	require.True(t, ok)
	require.Equal(t, 1, p.df()) // "apple" was index 0 -> df 1
	p, ok = s.Lookup("apricot")
	require.True(t, ok)
	require.Equal(t, 2, p.df())
	_, ok = s.Lookup("avocado")
	require.False(t, ok)
	_, ok = s.Lookup("")
	require.False(t, ok)
}

// TestPrefixRange: the `word*` enumeration — prefix hit, boundary, no-match, and
// empty prefix (all terms).
func TestPrefixRange(t *testing.T) {
	s := segmentWithTerms("app", "apple", "apply", "banana", "band", "bat")

	require.Equal(t, []string{"app", "apple", "apply"}, s.PrefixRange("app"))
	require.Equal(t, []string{"banana", "band"}, s.PrefixRange("ban"))
	require.Equal(t, []string{"bat"}, s.PrefixRange("bat"))
	// prefix matches a whole run
	require.Equal(t, []string{"banana", "band", "bat"}, s.PrefixRange("b"))
	// no match — before, between, and after the key range
	require.Empty(t, s.PrefixRange("aa"))
	require.Empty(t, s.PrefixRange("az"))
	require.Empty(t, s.PrefixRange("z"))
	// empty prefix returns everything, in order
	require.Equal(t, s.sortedTerms, s.PrefixRange(""))
}

func TestPrefixRangeEmptySegment(t *testing.T) {
	s := NewSegment("empty", 0)
	require.Equal(t, 0, s.NumTerms())
	require.Empty(t, s.PrefixRange("x"))
	require.Empty(t, s.PrefixRange(""))
}
