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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// TestPrefilterMembership exercises the WHERE-clause membership prefilter end to end
// against a real C docfilter (the same bytes the runtime-filter pushdown ships): a
// candidate doc is admitted iff its pk passes the filter, at every admit point — the
// WAND walk (disjunctive), the full boolean evaluator (MUST), and NL phrase — and a
// nil filter is unchanged. It also pins the ord→pk encode in docFilterMembership.
func TestPrefilterMembership(t *testing.T) {
	mp := mpool.MustNewZero()
	b := NewBuilder("pf", int32(types.T_int64))
	for i := int64(0); i < 10; i++ {
		feed(t, b, i, "x", "y") // "x" and "y" occur in every doc
	}
	seg, err := b.Finish()
	require.NoError(t, err)
	idx := NewIndex([]*Segment{seg}, nil)

	// Membership over the EVEN pks {0,2,4,6,8}, built exactly as the pushdown does.
	vec := vector.NewVec(types.New(types.T_int64, 8, 0))
	for i := int64(0); i < 10; i += 2 {
		require.NoError(t, vector.AppendFixed(vec, i, false, mp))
	}
	fbytes, err := docfilter.Build(vec)
	require.NoError(t, err)
	filter, err := docfilter.New(fbytes)
	require.NoError(t, err)
	defer filter.Free()

	// docFilterMembership.Contains: even ords pass, odd rejected (pk-encode is correct).
	dfm := &docFilterMembership{seg: seg, f: filter}
	for i := int64(0); i < 10; i++ {
		require.Equalf(t, i%2 == 0, dfm.Contains(i), "ord %d", i)
	}

	even := []any{int64(0), int64(2), int64(4), int64(6), int64(8)}

	// WAND path (pure disjunction "x") with the filter → only even pks.
	rWand, err := idx.SearchQuery([]byte("x"), true, ParserDefault, BM25, 100, filter)
	require.NoError(t, err)
	require.ElementsMatch(t, even, resultIDs(rWand))

	// Full boolean evaluator (MUST "+x") with the filter → only even pks.
	rBool, err := idx.SearchQuery([]byte("+x"), true, ParserDefault, BM25, 100, filter)
	require.NoError(t, err)
	require.ElementsMatch(t, even, resultIDs(rBool))

	// NL phrase path ("x") with the filter → only even pks.
	rNL, err := idx.SearchQuery([]byte("x"), false, ParserDefault, BM25, 100, filter)
	require.NoError(t, err)
	require.ElementsMatch(t, even, resultIDs(rNL))

	// nil filter is unchanged: all 10 docs.
	rAll, err := idx.SearchQuery([]byte("x"), true, ParserDefault, BM25, 100, nil)
	require.NoError(t, err)
	require.Len(t, rAll, 10)

	// The filter also correctly bounds a pushed LIMIT to the FILTERED set: top-2 over
	// the even docs returns 2 even pks (not 2 of all 10 then post-filtered to fewer).
	rLimit, err := idx.SearchQuery([]byte("x"), true, ParserDefault, BM25, 2, filter)
	require.NoError(t, err)
	require.Len(t, rLimit, 2)
	for _, pk := range resultIDs(rLimit) {
		require.Contains(t, even, pk)
	}
}

// TestPhraseDfFilterIndependent pins re-review [0]: an NL phrase's idf df is the
// CORPUS phrase df (all live matches), not the WHERE-filtered subset, so a matched
// doc's score must NOT change when an unrelated WHERE prefilter is applied.
func TestPhraseDfFilterIndependent(t *testing.T) {
	mp := mpool.MustNewZero()
	b := NewBuilder("df", int32(types.T_int64))
	for i := int64(0); i < 4; i++ {
		feed(t, b, i, "alpha") // 4 docs contain the phrase
	}
	for i := int64(4); i < 8; i++ {
		feed(t, b, i, "other")
	}
	seg, err := b.Finish()
	require.NoError(t, err)
	idx := NewIndex([]*Segment{seg}, nil)

	scoreOf := func(rs []Result, pk int64) float64 {
		for _, r := range rs {
			if r.Pk.(int64) == pk {
				return r.Score
			}
		}
		t.Fatalf("pk %d not in results", pk)
		return 0
	}
	unfiltered := idx.SearchPhrase([]string{"alpha"}, BM25, 100, nil)
	require.Len(t, unfiltered, 4)
	s0 := scoreOf(unfiltered, 0)

	// WHERE prefilter allowing only {0,1} — df must stay 4 (corpus), so doc 0's score
	// is unchanged; before the fix df shrank to 2 and the score differed.
	vec := vector.NewVec(types.New(types.T_int64, 8, 0))
	require.NoError(t, vector.AppendFixed(vec, int64(0), false, mp))
	require.NoError(t, vector.AppendFixed(vec, int64(1), false, mp))
	fb, err := docfilter.Build(vec)
	require.NoError(t, err)
	filter, err := docfilter.New(fb)
	require.NoError(t, err)
	defer filter.Free()

	filtered := idx.SearchPhrase([]string{"alpha"}, BM25, 100, filter)
	require.Len(t, filtered, 2, "only the allowed docs are returned")
	require.InDelta(t, s0, scoreOf(filtered, 0), 1e-9,
		"doc 0's score must be filter-independent (corpus df, not filtered count)")
}
