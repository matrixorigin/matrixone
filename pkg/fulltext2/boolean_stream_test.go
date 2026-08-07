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
	"errors"
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

// TestStreamBoolean pins the no-LIMIT boolean streaming path (Index.streamBoolean): a
// NON-disjunctive boolean query (MUST / MUST-NOT / mixed phrase) must stream EXACTLY the
// (pk, score) set the materialized SearchQuery(k=all) produces — one segment at a time,
// with no global top-K sized to globalN — must dedup a pk across the base + CDC tail
// (per-segment liveness), and must propagate an emit error. This is precisely the shape
// that used to fall back to SearchQuery(globalN) and materialize the whole live corpus
// before emitting; +alpha (matches every doc) exercises the low-selectivity case.
func TestStreamBoolean(t *testing.T) {
	split := NewIndex([]*Segment{
		buildFtSeg(t, "a", 0, 0, 12),
		buildFtSeg(t, "b", 1, 12, 24),
	}, nil)

	cases := []struct {
		pat      string
		nonEmpty bool
	}{
		{"+alpha", true},               // MUST, matches EVERY doc (low selectivity)
		{"+alpha beta", true},          // MUST + SHOULD
		{"+alpha -delta", true},        // MUST + MUST-NOT
		{"alpha -gamma", true},         // SHOULD + MUST-NOT (non-disjunctive via -)
		{"+beta gamma", true},          // MUST + SHOULD
		{`+alpha "beta gamma"`, false}, // MUST + phrase (mixed) — parity even if empty
	}

	// Guard: every case is genuinely non-disjunctive, so StreamQuery routes it to
	// streamBoolean (NOT streamWAND / streamPhrase). If a future refactor made one
	// disjunctive, this test would silently exercise the wrong path — so pin it here.
	for _, c := range cases {
		q, err := buildBooleanQuery(c.pat, normalizeParser(ParserDefault))
		require.NoErrorf(t, err, "build %q", c.pat)
		_, disj := disjunctiveTerms(q)
		require.Falsef(t, disj, "%q must be non-disjunctive (exercise streamBoolean)", c.pat)
	}

	collect := func(pattern string) map[int64]float64 {
		m := map[int64]float64{}
		err := split.StreamQuery([]byte(pattern), true, ParserDefault, BM25, nil, false,
			func(keys *vectorindex.ColumnBuffer, dists []float64, _ [][]any) error {
				require.LessOrEqual(t, keys.N, streamBatch)
				for i, k := range int64ColumnBuffer(keys) {
					_, dup := m[k]
					require.Falsef(t, dup, "pk %d emitted twice", k) // liveness dedup
					m[k] = dists[i]
				}
				return nil
			})
		require.NoError(t, err)
		return m
	}

	for _, c := range cases {
		want, err := split.SearchQuery([]byte(c.pat), true, ParserDefault, BM25, 1000, nil)
		require.NoError(t, err)
		got := collect(c.pat)
		require.Equalf(t, len(want), len(got), "%q: same match count", c.pat)
		for _, r := range want {
			g, ok := got[r.Pk.(int64)]
			require.Truef(t, ok, "%q: pk %v streamed", c.pat, r.Pk)
			require.InDeltaf(t, r.Score, g, 1e-5, "%q: pk %v score", c.pat, r.Pk)
		}
		if c.nonEmpty {
			require.NotEmptyf(t, got, "%q: expected a non-empty streamed result", c.pat)
		}
	}

	// emit-error path: the callback error propagates and stops the boolean walk.
	sentinel := errors.New("consumer aborted")
	err := split.StreamQuery([]byte("+alpha beta"), true, ParserDefault, BM25, nil, false,
		func(keys *vectorindex.ColumnBuffer, dists []float64, _ [][]any) error { return sentinel })
	require.ErrorIs(t, err, sentinel)
}

// buildPhraseSeg builds a segment [lo,hi) where EVERY doc is "common foo bar" — so the
// term "common" and the contiguous phrase "foo bar" each match every doc (low selectivity).
func buildPhraseSeg(t *testing.T, id string, rec int64, lo, hi int) *Segment {
	b := NewBuilder(id, int32(types.T_int64))
	for i := lo; i < hi; i++ {
		feed(t, b, int64(i), "common", "foo", "bar")
	}
	s, err := b.Finish()
	require.NoError(t, err)
	s.Recency = rec
	return s
}

// TestStreamBooleanMixedPhraseLowSelectivity is the bounded-memory regression for the
// no-LIMIT streaming path: a mixed boolean query with a LOW-SELECTIVITY phrase over MANY
// segments must stream correctly WITHOUT retaining every segment's phrase hits. The old
// phraseHitsCache held one []docTf per (phrase, segment) for the whole query, so the first
// segment's scoring — which triggers the cross-segment phraseDf scan — materialized O(the
// global matching corpus) before a single row could be emitted (CN OOM). The phrase here
// matches ALL 120 docs across 3 segments; the streamed result must equal the materialized
// SearchQuery and be non-empty. (Correctness of count-and-discard phraseDf: the global
// phrase idf must still be identical to the retaining version — verified via score parity.)
func TestStreamBooleanMixedPhraseLowSelectivity(t *testing.T) {
	idx := NewIndex([]*Segment{
		buildPhraseSeg(t, "a", 0, 0, 40),
		buildPhraseSeg(t, "b", 1, 40, 80),
		buildPhraseSeg(t, "c", 2, 80, 120),
	}, nil)

	const pat = `+common "foo bar"` // MUST term + phrase (mixed, non-disjunctive → streamBoolean)
	q, err := buildBooleanQuery(pat, normalizeParser(ParserDefault))
	require.NoError(t, err)
	_, disj := disjunctiveTerms(q)
	require.False(t, disj, "must route to streamBoolean")

	got := map[int64]float64{}
	err = idx.StreamQuery([]byte(pat), true, ParserDefault, BM25, nil, false,
		func(keys *vectorindex.ColumnBuffer, dists []float64, _ [][]any) error {
			require.LessOrEqual(t, keys.N, streamBatch)
			for i, k := range int64ColumnBuffer(keys) {
				_, dup := got[k]
				require.Falsef(t, dup, "pk %d emitted twice", k)
				got[k] = dists[i]
			}
			return nil
		})
	require.NoError(t, err)

	want, err := idx.SearchQuery([]byte(pat), true, ParserDefault, BM25, 1000, nil)
	require.NoError(t, err)
	require.NotEmpty(t, got)              // non-empty, low selectivity
	require.Equal(t, 120, len(got))       // the phrase matches every doc across all 3 segments
	require.Equal(t, len(want), len(got)) // parity with the materialized path
	for _, r := range want {
		g, ok := got[r.Pk.(int64)]
		require.Truef(t, ok, "pk %v streamed", r.Pk)
		require.InDeltaf(t, r.Score, g, 1e-5, "pk %v score parity (global phrase idf unchanged)", r.Pk)
	}
}

// TestGlobalStatsNoPhraseHitRetention structurally guards the fix above: global phrase DF
// must be count-and-discard, so globalStats must NOT hold a field that retains per-segment
// matchPhrase slices ([]docTf). Reintroducing a cross-segment []docTf cache (the removed
// phraseHitsCache) would defeat the no-LIMIT streaming O(one segment) bound.
func TestGlobalStatsNoPhraseHitRetention(t *testing.T) {
	typ := reflect.TypeOf(globalStats{})
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		require.NotContainsf(t, f.Type.String(), "docTf",
			"globalStats.%s (%s) retains phrase hit slices; global phrase DF must count-and-discard, not cache []docTf",
			f.Name, f.Type)
	}
}
