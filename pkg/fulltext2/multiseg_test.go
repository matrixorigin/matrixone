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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// TestTemporalPkNoCollision pins the value-keyed liveness fix: two DATETIME pks that
// differ only in microseconds render to the SAME %v/String() (which truncates to
// whole seconds), so a string-keyed liveness map would collide them and silently drop
// one live doc. normalizeKey keys by the pk VALUE, so both stay distinct.
func TestTemporalPkNoCollision(t *testing.T) {
	const sec = int64(63745056000)           // arbitrary second count
	dt1 := types.Datetime(sec*1_000_000 + 1) // ...000001 microseconds
	dt2 := types.Datetime(sec*1_000_000 + 2) // ...000002 microseconds
	require.Equal(t, fmt.Sprintf("%v", dt1), fmt.Sprintf("%v", dt2),
		"precondition: the two pks DO collide under %%v (String drops microseconds)")

	b := NewBuilder("dt", int32(types.T_datetime))
	feed(t, b, dt1, "alpha")
	feed(t, b, dt2, "alpha")
	seg, err := b.Finish()
	require.NoError(t, err)
	idx := NewIndex([]*Segment{seg}, nil)

	require.Equal(t, int64(2), idx.globalN, "both microsecond-distinct pks must be live")
	require.Len(t, idx.SearchPhrase(phr("alpha"), BM25, 10, nil), 2,
		"both docs must be returned (neither dropped by a lossy key)")
}

// docWords are 24 docs with skewed term frequencies (alpha common, beta medium,
// gamma/delta rarer) and varying lengths, so idf and the BM25 length-norm both
// matter — a boolean ranking is only correct if scored on GLOBAL corpus stats.
func docWords() [][]string {
	out := make([][]string, 24)
	for i := 0; i < 24; i++ {
		w := []string{"alpha"} // alpha in every doc
		if i%2 == 0 {
			w = append(w, "beta", "beta")
		}
		if i%3 == 0 {
			w = append(w, "gamma")
		}
		if i%4 == 0 {
			w = append(w, "delta", "delta", "delta")
		}
		if i%5 == 0 {
			w = append(w, "beta")
		}
		out[i] = w
	}
	return out
}

func buildFtSeg(t *testing.T, id string, rec int64, lo, hi int) *Segment {
	b := NewBuilder(id, int32(types.T_int64))
	words := docWords()
	for i := lo; i < hi; i++ {
		feed(t, b, int64(i), words[i]...)
	}
	s, err := b.Finish()
	require.NoError(t, err)
	s.Recency = rec
	return s
}

// TestMultiSegmentGlobalStats pins the #5 fix: a boolean query over docs SPLIT across
// a base + CDC-tail (three segments) must return the IDENTICAL ranking + scores as the
// same docs in ONE segment. The global corpus stats (N, avgDocLen, per-term df summed
// across segments) make cross-segment scores comparable; before the fix each segment
// used its LOCAL N/df, so the split index scored — and thus ranked / truncated — docs
// differently from the single-segment truth.
func TestMultiSegmentGlobalStats(t *testing.T) {
	// Reference: all 24 docs in one segment.
	ref := NewIndex([]*Segment{buildFtSeg(t, "ref", 0, 0, 24)}, nil)
	// Split: disjoint pk ranges across three segments (a base + two CDC tails), each
	// with its own (smaller) local N/df.
	split := NewIndex([]*Segment{
		buildFtSeg(t, "a", 0, 0, 10),
		buildFtSeg(t, "b", 1, 10, 18),
		buildFtSeg(t, "c", 2, 18, 24),
	}, nil)

	scoreByPk := func(rs []Result) map[int64]float64 {
		m := make(map[int64]float64, len(rs))
		for _, r := range rs {
			m[r.Pk.(int64)] = r.Score
		}
		return m
	}
	for _, q := range []string{"alpha beta gamma delta", "beta gamma", "+alpha beta", "alpha delta"} {
		// Full result (k large): the split index must return the SAME set of pks, each
		// with the SAME score as the single-segment truth — the direct proof that the
		// per-segment idf drift is gone (a dropped or rescored doc would diverge here).
		want, err := ref.SearchQuery([]byte(q), true, ParserDefault, BM25, 1000, nil)
		require.NoError(t, err)
		got, err := split.SearchQuery([]byte(q), true, ParserDefault, BM25, 1000, nil)
		require.NoError(t, err)
		ws, gsc := scoreByPk(want), scoreByPk(got)
		require.Equalf(t, len(ws), len(gsc), "q=%q: same matched-doc count", q)
		for pk, s := range ws {
			g, ok := gsc[pk]
			require.Truef(t, ok, "q=%q: pk %d present in split", q, pk)
			require.InDeltaf(t, s, g, 1e-9, "q=%q pk %d: global score matches single-segment", q, pk)
		}
		// Bounded top-k: the ranked SCORE sequence is identical (which tie member fills
		// the k-th slot is arbitrary — the documented WAND tie caveat — but the scores at
		// every rank, and the count, must match; a truncation loss would shorten got).
		for _, k := range []int{3, 5, 12} {
			w, err := ref.SearchQuery([]byte(q), true, ParserDefault, BM25, k, nil)
			require.NoError(t, err)
			g, err := split.SearchQuery([]byte(q), true, ParserDefault, BM25, k, nil)
			require.NoError(t, err)
			require.Lenf(t, g, len(w), "q=%q k=%d: same count (no truncation loss)", q, k)
			for i := range w {
				require.InDeltaf(t, w[i].Score, g[i].Score, 1e-9, "q=%q k=%d rank=%d: score", q, k, i)
			}
		}
	}
}

// TestMultiSegmentPhraseStats pins the ② fix: a boolean-mode PHRASE clause scores on
// the cross-segment (global) phrase df, so a phrase query over a base + CDC-tail split
// ranks identically to the same docs in one segment (before the fix the phrase clause
// used per-segment idf and diverged).
func TestMultiSegmentPhraseStats(t *testing.T) {
	words := func(i int) []string {
		w := []string{"filler", "text"}
		if i%3 == 0 {
			w = append(w, "quick", "brown", "fox") // contiguous "quick brown" phrase
		}
		if i%2 == 0 {
			w = append(w, "quick") // 'quick' alone — not the phrase
		}
		return w
	}
	build := func(id string, rec int64, lo, hi int) *Segment {
		b := NewBuilder(id, int32(types.T_int64))
		for i := lo; i < hi; i++ {
			feed(t, b, int64(i), words(i)...)
		}
		s, err := b.Finish()
		require.NoError(t, err)
		s.Recency = rec
		return s
	}
	ref := NewIndex([]*Segment{build("ref", 0, 0, 18)}, nil)
	split := NewIndex([]*Segment{build("a", 0, 0, 9), build("b", 1, 9, 18)}, nil)

	byPk := func(rs []Result) map[int64]float64 {
		m := make(map[int64]float64, len(rs))
		for _, r := range rs {
			m[r.Pk.(int64)] = r.Score
		}
		return m
	}
	q := `"quick brown"` // quoted => boolean-mode phrase clause
	want, err := ref.SearchQuery([]byte(q), true, ParserDefault, BM25, 100, nil)
	require.NoError(t, err)
	got, err := split.SearchQuery([]byte(q), true, ParserDefault, BM25, 100, nil)
	require.NoError(t, err)
	ws, gsc := byPk(want), byPk(got)
	require.NotEmpty(t, ws)
	require.Equal(t, len(ws), len(gsc), "same phrase-matched set")
	for pk, s := range ws {
		g, ok := gsc[pk]
		require.Truef(t, ok, "pk %d present in split", pk)
		require.InDeltaf(t, s, g, 1e-9, "pk %d: global phrase idf matches single-segment", pk)
	}
}

// TestMultiSegmentLiveness pins the liveness-inside-the-walk half of #5: when a pk is
// UPDATEd (a later, higher-Recency segment holds a fresh copy), a boolean query must
// score/return only the LIVE copy — the dead copy in the older segment must never
// enter a segment's top-k (which would let the merge under-fill by dropping it).
func TestMultiSegmentLiveness(t *testing.T) {
	base := NewBuilder("base", int32(types.T_int64))
	feed(t, base, int64(1), "alpha", "alpha") // pk1: old text = alpha
	feed(t, base, int64(2), "alpha")          // pk2: alpha (never updated)
	baseSeg, err := base.Finish()
	require.NoError(t, err)
	baseSeg.Recency = 0

	tail := NewBuilder("tail", int32(types.T_int64))
	feed(t, tail, int64(1), "beta", "beta") // pk1 UPDATEd: new text = beta
	tailSeg, err := tail.Finish()
	require.NoError(t, err)
	tailSeg.Recency = 1

	idx := NewIndex([]*Segment{baseSeg, tailSeg}, nil)

	ids := func(q string) []any {
		r, err := idx.SearchQuery([]byte(q), true, ParserDefault, BM25, 100, nil)
		require.NoError(t, err)
		return resultIDs(r)
	}
	// pk1's OLD alpha copy is dead → alpha matches only pk2; pk1's live copy is beta.
	require.ElementsMatch(t, []any{int64(2)}, ids("alpha"))
	require.ElementsMatch(t, []any{int64(1)}, ids("beta"))
}
