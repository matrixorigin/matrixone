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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// TestHighDFCutoff covers the ranked bag-of-words high-DF term cutoff (highDFKept, driven
// by the per-query SearchBagOfWords dfCutoff / fulltext2_search_df_cutoff).
//
// Corpus of nD docs, constant docLen L (so BM25 length-norm is equal and score is
// monotonic in tf):
//   - "common"  in EVERY doc            -> df == N, idf = log10(N/N) = 0, score-neutral.
//   - "common2" in EVERY doc            -> df == N (second always-present term).
//   - "rare"    in docs 0..19, tf = i+1 -> distinct positive scores, df = 20.
//   - "mid"     in docs 0..(N/2-1)      -> df = N/2, positive idf.
func TestHighDFCutoff(t *testing.T) {
	const (
		L  = 48
		nD = 60
	)
	build := func(capacity int64) *Index {
		b := NewBuilder("idx", int32(types.T_int64))
		for i := 0; i < nD; i++ {
			words := []string{"common", "common2"}
			if i < 20 {
				for j := 0; j <= i; j++ {
					words = append(words, "rare")
				}
			}
			if i < nD/2 {
				words = append(words, "mid")
			}
			for len(words) < L {
				words = append(words, "pad")
			}
			feed(t, b, int64(i), words...)
		}
		segs, err := b.FinishSegments(capacity)
		require.NoError(t, err)
		return NewIndex(segs, nil)
	}

	list := func(idx *Index, ratio float64, k int, q string) ([]any, []float32) {
		res, err := idx.SearchBagOfWords([]byte(q), ParserDefault, BM25, k, nil, ratio)
		require.NoError(t, err)
		ks := make([]any, len(res))
		ss := make([]float32, len(res))
		for i, r := range res {
			ks[i], ss[i] = normalizeKey(r.Pk), r.Score
		}
		return ks, ss
	}

	// Build multi-segment so df is aggregated globally across segments (the df the cutoff
	// reads), not per-segment.
	idx := build(16)
	require.Greater(t, len(idx.segments), 1, "need multiple segments so the cutoff reads the GLOBAL df")

	// --- Strict neutrality: dropping a df==N term (idf==0) cannot change a truncating top-k.
	// off("rare common") scores "common" at 0 for every doc, so its top-8 == off("rare").
	// ratio 1.0 drops "common" outright, which must land on the identical keys AND scores.
	baseK, baseS := list(idx, 0, 8, "rare common")
	require.Equal(t, []any{int64(19), int64(18), int64(17), int64(16), int64(15), int64(14), int64(13), int64(12)}, baseK)
	rareK, rareS := list(idx, 0, 8, "rare")
	require.Equal(t, rareK, baseK, "common is score-neutral at df==N, so it must not change the top-8")
	require.Equal(t, rareS, baseS)
	cutK, cutS := list(idx, 1.0, 8, "rare common")
	require.Equal(t, baseK, cutK, "ratio 1.0 drops the df==N term: top-8 keys identical")
	require.Equal(t, baseS, cutS, "ratio 1.0 drops the df==N term: scores identical")

	// --- No-drop-all guard: a clause set of ONLY always-present terms would prune to empty,
	// so highDFKept returns it unchanged. Asserted on the helper directly -- an all-df==N
	// query scores every doc 0, and the top-k of all-equal ties is unspecified, so the
	// deterministic claim is "the clauses are returned unchanged", not a stable key list.
	gsGuard := idx.newGlobalStats()
	allHigh := []clause{
		{kind: clauseTerm, terms: []string{"common"}, weight: 1},
		{kind: clauseTerm, terms: []string{"common2"}, weight: 1},
	}
	require.Equal(t, allHigh, idx.highDFKept(allHigh, gsGuard, 1.0), "every term df==N: guard keeps the clauses")
	allHighK, _ := list(idx, 1.0, 8, "common common2")
	require.NotEmpty(t, allHighK, "guard keeps the query, so it still matches")

	// --- A kept term absent from the index (df 0) cannot match, so dropping the high-df
	// terms would empty the query: the guard keeps the original clauses.
	withAbsent := []clause{
		{kind: clauseTerm, terms: []string{"common"}, weight: 1},
		{kind: clauseTerm, terms: []string{"zzqx"}, weight: 1},
	}
	require.Equal(t, withAbsent, idx.highDFKept(withAbsent, gsGuard, 1.0), "only a df-0 term survives: guard keeps the clauses")
	absentK, _ := list(idx, 1.0, 8, "common zzqx")
	require.Len(t, absentK, 8, "the high-df term still matches when the survivor cannot")

	// --- Single-term clause sets are never pruned (would empty the query).
	single := []clause{{kind: clauseTerm, terms: []string{"common"}, weight: 1}}
	require.Equal(t, single, idx.highDFKept(single, gsGuard, 1.0), "single term is never dropped")
	oneK, _ := list(idx, 1.0, 8, "common")
	require.NotEmpty(t, oneK, "single high-df term is kept (never drop the only term)")

	// --- A mid-frequency term is dropped at ratio 0.5 (df == N/2 >= 0.5*N) but kept at a
	// ratio above its frequency. Dropping "mid" makes "rare mid" equivalent to "rare".
	dropMidK, dropMidS := list(idx, 0.5, 8, "rare mid")
	require.Equal(t, rareK, dropMidK, "ratio 0.5 drops df==N/2 'mid' -> query reduces to 'rare'")
	require.Equal(t, rareS, dropMidS)
	keepMidK, _ := list(idx, 0.9, 8, "rare mid")
	offMidK, _ := list(idx, 0, 8, "rare mid")
	require.Equal(t, offMidK, keepMidK, "ratio 0.9 keeps df==N/2 'mid' -> same as off")

	// --- Disabled sentinels leave every query untouched.
	for _, r := range []float64{0, -1, 1.5} {
		k, s := list(idx, r, 8, "rare common")
		require.Equal(t, baseK, k, "ratio %v disables the cutoff", r)
		require.Equal(t, baseS, s)
	}

	// --- The cutoff never touches a non-disjunctive shape (explicit MUST): SearchBoolean
	// applies the cutoff only when disjunctiveTerms(q) holds, which a MUST query fails, so a
	// user's +term is always honored regardless of the cutoff.
	mustQ := BoolQuery{must: []clause{{kind: clauseTerm, terms: []string{"common"}, weight: 1}}}
	_, isDisj := disjunctiveTerms(mustQ)
	require.False(t, isDisj, "a MUST query is not disjunctive, so the cutoff gate skips it")
}
