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
	"bytes"
	"math"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
	"github.com/stretchr/testify/require"
)

// qaCountingMembership is a test-only observation point already provided by the
// Segment search API. A WAND candidate calls Contains at the point it would be
// admitted; the exhaustive evaluator calls it for every matching candidate.
// Returning true keeps the observation semantics-preserving.
type qaCountingMembership struct {
	calls atomic.Int64
}

func (m *qaCountingMembership) Contains(int64) bool {
	m.calls.Add(1)
	return true
}

// qaDisableBlockMax keeps the term-level WAND bounds intact while making every
// block use those whole-list bounds. Comparing it with the original segment
// isolates block-max pruning from ordinary term-level cursor skipping.
func qaDisableBlockMax(seg *Segment) {
	for _, tp := range seg.terms {
		for i := range tp.blockMaxTf {
			tp.blockMaxTf[i] = tp.maxTf
		}
		for i := range tp.blockMinDocLn {
			tp.blockMinDocLn[i] = tp.minDocLen
		}
	}
}

// TestWANDRouteAndBlockMaxObservation proves both intended execution-path
// properties through the existing Membership seam: SearchBoolean reaches the
// WAND cursor path, and block-local bounds reduce admitted-candidate probes
// beyond term-level WAND alone. The exhaustive result remains the semantic
// control for every scorer.
func TestWANDRouteAndBlockMaxObservation(t *testing.T) {
	const nDocs = 2048
	docs := blockMaxCorpus(t, nDocs)
	withBlocks, err := BuildSegmentFromTokenized("qa-wand", int32(types.T_int64), docs)
	require.NoError(t, err)
	withoutBlocks, err := BuildSegmentFromTokenized("qa-wand-no-block", int32(types.T_int64), docs)
	require.NoError(t, err)
	qaDisableBlockMax(withoutBlocks)

	q, err := ParseBoolean([]byte("w00 w01"), tokenizer.NewSimpleTokenizer())
	require.NoError(t, err)
	_, eligible := disjunctiveTerms(q)
	require.True(t, eligible, "the probe query must be WAND-eligible")

	for _, algo := range []ScoreAlgo{BM25, TfIdf} {
		wandAllow := &qaCountingMembership{}
		got, err := withBlocks.SearchBoolean(q, algo, 5, wandAllow, nil)
		require.NoError(t, err)

		fullAllow := &qaCountingMembership{}
		want, err := withBlocks.searchBooleanFull(q, algo, 5, fullAllow, nil)
		require.NoError(t, err)
		requireSameRanking(t, "block-max-vs-full", got, want)

		termOnlyAllow := &qaCountingMembership{}
		termOnly, err := withoutBlocks.SearchBoolean(q, algo, 5, termOnlyAllow, nil)
		require.NoError(t, err)
		requireSameRanking(t, "block-max-vs-term-only", got, termOnly)

		wandCalls := wandAllow.calls.Load()
		fullCalls := fullAllow.calls.Load()
		termOnlyCalls := termOnlyAllow.calls.Load()
		t.Logf("algo=%v candidate probes: block-max=%d term-only=%d full=%d", algo, wandCalls, termOnlyCalls, fullCalls)
		require.Positive(t, fullCalls, "the full evaluator must probe matching candidates")
		require.Less(t, wandCalls, fullCalls,
			"SearchBoolean must use the WAND candidate path, not only return equal results")
		require.Less(t, wandCalls, termOnlyCalls,
			"original block bounds must prune beyond term-level WAND")
	}
}

type qaOracleDoc struct {
	pk    int64
	terms []string
}

type qaOracleTerm struct {
	term   string
	weight float32
}

type qaOracleResult struct {
	pk    int64
	score float32
}

// qaIndependentScores deliberately computes the two score formulas from the
// frozen tokenized corpus. It does not call fulltext2's scorer, statistics, or
// top-k helpers, so a shared BM25/TF-IDF defect cannot certify itself. Its N is
// the frozen live-doc count, df is the distinct live-doc count per term, and
// avgdl is the arithmetic mean token count across those docs. The oracle uses
// MatrixOne's documented float32 contribution boundary and allows arbitrary
// ordering only among the exact k-th score ties.
func qaIndependentScores(docs []qaOracleDoc, query []qaOracleTerm, algo ScoreAlgo) []qaOracleResult {
	termCounts := make(map[string]map[int]int)
	for di, d := range docs {
		for _, term := range d.terms {
			counts := termCounts[term]
			if counts == nil {
				counts = make(map[int]int)
				termCounts[term] = counts
			}
			counts[di]++
		}
	}
	avgDocLen := 0.0
	for _, d := range docs {
		avgDocLen += float64(len(d.terms))
	}
	if len(docs) > 0 {
		avgDocLen /= float64(len(docs))
	}

	idf := make(map[string]float64, len(query))
	for _, q := range query {
		df := len(termCounts[q.term])
		if df < 1 {
			continue
		}
		// MatrixOne's squared-idf convention, written independently here.
		x := math.Log10(float64(len(docs)) / float64(df))
		idf[q.term] = x * x
	}

	all := make([]qaOracleResult, 0, len(docs))
	for di, d := range docs {
		matched := false
		var score float32
		for _, q := range query {
			tf := termCounts[q.term][di]
			if tf == 0 {
				continue
			}
			matched = true
			idf2 := idf[q.term]
			var contribution float64
			if algo == BM25 {
				const k1 = 1.5
				const b = 0.75
				norm := 1.0
				if avgDocLen > 0 {
					norm = 1.0 - b + b*float64(len(d.terms))/avgDocLen
				}
				contribution = idf2 * (float64(tf) * (k1 + 1) / (float64(tf) + k1*norm))
			} else {
				contribution = float64(tf) * idf2
			}
			// Production scoring narrows each term contribution to float32 before
			// summing terms, so the independent oracle mirrors that boundary.
			score += q.weight * float32(contribution)
		}
		if matched {
			all = append(all, qaOracleResult{pk: d.pk, score: score})
		}
	}
	sort.SliceStable(all, func(i, j int) bool { return all[i].score > all[j].score })
	return all
}

func qaRequireIndependentTopK(t *testing.T, got []Result, all []qaOracleResult, k int, label string) {
	t.Helper()
	limit := k
	if limit > len(all) {
		limit = len(all)
	}
	require.Equalf(t, limit, len(got), "%s: result count", label)
	if limit == 0 {
		return
	}
	wantByPK := make(map[int64]float32, len(all))
	for _, r := range all {
		wantByPK[r.pk] = r.score
	}
	seen := make(map[int64]struct{}, len(got))
	expectedScores := make([]float32, 0, len(got))
	for _, r := range got {
		pk, ok := r.Pk.(int64)
		require.Truef(t, ok, "%s: result pk must be int64, got %T", label, r.Pk)
		expected, ok := wantByPK[pk]
		require.Truef(t, ok, "%s: unexpected pk %d", label, pk)
		require.InDeltaf(t, expected, r.Score, 1e-5, "%s: pk %d score", label, pk)
		expectedScores = append(expectedScores, expected)
		_, duplicate := seen[pk]
		require.Falsef(t, duplicate, "%s: duplicate pk %d", label, pk)
		seen[pk] = struct{}{}
		require.GreaterOrEqualf(t, r.Score, all[limit-1].score-1e-5,
			"%s: pk %d is below the top-k score boundary", label, pk)
	}
	for i := 1; i < len(expectedScores); i++ {
		require.GreaterOrEqualf(t, expectedScores[i-1], expectedScores[i]-1e-5,
			"%s: non-tie score order is wrong at result %d", label, i)
	}
	boundary := all[limit-1].score
	for _, r := range all {
		if r.score > boundary+1e-5 {
			_, ok := seen[r.pk]
			require.Truef(t, ok, "%s: dropped above-boundary pk %d", label, r.pk)
		}
	}
}

func qaLoadedOracleDocs() []qaOracleDoc {
	return []qaOracleDoc{
		{pk: 101, terms: []string{"alpha", "alpha", "beta"}},
		{pk: 102, terms: []string{"alpha", "gamma", "gamma", "gamma"}},
		{pk: 103, terms: []string{"beta", "beta", "delta"}},
		{pk: 104, terms: []string{"gamma", "delta", "delta", "delta"}},
		{pk: 105, terms: []string{"alpha", "beta", "gamma", "delta", "epsilon"}},
		{pk: 106, terms: []string{"epsilon", "zeta"}},
	}
}

func qaTokenizedDocs(docs []qaOracleDoc) []TokenizedDoc {
	out := make([]TokenizedDoc, len(docs))
	for i, d := range docs {
		out[i] = TokenizedDoc{Pk: d.pk, Terms: append([]string(nil), d.terms...)}
	}
	return out
}

// TestWANDIndependentScorePkOracle covers the loaded representation with an
// independent BM25/TF-IDF score keyed by PK. The weighted OR query creates
// distinct score-to-PK mappings while retaining WAND eligibility.
func TestWANDIndependentScorePkOracle(t *testing.T) {
	docs := qaLoadedOracleDocs()
	built, err := BuildSegmentFromTokenized("qa-loaded", int32(types.T_int64), qaTokenizedDocs(docs))
	require.NoError(t, err)
	blob, err := built.Serialize()
	require.NoError(t, err)
	loaded, err := Deserialize("qa-loaded", bytes.NewReader(blob))
	require.NoError(t, err)
	t.Cleanup(func() {
		if loaded.dict != nil {
			_ = loaded.dict.Close()
		}
		loaded.Free()
	})

	q, err := ParseBoolean([]byte(">alpha beta"), tokenizer.NewSimpleTokenizer())
	require.NoError(t, err)
	_, eligible := disjunctiveTerms(q)
	require.True(t, eligible, "weighted OR must remain WAND-eligible")
	query := []qaOracleTerm{{term: "alpha", weight: 1.1}, {term: "beta", weight: 1}}
	for _, algo := range []ScoreAlgo{BM25, TfIdf} {
		got, err := loaded.SearchBoolean(q, algo, 4, nil, nil)
		require.NoError(t, err)
		all := qaIndependentScores(docs, query, algo)
		qaRequireIndependentTopK(t, got, all, 4, "loaded")
	}
}

// TestWANDIndependentTieOracle makes the tie rule executable: either PK at an
// exact top-k boundary is valid, while the returned PK must still belong to
// the independently scored tied set.
func TestWANDIndependentTieOracle(t *testing.T) {
	docs := []qaOracleDoc{
		{pk: 201, terms: []string{"tie"}},
		{pk: 202, terms: []string{"tie"}},
		{pk: 203, terms: []string{"other"}},
	}
	built, err := BuildSegmentFromTokenized("qa-tie", int32(types.T_int64), qaTokenizedDocs(docs))
	require.NoError(t, err)
	blob, err := built.Serialize()
	require.NoError(t, err)
	loaded, err := Deserialize("qa-tie", bytes.NewReader(blob))
	require.NoError(t, err)
	t.Cleanup(func() {
		if loaded.dict != nil {
			_ = loaded.dict.Close()
		}
		loaded.Free()
	})

	q, err := ParseBoolean([]byte("tie"), tokenizer.NewSimpleTokenizer())
	require.NoError(t, err)
	_, eligible := disjunctiveTerms(q)
	require.True(t, eligible, "tie query must be WAND-eligible")
	query := []qaOracleTerm{{term: "tie", weight: 1}}
	for _, algo := range []ScoreAlgo{BM25, TfIdf} {
		got, err := loaded.SearchBoolean(q, algo, 1, nil, nil)
		require.NoError(t, err)
		all := qaIndependentScores(docs, query, algo)
		require.Len(t, all, 2)
		require.InDelta(t, all[0].score, all[1].score, 1e-7)
		qaRequireIndependentTopK(t, got, all, 1, "exact-tie")
	}
}

// TestWANDIndependentScorePkOracleWithTailLiveness exercises the loaded base
// plus a CDC tail containing an update, delete, and insert. The oracle is bound
// to the final live corpus (N/df/avgdl and liveness), so stale base postings
// cannot certify the result.
func TestWANDIndependentScorePkOracleWithTailLiveness(t *testing.T) {
	baseDocs := []qaOracleDoc{
		{pk: 1, terms: []string{"alpha", "alpha", "beta"}},
		{pk: 2, terms: []string{"alpha", "gamma", "gamma", "gamma"}},
		{pk: 3, terms: []string{"beta", "delta"}},
		{pk: 4, terms: []string{"gamma", "gamma", "delta"}},
		{pk: 5, terms: []string{"alpha", "beta", "gamma", "delta"}},
		{pk: 6, terms: []string{"epsilon"}},
	}
	built, err := BuildSegmentFromTokenized("qa-base", int32(types.T_int64), qaTokenizedDocs(baseDocs))
	require.NoError(t, err)
	blob, err := built.Serialize()
	require.NoError(t, err)
	base, err := Deserialize("qa-base", bytes.NewReader(blob))
	require.NoError(t, err)
	base.Recency = 0

	cdc := NewCdc(int32(types.T_int64))
	cdc.Upsert(int64(2), "beta beta gamma", nil) // replace old alpha-heavy version
	cdc.Delete(int64(3))                         // remove the old beta/delta row
	cdc.Insert(int64(7), "alpha delta delta delta", nil)
	idx := buildTailIndexLWW(t, base, cdc, 1000)
	t.Cleanup(func() {
		idx.Free()
		for _, seg := range idx.segments {
			if seg != nil && seg.dict != nil {
				_ = seg.dict.Close()
			}
		}
	})

	finalDocs := []qaOracleDoc{
		{pk: 1, terms: []string{"alpha", "alpha", "beta"}},
		{pk: 2, terms: []string{"beta", "beta", "gamma"}},
		{pk: 4, terms: []string{"gamma", "gamma", "delta"}},
		{pk: 5, terms: []string{"alpha", "beta", "gamma", "delta"}},
		{pk: 6, terms: []string{"epsilon"}},
		{pk: 7, terms: []string{"alpha", "delta", "delta", "delta"}},
	}
	q, err := ParseBoolean([]byte(">alpha beta gamma"), tokenizer.NewSimpleTokenizer())
	require.NoError(t, err)
	_, eligible := disjunctiveTerms(q)
	require.True(t, eligible, "tail oracle query must be WAND-eligible")
	query := []qaOracleTerm{
		{term: "alpha", weight: 1.1},
		{term: "beta", weight: 1},
		{term: "gamma", weight: 1},
	}
	for _, algo := range []ScoreAlgo{BM25, TfIdf} {
		got, err := idx.SearchBoolean(q, algo, 4, nil)
		require.NoError(t, err)
		all := qaIndependentScores(finalDocs, query, algo)
		qaRequireIndependentTopK(t, got, all, 4, "tail")
	}
}
