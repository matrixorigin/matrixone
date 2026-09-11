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
	"math/rand"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
	"github.com/stretchr/testify/require"
)

type ordSet map[int64]struct{}

func (m ordSet) Contains(ord int64) bool {
	_, ok := m[ord]
	return ok
}

func resultScoreBits(rs []Result) map[any]uint32 {
	out := make(map[any]uint32, len(rs))
	for _, r := range rs {
		out[r.Pk] = math.Float32bits(r.Score)
	}
	return out
}

func requireConjunctiveParity(t *testing.T, s *Segment, query string, allow Membership) {
	t.Helper()
	q, err := ParseBoolean([]byte(query), tokenizer.NewSimpleTokenizer())
	require.NoError(t, err)
	clauses, ok := conjunctiveTerms(q)
	require.True(t, ok, "query must be conjunctive: %s", query)
	for _, algo := range []ScoreAlgo{TfIdf, BM25} {
		want, err := s.searchBooleanFull(q, algo, int(s.N)+1, allow, nil)
		require.NoError(t, err)
		got := s.searchConjunctiveTerms(clauses, algo, int(s.N)+1, allow, nil)
		wantBits := resultScoreBits(want)
		gotBits := resultScoreBits(got)
		require.Len(t, wantBits, len(want), "query=%q algo=%d fallback PKs must be unique", query, algo)
		require.Len(t, gotBits, len(got), "query=%q algo=%d conjunctive PKs must be unique", query, algo)
		require.Equal(t, wantBits, gotBits,
			"query=%q algo=%d must preserve pk and float32 score bits", query, algo)

		// k<=0 is the materializing API's empty-result boundary. The no-LIMIT
		// SQL path instead uses StreamQuery/streamBoolean, covered separately.
		for _, k := range []int{0, 1, 3, 10, int(s.N) + 1} {
			routed, err := s.SearchBoolean(q, algo, k, allow, nil)
			require.NoError(t, err)
			legacy, err := s.searchBooleanFull(q, algo, k, allow, nil)
			require.NoError(t, err)
			require.Equal(t, len(legacy), len(routed), "query=%q algo=%d k=%d", query, algo, k)
			routedBits := resultScoreBits(routed)
			require.Len(t, routedBits, len(routed), "query=%q algo=%d k=%d routed PKs must be unique", query, algo, k)
			for pk, bits := range routedBits {
				wantScore, ok := wantBits[pk]
				require.True(t, ok, "query=%q algo=%d k=%d routed PK is not a full-evaluator hit: %v", query, algo, k, pk)
				require.Equal(t, wantScore, bits, "query=%q algo=%d k=%d routed PK score bits", query, algo, k)
			}
			for i := range legacy {
				require.Equal(t, math.Float32bits(legacy[i].Score), math.Float32bits(routed[i].Score),
					"query=%q algo=%d k=%d rank=%d", query, algo, k, i)
			}
		}
	}
}

func TestConjunctiveTermsRouting(t *testing.T) {
	term := clause{kind: clauseTerm, terms: []string{"alpha"}, weight: 1}
	phrase := clause{kind: clausePhrase, phrase: phr("alpha", "beta"), weight: 1}
	prefix := clause{kind: clausePrefix, terms: []string{"alp"}, weight: 1}
	group := clause{kind: clauseGroup, children: []clause{term}, weight: 1}

	_, ok := conjunctiveTerms(BoolQuery{must: []clause{term}})
	require.True(t, ok)
	_, ok = conjunctiveTerms(BoolQuery{must: []clause{term, term}})
	require.True(t, ok)
	for _, q := range []BoolQuery{
		{},
		{must: []clause{phrase}},
		{must: []clause{prefix}},
		{must: []clause{group}},
		{must: []clause{{kind: clauseTerm}}},
		{must: []clause{{kind: clauseTerm, terms: []string{"alpha", "beta"}}}},
		{must: []clause{term}, should: []clause{term}},
		{must: []clause{term}, mustNot: []clause{term}},
		{must: []clause{term}, adjust: []clause{term}},
	} {
		_, ok = conjunctiveTerms(q)
		require.False(t, ok)
	}
}

func TestConjunctiveTermsRejectsBareNgramCJKPhrases(t *testing.T) {
	for _, pattern := range []string{"+共和国", "+中华人民共", "+中华人民共和国"} {
		q, err := buildBooleanQuery(pattern, ParserNgram)
		require.NoError(t, err)
		require.Len(t, q.must, 1)
		require.Equal(t, clausePhrase, q.must[0].kind, "pattern=%q", pattern)
		_, ok := conjunctiveTerms(q)
		require.False(t, ok, "bare ngram CJK phrase must stay on the phrase/Boolean evaluator: %q", pattern)
	}
}

func TestConjunctiveRandomizedParity(t *testing.T) {
	rng := rand.New(rand.NewSource(20260806))
	terms := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta"}
	b := NewBuilder("random", int32(types.T_int64))
	for doc := 0; doc < 300; doc++ {
		pos := int32(0)
		for _, term := range terms {
			for n := rng.Intn(4); n > 0; n-- {
				require.NoError(t, b.Add(term, pos, int64(doc)))
				pos += int32(len(term) + 1)
			}
		}
	}
	build, err := b.Finish()
	require.NoError(t, err)
	blob, err := build.Serialize()
	require.NoError(t, err)
	loaded, err := Deserialize("random-loaded", bytes.NewReader(blob))
	require.NoError(t, err)
	t.Cleanup(func() { _ = loaded.dict.Close() })

	for n := 0; n < 100; n++ {
		count := 1 + rng.Intn(4)
		parts := make([]string, count)
		for i := range parts {
			term := terms[rng.Intn(len(terms))]
			if n%13 == 0 && i == count-1 {
				term = "missing"
			}
			parts[i] = "+" + term
		}
		query := strings.Join(parts, " ")
		for _, seg := range []*Segment{build, loaded} {
			requireConjunctiveParity(t, seg, query, nil)
		}
	}
}

func legacyIndexConjunction(t *testing.T, idx *Index, q BoolQuery, algo ScoreAlgo, k int) []Result {
	t.Helper()
	gs := idx.newGlobalStats()
	matched := make(map[any]Result)
	for si, seg := range idx.segments {
		allow := &livenessMembership{idx: idx, si: si}
		res, err := seg.searchBooleanFull(q, algo, k, allow, gs)
		require.NoError(t, err)
		for _, r := range res {
			matched[normalizeKey(r.Pk)] = r
		}
	}
	results := make([]Result, 0, len(matched))
	for _, r := range matched {
		results = append(results, r)
	}
	return topKResults(results, k)
}

func TestConjunctiveMultiSegmentLivenessParity(t *testing.T) {
	base := NewBuilder("base", int32(types.T_int64))
	feed(t, base, int64(1), "alpha", "beta")
	feed(t, base, int64(2), "alpha", "beta", "beta")
	feed(t, base, int64(3), "alpha", "beta")
	feed(t, base, int64(4), "alpha")
	baseSeg, err := base.Finish()
	require.NoError(t, err)
	baseSeg.Recency = 0

	tail := NewBuilder("tail", int32(types.T_int64))
	feed(t, tail, int64(1), "alpha")
	feed(t, tail, int64(5), "alpha", "beta", "beta", "beta")
	tailSeg, err := tail.Finish()
	require.NoError(t, err)
	tailSeg.Recency = 1

	idx := NewIndex([]*Segment{baseSeg, tailSeg}, map[any]int64{int64(3): 2})
	q, err := ParseBoolean([]byte("+alpha +beta"), tokenizer.NewSimpleTokenizer())
	require.NoError(t, err)
	for _, algo := range []ScoreAlgo{TfIdf, BM25} {
		want := legacyIndexConjunction(t, idx, q, algo, 100)
		got, err := idx.SearchBoolean(q, algo, 100, nil)
		require.NoError(t, err)
		require.Equal(t, resultScoreBits(want), resultScoreBits(got))
		require.ElementsMatch(t, []any{int64(2), int64(5)}, resultIDs(got))
	}
}

func TestConjunctiveParityBuildAndLoaded(t *testing.T) {
	build := syntheticCorpus(t)
	blob, err := build.Serialize()
	require.NoError(t, err)
	loaded, err := Deserialize("syn", bytes.NewReader(blob))
	require.NoError(t, err)
	t.Cleanup(func() { _ = loaded.dict.Close() })

	queries := []string{
		"+alpha",
		"+alpha +beta",
		"+delta +alpha +beta",
		"+alpha +alpha +gamma",
		"+alpha +missing",
		"+epsilon +filler",
	}
	for _, name := range []string{"build", "loaded"} {
		s := build
		if name == "loaded" {
			s = loaded
		}
		t.Run(name, func(t *testing.T) {
			for _, q := range queries {
				requireConjunctiveParity(t, s, q, nil)
			}
			a := ordSet{1: {}, 5: {}, 11: {}, 17: {}}
			b := ordSet{5: {}, 11: {}, 23: {}}
			requireConjunctiveParity(t, s, "+alpha +beta", andMembership{a: a, b: b})
		})
	}
}

// TestConjunctiveBoundaryAndEmptyParity exercises the ordered posting
// intersection across multiple posting blocks.  common/alpha are dense and
// beta is selective but still spans more than one block, while missing-term
// cases must return an empty result without changing the build/loaded score
// contract.
func TestConjunctiveBoundaryAndEmptyParity(t *testing.T) {
	const nDocs = 3*BlockSize + 5
	b := NewBuilder("conj-boundary", int32(types.T_int64))
	for i := 0; i < nDocs; i++ {
		words := []string{"common", "alpha"}
		if i%2 == 0 {
			words = append(words, "beta")
			if i == 2*BlockSize {
				words = append(words, "beta") // a tf>1 boundary witness
			}
		}
		feed(t, b, int64(i+1), words...)
	}
	build, err := b.Finish()
	require.NoError(t, err)
	blob, err := build.Serialize()
	require.NoError(t, err)
	loaded, err := Deserialize("conj-boundary-loaded", bytes.NewReader(blob))
	require.NoError(t, err)
	t.Cleanup(func() { _ = loaded.dict.Close() })
	require.Greater(t, build.terms["beta"].df(), BlockSize)
	loadedBeta, ok := loaded.lookup("beta")
	require.True(t, ok)
	require.Greater(t, loadedBeta.df(), BlockSize)

	queries := []string{
		"+alpha +beta",
		"+beta +alpha",         // reversed clauses must retain the same intersection
		"+alpha +alpha +beta",  // duplicate MUST keeps both score contributions
		"+common +alpha +beta", // dense driver plus sparse boundary intersection
		"+alpha +missing",
		"+missing +beta",
	}
	for _, variant := range []struct {
		name string
		seg  *Segment
	}{
		{name: "build", seg: build},
		{name: "loaded", seg: loaded},
	} {
		t.Run(variant.name, func(t *testing.T) {
			for _, q := range queries {
				t.Run(q, func(t *testing.T) { requireConjunctiveParity(t, variant.seg, q, nil) })
			}
		})
	}
}

// TestConjunctiveLivenessAtBlockBoundary compares the cursor path with the
// full evaluator after a base copy is superseded at a posting-block boundary.
// Both segments are serialized so the base's dirty live-DF walk must decode
// only loaded blocks while the tail remains fully live.
func TestConjunctiveLivenessAtBlockBoundary(t *testing.T) {
	const nBase = 2*BlockSize + 3
	baseBuilder := NewBuilder("conj-live-base", int32(types.T_int64))
	for i := 0; i < nBase; i++ {
		feed(t, baseBuilder, int64(i+1), "alpha", "beta")
	}
	base := loadedSeg(t, baseBuilder)
	base.Recency = 0

	// Replace one copy immediately before the first block boundary and one at
	// the second boundary; the first replacement intentionally removes beta.
	replacePk := int64(BlockSize)
	keepPk := int64(2*BlockSize + 1)
	tailBuilder := NewBuilder("conj-live-tail", int32(types.T_int64))
	feed(t, tailBuilder, replacePk, "alpha")
	feed(t, tailBuilder, keepPk, "alpha", "beta", "beta")
	feed(t, tailBuilder, int64(nBase+1), "alpha", "beta")
	tail := loadedSeg(t, tailBuilder)
	tail.Recency = 10

	idx := NewIndex([]*Segment{base, tail}, nil)
	q, err := ParseBoolean([]byte("+alpha +beta"), tokenizer.NewSimpleTokenizer())
	require.NoError(t, err)
	_, ok := conjunctiveTerms(q)
	require.True(t, ok)
	require.NotNil(t, idx.liveOrd[0], "superseded base copies need a liveness bitmap")

	for _, algo := range []ScoreAlgo{TfIdf, BM25} {
		want := legacyIndexConjunction(t, idx, q, algo, nBase+2)
		got, err := idx.SearchBoolean(q, algo, nBase+2, nil)
		require.NoError(t, err)
		require.Equal(t, resultScoreBits(want), resultScoreBits(got), "algo=%d score bits", algo)
		require.ElementsMatch(t, resultIDs(want), resultIDs(got), "algo=%d membership", algo)
		require.NotContains(t, resultIDs(got), replacePk, "superseded copy without beta must not match")
		require.Contains(t, resultIDs(got), keepPk, "newer copy at the second boundary must match")
	}
}

// TestConjunctiveRoutingControlsUseFullEvaluator guards the negative side of
// the route predicate: phrases, prefixes, groups, SHOULD, MUST-NOT, ADJUST,
// and mixed Boolean shapes keep their existing evaluator and result contract.
func TestConjunctiveRoutingControlsUseFullEvaluator(t *testing.T) {
	s := fulltextCorpus(t)
	queries := []string{
		`+quick "brown fox"`,  // MUST + phrase
		"+qui* +fox",          // MUST + prefix
		"+(quick brown) +fox", // MUST + group
		"+quick brown",        // MUST + SHOULD
		"+lazy -fox",          // MUST + MUST-NOT
		"+quick ~fox",         // MUST + ADJUST
		`"quick brown"`,       // phrase-only SHOULD
	}
	for _, pattern := range queries {
		t.Run(pattern, func(t *testing.T) {
			q, err := ParseBoolean([]byte(pattern), tokenizer.NewSimpleTokenizer())
			require.NoError(t, err)
			_, pure := conjunctiveTerms(q)
			require.False(t, pure, "control query must stay on the full evaluator")
			for _, algo := range []ScoreAlgo{TfIdf, BM25} {
				full, err := s.searchBooleanFull(q, algo, int(s.N)+1, nil, nil)
				require.NoError(t, err)
				routed, err := s.SearchBoolean(q, algo, int(s.N)+1, nil, nil)
				require.NoError(t, err)
				require.Equal(t, resultScoreBits(full), resultScoreBits(routed), "algo=%d score bits", algo)
				require.ElementsMatch(t, resultIDs(full), resultIDs(routed), "algo=%d membership", algo)
			}
		})
	}
}

func TestConjunctiveStringPrimaryKey(t *testing.T) {
	b := NewBuilder("str", int32(types.T_varchar))
	feed(t, b, "doc-a", "alpha", "beta")
	feed(t, b, "doc-b", "alpha")
	feed(t, b, "doc-c", "alpha", "beta", "beta")
	s, err := b.Finish()
	require.NoError(t, err)
	requireConjunctiveParity(t, s, "+alpha +beta", nil)
}

func TestConjunctiveIncludePrefilterAndOutput(t *testing.T) {
	idx := incIdx(t)
	preds, err := compileIncludePredicates(
		[]byte(`[{"col":0,"op":"=","val":"active"}]`), idx.includeTypes(), idx.pkType())
	require.NoError(t, err)
	filter := &prefilter{include: preds}
	q, err := ParseBoolean([]byte("+x"), tokenizer.NewSimpleTokenizer())
	require.NoError(t, err)

	got, err := idx.SearchBoolean(q, BM25, 100, filter)
	require.NoError(t, err)
	allow := andAllow(mkAllow(idx.segments[0], filter), &livenessMembership{idx: idx, si: 0})
	want, err := idx.segments[0].searchBooleanFull(q, BM25, 100, allow, idx.newGlobalStats())
	require.NoError(t, err)
	require.Equal(t, resultScoreBits(want), resultScoreBits(got))
	require.ElementsMatch(t, []any{int64(1), int64(3)}, resultIDs(got))
	for _, r := range got {
		require.Len(t, r.Include, 2)
		require.Equal(t, []byte("active"), r.Include[0])
	}
}

func BenchmarkBooleanConjunctiveLoaded(b *testing.B) {
	bb := NewBuilder("and-bench", int32(types.T_int64))
	for i := 0; i < 50000; i++ {
		words := []string{"alpha"}
		if i%2 == 0 {
			words = append(words, "beta", "beta")
		}
		if i%10 == 0 {
			words = append(words, "gamma", "gamma", "gamma")
		}
		if i%97 == 0 {
			words = append(words, "delta")
		}
		pos := int32(0)
		for _, word := range strings.Fields(strings.Join(words, " ")) {
			if err := bb.Add(word, pos, int64(i)); err != nil {
				b.Fatal(err)
			}
			pos += int32(len(word) + 1)
		}
	}
	seg, err := bb.Finish()
	if err != nil {
		b.Fatal(err)
	}
	blob, err := seg.Serialize()
	if err != nil {
		b.Fatal(err)
	}
	loaded, err := Deserialize("and-bench", bytes.NewReader(blob))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = loaded.dict.Close() })
	q, err := ParseBoolean([]byte("+alpha +beta +gamma"), tokenizer.NewSimpleTokenizer())
	if err != nil {
		b.Fatal(err)
	}
	clauses, ok := conjunctiveTerms(q)
	if !ok {
		b.Fatal("benchmark query did not classify as conjunctive")
	}

	b.Run("cursor", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = loaded.searchConjunctiveTerms(clauses, BM25, 100, nil, nil)
		}
	})
	b.Run("legacy", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := loaded.searchBooleanFull(q, BM25, 100, nil, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
}
