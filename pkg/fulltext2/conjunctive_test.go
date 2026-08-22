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
		require.Equal(t, resultScoreBits(want), resultScoreBits(got),
			"query=%q algo=%d must preserve pk and float32 score bits", query, algo)

		for _, k := range []int{1, 3, 10, int(s.N) + 1} {
			routed, err := s.SearchBoolean(q, algo, k, allow, nil)
			require.NoError(t, err)
			legacy, err := s.searchBooleanFull(q, algo, k, allow, nil)
			require.NoError(t, err)
			require.Equal(t, len(legacy), len(routed), "query=%q algo=%d k=%d", query, algo, k)
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
