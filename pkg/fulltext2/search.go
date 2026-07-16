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
)

// ScoreAlgo selects the relevance formula (mirrors ft_relevancy_algorithm). Both
// use MatrixOne's squared-idf convention.
type ScoreAlgo int

const (
	TfIdf ScoreAlgo = iota // w·tf·idf²
	BM25                   // w·idf²·bm25Factor(tf, docLen, avgDocLen)
)

// fulltext2 has its OWN relevance session variable, distinct from classic
// fulltext's ft_relevancy_algorithm, and defaults to BM25 (the better ranked-
// retrieval default) rather than TF-IDF. Users opt into TF-IDF explicitly with
// SET ft2_relevancy_algorithm='TF-IDF'.
const (
	Fulltext2RelevancyAlgo       = "ft2_relevancy_algorithm"
	Fulltext2RelevancyAlgo_bm25  = "BM25"
	Fulltext2RelevancyAlgo_tfidf = "TF-IDF"
)

const (
	bm25K1 = 1.5
	bm25B  = 0.75
)

// Result is one ranked hit: the source pk and its relevance score.
type Result struct {
	Pk    any
	Score float64
}

// lookup resolves a term to its posting list on either a build-side segment (the
// terms map) or a loaded one (the FST ordinal). This lets the evaluator run
// against a freshly built segment or a deserialized one uniformly.
func (s *Segment) lookup(term string) (*termPostings, bool) {
	if s.dict != nil {
		return s.LookupLoaded(term)
	}
	p, ok := s.terms[term]
	return p, ok
}

// SearchPhrase runs an NL exact-phrase query: the terms (the tokenized query, in
// order) must occur as a CONTIGUOUS phrase in a document (MatrixOne NL semantics,
// §6). A single term degenerates to a plain term query. Returns up to k hits
// ordered by score desc (ties by ascending doc ord, for determinism).
//
// This is the correctness-first evaluator: it intersects the terms' posting
// lists and verifies positional adjacency exactly, without WAND block-max early
// termination (that optimization layers on later without changing results).
func (s *Segment) SearchPhrase(terms []string, algo ScoreAlgo, k int) []Result {
	if k <= 0 || s.N == 0 {
		return nil
	}
	hits := s.matchPhrase(terms)
	if len(hits) == 0 {
		return nil
	}

	// idf uses the PHRASE document frequency (number of docs the phrase occurs
	// in), squared per MatrixOne. df <= N, so idf >= 0.
	idf2 := idfSquared(s.N, len(hits))
	avgDocLen := s.avgDocLenOrMean()

	results := make([]Result, len(hits))
	for i, h := range hits {
		results[i] = Result{
			Pk:    s.pks[h.ord],
			Score: s.scoreTerm(algo, float64(h.tf), idf2, h.ord, avgDocLen),
		}
	}

	// Order by score desc, ties by ascending ord (hits are already in ord order,
	// so a stable sort on score desc keeps the ord tiebreak).
	sort.SliceStable(results, func(a, b int) bool { return results[a].Score > results[b].Score })
	if len(results) > k {
		results = results[:k]
	}
	return results
}

// docTf is a document ord and the term/phrase occurrence count in it.
type docTf struct {
	ord int64
	tf  int
}

// matchPhrase returns the documents that contain terms as a CONTIGUOUS phrase,
// each with the phrase occurrence count, in ascending doc ord. Returns nil if
// terms is empty or any term is absent from the segment. Shared by SearchPhrase
// and the boolean phrase clause.
func (s *Segment) matchPhrase(terms []string) []docTf {
	if len(terms) == 0 {
		return nil
	}
	pls := make([]*termPostings, len(terms))
	rare := 0
	for i, t := range terms {
		pl, ok := s.lookup(t)
		if !ok {
			return nil
		}
		pls[i] = pl
		if pl.df() < pls[rare].df() {
			rare = i
		}
	}
	// Adjacency verification needs positions — materialize each term's positions
	// ONCE (decoding the loaded-side compressed posRaw transiently; build-side
	// returns its [][]int32 directly), then index by doc position. WAND ranking
	// never materializes; only phrase verification does.
	mats := make([][][]int32, len(pls))
	for j, pl := range pls {
		mats[j] = pl.materializePositions()
	}
	var hits []docTf
	posLists := make([][]int32, len(terms))
	for _, ord := range pls[rare].docIDs {
		ok := true
		for j, pl := range pls {
			pos, has := positionsInDoc(pl, mats[j], ord)
			if !has {
				ok = false
				break
			}
			posLists[j] = pos
		}
		if !ok {
			continue
		}
		if tf := countPhrase(posLists); tf > 0 {
			hits = append(hits, docTf{ord, tf})
		}
	}
	return hits
}

// idfSquared is MatrixOne's squared inverse document frequency: (log10(N/df))².
// df is clamped to >= 1.
func idfSquared(n int64, df int) float64 {
	if df < 1 {
		df = 1
	}
	idf := math.Log10(float64(n) / float64(df))
	return idf * idf
}

// scoreTerm scores one (tf, idf², doc) contribution under the active algorithm.
func (s *Segment) scoreTerm(algo ScoreAlgo, tf, idf2 float64, ord int64, avgDocLen float64) float64 {
	if algo == BM25 {
		return idf2 * bm25Factor(tf, s.docLen[ord], avgDocLen)
	}
	return tf * idf2 // TfIdf
}

// avgDocLenOrMean returns the segment's AvgDocLen, falling back to the
// single-segment mean when it is unset (a lone segment queried directly).
func (s *Segment) avgDocLenOrMean() float64 {
	if s.AvgDocLen != 0 {
		return s.AvgDocLen
	}
	return meanDocLen(s.docLen)
}

// positionsInDoc returns the ascending token positions of the term (whose posting
// list is pl) within the document ord, or ok=false if the term does not occur in
// that doc. Binary search over the ascending docIDs.
func positionsInDoc(pl *termPostings, mat [][]int32, ord int64) (pos []int32, ok bool) {
	i := sort.Search(len(pl.docIDs), func(i int) bool { return pl.docIDs[i] >= ord })
	if i < len(pl.docIDs) && pl.docIDs[i] == ord {
		return mat[i], true
	}
	return nil, false
}

// countPhrase counts contiguous-phrase occurrences given, per query term j, its
// ascending positions in one document (posLists[j]). A phrase occurs at anchor p
// when term j is at p+j for every j. Handles a repeated query term (the same
// posting list appears at multiple j) correctly.
func countPhrase(posLists [][]int32) int {
	if len(posLists) == 1 {
		return len(posLists[0])
	}
	count := 0
	for _, anchor := range posLists[0] {
		matched := true
		for j := 1; j < len(posLists); j++ {
			if !sortedContainsInt32(posLists[j], anchor+int32(j)) {
				matched = false
				break
			}
		}
		if matched {
			count++
		}
	}
	return count
}

// sortedContainsInt32 reports whether v is in the ascending slice a.
func sortedContainsInt32(a []int32, v int32) bool {
	i := sort.Search(len(a), func(i int) bool { return a[i] >= v })
	return i < len(a) && a[i] == v
}

// bm25Factor is MatrixOne's BM25 tf component: tf·(k1+1)/(tf + k1·(1−b+b·dl/avgdl)).
func bm25Factor(tf float64, docLen int32, avgDocLen float64) float64 {
	norm := 1.0
	if avgDocLen > 0 {
		norm = 1.0 - bm25B + bm25B*float64(docLen)/avgDocLen
	}
	return tf * (bm25K1 + 1) / (tf + bm25K1*norm)
}

// meanDocLen is the single-segment average document length (Σ docLen / N). For a
// multi-segment index AvgDocLen is set at load across all segments (§4); this is
// the fallback when a lone segment is queried directly.
func meanDocLen(docLen []int32) float64 {
	if len(docLen) == 0 {
		return 0
	}
	var sum int64
	for _, d := range docLen {
		sum += int64(d)
	}
	return float64(sum) / float64(len(docLen))
}
