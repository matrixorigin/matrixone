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
	if len(terms) == 0 || k <= 0 || s.N == 0 {
		return nil
	}

	// Resolve every query term; a missing term means the phrase cannot occur.
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

	// Candidate docs are those in the rarest term's list; probe the others for
	// membership + positional adjacency. (The rarest term bounds the work.)
	type hit struct {
		ord int64
		tf  int // phrase occurrences in the doc
	}
	var hits []hit
	posLists := make([][]int32, len(terms))
	for _, ord := range pls[rare].docIDs {
		ok := true
		for j, pl := range pls {
			pos, has := positionsInDoc(pl, ord)
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
			hits = append(hits, hit{ord, tf})
		}
	}
	if len(hits) == 0 {
		return nil
	}

	// idf uses the PHRASE document frequency (number of docs the phrase occurs
	// in), squared per MatrixOne. df <= N, so idf >= 0.
	df := float64(len(hits))
	idf := math.Log10(float64(s.N) / df)
	idf2 := idf * idf

	avgDocLen := s.AvgDocLen
	if avgDocLen == 0 {
		avgDocLen = meanDocLen(s.docLen)
	}

	results := make([]Result, len(hits))
	for i, h := range hits {
		tf := float64(h.tf)
		var score float64
		switch algo {
		case BM25:
			score = idf2 * bm25Factor(tf, s.docLen[h.ord], avgDocLen)
		default: // TfIdf
			score = tf * idf2
		}
		results[i] = Result{Pk: s.pks[h.ord], Score: score}
	}

	// Order by score desc, ties by ascending ord (hits are already in ord order,
	// so a stable sort on score desc keeps the ord tiebreak).
	sort.SliceStable(results, func(a, b int) bool { return results[a].Score > results[b].Score })
	if len(results) > k {
		results = results[:k]
	}
	return results
}

// positionsInDoc returns the ascending token positions of the term (whose posting
// list is pl) within the document ord, or ok=false if the term does not occur in
// that doc. Binary search over the ascending docIDs.
func positionsInDoc(pl *termPostings, ord int64) (pos []int32, ok bool) {
	i := sort.Search(len(pl.docIDs), func(i int) bool { return pl.docIDs[i] >= ord })
	if i < len(pl.docIDs) && pl.docIDs[i] == ord {
		return pl.positions[i], true
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
