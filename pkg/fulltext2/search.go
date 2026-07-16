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
func (s *Segment) SearchPhrase(slots []phraseSlot, algo ScoreAlgo, k int) []Result {
	if k <= 0 || s.N == 0 {
		return nil
	}
	hits := s.matchPhrase(slots)
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
// phraseSlot is one position of a positional phrase: a term (star=false) or a word*
// prefix (star=true) that must occur at byte offset `off` from the phrase's first slot.
// Byte offsets (not token indices) match classic fulltext's ngram phrase JOIN, so a
// redundant-ngram-removed CJK phrase (with gaps between kept trigrams) and a prefix tail
// both verify correctly against the doc's byte-position postings.
type phraseSlot struct {
	term string
	star bool
	off  int32
}

// matchPhrase verifies a positional phrase against the segment: for a doc to hit, some
// occurrence of slot[0] at byte position p must have every slot[j] present at p+off[j]
// (a star slot: ANY term with that prefix at p+off[j]). Returns per-doc phrase counts.
func (s *Segment) matchPhrase(slots []phraseSlot) []docTf {
	if len(slots) == 0 {
		return nil
	}
	// Per slot, a doc→sorted-byte-positions map, merged over a star slot's expansions.
	slotPos := make([]map[int64][]int32, len(slots))
	rare := 0
	for i, sl := range slots {
		var terms []string
		if sl.star {
			ts, err := s.prefixTerms(sl.term)
			if err != nil {
				return nil
			}
			terms = ts
		} else {
			terms = []string{sl.term}
		}
		m := make(map[int64][]int32)
		for _, t := range terms {
			pl, ok := s.lookup(t)
			if !ok {
				continue
			}
			docs := pl.materializeDocIDs()
			pos := pl.materializePositions()
			for di, ord := range docs {
				m[ord] = append(m[ord], pos[di]...)
			}
		}
		if len(m) == 0 {
			return nil // a slot no doc satisfies ⇒ the phrase can't occur
		}
		for ord := range m { // sortedContains needs ascending positions
			p := m[ord]
			sort.Slice(p, func(a, b int) bool { return p[a] < p[b] })
			m[ord] = p
		}
		slotPos[i] = m
		if len(m) < len(slotPos[rare]) {
			rare = i
		}
	}
	var hits []docTf
	for ord := range slotPos[rare] {
		base, ok := slotPos[0][ord]
		if !ok {
			continue
		}
		cnt := 0
		for _, anchor := range base {
			matched := true
			for j := 1; j < len(slots); j++ {
				pj, has := slotPos[j][ord]
				if !has || !sortedContainsInt32(pj, anchor+slots[j].off) {
					matched = false
					break
				}
			}
			if matched {
				cnt++
			}
		}
		if cnt > 0 {
			hits = append(hits, docTf{ord, cnt})
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
