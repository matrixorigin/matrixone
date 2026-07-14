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
	"container/heap"
	"math"
	"sort"
)

// WAND (Weak-AND, Broder et al. 2003) disjunctive top-k for a pure OR of single
// terms. Each term posting iterator carries a max-impact upper bound (its
// largest possible weighted contribution); the algorithm keeps a top-k min-heap
// whose smallest score is the threshold θ, and skips any document whose summed
// upper bounds cannot reach θ — so most low-scoring documents are never scored.
//
// The result is the IDENTICAL ranking the full scan produces (same score, same
// ascending-ord tiebreak); WAND only avoids work. This is the classic term-level
// WAND; block-max WAND (using termPostings' per-block bounds for tighter skips)
// is a later enhancement and does not change results.

// wandIter is a term's posting cursor for WAND.
type wandIter struct {
	docIDs    []int64
	tfs       []uint8
	idx       int
	idf2      float64
	weight    float64
	maxImpact float64 // upper bound of this term's weighted contribution
}

func (it *wandIter) atEnd() bool { return it.idx >= len(it.docIDs) }

// doc returns the current doc ord, or math.MaxInt64 when exhausted (so an
// exhausted cursor sorts last).
func (it *wandIter) doc() int64 {
	if it.atEnd() {
		return math.MaxInt64
	}
	return it.docIDs[it.idx]
}

// skipTo advances the cursor to the first doc >= d (binary search over the
// remaining ascending docIDs).
func (it *wandIter) skipTo(d int64) {
	rem := it.docIDs[it.idx:]
	it.idx += sort.Search(len(rem), func(i int) bool { return rem[i] >= d })
}

// scoredDoc / minScoreHeap: a bounded top-k min-heap keyed by score (smallest at
// the root, so it is the eviction candidate and the threshold).
type scoredDoc struct {
	ord   int64
	score float64
}

type minScoreHeap []scoredDoc

func (h minScoreHeap) Len() int { return len(h) }

// Less: smallest score at the root; among equal scores the LARGER ord is
// "smaller" so it is the eviction candidate — keeping the smaller-ord doc, which
// matches the full scan's (score desc, ord asc) ranking on ties. With documents
// scored in ascending ord, this makes WAND's top-k byte-identical to the full
// scan's for every k.
func (h minScoreHeap) Less(i, j int) bool {
	if h[i].score != h[j].score {
		return h[i].score < h[j].score
	}
	return h[i].ord > h[j].ord
}
func (h minScoreHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *minScoreHeap) Push(x any)   { *h = append(*h, x.(scoredDoc)) }
func (h *minScoreHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// searchWAND runs WAND over the disjunctive single-term SHOULD clauses and
// returns the top-k, score desc (ties by ascending ord).
func (s *Segment) searchWAND(clauses []clause, algo ScoreAlgo, k int) []Result {
	avgDocLen := s.avgDocLenOrMean()

	iters := make([]*wandIter, 0, len(clauses))
	for _, c := range clauses {
		pl, ok := s.lookup(c.terms[0])
		if !ok || pl.df() == 0 {
			continue
		}
		idf2 := idfSquared(s.N, pl.df())
		iters = append(iters, &wandIter{
			docIDs:    pl.docIDs,
			tfs:       pl.tfs,
			idf2:      idf2,
			weight:    c.weight,
			maxImpact: c.weight * s.termMaxImpact(algo, idf2, pl, avgDocLen),
		})
	}
	if len(iters) == 0 {
		return nil
	}

	h := &minScoreHeap{}
	theta := math.Inf(-1) // until the heap holds k, accept everything

	for {
		// Order cursors by current doc ascending (exhausted ones sort last).
		sort.Slice(iters, func(a, b int) bool { return iters[a].doc() < iters[b].doc() })
		if iters[0].atEnd() {
			break // all exhausted
		}

		// Pivot: first cursor at which the cumulative max-impact reaches θ.
		acc, pivot := 0.0, -1
		for i, it := range iters {
			if it.atEnd() {
				break
			}
			acc += it.maxImpact
			if acc >= theta {
				pivot = i
				break
			}
		}
		if pivot < 0 {
			break // no remaining document can reach the threshold
		}
		pivotDoc := iters[pivot].doc()

		if iters[0].doc() == pivotDoc {
			// All cursors before the pivot are already on pivotDoc → score it
			// fully across every cursor positioned there, then advance them.
			var score float64
			for _, it := range iters {
				if it.doc() == pivotDoc {
					score += it.weight * s.scoreTerm(algo, float64(it.tfs[it.idx]), it.idf2, pivotDoc, avgDocLen)
				}
			}
			if h.Len() < k {
				heap.Push(h, scoredDoc{pivotDoc, score})
				if h.Len() == k {
					theta = (*h)[0].score
				}
			} else if score > theta {
				heap.Pop(h)
				heap.Push(h, scoredDoc{pivotDoc, score})
				theta = (*h)[0].score
			}
			for _, it := range iters {
				if it.doc() == pivotDoc {
					it.idx++
				}
			}
		} else {
			// Move a lagging cursor (before the pivot) up to pivotDoc.
			iters[0].skipTo(pivotDoc)
		}
	}

	return heapToResults(s, h)
}

// termMaxImpact is the largest weighted-free score a term can contribute to any
// document, from its raw maxTf (and minDocLen for BM25's saturating factor).
func (s *Segment) termMaxImpact(algo ScoreAlgo, idf2 float64, pl *termPostings, avgDocLen float64) float64 {
	if algo == BM25 {
		return idf2 * bm25Factor(float64(pl.maxTf), pl.minDocLen, avgDocLen)
	}
	return float64(pl.maxTf) * idf2 // TfIdf
}

// heapToResults drains a top-k min-heap into results ordered by score desc, ties
// by ascending ord — matching the full evaluator's ordering.
func heapToResults(s *Segment, h *minScoreHeap) []Result {
	docs := *h
	sort.Slice(docs, func(a, b int) bool {
		if docs[a].score != docs[b].score {
			return docs[a].score > docs[b].score
		}
		return docs[a].ord < docs[b].ord
	})
	results := make([]Result, len(docs))
	for i, d := range docs {
		results[i] = Result{Pk: s.pks[d.ord], Score: d.score}
	}
	return results
}
