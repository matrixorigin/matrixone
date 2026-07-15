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
// The result is the SAME top-k the full scan produces — the same set of documents
// and the same score for each; WAND only avoids work. It layers Block-Max WAND
// (Ding & Suel 2011) on top of classic term-level WAND: besides each term's
// whole-list max-impact, the per-block bounds (blockMaxTf/blockMinDocLn over
// BlockSize-doc blocks, derived by deriveTermStats) give a tighter score UB for the
// block covering the pivot, so a whole block region whose summed block-UB can't
// reach θ is skipped without scoring any doc in it. Block-skip only removes docs
// whose score UB is ≤ θ — which the `score > θ` insertion rule would reject anyway.
//
// Tie caveat: the ORDER of documents that share an equal score is unspecified (like
// bm25, which documents "ties arbitrary"). WAND sums a doc's term contributions in
// cursor order and the full scan in clause-map order, so the two can differ in the
// last float ULP and sort exactly-tied docs differently; and at the k-th boundary a
// tie may include one arbitrary member over another. The top-k SET (above the
// boundary) and the score multiset are identical either way.

// wandIter is a term's posting cursor for WAND. It holds the whole termPostings so
// the walk can read both the term-level max-impact and the per-block Block-Max
// bounds (blockLastDoc/blockMaxTf/blockMinDocLn).
type wandIter struct {
	tp        *termPostings
	idx       int
	idf2      float64
	weight    float64
	maxImpact float64 // term-level upper bound of this term's weighted contribution
}

func (it *wandIter) atEnd() bool { return it.idx >= len(it.tp.docIDs) }

// doc returns the current doc ord, or math.MaxInt64 when exhausted (so an
// exhausted cursor sorts last).
func (it *wandIter) doc() int64 {
	if it.atEnd() {
		return math.MaxInt64
	}
	return it.tp.docIDs[it.idx]
}

// skipTo advances the cursor to the first doc >= d (binary search over the
// remaining ascending docIDs).
func (it *wandIter) skipTo(d int64) {
	rem := it.tp.docIDs[it.idx:]
	it.idx += sort.Search(len(rem), func(i int) bool { return rem[i] >= d })
}

// blockIndexAt returns the index of the block (>= the current block) containing
// doc d — the first block whose last ord >= d. len(blocks) if d is past the
// cursor's last posting. Mirrors bm25's cursor.blockIndexAt.
func (it *wandIter) blockIndexAt(d int64) int {
	bl := it.tp.blockLastDoc
	b := it.idx / BlockSize
	for b < len(bl) && bl[b] < d {
		b++
	}
	return b
}

// blockMax is the weighted Block-Max score upper bound for the block containing
// doc d, in the SAME scorer as termMaxImpact (so it is ≤ the term-level bound). 0
// if d is past the list. blockMaxTf (max tf in block) and blockMinDocLn (min doc
// length in block) give the largest bm25Factor achievable in the block.
func (it *wandIter) blockMax(d int64, algo ScoreAlgo, avgDocLen float64) float64 {
	b := it.blockIndexAt(d)
	if b >= len(it.tp.blockLastDoc) {
		return 0
	}
	if algo == BM25 {
		return it.weight * it.idf2 * bm25Factor(float64(it.tp.blockMaxTf[b]), it.tp.blockMinDocLn[b], avgDocLen)
	}
	return it.weight * float64(it.tp.blockMaxTf[b]) * it.idf2 // TfIdf
}

// blockEndAt is the last ord of the block containing doc d (the upper edge of the
// region for which blockMax(d) is a valid bound). math.MaxInt64 if past the list.
func (it *wandIter) blockEndAt(d int64) int64 {
	b := it.blockIndexAt(d)
	if b >= len(it.tp.blockLastDoc) {
		return math.MaxInt64
	}
	return it.tp.blockLastDoc[b]
}

// chooseSkip picks a cursor in [0..pivot] whose curDoc < target (so it makes
// progress), preferring the largest term max-impact (skip the heaviest list). The
// block-skip / align callers guarantee at least one such cursor exists.
func chooseSkip(iters []*wandIter, pivot int, target int64) int {
	best := -1
	var bestImpact float64
	for i := 0; i <= pivot; i++ {
		if iters[i].doc() < target && (best < 0 || iters[i].maxImpact > bestImpact) {
			best = i
			bestImpact = iters[i].maxImpact
		}
	}
	if best < 0 {
		best = 0 // defensive; should not happen
	}
	return best
}

// scoredDoc / minScoreHeap: a bounded top-k min-heap keyed by score (smallest at
// the root, so it is the eviction candidate and the threshold).
type scoredDoc struct {
	ord   int64
	score float64
}

type minScoreHeap []scoredDoc

func (h minScoreHeap) Len() int { return len(h) }

// Less: smallest score at the root, so the root is the eviction candidate and its
// score is the threshold θ. Among equal scores the LARGER ord is treated as
// "smaller" (evicted first), a best-effort ascending-ord tiebreak; exact tie order
// is still unspecified (see the float-ULP caveat on searchWAND).
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
			tp:        pl,
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

		// Term-level WAND pivot: first cursor at which the cumulative max-impact
		// reaches θ.
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

		// Extend the pivot over every cursor also sitting on pivotDoc, so the
		// block-max sum + skip bounds account for ALL of pivotDoc's contributors (a
		// cursor past the term-UB pivot can still be at pivotDoc). Required for the
		// block-skip bound to be a valid upper bound.
		for pivot+1 < len(iters) && iters[pivot+1].doc() == pivotDoc {
			pivot++
		}

		// Block-Max refinement: the summed per-block upper bounds of iters[0..pivot]
		// for the blocks covering pivotDoc bound every doc in [pivotDoc, minBlockEnd].
		// If that can't exceed θ, skip the whole region instead of scoring pivotDoc.
		// Skipped docs have score ≤ blockSum ≤ θ, which `score > θ` rejects anyway →
		// same top-k SET, less work. (bm25 uses the identical `blockSum <= theta`.)
		blockSum := 0.0
		for i := 0; i <= pivot; i++ {
			blockSum += iters[i].blockMax(pivotDoc, algo, avgDocLen)
		}
		if blockSum <= theta {
			next := int64(math.MaxInt64)
			for i := 0; i <= pivot; i++ {
				if e := iters[i].blockEndAt(pivotDoc); e < next {
					next = e
				}
			}
			next++ // first doc beyond the limiting block
			if pivot+1 < len(iters) {
				if nd := iters[pivot+1].doc(); nd < next {
					next = nd // never skip past an unaccounted contributor
				}
			}
			if next <= pivotDoc {
				next = pivotDoc + 1 // guarantee forward progress
			}
			iters[chooseSkip(iters, pivot, next)].skipTo(next)
			continue
		}

		if iters[0].doc() == pivotDoc {
			// All cursors before the pivot are already on pivotDoc → score it
			// fully across every cursor positioned there, then advance them.
			var score float64
			for _, it := range iters {
				if it.doc() == pivotDoc {
					score += it.weight * s.scoreTerm(algo, float64(it.tp.tfs[it.idx]), it.idf2, pivotDoc, avgDocLen)
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
			// Not aligned: move a lagging cursor (before the pivot) up to pivotDoc.
			iters[chooseSkip(iters, pivot, pivotDoc)].skipTo(pivotDoc)
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
