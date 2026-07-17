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

	"github.com/matrixorigin/matrixone/pkg/vectorindex"
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

// wandIter is a term's posting cursor for WAND. It holds the termPostings for the
// resident Block-Max bounds (blockLastDoc/blockMaxTf/blockMinDocLn), plus a small
// PER-CURSOR decoded-block cache: docIDs/tfs are NOT resident on a loaded segment
// (they live block-compressed in the shared, read-only mmap), so the cursor decodes
// one block at a time into bDocs/bTfs. The cache is per-cursor, so concurrent queries
// over the same shared segment never race. WAND's block-skip means most blocks are
// never decoded.
type wandIter struct {
	tp        *termPostings
	idx       int
	curBlk    int     // block index currently decoded into bDocs/bTfs (-1 = none)
	blen      int     // valid entries in bDocs/bTfs
	bDocs     []int64 // decoded docIDs of curBlk (cap BlockSize)
	bTfs      []uint8 // decoded tfs of curBlk (cap BlockSize)
	idf2      float64
	weight    float64
	maxImpact float64 // term-level upper bound of this term's weighted contribution
}

func (it *wandIter) atEnd() bool { return it.idx >= it.tp.df() }

// ensure decodes the block containing the cursor's current idx into bDocs/bTfs, if
// not already cached. Cheap (a field compare) when the cursor stays within a block.
func (it *wandIter) ensure() {
	b := it.idx / BlockSize
	if b != it.curBlk {
		it.blen = it.tp.fillBlock(b, it.bDocs, it.bTfs)
		it.curBlk = b
	}
}

// doc returns the current doc ord, or math.MaxInt64 when exhausted (so an
// exhausted cursor sorts last).
func (it *wandIter) doc() int64 {
	if it.atEnd() {
		return math.MaxInt64
	}
	it.ensure()
	return it.bDocs[it.idx%BlockSize]
}

// tf returns the current posting's capped term frequency.
func (it *wandIter) tf() uint8 {
	it.ensure()
	return it.bTfs[it.idx%BlockSize]
}

// skipTo advances the cursor to the first doc >= d: locate the block via the
// RESIDENT blockLastDoc (no decode), then binary-search within that one block.
func (it *wandIter) skipTo(d int64) {
	b := it.blockIndexAt(d)
	if b >= it.tp.nblk() {
		it.idx = it.tp.df() // past the last posting → exhausted
		return
	}
	if b != it.curBlk {
		it.blen = it.tp.fillBlock(b, it.bDocs, it.bTfs)
		it.curBlk = b
	}
	// d <= blockLastDoc[b] = bDocs[blen-1], so the lower bound is within the block
	// and never moves the cursor backward (d >= current doc).
	w := sort.Search(it.blen, func(i int) bool { return it.bDocs[i] >= d })
	it.idx = b*BlockSize + w
}

// blockIndexAt returns the index of the block (>= the current block) containing
// doc d — the first block whose last ord >= d. nblk() if d is past the cursor's
// last posting. Uses only the resident blockLastDoc (no block decode).
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

// topKHeap is the bounded top-k-by-score heap shared by the WAND and full boolean
// evaluators: a SoA FastMaxHeap keyed by ord (int64) with distance = -score, so keeping
// the k SMALLEST distances keeps the k HIGHEST scores with zero per-candidate allocation
// and no interface dispatch. Its root distance is the current threshold θ (the k-th best
// score, negated). Equal scores are equally relevant — ties are unspecified.
type topKHeap = vectorindex.FastMaxHeap[float64, int64]

// newTopKHeap allocates the bounded heap's backing buffers. k is capped to maxDocs (the
// segment's live doc count) because the heap can never hold more than that many results:
// FastMaxHeap needs pre-sized buffers, so an absurd pushed LIMIT (e.g. ORDER BY score
// LIMIT 5e8) would otherwise eagerly allocate 2×k slices (~GBs) and OOM the CN, even on a
// tiny table. Bounding to maxDocs is the FastMaxHeap analogue of bm25's lazily-grown topK.
func newTopKHeap(k int, maxDocs int64) *topKHeap {
	if int64(k) > maxDocs {
		k = int(maxDocs)
	}
	if k < 0 {
		k = 0
	}
	return vectorindex.NewFastMaxHeap[float64, int64](k, make([]int64, k), make([]float64, k))
}

// searchWAND runs WAND over the disjunctive single-term SHOULD clauses and
// returns the top-k, score desc (ties by ascending ord). allow (nil = allow all) is
// the WHERE-clause prefilter: a doc is admitted to the top-k only if its ord passes,
// so the LIMIT bounds the FILTERED set. Block-skip stays valid — blockSum is a score
// upper bound over every doc in the region regardless of the filter, so a skipped
// region contains no admissible doc that could beat θ.
// buildWandIters makes a posting cursor for each present SHOULD term, with its
// max-impact bound under the given (global or local) stats. Shared by the top-k
// searchWAND and the no-LIMIT streamWAND.
func (s *Segment) buildWandIters(clauses []clause, algo ScoreAlgo, gs *globalStats, avgDocLen float64) []*wandIter {
	iters := make([]*wandIter, 0, len(clauses))
	for _, c := range clauses {
		pl, ok := s.lookup(c.terms[0])
		if !ok || pl.df() == 0 {
			continue
		}
		idf2 := gs.idfFor(s, c.terms[0], pl)
		iters = append(iters, &wandIter{
			tp:        pl,
			curBlk:    -1,
			bDocs:     make([]int64, BlockSize),
			bTfs:      make([]uint8, BlockSize),
			idf2:      idf2,
			weight:    c.weight,
			maxImpact: c.weight * s.termMaxImpact(algo, idf2, pl, avgDocLen),
		})
	}
	return iters
}

func (s *Segment) searchWAND(clauses []clause, algo ScoreAlgo, k int, allow Membership, gs *globalStats) []Result {
	avgDocLen := gs.avgdl(s)

	iters := s.buildWandIters(clauses, algo, gs, avgDocLen)
	if len(iters) == 0 {
		return nil
	}

	h := newTopKHeap(k, s.N)
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
			// All cursors before the pivot are already on pivotDoc. Score + admit it
			// only if it passes the WHERE prefilter; the cursor advance always runs so a
			// filtered-out pivot never stalls the walk.
			if allowed(allow, pivotDoc) {
				var score float64
				for _, it := range iters {
					if it.doc() == pivotDoc {
						// tf-aware, matching the full-scan evalClause scorer so WAND returns the
						// identical ranking (fulltext2 keeps real tf; see termMaxImpact).
						score += it.weight * s.scoreTerm(algo, float64(it.tf()), it.idf2, pivotDoc, avgDocLen)
					}
				}
				// FastMaxHeap.Push fills until full, then admits iff -score < root
				// distance (i.e. score > θ) with a single sift — the same replace-root
				// behaviour, done internally. θ is the k-th best score = negated root.
				h.Push(pivotDoc, -score)
				if h.Full() {
					_, negRoot, _ := h.Peek()
					theta = -negRoot
				}
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

// heapToResults drains the bounded top-k heap into results ordered by score descending
// (equal scores unspecified). FastMaxHeap.Pop yields the largest distance (= lowest
// score) first, so filling back-to-front lands the output in score-desc order.
func heapToResults(s *Segment, h *topKHeap) []Result {
	n := h.Len()
	results := make([]Result, n)
	for i := n - 1; i >= 0; i-- {
		ord, negScore, _ := h.Pop()
		results[i] = Result{Pk: s.pks[ord], Score: -negScore}
	}
	return results
}
