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

package wand

import "sort"

// SearchResult is one ranked hit: the original primary key and its TF-IDF score.
type SearchResult struct {
	DocID any
	Score float64
}

// Membership is the doc-ord allow-set consulted during the WAND walk for
// prefiltering (mirrors cuVS filtered search). It operates on dense int64 doc
// ords so an implementation can be a roaring/cbitmap built once at search setup
// by translating the WHERE filter's pks through the dictionary. nil = unfiltered.
type Membership interface {
	Contains(ord int64) bool
}

const ordEnd = int64(0x7fffffffffffffff)

type cursor struct {
	tp        *termPostings
	idfSq     float64
	weight    float64
	maxScore  float64
	docLen    []int32
	avgDocLen float64
	pos       int
}

func (c *cursor) curDoc() int64 {
	if c.pos < len(c.tp.docIDs) {
		return c.tp.docIDs[c.pos]
	}
	return ordEnd
}

// score is the BM25 contribution at the current posting:
// weight · idf² · bm25Factor(tf, dl, avgdl).
func (c *cursor) score() float64 {
	ord := c.tp.docIDs[c.pos]
	return c.weight * c.idfSq * bm25Factor(float64(c.tp.tfs[c.pos]), c.docLen[ord], c.avgDocLen)
}
func (c *cursor) advance() { c.pos++ }

func (c *cursor) skipTo(d int64) {
	docs := c.tp.docIDs
	lo, hi := c.pos, len(docs)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if docs[mid] < d {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	c.pos = lo
}

// blockIndexAt returns the index of the block (>= the current block) that
// contains doc d (the first block whose last ord >= d). len(blocks) if d is past
// the cursor's last posting.
func (c *cursor) blockIndexAt(d int64) int {
	bl := c.tp.blockLastDoc
	b := c.pos / BlockSize
	for b < len(bl) && bl[b] < d {
		b++
	}
	return b
}

// blockMax is the Block-Max score upper bound for the block containing doc d:
// weight·idf²·bm25Factor(blockMaxTf, blockMinDl, avgdl). 0 if d is past the list.
func (c *cursor) blockMax(d int64) float64 {
	b := c.blockIndexAt(d)
	if b >= len(c.tp.blockLastDoc) {
		return 0
	}
	return c.weight * c.idfSq * bm25Factor(float64(c.tp.blockMaxTf[b]), c.tp.blockMinDl[b], c.avgDocLen)
}

// blockEndAt is the last ord of the block containing doc d (the upper edge of the
// region for which blockMax(d) is a valid bound). ordEnd if past the list.
func (c *cursor) blockEndAt(d int64) int64 {
	b := c.blockIndexAt(d)
	if b >= len(c.tp.blockLastDoc) {
		return ordEnd
	}
	return c.tp.blockLastDoc[b]
}

// Search runs WAND disjunctive top-K over the query terms (duplicates allowed →
// per-term weight). limit is K. allow, if non-nil, restricts to allowed doc
// ords. Returns hits (original pk + score) sorted by score desc.
func (m *WandModel) Search(terms []string, limit int, allow Membership) []SearchResult {
	if limit <= 0 || len(terms) == 0 || m.N <= 0 {
		return nil
	}

	// Resolve query terms to word-ids and collapse duplicates into weights.
	weights := make(map[int32]float64, len(terms))
	for _, t := range terms {
		id, ok, err := m.resolveWordID(t)
		if err != nil || !ok {
			continue // not a dictionary word and not in this index's overflow set
		}
		weights[id]++
	}
	cursors := make([]*cursor, 0, len(weights))
	for id, w := range weights {
		tp, ok := m.terms[id]
		if !ok {
			continue // word resolved but absent from this corpus
		}
		idfSq := m.idfSq(len(tp.docIDs))
		cursors = append(cursors, &cursor{
			tp:        tp,
			idfSq:     idfSq,
			weight:    w,
			maxScore:  w * idfSq * tp.maxFactor,
			docLen:    m.docLen,
			avgDocLen: m.AvgDocLen,
		})
	}
	if len(cursors) == 0 {
		return nil
	}

	h := newTopK(limit)
	for {
		live := cursors[:0]
		for _, c := range cursors {
			if c.curDoc() != ordEnd {
				live = append(live, c)
			}
		}
		cursors = live
		if len(cursors) == 0 {
			break
		}
		sort.Slice(cursors, func(i, j int) bool { return cursors[i].curDoc() < cursors[j].curDoc() })

		theta := -1.0
		if h.full() {
			theta = h.min()
		}

		// Pivot by term-level max-score upper bounds (classic WAND).
		cum := 0.0
		pivot := -1
		for i, c := range cursors {
			cum += c.maxScore
			if cum > theta {
				pivot = i
				break
			}
		}
		if pivot < 0 {
			break // no remaining doc can beat the current top-K
		}
		pivotDoc := cursors[pivot].curDoc()

		// Block-Max refinement: the sum of the per-block upper bounds of
		// cursors[0..pivot] for the blocks covering pivotDoc is a valid bound for
		// every doc in [pivotDoc, minBlockEnd]. If it can't beat theta, skip the
		// whole region instead of evaluating pivotDoc.
		blockSum := 0.0
		for i := 0; i <= pivot; i++ {
			blockSum += cursors[i].blockMax(pivotDoc)
		}
		if blockSum <= theta {
			next := ordEnd
			for i := 0; i <= pivot; i++ {
				if e := cursors[i].blockEndAt(pivotDoc); e < next {
					next = e
				}
			}
			next++ // first doc beyond the limiting block
			if pivot+1 < len(cursors) {
				if nd := cursors[pivot+1].curDoc(); nd < next {
					next = nd
				}
			}
			cursors[chooseSkip(cursors, pivot, next)].skipTo(next)
			continue
		}

		if cursors[0].curDoc() == pivotDoc {
			if allow == nil || allow.Contains(pivotDoc) {
				score := 0.0
				for _, c := range cursors {
					if c.curDoc() == pivotDoc {
						score += c.score()
					}
				}
				h.push(pivotDoc, score)
			}
			for _, c := range cursors {
				if c.curDoc() == pivotDoc {
					c.advance()
				}
			}
		} else {
			// Not aligned: move a cursor before the pivot up to pivotDoc.
			cursors[chooseSkip(cursors, pivot, pivotDoc)].skipTo(pivotDoc)
		}
	}

	return h.sorted(m)
}

// chooseSkip picks a cursor in [0..pivot] whose curDoc < target (so it makes
// progress), preferring the largest term max-score (skip the heaviest list).
// The block-skip / align callers guarantee at least one such cursor exists.
func chooseSkip(cursors []*cursor, pivot int, target int64) int {
	best := -1
	var bestScore float64
	for i := 0; i <= pivot; i++ {
		if cursors[i].curDoc() < target && (best < 0 || cursors[i].maxScore > bestScore) {
			best = i
			bestScore = cursors[i].maxScore
		}
	}
	if best < 0 {
		best = 0 // defensive; should not happen
	}
	return best
}

// ---------------------------------------------------------------------------
// bounded top-K heap on doc ords (keeps the K largest scores; root = minimum)
// ---------------------------------------------------------------------------

type topKEntry struct {
	ord   int64
	score float64
}

type topK struct {
	limit   int
	entries []topKEntry
}

func newTopK(limit int) *topK {
	capHint := limit
	if capHint > 1024 {
		capHint = 1024
	}
	return &topK{limit: limit, entries: make([]topKEntry, 0, capHint)}
}

func (h *topK) full() bool { return len(h.entries) >= h.limit }

func (h *topK) min() float64 {
	if len(h.entries) == 0 {
		return -1.0
	}
	return h.entries[0].score
}

func (h *topK) push(ord int64, score float64) {
	if len(h.entries) < h.limit {
		h.entries = append(h.entries, topKEntry{ord, score})
		h.siftUp(len(h.entries) - 1)
		return
	}
	if score > h.entries[0].score {
		h.entries[0] = topKEntry{ord, score}
		h.siftDown(0)
	}
}

func (h *topK) siftUp(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if h.entries[i].score >= h.entries[parent].score {
			break
		}
		h.entries[i], h.entries[parent] = h.entries[parent], h.entries[i]
		i = parent
	}
}

func (h *topK) siftDown(i int) {
	n := len(h.entries)
	for {
		l := 2*i + 1
		if l >= n {
			break
		}
		s := l
		if r := l + 1; r < n && h.entries[r].score < h.entries[l].score {
			s = r
		}
		if h.entries[s].score >= h.entries[i].score {
			break
		}
		h.entries[i], h.entries[s] = h.entries[s], h.entries[i]
		i = s
	}
}

// sorted drains the heap into results ordered by score desc, then ord asc, and
// maps each ord back to its original pk for output.
func (h *topK) sorted(m *WandModel) []SearchResult {
	tmp := make([]topKEntry, len(h.entries))
	copy(tmp, h.entries)
	sort.Slice(tmp, func(i, j int) bool {
		if tmp[i].score != tmp[j].score {
			return tmp[i].score > tmp[j].score
		}
		return tmp[i].ord < tmp[j].ord
	})
	out := make([]SearchResult, len(tmp))
	for i, e := range tmp {
		out[i] = SearchResult{DocID: m.PkAt(e.ord), Score: e.score}
	}
	return out
}
