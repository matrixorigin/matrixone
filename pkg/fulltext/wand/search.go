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

// ordAllowSet is a dense per-segment allow-set over ords [0, n) used to carry
// precomputed liveness (owner-by-chunk_id ∩ not-deleted) into the WAND walk via the
// existing Membership interface. allow[ord]==true ⇒ the ord is live.
type ordAllowSet struct{ allow []bool }

func (s *ordAllowSet) Contains(ord int64) bool {
	return ord >= 0 && ord < int64(len(s.allow)) && s.allow[ord]
}

// andMembership is the conjunction of two Membership filters (either may be
// nil = "allow all"); used to AND a WHERE-prefilter with per-segment liveness.
type andMembership struct{ a, b Membership }

func (m andMembership) Contains(ord int64) bool {
	if m.a != nil && !m.a.Contains(ord) {
		return false
	}
	if m.b != nil && !m.b.Contains(ord) {
		return false
	}
	return true
}

func andAllow(a, b Membership) Membership {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	default:
		return andMembership{a, b}
	}
}

// ComputeLiveness resolves, once when a segment set is assembled (load time),
// which ord in each segment is the LIVE copy of its pk — the chunk_id-as-identity
// rule that makes CDC delete-then-reinsert / UPDATE correct over immutable
// segments:
//
//   - a pk's live copy is the one in the HIGHEST-ChunkId segment that holds it
//     (older copies of an UPDATEd pk are superseded — dedup, no duplicate row);
//   - that copy is dead iff a delete exists with deleteChunkId > thatSegmentChunkId
//     (a delete after the latest insert; a delete before it is superseded).
//
// deletes maps normalizeKey(pk) -> max delete-frame chunk_id for that pk (nil =
// none). The result is parallel to segs: entry i is a Membership over segment i's
// ords (nil ⇒ every ord live, the fast path for a single/compacted segment),
// passed to SearchSegmentsLive. O(total docs), done once per load, not per query.
func ComputeLiveness(segs []*WandModel, deletes map[any]int64) []Membership {
	if len(segs) == 0 {
		return nil
	}
	// Fast path: a single segment with no deletes — everything is live.
	if len(segs) == 1 && len(deletes) == 0 {
		return []Membership{nil}
	}

	// owner[pk] = the max segment chunk_id holding pk (the live copy's segment).
	owner := make(map[any]int64)
	for _, s := range segs {
		for _, pk := range s.pks {
			k := normalizeKey(pk)
			if cur, ok := owner[k]; !ok || s.ChunkId >= cur {
				owner[k] = s.ChunkId
			}
		}
	}

	out := make([]Membership, len(segs))
	for i, s := range segs {
		allLive := true
		allow := make([]bool, len(s.pks))
		for ord, pk := range s.pks {
			k := normalizeKey(pk)
			live := owner[k] == s.ChunkId // this segment owns the live copy
			if live && deletes != nil {
				if dl, ok := deletes[k]; ok && dl > s.ChunkId {
					live = false // deleted after the latest insert
				}
			}
			allow[ord] = live
			if !live {
				allLive = false
			}
		}
		if allLive {
			out[i] = nil
		} else {
			out[i] = &ordAllowSet{allow: allow}
		}
	}
	return out
}

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

// Search runs WAND disjunctive top-K over a single index. Convenience wrapper
// over SearchSegments.
func (m *WandModel) Search(terms []string, limit int, allow Membership) []SearchResult {
	return SearchSegments([]*WandModel{m}, terms, limit, allow)
}

// SearchSegments runs the WAND top-K over the segment set with no liveness
// filtering — valid for a single index or DISJOINT FinishSegments partitions
// (whose pks never collide). For CDC delta segments (where a pk can recur across
// segments) use SearchSegmentsLive with ComputeLiveness, else a re-inserted pk
// would appear once per segment.
func SearchSegments(segs []*WandModel, terms []string, limit int, allow Membership) []SearchResult {
	return SearchSegmentsLive(segs, terms, limit, allow, nil)
}

// SearchSegmentsLive runs WAND disjunctive top-K across one or more index
// segments with CORPUS-GLOBAL BM25 scoring, so the merged top-K is correctly
// ranked even when each segment holds only a slice of the corpus. Global N,
// avgdl and per-term df are aggregated across segments, then each segment's
// Block-Max walk pushes into one shared bounded heap (the running k-th score
// prunes later segments too). limit is K; allow, if non-nil, is the WHERE-clause
// prefilter over doc ords. live, if non-nil, is parallel to segs (from
// ComputeLiveness): live[i] is ANDed with allow for segment i so superseded /
// deleted ords are skipped. A nil live or a nil live[i] means "all ords live".
func SearchSegmentsLive(segs []*WandModel, terms []string, limit int, allow Membership, live []Membership) []SearchResult {
	if limit <= 0 || len(terms) == 0 || len(segs) == 0 {
		return nil
	}

	// Corpus-global N + average doc length.
	var gN int64
	var totalDocLen float64
	for _, s := range segs {
		gN += s.N
		totalDocLen += s.AvgDocLen * float64(s.N)
	}
	if gN <= 0 {
		return nil
	}
	gAvgDocLen := totalDocLen / float64(gN)

	// Query terms are tokenized words; dedup → weight. Resolution of a word to a
	// segment's word-id is done PER SEGMENT (here and in searchInto): an
	// out-of-jieba-dict "overflow" word gets a per-segment id, so a query word can
	// be absent from one segment (e.g. the compacted base) yet present in a later
	// CDC-delta segment, and independently-built segments may assign it different
	// overflow ids. Resolving once against a single segment would drop or mis-map
	// such a word (in-dict words resolve to a stable global id, unaffected).
	weights := make(map[string]float64, len(terms))
	for _, t := range terms {
		weights[t]++
	}
	if len(weights) == 0 {
		return nil
	}

	// Corpus-global df per query word (each segment resolves the word to its own
	// id, so both in-dict and overflow words are summed correctly).
	gdf := make(map[string]int, len(weights))
	for w := range weights {
		df := 0
		for _, s := range segs {
			if id, ok, err := s.resolveWordID(w); err == nil && ok {
				if tp, ok2 := s.terms[id]; ok2 {
					df += len(tp.docIDs)
				}
			}
		}
		gdf[w] = df
	}

	h := newTopK(limit)
	for i, s := range segs {
		segAllow := allow
		if i < len(live) {
			segAllow = andAllow(allow, live[i])
		}
		s.searchInto(h, weights, gN, gAvgDocLen, gdf, segAllow)
	}
	return h.sorted()
}

// searchSegsLive is the multi-segment search entry point used by the load-path
// adapter (WandSearch): it computes per-segment liveness (owner-by-chunk_id ∩
// not-deleted) over segs+deletes, optionally ANDs a per-segment WHERE prefilter
// built by mkAllow (nil = unfiltered), and runs the corpus-global WAND top-K.
// mkAllow is called once per segment so a pk-based filter resolves against that
// segment's own ord→pk dictionary — a single filter over "ords" would be wrong,
// since ord i denotes a different pk in each segment.
func searchSegsLive(segs []*WandModel, deletes map[any]int64, terms []string, limit int, mkAllow func(*WandModel) Membership) []SearchResult {
	live := ComputeLiveness(segs, deletes)
	if mkAllow != nil {
		if live == nil {
			live = make([]Membership, len(segs))
		}
		for i, s := range segs {
			live[i] = andAllow(mkAllow(s), live[i])
		}
	}
	return SearchSegmentsLive(segs, terms, limit, nil, live)
}

// searchInto runs the Block-Max WAND walk over one segment using the supplied
// global stats, pushing (pk, score) into the shared heap h.
func (m *WandModel) searchInto(h *topK, weights map[string]float64, gN int64, gAvgDocLen float64, gdf map[string]int, allow Membership) {
	cursors := make([]*cursor, 0, len(weights))
	for word, w := range weights {
		id, ok, err := m.resolveWordID(word)
		if err != nil || !ok {
			continue // word not resolvable in this segment
		}
		tp, ok := m.terms[id]
		if !ok {
			continue // word absent from this segment
		}
		df := gdf[word]
		if df <= 0 {
			df = len(tp.docIDs)
		}
		idf := log10(float64(gN) / float64(df))
		idfSq := idf * idf
		cursors = append(cursors, &cursor{
			tp:        tp,
			idfSq:     idfSq,
			weight:    w,
			maxScore:  w * idfSq * bm25Factor(float64(tp.maxTf), tp.minDl, gAvgDocLen),
			docLen:    m.docLen,
			avgDocLen: gAvgDocLen,
		})
	}
	if len(cursors) == 0 {
		return
	}

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

		// Extend the pivot over every cursor also sitting on pivotDoc, so the
		// block-max sum and skip bounds account for all of pivotDoc's
		// contributors (a cursor beyond the term-UB pivot can still be at
		// pivotDoc and add to its score).
		for pivot+1 < len(cursors) && cursors[pivot+1].curDoc() == pivotDoc {
			pivot++
		}

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
			if next <= pivotDoc {
				// guarantee forward progress: when cursors are aligned at
				// pivotDoc (or the next cursor sits on it), the smallest skip
				// that still advances is past pivotDoc.
				next = pivotDoc + 1
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
				h.push(m.PkAt(pivotDoc), score)
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
	pk    any // original primary key (resolved at push time; segments share one heap)
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

func (h *topK) push(pk any, score float64) {
	if len(h.entries) < h.limit {
		h.entries = append(h.entries, topKEntry{pk, score})
		h.siftUp(len(h.entries) - 1)
		return
	}
	if score > h.entries[0].score {
		h.entries[0] = topKEntry{pk, score}
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

// sorted drains the heap into results ordered by score desc (ties arbitrary).
func (h *topK) sorted() []SearchResult {
	out := make([]SearchResult, len(h.entries))
	for i, e := range h.entries {
		out[i] = SearchResult{DocID: e.pk, Score: e.score}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Score > out[j].Score })
	return out
}
