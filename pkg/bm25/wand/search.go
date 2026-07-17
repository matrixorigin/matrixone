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

// docBitset is a dense per-doc-ord bitset (doc ords are dense in [0, n) per segment),
// 1 bit/doc — ~8× smaller than a []bool. Mirrors fulltext2's docBitset.
type docBitset []uint64

func newDocBitset(n int) docBitset { return make(docBitset, (n+63)/64) }
func (b docBitset) set(i int)      { b[i>>6] |= uint64(1) << (uint(i) & 63) }
func (b docBitset) clear(i int)    { b[i>>6] &^= uint64(1) << (uint(i) & 63) }
func (b docBitset) get(i int) bool { return b[i>>6]&(uint64(1)<<(uint(i)&63)) != 0 }

// ordAllowSet is a dense per-segment allow-set over ords [0, n) used to carry
// precomputed liveness (owner-by-chunk_id ∩ not-deleted) into the WAND walk via the
// existing Membership interface. A set bit ⇒ the ord is live.
type ordAllowSet struct {
	bits docBitset
	n    int
}

func (s *ordAllowSet) Contains(ord int64) bool {
	return ord >= 0 && ord < int64(s.n) && s.bits.get(int(ord))
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
			if cur, ok := owner[k]; !ok || s.Recency >= cur {
				owner[k] = s.Recency
			}
		}
	}

	out := make([]Membership, len(segs))
	for i, s := range segs {
		// Lazily allocate the allow-bitmap only when a dead ord appears — a fully-live
		// segment (the common multi-base case with no pending deletes) keeps out[i]=nil and
		// costs no O(doc-count) allocation. On the first dead ord, backfill the earlier ords
		// (all live so far) so the bitmap stays ord-aligned.
		var bits docBitset
		for ord, pk := range s.pks {
			k := normalizeKey(pk)
			live := owner[k] == s.Recency // this segment owns the live copy
			if live && deletes != nil {
				if dl, ok := deletes[k]; ok && dl > s.Recency {
					live = false // deleted after the latest insert
				}
			}
			if !live && bits == nil {
				bits = newDocBitset(len(s.pks))
				for j := 0; j < ord; j++ {
					bits.set(j) // earlier ords were all live
				}
			}
			if bits != nil && live {
				bits.set(ord)
			}
		}
		if bits != nil {
			out[i] = &ordAllowSet{bits: bits, n: len(s.pks)}
		}
	}
	return out
}

// cursor is a term's posting cursor for the Block-Max WAND walk. On a loaded model
// the postings are NOT resident (they live block-compressed in the shared, read-only
// mmap), so the cursor decodes one block at a time into bDocs/bTfs via
// termPostings.fillBlock. The cache is per-cursor, so concurrent queries over the
// same shared model never race; WAND's block-skip means most blocks are never
// decoded. On a build-side model fillBlock copies from the resident flat slices, so
// the same cursor serves both.
type cursor struct {
	tp        *termPostings
	idfSq     float64
	weight    float64
	maxScore  float64
	docLen    []int32
	avgDocLen float64
	pos       int     // global posting index into the term's df postings
	curBlk    int     // block currently decoded into bDocs/bTfs (-1 = none)
	blen      int     // valid entries in bDocs/bTfs
	cur       int64   // cached curDoc for the current pos (ordEnd when exhausted)
	bDocs     []int64 // decoded docIDs of curBlk (cap BlockSize)
	bTfs      []uint8 // decoded tfs of curBlk (cap BlockSize)
}

func newCursor(tp *termPostings, idfSq, weight, maxScore, avgDocLen float64, docLen []int32) *cursor {
	c := &cursor{
		tp: tp, idfSq: idfSq, weight: weight, maxScore: maxScore,
		docLen: docLen, avgDocLen: avgDocLen, curBlk: -1,
		bDocs: make([]int64, BlockSize), bTfs: make([]uint8, BlockSize),
	}
	c.refresh()
	return c
}

// ensure decodes the block containing pos into bDocs/bTfs, if not already cached.
// Cheap (a field compare) when the cursor stays within a block.
func (c *cursor) ensure() {
	b := c.pos / BlockSize
	if b != c.curBlk {
		c.blen = c.tp.fillBlock(b, c.bDocs, c.bTfs)
		c.curBlk = b
	}
}

// curDoc returns the cached current doc. It is read many times per pivot
// iteration (insertion sort, pivot scan, blockMax, chooseSkip, alignment) while
// the cursor moves at most once, so cur is recomputed only on move (refresh),
// avoiding the df() check, the pos/BlockSize division in ensure, and the modulo
// on every read.
func (c *cursor) curDoc() int64 { return c.cur }

// refresh recomputes cur after pos changes (advance/skipTo/construction).
func (c *cursor) refresh() {
	if c.pos >= c.tp.df() {
		c.cur = ordEnd
		return
	}
	c.ensure()
	c.cur = c.bDocs[c.pos%BlockSize]
}

// score is the BM25 contribution at the current posting:
// weight · idf² · bm25Factor(tf, dl, avgdl).
func (c *cursor) score() float64 {
	c.ensure()
	i := c.pos % BlockSize
	ord := c.bDocs[i]
	return c.weight * c.idfSq * bm25Factor(float64(c.bTfs[i]), c.docLen[ord], c.avgDocLen)
}
func (c *cursor) advance() { c.pos++; c.refresh() }

// skipTo advances the cursor to the first doc >= d: locate the block via the
// RESIDENT blockLastDoc (no decode), then binary-search within that one block.
func (c *cursor) skipTo(d int64) {
	b := c.blockIndexAt(d)
	if b >= c.tp.nblk() {
		c.pos = c.tp.df() // past the last posting → exhausted
		c.cur = ordEnd
		return
	}
	if b != c.curBlk {
		c.blen = c.tp.fillBlock(b, c.bDocs, c.bTfs)
		c.curBlk = b
	}
	// d <= blockLastDoc[b] = bDocs[blen-1], so the lower bound is within the block
	// and never moves the cursor backward (d >= current doc). Inline lower-bound
	// binary search rather than sort.Search: it avoids the per-call closure
	// (skipTo.func1) and the generic sort.Search frame in this hot skip path.
	lo, hi := 0, c.blen
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if c.bDocs[mid] < d {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	c.pos = b*BlockSize + lo
	c.cur = c.bDocs[lo]
}

// blockIndexAt returns the index of the block (>= the current block) that
// contains doc d (the first block whose last ord >= d). nblk() if d is past
// the cursor's last posting. Uses only the resident blockLastDoc (no block decode).
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
// corpusStats returns the corpus-global doc count and average doc length over the
// segment set (both include superseded/deleted docs — the accepted stat drift
// until compaction). Query-INDEPENDENT: it depends only on the loaded segments, so
// the search adapter (WandSearch) precomputes it once at Load and passes it to
// searchSegmentsLiveStats, keeping it off the per-query path.
func corpusStats(segs []*WandModel) (gN int64, gAvgDocLen float64) {
	var totalDocLen float64
	for _, s := range segs {
		gN += s.N
		totalDocLen += s.AvgDocLen * float64(s.N)
	}
	if gN > 0 {
		gAvgDocLen = totalDocLen / float64(gN)
	}
	return gN, gAvgDocLen
}

// SearchSegmentsLive computes the corpus stats inline and delegates. Callers that
// already hold precomputed stats (the load-cached WandSearch) call
// searchSegmentsLiveStats directly.
func SearchSegmentsLive(segs []*WandModel, terms []string, limit int, allow Membership, live []Membership) []SearchResult {
	gN, gAvgDocLen := corpusStats(segs)
	return searchSegmentsLiveStats(segs, terms, limit, allow, live, gN, gAvgDocLen)
}

// searchSegmentsLiveStats is the WAND top-K core with the corpus stats supplied by
// the caller. `live` (per-segment liveness, query-independent) is likewise supplied
// precomputed; only the term-dependent work (weights, per-term df, the walk) runs
// here — so a load-cached adapter pays the O(total-docs) liveness + stats once per
// load, not once per query.
func searchSegmentsLiveStats(segs []*WandModel, terms []string, limit int, allow Membership, live []Membership, gN int64, gAvgDocLen float64) []SearchResult {
	if limit <= 0 || len(terms) == 0 || len(segs) == 0 {
		return nil
	}
	if gN <= 0 {
		return nil
	}

	weights, gdf := queryWeights(segs, terms)
	if len(weights) == 0 {
		return nil
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

// queryWeights builds the dedup'd query-term weights and the corpus-global df per
// word. Resolution of a word to a segment's word-id is done PER SEGMENT: an
// out-of-jieba-dict "overflow" word gets a per-segment id, so a query word can be
// absent from one segment (e.g. the compacted base) yet present in a later
// CDC-delta segment, and independently-built segments may assign it different
// overflow ids. Resolving once against a single segment would drop or mis-map such
// a word (in-dict words resolve to a stable global id, unaffected).
func queryWeights(segs []*WandModel, terms []string) (map[string]float64, map[string]int) {
	weights := make(map[string]float64, len(terms))
	for _, t := range terms {
		weights[t]++
	}
	gdf := make(map[string]int, len(weights))
	for w := range weights {
		df := 0
		for _, s := range segs {
			if id, ok, err := s.resolveWordID(w); err == nil && ok {
				if tp, ok2 := s.lookupTerm(id); ok2 {
					df += tp.df()
				}
			}
		}
		gdf[w] = df
	}
	return weights, gdf
}

// streamBatch is the max rows a streamSink buffers before flushing to emit.
const streamBatch = 8192

// streamSink batches (pk, score) results and flushes them to emit in bounded
// chunks, so a no-LIMIT retrieval query returns every matching doc without ever
// materializing them all. On an emit error it records it and stops (the walk
// checks stopped and bails), so a cancelled consumer terminates the walk promptly.
type streamSink struct {
	emit    func(keys []any, distances []float64) error
	keys    []any
	scores  []float64
	err     error
	stopped bool
}

func (s *streamSink) push(pk any, score float64) {
	if s.stopped {
		return
	}
	s.keys = append(s.keys, pk)
	s.scores = append(s.scores, score)
	if len(s.keys) >= streamBatch {
		s.flush()
	}
}

func (s *streamSink) flush() {
	if s.stopped || len(s.keys) == 0 {
		return
	}
	if e := s.emit(s.keys, s.scores); e != nil {
		s.err = e
		s.stopped = true
		return
	}
	// Hand ownership of the batch to emit; the next batch reallocates on append.
	s.keys = nil
	s.scores = nil
}

// streamInto does a plain document-at-a-time OR merge over one segment: it visits
// every matching doc in ord order, scores it (BM25 over the query terms present),
// and pushes it to the sink — no top-K heap, no WAND pruning. The no-LIMIT case
// wants every match; ranking is done by the upstream ORDER BY score node.
func (m *WandModel) streamInto(sink *streamSink, weights map[string]float64, gN int64, gAvgDocLen float64, gdf map[string]int, allow Membership) {
	cursors := make([]*cursor, 0, len(weights))
	for word, w := range weights {
		id, ok, err := m.resolveWordID(word)
		if err != nil || !ok {
			continue
		}
		tp, ok := m.lookupTerm(id)
		if !ok {
			continue
		}
		df := gdf[word]
		if df <= 0 {
			df = tp.df()
		}
		idf := log10(float64(gN) / float64(df))
		idfSq := idf * idf
		cursors = append(cursors, newCursor(tp, idfSq, w,
			w*idfSq*bm25Factor(float64(tp.maxTf), tp.minDl, gAvgDocLen), gAvgDocLen, m.docLen))
	}
	if len(cursors) == 0 {
		return
	}
	for !sink.stopped {
		minDoc := ordEnd
		for _, c := range cursors {
			if d := c.curDoc(); d < minDoc {
				minDoc = d
			}
		}
		if minDoc == ordEnd {
			break
		}
		if allow == nil || allow.Contains(minDoc) {
			score := 0.0
			for _, c := range cursors {
				if c.curDoc() == minDoc {
					score += c.score()
				}
			}
			sink.push(m.PkAt(minDoc), score)
		}
		for _, c := range cursors {
			if c.curDoc() == minDoc {
				c.advance()
			}
		}
	}
}

// streamSegmentsLiveStats is the no-LIMIT streaming counterpart of
// searchSegmentsLiveStats: it walks every segment with liveness + the optional
// WHERE prefilter and emits all matching (pk, score) rows in bounded batches via
// emit, with no top-K heap and no internal sort.
func streamSegmentsLiveStats(segs []*WandModel, terms []string, emit func(keys []any, distances []float64) error, allow Membership, live []Membership, gN int64, gAvgDocLen float64) error {
	if len(terms) == 0 || len(segs) == 0 || gN <= 0 {
		return nil
	}
	weights, gdf := queryWeights(segs, terms)
	if len(weights) == 0 {
		return nil
	}
	sink := &streamSink{emit: emit}
	for i, s := range segs {
		segAllow := allow
		if i < len(live) {
			segAllow = andAllow(allow, live[i])
		}
		s.streamInto(sink, weights, gN, gAvgDocLen, gdf, segAllow)
		if sink.stopped {
			return sink.err
		}
	}
	sink.flush() // final partial batch
	return sink.err
}

// searchSegsLive is a standalone convenience that computes per-segment liveness
// (owner-by-chunk_id ∩ not-deleted) over segs+deletes, optionally ANDs a
// per-segment WHERE prefilter built by mkAllow (nil = unfiltered), and runs the
// corpus-global WAND top-K. mkAllow is called once per segment so a pk-based
// filter resolves against that segment's own ord→pk dictionary — a single filter
// over "ords" would be wrong, since ord i denotes a different pk in each segment.
//
// NB: this recomputes liveness+stats every call. The production load-path adapter
// (WandSearch) does NOT use it — it precomputes liveness+stats at Load and calls
// searchSegmentsLiveStats per query (Phase-C item 3). Kept for tests / one-shot use.
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
		tp, ok := m.lookupTerm(id)
		if !ok {
			continue // word absent from this segment
		}
		df := gdf[word]
		if df <= 0 {
			df = tp.df()
		}
		idf := log10(float64(gN) / float64(df))
		idfSq := idf * idf
		cursors = append(cursors, newCursor(tp, idfSq, w,
			w*idfSq*bm25Factor(float64(tp.maxTf), tp.minDl, gAvgDocLen), gAvgDocLen, m.docLen))
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
		// Keep cursors sorted by curDoc. Insertion sort, NOT sort.Slice: the
		// cursors are nearly sorted between iterations (only the skipped cursor
		// moved), so this is O(len) here; more importantly sort.Slice boxes the
		// slice into an interface{} (runtime.convTslice) and heap-allocates the
		// less closure every call — in this hot per-pivot loop that alloc churn
		// (and the reflect-based Swapper) dominated the entire query CPU.
		for i := 1; i < len(cursors); i++ {
			ci := cursors[i]
			di := ci.curDoc()
			j := i - 1
			for j >= 0 && cursors[j].curDoc() > di {
				cursors[j+1] = cursors[j]
				j--
			}
			cursors[j+1] = ci
		}

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
