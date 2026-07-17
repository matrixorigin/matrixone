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
	"math/bits"
	"slices"
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

// matchPhrase verifies a positional phrase against the segment, returning per-doc phrase
// occurrence counts. The common shape — a multi-slot phrase of EXACT trigrams (every CJK
// query >= 3 chars decomposes to these) — runs the BLOCK-CURSOR conjunctive intersection
// (matchPhraseCursor): it advances a Block-Max cursor per slot in doc order, decoding one
// docID block + one position block at a time, so peak memory is O(nslots × BlockSize)
// regardless of corpus size. A single-slot phrase or one containing a star (prefix) slot
// — only 1-2 char CJK runs — takes the materialize fallback (which handles the presence-
// only prefix union).
func (s *Segment) matchPhrase(slots []phraseSlot) []docTf {
	if len(slots) < 2 {
		return s.matchPhraseFallback(slots)
	}
	for _, sl := range slots {
		if sl.star {
			return s.matchPhraseFallback(slots)
		}
	}
	return s.matchPhraseCursor(slots)
}

// matchPhraseFallback is the materialize-then-probe evaluator: it resolves each slot to
// doc-sorted postings, anchors on the rarest, and probes the others by binary search. It
// backs the single-slot and star-slot cases (a lone star slot is presence-only via a
// doc-ord bitset). No per-slot map, no re-sort (positions are stored ascending).
func (s *Segment) matchPhraseFallback(slots []phraseSlot) []docTf {
	if len(slots) == 0 {
		return nil
	}
	// Resolve each slot to its doc-sorted postings (docs[i] parallel to pos[i]). A lone
	// star slot skips positions (presence-only); a resolved slot no doc satisfies ⇒ nil.
	single := len(slots) == 1
	docsOf := make([][]int64, len(slots))
	posOf := make([][][]int32, len(slots))
	rare := 0
	for i, sl := range slots {
		d, p, ok := s.slotPostings(sl, single)
		if !ok {
			return nil
		}
		docsOf[i], posOf[i] = d, p
		if len(d) < len(docsOf[rare]) {
			rare = i
		}
	}

	// Single slot: every doc containing it is a hit; tf = its occurrence count (1 for a
	// presence-only star slot).
	if single {
		hits := make([]docTf, 0, len(docsOf[0]))
		for di, ord := range docsOf[0] {
			tf := 1
			if posOf[0] != nil {
				tf = len(posOf[0][di])
			}
			hits = append(hits, docTf{ord, tf})
		}
		return hits
	}

	// Multi-slot: iterate the rarest slot's docs; for each, look up this doc's positions in
	// every slot (binary search), then anchor on the rare slot's positions and verify the
	// others at B+off by binary search. Common slots are probed, never materialized whole.
	var hits []docTf
	posForDoc := make([][]int32, len(slots)) // scratch, reused across docs
	for ri, ord := range docsOf[rare] {
		present := true
		for j := range slots {
			if j == rare {
				posForDoc[j] = posOf[rare][ri]
				continue
			}
			k := sortedIndexInt64(docsOf[j], ord)
			if k < 0 {
				present = false
				break
			}
			posForDoc[j] = posOf[j][k]
		}
		if !present {
			continue
		}
		cnt := 0
		for _, pr := range posForDoc[rare] {
			start := pr - slots[rare].off // candidate phrase-start byte position
			matched := true
			for j := range slots {
				if j == rare {
					continue
				}
				if !sortedContainsInt32(posForDoc[j], start+slots[j].off) {
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

// slotPostings resolves one phrase slot to doc-sorted postings (docs parallel to pos).
// An exact slot reuses the term's stored, already-ascending positions directly (no copy,
// no re-sort). A star slot expands its prefix: a LONE star slot (single-slot phrase) is
// presence-only — it unions just the doc IDs (pos==nil) so a hot 2-char prefix cannot
// materialize millions of positions; an embedded star slot (rare) merges positions.
func (s *Segment) slotPostings(sl phraseSlot, single bool) ([]int64, [][]int32, bool) {
	if !sl.star {
		pl, ok := s.lookup(sl.term)
		if !ok {
			return nil, nil, false
		}
		return pl.materializeDocIDs(), pl.materializePositions(), true
	}
	terms, err := s.prefixTerms(sl.term)
	if err != nil || len(terms) == 0 {
		return nil, nil, false
	}
	if single {
		docs := s.unionDocIDs(terms)
		if len(docs) == 0 {
			return nil, nil, false
		}
		return docs, nil, true
	}
	return s.mergeStarPostings(terms)
}

// unionDocIDs returns the ascending union of the doc IDs of the given terms — no
// positions (the presence-only path for a lone prefix slot). It unions into a doc-ord
// BITSET rather than a map: a hot 2-char prefix (e.g. 中文*) expands to many trigrams
// covering most of the corpus, and a growing map[int64]struct{} rehashed itself to death
// (the dominant cost once matchPhrase's own map was removed). A bitset is O(N/8) with no
// per-doc allocation or rehash, and the bit walk yields docs already ascending.
func (s *Segment) unionDocIDs(terms []string) []int64 {
	if s.N == 0 {
		return nil
	}
	bset := make([]uint64, (s.N+63)/64)
	any := false
	for _, t := range terms {
		pl, ok := s.lookup(t)
		if !ok {
			continue
		}
		for _, ord := range pl.materializeDocIDs() {
			bset[ord>>6] |= uint64(1) << (uint64(ord) & 63)
			any = true
		}
	}
	if !any {
		return nil
	}
	docs := make([]int64, 0, 256)
	for w, word := range bset {
		base := int64(w) << 6
		for word != 0 {
			docs = append(docs, base+int64(bits.TrailingZeros64(word)))
			word &= word - 1
		}
	}
	return docs
}

// mergeStarPostings merges the doc-sorted postings of a prefix expansion into one
// (docs, positions) with per-doc positions sorted (they come from different terms). This
// is the rare embedded-prefix path (a 1-2 char CJK run inside a longer phrase), so the
// map here is bounded and off the hot path.
func (s *Segment) mergeStarPostings(terms []string) ([]int64, [][]int32, bool) {
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
		return nil, nil, false
	}
	docs := make([]int64, 0, len(m))
	for ord := range m {
		docs = append(docs, ord)
	}
	slices.Sort(docs)
	pos := make([][]int32, len(docs))
	for i, ord := range docs {
		p := m[ord]
		slices.Sort(p)
		pos[i] = p
	}
	return docs, pos, true
}

// sortedIndexInt64 returns the index of v in the ascending slice a, or -1 if absent.
func sortedIndexInt64(a []int64, v int64) int {
	i := sort.Search(len(a), func(i int) bool { return a[i] >= v })
	if i < len(a) && a[i] == v {
		return i
	}
	return -1
}

// phraseCursor walks one exact term's Block-Max postings in doc order for the phrase
// intersection, decoding ONE docID block + ONE position block at a time (never the whole
// list). It is the phrase analogue of wandIter, but also carries the block's positions.
type phraseCursor struct {
	tp     *termPostings
	off    int32     // this slot's byte offset within the phrase
	idx    int       // current global posting index (0..df-1)
	curBlk int       // block currently decoded into bDocs/bPos (-1 = none)
	blen   int       // valid entries in bDocs/bPos
	bDocs  []int64   // decoded docIDs of curBlk (cap BlockSize)
	bPos   [][]int32 // decoded positions of curBlk (cap BlockSize)
	tfbuf  []uint8   // scratch for fillBlock's tfs (phrase ignores tf)
}

func newPhraseCursor(tp *termPostings, off int32) *phraseCursor {
	return &phraseCursor{
		tp: tp, off: off, curBlk: -1,
		bDocs: make([]int64, BlockSize),
		bPos:  make([][]int32, BlockSize),
		tfbuf: make([]uint8, BlockSize),
	}
}

func (c *phraseCursor) atEnd() bool { return c.idx >= c.tp.df() }

// decode ensures block b is the one loaded in bDocs/bPos (a field compare when the cursor
// stays within a block; skipped blocks are never decoded).
func (c *phraseCursor) decode(b int) {
	if b == c.curBlk {
		return
	}
	c.blen = c.tp.fillBlock(b, c.bDocs, c.tfbuf)
	c.tp.fillBlockPositions(b, c.bPos)
	c.curBlk = b
}

// doc returns the current doc ord, or MaxInt64 when exhausted (sorts last).
func (c *phraseCursor) doc() int64 {
	if c.atEnd() {
		return math.MaxInt64
	}
	c.decode(c.idx / BlockSize)
	return c.bDocs[c.idx%BlockSize]
}

// positions returns the current doc's ascending byte positions.
func (c *phraseCursor) positions() []int32 {
	c.decode(c.idx / BlockSize)
	return c.bPos[c.idx%BlockSize]
}

// skipTo advances (forward only) to the first doc >= target, using the resident
// blockLastDoc to pick the block so non-overlapping blocks are skipped without decoding.
func (c *phraseCursor) skipTo(target int64) {
	bl := c.tp.blockLastDoc
	b := c.idx / BlockSize
	for b < len(bl) && bl[b] < target {
		b++
	}
	if b >= c.tp.nblk() {
		c.idx = c.tp.df() // past the last posting → exhausted
		return
	}
	c.decode(b)
	w := sort.Search(c.blen, func(i int) bool { return c.bDocs[i] >= target })
	c.idx = b*BlockSize + w
}

// matchPhraseCursor runs the block-cursor conjunctive phrase intersection over EXACT
// slots: it drives the rarest slot in doc order and, for each of its docs, skips the
// other cursors to that doc (block-skipping non-overlapping regions) and verifies byte-
// position adjacency. Peak memory is O(nslots × BlockSize) — one decoded block per
// cursor — independent of corpus size, vs the fallback's whole-posting-list materialize.
func (s *Segment) matchPhraseCursor(slots []phraseSlot) []docTf {
	cursors := make([]*phraseCursor, len(slots))
	rare := 0
	for i, sl := range slots {
		pl, ok := s.lookup(sl.term)
		if !ok {
			return nil // a slot no doc satisfies ⇒ the phrase can't occur
		}
		cursors[i] = newPhraseCursor(pl, sl.off)
		if pl.df() < cursors[rare].tp.df() {
			rare = i
		}
	}
	rc := cursors[rare]
	var hits []docTf
	for !rc.atEnd() {
		d := rc.doc()
		present := true
		for j, c := range cursors {
			if j == rare {
				continue
			}
			c.skipTo(d)
			if c.doc() != d {
				present = false
				break
			}
		}
		if present {
			cnt := 0
			for _, pr := range rc.positions() {
				start := pr - slots[rare].off // candidate phrase-start byte position
				matched := true
				for j := range slots {
					if j == rare {
						continue
					}
					if !sortedContainsInt32(cursors[j].positions(), start+slots[j].off) {
						matched = false
						break
					}
				}
				if matched {
					cnt++
				}
			}
			if cnt > 0 {
				hits = append(hits, docTf{d, cnt})
			}
		}
		rc.idx++
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
