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
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
)

// Index is a queryable fulltext v2 index made of several loaded segments — a
// tag=0 base plus tag=1 CDC tail deltas (§4) — with per-pk liveness and a delete
// set. It resolves the two things a lone Segment cannot:
//
//   - GLOBAL stats: N and avgDocLen span all segments (a term's idf and BM25's
//     length norm must be global, not per-segment), computed once at construction.
//   - Liveness: the same pk may land in several segments (UPDATE / reinsert / a
//     stale base copy); only the highest-Recency copy is live, and a delete at
//     recency d kills copies with recency < d. Dead copies never appear in
//     results and never contribute to a phrase's document frequency.
type Index struct {
	segments []*Segment
	deletes  map[any]int64 // normalizeKey(pk) -> recency at/after which older copies are dead

	globalN         int64
	globalAvgDocLen float64
	// liveLoc maps a live pk key (normalizeKey) to the (segment index, doc ord) of its live copy.
	liveLoc map[any]docLoc
	// liveOrd[si] is a per-segment ord-indexed liveness bitmap (built once in resolve),
	// so the boolean walk's livenessMembership is O(1) and allocation-free — no per-
	// candidate keyOf + map lookup. A fully-live segment keeps liveOrd[si]==nil ("all
	// live" fast path). Mirrors bm25's ComputeLiveness []Membership.
	liveOrd [][]bool
}

type docLoc struct {
	si  int
	ord int64
}

// NewIndex builds an index over the given loaded segments (each carrying its
// Recency) and delete set, computing global stats and the liveness map. deletes
// may be nil.
func NewIndex(segments []*Segment, deletes map[any]int64) *Index {
	idx := &Index{segments: segments, deletes: deletes}
	idx.resolve()
	return idx
}

// resolve computes the live copy of each pk (highest Recency, not delete-shadowed),
// then the global doc count + average document length; each segment's AvgDocLen is
// set to the global value (§4) so BM25 length-normalizes consistently.
func (idx *Index) resolve() {
	type best struct {
		rec int64
		loc docLoc
	}
	top := make(map[any]best)
	for si, seg := range idx.segments {
		for ord := range seg.pks {
			key := normalizeKey(seg.pks[ord])
			cand := best{seg.Recency, docLoc{si, int64(ord)}}
			if cur, ok := top[key]; !ok || cand.rec > cur.rec {
				top[key] = cand
			}
		}
	}

	idx.liveLoc = make(map[any]docLoc, len(top))
	var sumDocLen int64
	for key, b := range top {
		if d, ok := idx.deletes[key]; ok && b.rec < d {
			continue // shadowed by a later delete
		}
		idx.liveLoc[key] = b.loc
		sumDocLen += int64(idx.segments[b.loc.si].docLen[b.loc.ord])
	}
	idx.globalN = int64(len(idx.liveLoc))
	if idx.globalN > 0 {
		idx.globalAvgDocLen = float64(sumDocLen) / float64(idx.globalN)
	}
	for _, seg := range idx.segments {
		seg.AvgDocLen = idx.globalAvgDocLen
	}

	// Per-segment liveness bitmap (mirrors bm25's ComputeLiveness): a fully-live
	// segment keeps liveOrd[si]==nil (the common single-base / append-only case) so it
	// costs no O(doc-count) allocation; otherwise mark each live ord true. The boolean
	// walk's livenessMembership then indexes this O(1) instead of keyOf+map per candidate.
	idx.liveOrd = make([][]bool, len(idx.segments))
	liveCount := make([]int, len(idx.segments))
	for _, loc := range idx.liveLoc {
		liveCount[loc.si]++
	}
	for si, seg := range idx.segments {
		if liveCount[si] < len(seg.pks) { // some dead ord in this segment → need the bitmap
			idx.liveOrd[si] = make([]bool, len(seg.pks))
		}
	}
	for _, loc := range idx.liveLoc {
		if b := idx.liveOrd[loc.si]; b != nil {
			b[loc.ord] = true
		}
	}
}

// isLive reports whether (si, ord) is the live copy of key (a normalizeKey value).
func (idx *Index) isLive(si int, ord int64, key any) bool {
	l, ok := idx.liveLoc[key]
	return ok && l.si == si && l.ord == ord
}

// SearchPhrase runs an NL exact-phrase query across all segments and returns the
// global top-k (score desc, ties by ascending pk key). A single term is the
// degenerate one-term phrase. Scoring uses global N + global avgDocLen, and the
// phrase's document frequency is the number of LIVE documents that match (dead
// and delete-shadowed copies are excluded), so idf is exact.
func (idx *Index) SearchPhrase(terms []string, algo ScoreAlgo, k int, filter docfilter.MembershipFilter) []Result {
	if k <= 0 || idx.globalN == 0 || len(terms) == 0 {
		return nil
	}
	// Collect the live matches across segments (one per pk — the live copy).
	type match struct {
		seg *Segment
		ord int64
		tf  int
	}
	matched := make(map[any]match)
	// idf df is the CORPUS phrase df — all live docs containing the phrase, independent
	// of the WHERE prefilter (else the same doc's score would change with an unrelated
	// WHERE clause, and NL would disagree with BOOLEAN's gs.phraseDf). Only build the
	// separate unfiltered set when a filter is present; without one, matched IS every
	// live match.
	var dfSet map[any]struct{}
	if filter != nil {
		dfSet = make(map[any]struct{})
	}
	for si, seg := range idx.segments {
		allow := mkAllow(seg, filter) // WHERE prefilter over this segment's ords (nil = none)
		for _, h := range seg.matchPhrase(terms) {
			key := normalizeKey(seg.pks[h.ord])
			if !idx.isLive(si, h.ord, key) {
				continue
			}
			if dfSet != nil {
				dfSet[key] = struct{}{}
			}
			if allowed(allow, h.ord) {
				matched[key] = match{seg, h.ord, h.tf}
			}
		}
	}
	if len(matched) == 0 {
		return nil
	}

	df := len(matched)
	if dfSet != nil {
		df = len(dfSet)
	}
	idf2 := idfSquared(idx.globalN, df) // df = live phrase-matched docs, filter-independent
	results := make([]Result, 0, len(matched))
	for _, m := range matched {
		results = append(results, Result{
			Pk:    m.seg.pks[m.ord],
			Score: m.seg.scoreTerm(algo, float64(m.tf), idf2, m.ord, idx.globalAvgDocLen),
		})
	}
	sortResults(results) // score desc, ties by ascending pk (deterministic)
	if len(results) > k {
		results = results[:k]
	}
	return results
}

// sortResults orders results by score descending, breaking ties by the pk's sortKey
// (ascending) for a deterministic, reproducible order.
func sortResults(results []Result) {
	sort.Slice(results, func(a, b int) bool {
		if results[a].Score != results[b].Score {
			return results[a].Score > results[b].Score
		}
		return sortKey(results[a].Pk) < sortKey(results[b].Pk)
	})
}

// SearchText tokenizes query with tok and runs an NL exact-phrase search across
// the index.
func (idx *Index) SearchText(query []byte, tok tokenizer.Tokenizer, algo ScoreAlgo, k int, filter docfilter.MembershipFilter) ([]Result, error) {
	terms, err := tokenizeToTerms(query, tok)
	if err != nil {
		return nil, err
	}
	return idx.SearchPhrase(terms, algo, k, filter), nil
}

// globalStats carries the corpus-global scoring inputs (N, average doc length, and
// per-term document frequency) so a boolean query scores every segment on the SAME
// scale. Without it each segment used its LOCAL N/df, so appending a CDC tail shifted
// a doc's score and could drop a globally-top-k doc at the merge. df is summed across
// segments (like bm25's gdf) and memoized per query; it is built fresh per query and
// used sequentially across segments, so the cache needs no lock.
type globalStats struct {
	n             int64
	avgDocLen     float64
	idx           *Index
	dfCache       map[string]int
	phraseDfCache map[string]int
	// phraseHitsCache memoizes matchPhrase per (phrase, segment) so a boolean phrase
	// clause's own scoring scan and phraseDf's cross-segment df scan share ONE
	// matchPhrase per segment instead of running it twice.
	phraseHitsCache map[string]map[*Segment][]docTf
}

func (idx *Index) newGlobalStats() *globalStats {
	return &globalStats{n: idx.globalN, avgDocLen: idx.globalAvgDocLen, idx: idx,
		dfCache: make(map[string]int), phraseDfCache: make(map[string]int),
		phraseHitsCache: make(map[string]map[*Segment][]docTf)}
}

// phraseHits returns seg's matchPhrase hits, memoized per (phrase, segment) so the
// scoring pass and the phraseDf pass share one scan.
func (gs *globalStats) phraseHits(seg *Segment, terms []string) []docTf {
	key := strings.Join(terms, "\x00")
	m := gs.phraseHitsCache[key]
	if m == nil {
		m = make(map[*Segment][]docTf)
		gs.phraseHitsCache[key] = m
	}
	if h, ok := m[seg]; ok {
		return h
	}
	h := seg.matchPhrase(terms)
	m[seg] = h
	return h
}

// df returns term's corpus-global document frequency (summed over segments). Dead
// copies are counted — df is an idf input where a small over-count is immaterial and
// this matches bm25's gdf.
func (gs *globalStats) df(term string) int {
	if d, ok := gs.dfCache[term]; ok {
		return d
	}
	d := 0
	for _, seg := range gs.idx.segments {
		if pl, ok := seg.lookup(term); ok {
			d += pl.df()
		}
	}
	gs.dfCache[term] = d
	return d
}

// idfFor is the term's idf² under the global corpus stats, or the segment-local stats
// when gs is nil (a direct Segment query / single-segment test, where local == global).
func (gs *globalStats) idfFor(seg *Segment, term string, pl *termPostings) float64 {
	if gs == nil {
		return idfSquared(seg.N, pl.df())
	}
	return idfSquared(gs.n, gs.df(term))
}

// phraseDf returns the corpus-global document frequency of a phrase (the number of
// docs matching the contiguous phrase, summed over segments), memoized per query.
// Dead copies are counted, matching the term df above.
func (gs *globalStats) phraseDf(terms []string) int {
	key := strings.Join(terms, "\x00")
	if d, ok := gs.phraseDfCache[key]; ok {
		return d
	}
	d := 0
	for _, seg := range gs.idx.segments {
		d += len(gs.phraseHits(seg, terms)) // shares the memoized scan with scoring
	}
	gs.phraseDfCache[key] = d
	return d
}

// phraseIdfFor is a phrase clause's idf² under the global corpus stats (global N +
// cross-segment phrase df), or the segment-local stats when gs is nil — so a phrase
// clause ranks consistently across a base + CDC tail, like the term/prefix clauses.
func (gs *globalStats) phraseIdfFor(seg *Segment, terms []string, localHits int) float64 {
	if gs == nil {
		return idfSquared(seg.N, localHits)
	}
	return idfSquared(gs.n, gs.phraseDf(terms))
}

// avgdl is the global average doc length, or the segment's own when gs is nil.
func (gs *globalStats) avgdl(seg *Segment) float64 {
	if gs == nil {
		return seg.avgDocLenOrMean()
	}
	return gs.avgDocLen
}

// SearchBoolean runs a parsed boolean-mode query across the index and returns the
// global top-k (score desc, ties by ascending pk key). Each segment is scored on the
// GLOBAL corpus stats (globalStats), and its walk admits only the LIVE copy of a pk
// (liveness ANDed into the WHERE prefilter), so a segment's top-k is k live docs on
// the global scale — the merge below is then the exact global top-k even across a
// base + CDC tail. All clause types (term, prefix, phrase, and the disjunctive WAND
// path) score on global stats — a phrase clause uses the cross-segment phrase df.
func (idx *Index) SearchBoolean(q BoolQuery, algo ScoreAlgo, k int, filter docfilter.MembershipFilter) ([]Result, error) {
	if k <= 0 || idx.globalN == 0 {
		return nil, nil
	}
	gs := idx.newGlobalStats()
	matched := make(map[any]Result)
	for si, seg := range idx.segments {
		allow := andAllow(mkAllow(seg, filter), &livenessMembership{idx: idx, si: si})
		res, err := seg.SearchBoolean(q, algo, k, allow, gs)
		if err != nil {
			return nil, err
		}
		for _, r := range res {
			matched[normalizeKey(r.Pk)] = r // liveness inside the walk → one live copy per pk
		}
	}
	if len(matched) == 0 {
		return nil, nil
	}
	results := make([]Result, 0, len(matched))
	for _, r := range matched {
		results = append(results, r)
	}
	sortResults(results)
	if len(results) > k {
		results = results[:k]
	}
	return results, nil
}

// SearchBooleanText tokenizes+parses query in boolean mode with tok, then
// evaluates it across the index — the convenience entry mirroring
// MATCH(col) AGAINST('query' IN BOOLEAN MODE) with a fixed tokenizer.
func (idx *Index) SearchBooleanText(query []byte, tok tokenizer.Tokenizer, algo ScoreAlgo, k int) ([]Result, error) {
	q, err := ParseBoolean(query, tok)
	if err != nil {
		return nil, err
	}
	return idx.SearchBoolean(q, algo, k, nil) // convenience entry: no WHERE prefilter
}

// ReconstructLiveDocs reconstructs each LIVE document as (pk, ordered terms) from
// the positional postings across all loaded segments — the input to a MERGE
// compaction that folds base + tail into a fresh dead-doc-free base WITHOUT
// re-tokenizing the source. Per-doc term order is recovered from token positions;
// docs are returned in ascending pk-key order (deterministic output).
func (idx *Index) ReconstructLiveDocs() ([]TokenizedDoc, error) {
	type posTerm struct {
		pos  int32
		term string
	}
	// Bucket postings only for LIVE (si, ord) locations.
	buckets := make(map[docLoc][]posTerm, len(idx.liveLoc))
	for _, l := range idx.liveLoc {
		buckets[l] = nil
	}
	for si, seg := range idx.segments {
		err := seg.forEachPosting(func(term string, tp *termPostings) {
			mat := tp.materializePositions() // decode this term's positions once
			docs := tp.materializeDocIDs()   // and its docIDs (block-compressed on load)
			for i, ord := range docs {
				l := docLoc{si, ord}
				if _, live := buckets[l]; !live {
					continue
				}
				for _, pos := range mat[i] {
					buckets[l] = append(buckets[l], posTerm{pos, term})
				}
			}
		})
		if err != nil {
			return nil, err
		}
	}

	// Decorate-sort-undecorate: compute each pk's sortKey ONCE (O(n)) rather than per
	// comparison (O(n log n) Sprintf/allocs), which matters at MERGE over a large index.
	type keyed struct {
		sk  string
		loc docLoc
	}
	keys := make([]keyed, 0, len(idx.liveLoc))
	for k, l := range idx.liveLoc {
		keys = append(keys, keyed{sortKey(k), l})
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].sk < keys[j].sk }) // deterministic output
	out := make([]TokenizedDoc, 0, len(keys))
	for _, kd := range keys {
		l := kd.loc
		b := buckets[l]
		sort.SliceStable(b, func(i, j int) bool { return b[i].pos < b[j].pos })
		terms := make([]string, len(b))
		for i, pt := range b {
			terms[i] = pt.term
		}
		out = append(out, TokenizedDoc{Pk: idx.segments[l.si].pks[l.ord], Terms: terms})
	}
	return out, nil
}

// NumDocs returns the number of live documents in the index.
func (idx *Index) NumDocs() int64 { return idx.globalN }

// Free releases the off-heap posting buffers of every segment the index owns.
// Called on cache eviction (Fulltext2Search.Destroy) and by single-shot loaders
// (compact) once done. Build-side segments free nothing (no-op).
func (idx *Index) Free() {
	if idx != nil {
		freeSegs(idx.segments)
	}
}

// normalizeKey converts a pk to a comparable map key ([]byte -> string) and is the
// key for every pk-keyed map (liveness, deletes, dedup). Keying by the pk VALUE (not
// its String()) makes it INJECTIVE for every comparable pk type via Go == : a
// microsecond-precision DATETIME/TIME/TIMESTAMP (whose String() truncates to whole
// seconds), a raw uuid ([16]byte), a decimal128 struct — all key by their exact value
// and can never collide, so no distinct live document is dropped. Mirrors bm25's
// normalizeKey exactly (the complete port; a %v/String() key was lossy).
func normalizeKey(pk any) any {
	if b, ok := pk.([]byte); ok {
		return string(b)
	}
	return pk
}

// sortKey is a deterministic INJECTIVE string projection of a pk, used to order
// equal-score results (and MERGE output) reproducibly — never as a map key. The
// temporal types are keyed by their underlying int64 because %v/String() truncates
// them to whole seconds, which would make two equal-score DATETIME(6) rows sharing a
// second sort nondeterministically at a LIMIT boundary; every other scalar pk has an
// injective %v (int/decimal exact, uuid canonical, date a distinct day).
func sortKey(pk any) string {
	switch v := pk.(type) {
	case []byte:
		return string(v)
	case string:
		return v
	case types.Datetime:
		return strconv.FormatInt(int64(v), 10)
	case types.Time:
		return strconv.FormatInt(int64(v), 10)
	case types.Timestamp:
		return strconv.FormatInt(int64(v), 10)
	default:
		return fmt.Sprintf("%v", v)
	}
}
