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
	deletes  map[string]int64 // pk key -> recency at/after which older copies are dead

	globalN         int64
	globalAvgDocLen float64
	// liveLoc maps a live pk key to the (segment index, doc ord) of its live copy.
	liveLoc map[string]docLoc
}

type docLoc struct {
	si  int
	ord int64
}

// NewIndex builds an index over the given loaded segments (each carrying its
// Recency) and delete set, computing global stats and the liveness map. deletes
// may be nil.
func NewIndex(segments []*Segment, deletes map[string]int64) *Index {
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
	top := make(map[string]best)
	for si, seg := range idx.segments {
		for ord := range seg.pks {
			key := keyOf(seg.pks[ord])
			cand := best{seg.Recency, docLoc{si, int64(ord)}}
			if cur, ok := top[key]; !ok || cand.rec > cur.rec {
				top[key] = cand
			}
		}
	}

	idx.liveLoc = make(map[string]docLoc, len(top))
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
}

// isLive reports whether (si, ord) is the live copy of key.
func (idx *Index) isLive(si int, ord int64, key string) bool {
	l, ok := idx.liveLoc[key]
	return ok && l.si == si && l.ord == ord
}

// SearchPhrase runs an NL exact-phrase query across all segments and returns the
// global top-k (score desc, ties by ascending pk key). A single term is the
// degenerate one-term phrase. Scoring uses global N + global avgDocLen, and the
// phrase's document frequency is the number of LIVE documents that match (dead
// and delete-shadowed copies are excluded), so idf is exact.
func (idx *Index) SearchPhrase(terms []string, algo ScoreAlgo, k int) []Result {
	if k <= 0 || idx.globalN == 0 || len(terms) == 0 {
		return nil
	}
	// Collect the live matches across segments (one per pk — the live copy).
	type match struct {
		seg *Segment
		ord int64
		tf  int
	}
	matched := make(map[string]match)
	for si, seg := range idx.segments {
		for _, h := range seg.matchPhrase(terms) {
			key := keyOf(seg.pks[h.ord])
			if idx.isLive(si, h.ord, key) {
				matched[key] = match{seg, h.ord, h.tf}
			}
		}
	}
	if len(matched) == 0 {
		return nil
	}

	idf2 := idfSquared(idx.globalN, len(matched)) // df = live matched docs (exact)
	keys := make([]string, 0, len(matched))
	for key := range matched {
		keys = append(keys, key)
	}
	sort.Strings(keys) // deterministic tiebreak

	results := make([]Result, len(keys))
	for i, key := range keys {
		m := matched[key]
		results[i] = Result{
			Pk:    m.seg.pks[m.ord],
			Score: m.seg.scoreTerm(algo, float64(m.tf), idf2, m.ord, idx.globalAvgDocLen),
		}
	}
	sort.SliceStable(results, func(a, b int) bool { return results[a].Score > results[b].Score })
	if len(results) > k {
		results = results[:k]
	}
	return results
}

// SearchText tokenizes query with tok and runs an NL exact-phrase search across
// the index.
func (idx *Index) SearchText(query []byte, tok tokenizer.Tokenizer, algo ScoreAlgo, k int) ([]Result, error) {
	terms, err := tokenizeToTerms(query, tok)
	if err != nil {
		return nil, err
	}
	return idx.SearchPhrase(terms, algo, k), nil
}

// SearchBoolean runs a parsed boolean-mode query across the index: each segment
// is searched, only the LIVE copy of a pk contributes, and the merged matches
// are returned as the global top-k (score desc, ties by ascending pk key).
//
// Scoring uses each segment's local stats. For the common single-base index that
// is exact (the one segment's stats ARE the global stats); across many segments
// the per-clause global df is an approximation — a cross-segment global-stats
// boolean pass is a follow-up (as for the delete-frame folding).
func (idx *Index) SearchBoolean(q BoolQuery, algo ScoreAlgo, k int) ([]Result, error) {
	if k <= 0 || idx.globalN == 0 {
		return nil, nil
	}
	matched := make(map[string]Result)
	for si, seg := range idx.segments {
		res, err := seg.SearchBoolean(q, algo, k)
		if err != nil {
			return nil, err
		}
		for _, r := range res {
			key := keyOf(r.Pk)
			if loc, ok := idx.liveLoc[key]; ok && loc.si == si {
				matched[key] = r
			}
		}
	}
	if len(matched) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(matched))
	for key := range matched {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	results := make([]Result, len(keys))
	for i, key := range keys {
		results[i] = matched[key]
	}
	sort.SliceStable(results, func(a, b int) bool { return results[a].Score > results[b].Score })
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
	return idx.SearchBoolean(q, algo, k)
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

	keys := make([]string, 0, len(idx.liveLoc))
	for k := range idx.liveLoc {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]TokenizedDoc, 0, len(keys))
	for _, k := range keys {
		l := idx.liveLoc[k]
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

// keyOf produces a stable string key for a primary-key value, used for
// cross-segment liveness and result dedup. Bytes/strings pass through; other
// types use their default formatting (deterministic for the scalar pk types).
func keyOf(pk any) string {
	switch v := pk.(type) {
	case []byte:
		return string(v)
	case string:
		return v
	default:
		return fmt.Sprintf("%v", v)
	}
}
