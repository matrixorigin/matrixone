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
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
)

const (
	// MaxCappedTf caps stored term frequency at one byte (matches classic
	// fulltext's tf cap and bm25's MaxCappedTf) — long docs with a hugely
	// repeated term saturate rather than skewing the score.
	MaxCappedTf = 255

	// BlockSize is the doc count per block-max skip block (matches bm25's
	// BlockSize). One (lastDoc, maxTf, minDocLen) triple is derived per block at
	// load, letting WAND skip whole blocks whose score bound can't make top-k.
	BlockSize = 128
)

// termPostings is the in-memory posting list for one term, ordered by doc ord.
//
// It mirrors bm25's termPostings but carries POSITIONS (per doc, the token
// offsets of this term) — the positional payload phrase / NL adjacency and
// ngram reassembly need, which the position-free bm25 engine omits. The score
// upper-bound inputs (maxTf, minDocLen, block-max) are stored RAW and
// idf/avgdl-free so one segment serves both TF-IDF and BM25; the active scorer
// derives its max-impact bound at query time.
type termPostings struct {
	docIDs    []int64   // doc ords, ascending, len == df
	tfs       []uint8   // parallel, capped tf (<= MaxCappedTf)
	positions [][]int32 // BUILD-side per-doc positions (len == df); nil on a LOADED segment

	// LOADED-side positions (Deserialize): a flat buffer shared across the
	// segment's terms (points into the segment's off-heap bigPos), indexed by
	// per-term absolute offsets posOff (len df+1): doc i's positions are
	// posFlat[posOff[i]:posOff[i+1]]. nil on a build-side segment. Use posAt().
	posFlat []int32
	posOff  []int64

	// Term-level score-UB inputs (raw, scorer-agnostic).
	maxTf     uint8 // max tf over all postings
	minDocLen int32 // min doc length over all postings

	// Block-Max skip-block metadata, one entry per ceil(df/BlockSize), derived
	// at load. Also raw / scorer-agnostic.
	blockLastDoc  []int64 // last (max) ord in each block
	blockMaxTf    []uint8 // max tf in each block
	blockMinDocLn []int32 // min doc length in each block
}

// df is the document frequency (number of docs containing the term).
func (p *termPostings) df() int { return len(p.docIDs) }

// posAt returns doc i's ascending token positions — from the build-side
// per-doc [][]int32, or a view into the loaded-side flat off-heap buffer.
func (p *termPostings) posAt(i int) []int32 {
	if p.positions != nil {
		return p.positions[i]
	}
	return p.posFlat[p.posOff[i]:p.posOff[i+1]]
}

// Segment is one loadable in-memory fulltext v2 index unit — a tag=0 base sub or
// a tag=1 CDC tail frame (§4 of fulltext2.md). It is the positional analogue of
// bm25's WandModel: same docmap (ord → pk, docLen) and avgDocLen, but its term
// dict is keyed by the actual indexed term STRING and kept sorted so one
// structure serves both O(log n) lookup and prefix enumeration (`word*`).
type Segment struct {
	Id        string
	N         int64   // number of documents (= len(pks))
	PkType    int32   // types.T of the source primary key (output decode + membership)
	AvgDocLen float64 // average doc length; set at LOAD across all loaded segments

	// Recency orders segments for liveness when the same pk lands in several
	// (UPDATE / reinsert / a stale base copy) — only the highest-Recency copy is
	// live. Same semantics as bm25's WandModel.Recency (chunk_id for a tag=1
	// tail delta, metadata.recency for a tag=0 base).
	Recency int64

	pks    []any   // ord -> original pk value (for output)
	docLen []int32 // ord -> document length (token count), for BM25

	// term dict, BUILD-side representation — dictionary-free, keyed by the
	// indexed term string. `terms` accumulates postings during build and gives
	// O(1) exact lookup; `sortedTerms` is the ascending key list. On serialize
	// these feed buildTermDictFST (termdict.go), whose vellum FST is the compact
	// on-disk / loaded form used for query lookup + prefix. Kept consistent:
	// every key in `terms` appears once in `sortedTerms`.
	terms       map[string]*termPostings
	sortedTerms []string

	// term dict, LOADED-side representation — set by Deserialize, nil on a
	// build-side segment. `dict` is the vellum FST mapping term → ordinal (its
	// position in sorted order); `loaded` holds the posting lists indexed by that
	// ordinal. Query on a loaded segment resolves term → ordinal → posting list;
	// the build-side `terms` map is left nil. (Mirrors bm25's build-side per-term
	// slices vs loaded buffers split.)
	dict   *termDict
	loaded []*termPostings

	// deallocators frees the off-heap (C-allocated) posting buffers of a LOADED
	// segment (Deserialize keeps docIDs/tfs/positions off the Go heap so a
	// multi-GB cached index is not GC-scanned and releases RSS deterministically
	// on eviction — mirrors bm25's WandModel). A build-side segment leaves this
	// nil, so Free() is a no-op there.
	deallocators []malloc.Deallocator
}

// Free releases a loaded segment's off-heap posting buffers. Safe on a build-side
// segment (nil deallocators → no-op) and idempotent. After Free the segment must
// not be queried; the VectorIndexCache holds the write lock when calling Destroy,
// and single-shot loaders (compact) Free after they finish reading.
func (s *Segment) Free() {
	for _, d := range s.deallocators {
		d.Deallocate()
	}
	s.deallocators = nil
	s.loaded = nil
}

// freeSegs frees every segment's off-heap buffers (nil-safe on build-side segs).
func freeSegs(segs []*Segment) {
	for _, s := range segs {
		if s != nil {
			s.Free()
		}
	}
}

// NewSegment returns an empty segment with the given id and pk type. Postings
// are added by the builder (a later slice); this is the shared zero value used
// by both the build sink and the loader.
func NewSegment(id string, pkType int32) *Segment {
	return &Segment{
		Id:     id,
		PkType: pkType,
		terms:  make(map[string]*termPostings),
	}
}

// NumDocs returns the document count in this segment.
func (s *Segment) NumDocs() int64 { return s.N }

// NumTerms returns the number of distinct terms in this segment.
func (s *Segment) NumTerms() int { return len(s.sortedTerms) }

// Lookup returns the posting list for an exact term, or (nil, false) if the term
// is not in this segment. O(1).
func (s *Segment) Lookup(term string) (*termPostings, bool) {
	p, ok := s.terms[term]
	return p, ok
}

// forEachPosting calls fn for every (term, posting-list) in the segment, whether
// build-side (terms map) or loaded-side (FST dict + loaded slices). Used by MERGE
// compaction to reconstruct each doc's terms from the positional postings.
func (s *Segment) forEachPosting(fn func(term string, tp *termPostings)) error {
	if s.dict == nil {
		for term, tp := range s.terms {
			fn(term, tp)
		}
		return nil
	}
	terms, err := s.dict.prefixTerms("") // every term, ascending
	if err != nil {
		return err
	}
	for _, term := range terms {
		if tp, ok := s.LookupLoaded(term); ok {
			fn(term, tp)
		}
	}
	return nil
}

// PrefixRange returns the sorted terms of this segment that start with prefix,
// in ascending order — the enumeration the `word*` boolean operator expands into
// a disjunctive slot. An empty prefix returns all terms. O(log n) to locate the
// range start, then linear in the number of matches.
//
// It relies solely on the sorted key list, so it works uniformly for gojieba
// words and ngram bigrams (both live in the same dictionary-free term dict).
func (s *Segment) PrefixRange(prefix string) []string {
	if prefix == "" {
		return s.sortedTerms
	}
	// First index whose term >= prefix.
	lo := sort.SearchStrings(s.sortedTerms, prefix)
	hi := lo
	for hi < len(s.sortedTerms) && hasPrefix(s.sortedTerms[hi], prefix) {
		hi++
	}
	return s.sortedTerms[lo:hi]
}

// hasPrefix reports whether s begins with prefix. (Local rather than
// strings.HasPrefix to keep the hot prefix-scan allocation-free and explicit.)
func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

// setTerms installs the term dict from a built/loaded map and (re)derives the
// sorted key list. Used by the builder and the loader; kept here so the
// terms/sortedTerms consistency invariant lives in one place.
func (s *Segment) setTerms(terms map[string]*termPostings) {
	s.terms = terms
	s.sortedTerms = make([]string, 0, len(terms))
	for t := range terms {
		s.sortedTerms = append(s.sortedTerms, t)
	}
	sort.Strings(s.sortedTerms)
}
