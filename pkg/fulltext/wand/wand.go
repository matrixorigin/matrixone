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

// Package wand implements an in-memory, doc-ordered, skippable posting
// structure answering disjunctive (OR) top-K fulltext queries with the WAND /
// Block-Max WAND family, instead of materializing the whole match set and
// feeding it through a SQL ORDER BY ... LIMIT sort. It backs the `retrieval`
// fulltext parser / IN RETRIEVAL MODE.
//
// Internals are pure integers for speed/compactness:
//   - doc id  -> dense int64 "ord" (map[any]int64 dictionary, like fulltext.go's
//     normalizeDocID; any PK type supported, []byte normalized to string keys).
//   - word    -> int32 word-id: jieba dictionary words use their global line-id
//     (tokenizer.WordID); out-of-dict tokens get per-index overflow ids
//     (>= tokenizer.DictWordIDLimit).
//
// Scoring is MatrixOne's default BM25: weight * idf^2 * bm25Factor(tf, dl, avgdl)
// with idf = log10(N/df), matching fulltext.go ALGO_BM25.
//
// The serialized form is a tar archive (see serialize.go) with members:
// docmap (pkType + ord->pk), termdict (overflow word->id), wand (postings).
package wand

import (
	"math"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
)

const (
	// MaxCappedTf mirrors fulltext.cappedTfExpr (cap tf at 255 so it fits a
	// uint8). The builder accumulates occurrence counts and caps here.
	MaxCappedTf = 255

	// BlockSize is the number of postings per Block-Max skip block.
	BlockSize = 128

	// BM25 parameters — match fulltext.BM25_K1 / BM25_B (the default score).
	bm25K1 = 1.5
	bm25B  = 0.75
)

// bm25Factor is the BM25 tf component: tf·(k1+1)/(tf + k1·(1-b+b·dl/avgdl)).
// The full per-term contribution is weight·idf²·bm25Factor (MatrixOne's BM25).
func bm25Factor(tf float64, dl int32, avgDocLen float64) float64 {
	norm := 1.0
	if avgDocLen > 0 {
		norm = 1.0 - bm25B + bm25B*float64(dl)/avgDocLen
	}
	return tf * (bm25K1 + 1) / (tf + bm25K1*norm)
}

// termPostings is the in-memory posting list for one word-id, ordered by doc ord.
type termPostings struct {
	docIDs []int64 // doc ords, ascending, len == df
	tfs    []uint8 // parallel, capped tf

	// Term-level score upper-bound inputs, idf-AND-avgdl-FREE so the bound stays
	// valid under a global idf/avgdl (segments, incremental). The term UB is
	// weight·idf²·bm25Factor(maxTf, minDl, avgdl), computed at query time.
	maxTf uint8 // max tf over all postings
	minDl int32 // min doc length over all postings

	// Block-Max skip-block metadata (one entry per ceil(df/BlockSize)), derived
	// at load. Also idf/avgdl-free. block UB = weight·idf²·bm25Factor(blockMaxTf,
	// blockMinDl, avgdl).
	blockLastDoc []int64 // max (last) ord in each block (ascending → last)
	blockMaxTf   []uint8 // max tf in each block
	blockMinDl   []int32 // min doc length in each block
}

// WandModel is the loadable in-memory index (one segment).
type WandModel struct {
	Id        string
	N         int64   // number of documents (= len(pks))
	PkType    int32   // types.T of the source primary key, for output decode + membership
	AvgDocLen float64 // average doc length (derived from DocLen), for BM25

	// ChunkId is the segment's append position in the single tag=1 CdcTail log —
	// the recency key for CDC delta segments, assigned at load from the frame's
	// chunk_id (NOT an ISCP LSN; see fulltext_wand.md "single CdcTail log,
	// chunk_id-ordered"). When the same pk lands in multiple segments (UPDATE /
	// reinsert), only the highest-ChunkId copy is live (see ComputeLiveness). 0
	// for the tag=0 base or for disjoint FinishSegments partitions (whose pks
	// never collide), where dedup is a no-op.
	ChunkId int64

	pks      []any                   // ord -> original pk value (for output via AppendAny)
	docLen   []int32                 // ord -> document length (token count), for BM25
	terms    map[int32]*termPostings // word-id -> postings (slices into bigOrds/bigTfs when C-loaded)
	overflow map[string]int32        // out-of-dict term -> overflow word-id (query resolution)

	// When loaded from storage the postings live OFF the Go heap (C allocator):
	// all term doc-ords/tfs are concatenated into these two buffers and each
	// termPostings slices into them. Freed via deallocators on cache eviction.
	// Build-side models leave these nil (per-term Go-heap slices) — Free is then
	// a no-op.
	bigOrds      []int64
	bigTfs       []uint8
	deallocators []malloc.Deallocator
}

// Free releases the off-heap (C-allocated) postings buffers. Safe to call on a
// build-side model (no deallocators → no-op). After Free the model must not be
// searched; the VectorIndexCache holds the write lock when calling Destroy.
func (m *WandModel) Free() {
	for _, d := range m.deallocators {
		d.Deallocate()
	}
	m.deallocators = nil
	m.bigOrds = nil
	m.bigTfs = nil
	m.terms = nil
}

// NewWandModel returns an empty model.
func NewWandModel(id string, pkType int32) *WandModel {
	return &WandModel{
		Id:       id,
		PkType:   pkType,
		terms:    make(map[int32]*termPostings),
		overflow: make(map[string]int32),
	}
}

// finalizeScoring derives AvgDocLen, each term's max BM25 factor, and the
// per-term Block-Max skip-block stats. Called by the builder's Finish and after
// Deserialize (both have docLen + sorted postings).
func (m *WandModel) finalizeScoring() {
	var sum int64
	for _, dl := range m.docLen {
		sum += int64(dl)
	}
	if len(m.docLen) > 0 {
		m.AvgDocLen = float64(sum) / float64(len(m.docLen))
	}
	for _, tp := range m.terms {
		df := len(tp.docIDs)
		nblk := (df + BlockSize - 1) / BlockSize
		tp.blockLastDoc = make([]int64, nblk)
		tp.blockMaxTf = make([]uint8, nblk)
		tp.blockMinDl = make([]int32, nblk)
		var termMaxTf uint8
		termMinDl := int32(math.MaxInt32)
		for b := 0; b < nblk; b++ {
			lo := b * BlockSize
			hi := lo + BlockSize
			if hi > df {
				hi = df
			}
			var maxTf uint8
			minDl := int32(math.MaxInt32)
			for i := lo; i < hi; i++ {
				if tp.tfs[i] > maxTf {
					maxTf = tp.tfs[i]
				}
				if dl := m.docLen[tp.docIDs[i]]; dl < minDl {
					minDl = dl
				}
			}
			tp.blockLastDoc[b] = tp.docIDs[hi-1] // ascending → last is max
			tp.blockMaxTf[b] = maxTf
			tp.blockMinDl[b] = minDl
			if maxTf > termMaxTf {
				termMaxTf = maxTf
			}
			if minDl < termMinDl {
				termMinDl = minDl
			}
		}
		tp.maxTf = termMaxTf
		tp.minDl = termMinDl
	}
}

// Merge combines several index segments (disjoint document sets) into one
// segment — the compaction primitive for incremental indexing. Segment i's docs
// are appended after the previous segments (ords re-based), so each term's
// concatenated postings stay globally sorted. Overflow word-ids are reconciled
// by word into a single dictionary (dictionary word-ids < DictWordIDLimit are
// global and unchanged). The result is finalized (block/term stats + avgdl) and
// self-contained. Callers must pass segments with disjoint pk sets.
func Merge(id string, segs ...*WandModel) *WandModel {
	m := NewWandModel(id, 0)
	if len(segs) > 0 {
		m.PkType = segs[0].PkType
	}
	var nextOverflow int32 // next free per-corpus overflow offset
	var base int64         // ord offset for the current segment

	for _, s := range segs {
		// Reconcile this segment's overflow ids into the merged dictionary.
		var remap map[int32]int32
		if len(s.overflow) > 0 {
			remap = make(map[int32]int32, len(s.overflow))
			for word, sid := range s.overflow {
				mid, ok := m.overflow[word]
				if !ok {
					mid = tokenizer.DictWordIDLimit + nextOverflow
					nextOverflow++
					m.overflow[word] = mid
				}
				remap[sid] = mid
			}
		}

		m.pks = append(m.pks, s.pks...)
		m.docLen = append(m.docLen, s.docLen...)

		for wid, tp := range s.terms {
			mwid := wid
			if wid >= tokenizer.DictWordIDLimit {
				mwid = remap[wid]
			}
			mtp := m.terms[mwid]
			if mtp == nil {
				mtp = &termPostings{}
				m.terms[mwid] = mtp
			}
			for i, ord := range tp.docIDs {
				mtp.docIDs = append(mtp.docIDs, ord+base)
				mtp.tfs = append(mtp.tfs, tp.tfs[i])
			}
		}
		base += s.N
	}

	m.N = base
	m.finalizeScoring()
	return m
}

// NumTerms returns the number of distinct word-ids in the index.
func (m *WandModel) NumTerms() int { return len(m.terms) }

// PkAt returns the original pk value for a doc ord (for output).
func (m *WandModel) PkAt(ord int64) any {
	if ord < 0 || ord >= int64(len(m.pks)) {
		return nil
	}
	return m.pks[ord]
}

// idfSq returns idf(df)^2 = (log10(N/df))^2.
func (m *WandModel) idfSq(df int) float64 {
	if df <= 0 || m.N <= 0 {
		return 0
	}
	idf := log10(float64(m.N) / float64(df))
	return idf * idf
}

// resolveWordID maps a query/build word to its word-id. ok is false when the
// word is neither a dictionary word nor (for queries) a known overflow term.
func (m *WandModel) resolveWordID(word string) (int32, bool, error) {
	id, ok, err := tokenizer.WordID(word)
	if err != nil {
		return 0, false, err
	}
	if ok {
		return id, true, nil
	}
	oid, ok := m.overflow[word]
	return oid, ok, nil
}

// ---------------------------------------------------------------------------
// Build
// ---------------------------------------------------------------------------

// Builder accumulates postings — one Add per (word, doc) occurrence, in any
// order — and produces a WandModel. tf per (word, doc) is the occurrence count
// (capped). doc ords and overflow word-ids are assigned on first sight.
type Builder struct {
	model        *WandModel
	ordMap       map[any]int64           // normalized pk -> ord
	overflowNext int32                   // next overflow word-id offset
	posOf        map[int32]map[int64]int // word-id -> ord -> index in termPostings
}

// NewBuilder creates a Builder for an index id and source pk type (types.T).
func NewBuilder(id string, pkType int32) *Builder {
	return &Builder{
		model:  NewWandModel(id, pkType),
		ordMap: make(map[any]int64),
		posOf:  make(map[int32]map[int64]int),
	}
}

// normalizeKey converts a pk to a comparable map key ([]byte -> string), like
// fulltext.go's normalizeDocID.
func normalizeKey(pk any) any {
	if b, ok := pk.([]byte); ok {
		return string(b)
	}
	return pk
}

// copyPk returns a value safe to retain ([]byte is copied; the source buffer may
// be reused by the caller).
func copyPk(pk any) any {
	if b, ok := pk.([]byte); ok {
		c := make([]byte, len(b))
		copy(c, b)
		return c
	}
	return pk
}

// docOrd returns the dense ord for a pk, assigning one on first sight.
func (b *Builder) docOrd(pk any) int64 {
	key := normalizeKey(pk)
	if o, ok := b.ordMap[key]; ok {
		return o
	}
	o := int64(len(b.model.pks))
	b.ordMap[key] = o
	b.model.pks = append(b.model.pks, copyPk(pk))
	b.model.docLen = append(b.model.docLen, 0)
	return o
}

// wordID returns the word-id for a build-time word, assigning an overflow id for
// out-of-dictionary tokens.
func (b *Builder) wordID(word string) (int32, error) {
	id, ok, err := tokenizer.WordID(word)
	if err != nil {
		return 0, err
	}
	if ok {
		return id, nil
	}
	if oid, ok := b.model.overflow[word]; ok {
		return oid, nil
	}
	oid := tokenizer.DictWordIDLimit + b.overflowNext
	b.overflowNext++
	b.model.overflow[word] = oid
	return oid, nil
}

// Add records one (word, doc) occurrence (any order). tf is accumulated per
// (word-id, ord), capped at MaxCappedTf.
func (b *Builder) Add(word string, pk any) error {
	if word == "" {
		return moerr.NewInternalErrorNoCtx("wand builder: empty word")
	}
	id, err := b.wordID(word)
	if err != nil {
		return err
	}
	ord := b.docOrd(pk)
	b.model.docLen[ord]++ // one token occurrence contributes to this doc's length

	tp := b.model.terms[id]
	if tp == nil {
		tp = &termPostings{}
		b.model.terms[id] = tp
		b.posOf[id] = make(map[int64]int)
	}
	if pos, dup := b.posOf[id][ord]; dup {
		if tp.tfs[pos] < MaxCappedTf {
			tp.tfs[pos]++
		}
	} else {
		b.posOf[id][ord] = len(tp.docIDs)
		tp.docIDs = append(tp.docIDs, ord)
		tp.tfs = append(tp.tfs, 1)
	}
	return nil
}

// Finish produces a single-segment index (no capacity limit).
func (b *Builder) Finish() *WandModel {
	return b.FinishSegments(0)[0]
}

// FinishSegments finalizes the build into one or more index segments, each
// holding at most `capacity` documents (by doc-ord range). capacity <= 0 means
// no limit → a single segment. Each segment is self-contained (local 0-based
// ords, its own pks/docLen/postings) and scored corpus-globally at query time by
// SearchSegments. Mirrors HNSW's multi-mini-index rollover.
func (b *Builder) FinishSegments(capacity int64) []*WandModel {
	full := b.model
	for _, tp := range full.terms {
		sortPostings(tp) // global ascending order, so range-splits are contiguous
	}
	n := int64(len(full.pks))

	if capacity <= 0 || n <= capacity {
		full.N = n
		full.finalizeScoring()
		return []*WandModel{full}
	}

	nseg := int((n + capacity - 1) / capacity)
	segs := make([]*WandModel, nseg)
	for s := 0; s < nseg; s++ {
		lo := int64(s) * capacity
		hi := lo + capacity
		if hi > n {
			hi = n
		}
		seg := NewWandModel(full.Id, full.PkType)
		seg.pks = full.pks[lo:hi]       // build-side view; serialized independently
		seg.docLen = full.docLen[lo:hi] // local ord i == global ord lo+i
		seg.overflow = full.overflow    // identical dict across segments
		seg.N = hi - lo
		segs[s] = seg
	}

	// Partition each term's (globally-sorted) postings into segment ranges,
	// remapping global ords to per-segment local ords.
	for wid, tp := range full.terms {
		i, df := 0, len(tp.docIDs)
		for s := 0; s < nseg && i < df; s++ {
			lo := int64(s) * capacity
			hi := lo + capacity
			start := i
			for i < df && tp.docIDs[i] < hi {
				i++
			}
			if i == start {
				continue
			}
			stp := &termPostings{
				docIDs: make([]int64, i-start),
				tfs:    append([]uint8(nil), tp.tfs[start:i]...),
			}
			for j := start; j < i; j++ {
				stp.docIDs[j-start] = tp.docIDs[j] - lo // local ord
			}
			segs[s].terms[wid] = stp
		}
	}

	for _, seg := range segs {
		seg.finalizeScoring()
	}
	return segs
}

func sortPostings(tp *termPostings) {
	if sort.SliceIsSorted(tp.docIDs, func(i, j int) bool { return tp.docIDs[i] < tp.docIDs[j] }) {
		return
	}
	idx := make([]int, len(tp.docIDs))
	for i := range idx {
		idx[i] = i
	}
	sort.Slice(idx, func(i, j int) bool { return tp.docIDs[idx[i]] < tp.docIDs[idx[j]] })
	docs := make([]int64, len(idx))
	tfs := make([]uint8, len(idx))
	for i, j := range idx {
		docs[i] = tp.docIDs[j]
		tfs[i] = tp.tfs[j]
	}
	tp.docIDs = docs
	tp.tfs = tfs
}
