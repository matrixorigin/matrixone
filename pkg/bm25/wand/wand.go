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
	"encoding/binary"
	"math"
	"os"
	"sort"

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

// termPostings is the posting list for one word-id, ordered by doc ord. It has
// two representations (mirrors fulltext2's termPostings):
//
//   - BUILD-side: docIDs/tfs are resident Go slices; df == len(docIDs). Produced by
//     the Builder, Merge, FilterLive, Split. blockLastDoc/blockMaxTf/blockMinDl are
//     derived by finalizeScoring.
//   - LOADED-side (Deserialize / mmap): docIDs/tfs are nil — the postings live
//     block-compressed in blockData (a view into the mmap/blob, one BlockSize-doc
//     block at a time: docID gaps as delta+varint from the previous block's last
//     ord, then raw tf bytes). ndoc is the authoritative df. The block-max directory
//     (blockLastDoc/blockMaxTf/blockMinDl + blockOff) is decoded RESIDENT per term on
//     demand (decodeTermEntry) — never re-derived from the compressed blocks. The
//     WAND cursor decodes only the blocks its block-max walk lands on (fillBlock).
type termPostings struct {
	docIDs []int64 // BUILD-side doc ords, ascending, len == df; nil when LOADED
	tfs    []uint8 // BUILD-side parallel capped tf; nil when LOADED

	// ndoc is the document frequency on a LOADED posting list (docIDs not expanded);
	// on a build-side list it equals len(docIDs).
	ndoc int

	// LOADED-side compressed docID/tf blocks (a view into the segment's mmap/blob)
	// plus the per-block byte offsets within blockData (len nblk+1, cumulative). nil
	// on a build-side list.
	blockData []byte
	blockOff  []int32

	// Term-level score upper-bound inputs, idf-AND-avgdl-FREE so the bound stays
	// valid under a global idf/avgdl (segments, incremental). The term UB is
	// weight·idf²·bm25Factor(maxTf, minDl, avgdl), computed at query time.
	maxTf uint8 // max tf over all postings
	minDl int32 // min doc length over all postings

	// Block-Max skip-block metadata (one entry per ceil(df/BlockSize)). Also
	// idf/avgdl-free. block UB = weight·idf²·bm25Factor(blockMaxTf, blockMinDl,
	// avgdl). Derived by finalizeScoring (build) or decodeTermEntry (loaded).
	blockLastDoc []int64 // max (last) ord in each block (ascending → last)
	blockMaxTf   []uint8 // max tf in each block
	blockMinDl   []int32 // min doc length in each block
}

// df is the document frequency (build-side authoritative len; loaded-side ndoc).
func (p *termPostings) df() int {
	if p.docIDs != nil {
		return len(p.docIDs)
	}
	return p.ndoc
}

// nblk is the number of Block-Max skip blocks (== ceil(df/BlockSize)).
func (p *termPostings) nblk() int { return len(p.blockLastDoc) }

// blockLen is the number of postings in block b (BlockSize, or the last remainder).
func (p *termPostings) blockLen(b int) int {
	n := p.df() - b*BlockSize
	if n > BlockSize {
		n = BlockSize
	}
	return n
}

// fillBlock decodes block b's docIDs and tfs into outDocs/outTfs (each cap >=
// BlockSize) and returns the block length. Build-side copies the flat slices;
// loaded-side varint-decodes the block from blockData (the mmap view): docID gaps
// accumulate from the previous block's last ord (resident blockLastDoc[b-1]), then
// the raw tf bytes follow. The WAND cursor calls this once per block it lands on;
// materializeDocIDs/Tfs call it per block to rebuild the flat arrays.
func (p *termPostings) fillBlock(b int, outDocs []int64, outTfs []uint8) int {
	blen := p.blockLen(b)
	if p.docIDs != nil { // build-side: copy from the flat arrays
		lo := b * BlockSize
		copy(outDocs[:blen], p.docIDs[lo:lo+blen])
		copy(outTfs[:blen], p.tfs[lo:lo+blen])
		return blen
	}
	data := p.blockData[p.blockOff[b]:p.blockOff[b+1]]
	var prev int64
	if b > 0 {
		prev = p.blockLastDoc[b-1]
	}
	off := 0
	for i := 0; i < blen; i++ {
		g, n := binary.Uvarint(data[off:])
		off += n
		prev += int64(g)
		outDocs[i] = prev
	}
	copy(outTfs[:blen], data[off:off+blen])
	return blen
}

// materializeDocIDs returns this term's full ascending doc ords (df entries): the
// build-side flat slice as-is, or a transient decode of every loaded block. Cold
// paths (Merge / FilterLive / Split / re-Serialize) that scan all postings call
// this ONCE; WAND ranking never does (it decodes only the blocks its walk lands on).
func (p *termPostings) materializeDocIDs() []int64 {
	if p.docIDs != nil {
		return p.docIDs
	}
	out := make([]int64, p.ndoc)
	var scratch [BlockSize]uint8
	for b := 0; b < p.nblk(); b++ {
		lo := b * BlockSize
		p.fillBlock(b, out[lo:], scratch[:])
	}
	return out
}

// materializeTfs returns this term's full capped tf bytes (df entries): the
// build-side flat slice as-is, or a transient decode of every loaded block.
func (p *termPostings) materializeTfs() []uint8 {
	if p.tfs != nil {
		return p.tfs
	}
	out := make([]uint8, p.ndoc)
	var scratch [BlockSize]int64
	for b := 0; b < p.nblk(); b++ {
		lo := b * BlockSize
		p.fillBlock(b, scratch[:], out[lo:])
	}
	return out
}

// WandModel is the loadable in-memory index (one segment).
type WandModel struct {
	Id        string
	N         int64   // number of documents (= len(pks))
	PkType    int32   // types.T of the source primary key, for output decode + membership
	AvgDocLen float64 // average doc length (derived from DocLen), for BM25

	// Recency is the segment's ordering key for liveness. For a tag=1 CdcTail delta it is
	// the frame's append position (storage chunk_id, NOT an ISCP LSN; see fulltext_wand.md
	// "single CdcTail log, chunk_id-ordered"); for a tag=0 base sub it is metadata.recency
	// (0 = full-build/oldest, K = folded). When the same pk lands in multiple segments
	// (UPDATE / reinsert / a stale base copy), only the highest-Recency copy is live (see
	// ComputeLiveness). Named distinctly from the storage table's chunk_id (a physical
	// chunk position within a blob), which is an unrelated concept.
	Recency int64

	pks      []any                   // ord -> original pk value (for output via AppendAny)
	docLen   []int32                 // ord -> document length (token count), for BM25
	terms    map[int32]*termPostings // BUILD-side word-id -> postings; nil on a LOADED model
	overflow map[string]int32        // out-of-dict term -> overflow word-id (query resolution)

	// LOADED-side term dict (set by decodeLoaded / bindWand; nil on a build-side
	// model). termOffsets maps word-id -> the BYTE OFFSET of that term's
	// self-contained directory entry in `ranking`; a loaded model does NOT expand any
	// term at load — lookupTerm decodes just the touched term's directory entry from
	// `ranking` on demand and points its blockData at `blocks` — so the resident
	// directory heap is O(the current query), not O(vocabulary). `ranking`/`blocks`
	// are views into the mmap/blob (kept alive by mmapData or GC). The build-side
	// `terms` map is left nil.
	termOffsets     map[int32]int64
	ranking, blocks []byte

	// mmapData is the shared read-only mmap of a base segment's on-disk file: the
	// ranking directory and the compressed docID/tf blocks are views into it
	// (page-cache-backed, reclaimable, shared by all concurrent queries — no copy, no
	// off-heap). mmapPath is that file (empty for the anonymous SSD file, whose inode
	// is freed by munmap). Both are released by Free() under the cache's eviction
	// write-lock. nil on a build-side or in-memory (tail) model, whose bytes are
	// GC-managed Go slices.
	mmapData []byte
	mmapPath string
}

// Free releases a loaded model's mmap (and its backing file, if linked). Safe on a
// build-side / in-memory tail model (nil mmapData → no-op) and idempotent. After
// Free the model must not be searched; the VectorIndexCache holds the write lock
// when calling Destroy. The loaded posting blocks (blockData) are views into
// mmapData, so munmap reclaims them — there is no off-heap buffer to deallocate.
func (m *WandModel) Free() {
	if m.mmapData != nil {
		_ = munmap(m.mmapData)
		m.mmapData = nil
	}
	if m.mmapPath != "" {
		_ = os.Remove(m.mmapPath)
		m.mmapPath = ""
	}
	m.ranking, m.blocks = nil, nil
	m.termOffsets = nil
	m.terms = nil
}

// lookupTerm resolves a word-id to its posting list, transparently across the two
// representations: the build-side terms map, or a lazy per-term decode of the loaded
// directory entry (decodeTermEntry). Returns (nil,false) if the word-id is absent.
func (m *WandModel) lookupTerm(id int32) (*termPostings, bool) {
	if m.termOffsets != nil { // loaded
		off, ok := m.termOffsets[id]
		if !ok {
			return nil, false
		}
		return m.decodeTermEntry(off)
	}
	tp, ok := m.terms[id]
	return tp, ok
}

// forEachTerm calls fn for every (word-id, posting-list), whether build-side (terms
// map) or loaded-side (lazy directory decode). Used by the cold full-scan paths
// (Merge / FilterLive / Split / re-Serialize). On a loaded model each tp is a
// transient decode whose blockData views the mmap.
func (m *WandModel) forEachTerm(fn func(int32, *termPostings)) {
	if m.termOffsets != nil { // loaded
		for id, off := range m.termOffsets {
			if tp, ok := m.decodeTermEntry(off); ok {
				fn(id, tp)
			}
		}
		return
	}
	for id, tp := range m.terms {
		fn(id, tp)
	}
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

// computeAvgDocLen sets AvgDocLen from docLen. Called by both finalizeScoring
// (build) and decodeLoaded (load) — each model carries its own AvgDocLen, which
// corpusStats aggregates across segments.
func (m *WandModel) computeAvgDocLen() {
	var sum int64
	for _, dl := range m.docLen {
		sum += int64(dl)
	}
	if len(m.docLen) > 0 {
		m.AvgDocLen = float64(sum) / float64(len(m.docLen))
	}
}

// finalizeScoring derives AvgDocLen and every BUILD-side term's max BM25 factor +
// per-term Block-Max skip-block stats. Called by the builder's Finish and by
// Merge/FilterLive/Split (all build-side). A LOADED model reads the block-max
// directory back from disk (decodeTermEntry) instead — it never calls this.
func (m *WandModel) finalizeScoring() {
	m.computeAvgDocLen()
	for _, tp := range m.terms {
		deriveTermStats(tp, m.docLen)
	}
}

// deriveTermStats fills one BUILD-side posting list's term-level maxTf/minDl and its
// per-block Block-Max skip metadata (blockLastDoc/blockMaxTf/blockMinDl), one entry
// per ceil(df/BlockSize), from its resident docIDs/tfs + docLen. Raw / idf-avgdl-free.
func deriveTermStats(tp *termPostings, docLen []int32) {
	df := len(tp.docIDs)
	tp.ndoc = df
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
			if dl := docLen[tp.docIDs[i]]; dl < minDl {
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

		// forEachTerm handles both a build-side input (terms map) and a LOADED input
		// (lazy directory decode) — CompactSegments feeds FilterLive'd loaded tail
		// segments here. materializeDocIDs/Tfs expand a loaded term's compressed blocks
		// transiently; a build-side term returns its resident slices.
		s.forEachTerm(func(wid int32, tp *termPostings) {
			mwid := wid
			if wid >= tokenizer.DictWordIDLimit {
				mwid = remap[wid]
			}
			mtp := m.terms[mwid]
			if mtp == nil {
				mtp = &termPostings{}
				m.terms[mwid] = mtp
			}
			docs := tp.materializeDocIDs()
			tfs := tp.materializeTfs()
			for i, ord := range docs {
				mtp.docIDs = append(mtp.docIDs, ord+base)
				mtp.tfs = append(mtp.tfs, tfs[i])
			}
		})
		base += s.N
	}

	m.N = base
	m.finalizeScoring()
	return m
}

// NumTerms returns the number of distinct word-ids in the index (build- or
// loaded-side).
func (m *WandModel) NumTerms() int {
	if m.termOffsets != nil {
		return len(m.termOffsets)
	}
	return len(m.terms)
}

// PkAt returns the original pk value for a doc ord (for output).
func (m *WandModel) PkAt(ord int64) any {
	if ord < 0 || ord >= int64(len(m.pks)) {
		return nil
	}
	return m.pks[ord]
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
