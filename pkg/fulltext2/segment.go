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
	"encoding/binary"
	"os"
	"sort"
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
	docIDs    []int64   // BUILD-side doc ords, ascending, len == df; nil on a LOADED segment
	tfs       []uint8   // BUILD-side parallel capped tf (<= MaxCappedTf); nil on a LOADED segment
	positions [][]int32 // BUILD-side per-doc positions (len == df); nil on a LOADED segment

	// ndoc is the document frequency (df). On a LOADED segment docIDs/tfs are NOT
	// expanded — they live block-compressed in blockData (a view into the mmap) —
	// so ndoc is the authoritative count. On a build-side segment it equals
	// len(docIDs) (set by deriveTermStats).
	ndoc int

	// LOADED-side docID/tf blocks: this term's postings kept COMPRESSED as
	// BlockSize-doc blocks (per block: docID gaps as delta+varint from the previous
	// block's last ord, then the block's raw tf bytes), a view into the segment's
	// mmap. docIDs are the largest resident section (~46% at load), needed in full
	// ONLY by phrase verification / MERGE / boolean — WAND ranking touches only the
	// blocks its block-max walk lands on. So they are NOT expanded at load; the WAND
	// cursor decodes one block on demand (fillBlock), and the cold paths materialize
	// transiently (materializeDocIDs/materializeTfs). blockOff[b] is block b's byte
	// offset within blockData (len nblk+1, cumulative). nil on a build-side segment.
	blockData []byte
	blockOff  []int64

	// LOADED-side positions (Deserialize): this term's positions kept COMPRESSED
	// (delta+varint, per doc: pc + position gaps), a view into the segment's
	// resident positions buffer. Positions are the largest section (~48%) and are
	// needed ONLY for phrase verification / MERGE — never for WAND ranking — so they
	// are NOT expanded into RAM at load; callers decode on demand via
	// materializePositions() (whole list) or fillBlockPositions() (one block, for the
	// phrase cursor). nil on a build-side segment.
	posRaw []byte
	// blockPosOff[b] is block b's byte offset within posRaw (len nblk+1, cumulative),
	// so a block's positions are seekable without decoding the prior blocks — the
	// position analogue of blockOff. nil on a build-side segment (positions is flat there).
	blockPosOff []int64

	// Term-level score-UB inputs (raw, scorer-agnostic).
	maxTf     uint8 // max tf over all postings
	minDocLen int32 // min doc length over all postings

	// Block-Max skip-block metadata, one entry per ceil(df/BlockSize). Computed at
	// BUILD (deriveTermStats) and STORED in the ranking directory, read back RESIDENT
	// at load (never re-derived from the compressed blocks). Raw / scorer-agnostic.
	blockLastDoc  []int64 // last (max) ord in each block
	blockMaxTf    []uint8 // max tf in each block
	blockMinDocLn []int32 // min doc length in each block
}

// df is the document frequency (number of docs containing the term).
func (p *termPostings) df() int {
	if p.docIDs != nil {
		return len(p.docIDs) // build-side (authoritative before ndoc is set)
	}
	return p.ndoc
}

// nblk is the number of Block-Max skip blocks (== ceil(df/BlockSize)).
func (p *termPostings) nblk() int { return len(p.blockLastDoc) }

// blockLen is the number of postings in block b (BlockSize, or the remainder for
// the last block).
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
		// Guard corrupt/torn block bytes that survive the CRC: a malformed docID varint
		// (n<=0) or one that over-consumes would otherwise make the tf copy below slice
		// out of range and panic the query goroutine. Return the docs decoded so far —
		// the sibling fillBlockPositions degrades the same way rather than crashing.
		if off >= len(data) {
			return i
		}
		g, n := binary.Uvarint(data[off:])
		if n <= 0 {
			return i
		}
		off += n
		prev += int64(g)
		outDocs[i] = prev
	}
	// tfs are blen raw bytes after the docID gaps; on corruption they may not all be
	// present. Copy only what the block actually holds (never past len(data)).
	if off+blen > len(data) {
		blen = len(data) - off
	}
	copy(outTfs[:blen], data[off:off+blen])
	return blen
}

// fillBlockPositions decodes block b's per-doc token positions into out[:blen],
// returning the block length. loaded-side varint-decodes only block b's slice of posRaw
// (via blockPosOff) — the per-block analogue of fillBlock, so the phrase cursor holds ONE
// block's positions (O(BlockSize)) instead of the whole list.
//
// out[i] is REUSED in place (out[i] = append(out[i][:0], …)): out is a pooled phraseBuf
// buffer (getPhraseBuf), and reallocating a fresh []int32 per doc per block both churned
// the allocator under concurrent phrase load and left the pool holding stale per-doc
// arrays. Reusing keeps each slot at its high-water capacity and makes the pool actually
// pool. The build-side branch COPIES the flat slice into the owned out[i] (rather than
// aliasing p.positions) so a buffer reused later on a loaded segment can never write back
// into build data. matchPhrase reads positions via range / sortedContainsInt32, both of
// which treat a nil and an empty slice identically, so the degenerate (no-data / corrupt)
// slots leave out[i] empty rather than nil.
func (p *termPostings) fillBlockPositions(b int, out [][]int32) int {
	blen := p.blockLen(b)
	if p.positions != nil { // build-side: copy the flat slice into the buffer's own arrays
		lo := b * BlockSize
		for i := 0; i < blen; i++ {
			out[i] = append(out[i][:0], p.positions[lo+i]...)
		}
		return blen
	}
	data := p.posRaw[p.blockPosOff[b]:p.blockPosOff[b+1]]
	off := 0
	for i := 0; i < blen; i++ {
		out[i] = out[i][:0] // reuse the pooled backing array; empty ≡ nil for the reader
		if off >= len(data) {
			continue
		}
		pc, n := binary.Uvarint(data[off:])
		if n <= 0 {
			continue
		}
		off += n
		if pc > uint64(len(data)-off) { // corrupt guard (each gap is >= 1 byte)
			pc = uint64(len(data) - off)
		}
		var pp int32
		for m := uint64(0); m < pc; m++ {
			g, k := binary.Uvarint(data[off:])
			if k <= 0 {
				break
			}
			off += k
			pp += int32(g)
			out[i] = append(out[i], pp)
		}
	}
	return blen
}

// materializeDocIDs returns this term's full ascending doc ords (df entries): the
// build-side flat slice as-is, or a transient decode of every loaded block. Cold
// paths (phrase / MERGE / boolean) that scan all postings call this ONCE; WAND
// ranking never does (it decodes only the blocks its walk lands on).
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

// materializePositions decodes this term's per-doc token positions (df entries):
// the build-side stored [][]int32 as-is, or the loaded-side compressed posRaw
// (delta+varint) transiently. Positions are NOT resident on a loaded segment, so a
// caller that needs them (phrase verification, MERGE) materializes ONCE per query
// and discards — WAND ranking never calls this. Panics on a build-side... no: a
// build-side segment returns tp.positions directly.
func (p *termPostings) materializePositions() [][]int32 {
	if p.positions != nil {
		return p.positions
	}
	df := p.df()
	out := make([][]int32, df)
	off := 0
	for i := 0; i < df; i++ {
		if off >= len(p.posRaw) {
			break // truncated/corrupt (defense-in-depth on already-checksummed data)
		}
		pc, n := binary.Uvarint(p.posRaw[off:])
		if n <= 0 {
			break
		}
		off += n
		// A doc's position count cannot exceed the remaining bytes (each gap is >= 1
		// varint byte), so a garbage pc can't make() an unbounded slice.
		if pc > uint64(len(p.posRaw)-off) {
			pc = uint64(len(p.posRaw) - off)
		}
		doc := make([]int32, pc)
		var pp int32
		for m := uint64(0); m < pc; m++ {
			g, k := binary.Uvarint(p.posRaw[off:])
			if k <= 0 {
				break
			}
			off += k
			pp += int32(g)
			doc[m] = pp
		}
		out[i] = doc
	}
	return out
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

	// pks is the BUILD-side ord->pk slice (set while building an in-memory segment).
	// On a LOADED segment it is nil: instead of materializing N boxed `any` pks (~24 B
	// each — the O(docs) resident floor after the liveLoc removal), decodeDocmap keeps
	// pkOffsets (4 B/doc, the byte offset of each pk in the docmap bytes) + pkRaw (a
	// VIEW into those bytes, mmap-backed for a base), and pk(ord) decodes on demand.
	pks       []any
	pkOffsets []int32 // loaded-side: ord -> offset of pk's length prefix in pkRaw
	pkRaw     []byte  // loaded-side: the docmap bytes (view; pks live here len-prefixed)
	docLen    []int32 // ord -> document length (token count), for BM25

	// INCLUDE columns: actual per-doc scalar values stored in the docmap (ord-aligned),
	// used for in-index prefiltering and covering projection so a covered query needs no
	// JOIN back to the base table. Encoded with the same pk codec (fixed-width dense for
	// numerics, [u32 len][content] for varchar/char). All slices are nil when the index has
	// no INCLUDE columns.
	//
	// Build-side: includeVals[ord] is the row's per-column values ([]any of len(includeTypes));
	// a nil element is SQL NULL. Loaded-side: includeVals is nil, and
	// includeOffsets[ord*nIncl+col] is the byte offset (into includeRaw, a view over the
	// docmap bytes == pkRaw) of column col's [nullFlag][u32 len][content] entry, decoded on
	// demand by includeVal(ord, col).
	// The docmap include section splits columns into a dense stride-addressed FIXED region
	// (the integer family — no per-doc offset needed, computed by stride) and an
	// offset-addressed VARLENA region (varchar/char). So a numeric-only INCLUDE index keeps
	// ZERO resident offset entries; only varlena columns cost a resident offset table.
	includeTypes []int32       // types.T of each INCLUDE column (len = nIncl, col order)
	includeVals  [][]any       // build-side: ord -> per-column values (nil element = NULL)
	includeRaw   []byte        // loaded-side: docmap bytes view (== pkRaw), mmap-backed on a base
	includeLay   includeLayout // loaded-side: derived per-column fixed/varlena addressing
	// loaded-side: byte offset of the FIXED region within includeRaw; the VARLENA offset table
	// (ord*nVarlena+varlenaIdx -> offset). includeVarOffsets is nil when all columns are fixed.
	includeFixedStart int
	includeVarOffsets []int32

	// term dict, BUILD-side representation — dictionary-free, keyed by the
	// indexed term string. `terms` accumulates postings during build and gives
	// O(1) exact lookup; `sortedTerms` is the ascending key list. On serialize
	// these feed buildTermDictFST (termdict.go), whose vellum FST is the compact
	// on-disk / loaded form used for query lookup + prefix. Kept consistent:
	// every key in `terms` appears once in `sortedTerms`.
	terms       map[string]*termPostings
	sortedTerms []string

	// term dict, LOADED-side representation — set by Deserialize, nil on a
	// build-side segment. `dict` is the vellum FST mapping term → the BYTE OFFSET of
	// that term's self-contained directory entry in `ranking`. A loaded segment does
	// NOT expand any term at load: query lookup (LookupLoaded) decodes just the touched
	// term's directory entry from `ranking` on demand and points its blocks/positions at
	// `blocks`/`positions` — so the resident directory heap is O(the current query), not
	// O(vocabulary). `ranking`/`blocks`/`positions` are views into the mmap/blob (kept
	// alive by mmapData or GC). The build-side `terms` map is left nil.
	dict                       *termDict
	ranking, blocks, positions []byte

	// mmapData is the shared read-only mmap of a base segment's on-disk file: the
	// FST, the compressed docID/tf blocks, and the compressed positions section are
	// all views into it (page-cache-backed, reclaimable, shared by all concurrent
	// queries — no copy, no off-heap). mmapPath is that file (empty for the anonymous
	// SSD file, whose inode is freed by munmap). Both are released by Free() under the
	// cache's eviction write-lock (no reader in flight). nil on a build-side or
	// in-memory (tail) segment, whose bytes are GC-managed Go slices.
	mmapData []byte
	mmapPath string
}

// numDocs is the segment's document count, valid for both a build-side segment (== len(pks))
// and a loaded segment (== N, where pks is nil and pk(ord) decodes from pkRaw).
func (s *Segment) numDocs() int { return int(s.N) }

// pk returns the original pk value at ord. Build-side segments read the resident pks
// slice; loaded segments decode on demand from pkRaw at pkOffsets[ord] (a length-prefixed
// pk), so no O(docs) []any is held resident. pkType is validated once at decodeDocmap, so
// decodePk cannot fail here for a well-formed segment.
func (s *Segment) pk(ord int64) any {
	if s.pks != nil {
		return s.pks[ord]
	}
	off := int(s.pkOffsets[ord])
	l := int(binary.LittleEndian.Uint32(s.pkRaw[off:]))
	v, _ := decodePk(s.PkType, s.pkRaw[off+4:off+4+l])
	return v
}

// includeLayout is the derived per-column addressing for the docmap include section: a dense
// stride-addressed FIXED region (the integer family) plus an offset-addressed VARLENA region
// (varchar/char). Fixed columns need NO per-doc offset — their value sits at a computed
// (ord*fixedStride + maskBytes + fixedValOff[col]) position — so only varlena columns cost a
// resident offset table. Derived once from the column types (build encode + load decode).
type includeLayout struct {
	fixedIdx    []int // per col: index among fixed cols (0..nFixed-1), or -1 if varlena
	varlenaIdx  []int // per col: index among varlena cols (0..nVarlena-1), or -1 if fixed
	fixedValOff []int // per col: byte offset of its value within a row's fixed value region (fixed cols)
	fixedWidth  []int // per col: byte width (fixed cols)
	nFixed      int
	nVarlena    int
	maskBytes   int // ceil(nFixed/8): the per-row NULL bitmask for fixed cols
	fixedStride int // maskBytes + sum(fixedWidth): bytes per doc in the fixed region
}

// computeIncludeLayout derives the fixed/varlena addressing from the INCLUDE column types.
func computeIncludeLayout(types []int32) includeLayout {
	l := includeLayout{
		fixedIdx:    make([]int, len(types)),
		varlenaIdx:  make([]int, len(types)),
		fixedValOff: make([]int, len(types)),
		fixedWidth:  make([]int, len(types)),
	}
	valOff := 0
	for c, t := range types {
		l.fixedIdx[c], l.varlenaIdx[c] = -1, -1
		if w, ok := fixedPkByteWidth(t); ok {
			l.fixedIdx[c] = l.nFixed
			l.fixedWidth[c] = w
			l.fixedValOff[c] = valOff
			valOff += w
			l.nFixed++
		} else {
			l.varlenaIdx[c] = l.nVarlena
			l.nVarlena++
		}
	}
	l.maskBytes = (l.nFixed + 7) / 8
	l.fixedStride = l.maskBytes + valOff
	return l
}

// nIncludeCols is the number of INCLUDE columns carried by this segment (0 = none).
func (s *Segment) nIncludeCols() int { return len(s.includeTypes) }

// decodeInclude returns the doc's INCLUDE column values (all columns, in index order; a nil
// element = SQL NULL), or nil when the segment has no INCLUDE columns. Carried out with a
// search Result so a covering query needs no base-table JOIN.
func (s *Segment) decodeInclude(ord int64) []any {
	n := len(s.includeTypes)
	if n == 0 {
		return nil
	}
	out := make([]any, n)
	for c := 0; c < n; c++ {
		if v, isNull, err := s.includeVal(ord, c); err == nil && !isNull {
			out[c] = v
		}
	}
	return out
}

// includeVal returns the value of INCLUDE column col at ord and whether it is SQL NULL.
// Build-side reads the resident includeVals; a loaded segment decodes on demand: a FIXED
// (integer) column from the dense stride-addressed region (no offset — computed position + a
// NULL bitmask), a VARLENA column from includeVarOffsets (a [nullFlag][u32 len][content]
// entry). Returns (nil, true, nil) when the segment has no INCLUDE columns.
func (s *Segment) includeVal(ord int64, col int) (any, bool, error) {
	nIncl := len(s.includeTypes)
	if nIncl == 0 || col < 0 || col >= nIncl {
		return nil, true, nil
	}
	if s.includeVals != nil { // build-side
		var v any
		if int(ord) < len(s.includeVals) && col < len(s.includeVals[ord]) {
			v = s.includeVals[ord][col]
		}
		return v, v == nil, nil
	}
	lay := &s.includeLay
	if fi := lay.fixedIdx[col]; fi >= 0 {
		// FIXED region: value at a stride-computed position, NULL via the per-row bitmask.
		rowBase := s.includeFixedStart + int(ord)*lay.fixedStride
		if s.includeRaw[rowBase+fi/8]&(1<<(uint(fi)&7)) != 0 {
			return nil, true, nil
		}
		valOff := rowBase + lay.maskBytes + lay.fixedValOff[col]
		v, err := decodePk(s.includeTypes[col], s.includeRaw[valOff:valOff+lay.fixedWidth[col]])
		if err != nil {
			return nil, false, err
		}
		return v, false, nil
	}
	// VARLENA region: offset-addressed [nullFlag][u32 len][content].
	off := int(s.includeVarOffsets[int(ord)*lay.nVarlena+lay.varlenaIdx[col]])
	if s.includeRaw[off] == includeNull {
		return nil, true, nil
	}
	l := int(binary.LittleEndian.Uint32(s.includeRaw[off+1:]))
	v, err := decodePk(s.includeTypes[col], s.includeRaw[off+1+4:off+1+4+l])
	if err != nil {
		return nil, false, err
	}
	return v, false, nil
}

// Free releases a loaded segment's mmap (and its backing file, if linked). Safe on
// a build-side / in-memory tail segment (nil mmapData → no-op) and idempotent. After
// Free the segment must not be queried; the VectorIndexCache holds the write lock
// when calling Destroy, and single-shot loaders (compact) Free after they finish
// reading. The loaded posting blocks (blockData) are views into mmapData, so
// munmap reclaims them — there is no off-heap buffer to deallocate.
func (s *Segment) Free() {
	if s.mmapData != nil {
		_ = munmap(s.mmapData)
		s.mmapData = nil
	}
	if s.mmapPath != "" {
		_ = os.Remove(s.mmapPath)
		s.mmapPath = ""
	}
	s.ranking, s.blocks, s.positions = nil, nil, nil
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
	// Stream the FST (every term, ascending) instead of materializing the whole
	// vocabulary as a []string — MERGE reconstruction over a large index must not add an
	// O(vocabulary) string spike on top of the reconstruction buckets.
	return s.dict.forEachTerm(func(term string) error {
		if tp, ok := s.LookupLoaded(term); ok {
			fn(term, tp)
		}
		return nil
	})
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
