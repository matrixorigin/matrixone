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
		g, n := binary.Uvarint(data[off:])
		off += n
		prev += int64(g)
		outDocs[i] = prev
	}
	copy(outTfs[:blen], data[off:off+blen])
	return blen
}

// fillBlockPositions decodes block b's per-doc token positions into out[:blen],
// returning the block length. Build-side views the flat positions slice; loaded-side
// varint-decodes only block b's slice of posRaw (via blockPosOff) — the per-block
// analogue of fillBlock, so the phrase cursor holds ONE block's positions (O(BlockSize))
// instead of the whole list. out[i] is (re)allocated to that doc's position count.
func (p *termPostings) fillBlockPositions(b int, out [][]int32) int {
	blen := p.blockLen(b)
	if p.positions != nil { // build-side: view into the flat slice
		lo := b * BlockSize
		copy(out[:blen], p.positions[lo:lo+blen])
		return blen
	}
	data := p.posRaw[p.blockPosOff[b]:p.blockPosOff[b+1]]
	off := 0
	for i := 0; i < blen; i++ {
		if off >= len(data) {
			out[i] = nil
			continue
		}
		pc, n := binary.Uvarint(data[off:])
		if n <= 0 {
			out[i] = nil
			continue
		}
		off += n
		if pc > uint64(len(data)-off) { // corrupt guard (each gap is >= 1 byte)
			pc = uint64(len(data) - off)
		}
		doc := make([]int32, pc)
		var pp int32
		for m := uint64(0); m < pc; m++ {
			g, k := binary.Uvarint(data[off:])
			if k <= 0 {
				break
			}
			off += k
			pp += int32(g)
			doc[m] = pp
		}
		out[i] = doc
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
