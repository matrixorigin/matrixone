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
	"archive/tar"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// Tar member names — one archive per segment (same multi-member shape as bm25).
const (
	memberDocmap    = "docmap"    // pkType + N + ord->pk + ord->docLen
	memberTermDict  = "termdict"  // the vellum FST: term -> ordinal
	memberPostings  = "postings"  // ranking DIRECTORY: per-term df, per-block max/skip meta (resident)
	memberBlocks    = "blocks"    // per-term docID/tf blocks (delta+varint; mmap, block-decoded on demand)
	memberPositions = "positions" // per-term compressed positions (phrase-only, lazy)
)

// Checksum returns the CRC32 (IEEE) of the serialized bytes (matches bm25's
// Checksum so the shared storage/CDC framing verifies both engines uniformly).
func Checksum(b []byte) uint32 { return crc32.ChecksumIEEE(b) }

// Serialize encodes the (build-side) segment into a tar archive of three
// members: docmap, the FST term dict, and the positional postings. Terms are
// written in sorted order and the FST maps each term to its ordinal (index into
// the postings member), so load resolves term -> ordinal -> posting list.
func (s *Segment) Serialize() ([]byte, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	docmap, err := s.encodeDocmap()
	if err != nil {
		return nil, err
	}
	if err = writeMember(tw, memberDocmap, docmap); err != nil {
		return nil, err
	}

	fst, ranking, blocks, positions, err := s.encodeTermsAndPostings()
	if err != nil {
		return nil, err
	}
	if err = writeMember(tw, memberTermDict, fst); err != nil {
		return nil, err
	}
	if err = writeMember(tw, memberPostings, ranking); err != nil {
		return nil, err
	}
	if err = writeMember(tw, memberBlocks, blocks); err != nil {
		return nil, err
	}
	if err = writeMember(tw, memberPositions, positions); err != nil {
		return nil, err
	}

	if err = tw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Deserialize parses a tar archive produced by Serialize into a loaded, queryable
// segment (dict + loaded[] postings, NOT the build-side terms map). id is the
// segment id (carried from storage, not stored in the archive). AvgDocLen is left
// unset — it is a cross-segment aggregate computed when all segments are loaded
// (§4), not a per-segment value.
func Deserialize(id string, r io.Reader) (*Segment, error) {
	// In-memory path (tail deltas, tests): read the whole archive to a Go slice and
	// point the FST + positions at it (no mmap). Bases use the mmap path in storage.
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	s := &Segment{Id: id}
	if err := s.decodeSegment(data); err != nil {
		return nil, err
	}
	return s, nil
}

// sliceMembers returns the FST / docmap / ranking / positions members as SLICES
// into data (zero-copy) by walking the tar and tracking the reader offset, so the
// same code serves an in-memory blob or an mmap'd file.
func sliceMembers(data []byte) (docmap, fst, ranking, blocks, positions []byte, err error) {
	br := bytes.NewReader(data)
	tr := tar.NewReader(br)
	for {
		h, e := tr.Next()
		if e == io.EOF {
			break
		}
		if e != nil {
			return nil, nil, nil, nil, nil, e
		}
		off := int64(len(data)) - int64(br.Len()) // tar positions br at the content
		if off < 0 || off+h.Size > int64(len(data)) {
			return nil, nil, nil, nil, nil, moerr.NewInternalErrorNoCtx("fulltext2: member out of range")
		}
		seg := data[off : off+h.Size]
		switch h.Name {
		case memberDocmap:
			docmap = seg
		case memberTermDict:
			fst = seg
		case memberPostings:
			ranking = seg
		case memberBlocks:
			blocks = seg
		case memberPositions:
			positions = seg
		}
	}
	return docmap, fst, ranking, blocks, positions, nil
}

// decodeSegment builds the loaded segment over data (an in-memory blob or an mmap):
// the ranking DIRECTORY (per-block skip/max meta) expands RESIDENT, while the FST,
// the docID/tf blocks, and the compressed positions stay as views into data (decoded
// on demand). Callers keep data alive (mmapData for a base, GC for a tail).
func (s *Segment) decodeSegment(data []byte) error {
	docmap, fst, ranking, blocks, positions, err := sliceMembers(data)
	if err != nil {
		return err
	}
	if err := s.decodeDocmap(docmap); err != nil {
		return err
	}
	if err := s.decodePostings(ranking, blocks, positions); err != nil {
		return err
	}
	dict, err := loadTermDict(fst)
	if err != nil {
		return err
	}
	s.dict = dict
	// s.N is set by decodeDocmap (loaded segments have pks==nil, so len(s.pks) is 0).
	return nil
}

func writeMember(tw *tar.Writer, name string, data []byte) error {
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(data)), Typeflag: tar.TypeReg}); err != nil {
		return err
	}
	_, err := tw.Write(data)
	return err
}

// leBuf appends little-endian scalars without the per-call heap allocation
// binary.Write incurs (its `data any` boxes every scalar) — the bytes are
// identical to binary.Write(LittleEndian, ...). Mirrors bm25's leBuf.
type leBuf struct {
	b   bytes.Buffer
	tmp [binary.MaxVarintLen64]byte
}

func (w *leBuf) u32(v uint32) { binary.LittleEndian.PutUint32(w.tmp[:4], v); w.b.Write(w.tmp[:4]) }
func (w *leBuf) u64(v uint64) { binary.LittleEndian.PutUint64(w.tmp[:8], v); w.b.Write(w.tmp[:8]) }
func (w *leBuf) i32(v int32)  { w.u32(uint32(v)) }
func (w *leBuf) i64(v int64)  { w.u64(uint64(v)) }

// uvarint appends v as a LEB128 varint (1–10 bytes). Used for the delta-encoded
// posting streams (docID gaps, position gaps) where values are small.
func (w *leBuf) uvarint(v uint64) { n := binary.PutUvarint(w.tmp[:], v); w.b.Write(w.tmp[:n]) }

// ---- docmap: pkType + N + ord->pk + ord->docLen [+ INCLUDE section] ----

const (
	// docmapIncludeTag marks an optional INCLUDE-values section appended after docLen.
	// Its presence (a byte remaining after docLen) is how a decoder tells an INCLUDE index
	// from a plain one — fulltext2 is new, so there is no old format to stay compatible with;
	// a non-INCLUDE docmap is byte-identical to before. The tag guards against a future
	// trailing section being misread as INCLUDE values.
	docmapIncludeTag byte = 0x49 // 'I'
	includeNotNull   byte = 0    // per-value flag
	includeNull      byte = 1
	// maxIncludeCols bounds the decoded column count so a corrupt header can't allocate an
	// enormous offset table. Well above the DDL-time practical limit.
	maxIncludeCols = 64
)

func (s *Segment) encodeDocmap() ([]byte, error) {
	var w leBuf
	w.i32(s.PkType)
	w.i64(int64(len(s.pks)))
	for _, pk := range s.pks {
		if err := w.encodePkLen(s.PkType, pk); err != nil {
			return nil, err
		}
	}
	for _, dl := range s.docLen {
		w.i32(dl)
	}
	// INCLUDE section (only when the index has INCLUDE columns): tag, nCols, per-col type,
	// then a dense stride-addressed FIXED region (integer cols: per-doc [NULL bitmask][values])
	// followed by an offset-addressed VARLENA region (varchar/char cols: per-doc per-col
	// [nullFlag][u32 len][content]). Fixed cols carry no per-doc offset (stride-computed), so a
	// numeric-only INCLUDE index needs no resident offset table.
	if nIncl := len(s.includeTypes); nIncl > 0 {
		if len(s.includeVals) != len(s.pks) {
			return nil, moerr.NewInternalErrorNoCtxf(
				"fulltext2: includeVals len %d != docs %d", len(s.includeVals), len(s.pks))
		}
		w.b.WriteByte(docmapIncludeTag)
		w.u32(uint32(nIncl))
		for _, t := range s.includeTypes {
			w.i32(t)
		}
		lay := computeIncludeLayout(s.includeTypes)
		valRegion := lay.fixedStride - lay.maskBytes
		// FIXED region.
		for ord := range s.pks {
			row := s.includeVals[ord]
			mask := make([]byte, lay.maskBytes)
			vals := make([]byte, valRegion)
			for c, t := range s.includeTypes {
				fi := lay.fixedIdx[c]
				if fi < 0 {
					continue
				}
				var v any
				if c < len(row) {
					v = row[c]
				}
				if v == nil {
					mask[fi/8] |= 1 << (uint(fi) & 7)
					continue // value bytes stay zero
				}
				vb, err := encodePk(t, v)
				if err != nil {
					return nil, err
				}
				if len(vb) != lay.fixedWidth[c] {
					return nil, moerr.NewInternalErrorNoCtxf("fulltext2: include fixed col %d width %d != %d", c, len(vb), lay.fixedWidth[c])
				}
				copy(vals[lay.fixedValOff[c]:], vb)
			}
			w.b.Write(mask)
			w.b.Write(vals)
		}
		// VARLENA region.
		if lay.nVarlena > 0 {
			for ord := range s.pks {
				row := s.includeVals[ord]
				for c, t := range s.includeTypes {
					if lay.varlenaIdx[c] < 0 {
						continue
					}
					var v any
					if c < len(row) {
						v = row[c]
					}
					if v == nil {
						w.b.WriteByte(includeNull)
						continue
					}
					w.b.WriteByte(includeNotNull)
					if err := w.encodePkLen(t, v); err != nil {
						return nil, err
					}
				}
			}
		}
	}
	return w.b.Bytes(), nil
}

func (s *Segment) decodeDocmap(data []byte) error {
	if len(data) < 12 {
		return moerr.NewInternalErrorNoCtx("fulltext2: docmap too short")
	}
	s.PkType = int32(binary.LittleEndian.Uint32(data[0:4]))
	n := int64(binary.LittleEndian.Uint64(data[4:12]))
	if n < 0 {
		return moerr.NewInternalErrorNoCtx("fulltext2: docmap negative N")
	}
	s.N = n
	// Build a pk offset table into the (view) docmap bytes rather than materializing N
	// boxed `any` pks — pk(ord) decodes on demand from pkRaw. pkOffsets[i] is the byte
	// offset of pk i's u32 length prefix within data; pkRaw retains the view so the pk
	// bytes stay mmap-backed (base) / GC-alive (tail). This is the O(docs) resident-heap
	// reduction: 4 B/doc (offset) instead of ~24 B/doc (interface + boxed value).
	offs := make([]int32, n)
	pos := 12
	for i := int64(0); i < n; i++ {
		if pos+4 > len(data) {
			return moerr.NewInternalErrorNoCtx("fulltext2: docmap truncated pk length")
		}
		if pos > math.MaxInt32 {
			// pkOffsets is []int32; a >2GB docmap (e.g. huge blob pks) would wrap the offset
			// negative and panic in pk(ord). Fail loudly instead. Bounded by max_index_capacity
			// docs, so this only trips with very large per-pk payloads.
			return moerr.NewInternalErrorNoCtx("fulltext2: docmap pk section exceeds 2GB")
		}
		offs[i] = int32(pos)
		l := int(binary.LittleEndian.Uint32(data[pos:]))
		// For a fixed-width pk type the stored length MUST equal the type width; a
		// corrupt smaller length would pass the pos bound below but then make pk(ord)
		// read past its slice (LittleEndian.Uint64 on <8 bytes panics). Validate here at
		// load so corruption fails cleanly instead of crashing a query goroutine.
		if w, fixed := fixedPkByteWidth(s.PkType); fixed && l != w {
			return moerr.NewInternalErrorNoCtxf("fulltext2: docmap pk %d width %d != expected %d", i, l, w)
		}
		pos += 4 + l
		if pos < 0 || pos > len(data) {
			return moerr.NewInternalErrorNoCtx("fulltext2: docmap truncated pk data")
		}
	}
	// Validate pkType ONCE (decodePk rejects unsupported types) so pk(ord) can trust it;
	// per-pk width was already checked in the loop above for fixed-width types.
	if n > 0 {
		off := int(offs[0])
		l := int(binary.LittleEndian.Uint32(data[off:]))
		if _, err := decodePk(s.PkType, data[off+4:off+4+l]); err != nil {
			return err
		}
	}
	s.pks = nil
	s.pkOffsets = offs
	s.pkRaw = data // pkOffsets are absolute offsets into data

	// docLen: N contiguous little-endian int32 after the pk section.
	if pos+int(n)*4 > len(data) {
		return moerr.NewInternalErrorNoCtx("fulltext2: docmap truncated docLen")
	}
	dl := make([]int32, n)
	for i := int64(0); i < n; i++ {
		dl[i] = int32(binary.LittleEndian.Uint32(data[pos+int(i)*4:]))
	}
	s.docLen = dl
	pos += int(n) * 4

	// Optional INCLUDE section. Absent ⇒ nothing after docLen (plain index).
	if pos < len(data) {
		if err := s.decodeIncludeSection(data, pos); err != nil {
			return err
		}
	}
	return nil
}

// decodeIncludeSection parses the trailing INCLUDE-values section starting at pos: a tag, the
// column count + types, then the dense FIXED region (skipped by stride — no offsets) and the
// VARLENA region (for which it builds an (ord*nVarlena+varlenaIdx)->offset table into `data`,
// a view; mmap-backed for a base). Fixed values are read directly by stride at query time; no
// value is materialized at load, and a numeric-only INCLUDE index builds NO offset table.
func (s *Segment) decodeIncludeSection(data []byte, pos int) error {
	if data[pos] != docmapIncludeTag {
		return moerr.NewInternalErrorNoCtx("fulltext2: docmap unknown trailing section")
	}
	pos++
	if pos+4 > len(data) {
		return moerr.NewInternalErrorNoCtx("fulltext2: docmap truncated include header")
	}
	nIncl := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4
	if nIncl <= 0 || nIncl > maxIncludeCols {
		return moerr.NewInternalErrorNoCtxf("fulltext2: docmap bad include col count %d", nIncl)
	}
	itypes := make([]int32, nIncl)
	for c := 0; c < nIncl; c++ {
		if pos+4 > len(data) {
			return moerr.NewInternalErrorNoCtx("fulltext2: docmap truncated include types")
		}
		itypes[c] = int32(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
	}
	lay := computeIncludeLayout(itypes)

	// FIXED region: N rows × fixedStride bytes, read directly by stride at query time.
	fixedStart := pos
	fixedSize := int(s.N) * lay.fixedStride
	if fixedSize < 0 || fixedStart+fixedSize > len(data) {
		return moerr.NewInternalErrorNoCtx("fulltext2: docmap truncated include fixed region")
	}
	pos += fixedSize

	// VARLENA region: build the offset table (only varlena cols cost resident offsets).
	var varOffs []int32
	if lay.nVarlena > 0 {
		varOffs = make([]int32, int(s.N)*lay.nVarlena)
		for ord := int64(0); ord < s.N; ord++ {
			for vi := 0; vi < lay.nVarlena; vi++ {
				if pos >= len(data) {
					return moerr.NewInternalErrorNoCtx("fulltext2: docmap truncated include varlena values")
				}
				if pos > math.MaxInt32 {
					return moerr.NewInternalErrorNoCtx("fulltext2: docmap include section exceeds 2GB")
				}
				varOffs[int(ord)*lay.nVarlena+vi] = int32(pos)
				flag := data[pos]
				pos++
				if flag == includeNull {
					continue
				}
				if flag != includeNotNull {
					return moerr.NewInternalErrorNoCtxf("fulltext2: docmap bad include null-flag %d", flag)
				}
				if pos+4 > len(data) {
					return moerr.NewInternalErrorNoCtx("fulltext2: docmap truncated include value length")
				}
				l := int(binary.LittleEndian.Uint32(data[pos:]))
				pos += 4 + l
				if pos < 0 || pos > len(data) {
					return moerr.NewInternalErrorNoCtx("fulltext2: docmap truncated include value data")
				}
			}
		}
	}
	s.includeTypes = itypes
	s.includeRaw = data
	s.includeLay = lay
	s.includeFixedStart = fixedStart
	s.includeVarOffsets = varOffs
	return nil
}

// ---- termdict (FST) + postings ----

// encodeTermsAndPostings builds the FST (term -> ordinal) and THREE posting sections
// in one pass over the sorted terms (ordinal i in the FST is posting list i):
//
//   - RANKING directory (kept RESIDENT at load): version, nterms, then per term
//     df(varint), nblk(varint), the term's absolute base offsets into the blocks/positions
//     sections + term-level maxTf/minDocLen, and per BlockSize-doc block
//     {lastDocGap(varint, from the previous block's last ord), maxTf(1 byte),
//     minDocLn(varint), blkByteLen(varint), posByteLen(varint)}. This is the Block-Max
//     skip metadata + a directory into the blocks section; it is all WAND needs to prune,
//     and it is O(df/BlockSize) — ~1/128 the postings. Each entry is SELF-CONTAINED and the
//     FST value is its BYTE OFFSET, so LookupLoaded decodes one term's directory on demand
//     (resident directory heap is O(the current query), not O(vocabulary)).
//   - BLOCKS (mmap, block-decoded on demand): per term, per block, the docID GAPS
//     (varint, from the previous block's last ord → tiny deltas) then the block's raw
//     tf bytes. The WAND cursor decodes only the blocks its walk lands on; docIDs
//     never fully expand into RAM at load (the OOM-scaling fix).
//   - POSITIONS (mmap, phrase-only): per term (same order) per doc pc(varint) +
//     position GAPS(varint). Never touched by ranking.
//
// Delta+varint shrinks docIDs/positions ~4×; blocking docIDs (vs one flat per-term
// stream) is what lets the loader keep them on the mmap and random-access one block.
//
// postingsFormatV1 is the single, current on-disk format for BOTH base index segments and
// CDC tail segments (they share this serialization). fulltext2 is experimental and
// fresh-deploy-only, so there is no migration and the version is NOT advanced as the engine
// evolves — the version byte is only a stale-blob guard. New per-doc data (e.g. INCLUDE
// column values) is added in the docmap member (presence-tagged), not by bumping this.
const postingsFormatV1 byte = 1

func (s *Segment) encodeTermsAndPostings() (fst, ranking, blocks, positions []byte, err error) {
	n := len(s.sortedTerms)
	values := make([]uint64, n)
	var w, bw, pw leBuf // w=ranking directory, bw=blocks, pw=positions
	w.b.WriteByte(postingsFormatV1)
	w.i64(int64(n))
	for i, term := range s.sortedTerms {
		tp := s.terms[term]
		if tp.blockLastDoc == nil { // ensure the block-max directory (build normally sets it)
			deriveTermStats(tp, s.docLen)
		}
		df := len(tp.docIDs)
		nblk := (df + BlockSize - 1) / BlockSize
		// Self-contained entry: the FST maps term -> this byte offset, and the entry
		// carries its own blocks/positions base offsets + term-level max/min so it can be
		// decoded in isolation (no walk of prior terms).
		values[i] = uint64(w.b.Len())
		w.uvarint(uint64(df))
		w.uvarint(uint64(nblk))
		w.uvarint(uint64(bw.b.Len())) // blockDataBase: absolute offset into the blocks section
		w.uvarint(uint64(pw.b.Len())) // posRawBase: absolute offset into the positions section
		w.b.WriteByte(tp.maxTf)       // term-level max tf (was recomputed at load in V5)
		w.uvarint(uint64(tp.minDocLen))
		var prevLast int64
		for b := 0; b < nblk; b++ {
			lo := b * BlockSize
			hi := lo + BlockSize
			if hi > df {
				hi = df
			}
			// block bytes: docID gaps (from prevLast) then raw tfs.
			blkStart := bw.b.Len()
			prev := prevLast
			for j := lo; j < hi; j++ {
				bw.uvarint(uint64(tp.docIDs[j] - prev))
				prev = tp.docIDs[j]
			}
			bw.b.Write(tp.tfs[lo:hi])
			// this block's positions (parallel to its docs) — grouped per block so the
			// directory can record each block's byte length, making positions block-
			// SEEKABLE for the phrase cursor (posRaw bytes are unchanged vs V4: still
			// per-doc pc + ascending gaps in doc order, so materializePositions is
			// unaffected). V4 wrote them once per term after all blocks.
			posStart := pw.b.Len()
			if tp.positions != nil { // position-free segments omit the positional payload
				for j := lo; j < hi; j++ {
					pos := tp.positions[j]
					pw.uvarint(uint64(len(pos)))
					var pp int32
					for _, p := range pos { // ascending positions → non-negative gaps
						pw.uvarint(uint64(p - pp))
						pp = p
					}
				}
			}
			// directory entry.
			w.uvarint(uint64(tp.blockLastDoc[b] - prevLast)) // lastDocGap
			w.b.WriteByte(tp.blockMaxTf[b])
			w.uvarint(uint64(tp.blockMinDocLn[b]))
			w.uvarint(uint64(bw.b.Len() - blkStart)) // this block's docID/tf byte length
			w.uvarint(uint64(pw.b.Len() - posStart)) // this block's positions byte length (V5)
			prevLast = tp.blockLastDoc[b]
		}
	}
	if fst, err = buildTermDictFST(s.sortedTerms, values); err != nil {
		return nil, nil, nil, nil, err
	}
	return fst, w.b.Bytes(), bw.b.Bytes(), pw.b.Bytes(), nil
}

// decodePostings BINDS the loaded segment to its (mmap'd) ranking/blocks/positions
// sections WITHOUT expanding any term. V6 entries are self-contained and reachable by
// byte offset (the FST value), so terms decode lazily in LookupLoaded — the resident
// directory heap is O(the current query), not O(vocabulary).
func (s *Segment) decodePostings(ranking, blocks, positions []byte) error {
	if len(ranking) == 0 {
		s.ranking, s.blocks, s.positions = nil, nil, nil
		return nil
	}
	if len(ranking) < 1+8 {
		return moerr.NewInternalErrorNoCtx("fulltext2: ranking blob too short")
	}
	if ranking[0] != postingsFormatV1 {
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("fulltext2: unsupported postings format %d", ranking[0]))
	}
	s.ranking, s.blocks, s.positions = ranking, blocks, positions
	return nil
}

// decodeTermEntry lazily decodes ONE term's self-contained directory entry at byte
// offset `off` in s.ranking (the FST value) into a transient termPostings whose
// blockData/posRaw are views into s.blocks/s.positions. Callers hold the result for the
// query's lifetime (WAND/phrase cursors, evalClause), so the entry decodes at most a few
// times per query and never persists — resident directory heap is O(query), not O(vocab).
// Returns (nil,false) on a corrupt/out-of-bounds entry (defense-in-depth on already-CRC'd
// data).
func (s *Segment) decodeTermEntry(off int) (*termPostings, bool) {
	r := s.ranking
	if off < 0 || off >= len(r) {
		return nil, false
	}
	p := off
	uv := func() (uint64, bool) {
		v, n := binary.Uvarint(r[p:])
		if n <= 0 {
			return 0, false
		}
		p += n
		return v, true
	}
	dfu, ok := uv()
	if !ok {
		return nil, false
	}
	nblku, ok := uv()
	if !ok {
		return nil, false
	}
	baseB, ok := uv() // blockDataBase: absolute offset into s.blocks
	if !ok {
		return nil, false
	}
	baseP, ok := uv() // posRawBase: absolute offset into s.positions
	if !ok {
		return nil, false
	}
	if p >= len(r) {
		return nil, false
	}
	termMaxTf := r[p]
	p++
	minDLu, ok := uv()
	if !ok {
		return nil, false
	}
	// A df/nblk larger than the blocks section is impossible (each posting is >= 1 byte).
	if dfu > uint64(len(s.blocks)) || nblku > uint64(len(s.blocks)) {
		return nil, false
	}
	df, nblk := int(dfu), int(nblku)
	tp := &termPostings{ndoc: df, maxTf: termMaxTf, minDocLen: int32(minDLu)}
	if nblk == 0 {
		return tp, true
	}
	tp.blockLastDoc = make([]int64, nblk)
	tp.blockMaxTf = make([]uint8, nblk)
	tp.blockMinDocLn = make([]int32, nblk)
	tp.blockOff = make([]int64, nblk+1)    // byte offsets RELATIVE to this term's blockData
	tp.blockPosOff = make([]int64, nblk+1) // byte offsets RELATIVE to this term's posRaw
	var prevLast, cb, cpos int64
	for b := 0; b < nblk; b++ {
		gap, ok := uv()
		if !ok {
			return nil, false
		}
		prevLast += int64(gap)
		tp.blockLastDoc[b] = prevLast
		if p >= len(r) {
			return nil, false
		}
		tp.blockMaxTf[b] = r[p]
		p++
		mdl, ok := uv()
		if !ok {
			return nil, false
		}
		tp.blockMinDocLn[b] = int32(mdl)
		blkLen, ok := uv()
		if !ok || blkLen > uint64(len(s.blocks)) {
			return nil, false
		}
		tp.blockOff[b] = cb
		cb += int64(blkLen)
		posLen, ok := uv()
		if !ok || posLen > uint64(len(s.positions)) {
			return nil, false
		}
		tp.blockPosOff[b] = cpos
		cpos += int64(posLen)
	}
	tp.blockOff[nblk] = cb
	tp.blockPosOff[nblk] = cpos
	// blockData/posRaw are views into the mmap sections at this term's stored bases.
	if int64(baseB)+cb > int64(len(s.blocks)) || int64(baseP)+cpos > int64(len(s.positions)) {
		return nil, false
	}
	tp.blockData = s.blocks[baseB : int64(baseB)+cb]
	tp.posRaw = s.positions[baseP : int64(baseP)+cpos]
	return tp, true
}

// deriveTermStats fills tp's raw, scorer-agnostic score-UB fields from its
// postings + docLen: the term-level maxTf/minDocLen AND the per-block Block-Max
// skip metadata (blockLastDoc/blockMaxTf/blockMinDocLn), one entry per
// ceil(df/BlockSize). Both are idf/avgdl-free so one segment serves BM25 and
// TF-IDF; the active scorer derives its impact bound at query time. Computed in a
// single pass (the term-level max/min is the max/min over the block stats), and
// called from BOTH the builder and Deserialize — the direct analogue of bm25's
// WandModel.finalizeScoring. Assumes df >= 1 (a term with no postings is never
// stored).
func deriveTermStats(tp *termPostings, docLen []int32) {
	df := len(tp.docIDs)
	tp.ndoc = df
	if df == 0 {
		return
	}
	nblk := (df + BlockSize - 1) / BlockSize
	tp.blockLastDoc = make([]int64, nblk)
	tp.blockMaxTf = make([]uint8, nblk)
	tp.blockMinDocLn = make([]int32, nblk)
	var termMaxTf uint8
	termMinDL := int32(math.MaxInt32)
	for b := 0; b < nblk; b++ {
		lo := b * BlockSize
		hi := lo + BlockSize
		if hi > df {
			hi = df
		}
		var maxTf uint8
		minDL := int32(math.MaxInt32)
		for i := lo; i < hi; i++ {
			if tp.tfs[i] > maxTf {
				maxTf = tp.tfs[i]
			}
			if dl := docLen[tp.docIDs[i]]; dl < minDL {
				minDL = dl
			}
		}
		tp.blockLastDoc[b] = tp.docIDs[hi-1] // ascending → last is the block's max ord
		tp.blockMaxTf[b] = maxTf
		tp.blockMinDocLn[b] = minDL
		if maxTf > termMaxTf {
			termMaxTf = maxTf
		}
		if minDL < termMinDL {
			termMinDL = minDL
		}
	}
	tp.maxTf = termMaxTf
	tp.minDocLen = termMinDL
}

// LookupLoaded resolves a term to its posting list on a LOADED segment (via the
// FST ordinal). Returns (nil, false) if the segment is not loaded or the term is
// absent. Query-side counterpart of the build-side Lookup.
func (s *Segment) LookupLoaded(term string) (*termPostings, bool) {
	if s.dict == nil {
		return nil, false
	}
	off, ok, err := s.dict.get(term) // V6: the FST value is the entry's byte offset in ranking
	if err != nil || !ok {
		return nil, false
	}
	return s.decodeTermEntry(int(off))
}
