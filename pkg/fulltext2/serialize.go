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
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
	s.N = int64(len(s.pks))
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

// ---- docmap: pkType + N + ord->pk + ord->docLen ----

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
	return w.b.Bytes(), nil
}

func (s *Segment) decodeDocmap(data []byte) error {
	r := bytes.NewReader(data)
	if err := binary.Read(r, binary.LittleEndian, &s.PkType); err != nil {
		return err
	}
	var n int64
	if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
		return err
	}
	s.pks = make([]any, n)
	for i := int64(0); i < n; i++ {
		var l uint32
		if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
			return err
		}
		raw := make([]byte, l)
		if _, err := io.ReadFull(r, raw); err != nil {
			return err
		}
		v, err := decodePk(s.PkType, raw)
		if err != nil {
			return err
		}
		s.pks[i] = v
	}
	s.docLen = make([]int32, n)
	if err := binary.Read(r, binary.LittleEndian, s.docLen); err != nil {
		return err
	}
	return nil
}

// ---- termdict (FST) + postings ----

// encodeTermsAndPostings builds the FST (term -> ordinal) and THREE posting sections
// in one pass over the sorted terms (ordinal i in the FST is posting list i):
//
//   - RANKING directory (postingsFormatV4, kept RESIDENT at load): version, nterms,
//     then per term df(varint), nblk(varint), and per BlockSize-doc block
//     {lastDocGap(varint, from the previous block's last ord), maxTf(1 byte),
//     minDocLn(varint), blkByteLen(varint)}, then posByteLen(varint). This is the
//     Block-Max skip metadata + a directory into the blocks section; it is all WAND
//     needs to prune, and it is O(df/BlockSize) — ~1/128 the postings.
//   - BLOCKS (mmap, block-decoded on demand): per term, per block, the docID GAPS
//     (varint, from the previous block's last ord → tiny deltas) then the block's raw
//     tf bytes. The WAND cursor decodes only the blocks its walk lands on; docIDs
//     never fully expand into RAM at load (the OOM-scaling fix).
//   - POSITIONS (mmap, phrase-only): per term (same order) per doc pc(varint) +
//     position GAPS(varint). Never touched by ranking.
//
// Delta+varint shrinks docIDs/positions ~4×; blocking docIDs (vs one flat per-term
// stream) is what lets the loader keep them on the mmap and random-access one block.
// fulltext2 is experimental, so the old layout is dropped (nothing to migrate); a
// version byte guards a stale blob.
const postingsFormatV4 byte = 4

func (s *Segment) encodeTermsAndPostings() (fst, ranking, blocks, positions []byte, err error) {
	n := len(s.sortedTerms)
	values := make([]uint64, n)
	var w, bw, pw leBuf // w=ranking directory, bw=blocks, pw=positions
	w.b.WriteByte(postingsFormatV4)
	w.i64(int64(n))
	for i, term := range s.sortedTerms {
		values[i] = uint64(i)
		tp := s.terms[term]
		if tp.blockLastDoc == nil { // ensure the block-max directory (build normally sets it)
			deriveTermStats(tp, s.docLen)
		}
		df := len(tp.docIDs)
		nblk := (df + BlockSize - 1) / BlockSize
		w.uvarint(uint64(df))
		w.uvarint(uint64(nblk))
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
			// directory entry.
			w.uvarint(uint64(tp.blockLastDoc[b] - prevLast)) // lastDocGap
			w.b.WriteByte(tp.blockMaxTf[b])
			w.uvarint(uint64(tp.blockMinDocLn[b]))
			w.uvarint(uint64(bw.b.Len() - blkStart)) // this block's byte length
			prevLast = tp.blockLastDoc[b]
		}
		before := pw.b.Len()
		for _, pos := range tp.positions {
			pw.uvarint(uint64(len(pos)))
			var pp int32
			for _, p := range pos { // ascending positions → non-negative gaps
				pw.uvarint(uint64(p - pp))
				pp = p
			}
		}
		w.uvarint(uint64(pw.b.Len() - before)) // this term's positions byte length
	}
	if fst, err = buildTermDictFST(s.sortedTerms, values); err != nil {
		return nil, nil, nil, nil, err
	}
	return fst, w.b.Bytes(), bw.b.Bytes(), pw.b.Bytes(), nil
}

// decodePostings loads the ranking DIRECTORY into s.loaded (indexed by ordinal):
// the per-block Block-Max skip metadata (blockLastDoc/blockMaxTf/blockMinDocLn) and
// a byte-offset directory (blockOff) into the blocks section expand RESIDENT, while
// each term's docID/tf blocks (blockData) and compressed positions (posRaw) stay as
// views into the mmap — decoded on demand (the WAND cursor decodes one block; cold
// paths materialize). No off-heap allocation and no re-derivation: the score-UB
// fields are read straight from the directory the builder wrote.
func (s *Segment) decodePostings(ranking, blocks, positions []byte) error {
	if len(ranking) < 1+8 {
		if len(ranking) == 0 {
			s.loaded = nil
			return nil
		}
		return moerr.NewInternalErrorNoCtx("fulltext2: ranking blob too short")
	}
	if ranking[0] != postingsFormatV4 {
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("fulltext2: unsupported postings format %d", ranking[0]))
	}
	p := 1
	nterms := int64(binary.LittleEndian.Uint64(ranking[p:]))
	p += 8

	uv := func() (uint64, bool) {
		v, n := binary.Uvarint(ranking[p:])
		if n <= 0 {
			return 0, false
		}
		p += n
		return v, true
	}
	rb := func() (uint8, bool) { // read one raw byte from the directory
		if p >= len(ranking) {
			return 0, false
		}
		v := ranking[p]
		p++
		return v, true
	}
	bad := func() error { return moerr.NewInternalErrorNoCtx("fulltext2: corrupt postings stream") }

	s.loaded = make([]*termPostings, nterms)
	var cb, cpos int64 // running offsets into the blocks and positions sections
	for t := int64(0); t < nterms; t++ {
		dfu, ok := uv()
		if !ok {
			return bad()
		}
		nblku, ok := uv()
		if !ok {
			return bad()
		}
		df, nblk := int(dfu), int(nblku)
		tp := &termPostings{ndoc: df}
		if nblk > 0 {
			tp.blockLastDoc = make([]int64, nblk)
			tp.blockMaxTf = make([]uint8, nblk)
			tp.blockMinDocLn = make([]int32, nblk)
			tp.blockOff = make([]int32, nblk+1)
		}
		blkBase := cb
		var prevLast int64
		var termMaxTf uint8
		termMinDL := int32(math.MaxInt32)
		for b := 0; b < nblk; b++ {
			gap, ok := uv()
			if !ok {
				return bad()
			}
			prevLast += int64(gap)
			tp.blockLastDoc[b] = prevLast
			maxTf, ok := rb()
			if !ok {
				return bad()
			}
			tp.blockMaxTf[b] = maxTf
			minDL, ok := uv()
			if !ok {
				return bad()
			}
			tp.blockMinDocLn[b] = int32(minDL)
			blkLen, ok := uv()
			if !ok {
				return bad()
			}
			tp.blockOff[b] = int32(cb - blkBase)
			cb += int64(blkLen)
			if maxTf > termMaxTf {
				termMaxTf = maxTf
			}
			if int32(minDL) < termMinDL {
				termMinDL = int32(minDL)
			}
		}
		if nblk > 0 {
			tp.blockOff[nblk] = int32(cb - blkBase)
			if cb > int64(len(blocks)) {
				return bad()
			}
			tp.blockData = blocks[blkBase:cb]
			tp.maxTf = termMaxTf
			tp.minDocLen = termMinDL
		}
		posLen, ok := uv() // byte length of this term's compressed positions
		if !ok || cpos+int64(posLen) > int64(len(positions)) {
			return bad()
		}
		tp.posRaw = positions[cpos : cpos+int64(posLen)]
		cpos += int64(posLen)
		s.loaded[t] = tp
	}
	return nil
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
	ord, ok, err := s.dict.get(term)
	if err != nil || !ok || ord >= uint64(len(s.loaded)) {
		return nil, false
	}
	return s.loaded[ord], true
}

// ---- pk codec (by types.T) ----
//
// NOTE: byte-for-byte identical to pkg/bm25/wand's pk codec — both engines store
// any-typed source PKs the same way (SQL-CDC delivers uuid/temporal/decimal
// consistently; see that file's rationale). Duplicated here to avoid exporting it
// out of package wand for now; extract to a shared pk-codec package once this
// stabilizes (rule of three — bm25 + fulltext2 are the first two users).

func (w *leBuf) encodePkLen(pkType int32, v any) error {
	switch types.T(pkType) {
	case types.T_int64:
		w.u32(8)
		w.u64(uint64(v.(int64)))
	case types.T_uint64:
		w.u32(8)
		w.u64(v.(uint64))
	case types.T_int32:
		w.u32(4)
		w.u32(uint32(v.(int32)))
	case types.T_uint32:
		w.u32(4)
		w.u32(v.(uint32))
	case types.T_varchar, types.T_char, types.T_text, types.T_datalink,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_json:
		raw := asBytes(v)
		w.u32(uint32(len(raw)))
		w.b.Write(raw)
	default:
		pkb, err := encodePk(pkType, v)
		if err != nil {
			return err
		}
		w.u32(uint32(len(pkb)))
		w.b.Write(pkb)
	}
	return nil
}

func encodePk(pkType int32, v any) ([]byte, error) {
	switch types.T(pkType) {
	case types.T_int64:
		return packUint64(uint64(v.(int64))), nil
	case types.T_uint64:
		return packUint64(v.(uint64)), nil
	case types.T_int32:
		return packUint32(uint32(v.(int32))), nil
	case types.T_uint32:
		return packUint32(v.(uint32)), nil
	case types.T_varchar, types.T_char, types.T_text, types.T_datalink,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_json:
		return asBytes(v), nil
	case types.T_uuid:
		switch x := v.(type) {
		case string:
			return []byte(x), nil
		case types.Uuid:
			return []byte(x.String()), nil
		case []byte:
			return append([]byte(nil), x...), nil
		default:
			return nil, moerr.NewInternalErrorNoCtxf("fulltext2: uuid pk unexpected go type %T", v)
		}
	case types.T_date:
		return packUint32(uint32(int32(v.(types.Date)))), nil
	case types.T_datetime:
		return packUint64(uint64(int64(v.(types.Datetime)))), nil
	case types.T_time:
		return packUint64(uint64(int64(v.(types.Time)))), nil
	case types.T_timestamp:
		return packUint64(uint64(int64(v.(types.Timestamp)))), nil
	case types.T_decimal64:
		return packUint64(uint64(v.(types.Decimal64))), nil
	case types.T_decimal128:
		d := v.(types.Decimal128)
		b := make([]byte, 16)
		binary.LittleEndian.PutUint64(b[0:8], d.B0_63)
		binary.LittleEndian.PutUint64(b[8:16], d.B64_127)
		return b, nil
	default:
		return nil, moerr.NewInternalErrorNoCtxf("fulltext2: unsupported pk type %d", pkType)
	}
}

func decodePk(pkType int32, b []byte) (any, error) {
	switch types.T(pkType) {
	case types.T_int64:
		return int64(binary.LittleEndian.Uint64(b)), nil
	case types.T_uint64:
		return binary.LittleEndian.Uint64(b), nil
	case types.T_int32:
		return int32(binary.LittleEndian.Uint32(b)), nil
	case types.T_uint32:
		return binary.LittleEndian.Uint32(b), nil
	case types.T_varchar, types.T_char, types.T_text, types.T_datalink,
		types.T_binary, types.T_varbinary, types.T_blob, types.T_json:
		return append([]byte(nil), b...), nil
	case types.T_uuid:
		u, err := types.ParseUuid(string(b))
		if err != nil {
			return nil, err
		}
		return u, nil
	case types.T_date:
		return types.Date(int32(binary.LittleEndian.Uint32(b))), nil
	case types.T_datetime:
		return types.Datetime(int64(binary.LittleEndian.Uint64(b))), nil
	case types.T_time:
		return types.Time(int64(binary.LittleEndian.Uint64(b))), nil
	case types.T_timestamp:
		return types.Timestamp(int64(binary.LittleEndian.Uint64(b))), nil
	case types.T_decimal64:
		return types.Decimal64(binary.LittleEndian.Uint64(b)), nil
	case types.T_decimal128:
		return types.Decimal128{
			B0_63:   binary.LittleEndian.Uint64(b[0:8]),
			B64_127: binary.LittleEndian.Uint64(b[8:16]),
		}, nil
	default:
		return nil, moerr.NewInternalErrorNoCtxf("fulltext2: unsupported pk type %d", pkType)
	}
}

func packUint64(v uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v)
	return b
}

func packUint32(v uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, v)
	return b
}

func asBytes(v any) []byte {
	switch x := v.(type) {
	case []byte:
		return x
	case string:
		return []byte(x)
	default:
		return nil
	}
}
